"""
categorise_das.py
-----------------
Stage 4 of the FAIR longitudinal study pipeline.

Categorises every data availability statement (DAS) in the
`data_availability` table using rule-based text analysis.
Also covers articles with NO explicit DAS by checking whether they
have supplementary materials or accession numbers (the pre-2016 proxies).

Primary categories (one per article, mutually exclusive, ranked by
data-sharing transparency)
----------------------------------------------------------------------
  repository_named    - DAS explicitly names a repository AND provides a
                        link or accession number.  Highest FAIR compliance.
                        Examples: "deposited in Zenodo (doi:10.5281/...)"
                                  "available at GEO (GSE123456)"

  accession_provided  - DAS mentions a database accession number but may
                        not name the repo or provide a URL.
                        Examples: "data are available as GSE123456"

  open_unspecified    - DAS states data are open/public but gives no
                        specific location or accession.
                        Examples: "all data are publicly available"
                                  "data will be shared upon reasonable request"
                                  [but without the anti-FAIR phrasing]

  supplementary_only  - Data is stated to be in supplementary files only.
                        Examples: "data are provided as supplementary tables"

  available_on_request - The anti-FAIR pattern. Data exist but require
                         contacting the authors.
                         Examples: "available upon request"
                                   "available from the corresponding author"

  restricted          - Data cannot be freely shared due to ethics,
                        privacy, legal, or commercial reasons.
                        Examples: "data are not publicly available due to
                                   patient privacy", "proprietary data"

  no_statement        - No DAS found AND no supplementary material AND
                        no accession numbers. Article gives no data-sharing
                        signal at all.

  no_das_but_supp     - No explicit DAS, but article has supplementary
                        materials (pre-2016 primary sharing mode).

  no_das_but_accession - No explicit DAS, but article has inline accession
                         numbers (e.g., GEO, SRA). Common in genomics.

Secondary boolean signals (stored alongside primary category)
-------------------------------------------------------------
  mentions_license    - DAS mentions a license (CC-BY, CC0, MIT, GPL…)
  mentions_doi        - DAS contains a DOI
  mentions_github     - DAS references GitHub/GitLab
  has_url             - DAS contains at least one URL
  has_accession       - DAS contains a database accession number pattern
  word_count          - token count of the DAS text (quality proxy)
  sentence_count      - number of sentences

Data-sharing score (0–10, computed from category + secondary signals)
-----------------------------------------------------------------------
  repository_named   base=8  + license +1, doi +1            → max 10
  accession_provided base=7  + license +1, doi +1            → max  9
  open_unspecified   base=4  + url +1, license +1            → max  6
  supplementary_only base=3  + doi +1                        → max  4
  available_on_request base=1                                → max  1
  restricted         base=2                                  → max  2
  no_statement       base=0                                  → max  0
  no_das_but_supp    base=2                                  → max  2
  no_das_but_accession base=6 + license +1                   → max  7

This is deliberately NOT called a "FAIR score": it only reflects how
transparently data sharing is disclosed in the DAS text (category +
license/DOI mentions), not an assessment of the FAIR principles
(Findability, Accessibility, Interoperability, Reusability) themselves,
and it is not comparable to F-UJI's (Stage 5) metric-based FAIR
assessment. It is intentionally simple and transparent, giving an
immediately queryable proxy for data-sharing trends over time.

Schema
------
  das_category  - one row per article  (includes articles without DAS)

Usage
-----
  python categorise_das.py --db pmc_fair.db
  python categorise_das.py --db pmc_fair.db --reset
  python categorise_das.py --db pmc_fair.db --report
  python categorise_das.py --db pmc_fair.db --report --year 2016
"""

import argparse
import logging
import re
import resource
import sqlite3
import sys
import time
from dataclasses import dataclass
from pathlib import Path

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

LOG_FMT  = "%(asctime)s  %(levelname)-7s  %(message)s"
LOG_DATE = "%Y-%m-%d %H:%M:%S"


def setup_logging(log_path: Path) -> logging.Logger:
    logger = logging.getLogger("categorise_das")
    logger.setLevel(logging.DEBUG)
    logger.handlers.clear()
    fmt = logging.Formatter(LOG_FMT, datefmt=LOG_DATE)
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(fmt)
    logger.addHandler(ch)
    fh = logging.FileHandler(log_path, encoding="utf-8", mode="a")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)
    logger.addHandler(fh)
    return logger


def _peak_rss_mb() -> float:
    try:
        kb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        return kb / 1024 if sys.platform != "darwin" else kb / (1024 * 1024)
    except Exception:
        return 0.0


# ---------------------------------------------------------------------------
# Category constants
# ---------------------------------------------------------------------------

CAT_REPO_NAMED       = "repository_named"
CAT_ACCESSION        = "accession_provided"
CAT_OPEN_UNSPEC      = "open_unspecified"
CAT_SUPP_ONLY        = "supplementary_only"
CAT_ON_REQUEST       = "available_on_request"
CAT_RESTRICTED       = "restricted"
CAT_NO_STATEMENT     = "no_statement"
CAT_NO_DAS_SUPP      = "no_das_but_supp"
CAT_NO_DAS_ACC       = "no_das_but_accession"

# Data-sharing score base values per category
SCORE_BASE: dict[str, int] = {
    CAT_REPO_NAMED:   8,
    CAT_ACCESSION:    7,
    CAT_OPEN_UNSPEC:  4,
    CAT_SUPP_ONLY:    3,
    CAT_ON_REQUEST:   1,
    CAT_RESTRICTED:   2,
    CAT_NO_STATEMENT: 0,
    CAT_NO_DAS_SUPP:  2,
    CAT_NO_DAS_ACC:   6,
}


# ---------------------------------------------------------------------------
# Regex patterns
# ---------------------------------------------------------------------------

# ── Known repository names ───────────────────────────────────────────────
_REPO_NAMES = re.compile(
    r"\b("
    r"zenodo|figshare|dryad|dataverse|mendeley\s+data|osf|open\s+science\s+framework|"
    r"pangaea|re3data|b2share|openicpsr|icpsr|ukdata|gesis|"
    r"geo|gene\s+expression\s+omnibus|sra|sequence\s+read\s+archive|"
    r"arrayexpress|pride|proteomexchange|ena|european\s+nucleotide|"
    r"uniprot|pdb|protein\s+data\s+bank|wwpdb|emdb|"
    r"biosample|bioproject|dbgap|genbank|refseq|"
    r"metabolights|metabolomics\s+workbench|"
    r"openneuro|neurovault|ieeg|"
    r"harvard\s+dataverse|github|gitlab|bitbucket|gigadb|"
    r"code\s+ocean|zenodo\.org|figshare\.com|datadryad\.org|osf\.io|"
    r"gtex|immport|idr|allen\s+(brain|institute)|"
    r"nasa|noaa|pangaea|iodp|iris\s+federation|unavco|"
    r"clinicaltrials\.gov|ebi\.ac\.uk|ncbi\.nlm"
    r")\b",
    re.IGNORECASE,
)

# ── Accession number patterns ─────────────────────────────────────────────
_ACCESSION_PAT = re.compile(
    r"\b("
    r"GSE\d{3,8}|GDS\d{3,7}|GPL\d{3,7}|GSM\d{4,9}|"       # GEO
    r"SRP\d{5,9}|SRR\d{5,9}|SRX\d{5,9}|SRS\d{5,9}|"       # SRA
    r"ERP\d{5,9}|ERR\d{5,9}|"                               # ENA
    r"E-[A-Z]{4}-\d{4,6}|"                                  # ArrayExpress
    r"phs\d{6}|"                                             # dbGaP
    r"PXD\d{6}|"                                             # PRIDE
    r"PRJ(?:NA|EB|DB|CA)\d{4,9}|"                           # BioProject
    r"SAM[NED]\d{6,9}|"                                     # BioSample
    r"pdb\s*(?:id|code|accession|entry)?\s*[:#]?\s*[1-9][a-z][a-z0-9]{2}\b|"  # PDB (context-anchored on the word "pdb" to avoid matching arbitrary 4-char tokens)
    r"[A-Z]{1,2}\d{5,8}|"                                   # GenBank
    r"[OPQ][0-9][A-Z0-9]{3}[0-9]"                           # UniProt
    r")\b",
    re.IGNORECASE,
)

# ── DOI pattern ───────────────────────────────────────────────────────────
_DOI_PAT = re.compile(
    r"(?:doi\.org/|doi:\s*|https?://doi\.org/)10\.\d{4,}/\S+",
    re.IGNORECASE,
)

# ── URL pattern ───────────────────────────────────────────────────────────
_URL_PAT = re.compile(r"https?://\S+", re.IGNORECASE)

# ── License pattern ───────────────────────────────────────────────────────
_LICENSE_PAT = re.compile(
    r"\b(cc[\s\-]?by|cc[\s\-]?0|cc[\s\-]?zero|creative\s+commons|"
    r"mit\s+licen[sc]e|apache\s+licen[sc]e|gpl|"
    r"open\s+data\s+commons|odc|pddl|"
    r"licen[sc]ed?\s+under)\b",
    re.IGNORECASE,
)

# ── GitHub / GitLab ───────────────────────────────────────────────────────
_GITHUB_PAT = re.compile(r"\bgithub\.com|gitlab\.com\b", re.IGNORECASE)

# ── Available on request (anti-FAIR) ─────────────────────────────────────
# "available from the corresponding author" is anti-FAIR only when it implies
# restricted access. Negative lookbehind prevents false positives like
# "publicly available from the corresponding author website".
_ON_REQUEST_PAT = re.compile(
    r"\b("
    r"available\s+(?:upon|on)\s+(?:reasonable\s+)?request|"
    r"upon\s+(?:reasonable\s+)?request|"
    r"available\s+from\s+(?:the\s+)?(?:corresponding\s+)?authors?"
    r"(?!\s+(?:website|lab|group|page|site|s\s+(?:website|lab|group|page)))|"
    r"request(?:ed|ing)?\s+(?:the\s+)?(?:raw\s+)?data\s+from|"
    r"contact(?:ing)?\s+(?:the\s+)?(?:corresponding\s+)?authors?\s+(?:for|to\s+obtain|to\s+request)|"
    r"can\s+be\s+(?:made\s+)?available\s+(?:upon|on)\s+(?:reasonable\s+)?request|"
    r"data\s+not\s+(?:publicly\s+)?available\s+but"
    r")\b",
    re.IGNORECASE,
)

# Override: if text contains "publicly/freely/openly available" before the
# "from author" phrase, treat it as open rather than on-request.
_OPENLY_AVAIL_PAT = re.compile(
    r"(?:publicly|freely|openly)\s+available",
    re.IGNORECASE,
)

# ── Restricted / cannot share ────────────────────────────────────────────
_RESTRICTED_PAT = re.compile(
    r"\b("
    r"not\s+(?:publicly\s+)?(?:available|shared|deposited)|"
    r"cannot\s+be\s+(?:publicly\s+)?shared|"
    r"(?:patient|participant|subject)\s+(?:privacy|confidential)|"
    r"(?:privacy|confidential(?:ity)?|sensitive)\s+(?:data|information|concern)|"
    r"data\s+(?:protection|governance|transfer\s+agreement)|"
    r"proprietary|embargoed?|under\s+embargo|"
    r"third[\s\-]party|institutional\s+restrictions|"
    r"ethic[as]\s+(?:committee|approval|restriction)|"
    r"(?:legal|regulatory)\s+restrict|"
    r"not\s+available\s+(?:for|to)\s+(?:the\s+)?public|"
    r"data\s+(?:are\s+)?(?:not\s+)?available\s+upon\s+(?:reasonable\s+)?request.*privacy"
    r")\b",
    re.IGNORECASE,
)

# ── Supplementary-only signal ────────────────────────────────────────────
# NOTE: No leading \b — optional (?:as\s+)? groups prevent \b from working
# correctly when the match starts mid-phrase (e.g. "provided as supplementary")
_SUPP_ONLY_PAT = re.compile(
    r"("
    r"(?:as\s+)?supplementary\s+(?:data|material|table|figure|file|information)|"
    r"(?:as\s+)?supporting\s+(?:information|data|material)|"
    r"(?:as\s+)?additional\s+(?:file|data|material)|"
    r"provided\s+(?:as\s+)?supplement|"
    r"supplement(?:ary)?\s+(?:data|file)"
    r")",
    re.IGNORECASE,
)

# ── Open/publicly available (unspecified location) ────────────────────────
_OPEN_UNSPEC_PAT = re.compile(
    r"\b("
    r"(?:all\s+)?(?:data\s+(?:are\s+|is\s+))?(?:freely\s+|fully\s+|publicly\s+|openly\s+)"
    r"available|"
    r"open(?:ly)?\s+(?:accessible|available|shared|access)|"
    r"data\s+(?:are\s+)?(?:available|accessible)\s+(?:at|from|in|on)\b|"
    r"data\s+(?:have\s+been\s+)?(?:deposited|uploaded|submitted|released|archived)|"
    r"(?:raw\s+)?data\s+(?:have\s+been\s+)?made\s+(?:publicly\s+)?available|"
    r"data\s+sharing"
    r")\b",
    re.IGNORECASE,
)

# Sentence splitter (simple)
_SENT_SPLIT = re.compile(r"[.!?]+\s+")


# ---------------------------------------------------------------------------
# DAS result dataclass
# ---------------------------------------------------------------------------

@dataclass
class DASCategory:
    article_id:        int
    primary_category:  str  = CAT_NO_STATEMENT
    data_sharing_score: int = 0
    mentions_license:  int  = 0
    mentions_doi:      int  = 0
    mentions_github:   int  = 0
    has_url:           int  = 0
    has_accession:     int  = 0
    word_count:        int  = 0
    sentence_count:    int  = 0
    notes:             str  = ""   # brief human-readable justification


# ---------------------------------------------------------------------------
# Core categorisation logic
# ---------------------------------------------------------------------------

def _secondary_signals(text: str) -> dict:
    """Extract secondary boolean signals from DAS text."""
    return {
        "mentions_license": int(bool(_LICENSE_PAT.search(text))),
        "mentions_doi":     int(bool(_DOI_PAT.search(text))),
        "mentions_github":  int(bool(_GITHUB_PAT.search(text))),
        "has_url":          int(bool(_URL_PAT.search(text))),
        "has_accession":    int(bool(_ACCESSION_PAT.search(text))),
        "word_count":       len(text.split()),
        "sentence_count":   max(1, len(_SENT_SPLIT.split(text.strip()))),
    }


def _compute_sharing_score(category: str, signals: dict) -> int:
    """Compute data-sharing score (0–10) from base + secondary bonuses."""
    base = SCORE_BASE.get(category, 0)
    bonus = 0
    if category in (CAT_REPO_NAMED, CAT_ACCESSION, CAT_NO_DAS_ACC):
        bonus += signals["mentions_license"]
        bonus += signals["mentions_doi"]
    elif category == CAT_OPEN_UNSPEC:
        bonus += signals["has_url"]
        bonus += signals["mentions_license"]
    elif category == CAT_SUPP_ONLY:
        bonus += signals["mentions_doi"]
    return min(10, base + bonus)


def categorise_das_text(article_id: int, text: str) -> DASCategory:
    """
    Classify a DAS text string into a primary category.
    Rules are evaluated in priority order (most specific → least specific).
    """
    result  = DASCategory(article_id=article_id)
    signals = _secondary_signals(text)

    # Populate secondary signals
    result.mentions_license = signals["mentions_license"]
    result.mentions_doi     = signals["mentions_doi"]
    result.mentions_github  = signals["mentions_github"]
    result.has_url          = signals["has_url"]
    result.has_accession    = signals["has_accession"]
    result.word_count       = signals["word_count"]
    result.sentence_count   = signals["sentence_count"]

    has_repo_name  = bool(_REPO_NAMES.search(text))
    has_accession  = bool(_ACCESSION_PAT.search(text))
    has_url        = bool(_URL_PAT.search(text))
    is_restricted  = bool(_RESTRICTED_PAT.search(text))
    is_on_request  = bool(_ON_REQUEST_PAT.search(text))
    # Suppress on_request if text also signals open/public availability
    # (e.g. "publicly available from the corresponding author website")
    if is_on_request and bool(_OPENLY_AVAIL_PAT.search(text)):
        is_on_request = False
    is_supp_only   = bool(_SUPP_ONLY_PAT.search(text))
    is_open        = bool(_OPEN_UNSPEC_PAT.search(text))

    # ── Priority 1: restricted (trumps repo/accession/url mentions — a cited
    #    repository or accession commonly refers to a derived/summary dataset
    #    while the primary data stays restricted, e.g. "individual patient
    #    data are not available; aggregated data are in GEO GSE12345".
    #    Checked empirically: gating this on an "openly available" phrase
    #    anywhere in the (often multi-sentence) statement over-triggers, since
    #    that phrase frequently describes something else in the same text
    #    (e.g. a cited report). So restriction language wins unconditionally.
    if is_restricted:
        result.primary_category = CAT_RESTRICTED
        result.notes = "Restriction signal detected"

    # ── Priority 2: repository_named (repo + link/accession = best FAIR) ──
    elif has_repo_name and (has_accession or has_url):
        result.primary_category = CAT_REPO_NAMED
        result.notes = "Named repository with link or accession"

    # ── Priority 3: accession_provided (accession present, no repo name) ──
    elif has_accession:
        result.primary_category = CAT_ACCESSION
        result.notes = "Accession number detected"

    # ── Priority 4: repository_named without accession/url ────────────────
    #    (repo is named but no concrete link — still better than unspecified)
    elif has_repo_name:
        result.primary_category = CAT_REPO_NAMED
        result.notes = "Named repository (no explicit link/accession in DAS)"

    # ── Priority 5: available on request (anti-FAIR) ──────────────────────
    #    Check before open_unspecified — some statements say "available but
    #    only on request", which is a mixed signal; request pattern wins.
    elif is_on_request:
        result.primary_category = CAT_ON_REQUEST
        result.notes = "Available-on-request pattern detected"

    # ── Priority 6: supplementary only ────────────────────────────────────
    elif is_supp_only and not is_open:
        result.primary_category = CAT_SUPP_ONLY
        result.notes = "Supplementary material only"

    # ── Priority 7: open but unspecified location ──────────────────────────
    elif is_open or has_url:
        result.primary_category = CAT_OPEN_UNSPEC
        result.notes = "Open/available statement without specific repository"

    # ── Fallback: DAS exists but we couldn't classify it ──────────────────
    else:
        result.primary_category = CAT_OPEN_UNSPEC
        result.notes = "DAS present but no strong signal; treated as open_unspecified"

    result.data_sharing_score = _compute_sharing_score(result.primary_category, signals)
    return result


def categorise_no_das(article_id: int,
                      has_supp: bool, has_acc: bool) -> DASCategory:
    """
    Assign a category to an article that has NO explicit DAS,
    based on supplementary materials and accession numbers.
    """
    result = DASCategory(article_id=article_id)
    if has_acc:
        result.primary_category = CAT_NO_DAS_ACC
        result.notes = "No DAS but has inline accession numbers"
    elif has_supp:
        result.primary_category = CAT_NO_DAS_SUPP
        result.notes = "No DAS but has supplementary materials"
    else:
        result.primary_category = CAT_NO_STATEMENT
        result.notes = "No DAS, no supplementary, no accession"
    result.data_sharing_score = SCORE_BASE[result.primary_category]
    return result


# ---------------------------------------------------------------------------
# Database schema
# ---------------------------------------------------------------------------

SCHEMA = """
CREATE TABLE IF NOT EXISTS das_category (
    cat_id            INTEGER PRIMARY KEY AUTOINCREMENT,
    article_id        INTEGER NOT NULL UNIQUE REFERENCES article(article_id),

    -- Primary classification
    primary_category  TEXT NOT NULL,
    -- Possible values:
    --   repository_named | accession_provided | open_unspecified |
    --   supplementary_only | available_on_request | restricted |
    --   no_statement | no_das_but_supp | no_das_but_accession

    -- Data-sharing transparency score (0-10) -- NOT a FAIR-principles
    -- assessment; see Stage 5 F-UJI for FAIR metric evaluation.
    data_sharing_score INTEGER NOT NULL DEFAULT 0,

    -- Secondary signals
    mentions_license  INTEGER NOT NULL DEFAULT 0,  -- 0/1
    mentions_doi      INTEGER NOT NULL DEFAULT 0,  -- 0/1
    mentions_github   INTEGER NOT NULL DEFAULT 0,  -- 0/1
    has_url           INTEGER NOT NULL DEFAULT 0,  -- 0/1
    has_accession     INTEGER NOT NULL DEFAULT 0,  -- 0/1
    word_count        INTEGER NOT NULL DEFAULT 0,
    sentence_count    INTEGER NOT NULL DEFAULT 0,

    -- Human-readable classification justification
    notes             TEXT
);

CREATE INDEX IF NOT EXISTS idx_dc_article  ON das_category(article_id);
CREATE INDEX IF NOT EXISTS idx_dc_category ON das_category(primary_category);
CREATE INDEX IF NOT EXISTS idx_dc_score    ON das_category(data_sharing_score);
"""

RESET_TABLES = ["das_category"]


# ---------------------------------------------------------------------------
# DB operations
# ---------------------------------------------------------------------------

def ensure_schema(con: sqlite3.Connection, logger: logging.Logger) -> None:
    con.executescript(SCHEMA)
    con.commit()
    logger.info("  das_category table ready.")


def get_articles_to_process(con: sqlite3.Connection) -> list[tuple]:
    """
    Return (article_id, pub_year, das_statement, has_supp, has_acc)
    for all articles not yet in das_category.
    """
    return con.execute("""
        SELECT
            a.article_id,
            a.pub_year,
            COALESCE(da.statement, '')           AS statement,
            CASE WHEN sm.article_id IS NOT NULL THEN 1 ELSE 0 END AS has_supp,
            CASE WHEN ac.article_id IS NOT NULL THEN 1 ELSE 0 END AS has_acc
        FROM article a
        LEFT JOIN data_availability  da ON da.article_id = a.article_id
        LEFT JOIN (
            SELECT DISTINCT article_id FROM supplementary_material
        ) sm ON sm.article_id = a.article_id
        LEFT JOIN (
            SELECT DISTINCT article_id FROM accession_number
        ) ac ON ac.article_id = a.article_id
        LEFT JOIN das_category dc ON dc.article_id = a.article_id
        WHERE dc.article_id IS NULL
        ORDER BY a.article_id
    """).fetchall()


def insert_categories(con: sqlite3.Connection,
                      rows: list[tuple],
                      logger: logging.Logger) -> dict:
    """
    Categorise each article and insert into das_category.
    Returns a summary dict of counts.
    """
    cur = con.cursor()
    counts: dict[str, int] = {}
    total = len(rows)

    for i, (article_id, pub_year, statement, has_supp, has_acc) in enumerate(rows, 1):
        if statement.strip():
            dc = categorise_das_text(article_id, statement)
        else:
            dc = categorise_no_das(article_id, bool(has_supp), bool(has_acc))

        cur.execute("""
            INSERT OR IGNORE INTO das_category
                (article_id, primary_category, data_sharing_score,
                 mentions_license, mentions_doi, mentions_github,
                 has_url, has_accession, word_count, sentence_count, notes)
            VALUES (?,?,?,?,?,?,?,?,?,?,?)
        """, (
            dc.article_id, dc.primary_category, dc.data_sharing_score,
            dc.mentions_license, dc.mentions_doi, dc.mentions_github,
            dc.has_url, dc.has_accession, dc.word_count, dc.sentence_count,
            dc.notes,
        ))

        counts[dc.primary_category] = counts.get(dc.primary_category, 0) + 1

        if i % 1000 == 0 or i == total:
            con.commit()
            logger.info(f"  Categorised {i:,}/{total:,} articles …")

    con.commit()
    return counts


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------

CATEGORY_ORDER = [
    CAT_REPO_NAMED, CAT_ACCESSION, CAT_NO_DAS_ACC,
    CAT_OPEN_UNSPEC, CAT_SUPP_ONLY, CAT_NO_DAS_SUPP,
    CAT_ON_REQUEST, CAT_RESTRICTED, CAT_NO_STATEMENT,
]

SCORE_LABELS = {
    (8, 10): "High",
    (5,  7): "Medium",
    (2,  4): "Low",
    (0,  1): "Minimal",
}


def print_report(con: sqlite3.Connection, logger: logging.Logger,
                 year_filter: str | None = None) -> None:
    # Filter on the authoritative derived `article.year` (derive_year.py),
    # not the raw `pub_year` column, which is inconsistently formatted.
    year_clause = f"AND a.year = {int(year_filter)}" if year_filter else ""

    logger.info("")
    logger.info("=" * 68)
    logger.info(f"DAS CATEGORISATION REPORT"
                + (f"  (year={year_filter})" if year_filter else "  (all years)"))
    logger.info("=" * 68)

    # ── Overall totals ────────────────────────────────────────────────────
    total = con.execute(f"""
        SELECT COUNT(*) FROM das_category dc
        JOIN article a ON a.article_id = dc.article_id
        WHERE 1=1 {year_clause}
    """).fetchone()[0]

    has_das = con.execute(f"""
        SELECT COUNT(*) FROM das_category dc
        JOIN article a ON a.article_id = dc.article_id
        WHERE dc.primary_category NOT IN (?,?,?)
        {year_clause}
    """, (CAT_NO_STATEMENT, CAT_NO_DAS_SUPP, CAT_NO_DAS_ACC)).fetchone()[0]

    logger.info(f"  Total articles analysed  : {total:>10,}")
    logger.info(f"  Articles with explicit DAS: {has_das:>9,}  ({has_das/total*100:.1f}%)" if total else "")

    # ── Category distribution ─────────────────────────────────────────────
    logger.info("")
    logger.info(f"  {'Category':<25} {'Count':>8}  {'%':>6}  {'AvgScore':>9}  {'Level'}")
    logger.info("  " + "─" * 64)

    for cat in CATEGORY_ORDER:
        row = con.execute(f"""
            SELECT COUNT(*), AVG(dc.data_sharing_score)
            FROM das_category dc
            JOIN article a ON a.article_id = dc.article_id
            WHERE dc.primary_category = ? {year_clause}
        """, (cat,)).fetchone()
        n, avg_score = row[0], row[1] or 0.0
        pct = n / total * 100 if total else 0
        bar = "█" * min(30, int(pct / 2))
        level = next(
            (v for (lo, hi), v in SCORE_LABELS.items() if lo <= avg_score <= hi),
            "Unknown"
        )
        logger.info(f"  {cat:<25} {n:>8,}  {pct:>5.1f}%  {avg_score:>8.2f}  {level:<14}  {bar}")

    # ── Data-sharing score distribution ─────────────────────────────────────
    logger.info("")
    logger.info("  Data-Sharing Score Distribution (0–10)")
    logger.info("  " + "─" * 40)
    for score in range(11):
        n = con.execute(f"""
            SELECT COUNT(*) FROM das_category dc
            JOIN article a ON a.article_id = dc.article_id
            WHERE dc.data_sharing_score = ? {year_clause}
        """, (score,)).fetchone()[0]
        bar = "█" * int(n / max(total, 1) * 40)
        logger.info(f"  Score {score:>2}  {n:>8,}  {bar}")

    # ── Secondary signal rates ────────────────────────────────────────────
    logger.info("")
    logger.info("  Secondary Signals (articles with explicit DAS only)")
    logger.info("  " + "─" * 50)
    das_only_clause = f"""
        WHERE dc.primary_category NOT IN ('{CAT_NO_STATEMENT}','{CAT_NO_DAS_SUPP}','{CAT_NO_DAS_ACC}')
        {year_clause.replace('AND', 'AND')}
    """
    for col, label in [
        ("mentions_doi",     "Mentions DOI"),
        ("mentions_license", "Mentions License"),
        ("mentions_github",  "Mentions GitHub/GitLab"),
        ("has_url",          "Contains URL"),
        ("has_accession",    "Contains Accession #"),
    ]:
        n = con.execute(f"""
            SELECT COUNT(*) FROM das_category dc
            JOIN article a ON a.article_id = dc.article_id
            {das_only_clause.replace('WHERE', 'WHERE')} AND dc.{col} = 1
        """).fetchone()[0]
        pct = n / has_das * 100 if has_das else 0
        logger.info(f"  {label:<28} {n:>8,}  ({pct:.1f}%)")

    # ── Year-over-year trend table ────────────────────────────────────────
    logger.info("")
    logger.info("  Year-over-Year Trend  (primary category counts)")
    logger.info("  " + "─" * 100)

    # Short column headers
    short = {
        CAT_REPO_NAMED:   "Repo",
        CAT_ACCESSION:    "Acc#",
        CAT_NO_DAS_ACC:   "NoDAS+Acc",
        CAT_OPEN_UNSPEC:  "OpenUnsp",
        CAT_SUPP_ONLY:    "SuppOnly",
        CAT_NO_DAS_SUPP:  "NoDAS+Sup",
        CAT_ON_REQUEST:   "OnReq",
        CAT_RESTRICTED:   "Restr",
        CAT_NO_STATEMENT: "NoStmt",
    }
    hdr = f"  {'Year':<6}" + "".join(f" {short[c]:>10}" for c in CATEGORY_ORDER) + f"  {'AvgScore':>8}"
    logger.info(hdr)

    # Grouped on the authoritative derived `article.year`, not raw `pub_year`.
    year_rows = con.execute("""
        SELECT a.year FROM article a
        JOIN das_category dc ON dc.article_id = a.article_id
        WHERE a.year IS NOT NULL
        GROUP BY a.year ORDER BY a.year
    """).fetchall()

    for (yr,) in year_rows:
        counts_row = []
        for cat in CATEGORY_ORDER:
            n = con.execute("""
                SELECT COUNT(*) FROM das_category dc
                JOIN article a ON a.article_id = dc.article_id
                WHERE dc.primary_category = ? AND a.year = ?
            """, (cat, yr)).fetchone()[0]
            counts_row.append(n)
        avg_score = con.execute("""
            SELECT AVG(dc.data_sharing_score) FROM das_category dc
            JOIN article a ON a.article_id = dc.article_id
            WHERE a.year = ?
        """, (yr,)).fetchone()[0] or 0.0
        row_str = f"  {yr:<6}" + "".join(f" {n:>10,}" for n in counts_row) + f"  {avg_score:>8.2f}"
        logger.info(row_str)

    # ── Request-based access spotlight ──────────────────────────────────────
    logger.info("")
    logger.info("  Request-Based Access Spotlight: 'Available on Request' over time")
    logger.info("  " + "─" * 50)
    req_rows = con.execute("""
        SELECT a.year, COUNT(*) as n,
               ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY a.year), 1) AS pct
        FROM das_category dc
        JOIN article a ON a.article_id = dc.article_id
        WHERE dc.primary_category = ? AND a.year IS NOT NULL
        GROUP BY a.year ORDER BY a.year
    """, (CAT_ON_REQUEST,)).fetchall()
    for yr, n, pct in req_rows:
        bar = "█" * min(30, int((pct or 0) / 2))
        logger.info(f"  {yr}  {n:>8,}  {pct:>5.1f}%  {bar}")

    logger.info("=" * 68)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Categorise DAS text — FAIR longitudinal study Stage 4.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python categorise_das.py --db pmc_fair.db
  python categorise_das.py --db pmc_fair.db --reset
  python categorise_das.py --db pmc_fair.db --report
  python categorise_das.py --db pmc_fair.db --report --year 2016
        """,
    )
    parser.add_argument("--db", default="pmc_fair.db",
        help="SQLite database (default: pmc_fair.db).")
    parser.add_argument("--reset", action="store_true",
        help="Drop das_category table and reclassify all articles.")
    parser.add_argument("--report", action="store_true",
        help="Print categorisation report after processing.")
    parser.add_argument("--year", default=None,
        help="Filter report to a specific year (e.g. --year 2016).")
    args = parser.parse_args()

    db_path  = Path(args.db)
    log_path = db_path.with_suffix(".das_cat.log")

    if not db_path.exists():
        sys.exit(f"Database not found: {db_path}")

    logger  = setup_logging(log_path)
    t0      = time.perf_counter()
    rss0    = _peak_rss_mb()

    logger.info("=" * 60)
    logger.info("DAS Text Categorisation  —  FAIR Longitudinal Study (Stage 4)")
    logger.info("=" * 60)
    logger.info(f"  Database : {db_path}")
    logger.info(f"  Log      : {log_path}")
    logger.info("")

    con = sqlite3.connect(db_path)
    con.execute("PRAGMA foreign_keys = ON")

    if args.reset:
        logger.info("  --reset: dropping das_category …")
        for tbl in RESET_TABLES:
            con.execute(f"DROP TABLE IF EXISTS {tbl}")
        con.commit()

    ensure_schema(con, logger)

    # Get articles not yet categorised
    rows = get_articles_to_process(con)
    logger.info(f"  Articles to categorise: {len(rows):,}")

    if rows:
        counts = insert_categories(con, rows, logger)
        elapsed  = time.perf_counter() - t0
        peak_rss = max(_peak_rss_mb() - rss0, 0)
        logger.info("")
        logger.info(f"  Categorisation complete in {elapsed:.1f}s  |  +{peak_rss:.1f} MB RSS")
        logger.info("  Category breakdown:")
        for cat in CATEGORY_ORDER:
            n = counts.get(cat, 0)
            logger.info(f"    {cat:<25} {n:>8,}")
    else:
        logger.info("  No uncategorised articles found.")
        logger.info("  (Use --reset to reclassify all articles.)")

    if args.report:
        print_report(con, logger, year_filter=args.year)

    con.close()
    logger.info(f"\nDone. Log: {log_path}")


if __name__ == "__main__":
    main()
