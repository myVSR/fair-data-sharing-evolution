"""
enrich_metadata.py
------------------
Stage 6 of the FAIR longitudinal study pipeline.

Enriches the article and journal tables with three additional dimensions
that are critical for stratified analysis of FAIR adoption:

  1. DISCIPLINE (via MeSH major topic terms from PubMed E-utilities)
     Maps each article to a top-level research discipline using the
     National Library of Medicine's Medical Subject Headings hierarchy.
     MeSH terms → broad category mapping covers 14 disciplines:
       genomics, structural_biology, clinical, neuroscience, ecology,
       chemistry, bioinformatics, public_health, pharmacology,
       immunology, microbiology, plant_biology, general, other

  2. OPEN ACCESS STATUS (via DOAJ API + ISSN lookup)
     For each journal, queries the Directory of Open Access Journals (both
     the print and online ISSN) to determine whether it is DOAJ-listed gold
     OA. DOAJ is a directory of qualifying OA journals, not a complete
     registry of subscription/hybrid journals, so a journal NOT found in
     DOAJ is recorded as "not_listed_in_doaj" -- NOT "subscription". A
     small publisher-name heuristic (Elsevier/Springer/Wiley/...) labels a
     journal "likely_subscription" when no DOAJ match is needed to check
     first, but this is a heuristic on the PUBLISHER, not a confirmed
     journal-level status (these publishers also publish fully-OA
     journals). This is a *current* DOAJ snapshot, not the journal's
     access model at its actual publication year(s) -- treat any
     longitudinal claim from this column accordingly.

  3. FUNDER ATTRIBUTION (from PMC <funding-group> XML, read directly from
     the raw pmc_YYYY.xml files via --xml-dir -- NOT from any DB table;
     02_parse_to_sqlite.py never parsed <funding-group> into SQLite, so
     there is nothing to join against there)
     Extracts <award-group>/<funding-source>/<award-id> structured data
     where present, falling back to regex over a plain-text
     <funding-statement> otherwise. Maps funders to known major agencies:
       NIH, Wellcome, ERC, UKRI, DFG, NSF, Gates Foundation, etc.
     Funder mandates (NIH 2003, Wellcome 2007, ERC 2013) are milestones
     that predate or overlap the FAIR publication and confound the signal.

Schema added (four tables)
--------------------------
  article_mesh
    article_id   INTEGER FK
    mesh_term    TEXT              -- e.g. "Genomics"
    mesh_major   INTEGER 0/1      -- is it a major/asterisked topic?
    mesh_broad   TEXT             -- broad discipline bucket

  article_discipline
    article_id   INTEGER FK UNIQUE
    discipline   TEXT             -- primary discipline bucket

  journal_oa
    journal_id   INTEGER FK UNIQUE
    oa_status    TEXT             -- 'gold_oa' / 'hybrid' / 'subscription' / 'unknown'
    doaj_title   TEXT
    doaj_checked TEXT             -- ISO date of lookup

  article_funder
    funder_id    INTEGER PK
    article_id   INTEGER FK
    funder_raw   TEXT             -- raw funder name from XML
    funder_norm  TEXT             -- normalised agency name
    grant_id     TEXT             -- grant number if present
    funder_country TEXT           -- country of funding agency

Usage
-----
  python enrich_metadata.py --db pmc_fair.db --xml-dir data/raw --ncbi-key YOUR_KEY
  python enrich_metadata.py --db pmc_fair.db --skip-mesh      # only OA + funders
  python enrich_metadata.py --db pmc_fair.db --skip-doaj      # only MeSH + funders
  python enrich_metadata.py --db pmc_fair.db --report

  # One-time, after upgrading from the old DAS-based funder extraction:
  python enrich_metadata.py --db pmc_fair.db --xml-dir data/raw --reset-funders --skip-mesh --skip-doaj
  # One-time, to relabel journal_oa without re-running MeSH (no NCBI calls):
  python enrich_metadata.py --db pmc_fair.db --reset-doaj --skip-mesh --skip-funders
"""

import argparse
import logging
import re
import resource
import sqlite3
import sys
import time
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from pathlib import Path

try:
    import requests
except ImportError:
    sys.exit("requests is required: pip install requests --break-system-packages")

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

LOG_FMT  = "%(asctime)s  %(levelname)-7s  %(message)s"
LOG_DATE = "%Y-%m-%d %H:%M:%S"


def setup_logging(log_path: Path) -> logging.Logger:
    logger = logging.getLogger("enrich_metadata")
    logger.setLevel(logging.DEBUG)
    logger.handlers.clear()
    fmt = logging.Formatter(LOG_FMT, datefmt=LOG_DATE)
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO); ch.setFormatter(fmt)
    logger.addHandler(ch)
    fh = logging.FileHandler(log_path, encoding="utf-8", mode="a")
    fh.setLevel(logging.DEBUG); fh.setFormatter(fmt)
    logger.addHandler(fh)
    return logger


def _peak_rss_mb():
    try:
        kb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        return kb / 1024 if sys.platform != "darwin" else kb / (1024 * 1024)
    except Exception:
        return 0.0


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

SCHEMA = """
CREATE TABLE IF NOT EXISTS article_mesh (
    mesh_id     INTEGER PRIMARY KEY AUTOINCREMENT,
    article_id  INTEGER NOT NULL REFERENCES article(article_id),
    mesh_term   TEXT NOT NULL,
    mesh_major  INTEGER NOT NULL DEFAULT 0,   -- 1 if MajorTopicYN=Y
    mesh_broad  TEXT                           -- broad discipline bucket
);
CREATE INDEX IF NOT EXISTS idx_am_article   ON article_mesh(article_id);
CREATE INDEX IF NOT EXISTS idx_am_broad     ON article_mesh(mesh_broad);
-- Preventive: PubMed can list the same descriptor name twice for one PMID
-- (e.g. "Humans" as both a major and a qualified topic); without this,
-- INSERT OR IGNORE below has no conflict target to dedupe against.
CREATE UNIQUE INDEX IF NOT EXISTS idx_am_unique ON article_mesh(article_id, mesh_term);

CREATE TABLE IF NOT EXISTS article_discipline (
    article_id  INTEGER PRIMARY KEY REFERENCES article(article_id),
    discipline  TEXT NOT NULL,
    method      TEXT NOT NULL DEFAULT 'mesh'   -- 'mesh' / 'keyword' / 'manual'
);
CREATE INDEX IF NOT EXISTS idx_ad_disc ON article_discipline(discipline);

CREATE TABLE IF NOT EXISTS journal_oa (
    journal_id   INTEGER PRIMARY KEY REFERENCES journal(journal_id),
    -- 'gold_oa' (DOAJ-listed or known-OA publisher) / 'likely_subscription'
    -- (known-subscription publisher heuristic) / 'not_listed_in_doaj'
    -- (queried DOAJ, no match -- NOT proof of subscription status) /
    -- 'unknown' (no ISSN to query / lookup failed)
    oa_status    TEXT NOT NULL DEFAULT 'unknown',
    doaj_title   TEXT,
    doaj_checked TEXT
);

CREATE TABLE IF NOT EXISTS article_funder (
    funder_id      INTEGER PRIMARY KEY AUTOINCREMENT,
    article_id     INTEGER NOT NULL REFERENCES article(article_id),
    funder_raw     TEXT,
    funder_norm    TEXT,
    grant_id       TEXT,
    funder_country TEXT,
    UNIQUE(article_id, funder_norm, grant_id)
);
CREATE INDEX IF NOT EXISTS idx_af_article ON article_funder(article_id);
CREATE INDEX IF NOT EXISTS idx_af_norm    ON article_funder(funder_norm);
"""

RESET_TABLES = ["article_mesh", "article_discipline", "journal_oa", "article_funder"]


# ---------------------------------------------------------------------------
# 1. MeSH → Discipline mapping
# ---------------------------------------------------------------------------

# Broad discipline buckets mapped from MeSH tree numbers / term names.
# MeSH major topics are the asterisked (*) descriptors in PubMed records.
# We map top-level MeSH categories to 14 research disciplines.
MESH_DISCIPLINE_MAP = [
    # (regex pattern on MeSH term, discipline)
    (re.compile(r"genomic|sequenc|transcriptom|epigenom|gene\s+expression|RNA|DNA|chromosome|variant|SNP|genome", re.I), "genomics"),
    (re.compile(r"protein\s+structure|crystallograph|cryo.EM|NMR\s+spectroscopy|protein\s+folding|structural\s+biol", re.I), "structural_biology"),
    (re.compile(r"clinical\s+trial|patient|disease|diagnosis|therapy|treatment|surgery|cancer|tumor|cohort\s+stud", re.I), "clinical"),
    (re.compile(r"neuroscience|brain|neural|neuron|cogniti|behav|psychiatry|alzheimer|parkinson", re.I), "neuroscience"),
    (re.compile(r"ecolog|biodiversity|species|habitat|conservation|climate|environment|ecosystem", re.I), "ecology"),
    (re.compile(r"chemistry|metabolom|mass\s+spectrom|compound|reaction|synthesis|chemical", re.I), "chemistry"),
    (re.compile(r"bioinformat|computational\s+biol|machine\s+learning|algorithm|software|database|ontolog", re.I), "bioinformatics"),
    (re.compile(r"public\s+health|epidemiol|population|survey|mortality|morbidity|vaccination|pandemic", re.I), "public_health"),
    (re.compile(r"pharmacol|drug|therapeut|pharmac|toxicol|dose|receptor|ligand", re.I), "pharmacology"),
    (re.compile(r"immunol|immunit|antibod|vaccine|T.cell|B.cell|cytokine|inflammation", re.I), "immunology"),
    (re.compile(r"microbiol|bacteria|virus|fungi|pathogen|infection|microbiom", re.I), "microbiology"),
    (re.compile(r"plant|botany|crop|agriculture|photosynthes|Arabidopsis|maize|rice", re.I), "plant_biology"),
]


def mesh_to_discipline(mesh_term: str) -> str:
    for pat, disc in MESH_DISCIPLINE_MAP:
        if pat.search(mesh_term):
            return disc
    return "general"


def fetch_mesh_for_pmids(
    pmids: list[str],
    ncbi_key: str | None,
    logger: logging.Logger,
) -> dict[str, list[dict]]:
    """
    Call PubMed EFetch for a batch of PMIDs and return MeSH headings.
    Returns {pmid: [{"term":..., "major":0/1, "broad":...}]}
    """
    if not pmids:
        return {}

    base = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi"
    params = {
        "db":       "pubmed",
        "rettype":  "xml",
        "retmode":  "xml",
        "id":       ",".join(pmids),
    }
    if ncbi_key:
        params["api_key"] = ncbi_key

    try:
        r = requests.get(base, params=params, timeout=30)
        r.raise_for_status()
    except Exception as e:
        logger.warning(f"  MeSH fetch error for {len(pmids)} PMIDs: {e}")
        return {}

    # Parse XML — use regex for speed (no full XML parse needed)
    xml = r.text
    results: dict[str, list[dict]] = {}

    # Split by article
    for article_xml in re.split(r"<PubmedArticle>", xml)[1:]:
        pmid_m = re.search(r"<PMID[^>]*>(\d+)</PMID>", article_xml)
        if not pmid_m:
            continue
        pmid = pmid_m.group(1)
        headings = []

        for mh in re.finditer(
            r"<MeshHeading>(.*?)</MeshHeading>", article_xml, re.DOTALL
        ):
            block = mh.group(1)
            desc_m = re.search(
                r'<DescriptorName[^>]*MajorTopicYN="([YN])"[^>]*>([^<]+)</DescriptorName>',
                block,
            )
            if desc_m:
                major = 1 if desc_m.group(1) == "Y" else 0
                term  = desc_m.group(2).strip()
                broad = mesh_to_discipline(term)
                headings.append({"term": term, "major": major, "broad": broad})

        results[pmid] = headings

    return results


def assign_discipline_from_mesh(mesh_rows: list[dict]) -> str:
    """Pick the primary discipline from a list of MeSH rows for one article."""
    if not mesh_rows:
        return "general"
    # Count disciplines, weighting major topics × 3
    counts: dict[str, int] = {}
    for row in mesh_rows:
        disc = row["broad"]
        weight = 3 if row["major"] else 1
        counts[disc] = counts.get(disc, 0) + weight
    # Remove "general" unless it's the only option
    counts_filtered = {k: v for k, v in counts.items() if k != "general"} or counts
    return max(counts_filtered, key=lambda k: counts_filtered[k])


def assign_discipline_from_keywords(keywords: str) -> str:
    """Fallback: infer discipline from article keywords when MeSH unavailable."""
    for pat, disc in MESH_DISCIPLINE_MAP:
        if pat.search(keywords):
            return disc
    return "general"


def run_mesh_enrichment(
    con: sqlite3.Connection,
    ncbi_key: str | None,
    batch_size: int,
    logger: logging.Logger,
) -> int:
    """Fetch MeSH terms for all articles that have a PubMed ID."""
    # Get articles needing MeSH enrichment
    rows = con.execute("""
        SELECT a.article_id, a.pubmed_id, a.keywords
        FROM article a
        LEFT JOIN article_discipline ad ON ad.article_id = a.article_id
        WHERE ad.article_id IS NULL
        ORDER BY a.article_id
    """).fetchall()

    total   = len(rows)
    done    = 0
    rate    = 10 if ncbi_key else 3   # NCBI rate limit: 10/s with key, 3/s without
    delay   = 1.0 / rate

    logger.info(f"  Articles needing discipline: {total:,}")

    # Separate those with PMIDs (can get real MeSH) from those without
    with_pmid    = [(aid, pmid, kw) for aid, pmid, kw in rows if pmid]
    without_pmid = [(aid, pmid, kw) for aid, pmid, kw in rows if not pmid]

    # Batch fetch MeSH for articles with PMIDs
    cur = con.cursor()
    for batch_start in range(0, len(with_pmid), batch_size):
        batch = with_pmid[batch_start: batch_start + batch_size]
        pmids = [str(pmid) for _, pmid, _ in batch]
        pmid_to_aid_kw = {str(pmid): (aid, kw) for aid, pmid, kw in batch}

        mesh_data = fetch_mesh_for_pmids(pmids, ncbi_key, logger)
        time.sleep(delay)

        for pmid, headings in mesh_data.items():
            if pmid not in pmid_to_aid_kw:
                continue
            article_id, kw = pmid_to_aid_kw[pmid]

            for h in headings:
                cur.execute("""
                    INSERT OR IGNORE INTO article_mesh
                        (article_id, mesh_term, mesh_major, mesh_broad)
                    VALUES (?,?,?,?)
                """, (article_id, h["term"], h["major"], h["broad"]))

            disc = assign_discipline_from_mesh(headings) if headings \
                   else assign_discipline_from_keywords(kw or "")
            cur.execute("""
                INSERT OR REPLACE INTO article_discipline (article_id, discipline, method)
                VALUES (?,?,?)
            """, (article_id, disc, "mesh" if headings else "keyword"))

        # Articles in batch but not returned by PubMed → use keywords
        returned = set(mesh_data.keys())
        for pmid_str, (article_id, kw) in pmid_to_aid_kw.items():
            if pmid_str not in returned:
                disc = assign_discipline_from_keywords(kw or "")
                cur.execute("""
                    INSERT OR IGNORE INTO article_discipline (article_id, discipline, method)
                    VALUES (?,?,'keyword')
                """, (article_id, disc))

        done += len(batch)
        con.commit()
        if done % 500 == 0 or done == len(with_pmid):
            logger.info(f"  MeSH enrichment: {done:,}/{len(with_pmid):,} (with PMID)")

    # Articles without PMIDs → keyword fallback
    for article_id, _, kw in without_pmid:
        disc = assign_discipline_from_keywords(kw or "")
        cur.execute("""
            INSERT OR IGNORE INTO article_discipline (article_id, discipline, method)
            VALUES (?,?,'keyword')
        """, (article_id, disc))
    con.commit()
    logger.info(f"  Keyword fallback: {len(without_pmid):,} articles")

    return total


# ---------------------------------------------------------------------------
# 2. DOAJ Open Access status
# ---------------------------------------------------------------------------

# Known OA journals / publishers — checked before hitting DOAJ API
# to save quota for less well-known journals
KNOWN_OA_PUBLISHERS = {
    "public library of science": "gold_oa",
    "plos":                      "gold_oa",
    "biomed central":            "gold_oa",
    "bmc":                       "gold_oa",
    "frontiers":                 "gold_oa",
    "mdpi":                      "gold_oa",
    "hindawi":                   "gold_oa",
    "pensoft":                   "gold_oa",
    "copernicus":                "gold_oa",
    "f1000":                     "gold_oa",
    "elife":                     "gold_oa",
    "peerj":                     "gold_oa",
}

KNOWN_SUBSCRIPTION = {
    "elsevier", "springer", "wiley", "nature", "cell press",
    "oxford university press", "cambridge university press",
    "american chemical society", "royal society of chemistry",
}


def _lookup_doaj_one(issn: str, logger: logging.Logger) -> str | None:
    """
    Query the DOAJ API for a single ISSN.
    Returns "gold_oa" on a DOAJ match, None if no match/lookup failed
    (caller decides what "no match" means -- DOAJ is a directory of
    qualifying OA journals, NOT a complete registry of subscription/hybrid
    journals, so absence from it is not evidence of subscription status).
    """
    if not issn:
        return None
    url = f"https://doaj.org/api/v2/journals?issn={issn.strip()}"
    try:
        r = requests.get(url, timeout=15,
                         headers={"User-Agent": "fair-longitudinal-study/1.0"})
        if r.status_code == 404:
            return None
        r.raise_for_status()
        data = r.json()
        if data.get("results", []):
            return "gold_oa"
        return None
    except Exception as e:
        logger.debug(f"  DOAJ lookup failed for {issn}: {e}")
        return None


def lookup_doaj(issn_ppub: str, issn_epub: str, logger: logging.Logger) -> str:
    """
    Try both ISSNs (previously only issn_ppub or issn_epub -- the first
    non-empty one -- was tried, so a failed ppub lookup never fell back to
    checking the epub ISSN). Returns "gold_oa" on either match,
    "not_listed_in_doaj" when at least one ISSN was queried and matched
    nothing (an absence of evidence, not evidence of subscription status),
    or "unknown" when neither ISSN could be queried at all.
    """
    tried = False
    issns = [i for i in (issn_ppub, issn_epub) if i]
    for idx, issn in enumerate(issns):
        tried = True
        status = _lookup_doaj_one(issn, logger)
        if idx < len(issns) - 1:
            time.sleep(1.1)  # DOAJ rate limit: ~1 req/s, only between actual HTTP calls
        if status == "gold_oa":
            return "gold_oa"
    return "not_listed_in_doaj" if tried else "unknown"


def run_doaj_enrichment(
    con: sqlite3.Connection,
    logger: logging.Logger,
) -> int:
    from datetime import date
    today = date.today().isoformat()

    journals = con.execute("""
        SELECT j.journal_id, j.title, j.publisher, j.issn_ppub, j.issn_epub
        FROM journal j
        LEFT JOIN journal_oa jo ON jo.journal_id = j.journal_id
        WHERE jo.journal_id IS NULL
    """).fetchall()

    logger.info(f"  Journals needing OA lookup: {len(journals):,}")
    cur = con.cursor()
    done = 0

    for journal_id, title, publisher, issn_ppub, issn_epub in journals:
        pub_lower = (publisher or "").lower()
        title_lower = (title or "").lower()

        # Fast-path: known publishers
        oa_status = None
        for kw, status in KNOWN_OA_PUBLISHERS.items():
            if kw in pub_lower or kw in title_lower:
                oa_status = status
                break
        if oa_status is None:
            for kw in KNOWN_SUBSCRIPTION:
                if kw in pub_lower:
                    # A heuristic on the PUBLISHER, not the journal: Elsevier/
                    # Springer/Wiley etc. publish both subscription AND fully-OA
                    # journals, so this is "probably not gold OA", not a
                    # confirmed subscription status for this specific journal.
                    oa_status = "likely_subscription"
                    break

        # DOAJ API fallback (only for unknowns; tries both ISSNs, rate-limited to ~1/s)
        if oa_status is None:
            oa_status = lookup_doaj(issn_ppub, issn_epub, logger)

        cur.execute("""
            INSERT OR REPLACE INTO journal_oa (journal_id, oa_status, doaj_title, doaj_checked)
            VALUES (?,?,?,?)
        """, (journal_id, oa_status, title, today))
        done += 1
        if done % 100 == 0:
            con.commit()
            logger.info(f"  OA lookup: {done:,}/{len(journals):,}")

    con.commit()
    return done


# ---------------------------------------------------------------------------
# 3. Funder extraction
# ---------------------------------------------------------------------------

# Normalised funder registry — maps regex patterns → (normalised name, country)
FUNDER_PATTERNS = [
    # US Federal
    (re.compile(r"\bNIH\b|National Institutes? of Health", re.I),                    "NIH", "US"),
    (re.compile(r"\bNSF\b|National Science Foundation", re.I),                        "NSF", "US"),
    (re.compile(r"\bNIAID\b|National Institute of Allergy", re.I),                    "NIAID", "US"),
    (re.compile(r"\bNCI\b|National Cancer Institute", re.I),                          "NCI", "US"),
    (re.compile(r"\bNIMH\b|National Institute of Mental Health", re.I),               "NIMH", "US"),
    (re.compile(r"\bDOE\b|Department of Energy", re.I),                               "DOE", "US"),
    (re.compile(r"\bNASA\b", re.I),                                                    "NASA", "US"),
    (re.compile(r"Gates Foundation|Bill.*Melinda Gates", re.I),                        "Gates Foundation", "US"),
    (re.compile(r"Howard Hughes", re.I),                                               "HHMI", "US"),
    # UK
    (re.compile(r"\bUKRI\b|UK Research and Innovation", re.I),                        "UKRI", "UK"),
    (re.compile(r"Wellcome Trust|Wellcome\b", re.I),                                   "Wellcome Trust", "UK"),
    (re.compile(r"\bBBSRC\b|Biotechnology and Biological Sciences", re.I),             "BBSRC", "UK"),
    (re.compile(r"\bMRC\b|Medical Research Council", re.I),                            "MRC", "UK"),
    (re.compile(r"\bEPSRC\b", re.I),                                                   "EPSRC", "UK"),
    (re.compile(r"Cancer Research UK", re.I),                                          "Cancer Research UK", "UK"),
    # European
    (re.compile(r"\bERC\b|European Research Council", re.I),                          "ERC", "EU"),
    (re.compile(r"Horizon 2020|Horizon Europe|FP7\b", re.I),                          "EU Horizon", "EU"),
    (re.compile(r"\bDFG\b|Deutsche Forschungsgemeinschaft", re.I),                    "DFG", "DE"),
    (re.compile(r"Agence Nationale de la Recherche|\bANR\b", re.I),                   "ANR", "FR"),
    (re.compile(r"Ministerio de Ciencia|MICINN|MINECO|AEI\b", re.I),                  "AEI", "ES"),
    (re.compile(r"Swiss National Science|SNSF|SNF\b", re.I),                          "SNSF", "CH"),
    (re.compile(r"Netherlands Organisation|NWO\b", re.I),                             "NWO", "NL"),
    # Asia-Pacific
    (re.compile(r"National Natural Science Foundation.*China|NSFC\b", re.I),          "NSFC", "CN"),
    (re.compile(r"Chinese Academy of Sciences|\bCAS\b", re.I),                        "CAS", "CN"),
    (re.compile(r"Japan Society.*Promotion of Science|JSPS\b", re.I),                 "JSPS", "JP"),
    (re.compile(r"Australian Research Council|\bARC\b", re.I),                        "ARC", "AU"),
    (re.compile(r"National Health and Medical Research|NHMRC\b", re.I),               "NHMRC", "AU"),
    # Philanthropy / charity
    (re.compile(r"Chan Zuckerberg", re.I),                                             "Chan Zuckerberg Initiative", "US"),
    (re.compile(r"Simons Foundation", re.I),                                           "Simons Foundation", "US"),
]

GRANT_ID_PAT = re.compile(
    r"\b((?:[A-Z]{1,4}[-\s]?)?\d{2,8}(?:[-/]\d{2,6})?(?:[A-Z]{1,4})?)\b"
)


def _xml_text(el) -> str:
    if el is None:
        return ""
    return " ".join(el.itertext()).strip()


def _xml_clean(s: str) -> str:
    return re.sub(r"\s+", " ", str(s or "")).strip()


def extract_funders(funding_text: str) -> list[dict]:
    """
    Free-text fallback: extract normalised funders + grant IDs from a
    funding acknowledgement TEXT BLOB (used only when an article's
    <funding-group> has no structured <award-group>/<award-id>, just a
    plain-text <funding-statement>). Structured XML is always preferred —
    see extract_funding_from_xml().
    """
    results = []
    seen = set()   # (norm_name, grant_id) pairs already emitted for this text

    for pat, norm_name, country in FUNDER_PATTERNS:
        for m in pat.finditer(funding_text):
            # Look for grant IDs in a window around the match
            window_start = max(0, m.start() - 20)
            window_end   = min(len(funding_text), m.end() + 100)
            window = funding_text[window_start:window_end]

            grant_ids = [
                g.group(1) for g in GRANT_ID_PAT.finditer(window)
                if len(g.group(1)) >= 5   # filter out short noise
            ] or [None]

            for grant_id in grant_ids:
                key = (norm_name, grant_id)
                if key in seen:
                    continue
                seen.add(key)
                results.append({
                    "funder_raw":     m.group(0),
                    "funder_norm":    norm_name,
                    "grant_id":       grant_id,
                    "funder_country": country,
                })

    return results


def _normalise_funder_source(funding_source: str) -> tuple[str, str | None]:
    """Match a <funding-source> institution string against the curated
    FUNDER_PATTERNS registry; fall back to the cleaned raw text itself
    (rather than silently dropping funders outside the ~30-pattern
    allow-list)."""
    for pat, norm_name, country in FUNDER_PATTERNS:
        if pat.search(funding_source):
            return norm_name, country
    cleaned = _xml_clean(funding_source)
    return (cleaned[:120] if cleaned else "unspecified"), None


def extract_funding_from_xml(article_el) -> list[dict]:
    """
    Extract funder/grant info from the article's OWN
    front/article-meta/funding-group -- NOT from the data-availability
    statement (the data_availability table has no connection to how an
    article's research was actually funded; it was a bug that the old
    DAS-text-regex approach was reading from it at all).

    Priority within each <funding-group>:
      1. Structured <award-group> -> <funding-source> (institution name)
         + one row per <award-id> (grant number), since one funder can
         fund an article under multiple distinct grants.
      2. If no <award-group> exists at all, fall back to regex-scanning
         the free-text <funding-statement>, if present.
    Returns [] if the article has no funding-group / funding-statement at
    all (caller records this as the 'none' sentinel).
    """
    ameta = article_el.find("front/article-meta")
    if ameta is None:
        return []

    results: list[dict] = []
    for fg in ameta.findall("funding-group"):
        award_groups = fg.findall("award-group")
        if award_groups:
            for ag in award_groups:
                fs_el = ag.find(".//funding-source")
                funding_source = _xml_text(fs_el)
                norm_name, country = _normalise_funder_source(funding_source)
                award_ids = [_xml_text(a) for a in ag.findall("award-id") if _xml_text(a)]
                if not award_ids:
                    results.append({
                        "funder_raw": funding_source, "funder_norm": norm_name,
                        "grant_id": None, "funder_country": country,
                    })
                else:
                    for aid in award_ids:
                        results.append({
                            "funder_raw": funding_source, "funder_norm": norm_name,
                            "grant_id": aid, "funder_country": country,
                        })
        else:
            fstmt_el = fg.find("funding-statement")
            text = _xml_text(fstmt_el) if fstmt_el is not None else ""
            if text:
                results.extend(extract_funders(text))

    return results


def build_funder_id_lookup(con: sqlite3.Connection, logger: logging.Logger) -> dict[str, int]:
    """Map pmc_id / pubmed_id / doi -> article_id for every article in the DB."""
    lookup: dict[str, int] = {}
    for pmc_id, pubmed_id, doi, article_id in con.execute(
        "SELECT pmc_id, pubmed_id, doi, article_id FROM article"
    ):
        if pmc_id:    lookup[str(pmc_id).strip()]    = article_id
        if pubmed_id: lookup[str(pubmed_id).strip()] = article_id
        if doi:       lookup[str(doi).strip()]       = article_id
    logger.info(f"  ID lookup: {len(lookup):,} keys (pmc_id + pubmed_id + doi)")
    return lookup


def run_funder_enrichment(con: sqlite3.Connection, xml_dir: Path, logger: logging.Logger) -> int:
    """
    Extract funders from each article's OWN raw XML <funding-group>
    (see extract_funding_from_xml) and insert into article_funder.

    This scans data/raw/pmc_*.xml directly, the same pattern
    extract_ethics.py uses, because 02_parse_to_sqlite.py never parsed
    <funding-group> into any DB table -- there is nothing to join against
    in SQLite for this stage.

    Resume-safe: articles already present in article_funder (including the
    sentinel 'none' rows) are skipped.
    """
    xml_files = sorted(Path(xml_dir).glob("pmc_*.xml"))
    if not xml_files:
        logger.warning(f"  No pmc_*.xml files found in {xml_dir} -- "
                        f"skipping funder extraction (pass --xml-dir).")
        return 0

    id_lookup = build_funder_id_lookup(con, logger)
    already = {r[0] for r in con.execute("SELECT article_id FROM article_funder").fetchall()}
    logger.info(f"  Already extracted : {len(already):,} articles")

    cur = con.cursor()
    done = 0

    for idx, xml_path in enumerate(xml_files, 1):
        t_file = time.perf_counter()
        n_inserted = n_no_match = n_existing = 0

        for event, elem in ET.iterparse(str(xml_path), events=("end",)):
            if elem.tag != "article":
                continue

            ameta = elem.find("front/article-meta")
            id_els = ameta.findall("article-id") if ameta is not None else []
            article_id = None
            for id_el in id_els:
                id_type = (id_el.get("pub-id-type", "") or "").lower()
                id_val  = (id_el.text or "").strip()
                if id_type in ("pmc", "pmid", "doi") and id_val:
                    article_id = id_lookup.get(id_val)
                    if article_id:
                        break

            if not article_id:
                n_no_match += 1
                elem.clear()
                continue
            if article_id in already:
                n_existing += 1
                elem.clear()
                continue

            funders = extract_funding_from_xml(elem)
            elem.clear()

            if funders:
                for f in funders:
                    cur.execute("""
                        INSERT OR IGNORE INTO article_funder
                            (article_id, funder_raw, funder_norm, grant_id, funder_country)
                        VALUES (?,?,?,?,?)
                    """, (article_id, f["funder_raw"], f["funder_norm"],
                          f["grant_id"], f["funder_country"]))
            else:
                cur.execute("""
                    INSERT OR IGNORE INTO article_funder
                        (article_id, funder_raw, funder_norm, grant_id, funder_country)
                    VALUES (?, '', 'none', NULL, NULL)
                """, (article_id,))

            already.add(article_id)
            n_inserted += 1
            done += 1

        con.commit()
        elapsed = time.perf_counter() - t_file
        logger.info(
            f"  [{idx}/{len(xml_files)}] {xml_path.name}: "
            f"processed={n_inserted:,}  no_db_match={n_no_match:,}  "
            f"already_done={n_existing:,}  ({elapsed:.1f}s)"
        )

    return done


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------

def print_report(con: sqlite3.Connection, logger: logging.Logger) -> None:
    logger.info("")
    logger.info("=" * 65)
    logger.info("ENRICHMENT REPORT")
    logger.info("=" * 65)

    # Discipline distribution
    logger.info("\n  Discipline distribution:")
    rows = con.execute("""
        SELECT discipline, COUNT(*) AS n,
               ROUND(COUNT(*)*100.0/SUM(COUNT(*)) OVER(), 1) AS pct
        FROM article_discipline
        GROUP BY discipline ORDER BY n DESC
    """).fetchall()
    for disc, n, pct in rows:
        bar = "█" * int(pct / 2)
        logger.info(f"    {disc:<20} {n:>8,}  {pct:>5.1f}%  {bar}")

    # Discipline × data-sharing category cross-tab
    logger.info("\n  Mean data-sharing score by discipline:")
    rows = con.execute("""
        SELECT ad.discipline,
               ROUND(AVG(dc.data_sharing_score), 2) AS avg_score,
               COUNT(*) AS n
        FROM article_discipline ad
        JOIN das_category dc ON dc.article_id = ad.article_id
        GROUP BY ad.discipline ORDER BY avg_score DESC
    """).fetchall()
    for disc, avg, n in rows:
        logger.info(f"    {disc:<20} avg score={avg:>5.2f}  n={n:,}")

    # OA status
    logger.info("\n  Journal OA status distribution:")
    rows = con.execute("""
        SELECT jo.oa_status, COUNT(*) AS journals,
               COUNT(DISTINCT a.article_id) AS articles
        FROM journal_oa jo
        JOIN journal j ON j.journal_id = jo.journal_id
        LEFT JOIN article a ON a.journal_id = j.journal_id
        GROUP BY jo.oa_status ORDER BY articles DESC
    """).fetchall()
    for status, journals, articles in rows:
        logger.info(f"    {status:<15} journals={journals:>5,}  articles={articles:>8,}")

    # Top funders
    logger.info("\n  Top 15 funders:")
    rows = con.execute("""
        SELECT funder_norm, funder_country, COUNT(DISTINCT article_id) AS articles
        FROM article_funder
        WHERE funder_norm != 'none'
        GROUP BY funder_norm, funder_country ORDER BY articles DESC LIMIT 15
    """).fetchall()
    for funder, country, n in rows:
        logger.info(f"    {funder:<35} [{country}]  {n:>6,} articles")

    # Funder × data-sharing score
    logger.info("\n  Data-sharing score by top funder:")
    rows = con.execute("""
        SELECT af.funder_norm,
               ROUND(AVG(dc.data_sharing_score), 2) AS avg_score,
               COUNT(DISTINCT dc.article_id) AS n
        FROM article_funder af
        JOIN das_category dc ON dc.article_id = af.article_id
        WHERE af.funder_norm != 'none'
        GROUP BY af.funder_norm HAVING n >= 10
        ORDER BY avg_score DESC LIMIT 10
    """).fetchall()
    for funder, avg, n in rows:
        logger.info(f"    {funder:<35} avg score={avg:>5.2f}  n={n:,}")

    logger.info("=" * 65)


# ---------------------------------------------------------------------------
# Schema management
# ---------------------------------------------------------------------------

def ensure_schema(con: sqlite3.Connection, logger: logging.Logger) -> None:
    con.executescript(SCHEMA)
    con.commit()
    logger.info("  Enrichment tables ready.")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Metadata enrichment — FAIR longitudinal study Stage 6.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--db", default="pmc_fair.db")
    parser.add_argument("--xml-dir", default="data/raw",
        help="Directory containing pmc_YYYY.xml files -- required for funder "
             "extraction, which reads <funding-group> directly from the raw "
             "XML (default: data/raw).")
    parser.add_argument("--ncbi-key", default=None,
        help="NCBI API key (10 req/s vs 3 req/s without).")
    parser.add_argument("--mesh-batch", type=int, default=50,
        help="PMIDs per MeSH API batch request (default: 50, max: 200).")
    parser.add_argument("--skip-mesh",  action="store_true")
    parser.add_argument("--skip-doaj",  action="store_true")
    parser.add_argument("--skip-funders", action="store_true")
    parser.add_argument("--reset",      action="store_true",
        help="Drop ALL enrichment tables and re-run from scratch.")
    parser.add_argument("--reset-funders", action="store_true",
        help="Clear ONLY article_funder before running. Required the first "
             "time you run this corrected version: prior runs populated "
             "article_funder from data_availability text (a bug) rather "
             "than from <funding-group> XML, and the resume logic would "
             "otherwise treat those wrong rows as already-done and skip "
             "every article.")
    parser.add_argument("--reset-doaj", action="store_true",
        help="Clear ONLY journal_oa before running -- use this to re-apply "
             "the corrected DOAJ-absence labelling "
             "('not_listed_in_doaj'/'likely_subscription' instead of a "
             "blanket 'subscription') to the small journal table (tens of "
             "thousands of rows) without re-running MeSH enrichment "
             "(millions of NCBI calls).")
    parser.add_argument("--report",     action="store_true")
    args = parser.parse_args()

    db_path  = Path(args.db)
    log_path = db_path.with_suffix(".enrich.log")
    if not db_path.exists():
        sys.exit(f"Database not found: {db_path}")

    logger = setup_logging(log_path)
    t0     = time.perf_counter()

    logger.info("=" * 60)
    logger.info("Metadata Enrichment  —  FAIR Longitudinal Study (Stage 6)")
    logger.info("=" * 60)

    con = sqlite3.connect(db_path)
    con.execute("PRAGMA foreign_keys = ON")
    con.execute("PRAGMA journal_mode = WAL")
    if args.reset:
        for tbl in RESET_TABLES:
            con.execute(f"DROP TABLE IF EXISTS {tbl}")
        con.commit()
        logger.info("  Tables reset.")

    ensure_schema(con, logger)

    if args.reset_funders:
        con.execute("DELETE FROM article_funder")
        con.commit()
        logger.info("  article_funder cleared (--reset-funders).")
    if args.reset_doaj:
        con.execute("DELETE FROM journal_oa")
        con.commit()
        logger.info("  journal_oa cleared (--reset-doaj).")

    if not args.skip_mesh:
        logger.info("\n── Stage 6a: MeSH / Discipline enrichment ──")
        n = run_mesh_enrichment(con, args.ncbi_key, args.mesh_batch, logger)
        logger.info(f"  Done: {n:,} articles processed.")

    if not args.skip_doaj:
        logger.info("\n── Stage 6b: Journal OA status (DOAJ) ──")
        n = run_doaj_enrichment(con, logger)
        logger.info(f"  Done: {n:,} journals processed.")

    if not args.skip_funders:
        logger.info("\n── Stage 6c: Funder extraction ──")
        n = run_funder_enrichment(con, Path(args.xml_dir), logger)
        logger.info(f"  Done: {n:,} articles processed.")

    elapsed = time.perf_counter() - t0
    logger.info(f"\nTotal enrichment time: {elapsed/60:.1f} min")

    if args.report:
        print_report(con, logger)

    con.close()
    logger.info(f"Done. Log: {log_path}")


if __name__ == "__main__":
    main()
