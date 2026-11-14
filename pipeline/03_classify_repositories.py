"""
classify_repositories.py
-------------------------
Stage 3 of the FAIR longitudinal study pipeline.

Classifies every URL in the `da_link` table (and cross-references
`accession_number` entries) by:

  repo_name       - human-readable repository name  (e.g. "Zenodo")
  repo_type       - category of repository
  is_known_repo   - 1 if matched to a curated known repository, else 0
  repo_infra_tier - gold / silver / bronze / unknown (see "Repository
                    infrastructure tiers" below)
  is_open_access  - 1 if data is freely accessible without login
  has_persistent_id - 1 if repository issues DOIs or other PIDs
  domain          - extracted domain from the URL (e.g. "zenodo.org")

Repository types used
---------------------
  general_purpose   - Zenodo, Figshare, Dryad, OSF, Mendeley Data, Harvard Dataverse
  domain_specific   - GEO, SRA, ArrayExpress, UniProt, PDB, dbGaP, PRIDE, etc.
  code_repository   - GitHub, GitLab, Bitbucket, SourceForge
  journal_platform  - journal supplementary portals, publisher data repositories
  institutional     - university or institute repositories
  cloud_storage     - Dropbox, Google Drive, Box, OneDrive (low FAIR)
  preprint          - bioRxiv, arXiv, OSF Preprints
  request_only      - "available upon request" flagged URLs / email links
  repository_registry - directories of repositories, not repositories
                        themselves (e.g. re3data) — never a deposit target
  other             - any URL not matched to a known category

Repository infrastructure tiers
--------------------------------
This is a static, repository-TYPE-level heuristic (every URL at the same
repository gets the same tier) reflecting typical infrastructure
capabilities -- NOT a per-dataset FAIR assessment of what was actually
deposited. A "gold" repository can still host a dataset with no license
and poor metadata; this tier says nothing about that specific deposit.
For an actual, metric-based FAIR assessment of specific dataset URLs, see
Stage 5 (F-UJI, `05_assess_fuji.py`), which is the only stage name-checking
"FAIR" results in this pipeline. (F-UJI's own assessment targets are drawn
from the gold/silver subset below, so the two are NOT independent checks
on each other -- see the paper's Methods/Limitations.)

  gold    - Typically offers a persistent ID (DOI/accession), open
            metadata, long-term preservation commitment, license support.
            Examples: Zenodo, Dryad, GEO, SRA, UniProt, Figshare (with DOI)

  silver  - Typically offers most of the above, with some limitation.
            Examples: GitHub (no guaranteed persistence, but widely used),
            OSF (persistent but variable metadata quality),
            institutional repos (variable standards)

  bronze  - Provides access but with limited infrastructure support for
            the above.
            Examples: journal supplementary files, cloud storage links,
            personal/lab websites

  unknown - URL not matched to any known repository.

The script adds three new columns to da_link (ALTER TABLE if needed) and
writes results to a new `repo_classification` table linked to da_link.

Usage
-----
  python classify_repositories.py --db pmc_fair.db
  python classify_repositories.py --db pmc_fair.db --reset-classifications
  python classify_repositories.py --db pmc_fair.db --report
"""

import argparse
import logging
import re
import sqlite3
import sys
import time
import resource
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlparse

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

LOG_FMT  = "%(asctime)s  %(levelname)-7s  %(message)s"
LOG_DATE = "%Y-%m-%d %H:%M:%S"


def setup_logging(log_path: Path) -> logging.Logger:
    logger = logging.getLogger("classify_repos")
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
# Classification result dataclass
# ---------------------------------------------------------------------------

@dataclass
class RepoClassification:
    repo_name:          str  = "Unknown"
    repo_type:          str  = "other"
    is_known_repo:      int  = 0        # 0 / 1
    repo_infra_tier:    str  = "unknown"
    is_open_access:     int  = 0        # 0 / 1
    has_persistent_id:  int  = 0        # 0 / 1
    domain:             str  = ""


# ---------------------------------------------------------------------------
# Repository knowledge base
# Each entry:
#   domain_pattern  - regex matched against the URL's netloc (domain)
#   path_pattern    - optional regex matched against the URL path (None = skip)
#   repo_name       - display name
#   repo_type       - one of the type constants above
#   repo_infra_tier - gold / silver / bronze
#   is_open_access  - bool
#   has_persistent_id - bool
# ---------------------------------------------------------------------------

REPO_TYPE_GENERAL    = "general_purpose"
REPO_TYPE_DOMAIN     = "domain_specific"
REPO_TYPE_CODE       = "code_repository"
REPO_TYPE_JOURNAL    = "journal_platform"
REPO_TYPE_INSTITUTION= "institutional"
REPO_TYPE_CLOUD      = "cloud_storage"
REPO_TYPE_PREPRINT   = "preprint"
REPO_TYPE_REQUEST    = "request_only"
REPO_TYPE_REGISTRY   = "repository_registry"
REPO_TYPE_OTHER      = "other"

TIER_GOLD    = "gold"
TIER_SILVER  = "silver"
TIER_BRONZE  = "bronze"
TIER_UNKNOWN = "unknown"

# Each entry: (domain_regex, path_regex_or_None, name, type, tier, open_access, persistent_id)
REPOSITORY_KB: list[tuple] = [

    # ── General-purpose data repositories (Gold tier) ──────────────────────
    (r"zenodo\.org",             None,                   "Zenodo",           REPO_TYPE_GENERAL,  TIER_GOLD,   True,  True),
    (r"figshare\.com",           None,                   "Figshare",         REPO_TYPE_GENERAL,  TIER_GOLD,   True,  True),
    (r"datadryad\.org",          None,                   "Dryad",            REPO_TYPE_GENERAL,  TIER_GOLD,   True,  True),
    (r"dataverse\.",             None,                   "Dataverse",        REPO_TYPE_GENERAL,  TIER_GOLD,   True,  True),
    (r"dataverse\.harvard\.edu", None,                   "Harvard Dataverse",REPO_TYPE_GENERAL,  TIER_GOLD,   True,  True),
    (r"mendeley\.com",           r"/datasets?/",         "Mendeley Data",    REPO_TYPE_GENERAL,  TIER_GOLD,   True,  True),
    (r"data\.mendeley\.com",     None,                   "Mendeley Data",    REPO_TYPE_GENERAL,  TIER_GOLD,   True,  True),
    (r"openicpsr\.org",          None,                   "OpenICPSR",        REPO_TYPE_GENERAL,  TIER_GOLD,   True,  True),
    (r"repositori\.upf\.edu",    None,                   "UPF Repository",   REPO_TYPE_INSTITUTION, TIER_SILVER, True, True),
    (r"pangaea\.de",             None,                   "PANGAEA",          REPO_TYPE_DOMAIN,   TIER_GOLD,   True,  True),
    (r"re3data\.org",            None,                   "re3data",          REPO_TYPE_REGISTRY, TIER_UNKNOWN, False, False),
    (r"b2share\.eudat\.eu",      None,                   "EUDAT B2SHARE",    REPO_TYPE_GENERAL,  TIER_GOLD,   True,  True),
    (r"osf\.io",                 None,                   "OSF",              REPO_TYPE_GENERAL,  TIER_SILVER, True,  True),

    # ── Life-science domain-specific (Gold tier) ────────────────────────────
    (r"ncbi\.nlm\.nih\.gov/geo", None,                   "NCBI GEO",         REPO_TYPE_DOMAIN,   TIER_GOLD,   True,  True),
    (r"ncbi\.nlm\.nih\.gov/sra", None,                   "NCBI SRA",         REPO_TYPE_DOMAIN,   TIER_GOLD,   True,  True),
    (r"ebi\.ac\.uk/arrayexpress",None,                   "ArrayExpress",     REPO_TYPE_DOMAIN,   TIER_GOLD,   True,  True),
    (r"ebi\.ac\.uk/pride",       None,                   "PRIDE",            REPO_TYPE_DOMAIN,   TIER_GOLD,   True,  True),
    (r"ebi\.ac\.uk/ena",         None,                   "ENA",              REPO_TYPE_DOMAIN,   TIER_GOLD,   True,  True),
    (r"uniprot\.org",            None,                   "UniProt",          REPO_TYPE_DOMAIN,   TIER_GOLD,   True,  True),
    (r"rcsb\.org",               None,                   "PDB (RCSB)",       REPO_TYPE_DOMAIN,   TIER_GOLD,   True,  True),
    (r"wwpdb\.org",              None,                   "wwPDB",            REPO_TYPE_DOMAIN,   TIER_GOLD,   True,  True),
    (r"emdatabank\.org|emdb\.",  None,                   "EMDB",             REPO_TYPE_DOMAIN,   TIER_GOLD,   True,  True),
    (r"ncbi\.nlm\.nih\.gov/dbgap",None,                  "dbGaP",            REPO_TYPE_DOMAIN,   TIER_GOLD,   False, True),
    (r"ncbi\.nlm\.nih\.gov/genbank|ncbi\.nlm\.nih\.gov/nuccore", None, "GenBank", REPO_TYPE_DOMAIN, TIER_GOLD, True, True),
    (r"ncbi\.nlm\.nih\.gov/biosample", None,             "BioSample",        REPO_TYPE_DOMAIN,   TIER_GOLD,   True,  True),
    (r"ncbi\.nlm\.nih\.gov/bioproject", None,            "BioProject",       REPO_TYPE_DOMAIN,   TIER_GOLD,   True,  True),
    (r"ncbi\.nlm\.nih\.gov",     None,                   "NCBI",             REPO_TYPE_DOMAIN,   TIER_GOLD,   True,  True),
    (r"ebi\.ac\.uk",             None,                   "EMBL-EBI",         REPO_TYPE_DOMAIN,   TIER_GOLD,   True,  True),
    (r"metabolomicsworkbench\.org", None,                "Metabolomics Workbench", REPO_TYPE_DOMAIN, TIER_GOLD, True, True),
    (r"metabolights\.",          None,                   "MetaboLights",     REPO_TYPE_DOMAIN,   TIER_GOLD,   True,  True),
    (r"humanproteinatlas\.org",  None,                   "Human Protein Atlas", REPO_TYPE_DOMAIN, TIER_GOLD,  True,  True),
    (r"gtexportal\.org",         None,                   "GTEx",             REPO_TYPE_DOMAIN,   TIER_GOLD,   True,  True),
    (r"clinicaltrials\.gov",     None,                   "ClinicalTrials.gov",REPO_TYPE_DOMAIN,  TIER_GOLD,   True,  True),
    (r"immport\.org",            None,                   "ImmPort",          REPO_TYPE_DOMAIN,   TIER_GOLD,   True,  True),
    (r"idr\.openmicroscopy\.org",None,                   "IDR",              REPO_TYPE_DOMAIN,   TIER_GOLD,   True,  True),
    (r"morphic\.org",            None,                   "MorphIC",          REPO_TYPE_DOMAIN,   TIER_GOLD,   True,  True),
    (r"brain-map\.org|alleninstitute\.org", None,        "Allen Brain Atlas",REPO_TYPE_DOMAIN,   TIER_GOLD,   True,  True),
    (r"openneuro\.org",          None,                   "OpenNeuro",        REPO_TYPE_DOMAIN,   TIER_GOLD,   True,  True),
    (r"neurovault\.org",         None,                   "NeuroVault",       REPO_TYPE_DOMAIN,   TIER_SILVER, True,  True),
    (r"ieeg\.org",               None,                   "iEEG.org",         REPO_TYPE_DOMAIN,   TIER_SILVER, True,  True),
    (r"wordbank\.stanford\.edu", None,                   "Wordbank",         REPO_TYPE_DOMAIN,   TIER_SILVER, True,  True),

    # ── Physical/earth sciences ─────────────────────────────────────────────
    (r"earthdata\.nasa\.gov|daac\.", None,               "NASA EarthData",   REPO_TYPE_DOMAIN,   TIER_GOLD,   True,  True),
    (r"noaa\.gov",               None,                   "NOAA",             REPO_TYPE_DOMAIN,   TIER_GOLD,   True,  True),
    (r"iodp\.org",               None,                   "IODP",             REPO_TYPE_DOMAIN,   TIER_GOLD,   True,  True),
    (r"iris\.edu|ds\.iris\.edu", None,                   "IRIS/SAGE",        REPO_TYPE_DOMAIN,   TIER_GOLD,   True,  True),
    (r"unavco\.org",             None,                   "UNAVCO",           REPO_TYPE_DOMAIN,   TIER_GOLD,   True,  True),

    # ── Social science / economics ──────────────────────────────────────────
    (r"icpsr\.umich\.edu",       None,                   "ICPSR",            REPO_TYPE_DOMAIN,   TIER_GOLD,   True,  True),
    (r"ukdataservice\.ac\.uk",   None,                   "UK Data Service",  REPO_TYPE_DOMAIN,   TIER_GOLD,   True,  True),
    (r"gesis\.org",              None,                   "GESIS",            REPO_TYPE_DOMAIN,   TIER_GOLD,   True,  True),

    # ── Code repositories (Silver tier — no guaranteed persistence) ─────────
    (r"github\.com",             None,                   "GitHub",           REPO_TYPE_CODE,     TIER_SILVER, True,  False),
    (r"gitlab\.com",             None,                   "GitLab",           REPO_TYPE_CODE,     TIER_SILVER, True,  False),
    (r"bitbucket\.org",          None,                   "Bitbucket",        REPO_TYPE_CODE,     TIER_SILVER, True,  False),
    (r"sourceforge\.net",        None,                   "SourceForge",      REPO_TYPE_CODE,     TIER_SILVER, True,  False),
    (r"codeocean\.com",          None,                   "Code Ocean",       REPO_TYPE_CODE,     TIER_GOLD,   True,  True),
    (r"codepen\.io",             None,                   "CodePen",          REPO_TYPE_CODE,     TIER_BRONZE, True,  False),
    (r"replit\.com",             None,                   "Replit",           REPO_TYPE_CODE,     TIER_BRONZE, True,  False),
    (r"mybinder\.org|binder\.",  None,                   "Binder",           REPO_TYPE_CODE,     TIER_SILVER, True,  False),

    # ── Preprint servers ────────────────────────────────────────────────────
    (r"biorxiv\.org",            None,                   "bioRxiv",          REPO_TYPE_PREPRINT,  TIER_SILVER, True,  True),
    (r"arxiv\.org",              None,                   "arXiv",            REPO_TYPE_PREPRINT,  TIER_SILVER, True,  True),
    (r"medrxiv\.org",            None,                   "medRxiv",          REPO_TYPE_PREPRINT,  TIER_SILVER, True,  True),
    (r"preprints\.org",          None,                   "Preprints.org",    REPO_TYPE_PREPRINT,  TIER_SILVER, True,  True),
    (r"ssrn\.com",               None,                   "SSRN",             REPO_TYPE_PREPRINT,  TIER_BRONZE, True,  False),

    # ── Cloud/file storage (Bronze — low FAIR) ──────────────────────────────
    (r"dropbox\.com",            None,                   "Dropbox",          REPO_TYPE_CLOUD,    TIER_BRONZE, True,  False),
    (r"drive\.google\.com|docs\.google\.com", None,      "Google Drive",     REPO_TYPE_CLOUD,    TIER_BRONZE, True,  False),
    (r"onedrive\.live\.com|1drv\.ms", None,              "OneDrive",         REPO_TYPE_CLOUD,    TIER_BRONZE, True,  False),
    (r"box\.com",                None,                   "Box",              REPO_TYPE_CLOUD,    TIER_BRONZE, True,  False),
    (r"sharepoint\.com",         None,                   "SharePoint",       REPO_TYPE_CLOUD,    TIER_BRONZE, False, False),
    (r"wetransfer\.com",         None,                   "WeTransfer",       REPO_TYPE_CLOUD,    TIER_BRONZE, True,  False),

    # ── Publisher/journal platforms (Bronze — supplementary files) ──────────
    (r"nature\.com",             r"/articles?/.+/suppl", "Nature Supplementary", REPO_TYPE_JOURNAL, TIER_BRONZE, True, False),
    (r"science\.org|sciencemag\.org", r"/suppl/",        "Science Supplementary",REPO_TYPE_JOURNAL, TIER_BRONZE, True, False),
    (r"plos\.org",               None,                   "PLOS",             REPO_TYPE_JOURNAL,  TIER_SILVER, True,  True),
    (r"frontiersin\.org",        None,                   "Frontiers",        REPO_TYPE_JOURNAL,  TIER_SILVER, True,  False),
    (r"springer\.com|link\.springer",None,               "Springer",         REPO_TYPE_JOURNAL,  TIER_BRONZE, False, False),
    (r"elsevier\.com|sciencedirect", None,               "Elsevier",         REPO_TYPE_JOURNAL,  TIER_BRONZE, False, False),
    (r"wiley\.com|onlinelibrary\.wiley", None,           "Wiley",            REPO_TYPE_JOURNAL,  TIER_BRONZE, False, False),
    (r"tandfonline\.com",        None,                   "Taylor & Francis",  REPO_TYPE_JOURNAL,  TIER_BRONZE, False, False),
    (r"oup\.com|academic\.oup",  None,                   "Oxford Academic",   REPO_TYPE_JOURNAL,  TIER_BRONZE, False, False),
    (r"journals\.plos\.org",     None,                   "PLOS",             REPO_TYPE_JOURNAL,  TIER_SILVER, True,  True),
    (r"f1000research\.com",      None,                   "F1000Research",    REPO_TYPE_JOURNAL,  TIER_SILVER, True,  True),
    (r"gigadb\.org",             None,                   "GigaDB",           REPO_TYPE_DOMAIN,   TIER_GOLD,   True,  True),

    # ── Request-only signals (detected by mailto: or keyword) ───────────────
    (r"^mailto:",                None,                   "Email (request)",  REPO_TYPE_REQUEST,  TIER_BRONZE, False, False),
]

# Compile domain patterns once
_COMPILED_KB: list[tuple] = [
    (re.compile(dom, re.IGNORECASE),
     re.compile(path, re.IGNORECASE) if path else None,
     name, rtype, tier, oa, pid)
    for dom, path, name, rtype, tier, oa, pid in REPOSITORY_KB
]

# Keywords in the URL or domain that suggest institutional repos
_INSTITUTIONAL_PATTERNS = re.compile(
    r"\.edu$|\.edu\.|\.ac\.\w{2}$|\.ac\.\w{2}\.|repository\.|repo\.|institutional|dspace|eprints|pure\.",
    re.IGNORECASE,
)

# "Available on request" signal in label text
_REQUEST_PATTERN = re.compile(
    r"available\s+(?:upon|on)\s+request|request\s+(?:the\s+)?data|contact\s+(?:the\s+)?author",
    re.IGNORECASE,
)


# ---------------------------------------------------------------------------
# Core classification function
# ---------------------------------------------------------------------------

def classify_url(url: str, label: str = "") -> RepoClassification:
    """
    Classify a single URL into a RepoClassification.
    Falls back gracefully for malformed URLs.
    """
    result = RepoClassification()

    if not url:
        return result

    # Extract domain
    try:
        parsed = urlparse(url if "://" in url else f"https://{url}")
        domain = parsed.netloc.lower().removeprefix("www.")
        path   = parsed.path
    except Exception:
        domain = url.lower()
        path   = ""

    result.domain = domain

    # Check request-only signals first
    if url.startswith("mailto:") or _REQUEST_PATTERN.search(label):
        result.repo_name         = "Email (request)"
        result.repo_type         = REPO_TYPE_REQUEST
        result.repo_infra_tier   = TIER_BRONZE
        # Not a repository match — a contact-request signal, so don't count
        # it toward "known repository" rates.
        result.is_known_repo     = 0
        result.is_open_access    = 0
        result.has_persistent_id = 0
        return result

    # Match against knowledge base
    full_url = url.lower()
    for dom_pat, path_pat, name, rtype, tier, oa, pid in _COMPILED_KB:
        if dom_pat.search(domain) or dom_pat.search(full_url):
            if path_pat is None or path_pat.search(path):
                result.repo_name         = name
                result.repo_type         = rtype
                result.repo_infra_tier   = tier
                result.is_known_repo     = 1
                result.is_open_access    = int(oa)
                result.has_persistent_id = int(pid)
                return result

    # Heuristic fallback: institutional repository signal
    if _INSTITUTIONAL_PATTERNS.search(domain) or _INSTITUTIONAL_PATTERNS.search(path):
        result.repo_name       = f"Institutional ({domain})"
        result.repo_type       = REPO_TYPE_INSTITUTION
        result.repo_infra_tier = TIER_SILVER
        result.is_known_repo = 0
        return result

    # Unknown
    result.repo_name = domain if domain else "Unknown"
    result.repo_type = REPO_TYPE_OTHER
    result.repo_infra_tier = TIER_UNKNOWN
    return result


# ---------------------------------------------------------------------------
# Schema additions
# ---------------------------------------------------------------------------

CLASSIFICATION_SCHEMA = """
-- Repository classification for each da_link
CREATE TABLE IF NOT EXISTS repo_classification (
    class_id         INTEGER PRIMARY KEY AUTOINCREMENT,
    link_id          INTEGER NOT NULL UNIQUE REFERENCES da_link(link_id),
    domain           TEXT,
    repo_name        TEXT,
    repo_type        TEXT,    -- general_purpose / domain_specific / code_repository /
                              --   journal_platform / institutional / cloud_storage /
                              --   preprint / request_only / other
    repo_infra_tier  TEXT,    -- gold / silver / bronze / unknown
    is_known_repo    INTEGER, -- 0 / 1
    is_open_access   INTEGER, -- 0 / 1
    has_persistent_id INTEGER -- 0 / 1
);

CREATE INDEX IF NOT EXISTS idx_rc_link     ON repo_classification(link_id);
CREATE INDEX IF NOT EXISTS idx_rc_type     ON repo_classification(repo_type);
CREATE INDEX IF NOT EXISTS idx_rc_tier     ON repo_classification(repo_infra_tier);
CREATE INDEX IF NOT EXISTS idx_rc_domain   ON repo_classification(domain);
CREATE INDEX IF NOT EXISTS idx_rc_known    ON repo_classification(is_known_repo);
"""

RESET_CLASSIFICATION = ["repo_classification"]


# ---------------------------------------------------------------------------
# DB operations
# ---------------------------------------------------------------------------

def ensure_schema(con: sqlite3.Connection, logger: logging.Logger) -> None:
    con.executescript(CLASSIFICATION_SCHEMA)
    con.commit()
    logger.info("  repo_classification table ready.")


def get_unclassified_links(con: sqlite3.Connection) -> list[tuple]:
    """Return (link_id, url, label) for all da_link rows not yet classified."""
    return con.execute("""
        SELECT dl.link_id, dl.url, COALESCE(dl.label, '')
        FROM da_link dl
        LEFT JOIN repo_classification rc ON rc.link_id = dl.link_id
        WHERE rc.link_id IS NULL
          AND dl.url IS NOT NULL
          AND dl.url != ''
        ORDER BY dl.link_id
    """).fetchall()


def insert_classifications(con: sqlite3.Connection,
                            rows: list[tuple],
                            logger: logging.Logger) -> dict:
    """
    Classify each (link_id, url, label) and insert into repo_classification.
    Returns a summary dict of counts by tier and type.
    """
    cur = con.cursor()
    counts: dict = {
        "total": 0,
        "tiers": {TIER_GOLD: 0, TIER_SILVER: 0, TIER_BRONZE: 0, TIER_UNKNOWN: 0},
        "types": {},
        "known": 0,
    }

    for i, (link_id, url, label) in enumerate(rows, 1):
        rc = classify_url(url, label)

        cur.execute("""
            INSERT OR IGNORE INTO repo_classification
                (link_id, domain, repo_name, repo_type,
                 repo_infra_tier, is_known_repo, is_open_access, has_persistent_id)
            VALUES (?,?,?,?,?,?,?,?)
        """, (link_id, rc.domain, rc.repo_name, rc.repo_type,
              rc.repo_infra_tier, rc.is_known_repo, rc.is_open_access, rc.has_persistent_id))

        counts["total"]             += 1
        counts["tiers"][rc.repo_infra_tier] = counts["tiers"].get(rc.repo_infra_tier, 0) + 1
        counts["types"][rc.repo_type] = counts["types"].get(rc.repo_type, 0) + 1
        if rc.is_known_repo:
            counts["known"] += 1

        if i % 1000 == 0:
            con.commit()
            logger.info(f"  Classified {i:,}/{len(rows):,} links …")

    con.commit()
    return counts


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------

def print_report(con: sqlite3.Connection, logger: logging.Logger) -> None:
    logger.info("")
    logger.info("=" * 62)
    logger.info("REPOSITORY CLASSIFICATION REPORT")
    logger.info("=" * 62)

    # Overall totals
    total = con.execute("SELECT COUNT(*) FROM repo_classification").fetchone()[0]
    known = con.execute("SELECT COUNT(*) FROM repo_classification WHERE is_known_repo=1").fetchone()[0]
    logger.info(f"  Total classified links : {total:>10,}")
    logger.info(f"  Matched to known repo  : {known:>10,}  ({known/total*100:.1f}%)" if total else "")

    # By repository infrastructure tier
    logger.info("")
    logger.info("  Repository Infrastructure Tier Distribution")
    logger.info("  " + "─" * 40)
    for tier in (TIER_GOLD, TIER_SILVER, TIER_BRONZE, TIER_UNKNOWN):
        n = con.execute(
            "SELECT COUNT(*) FROM repo_classification WHERE repo_infra_tier=?", (tier,)
        ).fetchone()[0]
        bar = "█" * int(n / max(total, 1) * 30)
        logger.info(f"  {tier:<10} {n:>8,}  ({n/total*100:5.1f}%)  {bar}" if total else f"  {tier:<10} {n:>8,}")

    # By repo type
    logger.info("")
    logger.info("  Repository Type Distribution")
    logger.info("  " + "─" * 40)
    rows = con.execute("""
        SELECT repo_type, COUNT(*) as n
        FROM repo_classification
        GROUP BY repo_type ORDER BY n DESC
    """).fetchall()
    for rtype, n in rows:
        logger.info(f"  {rtype:<22} {n:>8,}  ({n/total*100:5.1f}%)" if total else f"  {rtype:<22} {n:>8,}")

    # Top 20 repositories
    logger.info("")
    logger.info("  Top 20 Repositories by Link Count")
    logger.info("  " + "─" * 50)
    top = con.execute("""
        SELECT repo_name, repo_infra_tier, COUNT(*) as n
        FROM repo_classification
        WHERE is_known_repo = 1
        GROUP BY repo_name ORDER BY n DESC LIMIT 20
    """).fetchall()
    for name, tier, n in top:
        logger.info(f"  {name:<30} {tier:<8}  {n:>8,}")

    # Year-over-year breakdown (requires join to article)
    logger.info("")
    logger.info("  Repository Infrastructure Tier by Publication Year")
    logger.info("  " + "─" * 62)
    header = f"  {'Year':<6} {'Gold':>8} {'Silver':>8} {'Bronze':>8} {'Unknown':>8} {'Total':>8}"
    logger.info(header)
    year_rows = con.execute("""
        SELECT a.year,
               SUM(CASE WHEN rc.repo_infra_tier='gold'    THEN 1 ELSE 0 END) AS gold,
               SUM(CASE WHEN rc.repo_infra_tier='silver'  THEN 1 ELSE 0 END) AS silver,
               SUM(CASE WHEN rc.repo_infra_tier='bronze'  THEN 1 ELSE 0 END) AS bronze,
               SUM(CASE WHEN rc.repo_infra_tier='unknown' THEN 1 ELSE 0 END) AS unknown,
               COUNT(*) AS total
        FROM repo_classification rc
        JOIN da_link dl      ON dl.link_id   = rc.link_id
        JOIN data_availability da ON da.da_id = dl.da_id
        JOIN article a       ON a.article_id = da.article_id
        WHERE a.year IS NOT NULL
        GROUP BY a.year
        ORDER BY a.year
    """).fetchall()
    for yr, gold, silver, bronze, unknown, tot in year_rows:
        logger.info(f"  {yr:<6} {gold:>8,} {silver:>8,} {bronze:>8,} {unknown:>8,} {tot:>8,}")

    # "Available on request" signal
    logger.info("")
    req_count = con.execute(
        "SELECT COUNT(*) FROM repo_classification WHERE repo_type='request_only'"
    ).fetchone()[0]
    logger.info(f"  'Available on request' links  : {req_count:>8,}")

    # Open access vs restricted
    oa_count = con.execute(
        "SELECT COUNT(*) FROM repo_classification WHERE is_open_access=1"
    ).fetchone()[0]
    logger.info(f"  Open access links             : {oa_count:>8,}")

    # Persistent ID
    pid_count = con.execute(
        "SELECT COUNT(*) FROM repo_classification WHERE has_persistent_id=1"
    ).fetchone()[0]
    logger.info(f"  Links with persistent ID      : {pid_count:>8,}")

    logger.info("=" * 62)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Classify repository URLs in da_link — FAIR study Stage 3.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python classify_repositories.py --db pmc_fair.db
  python classify_repositories.py --db pmc_fair.db --reset-classifications
  python classify_repositories.py --db pmc_fair.db --report
        """,
    )
    parser.add_argument("--db", default="pmc_fair.db",
        help="SQLite database file (default: pmc_fair.db).")
    parser.add_argument("--reset-classifications", action="store_true",
        help="Drop and rebuild the repo_classification table.")
    parser.add_argument("--report", action="store_true",
        help="Print classification report (can be combined with classification run).")
    args = parser.parse_args()

    db_path  = Path(args.db)
    log_path = db_path.with_suffix(".classify.log")

    if not db_path.exists():
        sys.exit(f"Database not found: {db_path}")

    logger = setup_logging(log_path)
    t0     = time.perf_counter()
    rss0   = _peak_rss_mb()

    logger.info("=" * 60)
    logger.info("Repository Classification  —  FAIR Longitudinal Study (Stage 3)")
    logger.info("=" * 60)
    logger.info(f"  Database         : {db_path}")
    logger.info(f"  Log              : {log_path}")
    logger.info(f"  Known repos in KB: {len(REPOSITORY_KB)}")
    logger.info("")

    con = sqlite3.connect(db_path)
    con.execute("PRAGMA foreign_keys = ON")

    # Reset if requested
    if args.reset_classifications:
        logger.info("  --reset-classifications: dropping repo_classification …")
        for tbl in RESET_CLASSIFICATION:
            con.execute(f"DROP TABLE IF EXISTS {tbl}")
        con.commit()

    ensure_schema(con, logger)

    # Fetch unclassified links
    unclassified = get_unclassified_links(con)
    logger.info(f"  Links to classify: {len(unclassified):,}")

    if unclassified:
        logger.info("  Classifying …")
        counts = insert_classifications(con, unclassified, logger)

        elapsed  = time.perf_counter() - t0
        peak_rss = max(_peak_rss_mb() - rss0, 0)

        logger.info(f"  Done. {counts['total']:,} links classified in {elapsed:.1f}s | +{peak_rss:.1f} MB RSS")
        logger.info(f"  Known repo matches : {counts['known']:,}")
        logger.info(f"  Tier breakdown     : "
                    + "  ".join(f"{k}={v:,}" for k, v in counts["tiers"].items()))
    else:
        logger.info("  No unclassified links found. All links already classified.")
        logger.info("  (Use --reset-classifications to reclassify everything.)")

    # Report
    if args.report or not unclassified:
        print_report(con, logger)

    con.close()
    logger.info(f"\nDone. Log: {log_path}")


if __name__ == "__main__":
    main()
