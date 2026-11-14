"""
assess_fuji.py
--------------
Stage 5 of the FAIR longitudinal study pipeline.

Runs automated FAIR assessments via a self-hosted F-UJI instance against
URLs and DOIs collected in Stages 1–3.  Results are stored in two new tables:

  fuji_assessment  — one row per assessed URL/DOI (overall scores)
  fuji_metric      — one row per FAIR metric per assessment (16 metrics)

What is assessed
----------------
F-UJI evaluates *dataset landing pages* — it cannot assess arbitrary HTML
pages or author websites.  We therefore limit assessments to URLs that:

  (a) come from known, FAIR-capable repositories (gold/silver tier in
      repo_classification), OR
  (b) are DOIs that resolve to a data repository landing page

Deduplication strategy
----------------------
Many articles link to the same dataset (e.g. the same Zenodo DOI appears in
hundreds of papers).  We deduplicate by unique (normalised_url) before
sending to F-UJI, so each dataset is assessed once regardless of how many
articles reference it.  Results are then propagated back to all articles
that share the same URL.

Sampling
--------
For large corpora (13 years × many articles) a full run can take days.
Use --sample N to assess a random sample of N URLs per year, or
--domains zenodo.org,figshare.com to restrict to specific repositories.

Self-hosted F-UJI setup (required)
-----------------------------------
F-UJI must be running locally.  Quickest setup with Docker:

  docker run -d -p 1071:1071 ghcr.io/pangaea-data-publisher/fuji

Then run this script with:

  python assess_fuji.py --db pmc_fair.db --fuji-url http://localhost:1071

F-UJI uses HTTP Basic Authentication.  Default credentials are
username=marvel, password=wonderwoman (set in fuji_server/config/users.py).
Override with --fuji-user / --fuji-pass.

API endpoint used
-----------------
POST http://localhost:1071/fuji/api/v1/evaluate
Content-Type: application/json
Authorization: Basic <base64(user:pass)>   (marvel/wonderwoman by default)

Request body:
{
  "object_identifier":      "https://doi.org/10.5281/zenodo.123456",
  "test_debug":             false,
  "use_datacite":           true,
  "metadata_service_endpoint": "",
  "metadata_service_type":  "oai_pmh"
}

Verify F-UJI is running:
  curl http://localhost:1071/fuji/api/v1/ui/
  (opens the Swagger UI — if you see JSON or HTML, the server is up)

Response (abbreviated):
{
  "id": "...",
  "score": {"earned": 9, "total": 17},
  "summary": {
    "score_percent": {"FAIR": 52.9, "F": 75.0, "A": 50.0, "I": 25.0, "R": 50.0},
    "maturity":      {"FAIR": 1,    "F": 2,     "A": 1,    "I": 0,    "R": 1}
  },
  "results": [
    {
      "metric_identifier": "FsF-F1-01D",
      "metric_name": "Globally unique identifier",
      "score": {"earned": 1, "total": 1},
      "maturity": 3,
      "test_status": "pass",
      ...
    }, ...
  ]
}

Schema added (two tables)
--------------------------
fuji_assessment
  assessment_id   INTEGER PK
  url             TEXT UNIQUE NOT NULL   -- the URL/DOI assessed
  assessed_at     TEXT                   -- ISO timestamp
  http_status     INTEGER                -- HTTP status of the F-UJI call
  error_message   TEXT                   -- if F-UJI returned an error
  score_earned    INTEGER                -- overall score earned
  score_total     INTEGER                -- overall score total
  score_pct_fair  REAL                   -- % FAIR overall
  score_pct_f     REAL                   -- % Findable
  score_pct_a     REAL                   -- % Accessible
  score_pct_i     REAL                   -- % Interoperable
  score_pct_r     REAL                   -- % Reusable
  maturity_fair   INTEGER                -- 0=incomplete,1=initial,2=moderate,3=advanced
  maturity_f      INTEGER
  maturity_a      INTEGER
  maturity_i      INTEGER
  maturity_r      INTEGER

fuji_metric
  metric_id       INTEGER PK
  assessment_id   INTEGER FK → fuji_assessment
  metric_identifier TEXT                 -- e.g. "FsF-F1-01D"
  metric_name     TEXT
  principle       TEXT                   -- F / A / I / R
  score_earned    INTEGER
  score_total     INTEGER
  maturity        INTEGER
  test_status     TEXT                   -- pass / fail / incomplete

fuji_article_link
  link_id         INTEGER PK
  assessment_id   INTEGER FK → fuji_assessment
  article_id      INTEGER FK → article
  -- maps each article to every F-UJI assessment of a URL it referenced

Usage
-----
  # Assess all gold/silver URLs (deduped), 4 concurrent workers
  python assess_fuji.py --db pmc_fair.db

  # Restrict to specific domains
  python assess_fuji.py --db pmc_fair.db --domains zenodo.org,figshare.com

  # Random sample: 50 unique URLs per year
  python assess_fuji.py --db pmc_fair.db --sample 50

  # Use a remote F-UJI instance with custom credentials
  python assess_fuji.py --db pmc_fair.db \\
      --fuji-url http://myserver:1071 \\
      --fuji-user myuser --fuji-pass mypass

  # Dry run: show what would be assessed without calling F-UJI
  python assess_fuji.py --db pmc_fair.db --dry-run

  # Report after a completed run
  python assess_fuji.py --db pmc_fair.db --report
"""

import argparse
import asyncio
import base64
import json
import logging
import random
import re
import resource
import sqlite3
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

try:
    import aiohttp
except ImportError:
    sys.exit(
        "aiohttp is required for Stage 5.\n"
        "Install with:  pip install aiohttp\n"
        "or:            pip install aiohttp --break-system-packages"
    )

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

LOG_FMT  = "%(asctime)s  %(levelname)-7s  %(message)s"
LOG_DATE = "%Y-%m-%d %H:%M:%S"


def setup_logging(log_path: Path) -> logging.Logger:
    logger = logging.getLogger("assess_fuji")
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
# Constants
# ---------------------------------------------------------------------------

FUJI_ENDPOINT   = "/fuji/api/v1/evaluate"
DEFAULT_USER    = "marvel"
DEFAULT_PASS    = "wonderwoman"
# metric_version is server-side config in F-UJI; not sent in evaluate request body
DEFAULT_WORKERS = 4
DEFAULT_TIMEOUT = 120   # seconds per F-UJI call (assessments can be slow)

# Only assess URLs from repos we know F-UJI can meaningfully evaluate.
# F-UJI works best with DOIs and repositories that expose structured metadata.
ASSESSABLE_TIERS = {"gold", "silver"}

# Domains excluded from F-UJI assessment.
#
# CATEGORY 1 — Cloud storage / personal file hosts: F-UJI always scores ~0
SKIP_DOMAINS_CLOUD = {
    "dropbox.com", "drive.google.com", "box.com", "onedrive.live.com",
    "wetransfer.com", "mega.nz", "mediafire.com",
}

# CATEGORY 2 — Domain-specific DB landing pages: these repos are all gold-tier
# and already classified in Stage 3. F-UJI cannot parse their metadata formats
# well AND they enforce strict rate limits (RCSB: 100/min, NCBI: 3/s without
# key, UniProt: 10/s). Assessments would be noisy and waste quota.
# Their FAIR compliance is better captured by Stage 3 (repo_classification)
# and Stage 4 (das_category) than by F-UJI landing-page scraping.
SKIP_DOMAINS_DB = {
    "www.rcsb.org", "rcsb.org",                          # PDB  — 429 at scale
    "www.ncbi.nlm.nih.gov", "ncbi.nlm.nih.gov",          # GEO, SRA, GenBank, dbGaP
    "ebi.ac.uk", "www.ebi.ac.uk",                         # ArrayExpress, UniProt EBI
    "uniprot.org", "www.uniprot.org",                     # UniProt
    "www.ebi.ac.uk",
    "pride.ebi.ac.uk",                                    # PRIDE
    "immport.org", "www.immport.org",                     # ImmPort
    "idr.openmicroscopy.org",                             # IDR
}

SKIP_DOMAINS = SKIP_DOMAINS_CLOUD | SKIP_DOMAINS_DB | {"", "mailto:"}

# Per-domain rate limits (max requests per second).
# Applied on top of the global --workers concurrency limit.
# If a domain is not listed here, no extra throttle is applied.
DOMAIN_RATE_LIMITS: dict[str, float] = {
    "zenodo.org":           2.0,   # generous but polite
    "figshare.com":         2.0,
    "datadryad.org":        1.0,
    "osf.io":               2.0,
    "github.com":           1.0,   # not a data repo, but sometimes assessed
    "dataverse.harvard.edu": 1.0,
    "data.mendeley.com":    1.0,
    "pangaea.de":           1.0,
}

# Tracks last-request time per domain for throttling
_domain_last_call: dict[str, float] = {}
_domain_locks:     dict[str, "asyncio.Lock"] = {}


def _get_domain(url: str) -> str:
    try:
        return urlparse(url).netloc.lower().lstrip("www.")
    except Exception:
        return ""


async def _throttle_domain(domain: str) -> None:
    """Sleep if needed to respect per-domain rate limit."""
    if domain not in DOMAIN_RATE_LIMITS:
        return
    if domain not in _domain_locks:
        _domain_locks[domain] = asyncio.Lock()
    async with _domain_locks[domain]:
        min_interval = 1.0 / DOMAIN_RATE_LIMITS[domain]
        last = _domain_last_call.get(domain, 0.0)
        wait = min_interval - (asyncio.get_event_loop().time() - last)
        if wait > 0:
            await asyncio.sleep(wait)
        _domain_last_call[domain] = asyncio.get_event_loop().time()


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass
class AssessmentTarget:
    url: str
    article_ids: list[int]      = field(default_factory=list)
    repo_name:   str            = ""
    repo_infra_tier:   str            = ""
    pub_years:   list[int]      = field(default_factory=list)


@dataclass
class FujiResult:
    url:           str
    http_status:   int   = 0
    error_message: str   = ""
    score_earned:  int   = 0
    score_total:   int   = 0
    score_pct_fair: float = 0.0
    score_pct_f:   float = 0.0
    score_pct_a:   float = 0.0
    score_pct_i:   float = 0.0
    score_pct_r:   float = 0.0
    maturity_fair: int   = 0
    maturity_f:    int   = 0
    maturity_a:    int   = 0
    maturity_i:    int   = 0
    maturity_r:    int   = 0
    metrics:       list  = field(default_factory=list)   # list of dicts


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

SCHEMA = """
CREATE TABLE IF NOT EXISTS fuji_assessment (
    assessment_id   INTEGER PRIMARY KEY AUTOINCREMENT,
    url             TEXT NOT NULL UNIQUE,
    assessed_at     TEXT,
    http_status     INTEGER,
    error_message   TEXT,
    score_earned    INTEGER,
    score_total     INTEGER,
    score_pct_fair  REAL,
    score_pct_f     REAL,
    score_pct_a     REAL,
    score_pct_i     REAL,
    score_pct_r     REAL,
    maturity_fair   INTEGER,
    maturity_f      INTEGER,
    maturity_a      INTEGER,
    maturity_i      INTEGER,
    maturity_r      INTEGER
);

CREATE TABLE IF NOT EXISTS fuji_metric (
    metric_id           INTEGER PRIMARY KEY AUTOINCREMENT,
    assessment_id       INTEGER NOT NULL REFERENCES fuji_assessment(assessment_id),
    metric_identifier   TEXT NOT NULL,
    metric_name         TEXT,
    principle           TEXT,
    score_earned        INTEGER,
    score_total         INTEGER,
    maturity            INTEGER,
    test_status         TEXT
);

CREATE TABLE IF NOT EXISTS fuji_article_link (
    link_id         INTEGER PRIMARY KEY AUTOINCREMENT,
    assessment_id   INTEGER NOT NULL REFERENCES fuji_assessment(assessment_id),
    article_id      INTEGER NOT NULL REFERENCES article(article_id),
    UNIQUE(assessment_id, article_id)
);

CREATE INDEX IF NOT EXISTS idx_fa_url        ON fuji_assessment(url);
CREATE INDEX IF NOT EXISTS idx_fm_assess     ON fuji_metric(assessment_id);
CREATE INDEX IF NOT EXISTS idx_fm_metric     ON fuji_metric(metric_identifier);
CREATE INDEX IF NOT EXISTS idx_fal_article   ON fuji_article_link(article_id);
"""

RESET_TABLES = ["fuji_article_link", "fuji_metric", "fuji_assessment"]


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

def ensure_schema(con: sqlite3.Connection, logger: logging.Logger) -> None:
    con.executescript(SCHEMA)
    con.commit()
    logger.info("  fuji_assessment / fuji_metric / fuji_article_link tables ready.")


def get_already_assessed(con: sqlite3.Connection) -> set[str]:
    return {row[0] for row in con.execute("SELECT url FROM fuji_assessment")}


def _normalise_url(url: str) -> str:
    """Lowercase scheme+host, strip trailing slashes and tracking params."""
    url = url.strip()
    try:
        p = urlparse(url)
        # Prefer DOI form for anything that looks like a DOI URL
        if "doi.org" in (p.netloc or ""):
            return url.rstrip("/").lower()
        return url.rstrip("/")
    except Exception:
        return url


def get_targets(
    con: sqlite3.Connection,
    already_assessed: set[str],
    domains_filter: set[str] | None,
    logger: logging.Logger,
) -> list[AssessmentTarget]:
    """
    Build the list of unique URLs to assess.

    Priority order:
      1. DOIs from da_link that point to gold/silver repos
      2. Non-DOI URLs from da_link that are gold/silver
      3. DOIs from accession_number (accession_url column)

    Returns one AssessmentTarget per unique URL, with all referencing
    article_ids and pub_years filled in.
    """
    logger.info("  Collecting assessment targets …")

    # ── Source 1: da_link × repo_classification ───────────────────────────
    # NOTE: restricting to gold/silver repo_infra_tier means this F-UJI
    # sample is NOT independent of Stage 3's repository-type heuristic --
    # it's drawn FROM the set that heuristic already called high-quality.
    # Don't describe F-UJI results as independently "corroborating" the
    # tier heuristic in the paper; state this sampling restriction instead.
    rows = con.execute("""
        SELECT
            dl.url,
            rc.repo_name,
            rc.repo_infra_tier,
            da.article_id,
            a.pub_year
        FROM da_link dl
        JOIN repo_classification rc ON rc.link_id = dl.link_id
        JOIN data_availability  da ON da.da_id    = dl.da_id
        JOIN article             a  ON a.article_id = da.article_id
        WHERE rc.repo_infra_tier IN ('gold','silver')
          AND dl.url IS NOT NULL
          AND dl.url != ''
          AND dl.url NOT LIKE 'mailto:%'
        ORDER BY dl.url
    """).fetchall()


    # Merge
    all_rows = [(r[0], r[1], r[2], r[3], r[4]) for r in rows]

    # Group by normalised URL
    targets: dict[str, AssessmentTarget] = {}
    skipped_domain = 0
    skipped_assessed = 0

    for url, repo_name, repo_infra_tier, article_id, pub_year in all_rows:
        if not url:
            continue
        norm = _normalise_url(url)
        if not norm or norm.startswith("mailto:"):
            continue

        # Domain filter
        try:
            domain = urlparse(norm).netloc.lower().lstrip("www.")
        except Exception:
            domain = ""

        if domain in SKIP_DOMAINS:
            skipped_domain += 1
            continue
        # Also skip bare IP addresses and localhost
        if re.match(r"^\d+\.\d+\.\d+\.\d+$", domain) or domain in ("localhost", "127.0.0.1"):
            skipped_domain += 1
            continue

        if domains_filter and not any(
            domain == d or domain.endswith("." + d) for d in domains_filter
        ):
            continue

        if norm in already_assessed:
            skipped_assessed += 1
            continue

        if norm not in targets:
            targets[norm] = AssessmentTarget(
                url=norm, repo_name=repo_name or "", repo_infra_tier=repo_infra_tier or "unknown"
            )
        t = targets[norm]
        if article_id and article_id not in t.article_ids:
            t.article_ids.append(article_id)
        if pub_year and pub_year not in t.pub_years:
            t.pub_years.append(pub_year)

    logger.info(
        f"  Unique assessable URLs: {len(targets):,}  "
        f"(skipped already-assessed: {skipped_assessed:,}, "
        f"skipped bad domains: {skipped_domain:,})"
    )
    return list(targets.values())


def apply_sample(
    targets: list[AssessmentTarget],
    sample_n: int,
    logger: logging.Logger,
) -> list[AssessmentTarget]:
    """
    If --sample N is set, take N random targets per publication year.
    This gives a temporally stratified sample, which is ideal for
    year-over-year trend analysis.
    """
    if not sample_n or sample_n <= 0:
        return targets

    # Group by earliest pub year
    by_year: dict[int, list] = {}
    for t in targets:
        yr = min(t.pub_years) if t.pub_years else 0
        by_year.setdefault(yr, []).append(t)

    sampled = []
    for yr in sorted(by_year):
        pool = by_year[yr]
        take = min(sample_n, len(pool))
        chosen = random.sample(pool, take)
        sampled.extend(chosen)
        logger.info(f"    Year {yr}: {len(pool):>6,} available → {take} sampled")

    logger.info(f"  Total after sampling: {len(sampled):,}")
    return sampled


# ---------------------------------------------------------------------------
# F-UJI API call
# ---------------------------------------------------------------------------

def _auth_header(user: str, pwd: str) -> str:
    token = base64.b64encode(f"{user}:{pwd}".encode()).decode()
    return f"Basic {token}"


def _extract_principle(metric_id: str) -> str:
    """Extract FAIR principle letter from metric identifier e.g. FsF-F1-01D → F"""
    m = re.match(r"FsF-([FAIR])", metric_id, re.IGNORECASE)
    return m.group(1).upper() if m else "?"


async def call_fuji(
    session:     "aiohttp.ClientSession",
    target:      AssessmentTarget,
    fuji_url:    str,
    auth_header: str,
    metric_ver:  str,
    timeout:     int,
    logger:      logging.Logger,
) -> FujiResult:
    """
    Call the F-UJI REST API for one URL and return a FujiResult.
    """
    url = target.url
    endpoint = f"{fuji_url.rstrip('/')}{FUJI_ENDPOINT}"
    payload = {
        "object_identifier":      url,
        "test_debug":             False,
        "use_datacite":           True,
        "metadata_service_endpoint": "",
        "metadata_service_type":  "oai_pmh",
    }
    headers = {
        "Content-Type":  "application/json",
        "Authorization": auth_header,
    }

    result = FujiResult(url=url)

    # Respect per-domain rate limit before sending
    domain = _get_domain(url)
    await _throttle_domain(domain)

    try:
        async with session.post(
            endpoint,
            json=payload,
            headers=headers,
            timeout=aiohttp.ClientTimeout(total=timeout),
        ) as resp:
            result.http_status = resp.status
            if resp.status == 429:
                # Rate limited — wait for Retry-After header if present, else 60s
                retry_after = int(resp.headers.get("Retry-After", 60))
                logger.warning(
                    f"  WARN 429 rate-limited for {url[:55]} — "
                    f"waiting {retry_after}s then retrying once"
                )
                await asyncio.sleep(retry_after)
                # Single retry after the back-off
                async with session.post(
                    endpoint, json=payload, headers=headers,
                    timeout=aiohttp.ClientTimeout(total=timeout),
                ) as retry_resp:
                    result.http_status = retry_resp.status
                    if retry_resp.status != 200:
                        body = await retry_resp.text()
                        result.error_message = f"HTTP {retry_resp.status} (after retry): {body[:200]}"
                        return result
                    data = await retry_resp.json(content_type=None)
            elif resp.status != 200:
                body = await resp.text()
                result.error_message = f"HTTP {resp.status}: {body[:200]}"
                logger.warning(f"  WARN F-UJI {resp.status} for {url[:60]}")
                return result

            data = await resp.json(content_type=None)

    except asyncio.TimeoutError:
        result.http_status = 0
        result.error_message = f"Timeout after {timeout}s"
        logger.warning(f"  WARN timeout for {url[:60]}")
        return result

    except Exception as e:
        result.http_status = 0
        result.error_message = str(e)[:300]
        logger.warning(f"  WARN error for {url[:60]}: {e}")
        return result

    # ── Parse overall score ───────────────────────────────────────────────
    try:
        score = data.get("score", {})
        result.score_earned = score.get("earned", 0) or 0
        result.score_total  = score.get("total", 0) or 0

        summary = data.get("summary", {})
        pcts    = summary.get("score_percent", {})
        mats    = summary.get("maturity", {})

        result.score_pct_fair = float(pcts.get("FAIR", 0) or 0)
        result.score_pct_f    = float(pcts.get("F", 0) or 0)
        result.score_pct_a    = float(pcts.get("A", 0) or 0)
        result.score_pct_i    = float(pcts.get("I", 0) or 0)
        result.score_pct_r    = float(pcts.get("R", 0) or 0)

        result.maturity_fair  = int(mats.get("FAIR", 0) or 0)
        result.maturity_f     = int(mats.get("F", 0) or 0)
        result.maturity_a     = int(mats.get("A", 0) or 0)
        result.maturity_i     = int(mats.get("I", 0) or 0)
        result.maturity_r     = int(mats.get("R", 0) or 0)

    except Exception as e:
        result.error_message = f"Parse error (score): {e}"
        logger.debug(f"  DEBUG parse error for {url}: {e}")

    # ── Parse per-metric results ──────────────────────────────────────────
    try:
        for item in data.get("results", []):
            mid = item.get("metric_identifier", "")
            ms  = item.get("score", {}) or {}
            result.metrics.append({
                "metric_identifier": mid,
                "metric_name":       item.get("metric_name", ""),
                "principle":         _extract_principle(mid),
                "score_earned":      int(ms.get("earned", 0) or 0),
                "score_total":       int(ms.get("total",  0) or 0),
                "maturity":          int(item.get("maturity", 0) or 0),
                "test_status":       item.get("test_status", ""),
            })
    except Exception as e:
        logger.debug(f"  DEBUG parse error (metrics) for {url}: {e}")

    logger.debug(
        f"  OK  {url[:55]}  FAIR={result.score_pct_fair:.0f}%  "
        f"({result.score_earned}/{result.score_total})"
    )
    return result


# ---------------------------------------------------------------------------
# DB write
# ---------------------------------------------------------------------------

def save_result(
    con:        sqlite3.Connection,
    result:     FujiResult,
    target:     AssessmentTarget,
) -> None:
    """Insert one F-UJI result into the three tables."""
    ts = datetime.now(timezone.utc).isoformat()
    cur = con.cursor()

    cur.execute("""
        INSERT OR REPLACE INTO fuji_assessment
            (url, assessed_at, http_status, error_message,
             score_earned, score_total,
             score_pct_fair, score_pct_f, score_pct_a, score_pct_i, score_pct_r,
             maturity_fair, maturity_f, maturity_a, maturity_i, maturity_r)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, (
        result.url, ts, result.http_status, result.error_message or None,
        result.score_earned, result.score_total,
        result.score_pct_fair, result.score_pct_f,
        result.score_pct_a, result.score_pct_i, result.score_pct_r,
        result.maturity_fair, result.maturity_f,
        result.maturity_a, result.maturity_i, result.maturity_r,
    ))
    assessment_id = cur.lastrowid

    for m in result.metrics:
        cur.execute("""
            INSERT INTO fuji_metric
                (assessment_id, metric_identifier, metric_name, principle,
                 score_earned, score_total, maturity, test_status)
            VALUES (?,?,?,?,?,?,?,?)
        """, (
            assessment_id,
            m["metric_identifier"], m["metric_name"], m["principle"],
            m["score_earned"], m["score_total"], m["maturity"], m["test_status"],
        ))

    for aid in target.article_ids:
        cur.execute("""
            INSERT OR IGNORE INTO fuji_article_link (assessment_id, article_id)
            VALUES (?,?)
        """, (assessment_id, aid))

    con.commit()


# ---------------------------------------------------------------------------
# Async runner
# ---------------------------------------------------------------------------

async def run_assessments(
    targets:     list[AssessmentTarget],
    con:         sqlite3.Connection,
    fuji_url:    str,
    fuji_user:   str,
    fuji_pass:   str,
    metric_ver:  str,
    workers:     int,
    timeout:     int,
    dry_run:     bool,
    logger:      logging.Logger,
) -> dict:
    """
    Run F-UJI assessments asynchronously with a semaphore-controlled
    concurrency limit.  Returns a summary dict.
    """
    auth_header = _auth_header(fuji_user, fuji_pass)
    sem = asyncio.Semaphore(workers)
    total = len(targets)
    counts = {"ok": 0, "error": 0, "timeout": 0}
    t0 = time.perf_counter()

    if dry_run:
        logger.info(f"  DRY RUN — would assess {total:,} URLs. First 10:")
        for t in targets[:10]:
            logger.info(f"    {t.url[:80]}  [{t.repo_infra_tier}]  articles={len(t.article_ids)}")
        if total > 10:
            logger.info(f"    … and {total - 10:,} more")
        return counts

    async def _assess_one(i: int, target: AssessmentTarget) -> None:
        async with sem:
            result = await call_fuji(
                session, target, fuji_url, auth_header,
                metric_ver, timeout, logger
            )
            # Write synchronously (SQLite is not thread/async safe across tasks)
            save_result(con, result, target)

            if result.http_status == 200:
                counts["ok"] += 1
            elif result.error_message and "Timeout" in result.error_message:
                counts["timeout"] += 1
            else:
                counts["error"] += 1

            if (i % 50 == 0) or (i == total):
                elapsed = time.perf_counter() - t0
                rate = i / elapsed if elapsed > 0 else 0
                eta  = (total - i) / rate if rate > 0 else 0
                logger.info(
                    f"  Progress: {i:>6,}/{total:,}  "
                    f"ok={counts['ok']:,}  err={counts['error']:,}  "
                    f"to={counts['timeout']:,}  "
                    f"{rate:.1f}/s  ETA {eta/60:.1f}min"
                )

    connector = aiohttp.TCPConnector(limit=workers, ssl=False)
    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = [
            asyncio.create_task(_assess_one(i + 1, t))
            for i, t in enumerate(targets)
        ]
        await asyncio.gather(*tasks)

    return counts


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------

def print_report(con: sqlite3.Connection, logger: logging.Logger) -> None:
    logger.info("")
    logger.info("=" * 70)
    logger.info("F-UJI ASSESSMENT REPORT")
    logger.info("=" * 70)

    total = con.execute("SELECT COUNT(*) FROM fuji_assessment").fetchone()[0]
    ok    = con.execute(
        "SELECT COUNT(*) FROM fuji_assessment WHERE http_status=200"
    ).fetchone()[0]

    logger.info(f"  Total assessments run : {total:>8,}")
    logger.info(f"  Successful (HTTP 200) : {ok:>8,}  ({ok/total*100:.1f}%)" if total else "")

    # Overall FAIR score distribution
    logger.info("")
    logger.info("  Overall FAIR Score Distribution (successful assessments)")
    logger.info("  " + "─" * 55)
    rows = con.execute("""
        SELECT
            ROUND(score_pct_fair/10)*10 AS bucket,
            COUNT(*) AS n,
            AVG(score_pct_fair) AS avg_fair
        FROM fuji_assessment WHERE http_status=200
        GROUP BY bucket ORDER BY bucket
    """).fetchall()
    for bucket, n, avg in rows:
        bar = "█" * int(n / max(ok, 1) * 40)
        logger.info(f"  {int(bucket or 0):>3}–{int(bucket or 0)+9:<3}%  {n:>6,}  {bar}")

    # FAIR principle breakdown
    logger.info("")
    logger.info("  FAIR Principle Scores (mean % across all successful assessments)")
    logger.info("  " + "─" * 45)
    avgs = con.execute("""
        SELECT AVG(score_pct_f), AVG(score_pct_a),
               AVG(score_pct_i), AVG(score_pct_r), AVG(score_pct_fair)
        FROM fuji_assessment WHERE http_status=200
    """).fetchone()
    if avgs:
        for label, val in zip(["Findable (F)", "Accessible (A)",
                                "Interoperable (I)", "Reusable (R)", "FAIR overall"],
                               avgs):
            bar = "█" * int((val or 0) / 5)
            logger.info(f"  {label:<20}  {val or 0:>5.1f}%  {bar}")

    # Per-metric pass rate
    logger.info("")
    logger.info("  Per-Metric Pass Rate (across successful assessments)")
    logger.info("  " + "─" * 60)
    logger.info(f"  {'Metric':<18} {'Principle':<12} {'Pass%':>6}  {'Avg score':>10}")
    metric_rows = con.execute("""
        SELECT
            fm.metric_identifier,
            fm.principle,
            ROUND(100.0 * SUM(CASE WHEN fm.test_status='pass' THEN 1 ELSE 0 END) / COUNT(*), 1) AS pass_pct,
            AVG(CAST(fm.score_earned AS REAL) / NULLIF(fm.score_total, 0)) * 100 AS avg_score_pct
        FROM fuji_metric fm
        JOIN fuji_assessment fa ON fa.assessment_id = fm.assessment_id
        WHERE fa.http_status = 200
        GROUP BY fm.metric_identifier, fm.principle
        ORDER BY fm.principle, fm.metric_identifier
    """).fetchall()
    for mid, principle, pass_pct, avg_pct in metric_rows:
        bar = "█" * int((pass_pct or 0) / 5)
        logger.info(
            f"  {mid:<18} {principle or '?':<12} {pass_pct or 0:>5.1f}%"
            f"  {avg_pct or 0:>8.1f}%  {bar}"
        )

    # Year-over-year FAIR trends
    logger.info("")
    logger.info("  Year-over-Year FAIR Score Trends")
    logger.info("  " + "─" * 65)
    logger.info(f"  {'Year':<6} {'N':>6}  {'F%':>6}  {'A%':>6}  {'I%':>6}  {'R%':>6}  {'FAIR%':>7}")
    yr_rows = con.execute("""
        SELECT
            a.pub_year,
            COUNT(DISTINCT fa.assessment_id) AS n,
            AVG(fa.score_pct_f)    AS f_pct,
            AVG(fa.score_pct_a)    AS a_pct,
            AVG(fa.score_pct_i)    AS i_pct,
            AVG(fa.score_pct_r)    AS r_pct,
            AVG(fa.score_pct_fair) AS fair_pct
        FROM fuji_assessment fa
        JOIN fuji_article_link fal ON fal.assessment_id = fa.assessment_id
        JOIN article a ON a.article_id = fal.article_id
        WHERE fa.http_status = 200 AND a.pub_year IS NOT NULL
        GROUP BY a.pub_year ORDER BY a.pub_year
    """).fetchall()
    for yr, n, f, aa, i, r, fair in yr_rows:
        logger.info(
            f"  {yr:<6} {n:>6,}  {f or 0:>5.1f}%  {aa or 0:>5.1f}%  "
            f"{i or 0:>5.1f}%  {r or 0:>5.1f}%  {fair or 0:>6.1f}%"
        )

    # Top domains by mean FAIR score
    logger.info("")
    logger.info("  Top 20 Domains by Mean FAIR Score")
    logger.info("  " + "─" * 55)
    dom_rows = con.execute("""
        SELECT
            LOWER(SUBSTR(url, INSTR(url,'://')+3,
                  INSTR(SUBSTR(url, INSTR(url,'://')+3), '/') - 1)) AS domain,
            COUNT(*) AS n,
            AVG(score_pct_fair) AS avg_fair,
            AVG(score_pct_f) AS avg_f, AVG(score_pct_a) AS avg_a,
            AVG(score_pct_i) AS avg_i, AVG(score_pct_r) AS avg_r
        FROM fuji_assessment WHERE http_status=200
        GROUP BY domain HAVING n >= 3
        ORDER BY avg_fair DESC LIMIT 20
    """).fetchall()
    logger.info(f"  {'Domain':<35} {'N':>5}  {'FAIR%':>6}  {'F%':>5}  {'A%':>5}  {'I%':>5}  {'R%':>5}")
    for domain, n, fair, f, aa, i, r in dom_rows:
        logger.info(
            f"  {(domain or 'unknown')[:35]:<35} {n:>5,}  "
            f"{fair or 0:>5.1f}%  {f or 0:>4.1f}%  {aa or 0:>4.1f}%  "
            f"{i or 0:>4.1f}%  {r or 0:>4.1f}%"
        )

    logger.info("=" * 70)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="F-UJI FAIR Assessment — FAIR longitudinal study Stage 5.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Prerequisites
-------------
  F-UJI must be running locally (or accessible at --fuji-url).
  Fastest setup:
    docker run -d -p 1071:1071 ghcr.io/pangaea-data-publisher/fuji

  Then verify it's up:
    curl http://localhost:1071/fuji/api/v1/metrics

Examples
--------
  # Full run (all gold/silver URLs, 4 workers)
  python assess_fuji.py --db pmc_fair.db

  # Stratified random sample: 50 URLs per year
  python assess_fuji.py --db pmc_fair.db --sample 50

  # Only Zenodo and Dryad
  python assess_fuji.py --db pmc_fair.db --domains zenodo.org,datadryad.org

  # Dry run — see what would be assessed without calling F-UJI
  python assess_fuji.py --db pmc_fair.db --dry-run

  # Report on a completed run
  python assess_fuji.py --db pmc_fair.db --report --no-assess
        """,
    )
    parser.add_argument("--db",          default="pmc_fair.db",
        help="SQLite database path (default: pmc_fair.db).")
    parser.add_argument("--fuji-url",    default="http://localhost:1071",
        help="Base URL of the F-UJI service (default: http://localhost:1071).")
    parser.add_argument("--fuji-user",   default=DEFAULT_USER,
        help=f"F-UJI basic-auth username (default: {DEFAULT_USER}).")
    parser.add_argument("--fuji-pass",   default=DEFAULT_PASS,
        help=f"F-UJI basic-auth password (default: {DEFAULT_PASS}).")
    # metric_version is configured server-side in F-UJI, not sent in the request body
    parser.add_argument("--workers",     type=int, default=DEFAULT_WORKERS,
        help=f"Number of concurrent F-UJI calls (default: {DEFAULT_WORKERS}). "
             "Keep ≤5 for a local instance.")
    parser.add_argument("--timeout",     type=int, default=DEFAULT_TIMEOUT,
        help=f"Seconds to wait per F-UJI call (default: {DEFAULT_TIMEOUT}).")
    parser.add_argument("--sample",      type=int, default=0,
        help="Stratified random sample: N URLs per publication year (0=all).")
    parser.add_argument("--domains",     default="",
        help="Comma-separated domain whitelist e.g. zenodo.org,figshare.com "
             "(default: all gold/silver repos).")
    parser.add_argument("--dry-run",     action="store_true",
        help="Show what would be assessed without calling F-UJI.")
    parser.add_argument("--no-assess",   action="store_true",
        help="Skip assessment, only run --report.")
    parser.add_argument("--reset",       action="store_true",
        help="Drop all F-UJI tables and start fresh.")
    parser.add_argument("--report",      action="store_true",
        help="Print FAIR assessment report after (or instead of) assessment.")
    parser.add_argument("--seed",        type=int, default=42,
        help="Random seed for --sample reproducibility (default: 42).")
    args = parser.parse_args()

    db_path  = Path(args.db)
    log_path = db_path.with_suffix(".fuji.log")

    if not db_path.exists():
        sys.exit(f"Database not found: {db_path}")

    logger = setup_logging(log_path)
    t0     = time.perf_counter()

    logger.info("=" * 65)
    logger.info("F-UJI FAIR Assessment  —  FAIR Longitudinal Study (Stage 5)")
    logger.info("=" * 65)
    logger.info(f"  Database    : {db_path}")
    logger.info(f"  F-UJI URL   : {args.fuji_url}")
    logger.info(f"  Workers     : {args.workers}")
    logger.info(f"  Timeout     : {args.timeout}s")
    logger.info(f"  Sample/year : {args.sample or 'all'}")
    logger.info(f"  Domains     : {args.domains or 'all gold/silver'}")
    logger.info(f"  Log         : {log_path}")

    random.seed(args.seed)
    con = sqlite3.connect(db_path)
    con.execute("PRAGMA foreign_keys = ON")
    con.execute("PRAGMA journal_mode = WAL")   # safer for long async writes

    if args.reset:
        logger.info("  --reset: dropping F-UJI tables …")
        for tbl in RESET_TABLES:
            con.execute(f"DROP TABLE IF EXISTS {tbl}")
        con.commit()

    ensure_schema(con, logger)

    if not args.no_assess:
        already = get_already_assessed(con)
        logger.info(f"  Already assessed URLs: {len(already):,}")

        domains_filter = (
            {d.strip().lower() for d in args.domains.split(",") if d.strip()}
            if args.domains else None
        )

        targets = get_targets(con, already, domains_filter, logger)

        if args.sample > 0:
            targets = apply_sample(targets, args.sample, logger)

        logger.info(f"  URLs to assess this run: {len(targets):,}")

        if targets:
            est_min = len(targets) * 15 / 60 / args.workers
            logger.info(
                f"  Estimated time: ~{est_min:.0f} min "
                f"(assuming ~15s/assessment, {args.workers} workers)"
            )
            logger.info("")

            counts = asyncio.run(run_assessments(
                targets=targets,
                con=con,
                fuji_url=args.fuji_url,
                fuji_user=args.fuji_user,
                fuji_pass=args.fuji_pass,
                metric_ver="",
                workers=args.workers,
                timeout=args.timeout,
                dry_run=args.dry_run,
                logger=logger,
            ))

            elapsed  = time.perf_counter() - t0
            peak_rss = _peak_rss_mb()
            logger.info("")
            logger.info(
                f"  Assessment complete: {elapsed/60:.1f} min  |  {peak_rss:.0f} MB RSS"
            )
            logger.info(
                f"  Results — ok: {counts['ok']:,}  "
                f"error: {counts['error']:,}  timeout: {counts['timeout']:,}"
            )
        else:
            logger.info("  Nothing new to assess.")

    if args.report:
        total_assessed = con.execute(
            "SELECT COUNT(*) FROM fuji_assessment"
        ).fetchone()[0]
        if total_assessed == 0:
            logger.info("  No assessment data yet — run without --no-assess first.")
        else:
            print_report(con, logger)

    con.close()
    logger.info(f"\nDone. Log: {log_path}")


if __name__ == "__main__":
    main()
