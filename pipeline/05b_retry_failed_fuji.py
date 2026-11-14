"""
05b_retry_failed_fuji.py
------------------------
Retry failed F-UJI assessments from Stage 5.

Reads all rows in fuji_assessment where http_status != 200 (i.e. HTTP 500
errors and connection failures), re-runs the F-UJI API call for each URL,
and updates the database in place.

Two failure modes from the original run:
  - http_status=0  : F-UJI was not reachable (connection refused / server down)
  - http_status=500: F-UJI returned an internal server error for that URL

Both are retried.  Rows whose retry still fails are left with their new
(updated) error state so subsequent retries can target them again.

Usage
-----
  python 05b_retry_failed_fuji.py --db /path/to/pmc_fair.db

  # Only retry connection errors (status 0)
  python 05b_retry_failed_fuji.py --db pmc_fair.db --status 0

  # Only retry HTTP 500 errors
  python 05b_retry_failed_fuji.py --db pmc_fair.db --status 500

  # Custom F-UJI URL / credentials
  python 05b_retry_failed_fuji.py --db pmc_fair.db \\
      --fuji-url http://localhost:1071 \\
      --fuji-user marvel --fuji-pass wonderwoman

  # Dry run — list what would be retried
  python 05b_retry_failed_fuji.py --db pmc_fair.db --dry-run
"""

import argparse
import asyncio
import base64
import logging
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
        "aiohttp is required.\n"
        "Install with:  pip install aiohttp"
    )

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

LOG_FMT  = "%(asctime)s  %(levelname)-7s  %(message)s"
LOG_DATE = "%Y-%m-%d %H:%M:%S"


def setup_logging(log_path: Path) -> logging.Logger:
    logger = logging.getLogger("retry_fuji")
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
# Constants (same as Stage 5)
# ---------------------------------------------------------------------------

FUJI_ENDPOINT   = "/fuji/api/v1/evaluate"
DEFAULT_USER    = "marvel"
DEFAULT_PASS    = "wonderwoman"
DEFAULT_WORKERS = 4
DEFAULT_TIMEOUT = 120

DOMAIN_RATE_LIMITS: dict[str, float] = {
    "zenodo.org":            2.0,
    "figshare.com":          2.0,
    "datadryad.org":         1.0,
    "osf.io":                2.0,
    "github.com":            1.0,
    "dataverse.harvard.edu": 1.0,
    "data.mendeley.com":     1.0,
    "pangaea.de":            1.0,
}

_domain_last_call: dict[str, float] = {}
_domain_locks:     dict[str, "asyncio.Lock"] = {}


def _get_domain(url: str) -> str:
    try:
        return urlparse(url).netloc.lower().lstrip("www.")
    except Exception:
        return ""


async def _throttle_domain(domain: str) -> None:
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
class RetryTarget:
    assessment_id: int
    url:           str
    article_ids:   list[int] = field(default_factory=list)
    old_status:    int       = 0
    old_error:     str       = ""


@dataclass
class FujiResult:
    url:            str
    http_status:    int   = 0
    error_message:  str   = ""
    score_earned:   int   = 0
    score_total:    int   = 0
    score_pct_fair: float = 0.0
    score_pct_f:    float = 0.0
    score_pct_a:    float = 0.0
    score_pct_i:    float = 0.0
    score_pct_r:    float = 0.0
    maturity_fair:  int   = 0
    maturity_f:     int   = 0
    maturity_a:     int   = 0
    maturity_i:     int   = 0
    maturity_r:     int   = 0
    metrics:        list  = field(default_factory=list)


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

def get_retry_targets(
    con: sqlite3.Connection,
    status_filter: list[int] | None,
    logger: logging.Logger,
) -> list[RetryTarget]:
    """
    Return all fuji_assessment rows that did not succeed (http_status != 200),
    along with their linked article_ids.
    """
    if status_filter:
        placeholders = ",".join("?" * len(status_filter))
        rows = con.execute(
            f"""
            SELECT assessment_id, url, http_status, error_message
            FROM fuji_assessment
            WHERE http_status IN ({placeholders})
            ORDER BY assessment_id
            """,
            status_filter,
        ).fetchall()
    else:
        rows = con.execute(
            """
            SELECT assessment_id, url, http_status, error_message
            FROM fuji_assessment
            WHERE http_status != 200
            ORDER BY assessment_id
            """
        ).fetchall()

    if not rows:
        return []

    targets: dict[int, RetryTarget] = {
        r[0]: RetryTarget(
            assessment_id=r[0],
            url=r[1],
            old_status=r[2],
            old_error=r[3] or "",
        )
        for r in rows
    }

    # Fetch article links in one query
    ids = list(targets.keys())
    chunk = 900   # SQLite SQLITE_MAX_VARIABLE_NUMBER is 999 by default
    for start in range(0, len(ids), chunk):
        batch = ids[start : start + chunk]
        ph    = ",".join("?" * len(batch))
        links = con.execute(
            f"SELECT assessment_id, article_id FROM fuji_article_link WHERE assessment_id IN ({ph})",
            batch,
        ).fetchall()
        for aid, article_id in links:
            if aid in targets:
                targets[aid].article_ids.append(article_id)

    logger.info(
        f"  Found {len(targets):,} failed assessments to retry "
        f"(status filter: {status_filter or 'all non-200'})"
    )
    return list(targets.values())


def save_result(
    con:           sqlite3.Connection,
    result:        FujiResult,
    assessment_id: int,
    article_ids:   list[int],
) -> None:
    """
    Update the existing fuji_assessment row in-place (by assessment_id).
    Replace fuji_metric rows for this assessment, keep article links.
    """
    ts = datetime.now(timezone.utc).isoformat()
    cur = con.cursor()

    cur.execute(
        """
        UPDATE fuji_assessment SET
            assessed_at    = ?,
            http_status    = ?,
            error_message  = ?,
            score_earned   = ?,
            score_total    = ?,
            score_pct_fair = ?,
            score_pct_f    = ?,
            score_pct_a    = ?,
            score_pct_i    = ?,
            score_pct_r    = ?,
            maturity_fair  = ?,
            maturity_f     = ?,
            maturity_a     = ?,
            maturity_i     = ?,
            maturity_r     = ?
        WHERE assessment_id = ?
        """,
        (
            ts, result.http_status, result.error_message or None,
            result.score_earned, result.score_total,
            result.score_pct_fair, result.score_pct_f,
            result.score_pct_a, result.score_pct_i, result.score_pct_r,
            result.maturity_fair, result.maturity_f,
            result.maturity_a, result.maturity_i, result.maturity_r,
            assessment_id,
        ),
    )

    # Replace metric rows for this assessment
    cur.execute(
        "DELETE FROM fuji_metric WHERE assessment_id = ?", (assessment_id,)
    )
    for m in result.metrics:
        cur.execute(
            """
            INSERT INTO fuji_metric
                (assessment_id, metric_identifier, metric_name, principle,
                 score_earned, score_total, maturity, test_status)
            VALUES (?,?,?,?,?,?,?,?)
            """,
            (
                assessment_id,
                m["metric_identifier"], m["metric_name"], m["principle"],
                m["score_earned"], m["score_total"], m["maturity"], m["test_status"],
            ),
        )

    # Ensure article links exist (idempotent)
    for aid in article_ids:
        cur.execute(
            "INSERT OR IGNORE INTO fuji_article_link (assessment_id, article_id) VALUES (?,?)",
            (assessment_id, aid),
        )

    con.commit()


# ---------------------------------------------------------------------------
# F-UJI API call (identical logic to Stage 5)
# ---------------------------------------------------------------------------

def _auth_header(user: str, pwd: str) -> str:
    token = base64.b64encode(f"{user}:{pwd}".encode()).decode()
    return f"Basic {token}"


def _extract_principle(metric_id: str) -> str:
    m = re.match(r"FsF-([FAIR])", metric_id, re.IGNORECASE)
    return m.group(1).upper() if m else "?"


async def call_fuji(
    session:     "aiohttp.ClientSession",
    url:         str,
    fuji_url:    str,
    auth_header: str,
    timeout:     int,
    logger:      logging.Logger,
) -> FujiResult:
    endpoint = f"{fuji_url.rstrip('/')}{FUJI_ENDPOINT}"
    payload  = {
        "object_identifier":         url,
        "test_debug":                False,
        "use_datacite":              True,
        "metadata_service_endpoint": "",
        "metadata_service_type":     "oai_pmh",
    }
    headers = {
        "Content-Type":  "application/json",
        "Authorization": auth_header,
    }

    result = FujiResult(url=url)
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
                retry_after = int(resp.headers.get("Retry-After", 60))
                logger.warning(
                    f"  WARN 429 rate-limited for {url[:55]} — "
                    f"waiting {retry_after}s then retrying once"
                )
                await asyncio.sleep(retry_after)
                async with session.post(
                    endpoint, json=payload, headers=headers,
                    timeout=aiohttp.ClientTimeout(total=timeout),
                ) as retry_resp:
                    result.http_status = retry_resp.status
                    if retry_resp.status != 200:
                        body = await retry_resp.text()
                        result.error_message = (
                            f"HTTP {retry_resp.status} (after retry): {body[:200]}"
                        )
                        return result
                    data = await retry_resp.json(content_type=None)
            elif resp.status != 200:
                body = await resp.text()
                result.error_message = f"HTTP {resp.status}: {body[:200]}"
                logger.warning(f"  WARN F-UJI {resp.status} for {url[:60]}")
                return result
            else:
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

    # Parse overall score
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
# Async runner
# ---------------------------------------------------------------------------

async def run_retries(
    targets:     list[RetryTarget],
    con:         sqlite3.Connection,
    fuji_url:    str,
    fuji_user:   str,
    fuji_pass:   str,
    workers:     int,
    timeout:     int,
    dry_run:     bool,
    logger:      logging.Logger,
) -> dict:
    auth_header = _auth_header(fuji_user, fuji_pass)
    sem         = asyncio.Semaphore(workers)
    total       = len(targets)
    counts      = {"ok": 0, "error": 0, "timeout": 0}
    t0          = time.perf_counter()

    if dry_run:
        logger.info(f"  DRY RUN — would retry {total:,} URLs. First 10:")
        for t in targets[:10]:
            logger.info(
                f"    [{t.old_status}] {t.url[:75]}  "
                f"(err: {t.old_error[:50]})"
            )
        if total > 10:
            logger.info(f"    … and {total - 10:,} more")
        return counts

    async def _retry_one(i: int, target: RetryTarget) -> None:
        async with sem:
            result = await call_fuji(
                session, target.url, fuji_url, auth_header, timeout, logger
            )
            save_result(con, result, target.assessment_id, target.article_ids)

            if result.http_status == 200:
                counts["ok"] += 1
            elif result.error_message and "Timeout" in result.error_message:
                counts["timeout"] += 1
            else:
                counts["error"] += 1

            if (i % 50 == 0) or (i == total):
                elapsed = time.perf_counter() - t0
                rate    = i / elapsed if elapsed > 0 else 0
                eta     = (total - i) / rate if rate > 0 else 0
                logger.info(
                    f"  Progress: {i:>6,}/{total:,}  "
                    f"ok={counts['ok']:,}  err={counts['error']:,}  "
                    f"to={counts['timeout']:,}  "
                    f"{rate:.1f}/s  ETA {eta/60:.1f}min"
                )

    connector = aiohttp.TCPConnector(limit=workers, ssl=False)
    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = [
            asyncio.create_task(_retry_one(i + 1, t))
            for i, t in enumerate(targets)
        ]
        await asyncio.gather(*tasks)

    return counts


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Retry failed F-UJI assessments and update pmc_fair.db.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--db", default="pmc_fair.db",
        help="Path to pmc_fair.db (default: pmc_fair.db).",
    )
    parser.add_argument(
        "--fuji-url", default="http://localhost:1071",
        help="F-UJI base URL (default: http://localhost:1071).",
    )
    parser.add_argument(
        "--fuji-user", default=DEFAULT_USER,
        help=f"F-UJI basic-auth username (default: {DEFAULT_USER}).",
    )
    parser.add_argument(
        "--fuji-pass", default=DEFAULT_PASS,
        help=f"F-UJI basic-auth password (default: {DEFAULT_PASS}).",
    )
    parser.add_argument(
        "--workers", type=int, default=DEFAULT_WORKERS,
        help=f"Concurrent F-UJI calls (default: {DEFAULT_WORKERS}).",
    )
    parser.add_argument(
        "--timeout", type=int, default=DEFAULT_TIMEOUT,
        help=f"Seconds per F-UJI call (default: {DEFAULT_TIMEOUT}).",
    )
    parser.add_argument(
        "--status", type=int, action="append", dest="statuses",
        metavar="CODE",
        help=(
            "HTTP status code(s) to retry (e.g. --status 0 --status 500). "
            "Default: retry all non-200 rows."
        ),
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Show what would be retried without calling F-UJI.",
    )
    args = parser.parse_args()

    db_path  = Path(args.db)
    log_path = db_path.with_suffix(".fuji_retry.log")

    if not db_path.exists():
        sys.exit(f"Database not found: {db_path}")

    logger = setup_logging(log_path)
    t0     = time.perf_counter()

    logger.info("=" * 65)
    logger.info("F-UJI Retry  —  FAIR Longitudinal Study (Stage 5b)")
    logger.info("=" * 65)
    logger.info(f"  Database    : {db_path}")
    logger.info(f"  F-UJI URL   : {args.fuji_url}")
    logger.info(f"  Workers     : {args.workers}")
    logger.info(f"  Timeout     : {args.timeout}s")
    logger.info(f"  Status filter: {args.statuses or 'all non-200'}")
    logger.info(f"  Log         : {log_path}")

    con = sqlite3.connect(db_path)
    con.execute("PRAGMA foreign_keys = ON")
    con.execute("PRAGMA journal_mode = WAL")

    targets = get_retry_targets(con, args.statuses, logger)

    if not targets:
        logger.info("  No failed assessments found — nothing to retry.")
        con.close()
        return

    logger.info(f"  URLs to retry: {len(targets):,}")
    est_min = len(targets) * 15 / 60 / args.workers
    logger.info(
        f"  Estimated time: ~{est_min:.0f} min "
        f"(assuming ~15s/assessment, {args.workers} workers)"
    )
    logger.info("")

    counts = asyncio.run(run_retries(
        targets=targets,
        con=con,
        fuji_url=args.fuji_url,
        fuji_user=args.fuji_user,
        fuji_pass=args.fuji_pass,
        workers=args.workers,
        timeout=args.timeout,
        dry_run=args.dry_run,
        logger=logger,
    ))

    elapsed  = time.perf_counter() - t0
    peak_rss = _peak_rss_mb()
    logger.info("")
    logger.info(f"  Retry complete: {elapsed/60:.1f} min  |  {peak_rss:.0f} MB RSS")
    logger.info(
        f"  Results — ok: {counts['ok']:,}  "
        f"error: {counts['error']:,}  timeout: {counts['timeout']:,}"
    )

    # Quick summary of remaining failures
    remaining = con.execute(
        "SELECT COUNT(*) FROM fuji_assessment WHERE http_status != 200"
    ).fetchone()[0]
    logger.info(f"  Remaining non-200 rows in DB: {remaining:,}")

    con.close()
    logger.info(f"\nDone. Log: {log_path}")


if __name__ == "__main__":
    main()
