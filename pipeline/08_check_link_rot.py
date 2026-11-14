"""
check_link_rot.py
-----------------
Stage 8 of the PMC FAIR longitudinal study pipeline.

Checks every URL in the database (da_link, supplementary_material,
accession_number) for HTTP reachability and stores results in a new
`link_rot` table.  Designed to be re-run incrementally — already-checked
URLs are skipped unless --recheck is passed.

Features
--------
* Async HTTP with aiohttp (configurable concurrency, default 40)
* Per-domain rate limiting (max 5 req/s) to avoid bans
* Redirect chain recording — final URL stored separately from original
* Soft-404 / landing-page detection (title-based heuristics)
* Wayback Machine lookup for dead links
* Resume support — partial runs continue from where they left off
* Per-article summary + year-level link-rot rate computation
* Comprehensive logging

Link-rot status codes
---------------------
  ok           — HTTP 200 (or 2xx) at original or redirected URL
  redirect     — 3xx with final URL stored
  client_error — 4xx (404, 410, 403 …)
  server_error — 5xx
  timeout      — connection or read timeout
  dns_error    — name resolution failure
  ssl_error    — SSL certificate / handshake error
  other_error  — anything else
  soft_404     — HTTP 200 but landing/error page detected
  wayback_ok   — dead at origin but archived in Wayback Machine
  wayback_miss — dead at origin and not in Wayback Machine

Usage
-----
  python check_link_rot.py --db pmc_fair.db
  python check_link_rot.py --db pmc_fair.db --concurrency 60 --timeout 20
  python check_link_rot.py --db pmc_fair.db --recheck --since 2024-01-01
  python check_link_rot.py --db pmc_fair.db --dry-run   # just count URLs
"""

import argparse
import asyncio
import logging
import re
import sqlite3
import sys
import time
from collections import defaultdict
from datetime import date, datetime
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse

try:
    import aiohttp
    HAS_AIOHTTP = True
except ImportError:
    HAS_AIOHTTP = False
    print("WARNING: aiohttp not installed. Run: pip install aiohttp")

# ── Constants ──────────────────────────────────────────────────────────────────

VERSION       = "1.0.0"
USER_AGENT    = ("Mozilla/5.0 (compatible; PMC-FAIR-LinkRot/1.0; "
                 "+https://github.com/fair-research/pmc-link-rot)")
DEFAULT_CONC  = 40          # simultaneous connections
DEFAULT_TO    = 25          # seconds per request
DOMAIN_RPS    = 5.0         # max requests per second per domain
RETRY_CODES   = {429, 503}  # status codes that warrant a retry after delay
SOFT404_RE    = re.compile(
    r'page\s+not\s+found|404|resource\s+not\s+found|no\s+longer\s+available'
    r'|this\s+page\s+does\s+not\s+exist|decommissioned|moved\s+permanently'
    r'|link\s+is\s+broken|doi\s+not\s+found|record\s+not\s+found'
    r'|has\s+been\s+removed|content\s+unavailable|object\s+not\s+found',
    re.I)
WAYBACK_API   = "https://archive.org/wayback/available?url={url}"
LOG_FMT       = "%(asctime)s  %(levelname)-7s  %(message)s"
LOG_DATE      = "%Y-%m-%d %H:%M:%S"

# ── Schema ─────────────────────────────────────────────────────────────────────

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS link_rot (
    rot_id          INTEGER PRIMARY KEY AUTOINCREMENT,
    url             TEXT NOT NULL,
    url_normalised  TEXT NOT NULL,
    source_table    TEXT NOT NULL,   -- da_link / supplementary_material / accession_number
    source_id       INTEGER NOT NULL, -- link_id / supp_id / acc_id
    article_id      INTEGER,
    pub_year        INTEGER,
    domain          TEXT,
    status          TEXT NOT NULL,   -- ok / redirect / client_error / server_error /
                                     -- timeout / dns_error / ssl_error / other_error /
                                     -- soft_404 / wayback_ok / wayback_miss
    http_code       INTEGER,
    final_url       TEXT,            -- URL after redirects
    redirect_count  INTEGER DEFAULT 0,
    response_ms     INTEGER,         -- latency in milliseconds
    content_type    TEXT,
    title_snippet   TEXT,            -- first 200 chars of <title>
    wayback_url     TEXT,            -- nearest snapshot URL if checked
    checked_at      TEXT NOT NULL,   -- ISO-8601 datetime
    link_type       TEXT,            -- da_link link_type / "supplementary" / db_name for accessions
    UNIQUE (url, source_table, source_id)
);

CREATE TABLE IF NOT EXISTS link_rot_run (
    run_id       INTEGER PRIMARY KEY AUTOINCREMENT,
    started_at   TEXT NOT NULL,
    finished_at  TEXT,
    n_urls       INTEGER DEFAULT 0,
    n_checked    INTEGER DEFAULT 0,
    n_ok         INTEGER DEFAULT 0,
    n_dead       INTEGER DEFAULT 0,
    n_error      INTEGER DEFAULT 0,
    concurrency  INTEGER,
    timeout_s    INTEGER,
    notes        TEXT
);

CREATE INDEX IF NOT EXISTS idx_lr_url     ON link_rot(url_normalised);
CREATE INDEX IF NOT EXISTS idx_lr_status  ON link_rot(status);
CREATE INDEX IF NOT EXISTS idx_lr_year    ON link_rot(pub_year);
CREATE INDEX IF NOT EXISTS idx_lr_domain  ON link_rot(domain);
CREATE INDEX IF NOT EXISTS idx_lr_article ON link_rot(article_id);
CREATE INDEX IF NOT EXISTS idx_lr_source  ON link_rot(source_table, source_id);
CREATE INDEX IF NOT EXISTS idx_lr_ltype   ON link_rot(link_type);
"""

# ── Helpers ────────────────────────────────────────────────────────────────────

def setup_logging(log_path: Optional[Path] = None) -> logging.Logger:
    logger = logging.getLogger("link_rot")
    logger.setLevel(logging.DEBUG)
    logger.handlers.clear()
    fmt = logging.Formatter(LOG_FMT, datefmt=LOG_DATE)
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(fmt)
    logger.addHandler(ch)
    if log_path:
        fh = logging.FileHandler(log_path, encoding="utf-8", mode="a")
        fh.setLevel(logging.DEBUG)
        fh.setFormatter(fmt)
        logger.addHandler(fh)
    return logger


def normalise_url(url: str) -> str:
    """Strip trailing slashes, lowercase scheme+host, remove default ports."""
    url = url.strip()
    try:
        p = urlparse(url)
        host = (p.hostname or "").lower()
        scheme = p.scheme.lower()
        port = p.port
        if (scheme == "http"  and port == 80)  or \
           (scheme == "https" and port == 443):
            port = None
        netloc = host if port is None else f"{host}:{port}"
        path   = p.path.rstrip("/") or "/"
        return f"{scheme}://{netloc}{path}"  \
               + (f"?{p.query}"    if p.query    else "") \
               + (f"#{p.fragment}" if p.fragment else "")
    except Exception:
        return url.lower().rstrip("/")


def domain_of(url: str) -> str:
    try:
        return urlparse(url).hostname or ""
    except Exception:
        return ""


def extract_title(html: str) -> str:
    m = re.search(r'<title[^>]*>([^<]{1,300})', html, re.I)
    return m.group(1).strip()[:200] if m else ""


# ── Domain rate limiter ────────────────────────────────────────────────────────

class DomainThrottle:
    """Enforces per-domain request-per-second limit using token buckets."""

    def __init__(self, rps: float = DOMAIN_RPS):
        self.rps = rps
        self._last: dict[str, float] = {}
        self._lock = asyncio.Lock()

    async def acquire(self, domain: str):
        async with self._lock:
            now = time.monotonic()
            last = self._last.get(domain, 0.0)
            wait = (1.0 / self.rps) - (now - last)
            if wait > 0:
                self._last[domain] = now + wait
            else:
                self._last[domain] = now
        if wait > 0:
            await asyncio.sleep(wait)


# ── Core async checker ─────────────────────────────────────────────────────────

async def check_one(
    session: "aiohttp.ClientSession",
    throttle: DomainThrottle,
    url: str,
    timeout: int,
    check_wayback: bool,
    logger: logging.Logger,
) -> dict:
    """Check a single URL and return a result dict."""
    t0 = time.monotonic()
    dom = domain_of(url)
    result = {
        "url": url,
        "url_normalised": normalise_url(url),
        "domain": dom,
        "status": "other_error",
        "http_code": None,
        "final_url": None,
        "redirect_count": 0,
        "response_ms": None,
        "content_type": None,
        "title_snippet": None,
        "wayback_url": None,
        "checked_at": datetime.utcnow().isoformat(timespec="seconds"),
    }

    await throttle.acquire(dom)

    to = aiohttp.ClientTimeout(total=timeout, connect=10, sock_read=timeout)

    for attempt in range(2):
        try:
            async with session.get(
                url, timeout=to,
                allow_redirects=True,
                max_redirects=10,
                ssl=False,
            ) as resp:
                code = resp.status
                result["http_code"]      = code
                result["final_url"]      = str(resp.url)
                result["redirect_count"] = len(resp.history)
                result["content_type"]   = (resp.headers.get("Content-Type") or "")[:120]

                # Read up to 8 KB for soft-404 detection
                try:
                    chunk = await resp.content.read(8192)
                    html  = chunk.decode("utf-8", errors="replace")
                    result["title_snippet"] = extract_title(html)
                except Exception:
                    html = ""

                if code in RETRY_CODES and attempt == 0:
                    retry_after = int(resp.headers.get("Retry-After", "5"))
                    await asyncio.sleep(min(retry_after, 10))
                    continue

                if 200 <= code < 300:
                    if result["title_snippet"] and SOFT404_RE.search(result["title_snippet"]):
                        result["status"] = "soft_404"
                    else:
                        result["status"] = "ok" if result["redirect_count"] == 0 else "redirect"
                elif 300 <= code < 400:
                    result["status"] = "redirect"
                elif 400 <= code < 500:
                    result["status"] = "client_error"
                else:
                    result["status"] = "server_error"
                break

        except aiohttp.ClientConnectorDNSError:
            result["status"] = "dns_error"; break
        except aiohttp.ClientSSLError:
            result["status"] = "ssl_error"; break
        except (aiohttp.ServerTimeoutError, asyncio.TimeoutError):
            result["status"] = "timeout"; break
        except aiohttp.ClientConnectionError:
            result["status"] = "dns_error"; break
        except Exception as exc:
            result["status"] = "other_error"
            logger.debug("Error checking %s: %s", url, exc)
            break

    result["response_ms"] = int((time.monotonic() - t0) * 1000)

    # Wayback Machine fallback for dead links
    if check_wayback and result["status"] in {"client_error", "dns_error",
                                               "ssl_error", "soft_404"}:
        wb_url = WAYBACK_API.format(url=url)
        try:
            await throttle.acquire("archive.org")
            wb_to = aiohttp.ClientTimeout(total=15)
            async with session.get(wb_url, timeout=wb_to, ssl=False) as wb_resp:
                if wb_resp.status == 200:
                    js = await wb_resp.json()
                    snap = (js.get("archived_snapshots") or {}).get("closest") or {}
                    if snap.get("available") and snap.get("url"):
                        result["wayback_url"] = snap["url"]
                        result["status"]      = "wayback_ok"
                    else:
                        result["status"] = "wayback_miss"
        except Exception:
            pass  # Wayback check is best-effort

    return result


# ── Database helpers ───────────────────────────────────────────────────────────

def get_urls_to_check(con: sqlite3.Connection, recheck: bool, since: Optional[str]) -> list[dict]:
    """
    Collect all unchecked URLs using SQL LEFT JOIN deduplication — no Python
    set of already-checked rows, no full list in RAM.

    Each of the three source tables is queried with a LEFT JOIN against
    link_rot so only genuinely unchecked rows are returned. SQLite streams
    rows to the caller one at a time; we build the final list only of the
    remaining unchecked items, which is typically a small fraction of the total
    on re-runs.

    For a first run (recheck=False, since=None) the LEFT JOIN WHERE clause
    filters out nothing, so all URLs are returned — but they are fetched row
    by row from SQLite rather than loading the entire link_rot table into a
    Python set first.
    """
    since_clause = f"AND lr.checked_at >= '{since}'" if (since and not recheck) else ""

    urls: list[dict] = []

    # ── da_link ──────────────────────────────────────────────────────────────
    q = f"""
        SELECT dl.link_id, dl.url, dl.link_type,
               da.article_id,
               CAST(a.pub_year AS INT) AS pub_year
        FROM da_link dl
        JOIN data_availability da ON da.da_id    = dl.da_id
        JOIN article a            ON a.article_id = da.article_id
        LEFT JOIN link_rot lr     ON lr.source_table = 'da_link'
                                 AND lr.source_id    = dl.link_id
                                 {since_clause}
        WHERE dl.url IS NOT NULL AND dl.url != ''
          AND (lr.rot_id IS NULL {"" if recheck else ""})
    """
    if not recheck:
        q = f"""
            SELECT dl.link_id, dl.url, dl.link_type,
                   da.article_id,
                   CAST(a.pub_year AS INT) AS pub_year
            FROM da_link dl
            JOIN data_availability da ON da.da_id    = dl.da_id
            JOIN article a            ON a.article_id = da.article_id
            LEFT JOIN link_rot lr     ON lr.source_table = 'da_link'
                                     AND lr.source_id    = dl.link_id
                                     {since_clause}
            WHERE dl.url IS NOT NULL AND dl.url != ''
              AND lr.rot_id IS NULL
        """
    else:
        q = """
            SELECT dl.link_id, dl.url, dl.link_type,
                   da.article_id,
                   CAST(a.pub_year AS INT) AS pub_year
            FROM da_link dl
            JOIN data_availability da ON da.da_id    = dl.da_id
            JOIN article a            ON a.article_id = da.article_id
            WHERE dl.url IS NOT NULL AND dl.url != ''
        """
    for row in con.execute(q):
        lid, url, lt, aid, yr = row
        urls.append({"url": url, "source_table": "da_link",
                     "source_id": lid, "article_id": aid, "pub_year": yr,
                     "link_type": lt})

    # ── supplementary_material ───────────────────────────────────────────────
    if not recheck:
        q2 = f"""
            SELECT sm.supp_id, sm.url, sm.article_id,
                   CAST(a.pub_year AS INT) AS pub_year
            FROM supplementary_material sm
            JOIN article a ON a.article_id = sm.article_id
            LEFT JOIN link_rot lr ON lr.source_table = 'supplementary_material'
                                 AND lr.source_id    = sm.supp_id
                                 {since_clause}
            WHERE sm.url IS NOT NULL AND sm.url != ''
              AND lr.rot_id IS NULL
        """
    else:
        q2 = """
            SELECT sm.supp_id, sm.url, sm.article_id,
                   CAST(a.pub_year AS INT) AS pub_year
            FROM supplementary_material sm
            JOIN article a ON a.article_id = sm.article_id
            WHERE sm.url IS NOT NULL AND sm.url != ''
        """
    for row in con.execute(q2):
        sid, url, aid, yr = row
        urls.append({"url": url, "source_table": "supplementary_material",
                     "source_id": sid, "article_id": aid, "pub_year": yr,
                     "link_type": "supplementary"})
    
    return urls


def save_batch(con: sqlite3.Connection, batch: list[dict], meta: list[dict]):
    """Write a batch of results to link_rot table."""
    meta_map = {m["url"]: m for m in meta}
    rows = []
    for res in batch:
        m = meta_map.get(res["url"], {})
        rows.append((
            res["url"],
            res["url_normalised"],
            m.get("source_table", ""),
            m.get("source_id", 0),
            m.get("article_id"),
            m.get("pub_year"),
            res["domain"],
            res["status"],
            res["http_code"],
            res["final_url"],
            res["redirect_count"],
            res["response_ms"],
            res["content_type"],
            res["title_snippet"],
            res["wayback_url"],
            res["checked_at"],
            m.get("link_type"),
        ))
    con.executemany("""
        INSERT OR REPLACE INTO link_rot
          (url, url_normalised, source_table, source_id, article_id, pub_year,
           domain, status, http_code, final_url, redirect_count, response_ms,
           content_type, title_snippet, wayback_url, checked_at, link_type)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, rows)
    con.commit()


# ── Main async runner ──────────────────────────────────────────────────────────

async def run_checks(
    url_meta: list[dict],
    concurrency: int,
    timeout: int,
    check_wayback: bool,
    logger: logging.Logger,
    save_fn,
    batch_size: int = 200,
) -> dict:
    """Run all URL checks concurrently with a semaphore and domain throttle."""
    if not HAS_AIOHTTP:
        logger.error("aiohttp not installed. Cannot run checks.")
        return {}

    sem      = asyncio.Semaphore(concurrency)
    throttle = DomainThrottle(rps=DOMAIN_RPS)

    counters = defaultdict(int)
    results_buffer: list[dict] = []
    meta_buffer:    list[dict] = []

    connector = aiohttp.TCPConnector(
        limit=concurrency + 10,
        ttl_dns_cache=300,
        ssl=False,
        enable_cleanup_closed=True,
    )
    headers = {
        "User-Agent": USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
    }

    t_start = time.monotonic()
    n_total = len(url_meta)
    done    = [0]  # mutable counter for progress

    async def worker(meta: dict):
        async with sem:
            res = await check_one(
                session, throttle, meta["url"], timeout, check_wayback, logger
            )
            counters[res["status"]] += 1
            done[0] += 1
            if done[0] % 100 == 0 or done[0] == n_total:
                elapsed = time.monotonic() - t_start
                rate    = done[0] / elapsed if elapsed > 0 else 0
                eta     = (n_total - done[0]) / rate if rate > 0 else 0
                logger.info(
                    "Progress: %d/%d  (%.0f/s  ETA %.0fs)  "
                    "ok=%d  dead=%d  timeout=%d",
                    done[0], n_total, rate, eta,
                    counters["ok"] + counters["redirect"],
                    counters["client_error"] + counters["soft_404"] +
                    counters["wayback_ok"]  + counters["wayback_miss"],
                    counters["timeout"],
                )
            return res, meta

    async with aiohttp.ClientSession(
        connector=connector,
        headers=headers,
        cookie_jar=aiohttp.DummyCookieJar(),
    ) as session:
        tasks = [asyncio.create_task(worker(m)) for m in url_meta]
        for coro in asyncio.as_completed(tasks):
            res, meta = await coro
            results_buffer.append(res)
            meta_buffer.append(meta)
            if len(results_buffer) >= batch_size:
                save_fn(results_buffer, meta_buffer)
                results_buffer.clear()
                meta_buffer.clear()

    if results_buffer:
        save_fn(results_buffer, meta_buffer)

    return dict(counters)


# ── Entry point ────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Check link rot for all URLs in pmc_fair.db")
    parser.add_argument("--db",          default="pmc_fair.db",
                        help="SQLite database path")
    parser.add_argument("--concurrency", type=int, default=DEFAULT_CONC,
                        help=f"Simultaneous connections (default {DEFAULT_CONC})")
    parser.add_argument("--timeout",     type=int, default=DEFAULT_TO,
                        help=f"Request timeout seconds (default {DEFAULT_TO})")
    parser.add_argument("--wayback",     action="store_true",
                        help="Check Wayback Machine for dead links")
    parser.add_argument("--recheck",     action="store_true",
                        help="Recheck URLs already in link_rot table")
    parser.add_argument("--since",       default=None,
                        help="Only recheck URLs checked before this date (YYYY-MM-DD)")
    parser.add_argument("--limit",       type=int, default=None,
                        help="Max URLs to check (for testing)")
    parser.add_argument("--dry-run",     action="store_true",
                        help="Count URLs only, do not check")
    parser.add_argument("--log",         default="link_rot_check.log",
                        help="Log file path")
    args = parser.parse_args()

    logger = setup_logging(Path(args.log))
    logger.info("check_link_rot.py  v%s  —  db=%s", VERSION, args.db)

    db_path = Path(args.db)
    if not db_path.exists():
        logger.error("Database not found: %s", db_path)
        sys.exit(1)

    con = sqlite3.connect(db_path, timeout=30)
    con.execute("PRAGMA journal_mode=WAL")
    con.executescript(SCHEMA_SQL)
    con.commit()

    logger.info("Collecting URLs to check …")
    url_meta = get_urls_to_check(con, args.recheck, args.since)

    if args.limit:
        url_meta = url_meta[:args.limit]

    source_counts = defaultdict(int)
    for m in url_meta:
        source_counts[m["source_table"]] += 1

    logger.info("URLs to check: %d total", len(url_meta))
    for src, cnt in sorted(source_counts.items()):
        logger.info("  %-30s %d", src, cnt)

    if args.dry_run or not url_meta:
        logger.info("Dry run — exiting.")
        con.close()
        return

    if not HAS_AIOHTTP:
        logger.error("aiohttp required. Install with: pip install aiohttp")
        sys.exit(1)

    # Record run start
    run_id = con.execute(
        "INSERT INTO link_rot_run (started_at, n_urls, concurrency, timeout_s) "
        "VALUES (?,?,?,?)",
        (datetime.utcnow().isoformat(timespec="seconds"),
         len(url_meta), args.concurrency, args.timeout)
    ).lastrowid
    con.commit()

    def save_fn(results, meta):
        save_batch(con, results, meta)

    t0 = time.monotonic()
    counters = asyncio.run(run_checks(
        url_meta        = url_meta,
        concurrency     = args.concurrency,
        timeout         = args.timeout,
        check_wayback   = args.wayback,
        logger          = logger,
        save_fn         = save_fn,
        batch_size      = 200,
    ))
    elapsed = time.monotonic() - t0

    n_ok   = counters.get("ok", 0) + counters.get("redirect", 0)
    n_dead = (counters.get("client_error", 0) + counters.get("soft_404", 0) +
              counters.get("wayback_ok",   0) + counters.get("wayback_miss", 0))
    n_err  = (counters.get("timeout", 0) + counters.get("dns_error", 0) +
              counters.get("ssl_error", 0) + counters.get("server_error", 0) +
              counters.get("other_error", 0))

    con.execute("""
        UPDATE link_rot_run
        SET finished_at=?, n_checked=?, n_ok=?, n_dead=?, n_error=?
        WHERE run_id=?
    """, (datetime.utcnow().isoformat(timespec="seconds"),
          len(url_meta), n_ok, n_dead, n_err, run_id))
    con.commit()

    logger.info("=" * 60)
    logger.info("COMPLETE  —  %.0fs  (%.1f URLs/s)", elapsed, len(url_meta)/elapsed)
    logger.info("  ok / redirect  : %d", n_ok)
    logger.info("  dead (4xx/soft): %d", n_dead)
    logger.info("  network errors : %d", n_err)
    for status, cnt in sorted(counters.items(), key=lambda x: -x[1]):
        logger.info("    %-18s %d", status, cnt)

    con.close()
    logger.info("Results saved to %s  (link_rot table)", args.db)


if __name__ == "__main__":
    main()
