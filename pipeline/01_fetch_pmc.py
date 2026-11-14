"""
fetch_pmc.py
------------
Fetches PubMed Central (PMC) article XML for one or more years.

Stage-1 fetcher for the FAIR principles longitudinal study (2011–2025).

Design decisions
----------------
- Two esearch calls per year:
    1. Unfiltered count  → total PMC articles that year (baseline denominator)
    2. Scoped count      → articles matching the active filter (numerator)
  Both numbers are logged so you can report the filtering ratio in your paper.
  Without --open-access or --extra-query both counts are equal (all articles).
- One XML file per year: pmc_YYYY.xml in the output directory.
- One log file per year: fetch_YYYY.log  (+ master fetch_pmc.log for the run).
- Resumable: --skip-existing skips years whose XML already exists.
- Ecological footprint: wall-clock time and peak RSS memory are logged for
  every year and for the full run.
- Rate-limit aware: NCBI 3 req/s (no key) / 10 req/s (with key),
  exponential back-off on errors.

Usage
-----
  python fetch_pmc.py                             # all years 2011–2025
  python fetch_pmc.py --years 2016                # single year
  python fetch_pmc.py --years 2011–2025 --api-key YOUR_KEY
  python fetch_pmc.py --years 2011–2025 --max-per-year 500   # test run
  python fetch_pmc.py --years 2011–2025 --skip-existing
  python fetch_pmc.py --output-dir ./data/raw
  python fetch_pmc.py --extra-query "ecology[MeSH]"  # subject filter
  python fetch_pmc.py --open-access                  # OA subset only (CC0/CC-BY/etc.)
  python fetch_pmc.py --open-access --oa-comm-only   # commercial-reuse licenses only

Open-access filtering
---------------------
--open-access adds ``"open access"[filter]`` to every esearch query.
This restricts results to the PMC Open Access Subset — articles whose
full text is freely available under an open licence (CC0, CC-BY, CC-BY-SA,
CC-BY-NC, CC-BY-ND, and NIH Author Manuscripts).

--oa-comm-only further restricts to the *commercial-use* sub-tier
(CC0 and CC-BY only) by adding ``"cc0"[filter] OR "cc by"[filter]``.
Use this when your downstream analysis requires data that can be
redistributed or re-published without restriction (e.g. OAMI imports,
text-mining with redistribution).

The two filters correspond to the file lists published at
https://pmc.ncbi.nlm.nih.gov/tools/openftlist/ :
  oa_file_list.txt          → --open-access
  oa_comm_use_file_list.txt → --oa-comm-only
"""

import argparse
import calendar
import io
import logging
import os
import resource          # RSS memory (Linux/macOS)
import sys
import time
import xml.etree.ElementTree as ET
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

BASE_URL      = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/"
DB            = "pmc"
RETMAX_LIMIT  = 9_500     # stay safely below NCBI's hard 10,000 ceiling per esearch call
                          # PMC/PubMed cannot return more than 10,000 IDs per esearch call
                          # regardless of retstart — the fix is date-window segmentation.
BATCH_SIZE    = 200       # IDs per efetch call (≤ 500 recommended)
READ_CHUNK    = 256 * 1024  # 256 KB per socket read — ensures the per-socket timeout
                            # fires on a stalled transfer instead of hanging forever.
                            # resp.read() with no argument blocks until EOF with no timeout;
                            # resp.read(n) is subject to the socket timeout on each call.
CONNECT_TIMEOUT = 60      # seconds — connect + first byte
READ_TIMEOUT    = 120     # seconds — max silence between consecutive chunks
DEFAULT_YEARS = "2011–2025"
DEFAULT_OUT_DIR = "."

# Open-access esearch filter tags
# "open access"[filter]  — full PMC OA subset (all open licences)
# The commercial-only sub-tier uses licence-specific filter tags.
OA_FILTER_ALL   = '"open access"[filter]'
OA_FILTER_COMM  = '("cc0"[filter] OR "cc by"[filter])'

# ---------------------------------------------------------------------------
# Memory helper
# ---------------------------------------------------------------------------

def _peak_rss_mb() -> float:
    """Return peak resident set size in MB (Linux/macOS). Returns 0 on Windows."""
    try:
        kb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        # Linux: kB; macOS: bytes
        return kb / 1024 if sys.platform != "darwin" else kb / (1024 * 1024)
    except Exception:
        return 0.0


# ---------------------------------------------------------------------------
# Logging helpers
# ---------------------------------------------------------------------------

LOG_FMT  = "%(asctime)s  %(levelname)-7s  %(message)s"
LOG_DATE = "%Y-%m-%d %H:%M:%S"


def _make_logger(name: str, log_file: Path, console: bool = True) -> logging.Logger:
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    logger.handlers.clear()
    fmt = logging.Formatter(LOG_FMT, datefmt=LOG_DATE)

    fh = logging.FileHandler(log_file, encoding="utf-8", mode="a")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    if console:
        ch = logging.StreamHandler(sys.stdout)
        ch.setLevel(logging.INFO)
        ch.setFormatter(fmt)
        logger.addHandler(ch)

    return logger


# ---------------------------------------------------------------------------
# Year argument parsing
# ---------------------------------------------------------------------------

def parse_years(years_arg: str) -> list[int]:
    """
    "2016"           → [2016]
    "2011–2025"      → [2011 … 2025]
    "2012,2016,2020" → [2012, 2016, 2020]
    """
    years: set[int] = set()
    for part in years_arg.split(","):
        part = part.strip()
        if "-" in part and not part.startswith("-"):
            s, e = part.split("-", 1)
            years.update(range(int(s), int(e) + 1))
        else:
            years.add(int(part))
    return sorted(years)


# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------

def build_url(endpoint: str, params: dict) -> str:
    return f"{BASE_URL}{endpoint}?{urlencode(params)}"


def fetch_url(url: str, retries: int = 5,
              logger: logging.Logger | None = None) -> bytes:
    """
    GET with exponential back-off retry and chunked body reading.

    Why chunked reads?
    ------------------
    ``urlopen(timeout=N)`` sets the socket timeout for connect and the first
    byte, but a single ``resp.read()`` call (no size argument) asks the OS to
    read until EOF.  If the remote server stalls mid-transfer, no timeout
    fires and the process hangs forever.  Reading in fixed-size chunks means
    each individual socket recv() is subject to READ_TIMEOUT, so a stalled
    transfer raises ``socket.timeout`` within READ_TIMEOUT seconds and the
    retry logic kicks in.
    """
    import socket as _socket
    pause = 1.0
    for attempt in range(1, retries + 1):
        try:
            req = Request(url, headers={"User-Agent": "FAIRStudyFetcher/2.0"})
            with urlopen(req, timeout=CONNECT_TIMEOUT) as resp:
                # Override the socket timeout for the body read phase.
                # urlopen sets it to CONNECT_TIMEOUT; we want a longer
                # window (READ_TIMEOUT) so large but healthy responses don't
                # retry unnecessarily, while stalled transfers still time out.
                try:
                    resp.fp.raw._sock.settimeout(READ_TIMEOUT)
                except Exception:
                    pass  # best-effort; harmless if the attribute path differs
                chunks: list[bytes] = []
                while True:
                    chunk = resp.read(READ_CHUNK)
                    if not chunk:
                        break
                    chunks.append(chunk)
                return b"".join(chunks)
        except HTTPError as exc:
            msg = f"HTTP {exc.code} attempt {attempt}/{retries}"
            if logger:
                logger.warning(msg)
            if exc.code == 429 or attempt == retries:
                raise
            time.sleep(pause * (2 ** (attempt - 1)))
        except OSError as exc:
            # Catches socket.timeout, ConnectionResetError, BrokenPipeError, etc.
            msg = f"Network error attempt {attempt}/{retries}: {exc}"
            if logger:
                logger.warning(msg)
            if attempt == retries:
                raise
            time.sleep(pause * (2 ** (attempt - 1)))


# ---------------------------------------------------------------------------
# NCBI E-utilities
# ---------------------------------------------------------------------------

def _esearch_count(term: str, mindate: str, maxdate: str,
                   api_key: str | None,
                   logger: logging.Logger) -> int:
    """Return total hit count for a query without fetching IDs."""
    params = {
        "db": DB, "term": term,
        "datetype": "pdat", "mindate": mindate, "maxdate": maxdate,
        "retmax": 0, "retmode": "xml",
    }
    if api_key:
        params["api_key"] = api_key
    raw  = fetch_url(build_url("esearch.fcgi", params), logger=logger)
    root = ET.fromstring(raw)
    el   = root.find("Count")
    return int(el.text) if el is not None else 0


def _iter_windows(year: int) -> list[tuple[str, str]]:
    """
    Return a list of (mindate, maxdate) pairs that together cover the full year,
    with each window guaranteed to contain fewer than RETMAX_LIMIT articles.

    Strategy — adaptive two-level segmentation:
      Level 1 — monthly windows (12 per year).
        For years with a small OA corpus (e.g. 2012 ~50k/year ≈ 4k/month)
        the monthly count stays under RETMAX_LIMIT and no further splitting
        is needed.
      Level 2 — daily windows within a month.
        For years with a large OA corpus (e.g. 2024 ~875k/year ≈ 73k/month)
        each month exceeds RETMAX_LIMIT, so we subdivide to one window per day
        (~2,400 articles/day for 2024 — safely under 10,000).

    This produces 12 esearch calls for quiet years and 365–366 for busy ones.
    Even at 3 req/s (no API key) the ID-collection phase takes under 2 minutes
    per year.

    Background: NCBI ESearch cannot return more than 10,000 UIDs for PMC or
    PubMed regardless of retstart.  The documented workaround is to split the
    query into sub-ranges each containing fewer than 10,000 records.
    See https://www.ncbi.nlm.nih.gov/books/NBK25499/ (ESearch, retmax section).
    """
    windows: list[tuple[str, str]] = []
    for month in range(1, 13):
        days_in_month = calendar.monthrange(year, month)[1]
        m_min = f"{year}/{month:02d}/01"
        m_max = f"{year}/{month:02d}/{days_in_month:02d}"
        windows.append((m_min, m_max))
    return windows


def _iter_daily(year: int, month: int) -> list[tuple[str, str]]:
    """Return one (mindate, maxdate) per day for the given month."""
    days_in_month = calendar.monthrange(year, month)[1]
    return [
        (f"{year}/{month:02d}/{day:02d}", f"{year}/{month:02d}/{day:02d}")
        for day in range(1, days_in_month + 1)
    ]


def esearch_counts(year: int, api_key: str | None,
                   extra_query: str | None,
                   oa_filter: str | None,
                   logger: logging.Logger) -> tuple[int, int, str]:
    """
    Return (total_pmc, total_matched, term) for the year without collecting IDs.
    Always called first so fetch_year can log the totals before starting efetch.
    """
    rate = 0.11 if api_key else 0.34

    total_pmc = _esearch_count(
        f"{year}[PDAT]", f"{year}/01/01", f"{year}/12/31", api_key, logger
    )
    logger.info(f"  [{year}] Total PMC articles in {year}          : {total_pmc:>10,}")
    time.sleep(rate)

    if oa_filter and extra_query:
        term = f"{oa_filter} AND ({extra_query})"
    elif oa_filter:
        term = oa_filter
    elif extra_query:
        term = f"({extra_query})"
    else:
        term = f"{year}[PDAT]"

    total_matched = _esearch_count(
        term, f"{year}/01/01", f"{year}/12/31", api_key, logger
    )
    pct = (total_matched / total_pmc * 100) if total_pmc else 0
    logger.info(
        f"  [{year}] Articles matching query               : {total_matched:>10,}"
        f"  ({pct:.1f}% of PMC)"
    )
    time.sleep(rate)

    return total_pmc, total_matched, term


def iter_id_batches(year: int, term: str, max_results: int,
                    total_matched: int, api_key: str | None,
                    logger: logging.Logger):
    """
    Generator — yields lists of PMC IDs in chunks of exactly BATCH_SIZE
    (or fewer for the last chunk).

    IDs are fetched window-by-window using adaptive date segmentation and
    accumulated only until BATCH_SIZE is reached, then yielded and discarded.
    At no point is more than one window's worth of IDs (~9,500) held in RAM
    alongside the already-yielded-and-discarded previous chunks.

    Memory cost: max(RETMAX_LIMIT, BATCH_SIZE) IDs at any one time —
    roughly 9,500 strings ≈ 700 KB, regardless of total_matched.

    NCBI ESearch cannot return more than 10,000 UIDs per call for PMC/PubMed.
    We work around this by segmenting the year into monthly windows, further
    subdividing any month whose count exceeds RETMAX_LIMIT into daily windows.
    See https://www.ncbi.nlm.nih.gov/books/NBK25499/ (ESearch, retmax).
    """
    rate       = 0.11 if api_key else 0.34
    n_to_fetch = min(total_matched, max_results)
    n_yielded  = 0
    buffer: list[str] = []   # holds IDs until we have a full BATCH_SIZE chunk

    for month_idx, (w_min, w_max) in enumerate(_iter_windows(year), 1):
        if n_yielded + len(buffer) >= n_to_fetch:
            break

        month_count = _esearch_count(term, w_min, w_max, api_key, logger)
        time.sleep(rate)

        if month_count == 0:
            continue

        if month_count <= RETMAX_LIMIT:
            windows_to_process = [(w_min, w_max)]
        else:
            month_num = month_idx
            daily = _iter_daily(year, month_num)
            windows_to_process = daily
            logger.debug(
                f"  [{year}] Month {month_num:02d}: {month_count:,} articles"
                f" — splitting into {len(daily)} daily windows"
            )

        for win_min, win_max in windows_to_process:
            if n_yielded + len(buffer) >= n_to_fetch:
                break

            params: dict = {
                "db": DB, "term": term,
                "datetype": "pdat", "mindate": win_min, "maxdate": win_max,
                "retmax": RETMAX_LIMIT, "retmode": "xml",
            }
            if api_key:
                params["api_key"] = api_key

            raw  = fetch_url(build_url("esearch.fcgi", params), logger=logger)
            root = ET.fromstring(raw)
            ids  = [el.text for el in root.findall(".//Id") if el.text]

            # Respect max_results cap
            remaining = n_to_fetch - n_yielded - len(buffer)
            buffer.extend(ids[:remaining])

            # Yield complete BATCH_SIZE chunks immediately so the caller can
            # efetch and write them to disk, then they are garbage-collected.
            while len(buffer) >= BATCH_SIZE:
                yield buffer[:BATCH_SIZE]
                n_yielded += BATCH_SIZE
                buffer     = buffer[BATCH_SIZE:]

            time.sleep(rate)

        logger.debug(
            f"  [{year}] After month {month_idx:02d}:"
            f" {n_yielded + len(buffer):,}/{n_to_fetch:,} IDs dispatched"
        )

    # Yield any remaining IDs that did not fill a complete batch
    if buffer:
        yield buffer
        n_yielded += len(buffer)


def efetch_batch(ids: list[str], api_key: str | None,
                 logger: logging.Logger) -> bytes:
    params: dict = {
        "db": DB, "id": ",".join(ids),
        "rettype": "full", "retmode": "xml",
    }
    if api_key:
        params["api_key"] = api_key
    return fetch_url(build_url("efetch.fcgi", params), logger=logger)


# ---------------------------------------------------------------------------
# Streaming XML writer
# ---------------------------------------------------------------------------
#
# Design constraints
# ------------------
# A full year of PMC OA articles is several GB of XML.  The three functions
# below ensure that at no point is more than one efetch batch (~3 MB) held in
# memory at once:
#
#   open_xml_file        — opens file with an 8 MB write buffer
#   write_batch_to_file  — streams through the batch with iterparse,
#                          writes + clears each article individually
#   close_xml_file       — flushes, writes the closing tag, closes
#
# Why iterparse instead of ET.fromstring + ET.tostring?
# ET.fromstring builds the full batch tree in RAM before writing anything.
# ET.tostring then re-serialises it — doubling the memory cost per batch and
# adding significant CPU work for 200-article batches.
# iterparse fires a callback for each completed element; we write it to disk
# and call elem.clear() to release it immediately.  Peak memory stays at
# roughly one article element (~50 KB) rather than one full batch tree (~8 MB).

def open_xml_file(out_path: Path) -> "io.BufferedWriter":
    """
    Open the output file in binary mode with an 8 MB OS write buffer.
    Write the XML declaration and root open tag, then return the handle.
    """
    import io
    fh = io.open(out_path, "wb", buffering=8 * 1024 * 1024)
    fh.write(b'<?xml version="1.0" encoding="UTF-8"?>\n')
    fh.write(b"<PMCArticleSet>\n")
    return fh


def write_batch_to_file(raw: bytes,
                        fh: "io.BufferedWriter",
                        logger: logging.Logger,
                        batch_num: int) -> int:
    """
    Stream one efetch response through iterparse, writing each top-level
    child element (one PMC article) to *fh* immediately and clearing it from
    memory.  Returns the number of article elements written.

    Memory cost: one article element at a time, regardless of batch size.
    """
    import io as _io
    written = 0
    depth   = 0
    try:
        for event, elem in ET.iterparse(_io.BytesIO(raw), events=("start", "end")):
            if event == "start":
                depth += 1
            else:                          # "end"
                if depth == 2:             # direct child of root just completed
                    fh.write(b"  ")
                    fh.write(ET.tostring(elem, encoding="unicode").encode("utf-8"))
                    fh.write(b"\n")
                    elem.clear()
                    written += 1
                depth -= 1
    except ET.ParseError as exc:
        logger.warning(f"Could not parse efetch batch {batch_num}: {exc}")
    return written


def close_xml_file(fh: "io.BufferedWriter", logger: logging.Logger) -> None:
    """Flush the write buffer, write the closing root tag, and close the file."""
    logger.debug("Flushing and closing XML output file…")
    fh.write(b"</PMCArticleSet>\n")
    fh.flush()
    fh.close()
    logger.debug("XML output file closed.")


# ---------------------------------------------------------------------------
# Per-year fetch
# ---------------------------------------------------------------------------

def fetch_year(year: int, out_dir: Path, api_key: str | None,
               max_per_year: int, extra_query: str | None,
               oa_filter: str | None,
               skip_existing: bool,
               master_logger: logging.Logger) -> dict:
    """Fetch one year. Returns a summary dict for the master log."""

    out_path  = out_dir / f"pmc_{year}.xml"
    log_path  = out_dir / f"fetch_{year}.log"

    # Per-year logger (file only — console output via master logger)
    yr_log = _make_logger(f"fetch_pmc.{year}", log_path, console=False)

    def log(msg: str, level: str = "info") -> None:
        getattr(master_logger, level)(msg)
        getattr(yr_log, level)(msg)

    if skip_existing and out_path.exists():
        size_kb = out_path.stat().st_size / 1024
        log(f"  [{year}] SKIPPED — {out_path.name} already exists ({size_kb:,.0f} KB). "
            "Use --no-skip-existing to re-fetch.")
        return {"year": year, "status": "skipped", "articles": 0,
                "total_pmc": 0, "total_das": 0,
                "size_kb": round(size_kb, 1), "elapsed_s": 0,
                "peak_rss_mb": 0, "path": str(out_path)}

    log(f"{'─'*60}")
    log(f"  [{year}] Starting fetch  →  {out_path.name}")
    log(f"  [{year}] Per-year log    →  {log_path.name}")
    t0    = time.perf_counter()
    rss0  = _peak_rss_mb()

    # Step 1 — counts only (no IDs collected yet)
    try:
        total_pmc, total_das, term = esearch_counts(
            year, api_key, extra_query, oa_filter, yr_log
        )
        master_logger.info(
            f"  [{year}] Total PMC / matched: {total_pmc:,} / {total_das:,}"
        )
    except Exception as exc:
        log(f"  [{year}] esearch FAILED: {exc}", "error")
        return {"year": year, "status": "error", "error": str(exc),
                "total_pmc": 0, "total_das": 0, "articles": 0}

    if total_das == 0:
        log(f"  [{year}] No articles matched — skipping efetch.")
        return {"year": year, "status": "empty", "articles": 0,
                "total_pmc": total_pmc, "total_das": total_das,
                "size_kb": 0, "elapsed_s": 0, "peak_rss_mb": 0}

    n_to_fetch    = min(total_das, max_per_year)
    rate          = 0.11 if api_key else 0.34
    total_batches = (n_to_fetch + BATCH_SIZE - 1) // BATCH_SIZE
    n_articles    = 0
    n_batches_done = 0
    fh            = open_xml_file(out_path)

    # Step 2 — iterate ID batches from the generator; efetch and write each
    # batch immediately, then it is garbage-collected.  The full ID list is
    # never materialised in RAM.
    try:
        for batch in iter_id_batches(
                year, term, max_per_year, total_das, api_key, yr_log):

            n_batches_done += 1

            if n_batches_done == 1 or n_batches_done % 100 == 0 \
                    or n_batches_done == total_batches:
                yr_log.info(
                    f"  [{year}] efetch batch {n_batches_done}/{total_batches}"
                    f"  articles written so far: {n_articles:,}"
                )

            try:
                raw = efetch_batch(batch, api_key, yr_log)
                n_articles += write_batch_to_file(raw, fh, yr_log, n_batches_done)
            except Exception as exc:
                yr_log.error(f"  [{year}] batch {n_batches_done} failed: {exc}")

            if n_batches_done % 100 == 0:
                fh.flush()

            time.sleep(rate)

    finally:
        close_xml_file(fh, yr_log)

    # Step 3 — stats (file is already written)
    elapsed    = time.perf_counter() - t0
    peak_rss   = max(_peak_rss_mb() - rss0, 0)
    size_kb    = out_path.stat().st_size / 1024

    log(f"  [{year}] Written: {n_articles:,} articles  |  "
        f"{size_kb:,.0f} KB  |  {elapsed:.1f}s  |  +{peak_rss:.1f} MB RSS")

    yr_log.info(f"  [{year}] Fetch complete.")
    yr_log.info(f"  [{year}]   Total PMC articles this year : {total_pmc:,}")
    yr_log.info(f"  [{year}]   Matched query                : {total_das:,}  "
                f"({total_das/total_pmc*100:.1f}% of PMC)" if total_pmc else "")
    yr_log.info(f"  [{year}]   efetch batches processed     : {n_batches_done:,}")
    yr_log.info(f"  [{year}]   Articles written to XML      : {n_articles:,}")
    yr_log.info(f"  [{year}]   Output file size             : {size_kb:,.1f} KB")
    yr_log.info(f"  [{year}]   Wall-clock time              : {elapsed:.1f} s")
    yr_log.info(f"  [{year}]   Peak RSS memory delta        : {peak_rss:.1f} MB")

    return {
        "year": year, "status": "ok",
        "total_pmc": total_pmc, "total_das": total_das,
        "articles": n_articles, "size_kb": round(size_kb, 1),
        "elapsed_s": round(elapsed, 1), "peak_rss_mb": round(peak_rss, 1),
        "path": str(out_path),
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Fetch PMC articles for a range of years.\n"
            "FAIR longitudinal study pipeline — Stage 1."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python fetch_pmc.py                                    # all years 2011–2025
  python fetch_pmc.py --years 2016                       # single year
  python fetch_pmc.py --years 2011–2025 --api-key KEY
  python fetch_pmc.py --years 2011–2025 --max-per-year 500   # test run
  python fetch_pmc.py --years 2012,2016,2020             # specific years
  python fetch_pmc.py --years 2011–2025 --skip-existing  # resume
  python fetch_pmc.py --open-access                      # OA subset (all licences)
  python fetch_pmc.py --oa-comm-only                     # CC0/CC-BY only (OAMI-eligible)
  python fetch_pmc.py --open-access --extra-query "ecology[MeSH]"
        """,
    )
    parser.add_argument("--years", default=DEFAULT_YEARS,
        help="Year(s): '2016', '2011–2025', or '2012,2016,2020'.")
    parser.add_argument("--output-dir", "-o", default=DEFAULT_OUT_DIR,
        help="Directory for pmc_YYYY.xml and per-year log files.")
    parser.add_argument("--max-per-year", type=int, default=999_999,
        help="Cap on articles per year (default: unlimited). Use ~500 for testing.")
    parser.add_argument("--api-key", default=os.environ.get("NCBI_API_KEY"),
        help="NCBI API key (or set env var NCBI_API_KEY). Raises rate limit to 10 req/s.")
    parser.add_argument("--skip-existing", action="store_true", default=True,
        help="Skip years whose pmc_YYYY.xml exists (default: on).")
    parser.add_argument("--no-skip-existing", dest="skip_existing",
        action="store_false",
        help="Re-fetch even if pmc_YYYY.xml already exists.")
    parser.add_argument("--extra-query", default=None,
        help="Additional search terms ANDed with the query (e.g. 'ecology[MeSH]').")
    # ── Open-access filtering ──────────────────────────────────────────────────
    oa_group = parser.add_mutually_exclusive_group()
    oa_group.add_argument(
        "--open-access", action="store_true", default=False,
        help=(
            "Restrict to the PMC Open Access Subset — articles available under "
            "any open licence (CC0, CC-BY, CC-BY-SA, CC-BY-NC, CC-BY-ND, NIH "
            "Author Manuscripts).  Adds \"open access\"[filter] to every query. "
            "Corresponds to oa_file_list.txt at "
            "https://pmc.ncbi.nlm.nih.gov/tools/openftlist/"
        ),
    )
    oa_group.add_argument(
        "--oa-comm-only", action="store_true", default=False,
        help=(
            "Restrict to the PMC commercial-reuse sub-tier only — CC0 and CC-BY "
            "licences that allow redistribution and commercial use without "
            "restriction.  Adds (\"cc0\"[filter] OR \"cc by\"[filter]) to every "
            "query.  Corresponds to oa_comm_use_file_list.txt at "
            "https://pmc.ncbi.nlm.nih.gov/tools/openftlist/"
        ),
    )
    args = parser.parse_args()

    out_dir  = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    years    = parse_years(args.years)

    # Resolve OA filter (mutually exclusive flags)
    if args.oa_comm_only:
        oa_filter = OA_FILTER_COMM
        oa_label  = "commercial-reuse only (CC0/CC-BY)"
    elif args.open_access:
        oa_filter = OA_FILTER_ALL
        oa_label  = "full OA subset (all open licences)"
    else:
        oa_filter = None
        oa_label  = "none (all PMC, open + subscription)"

    master_log_path = out_dir / "fetch_pmc.log"
    master  = _make_logger("fetch_pmc.master", master_log_path, console=True)

    run_t0   = time.perf_counter()
    run_rss0 = _peak_rss_mb()

    master.info("=" * 60)
    master.info("PMC Full Fetcher  —  FAIR Longitudinal Study")
    master.info("=" * 60)
    master.info(f"  Years          : {years[0]}–{years[-1]}  ({len(years)} years)")
    master.info(f"  Max/year       : {'unlimited' if args.max_per_year > 500_000 else f'{args.max_per_year:,}'}")
    master.info(f"  Output dir     : {out_dir.resolve()}")
    master.info(f"  API key        : {'SET' if args.api_key else 'NOT SET  (3 req/s limit)'}")
    master.info(f"  Skip existing  : {args.skip_existing}")
    master.info(f"  OA filter      : {oa_label}")
    master.info(f"  Master log     : {master_log_path.name}")
    if args.extra_query:
        master.info(f"  Extra query    : {args.extra_query}")
    master.info("")

    summaries: list[dict] = []
    for year in years:
        try:
            s = fetch_year(
                year=year, out_dir=out_dir, api_key=args.api_key,
                max_per_year=args.max_per_year, extra_query=args.extra_query,
                oa_filter=oa_filter,
                skip_existing=args.skip_existing, master_logger=master,
            )
            summaries.append(s)
        except Exception as exc:
            master.error(f"  [{year}] UNEXPECTED ERROR: {exc}")
            summaries.append({"year": year, "status": "error", "error": str(exc),
                               "total_pmc": 0, "total_das": 0, "articles": 0,
                               "size_kb": 0, "elapsed_s": 0, "peak_rss_mb": 0})
        time.sleep(1.0)   # polite pause between years

    # ---- Final summary table ----
    run_elapsed = time.perf_counter() - run_t0
    run_rss     = max(_peak_rss_mb() - run_rss0, 0)

    w = 62
    master.info("")
    master.info("=" * w)
    master.info("FETCH SUMMARY")
    master.info("=" * w)
    hdr = f"  {'Year':<6}  {'Status':<9}  {'PMC total':>10}  {'Matched':>10}  {'Fetched':>8}  {'KB':>8}  {'Time(s)':>8}  {'RSS MB':>7}"
    master.info(hdr)
    master.info("  " + "─" * (w - 2))

    tot_pmc = tot_das = tot_art = tot_kb = tot_t = tot_rss = 0
    for s in summaries:
        tot_pmc += s.get("total_pmc", 0)
        tot_das += s.get("total_das", 0)
        tot_art += s.get("articles",  0)
        tot_kb  += s.get("size_kb",   0)
        tot_t   += s.get("elapsed_s", 0)
        tot_rss += s.get("peak_rss_mb", 0)
        master.info(
            f"  {s['year']:<6}  {s['status']:<9}  "
            f"{s.get('total_pmc', 0):>10,}  {s.get('total_das', 0):>10,}  "
            f"{s.get('articles', 0):>8,}  {s.get('size_kb', 0):>8,.0f}  "
            f"{s.get('elapsed_s', 0):>8.1f}  {s.get('peak_rss_mb', 0):>7.1f}"
        )

    master.info("  " + "─" * (w - 2))
    master.info(
        f"  {'TOTAL':<6}  {'':<9}  "
        f"{tot_pmc:>10,}  {tot_das:>10,}  "
        f"{tot_art:>8,}  {tot_kb:>8,.0f}  "
        f"{run_elapsed:>8.1f}  {run_rss:>7.1f}"
    )
    master.info("=" * w)
    master.info(f"  Total wall-clock time : {run_elapsed/60:.1f} min")
    master.info(f"  Peak RSS memory (run) : {run_rss:.1f} MB")
    master.info(f"  Master log            : {master_log_path}")
    master.info("")
    master.info("Next step:")
    master.info("  python pubmed_to_sqlite.py --input ./data/raw --db pmc_fair.db")


if __name__ == "__main__":
    main()
