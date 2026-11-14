"""
check_year_coverage.py
-----------------------
Diagnostic utility for the PMC FAIR longitudinal study pipeline.

Compares, for each publication year, the authoritative "how many PMC
Open-Access-subset articles does NCBI have for this year" count against
how many articles we actually hold in the local database for that year.
This is how the 2018 gap (NCBI backfilled/updated far more 2018 OA
articles than our original fetch captured) was first spotted, and this
script is meant to catch the *next* such gap -- for 2018 or any other
year -- without re-parsing any XML.

It queries NCBI with exactly the same esearch term pipeline/01_fetch_pmc.py
uses at fetch time (``"open access"[filter]``, restricted to the year via
``datetype=pdat``), so the "NCBI OA count" column here is directly
comparable to what a ``--open-access`` fetch run would try to retrieve
today.

Usage
-----
  python check_year_coverage.py --db ../data/processed/pmc_fair.db
  python check_year_coverage.py --db ../data/processed/pmc_fair.db --years 2011-2025
  python check_year_coverage.py --db ../data/processed/pmc_fair.db --api-key YOUR_KEY
  python check_year_coverage.py --db ../data/processed/pmc_fair.db --csv coverage.csv
  python check_year_coverage.py --db ../data/processed/pmc_fair.db --threshold-pct 2.0

What a gap means
-----------------
A nonzero (NCBI OA count) - (DB count) gap for a year means one of:
  - that year hasn't been (re-)fetched since NCBI last added/updated
    articles for it (NCBI backfills/relicenses continuously, which is
    exactly what happened for 2018)
  - the fetch or parse step was interrupted or skipped for that year
  - articles were fetched under the OA filter at the time but have since
    been removed from the OA subset (licence changes; rare)
This script only flags the gap -- it does not try to guess which case
applies. A flagged year needs a closer look (re-run 01_fetch_pmc.py
--years YYYY --skip-existing, then 02_parse_to_sqlite.py, then
derive_year.py --fill-missing).

Note on `year` vs `pub_year`
-----------------------------
DB counts here use the derived `article.year` column (see derive_year.py),
not the raw `pub_year` column, since `pub_year` is known to be
inconsistently populated across publishers (see derive_year.py's
docstring). Run derive_year.py before this script if `year` hasn't been
backfilled for newly-fetched rows yet (--fill-missing is cheap and safe to
run speculatively).
"""

import argparse
import csv
import logging
import os
import sqlite3
import sys
import time
import xml.etree.ElementTree as ET
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import Request, urlopen

BASE_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/"
DB_NAME = "pmc"
OA_FILTER_ALL = '"open access"[filter]'
CONNECT_TIMEOUT = 60
READ_TIMEOUT = 120


def setup_logging() -> logging.Logger:
    logger = logging.getLogger("check_year_coverage")
    logger.setLevel(logging.INFO)
    fmt = logging.Formatter("%(asctime)s  %(levelname)-7s  %(message)s", "%H:%M:%S")
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    logger.addHandler(sh)
    return logger


def parse_years(years_arg: str) -> list[int]:
    years: set[int] = set()
    for part in years_arg.split(","):
        part = part.strip()
        if "-" in part and not part.startswith("-"):
            s, e = part.split("-", 1)
            years.update(range(int(s), int(e) + 1))
        else:
            years.add(int(part))
    return sorted(years)


def fetch_url(url: str, retries: int, logger: logging.Logger) -> bytes:
    pause = 1.0
    for attempt in range(1, retries + 1):
        try:
            req = Request(url, headers={"User-Agent": "FAIRStudyCoverageCheck/1.0"})
            with urlopen(req, timeout=CONNECT_TIMEOUT) as resp:
                return resp.read()
        except HTTPError as exc:
            logger.warning(f"HTTP {exc.code} attempt {attempt}/{retries}")
            if exc.code == 429 or attempt == retries:
                raise
            time.sleep(pause * (2 ** (attempt - 1)))
        except OSError as exc:
            logger.warning(f"Network error attempt {attempt}/{retries}: {exc}")
            if attempt == retries:
                raise
            time.sleep(pause * (2 ** (attempt - 1)))


def ncbi_oa_count(year: int, api_key: str | None, logger: logging.Logger) -> int:
    """Authoritative NCBI count of PMC OA-subset articles for `year`."""
    from urllib.parse import urlencode
    params = {
        "db": DB_NAME,
        "term": OA_FILTER_ALL,
        "datetype": "pdat",
        "mindate": f"{year}/01/01",
        "maxdate": f"{year}/12/31",
        "retmax": 0,
        "retmode": "xml",
    }
    if api_key:
        params["api_key"] = api_key
    url = f"{BASE_URL}esearch.fcgi?{urlencode(params)}"
    raw = fetch_url(url, retries=5, logger=logger)
    root = ET.fromstring(raw)
    el = root.find("Count")
    return int(el.text) if el is not None else 0


def db_year_counts(db_path: Path, years: list[int]) -> dict[int, int]:
    con = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    cols = {r[1] for r in con.execute("PRAGMA table_info(article)").fetchall()}
    if "year" not in cols:
        con.close()
        raise SystemExit(
            "article.year does not exist in this database -- run derive_year.py first."
        )
    rows = con.execute(
        "SELECT year, COUNT(*) FROM article WHERE year IS NOT NULL GROUP BY year"
    ).fetchall()
    con.close()
    counts = {y: 0 for y in years}
    for y, n in rows:
        if y in counts:
            counts[y] = n
    return counts


def main():
    parser = argparse.ArgumentParser(
        description="Compare NCBI's authoritative per-year PMC OA counts against "
                    "what's actually in our local database.",
        formatter_class=argparse.RawDescriptionHelpFormatter, epilog=__doc__)
    parser.add_argument("--db", default="../data/processed/pmc_fair.db",
                         help="Path to the SQLite database")
    parser.add_argument("--years", default="2011-2025",
                         help="Year or range to check, e.g. 2011-2025 or 2018,2019")
    parser.add_argument("--api-key", default=os.environ.get("NCBI_API_KEY"),
                         help="NCBI API key (10 req/s instead of 3 req/s)")
    parser.add_argument("--threshold-pct", type=float, default=1.0,
                         help="Flag years whose |NCBI - DB| gap exceeds this "
                              "percentage of the NCBI count (default: 1.0)")
    parser.add_argument("--csv", default=None, help="Optional path to write results as CSV")
    args = parser.parse_args()

    logger = setup_logging()
    years = parse_years(args.years)
    db_path = Path(args.db)
    if not db_path.exists():
        logger.error(f"Database not found: {db_path}")
        sys.exit(1)

    logger.info(f"Checking {len(years)} year(s) against {db_path} ...")
    db_counts = db_year_counts(db_path, years)

    rate = 0.11 if args.api_key else 0.34
    results = []
    for year in years:
        ncbi_n = ncbi_oa_count(year, args.api_key, logger)
        time.sleep(rate)
        db_n = db_counts.get(year, 0)
        diff = ncbi_n - db_n
        diff_pct = (diff / ncbi_n * 100) if ncbi_n else 0.0
        flagged = abs(diff_pct) > args.threshold_pct
        results.append((year, ncbi_n, db_n, diff, diff_pct, flagged))
        flag_str = "  <-- GAP" if flagged else ""
        logger.info(
            f"  {year}:  NCBI OA = {ncbi_n:>9,}   DB = {db_n:>9,}   "
            f"diff = {diff:>+9,}  ({diff_pct:>+6.2f}%){flag_str}"
        )

    logger.info("")
    flagged_years = [r[0] for r in results if r[5]]
    if flagged_years:
        logger.info(
            f"Years exceeding +/-{args.threshold_pct}% gap: "
            f"{', '.join(str(y) for y in flagged_years)}"
        )
        logger.info(
            "For each: re-run 01_fetch_pmc.py --years YYYY --open-access --skip-existing, "
            "then 02_parse_to_sqlite.py, then derive_year.py --fill-missing."
        )
    else:
        logger.info(f"No year exceeds the +/-{args.threshold_pct}% gap threshold.")

    if args.csv:
        with open(args.csv, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["year", "ncbi_oa_count", "db_count", "diff", "diff_pct", "flagged"])
            writer.writerows(results)
        logger.info(f"Wrote {args.csv}")


if __name__ == "__main__":
    main()
