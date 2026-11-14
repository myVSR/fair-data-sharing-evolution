"""
compute_gender_extras.py
-------------------------
One-time batch precomputation for the two notebooks/06_gender_analysis.ipynb
analyses that were still hitting DeadKernelError even after gender inference
itself was moved offline (see compute_gender.py): per-author productivity
trajectories (Section 8) and per-article international-collaboration country
counts (Section 9).

ROOT CAUSE (confirmed directly against the live database with
EXPLAIN QUERY PLAN and wall-clock timing, not guessed):

1. Productivity (Section 8) grouped over ALL 47.9M article_author rows
   (every author position, not just first authors like every other section
   in that notebook) while ALSO joining `author` and `given_name_gender` --
   a text-keyed join -- inside the same expensive GROUP BY. `article_author`
   had no index on `author_id` at all (only its (article_id, author_id)
   primary key), so any join plan touching it by author_id risked a near-full
   rescan per probe.

2. International-collaboration (Section 9) queried `author_affiliation`
   (36.5M rows) joined to `author`, `das_category`, and `given_name_gender`
   for EVERY author position on EVERY article, then ran a Python country
   regex over the full, duplicated result -- but that regex itself used the
   OLD per-row "loop over ~250 country names with a separate re.search()
   call each" pattern. notebooks/11_geographical.ipynb hit the exact same
   wall ("that loop, run against 5.4M affiliation rows, is what timed out")
   and already fixed it with a single compiled alternation regex; Section 9
   had not picked up that fix, and additionally ran the (slow or fast)
   regex once per author-affiliation row (36.5M) instead of once per
   *distinct* affiliation string (11.3M, and most of those are shared
   across many author rows since `affiliation.affiliation` is UNIQUE).

FIX APPLIED HERE, offline, once:
  - `author_productivity`: one server-side SQL GROUP BY over just
    article_author/article/das_category (no text-keyed join at all --
    gender is attached back in the notebook afterwards via a cheap
    in-memory pandas merge against the small author/given_name_gender
    tables).
  - `affiliation_country`: the compiled-alternation-regex country
    extractor from 11_geographical.ipynb, run once per distinct
    affiliation_id (11.3M rows, ~100k rows/s measured) rather than once
    per author-affiliation pair (36.5M) or with the old ~250-regex loop.
  - `article_country_count`: one server-side SQL GROUP BY joining only
    author_affiliation (3 integer columns) against the now-small
    affiliation_country table -- no text, no author/gender/das_category
    join needed for this step either.

Also adds `idx_aa_author` (an index on article_author.author_id) to the
database itself, since several existing and future queries join on that
column and none of them had an index to use.

Usage
-----
  python pipeline/compute_gender_extras.py --db data/processed/pmc_fair.db
  python pipeline/compute_gender_extras.py --db pmc_fair.db --reset
"""

import argparse
import logging
import re
import sqlite3
import sys
import time
from pathlib import Path

LOG_FMT = "%(asctime)s  %(levelname)-7s  %(message)s"
LOG_DATE = "%H:%M:%S"


def setup_logging(log_path: Path) -> logging.Logger:
    logger = logging.getLogger("compute_gender_extras")
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


SCHEMA = """
CREATE TABLE IF NOT EXISTS author_productivity (
    author_id  INTEGER PRIMARY KEY REFERENCES author(author_id),
    n_papers   INTEGER NOT NULL,
    avg_score  REAL,
    pre_score  REAL,
    post_score REAL,
    first_rate REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS affiliation_country (
    affiliation_id INTEGER PRIMARY KEY REFERENCES affiliation(affiliation_id),
    iso2           TEXT
);

CREATE TABLE IF NOT EXISTS article_country_count (
    article_id  INTEGER PRIMARY KEY REFERENCES article(article_id),
    n_countries INTEGER NOT NULL
);
"""

RESET_TABLES = ["author_productivity", "affiliation_country", "article_country_count"]

# ---------------------------------------------------------------------------
# Country extraction -- identical lookup table and compiled-alternation-regex
# strategy as notebooks/11_geographical.ipynb Section 5.0, which already
# solved the exact "per-row loop over ~250 regexes" slowdown this script is
# also avoiding. Kept self-contained here rather than imported, matching this
# codebase's existing convention of duplicating small helper blocks per
# script/notebook rather than factoring out a shared module.
# ---------------------------------------------------------------------------

def _build_country_lookup():
    import pycountry

    lookup_raw = {}
    for c in pycountry.countries:
        for n in [c.name, getattr(c, "official_name", None), getattr(c, "common_name", None)]:
            if n:
                lookup_raw[n.lower()] = c.alpha_2

    lookup_raw.update({
        "usa": "US", "u.s.a": "US", "u.s.a.": "US", "united states": "US",
        "united states of america": "US",
        "uk": "GB", "u.k.": "GB", "england": "GB", "scotland": "GB",
        "wales": "GB", "great britain": "GB", "united kingdom": "GB",
        "south korea": "KR", "republic of korea": "KR",
        "taiwan": "TW", "republic of china": "TW",
        "russia": "RU", "russian federation": "RU",
        "iran": "IR", "islamic republic of iran": "IR",
        "vietnam": "VN", "viet nam": "VN",
        "czech republic": "CZ", "czechia": "CZ",
        "peoples republic of china": "CN", "p.r. china": "CN",
        "p.r.china": "CN", "china mainland": "CN",
        "hong kong": "HK", "the netherlands": "NL", "holland": "NL",
        "uae": "AE", "united arab emirates": "AE",
        "ivory coast": "CI", "cote d'ivoire": "CI",
        "tanzania": "TZ", "laos": "LA", "moldova": "MD",
        "bolivia": "BO", "venezuela": "VE", "syria": "SY",
        "trinidad": "TT", "trinidad and tobago": "TT",
    })

    sorted_names = sorted(lookup_raw.keys(), key=len, reverse=True)
    alias_re = re.compile(r"\b(" + "|".join(re.escape(n) for n in sorted_names) + r")\b")
    return lookup_raw, alias_re


def _make_country_extractor():
    lookup_raw, alias_re = _build_country_lookup()

    def _country_from_affil(text):
        if not text:
            return None
        t = text.lower().rstrip(". \n")
        parts = [p.strip() for p in t.split(",")]
        for part in reversed(parts[-3:]):
            part_clean = re.sub(r"\d{4,}", "", part).strip()
            if part_clean in lookup_raw:
                return lookup_raw[part_clean]
        matches = alias_re.findall(t)
        if matches:
            return lookup_raw[max(matches, key=len)]
        return None

    return _country_from_affil


def ensure_schema(con: sqlite3.Connection, logger: logging.Logger) -> None:
    con.executescript(SCHEMA)
    existing_idx = {r[1] for r in con.execute("PRAGMA index_list(article_author)").fetchall()}
    if "idx_aa_author" not in existing_idx:
        logger.info("  Creating idx_aa_author on article_author(author_id) (one-time)...")
        t0 = time.perf_counter()
        con.execute("CREATE INDEX idx_aa_author ON article_author(author_id)")
        logger.info(f"    done in {time.perf_counter()-t0:.0f}s")
    con.commit()


def build_author_productivity(con: sqlite3.Connection, logger: logging.Logger) -> None:
    existing = con.execute("SELECT COUNT(*) FROM author_productivity").fetchone()[0]
    if existing:
        logger.info(f"  author_productivity already populated ({existing:,} rows) -- skipping.")
        return
    logger.info("  Building author_productivity (single server-side GROUP BY, "
                "no text-keyed join)...")
    t0 = time.perf_counter()
    con.execute("""
        INSERT INTO author_productivity
        SELECT aa.author_id,
               COUNT(DISTINCT aa.article_id)                                          AS n_papers,
               ROUND(AVG(dc.data_sharing_score), 3)                                   AS avg_score,
               ROUND(AVG(CASE WHEN a.year < 2016 THEN dc.data_sharing_score END), 3)   AS pre_score,
               ROUND(AVG(CASE WHEN a.year > 2016 THEN dc.data_sharing_score END), 3)   AS post_score,
               COUNT(CASE WHEN aa.author_position = 1 THEN 1 END) * 1.0
                 / COUNT(DISTINCT aa.article_id)                                       AS first_rate
        FROM article_author aa
        JOIN article      a  ON a.article_id  = aa.article_id
        JOIN das_category dc ON dc.article_id = aa.article_id
        WHERE a.year BETWEEN 2011 AND 2025
        GROUP BY aa.author_id
        HAVING n_papers >= 2
    """)
    con.commit()
    n = con.execute("SELECT COUNT(*) FROM author_productivity").fetchone()[0]
    logger.info(f"    author_productivity: {n:,} authors with >=2 papers, "
                f"{time.perf_counter()-t0:.0f}s")


def build_affiliation_country(con: sqlite3.Connection, logger: logging.Logger, batch_size: int) -> None:
    total = con.execute("SELECT COUNT(*) FROM affiliation").fetchone()[0]
    done = con.execute("SELECT COUNT(*) FROM affiliation_country").fetchone()[0]
    if done >= total and total > 0:
        logger.info(f"  affiliation_country already complete ({done:,}/{total:,}) -- skipping.")
        return
    logger.info(f"  Building affiliation_country ({done:,}/{total:,} already done; "
                "one regex pass per DISTINCT affiliation string, not per author row)...")
    extractor = _make_country_extractor()
    t0 = time.perf_counter()
    cur = con.execute("""
        SELECT affiliation_id, affiliation FROM affiliation
        WHERE affiliation_id NOT IN (SELECT affiliation_id FROM affiliation_country)
    """)
    buf = []
    n_done = 0
    while True:
        rows = cur.fetchmany(batch_size)
        if not rows:
            break
        for affiliation_id, text in rows:
            buf.append((affiliation_id, extractor(text)))
        con.executemany(
            "INSERT OR IGNORE INTO affiliation_country (affiliation_id, iso2) VALUES (?,?)", buf)
        con.commit()
        n_done += len(buf)
        buf = []
        elapsed = time.perf_counter() - t0
        rate = n_done / elapsed if elapsed > 0 else 0
        logger.info(f"    {done+n_done:,}/{total:,} affiliations classified "
                    f"({rate:.0f}/s)")
    n_resolved = con.execute(
        "SELECT COUNT(*) FROM affiliation_country WHERE iso2 IS NOT NULL").fetchone()[0]
    logger.info(f"    affiliation_country: +{n_done:,} rows in {time.perf_counter()-t0:.0f}s "
                f"({n_resolved:,} resolved to a country overall)")


def build_article_country_count(con: sqlite3.Connection, logger: logging.Logger) -> None:
    existing = con.execute("SELECT COUNT(*) FROM article_country_count").fetchone()[0]
    if existing:
        logger.info(f"  article_country_count already populated ({existing:,} rows) -- skipping.")
        return
    logger.info("  Building article_country_count (single server-side GROUP BY, "
                "integer-only join)...")
    t0 = time.perf_counter()
    con.execute("""
        INSERT INTO article_country_count
        SELECT aaf.article_id, COUNT(DISTINCT ac.iso2) AS n_countries
        FROM author_affiliation aaf
        JOIN affiliation_country ac ON ac.affiliation_id = aaf.affiliation_id
        WHERE ac.iso2 IS NOT NULL
        GROUP BY aaf.article_id
    """)
    con.commit()
    n = con.execute("SELECT COUNT(*) FROM article_country_count").fetchone()[0]
    logger.info(f"    article_country_count: {n:,} articles, {time.perf_counter()-t0:.0f}s")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Precompute author_productivity, affiliation_country, and "
                     "article_country_count for notebooks/06_gender_analysis.ipynb.",
        formatter_class=argparse.RawDescriptionHelpFormatter, epilog=__doc__,
    )
    parser.add_argument("--db", default="pmc_fair.db")
    parser.add_argument("--reset", action="store_true")
    parser.add_argument("--batch-size", type=int, default=50_000)
    args = parser.parse_args()

    if args.batch_size <= 0:
        sys.exit(f"--batch-size must be positive, got {args.batch_size}")

    db_path = Path(args.db)
    log_path = db_path.with_suffix(".gender_extras.log")
    if not db_path.exists():
        sys.exit(f"Database not found: {db_path}")

    logger = setup_logging(log_path)
    t0 = time.perf_counter()

    logger.info("=" * 62)
    logger.info("Gender-Analysis Extras Precomputation  -  FAIR Study")
    logger.info("=" * 62)
    logger.info(f"  Database : {db_path}")
    logger.info("")

    con = sqlite3.connect(db_path)
    con.execute("PRAGMA journal_mode = WAL")
    con.execute("PRAGMA synchronous  = NORMAL")
    con.execute("PRAGMA cache_size   = -1000000")  # ~1GB page cache for this connection

    if args.reset:
        for tbl in RESET_TABLES:
            con.execute(f"DROP TABLE IF EXISTS {tbl}")
        con.commit()

    ensure_schema(con, logger)
    build_author_productivity(con, logger)
    build_affiliation_country(con, logger, args.batch_size)
    build_article_country_count(con, logger)

    logger.info("")
    logger.info("=" * 62)
    logger.info("PRECOMPUTATION COMPLETE")
    logger.info(f"  Wall-clock time : {(time.perf_counter()-t0)/60:.1f} min")
    con.close()


if __name__ == "__main__":
    main()
