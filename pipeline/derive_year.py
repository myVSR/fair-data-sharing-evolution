"""
derive_year.py
---------------
Utility stage for the PMC FAIR longitudinal study pipeline.

Computes a single, reliable publication year for every article and stores it
in a persisted, indexed `article.year` column. Every analysis notebook reads
this column directly instead of `article.pub_year` — this script is the one
and only place the derivation logic lives, and the one place to update it
(e.g. when the corpus is extended to new years) or re-run it (e.g. after a
fresh pipeline run repopulates `article` from new XML).

Only the `article.year` column is written. No other column (`pub_year`,
`epub_date`, `received_date`, `accepted_date`, ...) is ever modified.

Why not just use `article.pub_year`?
-------------------------------------
`pubmed_to_sqlite.py` (Stage 2) sets `pub_year` from whichever `<pub-date>`
node — `ppub`/`collection` or `epub` — happens to appear FIRST in a given
publisher's JATS XML (see `parse_article()`). This is not a deliberate
"print year" policy; it just means `pub_year` mixes epub-year and ppub-year
inconsistently across publishers. Measured effect: ~176k articles have
`pub_year` exactly one year after their `epub_date` — the classic
"epublished in December, filed under next year's print issue" pattern.

Derivation logic (checked in this order)
-----------------------------------------
1. **`pub_year` is the anchor.** If it's present and plausible (1900-2030):
   - If `epub_date`'s year is within ±1 of `pub_year`, use `epub_date`'s year
     instead. A gap this small is the online-first-vs-print-issue lag above,
     and `epub_date` is unambiguous (it only ever comes from a genuine
     `epub`/`epub-ppub` XML node, never from `ppub`/`collection`).
   - A bigger gap means reprinted/digitised legacy content (e.g. a 1989 book
     chapter digitised into PMC in 2014 has `epub_date` 2014 but should stay
     1989 — a DAS study cares about when the data-sharing statement reflects
     real practice, not when a scan was uploaded), so `pub_year` wins.
2. **`epub_date` alone**, if `pub_year` is missing/implausible.
3. **`accepted_date`, then `received_date`**, if both of the above are
   missing. These predate actual publication (peer review + production lag),
   so they're a weaker proxy — validated against the ~5.8M articles where we
   know the true year: `accepted_date`'s year matches exactly 89.4% of the
   time and is within ±1 year 99.7% of the time (`received_date`: 62.1% /
   98.4% — used only when `accepted_date` is also missing). This recovers
   ~21,245 of the ~25,656 articles that would otherwise have no year at all,
   of which ~13,924 carry a real Data Availability Statement that every
   year-filtered notebook query would otherwise silently drop.
4. Otherwise `year` is `NULL` (currently ~4,411 articles — no usable date of
   any kind).

Usage
-----
  python derive_year.py --db ../data/processed/pmc_fair.db
  python derive_year.py --db ../data/processed/pmc_fair.db --recompute
  python derive_year.py --db ../data/processed/pmc_fair.db --fill-missing
  python derive_year.py --db ../data/processed/pmc_fair.db --dry-run

--recompute vs --fill-missing
------------------------------
Both give the identical final result when the derivation logic above hasn't
changed -- the only difference is how many rows get touched (and therefore
how long it takes, especially on slow/network-mounted storage):

  --recompute      Re-derives year for every row in `article`, including
                    ones that already have a correct value. Use this if you
                    changed the CASE logic above and need existing rows
                    re-evaluated against the new rule.

  --fill-missing   Only derives year for rows where it is currently NULL.
                    Use this after re-fetching/re-parsing a new year of data
                    (e.g. a 2018 re-fetch) when the logic itself is
                    unchanged -- the newly inserted rows are the only ones
                    with NULL year, so this reaches the same end state as
                    --recompute while writing a small fraction of the rows.
"""

import argparse
import logging
import sqlite3
import sys
import time
from pathlib import Path

YEAR_CASE_SQL = """
    CASE
        -- 1. pub_year is the anchor when present and plausible
        WHEN pub_year IS NOT NULL AND pub_year != ''
             AND CAST(pub_year AS INTEGER) BETWEEN 1900 AND 2030
        THEN
            CASE
                -- ...but defer to epub_date when within 1 year of pub_year
                -- (online-first-vs-print-issue lag); a bigger gap means
                -- reprint/digitisation of older content, so keep pub_year
                WHEN epub_date IS NOT NULL AND epub_date != ''
                     AND CAST(substr(epub_date,1,4) AS INTEGER) BETWEEN 1900 AND 2030
                     AND ABS(CAST(substr(epub_date,1,4) AS INTEGER)
                             - CAST(pub_year AS INTEGER)) <= 1
                THEN CAST(substr(epub_date,1,4) AS INTEGER)
                ELSE CAST(pub_year AS INTEGER)
            END
        -- 2. pub_year missing/implausible: epub_date alone
        WHEN epub_date IS NOT NULL AND epub_date != ''
             AND CAST(substr(epub_date,1,4) AS INTEGER) BETWEEN 1900 AND 2030
        THEN CAST(substr(epub_date,1,4) AS INTEGER)
        -- 3. both missing: accepted_date, then received_date (weaker proxies --
        --    pre-publication dates, but validated to within +/-1yr >99% of the time)
        WHEN accepted_date IS NOT NULL AND accepted_date != ''
             AND CAST(substr(accepted_date,1,4) AS INTEGER) BETWEEN 1900 AND 2030
        THEN CAST(substr(accepted_date,1,4) AS INTEGER)
        WHEN received_date IS NOT NULL AND received_date != ''
             AND CAST(substr(received_date,1,4) AS INTEGER) BETWEEN 1900 AND 2030
        THEN CAST(substr(received_date,1,4) AS INTEGER)
        -- 4. nothing usable
        ELSE NULL
    END
"""


def setup_logging(log_path: Path) -> logging.Logger:
    logger = logging.getLogger("derive_year")
    logger.setLevel(logging.INFO)
    fmt = logging.Formatter("%(asctime)s  %(levelname)-7s  %(message)s", "%H:%M:%S")
    fh = logging.FileHandler(log_path)
    fh.setFormatter(fmt)
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    logger.addHandler(fh)
    logger.addHandler(sh)
    return logger


def summarise(con: sqlite3.Connection, logger: logging.Logger) -> None:
    total, with_year = con.execute(
        "SELECT COUNT(*), SUM(CASE WHEN year IS NOT NULL THEN 1 ELSE 0 END) FROM article"
    ).fetchone()
    tiers = con.execute("""
        SELECT
            SUM(CASE WHEN pub_year IS NOT NULL AND pub_year != ''
                          AND CAST(pub_year AS INTEGER) BETWEEN 1900 AND 2030
                     THEN 1 ELSE 0 END) AS has_pub_year,
            SUM(CASE WHEN (pub_year IS NULL OR pub_year = ''
                          OR CAST(pub_year AS INTEGER) NOT BETWEEN 1900 AND 2030)
                          AND epub_date IS NOT NULL AND epub_date != ''
                          AND CAST(substr(epub_date,1,4) AS INTEGER) BETWEEN 1900 AND 2030
                     THEN 1 ELSE 0 END) AS epub_only,
            SUM(CASE WHEN year IS NOT NULL
                          AND (pub_year IS NULL OR pub_year = ''
                               OR CAST(pub_year AS INTEGER) NOT BETWEEN 1900 AND 2030)
                          AND (epub_date IS NULL OR epub_date = ''
                               OR CAST(substr(epub_date,1,4) AS INTEGER) NOT BETWEEN 1900 AND 2030)
                     THEN 1 ELSE 0 END) AS accepted_or_received_tier,
            SUM(CASE WHEN year IS NULL THEN 1 ELSE 0 END) AS still_null
        FROM article
    """).fetchone()
    logger.info(f"article.year populated: {with_year:,} / {total:,} ({with_year/total*100:.2f}%)")
    logger.info(f"  tier 3 (accepted_date/received_date fallback): {tiers[2]:,} articles")
    logger.info(f"  still NULL (no usable date at all):            {tiers[3]:,} articles")


def main():
    parser = argparse.ArgumentParser(
        description="Derive a reliable article.year column -- Stage util for the FAIR study pipeline.",
        formatter_class=argparse.RawDescriptionHelpFormatter, epilog=__doc__)
    parser.add_argument("--db", default="pmc_fair.db", help="Path to the SQLite database")
    parser.add_argument("--recompute", action="store_true",
                         help="Re-run the UPDATE for ALL rows, even already-populated "
                              "ones (use after changing the derivation logic above). "
                              "Touches the whole table -- slow on network-mounted "
                              "storage; use --fill-missing instead if the logic hasn't "
                              "changed and you only added new rows.")
    parser.add_argument("--fill-missing", action="store_true",
                         help="Only compute year for rows where it is currently NULL "
                              "(e.g. after re-fetching/re-parsing a new year of data, "
                              "when the derivation logic itself hasn't changed). Gives "
                              "the exact same result as --recompute but touches far "
                              "fewer rows, so it's much faster on slow/network-mounted "
                              "storage.")
    parser.add_argument("--dry-run", action="store_true",
                         help="Report what would happen without writing anything")
    parser.add_argument("--log", default="derive_year.log", help="Log file path")
    args = parser.parse_args()

    logger = setup_logging(Path(args.log))
    logger.info("derive_year.py -- PMC FAIR study pipeline")

    db_path = Path(args.db)
    if not db_path.exists():
        logger.error(f"Database not found: {db_path}")
        sys.exit(1)

    con = sqlite3.connect(db_path)
    cols = {r[1] for r in con.execute("PRAGMA table_info(article)").fetchall()}

    where_clause = ""

    if "year" not in cols:
        if args.dry_run:
            logger.info("[dry-run] Would ALTER TABLE article ADD COLUMN year INTEGER, "
                        "backfill, and index it.")
            return
        logger.info("article.year does not exist -- adding it (one-time schema change)")
        con.execute("ALTER TABLE article ADD COLUMN year INTEGER")
        con.commit()
        do_backfill = True
    elif args.recompute:
        do_backfill = True
        logger.info("article.year already exists -- --recompute passed, re-running backfill for ALL rows")
    elif args.fill_missing:
        do_backfill = True
        where_clause = " WHERE year IS NULL"
        n_missing = con.execute("SELECT COUNT(*) FROM article WHERE year IS NULL").fetchone()[0]
        logger.info(f"article.year already exists -- --fill-missing passed, "
                    f"backfilling only the {n_missing:,} row(s) where year IS NULL")
    else:
        logger.info("article.year already exists -- skipping backfill (pass --recompute or --fill-missing to force)")
        do_backfill = False

    if do_backfill:
        if args.dry_run:
            scope = "rows where year IS NULL" if where_clause else "all rows"
            logger.info(f"[dry-run] Would UPDATE article SET year = <derivation logic> for {scope}.")
        else:
            t0 = time.time()
            con.execute(f"UPDATE article SET year = {YEAR_CASE_SQL}{where_clause}")
            con.commit()
            scope = "missing (NULL) rows only" if where_clause else "all rows"
            logger.info(f"Backfilled article.year for {scope} in {time.time()-t0:.1f}s")

    if not args.dry_run:
        con.execute("CREATE INDEX IF NOT EXISTS idx_article_year2 ON article(year)")
        con.commit()
        logger.info("Index idx_article_year2 on article(year) ready")
        summarise(con, logger)

    con.close()
    logger.info("Done.")


if __name__ == "__main__":
    main()
