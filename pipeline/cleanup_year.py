"""
cleanup_year.py
----------------
Utility: cleanly remove every row belonging to one publication year from
pmc_fair.db before re-fetching and re-parsing that year from scratch.

Why this exists
----------------
02_parse_to_sqlite.py's insert_article() reuses the existing article_id when
a pmc_id is already present, rather than re-inserting or updating it. Two of
its child tables -- da_link and supplementary_material -- have no UNIQUE
constraint at all, so re-parsing a year's full XML on top of a database that
already has some of that year's articles in it will silently DUPLICATE those
two tables' rows for every already-present article (accession_number is safe
either way -- it has a real UNIQUE(article_id,db_name,accession) constraint
with INSERT OR IGNORE).

Deleting a year's rows first and then letting fetch+parse repopulate it from
nothing avoids that duplication and gives you one clean pass, exactly like
every other year got the first time.

What this does NOT touch
-------------------------
Shared/master tables that other years' articles also reference are never
deleted from, only unlinked: journal, author, affiliation, keyword (their
get_or_create() lookups in 02_parse_to_sqlite.py will just find-or-reuse the
existing row again on re-parse -- no duplication risk there, since those
tables DO have UNIQUE constraints on their natural key).
fuji_assessment / fuji_metric are also left alone: they're keyed by URL, not
article, and may be shared with other years' articles citing the same
dataset -- only this year's fuji_article_link rows (the join table) are
removed.

Deletion order (children before parents, so no FK violations regardless of
whether `PRAGMA foreign_keys` is on in the calling session):
  ethics_sub_type -> ethics_statement -> fuji_article_link ->
  repo_classification -> da_link -> data_availability -> das_category ->
  supplementary_material -> accession_number -> article_mesh ->
  article_discipline -> article_funder -> article_keyword ->
  author_affiliation -> article_author -> link_rot -> article

Usage
-----
  # Dry run (default) -- shows what WOULD be deleted, changes nothing:
  python cleanup_year.py --db ../data/processed/pmc_fair.db --year 2018

  # Actually delete (only after reviewing the dry-run output):
  python cleanup_year.py --db ../data/processed/pmc_fair.db --year 2018 --execute

Always back up pmc_fair.db before running with --execute.
"""

import argparse
import sqlite3
import sys
import time
from pathlib import Path

# (child_table, join_condition_to_article_ids_cte)
# Every condition assumes a CTE named target_articles(article_id) exists,
# listing exactly the article_id values for the target year.
DELETE_STEPS = [
    ("ethics_sub_type",
     "ethics_id IN (SELECT ethics_id FROM ethics_statement "
     "WHERE article_id IN (SELECT article_id FROM target_articles))"),
    ("ethics_statement",
     "article_id IN (SELECT article_id FROM target_articles)"),
    ("fuji_article_link",
     "article_id IN (SELECT article_id FROM target_articles)"),
    ("repo_classification",
     "link_id IN (SELECT dl.link_id FROM da_link dl "
     "JOIN data_availability da ON da.da_id = dl.da_id "
     "WHERE da.article_id IN (SELECT article_id FROM target_articles))"),
    ("da_link",
     "da_id IN (SELECT da_id FROM data_availability "
     "WHERE article_id IN (SELECT article_id FROM target_articles))"),
    ("data_availability",
     "article_id IN (SELECT article_id FROM target_articles)"),
    ("das_category",
     "article_id IN (SELECT article_id FROM target_articles)"),
    ("supplementary_material",
     "article_id IN (SELECT article_id FROM target_articles)"),
    ("accession_number",
     "article_id IN (SELECT article_id FROM target_articles)"),
    ("article_mesh",
     "article_id IN (SELECT article_id FROM target_articles)"),
    ("article_discipline",
     "article_id IN (SELECT article_id FROM target_articles)"),
    ("article_funder",
     "article_id IN (SELECT article_id FROM target_articles)"),
    ("article_keyword",
     "article_id IN (SELECT article_id FROM target_articles)"),
    ("author_affiliation",
     "article_id IN (SELECT article_id FROM target_articles)"),
    ("article_author",
     "article_id IN (SELECT article_id FROM target_articles)"),
    ("link_rot",
     "article_id IN (SELECT article_id FROM target_articles)"),
    ("article",
     "article_id IN (SELECT article_id FROM target_articles)"),
]

# Tables worth reporting a before/after count for even though we don't
# delete from them directly (sanity check that master tables are untouched).
UNTOUCHED_SANITY_TABLES = ["journal", "author", "affiliation", "keyword",
                           "fuji_assessment", "fuji_metric"]


def count(con, table, where=None):
    sql = f"SELECT COUNT(*) FROM {table}"
    if where:
        sql += f" WHERE {where}"
    return con.execute(sql).fetchone()[0]


def main():
    ap = argparse.ArgumentParser(
        description="Delete all rows for one publication year, in dependency order, "
                    "so that year can be cleanly re-fetched and re-parsed.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    ap.add_argument("--db", required=True, help="Path to pmc_fair.db")
    ap.add_argument("--year", type=int, required=True,
                     help="Publication year to delete (matches article.year)")
    ap.add_argument("--execute", action="store_true",
                     help="Actually commit the deletion. Without this flag, "
                          "only a dry-run report is printed and nothing changes.")
    args = ap.parse_args()

    db_path = Path(args.db)
    if not db_path.exists():
        sys.exit(f"Database not found: {db_path}")

    con = sqlite3.connect(db_path)
    con.execute("PRAGMA foreign_keys = OFF")  # we order deletes ourselves
    con.row_factory = sqlite3.Row

    n_target = con.execute(
        "SELECT COUNT(*) FROM article WHERE year = ?", (args.year,)
    ).fetchone()[0]
    if n_target == 0:
        print(f"No articles with year = {args.year} found in {db_path}. Nothing to do.")
        return

    print(f"Database        : {db_path}")
    print(f"Target year     : {args.year}")
    print(f"Articles to remove: {n_target:,}")
    print(f"Mode            : {'EXECUTE (will commit)' if args.execute else 'DRY RUN (no changes)'}")
    print()

    # Build the one-time scratch CTE as a temp table so every step can reuse
    # it cheaply instead of re-evaluating "article.year = ?" per step.
    con.execute("DROP TABLE IF EXISTS temp.target_articles")
    con.execute(
        "CREATE TEMP TABLE target_articles AS "
        "SELECT article_id FROM article WHERE year = ?", (args.year,)
    )
    con.execute("CREATE INDEX idx_tmp_target ON target_articles(article_id)")

    print(f"{'table':<24} {'before':>12} {'to delete':>12} {'after (planned)':>16}")
    print("-" * 68)

    plan = []
    for table, where in DELETE_STEPS:
        before = count(con, table)
        to_delete = count(con, table, where)
        plan.append((table, where, before, to_delete))
        print(f"{table:<24} {before:>12,} {to_delete:>12,} {before - to_delete:>16,}")

    print()
    print("Sanity check -- these should NOT change at all:")
    sanity_before = {}
    for t in UNTOUCHED_SANITY_TABLES:
        sanity_before[t] = count(con, t)
        print(f"  {t:<20} {sanity_before[t]:>12,} rows (untouched)")

    if not args.execute:
        print()
        print("Dry run only -- no changes made. Re-run with --execute to apply.")
        con.close()
        return

    print()
    print("Executing deletes in a single transaction...")
    t0 = time.perf_counter()
    try:
        con.execute("BEGIN")
        for table, where, before, to_delete in plan:
            cur = con.execute(f"DELETE FROM {table} WHERE {where}")
            if cur.rowcount != to_delete:
                raise RuntimeError(
                    f"{table}: planned to delete {to_delete:,} rows but "
                    f"actually deleted {cur.rowcount:,} -- aborting, rolling back."
                )
            print(f"  {table:<24} deleted {cur.rowcount:,} rows")
        con.commit()
    except Exception:
        con.rollback()
        print("ERROR -- transaction rolled back, database unchanged.")
        raise
    elapsed = time.perf_counter() - t0
    print(f"\nCommitted in {elapsed:.1f}s.")

    print()
    print("Post-delete verification:")
    remaining = count(con, "article", f"year = {args.year}")
    print(f"  article rows with year={args.year} remaining: {remaining:,} (should be 0)")
    for t in UNTOUCHED_SANITY_TABLES:
        after = count(con, t)
        tag = "OK" if after == sanity_before[t] else "CHANGED -- investigate!"
        print(f"  {t:<20} {after:>12,} rows  [{tag}]")

    con.execute("VACUUM" if False else "PRAGMA optimize")  # cheap, safe; no full VACUUM here
    con.close()
    print("\nDone. 2018 is now cleanly removed -- re-run the pipeline from Stage 1 for this year.")


if __name__ == "__main__":
    main()
