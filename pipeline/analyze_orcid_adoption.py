"""
analyze_orcid_adoption.py
--------------------------
Reporting utility for the FAIR longitudinal study pipeline.

ORCID is already parsed by 02_parse_to_sqlite.py into author.orcid (see
parse_article(), which reads <contrib-id contrib-id-type="orcid">) -- no
new XML extraction is needed for this. This script just reports ORCID
coverage over time: author-level persistent-identifier adoption is a
direct, author-side analogue to the dataset-level persistent-identifier
flag already reported via repo_classification.has_persistent_id, and is
a clean, zero-extraction-cost FAIR (Findability/F1) signal.

Usage
-----
  python analyze_orcid_adoption.py --db data/processed/pmc_fair.db
  python analyze_orcid_adoption.py --db data/processed/pmc_fair.db --csv orcid_by_year.csv
"""

import argparse
import csv
import sqlite3
import sys
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(
        description="Report ORCID adoption over time from already-parsed author data.")
    parser.add_argument("--db", default="pmc_fair.db")
    parser.add_argument("--csv", default=None, help="Optional path to write per-year results as CSV")
    args = parser.parse_args()

    db_path = Path(args.db)
    if not db_path.exists():
        sys.exit(f"Database not found: {db_path}")

    con = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)

    # Overall coverage
    total, with_orcid = con.execute(
        "SELECT COUNT(*), SUM(CASE WHEN orcid IS NOT NULL AND orcid!='' THEN 1 ELSE 0 END) FROM author"
    ).fetchone()
    print(f"Overall author ORCID coverage: {with_orcid:,} / {total:,} ({with_orcid/total*100:.1f}%)")
    print()

    # Per-year coverage, two views:
    #   "any author"   -- does at least one author on the article have an ORCID?
    #   "first author" -- does the FIRST-listed author (author_position=1) have one?
    rows = con.execute(f"""
        SELECT a.year,
               COUNT(DISTINCT a.article_id) AS n_articles,
               COUNT(DISTINCT CASE WHEN au.orcid IS NOT NULL AND au.orcid!=''
                                    THEN a.article_id END) AS n_any_orcid
        FROM article a
        JOIN article_author aa ON aa.article_id = a.article_id
        JOIN author au         ON au.author_id  = aa.author_id
        WHERE a.year IS NOT NULL
        GROUP BY a.year
        ORDER BY a.year
    """).fetchall()

    print(f"{'Year':<6}{'Articles':>10}{'Any-author ORCID':>20}{'Pct':>8}")
    results = []
    for year, n_articles, n_any in rows:
        pct = n_any / n_articles * 100 if n_articles else 0
        print(f"{year:<6}{n_articles:>10,}{n_any:>20,}{pct:>7.1f}%")
        results.append((year, n_articles, n_any, round(pct, 1)))

    print("\n(first-author breakdown using article_author.author_position=1)")
    first_rows = con.execute("""
        SELECT a.year,
               COUNT(DISTINCT a.article_id) AS n_articles,
               COUNT(DISTINCT CASE WHEN au.orcid IS NOT NULL AND au.orcid!=''
                                    THEN a.article_id END) AS n_first_orcid
        FROM article a
        JOIN article_author aa ON aa.article_id = a.article_id AND aa.author_position = 1
        JOIN author au         ON au.author_id  = aa.author_id
        WHERE a.year IS NOT NULL
        GROUP BY a.year
        ORDER BY a.year
    """).fetchall()
    print(f"{'Year':<6}{'Articles':>10}{'First-author ORCID':>20}{'Pct':>8}")
    for year, n_articles, n_first in first_rows:
        pct = n_first / n_articles * 100 if n_articles else 0
        print(f"{year:<6}{n_articles:>10,}{n_first:>20,}{pct:>7.1f}%")

    if args.csv:
        with open(args.csv, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["year", "n_articles", "n_any_author_orcid", "pct_any_author_orcid"])
            writer.writerows(results)
        print(f"\nWrote {args.csv}")

    con.close()


if __name__ == "__main__":
    main()
