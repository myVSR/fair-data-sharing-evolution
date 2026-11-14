"""
extract_trial_registrations.py
--------------------------------
Utility stage for the FAIR longitudinal study pipeline.

Extracts clinical-trial registry identifiers (ClinicalTrials.gov NCT IDs
and nine other major registries) mentioned anywhere in an article's text
-- abstract, methods, a dedicated "Trial registration" section, or
anywhere else -- and stores them in a new `trial_registration` table.

Unlike ethics/DAS/code-availability statements, trial registration
numbers are NOT extracted via a sec-type/title cascade: they commonly
appear inline in the abstract with no dedicated section at all, so this
scans the full visible text of the whole <article> element instead.
Each registry's ID format is distinctive enough (a fixed literal prefix
plus a fixed digit count) that false positives are a non-issue without
needing section-level anchoring -- the one exception, EudraCT numbers
(plain YYYY-NNNNNN-CC), is deliberately NOT covered here since that
format alone is not distinctive enough to avoid false positives on other
numeric identifiers in the same text.

An article can report zero, one, or (systematic reviews / meta-analyses)
many distinct trial registrations; this stores one row per
(article_id, registry, trial_id) found.

Resume-safe: articles already present in trial_registration (including a
'none' sentinel row for articles with no trial ID found) are skipped.

Usage
-----
  python extract_trial_registrations.py --xml-dir data/raw --db data/processed/pmc_fair.db
  python extract_trial_registrations.py --xml-dir data/raw --db pmc_fair.db --reset
"""

import argparse
import logging
import re
import resource
import sqlite3
import sys
import time
import xml.etree.ElementTree as ET
from pathlib import Path

LOG_FMT  = "%(asctime)s  %(levelname)-7s  %(message)s"
LOG_DATE = "%H:%M:%S"


def setup_logging(log_path: Path) -> logging.Logger:
    logger = logging.getLogger("extract_trial_registrations")
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


def _peak_rss_mb() -> float:
    try:
        kb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        return kb / 1024 if sys.platform != "darwin" else kb / (1024 * 1024)
    except Exception:
        return 0.0


SCHEMA = """
CREATE TABLE IF NOT EXISTS trial_registration (
    trial_reg_id INTEGER PRIMARY KEY AUTOINCREMENT,
    article_id   INTEGER NOT NULL REFERENCES article(article_id),
    registry     TEXT NOT NULL,   -- e.g. 'ClinicalTrials.gov', 'ISRCTN', 'none'
    trial_id     TEXT,            -- NULL for the 'none' sentinel row
    UNIQUE(article_id, registry, trial_id)
);
CREATE INDEX IF NOT EXISTS idx_tr_article  ON trial_registration(article_id);
CREATE INDEX IF NOT EXISTS idx_tr_registry ON trial_registration(registry);
"""

RESET_TABLES = ["trial_registration"]

# Each pattern's own literal prefix makes it distinctive enough that no
# section-level anchoring is needed (see module docstring re: EudraCT,
# deliberately excluded for the opposite reason).
TRIAL_PATTERNS = [
    (re.compile(r"\bNCT\d{8}\b"),                         "ClinicalTrials.gov"),
    (re.compile(r"\bISRCTN\d{8}\b", re.I),                "ISRCTN"),
    (re.compile(r"\bChiCTR-?[A-Z]{0,4}-?\d{6,10}\b", re.I), "ChiCTR"),
    (re.compile(r"\bACTRN\d{14}\b", re.I),                "ACTRN"),
    (re.compile(r"\bDRKS\d{8}\b", re.I),                  "DRKS"),
    (re.compile(r"\bUMIN0*\d{9}\b", re.I),                "UMIN-CTR"),
    (re.compile(r"\bCTRI/\d{4}/\d{2,3}/\d{6}\b", re.I),   "CTRI"),
    (re.compile(r"\bPACTR\d{15}\b", re.I),                "PACTR"),
    (re.compile(r"\bIRCT\d{8,20}N\d+\b", re.I),           "IRCT"),
    (re.compile(r"\bCRD4\d{7}\b"),                        "PROSPERO"),
]


def _xml_text(el) -> str:
    if el is None:
        return ""
    return " ".join(el.itertext())


def extract_trial_ids(article_el) -> list[tuple[str, str]]:
    """Scan the whole article's visible text for trial-registry IDs.
    Returns a deduplicated list of (registry, trial_id)."""
    text = _xml_text(article_el)
    seen: set[tuple[str, str]] = set()
    results: list[tuple[str, str]] = []
    for pat, registry in TRIAL_PATTERNS:
        for m in pat.finditer(text):
            trial_id = m.group(0).upper()
            key = (registry, trial_id)
            if key not in seen:
                seen.add(key)
                results.append(key)
    return results


def ensure_schema(con: sqlite3.Connection) -> None:
    con.executescript(SCHEMA)
    con.commit()


def build_id_lookup(con: sqlite3.Connection, logger: logging.Logger) -> dict[str, int]:
    lookup: dict[str, int] = {}
    for pmc_id, pubmed_id, doi, article_id in con.execute(
        "SELECT pmc_id, pubmed_id, doi, article_id FROM article"
    ):
        if pmc_id:    lookup[str(pmc_id).strip()]    = article_id
        if pubmed_id: lookup[str(pubmed_id).strip()] = article_id
        if doi:       lookup[str(doi).strip()]       = article_id
    logger.info(f"  ID lookup: {len(lookup):,} keys (pmc_id + pubmed_id + doi)")
    return lookup


def get_already_processed(con: sqlite3.Connection) -> set[int]:
    return {r[0] for r in con.execute("SELECT DISTINCT article_id FROM trial_registration").fetchall()}


def process_xml_file(
    xml_path: Path,
    id_lookup: dict[str, int],
    already: set[int],
    con: sqlite3.Connection,
    batch_size: int,
    logger: logging.Logger,
) -> tuple[int, int, int, int]:
    """Returns (articles_with_trial, articles_with_none, skipped_no_match, skipped_existing)."""
    n_with_trial = n_none = skipped_no_match = skipped_existing = 0
    buf: list[tuple] = []

    def flush(buf):
        cur = con.cursor()
        for params in buf:
            cur.execute(
                "INSERT OR IGNORE INTO trial_registration (article_id, registry, trial_id) "
                "VALUES (?,?,?)", params,
            )
        con.commit()
        return []

    try:
        for event, elem in ET.iterparse(str(xml_path), events=("end",)):
            if elem.tag != "article":
                continue

            article_id = None
            ameta = elem.find("front/article-meta")
            id_els = ameta.findall("article-id") if ameta is not None else []
            for id_el in id_els:
                id_type = (id_el.get("pub-id-type", "") or "").lower()
                id_val  = (id_el.text or "").strip()
                if id_type in ("pmc", "pmid", "doi") and id_val:
                    article_id = id_lookup.get(id_val)
                    if article_id:
                        break

            if not article_id:
                skipped_no_match += 1
                elem.clear()
                continue
            if article_id in already:
                skipped_existing += 1
                elem.clear()
                continue

            trials = extract_trial_ids(elem)
            elem.clear()

            if trials:
                n_with_trial += 1
                for registry, trial_id in trials:
                    buf.append((article_id, registry, trial_id))
            else:
                n_none += 1
                buf.append((article_id, "none", None))

            already.add(article_id)
            if len(buf) >= batch_size:
                buf = flush(buf)

    except ET.ParseError as exc:
        logger.error(f"  Parse error in {xml_path.name}: {exc}")

    if buf:
        flush(buf)

    return n_with_trial, n_none, skipped_no_match, skipped_existing


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Extract clinical-trial registration numbers from PMC XML.",
        formatter_class=argparse.RawDescriptionHelpFormatter, epilog=__doc__,
    )
    parser.add_argument("--xml-dir", "-x", default=".")
    parser.add_argument("--db", default="pmc_fair.db")
    parser.add_argument("--reset", action="store_true")
    parser.add_argument("--batch-size", type=int, default=5_000)
    args = parser.parse_args()

    db_path  = Path(args.db)
    xml_dir  = Path(args.xml_dir)
    log_path = db_path.with_suffix(".trial_registration.log")

    if not db_path.exists():
        sys.exit(f"Database not found: {db_path}")
    xml_files = sorted(xml_dir.glob("pmc_*.xml"))
    if not xml_files:
        sys.exit(f"No pmc_YYYY.xml files found in: {xml_dir.resolve()}")

    logger = setup_logging(log_path)
    t0     = time.perf_counter()

    logger.info("=" * 62)
    logger.info("Clinical Trial Registration Extraction  —  FAIR Study")
    logger.info("=" * 62)
    logger.info(f"  XML directory : {xml_dir.resolve()}")
    logger.info(f"  Files found   : {len(xml_files)}")
    logger.info(f"  Database      : {db_path}")
    logger.info("")

    con = sqlite3.connect(db_path)
    con.execute("PRAGMA journal_mode = WAL")
    con.execute("PRAGMA synchronous  = NORMAL")

    if args.reset:
        for tbl in RESET_TABLES:
            con.execute(f"DROP TABLE IF EXISTS {tbl}")
        con.commit()

    ensure_schema(con)

    id_lookup = build_id_lookup(con, logger)
    already   = get_already_processed(con)
    logger.info(f"  Already processed : {len(already):,} articles")
    logger.info("")

    grand_trial = grand_none = grand_no_match = grand_existing = 0

    for idx, xml_path in enumerate(xml_files, 1):
        t_file = time.perf_counter()
        n_trial, n_none, no_match, exist = process_xml_file(
            xml_path, id_lookup, already, con, args.batch_size, logger,
        )
        elapsed = time.perf_counter() - t_file
        grand_trial    += n_trial
        grand_none     += n_none
        grand_no_match += no_match
        grand_existing += exist
        logger.info(
            f"[{idx}/{len(xml_files)}] {xml_path.name}: "
            f"with_trial={n_trial:,}  none={n_none:,}  "
            f"no_db_match={no_match:,}  already_done={exist:,}  ({elapsed:.1f}s)"
        )

    logger.info("")
    logger.info("=" * 62)
    logger.info("EXTRACTION COMPLETE")
    logger.info(f"  Articles with >=1 trial ID : {grand_trial:,}")
    logger.info(f"  Articles with none found   : {grand_none:,}")
    logger.info(f"  No DB match                : {grand_no_match:,}")
    total_rows = con.execute(
        "SELECT COUNT(*) FROM trial_registration WHERE registry != 'none'"
    ).fetchone()[0]
    logger.info(f"  Total trial-ID rows        : {total_rows:,}")
    logger.info(f"  Wall-clock time            : {(time.perf_counter()-t0)/60:.1f} min")
    con.close()


if __name__ == "__main__":
    main()
