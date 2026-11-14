"""
extract_ethics.py
-----------------
Stage 2b of the FAIR longitudinal study pipeline.

Extracts ethics statements from PMC JATS XML files and stores them in two
new SQLite tables:

  ethics_statement  — one row per article that has an ethics section
  ethics_sub_type   — sub-section type attributes found inside the section

This script uses the same six-level priority cascade as the notebook
fair_ethics_analysis.ipynb, moving XML parsing out of the notebook and into
the pipeline where it belongs.  The notebook remains for analysis only.

Resume-safe: articles already present in ethics_statement are skipped, so
re-running after an interruption continues from where it stopped.

Usage
-----
  python extract_ethics.py --xml-dir data/raw --db data/processed/pmc_fair.db
  python extract_ethics.py --xml-dir data/raw --db pmc_fair.db --reset
  python extract_ethics.py --xml-dir data/raw --db pmc_fair.db --batch-size 5000
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

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

LOG_FMT  = "%(asctime)s  %(levelname)-7s  %(message)s"
LOG_DATE = "%Y-%m-%d %H:%M:%S"


def setup_logging(log_path: Path) -> logging.Logger:
    logger = logging.getLogger("extract_ethics")
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
# Schema
# ---------------------------------------------------------------------------

SCHEMA = """
CREATE TABLE IF NOT EXISTS ethics_statement (
    ethics_id          INTEGER PRIMARY KEY AUTOINCREMENT,
    article_id         INTEGER NOT NULL UNIQUE REFERENCES article(article_id),
    statement          TEXT,
    source             TEXT,
    word_count         INTEGER DEFAULT 0,
    sentence_count     INTEGER DEFAULT 0,
    has_irb            INTEGER DEFAULT 0,
    has_consent        INTEGER DEFAULT 0,
    has_helsinki       INTEGER DEFAULT 0,
    has_human          INTEGER DEFAULT 0,
    has_animal         INTEGER DEFAULT 0,
    has_waived         INTEGER DEFAULT 0,
    has_approval_num   INTEGER DEFAULT 0,
    has_data_anon      INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS ethics_sub_type (
    sub_id       INTEGER PRIMARY KEY AUTOINCREMENT,
    ethics_id    INTEGER NOT NULL REFERENCES ethics_statement(ethics_id),
    source_attr  TEXT,
    value        TEXT
);

CREATE INDEX IF NOT EXISTS idx_eth_article ON ethics_statement(article_id);
CREATE INDEX IF NOT EXISTS idx_eth_source  ON ethics_statement(source);
CREATE INDEX IF NOT EXISTS idx_est_ethics  ON ethics_sub_type(ethics_id);
"""

RESET_TABLES = ["ethics_sub_type", "ethics_statement"]


# ---------------------------------------------------------------------------
# XML helpers
# ---------------------------------------------------------------------------

def _xml_text(el) -> str:
    if el is None:
        return ""
    return " ".join(el.itertext()).strip()


def _xml_clean(s: str) -> str:
    return re.sub(r"\s+", " ", str(s or "")).strip()


def _collect_sec_text(el, skip_title: bool = False) -> str:
    parts = []
    for child in el:
        if skip_title and child.tag == "title":
            continue
        if child.tag in ("p", "list", "disp-quote", "def-list"):
            t = _xml_clean(_xml_text(child))
            if t:
                parts.append(t)
        elif child.tag == "sec":
            title_el = child.find("title")
            st = _xml_clean(_xml_text(title_el)) if title_el is not None else ""
            sp = [_xml_clean(_xml_text(p)) for p in child.iter("p")
                  if _xml_clean(_xml_text(p))]
            parts.append((st + ": " if st else "") + " ".join(sp))
    if not parts:
        t = _xml_clean(_xml_text(el))
        if t:
            parts.append(t)
    return " ".join(parts)


# ---------------------------------------------------------------------------
# Signal patterns (shared with fair_ethics_analysis.ipynb)
# ---------------------------------------------------------------------------

_ETHICS_ATTR_PAT = re.compile(r"ethics", re.IGNORECASE)
_ETHICS_TITLE_PAT = re.compile(
    r"\bEthics\b|\bEthical\b|\bIRB\b|institutional.{0,6}review"
    r"|informed.{0,6}consent|animal.{0,6}care|human.{0,6}subject"
    r"|\bHelsinki\b|IACUC|research.{0,6}ethics|ethics.{0,6}approval"
    # A few additional title variants seen in the real corpus but not
    # covered by the patterns above.
    r"|patient.{0,4}and.{0,4}public.{0,6}involvement|animal.{0,6}welfare"
    r"|use.{0,6}of.{0,6}animals|human.{0,6}subjects.{0,6}statement",
    re.IGNORECASE,
)
_IRB_PAT = re.compile(
    r"IRB|institutional.{0,10}review.{0,10}board|ethics.{0,10}committee"
    r"|review.{0,6}board|IACUC|animal.{0,6}care.{0,6}committee"
    r"|ethics.{0,6}approval|ethical.{0,6}approval|approved.{0,10}by",
    re.IGNORECASE,
)
_CONSENT_PAT = re.compile(
    r"informed.{0,6}consent|written.{0,6}consent|verbal.{0,6}consent"
    r"|consent.{0,6}to.{0,6}publish|patient.{0,6}consent|parental.{0,6}consent",
    re.IGNORECASE,
)
_HELSINKI_PAT = re.compile(
    r"\bHelsinki\b|ICMJE|Declaration.{0,8}of.{0,8}Helsinki",
    re.IGNORECASE,
)
_HUMAN_PAT = re.compile(
    r"human.{0,8}subject|patients?\b|participants?\b|volunteers?\b"
    r"|human.{0,8}research|clinical.{0,8}trial",
    re.IGNORECASE,
)
_ANIMAL_PAT = re.compile(
    r"\banimal\b|\bmouse\b|\bmice\b|\brat\b|\brats\b"
    r"|\bprimate\b|IACUC|\bveterinar|animal.{0,8}care|animal.{0,8}experiment",
    re.IGNORECASE,
)
_WAIVED_PAT = re.compile(
    r"\bwaived\b|not.{0,6}required|\bexempt\b|\bexempted\b"
    r"|no.{0,6}ethical.{0,6}approval|no.{0,6}approval.{0,6}required"
    # Confirmed via a sample of the real DB: "Not applicable" / "None sought"
    # are common legitimate waiver/no-approval-needed boilerplate that the
    # patterns above miss entirely.
    r"|not.{0,4}applicable|none.{0,4}sought|not.{0,4}sought",
    re.IGNORECASE,
)
_ANON_PAT = re.compile(
    r"anonymi[sz]|de.{0,4}identif|\bpseudonym|\bconfidential",
    re.IGNORECASE,
)
_APPR_NUM_PAT = re.compile(
    r"(?:approval|protocol|reference|application|registration|permit"
    r"|license)\s*(?:number|no\.?|#|ID|code)?\s*:?\s*([A-Z0-9][A-Z0-9\-/_.]{2,24})",
    re.IGNORECASE,
)


def signal_flags(text: str) -> dict:
    t = str(text or "")
    return {
        "has_irb":          int(bool(_IRB_PAT.search(t))),
        "has_consent":      int(bool(_CONSENT_PAT.search(t))),
        "has_helsinki":     int(bool(_HELSINKI_PAT.search(t))),
        "has_human":        int(bool(_HUMAN_PAT.search(t))),
        "has_animal":       int(bool(_ANIMAL_PAT.search(t))),
        "has_waived":       int(bool(_WAIVED_PAT.search(t))),
        "has_data_anon":    int(bool(_ANON_PAT.search(t))),
        "has_approval_num": int(bool(_APPR_NUM_PAT.search(t))),
        "word_count":       len(t.split()),
        "sentence_count":   max(1, len(re.findall(r"[.!?]", t))),
    }


# ---------------------------------------------------------------------------
# Ethics extractor (six-level priority cascade)
# ---------------------------------------------------------------------------

def _extract_sub_types(sec_el) -> list[tuple[str, str]]:
    results = []
    for sub in sec_el.findall("sec"):
        st = sub.get("sec-type", "")
        if st:
            results.append(("sec-type", st))
        else:
            title_el = sub.find("title")
            if title_el is not None:
                results.append(("sec-title", _xml_clean(_xml_text(title_el))))
    for p in sec_el.findall(".//p"):
        ct = p.get("content-type", "")
        if ct and _ETHICS_ATTR_PAT.search(ct):
            results.append(("p-content-type", ct))
    for nc in sec_el.findall(".//named-content"):
        ct = nc.get("content-type", "")
        if ct and _ETHICS_ATTR_PAT.search(ct):
            results.append(("named-content-type", ct))
    return results


def extract_ethics(article_el) -> tuple[str, str, list]:
    """
    Extract an ethics statement from a JATS article element.

    Returns (statement_text, source_tag, sub_types_list).
    Returns ('', '', []) if no ethics section is found.

    Priority cascade:
      1. sec[@sec-type contains 'ethics']
      2. sec whose <title> matches ethics keywords
      3. notes[@notes-type contains 'ethics']
      4. fn[@fn-type contains 'ethics']
      5. p[@content-type contains 'ethics']
      6. named-content[@content-type contains 'ethics']
    """
    containers = (
        list(article_el.findall("back"))
        + list(article_el.findall("body"))
        + list(article_el.findall("front"))
    )

    # Priority 1
    for c in containers:
        for sec in c.iter("sec"):
            st = sec.get("sec-type", "")
            if _ETHICS_ATTR_PAT.search(st):
                text = _collect_sec_text(sec, skip_title=True)
                if text:
                    return text, f"sec[@sec-type='{st}']", _extract_sub_types(sec)

    # Priority 2
    # NOTE: previously skipped any <sec> that had a non-empty sec-type at
    # all, which missed e.g. a sec-type="methods" section whose <title> is
    # literally "Ethics Statement". Priority 1 already returns on any
    # sec-type matching the ethics pattern, so by the time we reach here no
    # ethics-typed section remains unhandled -- checking the title of a
    # section with an unrelated sec-type only adds coverage, never
    # double-processes an already-matched section.
    for c in containers:
        for sec in c.iter("sec"):
            title_el = sec.find("title")
            if title_el is not None:
                tt = _xml_clean(_xml_text(title_el))
                if _ETHICS_TITLE_PAT.search(tt):
                    text = _collect_sec_text(sec, skip_title=True)
                    if text:
                        return text, f"sec/title='{tt}'", _extract_sub_types(sec)

    # Priority 3
    for notes in article_el.findall(".//notes"):
        nt = notes.get("notes-type", "")
        if _ETHICS_ATTR_PAT.search(nt):
            text = _collect_sec_text(notes, skip_title=True)
            if text:
                return text, f"notes[@notes-type='{nt}']", []

    # Priority 4
    for fn in article_el.findall(".//fn"):
        ft = fn.get("fn-type", "")
        if _ETHICS_ATTR_PAT.search(ft):
            text = _xml_clean(_xml_text(fn))
            if text:
                return text, f"fn[@fn-type='{ft}']", []

    # Priority 5
    for p in article_el.findall(".//p"):
        ct = p.get("content-type", "")
        if ct and _ETHICS_ATTR_PAT.search(ct):
            text = _xml_clean(_xml_text(p))
            if text:
                return text, f"p[@content-type='{ct}']", []

    # Priority 6
    for nc in article_el.findall(".//named-content"):
        ct = nc.get("content-type", "")
        if ct and _ETHICS_ATTR_PAT.search(ct):
            text = _xml_clean(_xml_text(nc))
            if text:
                return text, f"named-content[@content-type='{ct}']", []

    return "", "", []


# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------

def ensure_schema(con: sqlite3.Connection) -> None:
    con.executescript(SCHEMA)
    con.commit()


def build_id_lookup(con: sqlite3.Connection, logger: logging.Logger) -> dict[str, int]:
    """
    Map pmc_id / pubmed_id / doi → article_id for every article in the DB.
    Used to match XML article elements to DB rows.
    """
    lookup: dict[str, int] = {}
    for row in con.execute(
        "SELECT pmc_id, pubmed_id, doi, article_id FROM article"
    ):
        pmc_id, pubmed_id, doi, article_id = row
        if pmc_id:    lookup[str(pmc_id).strip()]    = article_id
        if pubmed_id: lookup[str(pubmed_id).strip()]  = article_id
        if doi:       lookup[str(doi).strip()]        = article_id
    logger.info(f"  ID lookup: {len(lookup):,} keys (pmc_id + pubmed_id + doi)")
    return lookup


def get_already_processed(con: sqlite3.Connection) -> set[int]:
    """Return article_ids already present in ethics_statement."""
    rows = con.execute("SELECT article_id FROM ethics_statement").fetchall()
    return {r[0] for r in rows}


# ---------------------------------------------------------------------------
# Per-file extraction
# ---------------------------------------------------------------------------

def process_xml_file(
    xml_path: Path,
    id_lookup: dict[str, int],
    already: set[int],
    con: sqlite3.Connection,
    batch_size: int,
    logger: logging.Logger,
) -> tuple[int, int, int]:
    """
    Stream one XML file with iterparse, extract ethics, insert into DB.
    Returns (inserted, skipped_no_match, skipped_existing).
    """
    inserted = skipped_no_match = skipped_existing = 0
    buf_rows: list[tuple]  = []    # ethics_statement rows
    buf_subs: list[tuple]  = []    # ethics_sub_type rows pending last insert

    def flush(buf_rows, buf_subs):
        nonlocal inserted
        cur = con.cursor()
        for params in buf_rows:
            try:
                cur.execute(
                    "INSERT OR IGNORE INTO ethics_statement "
                    "(article_id, statement, source, word_count, sentence_count,"
                    " has_irb, has_consent, has_helsinki, has_human, has_animal,"
                    " has_waived, has_approval_num, has_data_anon) "
                    "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    params,
                )
                if cur.rowcount:
                    inserted += 1
            except sqlite3.IntegrityError:
                pass
        # sub_types — need ethics_id for each article_id
        for article_id, src_attr, val in buf_subs:
            row = con.execute(
                "SELECT ethics_id FROM ethics_statement WHERE article_id=?",
                (article_id,),
            ).fetchone()
            if row:
                cur.execute(
                    "INSERT INTO ethics_sub_type (ethics_id, source_attr, value) "
                    "VALUES (?,?,?)",
                    (row[0], src_attr, val),
                )
        con.commit()
        return [], []

    try:
        for event, elem in ET.iterparse(str(xml_path), events=("end",)):
            if elem.tag != "article":
                continue

            # Match to DB article_id.
            # Scoped to front/article-meta/article-id only -- the exact same
            # scope 02_parse_to_sqlite.py reads pmc_id/pubmed_id/doi from.
            # A recursive ".//article-id" search would also pick up IDs from
            # a <related-article> cross-reference elsewhere in body/back
            # (common in corrections/commentaries), risking a match against
            # a *different* article that happens to also be in id_lookup.
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

            text, source, sub_types = extract_ethics(elem)
            elem.clear()

            if not text:
                continue

            f = signal_flags(text)
            buf_rows.append((
                # Store the full text -- word/sentence counts and signal
                # flags are computed on it above, so a truncated copy would
                # silently diverge from the counts stored alongside it.
                article_id, text, source,
                f["word_count"], f["sentence_count"],
                f["has_irb"], f["has_consent"], f["has_helsinki"],
                f["has_human"], f["has_animal"], f["has_waived"],
                f["has_approval_num"], f["has_data_anon"],
            ))
            already.add(article_id)
            for src_attr, val in sub_types:
                buf_subs.append((article_id, src_attr, val))

            if len(buf_rows) >= batch_size:
                buf_rows, buf_subs = flush(buf_rows, buf_subs)

    except ET.ParseError as exc:
        logger.error(f"  Parse error in {xml_path.name}: {exc}")

    if buf_rows:
        flush(buf_rows, buf_subs)

    return inserted, skipped_no_match, skipped_existing


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Extract ethics statements from PMC XML — pipeline Stage 2b.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python extract_ethics.py --xml-dir data/raw --db data/processed/pmc_fair.db
  python extract_ethics.py --xml-dir data/raw --db pmc_fair.db --reset
  python extract_ethics.py --xml-dir data/raw --db pmc_fair.db --batch-size 2000
        """,
    )
    parser.add_argument("--xml-dir", "-x", default=".",
        help="Directory containing pmc_YYYY.xml files (default: current dir).")
    parser.add_argument("--db", default="pmc_fair.db",
        help="SQLite database (default: pmc_fair.db).")
    parser.add_argument("--reset", action="store_true",
        help="Drop and rebuild ethics tables before extraction.")
    parser.add_argument("--batch-size", type=int, default=5_000,
        help="DB insert batch size (default: 5000).")
    args = parser.parse_args()

    db_path  = Path(args.db)
    xml_dir  = Path(args.xml_dir)
    log_path = db_path.with_suffix(".ethics.log")

    if not db_path.exists():
        sys.exit(f"Database not found: {db_path}")

    xml_files = sorted(xml_dir.glob("pmc_*.xml"))
    if not xml_files:
        sys.exit(f"No pmc_YYYY.xml files found in: {xml_dir.resolve()}")

    logger  = setup_logging(log_path)
    t0      = time.perf_counter()
    rss0    = _peak_rss_mb()

    logger.info("=" * 62)
    logger.info("Ethics Statement Extraction  —  FAIR Study (Stage 2b)")
    logger.info("=" * 62)
    logger.info(f"  XML directory  : {xml_dir.resolve()}")
    logger.info(f"  Files found    : {len(xml_files)}")
    logger.info(f"  Database       : {db_path}")
    logger.info(f"  Log            : {log_path}")
    logger.info(f"  Batch size     : {args.batch_size:,}")
    logger.info("")

    con = sqlite3.connect(db_path)
    con.execute("PRAGMA journal_mode = WAL")
    con.execute("PRAGMA cache_size   = -32768")
    con.execute("PRAGMA synchronous  = NORMAL")

    if args.reset:
        logger.info("  --reset: dropping ethics tables ...")
        for tbl in RESET_TABLES:
            con.execute(f"DROP TABLE IF EXISTS {tbl}")
        con.commit()

    ensure_schema(con)
    logger.info("  Schema ready.")

    id_lookup = build_id_lookup(con, logger)
    already   = get_already_processed(con)
    logger.info(f"  Already extracted : {len(already):,} articles")
    logger.info("")

    grand_inserted = grand_no_match = grand_existing = 0

    for idx, xml_path in enumerate(xml_files, 1):
        t_file = time.perf_counter()
        logger.info(f"[{idx}/{len(xml_files)}] {xml_path.name}")

        ins, no_match, exist = process_xml_file(
            xml_path, id_lookup, already, con,
            args.batch_size, logger,
        )
        elapsed = time.perf_counter() - t_file
        grand_inserted  += ins
        grand_no_match  += no_match
        grand_existing  += exist

        logger.info(
            f"  inserted={ins:,}  no_db_match={no_match:,}  "
            f"already_done={exist:,}  ({elapsed:.1f}s)"
        )

    run_elapsed = time.perf_counter() - t0
    peak_rss    = max(_peak_rss_mb() - rss0, 0)

    logger.info("")
    logger.info("=" * 62)
    logger.info("EXTRACTION COMPLETE")
    logger.info("=" * 62)
    logger.info(f"  Total ethics rows inserted : {grand_inserted:,}")
    logger.info(f"  Articles with no DB match  : {grand_no_match:,}")
    logger.info(f"  Articles already extracted : {grand_existing:,}")
    total_eth = con.execute(
        "SELECT COUNT(*) FROM ethics_statement"
    ).fetchone()[0]
    logger.info(f"  ethics_statement total rows: {total_eth:,}")
    logger.info(f"  Wall-clock time            : {run_elapsed/60:.1f} min")
    logger.info(f"  Peak RSS delta             : {peak_rss:.1f} MB")
    logger.info(f"  Log                        : {log_path}")

    con.close()


if __name__ == "__main__":
    main()
