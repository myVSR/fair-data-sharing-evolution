"""
extract_author_contributions.py
---------------------------------
Utility stage for the FAIR longitudinal study pipeline.

Extracts author-contribution information from PMC JATS XML in two forms:

  1. STRUCTURED CRediT roles -- <contrib><role vocab-term="..." ...>...</role>
     elements, per the JATS4R CRediT tagging recommendation. One row per
     (article, author_position, role) in `author_contribution`, with
     author_position computed the exact same way 02_parse_to_sqlite.py
     does (enumerate over contrib[@contrib-type='author'] in document
     order), so it joins cleanly against the existing article_author
     table.

  2. FREE-TEXT "Author Contributions" statement -- most PMC-OA articles
     predate or don't use structured CRediT tagging at all; this falls
     back to the same six-level sec-type/title/notes/fn cascade as
     extract_ethics.py / extract_code_availability.py, storing the whole
     statement (not split per-author -- that would need NLP, out of
     scope here) in `author_contribution_statement`.

These are independent signals: an article can have (1) only, (2) only,
both, or neither. `uses_credit` on the statement table flags whether the
free-text statement itself mentions "CRediT" (a weak proxy for "this
journal/article has adopted the CRediT taxonomy even without structured
markup").

Note on scope: this is lower priority than the ethics/code/DAS
extractors for a FAIR data-sharing study -- it's more directly useful for
an authorship/credit-attribution analysis than for FAIR itself. Built on
request alongside the clinical-trial-registration extractor.

Resume-safe: articles already present in EITHER output table are skipped
for that table independently.

Usage
-----
  python extract_author_contributions.py --xml-dir data/raw --db data/processed/pmc_fair.db
  python extract_author_contributions.py --xml-dir data/raw --db pmc_fair.db --reset
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
    logger = logging.getLogger("extract_author_contributions")
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
CREATE TABLE IF NOT EXISTS author_contribution (
    contrib_id      INTEGER PRIMARY KEY AUTOINCREMENT,
    article_id      INTEGER NOT NULL REFERENCES article(article_id),
    author_position INTEGER,          -- aligns with article_author.author_position
    role_text       TEXT NOT NULL,
    role_vocab_term TEXT,             -- normalised CRediT term when vocab-term attr present
    UNIQUE(article_id, author_position, role_text)
);
CREATE INDEX IF NOT EXISTS idx_ac_article ON author_contribution(article_id);

CREATE TABLE IF NOT EXISTS author_contribution_statement (
    acs_id      INTEGER PRIMARY KEY AUTOINCREMENT,
    article_id  INTEGER NOT NULL UNIQUE REFERENCES article(article_id),
    statement   TEXT,
    source      TEXT,
    uses_credit INTEGER DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_acs_article ON author_contribution_statement(article_id);
"""

RESET_TABLES = ["author_contribution", "author_contribution_statement"]


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
# 1. Structured CRediT roles
# ---------------------------------------------------------------------------

def extract_structured_roles(article_el) -> list[tuple[int, str, str | None]]:
    """Returns [(author_position, role_text, role_vocab_term_or_None), ...].
    author_position mirrors 02_parse_to_sqlite.py's own enumerate() over the
    exact same contrib[@contrib-type='author'] selector, in document order."""
    ameta = article_el.find("front/article-meta")
    if ameta is None:
        return []
    results = []
    for pos, contrib in enumerate(ameta.findall(".//contrib[@contrib-type='author']"), 1):
        for role_el in contrib.findall("role"):
            role_text = _xml_clean(_xml_text(role_el))
            if not role_text:
                continue
            vocab_term = (role_el.get("vocab-term") or role_el.get("content-type") or None)
            results.append((pos, role_text, vocab_term))
    return results


# ---------------------------------------------------------------------------
# 2. Free-text "Author Contributions" statement (six-level cascade)
# ---------------------------------------------------------------------------

_CONTRIB_PAT = re.compile(
    r"author.{0,4}contribution|contribution.{0,4}of.{0,4}authors?"
    r"|authors?.{0,4}role|CRediT|contributor.{0,4}role",
    re.IGNORECASE,
)
_CREDIT_PAT = re.compile(r"CRediT", re.IGNORECASE)


def extract_contribution_statement(article_el) -> tuple[str, str]:
    """Returns (statement_text, source_tag). ('', '') if not found."""
    containers = list(article_el.findall("back")) + list(article_el.findall("body"))

    for c in containers:
        for sec in c.iter("sec"):
            st = sec.get("sec-type", "")
            if _CONTRIB_PAT.search(st):
                text = _collect_sec_text(sec, skip_title=True)
                if text:
                    return text, f"sec[@sec-type='{st}']"

    for c in containers:
        for sec in c.iter("sec"):
            title_el = sec.find("title")
            if title_el is not None:
                tt = _xml_clean(_xml_text(title_el))
                if _CONTRIB_PAT.search(tt):
                    text = _collect_sec_text(sec, skip_title=True)
                    if text:
                        return text, f"sec/title='{tt}'"

    for notes in article_el.findall(".//notes"):
        nt = notes.get("notes-type", "")
        if _CONTRIB_PAT.search(nt):
            text = _collect_sec_text(notes, skip_title=True)
            if text:
                return text, f"notes[@notes-type='{nt}']"

    for fn in article_el.findall(".//fn"):
        ft = fn.get("fn-type", "")
        if _CONTRIB_PAT.search(ft):
            text = _xml_clean(_xml_text(fn))
            if text:
                return text, f"fn[@fn-type='{ft}']"

    for p in article_el.findall(".//p"):
        ct = p.get("content-type", "")
        if ct and _CONTRIB_PAT.search(ct):
            text = _xml_clean(_xml_text(p))
            if text:
                return text, f"p[@content-type='{ct}']"

    for nc in article_el.findall(".//named-content"):
        ct = nc.get("content-type", "")
        if ct and _CONTRIB_PAT.search(ct):
            text = _xml_clean(_xml_text(nc))
            if text:
                return text, f"named-content[@content-type='{ct}']"

    return "", ""


# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------

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


def process_xml_file(
    xml_path: Path,
    id_lookup: dict[str, int],
    already_roles: set[int],
    already_stmt: set[int],
    con: sqlite3.Connection,
    batch_size: int,
    logger: logging.Logger,
) -> tuple[int, int, int, int]:
    """Returns (n_structured, n_statement, skipped_no_match, skipped_existing_both)."""
    n_structured = n_statement = skipped_no_match = skipped_existing = 0
    buf_roles: list[tuple] = []
    buf_stmt:  list[tuple] = []

    def flush(buf_roles, buf_stmt):
        cur = con.cursor()
        for params in buf_roles:
            cur.execute(
                "INSERT OR IGNORE INTO author_contribution "
                "(article_id, author_position, role_text, role_vocab_term) VALUES (?,?,?,?)",
                params,
            )
        for params in buf_stmt:
            cur.execute(
                "INSERT OR IGNORE INTO author_contribution_statement "
                "(article_id, statement, source, uses_credit) VALUES (?,?,?,?)",
                params,
            )
        con.commit()
        return [], []

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
            if article_id in already_roles and article_id in already_stmt:
                skipped_existing += 1
                elem.clear()
                continue

            if article_id not in already_roles:
                roles = extract_structured_roles(elem)
                if roles:
                    n_structured += 1
                    for pos, role_text, vocab_term in roles:
                        buf_roles.append((article_id, pos, role_text, vocab_term))
                already_roles.add(article_id)

            if article_id not in already_stmt:
                text, source = extract_contribution_statement(elem)
                if text:
                    n_statement += 1
                    uses_credit = int(bool(_CREDIT_PAT.search(text)))
                    buf_stmt.append((article_id, text, source, uses_credit))
                already_stmt.add(article_id)

            elem.clear()

            if len(buf_roles) >= batch_size or len(buf_stmt) >= batch_size:
                buf_roles, buf_stmt = flush(buf_roles, buf_stmt)

    except ET.ParseError as exc:
        logger.error(f"  Parse error in {xml_path.name}: {exc}")

    if buf_roles or buf_stmt:
        flush(buf_roles, buf_stmt)

    return n_structured, n_statement, skipped_no_match, skipped_existing


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Extract CRediT roles / author-contribution statements from PMC XML.",
        formatter_class=argparse.RawDescriptionHelpFormatter, epilog=__doc__,
    )
    parser.add_argument("--xml-dir", "-x", default=".")
    parser.add_argument("--db", default="pmc_fair.db")
    parser.add_argument("--reset", action="store_true")
    parser.add_argument("--batch-size", type=int, default=5_000)
    args = parser.parse_args()

    db_path  = Path(args.db)
    xml_dir  = Path(args.xml_dir)
    log_path = db_path.with_suffix(".author_contributions.log")

    if not db_path.exists():
        sys.exit(f"Database not found: {db_path}")
    xml_files = sorted(xml_dir.glob("pmc_*.xml"))
    if not xml_files:
        sys.exit(f"No pmc_YYYY.xml files found in: {xml_dir.resolve()}")

    logger = setup_logging(log_path)
    t0     = time.perf_counter()

    logger.info("=" * 62)
    logger.info("Author Contribution / CRediT Extraction  —  FAIR Study")
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

    id_lookup     = build_id_lookup(con, logger)
    already_roles = {r[0] for r in con.execute(
        "SELECT DISTINCT article_id FROM author_contribution").fetchall()}
    already_stmt  = {r[0] for r in con.execute(
        "SELECT article_id FROM author_contribution_statement").fetchall()}
    logger.info(f"  Already processed (structured roles) : {len(already_roles):,}")
    logger.info(f"  Already processed (free-text stmt)   : {len(already_stmt):,}")
    logger.info("")

    grand_struct = grand_stmt = grand_no_match = grand_existing = 0

    for idx, xml_path in enumerate(xml_files, 1):
        t_file = time.perf_counter()
        n_struct, n_stmt, no_match, exist = process_xml_file(
            xml_path, id_lookup, already_roles, already_stmt, con, args.batch_size, logger,
        )
        elapsed = time.perf_counter() - t_file
        grand_struct   += n_struct
        grand_stmt     += n_stmt
        grand_no_match += no_match
        grand_existing += exist
        logger.info(
            f"[{idx}/{len(xml_files)}] {xml_path.name}: "
            f"structured={n_struct:,}  free_text={n_stmt:,}  "
            f"no_db_match={no_match:,}  already_done={exist:,}  ({elapsed:.1f}s)"
        )

    logger.info("")
    logger.info("=" * 62)
    logger.info("EXTRACTION COMPLETE")
    logger.info(f"  Articles with structured CRediT roles : {grand_struct:,}")
    logger.info(f"  Articles with free-text contrib stmt  : {grand_stmt:,}")
    logger.info(f"  No DB match                           : {grand_no_match:,}")
    logger.info(f"  Wall-clock time                       : {(time.perf_counter()-t0)/60:.1f} min")
    con.close()


if __name__ == "__main__":
    main()
