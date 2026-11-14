"""
extract_code_availability.py
-----------------------------
Utility stage for the FAIR longitudinal study pipeline.

Extracts dedicated Code/Software Availability statements from PMC JATS
XML and stores them in two new SQLite tables:

  code_availability  — one row per article that has a code/software
                        availability section
  code_link          — URLs found in that section (same shape as da_link)

Why this is a SEPARATE stage from data_availability
-----------------------------------------------------
02_parse_to_sqlite.py's DAS extractor (`_DAS_PAT`) requires the word
"data" to appear in the matched sec-type/title (data.avail, data access,
data and code, code and data, ...). A standalone section titled just
"Code Availability" or "Software Availability" -- common in PLOS,
Nature-family, eLife and others, especially from ~2019 on -- contains no
such word and is silently missed entirely by that extractor. This script
closes that gap using the same six-level priority cascade as
extract_ethics.py.

This is INTENTIONALLY independent of, and overlaps with, data_availability:
a combined "Data and Code Availability" section will be captured by BOTH
this script (since its pattern also matches "data and code"/"code and
data") and by data_availability. That overlap is deliberate --
`is_combined_with_das` flags it, so downstream analysis can cleanly ask
"has a standalone code statement" vs "code mentioned only inside a
combined/data statement" vs "no code statement at all".

Resume-safe: articles already present in code_availability are skipped.

Usage
-----
  python extract_code_availability.py --xml-dir data/raw --db data/processed/pmc_fair.db
  python extract_code_availability.py --xml-dir data/raw --db pmc_fair.db --reset
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
LOG_DATE = "%H:%M:%S"


def setup_logging(log_path: Path) -> logging.Logger:
    logger = logging.getLogger("extract_code_availability")
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


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

SCHEMA = """
CREATE TABLE IF NOT EXISTS code_availability (
    code_id              INTEGER PRIMARY KEY AUTOINCREMENT,
    article_id           INTEGER NOT NULL UNIQUE REFERENCES article(article_id),
    statement             TEXT,
    source                TEXT,
    word_count            INTEGER DEFAULT 0,
    is_combined_with_das  INTEGER DEFAULT 0,   -- heading also mentions "data"
    has_github            INTEGER DEFAULT 0,
    has_gitlab            INTEGER DEFAULT 0,
    has_bitbucket         INTEGER DEFAULT 0,
    has_zenodo            INTEGER DEFAULT 0,
    has_osf               INTEGER DEFAULT 0,
    has_doi               INTEGER DEFAULT 0,
    has_license           INTEGER DEFAULT 0,
    has_request_only      INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS code_link (
    link_id    INTEGER PRIMARY KEY AUTOINCREMENT,
    code_id    INTEGER NOT NULL REFERENCES code_availability(code_id),
    url        TEXT,
    label      TEXT,
    link_type  TEXT
);

CREATE INDEX IF NOT EXISTS idx_ca_article ON code_availability(article_id);
CREATE INDEX IF NOT EXISTS idx_ca_source  ON code_availability(source);
CREATE INDEX IF NOT EXISTS idx_cl_code    ON code_link(code_id);
"""

RESET_TABLES = ["code_link", "code_availability"]

XLINK = "http://www.w3.org/1999/xlink"


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


_URL_RE = re.compile(r"https?://[^\s\"'<>\)\]]+", re.IGNORECASE)


def _extract_links(el, statement_text: str) -> list[tuple[str, str, str]]:
    links, seen = [], set()

    def _add(url, label, lt):
        url = (url or "").strip()
        if url and url not in seen:
            seen.add(url)
            links.append((url, (label or "").strip(), lt))

    if el is not None:
        for ext in el.iter("ext-link"):
            url   = ext.get(f"{{{XLINK}}}href") or ext.get("xlink:href") or ext.get("href") or ""
            label = _xml_clean(_xml_text(ext))
            lt    = ext.get("ext-link-type", "uri")
            if not url and label.startswith("http"):
                url = label
            _add(url, label, lt)
        for uri_el in el.iter("uri"):
            url = uri_el.get(f"{{{XLINK}}}href") or uri_el.get("xlink:href") or _xml_clean(_xml_text(uri_el))
            _add(url, _xml_clean(_xml_text(uri_el)), "uri")
        for ro in el.iter("related-object"):
            url = ro.get(f"{{{XLINK}}}href") or ro.get("xlink:href") or ro.get("href") or ""
            _add(url, _xml_clean(_xml_text(ro)) or ro.get("source", ""), "related-object")

    for m in _URL_RE.finditer(statement_text):
        _add(m.group(), "", "text-url")

    return links


# ---------------------------------------------------------------------------
# Signal patterns
# ---------------------------------------------------------------------------

_CODE_PAT = re.compile(
    r"code.{0,4}availab|software.{0,4}availab|source.{0,4}code.{0,4}availab"
    r"|availability.{0,10}of.{0,10}(the.{0,4})?(source.{0,4})?code"
    r"|availability.{0,10}of.{0,10}software"
    r"|code.{0,4}and.{0,4}data.{0,4}availab|data.{0,4}and.{0,4}code.{0,4}availab"
    r"|software.{0,4}and.{0,4}data.{0,4}availab|data.{0,4}and.{0,4}software.{0,4}availab",
    re.IGNORECASE,
)
_HAS_DATA_WORD = re.compile(r"\bdata\b", re.IGNORECASE)

_GITHUB_PAT    = re.compile(r"github", re.IGNORECASE)
_GITLAB_PAT    = re.compile(r"gitlab", re.IGNORECASE)
_BITBUCKET_PAT = re.compile(r"bitbucket", re.IGNORECASE)
_ZENODO_PAT    = re.compile(r"zenodo", re.IGNORECASE)
_OSF_PAT       = re.compile(r"\bosf\.io\b|open science framework", re.IGNORECASE)
_DOI_PAT       = re.compile(r"\b10\.\d{4,9}/|doi\.org", re.IGNORECASE)
_LICENSE_PAT   = re.compile(
    r"\bMIT license\b|\bGPL\b|\bGNU\b|Apache.{0,4}license|BSD.{0,4}license"
    r"|\bCC.{0,2}BY\b|open.{0,4}source.{0,4}license",
    re.IGNORECASE,
)
_REQUEST_ONLY_PAT = re.compile(
    r"available.{0,6}upon.{0,6}request|available.{0,6}on.{0,6}request"
    r"|request(ed)?.{0,6}from.{0,6}(the.{0,4})?(corresponding.{0,4})?authors?",
    re.IGNORECASE,
)


def signal_flags(text: str) -> dict:
    t = str(text or "")
    return {
        "word_count":       len(t.split()),
        "has_github":       int(bool(_GITHUB_PAT.search(t))),
        "has_gitlab":       int(bool(_GITLAB_PAT.search(t))),
        "has_bitbucket":    int(bool(_BITBUCKET_PAT.search(t))),
        "has_zenodo":       int(bool(_ZENODO_PAT.search(t))),
        "has_osf":          int(bool(_OSF_PAT.search(t))),
        "has_doi":          int(bool(_DOI_PAT.search(t))),
        "has_license":      int(bool(_LICENSE_PAT.search(t))),
        "has_request_only": int(bool(_REQUEST_ONLY_PAT.search(t))),
    }


# ---------------------------------------------------------------------------
# Extractor (six-level priority cascade, same shape as extract_ethics.py)
# ---------------------------------------------------------------------------

def extract_code_availability(article_el) -> tuple[str, str, list]:
    """
    Returns (statement_text, source_tag, links). ('', '', []) if none found.

    Priority cascade:
      1. sec[@sec-type matches _CODE_PAT]
      2. sec/title matches _CODE_PAT (regardless of the section's own
         sec-type -- see extract_ethics.py's equivalent fix: Priority 1
         already handles any sec-type that matches on its own, so this
         only adds coverage, never double-processes)
      3. notes[@notes-type matches]
      4. fn[@fn-type matches]
      5. p[@content-type matches]
      6. named-content[@content-type matches]
    """
    containers = list(article_el.findall("back")) + list(article_el.findall("body"))

    for c in containers:
        for sec in c.iter("sec"):
            st = sec.get("sec-type", "")
            if _CODE_PAT.search(st):
                text = _collect_sec_text(sec, skip_title=True)
                if text:
                    return text, f"sec[@sec-type='{st}']", _extract_links(sec, text)

    for c in containers:
        for sec in c.iter("sec"):
            title_el = sec.find("title")
            if title_el is not None:
                tt = _xml_clean(_xml_text(title_el))
                if _CODE_PAT.search(tt):
                    text = _collect_sec_text(sec, skip_title=True)
                    if text:
                        return text, f"sec/title='{tt}'", _extract_links(sec, text)

    for notes in article_el.findall(".//notes"):
        nt = notes.get("notes-type", "")
        if _CODE_PAT.search(nt):
            text = _collect_sec_text(notes, skip_title=True)
            if text:
                return text, f"notes[@notes-type='{nt}']", _extract_links(notes, text)

    for fn in article_el.findall(".//fn"):
        ft = fn.get("fn-type", "")
        if _CODE_PAT.search(ft):
            text = _xml_clean(_xml_text(fn))
            if text:
                return text, f"fn[@fn-type='{ft}']", _extract_links(fn, text)

    for p in article_el.findall(".//p"):
        ct = p.get("content-type", "")
        if ct and _CODE_PAT.search(ct):
            text = _xml_clean(_xml_text(p))
            if text:
                return text, f"p[@content-type='{ct}']", _extract_links(p, text)

    for nc in article_el.findall(".//named-content"):
        ct = nc.get("content-type", "")
        if ct and _CODE_PAT.search(ct):
            text = _xml_clean(_xml_text(nc))
            if text:
                return text, f"named-content[@content-type='{ct}']", _extract_links(nc, text)

    return "", "", []


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


def get_already_processed(con: sqlite3.Connection) -> set[int]:
    return {r[0] for r in con.execute("SELECT article_id FROM code_availability").fetchall()}


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
    inserted = skipped_no_match = skipped_existing = 0
    buf_rows: list[tuple] = []
    buf_links: list[tuple] = []   # (article_id, url, label, link_type)

    def flush(buf_rows, buf_links):
        nonlocal inserted
        cur = con.cursor()
        for params in buf_rows:
            try:
                cur.execute(
                    "INSERT OR IGNORE INTO code_availability "
                    "(article_id, statement, source, word_count, is_combined_with_das,"
                    " has_github, has_gitlab, has_bitbucket, has_zenodo, has_osf,"
                    " has_doi, has_license, has_request_only) "
                    "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    params,
                )
                if cur.rowcount:
                    inserted += 1
            except sqlite3.IntegrityError:
                pass
        for article_id, url, label, link_type in buf_links:
            row = con.execute(
                "SELECT code_id FROM code_availability WHERE article_id=?", (article_id,)
            ).fetchone()
            if row:
                cur.execute(
                    "INSERT INTO code_link (code_id, url, label, link_type) VALUES (?,?,?,?)",
                    (row[0], url, label, link_type),
                )
        con.commit()
        return [], []

    try:
        for event, elem in ET.iterparse(str(xml_path), events=("end",)):
            if elem.tag != "article":
                continue

            # Match to DB article_id -- scoped to front/article-meta/article-id
            # only (see extract_ethics.py for why: a recursive search risks
            # picking up a <related-article> cross-reference's own id).
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

            text, source, links = extract_code_availability(elem)
            elem.clear()

            if not text:
                continue

            f = signal_flags(text)
            is_combined = int(bool(_HAS_DATA_WORD.search(source)))
            buf_rows.append((
                article_id, text, source, f["word_count"], is_combined,
                f["has_github"], f["has_gitlab"], f["has_bitbucket"],
                f["has_zenodo"], f["has_osf"], f["has_doi"],
                f["has_license"], f["has_request_only"],
            ))
            already.add(article_id)
            for url, label, link_type in links:
                buf_links.append((article_id, url, label, link_type))

            if len(buf_rows) >= batch_size:
                buf_rows, buf_links = flush(buf_rows, buf_links)

    except ET.ParseError as exc:
        logger.error(f"  Parse error in {xml_path.name}: {exc}")

    if buf_rows:
        flush(buf_rows, buf_links)

    return inserted, skipped_no_match, skipped_existing


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Extract dedicated code/software availability statements from PMC XML.",
        formatter_class=argparse.RawDescriptionHelpFormatter, epilog=__doc__,
    )
    parser.add_argument("--xml-dir", "-x", default=".",
        help="Directory containing pmc_YYYY.xml files (default: current dir).")
    parser.add_argument("--db", default="pmc_fair.db")
    parser.add_argument("--reset", action="store_true",
        help="Drop and rebuild code_availability/code_link before extraction.")
    parser.add_argument("--batch-size", type=int, default=5_000)
    args = parser.parse_args()

    db_path  = Path(args.db)
    xml_dir  = Path(args.xml_dir)
    log_path = db_path.with_suffix(".code_availability.log")

    if not db_path.exists():
        sys.exit(f"Database not found: {db_path}")

    xml_files = sorted(xml_dir.glob("pmc_*.xml"))
    if not xml_files:
        sys.exit(f"No pmc_YYYY.xml files found in: {xml_dir.resolve()}")

    logger = setup_logging(log_path)
    t0     = time.perf_counter()
    rss0   = _peak_rss_mb()

    logger.info("=" * 62)
    logger.info("Code/Software Availability Extraction  —  FAIR Study")
    logger.info("=" * 62)
    logger.info(f"  XML directory  : {xml_dir.resolve()}")
    logger.info(f"  Files found    : {len(xml_files)}")
    logger.info(f"  Database       : {db_path}")
    logger.info("")

    con = sqlite3.connect(db_path)
    con.execute("PRAGMA journal_mode = WAL")
    con.execute("PRAGMA cache_size   = -32768")
    con.execute("PRAGMA synchronous  = NORMAL")

    if args.reset:
        logger.info("  --reset: dropping tables ...")
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
            xml_path, id_lookup, already, con, args.batch_size, logger,
        )
        elapsed = time.perf_counter() - t_file
        grand_inserted += ins
        grand_no_match += no_match
        grand_existing += exist

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
    logger.info(f"  Total code_availability rows inserted : {grand_inserted:,}")
    logger.info(f"  Articles with no DB match              : {grand_no_match:,}")
    logger.info(f"  Articles already extracted             : {grand_existing:,}")
    total = con.execute("SELECT COUNT(*) FROM code_availability").fetchone()[0]
    combined = con.execute(
        "SELECT COUNT(*) FROM code_availability WHERE is_combined_with_das=1"
    ).fetchone()[0]
    logger.info(f"  code_availability total rows           : {total:,}")
    logger.info(f"    of which combined with a DAS heading : {combined:,}")
    logger.info(f"  Wall-clock time                        : {run_elapsed/60:.1f} min")
    logger.info(f"  Peak RSS delta                         : {peak_rss:.1f} MB")
    logger.info(f"  Log                                    : {log_path}")

    con.close()


if __name__ == "__main__":
    main()
