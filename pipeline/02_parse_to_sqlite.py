"""
pubmed_to_sqlite.py
-------------------
Parses PMC XML files and stores structured metadata in a normalised SQLite DB.
Stage-2 of the FAIR longitudinal study pipeline.

Schema changes vs previous version
------------------------------------
  article table  : abstract REMOVED; added subject, copyright_statement,
                   license, keywords (stored inline as semicolon list)
  new table       : supplementary_material  — files attached to the article
  new table       : accession_number        — inline database accession IDs
  data_availability: unchanged
  da_link         : unchanged

Data-sharing channels captured (important for pre-2016 articles)
-----------------------------------------------------------------
  1. data_availability      — explicit DAS section (common post-2014)
  2. supplementary_material — <supplementary-material> blocks (all years)
  3. accession_number       — inline DB refs: GEO, SRA, GenBank, UniProt, etc.

Usage
-----
  python 02_parse_to_sqlite.py --input pmc_2016.xml --db pmc_fair.db
  python 02_parse_to_sqlite.py --input ./data/raw   --db pmc_fair.db
  python 02_parse_to_sqlite.py --input ./data/raw   --db pmc_fair.db --reset
"""

import argparse
import logging
import re
import sqlite3
import sys
import time
import resource
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional
import xml.etree.ElementTree as ET

LOG_FMT  = "%(asctime)s  %(levelname)-7s  %(message)s"
LOG_DATE = "%Y-%m-%d %H:%M:%S"

def setup_logging(log_path):
    logger = logging.getLogger("02_parse_to_sqlite")
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

def _peak_rss_mb():
    try:
        kb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        return kb / 1024 if sys.platform != "darwin" else kb / (1024*1024)
    except Exception:
        return 0.0

@dataclass
class Author:
    surname:      str       = ""
    given:        str       = ""
    full_name:    str       = ""
    orcid:        str       = ""
    email:        str       = ""
    affiliations: list      = field(default_factory=list)

@dataclass
class SupplementaryMaterial:
    label:      str = ""
    caption:    str = ""
    media_type: str = ""
    url:        str = ""

@dataclass
class AccessionNumber:
    db_name:   str = ""
    accession: str = ""
    url:       str = ""

@dataclass
class Article:
    pmc_id:     str = ""
    pubmed_id:  str = ""
    doi:        str = ""
    title:               str  = ""
    article_type:        str  = ""
    subject:             str  = ""
    keywords:            list = field(default_factory=list)
    copyright_statement: str  = ""
    license:             str  = ""
    pub_year:       str = ""
    pub_month:      str = ""
    pub_day:        str = ""
    received_date:  str = ""
    accepted_date:  str = ""
    epub_date:      str = ""
    journal_title:  str = ""
    journal_abbrev: str = ""
    issn_ppub:      str = ""
    issn_epub:      str = ""
    publisher:      str = ""
    authors:                 list = field(default_factory=list)
    data_availability_statement: str  = ""
    data_availability_source:    str  = ""
    data_availability_links:     list = field(default_factory=list)
    supplementary_materials:     list = field(default_factory=list)
    accession_numbers:           list = field(default_factory=list)

def _text(el):
    if el is None: return ""
    return " ".join(el.itertext()).strip()

def _clean(s):
    return re.sub(r"\s+", " ", s).strip()

def _find_text(parent, *paths):
    for path in paths:
        el = parent.find(path)
        if el is not None:
            t = _clean(_text(el))
            if t: return t
    return ""

def _build_date(el):
    if el is None: return ""
    y = (el.findtext("year")  or "").strip()
    m = (el.findtext("month") or "").strip().zfill(2) if el.findtext("month") else ""
    d = (el.findtext("day")   or "").strip().zfill(2) if el.findtext("day")   else ""
    return "-".join(p for p in (y, m, d) if p)

ACCESSION_PATTERNS = [
    # GEO: GSE/GDS/GPL/GSM — all start with G and have a well-defined prefix
    ("GEO",       re.compile(r"\b(GSE\d{3,8}|GDS\d{3,7}|GPL\d{3,7}|GSM\d{4,9})\b"), "https://www.ncbi.nlm.nih.gov/geo/query/acc.cgi?acc={acc}"),
    # SRA: SRP/SRR/SRX/SRS (NCBI) and ERP/ERR (ENA) — all have 2-letter prefix + digits
    ("SRA",       re.compile(r"\b(SRP\d{5,9}|SRR\d{5,9}|SRX\d{5,9}|SRS\d{5,9}|ERP\d{5,9}|ERR\d{5,9})\b"), "https://www.ncbi.nlm.nih.gov/sra/{acc}"),
    # GenBank: 1-2 letters + 5-8 digits — require both letters to be uppercase
    # Tightened: must be preceded by non-alphanumeric to avoid grabbing words
    ("GenBank",   re.compile(r"(?<![A-Z0-9])([A-Z]{2}\d{6,8}|[A-Z]\d{5})(?![A-Z0-9])"), "https://www.ncbi.nlm.nih.gov/nuccore/{acc}"),
    # UniProt: strict format — starts O/P/Q or specific letter+digit combos
    ("UniProt",   re.compile(r"\b([OPQ][0-9][A-Z0-9]{3}[0-9]|[A-NR-Z][0-9]([A-Z][A-Z0-9]{2}[0-9]){1,2})\b"), "https://www.uniprot.org/uniprot/{acc}"),
    # PDB: FIXED — real PDB IDs must contain at least one letter AND one digit
    # in positions 2-4, and position 1 must be a digit. E.g. 1ABC, 3J7C, not 3771.
    # Pattern: digit + (at least one letter among remaining 3 alphanumeric chars)
    ("PDB",       re.compile(r"\b([1-9][A-Z0-9]{0,2}[A-Z][A-Z0-9]{0,2})\b(?=\s|[,;.)]|$)"), "https://www.rcsb.org/structure/{acc}"),
    # ArrayExpress: strict E-XXXX-NNNN format
    ("ArrayExpr", re.compile(r"\b(E-[A-Z]{4}-\d{4,6})\b"), "https://www.ebi.ac.uk/arrayexpress/experiments/{acc}"),
    # dbGaP: phs + 6 digits, optionally with version
    ("dbGaP",     re.compile(r"\b(phs\d{6}\.v\d+\.p\d+|phs\d{6})\b"), "https://www.ncbi.nlm.nih.gov/projects/gap/cgi-bin/study.cgi?study_id={acc}"),
    # URL-based: only match when the full URL context is present
    ("Zenodo",    re.compile(r"zenodo\.org/(?:record|deposit)/(\d+)"), "https://zenodo.org/record/{acc}"),
    ("Figshare",  re.compile(r"figshare\.com/(?:articles?/[^/]+/)?(\d+)"), "https://figshare.com/articles/{acc}"),
    ("Dryad",     re.compile(r"datadryad\.org/stash/dataset/(doi:[^\s,]+)"), "https://datadryad.org/stash/dataset/{acc}"),
    ("OSF",       re.compile(r"osf\.io/([a-z0-9]{5})"), "https://osf.io/{acc}"),
]

def _extract_accession_numbers(article_el):
    results, seen = [], set()
    def _add(db, acc, url):
        key = (db, acc)
        if key not in seen:
            seen.add(key)
            results.append(AccessionNumber(db_name=db, accession=acc, url=url))
    for tag in ("body", "back", "front"):
        sec = article_el.find(tag)
        if sec is None: continue
        txt = _clean(_text(sec))
        for db_name, pat, url_tpl in ACCESSION_PATTERNS:
            for m in pat.finditer(txt):
                acc = m.group(1) if m.lastindex else m.group(0)
                _add(db_name, acc, url_tpl.format(acc=acc))
    for oid in article_el.findall(".//object-id"):
        db  = oid.get("pub-id-type", "")
        val = (oid.text or "").strip()
        if db and val:
            url = ""
            for db_name, _, url_tpl in ACCESSION_PATTERNS:
                if db_name.lower() in db.lower():
                    url = url_tpl.format(acc=val); break
            _add(db, val, url)
    return results

def _extract_supplementary_materials(article_el):
    XLINK = "http://www.w3.org/1999/xlink"
    results = []
    for sm in article_el.findall(".//supplementary-material"):
        label_el   = sm.find("label")
        caption_el = sm.find(".//caption/p") or sm.find("caption")
        label      = _clean(_text(label_el))   if label_el   is not None else ""
        caption    = _clean(_text(caption_el)) if caption_el is not None else ""
        mt         = sm.get("mimetype", "")
        sub        = sm.get("mime-subtype", "")
        media_type = f"{mt}/{sub}" if mt and sub else mt
        url = sm.get(f"{{{XLINK}}}href") or sm.get("xlink:href") or sm.get("href") or ""
        results.append(SupplementaryMaterial(label=label, caption=caption, media_type=media_type, url=url))
    return results

_DAS_PAT = re.compile(
    r"data.avail|availability of data|data access|data sharing|"
    r"availability of supporting|supporting data|data and (code|material)|"
    r"(code|material) and data", re.IGNORECASE)
_URL_RE = re.compile(r"https?://[^\s\"'<>\)\]]+", re.IGNORECASE)

def _extract_das_links(el, statement_text):
    XLINK = "http://www.w3.org/1999/xlink"
    links, seen_urls = [], set()
    def _add(url, label, lt):
        url = url.strip()
        if url and url not in seen_urls:
            seen_urls.add(url)
            links.append({"url": url, "label": label.strip(), "link_type": lt})
    for ext in el.iter("ext-link"):
        url = ext.get(f"{{{XLINK}}}href") or ext.get("xlink:href") or ext.get("href") or ""
        label = _clean(_text(ext))
        lt    = ext.get("ext-link-type", "uri")
        if not url and label.startswith("http"): url = label
        _add(url, label, lt)
    for uri_el in el.iter("uri"):
        url = uri_el.get(f"{{{XLINK}}}href") or uri_el.get("xlink:href") or _clean(_text(uri_el))
        _add(url, _clean(_text(uri_el)), "uri")
    for ro in el.iter("related-object"):
        url = ro.get(f"{{{XLINK}}}href") or ro.get("xlink:href") or ro.get("href") or ""
        _add(url, _clean(_text(ro)) or ro.get("source", ""), "related-object")
    for m in _URL_RE.finditer(statement_text):
        _add(m.group(), "", "text-url")
    return links

def _collect_sec_text(el, skip_title=False):
    parts = []
    for child in el:
        if skip_title and child.tag == "title": continue
        if child.tag in ("p", "list", "disp-quote", "def-list"):
            t = _clean(_text(child))
            if t: parts.append(t)
        elif child.tag == "sec":
            st = _clean(_text(child.find("title"))) if child.find("title") is not None else ""
            sp = [_clean(_text(p)) for p in child.iter("p") if _clean(_text(p))]
            parts.append((st + ": " if st else "") + " ".join(sp))
    if not parts:
        t = _clean(_text(el))
        if t: parts.append(t)
    return " ".join(parts)

def _extract_das(article_el):
    def _r(el, text, src):
        links = _extract_das_links(el, text) if el is not None else []
        if el is None:
            for m in _URL_RE.finditer(text):
                links.append({"url": m.group().strip(), "label": "", "link_type": "text-url"})
        return text, src, links
    da_el = article_el.find(".//data-availability")
    if da_el is not None:
        text = _collect_sec_text(da_el)
        if text: return _r(da_el, text, "data-availability element")
    containers = list(article_el.findall("back")) + list(article_el.findall("body"))
    for c in containers:
        for sec in c.iter("sec"):
            st = sec.get("sec-type", "")
            if _DAS_PAT.search(st):
                text = _collect_sec_text(sec, skip_title=True)
                if text: return _r(sec, text, f"sec[@sec-type='{st}']")
    for c in containers:
        for sec in c.iter("sec"):
            te = sec.find("title")
            if te is not None:
                tt = _clean(_text(te))
                if _DAS_PAT.search(tt):
                    text = _collect_sec_text(sec, skip_title=True)
                    if text: return _r(sec, text, f"sec/title='{tt}'")
    for cm in article_el.findall(".//custom-meta"):
        ne = cm.find("meta-name"); ve = cm.find("meta-value")
        if ne is not None and ve is not None and _DAS_PAT.search(_text(ne)):
            text = _clean(_text(ve))
            if text: return _r(ve, text, f"custom-meta/{_clean(_text(ne))}")
    for notes in article_el.findall(".//notes"):
        nt = notes.get("notes-type", "")
        if _DAS_PAT.search(nt):
            text = _collect_sec_text(notes, skip_title=True)
            if text: return _r(notes, text, f"notes[@notes-type='{nt}']")
    for fn in article_el.findall(".//fn-group/fn"):
        ft = _clean(_text(fn))
        if _DAS_PAT.search(ft): return _r(fn, ft, "fn-group/fn")
    return "", "", []

def parse_article(article_el):
    art = Article()
    art.article_type = article_el.get("article-type", "")
    front = article_el.find("front")
    if front is None: return art
    jmeta = front.find("journal-meta")
    if jmeta is not None:
        art.journal_title  = _find_text(jmeta, "journal-title-group/journal-title", "journal-title")
        art.journal_abbrev = _find_text(jmeta, "journal-title-group/abbrev-journal-title", "abbrev-journal-title")
        art.publisher      = _find_text(jmeta, "publisher/publisher-name")
        for issn_el in jmeta.findall("issn"):
            pt  = issn_el.get("pub-type", "")
            val = (issn_el.text or "").strip()
            if pt == "ppub":   art.issn_ppub = val
            elif pt == "epub": art.issn_epub = val
            elif not art.issn_ppub: art.issn_ppub = val
    ameta = front.find("article-meta")
    if ameta is None: return art
    for id_el in ameta.findall("article-id"):
        t = id_el.get("pub-id-type", ""); v = (id_el.text or "").strip()
        if t == "pmc":  art.pmc_id    = v
        elif t == "pmid": art.pubmed_id = v
        elif t == "doi":  art.doi       = v
    art.title = _clean(_text(ameta.find("title-group/article-title")))
    for subj in ameta.findall(".//article-categories//subject"):
        t = _clean(_text(subj))
        if t: art.subject = t; break
    for kwd in ameta.findall(".//kwd"):
        t = _clean(_text(kwd))
        if t: art.keywords.append(t)
    perms = ameta.find("permissions")
    if perms is not None:
        art.copyright_statement = _clean(_text(perms.find("copyright-statement")))
        lic_el = perms.find("license")
        if lic_el is not None:
            XLINK = "http://www.w3.org/1999/xlink"
            art.license = (lic_el.get(f"{{{XLINK}}}href") or lic_el.get("xlink:href")
                           or _clean(_text(lic_el.find("license-p"))) or _clean(_text(lic_el)))
    for pd in ameta.findall("pub-date"):
        pt = pd.get("pub-type", pd.get("date-type", ""))
        if pt in ("ppub", "collection"):
            art.pub_year  = art.pub_year  or (pd.findtext("year",  "") or "").strip()
            art.pub_month = art.pub_month or (pd.findtext("month", "") or "").strip()
            art.pub_day   = art.pub_day   or (pd.findtext("day",   "") or "").strip()
        elif pt in ("epub", "epub-ppub"):
            art.epub_date = art.epub_date or _build_date(pd)
            art.pub_year  = art.pub_year  or (pd.findtext("year",  "") or "").strip()
            art.pub_month = art.pub_month or (pd.findtext("month", "") or "").strip()
    for date_el in ameta.findall("history/date"):
        dt = date_el.get("date-type", ""); ds = _build_date(date_el)
        if dt == "received": art.received_date = ds
        elif dt in ("accepted", "accept"): art.accepted_date = ds
    aff_map = {}
    for aff in ameta.findall(".//aff"):
        aid = aff.get("id", ""); lbl = aff.find("label")
        if lbl is not None: aff.remove(lbl)
        at = _clean(_text(aff))
        if aid and at: aff_map[aid] = at
    for contrib in ameta.findall(".//contrib[@contrib-type='author']"):
        author = Author()
        ne = contrib.find("name")
        if ne is not None:
            author.surname   = _clean(_text(ne.find("surname")))
            author.given     = _clean(_text(ne.find("given-names")))
            author.full_name = f"{author.given} {author.surname}".strip()
        else:
            collab = contrib.find("collab")
            if collab is not None: author.full_name = _clean(_text(collab))
        for cid in contrib.findall("contrib-id"):
            if cid.get("contrib-id-type", "").lower() == "orcid":
                raw = (cid.text or "").strip()
                author.orcid = raw.replace("https://orcid.org/", "").replace("http://orcid.org/", "")
        ee = contrib.find(".//email")
        if ee is not None: author.email = (ee.text or "").strip()
        for xref in contrib.findall("xref[@ref-type='aff']"):
            for r in xref.get("rid", "").split():
                if r in aff_map: author.affiliations.append(aff_map[r])
        seen = set()
        author.affiliations = [a for a in author.affiliations if not (a in seen or seen.add(a))]
        art.authors.append(author)
    art.data_availability_statement, art.data_availability_source, art.data_availability_links = _extract_das(article_el)
    art.supplementary_materials = _extract_supplementary_materials(article_el)
    art.accession_numbers       = _extract_accession_numbers(article_el)
    return art

def _iter_articles(path: Path, logger):
    """
    Stream article elements from a large PMC XML file one at a time using
    iterparse.  Each element is yielded then immediately cleared from memory.

    Memory cost: one <article> element at a time (~50-200 KB), regardless of
    the total file size.  Replaces ET.parse() which loaded the full 20 GB DOM
    tree into RAM before any processing could begin.
    """
    logger.info(f"  Streaming {path.name} ...")
    n_articles = 0
    try:
        for event, elem in ET.iterparse(str(path), events=("end",)):
            if elem.tag == "article":
                yield elem
                n_articles += 1
                elem.clear()   # release this element's memory immediately
    except ET.ParseError as exc:
        logger.error(f"  Parse error in {path.name}: {exc}")
    logger.info(f"  Streamed {n_articles:,} articles from {path.name}")

SCHEMA = """
PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS journal (
    journal_id   INTEGER PRIMARY KEY AUTOINCREMENT,
    title        TEXT NOT NULL,
    abbrev       TEXT,
    issn_ppub    TEXT,
    issn_epub    TEXT,
    publisher    TEXT,
    UNIQUE (title)
);

CREATE TABLE IF NOT EXISTS article (
    article_id          INTEGER PRIMARY KEY AUTOINCREMENT,
    journal_id          INTEGER REFERENCES journal(journal_id),
    pmc_id              TEXT UNIQUE,
    pubmed_id           TEXT,
    doi                 TEXT,
    article_type        TEXT,
    title               TEXT,
    subject             TEXT,
    keywords            TEXT,
    copyright_statement TEXT,
    license             TEXT,
    pub_year            TEXT,
    pub_month           TEXT,
    pub_day             TEXT,
    received_date       TEXT,
    accepted_date       TEXT,
    epub_date           TEXT
);

CREATE TABLE IF NOT EXISTS author (
    author_id  INTEGER PRIMARY KEY AUTOINCREMENT,
    surname    TEXT,
    given_name TEXT,
    full_name  TEXT,
    orcid      TEXT,
    email      TEXT,
    UNIQUE (surname, given_name, orcid)
);

CREATE TABLE IF NOT EXISTS affiliation (
    affiliation_id INTEGER PRIMARY KEY AUTOINCREMENT,
    affiliation    TEXT UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS article_author (
    article_id      INTEGER NOT NULL REFERENCES article(article_id),
    author_id       INTEGER NOT NULL REFERENCES author(author_id),
    author_position INTEGER NOT NULL,
    PRIMARY KEY (article_id, author_id)
);

CREATE TABLE IF NOT EXISTS author_affiliation (
    article_id     INTEGER NOT NULL REFERENCES article(article_id),
    author_id      INTEGER NOT NULL REFERENCES author(author_id),
    affiliation_id INTEGER NOT NULL REFERENCES affiliation(affiliation_id),
    PRIMARY KEY (article_id, author_id, affiliation_id)
);

CREATE TABLE IF NOT EXISTS keyword (
    keyword_id INTEGER PRIMARY KEY AUTOINCREMENT,
    keyword    TEXT UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS article_keyword (
    article_id INTEGER NOT NULL REFERENCES article(article_id),
    keyword_id INTEGER NOT NULL REFERENCES keyword(keyword_id),
    PRIMARY KEY (article_id, keyword_id)
);

CREATE TABLE IF NOT EXISTS data_availability (
    da_id      INTEGER PRIMARY KEY AUTOINCREMENT,
    article_id INTEGER NOT NULL UNIQUE REFERENCES article(article_id),
    statement  TEXT NOT NULL,
    source     TEXT
);

CREATE TABLE IF NOT EXISTS da_link (
    link_id   INTEGER PRIMARY KEY AUTOINCREMENT,
    da_id     INTEGER NOT NULL REFERENCES data_availability(da_id),
    url       TEXT NOT NULL,
    label     TEXT,
    link_type TEXT
);

CREATE TABLE IF NOT EXISTS supplementary_material (
    supp_id    INTEGER PRIMARY KEY AUTOINCREMENT,
    article_id INTEGER NOT NULL REFERENCES article(article_id),
    label      TEXT,
    caption    TEXT,
    media_type TEXT,
    url        TEXT
);

CREATE TABLE IF NOT EXISTS accession_number (
    acc_id     INTEGER PRIMARY KEY AUTOINCREMENT,
    article_id INTEGER NOT NULL REFERENCES article(article_id),
    db_name    TEXT NOT NULL,
    accession  TEXT NOT NULL,
    url        TEXT,
    UNIQUE (article_id, db_name, accession)
);

CREATE INDEX IF NOT EXISTS idx_article_pmc    ON article(pmc_id);
CREATE INDEX IF NOT EXISTS idx_article_doi    ON article(doi);
CREATE INDEX IF NOT EXISTS idx_article_year   ON article(pub_year);
CREATE INDEX IF NOT EXISTS idx_article_jrnl   ON article(journal_id);
CREATE INDEX IF NOT EXISTS idx_author_orcid   ON author(orcid);
CREATE INDEX IF NOT EXISTS idx_da_article     ON data_availability(article_id);
CREATE INDEX IF NOT EXISTS idx_da_link_da     ON da_link(da_id);
CREATE INDEX IF NOT EXISTS idx_da_link_url    ON da_link(url);
CREATE INDEX IF NOT EXISTS idx_supp_article   ON supplementary_material(article_id);
CREATE INDEX IF NOT EXISTS idx_acc_article    ON accession_number(article_id);
CREATE INDEX IF NOT EXISTS idx_acc_db         ON accession_number(db_name);
"""

RESET_TABLES = [
    "author_affiliation","article_keyword","article_author",
    "da_link","data_availability","supplementary_material","accession_number",
    "keyword","affiliation","author","article","journal",
]
SUMMARY_TABLES = [
    "journal","article","author","affiliation",
    "keyword","article_author","author_affiliation","article_keyword",
    "data_availability","da_link","supplementary_material","accession_number",
]

def get_or_create(cur, table, unique_cols, extra_cols=None):
    where = " AND ".join(f"{c} = ?" if v is not None else f"{c} IS NULL" for c,v in unique_cols.items())
    vals  = [v for v in unique_cols.values() if v is not None]
    cur.execute(f"SELECT rowid FROM {table} WHERE {where}", vals)
    row = cur.fetchone()
    if row: return row[0]
    all_cols = {**unique_cols, **(extra_cols or {})}
    cols = ", ".join(all_cols.keys())
    ph   = ", ".join(["?"] * len(all_cols))
    cur.execute(f"INSERT INTO {table} ({cols}) VALUES ({ph})", list(all_cols.values()))
    return cur.lastrowid

def insert_article(cur, art):
    """
    Insert a single Article into the database and return
    (n_das, n_supp, n_acc) counts (each 0 or 1 or more for this article).
    The caller is responsible for periodic commits.
    """
    journal_id = get_or_create(cur, "journal",
        unique_cols={"title": art.journal_title or "Unknown"},
        extra_cols={"abbrev": art.journal_abbrev or None,
                    "issn_ppub": art.issn_ppub or None,
                    "issn_epub": art.issn_epub or None,
                    "publisher": art.publisher or None})
    pmc_id_val = art.pmc_id or None
    article_id = None
    if pmc_id_val:
        row = cur.execute(
            "SELECT article_id FROM article WHERE pmc_id = ?", (pmc_id_val,)
        ).fetchone()
        if row:
            article_id = row[0]
    if article_id is None:
        cur.execute("""INSERT INTO article
            (journal_id,pmc_id,pubmed_id,doi,article_type,title,
             subject,keywords,copyright_statement,license,
             pub_year,pub_month,pub_day,received_date,accepted_date,epub_date)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (journal_id, pmc_id_val, art.pubmed_id or None, art.doi or None,
             art.article_type or None, art.title or None, art.subject or None,
             "; ".join(art.keywords) or None, art.copyright_statement or None,
             art.license or None, art.pub_year or None, art.pub_month or None,
             art.pub_day or None, art.received_date or None,
             art.accepted_date or None, art.epub_date or None))
        article_id = cur.lastrowid
    for kw in art.keywords:
        kw_id = get_or_create(cur, "keyword", {"keyword": kw})
        cur.execute(
            "INSERT OR IGNORE INTO article_keyword (article_id,keyword_id) VALUES (?,?)",
            (article_id, kw_id))
    for pos, author in enumerate(art.authors, 1):
        author_id = get_or_create(cur, "author",
            unique_cols={"surname": author.surname or None,
                         "given_name": author.given or None,
                         "orcid": author.orcid or None},
            extra_cols={"full_name": author.full_name or None,
                        "email": author.email or None})
        cur.execute(
            "INSERT OR IGNORE INTO article_author (article_id,author_id,author_position) VALUES (?,?,?)",
            (article_id, author_id, pos))
        for aff_text in author.affiliations:
            aff_id = get_or_create(cur, "affiliation", {"affiliation": aff_text})
            cur.execute(
                "INSERT OR IGNORE INTO author_affiliation (article_id,author_id,affiliation_id) VALUES (?,?,?)",
                (article_id, author_id, aff_id))
    n_das = 0
    if art.data_availability_statement:
        n_das = 1
        cur.execute("""INSERT INTO data_availability (article_id,statement,source) VALUES (?,?,?)
            ON CONFLICT(article_id) DO UPDATE SET statement=excluded.statement,source=excluded.source""",
            (article_id, art.data_availability_statement, art.data_availability_source or None))
        da_id = cur.execute(
            "SELECT da_id FROM data_availability WHERE article_id=?", (article_id,)
        ).fetchone()[0]
        for lnk in art.data_availability_links:
            cur.execute("INSERT INTO da_link (da_id,url,label,link_type) VALUES (?,?,?,?)",
                (da_id, lnk["url"], lnk.get("label") or None, lnk.get("link_type") or None))
    n_supp = 0
    for sm in art.supplementary_materials:
        cur.execute(
            "INSERT INTO supplementary_material (article_id,label,caption,media_type,url) VALUES (?,?,?,?,?)",
            (article_id, sm.label or None, sm.caption or None, sm.media_type or None, sm.url or None))
        n_supp += 1
    n_acc = 0
    for acc in art.accession_numbers:
        try:
            cur.execute(
                "INSERT OR IGNORE INTO accession_number (article_id,db_name,accession,url) VALUES (?,?,?,?)",
                (article_id, acc.db_name, acc.accession, acc.url or None))
            n_acc += 1
        except sqlite3.IntegrityError:
            pass
    return n_das, n_supp, n_acc

def print_summary(con, logger):
    logger.info("")
    logger.info("Database summary")
    logger.info("─" * 46)
    for t in SUMMARY_TABLES:
        cnt = con.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
        logger.info(f"  {t:<28} {cnt:>10,} rows")
    logger.info("─" * 46)

def collect_xml_files(input_path):
    if input_path.is_file(): return [input_path]
    if input_path.is_dir():
        files = sorted(input_path.glob("pmc_*.xml"))
        if not files: sys.exit(f"No pmc_YYYY.xml files in: {input_path}")
        return files
    sys.exit(f"Input not found: {input_path}")

def main():
    parser = argparse.ArgumentParser(description="Parse PMC XML to SQLite — FAIR study Stage 2.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python 02_parse_to_sqlite.py --input pmc_2016.xml --db pmc_fair.db
  python 02_parse_to_sqlite.py --input ./data/raw   --db pmc_fair.db
  python 02_parse_to_sqlite.py --input ./data/raw   --db pmc_fair.db --reset
        """)
    parser.add_argument("--input", "-i", default="pmc_2016.xml")
    parser.add_argument("--db", default="pmc_fair.db")
    parser.add_argument("--reset", action="store_true")
    parser.add_argument("--skip-summary", action="store_true",
        help="Skip the final COUNT(*)-per-table summary. On a multi-million-row "
             "database this COUNT(*) is a full-table-scan on every table and can "
             "take hours by itself, regardless of how many new rows this run "
             "actually added -- useful when re-running a single year's file "
             "incrementally against an already-large database.")
    args = parser.parse_args()

    xml_files = collect_xml_files(Path(args.input))
    db_path   = Path(args.db)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    log_path  = db_path.with_suffix(".parse.log")
    logger    = setup_logging(log_path)
    run_t0    = time.perf_counter()
    run_rss0  = _peak_rss_mb()

    logger.info("=" * 55)
    logger.info("PMC XML to SQLite  —  FAIR Longitudinal Study (Stage 2)")
    logger.info("=" * 55)
    logger.info(f"  Input   : {args.input}")
    logger.info(f"  Files   : {len(xml_files)}")
    logger.info(f"  Database: {db_path}")
    logger.info(f"  Log     : {log_path}")
    logger.info("")

    con = sqlite3.connect(db_path)
    if args.reset:
        logger.info("  --reset: dropping all tables ...")
        cur = con.cursor()
        for tbl in RESET_TABLES: cur.execute(f"DROP TABLE IF EXISTS {tbl}")
        con.commit()
    con.executescript(SCHEMA)
    con.commit()
    logger.info("  Schema ready.\n")

    # Apply WAL mode and a large page cache so SQLite is not the bottleneck.
    con.execute("PRAGMA journal_mode = WAL")
    con.execute("PRAGMA cache_size   = -65536")  # 64 MB page cache
    con.execute("PRAGMA synchronous  = NORMAL")   # safe with WAL; much faster than FULL

    COMMIT_EVERY = 500    # commit once per N articles — balances safety vs speed

    grand_total = 0
    for idx, xml_path in enumerate(xml_files, 1):
        t0   = time.perf_counter()
        logger.info(f"[{idx}/{len(xml_files)}] {xml_path.name}")

        cur = con.cursor()
        cur.execute("PRAGMA foreign_keys = ON")
        n_file = n_das = n_supp = n_acc = 0

        # Stream one <article> element at a time — no full-file DOM in RAM.
        for article_el in _iter_articles(xml_path, logger):
            art = parse_article(article_el)
            d, s, a = insert_article(cur, art)
            n_file += 1; n_das += d; n_supp += s; n_acc += a

            if n_file % COMMIT_EVERY == 0:
                con.commit()
                rss = _peak_rss_mb()
                logger.info(
                    f"  {n_file:,} articles inserted  "
                    f"({time.perf_counter()-t0:.0f}s  RSS {rss:.0f} MB)"
                )

        con.commit()   # final commit for remainder
        elapsed = time.perf_counter() - t0
        grand_total += n_file
        logger.info(
            f"  Done: {n_file:,} articles  "
            f"DAS {n_das:,}  Supp {n_supp:,}  Acc {n_acc:,}  "
            f"({elapsed:.1f}s)"
        )
        logger.info("")

    elapsed  = time.perf_counter() - run_t0
    peak_rss = max(_peak_rss_mb() - run_rss0, 0)
    logger.info(f"Grand total : {grand_total:,} articles  |  {len(xml_files)} file(s)")
    logger.info(f"Wall-clock  : {elapsed/60:.1f} min  ({elapsed:.0f}s)")
    logger.info(f"Peak RSS    : {peak_rss:.1f} MB")
    if args.skip_summary:
        logger.info("  (--skip-summary passed: omitting the full-table-scan row-count summary)")
    else:
        print_summary(con, logger)
    con.close()
    logger.info(f"\nDone. DB: {db_path}  |  Log: {log_path}")

if __name__ == "__main__":
    main()
