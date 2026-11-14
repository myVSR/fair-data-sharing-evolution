"""
export_knowledge_graph.py
--------------------------
Stage 7 of the FAIR longitudinal study pipeline.

KEY DESIGN: streaming year-by-year serialisation
-------------------------------------------------
At 15M+ triples the full graph cannot fit in RAM for serialisation.
This script processes one year at a time:
  1. Build the shared/static graph (journals, authors, repos) — kept in memory
  2. For each year: build a small per-year graph, serialise it by appending
     to the output file, then discard it from memory.

This keeps peak RAM under ~2 GB regardless of corpus size.

Ontologies used
---------------
  schema:   https://schema.org/
  dcterms:  http://purl.org/dc/terms/
  dcat:     http://www.w3.org/ns/dcat#
  foaf:     http://xmlns.com/foaf/0.1/
  prov:     http://www.w3.org/ns/prov#
  fair:     https://w3id.org/fair/principles/terms/
  fairms:   https://w3id.org/fair/maturity_model/terms/
  bibo:     http://purl.org/ontology/bibo/
  skos:     http://www.w3.org/2004/02/skos/core#
  datacite: http://purl.org/spar/datacite/

Output formats
--------------
  n-quads  (.nq)   — RECOMMENDED: fastest, preserves named graphs, streamable
  turtle   (.ttl)  — readable but written per-year, each chunk is valid Turtle
  sparql   (.rq)   — bundled example SPARQL queries

SPARQL endpoint
---------------
  docker run -p 7878:7878 -v $(pwd)/kg:/data \\
    oxigraph/oxigraph --location /data serve --bind 0.0.0.0:7878
  # Load: POST the .nq file to http://localhost:7878/store

Usage
-----
  python export_knowledge_graph.py --db pmc_fair.db
  python export_knowledge_graph.py --db pmc_fair.db --formats n-quads sparql
  python export_knowledge_graph.py --db pmc_fair.db --years 2014-2018
  python export_knowledge_graph.py --db pmc_fair.db --include-accessions
  python export_knowledge_graph.py --db pmc_fair.db --sparql-demo
"""

import argparse
import logging
import re
import resource
import sqlite3
import sys
import time
from pathlib import Path

try:
    from rdflib import Graph, Dataset, URIRef, Literal, BNode, Namespace
    from rdflib.namespace import RDF, RDFS, OWL, XSD, SKOS, DCTERMS, FOAF, PROV
except ImportError:
    sys.exit("rdflib is required:  pip install rdflib --break-system-packages")

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

LOG_FMT  = "%(asctime)s  %(levelname)-7s  %(message)s"
LOG_DATE = "%Y-%m-%d %H:%M:%S"


def setup_logging(log_path: Path) -> logging.Logger:
    logger = logging.getLogger("export_kg")
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
# Namespaces
# ---------------------------------------------------------------------------

BASE     = Namespace("https://fair-study.org/")
SCHEMA   = Namespace("https://schema.org/")
DCAT     = Namespace("http://www.w3.org/ns/dcat#")
BIBO     = Namespace("http://purl.org/ontology/bibo/")
FAIR     = Namespace("https://w3id.org/fair/principles/terms/")
FAIRMS   = Namespace("https://w3id.org/fair/maturity_model/terms/")
DATACITE = Namespace("http://purl.org/spar/datacite/")
GR       = Namespace("https://fair-study.org/graph/")
ENT      = Namespace("https://fair-study.org/entity/")
REPO     = Namespace("https://fair-study.org/repository/")
DISC     = Namespace("https://fair-study.org/discipline/")
FUNDER   = Namespace("https://fair-study.org/funder/")

ALL_PREFIXES = {
    "base": BASE, "schema": SCHEMA, "dcterms": DCTERMS, "dcat": DCAT,
    "foaf": FOAF, "prov": PROV, "skos": SKOS, "bibo": BIBO,
    "fair": FAIR, "fairms": FAIRMS, "datacite": DATACITE,
    "ent": ENT, "repo": REPO, "disc": DISC, "funder": FUNDER, "gr": GR,
}

CATEGORY_CONCEPTS = {
    "repository_named":     FAIR["F1"],
    "accession_provided":   FAIR["F1"],
    "open_unspecified":     FAIR["A1"],
    "supplementary_only":   FAIR["A1"],
    "available_on_request": FAIR["A1.2"],
    "restricted":           FAIR["A1.2"],
    "no_statement":         FAIR["F1"],
    "no_das_but_supp":      FAIR["A1"],
    "no_das_but_accession": FAIR["F1"],
}

TIER_MATURITY = {
    "gold":    FAIRMS["advanced"],
    "silver":  FAIRMS["moderate"],
    "bronze":  FAIRMS["initial"],
    "unknown": FAIRMS["incomplete"],
}

# IRI-illegal ASCII chars that break Turtle/NQuads serialisers
_IRI_ILLEGAL = re.compile(r'[\x00-\x20<>"{}|\\^\x7F\[\]]')


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _safe_seg(s: str) -> str:
    """Make a string safe for use as an IRI path segment."""
    return re.sub(r"[^A-Za-z0-9._~:@!$&'()*+,;=-]", "_", s or "_")[:80]


def safe_uri(url: str) -> URIRef | None:
    """
    Return a URIRef for a URL, percent-encoding any characters that are
    illegal in IRIs and would break the Turtle/NQuads serialiser.
    Returns None if the URL is empty or not HTTP.
    """
    if not url or not url.startswith("http"):
        return None
    if _IRI_ILLEGAL.search(url):
        fixed = ""
        for ch in url:
            if _IRI_ILLEGAL.match(ch):
                fixed += f"%{ord(ch):02X}"
            else:
                fixed += ch
        url = fixed
    try:
        return URIRef(url)
    except Exception:
        return None


def article_iri(pmc_id, article_id: int) -> URIRef:
    if pmc_id:
        return ENT[f"article/PMC{str(pmc_id).lstrip('PMC')}"]
    return ENT[f"article/{article_id}"]


def doi_iri(doi_raw) -> URIRef | None:
    if not doi_raw:
        return None
    clean = str(doi_raw).strip().lstrip("https://doi.org/").lstrip("doi:")
    return safe_uri(f"https://doi.org/{clean}")


def author_iri(author_id: int, orcid) -> URIRef:
    if orcid:
        clean = str(orcid).strip().replace("https://orcid.org/", "")
        return URIRef(f"https://orcid.org/{clean}")
    return ENT[f"author/{author_id}"]


def journal_iri(journal_id: int, issn) -> URIRef:
    if issn:
        return URIRef(f"https://portal.issn.org/resource/ISSN/{str(issn).strip()}")
    return ENT[f"journal/{journal_id}"]


def year_graph_iri(year: int) -> URIRef:
    return GR[f"year/{year}"]


# ---------------------------------------------------------------------------
# Namespace binding helper
# ---------------------------------------------------------------------------

def bind_ns(g: Graph) -> None:
    for prefix, ns in ALL_PREFIXES.items():
        g.bind(prefix, ns)
    for prefix, ns in [("rdf", RDF), ("rdfs", RDFS), ("xsd", XSD),
                        ("skos", SKOS), ("owl", OWL), ("foaf", FOAF),
                        ("prov", PROV), ("dcterms", DCTERMS)]:
        g.bind(prefix, ns)


# ---------------------------------------------------------------------------
# Static/shared graph (journals, authors, repos) — built once, kept in RAM
# ---------------------------------------------------------------------------

def build_static_graph(con: sqlite3.Connection, logger: logging.Logger) -> tuple[Graph, dict, dict, dict]:
    """
    Build the static subgraph: journals, authors, repositories.
    Returns (graph, journal_cache, author_cache, repo_cache).
    These entities are referenced from year graphs but only declared once.
    """
    g = Graph()
    bind_ns(g)
    meta = GR["meta"]

    # Ontology header
    onto = BASE["ontology"]
    g.add((onto, RDF.type,       OWL.Ontology))
    g.add((onto, RDFS.label,     Literal("FAIR Longitudinal Study Knowledge Graph")))
    g.add((onto, DCTERMS.created, Literal(time.strftime("%Y-%m-%d"), datatype=XSD.date)))
    pipeline = BASE["pipeline"]
    g.add((pipeline, RDF.type,   PROV.SoftwareAgent))
    g.add((pipeline, RDFS.label, Literal("FAIR Study Pipeline v1.0")))

    # ── Repositories ──────────────────────────────────────────────────────
    repo_cache: dict[str, URIRef] = {}
    for repo_name, repo_type, repo_infra_tier, is_oa, has_pid, domain in con.execute("""
        SELECT DISTINCT repo_name, repo_type, repo_infra_tier,
                        is_open_access, has_persistent_id, domain
        FROM repo_classification
        WHERE repo_name IS NOT NULL AND repo_name != ''
    """).fetchall():
        iri = REPO[_safe_seg(repo_name)]
        repo_cache[repo_name] = iri
        g.add((iri, RDF.type,       DCAT["DataService"]))
        g.add((iri, RDF.type,       SCHEMA["DataCatalog"]))
        g.add((iri, SCHEMA["name"], Literal(repo_name)))
        if domain:
            u = safe_uri(f"https://{domain}")
            if u: g.add((iri, SCHEMA["url"], u))
        if repo_type:
            g.add((iri, DCTERMS["type"], Literal(repo_type)))
        if repo_infra_tier in TIER_MATURITY:
            g.add((iri, FAIRMS["hasMaturityLevel"], TIER_MATURITY[repo_infra_tier]))
        if is_oa:
            g.add((iri, DCTERMS["accessRights"],
                   URIRef("http://purl.org/coar/access_right/c_abf2")))
    logger.info(f"  Repositories : {len(repo_cache):,}")

    # ── Journals ──────────────────────────────────────────────────────────
    journal_cache: dict[int, URIRef] = {}
    for jid, title, abbrev, issn_p, issn_e, publisher, oa_status in con.execute("""
        SELECT j.journal_id, j.title, j.abbrev, j.issn_ppub, j.issn_epub,
               j.publisher, jo.oa_status
        FROM journal j LEFT JOIN journal_oa jo ON jo.journal_id = j.journal_id
    """).fetchall():
        iri = journal_iri(jid, issn_p or issn_e)
        journal_cache[jid] = iri
        g.add((iri, RDF.type,        BIBO["Journal"]))
        g.add((iri, RDF.type,        SCHEMA["Periodical"]))
        if title:   g.add((iri, SCHEMA["name"],      Literal(title)))
        if abbrev:  g.add((iri, BIBO["shortTitle"],  Literal(abbrev)))
        if publisher: g.add((iri, SCHEMA["publisher"], Literal(publisher)))
        for issn in filter(None, [issn_p, issn_e]):
            g.add((iri, BIBO["issn"], Literal(issn)))
        if oa_status: g.add((iri, DCTERMS["accessRights"], Literal(oa_status)))
    logger.info(f"  Journals     : {len(journal_cache):,}")

    # ── Authors ───────────────────────────────────────────────────────────
    author_cache: dict[int, URIRef] = {}
    for aid, surname, given, full_name, orcid, email in con.execute(
        "SELECT author_id, surname, given_name, full_name, orcid, email FROM author"
    ).fetchall():
        iri = author_iri(aid, orcid)
        author_cache[aid] = iri
        g.add((iri, RDF.type, SCHEMA["Person"]))
        g.add((iri, RDF.type, FOAF["Person"]))
        if full_name: g.add((iri, SCHEMA["name"],    Literal(full_name)))
        if surname:   g.add((iri, FOAF["familyName"], Literal(surname)))
        if given:     g.add((iri, FOAF["givenName"],  Literal(given)))
        if orcid:
            clean = str(orcid).strip().replace("https://orcid.org/", "")
            g.add((iri, DATACITE["hasIdentifier"],
                   URIRef(f"https://orcid.org/{clean}")))
        if email: g.add((iri, SCHEMA["email"], Literal(email)))
    logger.info(f"  Authors      : {len(author_cache):,}")

    # ── Funders (shared across years) ─────────────────────────────────────
    tables = {r[0] for r in con.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    ).fetchall()}
    if "article_funder" in tables:
        funder_iris: dict[str, URIRef] = {}
        for funder_norm, country in con.execute("""
            SELECT DISTINCT funder_norm, funder_country FROM article_funder
            WHERE funder_norm != 'none'
        """).fetchall():
            f_iri = FUNDER[_safe_seg(funder_norm)]
            funder_iris[funder_norm] = f_iri
            g.add((f_iri, RDF.type,        SCHEMA["FundingAgency"]))
            g.add((f_iri, RDF.type,        FOAF["Organization"]))
            g.add((f_iri, SCHEMA["name"],  Literal(funder_norm)))
            if country:
                g.add((f_iri, SCHEMA["addressCountry"], Literal(country)))
        logger.info(f"  Funders      : {len(funder_iris):,}")

    logger.info(f"  Static graph : {len(g):,} triples")
    return g, journal_cache, author_cache, repo_cache


# ---------------------------------------------------------------------------
# Per-year article graph — built and discarded for each year
# ---------------------------------------------------------------------------

def build_year_graph(
    con:               sqlite3.Connection,
    year:              int,
    journal_cache:     dict[int, URIRef],
    author_cache:      dict[int, URIRef],
    repo_cache:        dict[str, URIRef],
    include_accessions: bool,
    logger:            logging.Logger,
) -> Graph:
    """
    Build the subgraph for a single year. Returned graph uses a named
    graph IRI but is serialised as a plain Graph (NQuads handles the
    context externally via append logic).
    """
    g = Graph(identifier=year_graph_iri(year))
    bind_ns(g)
    yr_graph = year_graph_iri(year)
    FAIR_YEAR = 2016

    # ── Collect author links for this year ────────────────────────────────
    author_links: dict[int, list[tuple[int, int]]] = {}
    for article_id, author_id, pos in con.execute("""
        SELECT aa.article_id, aa.author_id, aa.author_position
        FROM article_author aa
        JOIN article a ON a.article_id = aa.article_id
        WHERE a.pub_year = ?
    """, (year,)).fetchall():
        author_links.setdefault(article_id, []).append((author_id, pos))

    # ── Articles ──────────────────────────────────────────────────────────
    article_cache: dict[int, URIRef] = {}
    rows = con.execute("""
        SELECT a.article_id, a.pmc_id, a.pubmed_id, a.doi,
               a.article_type, a.title, a.pub_year, a.journal_id,
               a.subject, a.keywords, a.license,
               da.statement, da.source,
               dc.primary_category, dc.data_sharing_score,
               dc.mentions_license, dc.mentions_doi, dc.mentions_github,
               dc.has_url, dc.has_accession,
               ad.discipline, jo.oa_status
        FROM article a
        LEFT JOIN data_availability  da ON da.article_id = a.article_id
        LEFT JOIN das_category       dc ON dc.article_id = a.article_id
        LEFT JOIN article_discipline ad ON ad.article_id = a.article_id
        LEFT JOIN journal_oa         jo ON jo.journal_id  = a.journal_id
        WHERE a.pub_year = ?
        ORDER BY a.article_id
    """, (year,)).fetchall()

    for row in rows:
        (article_id, pmc_id, pubmed_id, doi_raw,
         article_type, title, pub_year, journal_id,
         subject, keywords, license_,
         das_stmt, das_source,
         das_cat, das_score,
         das_lic, das_doi_flag, das_github, das_url, das_acc,
         discipline, oa_status) = row

        art_iri = article_iri(pmc_id, article_id)
        article_cache[article_id] = art_iri

        g.add((art_iri, RDF.type,        SCHEMA["ScholarlyArticle"]))
        g.add((art_iri, RDF.type,        BIBO["AcademicArticle"]))
        if title:
            g.add((art_iri, SCHEMA["name"],    Literal(title)))
            g.add((art_iri, DCTERMS["title"],  Literal(title)))
        if pub_year:
            g.add((art_iri, SCHEMA["datePublished"],
                   Literal(str(pub_year), datatype=XSD.gYear)))
        if pmc_id:
            pmc_url = safe_uri(
                f"https://www.ncbi.nlm.nih.gov/pmc/articles/"
                f"PMC{str(pmc_id).lstrip('PMC')}/"
            )
            if pmc_url: g.add((art_iri, DATACITE["hasIdentifier"], pmc_url))
        if pubmed_id:
            g.add((art_iri, BIBO["pmid"], Literal(str(pubmed_id))))
        doi_node = doi_iri(doi_raw)
        if doi_node:
            g.add((art_iri, SCHEMA["identifier"], doi_node))
        if article_type:
            g.add((art_iri, DCTERMS["type"], Literal(article_type)))
        if journal_id and journal_id in journal_cache:
            g.add((art_iri, SCHEMA["isPartOf"], journal_cache[journal_id]))
        if license_:
            g.add((art_iri, SCHEMA["license"], Literal(license_)))
        if oa_status:
            g.add((art_iri, DCTERMS["accessRights"], Literal(oa_status)))

        # Discipline
        if discipline:
            disc_iri = DISC[_safe_seg(discipline)]
            g.add((disc_iri, RDF.type,        SKOS["Concept"]))
            g.add((disc_iri, SKOS["prefLabel"], Literal(discipline)))
            g.add((art_iri,  DCTERMS["subject"], disc_iri))

        # FAIR period
        period = ("pre_FAIR" if int(pub_year or 0) < FAIR_YEAR
                  else "FAIR_year" if int(pub_year or 0) == FAIR_YEAR
                  else "post_FAIR")
        g.add((art_iri, BASE["FAIRPeriod"], Literal(period)))

        # Authors — direct triples, no BNode contribution nodes
        for auth_id, pos in author_links.get(article_id, []):
            if auth_id in author_cache:
                g.add((art_iri, SCHEMA["author"], author_cache[auth_id]))

        # DAS statement
        if das_stmt:
            das_iri = ENT[f"das/{article_id}"]
            g.add((das_iri, RDF.type,       BASE["DataAvailabilityStatement"]))
            g.add((das_iri, SCHEMA["text"], Literal(das_stmt)))
            g.add((art_iri, BASE["hasDataAvailabilityStatement"], das_iri))
            if das_source:
                g.add((das_iri, DCTERMS["source"], Literal(das_source)))

        # DAS category / data-sharing transparency assessment (NOT a FAIR
        # assessment -- see Stage 5 F-UJI for that)
        if das_cat:
            assess_iri = ENT[f"assessment/{article_id}"]
            g.add((assess_iri, RDF.type,  BASE["DataSharingAssessment"]))
            g.add((assess_iri, DCTERMS["subject"], art_iri))
            g.add((assess_iri, BASE["dasCategory"], Literal(das_cat)))
            g.add((assess_iri, BASE["dataSharingScore"],
                   Literal(das_score or 0, datatype=XSD.integer)))
            if das_cat in CATEGORY_CONCEPTS:
                g.add((assess_iri, FAIR["relatesTo"], CATEGORY_CONCEPTS[das_cat]))
            for flag, pred in [
                (das_lic,      BASE["mentionsLicense"]),
                (das_doi_flag, BASE["mentionsDOI"]),
                (das_github,   BASE["mentionsGitHub"]),
                (das_url,      BASE["hasURL"]),
                (das_acc,      BASE["hasAccessionNumber"]),
            ]:
                if flag:
                    g.add((assess_iri, pred, Literal(True, datatype=XSD.boolean)))
            g.add((art_iri, BASE["hasFAIRAssessment"], assess_iri))

    # ── DAS links ─────────────────────────────────────────────────────────
    for link_id, url, label, link_type, article_id, repo_name, repo_infra_tier in con.execute("""
        SELECT dl.link_id, dl.url, dl.label, dl.link_type,
               da.article_id, rc.repo_name, rc.repo_infra_tier
        FROM da_link dl
        JOIN data_availability da ON da.da_id = dl.da_id
        JOIN article a ON a.article_id = da.article_id
        LEFT JOIN repo_classification rc ON rc.link_id = dl.link_id
        WHERE a.pub_year = ? AND dl.url IS NOT NULL AND dl.url != ''
    """, (year,)).fetchall():
        if article_id not in article_cache:
            continue
        link_iri = ENT[f"link/{link_id}"]
        g.add((link_iri, RDF.type, DCAT["Distribution"]))
        url_ref = safe_uri(url)
        if url_ref:
            g.add((link_iri, DCAT["downloadURL"], url_ref))
        else:
            g.add((link_iri, DCAT["downloadURL"], Literal(url)))
        if label: g.add((link_iri, DCTERMS["title"], Literal(label)))
        if link_type: g.add((link_iri, DCTERMS["type"], Literal(link_type)))
        if repo_name and repo_name in repo_cache:
            g.add((link_iri, DCAT["servesDataset"], repo_cache[repo_name]))
        if repo_infra_tier in TIER_MATURITY:
            g.add((link_iri, FAIRMS["hasMaturityLevel"], TIER_MATURITY[repo_infra_tier]))
        art_iri = article_cache[article_id]
        g.add((art_iri, SCHEMA["distribution"], link_iri))

    # ── Accession numbers (opt-in — large volume) ─────────────────────────
    if include_accessions:
        for acc_id, article_id, db_name, accession, url in con.execute("""
            SELECT an.acc_id, an.article_id, an.db_name, an.accession, an.url
            FROM accession_number an
            JOIN article a ON a.article_id = an.article_id
            WHERE a.pub_year = ?
        """, (year,)).fetchall():
            if article_id not in article_cache:
                continue
            acc_iri = ENT[f"accession/{acc_id}"]
            g.add((acc_iri, RDF.type,               DCAT["Dataset"]))
            g.add((acc_iri, DATACITE["hasIdentifier"], Literal(accession)))
            g.add((acc_iri, DCTERMS["source"],       Literal(db_name)))
            url_ref = safe_uri(url) if url else None
            if url_ref:
                g.add((acc_iri, SCHEMA["url"],       url_ref))
                g.add((acc_iri, DCAT["landingPage"], url_ref))
            g.add((article_cache[article_id], DATACITE["isSupplementedBy"], acc_iri))

    # ── F-UJI assessments ─────────────────────────────────────────────────
    tables = {r[0] for r in con.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    ).fetchall()}
    if "fuji_assessment" in tables:
        for (assess_id, url, assessed_at, earned, total_s,
             pct_fair, pct_f, pct_a, pct_i, pct_r, maturity,
             article_id) in con.execute("""
            SELECT fa.assessment_id, fa.url, fa.assessed_at,
                   fa.score_earned, fa.score_total,
                   fa.score_pct_fair, fa.score_pct_f, fa.score_pct_a,
                   fa.score_pct_i, fa.score_pct_r, fa.maturity_fair,
                   fal.article_id
            FROM fuji_assessment fa
            JOIN fuji_article_link fal ON fal.assessment_id = fa.assessment_id
            JOIN article a ON a.article_id = fal.article_id
            WHERE fa.http_status = 200 AND a.pub_year = ?
        """, (year,)).fetchall():
            if article_id not in article_cache:
                continue
            fuji_iri = ENT[f"fuji/{assess_id}"]
            g.add((fuji_iri, RDF.type, BASE["FUJIAssessment"]))
            url_ref = safe_uri(url) if url else None
            if url_ref:
                g.add((fuji_iri, SCHEMA["itemReviewed"], url_ref))
            if assessed_at:
                g.add((fuji_iri, DCTERMS["date"],
                       Literal(str(assessed_at)[:10], datatype=XSD.date)))
            # Scores as simple literals (avoids BNode explosion)
            for pred, val in [
                (BASE["scoreFAIR"], pct_fair), (BASE["scoreF"], pct_f),
                (BASE["scoreA"],    pct_a),     (BASE["scoreI"], pct_i),
                (BASE["scoreR"],    pct_r),
            ]:
                if val is not None:
                    g.add((fuji_iri, pred,
                           Literal(round(float(val), 2), datatype=XSD.decimal)))
            if maturity is not None:
                mat_labels = {0:"incomplete",1:"initial",2:"moderate",3:"advanced"}
                g.add((fuji_iri, FAIRMS["hasMaturityLevel"],
                       FAIRMS[mat_labels.get(maturity, "unknown")]))
            # Per-metric (lightweight: one triple per metric for pass/fail)
            for mid, principle, m_earned, m_total, m_status in con.execute("""
                SELECT metric_identifier, principle,
                       score_earned, score_total, test_status
                FROM fuji_metric WHERE assessment_id = ?
            """, (assess_id,)).fetchall():
                metric_iri = ENT[f"metric/{_safe_seg(mid)}"]
                g.add((metric_iri, RDF.type,           BASE["FAIRMetric"]))
                g.add((metric_iri, DCTERMS["identifier"], Literal(mid)))
                if principle:
                    g.add((metric_iri, BASE["FAIRPrinciple"], Literal(principle)))
                result_node = ENT[f"fuji/{assess_id}/m/{_safe_seg(mid)}"]
                g.add((result_node, RDF.type,            BASE["MetricResult"]))
                g.add((result_node, BASE["metric"],      metric_iri))
                g.add((result_node, BASE["testStatus"],  Literal(m_status or "")))
                if m_earned is not None:
                    g.add((result_node, SCHEMA["value"],
                           Literal(m_earned, datatype=XSD.integer)))
                g.add((fuji_iri, BASE["hasMetricResult"], result_node))
            g.add((article_cache[article_id], BASE["hasFUJIAssessment"], fuji_iri))

    # ── Funders for this year ─────────────────────────────────────────────
    if "article_funder" in tables:
        for article_id, funder_norm, grant_id in con.execute("""
            SELECT af.article_id, af.funder_norm, af.grant_id
            FROM article_funder af
            JOIN article a ON a.article_id = af.article_id
            WHERE af.funder_norm != 'none' AND a.pub_year = ?
        """, (year,)).fetchall():
            if article_id not in article_cache:
                continue
            f_iri = FUNDER[_safe_seg(funder_norm)]
            grant = ENT[f"grant/{article_id}/{_safe_seg(funder_norm)}"]
            g.add((grant, RDF.type,          SCHEMA["Grant"]))
            g.add((grant, SCHEMA["funder"],  f_iri))
            if grant_id:
                g.add((grant, SCHEMA["identifier"], Literal(grant_id)))
            g.add((article_cache[article_id], SCHEMA["funding"], grant))

    return g


# ---------------------------------------------------------------------------
# Streaming serialiser — appends one year at a time to the output file
# ---------------------------------------------------------------------------

def _graph_to_nquads_bytes(g: Graph, graph_iri: URIRef) -> bytes:
    """
    Serialise a plain rdflib Graph as N-Quads bytes with an explicit
    named-graph IRI appended to every quad.

    We copy triples into a fresh, identifier-less Graph() before serialising
    as nt11 (N-Triples with full Unicode).  This prevents rdflib from seeing
    the graph's identifier attribute and attempting NQuads/RDF-XML fallback,
    which raises "not possible to serialize ... RDF/XML does not support
    named graphs".
    """
    # Copy into a bare graph (no identifier) to avoid serialiser confusion
    bare = Graph()
    for triple in g:
        bare.add(triple)

    nt = bare.serialize(format="nt11")
    graph_tok = f"<{graph_iri}>"
    lines = []
    for line in nt.splitlines():
        line = line.strip()
        if line and not line.startswith("#"):
            if line.endswith(" ."):
                lines.append(f"{line[:-2]} {graph_tok} .\n")
    return "".join(lines).encode("utf-8")


def stream_to_nquads(
    static_g:           Graph,
    con:                sqlite3.Connection,
    years:              list[int],
    out_path:           Path,
    journal_cache:      dict[int, URIRef],
    author_cache:       dict[int, URIRef],
    repo_cache:         dict[str, URIRef],
    include_accessions: bool,
    logger:             logging.Logger,
) -> int:
    """
    Write the knowledge graph as N-Quads by streaming one year at a time:
      1. Static graph (journals, authors, repos) → <meta> named graph
      2. Each year graph → <year/YYYY> named graph, then freed from memory
    Peak RAM stays proportional to one year's data, not the full corpus.
    Returns total triple count.
    """
    total    = 0
    meta_iri = GR["meta"]

    with open(out_path, "wb") as fout:
        # Static graph → meta named graph
        chunk = _graph_to_nquads_bytes(static_g, meta_iri)
        fout.write(chunk)
        total += chunk.count(b"\n")

        # Per-year graphs
        for year in years:
            t_yr = time.perf_counter()
            yr_g = build_year_graph(
                con, year, journal_cache, author_cache, repo_cache,
                include_accessions, logger,
            )
            yr_triples = len(yr_g)
            yr_iri     = year_graph_iri(year)

            chunk = _graph_to_nquads_bytes(yr_g, yr_iri)
            fout.write(chunk)
            fout.flush()
            total += yr_triples

            elapsed = time.perf_counter() - t_yr
            rss     = _peak_rss_mb()
            logger.info(
                f"  {year}  {yr_triples:>8,} triples  "
                f"{elapsed:>5.1f}s  {rss:>6.0f} MB RSS"
            )
            del yr_g  # release memory before next year

    return total


def stream_to_turtle(
    static_g:          Graph,
    con:               sqlite3.Connection,
    years:             list[int],
    out_path:          Path,
    journal_cache:     dict[int, URIRef],
    author_cache:      dict[int, URIRef],
    repo_cache:        dict[str, URIRef],
    include_accessions: bool,
    logger:            logging.Logger,
) -> int:
    """
    Write one self-contained Turtle file per year into a subdirectory.
    Each file is fully valid standalone Turtle (includes all @prefix lines).
    out_path is used as the directory name; individual files are named
    fair_kg_YYYY.ttl inside it.

    A single concatenated file is NOT produced for Turtle because stripping
    @prefix lines from subsequent chunks breaks SKOS/BIBO/etc. prefixes.
    Use N-Quads (stream_to_nquads) for a single merged file.
    """
    # out_path for turtle is actually a directory
    ttl_dir = out_path.parent / (out_path.stem + "_ttl_parts")
    ttl_dir.mkdir(exist_ok=True)

    total = 0

    # Static graph → fair_kg_meta.ttl
    bare_static = Graph()
    bind_ns(bare_static)
    for triple in static_g:
        bare_static.add(triple)
    meta_path = ttl_dir / "fair_kg_meta.ttl"
    meta_path.write_bytes(bare_static.serialize(format="turtle").encode("utf-8"))
    total += len(static_g)
    del bare_static

    for year in years:
        yr_g = build_year_graph(
            con, year, journal_cache, author_cache, repo_cache,
            include_accessions, logger,
        )
        # Copy to bare graph (no identifier) to prevent RDF/XML fallback
        bare_ttl = Graph()
        bind_ns(bare_ttl)
        for triple in yr_g:
            bare_ttl.add(triple)
        yr_path = ttl_dir / f"fair_kg_{year}.ttl"
        yr_path.write_bytes(bare_ttl.serialize(format="turtle").encode("utf-8"))
        total += len(yr_g)
        del bare_ttl, yr_g

    logger.info(f"  Turtle parts written to: {ttl_dir}/")
    logger.info(f"  Files: fair_kg_meta.ttl + {len(years)} year files")
    logger.info(f"  Load all into Oxigraph:")
    logger.info(f"    for f in {ttl_dir}/*.ttl; do")
    logger.info(f"      curl -X POST http://localhost:7878/store \\")
    logger.info(f"        -H 'Content-Type: text/turtle' --data-binary @$f; done")

    return total


# ---------------------------------------------------------------------------
# SPARQL example queries
# ---------------------------------------------------------------------------

SPARQL_EXAMPLES = """\
# =============================================================================
# SPARQL Example Queries — FAIR Longitudinal Study Knowledge Graph
# Load fair_knowledge_graph.nq into Oxigraph / Fuseki / GraphDB, then run.
# =============================================================================

PREFIX schema:   <https://schema.org/>
PREFIX dcterms:  <http://purl.org/dc/terms/>
PREFIX dcat:     <http://www.w3.org/ns/dcat#>
PREFIX fair:     <https://w3id.org/fair/principles/terms/>
PREFIX fairms:   <https://w3id.org/fair/maturity_model/terms/>
PREFIX base:     <https://fair-study.org/>
PREFIX ent:      <https://fair-study.org/entity/>
PREFIX repo:     <https://fair-study.org/repository/>
PREFIX disc:     <https://fair-study.org/discipline/>
PREFIX funder:   <https://fair-study.org/funder/>
PREFIX foaf:     <http://xmlns.com/foaf/0.1/>
PREFIX xsd:      <http://www.w3.org/2001/XMLSchema#>
PREFIX gr:       <https://fair-study.org/graph/>

# Q1: Articles per year with a named repository in DAS
SELECT ?year (COUNT(?article) AS ?n_repo_named)
WHERE {
  GRAPH ?g {
    ?article a schema:ScholarlyArticle ;
             schema:datePublished ?year ;
             base:hasFAIRAssessment ?assessment .
    ?assessment base:dasCategory "repository_named" .
  }
}
GROUP BY ?year ORDER BY ?year

# Q2: Mean proxy FAIR score per discipline, before vs after 2016
SELECT ?discipline ?period (AVG(?score) AS ?mean_score) (COUNT(?a) AS ?n)
WHERE {
  GRAPH ?g {
    ?a a schema:ScholarlyArticle ;
       dcterms:subject ?disc_iri ;
       base:FAIRPeriod ?period ;
       base:hasFAIRAssessment ?assessment .
    ?assessment base:proxyFAIRScore ?score .
    ?disc_iri <http://www.w3.org/2004/02/skos/core#prefLabel> ?discipline .
  }
}
GROUP BY ?discipline ?period ORDER BY ?discipline ?period

# Q3: Top 10 repositories by number of articles
SELECT ?repo_name (COUNT(DISTINCT ?article) AS ?n_articles)
WHERE {
  GRAPH ?g {
    ?article schema:distribution ?dist .
    ?dist dcat:servesDataset ?repo .
    ?repo schema:name ?repo_name .
  }
}
GROUP BY ?repo_name ORDER BY DESC(?n_articles) LIMIT 10

# Q4: 'Available on request' rate by funder
SELECT ?funder_name
       (COUNT(DISTINCT ?a) AS ?total)
       (SUM(IF(?cat = "available_on_request", 1, 0)) AS ?n_request)
WHERE {
  GRAPH ?g {
    ?a a schema:ScholarlyArticle ;
       schema:funding ?grant ;
       base:hasFAIRAssessment ?assessment .
    ?assessment base:dasCategory ?cat .
    ?grant schema:funder ?funder .
    ?funder schema:name ?funder_name .
  }
}
GROUP BY ?funder_name HAVING (?total > 10)
ORDER BY DESC(?n_request)

# Q5: F-UJI FAIR score by year
SELECT ?year (AVG(?fair) AS ?avg_fair) (AVG(?f) AS ?avg_f)
             (AVG(?a) AS ?avg_a) (AVG(?i) AS ?avg_i) (AVG(?r) AS ?avg_r)
WHERE {
  GRAPH ?g {
    ?article a schema:ScholarlyArticle ;
             schema:datePublished ?year ;
             base:hasFUJIAssessment ?fuji .
    ?fuji base:scoreFAIR ?fair ;
          base:scoreF ?f ;
          base:scoreA ?a ;
          base:scoreI ?i ;
          base:scoreR ?r .
  }
}
GROUP BY ?year ORDER BY ?year

# Q6: Author FAIR trajectory (authors with 3+ papers)
SELECT ?author_name (COUNT(?a) AS ?papers)
       (AVG(IF(?period = "pre_FAIR",  ?score, 0)) AS ?pre_score)
       (AVG(IF(?period = "post_FAIR", ?score, 0)) AS ?post_score)
WHERE {
  GRAPH ?g {
    ?a a schema:ScholarlyArticle ;
       schema:author ?author ;
       base:FAIRPeriod ?period ;
       base:hasFAIRAssessment ?assess .
    ?assess base:proxyFAIRScore ?score .
    ?author schema:name ?author_name .
  }
}
GROUP BY ?author_name HAVING (?papers >= 3)
ORDER BY DESC(?post_score - ?pre_score) LIMIT 50

# Q7: OA journals vs subscription — FAIR score difference
SELECT ?oa_status (AVG(?score) AS ?mean_fair) (COUNT(?a) AS ?n)
WHERE {
  GRAPH ?g {
    ?a a schema:ScholarlyArticle ;
       dcterms:accessRights ?oa_status ;
       base:hasFAIRAssessment ?assess .
    ?assess base:proxyFAIRScore ?score .
  }
}
GROUP BY ?oa_status ORDER BY DESC(?mean_fair)

# Q8: F-UJI metric pass rates by principle
SELECT ?principle ?metric_id
       (COUNT(?result) AS ?total)
       (SUM(IF(?status = "pass", 1, 0)) AS ?n_pass)
WHERE {
  GRAPH ?g {
    ?article base:hasFUJIAssessment ?fuji .
    ?fuji base:hasMetricResult ?result .
    ?result base:metric ?metric ;
            base:testStatus ?status .
    ?metric base:FAIRPrinciple ?principle ;
            dcterms:identifier ?metric_id .
  }
}
GROUP BY ?principle ?metric_id ORDER BY ?principle ?metric_id
"""


# ---------------------------------------------------------------------------
# SPARQL demo (in-memory, small corpus only)
# ---------------------------------------------------------------------------

def run_sparql_demo(
    static_g: Graph,
    con: sqlite3.Connection,
    journal_cache: dict, author_cache: dict, repo_cache: dict,
    logger: logging.Logger,
) -> None:
    """Build one year's graph and run example SPARQL queries against it."""
    logger.info("\n── SPARQL Demo (running against 2020 data) ──")
    demo_g = Dataset()
    bind_ns(demo_g)
    # Add static triples
    for t in static_g:
        demo_g.add(t + (GR["meta"],))
    # Add one year
    yr_g = build_year_graph(con, 2020, journal_cache, author_cache,
                             repo_cache, False, logger)
    yr_iri = year_graph_iri(2020)
    for t in yr_g:
        demo_g.add(t + (yr_iri,))

    q_count = """
        PREFIX schema: <https://schema.org/>
        PREFIX base:   <https://fair-study.org/>
        SELECT (COUNT(?a) AS ?n) (AVG(?score) AS ?avg)
        WHERE {
          GRAPH ?g {
            ?a a schema:ScholarlyArticle ;
               base:hasFAIRAssessment ?assess .
            ?assess base:proxyFAIRScore ?score .
          }
        }
    """
    result = list(demo_g.query(q_count))
    if result:
        n, avg = result[0]
        logger.info(f"  2020: {int(n)} articles, mean FAIR score = {float(avg or 0):.2f}")

    q_cats = """
        PREFIX base: <https://fair-study.org/>
        SELECT ?cat (COUNT(?a) AS ?n)
        WHERE { GRAPH ?g { ?a base:hasFAIRAssessment ?assess .
                           ?assess base:dasCategory ?cat . } }
        GROUP BY ?cat ORDER BY DESC(?n)
    """
    logger.info("  DAS categories (2020):")
    for row in demo_g.query(q_cats):
        logger.info(f"    {str(row.cat):<30}  {int(row.n):>6,}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Export knowledge graph — FAIR longitudinal study Stage 7.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--db",           default="pmc_fair.db")
    parser.add_argument("--output-dir",   default="./kg")
    parser.add_argument("--formats",      nargs="+",
        default=["n-quads", "sparql"],
        choices=["n-quads", "turtle", "sparql"],
        help="Formats to write. n-quads is fastest and recommended.")
    parser.add_argument("--years",        default=None,
        help="Year range e.g. 2014-2018. Default: all years in DB.")
    parser.add_argument("--include-accessions", action="store_true",
        help="Include accession number nodes (~4M extra triples, uses more RAM).")
    parser.add_argument("--sparql-demo",  action="store_true",
        help="Run example SPARQL queries against one year's data.")
    parser.add_argument("--reset",        action="store_true",
        help="Delete existing output files before export.")
    args = parser.parse_args()

    db_path    = Path(args.db)
    output_dir = Path(args.output_dir)
    log_path   = db_path.with_suffix(".kg.log")

    if not db_path.exists():
        sys.exit(f"Database not found: {db_path}")

    logger = setup_logging(log_path)
    t0     = time.perf_counter()

    logger.info("=" * 65)
    logger.info("Knowledge Graph Export — FAIR Longitudinal Study (Stage 7)")
    logger.info("=" * 65)
    logger.info(f"  Database            : {db_path}")
    logger.info(f"  Output dir          : {output_dir}")
    logger.info(f"  Formats             : {args.formats}")
    logger.info(f"  Include accessions  : {args.include_accessions}")

    output_dir.mkdir(parents=True, exist_ok=True)
    if args.reset:
        for f in output_dir.glob("fair_knowledge_graph.*"):
            f.unlink()
            logger.info(f"  Deleted: {f}")

    con = sqlite3.connect(db_path)

    # Determine years to export
    if args.years:
        lo, hi = args.years.split("-")
        years = list(range(int(lo), int(hi) + 1))
    else:
        years = [r[0] for r in con.execute("""
            SELECT DISTINCT CAST(pub_year AS INTEGER) FROM article
            WHERE pub_year IS NOT NULL ORDER BY pub_year
        """).fetchall()]
    logger.info(f"  Years               : {years[0]}–{years[-1]} ({len(years)} years)")

    # Build static subgraph (shared entities)
    logger.info("\n  Building static subgraph (journals, authors, repos) …")
    static_g, journal_cache, author_cache, repo_cache = build_static_graph(con, logger)

    logger.info(f"\n  Streaming per-year graphs to file …")
    logger.info(f"  {'Year':<6} {'Triples':>10}  {'Time':>6}  {'RSS':>8}")
    logger.info("  " + "─" * 40)

    total_triples = 0
    for fmt in args.formats:
        if fmt == "sparql":
            path = output_dir / "sparql_example_queries.rq"
            path.write_text(SPARQL_EXAMPLES, encoding="utf-8")
            logger.info(f"  ✅ SPARQL queries → {path}")
            continue

        out_path = output_dir / (
            "fair_knowledge_graph.nq" if fmt == "n-quads"
            else "fair_knowledge_graph.ttl"
        )
        t_fmt = time.perf_counter()

        if fmt == "n-quads":
            total_triples = stream_to_nquads(
                static_g, con, years, out_path,
                journal_cache, author_cache, repo_cache,
                args.include_accessions, logger,
            )
        else:
            total_triples = stream_to_turtle(
                static_g, con, years, out_path,
                journal_cache, author_cache, repo_cache,
                args.include_accessions, logger,
            )

        size_mb = out_path.stat().st_size / (1024 * 1024)
        elapsed_fmt = time.perf_counter() - t_fmt
        logger.info(
            f"\n  ✅ {fmt:<10} → {out_path}\n"
            f"     {total_triples:,} triples  |  {size_mb:.1f} MB  |  "
            f"{elapsed_fmt/60:.1f} min"
        )

    if args.sparql_demo:
        run_sparql_demo(static_g, con, journal_cache,
                        author_cache, repo_cache, logger)

    con.close()
    total_time = time.perf_counter() - t0
    peak_rss   = _peak_rss_mb()
    logger.info(f"\nTotal time : {total_time/60:.1f} min")
    logger.info(f"Peak RSS   : {peak_rss:.0f} MB")
    logger.info(f"Log        : {log_path}")
    logger.info("\nTo load into Oxigraph SPARQL endpoint:")
    logger.info("  docker run -p 7878:7878 -v $(pwd)/kg:/data \\")
    logger.info("    oxigraph/oxigraph serve --location /data --bind 0.0.0.0:7878")
    logger.info("  curl -X POST -H 'Content-Type:application/n-quads' \\")
    logger.info("    --data-binary @kg/fair_knowledge_graph.nq \\")
    logger.info("    http://localhost:7878/store")


if __name__ == "__main__":
    main()
