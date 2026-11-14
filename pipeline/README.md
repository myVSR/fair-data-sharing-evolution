# Pipeline stage → script mapping

The paper's Methods section (`paper/10yearsofFAIRDAS/main_specialissue.tex`,
§"Data Ingestion Pipeline and Database Schema" and related subsections)
describes the pipeline by numbered **stage**, not by script filename, so
that renaming or splitting a script never requires a manuscript edit. This
file is the authoritative mapping from stage number to the script that
currently implements it. Update this table, not the paper, when a script
is renamed, split, or replaced.

| Stage | Script | Role | Writes |
|---|---|---|---|
| 1 | `01_fetch_pmc.py` | Fetch PMC-OA article XML per year via NCBI E-utilities | raw XML files |
| 2 | `02_parse_to_sqlite.py` | Parse JATS XML into the relational schema | `journal`, `article`, `author`, `affiliation`, `article_author`, `author_affiliation`, `keyword`, `article_keyword`, `data_availability`, `da_link`, `supplementary_material`, `accession_number` |
| 2b | `extract_ethics.py` | Extract ethics statements, parallel to Stage 2 | `ethics_statement`, `ethics_sub_type` |
| — | `extract_code_availability.py` | Extract standalone Code/Software Availability sections (same six-level cascade as Stage 2b; closes a gap in Stage 2's DAS extractor, which requires the word "data") | `code_availability`, `code_link` |
| — | `derive_year.py` | Resolve one authoritative `article.year` from `pub_year`/`epub_date`/`accepted_date`/`received_date` | `article.year` |
| 3 | `03_classify_repositories.py` | Classify every DAS URL against the repository knowledge base | `repo_classification` |
| 4 | `04_categorise_das.py` | Assign DAS category and proxy data-sharing score | `das_category` |
| 5 | `05_assess_fuji.py` (+ `05b_retry_failed_fuji.py` for retries) | Automated FAIR assessment via self-hosted F-UJI | `fuji_assessment`, `fuji_metric`, `fuji_article_link` |
| 6 | `06_enrich_metadata.py` | Discipline (MeSH), open-access status (DOAJ), funder attribution | `article_mesh`, `article_discipline`, `journal_oa`, `article_funder` |
| 7 | `07_export_knowledge_graph.py` | Serialise the database as an RDF knowledge graph | external RDF export only, not used in the statistical analyses |
| 8 | `08_check_link_rot.py` | Re-check DAS/supplementary/accession URLs for reachability | `link_rot`, `link_rot_run`, not used in the statistical analyses |
| — | `compute_gender.py` | Offline name-based gender inference per distinct given name (used by `notebooks/06_gender_analysis.ipynb`, `notebooks/12_journal_profiles.ipynb`) | `given_name_gender` |
| — | `compute_gender_extras.py` | Offline precomputation of per-author productivity trajectories and per-article international-collaboration country counts for `notebooks/06_gender_analysis.ipynb` (moved out of the notebook after it hit `DeadKernelError` on the live joins) | cached CSVs under `results/fair_gender_exports/` |

Notebook-level refinements that are **not** pipeline stages (diamond/gold/hybrid/subscription
OA tiers, JATS4R compliance tiers, reproducibility signals R1–R6, the mandate-compliance
rubric) are implemented directly in the corresponding `notebooks/*.ipynb` file and are
described in the Methods section without naming that file, for the same reason.
