# Data availability

This study draws on and archives two distinct artefacts, each with its own DOI and
licence, reflecting their different natures (third-party raw input vs. our own derived
database):

- **Processed database.** The full SQLite database (`pmc_fair.db`, ~30 GB) that backs
  every number, table, and figure reported in the manuscript, together with the
  article-/author-level CSV exports that are too large for this repository (tens of
  MB to >1 GB each — one per notebook: `article_oa_scores.csv`, `article_license.csv`,
  `article_ethics.csv`, `article_compliance.csv` ×2, `nb_flags_per_article.csv`,
  `article_repro_flags.csv`, `author_gender_scores.csv`) is archived separately at
  Zenodo, [DOI: 10.5281/zenodo.24209894](https://doi.org/10.5281/zenodo.24209894),
  released under the **CC0 1.0 Universal** Public Domain Dedication.

Once downloaded, place the database at `data/processed/pmc_fair.db` and the
article-level CSVs back into their corresponding `results/fair_*_exports/`
subdirectories to reproduce the notebooks exactly as run.

## Regenerating the database from scratch

The database is fully reproducible from the raw PMC-OA XML via the pipeline
in `pipeline/` (see the top-level `README.md` and `pipeline/README.md`).
Raw XML is fetched directly from PMC/NCBI and is not redistributed here,
since it is already an open, independently citable source
(`pipeline/01_fetch_pmc.py`).

## Schema

See the top-level `README.md` for the full table list, and
`pipeline/README.md` for which pipeline stage writes which table.
