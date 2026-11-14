# 10 Years of FAIR Principles: How Did Data Sharing Evolve?

[![DOI](https://zenodo.org/badge/1457843498.svg)](https://doi.org/10.5281/zenodo.24231217)

Code, notebooks, and manuscript for a large-scale longitudinal analysis of
data-sharing practice in 6,874,176 biomedical articles from the PubMed
Central Open Access Subset (2011–2025), relative to the 2016 publication of
the FAIR Guiding Principles.

This repository accompanies a manuscript submitted to the *Scientific
Data* "10 Years of FAIR" special issue. It holds the code and data behind
the analysis; the manuscript itself is maintained separately and is not
included here.

## What's in this repository

| Path | Contents |
|---|---|
| `pipeline/` | The data-ingestion pipeline: fetches PMC-OA XML, parses it into a relational SQLite database, and runs all enrichment/classification stages. See [`pipeline/README.md`](pipeline/README.md) for the full stage → script mapping. |
| `notebooks/` | 13 topic-focused analysis notebooks plus `index.ipynb`, a table of contents and batch runner, that query the database and produce every figure and statistic reported in the manuscript. Executed with outputs intact. |
| `figures/` | The complete gallery of figures produced by the notebooks (146 PNGs), including analyses not cited in the condensed manuscript but available as supplementary evidence, plus one hand-drawn pipeline architecture diagram (`fig_pipeline_overview.png`, not notebook output). |
| `results/` | Aggregated, summary-level CSV exports from each notebook (per-year, per-journal, per-country, per-discipline, per-gender, etc.). Article-/author-level exports (tens of MB to >1 GB each) are excluded here — see [`data/README.md`](data/README.md). |
| `data/` | Pointer to where the underlying SQLite database and article-/author-level exports are archived (too large for a git repository) — see [`data/README.md`](data/README.md). |
| `requirements.txt` | Python dependencies for the pipeline and notebooks. |

## Reproducing the analysis

1. Install dependencies: `pip install -r requirements.txt` (Python ≥3.10).
2. Obtain the database: either run the pipeline from scratch (below) or
   download the archived database from the location in
   [`data/README.md`](data/README.md) and place it at
   `data/processed/pmc_fair.db`.
3. Run the pipeline stages in order, or use the driver scripts
   `pipeline/run_pipeline.sh` / `pipeline/run_pipeline_2011-2024.sh`. See
   [`pipeline/README.md`](pipeline/README.md) for what each stage does and
   writes. `pipeline/frun.sbatch` is the SLURM batch script used to run the
   full pipeline (including a self-hosted F-UJI instance) on the HPC cluster
   this study used; adapt it to your own environment.
4. Open any notebook in `notebooks/` and run it against the database — each
   notebook is self-contained and documents which database tables it reads
   at the top of its setup cell.

## Database schema

The pipeline populates a single normalised SQLite database with these
tables: `journal`, `article`, `author`, `affiliation`, `article_author`,
`author_affiliation`, `keyword`, `article_keyword`, `data_availability`,
`da_link`, `supplementary_material`, `accession_number`, `ethics_statement`,
`ethics_sub_type`, `code_availability`, `code_link`, `repo_classification`,
`das_category`, `fuji_assessment`, `fuji_metric`, `fuji_article_link`,
`article_mesh`, `article_discipline`, `journal_oa`, `article_funder`,
`given_name_gender`, `link_rot`, `link_rot_run`. See
[`pipeline/README.md`](pipeline/README.md) for which stage writes which
table.

## Citing this work

If you use this code, data, or findings, please cite the manuscript (full
citation to be added once the special issue is published) and, if you
reference the archived dataset directly, the dataset DOI in
[`data/README.md`](data/README.md).

## License

Code and notebooks in this repository are released under the Apache License 2.0
(see `LICENSE`). The archived dataset has its own license, stated at its
DOI landing page (see `data/README.md`).
