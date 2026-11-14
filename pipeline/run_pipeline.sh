#!/usr/bin/env bash
# run_pipeline.sh — reproduce the full PMC FAIR study pipeline
# Usage: bash run_pipeline.sh [--years 2011-2025] [--open-access] [--api-key KEY]

set -euo pipefail

YEARS="${YEARS:-2011-2025}"

DB="data/processed/pmc_fair.db"
RAW="data/raw/"
API_KEY="${NCBI_API_KEY:-}"

echo "=== Stage 1: Fetch PMC XML (OA subset) ==="
python pipeline/01_fetch_pmc.py \
    --years "$YEARS" --output-dir "$RAW" \
    --open-access \
    --skip-existing \
    --api-key "$API_KEY"

echo "=== Stage 2: Parse XML → SQLite ==="
python pipeline/02_parse_to_sqlite.py \
    --input "$RAW" --db "$DB"

echo "=== Stage 3: Classify repositories ==="
python pipeline/03_classify_repositories.py --db "$DB" --report

echo "=== Stage 4: Categorise DAS + FAIR scores ==="
python pipeline/04_categorise_das.py --db "$DB"

echo "=== Stage 5: F-UJI assessment (optional — requires local F-UJI) ==="
python pipeline/05_assess_fuji.py --db "$DB" --fuji-url http://localhost:1071
#python pipeline/05b_retry_failed_fuji.py --db "$DB" --fuji-url http://localhost:1071

echo "=== Stage 6: Enrich metadata (MeSH, DOAJ, funders) ==="
python pipeline/06_enrich_metadata.py --db "$DB" ${API_KEY:+--ncbi-key "$API_KEY"}
#python pipeline/06_enrich_metadata.py \
#	--skip-mesh --skip-doaj \
#    --db "$DB" ${API_KEY:+--ncbi-key "$API_KEY"}

echo "=== Pipeline complete. Open notebooks/ to run analysis. ==="
