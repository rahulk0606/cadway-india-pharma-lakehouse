#!/bin/bash
set -e
export PYTHONPATH=/app
PACKAGES="com.mysql:mysql-connector-j:8.3.0,org.apache.hadoop:hadoop-aws:3.3.4"

echo "=== Step 1: Generating Indian Pharma Bronze Datasets ==="
python3 data_generator/generate_india_pharma_data.py

echo "=== Step 2: Running Pytest Unit & SCD2 Verification ==="
pytest tests/ -v

echo "=== Step 3: Executing Bronze -> Silver (Cleaning, ABHA Masking, Quarantine) ==="
spark-submit --packages $PACKAGES src/jobs/01_bronze_to_silver.py

echo "=== Step 4: Executing SCD Type 2 Dimension Sync ==="
spark-submit --packages $PACKAGES src/jobs/02_sync_dimensions_scd2.py

echo "=== Step 5: Executing Point-In-Time Fact Loads & Indian KPI Mart ==="
spark-submit --packages $PACKAGES src/jobs/03_silver_to_gold.py

echo "=== Pipeline Completed Successfully! ==="
