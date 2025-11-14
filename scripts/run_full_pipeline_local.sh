#!/bin/bash

# E-commerce Seller Recommendation System - Full Pipeline Execution (Local)
# This script executes all ETL and consumption pipelines sequentially for local development

set -e  # Exit on any error

# Check if config file is provided
if [ $# -eq 0 ]; then
    echo "Usage: $0 <config_file>"
    echo "Example: $0 configs/ecomm_prod.yml"
    exit 1
fi

CONFIG_FILE="$1"

# Validate config file exists
if [ ! -f "$CONFIG_FILE" ]; then
    echo "Error: Configuration file '$CONFIG_FILE' not found!"
    exit 1
fi

echo "================================================================================"
echo "E-COMMERCE SELLER RECOMMENDATION SYSTEM - FULL PIPELINE EXECUTION (LOCAL)"
echo "================================================================================"
echo ""
echo "Configuration: $CONFIG_FILE"
echo "Start Time: $(date)"
echo ""
echo "This script will execute all ETL and consumption pipelines sequentially."
echo ""

# Record start time
START_TIME=$(date +%s)

# Get the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

# Run ETL pipelines
echo "Step 1/4: Running Seller Catalog ETL..."
echo "--------------------------------------------------------------------------------"
cd "$PROJECT_DIR"
python src/etl_seller_catalog.py --config "$CONFIG_FILE"
echo ""

echo "Step 2/4: Running Company Sales ETL..."
echo "--------------------------------------------------------------------------------"
python src/etl_company_sales.py --config "$CONFIG_FILE"
echo ""

echo "Step 3/4: Running Competitor Sales ETL..."
echo "--------------------------------------------------------------------------------"
python src/etl_competitor_sales.py --config "$CONFIG_FILE"
echo ""

echo "Step 4/4: Running Consumption Recommendation..."
echo "--------------------------------------------------------------------------------"
python src/consumption_recommendation.py --config "$CONFIG_FILE"
echo ""

# Calculate execution time
END_TIME=$(date +%s)
EXECUTION_TIME=$((END_TIME - START_TIME))
MINUTES=$((EXECUTION_TIME / 60))
SECONDS=$((EXECUTION_TIME % 60))

echo "================================================================================"
echo "PIPELINE EXECUTION COMPLETED SUCCESSFULLY!"
echo "================================================================================"
echo "Total Execution Time: ${MINUTES}m ${SECONDS}s"
echo "End Time: $(date)"
echo ""
echo "Output locations:"
echo "- Processed Data: data/processed/"
echo "- Recommendations: data/output/recommendations/"
echo "- Quarantine Data: data/quarantine/"
echo ""
echo "Next steps:"
echo "1. Review the generated recommendations in data/output/recommendations/"
echo "2. Check logs for any warnings or performance metrics"
echo "3. Validate data quality results in quarantine directories"
echo "================================================================================"