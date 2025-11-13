#!/bin/bash
################################################################################
# Master Pipeline Execution Script
#
# This script runs the complete E-commerce Recommendation pipeline:
# 1. ETL: Seller Catalog
# 2. ETL: Company Sales
# 3. ETL: Competitor Sales
# 4. Consumption: Recommendations
################################################################################

set -e  # Exit on error

echo "================================================================================"
echo "E-COMMERCE SELLER RECOMMENDATION SYSTEM - FULL PIPELINE EXECUTION"
echo "================================================================================"
echo ""
echo "This script will execute all ETL and consumption pipelines sequentially."
echo ""

# Record start time
START_TIME=$(date +%s)

# Run ETL pipelines
echo "Step 1/4: Running Seller Catalog ETL..."
echo "--------------------------------------------------------------------------------"
/app/scripts/etl_seller_catalog_spark_submit.sh
echo ""

echo "Step 2/4: Running Company Sales ETL..."
echo "--------------------------------------------------------------------------------"
/app/scripts/etl_company_sales_spark_submit.sh
echo ""

echo "Step 3/4: Running Competitor Sales ETL..."
echo "--------------------------------------------------------------------------------"
/app/scripts/etl_competitor_sales_spark_submit.sh
echo ""

echo "Step 4/4: Running Consumption Recommendation..."
echo "--------------------------------------------------------------------------------"
/app/scripts/consumption_recommendation_spark_submit.sh
echo ""

# Calculate execution time
END_TIME=$(date +%s)
DURATION=$((END_TIME - START_TIME))
MINUTES=$((DURATION / 60))
SECONDS=$((DURATION % 60))

echo "================================================================================"
echo "FULL PIPELINE EXECUTION COMPLETED SUCCESSFULLY!"
echo "================================================================================"
echo "Total execution time: ${MINUTES} minutes ${SECONDS} seconds"
echo ""
echo "Output locations:"
echo "  - Seller Catalog Hudi: /app/data/processed/seller_catalog_hudi/"
echo "  - Company Sales Hudi: /app/data/processed/company_sales_hudi/"
echo "  - Competitor Sales Hudi: /app/data/processed/competitor_sales_hudi/"
echo "  - Recommendations CSV: /app/data/output/recommendations/"
echo "  - Quarantine Data: /app/data/quarantine/"
echo "================================================================================"
