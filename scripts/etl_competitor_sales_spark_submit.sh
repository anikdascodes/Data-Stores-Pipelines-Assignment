#!/bin/bash
################################################################################
# Spark Submit Script for Competitor Sales ETL Pipeline
#
# This script submits the competitor sales ETL job to Spark with Hudi dependencies
################################################################################

set -e  # Exit on error

echo "=========================================="
echo "Competitor Sales ETL - Spark Submit"
echo "=========================================="

# Configuration
APP_DIR="/app"
CONFIG_FILE="${APP_DIR}/configs/ecomm_prod.yml"
PYTHON_SCRIPT="${APP_DIR}/src/etl_competitor_sales.py"

# Hudi and Hadoop packages
PACKAGES="org.apache.hudi:hudi-spark3.5-bundle_2.12:0.15.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262"

# Spark configuration
SPARK_CONF=(
    "--conf" "spark.serializer=org.apache.spark.serializer.KryoSerializer"
    "--conf" "spark.sql.legacy.timeParserPolicy=LEGACY"
    "--conf" "spark.driver.memory=4g"
    "--conf" "spark.executor.memory=4g"
)

# Execute spark-submit
spark-submit \
    --packages ${PACKAGES} \
    "${SPARK_CONF[@]}" \
    ${PYTHON_SCRIPT} \
    --config ${CONFIG_FILE}

echo "=========================================="
echo "Competitor Sales ETL - Completed"
echo "=========================================="
