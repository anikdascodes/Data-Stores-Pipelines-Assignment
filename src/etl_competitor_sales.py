#!/usr/bin/env python3
"""
ETL Pipeline for Competitor Sales Data
=======================================
This script performs ETL operations on competitor sales data including:
1. Data ingestion from CSV
2. Data cleaning (trim, type conversion, date standardization)
3. Data quality checks (6 validation rules)
4. Quarantine handling for failed records
5. Writing clean data to Apache Hudi table

Author: E-commerce Data Engineering Team
Date: 2025-11-13
"""

import argparse
import logging
import sys
from datetime import datetime
from pathlib import Path

import yaml
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, DoubleType, IntegerType

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def get_spark_session(app_name: str) -> SparkSession:
    """
    Create and configure Spark session with Hudi support.

    Args:
        app_name: Name of the Spark application

    Returns:
        Configured SparkSession
    """
    spark = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
        .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    logger.info(f"Spark session created: {app_name}")
    return spark


def load_config(config_path: str) -> dict:
    """
    Load YAML configuration file.

    Args:
        config_path: Path to YAML config file

    Returns:
        Configuration dictionary
    """
    try:
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        logger.info(f"Configuration loaded from {config_path}")
        return config
    except Exception as e:
        logger.error(f"Failed to load configuration: {e}")
        sys.exit(1)


def clean_competitor_sales(df, spark):
    """
    Clean competitor sales data:
    - Trim whitespace from string columns
    - Convert data types
    - Standardize date format
    - Fill missing numeric fields with 0

    Args:
        df: Raw DataFrame
        spark: SparkSession

    Returns:
        Cleaned DataFrame
    """
    logger.info("Starting data cleaning...")

    # Trim string columns
    df_clean = df.withColumn("item_id", F.trim(F.col("item_id")))
    df_clean = df_clean.withColumn("seller_id", F.trim(F.col("seller_id")))

    # Convert data types
    df_clean = df_clean.withColumn("units_sold", F.col("units_sold").cast(IntegerType()))
    df_clean = df_clean.withColumn("revenue", F.col("revenue").cast(DoubleType()))
    df_clean = df_clean.withColumn("marketplace_price", F.col("marketplace_price").cast(DoubleType()))

    # Parse and standardize sale_date
    df_clean = df_clean.withColumn("sale_date", F.to_date(F.col("sale_date"), "yyyy-MM-dd"))

    # Fill missing numeric values with 0
    df_clean = df_clean.fillna({
        "units_sold": 0,
        "revenue": 0.0,
        "marketplace_price": 0.0
    })

    logger.info(f"Data cleaning completed. Records: {df_clean.count()}")
    return df_clean


def apply_dq_checks(df, spark):
    """
    Apply data quality checks for competitor sales:
    1. item_id IS NOT NULL
    2. seller_id IS NOT NULL
    3. units_sold >= 0
    4. revenue >= 0
    5. marketplace_price >= 0
    6. sale_date IS NOT NULL AND sale_date <= current_date()

    Args:
        df: Cleaned DataFrame
        spark: SparkSession

    Returns:
        Tuple of (good_df, bad_df with failure reasons)
    """
    logger.info("Applying data quality checks...")

    current_date = datetime.now().date()

    # Add DQ validation columns
    df_dq = df.withColumn(
        "dq_item_id_null",
        F.when(F.col("item_id").isNull() | (F.trim(F.col("item_id")) == ""), F.lit("item_id_is_null")).otherwise(F.lit(None))
    ).withColumn(
        "dq_seller_id_null",
        F.when(F.col("seller_id").isNull() | (F.trim(F.col("seller_id")) == ""), F.lit("seller_id_is_null")).otherwise(F.lit(None))
    ).withColumn(
        "dq_units_sold_negative",
        F.when((F.col("units_sold").isNull()) | (F.col("units_sold") < 0), F.lit("units_sold_negative_or_null")).otherwise(F.lit(None))
    ).withColumn(
        "dq_revenue_negative",
        F.when((F.col("revenue").isNull()) | (F.col("revenue") < 0), F.lit("revenue_negative_or_null")).otherwise(F.lit(None))
    ).withColumn(
        "dq_marketplace_price_negative",
        F.when((F.col("marketplace_price").isNull()) | (F.col("marketplace_price") < 0), F.lit("marketplace_price_negative_or_null")).otherwise(F.lit(None))
    ).withColumn(
        "dq_sale_date_invalid",
        F.when(
            (F.col("sale_date").isNull()) | (F.col("sale_date") > F.lit(current_date)),
            F.lit("sale_date_null_or_future")
        ).otherwise(F.lit(None))
    )

    # Combine all DQ failure reasons
    df_dq = df_dq.withColumn(
        "dq_failure_reason",
        F.concat_ws(", ",
            F.col("dq_item_id_null"),
            F.col("dq_seller_id_null"),
            F.col("dq_units_sold_negative"),
            F.col("dq_revenue_negative"),
            F.col("dq_marketplace_price_negative"),
            F.col("dq_sale_date_invalid")
        )
    )

    # Separate good and bad records
    good_df = df_dq.filter((F.col("dq_failure_reason") == "") | (F.col("dq_failure_reason").isNull())) \
                   .drop("dq_item_id_null", "dq_seller_id_null", "dq_units_sold_negative",
                         "dq_revenue_negative", "dq_marketplace_price_negative", "dq_sale_date_invalid",
                         "dq_failure_reason")

    bad_df = df_dq.filter((F.col("dq_failure_reason") != "") & (F.col("dq_failure_reason").isNotNull())) \
                  .drop("dq_item_id_null", "dq_seller_id_null", "dq_units_sold_negative",
                        "dq_revenue_negative", "dq_marketplace_price_negative", "dq_sale_date_invalid")

    good_count = good_df.count()
    bad_count = bad_df.count()

    logger.info(f"DQ checks completed - Good records: {good_count}, Bad records: {bad_count}")

    return good_df, bad_df


def write_to_quarantine(df, quarantine_path: str, dataset_name: str):
    """
    Write failed records to quarantine zone with metadata.

    Args:
        df: DataFrame with DQ failures
        quarantine_path: Path to quarantine directory
        dataset_name: Name of the dataset
    """
    if df.count() == 0:
        logger.info("No records to quarantine")
        return

    # Add quarantine metadata
    df_quarantine = df.withColumn("dataset_name", F.lit(dataset_name)) \
                      .withColumn("quarantine_timestamp", F.lit(datetime.now().strftime("%Y-%m-%d %H:%M:%S")))

    # Reorder columns
    cols = ["dataset_name", "quarantine_timestamp", "dq_failure_reason"] + \
           [c for c in df.columns if c != "dq_failure_reason"]
    df_quarantine = df_quarantine.select(cols)

    # Write to quarantine
    try:
        df_quarantine.coalesce(1).write.mode("append").option("header", "true").csv(quarantine_path)
        logger.info(f"Quarantine records written to {quarantine_path}")
    except Exception as e:
        logger.error(f"Failed to write quarantine records: {e}")


def write_to_hudi(df, hudi_path: str, table_name: str):
    """
    Write cleaned data to Apache Hudi table.

    Args:
        df: Clean DataFrame
        hudi_path: Path to Hudi table
        table_name: Name of the Hudi table
    """
    logger.info(f"Writing to Hudi table: {table_name}")

    # Add timestamp for precombine
    df_hudi = df.withColumn("ts", F.lit(datetime.now().strftime("%Y-%m-%d %H:%M:%S")))

    # Hudi configuration with composite key (includes sale_date to preserve historical records)
    hudi_options = {
        'hoodie.table.name': table_name,
        'hoodie.datasource.write.recordkey.field': 'seller_id,item_id,sale_date',
        'hoodie.datasource.write.precombine.field': 'ts',
        'hoodie.datasource.write.table.type': 'COPY_ON_WRITE',
        'hoodie.datasource.write.operation': 'upsert',
        'hoodie.upsert.shuffle.parallelism': 2,
        'hoodie.insert.shuffle.parallelism': 2
    }

    try:
        df_hudi.write.format("hudi") \
            .options(**hudi_options) \
            .mode("overwrite") \
            .save(hudi_path)
        logger.info(f"Successfully wrote {df_hudi.count()} records to Hudi table")
    except Exception as e:
        logger.error(f"Failed to write to Hudi: {e}")
        raise


def main():
    """Main ETL pipeline execution."""
    parser = argparse.ArgumentParser(description='ETL pipeline for competitor sales data')
    parser.add_argument('--config', required=True, help='Path to YAML configuration file')
    args = parser.parse_args()

    logger.info("=" * 80)
    logger.info("Starting Competitor Sales ETL Pipeline")
    logger.info("=" * 80)

    # Load configuration
    config = load_config(args.config)
    competitor_config = config['competitor_sales']

    input_path = competitor_config['input_path']
    hudi_output_path = competitor_config['hudi_output_path']
    quarantine_path = competitor_config['quarantine_path']

    # Create Spark session
    spark = get_spark_session("EcommerceRecommendation_CompetitorSales_ETL")

    try:
        # Read raw data
        logger.info(f"Reading data from {input_path}")
        df_raw = spark.read.option("header", "true").csv(input_path)
        logger.info(f"Raw records read: {df_raw.count()}")

        # Clean data
        df_clean = clean_competitor_sales(df_raw, spark)

        # Apply DQ checks
        df_good, df_bad = apply_dq_checks(df_clean, spark)

        # Write bad records to quarantine
        write_to_quarantine(df_bad, quarantine_path, "competitor_sales")

        # Write good records to Hudi
        write_to_hudi(df_good, hudi_output_path, "competitor_sales")

        logger.info("=" * 80)
        logger.info("Competitor Sales ETL Pipeline Completed Successfully")
        logger.info("=" * 80)

    except Exception as e:
        logger.error(f"ETL pipeline failed: {e}")
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
