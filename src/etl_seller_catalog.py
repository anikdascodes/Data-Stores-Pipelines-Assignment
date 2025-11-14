#!/usr/bin/env python3
"""
ETL Pipeline for Seller Catalog Data
=====================================
This script performs ETL operations on seller catalog data including:
1. Data ingestion from CSV
2. Data cleaning (trim, normalize, type conversion)
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
from pyspark.sql.types import DoubleType, IntegerType, StringType

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
    # Check if we're running with Hudi packages (spark-submit)
    import os
    if 'SPARK_SUBMIT_OPTS' in os.environ or any('hudi' in str(arg).lower() for arg in sys.argv):
        logger.info("Detected Hudi packages, creating session with Hudi extensions...")
        spark = (
            SparkSession.builder
            .appName(app_name)
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
            .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog")
            .getOrCreate()
        )
    else:
        logger.info("Creating Spark session without Hudi extensions for testing...")
        # Fallback without Hudi extensions (for direct Python execution)
        spark = (
            SparkSession.builder
            .appName(app_name)
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
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


def clean_seller_catalog(df):
    """
    Clean seller catalog data:
    - Trim whitespace from string columns
    - Normalize category to lowercase
    - Convert data types
    - Fill missing stock_qty with 0

    Args:
        df: Raw DataFrame

    Returns:
        Cleaned DataFrame
    """
    logger.info("Starting data cleaning...")

    # Trim whitespace from all string columns
    df_clean = df.select([
        F.trim(F.col(c)).alias(c) if df.schema[c].dataType == StringType() else F.col(c)
        for c in df.columns
    ])

    # Normalize category to lowercase
    df_clean = df_clean.withColumn("category", F.lower(F.trim(F.col("category"))))

    # Normalize item_name
    df_clean = df_clean.withColumn("item_name", F.trim(F.col("item_name")))

    # Convert data types explicitly
    df_clean = df_clean.withColumn("marketplace_price", F.col("marketplace_price").cast(DoubleType()))
    df_clean = df_clean.withColumn("stock_qty", F.col("stock_qty").cast(IntegerType()))

    # Fill missing stock_qty with 0
    df_clean = df_clean.fillna({"stock_qty": 0})

    logger.info(f"Data cleaning completed. Records: {df_clean.count()}")
    return df_clean


def apply_dq_checks(df):
    """
    Apply data quality checks for seller catalog:
    1. seller_id IS NOT NULL
    2. item_id IS NOT NULL
    3. marketplace_price >= 0
    4. stock_qty >= 0
    5. item_name IS NOT NULL
    6. category IS NOT NULL

    Args:
        df: Cleaned DataFrame

    Returns:
        Tuple of (good_df, bad_df with failure reasons)
    """
    logger.info("Applying data quality checks...")

    # Add DQ validation columns
    df_dq = df.withColumn(
        "dq_seller_id_null",
        F.when(F.col("seller_id").isNull() | (F.trim(F.col("seller_id")) == ""), F.lit("seller_id_is_null")).otherwise(F.lit(None))
    ).withColumn(
        "dq_item_id_null",
        F.when(F.col("item_id").isNull() | (F.trim(F.col("item_id")) == ""), F.lit("item_id_is_null")).otherwise(F.lit(None))
    ).withColumn(
        "dq_marketplace_price_negative",
        F.when((F.col("marketplace_price").isNull()) | (F.col("marketplace_price") < 0), F.lit("marketplace_price_negative_or_null")).otherwise(F.lit(None))
    ).withColumn(
        "dq_stock_qty_negative",
        F.when((F.col("stock_qty").isNull()) | (F.col("stock_qty") < 0), F.lit("stock_qty_negative_or_null")).otherwise(F.lit(None))
    ).withColumn(
        "dq_item_name_null",
        F.when(F.col("item_name").isNull() | (F.trim(F.col("item_name")) == ""), F.lit("item_name_is_null")).otherwise(F.lit(None))
    ).withColumn(
        "dq_category_null",
        F.when(F.col("category").isNull() | (F.trim(F.col("category")) == ""), F.lit("category_is_null")).otherwise(F.lit(None))
    )

    # Combine all DQ failure reasons
    df_dq = df_dq.withColumn(
        "dq_failure_reason",
        F.concat_ws(", ",
            F.col("dq_seller_id_null"),
            F.col("dq_item_id_null"),
            F.col("dq_marketplace_price_negative"),
            F.col("dq_stock_qty_negative"),
            F.col("dq_item_name_null"),
            F.col("dq_category_null")
        )
    )

    # Separate good and bad records
    good_df = df_dq.filter((F.col("dq_failure_reason") == "") | (F.col("dq_failure_reason").isNull())) \
                   .drop("dq_seller_id_null", "dq_item_id_null", "dq_marketplace_price_negative",
                         "dq_stock_qty_negative", "dq_item_name_null", "dq_category_null", "dq_failure_reason")

    bad_df = df_dq.filter((F.col("dq_failure_reason") != "") & (F.col("dq_failure_reason").isNotNull())) \
                  .drop("dq_seller_id_null", "dq_item_id_null", "dq_marketplace_price_negative",
                        "dq_stock_qty_negative", "dq_item_name_null", "dq_category_null")

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

    # Reorder columns to have metadata first
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
    Write cleaned data to Apache Hudi table or Parquet as fallback.

    Args:
        df: Clean DataFrame
        hudi_path: Path to Hudi table
        table_name: Name of the Hudi table
    """
    logger.info(f"Writing to Hudi table: {table_name}")

    # Add timestamp for precombine
    df_hudi = df.withColumn("ts", F.lit(datetime.now().strftime("%Y-%m-%d %H:%M:%S")))

    # Hudi configuration
    hudi_options = {
        'hoodie.table.name': table_name,
        'hoodie.datasource.write.recordkey.field': 'seller_id,item_id',
        'hoodie.datasource.write.precombine.field': 'ts',
        'hoodie.datasource.write.partitionpath.field': 'category',
        'hoodie.datasource.write.table.type': 'COPY_ON_WRITE',
        'hoodie.datasource.write.operation': 'upsert',
        'hoodie.datasource.write.hive_style_partitioning': 'true',
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
        logger.warning(f"Failed to write to Hudi: {e}")
        logger.info("Falling back to Parquet format for testing...")
        try:
            # Fallback to Parquet with partitioning
            df_hudi.write \
                .mode("overwrite") \
                .partitionBy("category") \
                .parquet(hudi_path)
            logger.info(f"Successfully wrote {df_hudi.count()} records to Parquet format")
        except Exception as e2:
            logger.error(f"Failed to write to Parquet: {e2}")
            raise


def main():
    """Main ETL pipeline execution."""
    parser = argparse.ArgumentParser(description='ETL pipeline for seller catalog data')
    parser.add_argument('--config', required=True, help='Path to YAML configuration file')
    args = parser.parse_args()

    logger.info("=" * 80)
    logger.info("Starting Seller Catalog ETL Pipeline")
    logger.info("=" * 80)

    # Load configuration
    config = load_config(args.config)
    seller_config = config['seller_catalog']

    input_path = seller_config['input_path']
    hudi_output_path = seller_config['hudi_output_path']
    quarantine_path = seller_config['quarantine_path']

    # Create Spark session
    spark = get_spark_session("EcommerceRecommendation_SellerCatalog_ETL")

    try:
        # Read raw data
        logger.info(f"Reading data from {input_path}")
        df_raw = spark.read.option("header", "true").csv(input_path)
        logger.info(f"Raw records read: {df_raw.count()}")

        # Clean data
        df_clean = clean_seller_catalog(df_raw)

        # Apply DQ checks
        df_good, df_bad = apply_dq_checks(df_clean)

        # Write bad records to quarantine
        write_to_quarantine(df_bad, quarantine_path, "seller_catalog")

        # Write good records to Hudi
        write_to_hudi(df_good, hudi_output_path, "seller_catalog")

        logger.info("=" * 80)
        logger.info("Seller Catalog ETL Pipeline Completed Successfully")
        logger.info("=" * 80)

    except Exception as e:
        logger.error(f"ETL pipeline failed: {e}")
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
