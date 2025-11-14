#!/usr/bin/env python3
"""
Consumption Layer: Seller Recommendation System
================================================
This script generates recommendations for sellers by:
1. Reading Hudi tables (seller catalog, company sales, competitor sales)
2. Identifying top-10 selling items per category from company sales
3. Identifying top-10 selling items from competitor sales
4. Finding items missing from each seller's catalog
5. Calculating expected units sold and revenue
6. Outputting recommendations to CSV

Author: E-commerce Data Engineering Team
Date: 2025-11-13
"""

import argparse
import logging
import sys
from pathlib import Path

import yaml
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

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


def read_hudi_table(spark, hudi_path: str, table_name: str):
    """
    Read Hudi table or Parquet as fallback.

    Args:
        spark: SparkSession
        hudi_path: Path to Hudi table
        table_name: Name of table (for logging)

    Returns:
        DataFrame
    """
    logger.info(f"Reading table: {table_name} from {hudi_path}")
    try:
        # Try reading as Hudi first
        df = spark.read.format("hudi").load(hudi_path)
        count = df.count()
        logger.info(f"Successfully read {count} records from Hudi table {table_name}")
        return df
    except Exception as e:
        logger.warning(f"Failed to read as Hudi table {table_name}: {e}")
        logger.info(f"Attempting to read as Parquet format...")
        try:
            # Fallback to Parquet
            df = spark.read.parquet(hudi_path)
            count = df.count()
            logger.info(f"Successfully read {count} records from Parquet format {table_name}")
            return df
        except Exception as e2:
            logger.error(f"Failed to read table {table_name} in both Hudi and Parquet formats: {e2}")
            raise


def get_top_selling_company_items(company_sales_df, seller_catalog_df, top_n: int = 10):
    """
    Identify top N selling items per category from company sales.

    Args:
        company_sales_df: Company sales Hudi DataFrame
        seller_catalog_df: Seller catalog Hudi DataFrame (for category mapping)
        top_n: Number of top items per category

    Returns:
        DataFrame with top selling items per category
    """
    logger.info(f"Identifying top {top_n} selling items per category from company sales...")

    # Aggregate company sales by item_id
    company_agg = company_sales_df.groupBy("item_id").agg(
        F.sum("units_sold").alias("total_units_sold"),
        F.sum("revenue").alias("total_revenue")
    )

    # Join with seller catalog to get category and item_name
    # Get unique item, category, item_name mappings
    item_category = seller_catalog_df.select("item_id", "category", "item_name", "marketplace_price") \
        .dropDuplicates(["item_id"])

    company_with_category = company_agg.join(item_category, "item_id", "left")

    # Filter out items without category
    company_with_category = company_with_category.filter(F.col("category").isNotNull())

    # Rank items within each category by total units sold
    window_spec = Window.partitionBy("category").orderBy(F.desc("total_units_sold"))

    top_items = company_with_category.withColumn("rank", F.row_number().over(window_spec)) \
        .filter(F.col("rank") <= top_n) \
        .drop("rank")

    logger.info(f"Found {top_items.count()} top-selling items across all categories")
    return top_items


def get_top_selling_competitor_items(competitor_sales_df, top_n: int = 10):
    """
    Identify top N selling items from competitor sales.

    Args:
        competitor_sales_df: Competitor sales Hudi DataFrame
        top_n: Number of top items

    Returns:
        DataFrame with top selling competitor items
    """
    logger.info(f"Identifying top {top_n} selling items from competitor sales...")

    # Aggregate competitor sales by item_id
    competitor_agg = competitor_sales_df.groupBy("item_id").agg(
        F.sum("units_sold").alias("total_units_sold"),
        F.sum("revenue").alias("total_revenue"),
        F.avg("marketplace_price").alias("avg_marketplace_price")
    )

    # Rank by total units sold
    window_spec = Window.orderBy(F.desc("total_units_sold"))

    top_competitor_items = competitor_agg.withColumn("rank", F.row_number().over(window_spec)) \
        .filter(F.col("rank") <= top_n) \
        .drop("rank")

    logger.info(f"Found {top_competitor_items.count()} top-selling competitor items")
    return top_competitor_items


def generate_recommendations(seller_catalog_df, top_company_items, top_competitor_items, competitor_sales_df):
    """
    Generate recommendations for each seller based on missing items.

    Args:
        seller_catalog_df: Seller catalog Hudi DataFrame
        top_company_items: Top selling items from company
        top_competitor_items: Top selling competitor items
        competitor_sales_df: Competitor sales Hudi DataFrame (for seller count calculation)

    Returns:
        DataFrame with recommendations
    """
    logger.info("Generating recommendations for sellers...")

    # Get unique sellers
    sellers = seller_catalog_df.select("seller_id").distinct()

    # Get items each seller currently has
    seller_items = seller_catalog_df.select("seller_id", "item_id")

    # Recommendation 1: Missing top company items
    # Cross join sellers with top company items
    all_combinations_company = sellers.crossJoin(
        top_company_items.select("item_id", "item_name", "category",
                                  "marketplace_price", "total_units_sold")
    )

    # Anti-join to find missing items (items not in seller's catalog)
    missing_company_items = all_combinations_company.join(
        seller_items,
        (all_combinations_company.seller_id == seller_items.seller_id) &
        (all_combinations_company.item_id == seller_items.item_id),
        "left_anti"
    )

    # Calculate number of sellers selling each item (for expected units calculation)
    sellers_per_item = seller_catalog_df.groupBy("item_id").agg(
        F.countDistinct("seller_id").alias("num_sellers")
    )

    # Join to get seller count
    missing_company_items = missing_company_items.join(sellers_per_item, "item_id", "left")

    # Calculate expected_units_sold and expected_revenue
    missing_company_items = missing_company_items.withColumn(
        "expected_units_sold",
        F.when(F.col("num_sellers") > 0, F.col("total_units_sold") / F.col("num_sellers"))
         .otherwise(F.col("total_units_sold"))
    ).withColumn(
        "expected_revenue",
        F.col("expected_units_sold") * F.col("marketplace_price")
    ).withColumn(
        "recommendation_source",
        F.lit("company_top_items")
    )

    # Select final columns
    recommendations_company = missing_company_items.select(
        "seller_id",
        "item_id",
        "item_name",
        "category",
        F.col("marketplace_price").alias("market_price"),
        "expected_units_sold",
        "expected_revenue",
        "recommendation_source"
    )

    # Recommendation 2: Missing top competitor items
    # For competitor items, we need to get item details from competitor data
    all_combinations_competitor = sellers.crossJoin(
        top_competitor_items.select("item_id", "total_units_sold", "avg_marketplace_price")
    )

    # Anti-join to find missing items
    missing_competitor_items = all_combinations_competitor.join(
        seller_items,
        (all_combinations_competitor.seller_id == seller_items.seller_id) &
        (all_combinations_competitor.item_id == seller_items.item_id),
        "left_anti"
    )

    # Get item details from seller catalog (if available) or use competitor data
    item_details = seller_catalog_df.select(
        "item_id", "item_name", "category"
    ).dropDuplicates(["item_id"])

    missing_competitor_items = missing_competitor_items.join(item_details, "item_id", "left")

    # Fill missing item_name and category
    missing_competitor_items = missing_competitor_items.fillna({
        "item_name": "Unknown",
        "category": "uncategorized"
    })

    # Calculate sellers per item for competitor items
    competitor_sellers_per_item = competitor_sales_df.groupBy("item_id").agg(
        F.countDistinct("seller_id").alias("num_competitor_sellers")
    )

    missing_competitor_items = missing_competitor_items.join(
        competitor_sellers_per_item, "item_id", "left"
    )

    # Calculate expected metrics
    missing_competitor_items = missing_competitor_items.withColumn(
        "expected_units_sold",
        F.when(F.col("num_competitor_sellers") > 0,
               F.col("total_units_sold") / F.col("num_competitor_sellers"))
         .otherwise(F.col("total_units_sold"))
    ).withColumn(
        "expected_revenue",
        F.col("expected_units_sold") * F.col("avg_marketplace_price")
    ).withColumn(
        "recommendation_source",
        F.lit("competitor_top_items")
    )

    recommendations_competitor = missing_competitor_items.select(
        "seller_id",
        "item_id",
        "item_name",
        "category",
        F.col("avg_marketplace_price").alias("market_price"),
        "expected_units_sold",
        "expected_revenue",
        "recommendation_source"
    )

    # Union both recommendations
    all_recommendations = recommendations_company.union(recommendations_competitor)

    # Remove duplicates (same item recommended from both sources)
    # Keep the one with higher expected revenue
    window_dedup = Window.partitionBy("seller_id", "item_id").orderBy(F.desc("expected_revenue"))

    final_recommendations = all_recommendations.withColumn("row_num", F.row_number().over(window_dedup)) \
        .filter(F.col("row_num") == 1) \
        .drop("row_num")

    # Sort by seller and expected revenue
    final_recommendations = final_recommendations.orderBy("seller_id", F.desc("expected_revenue"))

    logger.info(f"Generated {final_recommendations.count()} recommendations")
    return final_recommendations


def write_recommendations(df, output_path: str):
    """
    Write recommendations to CSV.

    Args:
        df: Recommendations DataFrame
        output_path: Path to output CSV
    """
    logger.info(f"Writing recommendations to {output_path}")

    try:
        # Ensure output directory exists
        output_dir = Path(output_path).parent
        output_dir.mkdir(parents=True, exist_ok=True)

        # Write to CSV
        df.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_path)
        logger.info(f"Successfully wrote recommendations to {output_path}")
    except Exception as e:
        logger.error(f"Failed to write recommendations: {e}")
        raise


def main():
    """Main consumption pipeline execution."""
    parser = argparse.ArgumentParser(description='Consumption pipeline for seller recommendations')
    parser.add_argument('--config', required=True, help='Path to YAML configuration file')
    args = parser.parse_args()

    logger.info("=" * 80)
    logger.info("Starting Seller Recommendation Consumption Pipeline")
    logger.info("=" * 80)

    # Load configuration
    config = load_config(args.config)
    rec_config = config['recommendation']
    top_n = rec_config.get('top_n_items', 10)

    seller_catalog_path = rec_config['seller_catalog_hudi']
    company_sales_path = rec_config['company_sales_hudi']
    competitor_sales_path = rec_config['competitor_sales_hudi']
    output_csv_path = rec_config['output_csv']

    # Create Spark session
    spark = get_spark_session("EcommerceRecommendation_Consumption")

    try:
        # Read Hudi tables
        seller_catalog_df = read_hudi_table(spark, seller_catalog_path, "seller_catalog")
        company_sales_df = read_hudi_table(spark, company_sales_path, "company_sales")
        competitor_sales_df = read_hudi_table(spark, competitor_sales_path, "competitor_sales")

        # Get top selling items
        top_company_items = get_top_selling_company_items(company_sales_df, seller_catalog_df, top_n)
        top_competitor_items = get_top_selling_competitor_items(competitor_sales_df, top_n)

        # Generate recommendations
        recommendations = generate_recommendations(
            seller_catalog_df,
            top_company_items,
            top_competitor_items,
            competitor_sales_df
        )

        # Write recommendations
        write_recommendations(recommendations, output_csv_path)

        # Display summary
        logger.info("=" * 80)
        logger.info("RECOMMENDATION SUMMARY")
        logger.info("=" * 80)
        recommendations.groupBy("seller_id").count().orderBy("seller_id").show(100, False)

        logger.info("=" * 80)
        logger.info("Seller Recommendation Consumption Pipeline Completed Successfully")
        logger.info("=" * 80)

    except Exception as e:
        logger.error(f"Consumption pipeline failed: {e}")
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
