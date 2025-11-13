# E-commerce Seller Recommendation System

[![PySpark](https://img.shields.io/badge/PySpark-3.5.0-orange)](https://spark.apache.org/)
[![Apache Hudi](https://img.shields.io/badge/Apache%20Hudi-0.15.0-blue)](https://hudi.apache.org/)
[![Docker](https://img.shields.io/badge/Docker-Enabled-brightgreen)](https://www.docker.com/)
[![Python](https://img.shields.io/badge/Python-3.10-blue)](https://www.python.org/)

A production-ready data engineering pipeline built with **PySpark** and **Apache Hudi** that analyzes e-commerce sales data to generate intelligent seller recommendations. This system identifies top-selling items and recommends missing products to sellers based on market trends.

---

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Features](#features)
- [Prerequisites](#prerequisites)
- [Quick Start](#quick-start)
- [Project Structure](#project-structure)
- [Pipeline Details](#pipeline-details)
- [Configuration](#configuration)
- [Data Quality Rules](#data-quality-rules)
- [Outputs](#outputs)
- [Troubleshooting](#troubleshooting)
- [Advanced Usage](#advanced-usage)

---

## Overview

### Business Problem

In an e-commerce marketplace with multiple sellers:
- Each seller has a unique catalog of items
- Sales data exists for all items across all sellers
- Competitor sales data provides market insights
- **Challenge**: Sellers miss revenue opportunities by not stocking top-selling items

### Solution

This pipeline:
1. **Ingests** and **cleans** data from 3 sources (seller catalogs, company sales, competitor sales)
2. **Validates** data quality with strict DQ rules
3. **Identifies** top 10 selling items per category
4. **Recommends** missing items to each seller with expected revenue projections
5. **Stores** processed data in Apache Hudi tables for incremental updates

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                          RAW DATA LAYER (CSV)                           │
├──────────────────┬───────────────────────┬──────────────────────────────┤
│ Seller Catalog   │   Company Sales       │   Competitor Sales           │
│ (seller_id,      │   (item_id,           │   (item_id, seller_id,       │
│  item_id, ...)   │    units_sold, ...)   │    units_sold, ...)          │
└────────┬─────────┴───────────┬───────────┴──────────┬───────────────────┘
         │                     │                      │
         ▼                     ▼                      ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                       ETL LAYER (PySpark)                               │
├──────────────────┬───────────────────────┬──────────────────────────────┤
│ Data Cleaning    │  DQ Validation (6-4-6 rules)  │  Quarantine Handling│
│ - Trim spaces    │  - Null checks         │  - Failed records          │
│ - Normalize case │  - Negative values     │  - Failure reasons         │
│ - Type conversion│  - Future dates        │  - Timestamp metadata      │
└────────┬─────────┴───────────┬───────────┴──────────┬───────────────────┘
         │                     │                      │
         ▼                     ▼                      ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                   BRONZE/SILVER LAYER (Apache Hudi)                     │
├──────────────────┬───────────────────────┬──────────────────────────────┤
│ seller_catalog   │   company_sales       │   competitor_sales           │
│ _hudi            │   _hudi               │   _hudi                      │
│ (COPY_ON_WRITE)  │   (COPY_ON_WRITE)     │   (COPY_ON_WRITE)            │
└────────┬─────────┴───────────┬───────────┴──────────┬───────────────────┘
         │                     │                      │
         └─────────────────────┴──────────────────────┘
                               │
                               ▼
         ┌─────────────────────────────────────────────┐
         │     CONSUMPTION LAYER (Recommendations)     │
         │                                             │
         │  1. Identify top 10 items per category     │
         │  2. Find missing items for each seller     │
         │  3. Calculate expected revenue             │
         │  4. Generate recommendations CSV           │
         └─────────────────────────────────────────────┘
                               │
                               ▼
         ┌─────────────────────────────────────────────┐
         │       OUTPUT: seller_recommend_data.csv     │
         │  (seller_id, item_id, expected_revenue)    │
         └─────────────────────────────────────────────┘
```

---

## Features

### ETL Capabilities
- **Data Cleaning**: Trim whitespace, normalize casing, handle nulls
- **Type Conversion**: Automatic casting to proper data types
- **Data Quality Validation**: 16 total DQ rules across 3 datasets
- **Quarantine Management**: Failed records isolated with failure reasons
- **Incremental Processing**: Hudi UPSERT mode for idempotent writes
- **Schema Evolution**: Automatic schema updates via Hudi

### Business Analytics
- **Top Items Identification**: Rank items by sales volume per category
- **Market Analysis**: Compare company vs competitor performance
- **Revenue Projection**: Calculate expected units sold and revenue
- **Missing Item Detection**: Anti-join logic to find gaps in catalogs

### Technology Stack
- **PySpark 3.5.0**: Distributed data processing
- **Apache Hudi 0.15.0**: Incremental data lakehouse
- **Docker**: Containerized deployment
- **YAML**: Configuration management
- **Python 3.10**: Modern Python features

---

## Prerequisites

### Required Software
- **Docker**: Version 20.10+
- **Docker Compose**: Version 1.29+
- **Minimum 8GB RAM**: For Spark driver/executor memory
- **Minimum 10GB Disk**: For Docker images and data

### Optional (for non-Docker execution)
- **Java**: 8 or 11 (required for Spark)
- **Python**: 3.8+
- **Apache Spark**: 3.5.0

---

## Quick Start

### 1. Clone the Repository

```bash
git clone <repository-url>
cd Data-Stores-Pipelines-Assignment
```

### 2. Build Docker Image

```bash
docker-compose build
```

**Expected time**: 10-15 minutes (one-time download of Spark, Hudi JARs)

### 3. Run the Full Pipeline

**Option A: Run complete pipeline automatically**

```bash
docker-compose --profile auto-run up
```

**Option B: Interactive mode (recommended for first run)**

```bash
# Start container
docker-compose up -d

# Enter container
docker exec -it ecommerce-recommendation-system /bin/bash

# Inside container, run full pipeline
./scripts/run_full_pipeline.sh

# Or run individual pipelines
./scripts/etl_seller_catalog_spark_submit.sh
./scripts/etl_company_sales_spark_submit.sh
./scripts/etl_competitor_sales_spark_submit.sh
./scripts/consumption_recommendation_spark_submit.sh
```

### 4. View Results

```bash
# Recommendations CSV (inside container)
cat /app/data/output/recommendations/seller_recommend_data.csv/part-*.csv

# Or from host machine
cat data/output/recommendations/seller_recommend_data.csv/part-*.csv

# Quarantine records
cat data/quarantine/*/part-*.csv
```

---

## Project Structure

```
Data-Stores-Pipelines-Assignment/
├── configs/
│   └── ecomm_prod.yml              # Configuration file with all I/O paths
│
├── src/                             # PySpark ETL and consumption scripts
│   ├── etl_seller_catalog.py       # ETL: Seller catalog (6 DQ rules)
│   ├── etl_company_sales.py        # ETL: Company sales (4 DQ rules)
│   ├── etl_competitor_sales.py     # ETL: Competitor sales (6 DQ rules)
│   └── consumption_recommendation.py # Consumption: Generate recommendations
│
├── scripts/                         # Spark-submit shell scripts
│   ├── etl_seller_catalog_spark_submit.sh
│   ├── etl_company_sales_spark_submit.sh
│   ├── etl_competitor_sales_spark_submit.sh
│   ├── consumption_recommendation_spark_submit.sh
│   └── run_full_pipeline.sh        # Master script to run all pipelines
│
├── data/
│   ├── raw/                         # Input CSV files
│   │   ├── seller_catalog/
│   │   ├── company_sales/
│   │   └── competitor_sales/
│   ├── processed/                   # Hudi tables (Bronze/Silver)
│   │   ├── seller_catalog_hudi/
│   │   ├── company_sales_hudi/
│   │   └── competitor_sales_hudi/
│   ├── quarantine/                  # DQ-failed records
│   │   ├── seller_catalog/
│   │   ├── company_sales/
│   │   └── competitor_sales/
│   └── output/                      # Final recommendations
│       └── recommendations/
│
├── Dockerfile                       # Docker image with Spark + Hudi
├── docker-compose.yml               # Container orchestration
├── requirements.txt                 # Python dependencies
├── .gitignore                       # Git ignore rules
└── README.md                        # This file
```

---

## Pipeline Details

### ETL Pipeline 1: Seller Catalog

**Input**: `data/raw/seller_catalog/seller_catalog.csv`

**Schema**:
```
seller_id, item_id, item_name, category, marketplace_price, stock_qty
```

**Data Cleaning**:
- Trim whitespace from all string columns
- Normalize `category` to lowercase
- Convert `marketplace_price` to Double, `stock_qty` to Integer
- Fill missing `stock_qty` with 0

**DQ Rules** (6 rules - Quarantine if fails):
1. `seller_id IS NOT NULL`
2. `item_id IS NOT NULL`
3. `marketplace_price >= 0`
4. `stock_qty >= 0`
5. `item_name IS NOT NULL`
6. `category IS NOT NULL`

**Output**: Hudi table partitioned by `category`, record key: `seller_id,item_id`

---

### ETL Pipeline 2: Company Sales

**Input**: `data/raw/company_sales/company_sales.csv`

**Schema**:
```
item_id, units_sold, revenue, sale_date
```

**Data Cleaning**:
- Trim `item_id`
- Convert `units_sold` to Integer, `revenue` to Double
- Parse `sale_date` to Date format
- Fill missing numeric values with 0

**DQ Rules** (4 rules):
1. `item_id IS NOT NULL`
2. `units_sold >= 0`
3. `revenue >= 0`
4. `sale_date IS NOT NULL AND sale_date <= current_date()`

**Output**: Hudi table (non-partitioned), record key: `item_id`

---

### ETL Pipeline 3: Competitor Sales

**Input**: `data/raw/competitor_sales/competitor_sales.csv`

**Schema**:
```
item_id, seller_id, units_sold, revenue, marketplace_price, sale_date
```

**Data Cleaning**:
- Trim `item_id`, `seller_id`
- Convert numeric fields to proper types
- Parse `sale_date`
- Fill missing numeric values with 0

**DQ Rules** (6 rules):
1. `item_id IS NOT NULL`
2. `seller_id IS NOT NULL`
3. `units_sold >= 0`
4. `revenue >= 0`
5. `marketplace_price >= 0`
6. `sale_date IS NOT NULL AND sale_date <= current_date()`

**Output**: Hudi table (non-partitioned), composite record key: `seller_id,item_id`

---

### Consumption Pipeline: Recommendations

**Inputs**: All 3 Hudi tables

**Logic**:
1. **Aggregate company sales** by item_id → total_units_sold
2. **Join with seller catalog** to get category, item_name
3. **Rank items per category** using window function (top 10)
4. **Aggregate competitor sales** by item_id → total_units_sold
5. **Rank competitor items** globally (top 10)
6. **For each seller**:
   - Cross join with top items
   - Anti-join with seller's catalog to find missing items
   - Calculate `expected_units_sold = total_units_sold / num_sellers`
   - Calculate `expected_revenue = expected_units_sold × marketplace_price`
7. **Union** company and competitor recommendations
8. **Deduplicate** by keeping highest expected revenue

**Output Schema**:
```
seller_id, item_id, item_name, category, market_price,
expected_units_sold, expected_revenue, recommendation_source
```

---

## Configuration

### YAML Configuration (`configs/ecomm_prod.yml`)

```yaml
seller_catalog:
  input_path: "/app/data/raw/seller_catalog/seller_catalog.csv"
  hudi_output_path: "/app/data/processed/seller_catalog_hudi/"
  quarantine_path: "/app/data/quarantine/seller_catalog/"

company_sales:
  input_path: "/app/data/raw/company_sales/company_sales.csv"
  hudi_output_path: "/app/data/processed/company_sales_hudi/"
  quarantine_path: "/app/data/quarantine/company_sales/"

competitor_sales:
  input_path: "/app/data/raw/competitor_sales/competitor_sales.csv"
  hudi_output_path: "/app/data/processed/competitor_sales_hudi/"
  quarantine_path: "/app/data/quarantine/competitor_sales/"

recommendation:
  seller_catalog_hudi: "/app/data/processed/seller_catalog_hudi/"
  company_sales_hudi: "/app/data/processed/company_sales_hudi/"
  competitor_sales_hudi: "/app/data/processed/competitor_sales_hudi/"
  output_csv: "/app/data/output/recommendations/seller_recommend_data.csv"
  top_n_items: 10
```

**Customization**:
- Change `top_n_items` to recommend more/fewer items
- Update paths for S3/HDFS (e.g., `s3a://bucket/path/`)
- Modify Hudi table types in code (COPY_ON_WRITE vs MERGE_ON_READ)

---

## Data Quality Rules

### Summary

| Dataset            | Total Rules | Null Checks | Negative Value Checks | Date Checks |
|--------------------|-------------|-------------|----------------------|-------------|
| Seller Catalog     | 6           | 4           | 2                    | 0           |
| Company Sales      | 4           | 1           | 2                    | 1           |
| Competitor Sales   | 6           | 2           | 3                    | 1           |
| **TOTAL**          | **16**      | **7**       | **7**                | **2**       |

### Quarantine Record Format

```json
{
  "dataset_name": "seller_catalog",
  "quarantine_timestamp": "2025-11-13 18:30:45",
  "dq_failure_reason": "marketplace_price_negative_or_null, stock_qty_negative_or_null",
  "seller_id": "S005",
  "item_id": "I013",
  "item_name": "HDMI Cable",
  "category": "electronics",
  "marketplace_price": -5.99,
  "stock_qty": 100
}
```

---

## Outputs

### 1. Hudi Tables

**Location**: `data/processed/*_hudi/`

**Format**: Parquet files with Hudi metadata

**Access**:
```python
# Read Hudi table
df = spark.read.format("hudi").load("/app/data/processed/seller_catalog_hudi/")
df.show()
```

**Schema Evolution**: Automatically handled by Hudi

---

### 2. Recommendations CSV

**Location**: `data/output/recommendations/seller_recommend_data.csv/`

**Sample**:
```
seller_id,item_id,item_name,category,market_price,expected_units_sold,expected_revenue,recommendation_source
S001,I006,Running Shoes,sports,79.99,96.67,7731.53,company_top_items
S001,I008,Water Bottle,sports,19.99,330.00,6596.70,company_top_items
S002,I001,Wireless Mouse,electronics,25.99,153.33,3985.07,company_top_items
```

---

### 3. Quarantine Files

**Location**: `data/quarantine/*/`

**Format**: CSV with metadata columns

**Use Case**: Investigate and fix data quality issues

---

## Troubleshooting

### Issue 1: Docker Build Fails

**Symptoms**: `ERROR: failed to solve`

**Solution**:
```bash
# Clear Docker cache
docker system prune -a

# Rebuild without cache
docker-compose build --no-cache
```

---

### Issue 2: OutOfMemoryError

**Symptoms**: `java.lang.OutOfMemoryError: Java heap space`

**Solution**: Increase memory in `docker-compose.yml`:
```yaml
environment:
  - SPARK_DRIVER_MEMORY=8g
  - SPARK_EXECUTOR_MEMORY=8g
```

---

### Issue 3: Hudi Table Not Found

**Symptoms**: `Path does not exist: /app/data/processed/seller_catalog_hudi`

**Solution**:
- Ensure ETL pipelines run before consumption pipeline
- Check logs for ETL failures
- Verify paths in `ecomm_prod.yml`

---

### Issue 4: Permission Denied

**Symptoms**: `Permission denied: /app/scripts/run_full_pipeline.sh`

**Solution**:
```bash
chmod +x scripts/*.sh
```

---

### Issue 5: Package Download Timeout

**Symptoms**: `Failed to download org.apache.hudi:hudi-spark3.5-bundle_2.12:0.15.0`

**Solution**:
- Check internet connection
- Retry (packages are cached after first download)
- Use pre-downloaded JARs with `--jars` instead of `--packages`

---

## Advanced Usage

### Running with S3 Storage

**1. Update `ecomm_prod.yml`**:
```yaml
seller_catalog:
  input_path: "s3a://my-bucket/raw/seller_catalog/seller_catalog.csv"
  hudi_output_path: "s3a://my-bucket/processed/seller_catalog_hudi/"
```

**2. Add AWS credentials to docker-compose.yml**:
```yaml
environment:
  - AWS_ACCESS_KEY_ID=your_key
  - AWS_SECRET_ACCESS_KEY=your_secret
```

---

### Incremental Updates

**Scenario**: New data arrives daily

**Solution**: Run ETL pipelines again with new CSV data

Hudi will automatically:
- Upsert records (update existing, insert new)
- Maintain version history
- Handle schema changes

```bash
# Day 1
./scripts/run_full_pipeline.sh

# Day 2 (new data in raw/)
./scripts/run_full_pipeline.sh  # Automatically upserts!
```

---

### Custom DQ Rules

**Edit**: `src/etl_seller_catalog.py` (or other ETL scripts)

**Example**: Add rule "price must be < $1000"
```python
.withColumn(
    "dq_price_too_high",
    F.when(F.col("marketplace_price") > 1000, F.lit("price_exceeds_limit")).otherwise(F.lit(None))
)
```

---

### Performance Tuning

**Adjust parallelism** in Hudi options:
```python
hudi_options = {
    'hoodie.upsert.shuffle.parallelism': 10,  # Increase for larger datasets
    'hoodie.insert.shuffle.parallelism': 10,
}
```

**Partition optimization**:
- Use high-cardinality columns for partitioning (e.g., `sale_date`)
- Avoid over-partitioning (< 10MB per partition)

---

## Contributing

This project follows standard data engineering best practices:

- **Code Style**: PEP 8
- **Logging**: INFO level for pipeline steps, ERROR for failures
- **Error Handling**: Try-except with proper logging
- **Documentation**: Docstrings for all functions

---

## License

This project is part of a Data Engineering assignment.

---

## Contact

For issues or questions:
- Open a GitHub issue
- Contact: [Your Email]

---

## Acknowledgments

- **Apache Spark**: https://spark.apache.org/
- **Apache Hudi**: https://hudi.apache.org/
- **Docker**: https://www.docker.com/

---

**Built with ❤️ using PySpark, Hudi, and Docker**