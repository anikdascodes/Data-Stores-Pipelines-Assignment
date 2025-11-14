#!/bin/bash

# E-commerce Recommendation System - Complete Assignment Test
# This script validates the entire assignment implementation

set -e  # Exit on any error

echo "================================================================================"
echo "🧪 E-COMMERCE RECOMMENDATION SYSTEM - ASSIGNMENT VALIDATION TEST"
echo "================================================================================"
echo ""
echo "This script will validate all assignment requirements and functionality."
echo "Start Time: $(date)"
echo ""

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Test counters
TESTS_PASSED=0
TESTS_FAILED=0
TOTAL_TESTS=0

# Function to run test
run_test() {
    local test_name="$1"
    local test_command="$2"
    local expected_result="$3"
    
    TOTAL_TESTS=$((TOTAL_TESTS + 1))
    echo -e "${BLUE}Test $TOTAL_TESTS: $test_name${NC}"
    
    if eval "$test_command" > /dev/null 2>&1; then
        if [ -n "$expected_result" ]; then
            if eval "$expected_result" > /dev/null 2>&1; then
                echo -e "${GREEN}✅ PASSED${NC}"
                TESTS_PASSED=$((TESTS_PASSED + 1))
            else
                echo -e "${RED}❌ FAILED - Expected result not met${NC}"
                TESTS_FAILED=$((TESTS_FAILED + 1))
            fi
        else
            echo -e "${GREEN}✅ PASSED${NC}"
            TESTS_PASSED=$((TESTS_PASSED + 1))
        fi
    else
        echo -e "${RED}❌ FAILED - Command execution failed${NC}"
        TESTS_FAILED=$((TESTS_FAILED + 1))
    fi
    echo ""
}

# Function to check file exists and has content
check_file_content() {
    local file_path="$1"
    local min_size="$2"
    
    if [ -f "$file_path" ]; then
        local file_size=$(stat -f%z "$file_path" 2>/dev/null || stat -c%s "$file_path" 2>/dev/null || echo "0")
        if [ "$file_size" -gt "$min_size" ]; then
            return 0
        fi
    fi
    return 1
}

echo "================================================================================"
echo "📋 PHASE 1: SYSTEM REQUIREMENTS VALIDATION"
echo "================================================================================"

# Test 1: Python Installation
run_test "Python 3.8+ Installation" "python --version | grep -E 'Python 3\.[8-9]|Python 3\.1[0-9]'"

# Test 2: Required Python Packages
run_test "PySpark Installation" "python -c 'import pyspark; print(pyspark.__version__)'"
run_test "PyYAML Installation" "python -c 'import yaml'"
run_test "Pandas Installation" "python -c 'import pandas'"
run_test "Numpy Installation" "python -c 'import numpy'"

# Test 3: Project Structure
run_test "Project Structure - Source Files" "[ -f src/etl_seller_catalog.py ] && [ -f src/etl_company_sales.py ] && [ -f src/etl_competitor_sales.py ] && [ -f src/consumption_recommendation.py ]"
run_test "Project Structure - Config Files" "[ -f configs/ecomm_prod.yml ]"
run_test "Project Structure - Scripts" "[ -f scripts/run_full_pipeline_local.sh ] && [ -x scripts/run_full_pipeline_local.sh ]"

echo "================================================================================"
echo "📊 PHASE 2: DATA VALIDATION"
echo "================================================================================"

# Test 4: Input Data Files
run_test "Seller Catalog Data (1M records)" "[ -f data/raw/seller_catalog/seller_catalog_clean.csv ]" "[ \$(wc -l < data/raw/seller_catalog/seller_catalog_clean.csv) -gt 999000 ]"
run_test "Company Sales Data (1M records)" "[ -f data/raw/company_sales/company_sales_clean.csv ]" "[ \$(wc -l < data/raw/company_sales/company_sales_clean.csv) -gt 999000 ]"
run_test "Competitor Sales Data (1M records)" "[ -f data/raw/competitor_sales/competitor_sales_clean.csv ]" "[ \$(wc -l < data/raw/competitor_sales/competitor_sales_clean.csv) -gt 999000 ]"

echo "================================================================================"
echo "🔧 PHASE 3: ETL PIPELINE TESTING (15 MARKS)"
echo "================================================================================"

# Clean previous runs
rm -rf data/processed/* data/output/* data/quarantine/* 2>/dev/null || true

# Test 5: Individual ETL Components
echo -e "${YELLOW}Running Seller Catalog ETL...${NC}"
run_test "Seller Catalog ETL Execution" "timeout 120 python src/etl_seller_catalog.py --config configs/ecomm_prod.yml"
run_test "Seller Catalog Output Validation" "[ -d data/processed/seller_catalog_hudi ] && [ \$(find data/processed/seller_catalog_hudi -name '*.parquet' | wc -l) -gt 0 ]"

echo -e "${YELLOW}Running Company Sales ETL...${NC}"
run_test "Company Sales ETL Execution" "timeout 120 python src/etl_company_sales.py --config configs/ecomm_prod.yml"
run_test "Company Sales Output Validation" "[ -d data/processed/company_sales_hudi ] && [ \$(find data/processed/company_sales_hudi -name '*.parquet' | wc -l) -gt 0 ]"

echo -e "${YELLOW}Running Competitor Sales ETL...${NC}"
run_test "Competitor Sales ETL Execution" "timeout 120 python src/etl_competitor_sales.py --config configs/ecomm_prod.yml"
run_test "Competitor Sales Output Validation" "[ -d data/processed/competitor_sales_hudi ] && [ \$(find data/processed/competitor_sales_hudi -name '*.parquet' | wc -l) -gt 0 ]"

echo "================================================================================"
echo "🎯 PHASE 4: CONSUMPTION LAYER TESTING (5 MARKS)"
echo "================================================================================"

# Test 6: Recommendation Engine
echo -e "${YELLOW}Running Recommendation Engine...${NC}"
run_test "Recommendation Engine Execution" "timeout 180 python src/consumption_recommendation.py --config configs/ecomm_prod.yml"
run_test "Recommendations Output Validation" "[ -d data/output/recommendations/seller_recommend_data.csv ] && [ \$(find data/output/recommendations/seller_recommend_data.csv -name '*.csv' | wc -l) -gt 0 ]"

# Test 7: Output Content Validation
if [ -d data/output/recommendations/seller_recommend_data.csv ]; then
    RECOMMENDATION_FILE=$(find data/output/recommendations/seller_recommend_data.csv -name "part-*.csv" | head -1)
    if [ -n "$RECOMMENDATION_FILE" ]; then
        run_test "Recommendations Content Validation" "[ \$(wc -l < \"$RECOMMENDATION_FILE\") -gt 1000 ]"
        run_test "Recommendations Schema Validation" "head -1 \"$RECOMMENDATION_FILE\" | grep -q 'seller_id,item_id,item_name,category,market_price,expected_units_sold,expected_revenue'"
    fi
fi

echo "================================================================================"
echo "🚀 PHASE 5: FULL PIPELINE INTEGRATION TEST"
echo "================================================================================"

# Clean for full pipeline test
rm -rf data/processed/* data/output/* data/quarantine/* 2>/dev/null || true

# Test 8: Complete Pipeline Execution
echo -e "${YELLOW}Running Complete Pipeline...${NC}"
START_TIME=$(date +%s)
run_test "Full Pipeline Execution" "timeout 300 bash scripts/run_full_pipeline_local.sh configs/ecomm_prod.yml"
END_TIME=$(date +%s)
EXECUTION_TIME=$((END_TIME - START_TIME))

echo -e "${BLUE}Pipeline Execution Time: ${EXECUTION_TIME} seconds${NC}"

# Test 9: Final Output Validation
run_test "Final Recommendations Generated" "[ -d data/output/recommendations/seller_recommend_data.csv ]"

if [ -d data/output/recommendations/seller_recommend_data.csv ]; then
    FINAL_FILE=$(find data/output/recommendations/seller_recommend_data.csv -name "part-*.csv" | head -1)
    if [ -n "$FINAL_FILE" ]; then
        RECOMMENDATION_COUNT=$(tail -n +2 "$FINAL_FILE" | wc -l)
        echo -e "${BLUE}Total Recommendations Generated: $RECOMMENDATION_COUNT${NC}"
        run_test "Minimum Recommendations Threshold" "[ $RECOMMENDATION_COUNT -gt 1000 ]"
    fi
fi

echo "================================================================================"
echo "📋 PHASE 6: DATA QUALITY VALIDATION"
echo "================================================================================"

# Test 10: Data Quality Checks
run_test "No Quarantine Records (Clean Data)" "[ ! -d data/quarantine ] || [ \$(find data/quarantine -name '*.csv' 2>/dev/null | wc -l) -eq 0 ]"

echo "================================================================================"
echo "🐳 PHASE 7: DOCKER READINESS CHECK"
echo "================================================================================"

# Test 11: Docker Configuration
run_test "Dockerfile Exists" "[ -f Dockerfile ]"
run_test "Requirements.txt Exists" "[ -f requirements.txt ]"
run_test "Docker Compose Configuration" "[ -f docker-compose.yml ]"

echo "================================================================================"
echo "📊 TEST RESULTS SUMMARY"
echo "================================================================================"

echo ""
echo -e "${BLUE}Assignment Validation Results:${NC}"
echo -e "Total Tests Run: $TOTAL_TESTS"
echo -e "${GREEN}Tests Passed: $TESTS_PASSED${NC}"
echo -e "${RED}Tests Failed: $TESTS_FAILED${NC}"

if [ $TESTS_FAILED -eq 0 ]; then
    echo ""
    echo -e "${GREEN}🎉 ALL TESTS PASSED! ASSIGNMENT IS READY FOR SUBMISSION${NC}"
    echo ""
    echo "✅ ETL Ingestion (15 marks): IMPLEMENTED"
    echo "✅ Consumption Layer (5 marks): IMPLEMENTED"
    echo "✅ Data Quality Checks: PASSED"
    echo "✅ Large Dataset Processing: VALIDATED"
    echo "✅ Docker Support: CONFIGURED"
    echo ""
    echo -e "${BLUE}Performance Metrics:${NC}"
    echo "- Pipeline Execution Time: ${EXECUTION_TIME} seconds"
    echo "- Data Processing: 3M+ records"
    echo "- Recommendations Generated: $RECOMMENDATION_COUNT+"
    echo "- Data Quality: 100% (0 quarantine records)"
    echo ""
    echo -e "${GREEN}🚀 SYSTEM STATUS: PRODUCTION READY${NC}"
else
    echo ""
    echo -e "${RED}⚠️  SOME TESTS FAILED - PLEASE REVIEW AND FIX ISSUES${NC}"
    echo ""
    echo "Please check the failed tests above and ensure:"
    echo "1. All required dependencies are installed"
    echo "2. Data files are present and accessible"
    echo "3. Scripts have proper permissions"
    echo "4. System has sufficient memory (8GB+ recommended)"
fi

echo ""
echo "================================================================================"
echo "End Time: $(date)"
echo "================================================================================"

# Exit with appropriate code
if [ $TESTS_FAILED -eq 0 ]; then
    exit 0
else
    exit 1
fi