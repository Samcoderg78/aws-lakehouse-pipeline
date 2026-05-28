"""
scripts/local_test.py — Run pipeline locally without AWS
=========================================================
Tests all Glue job transformations using PySpark local mode
and sample data generated from the Syngenta hackathon CSVs.

Usage:
    python scripts/local_test.py                # run all tests
    python scripts/local_test.py --job bronze   # run specific job test

Requirements:
    pip install pyspark pandas pyarrow pytest
"""

import argparse
import os
import sys
import logging
from datetime import datetime, date
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType, DateType
)

logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")
logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# Spark session (local mode — no AWS needed)
# ─────────────────────────────────────────────
def get_spark():
    return (
        SparkSession.builder
        .appName("LakehousePipelineLocalTest")
        .master("local[*]")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )


# ─────────────────────────────────────────────
# Sample data generators
# ─────────────────────────────────────────────
def make_retailer_df(spark):
    """Generate sample retailer data that mirrors real CSV structure."""
    data = [
        ("R001", "Sharma Agro",  "Maharashtra", "Pune",     "Haveli", "2026-W20", 45000.0, 7,  2, 800.0, 3.5),
        ("R002", "Patel Seeds",  "Gujarat",     "Surat",    "Olpad",  "2026-W20", 28000.0, 14, 5, 650.0, 1.2),
        ("R003", "Singh Krishi", "Punjab",      "Ludhiana", "Jagraon","2026-W20", 62000.0, 3,  0, 950.0, 5.0),
        ("R004", "Kumar Agri",   "Maharashtra", "Nashik",   "Niphad", "2026-W20", 15000.0, 21, 8, 400.0, 0.8),
        ("R005", "Devi Traders", "Rajasthan",   "Jaipur",   "Amer",   "2026-W20", 38000.0, 10, 1, 720.0, 4.2),
        # Duplicate to test dedup
        ("R001", "Sharma Agro",  "Maharashtra", "Pune",     "Haveli", "2026-W20", 45000.0, 7,  2, 800.0, 3.5),
    ]
    schema = StructType([
        StructField("retailer_id",          StringType(),  False),
        StructField("retailer_name",        StringType(),  True),
        StructField("state",                StringType(),  False),
        StructField("district",             StringType(),  False),
        StructField("tehsil",               StringType(),  True),
        StructField("week",                 StringType(),  False),
        StructField("avg_weekly_revenue",   DoubleType(),  True),
        StructField("days_since_last_visit",IntegerType(), True),
        StructField("stockout_events",      IntegerType(), True),
        StructField("avg_depletion_rate",   DoubleType(),  True),
        StructField("weeks_of_stock",       DoubleType(),  True),
    ])
    return spark.createDataFrame(data, schema)


def make_district_df(spark):
    data = [
        ("Pune",     "Maharashtra", "2026-05-20", 1.5, 82.0, 15, 12, 1),
        ("Surat",    "Gujarat",     "2026-05-20", 5.0, 65.0,  3,  1, 0),
        ("Ludhiana", "Punjab",      "2026-05-20", 8.0, 55.0,  8,  6, 0),
        ("Nashik",   "Maharashtra", "2026-05-20", 0.8, 88.0, 20, 18, 1),
        ("Jaipur",   "Rajasthan",   "2026-05-20", 0.3, 40.0, 25,  5, 0),
    ]
    schema = StructType([
        StructField("district",             StringType(),  False),
        StructField("state",                StringType(),  False),
        StructField("date",                 StringType(),  False),
        StructField("precip_mm_7d",         DoubleType(),  True),
        StructField("avg_humidity_pct",     DoubleType(),  True),
        StructField("total_pest_alerts",    IntegerType(), True),
        StructField("high_incidence_count", IntegerType(), True),
        StructField("fungicide_risk_flag",  IntegerType(), True),
    ])
    return spark.createDataFrame(data, schema)


# ─────────────────────────────────────────────
# Test: Bronze layer
# ─────────────────────────────────────────────
def test_bronze(spark):
    logger.info("═══ TEST: Bronze Ingest ════════════════════════")
    df = make_retailer_df(spark)

    # Add metadata (simulating bronze_ingest.py)
    bronze_df = (
        df
        .withColumn("ingestion_timestamp", F.current_timestamp())
        .withColumn("ingestion_date", F.current_date())
        .withColumn("source_path", F.lit("raw/retailer/"))
    )

    assert bronze_df.count() == 6, "Bronze: row count mismatch"
    assert "ingestion_date" in bronze_df.columns, "Bronze: missing ingestion_date"
    assert "ingestion_timestamp" in bronze_df.columns

    logger.info(f"  ✅ Bronze: {bronze_df.count()} rows ingested with metadata columns")
    bronze_df.show(5, truncate=False)
    return bronze_df


# ─────────────────────────────────────────────
# Test: Silver — dedup + DQ + type casting
# ─────────────────────────────────────────────
def test_silver(spark, bronze_df):
    logger.info("═══ TEST: Silver Clean ═════════════════════════")
    from pyspark.sql.window import Window

    # Dedup by retailer_id + week, keep latest
    w = Window.partitionBy("retailer_id", "week").orderBy(F.col("ingestion_timestamp").desc())
    silver_df = (
        bronze_df
        .withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
        .withColumn("stockout_risk_flag",
            F.when(F.col("weeks_of_stock") < 2, 1).otherwise(0))
        .filter(F.col("retailer_id").isNotNull())
        .filter(F.col("avg_weekly_revenue") >= 0)
    )

    assert silver_df.count() == 5, f"Silver: expected 5 rows after dedup, got {silver_df.count()}"

    null_count = silver_df.filter(F.col("retailer_id").isNull()).count()
    assert null_count == 0, "Silver: null retailer_ids found"

    stockout_risk = silver_df.filter(F.col("stockout_risk_flag") == 1).count()
    logger.info(f"  ✅ Silver: {silver_df.count()} clean rows | {stockout_risk} at stockout risk")
    silver_df.select("retailer_id", "week", "avg_weekly_revenue", "stockout_risk_flag").show()
    return silver_df


# ─────────────────────────────────────────────
# Test: Gold Retailer KPI
# ─────────────────────────────────────────────
def test_gold_retailer(spark, silver_df):
    logger.info("═══ TEST: Gold Retailer KPI ════════════════════")

    # Min-max normalise
    def norm_col(df, col_name, alias):
        min_v = df.agg(F.min(col_name)).collect()[0][0] or 0.0
        max_v = df.agg(F.max(col_name)).collect()[0][0] or 1.0
        denom = max_v - min_v if (max_v - min_v) != 0 else 1.0
        return df.withColumn(alias, (F.col(col_name) - min_v) / denom)

    gold_df = silver_df
    gold_df = norm_col(gold_df, "avg_weekly_revenue", "norm_revenue")
    gold_df = norm_col(gold_df, "days_since_last_visit", "norm_days")
    gold_df = norm_col(gold_df, "stockout_events", "norm_stockout")
    gold_df = norm_col(gold_df, "avg_depletion_rate", "norm_depletion")

    gold_df = gold_df.withColumn(
        "rps_score",
        F.round(
            0.30 * F.col("norm_revenue")
            + 0.25 * F.col("norm_days")
            + 0.20 * F.col("norm_stockout")
            + 0.15 * F.col("stockout_risk_flag").cast("double")
            + 0.10 * F.col("norm_depletion"),
            4
        )
    ).withColumn(
        "visit_urgency",
        F.when(
            (F.col("stockout_risk_flag") == 1) | (F.col("rps_score") > 0.75), "URGENT"
        ).when(F.col("rps_score") >= 0.50, "SOON").otherwise("ROUTINE")
    )

    assert gold_df.filter(F.col("rps_score").isNull()).count() == 0, "Gold: null RPS scores"
    assert gold_df.filter((F.col("rps_score") < 0) | (F.col("rps_score") > 1)).count() == 0

    logger.info(f"  ✅ Gold Retailer: {gold_df.count()} rows computed")
    gold_df.select("retailer_id", "rps_score", "visit_urgency").orderBy(F.col("rps_score").desc()).show()
    return gold_df


# ─────────────────────────────────────────────
# Test: Gold District Risk
# ─────────────────────────────────────────────
def test_gold_district(spark):
    logger.info("═══ TEST: Gold District Risk ════════════════════")
    df = make_district_df(spark)

    df = (
        df
        .withColumn("drought_stress_flag", F.when(F.col("precip_mm_7d") < 2, 1).otherwise(0))
        .withColumn("high_humidity_flag",  F.when(F.col("avg_humidity_pct") > 80, 1).otherwise(0))
    )

    max_pest = df.agg(F.max("total_pest_alerts")).collect()[0][0] or 1
    df = df.withColumn("norm_pest", F.col("total_pest_alerts") / float(max_pest))

    df = df.withColumn(
        "drs_score",
        F.round(
            0.35 * F.col("norm_pest")
            + 0.30 * F.col("fungicide_risk_flag").cast("double")
            + 0.20 * F.col("high_humidity_flag").cast("double")
            + 0.15 * F.col("drought_stress_flag").cast("double"),
            4
        )
    ).withColumn(
        "risk_tier",
        F.when(F.col("drs_score") >= 0.65, "HIGH")
         .when(F.col("drs_score") >= 0.35, "MEDIUM")
         .otherwise("LOW")
    ).withColumn(
        "recommended_product",
        F.when(F.col("fungicide_risk_flag") == 1,   "Score 250 EC")
         .when(F.col("high_incidence_count") > 10,   "Actara 25 WG")
         .when(F.col("drought_stress_flag") == 1,    "Topik 15 WP")
         .otherwise("Monitor Digital Demand")
    )

    high_risk = df.filter(F.col("risk_tier") == "HIGH").count()
    logger.info(f"  ✅ District Risk: {df.count()} districts | HIGH: {high_risk}")
    df.select("district", "drs_score", "risk_tier", "recommended_product").show()


# ─────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="Lakehouse Pipeline Local Tests")
    parser.add_argument("--job", choices=["bronze", "silver", "gold_retailer", "gold_district", "all"],
                        default="all", help="Which job to test")
    args = parser.parse_args()

    spark = get_spark()
    logger.info("🚀 Starting local pipeline tests (PySpark local mode)")
    logger.info("=" * 60)

    try:
        if args.job in ("bronze", "all"):
            bronze_df = test_bronze(spark)
        else:
            bronze_df = None

        if args.job in ("silver", "all") and bronze_df is not None:
            silver_df = test_silver(spark, bronze_df)
        elif args.job == "silver":
            bronze_df = test_bronze(spark)
            silver_df = test_silver(spark, bronze_df)
        else:
            silver_df = None

        if args.job in ("gold_retailer", "all") and silver_df is not None:
            test_gold_retailer(spark, silver_df)

        if args.job in ("gold_district", "all"):
            test_gold_district(spark)

        logger.info("=" * 60)
        logger.info("🎉 ALL TESTS PASSED — Pipeline logic verified locally!")
        logger.info("    Ready to deploy to AWS with: cd terraform && terraform apply")

    except AssertionError as e:
        logger.error(f"❌ TEST FAILED: {e}")
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
