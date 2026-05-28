"""
silver_clean.py — AWS Glue PySpark Job
========================================
Layer   : BRONZE → SILVER
Purpose : Apply data quality rules, cast types precisely,
          deduplicate, and write clean Parquet to Silver prefix.

Glue Job Parameters:
  --JOB_NAME        : Name of this Glue job
  --RAW_BUCKET      : S3 bucket name
  --BRONZE_PREFIX   : S3 key prefix for bronze input
  --SILVER_PREFIX   : S3 key prefix for silver output
  --TABLE_NAME      : Logical table name (ignored, processing all 3 tables)
  --DQ_RESULTS_PREFIX : S3 prefix to write DQ failure reports
  --SNS_TOPIC_ARN   : SNS topic ARN for DQ failure alerts
"""

import sys
import json
import logging
from datetime import datetime, timezone

import boto3
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import (
    DoubleType, IntegerType, DateType, TimestampType, StringType
)

# ─────────────────────────────────────────────
# Initialise
# ─────────────────────────────────────────────
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "RAW_BUCKET",
    "BRONZE_PREFIX",
    "SILVER_PREFIX",
    "TABLE_NAME",
    "DQ_RESULTS_PREFIX",
    "SNS_TOPIC_ARN",
])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.shuffle.partitions", "50")

sns_client = boto3.client("sns")

# ─────────────────────────────────────────────
# Data Quality Engine
# ─────────────────────────────────────────────

class DQResult:
    def __init__(self, rule: str, passed: bool, fail_count: int, total: int, details: str = ""):
        self.rule = rule
        self.passed = passed
        self.fail_count = fail_count
        self.total = total
        self.details = details

    def to_dict(self):
        return {
            "rule": self.rule,
            "passed": self.passed,
            "fail_count": self.fail_count,
            "total": self.total,
            "fail_pct": round(self.fail_count / self.total * 100, 2) if self.total else 0,
            "details": self.details,
        }

def dq_not_null(df: DataFrame, columns: list) -> DQResult:
    condition = F.lit(False)
    for col in columns:
        if col in df.columns:
            condition = condition | F.col(col).isNull()

    fail_count = df.filter(condition).count()
    return DQResult(
        rule=f"NOT_NULL({', '.join(columns)})",
        passed=(fail_count == 0),
        fail_count=fail_count,
        total=df.count(),
        details=f"Null found in critical columns: {columns}",
    )

def dq_non_negative(df: DataFrame, columns: list) -> DQResult:
    condition = F.lit(False)
    for col in columns:
        if col in df.columns:
            condition = condition | (F.col(col) < 0)

    fail_count = df.filter(condition).count()
    total = df.count()
    return DQResult(
        rule=f"NON_NEGATIVE({', '.join(columns)})",
        passed=(fail_count == 0),
        fail_count=fail_count,
        total=total,
        details=f"Negative values found in: {columns}",
    )

def dq_no_duplicates(df: DataFrame, key_cols: list) -> DQResult:
    valid_key_cols = [c for c in key_cols if c in df.columns]
    total = df.count()
    distinct = df.select(valid_key_cols).distinct().count() if valid_key_cols else total
    fail_count = total - distinct
    return DQResult(
        rule=f"UNIQUE({', '.join(valid_key_cols)})",
        passed=(fail_count == 0),
        fail_count=fail_count,
        total=total,
        details=f"Duplicate rows on key: {valid_key_cols}",
    )

def dq_row_count(df: DataFrame, min_rows: int = 1) -> DQResult:
    total = df.count()
    passed = total >= min_rows
    return DQResult(
        rule=f"MIN_ROWS({min_rows})",
        passed=passed,
        fail_count=0 if passed else 1,
        total=total,
        details=f"Expected >= {min_rows} rows, got {total}",
    )


# ─────────────────────────────────────────────
# Table-specific transformation + DQ config
# ─────────────────────────────────────────────

def transform_retailer(df: DataFrame) -> tuple[DataFrame, list]:
    df = (
        df
        .withColumn("avg_weekly_revenue",    F.col("avg_weekly_revenue").cast(DoubleType()))
        .withColumn("days_since_last_visit",  F.col("days_since_last_visit").cast(IntegerType()))
        .withColumn("stockout_events",        F.col("stockout_events").cast(IntegerType()))
        .withColumn("avg_depletion_rate",     F.col("avg_depletion_rate").cast(DoubleType()))
        .withColumn("weeks_of_stock",         F.col("weeks_of_stock_remaining").cast(DoubleType()) if "weeks_of_stock_remaining" in df.columns else F.col("weeks_of_stock").cast(DoubleType()))
        .withColumn("stockout_risk_flag",
            F.when(F.col("weeks_of_stock") < 2, 1).otherwise(0)
        )
        .withColumn("revenue_tier",
            F.when(F.col("avg_weekly_revenue") >= 50000, "HIGH")
             .when(F.col("avg_weekly_revenue") >= 20000, "MID")
             .otherwise("LOW")
        )
    )

    dq_results = [
        dq_not_null(df, ["retailer_id", "district"]),
        dq_non_negative(df, ["avg_weekly_revenue", "stockout_events"]),
        dq_no_duplicates(df, ["retailer_id"]),
        dq_row_count(df, min_rows=10),
    ]
    return df, dq_results


def transform_district(df: DataFrame) -> tuple[DataFrame, list]:
    df = (
        df
        .withColumn("precip_mm_7d",         F.col("recent_precip_sum").cast(DoubleType()) if "recent_precip_sum" in df.columns else F.col("precip_mm_7d").cast(DoubleType()))
        .withColumn("avg_humidity_pct",      F.col("recent_humidity_avg").cast(DoubleType()) if "recent_humidity_avg" in df.columns else F.col("avg_humidity_pct").cast(DoubleType()))
        .withColumn("total_pest_alerts",     F.col("total_pest_alerts").cast(IntegerType()))
        .withColumn("high_incidence_count",  F.col("high_incidence_count").cast(IntegerType()))
        .withColumn("fungicide_risk_flag",   F.col("fungicide_risk_flag").cast(IntegerType()))
        .withColumn("drought_stress_flag",
            F.when(F.col("precip_mm_7d") < 2, 1).otherwise(0)
        )
        .withColumn("high_humidity_flag",
            F.when(F.col("avg_humidity_pct") > 80, 1).otherwise(0)
        )
    )

    dq_results = [
        dq_not_null(df, ["district", "state"]),
        dq_non_negative(df, ["total_pest_alerts", "high_incidence_count"]),
        dq_no_duplicates(df, ["district"]),
        dq_row_count(df, min_rows=5),
    ]
    return df, dq_results

def transform_demand(df: DataFrame) -> tuple[DataFrame, list]:
    # We joined inventory_weekly with retailer_features
    # So we have: retailer_id, sku_id, sku_name, sku_qty, week_end_date, state, district
    if "sku_qty" in df.columns:
        df = df.withColumn("actual_qty", F.col("sku_qty").cast(DoubleType()))
    if "week_end_date" in df.columns:
        # Depending on date format, just rename for now and ensure string works
        df = df.withColumn("week", F.col("week_end_date"))
        
    dq_results = [
        dq_not_null(df, ["sku_id", "district", "week"]),
        dq_no_duplicates(df, ["sku_id", "district", "retailer_id", "week"]),
    ]
    return df, dq_results


# ─────────────────────────────────────────────
# DQ Reporting & Alerting
# ─────────────────────────────────────────────

def save_dq_report(results: list, bucket: str, prefix: str, table: str):
    report = {
        "table": table,
        "run_time": datetime.now(timezone.utc).isoformat(),
        "rules": [r.to_dict() for r in results],
        "overall_passed": all(r.passed for r in results),
    }
    report_json = json.dumps(report, indent=2)
    key = f"{prefix}/{table}/dq_report_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.json"
    key = key.lstrip("/")
    
    s3 = boto3.client("s3")
    s3.put_object(Bucket=bucket, Key=key, Body=report_json, ContentType="application/json")
    
    logger.info(f"📋 DQ report saved: s3://{bucket}/{key}")
    return report

def send_dq_alert(sns_topic_arn: str, report: dict, table: str):
    failed_rules = [r for r in report["rules"] if not r["passed"]]
    if not failed_rules:
        return

    message = f"""
🚨 DATA QUALITY ALERT — AWS Lakehouse Pipeline
================================================
Table      : {table}
Run Time   : {report['run_time']}
Failed Rules: {len(failed_rules)}

FAILURES:
""" + "\n".join(
        f"  ❌ {r['rule']} | fail_count={r['fail_count']} | fail_pct={r['fail_pct']}% | {r['details']}"
        for r in failed_rules
    )

    sns_client.publish(
        TopicArn=sns_topic_arn,
        Subject=f"[ALERT] DQ Failure: {table}",
        Message=message,
    )
    logger.warning(f"📧 SNS alert sent for {len(failed_rules)} DQ failures.")


# ─────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────

def run():
    bucket          = args["RAW_BUCKET"]
    bronze_prefix   = args["BRONZE_PREFIX"].rstrip("/")
    silver_prefix   = args["SILVER_PREFIX"].rstrip("/")
    dq_prefix       = args["DQ_RESULTS_PREFIX"].rstrip("/")
    sns_topic_arn   = args["SNS_TOPIC_ARN"]

    tables = ["retailer", "district", "demand"]
    
    # Read the data frames first
    try:
        retailer_df = spark.read.parquet(f"s3://{bucket}/{bronze_prefix}/features_retailers/")
    except Exception as e:
        logger.warning("Could not read features_retailers")
        retailer_df = None
        
    try:
        district_df = spark.read.parquet(f"s3://{bucket}/{bronze_prefix}/features_district_risk/")
    except Exception as e:
        logger.warning("Could not read features_district_risk")
        district_df = None
        
    try:
        inventory_df = spark.read.parquet(f"s3://{bucket}/{bronze_prefix}/retailer_inventory_weekly/")
    except Exception as e:
        logger.warning("Could not read retailer_inventory_weekly")
        inventory_df = None

    for table_name in tables:
        logger.info(f"🚀 Silver clean job processing table={table_name}")
        
        df = None
        transform_fn = None
        key_cols = []
        
        if table_name == "retailer":
            df = retailer_df
            transform_fn = transform_retailer
            key_cols = ["retailer_id"]
        elif table_name == "district":
            df = district_df
            transform_fn = transform_district
            key_cols = ["district"]
        elif table_name == "demand":
            if inventory_df is not None and retailer_df is not None:
                # Need to join inventory with retailer to get district & state
                df = inventory_df.join(retailer_df.select("retailer_id", "district", "state"), "retailer_id", "inner")
            transform_fn = transform_demand
            key_cols = ["sku_id", "retailer_id", "week"]
            
        if df is None:
            logger.error(f"No Bronze data available for {table_name}, skipping.")
            continue
            
        logger.info(f"📥 Bronze records for {table_name}: {df.count():,}")

        # 2. Transform + DQ
        silver_df, dq_results = transform_fn(df)

        # 3. Deduplicate
        valid_key_cols = [c for c in key_cols if c in silver_df.columns]
        if valid_key_cols:
            window_col = "ingestion_timestamp"
            if window_col not in silver_df.columns:
                silver_df = silver_df.withColumn(window_col, F.lit("1970-01-01"))
                
            window = (
                F.row_number()
                 .over(
                     __import__("pyspark.sql.window", fromlist=["Window"])
                     .Window.partitionBy(valid_key_cols)
                     .orderBy(F.col(window_col).desc())
                 )
            )
            silver_df = silver_df.withColumn("_rn", window).filter(F.col("_rn") == 1).drop("_rn")

        logger.info(f"✅ Silver records after dedup: {silver_df.count():,}")

        # 4. Save DQ report & alert
        report = save_dq_report(dq_results, bucket, dq_prefix, table_name)
        send_dq_alert(sns_topic_arn, report, table_name)

        # 5. Write Silver
        # Important: Write directly to silver/{table_name}/ so Gold jobs can find it!
        silver_path = f"s3://{bucket}/{silver_prefix}/{table_name}/"
        
        # Determine partition column safely
        part_col = "ingestion_date"
        if part_col not in silver_df.columns:
            silver_df = silver_df.withColumn(part_col, F.lit(datetime.now(timezone.utc).strftime("%Y-%m-%d")))
            
        (
            silver_df.write
            .mode("overwrite")
            .partitionBy(part_col)
            .parquet(silver_path)
        )
        logger.info(f"✅ Silver write complete: {silver_path}")


run()
job.commit()
