"""
gold_retailer_kpi.py — AWS Glue PySpark Job
=============================================
Layer   : SILVER → GOLD
Purpose : Aggregate retailer data into business-ready KPI table.
          Computes Retailer Priority Score (RPS) using weighted formula,
          trends revenue week-over-week, and flags at-risk retailers.

Gold output schema (partitioned by state, ingestion_date):
  - retailer_id, retailer_name, state, district, tehsil
  - rps_score           : Retailer Priority Score [0,1]
  - revenue_tier        : HIGH / MID / LOW
  - stockout_risk_flag  : 1 if weeks_of_stock < 2
  - revenue_wow_pct     : week-over-week revenue change %
  - visit_urgency       : URGENT / SOON / ROUTINE
  - top5_in_tehsil      : 1 if retailer is top-5 RPS in tehsil
  - ingestion_date      : partition column
"""

import sys
import logging

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F, Window

# ─────────────────────────────────────────────
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "RAW_BUCKET",
    "SILVER_PREFIX",
    "GOLD_PREFIX",
])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)
spark.conf.set("spark.sql.adaptive.enabled", "true")

# ─────────────────────────────────────────────
# RPS weights (matching Syngenta scoring paper)
# ─────────────────────────────────────────────
W_REVENUE       = 0.30
W_DAYS_SINCE    = 0.25
W_STOCKOUT_EVT  = 0.20
W_STOCKOUT_RISK = 0.15
W_DEPLETION     = 0.10


def min_max_norm(df, col_name: str, alias: str):
    """Add a min-max normalised column [0,1] using window over full dataset."""
    min_val = df.agg(F.min(col_name)).collect()[0][0] or 0.0
    max_val = df.agg(F.max(col_name)).collect()[0][0] or 1.0
    denom = max_val - min_val if (max_val - min_val) != 0 else 1.0
    return df.withColumn(alias, (F.col(col_name) - F.lit(min_val)) / F.lit(denom))


def compute_rps(df):
    """
    Compute Retailer Priority Score:
    RPS = 0.30*norm_revenue + 0.25*norm_days_since + 0.20*norm_stockout_events
          + 0.15*stockout_risk_flag + 0.10*norm_depletion_rate
    """
    df = min_max_norm(df, "avg_weekly_revenue",    "norm_revenue")
    df = min_max_norm(df, "days_since_last_visit",  "norm_days_since")
    df = min_max_norm(df, "stockout_events",         "norm_stockout_events")
    df = min_max_norm(df, "avg_depletion_rate",      "norm_depletion")

    df = df.withColumn(
        "rps_score",
        F.round(
            W_REVENUE      * F.col("norm_revenue")
            + W_DAYS_SINCE * F.col("norm_days_since")
            + W_STOCKOUT_EVT * F.col("norm_stockout_events")
            + W_STOCKOUT_RISK * F.col("stockout_risk_flag").cast("double")
            + W_DEPLETION  * F.col("norm_depletion"),
            4
        )
    )
    return df


def compute_wow_revenue(df):
    """Revenue change using lag window (ordered by ingestion_date)."""
    order_col = "week" if "week" in df.columns else "ingestion_date"
    w = Window.partitionBy("retailer_id").orderBy(order_col)
    df = df.withColumn("prev_revenue", F.lag("avg_weekly_revenue", 1).over(w))
    df = df.withColumn(
        "revenue_wow_pct",
        F.when(
            F.col("prev_revenue").isNotNull() & (F.col("prev_revenue") != 0),
            F.round(
                (F.col("avg_weekly_revenue") - F.col("prev_revenue")) / F.col("prev_revenue") * 100,
                2
            )
        ).otherwise(F.lit(None))
    ).drop("prev_revenue")
    return df


def classify_visit_urgency(df):
    """
    Tag visit urgency based on RPS + stockout risk.
    URGENT  : stockout_risk_flag=1 OR rps_score > 0.75
    SOON    : rps_score 0.50 - 0.75
    ROUTINE : rps_score < 0.50
    """
    return df.withColumn(
        "visit_urgency",
        F.when(
            (F.col("stockout_risk_flag") == 1) | (F.col("rps_score") > 0.75),
            "URGENT"
        ).when(F.col("rps_score") >= 0.50, "SOON")
         .otherwise("ROUTINE")
    )


def flag_top5_in_tehsil(df):
    """Mark top-5 retailers by RPS score within each tehsil."""
    part_cols = ["tehsil"]
    if "week" in df.columns:
        part_cols.append("week")
    elif "ingestion_date" in df.columns:
        part_cols.append("ingestion_date")
    w = Window.partitionBy(part_cols).orderBy(F.col("rps_score").desc())
    return (
        df
        .withColumn("tehsil_rank", F.rank().over(w))
        .withColumn("top5_in_tehsil", F.when(F.col("tehsil_rank") <= 5, 1).otherwise(0))
        .drop("tehsil_rank")
    )


# ─────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────

def run():
    bucket         = args["RAW_BUCKET"]
    silver_prefix  = args["SILVER_PREFIX"].rstrip("/")
    gold_prefix    = args["GOLD_PREFIX"].rstrip("/")

    silver_path = f"s3://{bucket}/{silver_prefix}"
    gold_path   = f"s3://{bucket}/{gold_prefix}"

    logger.info(f"🚀 Gold Retailer KPI job | input={silver_path}")

    df = spark.read.parquet(silver_path)
    logger.info(f"📥 Silver records: {df.count():,}")

    # Transformations
    df = compute_rps(df)
    df = compute_wow_revenue(df)
    df = classify_visit_urgency(df)
    df = flag_top5_in_tehsil(df)

    # Select Gold columns
    gold_cols = [
        "retailer_id", "retailer_name", "state", "district", "tehsil",
        "week", "avg_weekly_revenue", "revenue_tier", "revenue_wow_pct",
        "rps_score", "stockout_risk_flag", "stockout_events",
        "weeks_of_stock", "visit_urgency", "top5_in_tehsil",
        "ingestion_date", "ingestion_timestamp",
    ]
    gold_df = df.select([c for c in gold_cols if c in df.columns])

    # Write Gold
    (
        gold_df.write
        .mode("overwrite")
        .partitionBy("state", "ingestion_date")
        .parquet(gold_path)
    )

    # Summary stats
    urgent_count = gold_df.filter(F.col("visit_urgency") == "URGENT").count()
    logger.info(f"✅ Gold written: {gold_df.count():,} rows | URGENT retailers: {urgent_count}")


run()
job.commit()
