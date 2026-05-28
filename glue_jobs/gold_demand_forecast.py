"""
gold_demand_forecast.py — AWS Glue PySpark Job
================================================
Layer   : SILVER → GOLD
Purpose : Compute 4-week rolling demand forecast per SKU per district.
          Uses a simple capped moving average (production-grade baseline).
          Output consumed by Athena dashboards and Redshift data mart.

Gold output schema (partitioned by state, ingestion_date):
  sku_id, sku_name, district, state,
  week, actual_qty, ma_4w, trend_slope,
  forecasted_qty_w1..w4, demand_category,
  stockout_risk_flag, ingestion_date
"""

import sys
import logging

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F, Window

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


def compute_4w_moving_average(df):
    """4-week rolling average of actual_qty per SKU per district."""
    w = (
        Window.partitionBy("sku_id", "district")
              .orderBy("week")
              .rowsBetween(-3, 0)           # current + 3 prior weeks
    )
    return df.withColumn("ma_4w", F.round(F.avg("actual_qty").over(w), 2))


def compute_trend_slope(df):
    """
    Simple linear trend: avg change in qty over last 4 weeks.
    Positive = growing demand, Negative = shrinking demand.
    """
    w = Window.partitionBy("sku_id", "district").orderBy("week")
    df = df.withColumn("qty_lag1", F.lag("actual_qty", 1).over(w))
    df = df.withColumn("qty_lag4", F.lag("actual_qty", 4).over(w))
    df = df.withColumn(
        "trend_slope",
        F.when(
            F.col("qty_lag4").isNotNull() & (F.col("qty_lag4") != 0),
            F.round((F.col("actual_qty") - F.col("qty_lag4")) / 4.0, 2)
        ).otherwise(F.lit(0.0))
    ).drop("qty_lag1", "qty_lag4")
    return df


def forecast_next_4_weeks(df):
    """
    Project 4 future weeks using capped moving average + trend:
    forecast_wN = ma_4w + N * trend_slope, floored at 0.
    """
    cap_factor = 1.5   # max 50% above historical MA (anti-spike cap)

    for n in range(1, 5):
        df = df.withColumn(
            f"forecasted_qty_w{n}",
            F.greatest(
                F.lit(0.0),
                F.least(
                    F.col("ma_4w") * F.lit(cap_factor),
                    F.round(F.col("ma_4w") + F.lit(n) * F.col("trend_slope"), 0)
                )
            )
        )
    return df


def classify_demand(df):
    """
    demand_category based on trend_slope:
    SPIKE   : slope > 20% of ma_4w
    GROWING : slope > 0
    STABLE  : slope ≈ 0 (±5)
    DECLINING : slope < 0
    """
    return df.withColumn(
        "demand_category",
        F.when(F.col("trend_slope") > F.col("ma_4w") * 0.20, "SPIKE")
         .when(F.col("trend_slope") > 5,   "GROWING")
         .when(F.col("trend_slope") < -5,  "DECLINING")
         .otherwise("STABLE")
    )


def flag_stockout_risk(df):
    """Flag SKU/district combos where forecasted_qty_w1 > current stock."""
    return df.withColumn(
        "stockout_risk_flag",
        F.when(
            F.col("forecasted_qty_w1") > F.col("current_stock_qty"),
            1
        ).otherwise(0)
    )


def run():
    bucket        = args["RAW_BUCKET"]
    silver_prefix = args["SILVER_PREFIX"].rstrip("/")
    gold_prefix   = args["GOLD_PREFIX"].rstrip("/")

    silver_path = f"s3://{bucket}/{silver_prefix}"
    gold_path   = f"s3://{bucket}/{gold_prefix}"

    logger.info(f"🚀 Gold Demand Forecast job | input={silver_path}")

    df = spark.read.parquet(silver_path)
    logger.info(f"📥 Silver records: {df.count():,}")

    # Ensure current_stock_qty col exists (default 0 if missing)
    if "current_stock_qty" not in df.columns:
        df = df.withColumn("current_stock_qty", F.lit(0.0))

    df = compute_4w_moving_average(df)
    df = compute_trend_slope(df)
    df = forecast_next_4_weeks(df)
    df = classify_demand(df)
    df = flag_stockout_risk(df)

    gold_cols = [
        "sku_id", "sku_name", "district", "state", "week",
        "actual_qty", "current_stock_qty",
        "ma_4w", "trend_slope", "demand_category", "stockout_risk_flag",
        "forecasted_qty_w1", "forecasted_qty_w2",
        "forecasted_qty_w3", "forecasted_qty_w4",
        "ingestion_date", "ingestion_timestamp",
    ]
    gold_df = df.select([c for c in gold_cols if c in df.columns])

    (
        gold_df.write
        .mode("overwrite")
        .partitionBy("state", "ingestion_date")
        .parquet(gold_path)
    )

    spikes = gold_df.filter(F.col("demand_category") == "SPIKE").count()
    stockout = gold_df.filter(F.col("stockout_risk_flag") == 1).count()
    logger.info(
        f"✅ Gold Forecast written: {gold_df.count():,} rows | "
        f"SPIKES: {spikes} | Stockout risk SKUs: {stockout}"
    )


run()
job.commit()
