"""
gold_district_risk.py — AWS Glue PySpark Job
==============================================
Layer   : SILVER → GOLD
Purpose : Aggregate district-level weather + pest data into
          District Risk Score (DRS) table with rolling stats.

DRS formula:
  DRS = 0.35 * norm(total_pest_alerts)
      + 0.30 * fungicide_risk_flag
      + 0.20 * high_humidity_flag
      + 0.15 * drought_stress_flag

Gold output schema (partitioned by state, ingestion_date):
  district, state, date, drs_score, risk_tier,
  fungicide_risk_flag, drought_stress_flag, high_humidity_flag,
  total_pest_alerts, high_incidence_count,
  drs_7d_avg, drs_trend (UP/DOWN/STABLE),
  recommended_product, ingestion_date
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

# DRS weights
W_PEST      = 0.35
W_FUNGICIDE = 0.30
W_HUMIDITY  = 0.20
W_DROUGHT   = 0.15


def compute_drs(df):
    """Compute District Risk Score [0, 1]."""
    # Normalise pest alerts
    max_pest = df.agg(F.max("total_pest_alerts")).collect()[0][0] or 1
    df = df.withColumn("norm_pest", F.col("total_pest_alerts") / F.lit(float(max_pest)))

    df = df.withColumn(
        "drs_score",
        F.round(
            W_PEST      * F.col("norm_pest")
            + W_FUNGICIDE * F.col("fungicide_risk_flag").cast("double")
            + W_HUMIDITY  * F.col("high_humidity_flag").cast("double")
            + W_DROUGHT   * F.col("drought_stress_flag").cast("double"),
            4
        )
    )
    return df


def assign_risk_tier(df):
    """
    HIGH  : drs_score >= 0.65
    MEDIUM: drs_score >= 0.35
    LOW   : drs_score < 0.35
    """
    return df.withColumn(
        "risk_tier",
        F.when(F.col("drs_score") >= 0.65, "HIGH")
         .when(F.col("drs_score") >= 0.35, "MEDIUM")
         .otherwise("LOW")
    )


def compute_7d_rolling_avg(df):
    """7-day rolling average DRS per district."""
    order_col = "date" if "date" in df.columns else "ingestion_date"
    w = (
        Window.partitionBy("district")
              .orderBy(F.col(order_col).cast("timestamp").cast("long"))
              .rowsBetween(-6, 0)
    )
    return df.withColumn("drs_7d_avg", F.round(F.avg("drs_score").over(w), 4))


def compute_trend(df):
    """
    Compare current DRS to previous row.
    UP if delta > +0.05, DOWN if delta < -0.05, else STABLE.
    """
    order_col = "date" if "date" in df.columns else "ingestion_date"
    w = Window.partitionBy("district").orderBy(order_col)
    df = df.withColumn("prev_drs", F.lag("drs_score", 1).over(w))
    df = df.withColumn(
        "drs_delta", F.col("drs_score") - F.col("prev_drs")
    )
    df = df.withColumn(
        "drs_trend",
        F.when(F.col("prev_drs").isNull(), "STABLE")
         .when(F.col("drs_delta") > 0.05, "UP")
         .when(F.col("drs_delta") < -0.05, "DOWN")
         .otherwise("STABLE")
    ).drop("prev_drs", "drs_delta")
    return df


def recommend_product(df):
    """
    Explainable product recommendation per Syngenta heuristic:
    1. fungicide_risk_flag=1  → Score 250 EC
    2. high_incidence_count>10 → Actara 25 WG
    3. drought_stress_flag=1   → Topik 15 WP
    4. else                    → Monitor (digital demand lookup)
    """
    return df.withColumn(
        "recommended_product",
        F.when(F.col("fungicide_risk_flag") == 1,       "Score 250 EC")
         .when(F.col("high_incidence_count") > 10,       "Actara 25 WG")
         .when(F.col("drought_stress_flag") == 1,        "Topik 15 WP")
         .otherwise("Monitor Digital Demand")
    )


def run():
    bucket        = args["RAW_BUCKET"]
    silver_prefix = args["SILVER_PREFIX"].rstrip("/")
    gold_prefix   = args["GOLD_PREFIX"].rstrip("/")

    silver_path = f"s3://{bucket}/{silver_prefix}"
    gold_path   = f"s3://{bucket}/{gold_prefix}"

    logger.info(f"🚀 Gold District Risk job | input={silver_path}")

    df = spark.read.parquet(silver_path)
    logger.info(f"📥 Silver records: {df.count():,}")

    df = compute_drs(df)
    df = assign_risk_tier(df)
    df = compute_7d_rolling_avg(df)
    df = compute_trend(df)
    df = recommend_product(df)

    gold_cols = [
        "district", "state", "date",
        "drs_score", "drs_7d_avg", "drs_trend", "risk_tier",
        "fungicide_risk_flag", "drought_stress_flag", "high_humidity_flag",
        "total_pest_alerts", "high_incidence_count",
        "recommended_product",
        "ingestion_date", "ingestion_timestamp",
    ]
    gold_df = df.select([c for c in gold_cols if c in df.columns])

    (
        gold_df.write
        .mode("overwrite")
        .partitionBy("state", "ingestion_date")
        .parquet(gold_path)
    )

    high_risk = gold_df.filter(F.col("risk_tier") == "HIGH").count()
    logger.info(f"✅ Gold District Risk written: {gold_df.count():,} rows | HIGH risk districts: {high_risk}")


run()
job.commit()
