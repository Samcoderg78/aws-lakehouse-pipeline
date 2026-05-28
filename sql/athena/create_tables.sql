-- ============================================================
-- Athena External Tables — Gold Layer
-- Project: AWS Lakehouse Pipeline
-- Run in AWS Athena console (workgroup: lakehouse-pipeline-dev)
-- ============================================================

-- Replace <YOUR_BUCKET> with your actual S3 bucket name
-- (output of terraform apply → data_lake_bucket)

-- ─────────────────────────────────────────────────────────────
-- 1. Gold: Retailer KPI Table
-- ─────────────────────────────────────────────────────────────
CREATE EXTERNAL TABLE IF NOT EXISTS lakehouse_dev.gold_retailer_kpi (
    retailer_id           STRING,
    retailer_name         STRING,
    district              STRING,
    tehsil                STRING,
    week                  STRING,
    avg_weekly_revenue    DOUBLE,
    revenue_tier          STRING,
    revenue_wow_pct       DOUBLE,
    rps_score             DOUBLE,
    stockout_risk_flag    INT,
    stockout_events       INT,
    weeks_of_stock        DOUBLE,
    visit_urgency         STRING,
    top5_in_tehsil        INT,
    ingestion_timestamp   TIMESTAMP
)
PARTITIONED BY (
    state           STRING,
    ingestion_date  DATE
)
STORED AS PARQUET
LOCATION 's3://<YOUR_BUCKET>/gold/retailer_kpi/'
TBLPROPERTIES (
    'parquet.compress'      = 'SNAPPY',
    'classification'        = 'parquet',
    'projection.enabled'    = 'false'
);

-- Load partitions (run after CREATE TABLE)
MSCK REPAIR TABLE lakehouse_dev.gold_retailer_kpi;


-- ─────────────────────────────────────────────────────────────
-- 2. Gold: District Risk Table
-- ─────────────────────────────────────────────────────────────
CREATE EXTERNAL TABLE IF NOT EXISTS lakehouse_dev.gold_district_risk (
    district              STRING,
    date                  STRING,
    drs_score             DOUBLE,
    drs_7d_avg            DOUBLE,
    drs_trend             STRING,
    risk_tier             STRING,
    fungicide_risk_flag   INT,
    drought_stress_flag   INT,
    high_humidity_flag    INT,
    total_pest_alerts     INT,
    high_incidence_count  INT,
    recommended_product   STRING,
    ingestion_timestamp   TIMESTAMP
)
PARTITIONED BY (
    state           STRING,
    ingestion_date  DATE
)
STORED AS PARQUET
LOCATION 's3://<YOUR_BUCKET>/gold/district_risk/'
TBLPROPERTIES ('parquet.compress' = 'SNAPPY');

MSCK REPAIR TABLE lakehouse_dev.gold_district_risk;


-- ─────────────────────────────────────────────────────────────
-- 3. Gold: Demand Forecast Table
-- ─────────────────────────────────────────────────────────────
CREATE EXTERNAL TABLE IF NOT EXISTS lakehouse_dev.gold_demand_forecast (
    sku_id                STRING,
    sku_name              STRING,
    district              STRING,
    week                  STRING,
    actual_qty            DOUBLE,
    current_stock_qty     DOUBLE,
    ma_4w                 DOUBLE,
    trend_slope           DOUBLE,
    demand_category       STRING,
    stockout_risk_flag    INT,
    forecasted_qty_w1     DOUBLE,
    forecasted_qty_w2     DOUBLE,
    forecasted_qty_w3     DOUBLE,
    forecasted_qty_w4     DOUBLE,
    ingestion_timestamp   TIMESTAMP
)
PARTITIONED BY (
    state           STRING,
    ingestion_date  DATE
)
STORED AS PARQUET
LOCATION 's3://<YOUR_BUCKET>/gold/demand_forecast/'
TBLPROPERTIES ('parquet.compress' = 'SNAPPY');

MSCK REPAIR TABLE lakehouse_dev.gold_demand_forecast;
