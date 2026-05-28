-- ============================================================
-- Redshift Data Mart Load Scripts (COPY Commands)
-- Project: AWS Lakehouse Pipeline | Author: Saurabh Yadav
-- ============================================================

-- Set context
SET search_path TO lakehouse;

-- ─────────────────────────────────────────────────────────────
-- 1. Load fact_retailer_kpi
-- ─────────────────────────────────────────────────────────────
-- Loads from s3://<bucket-name>/gold/retailer_kpi/ partition-based format
-- In production, the IAM role is attached to the Redshift cluster.
-- The partition schema matches: .../gold/retailer_kpi/ingestion_date=YYYY-MM-DD/
-- For partitioned loading, we can copy from the root or specify a manifest.
COPY lakehouse.fact_retailer_kpi (
    retailer_id,
    retailer_name,
    state,
    district,
    tehsil,
    week,
    avg_weekly_revenue,
    revenue_tier,
    revenue_wow_pct,
    rps_score,
    stockout_risk_flag,
    stockout_events,
    weeks_of_stock,
    visit_urgency,
    top5_in_tehsil,
    ingestion_date,
    ingestion_timestamp
)
FROM 's3://YOUR-BUCKET-NAME/gold/retailer_kpi/'
IAM_ROLE 'arn:aws:iam::YOUR-ACCOUNT-ID:role/SyngentaRedshiftS3ReadRole'
FORMAT AS PARQUET;


-- ─────────────────────────────────────────────────────────────
-- 2. Load fact_district_risk
-- ─────────────────────────────────────────────────────────────
COPY lakehouse.fact_district_risk (
    district,
    state,
    date,
    drs_score,
    drs_7d_avg,
    drs_trend,
    risk_tier,
    fungicide_risk_flag,
    drought_stress_flag,
    high_humidity_flag,
    total_pest_alerts,
    high_incidence_count,
    recommended_product,
    ingestion_date
)
FROM 's3://YOUR-BUCKET-NAME/gold/district_risk/'
IAM_ROLE 'arn:aws:iam::YOUR-ACCOUNT-ID:role/SyngentaRedshiftS3ReadRole'
FORMAT AS PARQUET;


-- ─────────────────────────────────────────────────────────────
-- 3. Load fact_demand_forecast
-- ─────────────────────────────────────────────────────────────
COPY lakehouse.fact_demand_forecast (
    sku_id,
    sku_name,
    district,
    state,
    week,
    actual_qty,
    current_stock_qty,
    ma_4w,
    trend_slope,
    demand_category,
    stockout_risk_flag,
    forecasted_qty_w1,
    forecasted_qty_w2,
    forecasted_qty_w3,
    forecasted_qty_w4,
    ingestion_date
)
FROM 's3://YOUR-BUCKET-NAME/gold/demand_forecast/'
IAM_ROLE 'arn:aws:iam::YOUR-ACCOUNT-ID:role/SyngentaRedshiftS3ReadRole'
FORMAT AS PARQUET;


-- ─────────────────────────────────────────────────────────────
-- 4. Sample Incremental Upsert Strategy (SCD Type 1 for Retailer KPIs)
-- ─────────────────────────────────────────────────────────────
-- Since COPY is append-only, duplicates might happen on partition updates.
-- Here is the standard Staging Table Upsert pattern used in Production Redshift:

-- Step A: Create staging table exactly like fact table (except identity columns)
CREATE TEMP TABLE staging_retailer_kpi (LIKE lakehouse.fact_retailer_kpi);

-- Step B: COPY incremental files into the temporary staging table
COPY staging_retailer_kpi (
    retailer_id,
    retailer_name,
    state,
    district,
    tehsil,
    week,
    avg_weekly_revenue,
    revenue_tier,
    revenue_wow_pct,
    rps_score,
    stockout_risk_flag,
    stockout_events,
    weeks_of_stock,
    visit_urgency,
    top5_in_tehsil,
    ingestion_date,
    ingestion_timestamp
)
FROM 's3://YOUR-BUCKET-NAME/gold/retailer_kpi/ingestion_date=2026-05-20/'
IAM_ROLE 'arn:aws:iam::YOUR-ACCOUNT-ID:role/SyngentaRedshiftS3ReadRole'
FORMAT AS PARQUET;

-- Step C: Use a transaction to delete existing records that are being updated,
--         and insert the new versions from staging.
BEGIN TRANSACTION;

-- Delete records in the target table that match the incoming keys (retailer_id + week)
DELETE FROM lakehouse.fact_retailer_kpi
USING staging_retailer_kpi
WHERE fact_retailer_kpi.retailer_id = staging_retailer_kpi.retailer_id
  AND fact_retailer_kpi.week = staging_retailer_kpi.week;

-- Insert all records from staging
INSERT INTO lakehouse.fact_retailer_kpi (
    retailer_id,
    retailer_name,
    state,
    district,
    tehsil,
    week,
    avg_weekly_revenue,
    revenue_tier,
    revenue_wow_pct,
    rps_score,
    stockout_risk_flag,
    stockout_events,
    weeks_of_stock,
    visit_urgency,
    top5_in_tehsil,
    ingestion_date,
    ingestion_timestamp
)
SELECT 
    retailer_id,
    retailer_name,
    state,
    district,
    tehsil,
    week,
    avg_weekly_revenue,
    revenue_tier,
    revenue_wow_pct,
    rps_score,
    stockout_risk_flag,
    stockout_events,
    weeks_of_stock,
    visit_urgency,
    top5_in_tehsil,
    ingestion_date,
    ingestion_timestamp
FROM staging_retailer_kpi;

END TRANSACTION;

-- Clean up staging table
DROP TABLE staging_retailer_kpi;
