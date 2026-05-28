-- ============================================================
-- Redshift Schema — Lakehouse Data Mart
-- Project: AWS Lakehouse Pipeline | Author: Saurabh Yadav
-- ============================================================

-- Create schema
CREATE SCHEMA IF NOT EXISTS lakehouse AUTHORIZATION admin;
SET search_path TO lakehouse;

-- ─────────────────────────────────────────────────────────────
-- 1. Retailer KPI Fact Table
-- ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS lakehouse.fact_retailer_kpi (
    retailer_id           VARCHAR(50)     NOT NULL,
    retailer_name         VARCHAR(200),
    state                 VARCHAR(100)    NOT NULL,
    district              VARCHAR(100)    NOT NULL,
    tehsil                VARCHAR(100),
    week                  VARCHAR(10)     NOT NULL,   -- YYYY-WW
    avg_weekly_revenue    DECIMAL(15,2),
    revenue_tier          VARCHAR(10),
    revenue_wow_pct       DECIMAL(8,2),
    rps_score             DECIMAL(6,4),
    stockout_risk_flag    SMALLINT        DEFAULT 0,
    stockout_events       INT             DEFAULT 0,
    weeks_of_stock        DECIMAL(6,2),
    visit_urgency         VARCHAR(10),
    top5_in_tehsil        SMALLINT        DEFAULT 0,
    ingestion_date        DATE            NOT NULL,
    ingestion_timestamp   TIMESTAMP,
    loaded_at             TIMESTAMP       DEFAULT GETDATE()
)
DISTSTYLE KEY
DISTKEY (district)
SORTKEY (ingestion_date, state, district)
;

-- ─────────────────────────────────────────────────────────────
-- 2. District Risk Fact Table
-- ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS lakehouse.fact_district_risk (
    district              VARCHAR(100)    NOT NULL,
    state                 VARCHAR(100)    NOT NULL,
    date                  DATE            NOT NULL,
    drs_score             DECIMAL(6,4),
    drs_7d_avg            DECIMAL(6,4),
    drs_trend             VARCHAR(10),
    risk_tier             VARCHAR(10),
    fungicide_risk_flag   SMALLINT        DEFAULT 0,
    drought_stress_flag   SMALLINT        DEFAULT 0,
    high_humidity_flag    SMALLINT        DEFAULT 0,
    total_pest_alerts     INT             DEFAULT 0,
    high_incidence_count  INT             DEFAULT 0,
    recommended_product   VARCHAR(100),
    ingestion_date        DATE            NOT NULL,
    loaded_at             TIMESTAMP       DEFAULT GETDATE()
)
DISTSTYLE KEY
DISTKEY (district)
SORTKEY (date, state)
;

-- ─────────────────────────────────────────────────────────────
-- 3. Demand Forecast Fact Table
-- ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS lakehouse.fact_demand_forecast (
    sku_id                VARCHAR(50)     NOT NULL,
    sku_name              VARCHAR(200),
    district              VARCHAR(100)    NOT NULL,
    state                 VARCHAR(100)    NOT NULL,
    week                  VARCHAR(10)     NOT NULL,
    actual_qty            DECIMAL(12,2),
    current_stock_qty     DECIMAL(12,2),
    ma_4w                 DECIMAL(12,2),
    trend_slope           DECIMAL(10,2),
    demand_category       VARCHAR(20),
    stockout_risk_flag    SMALLINT        DEFAULT 0,
    forecasted_qty_w1     DECIMAL(12,2),
    forecasted_qty_w2     DECIMAL(12,2),
    forecasted_qty_w3     DECIMAL(12,2),
    forecasted_qty_w4     DECIMAL(12,2),
    ingestion_date        DATE            NOT NULL,
    loaded_at             TIMESTAMP       DEFAULT GETDATE()
)
DISTSTYLE KEY
DISTKEY (district)
SORTKEY (ingestion_date, sku_id)
;

-- ─────────────────────────────────────────────────────────────
-- 4. Dimension: District Master
-- ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS lakehouse.dim_district (
    district_id    INT             IDENTITY(1,1),
    district       VARCHAR(100)    NOT NULL,
    state          VARCHAR(100)    NOT NULL,
    region         VARCHAR(100),
    created_at     TIMESTAMP       DEFAULT GETDATE(),
    PRIMARY KEY (district_id)
)
DISTSTYLE ALL
;

-- ─────────────────────────────────────────────────────────────
-- 5. Pipeline Run Audit Log
-- ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS lakehouse.pipeline_run_log (
    run_id              VARCHAR(100)    NOT NULL,
    execution_arn       VARCHAR(500),
    pipeline_name       VARCHAR(200),
    start_time          TIMESTAMP,
    end_time            TIMESTAMP,
    status              VARCHAR(20),     -- SUCCESS | FAILED | RUNNING
    rows_bronze         BIGINT,
    rows_silver         BIGINT,
    rows_gold           BIGINT,
    dq_failures         INT             DEFAULT 0,
    triggered_by        VARCHAR(100),    -- s3_event | scheduled | manual
    created_at          TIMESTAMP       DEFAULT GETDATE(),
    PRIMARY KEY (run_id)
)
DISTSTYLE ALL
SORTKEY (start_time)
;
