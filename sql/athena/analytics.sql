-- ============================================================
-- Athena Analytics Queries — Business Intelligence Layer
-- Project: AWS Lakehouse Pipeline | Author: Saurabh Yadav
-- ============================================================

-- ─────────────────────────────────────────────────────────────
-- Q1: Top 10 URGENT retailers by RPS score (today's action list)
-- Use case: Field rep morning brief
-- ─────────────────────────────────────────────────────────────
SELECT
    retailer_id,
    retailer_name,
    state,
    district,
    tehsil,
    rps_score,
    visit_urgency,
    revenue_tier,
    stockout_risk_flag,
    ROUND(avg_weekly_revenue, 0)   AS revenue_inr,
    revenue_wow_pct                AS wow_change_pct
FROM lakehouse_dev.gold_retailer_kpi
WHERE ingestion_date = CURRENT_DATE
  AND visit_urgency  = 'URGENT'
ORDER BY rps_score DESC
LIMIT 10;


-- ─────────────────────────────────────────────────────────────
-- Q2: District Risk Heatmap — HIGH risk districts today
-- Use case: Territory manager dashboard
-- ─────────────────────────────────────────────────────────────
SELECT
    state,
    district,
    ROUND(drs_score, 3)            AS drs_score,
    ROUND(drs_7d_avg, 3)          AS rolling_7d_avg,
    drs_trend,
    risk_tier,
    total_pest_alerts,
    high_incidence_count,
    recommended_product
FROM lakehouse_dev.gold_district_risk
WHERE ingestion_date = CURRENT_DATE
  AND risk_tier IN ('HIGH', 'MEDIUM')
ORDER BY drs_score DESC;


-- ─────────────────────────────────────────────────────────────
-- Q3: Revenue trend — week-over-week by state
-- Use case: Business performance review
-- ─────────────────────────────────────────────────────────────
SELECT
    state,
    week,
    COUNT(DISTINCT retailer_id)       AS active_retailers,
    ROUND(SUM(avg_weekly_revenue), 0) AS total_revenue_inr,
    ROUND(AVG(rps_score), 3)         AS avg_rps,
    SUM(stockout_risk_flag)           AS stockout_risk_count,
    COUNT(CASE WHEN visit_urgency = 'URGENT' THEN 1 END) AS urgent_visits
FROM lakehouse_dev.gold_retailer_kpi
WHERE ingestion_date >= DATE_ADD('day', -30, CURRENT_DATE)
GROUP BY state, week
ORDER BY state, week;


-- ─────────────────────────────────────────────────────────────
-- Q4: Demand forecast — SKUs with growing demand next month
-- Use case: Inventory planning
-- ─────────────────────────────────────────────────────────────
SELECT
    sku_name,
    state,
    district,
    ma_4w                           AS baseline_4w_avg,
    trend_slope,
    demand_category,
    forecasted_qty_w1,
    forecasted_qty_w2,
    forecasted_qty_w3,
    forecasted_qty_w4,
    (forecasted_qty_w1 + forecasted_qty_w2
     + forecasted_qty_w3 + forecasted_qty_w4) AS total_4w_forecast,
    stockout_risk_flag
FROM lakehouse_dev.gold_demand_forecast
WHERE ingestion_date = CURRENT_DATE
  AND demand_category IN ('GROWING', 'SPIKE')
ORDER BY total_4w_forecast DESC
LIMIT 20;


-- ─────────────────────────────────────────────────────────────
-- Q5: Data quality summary — pipeline health check
-- Use case: DE monitoring dashboard
-- ─────────────────────────────────────────────────────────────
SELECT
    ingestion_date,
    COUNT(*)                        AS total_retailer_records,
    COUNT(DISTINCT retailer_id)     AS unique_retailers,
    COUNT(DISTINCT district)        AS districts_covered,
    SUM(stockout_risk_flag)         AS at_risk_retailers,
    ROUND(AVG(rps_score), 4)        AS mean_rps,
    ROUND(MIN(rps_score), 4)        AS min_rps,
    ROUND(MAX(rps_score), 4)        AS max_rps,
    COUNT(CASE WHEN visit_urgency = 'URGENT' THEN 1 END) AS urgent_count,
    COUNT(CASE WHEN visit_urgency = 'SOON'   THEN 1 END) AS soon_count,
    COUNT(CASE WHEN visit_urgency = 'ROUTINE'THEN 1 END) AS routine_count
FROM lakehouse_dev.gold_retailer_kpi
WHERE ingestion_date >= DATE_ADD('day', -7, CURRENT_DATE)
GROUP BY ingestion_date
ORDER BY ingestion_date DESC;


-- ─────────────────────────────────────────────────────────────
-- Q6: Join retailer KPI with district risk (cross-layer join)
-- Use case: Contextual visit planning — "retailer risk + district risk"
-- ─────────────────────────────────────────────────────────────
SELECT
    r.retailer_id,
    r.retailer_name,
    r.state,
    r.district,
    r.rps_score,
    r.visit_urgency,
    d.drs_score,
    d.risk_tier            AS district_risk_tier,
    d.recommended_product,
    -- Combined urgency index
    ROUND((r.rps_score * 0.6) + (d.drs_score * 0.4), 4) AS combined_priority_score
FROM lakehouse_dev.gold_retailer_kpi  r
JOIN lakehouse_dev.gold_district_risk d
  ON r.district      = d.district
 AND r.state         = d.state
 AND r.ingestion_date = d.ingestion_date
WHERE r.ingestion_date = CURRENT_DATE
  AND d.risk_tier = 'HIGH'
ORDER BY combined_priority_score DESC
LIMIT 15;
