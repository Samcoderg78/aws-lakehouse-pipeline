# 🏗️ AWS End-to-End Data Lakehouse Pipeline

> **Production-grade, real-time + batch data pipeline** built on AWS — covering the full Data Engineering stack:  
> Kinesis Streaming · S3 Medallion Lake · Glue PySpark ETL · Step Functions · Athena · Redshift · CloudWatch · Terraform IaC · GitHub Actions CI/CD

---

## 📐 Architecture

```
                        ┌─────────────────────────────────────────────────────────────────┐
                        │                      INGESTION LAYER                           │
                        │                                                                 │
                        │   CSV / API  ──►  Kinesis Data Firehose  ──►  S3 Raw (JSON)    │
                        │   (Batch)   ──►  AWS Glue Crawler        ──►  Glue Catalog     │
                        └─────────────────────────┬───────────────────────────────────────┘
                                                  │
                                                  ▼
                        ┌─────────────────────────────────────────────────────────────────┐
                        │                   PROCESSING LAYER (AWS Glue PySpark)           │
                        │                                                                 │
                        │  🥉 BRONZE  Raw → Parquet (schema enforcement, dedup)          │
                        │       │                                                         │
                        │       ▼                                                         │
                        │  🥈 SILVER  Clean → validate nulls, cast types, DQ checks      │
                        │       │                                                         │
                        │       ▼                                                         │
                        │  🥇 GOLD   Aggregate → KPIs, risk scores, forecasts            │
                        └─────────────────────────┬───────────────────────────────────────┘
                                                  │
                                                  ▼
                        ┌─────────────────────────────────────────────────────────────────┐
                        │              ORCHESTRATION (AWS Step Functions)                 │
                        │                                                                 │
                        │  S3 Upload → Lambda Trigger → Step Functions State Machine     │
                        │  [Ingest] → [Validate] → [Transform] → [Aggregate] → [Alert]  │
                        │                                                                 │
                        │  ⏰ EventBridge Schedule: Daily 2 AM UTC                       │
                        └─────────────────────────┬───────────────────────────────────────┘
                                                  │
                                                  ▼
                        ┌─────────────────────────────────────────────────────────────────┐
                        │                  SERVING / ANALYTICS LAYER                     │
                        │                                                                 │
                        │   Athena (ad-hoc SQL over S3 Gold)                             │
                        │   Redshift Serverless (BI data mart)                           │
                        │   QuickSight (optional dashboards)                             │
                        └─────────────────────────┬───────────────────────────────────────┘
                                                  │
                                                  ▼
                        ┌─────────────────────────────────────────────────────────────────┐
                        │               MONITORING & GOVERNANCE                          │
                        │                                                                 │
                        │   CloudWatch Metrics + Alarms    SNS Email Alerts              │
                        │   Glue Data Catalog (schema registry)                          │
                        │   IAM Least-Privilege Roles      VPC (Redshift isolation)      │
                        └─────────────────────────────────────────────────────────────────┘
```

---

## 🧰 AWS Services Used

| Layer | Service | Purpose |
|---|---|---|
| Ingestion | **Kinesis Data Firehose** | Real-time streaming ingestion |
| Ingestion | **S3** | Raw, Bronze, Silver, Gold data lake layers |
| Processing | **AWS Glue** | PySpark ETL jobs, crawlers, Data Catalog |
| Orchestration | **Step Functions** | Pipeline DAG with error handling & retry |
| Trigger | **Lambda** | S3 event → Step Functions trigger |
| Scheduler | **EventBridge** | Daily pipeline cron schedule |
| Analytics | **Athena** | Serverless SQL over S3 Gold layer |
| Analytics | **Redshift Serverless** | Columnar data mart for BI |
| Monitoring | **CloudWatch** | Metrics, logs, alarms |
| Alerting | **SNS** | Pipeline failure & DQ violation alerts |
| Governance | **IAM** | Least-privilege roles for every service |
| IaC | **Terraform** | All resources provisioned as code |
| CI/CD | **GitHub Actions** | Lint → Test → Deploy on every push |

---

## 📂 Project Structure

```
aws-lakehouse-pipeline/
├── terraform/                    # Infrastructure as Code (Terraform)
│   ├── main.tf                   # Provider config, backend
│   ├── variables.tf              # All configurable variables
│   ├── s3.tf                     # S3 buckets (raw/bronze/silver/gold)
│   ├── glue.tf                   # Glue jobs, crawlers, catalog
│   ├── kinesis.tf                # Kinesis Firehose delivery stream
│   ├── step_functions.tf         # Step Functions state machine
│   ├── lambda.tf                 # Lambda trigger functions
│   ├── iam.tf                    # IAM roles and policies
│   ├── redshift.tf               # Redshift Serverless namespace
│   └── outputs.tf                # Output values (ARNs, endpoints)
│
├── glue_jobs/                    # PySpark ETL scripts
│   ├── bronze_ingest.py          # Raw → Bronze (CSV to Parquet)
│   ├── silver_clean.py           # Bronze → Silver (validate + dedupe)
│   ├── gold_retailer_kpi.py      # Silver → Gold (Retailer KPIs)
│   ├── gold_district_risk.py     # Silver → Gold (District Risk Score)
│   └── gold_demand_forecast.py   # Silver → Gold (Demand Forecast)
│
├── lambda/
│   ├── pipeline_trigger/         # S3 PutObject → trigger Step Functions
│   │   └── handler.py
│   └── dq_alert/                 # DQ failure → SNS notification
│       └── handler.py
│
├── step_functions/
│   └── pipeline.json             # Step Functions ASL state machine
│
├── sql/
│   ├── athena/
│   │   ├── create_tables.sql     # Athena external table DDL
│   │   └── analytics.sql         # Business intelligence queries
│   └── redshift/
│       ├── schema.sql            # Redshift schema + tables
│       └── data_mart.sql         # Data mart population queries
│
├── data_quality/
│   └── dq_checks.py             # Reusable DQ rule engine (PySpark)
│
├── monitoring/
│   └── cloudwatch_dashboard.json # CloudWatch dashboard definition
│
├── scripts/
│   ├── local_test.py             # Run Glue jobs locally (no AWS needed)
│   └── upload_to_s3.py           # Upload local CSVs to S3 raw prefix
│
└── .github/
    └── workflows/
        └── deploy.yml            # CI/CD: lint → test → deploy
```

---

## 🚀 Quick Start

### Prerequisites
```bash
pip install pyspark boto3 pandas pyarrow great_expectations
```

### 1. Run Locally (No AWS Required)
```bash
# Test all Glue jobs locally using sample data
python scripts/local_test.py
```

### 2. Deploy to AWS (with Terraform)
```bash
# Configure AWS credentials
aws configure

# Initialize and apply Terraform
cd terraform/
terraform init
terraform plan -out=tfplan
terraform apply tfplan
```

### 3. Trigger the Pipeline
```bash
# Upload raw data → triggers Lambda → triggers Step Functions
python scripts/upload_to_s3.py --env dev
```

### 4. Query Results in Athena
```sql
-- Run in AWS Athena console
SELECT district, AVG(district_risk_score) as avg_risk
FROM gold_district_risk
WHERE date_partition = '2026-05-23'
GROUP BY district
ORDER BY avg_risk DESC;
```

---

## 📊 Data Flow

The pipeline processes agricultural field intelligence data through 3 lake layers:

| Layer | Format | Description |
|---|---|---|
| **Raw** | JSON / CSV | Landing zone — untouched source data |
| **Bronze** | Parquet | Schema-enforced, typed, partitioned by date |
| **Silver** | Parquet | Cleaned, deduplicated, DQ-validated |
| **Gold** | Parquet | Business-ready KPIs and aggregations |

### Gold Layer Tables

| Table | Key Metrics |
|---|---|
| `gold_retailer_kpi` | RPS score, stockout risk, revenue per visit |
| `gold_district_risk` | DRS score, pest alerts, fungicide risk flag |
| `gold_demand_forecast` | 4-week SKU forecast, trend direction |
| `gold_anomalies` | Demand spikes, stockout clusters |

---

## 🧪 Data Quality Framework

Built using PySpark DataFrame assertions — rules run in Silver Glue job:

| Rule | Check |
|---|---|
| Not Null | `retailer_id`, `district`, `date` cannot be null |
| Range | `revenue` ≥ 0, `stock_qty` ≥ 0 |
| Uniqueness | No duplicate `(retailer_id, week)` combinations |
| Referential | `district` must exist in district master |
| Freshness | `ingestion_timestamp` within last 48 hours |

DQ failures trigger SNS email alert with row-level diagnostics.

---

## ⚙️ Step Functions State Machine

```
StartExecution
    │
    ├──► [BronzeIngestJob]     ──failure──► [HandleFailure] ──► [SNSAlert]
    │         │
    │         ▼
    ├──► [SilverCleanJob]      ──failure──► [HandleFailure] ──► [SNSAlert]
    │         │
    │         ▼
    ├──► [GoldRetailerKPI]   ─┐
    │    [GoldDistrictRisk]  ─┼─ Parallel ──► [GoldComplete]
    │    [GoldForecast]      ─┘
    │         │
    │         ▼
    └──► [SuccessNotification]
```

---

## 📈 Resume Talking Points

> Use these in interviews to explain this project:

1. **"I built a medallion architecture (Bronze/Silver/Gold) on S3 using AWS Glue PySpark jobs"**
2. **"I implemented real-time ingestion using Kinesis Firehose with a configurable delivery window"**
3. **"I orchestrated the pipeline with Step Functions, adding parallel execution for Gold aggregations and retry logic with exponential backoff"**
4. **"I wrote 5 data quality rules in PySpark that run in the Silver layer and trigger SNS alerts on failures"**
5. **"All infrastructure is provisioned with Terraform, and deployment is automated via GitHub Actions CI/CD"**

---

## 🏷️ Tags

`aws` `data-engineering` `pyspark` `glue` `s3` `kinesis` `step-functions` `athena` `redshift` `terraform` `github-actions` `data-lakehouse` `medallion-architecture` `etl-pipeline` `data-quality`
