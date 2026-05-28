# AWS Data Lakehouse Pipeline — Deployment Runbook & Learning Guide

This document is a step-by-step guide to deploying, running, monitoring, and tearing down the **AWS Data Lakehouse Pipeline** in a real AWS environment. 

We have designed this pipeline with **strict cost boundaries** (budget filters, low worker count, small data sizes) to ensure it runs entirely within the **AWS Free Tier** or at a cost of less than **₹35 (~$0.40) per run** (and $0 when idle!).

---

## 📖 Table of Contents
1. [Prerequisites & Local Tooling](#1-prerequisites--local-tooling)
2. [Setting Up a Safe AWS Account](#2-setting-up-a-safe-aws-account)
3. [Terraform Bootstrap & Deployment](#3-terraform-bootstrap--deployment)
4. [Data Ingestion & Execution](#4-data-ingestion--execution)
5. [Monitoring & Verification (Athena & Dashboards)](#5-monitoring--verification-athena--dashboards)
6. [Learning Lab: Break Things to Learn](#6-learning-lab-break-things-to-learn)
7. [Safe Tear-Down Checklist (Zero Cost)](#7-safe-tear-down-checklist-zero-cost)

---

## 1. Prerequisites & Local Tooling

Before deploying to AWS, make sure the following are installed on your system:

- **Git**: For version control.
- **Python 3.11+**: PySpark and local testing scripts are configured for this environment.
- **Terraform (v1.5.0+)**: To provision infrastructure as code. [Download Terraform](https://developer.hashicorp.com/terraform/downloads)
- **AWS CLI**: To communicate with your AWS account from your terminal. [Install AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html)

---

## 2. Setting Up a Safe AWS Account

### Step A: Create an AWS Account
1. Visit [aws.amazon.com](https://aws.amazon.com/) and sign up for a new account.
2. Select **Personal Account**.
3. You will need a credit/debit card for verification. AWS charges ₹2 ($1) for verification, which is immediately refunded.
4. Choose the **Free Support Plan**.

### Step B: Create an IAM User (Never use Root for CLI!)
1. Log in to the [AWS Management Console](https://console.aws.amazon.com/) as the root user.
2. Search for **IAM** (Identity and Access Management).
3. Click **Users** ➔ **Create user**.
   - **User name**: `saurabh-de-admin`
   - Check "Provide user access to the AWS Management Console" (Optional, but useful).
4. Click **Next** ➔ Select **Attach policies directly**.
5. Search for and select **AdministratorAccess** (gives admin permission to deploy resources via Terraform).
6. Click **Next** ➔ **Create user**.
7. Click on your newly created user `saurabh-de-admin` ➔ Go to the **Security credentials** tab.
8. Scroll to **Access keys** ➔ Click **Create access key**.
   - Choose **Command Line Interface (CLI)**.
   - Acknowledge the recommendations, click **Next**, then **Create access key**.
9. **CRITICAL**: Copy the **Access Key ID** and **Secret Access Key** and save them securely. Do NOT commit them to GitHub!

### Step C: Configure AWS CLI Locally
Open your Windows PowerShell and run:
```powershell
aws configure
```
Enter the details as prompted:
- **AWS Access Key ID**: `YOUR_ACCESS_KEY`
- **AWS Secret Access Key**: `YOUR_SECRET_ACCESS_KEY`
- **Default region name**: `ap-south-1` (Mumbai region — closest to Bengaluru, lowest latency)
- **Default output format**: `json`

Verify the configuration:
```powershell
aws sts get-caller-identity
```
You should see your account number and IAM user ARN in the response!

---

## 3. Terraform Bootstrap & Deployment

### Step A: Initialize the Directory
Navigate to the `terraform` directory in your terminal:
```powershell
cd c:\Users\gaura\Downloads\Hackathon_syngenta\Syngenta-Hackathon-2026\aws-lakehouse-pipeline\terraform
```

Initialize Terraform to download the AWS provider and plugins:
```powershell
terraform init
```

### Step B: Customize Variables
Open `terraform/variables.tf` and review the default variables.
Create a local variables file named `terraform.tfvars` to customize your environment:
```hcl
# terraform/terraform.tfvars
aws_region   = "ap-south-1"
environment  = "dev"
alert_email  = "23f1003171@ds.study.iitm.ac.in"  # Change to your primary active email
```

### Step C: Plan the Infrastructure
Run a dry run of your deployment to review the 30+ resources Terraform is going to build:
```powershell
terraform plan -out=tfplan
```
*Look at the output: S3 buckets, IAM roles, Glue jobs, Lambdas, CloudWatch dashboard, and billing budgets are listed.*

### Step D: Apply & Deploy
Deploy the infrastructure to AWS:
```powershell
terraform apply tfplan
```
*This will take about 2-3 minutes. Once complete, copy the outputs printed in the terminal (specifically the S3 bucket names, SNS Topic ARN, and Step Functions State Machine ARN).*

### Step E: Confirm Email Alerts
Check the inbox of the email address you set in `alert_email`. You will see a subscription confirmation email from AWS SNS:
- **Sender**: AWS Notifications
- **Subject**: AWS Notification - Subscription Confirmation
- **Action**: Click the **Confirm Subscription** link in the body! Without this, you will not receive budget or DQ warnings.

---

## 4. Data Ingestion & Execution

### Step A: Generate and Upload Raw Data
Run the S3 upload script to generate synthetic Syngenta CSV data and upload it to the newly created landing bucket.
*(The landing bucket name is printed in your Terraform output: e.g. `syngenta-lakehouse-dev-123456789012`)*

```powershell
cd c:\Users\gaura\Downloads\Hackathon_syngenta\Syngenta-Hackathon-2026\aws-lakehouse-pipeline\
python scripts/upload_to_s3.py --bucket syngenta-lakehouse-dev-<your-account-id>
```

### Step B: Watch the Automation Trigger
1. As soon as the CSV is uploaded to `/raw/retailer/` or `/raw/district/`, the S3 Event Notification triggers the **Pipeline Trigger Lambda** (`lambda/pipeline_trigger/handler.py`).
2. The Lambda function starts the **AWS Step Functions State Machine**.
3. Open the **AWS Console** ➔ Go to **Step Functions**.
4. Click on `syngenta-lakehouse-dev-pipeline`.
5. You will see an active execution running!
   - It will run `bronze_ingest` to convert CSVs to Parquet.
   - It will run `silver_clean` to apply cleaning rules and deduplicate.
   - It will trigger `gold_retailer_kpi`, `gold_district_risk`, and `gold_demand_forecast` in **parallel**!

*Each Glue job takes about 45 to 90 seconds to run. The entire pipeline should finish in about 3 minutes.*

---

## 5. Monitoring & Verification (Athena & Dashboards)

### Step A: Setup Glue Crawler
Once your Gold data is generated, you can crawl it to update the Athena tables.
1. In AWS Console, search for **AWS Glue** ➔ **Crawlers**.
2. Run the crawler `syngenta-lakehouse-dev-gold-crawler`. This will populate your database schema.

### Step B: Query in Athena
1. Go to **Amazon Athena** in the AWS Console.
2. In the query editor, select the **Workgroup**: `syngenta-lakehouse-dev-workgroup` (crucial to enable default query results locations).
3. Select your Database: `syngenta_lakehouse_dev_db`.
4. Run the queries from [sql/athena/analytics.sql](file:///c:/Users/gaura/Downloads/Hackathon_syngenta/Syngenta-Hackathon-2026/aws-lakehouse-pipeline/sql/athena/analytics.sql) to extract actionable business insights!

### Step C: View CloudWatch Dashboard
1. Search for **CloudWatch** in the AWS Console ➔ Go to **Dashboards**.
2. Click on `syngenta-lakehouse-dev-monitoring`.
3. Here you will see visual graphs tracking:
   - Step Functions success/failure rate.
   - Glue Job run times and active worker counts.
   - Lambda errors and execution durations.
   - S3 storage consumption per layer.

---

## 6. Learning Lab: Break Things to Learn

Hiring managers want to hear about **troubleshooting**. Use these labs to safely break your pipeline and see how AWS handles failures:

### Lab A: Trigger Data Quality Alerts (SNS Warning)
1. Edit one of the sample CSV files locally (e.g. add a line with negative `avg_weekly_revenue` or a null `retailer_id`).
2. Upload it to S3:
   ```powershell
   aws s3 cp bad_file.csv s3://syngenta-lakehouse-dev-<your-account-id>/raw/retailer/retailers_bad.csv
   ```
3. Watch the Step Function run. The `silver_clean` Glue job will execute, identify the invalid rows, send the counts to the Audit logs, and trigger the **DQ Alert Lambda**.
4. Check your email: You will receive a high-severity alert listing exactly how many rows failed DQ checks and why!

### Lab B: Fail a Job and Check Retry
1. Go to the AWS Glue Console ➔ Select `syngenta-lakehouse-dev-gold-retailer-kpi`.
2. Deliberately add a syntax error to the script directly in the Glue Console editor and save.
3. Upload a file to trigger the pipeline.
4. Watch Step Functions:
   - The Gold step will fail.
   - It will automatically attempt 2 retries (as defined in `step_functions/pipeline.json`).
   - On the 3rd failure, it will transition to the `Catch` state, notify you via SNS, and log the execution state as `FAILED`.

---

## 7. Safe Tear-Down Checklist (Zero Cost)

To ensure you are **never** billed when you are not actively using or demonstrating the project, run this clean up sequence. It will delete all AWS resources.

### Step 1: Empty S3 Buckets (Terraform cannot delete buckets that contain files)
Open PowerShell and run:
```powershell
# Empty landing bucket
aws s3 rm s3://syngenta-lakehouse-dev-<your-account-id> --recursive

# Empty Athena query results bucket (check outputs for the exact name)
aws s3 rm s3://syngenta-lakehouse-dev-athena-results-<your-account-id> --recursive
```

### Step 2: Destroy Infrastructure via Terraform
In the `terraform` directory, execute:
```powershell
terraform destroy
```
Type `yes` when prompted.
*This will delete all 30+ resources includingGlue jobs, Step Functions, Lambda functions, IAM roles, and CloudWatch metrics. Confirming this in your console leaves your account completely pristine and $0 charged.*
