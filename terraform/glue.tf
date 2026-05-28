##############################################################################
# glue.tf — AWS Glue Jobs, Crawlers, Database, and Catalog
##############################################################################

# ── Glue Data Catalog Database ───────────────────────────────────────────────
resource "aws_glue_catalog_database" "lakehouse" {
  name        = "${var.project_name}_${var.environment}"
  description = "AWS Lakehouse Pipeline — Glue Data Catalog"
}

# ── Glue Security Configuration (encryption at rest) ─────────────────────────
resource "aws_glue_security_configuration" "pipeline" {
  name = "${var.project_name}-${var.environment}-security"

  encryption_configuration {
    cloudwatch_encryption  { cloudwatch_encryption_mode = "DISABLED" }
    job_bookmarks_encryption { job_bookmarks_encryption_mode = "DISABLED" }
    s3_encryption {
      s3_encryption_mode = "SSE-S3"
    }
  }
}

# ── Glue Jobs ────────────────────────────────────────────────────────────────
locals {
  common_glue_args = {
    "--RAW_BUCKET"         = aws_s3_bucket.data_lake.bucket
    "--DATABASE_NAME"      = aws_glue_catalog_database.lakehouse.name
    "--SNS_TOPIC_ARN"      = aws_sns_topic.pipeline_alerts.arn
    "--DQ_RESULTS_PREFIX"  = "dq-reports"
    "--enable-metrics"     = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-glue-datacatalog" = "true"
    "--job-language"       = "python"
    "--TempDir"            = "s3://${aws_s3_bucket.glue_scripts.bucket}/tmp/"
  }
}

resource "aws_glue_job" "bronze_ingest" {
  name              = "${var.project_name}-${var.environment}-bronze-ingest"
  role_arn          = aws_iam_role.glue_service_role.arn
  glue_version      = "4.0"
  worker_type       = var.glue_worker_type
  number_of_workers = var.glue_num_workers
  timeout           = var.glue_timeout_minutes
  max_retries       = 1

  security_configuration = aws_glue_security_configuration.pipeline.name

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.glue_scripts.bucket}/scripts/bronze_ingest.py"
    python_version  = "3"
  }

  default_arguments = merge(local.common_glue_args, {
    "--JOB_NAME"      = "${var.project_name}-${var.environment}-bronze-ingest"
    "--RAW_PREFIX"    = "raw/retailer/"
    "--BRONZE_PREFIX" = "bronze/retailer/"
    "--TABLE_NAME"    = "retailer"
  })

  tags = { Layer = "bronze" }
}

resource "aws_glue_job" "silver_clean" {
  name              = "${var.project_name}-${var.environment}-silver-clean"
  role_arn          = aws_iam_role.glue_service_role.arn
  glue_version      = "4.0"
  worker_type       = var.glue_worker_type
  number_of_workers = var.glue_num_workers
  timeout           = var.glue_timeout_minutes
  max_retries       = 1

  security_configuration = aws_glue_security_configuration.pipeline.name

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.glue_scripts.bucket}/scripts/silver_clean.py"
    python_version  = "3"
  }

  default_arguments = merge(local.common_glue_args, {
    "--JOB_NAME"        = "${var.project_name}-${var.environment}-silver-clean"
    "--BRONZE_PREFIX"   = "bronze/retailer/"
    "--SILVER_PREFIX"   = "silver/retailer/"
    "--TABLE_NAME"      = "retailer"
  })

  tags = { Layer = "silver" }
}

resource "aws_glue_job" "gold_retailer_kpi" {
  name              = "${var.project_name}-${var.environment}-gold-retailer-kpi"
  role_arn          = aws_iam_role.glue_service_role.arn
  glue_version      = "4.0"
  worker_type       = var.glue_worker_type
  number_of_workers = var.glue_num_workers
  timeout           = var.glue_timeout_minutes
  max_retries       = 0

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.glue_scripts.bucket}/scripts/gold_retailer_kpi.py"
    python_version  = "3"
  }

  default_arguments = merge(local.common_glue_args, {
    "--JOB_NAME"      = "${var.project_name}-${var.environment}-gold-retailer-kpi"
    "--SILVER_PREFIX" = "silver/retailer/"
    "--GOLD_PREFIX"   = "gold/retailer_kpi/"
  })

  tags = { Layer = "gold" }
}

resource "aws_glue_job" "gold_district_risk" {
  name              = "${var.project_name}-${var.environment}-gold-district-risk"
  role_arn          = aws_iam_role.glue_service_role.arn
  glue_version      = "4.0"
  worker_type       = var.glue_worker_type
  number_of_workers = var.glue_num_workers
  timeout           = var.glue_timeout_minutes
  max_retries       = 0

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.glue_scripts.bucket}/scripts/gold_district_risk.py"
    python_version  = "3"
  }

  default_arguments = merge(local.common_glue_args, {
    "--JOB_NAME"      = "${var.project_name}-${var.environment}-gold-district-risk"
    "--SILVER_PREFIX" = "silver/district/"
    "--GOLD_PREFIX"   = "gold/district_risk/"
  })

  tags = { Layer = "gold" }
}

resource "aws_glue_job" "gold_demand_forecast" {
  name              = "${var.project_name}-${var.environment}-gold-demand-forecast"
  role_arn          = aws_iam_role.glue_service_role.arn
  glue_version      = "4.0"
  worker_type       = var.glue_worker_type
  number_of_workers = var.glue_num_workers
  timeout           = var.glue_timeout_minutes
  max_retries       = 0

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.glue_scripts.bucket}/scripts/gold_demand_forecast.py"
    python_version  = "3"
  }

  default_arguments = merge(local.common_glue_args, {
    "--JOB_NAME"      = "${var.project_name}-${var.environment}-gold-demand-forecast"
    "--SILVER_PREFIX" = "silver/demand/"
    "--GOLD_PREFIX"   = "gold/demand_forecast/"
  })

  tags = { Layer = "gold" }
}

# ── Glue Crawlers (auto-update catalog schema after each run) ─────────────────
resource "aws_glue_crawler" "gold_crawler" {
  name          = "${var.project_name}-${var.environment}-gold-crawler"
  database_name = aws_glue_catalog_database.lakehouse.name
  role          = aws_iam_role.glue_service_role.arn
  schedule      = "cron(30 2 * * ? *)"   # 30 min after pipeline runs

  s3_target { path = "s3://${aws_s3_bucket.data_lake.bucket}/gold/" }

  schema_change_policy {
    delete_behavior = "LOG"
    update_behavior = "UPDATE_IN_DATABASE"
  }

  configuration = jsonencode({
    Version = 1.0
    CrawlerOutput = {
      Partitions = { AddOrUpdateBehavior = "InheritFromTable" }
    }
  })
}

# ── Athena Workgroup ─────────────────────────────────────────────────────────
resource "aws_athena_workgroup" "lakehouse" {
  name  = "${var.project_name}-${var.environment}"
  state = "ENABLED"

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true

    result_configuration {
      output_location = "s3://${aws_s3_bucket.athena_results.bucket}/results/"

      encryption_configuration {
        encryption_option = "SSE_S3"
      }
    }

    engine_version {
      selected_engine_version = "Athena engine version 3"
    }
  }
}
