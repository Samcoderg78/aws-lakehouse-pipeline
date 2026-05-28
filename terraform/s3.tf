##############################################################################
# s3.tf — S3 Data Lake Buckets (Raw / Bronze / Silver / Gold)
##############################################################################

# ── Main Data Lake Bucket ───────────────────────────────────────────────────
resource "aws_s3_bucket" "data_lake" {
  bucket        = local.bucket_name
  force_destroy = var.environment == "dev" ? true : false

  lifecycle {
    prevent_destroy = false
  }
}

resource "aws_s3_bucket_versioning" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "data_lake" {
  bucket                  = aws_s3_bucket.data_lake.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# ── Lifecycle rules: auto-transition old data to cheaper storage ─────────────
resource "aws_s3_bucket_lifecycle_configuration" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id

  rule {
    id     = "bronze-to-ia"
    status = "Enabled"
    filter { prefix = "bronze/" }
    transition {
      days          = 30
      storage_class = "STANDARD_IA"
    }
  }

  rule {
    id     = "raw-cleanup"
    status = "Enabled"
    filter { prefix = "raw/" }
    expiration {
      days = 90   # raw files auto-deleted after 90 days
    }
  }

  rule {
    id     = "gold-glacier"
    status = "Enabled"
    filter { prefix = "gold/" }
    transition {
      days          = 180
      storage_class = "GLACIER_IR"
    }
  }
}

# ── S3 "folders" (prefix objects) ───────────────────────────────────────────
locals {
  lake_prefixes = ["raw/", "bronze/", "silver/", "gold/", "dq-reports/", "glue-scripts/"]
}

resource "aws_s3_object" "lake_prefixes" {
  for_each = toset(local.lake_prefixes)
  bucket   = aws_s3_bucket.data_lake.id
  key      = each.value
  content  = ""
}

# ── Glue Scripts Bucket (separate, scripts only) ────────────────────────────
resource "aws_s3_bucket" "glue_scripts" {
  bucket        = "${var.project_name}-${var.environment}-glue-scripts-${local.account_id}"
  force_destroy = true
}

resource "aws_s3_bucket_public_access_block" "glue_scripts" {
  bucket                  = aws_s3_bucket.glue_scripts.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# ── Upload Glue job scripts to S3 ───────────────────────────────────────────
locals {
  glue_scripts = {
    "bronze_ingest"       = "../glue_jobs/bronze_ingest.py"
    "silver_clean"        = "../glue_jobs/silver_clean.py"
    "gold_retailer_kpi"   = "../glue_jobs/gold_retailer_kpi.py"
    "gold_district_risk"  = "../glue_jobs/gold_district_risk.py"
    "gold_demand_forecast" = "../glue_jobs/gold_demand_forecast.py"
  }
}

resource "aws_s3_object" "glue_scripts" {
  for_each = local.glue_scripts
  bucket   = aws_s3_bucket.glue_scripts.id
  key      = "scripts/${each.key}.py"
  source   = each.value
  etag     = filemd5(each.value)
}

# ── Athena Query Results Bucket ─────────────────────────────────────────────
resource "aws_s3_bucket" "athena_results" {
  bucket        = "${var.project_name}-${var.environment}-athena-results-${local.account_id}"
  force_destroy = true
}

resource "aws_s3_bucket_lifecycle_configuration" "athena_results" {
  bucket = aws_s3_bucket.athena_results.id
  rule {
    id     = "cleanup-old-results"
    status = "Enabled"
    filter {}
    expiration { days = 7 }
  }
}
