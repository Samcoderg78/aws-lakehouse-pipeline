##############################################################################
# lambda.tf — Lambda Functions + SNS + Kinesis Firehose + CloudWatch Alarms
##############################################################################

# ── SNS Topic for all pipeline alerts ────────────────────────────────────────
resource "aws_sns_topic" "pipeline_alerts" {
  name = "${var.project_name}-${var.environment}-alerts"
}

resource "aws_sns_topic_subscription" "email_alert" {
  topic_arn = aws_sns_topic.pipeline_alerts.arn
  protocol  = "email"
  endpoint  = var.alert_email
}

# ── Lambda: Pipeline Trigger (S3 event → Step Functions) ────────────────────
data "archive_file" "pipeline_trigger_zip" {
  type        = "zip"
  source_file = "${path.module}/../lambda/pipeline_trigger/handler.py"
  output_path = "${path.module}/../lambda/pipeline_trigger/handler.zip"
}

resource "aws_lambda_function" "pipeline_trigger" {
  function_name    = "${var.project_name}-${var.environment}-pipeline-trigger"
  role             = aws_iam_role.lambda_role.arn
  filename         = data.archive_file.pipeline_trigger_zip.output_path
  source_code_hash = data.archive_file.pipeline_trigger_zip.output_base64sha256
  handler          = "handler.lambda_handler"
  runtime          = "python3.12"
  timeout          = 30

  environment {
    variables = {
      STATE_MACHINE_ARN = aws_sfn_state_machine.pipeline.arn
      ENVIRONMENT       = var.environment
    }
  }
}

# Allow S3 to invoke Lambda
resource "aws_lambda_permission" "s3_trigger" {
  statement_id  = "AllowS3Invoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.pipeline_trigger.function_name
  principal     = "s3.amazonaws.com"
  source_arn    = aws_s3_bucket.data_lake.arn
}

# S3 event notification: raw/ prefix → trigger Lambda
resource "aws_s3_bucket_notification" "raw_upload_trigger" {
  bucket = aws_s3_bucket.data_lake.id

  lambda_function {
    lambda_function_arn = aws_lambda_function.pipeline_trigger.arn
    events              = ["s3:ObjectCreated:*"]
    filter_prefix       = "raw/"
    filter_suffix       = ".csv"
  }

  depends_on = [aws_lambda_permission.s3_trigger]
}

resource "aws_cloudwatch_log_group" "pipeline_trigger_logs" {
  name              = "/aws/lambda/${aws_lambda_function.pipeline_trigger.function_name}"
  retention_in_days = 7
}

# ── Lambda: DQ Alert (invoked by Step Functions on DQ failure) ────────────────
data "archive_file" "dq_alert_zip" {
  type        = "zip"
  source_file = "${path.module}/../lambda/dq_alert/handler.py"
  output_path = "${path.module}/../lambda/dq_alert/handler.zip"
}

resource "aws_lambda_function" "dq_alert" {
  function_name    = "${var.project_name}-${var.environment}-dq-alert"
  role             = aws_iam_role.lambda_role.arn
  filename         = data.archive_file.dq_alert_zip.output_path
  source_code_hash = data.archive_file.dq_alert_zip.output_base64sha256
  handler          = "handler.lambda_handler"
  runtime          = "python3.12"
  timeout          = 30

  environment {
    variables = {
      SNS_TOPIC_ARN = aws_sns_topic.pipeline_alerts.arn
      ENVIRONMENT   = var.environment
    }
  }
}

resource "aws_cloudwatch_log_group" "dq_alert_logs" {
  name              = "/aws/lambda/${aws_lambda_function.dq_alert.function_name}"
  retention_in_days = 7
}



# ── CloudWatch Alarms ─────────────────────────────────────────────────────────
resource "aws_cloudwatch_metric_alarm" "glue_bronze_failure" {
  alarm_name          = "${var.project_name}-${var.environment}-bronze-failure"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = 1
  metric_name         = "glue.driver.aggregate.numFailedTasks"
  namespace           = "AWS/Glue"
  period              = 300
  statistic           = "Sum"
  threshold           = 1
  alarm_description   = "Bronze Glue job has failed tasks"
  alarm_actions       = [aws_sns_topic.pipeline_alerts.arn]

  dimensions = {
    JobName = aws_glue_job.bronze_ingest.name
    Type    = "count"
  }
}

resource "aws_cloudwatch_metric_alarm" "glue_silver_failure" {
  alarm_name          = "${var.project_name}-${var.environment}-silver-failure"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = 1
  metric_name         = "glue.driver.aggregate.numFailedTasks"
  namespace           = "AWS/Glue"
  period              = 300
  statistic           = "Sum"
  threshold           = 1
  alarm_description   = "Silver Glue job has failed tasks"
  alarm_actions       = [aws_sns_topic.pipeline_alerts.arn]

  dimensions = {
    JobName = aws_glue_job.silver_clean.name
    Type    = "count"
  }
}

# ── CloudWatch Dashboard ──────────────────────────────────────────────────────
resource "aws_cloudwatch_dashboard" "pipeline" {
  dashboard_name = "${var.project_name}-${var.environment}-pipeline"
  dashboard_body = templatefile("${path.module}/../monitoring/cloudwatch_dashboard.json", {
    region      = local.region
    account_id  = local.account_id
    environment = var.environment
  })
}
