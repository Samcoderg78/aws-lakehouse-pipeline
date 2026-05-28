##############################################################################
# step_functions.tf — Step Functions State Machine + EventBridge Schedule
##############################################################################

# ── Step Functions State Machine ─────────────────────────────────────────────
resource "aws_sfn_state_machine" "pipeline" {
  name     = "${var.project_name}-${var.environment}-pipeline"
  role_arn = aws_iam_role.step_functions_role.arn
  type     = "STANDARD"

  definition = templatefile("${path.module}/../step_functions/pipeline.json", {
    bronze_job_name         = aws_glue_job.bronze_ingest.name
    silver_job_name         = aws_glue_job.silver_clean.name
    gold_retailer_job_name  = aws_glue_job.gold_retailer_kpi.name
    gold_district_job_name  = aws_glue_job.gold_district_risk.name
    gold_forecast_job_name  = aws_glue_job.gold_demand_forecast.name
    sns_topic_arn           = aws_sns_topic.pipeline_alerts.arn
    region                  = local.region
  })

  logging_configuration {
    log_destination        = "${aws_cloudwatch_log_group.sfn_logs.arn}:*"
    include_execution_data = true
    level                  = "ERROR"
  }

  tracing_configuration {
    enabled = false
  }

  tags = { Component = "orchestration" }
}

resource "aws_cloudwatch_log_group" "sfn_logs" {
  name              = "/aws/states/${var.project_name}-${var.environment}"
  retention_in_days = 14
}

# ── EventBridge Scheduler (daily trigger) ────────────────────────────────────
resource "aws_scheduler_schedule" "daily_pipeline" {
  name       = "${var.project_name}-${var.environment}-daily"
  group_name = "default"

  flexible_time_window { mode = "OFF" }

  schedule_expression          = var.pipeline_schedule
  schedule_expression_timezone = "UTC"

  target {
    arn      = aws_sfn_state_machine.pipeline.arn
    role_arn = aws_iam_role.eventbridge_role.arn

    input = jsonencode({
      trigger_source = "eventbridge_scheduled"
      environment    = var.environment
    })

    retry_policy {
      maximum_retry_attempts = 2
    }
  }
}
