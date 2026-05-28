##############################################################################
# outputs.tf — Useful outputs after terraform apply
##############################################################################

output "data_lake_bucket" {
  description = "S3 data lake bucket name"
  value       = aws_s3_bucket.data_lake.bucket
}

output "glue_scripts_bucket" {
  description = "S3 bucket holding Glue scripts"
  value       = aws_s3_bucket.glue_scripts.bucket
}

output "athena_results_bucket" {
  description = "S3 bucket for Athena query results"
  value       = aws_s3_bucket.athena_results.bucket
}

output "step_functions_arn" {
  description = "Step Functions state machine ARN"
  value       = aws_sfn_state_machine.pipeline.arn
}

output "sns_topic_arn" {
  description = "SNS topic ARN for pipeline alerts"
  value       = aws_sns_topic.pipeline_alerts.arn
}



output "glue_catalog_database" {
  description = "Glue Data Catalog database name"
  value       = aws_glue_catalog_database.lakehouse.name
}

output "athena_workgroup" {
  description = "Athena workgroup name"
  value       = aws_athena_workgroup.lakehouse.name
}

output "pipeline_trigger_lambda" {
  description = "Pipeline trigger Lambda function name"
  value       = aws_lambda_function.pipeline_trigger.function_name
}

output "glue_job_names" {
  description = "All Glue job names"
  value = {
    bronze_ingest       = aws_glue_job.bronze_ingest.name
    silver_clean        = aws_glue_job.silver_clean.name
    gold_retailer_kpi   = aws_glue_job.gold_retailer_kpi.name
    gold_district_risk  = aws_glue_job.gold_district_risk.name
    gold_demand_forecast = aws_glue_job.gold_demand_forecast.name
  }
}
