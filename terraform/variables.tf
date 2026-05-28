##############################################################################
# variables.tf — All configurable inputs
##############################################################################

variable "aws_region" {
  description = "AWS region to deploy resources"
  type        = string
  default     = "ap-south-1"   # Mumbai — closest to Bengaluru
}

variable "environment" {
  description = "Deployment environment (dev | staging | prod)"
  type        = string
  default     = "dev"

  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "environment must be one of: dev, staging, prod"
  }
}

variable "project_name" {
  description = "Prefix for all AWS resource names"
  type        = string
  default     = "lakehouse-pipeline"
}

variable "glue_worker_type" {
  description = "AWS Glue worker type (G.1X | G.2X | G.025X)"
  type        = string
  default     = "G.1X"
}

variable "glue_num_workers" {
  description = "Number of Glue DPU workers"
  type        = number
  default     = 2
}

variable "glue_timeout_minutes" {
  description = "Glue job timeout in minutes"
  type        = number
  default     = 60
}

variable "redshift_db_name" {
  description = "Redshift Serverless database name"
  type        = string
  default     = "lakehouse_db"
}

variable "redshift_admin_username" {
  description = "Redshift admin username"
  type        = string
  default     = "admin"
  sensitive   = true
}

variable "redshift_admin_password" {
  description = "Redshift admin password (use AWS Secrets Manager in prod)"
  type        = string
  sensitive   = true
  default     = ""   # Set via TF_VAR_redshift_admin_password env var
}

variable "alert_email" {
  description = "Email address for SNS pipeline alerts"
  type        = string
  default     = "23f1003171@ds.study.iitm.ac.in"
}

variable "kinesis_buffer_interval_seconds" {
  description = "Kinesis Firehose buffer interval (60-900 seconds)"
  type        = number
  default     = 300
}

variable "pipeline_schedule" {
  description = "EventBridge cron expression for daily pipeline run"
  type        = string
  default     = "cron(0 2 * * ? *)"   # 2 AM UTC = 7:30 AM IST
}
