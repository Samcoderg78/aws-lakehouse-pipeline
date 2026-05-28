##############################################################################
# main.tf — AWS Provider + Terraform Backend Config
# Project: AWS Data Lakehouse Pipeline
# Author : Saurabh Yadav
##############################################################################

terraform {
  required_version = ">= 1.5.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }

  # Remote state in S3 (uncomment after bootstrapping the bucket manually)
  # backend "s3" {
  #   bucket         = "saurabh-tf-state"
  #   key            = "lakehouse-pipeline/terraform.tfstate"
  #   region         = var.aws_region
  #   encrypt        = true
  #   dynamodb_table = "terraform-state-lock"
  # }
}

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      Project     = "aws-lakehouse-pipeline"
      Owner       = "Saurabh Yadav"
      Environment = var.environment
      ManagedBy   = "Terraform"
    }
  }
}

provider "aws" {
  alias  = "us_east_1"
  region = "us-east-1"

  default_tags {
    tags = {
      Project     = "aws-lakehouse-pipeline"
      Owner       = "Saurabh Yadav"
      Environment = var.environment
      ManagedBy   = "Terraform"
    }
  }
}


# ── Data sources ────────────────────────────────────────────────────────────
data "aws_caller_identity" "current" {}
data "aws_region" "current" {}

locals {
  account_id  = data.aws_caller_identity.current.account_id
  region      = data.aws_region.current.name
  bucket_name = "${var.project_name}-${var.environment}-${local.account_id}"
}
