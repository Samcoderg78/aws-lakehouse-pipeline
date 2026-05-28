"""
bronze_ingest.py — AWS Glue PySpark Job
========================================
Layer   : RAW → BRONZE
Purpose : Read raw CSV files from S3 raw prefix, add metadata, 
          and write to S3 Bronze prefix in Parquet format.
"""

import sys
import logging
import boto3
from datetime import datetime, timezone

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "RAW_BUCKET",
    "RAW_PREFIX",
    "BRONZE_PREFIX",
    "DATABASE_NAME",
    "TABLE_NAME",
])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

def run():
    bucket        = args["RAW_BUCKET"]
    raw_prefix    = args["RAW_PREFIX"]
    bronze_prefix = args["BRONZE_PREFIX"]

    s3 = boto3.client('s3')
    logger.info(f"Listing files in s3://{bucket}/{raw_prefix}")
    
    paginator = s3.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket, Prefix=raw_prefix)
    
    for page in pages:
        for obj in page.get('Contents', []):
            key = obj['Key']
            if not key.endswith('.csv'):
                continue
                
            # Extract just the filename without extension (e.g. 'retailers')
            file_name = key.split('/')[-1].replace('.csv', '')
            
            s3_raw_path = f"s3://{bucket}/{key}"
            # Write to its own folder inside bronze prefix to avoid schema clashes
            s3_bronze_path = f"s3://{bucket}/{bronze_prefix}{file_name}/"

            logger.info(f"Reading raw data from: {s3_raw_path}")
            
            # 1. Simple read with inferSchema
            df = spark.read.option("header", "true").option("inferSchema", "true").csv(s3_raw_path)
            
            # 2. Add metadata
            now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
            today   = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            
            df = df.withColumn("ingestion_timestamp", F.lit(now_utc)) \
                   .withColumn("ingestion_date", F.lit(today)) \
                   .withColumn("source_path", F.lit(key))

            logger.info(f"Writing Bronze layer to: {s3_bronze_path}")
            
            # 3. Write to Bronze
            df.write.mode("overwrite").partitionBy("ingestion_date").parquet(s3_bronze_path)
            
    logger.info("✅ Bronze write complete for all files.")

run()
job.commit()
