"""
scripts/upload_to_s3.py — Upload local CSV data to S3 raw layer
================================================================
Uploads Syngenta hackathon CSVs to the S3 raw prefix,
which triggers the Lambda → Step Functions pipeline.

Usage:
    python scripts/upload_to_s3.py --bucket <bucket-name> --env dev
    python scripts/upload_to_s3.py --dry-run   # preview without uploading

Requirements:
    pip install boto3 tqdm
"""

import argparse
import os
import sys
import logging
from pathlib import Path

import boto3
from botocore.exceptions import ClientError

try:
    from tqdm import tqdm
    HAS_TQDM = True
except ImportError:
    HAS_TQDM = False

logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")
logger = logging.getLogger(__name__)

# ─── Paths to existing Syngenta CSV data ────────────────────────────────────
PROJECT_ROOT = Path(__file__).parent.parent.parent   # Goes up to repo root
DATA_SOURCES = {
    "retailer": [
        PROJECT_ROOT / "Data" / "Internal_Data",
        PROJECT_ROOT / "Data" / "Feature_Engineering",
    ],
    "district": [
        PROJECT_ROOT / "Data" / "External_Data",
    ],
}


def discover_csv_files(base_dirs: list) -> list:
    """Find all CSV files in given directories."""
    files = []
    for base_dir in base_dirs:
        if base_dir.exists():
            files.extend(list(base_dir.glob("**/*.csv")))
    return files


def upload_file(s3_client, local_path: Path, bucket: str, s3_key: str, dry_run: bool = False):
    """Upload a single file to S3."""
    if dry_run:
        logger.info(f"  [DRY RUN] Would upload: {local_path.name} → s3://{bucket}/{s3_key}")
        return True

    try:
        s3_client.upload_file(
            Filename=str(local_path),
            Bucket=bucket,
            Key=s3_key,
            ExtraArgs={"ServerSideEncryption": "AES256"},
        )
        return True
    except ClientError as e:
        logger.error(f"  ❌ Failed to upload {local_path.name}: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(description="Upload raw data to S3 lakehouse")
    parser.add_argument("--bucket",  required=False, help="S3 bucket name (from terraform output)")
    parser.add_argument("--env",     default="dev",  help="Environment (dev|staging|prod)")
    parser.add_argument("--prefix",  default="raw",  help="S3 key prefix (default: raw)")
    parser.add_argument("--dry-run", action="store_true", help="Preview without uploading")
    parser.add_argument("--region",  default="ap-south-1", help="AWS region")
    args = parser.parse_args()

    if not args.dry_run and not args.bucket:
        logger.error("--bucket is required unless --dry-run is set")
        logger.info("Tip: get bucket name from 'terraform output data_lake_bucket'")
        sys.exit(1)

    bucket   = args.bucket or "DRY-RUN-BUCKET"
    s3_client = boto3.client("s3", region_name=args.region) if not args.dry_run else None

    logger.info(f"🚀 Upload mode: {'DRY RUN' if args.dry_run else 'LIVE'}")
    logger.info(f"   Bucket: {bucket}")
    logger.info(f"   Prefix: {args.prefix}/")
    logger.info(f"   Env   : {args.env}")

    total_uploaded = 0
    total_failed   = 0

    for category, base_dirs in DATA_SOURCES.items():
        csv_files = discover_csv_files(base_dirs)

        if not csv_files:
            logger.warning(f"  ⚠️  No CSV files found for category: {category}")
            continue

        logger.info(f"\n📂 Category: {category} ({len(csv_files)} files found)")

        for csv_path in csv_files:
            s3_key = f"{args.prefix}/{category}/{csv_path.name}"

            success = upload_file(s3_client, csv_path, bucket, s3_key, args.dry_run)

            if success:
                total_uploaded += 1
                if not args.dry_run:
                    logger.info(f"  ✅ Uploaded: {csv_path.name}")
            else:
                total_failed += 1

    logger.info(f"\n{'='*50}")
    logger.info(f"📊 Upload Summary:")
    logger.info(f"   Uploaded : {total_uploaded} files")
    logger.info(f"   Failed   : {total_failed} files")

    if not args.dry_run and total_uploaded > 0:
        logger.info(f"\n🔔 Pipeline trigger:")
        logger.info(f"   S3 upload detected → Lambda triggered → Step Functions started")
        logger.info(f"   Monitor at: https://console.aws.amazon.com/states/home")


if __name__ == "__main__":
    main()
