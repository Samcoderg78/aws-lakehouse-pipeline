"""
Lambda: pipeline_trigger/handler.py
=====================================
Triggered by S3 PutObject event on raw/ prefix.
Starts the Step Functions state machine execution.
"""

import json
import os
import boto3
from datetime import datetime, timezone

sfn_client = boto3.client("stepfunctions")
STATE_MACHINE_ARN = os.environ["STATE_MACHINE_ARN"]
ENVIRONMENT       = os.environ.get("ENVIRONMENT", "dev")


def lambda_handler(event, context):
    """
    Entry point. Parse S3 event, extract bucket/key info,
    and start Step Functions execution.
    """
    print(f"[pipeline_trigger] Received event: {json.dumps(event)}")

    records = event.get("Records", [])
    if not records:
        return {"statusCode": 200, "body": "No records to process"}

    triggered_files = []

    for record in records:
        bucket = record["s3"]["bucket"]["name"]
        key    = record["s3"]["object"]["key"]
        size   = record["s3"]["object"].get("size", 0)

        print(f"  [+] New file: s3://{bucket}/{key} ({size} bytes)")
        triggered_files.append({"bucket": bucket, "key": key, "size": size})

    # Build execution name (must be unique, alphanumeric + hyphens)
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    execution_name = f"{ENVIRONMENT}-pipeline-{ts}"

    input_payload = {
        "trigger_source": "s3_event",
        "environment": ENVIRONMENT,
        "triggered_files": triggered_files,
        "execution_time": datetime.now(timezone.utc).isoformat(),
    }

    response = sfn_client.start_execution(
        stateMachineArn=STATE_MACHINE_ARN,
        name=execution_name,
        input=json.dumps(input_payload),
    )

    print(f"[pipeline_trigger] Started execution: {response['executionArn']}")

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": "Pipeline execution started",
            "executionArn": response["executionArn"],
            "execution_name": execution_name,
        }),
    }
