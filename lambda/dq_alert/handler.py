"""
Lambda: dq_alert/handler.py
============================
Invoked by Step Functions when a DQ check fails in the Silver job.
Publishes a formatted alert to SNS.
"""

import json
import os
import boto3

sns_client    = boto3.client("sns")
SNS_TOPIC_ARN = os.environ["SNS_TOPIC_ARN"]
ENVIRONMENT   = os.environ.get("ENVIRONMENT", "dev")


def lambda_handler(event, context):
    """
    Expected event payload:
    {
      "table_name": "retailer",
      "dq_report_path": "s3://bucket/dq-reports/...",
      "failed_rules": [
        {"rule": "NOT_NULL(retailer_id)", "fail_count": 5, "fail_pct": 2.5}
      ],
      "execution_id": "arn:aws:states:..."
    }
    """
    print(f"[dq_alert] Received: {json.dumps(event)}")

    table_name     = event.get("table_name", "unknown")
    failed_rules   = event.get("failed_rules", [])
    report_path    = event.get("dq_report_path", "N/A")
    execution_id   = event.get("execution_id", "N/A")

    rule_lines = "\n".join(
        f"  ❌ {r.get('rule')} | Failures: {r.get('fail_count')} ({r.get('fail_pct')}%)"
        for r in failed_rules
    )

    message = f"""
🚨 DATA QUALITY ALERT — [{ENVIRONMENT.upper()}] AWS Lakehouse Pipeline
══════════════════════════════════════════════════════════

Table          : {table_name}
DQ Report S3   : {report_path}
Step Fn Exec   : {execution_id}

FAILED RULES ({len(failed_rules)}):
{rule_lines}

ACTION REQUIRED:
  1. Check DQ report in S3: {report_path}
  2. Review CloudWatch logs for the Silver Glue job
  3. Fix source data and re-trigger pipeline

──────────────────────────────────────────────────────────
AWS Lakehouse Pipeline | Automated Alert
    """

    response = sns_client.publish(
        TopicArn=SNS_TOPIC_ARN,
        Subject=f"[{ENVIRONMENT.upper()}] 🚨 DQ FAILURE: {table_name}",
        Message=message.strip(),
    )

    print(f"[dq_alert] SNS published: {response['MessageId']}")

    return {
        "statusCode": 200,
        "message_id": response["MessageId"],
        "alert_sent": True,
    }
