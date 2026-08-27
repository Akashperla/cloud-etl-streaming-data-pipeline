import json
import os
from urllib.parse import unquote_plus

import boto3


sqs = boto3.client("sqs")
QUEUE_URL = os.environ.get("PROCESSING_QUEUE_URL")


def lambda_handler(event, context):
    if not QUEUE_URL:
        raise RuntimeError("PROCESSING_QUEUE_URL is not configured")

    messages = []

    for record in event.get("Records", []):
        bucket = record["s3"]["bucket"]["name"]
        key = unquote_plus(record["s3"]["object"]["key"])

        payload = {
            "bucket": bucket,
            "key": key,
            "event_name": record.get("eventName", "unknown"),
        }

        response = sqs.send_message(
            QueueUrl=QUEUE_URL,
            MessageBody=json.dumps(payload),
        )

        messages.append({
            "bucket": bucket,
            "key": key,
            "message_id": response.get("MessageId"),
        })

    return {
        "statusCode": 200,
        "processed": len(messages),
        "messages": messages,
    }
