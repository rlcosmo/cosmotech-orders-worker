"""Bounded SQS polling worker; only acknowledge completed processing."""

from __future__ import annotations

import json
import os
import signal
import time
from typing import Any

import boto3
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError

from orders.consumer.errors import PermanentProcessingError, TransientProcessingError
from orders.consumer.processing import DynamoDBOrderProcessor
from orders.consumer.repository import create_dynamodb_resource
from orders.runtime.health import WorkerHealth, start_health_server
from orders.shared.logging import get_logger
from orders.shared.tracing import configure_tracing, span
from orders.shared.validation import validate_order, validate_order_event

logger = get_logger(__name__)


def process_message(message: dict[str, Any], processor: DynamoDBOrderProcessor) -> None:
    """Accept v2 events and normalize the existing EKS queue contract."""
    payload = json.loads(message["Body"])
    if isinstance(payload, dict) and "version" not in payload and "customer_id" in payload:
        items = [{"sku": item.get("product_id"), "quantity": item.get("quantity")}
                 for item in payload.get("items", [])]
        order = validate_order({"order_id": payload.get("order_id"), "items": items})
        created_at = payload.get("created_at")
        normalized = validate_order_event({"event_type": "order.created", "version": 2,
            "order_id": order["order_id"], "order": order, "created_at": created_at})
    else:
        normalized = validate_order_event(payload)
    attributes = message.get("MessageAttributes", {})
    if normalized["order_id"].startswith(("INVENTORY#", "IDEMPOTENCY#")):
        raise ValueError("Order ID uses a reserved namespace")
    carrier = {key: value["StringValue"] for key, value in attributes.items()
               if key in {"traceparent", "tracestate"} and "StringValue" in value}
    with span("orders.consume", carrier):
        processor.persist(normalized["order"], normalized["created_at"])


def main() -> None:
    """Stop polling on SIGTERM, finishing at most the current batch."""
    configure_tracing("orders-worker")
    queue_url = os.getenv("ORDERS_QUEUE_URL") or os.environ["SQS_QUEUE_URL"]
    table_name = os.getenv("ORDERS_TABLE_NAME") or os.environ["DYNAMODB_TABLE"]
    region = os.getenv("AWS_REGION", os.getenv("AWS_DEFAULT_REGION", "us-east-1"))
    sqs = boto3.client("sqs", region_name=region, config=Config(
        connect_timeout=2, read_timeout=25, retries={"total_max_attempts": 3, "mode": "standard"},
    ))
    processor = DynamoDBOrderProcessor(table_name, dynamodb_resource=create_dynamodb_resource(
        region_name=region), key_schema=os.getenv("ORDERS_KEY_SCHEMA", "legacy"))
    health = WorkerHealth()
    start_health_server(health)
    stopping = False

    def stop(signum, frame):
        nonlocal stopping
        stopping = True
        health.draining = True

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    while not stopping:
        try:
            batch = sqs.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=1,
                WaitTimeSeconds=20, VisibilityTimeout=180, MessageAttributeNames=["All"])
            health.record_poll(True)
            for message in batch.get("Messages", []):
                try:
                    process_message(message, processor)
                    sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=message["ReceiptHandle"])
                except (KeyError, ValueError, TypeError, PermanentProcessingError,
                        TransientProcessingError, BotoCoreError, ClientError) as error:
                    logger.error("order_processing_failed", extra={"event": "order_processing_failed",
                                 "error_type": type(error).__name__})
        except (BotoCoreError, ClientError) as error:
            health.record_poll(False)
            logger.error("queue_poll_failed", extra={"event": "queue_poll_failed",
                         "error_type": type(error).__name__})
            time.sleep(5)


if __name__ == "__main__":
    main()
