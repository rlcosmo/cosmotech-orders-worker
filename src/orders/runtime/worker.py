"""Bounded SQS polling worker; only acknowledge completed processing."""

from __future__ import annotations

import json
import os
import signal
import time
from typing import Any

import boto3
from botocore.config import Config

from orders.consumer.processing import DynamoDBOrderProcessor
from orders.consumer.repository import create_dynamodb_resource
from orders.runtime.connectivity import start_connectivity_checks
from orders.runtime.controlled_consumer import mode_from_file, poll_once
from orders.runtime.demo_fault import check_synthetic_fault
from orders.runtime.health import WorkerHealth, start_health_server
from orders.runtime.worker_metrics import WorkerMetrics
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
    metrics = WorkerMetrics(os.getenv("RELEASE_VERSION", "unversioned"), os.getenv("RELEASE_CHANNEL", "stable"))
    start_health_server(health, metrics=metrics)
    control_file = os.getenv("WORKER_CONTROL_FILE")
    require_lease = os.getenv("RELEASE_CHANNEL", "stable") == "canary"
    connectivity_stop = start_connectivity_checks("orders-worker")
    stopping = False

    def stop(signum, frame):
        nonlocal stopping
        stopping = True
        health.draining = True
        if connectivity_stop is not None:
            connectivity_stop.set()

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    def controlled_process(message):
        check_synthetic_fault(message, channel=os.getenv("RELEASE_CHANNEL", "stable"), control_file=control_file)
        process_message(message, processor)

    while not stopping:
        result = poll_once(sqs, queue_url, controlled_process,
                           lambda: stopping or mode_from_file(control_file, require_lease=require_lease) != "run",
                           health, metrics)
        if result in {"drained", "poll_error"}:
            time.sleep(1 if result == "drained" else 5)


if __name__ == "__main__":
    main()
