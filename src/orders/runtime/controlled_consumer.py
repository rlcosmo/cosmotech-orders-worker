"""Drain-aware SQS consumer; same queue, real competing consumers, no HTTP weights."""

from __future__ import annotations

import json
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

from botocore.exceptions import BotoCoreError, ClientError

from orders.consumer.errors import PermanentProcessingError, TransientProcessingError
from orders.runtime.health import WorkerHealth
from orders.runtime.worker_metrics import WorkerMetrics
from orders.shared.logging import get_logger

logger = get_logger(__name__)
PROCESSING_ERRORS = (KeyError, ValueError, TypeError, PermanentProcessingError,
                     TransientProcessingError, BotoCoreError, ClientError)


def mode_from_file(path: str | None, *, require_lease: bool = False) -> str:
    """Missing/unreadable configured control is drain, never implicit permission."""
    if path is None:
        return "drain" if require_lease else "run"
    try:
        value = Path(path).read_text().strip()
    except OSError:
        return "drain"
    if not require_lease:
        return "run" if value == "run" else "drain"
    try:
        lease = json.loads(value)
        expires = lease["expires_at"]
        valid = (type(expires) in {float, int} and time.time() < expires <= time.time() + 300
                 and lease["mode"] == "run")
        return "run" if valid else "drain"
    except (ValueError, TypeError, KeyError):
        return "drain"


def poll_once(sqs: Any, queue_url: str, process: Callable[[dict], None],
              should_drain: Callable[[], bool], health: WorkerHealth, metrics: WorkerMetrics) -> str:
    """Finish a started item; a receive completing after drain is released untouched."""
    health.draining = metrics.draining = should_drain()
    if health.draining:
        return "drained"
    try:
        response = sqs.receive_message(
            QueueUrl=queue_url, MaxNumberOfMessages=1, WaitTimeSeconds=20,
            VisibilityTimeout=180, MessageAttributeNames=["All"],
            MessageSystemAttributeNames=["ApproximateReceiveCount"],
        )
        health.record_poll(True)
    except (BotoCoreError, ClientError):
        health.record_poll(False)
        metrics.poll_errors += 1
        return "poll_error"
    for message in response.get("Messages", []):
        if should_drain():
            health.draining = metrics.draining = True
            # This message has not entered business processing. Never acknowledge it.
            sqs.change_message_visibility(QueueUrl=queue_url, ReceiptHandle=message["ReceiptHandle"],
                                          VisibilityTimeout=0)
            return "drained"
        started = time.monotonic()
        retry = int(message.get("Attributes", {}).get("ApproximateReceiveCount", "1")) > 1
        metrics.start(retry)
        health.processing = True
        result, ddb_error = "success", False
        try:
            process(message)
        except PROCESSING_ERRORS as error:
            result = "error"
            # This boundary handles process(), not SQS polling/ack; AWS errors here are persistence errors.
            ddb_error = isinstance(error, (ClientError, BotoCoreError))
            logger.error("order_processing_failed", extra={"event": "order_processing_failed",
                         "error_type": type(error).__name__})
        except Exception:
            result = "error"
            raise
        else:
            try:
                sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=message["ReceiptHandle"])
            except (BotoCoreError, ClientError):
                # Persisted effects must be idempotent when this message is redelivered.
                result = "ack_error"
        finally:
            metrics.finish(result, time.monotonic() - started, ddb_error=ddb_error)
            health.processing = False
    health.draining = metrics.draining = should_drain()
    return "drained" if health.draining else "polled"
