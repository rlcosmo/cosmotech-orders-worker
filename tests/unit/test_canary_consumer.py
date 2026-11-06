import json
import time
from unittest.mock import Mock

import boto3
import pytest
from botocore.exceptions import ClientError
from moto import mock_aws

from orders.consumer.processing import DynamoDBOrderProcessor
from orders.runtime.controlled_consumer import mode_from_file, poll_once
from orders.runtime.health import WorkerHealth
from orders.runtime.worker import process_message
from orders.runtime.worker_metrics import WorkerMetrics


def metrics(channel="canary"):
    return WorkerMetrics("test-v2", channel)


def test_drain_does_not_receive():
    sqs = Mock()
    m, health = metrics(), WorkerHealth()
    assert poll_once(sqs, "queue", Mock(), lambda: True, health, m) == "drained"
    sqs.receive_message.assert_not_called()
    assert health.status("/health/live") == 200
    assert health.status("/health/ready") == 503


def test_drain_during_long_poll_returns_unprocessed_message():
    sqs, process = Mock(), Mock()
    sqs.receive_message.return_value = {"Messages": [{"ReceiptHandle": "test-handle"}]}
    calls = iter([False, True])
    poll_once(sqs, "queue", process, lambda: next(calls), WorkerHealth(), metrics())
    process.assert_not_called()
    sqs.delete_message.assert_not_called()
    sqs.change_message_visibility.assert_called_once_with(
        QueueUrl="queue", ReceiptHandle="test-handle", VisibilityTimeout=0)


def test_started_item_completes_before_drain():
    state = {"drain": False}
    sqs = Mock()
    sqs.receive_message.return_value = {"Messages": [{"ReceiptHandle": "h"}]}
    def process(_):
        state["drain"] = True
    m = metrics()
    assert poll_once(sqs, "q", process, lambda: state["drain"], WorkerHealth(), m) == "drained"
    sqs.delete_message.assert_called_once()
    assert m.inflight == 0 and m.counts["success"] == 1


def test_liveness_does_not_kill_a_busy_message():
    health = WorkerHealth()
    health.last_poll = time.monotonic() - 120
    health.processing = True
    assert health.status("/health/live") == 200
    health.draining = True
    assert health.status("/health/ready") == 503


def test_processing_failure_never_acknowledged_and_retry_measured():
    sqs, process = Mock(), Mock(side_effect=ValueError("invalid"))
    sqs.receive_message.return_value = {"Messages": [{"ReceiptHandle": "h",
        "Attributes": {"ApproximateReceiveCount": "2"}}]}
    m = metrics()
    poll_once(sqs, "q", process, lambda: False, WorkerHealth(), m)
    sqs.delete_message.assert_not_called()
    assert m.counts["error"] == 1 and m.retries == 1


def test_ack_error_is_distinct_from_processing_error():
    sqs = Mock()
    sqs.receive_message.return_value = {"Messages": [{"ReceiptHandle": "h"}]}
    sqs.delete_message.side_effect = ClientError({"Error": {"Code": "RequestTimeout"}}, "DeleteMessage")
    m = metrics()
    poll_once(sqs, "q", Mock(), lambda: False, WorkerHealth(), m)
    assert m.counts["ack_error"] == 1 and m.ddb_errors == 0


def test_ddb_error_and_poll_failure_distinct():
    sqs = Mock()
    sqs.receive_message.return_value = {"Messages": [{"ReceiptHandle": "h"}]}
    m = metrics()
    error = ClientError({"Error": {"Code": "ProvisionedThroughputExceededException"}}, "PutItem")
    poll_once(sqs, "q", Mock(side_effect=error), lambda: False, WorkerHealth(), m)
    assert m.ddb_errors == 1
    sqs.receive_message.side_effect = ClientError({"Error": {"Code": "AccessDenied"}}, "ReceiveMessage")
    assert poll_once(sqs, "q", Mock(), lambda: False, WorkerHealth(), m) == "poll_error"
    assert m.poll_errors == 1


def test_control_file_missing_or_invalid_fails_closed(tmp_path):
    path = tmp_path / "mode"
    assert mode_from_file(str(path)) == "drain"
    path.write_text("unexpected")
    assert mode_from_file(str(path)) == "drain"
    path.write_text("run\n")
    assert mode_from_file(str(path)) == "run"


def test_canary_lease_expiry_or_controller_failure_stops_polling(tmp_path):
    path = tmp_path / "lease"
    assert mode_from_file(None, require_lease=True) == "drain"
    path.write_text("run")
    assert mode_from_file(str(path), require_lease=True) == "drain"
    path.write_text(json.dumps({"mode": "run", "expires_at": time.time() + 180}))
    assert mode_from_file(str(path), require_lease=True) == "run"
    path.write_text(json.dumps({"mode": "run", "expires_at": time.time() - 1}))
    assert mode_from_file(str(path), require_lease=True) == "drain"


def test_metric_labels_bounded_and_histogram_counts_real_outcomes():
    with pytest.raises(ValueError):
        WorkerMetrics('contains"injection', "canary")
    m = metrics()
    m.start(True)
    m.finish("success", 0.2)
    text = m.render().decode()
    assert 'channel="canary"' in text
    assert 'le="0.5"} 1' in text
    assert 'le="0.05"} 0' in text
    assert 'result="success"} 1' in text
    assert 'cosmotech_worker_inflight' in text


@mock_aws
def test_stable_and_canary_duplicate_delivery_does_not_repeat_inventory_effects():
    resource = boto3.resource("dynamodb", region_name="us-east-1")
    table = resource.create_table(TableName="canary-isolated",
        KeySchema=[{"AttributeName": "order_id", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "order_id", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST")
    table.put_item(Item={"order_id": "INVENTORY#LAB", "stock": {"sku-test": 5}})
    stable = DynamoDBOrderProcessor(table.name, dynamodb_resource=resource, key_schema="legacy")
    canary = DynamoDBOrderProcessor(table.name, dynamodb_resource=resource, key_schema="legacy")
    message = {"Body": json.dumps({"order_id": "synthetic-isolated", "customer_id": "test",
        "items": [{"product_id": "sku-test", "quantity": 2}], "created_at": "2026-11-05T00:00:00Z"})}
    process_message(message, stable)
    process_message(message, canary)
    assert table.get_item(Key={"order_id": "INVENTORY#LAB"})["Item"]["stock"]["sku-test"] == 3


def test_visibility_release_denied_stays_drained_without_ack_or_process():
    sqs, process = Mock(), Mock()
    sqs.receive_message.return_value={"Messages":[{"ReceiptHandle":"test-handle"}]}
    sqs.change_message_visibility.side_effect=ClientError({"Error":{"Code":"AccessDenied"}},"ChangeMessageVisibility")
    calls=iter([False,True]);m,health=metrics(),WorkerHealth()
    assert poll_once(sqs,"queue",process,lambda:next(calls),health,m)=="drained"
    process.assert_not_called();sqs.delete_message.assert_not_called()
    assert m.visibility_errors==1 and m.inflight==0
    assert health.draining and health.status("/health/live")==200
    assert 'cosmotech_worker_visibility_errors_total' in m.render().decode()
