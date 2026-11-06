import json

import pytest

from orders.runtime.demo_fault import PREFIX, check_synthetic_fault


def test_fault_never_applies_to_stable_or_business_message(tmp_path):
    control = tmp_path / 'lease.json'
    control.with_name('failure-prefix').write_text(PREFIX)
    check_synthetic_fault({'Body': json.dumps({'order_id': PREFIX + 'test'})}, channel='stable', control_file=str(control))
    check_synthetic_fault({'Body': json.dumps({'order_id': 'real-order'})}, channel='canary', control_file=str(control))
    with pytest.raises(ValueError, match='synthetic-only'):
        check_synthetic_fault({'Body': json.dumps({'order_id': PREFIX + 'test'})}, channel='canary', control_file=str(control))


def test_missing_or_invalid_flag_never_injects_failure(tmp_path):
    path = tmp_path / 'lease.json'
    message = {'Body': json.dumps({'order_id': PREFIX + 'test'})}
    check_synthetic_fault(message, channel='canary', control_file=str(path))
    path.with_name('failure-prefix').write_text('real-')
    check_synthetic_fault(message, channel='canary', control_file=str(path))
