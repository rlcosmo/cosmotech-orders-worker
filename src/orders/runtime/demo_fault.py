"""Opt-in fault restricted to canary consumers and explicitly synthetic order IDs."""
from __future__ import annotations

import json
from pathlib import Path

PREFIX = 'cosmotech-canary-demo-'


def check_synthetic_fault(message: dict, *, channel: str, control_file: str | None) -> None:
    if channel != 'canary' or not control_file:
        return
    try:
        configured = Path(control_file).with_name('failure-prefix').read_text().strip()
    except OSError:
        return
    if configured != PREFIX:
        return
    body = json.loads(message['Body'])
    if str(body.get('order_id', '')).startswith(PREFIX):
        raise ValueError('Controlled synthetic-only canary demonstration failure')
