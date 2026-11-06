"""Small Prometheus exposition for real SQS processing, with bounded labels."""

from __future__ import annotations

import re
import threading
import time


class WorkerMetrics:
    """One process/consumer; no order IDs, receipt handles or payload labels."""

    BUCKETS = (0.05, 0.1, 0.5, 1, 2, 5, 10, 30, 60, 120, 180)

    def __init__(self, version: str, channel: str) -> None:
        if not re.fullmatch(r"[a-zA-Z0-9_.:-]{1,80}", version) or channel not in {"stable", "canary"}:
            raise ValueError("Invalid release metric labels")
        self.labels = f'version="{version}",channel="{channel}"'
        self.lock = threading.Lock()
        self.counts = {"success": 0, "error": 0, "ack_error": 0}
        self.retries = self.ddb_errors = self.poll_errors = self.inflight = 0
        self.duration_sum = 0.0
        self.buckets = [0] * len(self.BUCKETS)
        self.draining = False

    def start(self, retry: bool) -> None:
        with self.lock:
            self.inflight += 1
            self.retries += int(retry)

    def finish(self, result: str, seconds: float, *, ddb_error: bool = False) -> None:
        with self.lock:
            self.counts[result] += 1
            self.inflight -= 1
            self.ddb_errors += int(ddb_error)
            self.duration_sum += seconds
            for index, bound in enumerate(self.BUCKETS):
                self.buckets[index] += int(seconds <= bound)

    def render(self) -> bytes:
        with self.lock:
            lines = [f'cosmotech_worker_messages_total{{{self.labels},result="{result}"}} {value}'
                     for result, value in self.counts.items()]
            for name, value in {"retries_total": self.retries, "dynamodb_errors_total": self.ddb_errors,
                                "poll_errors_total": self.poll_errors, "inflight": self.inflight,
                                "draining": int(self.draining), "heartbeat_timestamp_seconds": time.time()}.items():
                lines.append(f"cosmotech_worker_{name}{{{self.labels}}} {value}")
            total = sum(self.counts.values())
            for bound, count in zip(self.BUCKETS, self.buckets, strict=True):
                lines.append(f'cosmotech_worker_processing_seconds_bucket{{{self.labels},le="{bound}"}} {count}')
            lines.extend([
                f'cosmotech_worker_processing_seconds_bucket{{{self.labels},le="+Inf"}} {total}',
                f"cosmotech_worker_processing_seconds_count{{{self.labels}}} {total}",
                f"cosmotech_worker_processing_seconds_sum{{{self.labels}}} {self.duration_sum}",
            ])
            return ("\n".join(lines) + "\n").encode()
