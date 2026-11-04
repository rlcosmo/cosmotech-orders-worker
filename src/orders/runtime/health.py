"""Read-only HTTP health for the SQS polling loop; never accepts orders."""
from __future__ import annotations

import json
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Thread


class WorkerHealth:
    """A stalled polling loop fails probes; queue failures clear readiness."""

    def __init__(self) -> None:
        self.last_poll = time.monotonic()
        self.ready = False
        self.draining = False

    def record_poll(self, succeeded: bool) -> None:
        self.last_poll = time.monotonic()
        self.ready = succeeded

    def status(self, path: str) -> int:
        if path not in {"/health/live", "/health/ready"}:
            return 404
        if self.draining or time.monotonic() - self.last_poll > 90:
            return 503
        return 200 if path == "/health/live" or self.ready else 503


def start_health_server(health: WorkerHealth, port: int = 8000) -> ThreadingHTTPServer:
    """Start a daemon HTTP listener; caller owns server shutdown in tests."""
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            status = health.status(self.path)
            body = json.dumps({"status": "ok" if status == 200 else "unavailable"}).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, format: str, *args: object) -> None:
            pass

    server = ThreadingHTTPServer(("0.0.0.0", port), Handler)
    Thread(target=server.serve_forever, daemon=True, name="worker-health").start()
    return server
