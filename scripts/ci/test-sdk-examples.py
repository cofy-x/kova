#!/usr/bin/env python3
"""Run the public Go and Python SDK examples against a local fake Service."""

from __future__ import annotations

import argparse
import json
import os
import signal
import subprocess
import sys
import tempfile
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

DIGEST_A = "sha256:" + "a" * 64
DIGEST_B = "sha256:" + "b" * 64
DIGEST_C = "sha256:" + "c" * 64
CREATED_AT = "2026-09-20T00:00:00Z"
TOKEN = "sdk-example-token-must-not-leak"
SOURCE_URI = f"oci://registry.example.com/team/sources@{DIGEST_A}"
TARGET = "registry.example.com/team/seed:build-example"


class Scenario:
    def __init__(self, mode: str) -> None:
        self.mode = mode
        self.requests: list[tuple[str, str, dict[str, str], bytes]] = []
        self.posts = 0
        self.gets = 0
        self.polled = threading.Event()
        self.lock = threading.Lock()

    def serve(self, request: BaseHTTPRequestHandler) -> None:
        length = int(request.headers.get("Content-Length", "0"))
        body = request.rfile.read(length)
        path = request.path.split("?", 1)[0]
        with self.lock:
            self.requests.append(
                (request.command, request.path, dict(request.headers.items()), body)
            )

        if path == "/version" and request.command == "GET":
            self.respond(
                request,
                200,
                {
                    "api_version": "v1",
                    "version": "vX.Y.Z",
                    "commit": "abc",
                    "build_date": CREATED_AT,
                },
            )
            return
        if path == "/readyz" and request.command == "GET":
            self.respond(request, 200, {"status": "ready"})
            return
        if path == "/v1/builds" and request.command == "POST":
            with self.lock:
                self.posts += 1
            if self.mode == "api-error":
                self.respond(
                    request,
                    429,
                    {
                        "code": "queue_capacity_exceeded",
                        "message": f"capacity unavailable; secret={TOKEN}",
                        "retryable": True,
                    },
                    {"Retry-After": "1"},
                )
                return
            self.respond(request, 202, job("queued"))
            return
        if path == "/v1/builds/build-example" and request.command == "GET":
            with self.lock:
                self.gets += 1
                gets = self.gets
            self.polled.set()
            if self.mode in {"timeout", "cancel"}:
                self.respond(request, 200, job("running"))
            elif self.mode == "failed":
                self.respond(request, 200, job("failed"))
            elif self.mode == "terminal-cancelled":
                self.respond(request, 200, job("cancelled"))
            elif gets == 1:
                self.respond(request, 200, job("running"))
            else:
                self.respond(request, 200, job("succeeded"))
            return
        if path == "/v1/builds/build-example/results" and request.command == "GET":
            outputs = (
                []
                if self.mode == "terminal-cancelled"
                else [output("oci", DIGEST_B)]
                if self.mode == "failed"
                else [
                    output("nydus", DIGEST_C),
                    output("oci", DIGEST_B),
                ]
            )
            self.respond(
                request,
                200,
                {
                    "id": "build-example",
                    "source_uri": SOURCE_URI,
                    "source_digest": DIGEST_A,
                    "idempotency_key": "example-key",
                    "outputs": outputs,
                },
            )
            return
        self.respond(
            request,
            500,
            {"code": "internal", "message": "unexpected request", "retryable": False},
        )

    @staticmethod
    def respond(
        request: BaseHTTPRequestHandler,
        status: int,
        value: dict[str, Any],
        headers: dict[str, str] | None = None,
    ) -> None:
        body = json.dumps(value).encode()
        request.send_response(status)
        request.send_header("Content-Type", "application/json")
        for name, header_value in (headers or {}).items():
            request.send_header(name, header_value)
        request.send_header("Content-Length", str(len(body)))
        request.end_headers()
        request.wfile.write(body)


def job(status: str) -> dict[str, Any]:
    value: dict[str, Any] = {
        "id": "build-example",
        "status": status,
        "created_at": CREATED_AT,
        "requester": "example:caller",
        "source_uri": SOURCE_URI,
        "source_digest": DIGEST_A,
        "idempotency_key": "example-key",
    }
    if status in {"failed", "cancelled"}:
        value.update(
            {
                "failure_code": "build_failed" if status == "failed" else "cancelled",
                "error": "one output failed" if status == "failed" else "build cancelled",
                "finished_at": CREATED_AT,
            }
        )
    return value


def output(format_name: str, digest: str) -> dict[str, str]:
    suffix = "_nydus_v3" if format_name == "nydus" else ""
    repository = "registry.example.com/team/seed"
    return {
        "format": format_name,
        "image": TARGET + suffix,
        "manifest_digest": digest,
        "immutable_ref": f"{repository}@{digest}",
        "platform": "linux/amd64",
    }


@contextmanager
def fake_service(mode: str) -> Iterator[tuple[Scenario, str]]:
    scenario = Scenario(mode)

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            scenario.serve(self)

        def do_POST(self) -> None:
            scenario.serve(self)

        def log_message(self, _format: str, *_args: object) -> None:
            return

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield scenario, f"http://127.0.0.1:{server.server_port}"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


def command_env(service_url: str, receipt_path: Path) -> dict[str, str]:
    env = os.environ.copy()
    for name in list(env):
        if name.startswith("KOVA_"):
            env.pop(name)
    env.update(
        {
            "KOVA_SERVICE_URL": service_url,
            "KOVA_SERVICE_TOKEN": TOKEN,
            "KOVA_SOURCE_URI": SOURCE_URI,
            "KOVA_SOURCE_DIGEST": DIGEST_A,
            "KOVA_RECIPE_DIGEST": DIGEST_B,
            "KOVA_TARGET": TARGET,
            "KOVA_TARGET_ROLE": "task",
            "KOVA_PLATFORM": "linux/amd64",
            "KOVA_BUILD_FORMAT": "both",
            "KOVA_IDEMPOTENCY_KEY": "example-key",
            "KOVA_RECEIPT_PATH": str(receipt_path),
            "KOVA_WAIT_TIMEOUT_SECONDS": "2",
            "KOVA_POLL_INTERVAL_SECONDS": "0.01",
        }
    )
    return env


def run_example(
    command: list[str], mode: str, temporary: Path, receipt_example: dict[str, Any]
) -> None:
    receipt_path = temporary / f"{mode}.receipt.json"
    with fake_service(mode) as (scenario, service_url):
        env = command_env(service_url, receipt_path)
        if mode == "timeout":
            env["KOVA_WAIT_TIMEOUT_SECONDS"] = "0.05"
        if mode == "cancel":
            process = subprocess.Popen(
                command,
                env=env,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            if not scenario.polled.wait(timeout=5):
                process.kill()
                raise AssertionError("example did not begin polling before cancellation")
            process.send_signal(signal.SIGINT)
            stdout, stderr = process.communicate(timeout=5)
            return_code = process.returncode
        else:
            completed = subprocess.run(
                command,
                env=env,
                capture_output=True,
                text=True,
                timeout=10,
                check=False,
            )
            stdout, stderr, return_code = (
                completed.stdout,
                completed.stderr,
                completed.returncode,
            )

    assert_secret_absent(stdout, stderr, receipt_path)
    if mode == "success":
        assert return_code == 0, failure(command, mode, return_code, stdout, stderr)
        receipt = load_receipt(receipt_path, receipt_example)
        assert receipt["status"] == "succeeded"
        assert [item["format"] for item in receipt["outputs"]] == ["oci", "nydus"]
        assert [item["immutable_ref"] for item in receipt["outputs"]] == [
            f"registry.example.com/team/seed@{DIGEST_B}",
            f"registry.example.com/team/seed@{DIGEST_C}",
        ]
        assert DIGEST_B in stdout and DIGEST_C in stdout
        assert_request_contract(scenario)
    elif mode == "api-error":
        assert return_code == 1, failure(command, mode, return_code, stdout, stderr)
        assert not receipt_path.exists()
        assert "HTTP 429" in stderr
        assert "code=queue_capacity_exceeded" in stderr
        assert "retryable=true" in stderr
        assert "retry_after=1s" in stderr
        assert scenario.posts == 1
    elif mode == "timeout":
        assert return_code == 2, failure(command, mode, return_code, stdout, stderr)
        assert not receipt_path.exists()
        assert "timed out" in stderr
    elif mode == "cancel":
        assert return_code == 2, failure(command, mode, return_code, stdout, stderr)
        assert not receipt_path.exists()
        assert "cancelled" in stderr
    elif mode == "failed":
        assert return_code == 3, failure(command, mode, return_code, stdout, stderr)
        assert stdout == ""
        assert "status failed" in stderr
        receipt = load_receipt(receipt_path, receipt_example)
        assert receipt["status"] == "failed"
        assert receipt["failure_code"] == "build_failed"
        assert len(receipt["outputs"]) == 1
        assert receipt["outputs"][0]["immutable_ref"] == (
            f"registry.example.com/team/seed@{DIGEST_B}"
        )
    elif mode == "terminal-cancelled":
        assert return_code == 3, failure(command, mode, return_code, stdout, stderr)
        assert stdout == ""
        assert "status cancelled" in stderr
        receipt = load_receipt(receipt_path, receipt_example)
        assert receipt["status"] == "cancelled"
        assert receipt["failure_code"] == "cancelled"
        assert receipt["outputs"] == []
    else:  # pragma: no cover - guarded by the caller
        raise AssertionError(f"unknown mode {mode}")


def assert_request_contract(scenario: Scenario) -> None:
    post = next(request for request in scenario.requests if request[0:2] == ("POST", "/v1/builds"))
    assert post[2].get("Authorization") == f"Bearer {TOKEN}"
    payload = json.loads(post[3])
    assert payload["source_uri"] == SOURCE_URI
    assert payload["source_digest"] == DIGEST_A
    assert payload["idempotency_key"] == "example-key"
    assert payload["targets"] == [{"target": TARGET, "platform": "linux/amd64"}]
    assert payload["format"] == "both"
    public = [request for request in scenario.requests if request[1] in {"/version", "/readyz"}]
    assert len(public) == 2
    assert all("Authorization" not in request[2] for request in public)


def assert_secret_absent(stdout: str, stderr: str, receipt_path: Path) -> None:
    combined = stdout + stderr
    if receipt_path.exists():
        combined += receipt_path.read_text()
    assert TOKEN not in combined, "bearer token leaked from an SDK example"


def load_receipt(path: Path, receipt_example: dict[str, Any]) -> dict[str, Any]:
    value = json.loads(path.read_text())
    assert value["schema"] == "kova.seed-build-receipt/v1"
    assert value["source_uri"] == SOURCE_URI
    assert value["source_digest"] == DIGEST_A
    assert value["recipe_digest"] == DIGEST_B
    assert value["idempotency_key"] == "example-key"
    assert value["kova_build_id"] == "build-example"
    assert value["kova_version"] == "vX.Y.Z"
    assert set(value) - {"failure_code"} == set(receipt_example)
    if value["outputs"]:
        assert set(value["outputs"][0]) == set(receipt_example["outputs"][0])
    return value


def failure(command: list[str], mode: str, code: int, stdout: str, stderr: str) -> str:
    return (f"{command[0]} {mode} returned {code}\nstdout:\n{stdout}\nstderr:\n{stderr}").replace(
        TOKEN, "[redacted]"
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--go-example", type=Path, required=True)
    parser.add_argument("--python-example", type=Path, required=True)
    parser.add_argument("--receipt-example", type=Path, required=True)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    receipt_example = json.loads(args.receipt_example.read_text())
    examples = {
        "go": [str(args.go_example)],
        "python": [sys.executable, str(args.python_example)],
    }
    with tempfile.TemporaryDirectory(prefix="kova-sdk-examples-") as directory:
        temporary = Path(directory)
        for language, command in examples.items():
            for mode in (
                "success",
                "api-error",
                "timeout",
                "cancel",
                "failed",
                "terminal-cancelled",
            ):
                run_example(command, mode, temporary / language, receipt_example)
    print("validated Go and Python SDK examples")


if __name__ == "__main__":
    main()
