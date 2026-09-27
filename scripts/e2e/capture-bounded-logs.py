#!/usr/bin/env python3
"""Capture a test command's logs with bounded, explicit head/tail evidence.

The complete streams are consumed and hashed, but at most 1 MiB of stdout and
64 KiB of stderr are retained. The receipt records truncation and exit status.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import selectors
import signal
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

STDOUT_LIMIT = 1 << 20
STDERR_LIMIT = 64 << 10
MARKER = b"\n[... Kova test log truncated; see capture receipt ...]\n"


class BoundedStream:
    def __init__(self, limit: int) -> None:
        self.limit = limit
        self.prefix = bytearray()
        self.tail = bytearray()
        self.total = 0
        self.digest = hashlib.sha256()

    def add(self, chunk: bytes) -> None:
        self.total += len(chunk)
        self.digest.update(chunk)
        if len(self.prefix) < self.limit:
            self.prefix.extend(chunk[: self.limit - len(self.prefix)])
        tail_limit = self.limit // 2
        self.tail.extend(chunk)
        if len(self.tail) > tail_limit:
            del self.tail[: len(self.tail) - tail_limit]

    def finish(self, token: bytes) -> tuple[bytes, dict]:
        if self.total <= self.limit:
            retained = bytes(self.prefix)
            head_bytes, tail_bytes = len(retained), 0
        else:
            tail_bytes = (self.limit - len(MARKER)) // 2
            head_bytes = self.limit - len(MARKER) - tail_bytes
            retained = bytes(self.prefix[:head_bytes]) + MARKER + bytes(self.tail[-tail_bytes:])
        redacted = bool(token and token in retained)
        if redacted:
            retained = retained.replace(token, b"*" * len(token))
        return retained, {
            "total_bytes": self.total,
            "retained_bytes": len(retained),
            "omitted_bytes": self.total - head_bytes - tail_bytes,
            "truncated": self.total > self.limit,
            "sha256": self.digest.hexdigest(),
            "test_token_redacted": redacted,
        }


def run_capture(
    command: list[str], stdout_path: Path, stderr_path: Path, receipt_path: Path
) -> int:
    if not command or len({stdout_path, stderr_path, receipt_path}) != 3:
        raise ValueError("capture requires a command and three distinct output paths")
    token = os.environ.get("KOVA_E2E_REDACT_TOKEN", "").encode()
    child_env = dict(os.environ)
    child_env.pop("KOVA_E2E_REDACT_TOKEN", None)
    process = subprocess.Popen(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=child_env,
        start_new_session=True,
    )
    assert process.stdout is not None and process.stderr is not None
    streams = {
        process.stdout: BoundedStream(STDOUT_LIMIT),
        process.stderr: BoundedStream(STDERR_LIMIT),
    }
    interrupted: int | None = None
    signal_deadline = 0.0
    killed = False

    def forward(signum: int, _frame: object) -> None:
        nonlocal interrupted, signal_deadline
        if interrupted is None:
            interrupted = signum
            signal_deadline = time.monotonic() + 5
            try:
                os.killpg(process.pid, signum)
            except ProcessLookupError:
                pass

    previous = {
        signum: signal.signal(signum, forward)
        for signum in (signal.SIGHUP, signal.SIGINT, signal.SIGTERM)
    }
    complete = True
    try:
        with selectors.DefaultSelector() as selector:
            for stream in streams:
                selector.register(stream, selectors.EVENT_READ)
            while selector.get_map():
                if interrupted is not None and time.monotonic() >= signal_deadline:
                    if not killed:
                        try:
                            os.killpg(process.pid, signal.SIGKILL)
                        except ProcessLookupError:
                            pass
                        killed = True
                        signal_deadline = time.monotonic() + 5
                    else:
                        complete = False
                        break
                for key, _ in selector.select(timeout=0.5):
                    chunk = os.read(key.fileobj.fileno(), 64 << 10)
                    if chunk:
                        streams[key.fileobj].add(chunk)
                    else:
                        selector.unregister(key.fileobj)
                        key.fileobj.close()
        try:
            returncode = process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            os.killpg(process.pid, signal.SIGKILL)
            returncode = process.wait(timeout=5)
            complete = False
    except BaseException:
        try:
            os.killpg(process.pid, signal.SIGTERM)
            process.wait(timeout=5)
        except (ProcessLookupError, subprocess.TimeoutExpired):
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            process.wait(timeout=5)
        raise
    finally:
        for signum, handler in previous.items():
            signal.signal(signum, handler)
        for stream in streams:
            stream.close()

    stdout, stdout_fact = streams[process.stdout].finish(token)
    stderr, stderr_fact = streams[process.stderr].finish(token)
    stdout_path.write_bytes(stdout)
    stderr_path.write_bytes(stderr)
    receipt = {
        "at": datetime.now(timezone.utc).isoformat(timespec="seconds"),  # noqa: UP017 (Python 3.10)
        "capture_complete": complete,
        "command_exit_code": returncode,
        "forwarded_signal": interrupted,
        "stdout": stdout_fact,
        "stderr": stderr_fact,
    }
    receipt_path.write_text(json.dumps(receipt, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    if interrupted is not None:
        return 128 + interrupted
    if not complete:
        return 1
    return returncode if returncode >= 0 else 128 - returncode


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--stdout", type=Path, required=True)
    parser.add_argument("--stderr", type=Path, required=True)
    parser.add_argument("--receipt", type=Path, required=True)
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    command = args.command[1:] if args.command[:1] == ["--"] else args.command
    try:
        return run_capture(command, args.stdout, args.stderr, args.receipt)
    except (OSError, ValueError, subprocess.SubprocessError) as error:
        print(f"bounded-log-capture: {type(error).__name__}: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
