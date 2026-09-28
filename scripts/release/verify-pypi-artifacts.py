"""Fail closed unless public PyPI files match this run's wheel and sdist hashes."""

from __future__ import annotations

import argparse
import email.parser
import errno
import hashlib
import http.client
import io
import json
import re
import socket
import ssl
import subprocess
import sys
import time
import urllib.error
import urllib.request
import zipfile
from pathlib import Path

METADATA_LIMIT = 2 * 1024 * 1024
VISIBILITY_DEADLINE_SECONDS = 300
PREFLIGHT_DEADLINE_SECONDS = 60
REQUEST_TIMEOUT_SECONDS = 10


class FetchFailure(Exception):
    def __init__(self, code: str, *, retryable: bool = False):
        super().__init__(code)
        self.code = code
        self.retryable = retryable


def transient_network_error(reason: object) -> bool:
    if isinstance(reason, ssl.SSLError):
        return False
    if isinstance(reason, socket.gaierror):
        return reason.errno == socket.EAI_AGAIN
    if isinstance(reason, (TimeoutError, ConnectionError, http.client.IncompleteRead)):
        return True
    return isinstance(reason, OSError) and reason.errno in {
        errno.ETIMEDOUT, errno.ECONNRESET, errno.ECONNABORTED, errno.ECONNREFUSED,
        errno.ENETUNREACH, errno.EHOSTUNREACH, errno.ENETDOWN, errno.EPIPE,
    }


def fetch_worker(version: str) -> dict:
    """One anonymous read; never print remote bodies or exception messages on error."""
    url = f"https://pypi.org/pypi/kova-client/{version}/json"
    try:
        with urllib.request.urlopen(url, timeout=REQUEST_TIMEOUT_SECONDS) as response:
            raw = response.read(METADATA_LIMIT + 1)
        if len(raw) > METADATA_LIMIT:
            return {"error": "metadata_size_guard", "retryable": False}
        return {"payload": json.loads(raw)}
    except urllib.error.HTTPError as exc:
        return {"error": f"http_{exc.code}", "retryable": exc.code == 404}
    except urllib.error.URLError as exc:
        return {"error": "network_error", "retryable": transient_network_error(exc.reason)}
    except (OSError, http.client.HTTPException) as exc:
        return {"error": "network_error", "retryable": transient_network_error(exc)}
    except (ValueError, UnicodeError):
        return {"error": "invalid_metadata", "retryable": False}


def fetch_public(version: str, timeout: float) -> dict:
    # Socket timeouts alone do not bound DNS or a trickling response body.
    # subprocess.run kills and reaps only this owned, childless read worker.
    try:
        result = subprocess.run(
            [sys.executable, str(Path(__file__).resolve()), "--fetch", version],
            capture_output=True, timeout=timeout, check=False,
        )
    except subprocess.TimeoutExpired:
        raise FetchFailure("request_timeout", retryable=True) from None
    if result.returncode != 0:
        raise FetchFailure("request_worker_failed")
    try:
        envelope = json.loads(result.stdout)
        if isinstance(envelope, dict) and set(envelope) == {"payload"}:
            return envelope["payload"]
        if (
            isinstance(envelope, dict) and set(envelope) == {"error", "retryable"}
            and isinstance(envelope["error"], str)
            and type(envelope["retryable"]) is bool
        ):
            raise FetchFailure(envelope["error"], retryable=envelope["retryable"])
    except (ValueError, UnicodeError):
        pass
    raise FetchFailure("request_worker_invalid_result")


def local_artifacts(directory: Path) -> tuple[str, dict[str, str]]:
    wheels = list(directory.glob("kova_client-*.whl"))
    sdists = list(directory.glob("kova_client-*.tar.gz"))
    if len(wheels) != 1 or len(sdists) != 1:
        raise ValueError("expected one wheel and one sdist")
    wheel_bytes = wheels[0].read_bytes()
    with zipfile.ZipFile(io.BytesIO(wheel_bytes)) as archive:
        metadata_paths = [
            name for name in archive.namelist() if name.endswith(".dist-info/METADATA")
        ]
        if len(metadata_paths) != 1:
            raise ValueError("expected one wheel METADATA")
        metadata = email.parser.BytesParser().parsebytes(archive.read(metadata_paths[0]))
    version = metadata.get("Version", "")
    if metadata.get("Name") != "kova-client" or not re.fullmatch(r"[0-9A-Za-z.+!-]+", version):
        raise ValueError("invalid package identity")
    return version, {
        wheels[0].name: hashlib.sha256(wheel_bytes).hexdigest(),
        sdists[0].name: hashlib.sha256(sdists[0].read_bytes()).hexdigest(),
    }


def verify_public(
    payload: dict, version: str, expected: dict[str, str], *, allow_subset: bool = False
) -> None:
    if not isinstance(payload, dict) or not isinstance(payload.get("info"), dict):
        raise ValueError("invalid public package metadata")
    if payload.get("info", {}).get("version") != version:
        raise ValueError("public package version differs")
    rows = payload.get("urls", [])
    if not isinstance(rows, list) or any(
        not isinstance(row, dict) or not isinstance(row.get("digests"), dict)
        or not isinstance(row.get("filename"), str)
        or not isinstance(row["digests"].get("sha256"), str)
        or type(row.get("yanked")) is not bool
        for row in rows
    ):
        raise ValueError("invalid public package files")
    actual = {row.get("filename"): row.get("digests", {}).get("sha256") for row in rows}
    matches = actual == expected
    if allow_subset:
        matches = all(
            name in expected and digest == expected[name] for name, digest in actual.items()
        )
    if len(rows) != len(actual) or not matches or any(row.get("yanked") for row in rows):
        raise ValueError(
            "public package files, hashes, or yank state differ from validated artifacts"
        )


def wait_for_public(
    version: str, expected: dict[str, str], *, preflight: bool = False,
    deadline_seconds: float = VISIBILITY_DEADLINE_SECONDS,
) -> None:
    started = time.monotonic()
    deadline = started + deadline_seconds
    attempt = 0
    backoff = 5.0
    last_status = "not_requested"
    while time.monotonic() < deadline:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break
        attempt += 1
        try:
            payload = fetch_public(version, min(REQUEST_TIMEOUT_SECONDS, remaining))
            if time.monotonic() >= deadline:
                last_status = "response_after_deadline"
                break
            verify_public(payload, version, expected, allow_subset=preflight)
            if time.monotonic() >= deadline:
                last_status = "verification_after_deadline"
                break
            print(f"PyPI {version} attempt={attempt} status=verified elapsed={time.monotonic() - started:.1f}s", file=sys.stderr)
            return
        except FetchFailure as exc:
            last_status = exc.code
            elapsed = time.monotonic() - started
            if exc.code == "http_404" and preflight and time.monotonic() < deadline:
                print(f"PyPI {version} attempt={attempt} status=absent_preflight elapsed={elapsed:.1f}s", file=sys.stderr)
                return
            if not exc.retryable:
                print(f"PyPI {version} attempt={attempt} status={exc.code} elapsed={elapsed:.1f}s retry=false", file=sys.stderr)
                raise
        except ValueError:
            print(f"PyPI {version} attempt={attempt} status=artifact_mismatch elapsed={time.monotonic() - started:.1f}s retry=false", file=sys.stderr)
            raise
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break
        delay = min(backoff, remaining)
        print(f"PyPI {version} attempt={attempt} status={last_status} elapsed={time.monotonic() - started:.1f}s retry_in={delay:.1f}s remaining={remaining:.1f}s", file=sys.stderr)
        time.sleep(delay)
        if not preflight:
            backoff = min(backoff * 2, 30.0)
    raise FetchFailure(f"visibility_deadline_expired attempts={attempt} last_status={last_status}")


def deadline_argument(value: str) -> int:
    if not re.fullmatch(r"[0-9]+", value) or not 1 <= int(value) <= 600:
        raise argparse.ArgumentTypeError("visibility deadline must be 1..600 seconds")
    return int(value)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path)
    parser.add_argument("--preflight", action="store_true")
    parser.add_argument("--visibility-deadline-seconds", type=deadline_argument)
    args = parser.parse_args(argv)
    if args.preflight and args.visibility_deadline_seconds is not None:
        parser.error("--visibility-deadline-seconds is post-publication only")
    version, expected = local_artifacts(args.directory)
    wait_for_public(
        version, expected, preflight=args.preflight,
        deadline_seconds=(PREFLIGHT_DEADLINE_SECONDS if args.preflight else
                          args.visibility_deadline_seconds or VISIBILITY_DEADLINE_SECONDS),
    )
    print(version)


if __name__ == "__main__":
    if len(sys.argv) == 3 and sys.argv[1] == "--fetch" and re.fullmatch(r"[0-9A-Za-z.+!-]+", sys.argv[2]):
        print(json.dumps(fetch_worker(sys.argv[2])))
    else:
        try:
            main()
        except (FetchFailure, ValueError) as exc:
            raise SystemExit(f"PyPI verification failed: {exc}") from None
