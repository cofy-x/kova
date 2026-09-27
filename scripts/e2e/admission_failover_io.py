#!/usr/bin/env python3
"""Secret-safe HTTP and UID-preconditioned writes for the admission failover E2E.

This helper has no default action.  The caller owns all Kind, workload, ledger,
and Lease gates; this process keeps the bearer token in memory only and writes
allowlisted response facts rather than arbitrary server-controlled content.
"""

from __future__ import annotations

import argparse
import base64
import binascii
import hashlib
import json
import os
from pathlib import Path
import re
import select
import subprocess
import sys
import time
from urllib.error import HTTPError, URLError
from urllib.request import OpenerDirector, Request, build_opener, ProxyHandler

MAX_RESPONSE = 1024 * 1024
BUILD_ID = re.compile(r"idem-[0-9a-f]{20}\Z")
POD_NAME = re.compile(r"kova-(?:job-idem-[0-9a-f]{20}|service-[a-z0-9-]+)\Z")
UID = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\Z")
OPENER: OpenerDirector = build_opener(ProxyHandler({}))


class SafetyError(Exception):
    """A refusal whose message must never include a credential or response body."""


def save_receipt(path: Path, facts: dict) -> None:
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    data = (json.dumps(facts, sort_keys=True, separators=(",", ":")) + "\n").encode()
    with open(path, "xb", opener=lambda target, flags: os.open(target, flags, 0o600)) as stream:
        stream.write(data)


def bounded_response(request: Request, *, timeout: int) -> tuple[int, dict, bytes]:
    try:
        with OPENER.open(request, timeout=timeout) as response:
            status = response.status
            headers = dict(response.headers.items())
            body = response.read(MAX_RESPONSE + 1)
    except HTTPError as error:
        status = error.code
        headers = dict(error.headers.items())
        body = error.read(MAX_RESPONSE + 1)
    except (OSError, URLError) as error:
        raise SafetyError(f"HTTP outcome unknown: {type(error).__name__}") from None
    if len(body) > MAX_RESPONSE:
        raise SafetyError("HTTP response exceeded bounded size; outcome unknown")
    return status, headers, body


def kubectl_json(kubeconfig: Path, namespace: str, resource: str, name: str) -> dict:
    result = subprocess.run(
        ["kubectl", "--kubeconfig", str(kubeconfig), "-n", namespace, "get", resource, name, "-o", "json"],
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        check=False,
        timeout=15,
    )
    if result.returncode or len(result.stdout) > MAX_RESPONSE:
        raise SafetyError(f"could not read exact {resource} identity")
    try:
        return json.loads(result.stdout)
    except (ValueError, UnicodeError):
        raise SafetyError(f"exact {resource} returned malformed JSON") from None


def test_token(kubeconfig: Path, namespace: str, expected_uid: str) -> str:
    secret = kubectl_json(kubeconfig, namespace, "secret", "kova-e2e-token")
    if (
        secret.get("metadata", {}).get("name") != "kova-e2e-token"
        or secret.get("metadata", {}).get("namespace") != namespace
        or secret.get("metadata", {}).get("uid") != expected_uid
        or secret.get("metadata", {}).get("deletionTimestamp") is not None
        or secret.get("type") != "Opaque"
    ):
        raise SafetyError("test-only Secret identity differs")
    encoded = secret.get("data", {}).get("token")
    if not isinstance(encoded, str):
        raise SafetyError("test-only Secret token key is missing")
    try:
        token = base64.b64decode(encoded, validate=True).decode("ascii")
    except (binascii.Error, UnicodeError):
        raise SafetyError("test-only Secret token encoding is invalid") from None
    if not 16 <= len(token) <= 512 or any(ord(char) < 33 or ord(char) > 126 for char in token):
        raise SafetyError("test-only Secret token characters or length are invalid")
    return token


def post_build(args: argparse.Namespace) -> None:
    if not BUILD_ID.fullmatch(args.expected_id):
        raise SafetyError("expected Build ID is not run-scoped")
    if not UID.fullmatch(args.secret_uid):
        raise SafetyError("POST requires the exact test-only Secret UID")
    if args.cluster and args.kubeconfig_sha256:
        assert_exact_kind(args.kubeconfig, args.cluster, args.kubeconfig_sha256)
    payload = sys.stdin.buffer.read(8193)
    if not payload or len(payload) > 8192:
        raise SafetyError("POST payload is empty or unbounded")
    token = test_token(args.kubeconfig, args.namespace, args.secret_uid)
    request = Request(
        f"http://127.0.0.1:{args.port}/v1/builds",
        data=payload,
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        method="POST",
    )
    started = time.monotonic()
    try:
        status, headers, body = bounded_response(request, timeout=15)
    except SafetyError:
        save_receipt(args.receipt, {"outcome": "unknown", "expected_id": args.expected_id})
        raise
    observed = next((value for key, value in headers.items() if key.lower() == "x-kova-build-id"), None)
    matched = observed == args.expected_id
    save_receipt(
        args.receipt,
        {
            "outcome": "accepted" if status == 202 and matched else "refused",
            "http_status": status,
            "expected_id": args.expected_id,
            "header_matches_expected": matched,
            "header_length": len(observed) if observed is not None else 0,
            "response_length": len(body),
            "elapsed_seconds": round(time.monotonic() - started, 6),
        },
    )
    if status != 202 or not matched:
        raise SafetyError("POST was not newly accepted with the expected Build ID")


def assert_exact_kind(kubeconfig: Path, cluster: str, expected_sha: str) -> None:
    if kubeconfig.stat().st_size > MAX_RESPONSE or hashlib.sha256(kubeconfig.read_bytes()).hexdigest() != expected_sha:
        raise SafetyError("dedicated kubeconfig bytes changed")
    clusters = subprocess.run(
        ["kind", "get", "clusters"], capture_output=True, text=True, check=False, timeout=15
    )
    if clusters.returncode or clusters.stdout.splitlines() != [cluster]:
        raise SafetyError("the only Kind cluster is not the expected dedicated cluster")
    context = subprocess.run(
        ["kubectl", "--kubeconfig", str(kubeconfig), "config", "current-context"],
        capture_output=True,
        text=True,
        check=False,
        timeout=15,
    )
    if context.returncode or context.stdout.strip() != f"kind-{cluster}":
        raise SafetyError("dedicated kubeconfig context changed")
    live = subprocess.run(
        ["kind", "get", "kubeconfig", "--name", cluster],
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        check=False,
        timeout=15,
    )
    if live.returncode or len(live.stdout) > MAX_RESPONSE or hashlib.sha256(live.stdout).hexdigest() != expected_sha:
        raise SafetyError("dedicated kubeconfig differs from the live Kind credentials/server")


def uid_delete(args: argparse.Namespace) -> None:
    if not UID.fullmatch(args.uid):
        raise SafetyError("atomic delete requires a recorded Kubernetes UID")
    if not UID.fullmatch(args.lease_uid) or not args.lease_holder or args.lease_name != "kova-service.kova.cofy.dev":
        raise SafetyError("atomic delete requires the exact admission Lease identity")
    if args.resource == "kovabuilds":
        if not BUILD_ID.fullmatch(args.name):
            raise SafetyError("refusing non-run-scoped KovaBuild deletion")
        path = f"/apis/kova.cofy.dev/v1alpha1/namespaces/{args.namespace}/kovabuilds/{args.name}"
    elif args.resource == "pods":
        if not POD_NAME.fullmatch(args.name):
            raise SafetyError("refusing an unexpected Pod name")
        path = f"/api/v1/namespaces/{args.namespace}/pods/{args.name}"
    else:
        raise SafetyError("unsupported delete resource")
    assert_exact_kind(args.kubeconfig, args.cluster, args.kubeconfig_sha256)
    current = kubectl_json(args.kubeconfig, args.namespace, args.resource, args.name)
    if current.get("metadata", {}).get("uid") != args.uid:
        raise SafetyError("exact object UID changed before atomic delete")
    proxy = subprocess.Popen(
        [
            "kubectl", "--kubeconfig", str(args.kubeconfig), "proxy", "--address=127.0.0.1",
            "--port=0", "--accept-hosts=^127\\.0\\.0\\.1$",
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        start_new_session=True,
    )
    try:
        deadline = time.monotonic() + 30
        port = None
        while time.monotonic() < deadline:
            if proxy.poll() is not None:
                raise SafetyError("loopback Kubernetes API proxy exited")
            ready, _, _ = select.select([proxy.stdout], [], [], 0.5)
            if ready:
                line = proxy.stdout.readline()
                match = re.search(r"Starting to serve on 127\.0\.0\.1:(\d+)", line)
                if match:
                    port = int(match.group(1))
                    break
        if port is None:
            raise SafetyError("loopback Kubernetes API proxy did not become ready")
        assert_exact_kind(args.kubeconfig, args.cluster, args.kubeconfig_sha256)
        lease = kubectl_json(args.kubeconfig, args.namespace, "lease", args.lease_name)
        if (
            lease.get("metadata", {}).get("uid") != args.lease_uid
            or lease.get("spec", {}).get("holderIdentity") != args.lease_holder
        ):
            raise SafetyError("admission Lease UID or holder changed before atomic delete")
        payload = json.dumps(
            {
                "apiVersion": "meta.k8s.io/v1",
                "kind": "DeleteOptions",
                "preconditions": {"uid": args.uid},
                "propagationPolicy": "Background",
            },
            separators=(",", ":"),
        ).encode()
        request = Request(
            f"http://127.0.0.1:{port}{path}",
            data=payload,
            headers={"Content-Type": "application/json"},
            method="DELETE",
        )
        try:
            status, _, body = bounded_response(request, timeout=20)
        except SafetyError:
            save_receipt(args.receipt, {"outcome": "unknown", "resource": args.resource, "name": args.name, "uid": args.uid})
            raise
        save_receipt(
            args.receipt,
            {
                "outcome": "accepted" if status in (200, 202) else "refused",
                "resource": args.resource,
                "name": args.name,
                "uid_precondition": args.uid,
                "http_status": status,
                "response_length": len(body),
            },
        )
        if status not in (200, 202):
            raise SafetyError("atomic UID-preconditioned delete was not accepted")
    finally:
        proxy.terminate()
        try:
            proxy.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proxy.kill()
            proxy.wait(timeout=5)
        if proxy.stdout is not None:
            proxy.stdout.close()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("operation", choices=("post", "delete"))
    parser.add_argument("--kubeconfig", type=Path, required=True)
    parser.add_argument("--namespace", required=True)
    parser.add_argument("--receipt", type=Path, required=True)
    parser.add_argument("--port", type=int)
    parser.add_argument("--expected-id")
    parser.add_argument("--secret-uid")
    parser.add_argument("--cluster")
    parser.add_argument("--kubeconfig-sha256")
    parser.add_argument("--resource", choices=("kovabuilds", "pods"))
    parser.add_argument("--name")
    parser.add_argument("--uid")
    parser.add_argument("--lease-name")
    parser.add_argument("--lease-uid")
    parser.add_argument("--lease-holder")
    args = parser.parse_args()
    try:
        if not re.fullmatch(r"[a-z][a-z0-9-]*", args.namespace):
            raise SafetyError("invalid Kubernetes namespace")
        if args.operation == "post":
            if not args.port or not 1 <= args.port <= 65535 or not args.expected_id or not args.secret_uid:
                raise SafetyError("POST requires exact loopback port, Build ID, and test Secret UID")
            if bool(args.cluster) != bool(args.kubeconfig_sha256):
                raise SafetyError("POST Kind identity arguments must be supplied together")
            post_build(args)
        else:
            if not all((args.cluster, args.kubeconfig_sha256, args.resource, args.name, args.uid,
                        args.lease_name, args.lease_uid, args.lease_holder)):
                raise SafetyError("atomic delete requires Kind, Lease, resource, name, and UID")
            uid_delete(args)
        return 0
    except (SafetyError, OSError, subprocess.TimeoutExpired) as error:
        print(f"error: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
