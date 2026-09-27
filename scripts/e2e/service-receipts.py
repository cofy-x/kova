#!/usr/bin/env python3
"""Private, run-scoped JSONL receipts for the Service Kind smoke.

Only projected API fields are persisted.  An attempt without a matching
submitted event is an uncertain POST, never proof that no build was created.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import stat
import sys
from datetime import datetime, timezone
from pathlib import Path

RUN_ID = re.compile(r"service-e2e-[0-9]{8}t[0-9]{6}z-[0-9a-f]{8}\Z")
JOB_ID = re.compile(r"[a-z0-9](?:[-a-z0-9]*[a-z0-9])?\Z")
DIGEST = re.compile(r"sha256:[0-9a-f]{64}\Z")
STATUS = {"queued", "starting", "running", "verifying", "succeeded", "failed", "cancelled"}
MAX_RESPONSE = 1 << 20
MAX_RECEIPT = 2 << 20


class ReceiptError(ValueError):
    pass


def require(condition: bool, message: str) -> None:
    if not condition:
        raise ReceiptError(message)


def timestamp() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def encode(record: dict) -> bytes:
    secret = os.environ.get("KOVA_SERVICE_E2E_RECEIPT_SECRET", "")
    payload = (json.dumps(record, sort_keys=True, separators=(",", ":")) + "\n").encode()
    require(not secret or secret.encode() not in payload, "receipt would disclose the Service bearer")
    require(len(payload) <= 64 * 1024, "receipt event exceeds 64 KiB")
    return payload


def check_parent(root: Path, parent: Path, *, create: bool = False) -> None:
    work = root / ".work"
    require(parent == work or work in parent.parents, "RESULT_JSONL escapes checkout .work")
    require(not root.is_symlink(), "checkout root is a symlink")
    current = root
    for component in (".work", *parent.relative_to(work).parts):
        current /= component
        require(not current.is_symlink(), "RESULT_JSONL has a symlink parent")
        if create and not current.exists():
            current.mkdir(mode=0o700)
        require(current.is_dir(), "RESULT_JSONL parent is not a directory")


def receipt_path(root_raw: str, base_raw: str, run_id: str, *, create_parent: bool) -> Path:
    require(RUN_ID.fullmatch(run_id) is not None, "invalid Service E2E run ID")
    root = Path(root_raw).resolve(strict=True)
    base = Path(base_raw)
    require(base_raw != "" and base.name.endswith(".jsonl") and len(base.name) > 6,
            "RESULT_JSONL must name a .jsonl file")
    require(".." not in base.parts, "RESULT_JSONL must not traverse parent directories")
    if not base.is_absolute():
        base = root / base
    work = root / ".work"
    require(base.parent == work or work in base.parent.parents, "RESULT_JSONL escapes checkout .work")
    require(not base.is_symlink() and not base.exists(), "RESULT_JSONL base already exists")
    path = base.with_name(base.name[:-6] + "-" + run_id + ".jsonl")
    old_umask = os.umask(0o077)
    try:
        check_parent(root, path.parent, create=create_parent)
    finally:
        os.umask(old_umask)
    require(not path.is_symlink() and not path.exists(), "run-scoped receipt already exists")
    return path


def open_existing(root_raw: str, path_raw: str, run_id: str) -> int:
    require(RUN_ID.fullmatch(run_id) is not None, "invalid Service E2E run ID")
    root = Path(root_raw).resolve(strict=True)
    path = Path(path_raw)
    require(path.is_absolute() and ".." not in path.parts, "receipt path is not an absolute checked path")
    require(path.name.endswith("-" + run_id + ".jsonl"), "receipt path differs from run ID")
    check_parent(root, path.parent)
    flags = os.O_RDWR | os.O_APPEND | getattr(os, "O_NOFOLLOW", 0)
    fd = os.open(path, flags)
    try:
        mode = os.fstat(fd).st_mode
        require(stat.S_ISREG(mode) and mode & 0o077 == 0, "receipt is not a private regular file")
        require(os.fstat(fd).st_size <= MAX_RECEIPT, "receipt exceeds bounded size")
        with os.fdopen(os.dup(fd), "rb") as stream:
            first = stream.readline(64 * 1024)
        header = json.loads(first)
        require(isinstance(header, dict) and header.get("run_id") == run_id,
                "receipt header belongs to another run")
        return fd
    except BaseException:
        os.close(fd)
        raise


def append(root: str, path: str, run_id: str, record: dict) -> None:
    payload = encode(record)
    fd = open_existing(root, path, run_id)
    try:
        require(os.fstat(fd).st_size + len(payload) <= MAX_RECEIPT,
                "receipt exceeds bounded size")
        require(os.write(fd, payload) == len(payload), "short receipt write")
        os.fsync(fd)
    finally:
        os.close(fd)


def read_response() -> dict:
    raw = sys.stdin.buffer.read(MAX_RESPONSE + 1)
    require(len(raw) <= MAX_RESPONSE, "Service response exceeds receipt input limit")
    try:
        response = json.loads(raw)
    except ValueError as error:
        raise ReceiptError("Service response is not JSON") from error
    require(isinstance(response, dict), "Service response is not a JSON object")
    return response


def expected(args: argparse.Namespace) -> dict:
    require(args.stage in {"negative", "positive"}, "invalid Service E2E stage")
    require(args.key == args.run_id + "-" + args.stage, "idempotency key differs from run")
    require(DIGEST.fullmatch(args.source_digest) is not None, "invalid source digest")
    # The URI pins the OCI source manifest. source_digest is the ZIP content
    # digest inside that manifest; the two digests are deliberately distinct.
    manifest_digest = args.source_uri.rpartition("@")[2]
    require(args.source_uri.startswith("oci://") and args.source_uri.count("@") == 1
            and "?" not in args.source_uri and "#" not in args.source_uri
            and DIGEST.fullmatch(manifest_digest) is not None,
            "source is not immutable OCI")
    require(args.target != "" and len(args.target) <= 512 and "@" not in args.target
            and "://" not in args.target and "?" not in args.target and "#" not in args.target,
            "invalid receipt target")
    return {
        "schema_version": 1,
        "run_id": args.run_id,
        "event": args.event,
        "stage": args.stage,
        "observed_at": timestamp(),
        "source_uri": args.source_uri,
        "source_digest": args.source_digest,
        "target": args.target,
        "idempotency_key": args.key,
    }


def project_job(args: argparse.Namespace, record: dict, response: dict) -> dict:
    job_id = response.get("id")
    status = response.get("status")
    require(isinstance(job_id, str) and len(job_id) <= 253 and JOB_ID.fullmatch(job_id) is not None, "invalid returned job ID")
    require(status in STATUS, "invalid returned job status")
    require(response.get("source_uri") == args.source_uri, "returned source URI differs")
    require(response.get("source_digest") == args.source_digest, "returned source digest differs")
    require(response.get("idempotency_key") == args.key, "returned idempotency key differs")
    require(response.get("requester") == "kova:e2e", "returned requester differs")
    if args.job_id:
        require(job_id == args.job_id, "returned job ID differs")
    if args.event == "terminal":
        require(status in {"succeeded", "failed", "cancelled"}, "job is not terminal")
    failure_code = response.get("failure_code", "")
    require(isinstance(failure_code, str) and re.fullmatch(r"[a-z_]{0,64}", failure_code) is not None, "invalid failure code")
    record.update(job_id=job_id, status=status, failure_code=failure_code)
    return record


def project_results(args: argparse.Namespace, record: dict, response: dict) -> dict:
    require(response.get("id") == args.job_id, "result job ID differs")
    require(response.get("source_uri") == args.source_uri, "result source URI differs")
    require(response.get("source_digest") == args.source_digest, "result source digest differs")
    require(response.get("idempotency_key") == args.key, "result idempotency key differs")
    outputs = response.get("outputs")
    require(isinstance(outputs, list) and len(outputs) <= 200, "invalid result outputs")
    projected = []
    for output in outputs:
        require(isinstance(output, dict), "invalid result output")
        digest = output.get("manifest_digest")
        image = output.get("image")
        immutable_ref = output.get("immutable_ref")
        require(isinstance(digest, str) and DIGEST.fullmatch(digest) is not None, "invalid output digest")
        require(isinstance(image, str) and image != "" and len(image) <= 512, "invalid output image")
        fmt = output.get("format")
        require(fmt in {"oci", "nydus"}, "invalid output format")
        expected_image = args.target if fmt == "oci" else args.target + "_nydus_v3"
        require(image == expected_image, "output differs from submitted target")
        repository, separator, tag = image.rpartition(":")
        require(separator != "" and "/" in repository and tag != "", "invalid tagged output")
        require(immutable_ref == repository + "@" + digest, "invalid immutable output")
        require(output.get("platform") in {"linux/amd64", "linux/arm64"}, "invalid output platform")
        projected.append({key: output[key] for key in ("format", "image", "platform", "manifest_digest", "immutable_ref")})
    record.update(job_id=args.job_id, outputs=projected)
    return record


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("init", "event"))
    parser.add_argument("--root", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--base")
    parser.add_argument("--path")
    parser.add_argument("--revision")
    parser.add_argument("--cluster")
    parser.add_argument("--namespace")
    parser.add_argument("--runner-namespace")
    parser.add_argument("--source-uri")
    parser.add_argument("--source-digest")
    parser.add_argument("--event")
    parser.add_argument("--stage")
    parser.add_argument("--target")
    parser.add_argument("--key")
    parser.add_argument("--job-id")
    args = parser.parse_args()
    try:
        if args.mode == "init":
            require(args.base is not None, "RESULT_JSONL base is required")
            require(re.fullmatch(r"[0-9a-f]{40}", args.revision or "") is not None, "invalid checkout revision")
            require(bool(args.cluster and args.namespace and args.runner_namespace), "missing cluster namespace identity")
            path = receipt_path(args.root, args.base, args.run_id, create_parent=True)
            header = {
                "schema_version": 1,
                "run_id": args.run_id,
                "event": "run_started",
                "observed_at": timestamp(),
                "revision": args.revision,
                "cluster": args.cluster,
                "namespace": args.namespace,
                "runner_namespace": args.runner_namespace,
            }
            old_umask = os.umask(0o077)
            try:
                flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0)
                fd = os.open(path, flags, 0o600)
                try:
                    payload = encode(header)
                    require(os.write(fd, payload) == len(payload), "short receipt header write")
                    os.fsync(fd)
                finally:
                    os.close(fd)
            finally:
                os.umask(old_umask)
            print(path)
            return 0
        require(args.path is not None and args.event is not None, "receipt path and event are required")
        require(args.source_uri is not None and args.source_digest is not None, "source identity is required")
        require(args.target is not None and args.key is not None, "job identity is required")
        require(args.event in {"submit_attempt", "submit_unconfirmed", "submitted", "terminal", "observation_incomplete", "results", "verified"}, "invalid receipt event")
        record = expected(args)
        if args.event in {"submitted", "terminal"}:
            record = project_job(args, record, read_response())
        elif args.event == "results":
            require(args.job_id is not None, "result job ID is required")
            record = project_results(args, record, read_response())
        elif args.event == "verified":
            require(args.stage == "positive" and args.job_id, "verification requires positive job ID")
            require(len(args.job_id) <= 253 and JOB_ID.fullmatch(args.job_id) is not None,
                    "invalid verified job ID")
            record["job_id"] = args.job_id
        elif args.job_id:
            require(len(args.job_id) <= 253 and JOB_ID.fullmatch(args.job_id) is not None, "invalid expected job ID")
            record["job_id"] = args.job_id
        append(args.root, args.path, args.run_id, record)
        if args.event == "submitted":
            print(record["job_id"])
        return 0
    except (OSError, ReceiptError, ValueError, TypeError, KeyError, UnicodeError, RecursionError) as error:
        # Diagnostics are fixed classes only; raw API bodies and bearer values
        # are deliberately omitted even when a caller sends malicious JSON.
        print(f"service-receipts: {type(error).__name__}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
