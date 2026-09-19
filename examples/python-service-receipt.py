#!/usr/bin/env python3
"""Submit one immutable source and persist a caller-owned build receipt."""

from __future__ import annotations

import argparse
import json
import os
import tempfile
from collections.abc import Mapping
from pathlib import Path

from kova_client import ClientConfig, CreateBuildRequest, JobStatus, KovaClient


def main() -> None:
    args = parse_args()
    config = ClientConfig.from_env()
    with KovaClient(config) as kova:
        created = kova.create_build(
            CreateBuildRequest(
                source_uri=args.source_uri,
                source_digest=args.source_digest,
                targets=tuple(args.target),
                concurrency=args.concurrency,
                format=args.format,
                idempotency_key=args.idempotency_key,
            )
        )
        terminal = kova.wait_build(created.id, timeout=args.wait_timeout)
        if terminal.status is not JobStatus.SUCCEEDED:
            raise SystemExit(
                f"Kova build {terminal.id} ended with status {terminal.status.value}: "
                f"{terminal.error or terminal.failure_code or 'no error summary'}"
            )
        results = kova.get_results(terminal.id)

    receipt = {
        "source_uri": results.source_uri,
        "source_digest": results.source_digest,
        "build_id": terminal.id,
        "idempotency_key": results.idempotency_key,
        "outputs": [
            {
                "format": output.format.value,
                "manifest_digest": output.manifest_digest,
                "immutable_ref": output.immutable_ref,
            }
            for output in results.outputs
        ],
    }
    write_atomic(args.receipt, receipt)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-uri", required=True)
    parser.add_argument("--source-digest", required=True)
    parser.add_argument("--target", action="append", required=True)
    parser.add_argument("--idempotency-key", required=True)
    parser.add_argument("--receipt", type=Path, required=True)
    parser.add_argument("--format", choices=("oci", "nydus", "both"), default="oci")
    parser.add_argument("--concurrency", type=int, default=1)
    parser.add_argument("--wait-timeout", type=float, default=1800)
    return parser.parse_args()


def write_atomic(path: Path, receipt: Mapping[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="w",
        encoding="utf-8",
        dir=path.parent,
        prefix=f".{path.name}.",
        delete=False,
    ) as temporary:
        json.dump(receipt, temporary, indent=2, sort_keys=True)
        temporary.write("\n")
        temporary_path = Path(temporary.name)
    os.replace(temporary_path, path)


if __name__ == "__main__":
    main()
