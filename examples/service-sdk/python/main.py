#!/usr/bin/env python3
"""Submit an immutable source and persist a caller-owned seed build receipt."""

from __future__ import annotations

import json
import os
import re
import signal
import sys
import tempfile
import threading
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

from kova_client import (
    BuildFormat,
    BuildJob,
    BuildOutput,
    ClientConfig,
    CreateBuildRequest,
    JobStatus,
    KovaAPIError,
    KovaClient,
    KovaProtocolError,
    KovaWaitCancelled,
    Platform,
    TargetSpec,
)

RECEIPT_SCHEMA = "kova.seed-build-receipt/v1"
DIGEST = re.compile(r"^sha256:[a-f0-9]{64}$")


@dataclass(frozen=True, slots=True)
class Settings:
    source_uri: str
    source_digest: str
    target: str
    platform: Platform
    idempotency_key: str
    receipt_path: Path
    recipe_digest: str
    target_role: str
    build_format: BuildFormat
    wait_timeout: float
    poll_interval: float


def main() -> int:
    token = os.environ.get("KOVA_SERVICE_TOKEN", "").strip()
    try:
        settings = load_settings()
        config = ClientConfig.from_env()
    except (TypeError, ValueError) as error:
        return fail(1, f"configuration error: {sanitize(str(error), token)}")

    cancelled = threading.Event()

    def cancel_wait(_signum: int, _frame: object) -> None:
        cancelled.set()

    signal.signal(signal.SIGINT, cancel_wait)
    signal.signal(signal.SIGTERM, cancel_wait)

    try:
        with KovaClient(config) as kova:
            version = kova.version()
            if version.api_version != "v1":
                raise KovaProtocolError(
                    f"Kova Service API {version.api_version!r} is incompatible with this example"
                )
            kova.ready()
            created = kova.create_build(
                CreateBuildRequest(
                    source_uri=settings.source_uri,
                    source_digest=settings.source_digest,
                    targets=(TargetSpec(settings.target, settings.platform),),
                    concurrency=1,
                    format=settings.build_format,
                    idempotency_key=settings.idempotency_key,
                )
            )
            terminal = kova.wait_build(
                created.id,
                timeout=settings.wait_timeout,
                poll_interval=settings.poll_interval,
                cancel_event=cancelled,
            )
            results = kova.get_results(terminal.id)
    except KovaAPIError as error:
        retry_after = error.retry_after.total_seconds() if error.retry_after else 0
        return fail(
            1,
            "Kova API error: "
            f"HTTP {error.status_code} code={error.code} retryable={str(error.retryable).lower()} "
            f"retry_after={retry_after:g}s message={sanitize(error.message, token)}",
        )
    except TimeoutError:
        return fail(2, "timed out waiting for Kova build")
    except KovaWaitCancelled:
        return fail(2, "wait for Kova build was cancelled")
    except (KovaProtocolError, OSError, ValueError) as error:
        return fail(1, f"Kova request failed: {sanitize(str(error), token)}")
    except Exception as error:  # SDK transport errors remain caller-visible.
        return fail(1, f"Kova request failed: {sanitize(str(error), token)}")

    try:
        validate_results(
            results.id,
            terminal.id,
            results.source_uri,
            results.source_digest,
            results.idempotency_key,
            settings,
        )
        outputs = receipt_outputs(results.outputs, settings.target_role, settings.platform)
        if terminal.status is JobStatus.SUCCEEDED and not outputs:
            raise ValueError("successful build has no verified outputs")
        if terminal.status is JobStatus.SUCCEEDED:
            validate_successful_formats(outputs, settings.build_format)
    except ValueError as error:
        return fail(1, f"invalid build results: {sanitize(str(error), token)}")
    receipt: dict[str, object] = {
        "schema": RECEIPT_SCHEMA,
        "recipe_digest": settings.recipe_digest,
        "source_uri": results.source_uri,
        "source_digest": results.source_digest,
        "kova_build_id": terminal.id,
        "kova_version": version.version,
        "idempotency_key": results.idempotency_key or settings.idempotency_key,
        "status": terminal.status.value,
        "outputs": outputs,
    }
    if terminal.failure_code is not None:
        receipt["failure_code"] = terminal.failure_code.value
    try:
        write_atomic(settings.receipt_path, receipt)
    except OSError as error:
        return fail(1, f"write caller-owned receipt: {sanitize(str(error), token)}")

    if terminal.status is not JobStatus.SUCCEEDED:
        return terminal_failure(terminal, len(outputs))
    for output in outputs:
        print(output["format"], output["manifest_digest"], output["immutable_ref"])
    return 0


def load_settings(env: Mapping[str, str] | None = None) -> Settings:
    values = os.environ if env is None else env

    def required(name: str) -> str:
        value = values.get(name, "").strip()
        if not value:
            raise ValueError(f"{name} is required")
        return value

    source_digest = required("KOVA_SOURCE_DIGEST")
    recipe_digest = required("KOVA_RECIPE_DIGEST")
    required("KOVA_SERVICE_TOKEN")
    if not DIGEST.fullmatch(source_digest):
        raise ValueError("KOVA_SOURCE_DIGEST must be a SHA-256 digest")
    if not DIGEST.fullmatch(recipe_digest):
        raise ValueError("KOVA_RECIPE_DIGEST must be a SHA-256 digest")
    try:
        platform = Platform(required("KOVA_PLATFORM"))
    except ValueError as error:
        raise ValueError("KOVA_PLATFORM must be linux/amd64 or linux/arm64") from error
    try:
        build_format = BuildFormat(values.get("KOVA_BUILD_FORMAT", "oci").strip() or "oci")
    except ValueError as error:
        raise ValueError("KOVA_BUILD_FORMAT must be oci, nydus, or both") from error
    wait_timeout = positive_seconds(values, "KOVA_WAIT_TIMEOUT_SECONDS", 600)
    poll_interval = positive_seconds(values, "KOVA_POLL_INTERVAL_SECONDS", 2)
    return Settings(
        source_uri=required("KOVA_SOURCE_URI"),
        source_digest=source_digest,
        target=required("KOVA_TARGET"),
        platform=platform,
        idempotency_key=required("KOVA_IDEMPOTENCY_KEY"),
        receipt_path=Path(required("KOVA_RECEIPT_PATH")),
        recipe_digest=recipe_digest,
        target_role=values.get("KOVA_TARGET_ROLE", "seed").strip() or "seed",
        build_format=build_format,
        wait_timeout=wait_timeout,
        poll_interval=poll_interval,
    )


def positive_seconds(values: Mapping[str, str], name: str, default: float) -> float:
    try:
        value = float(values.get(name, str(default)))
    except ValueError as error:
        raise ValueError(f"{name} must be a positive number of seconds") from error
    if value <= 0:
        raise ValueError(f"{name} must be a positive number of seconds")
    return value


def validate_results(
    result_id: str,
    build_id: str,
    source_uri: str,
    source_digest: str,
    idempotency_key: str | None,
    settings: Settings,
) -> None:
    if result_id != build_id:
        raise ValueError("build ID does not match the requested build")
    if source_uri != settings.source_uri:
        raise ValueError("source URI does not match the submitted request")
    if source_digest != settings.source_digest:
        raise ValueError("source digest does not match the submitted request")
    if idempotency_key and idempotency_key != settings.idempotency_key:
        raise ValueError("idempotency key does not match the submitted request")


def receipt_outputs(
    outputs: tuple[BuildOutput, ...], role: str, platform: Platform
) -> list[dict[str, str]]:
    result: list[dict[str, str]] = []
    seen_formats: set[str] = set()
    for output in outputs:
        if output.platform is not platform:
            raise ValueError(
                f"output {output.image!r} has platform {output.platform.value!r} "
                f"instead of {platform.value!r}"
            )
        if not DIGEST.fullmatch(output.manifest_digest) or not output.immutable_ref.endswith(
            "@" + output.manifest_digest
        ):
            raise ValueError(f"output {output.image!r} has inconsistent manifest facts")
        if output.format.value in seen_formats:
            raise ValueError(
                f"multiple {output.format.value} outputs were returned for one logical target"
            )
        seen_formats.add(output.format.value)
        result.append(
            {
                "role": role,
                "platform": output.platform.value,
                "format": output.format.value,
                "image": output.image,
                "manifest_digest": output.manifest_digest,
                "immutable_ref": output.immutable_ref,
            }
        )
    format_order = {"oci": 0, "nydus": 1}
    result.sort(
        key=lambda output: (
            output["role"],
            output["platform"],
            format_order[output["format"]],
            output["image"],
            output["manifest_digest"],
            output["immutable_ref"],
        )
    )
    return result


def validate_successful_formats(outputs: list[dict[str, str]], requested: BuildFormat) -> None:
    expected = {
        BuildFormat.OCI: {"oci"},
        BuildFormat.NYDUS: {"nydus"},
        BuildFormat.BOTH: {"oci", "nydus"},
    }[requested]
    actual = {output["format"] for output in outputs}
    if actual != expected:
        raise ValueError(
            f"successful build output formats do not match requested format {requested.value!r}"
        )


def terminal_failure(terminal: BuildJob, output_count: int) -> int:
    failure_code = terminal.failure_code.value if terminal.failure_code else "unspecified"
    return fail(
        3,
        f"Kova build {terminal.id} ended with status {terminal.status.value} "
        f"({failure_code}); saved {output_count} verified output(s)",
    )


def write_atomic(path: Path, value: Mapping[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=path.parent,
            prefix=f".{path.name}.",
            delete=False,
        ) as temporary:
            json.dump(value, temporary, indent=2, sort_keys=True)
            temporary.write("\n")
            temporary_path = Path(temporary.name)
        os.replace(temporary_path, path)
    finally:
        if temporary_path is not None:
            temporary_path.unlink(missing_ok=True)


def sanitize(message: str, token: str) -> str:
    return message.replace(token, "[redacted]") if token else message


def fail(code: int, message: str) -> int:
    print(message, file=sys.stderr)
    return code


if __name__ == "__main__":
    raise SystemExit(main())
