from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any

JSON = dict[str, Any]


class JobStatus(str, Enum):
    QUEUED = "queued"
    STARTING = "starting"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"


class BuildFailureCode(str, Enum):
    INVALID_SOURCE = "invalid_source"
    INVALID_TARGETS = "invalid_targets"
    RUNNER_UNAVAILABLE = "runner_unavailable"
    BUILD_SUBMISSION_FAILED = "build_submission_failed"
    RESULT_VERIFICATION_FAILED = "result_verification_failed"
    BUILD_FAILED = "build_failed"
    CANCELLED = "cancelled"


class BuildFormat(str, Enum):
    OCI = "oci"
    NYDUS = "nydus"
    BOTH = "both"


class OutputFormat(str, Enum):
    OCI = "oci"
    NYDUS = "nydus"


@dataclass(frozen=True, slots=True)
class CreateBuildRequest:
    source_uri: str
    source_digest: str
    targets: tuple[str, ...]
    concurrency: int
    format: BuildFormat | str = BuildFormat.OCI
    timeout: int | None = None
    oom_cooldown: str | None = None
    fail_fast: bool | None = None
    verbose: bool | None = None
    variables: tuple[str, ...] = ()
    idempotency_key: str | None = None

    def to_dict(self) -> JSON:
        payload: JSON = {
            "source_uri": self.source_uri,
            "source_digest": self.source_digest,
            "targets": list(self.targets),
            "format": _enum_value(self.format),
            "concurrency": self.concurrency,
        }
        _put_if_not_none(payload, "timeout", self.timeout)
        _put_if_not_none(payload, "oom_cooldown", self.oom_cooldown)
        _put_if_not_none(payload, "fail_fast", self.fail_fast)
        _put_if_not_none(payload, "verbose", self.verbose)
        if self.variables:
            payload["variables"] = list(self.variables)
        _put_if_not_none(payload, "idempotency_key", self.idempotency_key)
        return payload


@dataclass(frozen=True, slots=True)
class VersionInfo:
    api_version: str
    version: str
    commit: str
    build_date: str

    @classmethod
    def from_dict(cls, value: JSON) -> VersionInfo:
        _require_fields(value, {"api_version", "version", "commit", "build_date"})
        return cls(
            api_version=_string(value, "api_version"),
            version=_string(value, "version"),
            commit=_string(value, "commit"),
            build_date=_string(value, "build_date"),
        )


@dataclass(frozen=True, slots=True)
class ReadyStatus:
    status: str

    @classmethod
    def from_dict(cls, value: JSON) -> ReadyStatus:
        _require_fields(value, {"status"})
        return cls(status=_string(value, "status"))


@dataclass(frozen=True, slots=True)
class BuildJob:
    id: str
    status: JobStatus
    created_at: datetime
    requester: str
    error: str | None = None
    failure_code: BuildFailureCode | None = None
    started_at: datetime | None = None
    finished_at: datetime | None = None
    expires_at: datetime | None = None
    source_digest: str | None = None
    source_uri: str | None = None
    idempotency_key: str | None = None
    cancellation_requested: bool = False
    requested_concurrency: int | None = None
    allocated_concurrency: int | None = None

    @classmethod
    def from_dict(cls, value: JSON) -> BuildJob:
        _require_fields(value, {"id", "status", "created_at", "requester"}, _BUILD_JOB_FIELDS)
        failure = value.get("failure_code")
        return cls(
            id=_string(value, "id"),
            status=JobStatus(_string(value, "status")),
            created_at=_datetime(value, "created_at"),
            requester=_string(value, "requester"),
            error=_optional_string(value, "error"),
            failure_code=BuildFailureCode(failure) if failure is not None else None,
            started_at=_optional_datetime(value, "started_at"),
            finished_at=_optional_datetime(value, "finished_at"),
            expires_at=_optional_datetime(value, "expires_at"),
            source_digest=_optional_digest(value, "source_digest"),
            source_uri=_optional_string(value, "source_uri"),
            idempotency_key=_optional_string(value, "idempotency_key"),
            cancellation_requested=_optional_bool(value, "cancellation_requested") or False,
            requested_concurrency=_optional_int(value, "requested_concurrency"),
            allocated_concurrency=_optional_int(value, "allocated_concurrency"),
        )


@dataclass(frozen=True, slots=True)
class BuildOutput:
    format: OutputFormat
    image: str
    manifest_digest: str
    immutable_ref: str

    @classmethod
    def from_dict(cls, value: JSON) -> BuildOutput:
        _require_fields(value, {"format", "image", "manifest_digest", "immutable_ref"})
        manifest_digest = _digest(value, "manifest_digest")
        immutable_ref = _string(value, "immutable_ref")
        if not _IMMUTABLE_REFERENCE.fullmatch(immutable_ref):
            raise ValueError("immutable_ref must be a manifest-pinned OCI reference")
        if not immutable_ref.endswith("@" + manifest_digest):
            raise ValueError("immutable_ref digest must match manifest_digest")
        return cls(
            format=OutputFormat(_string(value, "format")),
            image=_string(value, "image"),
            manifest_digest=manifest_digest,
            immutable_ref=immutable_ref,
        )


@dataclass(frozen=True, slots=True)
class BuildResults:
    id: str
    source_uri: str
    source_digest: str
    outputs: tuple[BuildOutput, ...]
    idempotency_key: str | None = None

    @classmethod
    def from_dict(cls, value: JSON) -> BuildResults:
        _require_fields(
            value,
            {"id", "source_uri", "source_digest", "outputs"},
            {"id", "source_uri", "source_digest", "outputs", "idempotency_key"},
        )
        outputs = value["outputs"]
        if not isinstance(outputs, list):
            raise TypeError("outputs must be an array")
        if len(outputs) > 200:
            raise ValueError("outputs must contain at most 200 items")
        return cls(
            id=_string(value, "id"),
            source_uri=_string(value, "source_uri"),
            source_digest=_digest(value, "source_digest"),
            outputs=tuple(BuildOutput.from_dict(_object(item)) for item in outputs),
            idempotency_key=_optional_string(value, "idempotency_key"),
        )


@dataclass(frozen=True, slots=True)
class JobList:
    jobs: tuple[BuildJob, ...]
    continue_token: str | None = None

    @classmethod
    def from_dict(cls, value: JSON) -> JobList:
        _require_fields(value, {"jobs"}, {"jobs", "continue"})
        jobs = value["jobs"]
        if not isinstance(jobs, list):
            raise TypeError("jobs must be an array")
        if len(jobs) > 500:
            raise ValueError("jobs must contain at most 500 items")
        return cls(
            jobs=tuple(BuildJob.from_dict(_object(item)) for item in jobs),
            continue_token=_optional_string(value, "continue"),
        )


_BUILD_JOB_FIELDS = {
    "id",
    "status",
    "error",
    "failure_code",
    "created_at",
    "started_at",
    "finished_at",
    "expires_at",
    "source_digest",
    "source_uri",
    "idempotency_key",
    "requester",
    "cancellation_requested",
    "requested_concurrency",
    "allocated_concurrency",
}
_DIGEST = re.compile(r"^sha256:[a-f0-9]{64}$")
_IMMUTABLE_REFERENCE = re.compile(r"^.+@sha256:[a-f0-9]{64}$")


def _enum_value(value: str | Enum) -> str:
    return str(value.value) if isinstance(value, Enum) else value


def _put_if_not_none(payload: JSON, key: str, value: Any) -> None:
    if value is not None:
        payload[key] = value


def _object(value: Any) -> JSON:
    if not isinstance(value, dict) or not all(isinstance(key, str) for key in value):
        raise TypeError("response must be a JSON object")
    return value


def _require_fields(value: JSON, required: set[str], allowed: set[str] | None = None) -> None:
    missing = required - value.keys()
    if missing:
        raise TypeError(f"response is missing fields: {', '.join(sorted(missing))}")
    unexpected = value.keys() - (allowed or required)
    if unexpected:
        raise TypeError(f"response has unexpected fields: {', '.join(sorted(unexpected))}")


def _string(value: JSON, key: str) -> str:
    result = value[key]
    if not isinstance(result, str):
        raise TypeError(f"{key} must be a string")
    return result


def _optional_string(value: JSON, key: str) -> str | None:
    if key not in value:
        return None
    return _string(value, key)


def _optional_int(value: JSON, key: str) -> int | None:
    if key not in value:
        return None
    result = value[key]
    if not isinstance(result, int) or isinstance(result, bool):
        raise TypeError(f"{key} must be an integer")
    return result


def _optional_bool(value: JSON, key: str) -> bool | None:
    if key not in value:
        return None
    result = value[key]
    if not isinstance(result, bool):
        raise TypeError(f"{key} must be a boolean")
    return result


def _digest(value: JSON, key: str) -> str:
    result = _string(value, key)
    if not _DIGEST.fullmatch(result):
        raise ValueError(f"{key} must be a SHA-256 digest")
    return result


def _optional_digest(value: JSON, key: str) -> str | None:
    if key not in value:
        return None
    return _digest(value, key)


def _datetime(value: JSON, key: str) -> datetime:
    raw = _string(value, key)
    parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError(f"{key} must include a timezone")
    return parsed


def _optional_datetime(value: JSON, key: str) -> datetime | None:
    if key not in value:
        return None
    return _datetime(value, key)
