from __future__ import annotations

from dataclasses import fields
from pathlib import Path

import pytest
import yaml

from kova_client import (
    BuildFailureCode,
    BuildJob,
    BuildOutput,
    BuildResults,
    CreateBuildRequest,
    JobList,
    JobStatus,
    OutputFormat,
    Platform,
    TargetSpec,
)

OPENAPI = Path(__file__).resolve().parents[3] / "api" / "openapi.yaml"


def test_python_models_match_openapi_properties_and_enums() -> None:
    document = yaml.safe_load(OPENAPI.read_text())
    schemas = document["components"]["schemas"]
    assert _field_names(CreateBuildRequest) == set(schemas["CreateBuildRequest"]["properties"])
    assert _field_names(TargetSpec) == set(schemas["TargetSpec"]["properties"])
    assert _field_names(BuildJob) == set(schemas["BuildJob"]["properties"])
    assert _field_names(BuildOutput) == set(schemas["BuildOutput"]["properties"])
    assert _field_names(BuildResults) == set(schemas["BuildResults"]["properties"])
    assert {item.value for item in JobStatus} == set(
        schemas["BuildJob"]["properties"]["status"]["enum"]
    )
    assert {item.value for item in BuildFailureCode} == set(
        schemas["BuildJob"]["properties"]["failure_code"]["enum"]
    )
    assert {item.value for item in OutputFormat} == set(
        schemas["BuildOutput"]["properties"]["format"]["enum"]
    )
    assert {item.value for item in Platform} == set(
        schemas["TargetSpec"]["properties"]["platform"]["enum"]
    )
    assert {item.value for item in Platform} == set(
        schemas["BuildOutput"]["properties"]["platform"]["enum"]
    )


def test_public_operations_exist_in_openapi() -> None:
    document = yaml.safe_load(OPENAPI.read_text())
    operations = {
        operation["operationId"]
        for path in document["paths"].values()
        for operation in path.values()
        if isinstance(operation, dict) and "operationId" in operation
    }
    assert operations == {
        "version",
        "ready",
        "createBuild",
        "listBuilds",
        "getBuild",
        "getResults",
        "getLogs",
        "cancelBuild",
    }


def test_target_platform_is_not_a_free_form_label() -> None:
    with pytest.raises(TypeError, match="Platform value"):
        TargetSpec(target="registry.example.com/team/image:dev", platform="linux/s390x")  # type: ignore[arg-type]


def test_job_preserves_unknown_future_failure_code() -> None:
    base = {
        "id": "job-1",
        "status": "failed",
        "created_at": "2026-09-27T00:00:00Z",
        "requester": "test-user",
    }
    assert BuildJob.from_dict({**base, "failure_code": "invalid_source"}).failure_code is BuildFailureCode.INVALID_SOURCE
    assert BuildJob.from_dict({**base, "failure_code": "future_source_fault"}).failure_code == "future_source_fault"
    with pytest.raises(TypeError, match="failure_code must be a string"):
        BuildJob.from_dict({**base, "failure_code": 42})
    with pytest.raises(ValueError, match="failure_code must not be empty"):
        BuildJob.from_dict({**base, "failure_code": ""})


def _field_names(model: type[object]) -> set[str]:
    names = {field.name for field in fields(model)}
    if model is JobList:
        names.remove("continue_token")
        names.add("continue")
    return names
