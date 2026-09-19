from __future__ import annotations

from dataclasses import fields
from pathlib import Path

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
)

OPENAPI = Path(__file__).resolve().parents[3] / "api" / "openapi.yaml"


def test_python_models_match_openapi_properties_and_enums() -> None:
    document = yaml.safe_load(OPENAPI.read_text())
    schemas = document["components"]["schemas"]
    assert _field_names(CreateBuildRequest) == set(schemas["CreateBuildRequest"]["properties"])
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


def _field_names(model: type[object]) -> set[str]:
    names = {field.name for field in fields(model)}
    if model is JobList:
        names.remove("continue_token")
        names.add("continue")
    return names
