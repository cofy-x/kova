#!/usr/bin/env python3
"""Bounded #51 Service burst on an operator-created, run-owned Kind fixture.

``check`` validates only local contracts. ``run`` is deliberately opt-in and
never creates a cluster, namespace, registry, source tag, or deployment. An
uncertain submission or changed identity quarantines the fixture; it is never
retried or turned into an automated broad cleanup.
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import http.client
import ipaddress
import json
import math
import os
import re
import select
import socket
import stat
import subprocess
import sys
import tempfile
import threading
import time
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote, urlsplit
from urllib.request import Request

ROOT = Path(__file__).resolve().parents[2]
MAX_JSON = 2 << 20
MAX_METRICS = 4 << 20
MAX_SOURCE = 512 << 20
MAX_JOURNAL = 16 << 20
SHA = re.compile(r"[0-9a-f]{64}\Z")
DIGEST = re.compile(r"sha256:[0-9a-f]{64}\Z")
UID = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\Z")
RUN_ID = re.compile(r"i51-[a-z0-9]{8,28}\Z")
DNS = re.compile(r"[a-z0-9](?:[-a-z0-9]*[a-z0-9])?\Z")
HOST = re.compile(r"[A-Za-z0-9][A-Za-z0-9.-]{0,252}\Z")
DOCKER_NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}\Z")
OWNER_LABEL = "kova.cofy.dev/run-id"
METRIC_NAMES = (
    "kova_service_kube_limiter_wait_seconds",
    "kova_service_kube_wire_round_trip_seconds",
    "kova_service_kube_wire_requests_total",
    "controller_runtime_reconcile_time_seconds",
    "controller_runtime_reconcile_total",
)
REQUIRED_COUNTERS = (
    "client_limiter_wait_ms",
    "kube_wire_round_trip_ms",
    "kube_verb_resource_status_counts",
    "service_stage_attempt_ms",
    "queue_to_pod_ms",
    "pod_schedule_ms",
    "source_fetch_ms",
    "target_execution_ms",
    "runner_pod_requests",
    "worker_solve_occupancy",
    "buildkit_vertex_union_ns",
    "buildkit_export_union_ns",
    "registry_push_union_ns",
    "export_push_overlap_ns",
)
GENESIS_NAME = "kova-service-admission-genesis"
ACTIVE_NAME = "kova-service-admission"
QUEUE_NAME = "kova-service-queue-admission"
BUILDKIT_REVISION = "e42e1bfd389af7203238cce77b1f7dad447285e9"


class ContractError(ValueError):
    pass


class UnknownOutcome(RuntimeError):
    """A write or live identity may have changed; scripted cleanup is unsafe."""


class MeasuredFailure(RuntimeError):
    """A fully identified workload failed; exact-UID cleanup is still safe."""


def need(condition: bool, message: str) -> None:
    if not condition:
        raise ContractError(message)


def now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")  # noqa: UP017 (Python 3.9)


def canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()


def sha(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def unique_pairs(pairs: list[tuple[str, object]]) -> dict:
    result = {}
    for key, value in pairs:
        need(key not in result, "duplicate JSON key")
        result[key] = value
    return result


def load_json(path: Path, maximum: int = MAX_JSON) -> tuple[dict, str]:
    info = path.lstat()
    need(
        stat.S_ISREG(info.st_mode) and info.st_size <= maximum,
        "input is not a bounded regular file",
    )
    raw = path.read_bytes()
    need(len(raw) <= maximum, "input exceeds byte limit")
    value = json.loads(raw, object_pairs_hook=unique_pairs)
    need(type(value) is dict, "input JSON is not an object")
    return value, sha(canonical(value))


def exact_keys(value: object, keys: set[str], where: str) -> dict:
    need(type(value) is dict and set(value) == keys, f"{where} has unexpected or missing fields")
    return value


def match(value: object, pattern: re.Pattern, where: str) -> str:
    need(
        type(value) is str and pattern.fullmatch(value) is not None, f"{where} has invalid identity"
    )
    return value


def same(value: object, wanted: object, where: str) -> None:
    need(type(value) is type(wanted) and value == wanted, f"{where} differs from frozen contract")


def job_matrix(run_id: str, repository: str) -> list[dict]:
    jobs = [{"mode": "warmup", "concurrency": 1, "ordinal": 1}]
    jobs += [
        {"mode": mode, "concurrency": level, "ordinal": ordinal}
        for mode in ("cached", "uncached")
        for level in (8, 12)
        for ordinal in range(1, level + 1)
    ]
    for job in jobs:
        label = f"load-{run_id}-{job['mode']}-c{job['concurrency']}-n{job['ordinal']}"
        job["source_tag"] = f"{repository}:source-{label}"
        job["output_tag"] = f"{repository}:{label}"
        job["key"] = label
    return jobs


def validate_plan(plan: dict) -> list[dict]:
    exact_keys(
        plan,
        {
            "schema_version",
            "run_id",
            "candidate",
            "fixture",
            "limits",
            "measurement",
            "budget_seconds",
            "telemetry",
        },
        "plan",
    )
    same(plan["schema_version"], 2, "plan schema")
    run_id = match(plan["run_id"], RUN_ID, "run ID")
    candidate = plan["candidate"]
    exact_keys(
        candidate,
        {
            "version",
            "kova_commit",
            "cli_sha256",
            "chart_sha256",
            "values_sha256",
            "source_template_sha256",
            "base_image",
            "role_images",
        },
        "candidate",
    )
    match(
        candidate["version"],
        re.compile(r"(?:v[0-9]+\.[0-9]+\.[0-9]+-rc\.[0-9]+|dev-[0-9a-f]{12,40})\Z"),
        "version",
    )
    if candidate["version"].startswith("dev-"):
        need(
            candidate["kova_commit"].startswith(candidate["version"][4:]),
            "development version differs from source candidate",
        )
    match(candidate["kova_commit"], re.compile(r"[0-9a-f]{40}\Z"), "kova commit")
    for field in ("cli_sha256", "chart_sha256", "values_sha256", "source_template_sha256"):
        match(candidate[field], SHA, field)
    images = exact_keys(candidate["role_images"], {"controller", "runner", "worker"}, "role images")
    for role, image in images.items():
        need(
            type(image) is str
            and "@sha256:" in image
            and DIGEST.fullmatch(image.rsplit("@", 1)[-1]) is not None,
            f"{role} image is not pinned",
        )
    need(
        len({image.rsplit("@", 1)[1] for image in images.values()}) == 3,
        "role image digests overlap",
    )
    need(
        type(candidate["base_image"]) is str
        and DIGEST.fullmatch(candidate["base_image"].rsplit("@", 1)[-1]) is not None,
        "base image is not pinned",
    )
    fixture = plan["fixture"]
    exact_keys(
        fixture,
        {
            "host",
            "host_boot_id",
            "context",
            "kubeconfig_sha256",
            "kube_system_namespace_uid",
            "control_namespace",
            "control_namespace_uid",
            "runner_namespace",
            "runner_namespace_uid",
            "receipt_namespace",
            "receipt_namespace_uid",
            "service_deployment_uid",
            "worker_deployment_uid",
            "worker_service_uid",
            "worker_configmap_uid",
            "worker_config_sha256",
            "service_deployment_generation",
            "worker_deployment_generation",
            "service_deployment_template_sha256",
            "worker_deployment_template_sha256",
            "service_pod_uids",
            "worker_pod_uids",
            "registry_repository",
            "registry_instance_sha256",
            "registry_image",
            "registry_image_id",
            "registry_volume_created_at",
            "registry_volume_mountpoint",
            "registry_network_id",
            "repository_owner_run_id",
        },
        "fixture",
    )
    match(fixture["host"], HOST, "fixture host")
    need(
        type(fixture["context"]) is str
        and 0 < len(fixture["context"]) <= 253
        and not any(char.isspace() for char in fixture["context"]),
        "Kubernetes context is not an explicit bounded identity",
    )
    same(fixture["context"], "kind-kova-issue51-" + run_id, "dedicated Kind context")
    for field in ("control_namespace", "runner_namespace", "receipt_namespace"):
        match(fixture[field], DNS, field)
        need(
            len(fixture[field]) <= 63 and fixture[field].endswith("-" + run_id),
            f"{field} is not run-scoped",
        )
    need(
        len(
            {
                fixture[field]
                for field in ("control_namespace", "runner_namespace", "receipt_namespace")
            }
        )
        == 3,
        "control, fresh runner and recovery-receipt namespaces must differ",
    )
    same(fixture["repository_owner_run_id"], run_id, "repository owner")
    match(fixture["host_boot_id"], UID, "host boot ID")
    for field in (
        "kube_system_namespace_uid",
        "control_namespace_uid",
        "runner_namespace_uid",
        "receipt_namespace_uid",
        "service_deployment_uid",
        "worker_deployment_uid",
        "worker_service_uid",
        "worker_configmap_uid",
    ):
        match(fixture[field], UID, field)
    for field, count in (("service_pod_uids", 2), ("worker_pod_uids", 3)):
        value = fixture[field]
        need(
            type(value) is list and len(value) == count and len(set(value)) == count,
            f"{field} must have {count} unique UIDs",
        )
        for uid in value:
            match(uid, UID, field)
    for field in (
        "kubeconfig_sha256",
        "registry_instance_sha256",
        "registry_network_id",
        "worker_config_sha256",
    ):
        match(fixture[field], SHA, field)
    for kind in ("service", "worker"):
        generation = fixture[f"{kind}_deployment_generation"]
        need(type(generation) is int and generation >= 1, f"{kind} Deployment generation missing")
        match(fixture[f"{kind}_deployment_template_sha256"], SHA, f"{kind} Pod template SHA")
    need(
        type(fixture["registry_image"]) is str
        and re.fullmatch(r"registry:2@sha256:[0-9a-f]{64}", fixture["registry_image"]) is not None,
        "registry image is not a pinned Distribution 2 image",
    )
    match(fixture["registry_image_id"], DIGEST, "registry image ID")
    need(
        type(fixture["registry_volume_created_at"]) is str
        and 0 < len(fixture["registry_volume_created_at"]) <= 128,
        "registry volume creation identity is missing",
    )
    mountpoint = fixture["registry_volume_mountpoint"]
    need(
        type(mountpoint) is str
        and mountpoint.startswith("/")
        and len(mountpoint) <= 512
        and mountpoint != "/"
        and ".." not in Path(mountpoint).parts,
        "registry volume mountpoint is not an exact absolute path",
    )
    all_uids = (
        [
            fixture[field]
            for field in (
                "kube_system_namespace_uid",
                "control_namespace_uid",
                "runner_namespace_uid",
                "receipt_namespace_uid",
                "service_deployment_uid",
                "worker_deployment_uid",
                "worker_service_uid",
                "worker_configmap_uid",
            )
        ]
        + fixture["service_pod_uids"]
        + fixture["worker_pod_uids"]
    )
    need(len(set(all_uids)) == len(all_uids), "fixture Kubernetes UIDs overlap")
    repository = fixture["registry_repository"]
    need(
        type(repository) is str
        and re.fullmatch(
            r"[a-z0-9][a-z0-9.-]*(?::[0-9]{2,5})?/(?:[a-z0-9_-]+/)*[a-z0-9_-]+-"
            + re.escape(run_id),
            repository,
        )
        is not None,
        "registry repository is not run-scoped",
    )
    limits = plan["limits"]
    exact_keys(
        limits,
        {
            "service_replicas",
            "worker_replicas",
            "max_active_jobs",
            "max_active_jobs_per_requester",
            "worker_slots",
            "max_queued_jobs",
            "max_queued_jobs_per_requester",
            "controller_concurrency",
            "kube_client_qps",
            "kube_client_burst",
            "leader_qps",
            "leader_burst",
            "readiness_qps",
            "readiness_burst",
        },
        "limits",
    )
    for field, wanted in {
        "service_replicas": 2,
        "worker_replicas": 3,
        "max_active_jobs": 12,
        "worker_slots": 12,
        "max_queued_jobs": 1000,
        "controller_concurrency": 8,
        "kube_client_qps": 20,
        "kube_client_burst": 40,
        "leader_qps": 5,
        "leader_burst": 10,
        "readiness_qps": 5,
        "readiness_burst": 10,
    }.items():
        same(limits[field], wanted, field)
    for field, ceiling in (
        ("max_active_jobs_per_requester", 12),
        ("max_queued_jobs_per_requester", 1000),
    ):
        need(type(limits[field]) is int and 1 <= limits[field] <= ceiling, f"invalid {field}")
    measurement = plan["measurement"]
    exact_keys(
        measurement,
        {
            "modes",
            "levels",
            "warmup_jobs",
            "measured_jobs",
            "format",
            "platform",
            "payload_mib",
            "cache_policy",
        },
        "measurement",
    )
    for field, wanted in {
        "modes": ["cached", "uncached"],
        "levels": [8, 12],
        "warmup_jobs": 1,
        "measured_jobs": 40,
        "format": "oci",
        "platform": "linux/amd64",
        "payload_mib": 32,
        "cache_policy": "one_shared_cached_key_unique_uncached_keys",
    }.items():
        same(measurement[field], wanted, field)
    budget = exact_keys(
        plan["budget_seconds"],
        {"injection", "cleanup", "source_prepare", "warmup", "each_wave"},
        "budget",
    )
    for field, wanted in {
        "injection": 1500,
        "cleanup": 300,
        "source_prepare": 300,
        "warmup": 120,
        "each_wave": 240,
    }.items():
        same(budget[field], wanted, field)
    telemetry = exact_keys(
        plan["telemetry"], {"counter_schema_sha256", "required_counters"}, "telemetry"
    )
    match(telemetry["counter_schema_sha256"], SHA, "counter schema")
    same(telemetry["required_counters"], list(REQUIRED_COUNTERS), "required telemetry counters")
    return job_matrix(run_id, repository)


def validate_sources(document: dict, plan: dict, jobs: list[dict]) -> list[dict]:
    exact_keys(document, {"schema_version", "run_id", "jobs"}, "source manifest")
    same(document["schema_version"], 1, "source manifest schema")
    same(document["run_id"], plan["run_id"], "source manifest run")
    entries = document["jobs"]
    need(type(entries) is list and len(entries) == 41, "source manifest must have 41 entries")
    seen_digests = set()
    cached_payload = None
    uncached_payloads = set()
    for expected, entry in zip(jobs, entries):
        exact_keys(
            entry,
            {
                "mode",
                "concurrency",
                "ordinal",
                "source_tag",
                "output_tag",
                "source_uri",
                "source_digest",
                "cache_cohort",
                "payload_member",
                "payload_sha256",
            },
            "source entry",
        )
        for field in ("mode", "concurrency", "ordinal", "source_tag", "output_tag"):
            same(entry[field], expected[field], f"source {field}")
        digest = match(entry["source_digest"], DIGEST, "ZIP digest")
        uri = entry["source_uri"]
        need(
            type(uri) is str
            and uri.startswith("oci://")
            and uri.count("@") == 1
            and DIGEST.fullmatch(uri.rsplit("@", 1)[1]) is not None
            and "?" not in uri
            and "#" not in uri,
            "source URI is not immutable OCI",
        )
        need(
            uri.rsplit("@", 1)[0] == "oci://" + entry["source_tag"].rsplit(":", 1)[0],
            "source URI repository differs",
        )
        need(uri.rsplit("@", 1)[1] != digest, "manifest digest incorrectly equals ZIP digest")
        match(entry["cache_cohort"], re.compile(r"[a-z0-9-]{1,64}\Z"), "cache cohort")
        expected_cohort = (
            "shared-cached"
            if entry["mode"] in ("warmup", "cached")
            else f"uncached-{entry['concurrency']}-{entry['ordinal']}"
        )
        same(entry["cache_cohort"], expected_cohort, "cache cohort")
        member = entry["payload_member"]
        need(
            type(member) is str
            and 0 < len(member) <= 256
            and not member.startswith("/")
            and ".." not in Path(member).parts,
            "invalid payload member",
        )
        payload_sha = match(entry["payload_sha256"], SHA, "payload SHA")
        if entry["mode"] in ("warmup", "cached"):
            cached_payload = payload_sha if cached_payload is None else cached_payload
            same(payload_sha, cached_payload, "shared cached payload SHA")
        else:
            need(payload_sha not in uncached_payloads, "uncached payload SHA is reused")
            uncached_payloads.add(payload_sha)
        seen_digests.add(uri.rsplit("@", 1)[1])
    need(len(seen_digests) == 41, "source manifests are not all distinct immutable tags")
    need(cached_payload not in uncached_payloads, "cold payload matches warm payload")
    return entries


def validate_runtime(runtime: dict, plan: dict, plan_sha: str, sources_sha: str) -> None:
    exact_keys(
        runtime,
        {
            "schema_version",
            "run_id",
            "plan_sha256",
            "sources_sha256",
            "service_deployment_name",
            "worker_deployment_name",
            "worker_service_name",
            "worker_service_port",
            "worker_configmap_name",
            "service_pods",
            "worker_pods",
            "registry_container_name",
            "registry_volume_name",
            "registry_network_name",
            "registry_url",
            "requester",
            "requester_uid",
            "service_port",
            "metrics_port",
            "genesis",
        },
        "runtime",
    )
    same(runtime["schema_version"], 2, "runtime schema")
    same(runtime["run_id"], plan["run_id"], "runtime run")
    same(runtime["plan_sha256"], plan_sha, "runtime plan SHA")
    same(runtime["sources_sha256"], sources_sha, "runtime source SHA")
    fixture = plan["fixture"]
    for kind in ("service", "worker"):
        match(runtime[f"{kind}_deployment_name"], DNS, f"{kind} deployment name")
        pods = runtime[f"{kind}_pods"]
        need(
            type(pods) is list and len(pods) == (2 if kind == "service" else 3),
            f"{kind} Pod identities missing",
        )
        for pod in pods:
            exact_keys(pod, {"name", "uid"}, f"{kind} Pod")
            match(pod["name"], DNS, f"{kind} Pod name")
            match(pod["uid"], UID, f"{kind} Pod UID")
        need(
            sorted(pod["uid"] for pod in pods) == sorted(fixture[f"{kind}_pod_uids"]),
            f"{kind} Pod UIDs differ from plan",
        )
        need(len({pod["name"] for pod in pods}) == len(pods), f"{kind} Pod names overlap")
    match(runtime["worker_service_name"], DNS, "worker Service name")
    match(runtime["worker_configmap_name"], DNS, "worker ConfigMap name")
    need(
        type(runtime["worker_service_port"]) is int
        and 1 <= runtime["worker_service_port"] <= 65535,
        "worker Service port is invalid",
    )
    for field in ("registry_container_name", "registry_volume_name", "registry_network_name"):
        match(runtime[field], DOCKER_NAME, field)
    for field in ("registry_container_name", "registry_volume_name"):
        need(runtime[field].endswith("-" + plan["run_id"]), f"{field} is not run-scoped")
    need(
        fixture["registry_repository"].split("/", 1)[0]
        == runtime["registry_container_name"] + ":5000",
        "source and output repository authority differs from exact registry network endpoint",
    )
    parsed = urlsplit(runtime["registry_url"])
    need(
        parsed.scheme == "http"
        and parsed.hostname == "127.0.0.1"
        and parsed.port is not None
        and not parsed.path
        and not parsed.query
        and not parsed.fragment
        and not parsed.username,
        "registry URL must be an exact loopback HTTP authority",
    )
    need(
        type(runtime["requester"]) is str
        and 0 < len(runtime["requester"]) <= 253
        and not any(c.isspace() for c in runtime["requester"]),
        "invalid requester",
    )
    need(
        type(runtime["requester_uid"]) is str
        and len(runtime["requester_uid"]) <= 253
        and not any(c.isspace() for c in runtime["requester_uid"]),
        "invalid requester UID",
    )
    for field in ("service_port", "metrics_port"):
        need(type(runtime[field]) is int and 1 <= runtime[field] <= 65535, f"invalid {field}")
    need(runtime["service_port"] != runtime["metrics_port"], "Service and metrics ports overlap")
    genesis = exact_keys(
        runtime["genesis"],
        {
            "secret_namespace",
            "secret_name",
            "secret_uid",
            "receipt_sha256",
            "genesis_uid",
            "active_ledger_uid",
            "queue_ledger_uid",
            "worker_pool_id",
        },
        "Genesis",
    )
    same(genesis["secret_namespace"], fixture["control_namespace"], "Genesis Secret namespace")
    match(genesis["secret_name"], DNS, "Genesis Secret name")
    for field in ("secret_uid", "genesis_uid", "active_ledger_uid", "queue_ledger_uid"):
        match(genesis[field], UID, f"Genesis {field}")
    match(genesis["receipt_sha256"], SHA, "Genesis receipt digest")
    match(
        genesis["worker_pool_id"],
        re.compile(r"[A-Za-z0-9._:/@+-]{1,128}\Z"),
        "externally assigned Genesis worker pool identity",
    )
    need(
        len(
            {
                genesis[field]
                for field in ("secret_uid", "genesis_uid", "active_ledger_uid", "queue_ledger_uid")
            }
        )
        == 4,
        "Genesis object UIDs overlap",
    )


def hash_file(path: Path, maximum: int | None = None) -> str:
    info = path.lstat()
    need(
        stat.S_ISREG(info.st_mode) and (maximum is None or info.st_size <= maximum),
        "artifact is not a bounded regular file",
    )
    h = hashlib.sha256()
    with path.open("rb") as stream:
        while block := stream.read(1 << 20):
            h.update(block)
    return h.hexdigest()


def local_artifacts(args: argparse.Namespace, plan: dict) -> None:
    candidate = plan["candidate"]
    for path, expected in (
        (args.cli, candidate["cli_sha256"]),
        (args.chart, candidate["chart_sha256"]),
        (args.values, candidate["values_sha256"]),
        (args.source_template, candidate["source_template_sha256"]),
        (args.counter_schema, plan["telemetry"]["counter_schema_sha256"]),
        (args.kubeconfig, plan["fixture"]["kubeconfig_sha256"]),
    ):
        same(hash_file(path), expected, f"artifact {path.name} SHA")
    need(os.access(args.cli, os.X_OK), "pinned CLI is not executable")


def seconds_left(deadline: float, cap: float = 20) -> float:
    left = deadline - time.monotonic()
    if left <= 0:
        raise MeasuredFailure("absolute deadline expired")
    return min(cap, left)


def command(argv: list[str], deadline: float, maximum: int = MAX_JSON) -> bytes:
    stop = min(deadline, time.monotonic() + 20)
    try:
        process = subprocess.Popen(
            argv,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            start_new_session=True,
        )
    except OSError as error:
        raise UnknownOutcome(f"{Path(argv[0]).name} could not start") from error
    output = bytearray()
    try:
        assert process.stdout is not None
        while True:
            remaining = stop - time.monotonic()
            if remaining <= 0:
                raise UnknownOutcome(f"{Path(argv[0]).name} exceeded bounded command deadline")
            ready, _, _ = select.select([process.stdout], [], [], remaining)
            if not ready:
                raise UnknownOutcome(f"{Path(argv[0]).name} timed out")
            block = os.read(
                process.stdout.fileno(), min(64 << 10, max(1, maximum + 1 - len(output)))
            )
            if not block:
                break
            output.extend(block)
            if len(output) > maximum:
                raise UnknownOutcome(f"{Path(argv[0]).name} output exceeded byte limit")
        process.wait(timeout=max(0.1, stop - time.monotonic()))
        if process.returncode != 0:
            raise UnknownOutcome(f"{Path(argv[0]).name} returned non-success")
        return bytes(output)
    except (OSError, subprocess.TimeoutExpired) as error:
        raise UnknownOutcome(
            f"{Path(argv[0]).name} result unknown: {type(error).__name__}"
        ) from error
    finally:
        if process.poll() is None:
            process.kill()
            process.wait(timeout=5)
        if process.stdout is not None:
            process.stdout.close()


class Kube:
    def __init__(self, path: Path, context: str):
        self.path = path
        self.context = context

    def argv(self, *parts: str) -> list[str]:
        return [
            "kubectl",
            "--kubeconfig",
            str(self.path),
            "--context",
            self.context,
            "--request-timeout=10s",
            *parts,
        ]

    def json(self, deadline: float, *parts: str, maximum: int = MAX_JSON) -> dict:
        raw = command(self.argv(*parts), deadline, maximum)
        try:
            value = json.loads(raw, object_pairs_hook=unique_pairs)
        except (ValueError, UnicodeError) as error:
            raise UnknownOutcome("Kubernetes returned invalid JSON") from error
        if type(value) is not dict:
            raise UnknownOutcome("Kubernetes returned non-object JSON")
        return value

    def get(self, deadline: float, kind: str, name: str, namespace: str | None = None) -> dict:
        parts = ["-n", namespace] if namespace else []
        return self.json(deadline, *parts, "get", kind, name, "-o", "json")

    def list(
        self, deadline: float, kind: str, namespace: str, selector: str | None = None
    ) -> list[dict]:
        parts = ["-n", namespace, "get", kind, "-o", "json"]
        if selector:
            parts += ["-l", selector]
        result = self.json(deadline, *parts, maximum=8 << 20)
        items = result.get("items")
        metadata = result.get("metadata", {})
        if (
            type(items) is not list
            or len(items) > 1000
            or type(metadata) is not dict
            or metadata.get("continue")
            or metadata.get("remainingItemCount", 0)
        ):
            raise UnknownOutcome("Kubernetes list is malformed or unbounded")
        return items


def metadata_uid(obj: dict, wanted: str, label: str) -> None:
    if (
        obj.get("metadata", {}).get("uid") != wanted
        or obj.get("metadata", {}).get("deletionTimestamp") is not None
    ):
        raise UnknownOutcome(f"{label} UID or deletion state changed")


def ready_pod(pod: dict, name: str, uid: str, image: str) -> None:
    meta, spec, status = pod.get("metadata", {}), pod.get("spec", {}), pod.get("status", {})
    if (
        meta.get("name") != name
        or meta.get("uid") != uid
        or meta.get("deletionTimestamp") is not None
    ):
        raise UnknownOutcome("role Pod identity changed")
    if status.get("phase") != "Running" or not any(
        c.get("type") == "Ready" and c.get("status") == "True" for c in status.get("conditions", [])
    ):
        raise UnknownOutcome("role Pod is not Ready")
    containers = spec.get("containers", [])
    if len(containers) != 1 or containers[0].get("image") != image:
        raise UnknownOutcome("role Pod container image differs from frozen candidate")
    statuses = status.get("containerStatuses", [])
    if len(statuses) != 1 or statuses[0].get("restartCount") != 0:
        raise UnknownOutcome("role Pod container restarted or has unknown runtime status")


def canonical_service_dns(pod: dict) -> None:
    spec = pod.get("spec", {})
    containers = spec.get("containers", [])
    mounts = containers[0].get("volumeMounts", []) if len(containers) == 1 else []
    if (
        spec.get("dnsPolicy") != "ClusterFirst"
        or spec.get("hostNetwork", False) is not False
        or spec.get("hostAliases", []) != []
        or spec.get("dnsConfig") not in (None, {})
        or any(
            mount.get("mountPath") in ("/etc", "/etc/hosts", "/etc/resolv.conf") for mount in mounts
        )
    ):
        raise UnknownOutcome("Service Pod can bypass the canonical in-cluster DNS path")


def canonical_role_entrypoint(pod: dict, kind: str, worker_port: int) -> None:
    container = pod["spec"]["containers"][0]
    expected_name = "kova-service" if kind == "service" else "buildkitd"
    if container.get("name") != expected_name or container.get("command") not in (None, []):
        raise UnknownOutcome(f"{kind} Pod overrides the pinned image entrypoint")
    if kind == "worker" and container.get("args") != [
        "--config",
        "/etc/buildkit/buildkitd.toml",
        "--addr",
        f"tcp://0.0.0.0:{worker_port}",
        "--oci-worker-no-process-sandbox",
    ]:
        raise UnknownOutcome("worker Pod does not run the exact BuildKit listener")


def buildkit_topology_preflight(
    kube: Kube,
    plan: dict,
    runtime: dict,
    deadline: float,
    worker_selector: dict,
    pods: list[dict],
) -> None:
    """Prove the only BuildKit DNS target is the three pinned, Ready workers."""
    fixture = plan["fixture"]
    namespace = fixture["control_namespace"]
    name = runtime["worker_service_name"]
    port = runtime["worker_service_port"]
    service = kube.get(deadline, "service", name, namespace)
    metadata_uid(service, fixture["worker_service_uid"], "worker Service")
    if service.get("metadata", {}).get("namespace") != namespace:
        raise UnknownOutcome("worker Service namespace changed")
    spec = service.get("spec", {})
    service_ports = spec.get("ports", [])
    if (
        type(worker_selector) is not dict
        or not worker_selector
        or type(service_ports) is not list
        or len(service_ports) != 1
        or spec.get("type") != "ClusterIP"
        or spec.get("clusterIP") != "None"
        or spec.get("clusterIPs") != ["None"]
        or spec.get("internalTrafficPolicy") != "Cluster"
        or spec.get("ipFamilies") != ["IPv4"]
        or spec.get("ipFamilyPolicy") != "SingleStack"
        or spec.get("sessionAffinity") != "None"
        or spec.get("selector") != worker_selector
        or spec.get("publishNotReadyAddresses", False) is not False
        or spec.get("externalIPs", []) != []
        or spec.get("externalName") is not None
        or spec.get("loadBalancerIP") is not None
        or spec.get("externalTrafficPolicy") is not None
        or service.get("status", {}).get("loadBalancer", {}).get("ingress", []) != []
    ):
        raise UnknownOutcome("worker Service is not the private headless worker selector")
    service_port = service_ports[0]
    target_port_name = service_port.get("targetPort")
    if (
        service_port.get("port") != port
        or service_port.get("protocol", "TCP") != "TCP"
        or service_port.get("nodePort") is not None
        or type(service_port.get("name")) is not str
        or DNS.fullmatch(service_port["name"]) is None
        or type(target_port_name) is not str
        or DNS.fullmatch(target_port_name) is None
    ):
        raise UnknownOutcome("worker Service port is not the exact internal BuildKit port")

    pinned = {item["name"]: item["uid"] for item in runtime["worker_pods"]}
    worker_ips = {}
    for pod in pods:
        meta = pod.get("metadata", {})
        pod_name = meta.get("name")
        selected = all(meta.get("labels", {}).get(k) == v for k, v in worker_selector.items())
        if selected != (pod_name in pinned):
            raise UnknownOutcome("worker Service selector reaches a non-worker or misses a worker")
        if pod_name not in pinned:
            continue
        pod_ip = pod.get("status", {}).get("podIP")
        if type(pod_ip) is not str:
            raise UnknownOutcome("worker Pod lacks an IPv4 address")
        try:
            ipaddress.IPv4Address(pod_ip)
        except (ValueError, TypeError) as error:
            raise UnknownOutcome("worker Pod lacks an IPv4 address") from error
        pod_ips = pod.get("status", {}).get("podIPs")
        if pod_ips is not None and pod_ips != [{"ip": pod_ip}]:
            raise UnknownOutcome("worker Pod IP inventory is not single-stack")
        container_ports = pod.get("spec", {}).get("containers", [{}])[0].get("ports", [])
        if not any(
            p.get("name") == target_port_name
            and p.get("containerPort") == port
            and p.get("protocol", "TCP") == "TCP"
            for p in container_ports
        ):
            raise UnknownOutcome("worker Pod BuildKit listener port differs")
        if pod_ip in worker_ips.values():
            raise UnknownOutcome("worker Pod IPs overlap")
        worker_ips[pod_name] = pod_ip
    if set(worker_ips) != set(pinned):
        raise UnknownOutcome("pinned worker Pod set is incomplete")

    slices = kube.list(deadline, "endpointslice", namespace)
    matched = []
    for item in slices:
        meta = item.get("metadata", {})
        owners = meta.get("ownerReferences", [])
        service_label = meta.get("labels", {}).get("kubernetes.io/service-name")
        references_service = any(
            owner.get("uid") == fixture["worker_service_uid"] for owner in owners
        )
        if service_label == name or references_service:
            matched.append(item)
    if not matched:
        raise UnknownOutcome("worker Service has no EndpointSlice")
    seen = set()
    for item in matched:
        meta = item.get("metadata", {})
        owners = meta.get("ownerReferences", [])
        if (
            meta.get("namespace") != namespace
            or meta.get("deletionTimestamp") is not None
            or meta.get("labels", {}).get("kubernetes.io/service-name") != name
            or len(owners) != 1
            or owners[0].get("apiVersion") != "v1"
            or owners[0].get("kind") != "Service"
            or owners[0].get("name") != name
            or owners[0].get("uid") != fixture["worker_service_uid"]
            or owners[0].get("controller") is not True
            or item.get("addressType") != "IPv4"
            or item.get("ports")
            != [{"name": service_port["name"], "protocol": "TCP", "port": port}]
        ):
            raise UnknownOutcome("worker EndpointSlice identity or port differs")
        endpoints = item.get("endpoints", [])
        if type(endpoints) is not list:
            raise UnknownOutcome("worker EndpointSlice endpoint inventory is malformed")
        for endpoint in endpoints:
            ref = endpoint.get("targetRef", {})
            pod_name = ref.get("name")
            if (
                pod_name not in pinned
                or pod_name in seen
                or ref.get("kind") != "Pod"
                or ref.get("namespace") != namespace
                or ref.get("uid") != pinned[pod_name]
                or endpoint.get("addresses") != [worker_ips[pod_name]]
                or endpoint.get("conditions", {}).get("ready") is not True
                or endpoint.get("conditions", {}).get("terminating") is True
            ):
                raise UnknownOutcome("worker EndpointSlice reaches an unpinned or unready target")
            seen.add(pod_name)
    if seen != set(pinned):
        raise UnknownOutcome("worker EndpointSlice set misses a pinned worker Pod")


def assert_empty_ledgers(active: dict, queue: dict, limits: dict) -> None:
    try:
        a = json.loads(active["data"]["reservations.json"], object_pairs_hook=unique_pairs)
        q = json.loads(queue["data"]["queue.json"], object_pairs_hook=unique_pairs)
    except (KeyError, TypeError, ValueError) as error:
        raise UnknownOutcome("admission ledger data unreadable") from error
    if (
        a.get("active") != {}
        or q.get("intents") != {}
        or a.get("maxJobs") != limits["max_active_jobs"]
        or a.get("maxPerRequester") != limits["max_active_jobs_per_requester"]
        or a.get("workerSlots") != limits["worker_slots"]
        or q.get("globalLimit") != limits["max_queued_jobs"]
        or q.get("requesterLimit") != limits["max_queued_jobs_per_requester"]
    ):
        raise UnknownOutcome("admission ledgers are nonempty or limits drifted")


def fixture_preflight(
    kube: Kube, plan: dict, runtime: dict, deadline: float, *, empty: bool
) -> list[dict]:
    fixture, genesis = plan["fixture"], runtime["genesis"]
    if (
        socket.gethostname() != fixture["host"]
        or Path("/proc/sys/kernel/random/boot_id").read_text().strip() != fixture["host_boot_id"]
    ):
        raise UnknownOutcome("host or boot identity differs")
    same(hash_file(kube.path), fixture["kubeconfig_sha256"], "kubeconfig SHA")
    context = command(kube.argv("config", "current-context"), deadline).decode().strip()
    if context != fixture["context"]:
        raise UnknownOutcome("Kubernetes context changed")
    metadata_uid(
        kube.get(deadline, "namespace", "kube-system"),
        fixture["kube_system_namespace_uid"],
        "kube-system",
    )
    for role in ("control", "runner", "receipt"):
        namespace = kube.get(deadline, "namespace", fixture[f"{role}_namespace"])
        metadata_uid(namespace, fixture[f"{role}_namespace_uid"], f"{role} namespace")
    worker_selector = None
    for kind in ("service", "worker"):
        deployment = kube.get(
            deadline,
            "deployment",
            runtime[f"{kind}_deployment_name"],
            fixture["control_namespace"],
        )
        metadata_uid(deployment, fixture[f"{kind}_deployment_uid"], f"{kind} deployment")
        if (
            deployment.get("metadata", {}).get("generation")
            != fixture[f"{kind}_deployment_generation"]
            or deployment.get("status", {}).get("observedGeneration")
            != fixture[f"{kind}_deployment_generation"]
            or deployment.get("spec", {}).get("replicas") != len(runtime[f"{kind}_pods"])
            or deployment.get("status", {}).get("readyReplicas") != len(runtime[f"{kind}_pods"])
            or sha(canonical(deployment.get("spec", {}).get("template")))
            != fixture[f"{kind}_deployment_template_sha256"]
        ):
            raise UnknownOutcome(f"{kind} deployment generation, template, or replicas drifted")
        if kind == "worker":
            selector = deployment.get("spec", {}).get("selector", {})
            if set(selector) != {"matchLabels"}:
                raise UnknownOutcome("worker Deployment selector is not exact matchLabels")
            worker_selector = selector["matchLabels"]
    pods = []
    for kind, image in (
        ("service", plan["candidate"]["role_images"]["controller"]),
        ("worker", plan["candidate"]["role_images"]["worker"]),
    ):
        for pin in runtime[f"{kind}_pods"]:
            pod = kube.get(deadline, "pod", pin["name"], fixture["control_namespace"])
            ready_pod(pod, pin["name"], pin["uid"], image)
            canonical_role_entrypoint(pod, kind, runtime["worker_service_port"])
            pods.append(pod)
            if kind == "service":
                canonical_service_dns(pod)
                args = pod["spec"]["containers"][0].get("args", [])
                required = {
                    "--namespace": fixture["runner_namespace"],
                    "--leader-election-namespace": fixture["control_namespace"],
                    "--auth-static-principal": runtime["requester"],
                    "--auth-mode": "static",
                    "--metrics-bind-address": f"127.0.0.1:{runtime['metrics_port']}",
                    "--runner-image": plan["candidate"]["role_images"]["runner"],
                    "--worker-pool-id": genesis["worker_pool_id"],
                    "--buildkit-platform-addr": (
                        "linux/amd64=tcp://"
                        f"{runtime['worker_service_name']}.{fixture['control_namespace']}.svc:"
                        f"{runtime['worker_service_port']}"
                    ),
                    "--admission-genesis-receipt-secret-namespace": genesis["secret_namespace"],
                    "--admission-genesis-receipt-secret-name": genesis["secret_name"],
                    "--admission-genesis-receipt-secret-uid": genesis["secret_uid"],
                }
                required.update(
                    {
                        "--max-active-jobs": str(plan["limits"]["max_active_jobs"]),
                        "--max-active-jobs-per-requester": str(
                            plan["limits"]["max_active_jobs_per_requester"]
                        ),
                        "--worker-slots": str(plan["limits"]["worker_slots"]),
                        "--max-queued-jobs": str(plan["limits"]["max_queued_jobs"]),
                        "--max-queued-jobs-per-requester": str(
                            plan["limits"]["max_queued_jobs_per_requester"]
                        ),
                        "--controller-concurrency": str(plan["limits"]["controller_concurrency"]),
                        "--kube-client-qps": str(plan["limits"]["kube_client_qps"]),
                        "--kube-client-burst": str(plan["limits"]["kube_client_burst"]),
                    }
                )
                parsed = {}
                for arg in args:
                    if type(arg) is not str or not arg.startswith("--") or "=" not in arg:
                        raise UnknownOutcome("Service arg is not a canonical flag=value")
                    key, value = arg.split("=", 1)
                    if key in parsed:
                        raise UnknownOutcome("duplicate Service flag")
                    parsed[key] = value
                if any(parsed.get(key) != value for key, value in required.items()):
                    raise UnknownOutcome("Service flags differ from frozen run and Genesis limits")
                receipt_file = "/etc/kova/admission-genesis/receipt.json"
                if parsed.get("--admission-genesis-receipt-file") != receipt_file:
                    raise UnknownOutcome("Service lacks the current Genesis receipt file flag")
                secret_volumes = {
                    volume.get("name")
                    for volume in pod["spec"].get("volumes", [])
                    if volume.get("secret", {}).get("secretName") == genesis["secret_name"]
                    and volume.get("secret", {}).get("items")
                    == [{"key": "receipt.json", "path": "receipt.json"}]
                }
                mounts = pod["spec"]["containers"][0].get("volumeMounts", [])
                if not any(
                    mount.get("name") in secret_volumes
                    and mount.get("mountPath") == "/etc/kova/admission-genesis"
                    and mount.get("readOnly") is True
                    for mount in mounts
                ):
                    raise UnknownOutcome("Service receipt file is not mounted from original Secret")
                if not any(
                    port.get("containerPort") == runtime["service_port"]
                    for port in pod["spec"]["containers"][0].get("ports", [])
                ):
                    raise UnknownOutcome("Service API port changed")
    secret = kube.get(deadline, "secret", genesis["secret_name"], genesis["secret_namespace"])
    metadata_uid(secret, genesis["secret_uid"], "original Genesis receipt Secret")
    try:
        raw = base64.b64decode(secret["data"]["receipt.json"], validate=True)
        receipt = json.loads(raw, object_pairs_hook=unique_pairs)
    except (KeyError, TypeError, ValueError) as error:
        raise UnknownOutcome("Genesis receipt cannot be verified") from error
    if (
        secret.get("immutable") is not True
        or len(raw) > 16 * 1024
        or sha(raw) != genesis["receipt_sha256"]
        or receipt.get("namespace") != fixture["runner_namespace"]
        or receipt.get("genesisName") != GENESIS_NAME
        or receipt.get("genesisUID") != genesis["genesis_uid"]
        or receipt.get("contract", {}).get("namespaceUID") != fixture["runner_namespace_uid"]
        or receipt.get("contract", {}).get("version") != 3
        or receipt.get("contract", {}).get("receiptNamespace") != fixture["receipt_namespace"]
        or receipt.get("contract", {}).get("receiptNamespaceUID")
        != fixture["receipt_namespace_uid"]
        or receipt.get("contract", {}).get("workerPoolID") != genesis["worker_pool_id"]
        or receipt.get("contract", {}).get("runnerImage")
        != plan["candidate"]["role_images"]["runner"]
    ):
        raise UnknownOutcome("original Genesis receipt identity changed")
    expected_limits = {
        "maxActiveJobs": plan["limits"]["max_active_jobs"],
        "maxActiveJobsPerRequester": plan["limits"]["max_active_jobs_per_requester"],
        "workerSlots": plan["limits"]["worker_slots"],
        "maxQueuedJobs": plan["limits"]["max_queued_jobs"],
        "maxQueuedJobsPerRequester": plan["limits"]["max_queued_jobs_per_requester"],
    }
    if receipt.get("contract", {}).get("limits") != expected_limits:
        raise UnknownOutcome("original Genesis receipt limits differ from frozen plan")
    cm = kube.get(deadline, "configmap", GENESIS_NAME, fixture["runner_namespace"])
    metadata_uid(cm, genesis["genesis_uid"], "Genesis ConfigMap")
    try:
        genesis_data = json.loads(cm["data"]["genesis.json"], object_pairs_hook=unique_pairs)
    except (KeyError, TypeError, ValueError) as error:
        raise UnknownOutcome("Genesis state unreadable") from error
    if (
        cm.get("immutable") is not True
        or genesis_data.get("phase") != "Committed"
        or genesis_data.get("contract") != receipt.get("contract")
        or genesis_data.get("activeLedgerUID") != genesis["active_ledger_uid"]
        or genesis_data.get("queueLedgerUID") != genesis["queue_ledger_uid"]
    ):
        raise UnknownOutcome("Genesis is not the original committed pair")
    active = kube.get(deadline, "configmap", ACTIVE_NAME, fixture["runner_namespace"])
    queue = kube.get(deadline, "configmap", QUEUE_NAME, fixture["runner_namespace"])
    metadata_uid(active, genesis["active_ledger_uid"], "active ledger")
    metadata_uid(queue, genesis["queue_ledger_uid"], "queue ledger")
    listed_pods = kube.list(deadline, "pod", fixture["control_namespace"])
    observed_pods = {
        item.get("metadata", {}).get("name"): item.get("metadata", {}).get("uid")
        for item in listed_pods
    }
    expected_pods = {
        pin["name"]: pin["uid"] for kind in ("service", "worker") for pin in runtime[f"{kind}_pods"]
    }
    if len(observed_pods) != len(listed_pods) or observed_pods != expected_pods:
        raise UnknownOutcome("control namespace contains a foreign or replaced Pod")
    buildkit_topology_preflight(kube, plan, runtime, deadline, worker_selector, pods)
    if empty:
        assert_empty_ledgers(active, queue, plan["limits"])
        if kube.list(deadline, "kovabuild", fixture["runner_namespace"]):
            raise UnknownOutcome("runner namespace has pre-existing KovaBuilds")
        if kube.list(deadline, "pod", fixture["runner_namespace"]):
            raise UnknownOutcome("fresh runner namespace has pre-existing Pods")
    receipts = kube.list(deadline, "configmap", fixture["receipt_namespace"])
    if len(receipts) > 1 or any(
        item.get("metadata", {}).get("name") != "kube-root-ca.crt"
        or item.get("metadata", {}).get("namespace") != fixture["receipt_namespace"]
        for item in receipts
    ):
        raise UnknownOutcome("recovery-receipt namespace is not empty during normal build probe")
    return pods


def open_http(request: Request, deadline: float):
    """One exact loopback request. http.client has no redirects or proxies."""
    parsed = urlsplit(request.full_url)
    try:
        port = parsed.port
    except ValueError as error:
        raise UnknownOutcome("HTTP endpoint has an invalid port") from error
    if (
        parsed.scheme != "http"
        or parsed.hostname != "127.0.0.1"
        or port is None
        or parsed.username is not None
        or parsed.password is not None
        or parsed.fragment
    ):
        raise UnknownOutcome("HTTP endpoint escaped exact loopback fixture")
    path = parsed.path or "/"
    if parsed.query:
        path += "?" + parsed.query
    connection = http.client.HTTPConnection("127.0.0.1", port, timeout=seconds_left(deadline, 15))
    timer = None
    try:
        connection.connect()
        wire = connection.sock
        if wire is None:
            raise UnknownOutcome("HTTP connection has no exact loopback socket")

        def interrupt() -> None:
            try:
                wire.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass

        timer = threading.Timer(max(0, deadline - time.monotonic()), interrupt)
        timer.daemon = True
        timer.start()
        connection.request(
            request.get_method(),
            path,
            body=request.data,
            headers=dict(request.header_items()),
        )
        response = connection.getresponse()
        seconds_left(deadline)
        return connection, response, wire, timer
    except BaseException as error:
        if timer is not None:
            timer.cancel()
        connection.close()
        if isinstance(error, (OSError, http.client.HTTPException)):
            raise UnknownOutcome(f"HTTP outcome unknown: {type(error).__name__}") from error
        raise


def close_http(connection, response, wire, timer) -> None:
    timer.cancel()
    try:
        response.close()
    finally:
        connection.close()
        wire.close()


def read_http_chunk(
    wire: socket.socket,
    response: http.client.HTTPResponse,
    deadline: float,
    maximum: int,
) -> bytes:
    try:
        wire.settimeout(seconds_left(deadline, 15))
        block = response.read1(maximum)
        if time.monotonic() >= deadline:
            raise UnknownOutcome("absolute HTTP deadline expired")
        return block
    except (OSError, http.client.HTTPException, AttributeError) as error:
        raise UnknownOutcome(f"HTTP body outcome unknown: {type(error).__name__}") from error


def http_bytes(request: Request, deadline: float, maximum: int) -> tuple[int, bytes]:
    connection, response, wire, timer = open_http(request, deadline)
    try:
        data = bytearray()
        while True:
            block = read_http_chunk(
                wire, response, deadline, min(64 << 10, maximum + 1 - len(data))
            )
            if not block:
                break
            data.extend(block)
            if len(data) > maximum:
                raise UnknownOutcome("HTTP body exceeded bounded size")
        return response.status, bytes(data)
    finally:
        close_http(connection, response, wire, timer)


def http_json(request: Request, deadline: float) -> tuple[int, dict]:
    code, raw = http_bytes(request, deadline, MAX_JSON)
    try:
        body = json.loads(raw, object_pairs_hook=unique_pairs)
    except (ValueError, UnicodeError) as error:
        raise UnknownOutcome("HTTP returned invalid JSON") from error
    if type(body) is not dict:
        raise UnknownOutcome("HTTP returned non-object JSON")
    return code, body


def docker_one(argv: list[str], deadline: float) -> dict:
    try:
        result = json.loads(command(argv, deadline, 4 << 20), object_pairs_hook=unique_pairs)
    except (ValueError, TypeError, UnicodeError) as error:
        raise UnknownOutcome("Docker inspection JSON is unreadable") from error
    if type(result) is not list or len(result) != 1 or type(result[0]) is not dict:
        raise UnknownOutcome("Docker inspection did not return exactly one object")
    return result[0]


def check_registry_container(plan: dict, runtime: dict, deadline: float) -> None:
    fixture = plan["fixture"]
    container = docker_one(["docker", "inspect", runtime["registry_container_name"]], deadline)
    config = container.get("Config")
    state = container.get("State")
    host_config = container.get("HostConfig")
    net = container.get("NetworkSettings")
    if not all(type(part) is dict for part in (config, state, host_config, net)):
        raise UnknownOutcome("registry container inspection is malformed")
    cid = container.get("Id")
    port = urlsplit(runtime["registry_url"]).port
    binding = [{"HostIp": "127.0.0.1", "HostPort": str(port)}]
    network_name = runtime["registry_network_name"]
    networks = net.get("Networks")
    network = networks.get(network_name) if type(networks) is dict else None
    mountpoint = fixture["registry_volume_mountpoint"]
    volume_name = runtime["registry_volume_name"]
    mounts = container.get("Mounts")
    if (
        type(cid) is not str
        or SHA.fullmatch(cid) is None
        or sha(cid.encode()) != fixture["registry_instance_sha256"]
        or container.get("Name") != "/" + runtime["registry_container_name"]
        or container.get("Image") != fixture["registry_image_id"]
        or state.get("Running") is not True
        or config.get("Image") != fixture["registry_image"]
        or config.get("Entrypoint") != ["/entrypoint.sh"]
        or config.get("Cmd") != ["/etc/docker/registry/config.yml"]
        or container.get("Path") != "/entrypoint.sh"
        or container.get("Args") != ["/etc/docker/registry/config.yml"]
        or type(config.get("Labels")) is not dict
        or config["Labels"].get(OWNER_LABEL) != plan["run_id"]
        or type(config.get("Env")) is not list
        or not all(
            type(value) is str and not value.startswith("REGISTRY_") for value in config["Env"]
        )
        or host_config.get("NetworkMode") != network_name
        or type(networks) is not dict
        or set(networks) != {network_name}
        or type(network) is not dict
        or network.get("NetworkID") != fixture["registry_network_id"]
        or type(network.get("Aliases")) is not list
        or runtime["registry_container_name"] not in network["Aliases"]
        or net.get("Ports") != {"5000/tcp": binding}
        or host_config.get("PortBindings") != {"5000/tcp": binding}
        or type(mounts) is not list
        or len(mounts) != 1
        or type(mounts[0]) is not dict
        or mounts[0].get("Type") != "volume"
        or mounts[0].get("Driver") != "local"
        or mounts[0].get("Name") != volume_name
        or mounts[0].get("Source") != mountpoint
        or mounts[0].get("Destination") != "/var/lib/registry"
        or mounts[0].get("RW") is not True
    ):
        raise UnknownOutcome("registry image, launch, network, port, or storage identity differs")
    volume = docker_one(["docker", "volume", "inspect", volume_name], deadline)
    if (
        volume.get("Name") != volume_name
        or volume.get("Driver") != "local"
        or volume.get("Scope") != "local"
        or volume.get("Options") not in ({}, None)
        or volume.get("CreatedAt") != fixture["registry_volume_created_at"]
        or volume.get("Mountpoint") != mountpoint
        or type(volume.get("Labels")) is not dict
        or volume["Labels"].get(OWNER_LABEL) != plan["run_id"]
    ):
        raise UnknownOutcome("registry volume is not an exact local run-owned volume")
    ids = command(["docker", "ps", "-aq", "--no-trunc"], deadline, 64 << 10).decode().splitlines()
    if (
        not 0 < len(ids) <= 256
        or len(set(ids)) != len(ids)
        or cid not in ids
        or any(SHA.fullmatch(value) is None for value in ids)
    ):
        raise UnknownOutcome("complete Docker container inventory is unavailable")
    for other_id in ids:
        if other_id == cid:
            continue
        other = docker_one(["docker", "inspect", other_id], deadline)
        other_mounts = other.get("Mounts")
        port_bindings = other.get("HostConfig", {}).get("PortBindings")
        if (
            other.get("Id") != other_id
            or type(other_mounts) is not list
            or not all(type(item) is dict for item in other_mounts)
            or any(
                item.get("Name") == volume_name or item.get("Source") == mountpoint
                for item in other_mounts
            )
            or type(port_bindings) is not dict
            or any(
                type(item) is dict and item.get("HostPort") == str(port)
                for bindings in port_bindings.values()
                if type(bindings) is list
                for item in bindings
            )
        ):
            raise UnknownOutcome("registry storage or host port has another Docker owner")


def check_registry_catalog(plan: dict, runtime: dict, sources: list[dict], deadline: float) -> None:
    repository = plan["fixture"]["registry_repository"].split("/", 1)[1]
    code, catalog = http_json(Request(runtime["registry_url"] + "/v2/_catalog?n=1000"), deadline)
    if code != 200 or catalog.get("repositories") != [repository]:
        raise UnknownOutcome("registry catalog is not an exclusive run-scoped repository")
    url = runtime["registry_url"] + "/v2/" + quote(repository, safe="/") + "/tags/list?n=1000"
    code, tags = http_json(Request(url), deadline)
    expected = sorted(entry["source_tag"].rsplit(":", 1)[1] for entry in sources)
    listed_tags = tags.get("tags")
    if (
        code != 200
        or tags.get("name") != repository
        or type(listed_tags) is not list
        or sorted(listed_tags) != expected
    ):
        raise UnknownOutcome("registry source tags differ from the frozen 41-tag manifest")


def inspect_source_zip(stream, entry: dict, maximum: int) -> None:
    try:
        with zipfile.ZipFile(stream) as archive:
            names = archive.namelist()
            if len(names) > 1000 or len(names) != len(set(names)):
                raise UnknownOutcome("source ZIP has unbounded or repeated entries")
            infos = archive.infolist()
            if (
                any(
                    info.filename.startswith("/")
                    or ".." in Path(info.filename).parts
                    or "\\" in info.filename
                    or stat.S_ISLNK(info.external_attr >> 16)
                    or info.file_size < 0
                    for info in infos
                )
                or sum(info.file_size for info in infos) > 64 << 20
            ):
                raise UnknownOutcome("source ZIP paths or expanded size exceed bounded fixture")
            metadata = [
                name for name in names if name.count("/") == 1 and name.endswith("/metadata.json")
            ]
            if len(metadata) != 1 or entry["payload_member"] not in names:
                raise UnknownOutcome("source ZIP has no single target metadata or payload")
            info = archive.getinfo(entry["payload_member"])
            if info.file_size != maximum:
                raise UnknownOutcome("source payload does not match frozen 32 MiB size")
            with archive.open(metadata[0]) as part:
                raw = part.read(64 * 1024 + 1)
            if len(raw) > 64 * 1024:
                raise UnknownOutcome("source metadata exceeds limit")
            details = json.loads(raw, object_pairs_hook=unique_pairs)
            if (
                details.get("target") != entry["output_tag"]
                or details.get("platform") != "linux/amd64"
            ):
                raise UnknownOutcome("source ZIP embedded target/platform differs")
            digest = hashlib.sha256()
            count = 0
            with archive.open(info) as part:
                while block := part.read(1 << 20):
                    count += len(block)
                    if count > maximum:
                        raise UnknownOutcome("source payload exceeded 32 MiB")
                    digest.update(block)
            if count != maximum or digest.hexdigest() != entry["payload_sha256"]:
                raise UnknownOutcome("source payload digest differs")
    except (zipfile.BadZipFile, OSError, ValueError, KeyError) as error:
        raise UnknownOutcome("source ZIP content unreadable") from error


def verify_source_tag(entry: dict, runtime: dict, deadline: float) -> tuple[str, dict]:
    source_tag = entry["source_tag"]
    repository = source_tag.split("/", 1)[1].rsplit(":", 1)[0]
    tag = source_tag.rsplit(":", 1)[1]
    uri_digest = entry["source_uri"].rsplit("@", 1)[1]
    endpoint = runtime["registry_url"] + "/v2/" + quote(repository, safe="/")
    request = Request(
        endpoint + "/manifests/" + quote(tag),
        headers={"Accept": "application/vnd.oci.image.manifest.v1+json"},
    )
    code, raw = http_bytes(request, deadline, 1 << 20)
    if code != 200 or "sha256:" + sha(raw) != uri_digest:
        raise UnknownOutcome("source tag manifest differs from immutable URI")
    try:
        manifest = json.loads(raw, object_pairs_hook=unique_pairs)
        layers = manifest["layers"]
        if type(layers) is not list or len(layers) != 1 or type(layers[0]) is not dict:
            raise ValueError("not one ZIP layer")
        layer = layers[0]
    except (ValueError, TypeError, KeyError, IndexError) as error:
        raise UnknownOutcome("source manifest JSON unreadable") from error
    if (
        manifest.get("schemaVersion") != 2
        or manifest.get("mediaType") != "application/vnd.oci.image.manifest.v1+json"
        or len(manifest.get("layers", [])) != 1
        or layer.get("mediaType") != "application/vnd.cofy.kova.source.v1+zip"
        or layer.get("digest") != entry["source_digest"]
        or type(layer.get("size")) is not int
        or not 0 < layer["size"] <= MAX_SOURCE
    ):
        raise UnknownOutcome("source manifest layer contract differs")
    return endpoint, layer


def verify_source(entry: dict, plan: dict, runtime: dict, deadline: float) -> dict:
    endpoint, layer = verify_source_tag(entry, runtime, deadline)
    source_tag = entry["source_tag"]
    uri_digest = entry["source_uri"].rsplit("@", 1)[1]
    request = Request(endpoint + "/blobs/" + quote(entry["source_digest"], safe=":"))
    digest = hashlib.sha256()
    count = 0
    connection, response, wire, timer = open_http(request, deadline)
    try:
        with tempfile.TemporaryFile() as temp:
            if response.status != 200:
                raise UnknownOutcome("source ZIP blob request was not 200")
            while block := read_http_chunk(wire, response, deadline, 1 << 20):
                if time.monotonic() >= deadline:
                    raise UnknownOutcome("source preparation deadline expired while streaming ZIP")
                count += len(block)
                if count > layer["size"] or count > MAX_SOURCE:
                    raise UnknownOutcome("source ZIP blob exceeds descriptor")
                digest.update(block)
                temp.write(block)
            if count != layer["size"] or "sha256:" + digest.hexdigest() != entry["source_digest"]:
                raise UnknownOutcome("source ZIP bytes differ from descriptor")
            temp.seek(0)
            inspect_source_zip(temp, entry, plan["measurement"]["payload_mib"] * (1 << 20))
    except OSError as error:
        raise UnknownOutcome(f"source ZIP retrieval unknown: {type(error).__name__}") from error
    finally:
        close_http(connection, response, wire, timer)
    return {
        "source_tag": source_tag,
        "manifest_digest": uri_digest,
        "zip_digest": entry["source_digest"],
        "payload_sha256": entry["payload_sha256"],
        "zip_bytes": count,
    }


class Journal:
    def __init__(self, directory: Path, token: str):
        base = ROOT / ".work"
        need(
            base.is_dir() and not base.is_symlink(),
            "checkout .work must already exist without a symlink",
        )
        need(
            directory.is_absolute() and directory.parent == base and not directory.exists(),
            "receipt directory must be a new direct .work child",
        )
        old = os.umask(0o077)
        try:
            directory.mkdir(mode=0o700)
            self.fd = os.open(
                directory / "events.jsonl",
                os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
                0o600,
            )
        finally:
            os.umask(old)
        self.directory = directory
        self.token = token
        self.lock = threading.Lock()
        self.size = 0

    def append(self, event: str, **fields: object) -> None:
        record = {"at": now(), "event": event, **fields}
        payload = canonical(record) + b"\n"
        if self.token.encode() in payload or len(payload) > 64 * 1024:
            raise UnknownOutcome("receipt would disclose bearer or exceed event limit")
        with self.lock:
            if self.size + len(payload) > MAX_JOURNAL:
                raise UnknownOutcome("receipt journal exceeds 16 MiB")
            if os.write(self.fd, payload) != len(payload):
                raise UnknownOutcome("short receipt journal write")
            os.fsync(self.fd)
            self.size += len(payload)

    def close(self) -> None:
        os.close(self.fd)

    def save_json(self, name: str, value: object) -> str:
        need(re.fullmatch(r"[a-z0-9-]{1,80}\.json", name) is not None, "invalid receipt file name")
        payload = canonical(value) + b"\n"
        if self.token.encode() in payload or len(payload) > 1 << 20:
            raise UnknownOutcome("projection would disclose bearer or exceed 1 MiB")
        fd = os.open(
            self.directory / name,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
            0o600,
        )
        try:
            if os.write(fd, payload) != len(payload):
                raise UnknownOutcome("short projection write")
            os.fsync(fd)
        finally:
            os.close(fd)
        return sha(payload)


def process_lines(process: subprocess.Popen, deadline: float):
    """Read startup lines without a buffered readline that can outlive the deadline."""
    if process.stdout is None:
        raise UnknownOutcome("subprocess has no bounded startup output")
    pending = bytearray()
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise UnknownOutcome("subprocess exited before exact loopback binding")
        ready, _, _ = select.select([process.stdout], [], [], seconds_left(deadline, 0.5))
        if not ready:
            continue
        try:
            block = os.read(process.stdout.fileno(), 4096)
        except OSError as error:
            raise UnknownOutcome("subprocess startup output unreadable") from error
        if not block:
            raise UnknownOutcome("subprocess closed startup output")
        pending.extend(block)
        while b"\n" in pending:
            line, _, remainder = pending.partition(b"\n")
            pending = bytearray(remainder)
            if len(line) > 4096:
                raise UnknownOutcome("subprocess startup line exceeded bound")
            yield line.decode("utf-8", "replace")
        if len(pending) > 4096:
            raise UnknownOutcome("subprocess startup line exceeded bound")
    raise UnknownOutcome("subprocess startup exceeded absolute deadline")


class Forward:
    def __init__(self, kube: Kube, namespace: str, pod: str, remote_port: int, deadline: float):
        argv = kube.argv(
            "-n", namespace, "port-forward", "--address=127.0.0.1", f"pod/{pod}", f":{remote_port}"
        )
        self.process = subprocess.Popen(
            argv, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, start_new_session=True
        )
        self.pod = pod
        self.port = 0
        try:
            for line in process_lines(self.process, deadline):
                found = re.fullmatch(r"Forwarding from 127\.0\.0\.1:(\d+) -> (\d+)\s*", line)
                if found and int(found.group(2)) == remote_port:
                    self.port = int(found.group(1))
                    break
            if not self.port:
                raise UnknownOutcome("exact Pod port-forward did not bind loopback")
        except BaseException:
            self.close()
            raise

    def close(self) -> None:
        if self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=5)
        if self.process.stdout is not None:
            self.process.stdout.close()


def metric_snapshot(forwards: list[Forward], deadline: float) -> list[dict]:
    snapshots = []
    for forward in forwards:
        code, raw = http_bytes(
            Request(f"http://127.0.0.1:{forward.port}/metrics"), deadline, MAX_METRICS
        )
        if code != 200:
            raise UnknownOutcome("controller metrics endpoint unavailable")
        series = {}
        for line in raw.decode("utf-8", "replace").splitlines():
            if line.startswith("#") or not line:
                continue
            name = line.split("{", 1)[0].split(" ", 1)[0]
            if not any(name == family or name.startswith(family + "_") for family in METRIC_NAMES):
                continue
            if len(line) > 2048 or len(series) > 3000:
                raise UnknownOutcome("metric sample cardinality exceeded bound")
            # Only a fixed metric family is retained, with its complete bounded
            # sample line. The family labels are fixed by the Kova collectors.
            if any(secret in line for secret in ("namespace=", "object=", "token=", "source_uri=")):
                raise UnknownOutcome("metric labels contain forbidden identity")
            series[line] = True
        snapshots.append({"pod": forward.pod, "at": now(), "series": sorted(series)})
    return snapshots


def expected_job_id(requester: str, key: str) -> str:
    return "idem-" + sha((requester + "\x00" + key).encode())[:20]


def check_build(
    build: dict,
    entry: dict,
    runtime: dict,
    plan: dict,
    uid: str | None = None,
    *,
    allow_deleting: bool = False,
) -> str:
    metadata, spec = build.get("metadata", {}), build.get("spec", {})
    expected = expected_job_id(runtime["requester"], entry["key"])
    actual_uid = metadata.get("uid")
    if (
        metadata.get("name") != expected
        or metadata.get("namespace") != plan["fixture"]["runner_namespace"]
        or type(actual_uid) is not str
        or UID.fullmatch(actual_uid) is None
        or (uid is not None and uid != actual_uid)
        or (metadata.get("deletionTimestamp") is not None and not allow_deleting)
        or spec.get("source") != {"uri": entry["source_uri"], "digest": entry["source_digest"]}
        or spec.get("idempotencyKey") != entry["key"]
        or spec.get("requester", {}).get("username") != runtime["requester"]
        or spec.get("requester", {}).get("uid", "") != runtime["requester_uid"]
        or spec.get("targets") != [{"target": entry["output_tag"], "platform": "linux/amd64"}]
    ):
        raise UnknownOutcome("returned KovaBuild UID/requester/source/target identity differs")
    options = spec.get("build", {})
    if (
        options.get("format") != "oci"
        or options.get("concurrency") != 1
        or options.get("timeout") != 300
    ):
        raise UnknownOutcome("returned KovaBuild build options differ")
    return actual_uid


def public_job(body: dict, entry: dict, runtime: dict, *, terminal: bool = False) -> None:
    if (
        body.get("id") != expected_job_id(runtime["requester"], entry["key"])
        or body.get("source_uri") != entry["source_uri"]
        or body.get("source_digest") != entry["source_digest"]
        or body.get("idempotency_key") != entry["key"]
        or body.get("requester") != runtime["requester"]
    ):
        raise UnknownOutcome("public job identity differs")
    if terminal and body.get("status") != "succeeded":
        raise MeasuredFailure("identified build did not succeed")


def parse_time(value: object) -> datetime:
    if type(value) is not str or len(value) > 64:
        raise UnknownOutcome("public timestamp missing or malformed")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise UnknownOutcome("public timestamp is invalid") from error
    if parsed.tzinfo is None:
        raise UnknownOutcome("public timestamp is timezone-naive")
    return parsed


def public_phase_ms(body: dict) -> dict:
    created = parse_time(body.get("created_at"))
    started = parse_time(body.get("started_at"))
    finished = parse_time(body.get("finished_at"))
    if not created <= started <= finished:
        raise UnknownOutcome("public job phase timestamps are backwards")
    return {
        "queue_ms": round((started - created).total_seconds() * 1000, 3),
        "active_ms": round((finished - started).total_seconds() * 1000, 3),
    }


def nearest_rank(values: list[float], fraction: float) -> float:
    need(values, "empty latency sample")
    return sorted(values)[math.ceil(len(values) * fraction) - 1]


def build_observation(raw: object) -> dict:
    """Copy the finite internal v1 projection, never arbitrary status fields."""
    unavailable = {"availability": "unavailable", "reason": "invalid_observation"}
    if raw is None:
        return {"availability": "unavailable", "reason": "missing_stream"}
    if type(raw) is not dict or len(canonical(raw)) > 4096:
        return unavailable
    scalar_keys = {
        "schemaVersion",
        "availability",
        "reason",
        "vertexCount",
        "vertexIntervalCount",
        "cachedVertexIntervalCount",
        "exportAvailability",
        "pushAvailability",
        "nydusAvailability",
        "workerSessionsAvailability",
    }
    duration_keys = {
        "vertexUnionNanoseconds",
        "exportUnionNanoseconds",
        "pushUnionNanoseconds",
        "exportPushOverlapNanoseconds",
        "nydusWallNanoseconds",
    }
    if set(raw) - scalar_keys - duration_keys:
        return unavailable
    if (
        type(raw.get("schemaVersion")) is not int
        or raw["schemaVersion"] != 1
        or raw.get("workerSessionsAvailability") != "unavailable"
    ):
        return unavailable
    availability, reason = raw.get("availability"), raw.get("reason", "")
    if (
        availability not in ("observed", "incomplete", "unavailable")
        or reason
        not in {
            "",
            "missing_stream",
            "malformed_stream",
            "limit_exceeded",
            "partial_stream",
            "command_failed",
            "cancelled",
            "invalid_observation",
        }
        or ((availability == "observed") != (reason == ""))
    ):
        return unavailable
    counts = [
        raw.get(key, 0)
        for key in ("vertexCount", "vertexIntervalCount", "cachedVertexIntervalCount")
    ]
    if (
        any(type(value) is not int or value < 0 for value in counts)
        or counts[0] > 4096
        or counts[1] > 16384
        or counts[2] > counts[1]
    ):
        return unavailable
    if any(
        type(raw[key]) is not int or not 0 <= raw[key] <= 86400000000000
        for key in duration_keys & set(raw)
    ):
        return unavailable
    for key, duration in (
        ("exportAvailability", "exportUnionNanoseconds"),
        ("pushAvailability", "pushUnionNanoseconds"),
        ("nydusAvailability", "nydusWallNanoseconds"),
    ):
        state = raw.get(key)
        if state not in ("observed", "unavailable") or ((state == "observed") != (duration in raw)):
            return unavailable
    overlap = raw.get("exportPushOverlapNanoseconds")
    if (overlap is not None) != ("exportUnionNanoseconds" in raw and "pushUnionNanoseconds" in raw):
        return unavailable
    if overlap is not None and overlap > min(
        raw["exportUnionNanoseconds"], raw["pushUnionNanoseconds"]
    ):
        return unavailable
    if availability == "observed" and (
        not counts[0] or not counts[1] or "vertexUnionNanoseconds" not in raw
    ):
        return unavailable
    if availability == "unavailable" and (
        any(counts) or (duration_keys - {"nydusWallNanoseconds"}) & set(raw)
    ):
        return unavailable
    return dict(raw)


def output_observation(build: dict, entry: dict, digest: str) -> dict:
    """Join diagnostics to this exact already-qualified CR and pushed digest."""
    results = build.get("status", {}).get("verificationResults", [])
    if type(results) is not list or len(results) != 1:
        raise UnknownOutcome("terminal build does not have its exact verification receipt")
    receipt = results[0]
    if (
        receipt.get("image") != entry["output_tag"]
        or receipt.get("format") != "oci"
        or receipt.get("platform") != "linux/amd64"
        or receipt.get("state") != "succeeded"
        or receipt.get("pushedDigest") != digest
    ):
        raise UnknownOutcome("internal verification receipt differs from public immutable output")
    return build_observation(receipt.get("buildObservation"))


def phase_observation_complete(value: dict) -> bool:
    return (
        value.get("availability") == "observed"
        and value.get("exportAvailability") == "observed"
        and value.get("pushAvailability") == "observed"
        and all(
            key in value
            for key in (
                "vertexUnionNanoseconds",
                "exportUnionNanoseconds",
                "pushUnionNanoseconds",
                "exportPushOverlapNanoseconds",
            )
        )
    )


def history_timestamp_ns(raw: object) -> int:
    """buildctl's json template uses encoding/json on timestamppb.Timestamp."""
    if type(raw) is not dict or set(raw) - {"seconds", "nanos"}:
        raise UnknownOutcome("BuildKit history timestamp is malformed")
    seconds, nanos = raw.get("seconds", 0), raw.get("nanos", 0)
    if (
        type(seconds) is not int
        or not 946684800 <= seconds <= 7258118400
        or type(nanos) is not int
        or not 0 <= nanos < 1000000000
    ):
        raise UnknownOutcome("BuildKit history timestamp is out of bounds")
    return seconds * 1000000000 + nanos


def project_history(raw: bytes, targets: set[str]) -> list[dict]:
    """Read worker-side Solve Ref records; do not invent transport Session IDs."""
    if len(raw) > 1 << 20 or len(raw.splitlines()) > 82:
        raise UnknownOutcome("BuildKit history snapshot exceeded its finite budget")
    rows = {}
    for line in raw.splitlines():
        if not line.strip():
            continue
        if len(line) > 64 << 10:
            raise UnknownOutcome("BuildKit history record exceeded 64 KiB")
        try:
            event = json.loads(line, object_pairs_hook=unique_pairs)
        except (ValueError, UnicodeError) as error:
            raise UnknownOutcome("BuildKit history returned invalid JSON") from error
        record = event.get("record") if type(event) is dict else None
        kind = event.get("type", 0) if type(event) is dict else None
        if type(record) is not dict or type(kind) is not int or kind not in (0, 1):
            raise UnknownOutcome("BuildKit history record is not STARTED/COMPLETE")
        ref = record.get("Ref")
        exporters = record.get("Exporters")
        if (
            type(ref) is not str
            or re.fullmatch(r"[A-Za-z0-9._-]{1,128}", ref) is None
            or type(exporters) is not list
            or len(exporters) != 1
            or type(exporters[0]) is not dict
            or exporters[0].get("Type") != "image"
            or type(exporters[0].get("Attrs")) is not dict
        ):
            raise UnknownOutcome("worker history is not an owned finite image solve")
        target = exporters[0].get("Attrs", {}).get("name")
        if type(target) is not str or target not in targets:
            raise UnknownOutcome("worker history contains an unowned output target")
        created = history_timestamp_ns(record.get("CreatedAt"))
        completed = history_timestamp_ns(record["CompletedAt"]) if "CompletedAt" in record else None
        if (kind == 1) != (completed is not None) or (
            completed is not None and (completed < created or completed - created > 86400000000000)
        ):
            raise UnknownOutcome("worker history lifetime is malformed")
        if record.get("error"):
            raise MeasuredFailure("owned worker solve recorded a failed completion")
        response = record.get("ExporterResponse", {})
        if type(response) is not dict:
            raise UnknownOutcome("worker exporter response is malformed")
        digest = response.get("containerimage.digest")
        if completed is not None and (type(digest) is not str or DIGEST.fullmatch(digest) is None):
            raise UnknownOutcome("completed worker solve has no pushed manifest digest")
        row = {
            "solve_ref": ref,
            "target": target,
            "created_ns": created,
            "completed_ns": completed,
            "pushed_digest": digest,
        }
        prior = rows.get(ref)
        if prior is not None:
            # EarlyExit takes active and durable snapshots separately. A solve
            # may legally complete between them, but may not change identity.
            if prior["target"] != target or prior["created_ns"] != created:
                raise UnknownOutcome("worker solve Ref changed identity")
            if prior["completed_ns"] is not None and row != prior:
                raise UnknownOutcome("worker history completion changed")
        if prior is None or completed is not None:
            rows[ref] = row
    if len(rows) > 41:
        raise UnknownOutcome("worker history contains more than 41 owned solves")
    return sorted(rows.values(), key=lambda item: item["solve_ref"])


def peak_solve_occupancy(rows: list[dict]) -> int | None:
    """Each worker's own clock only; half-open intervals, never CPU occupancy."""
    if any(row["completed_ns"] is None for row in rows):
        return None
    edges = [(row["created_ns"], 1) for row in rows if row["created_ns"] != row["completed_ns"]]
    edges += [(row["completed_ns"], -1) for row in rows if row["created_ns"] != row["completed_ns"]]
    active = peak = 0
    for _, delta in sorted(edges):
        active += delta
        peak = max(peak, active)
    return peak


def check_history_retention(config: str) -> None:
    """One fixed retention section fits all 41 solves within the 30m budget."""
    if type(config) is not str or len(config.encode()) > 64 << 10:
        raise UnknownOutcome("BuildKit configuration is missing or oversized")
    lines = [line.split("#", 1)[0].strip() for line in config.splitlines()]
    if sum(line == "[history]" for line in lines) != 1 or any(
        line.startswith("history.") or line.startswith("[[history") for line in lines
    ):
        raise UnknownOutcome("BuildKit history retention is not explicitly canonical")
    section = []
    for line in lines[lines.index("[history]") + 1 :]:
        if line.startswith("["):
            break
        if line:
            section.append(re.sub(r"\s+", "", line))
    if len(section) != 2 or set(section) != {'maxAge="1h"', "maxEntries=64"}:
        raise UnknownOutcome("BuildKit history must retain 64 records for one hour")


class WorkerSolveObserver:
    """Finite, read-only worker histories on exact Pod/container incarnations."""

    def __init__(self, kube: Kube, plan: dict, runtime: dict, journal: Journal):
        self.kube, self.plan, self.runtime, self.journal = kube, plan, runtime, journal
        self.targets = {
            job["output_tag"]
            for job in job_matrix(plan["run_id"], plan["fixture"]["registry_repository"])
        }
        self.identities: dict[str, dict] = {}
        self.records: dict[tuple[str, str], dict] = {}

    def pod_identity(self, pin: dict, deadline: float) -> dict:
        pod = self.kube.get(deadline, "pod", pin["name"], self.plan["fixture"]["control_namespace"])
        ready_pod(pod, pin["name"], pin["uid"], self.plan["candidate"]["role_images"]["worker"])
        canonical_role_entrypoint(pod, "worker", self.runtime["worker_service_port"])
        volumes = {item.get("name"): item for item in pod["spec"].get("volumes", [])}
        mounts = pod["spec"]["containers"][0].get("volumeMounts", [])
        if not any(
            item.get("mountPath") == "/etc/buildkit"
            and not item.get("subPath")
            and not item.get("subPathExpr")
            and volumes.get(item.get("name"), {}).get("configMap", {}).get("name")
            == self.runtime["worker_configmap_name"]
            for item in mounts
        ):
            raise UnknownOutcome("worker does not mount the exact history configuration")
        status = pod["status"]["containerStatuses"][0]
        container_id, image_id = status.get("containerID"), status.get("imageID")
        started = status.get("state", {}).get("running", {}).get("startedAt")
        if (
            status.get("name") != "buildkitd"
            or type(container_id) is not str
            or not container_id
            or len(container_id) > 256
            or type(image_id) is not str
            or not image_id
            or len(image_id) > 512
            or type(started) is not str
        ):
            raise UnknownOutcome("worker container incarnation is unavailable")
        parse_time(started)
        return {
            "pod": pin["name"],
            "pod_uid": pin["uid"],
            "container_id": container_id,
            "image_id": image_id,
            "container_started_at": started,
        }

    def read(self, pin: dict, operation: str, deadline: float) -> bytes:
        if operation not in ("workers", "histories"):
            raise UnknownOutcome("worker observer permits only read-only debug operations")
        before = self.pod_identity(pin, deadline)
        original = self.identities.get(pin["uid"])
        if original is not None and any(original[key] != value for key, value in before.items()):
            raise UnknownOutcome("worker container incarnation changed during measurement")
        # This command does not call Solve, Session, UpdateBuildHistory, or Prune.
        raw = command(
            self.kube.argv(
                "-n",
                self.plan["fixture"]["control_namespace"],
                "exec",
                "pod/" + pin["name"],
                "-c",
                "buildkitd",
                "--",
                "/usr/bin/buildctl",
                "--addr",
                f"tcp://127.0.0.1:{self.runtime['worker_service_port']}",
                "debug",
                operation,
                "--format",
                "{{json .}}",
            ),
            deadline,
            1 << 20,
        )
        if self.pod_identity(pin, deadline) != before:
            raise UnknownOutcome("worker changed during server-side history read")
        return raw

    def sample(self, deadline: float, *, baseline: bool = False) -> dict:
        deadline = min(deadline, time.monotonic() + 18)
        configmap = self.kube.get(
            deadline,
            "configmap",
            self.runtime["worker_configmap_name"],
            self.plan["fixture"]["control_namespace"],
        )
        metadata_uid(
            configmap, self.plan["fixture"]["worker_configmap_uid"], "worker configuration"
        )
        config = configmap.get("data", {}).get("buildkitd.toml")
        check_history_retention(config)
        if sha(config.encode()) != self.plan["fixture"]["worker_config_sha256"]:
            raise UnknownOutcome("worker history configuration changed")
        sampled = []
        for pin in self.runtime["worker_pods"]:
            raw_workers = self.read(pin, "workers", deadline)
            try:
                workers = json.loads(raw_workers, object_pairs_hook=unique_pairs)
            except (ValueError, UnicodeError) as error:
                raise UnknownOutcome("worker identity response is malformed") from error
            if type(workers) is not list or len(workers) != 1:
                raise UnknownOutcome("daemon does not expose exactly one worker")
            worker = workers[0]
            worker_id = worker.get("ID")
            version = worker.get("BuildkitVersion", {})
            if (
                type(worker_id) is not str
                or re.fullmatch(r"[A-Za-z0-9._-]{1,128}", worker_id) is None
                or version.get("package") != "github.com/moby/buildkit"
                or version.get("version") != "v0.31.2"
                or version.get("revision") != BUILDKIT_REVISION
            ):
                raise UnknownOutcome("worker daemon is not the pinned BuildKit version")
            identity = {
                **self.pod_identity(pin, deadline),
                "worker_id": worker_id,
                "buildkit_revision": version["revision"],
            }
            original = self.identities.get(pin["uid"])
            if original is not None and identity != original:
                raise UnknownOutcome("actual worker ID/version/incarnation changed")
            self.identities[pin["uid"]] = identity
            rows = project_history(self.read(pin, "histories", deadline), self.targets)
            if baseline and rows:
                raise UnknownOutcome("fresh worker history is not empty before first submission")
            for row in rows:
                key = (pin["uid"], row["solve_ref"])
                prior = self.records.get(key)
                if prior is not None and (
                    prior["target"] != row["target"]
                    or prior["created_ns"] != row["created_ns"]
                    or (prior["completed_ns"] is not None and prior != row)
                ):
                    raise UnknownOutcome("owned server-side solve lifetime changed")
                self.records[key] = row
            sampled.append(
                {
                    **identity,
                    "active_solves": sum(row["completed_ns"] is None for row in rows),
                    "records": rows,
                }
            )
        if len({value["worker_id"] for value in self.identities.values()}) != 3:
            raise UnknownOutcome("worker daemon identities overlap")
        if len(self.records) > 41:
            raise UnknownOutcome("more than 41 server-side solves were observed")
        sample = {
            "workers": sampled,
            "semantics": "non-internal server-side solve histories",
            "transport_session_ids": "unavailable",
        }
        self.journal.append("worker_solve_sample", **sample)
        return sample

    def summary(self, known: dict[str, dict]) -> dict:
        by_target: dict[str, list[tuple[str, dict]]] = {}
        for (uid, _), row in self.records.items():
            by_target.setdefault(row["target"], []).append((uid, row))
        joins, missing = [], []
        for job_id, job in known.items():
            records = by_target.get(job["entry"]["output_tag"], [])
            if len(records) != 1 or records[0][1]["completed_ns"] is None:
                missing.append(job_id)
                continue
            uid, row = records[0]
            if row["pushed_digest"] != job.get("output_manifest_digest"):
                raise UnknownOutcome("worker solve digest differs from exact build output")
            joins.append({"job_id": job_id, "build_uid": job["uid"], **self.identities[uid], **row})
        extras = sorted(set(by_target) - {job["entry"]["output_tag"] for job in known.values()})
        complete = (
            len(known) == 41
            and len(joins) == 41
            and not missing
            and not extras
            and set(self.identities) == {pin["uid"] for pin in self.runtime["worker_pods"]}
        )
        workers = []
        for uid, identity in self.identities.items():
            rows = [row for (owner, _), row in self.records.items() if owner == uid]
            workers.append(
                {
                    **identity,
                    "solves": len(rows),
                    "peak_solve_occupancy": peak_solve_occupancy(rows),
                }
            )
        result = {
            "complete_for_attribution": complete,
            "joined_builds": joins,
            "missing_or_ambiguous_builds": missing,
            "unjoined_targets": extras,
            "workers": workers,
            "clock_scope": "per-worker daemon; no cross-clock aggregate",
            "transport_session_ids": "unavailable",
            "pure_cpu_execution_time": "unavailable",
        }
        result["sha256"] = self.journal.save_json("worker-solve-attribution.json", result)
        self.journal.append(
            "worker_solve_attribution",
            **{key: value for key, value in result.items() if key != "joined_builds"},
        )
        return result


def sample_node_stats(kube: Kube, deadline: float, node_name: str, worker_uids: set[str]) -> dict:
    raw = command(
        kube.argv("get", "--raw", f"/api/v1/nodes/{node_name}/proxy/stats/summary"),
        deadline,
        8 << 20,
    )
    try:
        value = json.loads(raw)
        pods = value["pods"]
        node = value["node"]
    except (ValueError, TypeError, KeyError) as error:
        raise UnknownOutcome("kubelet node summary unavailable") from error
    results = {}
    for pod in pods:
        uid = pod.get("podRef", {}).get("uid")
        if uid in worker_uids:
            cpu = pod.get("cpu", {}).get("usageNanoCores")
            memory = pod.get("memory", {}).get("workingSetBytes")
            if type(cpu) is not int or type(memory) is not int or cpu < 0 or memory < 0:
                raise UnknownOutcome("worker Pod kubelet CPU/memory observation missing")
            results[uid] = {"cpu_nanocores": cpu, "memory_working_set_bytes": memory}
    node_cpu = node.get("cpu", {}).get("usageNanoCores")
    available = node.get("memory", {}).get("availableBytes")
    if type(node_cpu) is not int or type(available) is not int or node_cpu < 0 or available < 0:
        raise UnknownOutcome("node kubelet CPU/memory observation missing")
    return {
        "name": node_name,
        "cpu_nanocores": node_cpu,
        "memory_available_bytes": available,
        "workers": results,
    }


def sample_resources(kube: Kube, plan: dict, runtime: dict, deadline: float) -> dict:
    control_namespace = plan["fixture"]["control_namespace"]
    runner_namespace = plan["fixture"]["runner_namespace"]
    role_pods = kube.list(deadline, "pod", control_namespace)
    pods = kube.list(deadline, "pod", runner_namespace)
    builds = kube.list(deadline, "kovabuild", runner_namespace)
    allowed_ids = {
        expected_job_id(runtime["requester"], job["key"])
        for job in job_matrix(plan["run_id"], plan["fixture"]["registry_repository"])
    }
    by_build = {
        item.get("metadata", {}).get("name"): item.get("metadata", {}).get("uid") for item in builds
    }
    if len(by_build) != len(builds) or not set(by_build) <= allowed_ids:
        raise UnknownOutcome("resource sampler saw a foreign or repeated KovaBuild")
    by_name = {item.get("metadata", {}).get("name"): item for item in role_pods}
    if len(by_name) != len(role_pods):
        raise UnknownOutcome("control Pod listing contains duplicate names")
    workers, runners = [], []
    for kind, image in (
        ("service", plan["candidate"]["role_images"]["controller"]),
        ("worker", plan["candidate"]["role_images"]["worker"]),
    ):
        for pin in runtime[f"{kind}_pods"]:
            pod = by_name.get(pin["name"])
            if pod is None:
                raise UnknownOutcome("frozen role Pod disappeared")
            ready_pod(pod, pin["name"], pin["uid"], image)
            if kind == "worker":
                workers.append(
                    {
                        "name": pin["name"],
                        "uid": pin["uid"],
                        "node": pod["spec"].get("nodeName"),
                        "requests": pod["spec"]["containers"][0]
                        .get("resources", {})
                        .get("requests", {}),
                    }
                )
    role_names = {pin["name"] for kind in ("service", "worker") for pin in runtime[f"{kind}_pods"]}
    if set(by_name) != role_names:
        raise UnknownOutcome("foreign or replaced control Pod appeared")
    for pod in pods:
        meta = pod.get("metadata", {})
        if pod.get("metadata", {}).get("labels", {}).get("app.kubernetes.io/name") != "kova-runner":
            raise UnknownOutcome("foreign Pod appeared in fresh runner namespace")
        name = meta.get("name")
        build_id = name.removeprefix("kova-job-") if type(name) is str else ""
        owners = meta.get("ownerReferences", [])
        if (
            name != "kova-job-" + build_id
            or build_id not in by_build
            or type(owners) is not list
            or len(owners) != 1
            or owners[0].get("name") != build_id
            or owners[0].get("uid") != by_build[build_id]
            or owners[0].get("kind") != "KovaBuild"
        ):
            raise UnknownOutcome("runner Pod is not owned by an exact campaign build")
        status = pod.get("status", {})
        conditions = {
            condition.get("type"): condition.get("lastTransitionTime")
            for condition in status.get("conditions", [])
            if condition.get("status") == "True"
        }
        fetch = next(
            (
                c.get("state", {}).get("terminated", {})
                for c in status.get("initContainerStatuses", [])
                if c.get("name") == "source-fetch"
            ),
            {},
        )
        runners.append(
            {
                "name": meta.get("name"),
                "uid": meta.get("uid"),
                "owner_uids": [o.get("uid") for o in meta.get("ownerReferences", [])],
                "node": pod.get("spec", {}).get("nodeName"),
                "phase": status.get("phase"),
                "created_at": meta.get("creationTimestamp"),
                "scheduled_at": conditions.get("PodScheduled"),
                "ready_at": conditions.get("Ready"),
                "source_start_at": fetch.get("startedAt"),
                "source_end_at": fetch.get("finishedAt"),
                "requests": [
                    {"name": c.get("name"), "resources": c.get("resources", {}).get("requests", {})}
                    for c in pod.get("spec", {}).get("containers", [])
                ],
            }
        )
    node_set = {worker["node"] for worker in workers}
    if None in node_set or len(node_set) > 3:
        raise UnknownOutcome("worker node placement unavailable")
    nodes = sorted(node_set)
    stats = [sample_node_stats(kube, deadline, node, {w["uid"] for w in workers}) for node in nodes]
    seen = {uid for node in stats for uid in node["workers"]}
    if seen != {worker["uid"] for worker in workers}:
        raise UnknownOutcome("kubelet summary missed an exact worker Pod")
    return {"at": now(), "workers": workers, "runners": runners, "node_stats": stats}


class Sampler:
    def __init__(
        self,
        kube: Kube,
        plan: dict,
        runtime: dict,
        journal: Journal,
        deadline: float,
        worker_observer: WorkerSolveObserver | None = None,
    ):
        self.kube, self.plan, self.runtime, self.journal, self.deadline = (
            kube,
            plan,
            runtime,
            journal,
            deadline,
        )
        self.stop = threading.Event()
        self.worker_observer = worker_observer
        self.failure: str | None = None
        self.thread = threading.Thread(target=self._run, daemon=True)

    def start(self) -> None:
        self.thread.start()

    def _run(self) -> None:
        while not self.stop.is_set() and time.monotonic() < self.deadline:
            try:
                sample = sample_resources(self.kube, self.plan, self.runtime, self.deadline)
                self.journal.append("resource_sample", **sample)
                if self.worker_observer is not None:
                    self.worker_observer.sample(self.deadline)
            except Exception as error:
                # A malformed response must not silently kill this background
                # thread and leave the foreground campaign apparently healthy.
                self.failure = type(error).__name__
                self.stop.set()
                return
            self.stop.wait(10)

    def check(self) -> None:
        if self.failure:
            raise UnknownOutcome("bounded resource sampler produced UNKNOWN")

    def close(self) -> None:
        self.stop.set()
        self.thread.join(timeout=21)
        if self.thread.is_alive():
            raise UnknownOutcome("bounded resource sampler did not stop")
        self.check()


def snapshot_metrics(
    journal: Journal, forwards: list[Forward], label: str, deadline: float
) -> list[dict]:
    samples = metric_snapshot(forwards, deadline)
    summary = []
    for sample in samples:
        name = f"metrics-{label}-{sample['pod']}.json"
        digest = journal.save_json(name, sample)
        summary.append(
            {"pod": sample["pod"], "sha256": digest, "series": len(sample["series"]), "file": name}
        )
    journal.append("metrics_snapshot", label=label, pods=summary)
    return samples


def metric_deltas(before: list[dict], after: list[dict]) -> dict:
    if [item["pod"] for item in before] != [item["pod"] for item in after]:
        raise UnknownOutcome("metric Pod identity changed")
    deltas = {}
    missing = []
    for older, newer in zip(before, after):

        def parse(item: dict) -> dict[str, float]:
            result = {}
            for line in item["series"]:
                try:
                    name, raw = line.rsplit(" ", 1)
                    value = float(raw)
                except (ValueError, TypeError) as error:
                    raise UnknownOutcome("bounded Prometheus sample is not numeric") from error
                if not math.isfinite(value) or value < 0:
                    raise UnknownOutcome("bounded Prometheus counter is nonfinite/negative")
                result[name] = value
            return result

        old, new = parse(older), parse(newer)
        for name, value in new.items():
            previous = old.get(name, 0.0)
            if value < previous:
                raise UnknownOutcome("metric counter reset during wave")
            deltas[f"{newer['pod']}::{name}"] = round(value - previous, 6)
        for family in (
            "kova_service_kube_wire_requests_total",
            "kova_service_kube_limiter_wait_seconds_count",
            "kova_service_kube_wire_round_trip_seconds_count",
        ):
            if not any(name == family or name.startswith(family + "{") for name in new):
                missing.append({"pod": newer["pod"], "family": family})
    return {
        "nonzero": {key: value for key, value in deltas.items() if value > 0},
        "missing": missing,
    }


def post_one(
    entry: dict,
    index: int,
    barrier: threading.Barrier,
    api: list[Forward],
    token: str,
    kube: Kube,
    plan: dict,
    runtime: dict,
    journal: Journal,
    deadline: float,
) -> dict:
    key = entry["key"]
    expected_id = expected_job_id(runtime["requester"], key)
    barrier.wait(timeout=seconds_left(deadline, 20))
    if seconds_left(deadline, 20) < 20:
        raise MeasuredFailure("wave has insufficient time for a bounded POST")
    journal.append(
        "submit_attempt",
        key=key,
        expected_id=expected_id,
        source_uri=entry["source_uri"],
        source_digest=entry["source_digest"],
        target=entry["output_tag"],
        submission_client="bounded-python-http",
    )
    payload = canonical(
        {
            "source_uri": entry["source_uri"],
            "source_digest": entry["source_digest"],
            "targets": [{"target": entry["output_tag"], "platform": "linux/amd64"}],
            "format": "oci",
            "concurrency": 1,
            "timeout": 300,
            "idempotency_key": key,
        }
    )
    forward = api[index % len(api)]
    if forward.process.poll() is not None:
        raise UnknownOutcome("Service Pod forward exited before POST")
    started = time.monotonic()
    request = Request(
        f"http://127.0.0.1:{forward.port}/v1/builds",
        data=payload,
        method="POST",
        headers={"Authorization": "Bearer " + token, "Content-Type": "application/json"},
    )
    try:
        code, body = http_json(request, deadline)
    except BaseException:
        observe_uncertain_submit(kube, plan, runtime, entry, journal, deadline)
        journal.append(
            "submit_unknown", key=key, expected_id=expected_id, cause="transport_or_response"
        )
        raise
    duration_ms = round((time.monotonic() - started) * 1000, 3)
    if code != 202:
        observe_uncertain_submit(kube, plan, runtime, entry, journal, deadline)
        journal.append("submit_unknown", key=key, expected_id=expected_id, http_status=code)
        raise UnknownOutcome("Service POST did not return a fresh 202 receipt")
    try:
        public_job(body, entry, runtime)
        if body.get("id") != expected_id:
            raise UnknownOutcome("Service returned a different deterministic idempotent job ID")
    except UnknownOutcome:
        observe_uncertain_submit(kube, plan, runtime, entry, journal, deadline)
        journal.append("submit_unknown", key=key, expected_id=expected_id, cause="identity")
        raise
    journal.append(
        "submit_accepted",
        key=key,
        job_id=expected_id,
        submit_latency_ms=duration_ms,
        service_pod=forward.pod,
        status=body.get("status"),
    )
    try:
        build = kube.get(deadline, "kovabuild", expected_id, plan["fixture"]["runner_namespace"])
    except (UnknownOutcome, OSError, ValueError):
        journal.append("cr_unknown", key=key, expected_id=expected_id, exact_read_attempted=True)
        raise
    uid = check_build(build, entry, runtime, plan)
    journal.append("cr_confirmed", key=key, job_id=expected_id, uid=uid)
    return {
        "entry": entry,
        "id": expected_id,
        "uid": uid,
        "submit_start": started,
        "submit_ms": duration_ms,
        "last_phase": build.get("status", {}).get("phase", ""),
        "terminal": False,
    }


def observe_uncertain_submit(
    kube: Kube,
    plan: dict,
    runtime: dict,
    entry: dict,
    journal: Journal,
    deadline: float,
) -> None:
    """One exact deterministic-ID read is evidence, never retry authority."""
    job_id = expected_job_id(runtime["requester"], entry["key"])
    try:
        observed = kube.get(deadline, "kovabuild", job_id, plan["fixture"]["runner_namespace"])
        uid = check_build(observed, entry, runtime, plan)
        journal.append("submit_unknown_exact_read", job_id=job_id, uid=uid, identity="matched")
    except (UnknownOutcome, MeasuredFailure, OSError, ValueError):
        journal.append("submit_unknown_exact_read", job_id=job_id, identity="unconfirmed")


def submit_wave(
    entries: list[dict],
    api: list[Forward],
    token: str,
    kube: Kube,
    plan: dict,
    runtime: dict,
    journal: Journal,
    deadline: float,
) -> list[dict]:
    if len(entries) not in (1, 8, 12):
        raise ContractError("invalid wave size")
    barrier = threading.Barrier(len(entries))
    completed = []
    unknown = False
    failure = False
    with ThreadPoolExecutor(max_workers=len(entries)) as pool:
        futures = [
            pool.submit(
                post_one, entry, index, barrier, api, token, kube, plan, runtime, journal, deadline
            )
            for index, entry in enumerate(entries)
        ]
        for future in as_completed(futures):
            try:
                completed.append(future.result())
            except MeasuredFailure:
                failure = True
            except BaseException:
                unknown = True
    if unknown:
        raise UnknownOutcome("one or more concurrent submissions lacks confirmed UID")
    if failure or len(completed) != len(entries):
        raise UnknownOutcome("partial wave submission requires exact-ID quarantine")
    return completed


def get_public(api: Forward, token: str, job_id: str, suffix: str, deadline: float) -> dict:
    if api.process.poll() is not None:
        raise UnknownOutcome("Service Pod forward exited during observation")
    request = Request(
        f"http://127.0.0.1:{api.port}/v1/builds/{job_id}{suffix}",
        headers={"Authorization": "Bearer " + token},
    )
    code, body = http_json(request, deadline)
    if code != 200:
        raise UnknownOutcome("public job/result read was not 200")
    return body


def terminal_result(
    entry: dict,
    build: dict,
    api: Forward,
    token: str,
    runtime: dict,
    journal: Journal,
    deadline: float,
) -> dict:
    job_id = build["metadata"]["name"]
    job = get_public(api, token, job_id, "", deadline)
    public_job(job, entry, runtime, terminal=True)
    timings = public_phase_ms(job)
    result = get_public(api, token, job_id, "/results", deadline)
    outputs = result.get("outputs")
    if (
        result.get("id") != job_id
        or result.get("source_uri") != entry["source_uri"]
        or result.get("source_digest") != entry["source_digest"]
        or result.get("idempotency_key") != entry["key"]
        or type(outputs) is not list
        or len(outputs) != 1
    ):
        raise UnknownOutcome("public result identity differs")
    output = outputs[0]
    digest = output.get("manifest_digest")
    repository = entry["output_tag"].rsplit(":", 1)[0]
    if (
        output.get("format") != "oci"
        or output.get("platform") != "linux/amd64"
        or output.get("image") != entry["output_tag"]
        or type(digest) is not str
        or DIGEST.fullmatch(digest) is None
        or output.get("immutable_ref") != repository + "@" + digest
    ):
        raise UnknownOutcome("public output differs from exact target and digest receipt")
    journal.append(
        "job_terminal",
        job_id=job_id,
        uid=build["metadata"]["uid"],
        status="succeeded",
        phase_ms=timings,
        output_manifest_digest=digest,
        build_observation=output_observation(build, entry, digest),
    )
    observation = output_observation(build, entry, digest)
    return {
        "phase_ms": timings,
        "output_manifest_digest": digest,
        "build_observation": observation,
        "phase_observation_complete": phase_observation_complete(observation),
    }


def poll_wave(
    active: list[dict],
    known: dict[str, dict],
    kube: Kube,
    api: list[Forward],
    token: str,
    plan: dict,
    runtime: dict,
    journal: Journal,
    sampler: Sampler,
    deadline: float,
) -> list[dict]:
    namespace = plan["fixture"]["runner_namespace"]
    while time.monotonic() < deadline:
        sampler.check()
        builds = kube.list(deadline, "kovabuild", namespace)
        observed = {item.get("metadata", {}).get("name"): item for item in builds}
        if len(observed) != len(builds) or set(observed) != set(known):
            raise UnknownOutcome("KovaBuild set differs from exact campaign IDs")
        for item in active:
            build = observed[item["id"]]
            check_build(build, item["entry"], runtime, plan, item["uid"])
            phase = build.get("status", {}).get("phase", "")
            if phase != item["last_phase"]:
                journal.append("phase_observed", job_id=item["id"], uid=item["uid"], phase=phase)
                item["last_phase"] = phase
            if phase in ("Failed", "Cancelled"):
                raise MeasuredFailure("identified build reached non-success terminal phase")
            if phase == "Succeeded" and not item["terminal"]:
                result = terminal_result(
                    item["entry"],
                    build,
                    api[item["entry"]["ordinal"] % len(api)],
                    token,
                    runtime,
                    journal,
                    deadline,
                )
                item.update(result)
                item["terminal"] = True
                item["terminal_at"] = time.monotonic()
        if all(item["terminal"] for item in active):
            return active
        time.sleep(min(3, max(0, deadline - time.monotonic())))
    raise MeasuredFailure("absolute warmup/wave deadline expired before all terminal receipts")


def summarize_wave(active: list[dict], started: float) -> dict:
    elapsed = max(item["terminal_at"] for item in active) - started
    client_ms = [(item["terminal_at"] - item["submit_start"]) * 1000 for item in active]
    submit_ms = [item["submit_ms"] for item in active]
    return {
        "jobs": len(active),
        "elapsed_seconds": round(elapsed, 3),
        "throughput_jobs_per_second": round(len(active) / elapsed, 6),
        "client_end_to_end_p95_ms": round(nearest_rank(client_ms, 0.95), 3),
        "client_end_to_end_p99_ms": round(nearest_rank(client_ms, 0.99), 3),
        "submit_p95_ms": round(nearest_rank(submit_ms, 0.95), 3),
        "submit_p99_ms": round(nearest_rank(submit_ms, 0.99), 3),
        "queue_p95_ms": round(
            nearest_rank([item["phase_ms"]["queue_ms"] for item in active], 0.95), 3
        ),
    }


class Proxy:
    def __init__(self, kube: Kube, deadline: float):
        argv = kube.argv(
            "proxy", "--address=127.0.0.1", "--port=0", "--accept-hosts=^127\\.0\\.0\\.1$"
        )
        self.process = subprocess.Popen(
            argv, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, start_new_session=True
        )
        self.port = 0
        try:
            for line in process_lines(self.process, deadline):
                found = re.search(r"Starting to serve on 127\.0\.0\.1:(\d+)", line)
                if found:
                    self.port = int(found.group(1))
                    break
            if not self.port:
                raise UnknownOutcome("Kubernetes cleanup proxy did not bind loopback")
        except BaseException:
            self.close()
            raise

    def close(self) -> None:
        if self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=5)
        if self.process.stdout is not None:
            self.process.stdout.close()


def qualify_cleanup_pods(pods: list[dict], known: dict[str, dict], namespace: str) -> None:
    seen = set()
    for pod in pods:
        meta = pod.get("metadata", {})
        name, uid = meta.get("name"), meta.get("uid")
        build_id = name.removeprefix("kova-job-") if type(name) is str else ""
        owners = meta.get("ownerReferences")
        if (
            type(name) is not str
            or name != "kova-job-" + build_id
            or build_id not in known
            or name in seen
            or type(uid) is not str
            or UID.fullmatch(uid) is None
            or meta.get("namespace") != namespace
            or meta.get("labels", {}).get("app.kubernetes.io/name") != "kova-runner"
            or type(owners) is not list
            or len(owners) != 1
            or owners[0].get("apiVersion") != "kova.cofy.dev/v1alpha1"
            or owners[0].get("kind") != "KovaBuild"
            or owners[0].get("name") != build_id
            or owners[0].get("uid") != known[build_id]["uid"]
            or owners[0].get("controller") is not True
        ):
            raise UnknownOutcome("runner namespace contains an unqualified Pod during cleanup")
        seen.add(name)


def campaign_objects_preflight(
    kube: Kube, plan: dict, runtime: dict, known: dict[str, dict], deadline: float
) -> None:
    namespace = plan["fixture"]["runner_namespace"]
    builds = kube.list(deadline, "kovabuild", namespace)
    by_name = {item.get("metadata", {}).get("name"): item for item in builds}
    if len(by_name) != len(builds) or set(by_name) != set(known):
        raise UnknownOutcome("runner namespace KovaBuild set differs from confirmed campaign IDs")
    for name, item in known.items():
        check_build(by_name[name], item["entry"], runtime, plan, item["uid"])
    qualify_cleanup_pods(kube.list(deadline, "pod", namespace), known, namespace)


def qualify_delete_response(code: int, raw: bytes, item: dict, runtime: dict, plan: dict) -> None:
    """Only the original object or its exact Kubernetes Success Status qualifies."""
    if code not in (200, 202):
        raise UnknownOutcome("UID-preconditioned delete did not return success")
    try:
        value = json.loads(raw.decode("utf-8"), object_pairs_hook=unique_pairs)
    except (UnicodeError, ValueError, TypeError) as error:
        raise UnknownOutcome("UID-preconditioned delete response is unreadable") from error
    if type(value) is not dict:
        raise UnknownOutcome("UID-preconditioned delete response is not an object")
    if value.get("apiVersion") == "kova.cofy.dev/v1alpha1" and value.get("kind") == "KovaBuild":
        try:
            check_build(value, item["entry"], runtime, plan, item["uid"], allow_deleting=True)
        except (AttributeError, TypeError, ValueError) as error:
            raise UnknownOutcome("UID-preconditioned delete object is malformed") from error
        return
    details = value.get("details")
    if (
        value.get("apiVersion") != "v1"
        or value.get("kind") != "Status"
        or value.get("status") != "Success"
        or value.get("reason", "") != ""
        or type(value.get("code", 0)) is not int
        or value.get("code", 0) not in (0, code)
        or type(details) is not dict
        or details.get("uid") != item["uid"]
        or details.get("name") != expected_job_id(runtime["requester"], item["entry"]["key"])
        or details.get("group") != "kova.cofy.dev"
        or details.get("kind") != "kovabuilds"
    ):
        raise UnknownOutcome("UID-preconditioned delete response has a foreign identity")


def exact_cleanup(
    kube: Kube, plan: dict, runtime: dict, known: dict[str, dict], journal: Journal, deadline: float
) -> None:
    fixture = plan["fixture"]
    fixture_preflight(kube, plan, runtime, deadline, empty=False)
    campaign_objects_preflight(kube, plan, runtime, known, deadline)
    proxy = Proxy(kube, deadline)
    try:
        for job_id, item in sorted(known.items()):
            # Direct pre-read confirms complete identity; server DELETE applies
            # the UID precondition atomically. kubectl delete does not.
            build = kube.get(deadline, "kovabuild", job_id, fixture["runner_namespace"])
            check_build(build, item["entry"], runtime, plan, item["uid"])
            body = canonical(
                {
                    "apiVersion": "meta.k8s.io/v1",
                    "kind": "DeleteOptions",
                    "preconditions": {"uid": item["uid"]},
                    "propagationPolicy": "Background",
                }
            )
            path = (
                f"/apis/kova.cofy.dev/v1alpha1/namespaces/{fixture['runner_namespace']}"
                f"/kovabuilds/{job_id}"
            )
            request = Request(
                f"http://127.0.0.1:{proxy.port}{path}",
                data=body,
                method="DELETE",
                headers={"Content-Type": "application/json"},
            )
            journal.append("delete_attempt", job_id=job_id, uid_precondition=item["uid"])
            code, raw = http_bytes(request, deadline, MAX_JSON)
            journal.append(
                "delete_response",
                job_id=job_id,
                uid_precondition=item["uid"],
                http_status=code,
                body_sha256=sha(raw),
            )
            qualify_delete_response(code, raw, item, runtime, plan)
    finally:
        proxy.close()
    empty_reads = 0
    while time.monotonic() < deadline:
        current = kube.list(deadline, "kovabuild", fixture["runner_namespace"])
        runners = kube.list(deadline, "pod", fixture["runner_namespace"])
        qualify_cleanup_pods(runners, known, fixture["runner_namespace"])
        for build in current:
            name = build.get("metadata", {}).get("name")
            if name not in known:
                raise UnknownOutcome("foreign build appeared during exact cleanup")
            check_build(
                build,
                known[name]["entry"],
                runtime,
                plan,
                known[name]["uid"],
                allow_deleting=True,
            )
        if not current and not runners:
            active = kube.get(deadline, "configmap", ACTIVE_NAME, fixture["runner_namespace"])
            queue = kube.get(deadline, "configmap", QUEUE_NAME, fixture["runner_namespace"])
            metadata_uid(active, runtime["genesis"]["active_ledger_uid"], "active ledger")
            metadata_uid(queue, runtime["genesis"]["queue_ledger_uid"], "queue ledger")
            assert_empty_ledgers(active, queue, plan["limits"])
            empty_reads += 1
            journal.append("post_cleanup_empty_read", ordinal=empty_reads)
            if empty_reads == 2:
                fixture_preflight(kube, plan, runtime, deadline, empty=True)
                return
        else:
            empty_reads = 0
        time.sleep(min(2, max(0, deadline - time.monotonic())))
    raise UnknownOutcome("300-second cleanup deadline expired without two empty reads")


def may_cleanup(outcome: str, post_attempted: bool) -> bool:
    """Never mutate a fixture whose accepted set or identity is uncertain."""
    return post_attempted and outcome in ("FAILED", "INCOMPLETE", "COMPLETE")


def timing_diagnostics(
    kube: Kube,
    plan: dict,
    runtime: dict,
    known: dict[str, dict],
    journal: Journal,
    since: str,
    deadline: float,
) -> dict:
    """Project only stage/duration fields from bounded current-Pod logs."""
    observations = []
    truncated = []
    for pin in runtime["service_pods"]:
        pod = kube.get(deadline, "pod", pin["name"], plan["fixture"]["control_namespace"])
        ready_pod(pod, pin["name"], pin["uid"], plan["candidate"]["role_images"]["controller"])
        raw = command(
            kube.argv(
                "-n",
                plan["fixture"]["control_namespace"],
                "logs",
                f"pod/{pin['name']}",
                "-c",
                "kova-service",
                f"--since-time={since}",
                "--timestamps=true",
                "--limit-bytes=2097152",
            ),
            deadline,
            2 << 20,
        )
        if len(raw) >= 2 << 20:
            truncated.append(pin["name"])
        if len(raw.splitlines()) > 10000:
            raise UnknownOutcome("controller timing logs exceeded 10000-line bound")
        for line in raw.splitlines():
            if len(line) > 16 << 10:
                raise UnknownOutcome("controller timing log line exceeded 16 KiB")
            try:
                stamp, payload = line.split(b" ", 1)
                if not re.fullmatch(rb"\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(?:\.\d+)?Z", stamp):
                    continue
                value = json.loads(payload, object_pairs_hook=unique_pairs)
            except (ValueError, TypeError):
                continue
            if type(value) is not dict:
                continue
            message = value.get("msg")
            if message not in (
                "Service stage timing",
                "Runner startup timing",
                "Target execution timing",
            ):
                continue
            job_id = next(
                (
                    candidate
                    for candidate in (value.get("build"), value.get("name"))
                    if candidate in known
                ),
                None,
            )
            stage = (
                value.get("stage") if message != "Target execution timing" else "target_execution"
            )
            duration = value.get("duration_ms")
            if (
                job_id is None
                or type(duration) not in (int, float)
                or not math.isfinite(duration)
                or duration < 0
                or type(stage) is not str
                or stage
                not in {
                    "admission",
                    "source_inspect",
                    "submit",
                    "queue_to_pod",
                    "pod_schedule",
                    "source_wait",
                    "source_fetch",
                    "runner_ready",
                    "pod_startup",
                    "target_execution",
                }
            ):
                continue
            observations.append(
                {
                    "service_pod": pin["name"],
                    "job_id": job_id,
                    "stage": stage,
                    "result": value.get("result")
                    if value.get("result") in ("ok", "error")
                    else None,
                    "duration_ms": duration,
                    "observed_at": stamp.decode(),
                }
            )
    digest = journal.save_json(
        "timing-diagnostics.json",
        {"observations": observations, "possibly_truncated_pods": truncated},
    )
    by_stage = {}
    per_job = {job_id: set() for job_id in known}
    for item in observations:
        by_stage[item["stage"]] = by_stage.get(item["stage"], 0) + 1
        per_job[item["job_id"]].add(item["stage"])
    required = {
        "admission",
        "source_inspect",
        "submit",
        "queue_to_pod",
        "pod_schedule",
        "source_fetch",
        "target_execution",
    }
    missing = {
        job_id: sorted(required - stages) for job_id, stages in per_job.items() if required - stages
    }
    summary = {
        "sha256": digest,
        "observations": len(observations),
        "per_stage": by_stage,
        "possibly_truncated_pods": truncated,
        "missing_required_per_job": missing,
        "complete_for_attribution": not truncated and not missing,
        "scope": "bounded current Service Pod logs; best effort, not exactly once",
    }
    journal.append("timing_diagnostics", **summary)
    return summary


def prepare_outputs(
    args: argparse.Namespace,
    plan: dict,
    runtime: dict,
    sources: list[dict],
    plan_sha: str,
    sources_sha: str,
    runtime_sha: str,
    journal: Journal,
) -> None:
    journal.append(
        "run_started",
        run_id=plan["run_id"],
        plan_sha256=plan_sha,
        sources_sha256=sources_sha,
        runtime_sha256=runtime_sha,
        coordinator_sha256=hash_file(Path(__file__)),
        kova_commit=plan["candidate"]["kova_commit"],
        cli_sha256=plan["candidate"]["cli_sha256"],
        submission_client="bounded-python-http",
        measured_jobs=40,
        warmup_jobs=1,
        source_preparation="operator-prepublished-read-only",
    )
    journal.save_json(
        "source-pins.json",
        [
            {
                "key": item["key"],
                "mode": item["mode"],
                "source_uri": item["source_uri"],
                "source_digest": item["source_digest"],
                "payload_sha256": item["payload_sha256"],
                "target": item["output_tag"],
                "cache_cohort_declared": item["cache_cohort"],
            }
            for item in sources
        ],
    )


def run_campaign(
    args: argparse.Namespace,
    plan: dict,
    runtime: dict,
    sources: list[dict],
    plan_sha: str,
    sources_sha: str,
    runtime_sha: str,
) -> int:
    fixture = plan["fixture"]
    ack = (
        f"{plan['run_id']}/{fixture['control_namespace']}/"
        f"{fixture['runner_namespace']}/{plan_sha[:12]}"
    )
    need(args.ack == ack, f"run requires --ack {ack}")
    need(sys.platform == "linux", "live run is Linux-only")
    token_info = args.token_file.lstat()
    need(
        stat.S_ISREG(token_info.st_mode)
        and token_info.st_mode & 0o077 == 0
        and token_info.st_size <= 4096,
        "token file must be a private regular file of at most 4 KiB",
    )
    token = args.token_file.read_text(encoding="utf-8").strip()
    need(
        16 <= len(token) <= 4096 and not any(char.isspace() for char in token),
        "token file must contain one non-whitespace bearer of 16-4096 characters",
    )
    journal = Journal(args.out_dir, token)
    kube = Kube(args.kubeconfig, fixture["context"])
    api, metrics = [], []
    sampler = None
    worker_observer = None
    known: dict[str, dict] = {}
    post_attempted = False
    # Schema 2 requires real interval unions and server-side solve occupancy.
    # It does not require or manufacture pure CPU/transport Session metrics.
    attribution_complete = True
    outcome = "UNKNOWN"
    started_at = now()
    start = time.monotonic()
    injection_deadline = start + plan["budget_seconds"]["injection"]
    try:
        prepare_outputs(args, plan, runtime, sources, plan_sha, sources_sha, runtime_sha, journal)
        journal.append(
            "attribution_scope",
            interval_unions_are_not_additive=True,
            pure_cpu_execution_time="unavailable",
            transport_session_ids="unavailable",
            complete_means="this finite attribution only, not issue closure or capacity SLA",
        )
        source_deadline = min(injection_deadline, start + plan["budget_seconds"]["source_prepare"])
        check_registry_container(plan, runtime, source_deadline)
        fixture_preflight(kube, plan, runtime, source_deadline, empty=True)
        worker_observer = WorkerSolveObserver(kube, plan, runtime, journal)
        worker_observer.sample(source_deadline, baseline=True)
        check_registry_catalog(plan, runtime, sources, source_deadline)
        for entry in sources:
            observation = verify_source(entry, plan, runtime, source_deadline)
            journal.append("source_verified", **observation)
        for pod in runtime["service_pods"]:
            api.append(
                Forward(
                    kube,
                    fixture["control_namespace"],
                    pod["name"],
                    runtime["service_port"],
                    source_deadline,
                )
            )
            metrics.append(
                Forward(
                    kube,
                    fixture["control_namespace"],
                    pod["name"],
                    runtime["metrics_port"],
                    source_deadline,
                )
            )
        for forward in api:
            code, body = http_json(
                Request(f"http://127.0.0.1:{forward.port}/version"), source_deadline
            )
            observed_commit = body.get("commit")
            if (
                code != 200
                or body.get("api_version") != "v1"
                or type(observed_commit) is not str
                or len(observed_commit) not in (12, 40)
                or not plan["candidate"]["kova_commit"].startswith(observed_commit)
            ):
                raise UnknownOutcome("Service API candidate version differs")
        sampler = Sampler(kube, plan, runtime, journal, injection_deadline, worker_observer)
        sampler.start()
        waves = [
            ("warmup", sources[:1]),
            ("cached-c8", sources[1:9]),
            ("cached-c12", sources[9:21]),
            ("uncached-c8", sources[21:29]),
            ("uncached-c12", sources[29:41]),
        ]
        for label, entries in waves:
            sampler.check()
            check_registry_container(plan, runtime, injection_deadline)
            fixture_preflight(kube, plan, runtime, injection_deadline, empty=False)
            campaign_objects_preflight(kube, plan, runtime, known, injection_deadline)
            phase_started = time.monotonic()
            phase_seconds = (
                plan["budget_seconds"]["warmup"]
                if label == "warmup"
                else plan["budget_seconds"]["each_wave"]
            )
            phase_deadline = min(injection_deadline, phase_started + phase_seconds)
            for entry in entries:
                verify_source_tag(entry, runtime, phase_deadline)
            before = snapshot_metrics(journal, metrics, label + "-before", phase_deadline)
            started = time.monotonic()
            journal.append(
                "wave_started",
                label=label,
                jobs=len(entries),
                absolute_seconds_remaining=round(injection_deadline - started, 3),
            )
            post_attempted = True
            active = submit_wave(entries, api, token, kube, plan, runtime, journal, phase_deadline)
            for item in active:
                known[item["id"]] = item
            poll_wave(
                active, known, kube, api, token, plan, runtime, journal, sampler, phase_deadline
            )
            for entry in entries:
                verify_source_tag(entry, runtime, phase_deadline)
            check_registry_container(plan, runtime, phase_deadline)
            after = snapshot_metrics(journal, metrics, label + "-after", phase_deadline)
            summary = summarize_wave(active, started)
            delta = metric_deltas(before, after)
            summary["metric_delta_file"] = f"metric-delta-{label}.json"
            summary["metric_delta_sha256"] = journal.save_json(summary["metric_delta_file"], delta)
            summary["metric_missing"] = delta["missing"]
            if delta["missing"]:
                attribution_complete = False
            journal.append("wave_complete", label=label, **summary)
        sampler.close()
        sampler = None
        # Close the independent worker-side histories before CR deletion.
        # Missing or ambiguous accepted solves cannot be filled from CLI calls.
        worker_observer.sample(injection_deadline)
        worker_summary = worker_observer.summary(known)
        phase_missing = [
            job_id
            for job_id, job in known.items()
            if not job.get("phase_observation_complete", False)
        ]
        phase_receipt = {
            "complete_for_attribution": len(known) == 41 and not phase_missing,
            "missing_or_incomplete_builds": phase_missing,
            "jobs": [
                {
                    "job_id": job_id,
                    "build_uid": job["uid"],
                    "output_manifest_digest": job["output_manifest_digest"],
                    "build_observation": job["build_observation"],
                }
                for job_id, job in known.items()
            ],
            "semantics": "overlapping vertex/export/push interval unions, never additive phases",
        }
        phase_receipt_sha = journal.save_json("build-phase-attribution.json", phase_receipt)
        journal.append(
            "build_phase_attribution",
            sha256=phase_receipt_sha,
            missing_or_incomplete_builds=phase_missing,
        )
        attribution_complete = (
            attribution_complete
            and worker_summary["complete_for_attribution"]
            and phase_receipt["complete_for_attribution"]
        )
        outcome = "COMPLETE" if attribution_complete else "INCOMPLETE"
    except (Exception, KeyboardInterrupt) as error:
        outcome = "FAILED" if isinstance(error, MeasuredFailure) else "UNKNOWN"
        journal.append(
            "run_interrupted",
            classification=outcome,
            reason=type(error).__name__,
            post_attempted=post_attempted,
            known_uids=len(known),
        )
    finally:
        if sampler is not None:
            try:
                sampler.close()
            except UnknownOutcome:
                outcome = "UNKNOWN"
        for forward in api + metrics:
            forward.close()
    # Qualify the last read-only measurements before authorizing any CR delete.
    # Even an identified build failure cannot override an unknown observation.
    if may_cleanup(outcome, post_attempted):
        try:
            timing = timing_diagnostics(
                kube,
                plan,
                runtime,
                known,
                journal,
                started_at,
                time.monotonic() + 45,
            )
            if not timing["complete_for_attribution"] and outcome != "FAILED":
                outcome = "INCOMPLETE"
        except (UnknownOutcome, MeasuredFailure, OSError, ValueError) as error:
            outcome = "UNKNOWN"
            journal.append("timing_diagnostics_unknown", reason=type(error).__name__)
    if may_cleanup(outcome, post_attempted):
        cleanup_deadline = time.monotonic() + plan["budget_seconds"]["cleanup"]
        try:
            check_registry_container(plan, runtime, cleanup_deadline)
            exact_cleanup(kube, plan, runtime, known, journal, cleanup_deadline)
            journal.append(
                "cleanup_complete",
                uid_preconditioned_builds=len(known),
                empty_reads=2,
                source_and_output_tags_retained=True,
            )
        except (UnknownOutcome, MeasuredFailure, OSError, ValueError) as error:
            outcome = "UNKNOWN"
            journal.append("cleanup_unknown", reason=type(error).__name__, known_uids=len(known))
    if outcome == "UNKNOWN":
        journal.append(
            "quarantined",
            run_id=plan["run_id"],
            reason="manual exact-identity review required",
            no_auto_retry=True,
            no_broad_delete=True,
        )
    else:
        journal.append(
            "run_finished",
            outcome=outcome,
            measured_jobs=40 if outcome in ("COMPLETE", "INCOMPLETE") else None,
        )
    journal.close()
    return {"COMPLETE": 0, "FAILED": 1, "UNKNOWN": 3, "INCOMPLETE": 4}[outcome]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("check", "run"))
    for flag in (
        "plan",
        "runtime",
        "sources",
        "cli",
        "chart",
        "values",
        "source-template",
        "counter-schema",
        "kubeconfig",
    ):
        parser.add_argument("--" + flag, type=Path, required=True)
    parser.add_argument("--token-file", type=Path)
    parser.add_argument("--out-dir", type=Path)
    parser.add_argument("--ack")
    args = parser.parse_args()
    try:
        plan, plan_sha = load_json(args.plan, 32 << 10)
        jobs = validate_plan(plan)
        source_doc, sources_sha = load_json(args.sources, 64 << 10)
        sources = validate_sources(source_doc, plan, jobs)
        for entry, job in zip(sources, jobs):
            entry["key"] = job["key"]
        runtime, runtime_sha = load_json(args.runtime, 32 << 10)
        validate_runtime(runtime, plan, plan_sha, sources_sha)
        local_artifacts(args, plan)
        if args.mode == "check":
            print(
                json.dumps(
                    {
                        "classification": "local_contract_only",
                        "live_ready": False,
                        "run_id": plan["run_id"],
                        "plan_sha256": plan_sha,
                        "sources_sha256": sources_sha,
                        "runtime_sha256": runtime_sha,
                        "jobs": 41,
                        "waves": [1, 8, 12, 8, 12],
                    },
                    sort_keys=True,
                )
            )
            return 0
        need(
            args.token_file is not None and args.out_dir is not None,
            "run requires token file and private new output directory",
        )
        return run_campaign(args, plan, runtime, sources, plan_sha, sources_sha, runtime_sha)
    except (OSError, ValueError, ContractError, UnknownOutcome, MeasuredFailure) as error:
        print(f"service-burst: {type(error).__name__}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
