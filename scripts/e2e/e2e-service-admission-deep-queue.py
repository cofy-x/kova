#!/usr/bin/env python3
"""Bounded, API-server-backed #46 deep-queue acceptance on disposable Kind.

Check mode is read-only. Run mode requires an exact cluster acknowledgement and
keeps all receipts. It never creates a cluster, changes a ledger, or contacts a
registry. One test-owned unschedulable Pending runner holds the active slot;
the 100/500/1000 queued builds must have no runner Pods.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import re
import secrets
import shutil
import socket
import subprocess
import sys
import time
from collections.abc import Callable
from datetime import datetime, timezone
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import ProxyHandler, Request, build_opener

ROOT = Path(__file__).resolve().parents[2]
CLUSTER = "kova-deep-queue"
NAMESPACE = "kova"
RELEASE = "kova"
KUBECONFIG = ROOT / ".kind" / f"{CLUSTER}.kubeconfig"
STAGES = (100, 500, 1000)
POLL_SECONDS = 5
STEADY_SECONDS = 30
PORT = 18086
SERVICE_CPU_MAX_NANO = 2_000_000_000  # 2 vCPU per Service Pod
SERVICE_MEMORY_MAX = 2 * 1024**3
NODE_MEMORY_AVAILABLE_MIN = 2 * 1024**3
HOST_MEMORY_AVAILABLE_MIN = 8 * 1024**3
DISK_FREE_MIN = 20 * 1024**3
NODE_CPU_FRACTION_MAX = 0.80
API_QPS_MAX = 1200
HTTP_BODY_MAX = 1024 * 1024
SETTLE_DEADLINE_SECONDS = 300
CLEANUP_DEADLINE_SECONDS = 900
RUN_DEADLINE_SECONDS = 3600
SOURCE_DIGEST = "sha256:" + "a" * 64
OPENER = build_opener(ProxyHandler({}))


class BenchError(RuntimeError):
    pass


def fail(message: str) -> None:
    raise BenchError(message)


def note(message: str) -> None:
    print(f"deep-queue-e2e: {message}", file=sys.stderr, flush=True)


def now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")  # noqa: UP017 (Python 3.10)


def command(argv: list[str], *, input_text: str | None = None, timeout: int = 20) -> str:
    try:
        result = subprocess.run(
            argv, input=input_text, text=True, capture_output=True, timeout=timeout, check=False
        )
    except (OSError, subprocess.TimeoutExpired) as error:
        fail(f"{argv[0]} did not complete: {type(error).__name__}")
    if result.returncode != 0:
        fail(f"{argv[0]} returned {result.returncode}: {result.stderr[-300:].strip()}")
    return result.stdout


def kctl(*args: str, timeout: int = 20) -> str:
    return command(
        ["kubectl", "--kubeconfig", str(KUBECONFIG), "--request-timeout=15s", *args],
        timeout=timeout,
    )


def kjson(*args: str) -> dict:
    result = json.loads(kctl(*args))
    if not isinstance(result, dict):
        fail(f"kubectl returned non-object JSON for {args[:3]}")
    return result


def save_json(path: Path, value: object) -> None:
    with path.open("w", encoding="utf-8") as output:
        json.dump(value, output, sort_keys=True, indent=2)
        output.write("\n")
        output.flush()
        os.fsync(output.fileno())


def append_jsonl(path: Path, value: object) -> None:
    with path.open("a", encoding="utf-8") as output:
        output.write(json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n")
        output.flush()
        os.fsync(output.fileno())


def load_kubeconfig_identity(config: dict) -> tuple[str, str]:
    try:
        context = config["current-context"]
        cluster = config["clusters"][0]["cluster"]
        user = config["users"][0]["user"]
        identity = {
            "context": context,
            "server": cluster["server"],
            "ca": cluster["certificate-authority-data"],
            "cert": user["client-certificate-data"],
            "key": user["client-key-data"],
        }
    except (KeyError, IndexError, TypeError) as error:
        fail(f"Kind kubeconfig lacks an identity field: {type(error).__name__}")
    if any(not isinstance(value, str) or not value for value in identity.values()):
        fail("Kind kubeconfig identity has an empty field")
    digest = hashlib.sha256(json.dumps(identity, sort_keys=True).encode()).hexdigest()
    return context, digest


def exact_kind_identity() -> str:
    if not KUBECONFIG.is_file() or KUBECONFIG.is_symlink():
        fail(f"missing regular dedicated kubeconfig: {KUBECONFIG}")
    if command(["kind", "get", "clusters"]).splitlines() != [CLUSTER]:
        fail(f"the host must have exactly one Kind cluster named {CLUSTER}")
    actual = json.loads(
        command(
            [
                "kubectl",
                "--kubeconfig",
                str(KUBECONFIG),
                "config",
                "view",
                "--raw",
                "--minify",
                "-o",
                "json",
            ]
        )
    )
    live_yaml = command(["kind", "get", "kubeconfig", "--name", CLUSTER])
    live = json.loads(
        command(
            [
                "kubectl",
                "--kubeconfig",
                "/dev/stdin",
                "config",
                "view",
                "--raw",
                "--minify",
                "-o",
                "json",
            ],
            input_text=live_yaml,
        )
    )
    context, fingerprint = load_kubeconfig_identity(actual)
    live_context, live_fingerprint = load_kubeconfig_identity(live)
    if context != f"kind-{CLUSTER}" or live_context != context or live_fingerprint != fingerprint:
        fail(
            "dedicated kubeconfig context, server, CA, or client credentials differ from live Kind"
        )
    return fingerprint


def good_nodes(nodes: dict) -> list[dict]:
    items = nodes.get("items", [])
    expected = {f"{CLUSTER}-control-plane", f"{CLUSTER}-worker"}
    if {item.get("metadata", {}).get("name") for item in items} != expected:
        fail("Kind node names/count differ from the dedicated one-worker cluster")
    for item in items:
        conditions = {c["type"]: c["status"] for c in item.get("status", {}).get("conditions", [])}
        if conditions.get("Ready") != "True" or any(
            conditions.get(name) != "False"
            for name in ("DiskPressure", "MemoryPressure", "PIDPressure")
        ):
            fail("a Kind node is not Ready or reports resource pressure")
        if item.get("metadata", {}).get("labels", {}).get("never") == "true":
            fail("a Kind node matches the supposedly unschedulable never=true selector")
    return items


def single_service_args(deployment: dict) -> tuple[str, str]:
    spec = deployment.get("spec", {})
    status = deployment.get("status", {})
    if (
        any(status.get(key) != 2 for key in ("replicas", "readyReplicas", "updatedReplicas"))
        or spec.get("replicas") != 2
    ):
        fail("the Service Deployment is not exactly two ready, updated replicas")
    if status.get("observedGeneration") != deployment.get("metadata", {}).get("generation"):
        fail("Service Deployment generation has not converged")
    containers = [
        c
        for c in spec.get("template", {}).get("spec", {}).get("containers", [])
        if c.get("name") == "kova-service"
    ]
    if len(containers) != 1:
        fail("Service Deployment does not have one kova-service container")
    if containers[0].get("imagePullPolicy") != "Never":
        fail(
            "Service imagePullPolicy must be Never so this benchmark cannot fetch from the registry"
        )
    args = containers[0].get("args", [])
    required = {
        "--namespace=kova",
        "--max-active-jobs=1",
        "--max-active-jobs-per-requester=1",
        "--worker-slots=1",
        "--max-queued-jobs=1000",
        "--max-queued-jobs-per-requester=1000",
        "--runner-node-selector=never=true",
        "--auth-mode=static",
        "--auth-static-principal=kova:e2e",
        "--leader-elect=true",
        "--poll-interval=5s",
        "--leader-election-namespace=kova",
        "--wait=2h",
        "--max-build-duration=2h",
        "--runner-image-pull-policy=Never",
    }
    if not required.issubset(set(args)):
        fail("Service arguments differ from the exact deep-queue benchmark contract")
    platforms = [
        arg.split("=", 2)[1] for arg in args if arg.startswith("--buildkit-platform-addr=")
    ]
    if len(platforms) != 1 or platforms[0] not in ("linux/amd64", "linux/arm64"):
        fail("Service has no single supported test platform")
    return "kova:e2e", platforms[0]


def service_pods() -> list[dict]:
    pods = kjson(
        "-n",
        NAMESPACE,
        "get",
        "pods",
        "-l",
        f"app.kubernetes.io/instance={RELEASE},app.kubernetes.io/component=service",
        "-o",
        "json",
    )["items"]
    if len(pods) != 2:
        fail("not exactly two Service Pods")
    image_ids = set()
    for pod in pods:
        if pod["metadata"].get("deletionTimestamp"):
            fail("a Service Pod is terminating")
        if not any(
            c.get("type") == "Ready" and c.get("status") == "True"
            for c in pod.get("status", {}).get("conditions", [])
        ):
            fail("a Service Pod is not Ready")
        containers = [
            c
            for c in pod.get("status", {}).get("containerStatuses", [])
            if c.get("name") == "kova-service"
        ]
        if len(containers) != 1 or not containers[0].get("imageID"):
            fail("Service Pod image identity is unavailable")
        image_ids.add(containers[0]["imageID"])
    if len(image_ids) != 1:
        fail("Service Pods run different images")
    return sorted(pods, key=lambda pod: pod["metadata"]["name"])


def queue_ledgers(
    expected_queued: int, blocker_id: str | None = None, blocker_uid: str | None = None
) -> tuple[dict, dict]:
    active = kjson("-n", NAMESPACE, "get", "configmap", "kova-service-admission", "-o", "json")
    queue = kjson("-n", NAMESPACE, "get", "configmap", "kova-service-queue-admission", "-o", "json")
    try:
        active_data = json.loads(active["data"]["reservations.json"])
        queue_data = json.loads(queue["data"]["queue.json"])
    except (KeyError, TypeError, ValueError) as error:
        fail(f"admission ledger is unreadable: {type(error).__name__}")
    if (
        active_data.get("version"),
        active_data.get("maxJobs"),
        active_data.get("maxPerRequester"),
        active_data.get("workerSlots"),
    ) != (1, 1, 1, 1):
        fail("active ledger limits differ from the benchmark contract")
    if (
        queue_data.get("version"),
        queue_data.get("globalLimit"),
        queue_data.get("requesterLimit"),
    ) != (1, 1000, 1000):
        fail("queue ledger limits differ from the benchmark contract")
    grants = active_data.get("active")
    intents = queue_data.get("intents")
    if not isinstance(grants, dict) or not isinstance(intents, dict):
        fail("admission ledger entries are not objects")
    if len(intents) > expected_queued:
        fail("queue ledger has more intents than this stage submitted")
    if blocker_id is None:
        if grants or intents:
            fail("admission ledgers are not empty before the benchmark")
    elif grants != {blocker_uid: {"buildName": blocker_id, "requester": "kova:e2e", "slots": 1}}:
        fail("active ledger is not exactly the test-owned blocker grant")
    return grants, intents


def parse_prometheus_request_totals(data: str) -> dict[str, float]:
    totals: dict[str, float] = {}
    label_pattern = re.compile(r'(\w+)="((?:\\.|[^"\\])*)"')
    for line in data.splitlines():
        if not line.startswith("apiserver_request_total{"):
            continue
        try:
            labels = dict(label_pattern.findall(line.split("}", 1)[0]))
            value = float(line.rsplit(" ", 1)[1])
        except (ValueError, IndexError):
            fail("could not parse apiserver_request_total")
        if not math.isfinite(value) or value < 0:
            fail("apiserver_request_total has a nonfinite or negative value")
        key = "|".join(
            (
                labels.get("verb", "?"),
                labels.get("group", ""),
                labels.get("resource", ""),
                labels.get("code", "?"),
            )
        )
        totals[key] = totals.get(key, 0.0) + value
    if not totals:
        fail("apiserver_request_total is unavailable; cannot guard API pressure")
    return totals


def api_metrics() -> dict[str, float]:
    return parse_prometheus_request_totals(kctl("get", "--raw", "/metrics", timeout=30))


def cpu_quantity_nano(raw: str) -> int:
    if raw.endswith("m"):
        return int(raw[:-1]) * 1_000_000
    return int(raw) * 1_000_000_000


def local_image_fact(reference: str) -> dict:
    images = json.loads(command(["docker", "image", "inspect", reference], timeout=30))
    if len(images) != 1:
        fail(f"the local Docker image {reference} is unavailable or ambiguous")
    image = images[0]
    revision = (image.get("Config", {}).get("Labels") or {}).get(
        "org.opencontainers.image.revision"
    )
    if not isinstance(revision, str) or not re.fullmatch(r"[0-9a-f]{12,40}", revision):
        fail(f"local Docker image {reference} has no exact Kova revision label")
    return {
        "reference": reference,
        "image_id": image["Id"],
        "revision": revision,
        "architecture": image.get("Architecture"),
    }


def host_memory_available() -> int:
    for line in Path("/proc/meminfo").read_text().splitlines():
        if line.startswith("MemAvailable:"):
            return int(line.split()[1]) * 1024
    fail("Linux MemAvailable metric is absent")


def resource_sample(expected_pods: list[dict], stage_dir: Path | None, docker_root: Path) -> dict:
    nodes = good_nodes(kjson("get", "nodes", "-o", "json"))
    current_pods = service_pods()
    original_pods = {pod["metadata"]["name"]: pod["metadata"]["uid"] for pod in expected_pods}
    if {pod["metadata"]["name"]: pod["metadata"]["uid"] for pod in current_pods} != original_pods:
        fail("Service Pod identity changed during the benchmark")
    services = []
    node_samples = []
    for node in nodes:
        name = node["metadata"]["name"]
        summary = kjson("get", "--raw", f"/api/v1/nodes/{quote(name)}/proxy/stats/summary")
        cpu = summary.get("node", {}).get("cpu", {}).get("usageNanoCores")
        memory = summary.get("node", {}).get("memory", {}).get("workingSetBytes")
        available = summary.get("node", {}).get("memory", {}).get("availableBytes")
        if not all(isinstance(value, int) and value >= 0 for value in (cpu, memory, available)):
            fail(f"Kind node {name} has incomplete kubelet CPU/memory metrics")
        capacity = cpu_quantity_nano(node["status"]["allocatable"]["cpu"])
        node_samples.append(
            {
                "node": name,
                "cpu_nano": cpu,
                "cpu_allocatable_nano": capacity,
                "memory_working_bytes": memory,
                "memory_available_bytes": available,
            }
        )
        for pod in summary.get("pods", []):
            ref = pod.get("podRef", {})
            if ref.get("namespace") != NAMESPACE or ref.get("name") not in original_pods:
                continue
            pod_cpu = pod.get("cpu", {}).get("usageNanoCores")
            pod_memory = pod.get("memory", {}).get("workingSetBytes")
            if not all(isinstance(value, int) and value >= 0 for value in (pod_cpu, pod_memory)):
                fail(f"Service Pod {ref.get('name')} has incomplete kubelet CPU/memory metrics")
            services.append(
                {
                    "pod": ref["name"],
                    "node": name,
                    "cpu_nano": pod_cpu,
                    "memory_working_bytes": pod_memory,
                }
            )
    if {service["pod"] for service in services} != set(original_pods):
        fail("kubelet summary lacks one of the two Service Pods")
    host_mem = host_memory_available()
    root_free = shutil.disk_usage(ROOT).free
    docker_free = shutil.disk_usage(docker_root).free
    load1 = os.getloadavg()[0]
    sample = {
        "timestamp": now(),
        "services": services,
        "nodes": node_samples,
        "host_memory_available_bytes": host_mem,
        "host_load1": load1,
        "host_cpu_count": len(os.sched_getaffinity(0)),
        "workspace_disk_free_bytes": root_free,
        "docker_disk_free_bytes": docker_free,
    }
    if stage_dir is not None:
        append_jsonl(stage_dir / "resource-samples.jsonl", sample)
    if (
        host_mem < HOST_MEMORY_AVAILABLE_MIN
        or root_free < DISK_FREE_MIN
        or docker_free < DISK_FREE_MIN
    ):
        fail("host memory or workspace/Docker disk headroom fell below the safety floor")
    if (
        not isinstance(sample["host_cpu_count"], int)
        or sample["host_cpu_count"] < 1
        or load1 > sample["host_cpu_count"] * NODE_CPU_FRACTION_MAX
    ):
        fail("host CPU load exceeded the safety floor")
    for service in services:
        if (
            service["cpu_nano"] > SERVICE_CPU_MAX_NANO
            or service["memory_working_bytes"] > SERVICE_MEMORY_MAX
        ):
            fail(f"Service Pod {service['pod']} exceeded the CPU/memory safety limit")
    for node in node_samples:
        if (
            node["cpu_nano"] > node["cpu_allocatable_nano"] * NODE_CPU_FRACTION_MAX
            or node["memory_available_bytes"] < NODE_MEMORY_AVAILABLE_MIN
        ):
            fail(f"Kind node {node['node']} exceeded the CPU/memory safety limit")
    return sample


def request_json(
    port: int, method: str, path: str, token: str = "", payload: dict | None = None
) -> tuple[int, dict, dict, float]:
    data = json.dumps(payload).encode() if payload is not None else None
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    req = Request(f"http://127.0.0.1:{port}{path}", data=data, headers=headers, method=method)
    started = time.monotonic()
    try:
        with OPENER.open(req, timeout=15) as response:
            status, raw, response_headers = (
                response.status,
                response.read(HTTP_BODY_MAX + 1),
                dict(response.headers.items()),
            )
    except HTTPError as error:
        status, raw, response_headers = (
            error.code,
            error.read(HTTP_BODY_MAX + 1),
            dict(error.headers.items()),
        )
    except (OSError, URLError) as error:
        fail(f"{method} {path} transport outcome is unknown: {type(error).__name__}")
    if len(raw) > HTTP_BODY_MAX:
        fail(f"{method} {path} response exceeds {HTTP_BODY_MAX} bytes; outcome needs review")
    response_headers = {key.lower(): value for key, value in response_headers.items()}
    elapsed = time.monotonic() - started
    try:
        body = json.loads(raw) if raw else {}
    except ValueError:
        body = {"raw_response": raw.decode("utf-8", errors="replace")[:1000]}
    return status, body, response_headers, elapsed


def build_id(key: str) -> str:
    return "idem-" + hashlib.sha256(("kova:e2e\0" + key).encode()).hexdigest()[:20]


def check_runner(blocker_id: str, blocker_uid: str) -> dict:
    runners = kjson(
        "-n", NAMESPACE, "get", "pods", "-l", "app.kubernetes.io/name=kova-runner", "-o", "json"
    )["items"]
    if len(runners) != 1 or runners[0]["metadata"]["name"] != f"kova-job-{blocker_id}":
        fail("queued builds gained a runner Pod or blocker Pod is absent")
    pod = runners[0]
    if (
        pod["spec"].get("nodeSelector", {}).get("never") != "true"
        or pod["spec"].get("nodeName")
        or pod["status"].get("phase") != "Pending"
    ):
        fail("blocker runner is not unscheduled and Pending")
    owners = [
        owner for owner in pod["metadata"].get("ownerReferences", []) if owner.get("controller")
    ]
    if (
        len(owners) != 1
        or owners[0].get("kind") != "KovaBuild"
        or owners[0].get("name") != blocker_id
        or owners[0].get("uid") != blocker_uid
    ):
        fail("blocker runner owner UID does not match the test-owned KovaBuild")
    return pod


def wait_queue_state(
    expected_ids: dict[str, str],
    blocker_id: str,
    blocker_uid: str,
    stage_dir: Path,
    deadline: float,
    on_poll: Callable[[], None] | None = None,
) -> dict:
    expected = set(expected_ids)
    while time.monotonic() < deadline:
        if on_poll is not None:
            on_poll()
        builds = kjson("-n", NAMESPACE, "get", "kovabuilds", "-o", "json")
        by_name = {item["metadata"]["name"]: item for item in builds.get("items", [])}
        if set(by_name) != expected | {blocker_id}:
            fail("KovaBuild set differs from the exact accepted benchmark IDs")
        blocker = by_name[blocker_id]
        if (
            blocker["metadata"]["uid"] != blocker_uid
            or blocker.get("status", {}).get("phase") != "Starting"
        ):
            fail("active blocker phase or UID changed")
        if any(
            by_name[name]["spec"]["idempotencyKey"] != expected_ids[name]
            or by_name[name]["spec"]["source"]["uri"]
            != f"oci://kind-registry:5000/kova-sources/deep-queue@{SOURCE_DIGEST}"
            for name in expected
        ):
            fail("a queued KovaBuild differs from its exact immutable request")
        phases = {name: by_name[name].get("status", {}).get("phase", "") for name in expected}
        if any(phase not in ("", "Queued") for phase in phases.values()):
            fail("a queued KovaBuild entered an unexpected phase")
        check_runner(blocker_id, blocker_uid)
        _, intents = queue_ledgers(len(expected), blocker_id, blocker_uid)
        if set(intents) - expected:
            fail("queue ledger contains an unrecorded intent")
        if all(phase == "Queued" for phase in phases.values()) and set(intents) == expected:
            for name in expected:
                nonce = (
                    by_name[name]["metadata"]
                    .get("annotations", {})
                    .get("kova.cofy.dev/queue-intent")
                )
                if (
                    not isinstance(nonce, str)
                    or len(nonce) != 32
                    or intents[name].get("nonce") != nonce
                ):
                    fail("queued CR nonce and ledger intent differ")
            save_json(stage_dir / "builds-queued.json", builds)
            return builds
        time.sleep(2)
    fail("queued CR status/ledger did not converge before the bounded deadline")


def metric_delta(before: dict[str, float], after: dict[str, float], seconds: float) -> dict:
    delta = {}
    for key in set(before) | set(after):
        amount = after.get(key, 0.0) - before.get(key, 0.0)
        if amount < 0:
            fail("API server request counter reset during the stage")
        if amount:
            delta[key] = amount
    total = sum(delta.values())
    failures = sum(
        count
        for key, count in delta.items()
        if key.rsplit("|", 1)[-1].isdigit()
        and (int(key.rsplit("|", 1)[-1]) == 429 or int(key.rsplit("|", 1)[-1]) >= 500)
    )
    per_verb_resource: dict[str, float] = {}
    for key, count in delta.items():
        verb, group, resource, _ = key.split("|", 3)
        label = f"{verb}|{group}|{resource}"
        per_verb_resource[label] = per_verb_resource.get(label, 0.0) + count
    result = {
        "seconds": seconds,
        "total_requests": total,
        "total_qps": total / seconds,
        "error_429_or_5xx": failures,
        "by_verb_group_resource": per_verb_resource,
    }
    if failures or result["total_qps"] > API_QPS_MAX:
        fail(f"API server crossed the 429/5xx or {API_QPS_MAX} QPS safety limit")
    return result


def percentile(values: list[float], fraction: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    return ordered[max(0, math.ceil(len(ordered) * fraction) - 1)]


def resource_peaks(stage_dir: Path) -> dict:
    samples = [
        json.loads(line) for line in (stage_dir / "resource-samples.jsonl").read_text().splitlines()
    ]
    if not samples:
        fail("stage has no resource samples")
    return {
        "service_cpu_nano_max": max(
            service["cpu_nano"] for sample in samples for service in sample["services"]
        ),
        "service_memory_working_bytes_max": max(
            service["memory_working_bytes"] for sample in samples for service in sample["services"]
        ),
        "node_cpu_nano_max": max(
            node["cpu_nano"] for sample in samples for node in sample["nodes"]
        ),
        "node_memory_working_bytes_max": max(
            node["memory_working_bytes"] for sample in samples for node in sample["nodes"]
        ),
        "node_memory_available_bytes_min": min(
            node["memory_available_bytes"] for sample in samples for node in sample["nodes"]
        ),
        "host_memory_available_bytes_min": min(
            sample["host_memory_available_bytes"] for sample in samples
        ),
        "host_load1_max": max(sample["host_load1"] for sample in samples),
        "workspace_disk_free_bytes_min": min(
            sample["workspace_disk_free_bytes"] for sample in samples
        ),
        "docker_disk_free_bytes_min": min(sample["docker_disk_free_bytes"] for sample in samples),
    }


def observe_blocked_runtime(
    expected_ids: dict[str, str],
    blocker_id: str,
    blocker_uid: str,
    expected_pods: list[dict],
    stage_dir: Path,
    docker_root: Path,
) -> None:
    resource_sample(expected_pods, stage_dir, docker_root)
    if request_json(PORT, "GET", "/readyz")[0] != 200:
        fail("Service readiness failed during the benchmark")
    blocker = kjson("-n", NAMESPACE, "get", "kovabuild", blocker_id, "-o", "json")
    if (
        blocker["metadata"]["uid"] != blocker_uid
        or blocker.get("status", {}).get("phase") != "Starting"
    ):
        fail("active blocker UID or Starting phase changed during the benchmark")
    check_runner(blocker_id, blocker_uid)
    _, intents = queue_ledgers(len(expected_ids), blocker_id, blocker_uid)
    if set(intents) - set(expected_ids):
        fail("queue ledger contains an unrecorded intent during the benchmark")


def guard_api(
    stage_dir: Path, before: dict[str, float], before_time: float, *, force: bool = False
) -> tuple[dict[str, float], float]:
    observed_time = time.monotonic()
    if not force and observed_time - before_time < POLL_SECONDS:
        return before, before_time
    after = api_metrics()
    seconds = time.monotonic() - before_time
    append_jsonl(
        stage_dir / "api-guard.jsonl",
        {
            "timestamp": now(),
            "from": before_time,
            "to": time.monotonic(),
            "before": before,
            "after": after,
        },
    )
    delta = metric_delta(before, after, seconds)
    append_jsonl(stage_dir / "api-guard-summary.jsonl", {"timestamp": now(), **delta})
    return after, time.monotonic()


def preflight() -> dict:
    if sys.platform != "linux":
        fail("deep-queue acceptance runs only on an isolated Linux Kind host")
    if (
        os.environ.get("KIND_CLUSTER", CLUSTER) != CLUSTER
        or os.environ.get("KIND_KUBECONFIG", f".kind/{CLUSTER}.kubeconfig")
        != f".kind/{CLUSTER}.kubeconfig"
    ):
        fail("KIND_CLUSTER/KIND_KUBECONFIG must name the fixed disposable deep-queue Kind cluster")
    if (
        os.environ.get("NAMESPACE", NAMESPACE) != NAMESPACE
        or os.environ.get("RELEASE_NAME", RELEASE) != RELEASE
    ):
        fail("namespace/release overrides do not match the dedicated benchmark")
    for binary in ("kind", "kubectl", "docker", "helm", "git"):
        if shutil.which(binary) is None:
            fail(f"missing required command {binary}")
    fingerprint = exact_kind_identity()
    nodes = good_nodes(kjson("get", "nodes", "-o", "json"))
    deployment = kjson("-n", NAMESPACE, "get", "deployment", f"{RELEASE}-service", "-o", "json")
    principal, platform = single_service_args(deployment)
    pods = service_pods()
    helm_status = json.loads(
        command(
            [
                "helm",
                "status",
                RELEASE,
                "-n",
                NAMESPACE,
                "--kubeconfig",
                str(KUBECONFIG),
                "-o",
                "json",
            ]
        )
    )
    if helm_status.get("info", {}).get("status") != "deployed":
        fail("the dedicated Kova Helm release is not deployed")
    worker = kjson("-n", NAMESPACE, "get", "deployment", RELEASE, "-o", "json")
    if (
        worker.get("spec", {}).get("replicas") != 1
        or worker.get("status", {}).get("readyReplicas") != 1
    ):
        fail("the dedicated worker Deployment is not exactly one ready replica")
    worker_containers = worker["spec"]["template"]["spec"]["containers"]
    service_container = next(
        container
        for container in deployment["spec"]["template"]["spec"]["containers"]
        if container["name"] == "kova-service"
    )
    runner_arg = next(arg for arg in service_container["args"] if arg.startswith("--runner-image="))
    if len(worker_containers) != 1 or worker_containers[0].get("imagePullPolicy") != "Never":
        fail("worker image must be preloaded with imagePullPolicy Never")
    image_facts = {
        "controller": local_image_fact(service_container["image"]),
        "runner": local_image_fact(runner_arg.split("=", 1)[1]),
        "worker": local_image_fact(worker_containers[0]["image"]),
    }
    if len({fact["revision"] for fact in image_facts.values()}) != 1:
        fail("controller, runner, and worker local image revision labels differ")
    if kjson("get", "kovabuilds", "--all-namespaces", "-o", "json")["items"]:
        fail("another KovaBuild exists in the dedicated Kind cluster")
    if kjson(
        "get", "pods", "--all-namespaces", "-l", "app.kubernetes.io/name=kova-runner", "-o", "json"
    )["items"]:
        fail("another runner Pod exists in the dedicated Kind cluster")
    queue_ledgers(0)
    docker_root = Path(command(["docker", "info", "--format", "{{.DockerRootDir}}"]).strip())
    if not docker_root.is_dir():
        fail("Docker data root is unavailable")
    api_metrics()
    # Fail closed before writing if kubelet Pod/node metrics are unavailable
    # or the host is already too busy. This read-only sample writes no file.
    resource_sample(pods, None, docker_root)
    return {
        "cluster": CLUSTER,
        "namespace": NAMESPACE,
        "kubeconfig_fingerprint": fingerprint,
        "node_uids": {node["metadata"]["name"]: node["metadata"]["uid"] for node in nodes},
        "service_pods": {pod["metadata"]["name"]: pod["metadata"]["uid"] for pod in pods},
        "service_pod_image_ids": {
            pod["metadata"]["name"]: next(
                container["imageID"]
                for container in pod["status"]["containerStatuses"]
                if container["name"] == "kova-service"
            )
            for pod in pods
        },
        "local_role_images": image_facts,
        "helm_chart": helm_status.get("chart"),
        "kova_commit": command(["git", "-C", str(ROOT), "rev-parse", "HEAD"]).strip(),
        "principal": principal,
        "platform": platform,
        "docker_root": str(docker_root),
        "pods": pods,
    }


def live(pre: dict, token: str) -> Path:
    run_id = (
        "deep-queue-"
        + datetime.now(timezone.utc).strftime("%Y%m%dt%H%M%S")  # noqa: UP017 (Python 3.10)
        + "-"
        + secrets.token_hex(4)
    )
    run_dir = ROOT / ".work" / "deep-queue" / run_id
    run_dir.mkdir(parents=True, exist_ok=False)
    os.chmod(run_dir, 0o700)
    identity = {key: value for key, value in pre.items() if key != "pods"}
    identity.update(
        {
            "run_id": run_id,
            "started_at": now(),
            "stages": STAGES,
            "steady_seconds": STEADY_SECONDS,
            "poll_seconds": POLL_SECONDS,
            "limits": {
                "service_cpu_nano": SERVICE_CPU_MAX_NANO,
                "service_memory_bytes": SERVICE_MEMORY_MAX,
                "node_memory_available_bytes": NODE_MEMORY_AVAILABLE_MIN,
                "host_memory_available_bytes": HOST_MEMORY_AVAILABLE_MIN,
                "disk_free_bytes": DISK_FREE_MIN,
                "node_cpu_fraction": NODE_CPU_FRACTION_MAX,
                "api_qps": API_QPS_MAX,
            },
        }
    )
    save_json(run_dir / "identity.json", identity)
    expected_ids: dict[str, str] = {}
    forward = None
    overall_deadline = time.monotonic() + RUN_DEADLINE_SECONDS
    success = False
    cleanup_started = False
    try:
        with socket.socket() as probe:
            probe.bind(("127.0.0.1", PORT))
        pod_name = sorted(pre["service_pods"])[0]
        with (run_dir / "port-forward.log").open("w", encoding="utf-8") as log:
            forward = subprocess.Popen(
                [
                    "kubectl",
                    "--kubeconfig",
                    str(KUBECONFIG),
                    "-n",
                    NAMESPACE,
                    "port-forward",
                    "--address",
                    "127.0.0.1",
                    f"pod/{pod_name}",
                    f"{PORT}:8080",
                ],
                stdout=log,
                stderr=subprocess.STDOUT,
            )
        for _ in range(30):
            if forward.poll() is not None:
                fail("Service Pod port-forward exited")
            try:
                if request_json(PORT, "GET", "/readyz")[0] == 200:
                    break
            except BenchError:
                pass
            time.sleep(1)
        else:
            fail("Service Pod did not become ready through the local port-forward")

        source_uri = f"oci://kind-registry:5000/kova-sources/deep-queue@{SOURCE_DIGEST}"
        target_prefix = f"kind-registry:5000/kova-deep-queue/{run_id}"
        blocker_key = run_id + "-blocker"
        blocker_id = build_id(blocker_key)
        blocker_dir = run_dir / "blocker"
        blocker_dir.mkdir()
        append_jsonl(blocker_dir / "candidates.jsonl", {"key": blocker_key, "id": blocker_id})
        blocker_payload = {
            "source_uri": source_uri,
            "source_digest": SOURCE_DIGEST,
            "targets": [{"target": f"{target_prefix}:blocker", "platform": pre["platform"]}],
            "format": "oci",
            "concurrency": 1,
            "idempotency_key": blocker_key,
        }
        status, body, headers, elapsed = request_json(
            PORT, "POST", "/v1/builds", token, blocker_payload
        )
        append_jsonl(
            blocker_dir / "responses.jsonl",
            {
                "id": blocker_id,
                "status": status,
                "body": body,
                "build_id_header": headers.get("x-kova-build-id", ""),
                "elapsed_seconds": elapsed,
            },
        )
        if status != 202 or headers.get("x-kova-build-id", "") != blocker_id:
            fail("blocker POST did not accept its exact expected KovaBuild ID")
        blocker_uid = ""
        for _ in range(60):
            builds = kjson("-n", NAMESPACE, "get", "kovabuilds", "-o", "json")["items"]
            if len(builds) > 1 or (builds and builds[0]["metadata"]["name"] != blocker_id):
                fail("an unexpected KovaBuild appeared while waiting for the blocker")
            if builds:
                blocker = builds[0]
                blocker_uid = blocker["metadata"]["uid"]
                if (
                    blocker["spec"]["idempotencyKey"] != blocker_key
                    or blocker["spec"]["source"]["uri"] != source_uri
                ):
                    fail("blocker CR identity differs from the accepted request")
                if blocker.get("status", {}).get("phase") == "Starting":
                    runners = kjson(
                        "-n",
                        NAMESPACE,
                        "get",
                        "pods",
                        "-l",
                        "app.kubernetes.io/name=kova-runner",
                        "-o",
                        "json",
                    )["items"]
                    if runners:
                        pod = check_runner(blocker_id, blocker_uid)
                        _, intents = queue_ledgers(1, blocker_id, blocker_uid)
                    else:
                        intents = {"pending": True}
                    if not intents and runners:
                        save_json(blocker_dir / "build.json", blocker)
                        save_json(blocker_dir / "runner.json", pod)
                        break
            time.sleep(1)
        else:
            fail("blocker did not acquire exactly one active grant and Pending owned runner")

        previous = 0
        for target in STAGES:
            if time.monotonic() >= overall_deadline:
                fail("overall benchmark duration exceeded one hour")
            stage_dir = run_dir / f"stage-{target:04d}"
            stage_dir.mkdir()
            note(f"stage {target}: adding {target - previous} queued builds")
            guard_metrics = api_metrics()
            guard_at = time.monotonic()

            def stage_guard(force_api: bool = False) -> None:
                nonlocal guard_metrics, guard_at
                observe_blocked_runtime(
                    expected_ids,
                    blocker_id,
                    blocker_uid,
                    pre["pods"],
                    stage_dir,
                    Path(pre["docker_root"]),
                )
                guard_metrics, guard_at = guard_api(
                    stage_dir, guard_metrics, guard_at, force=force_api
                )

            stage_guard()
            submission_started = time.monotonic()
            latencies = []
            for index in range(previous + 1, target + 1):
                if (
                    time.monotonic() >= overall_deadline
                    or time.monotonic() - submission_started > 600
                ):
                    fail(f"stage {target} admission exceeded its bounded deadline")
                key = f"{run_id}-queued-{index:04d}"
                ident = build_id(key)
                append_jsonl(
                    stage_dir / "candidates.jsonl", {"index": index, "key": key, "id": ident}
                )
                payload = {
                    "source_uri": source_uri,
                    "source_digest": SOURCE_DIGEST,
                    "targets": [
                        {
                            "target": f"{target_prefix}:queued-{index:04d}",
                            "platform": pre["platform"],
                        }
                    ],
                    "format": "oci",
                    "concurrency": 1,
                    "idempotency_key": key,
                }
                status, body, headers, elapsed = request_json(
                    PORT, "POST", "/v1/builds", token, payload
                )
                observed_id = headers.get("x-kova-build-id", "")
                append_jsonl(
                    stage_dir / "responses.jsonl",
                    {
                        "index": index,
                        "id": ident,
                        "status": status,
                        "body": body,
                        "build_id_header": observed_id,
                        "elapsed_seconds": elapsed,
                    },
                )
                if status != 202 or observed_id != ident:
                    fail(f"stage {target} request {index} returned HTTP {status} or wrong ID")
                expected_ids[ident] = key
                latencies.append(elapsed)
                if index % 10 == 0 or elapsed > 1:
                    stage_guard()
            submitted_at = time.monotonic()
            wait_queue_state(
                expected_ids,
                blocker_id,
                blocker_uid,
                stage_dir,
                min(submitted_at + SETTLE_DEADLINE_SECONDS, overall_deadline),
                stage_guard,
            )
            settled_at = time.monotonic()
            stage_guard(force_api=True)
            before = api_metrics()
            save_json(stage_dir / "api-before.json", before)
            measured_start = time.monotonic()
            for _ in range(STEADY_SECONDS // POLL_SECONDS):
                time.sleep(POLL_SECONDS)
                stage_guard(force_api=True)
            measured_seconds = time.monotonic() - measured_start
            after = api_metrics()
            save_json(stage_dir / "api-after.json", after)
            api = metric_delta(before, after, measured_seconds)
            wait_queue_state(expected_ids, blocker_id, blocker_uid, stage_dir, time.monotonic() + 1)
            summary = {
                "stage": target,
                "timestamp": now(),
                "accepted_this_stage": target - previous,
                "accepted_total": target,
                "submission_seconds": submitted_at - submission_started,
                "submit_throughput_per_second": (target - previous)
                / (submitted_at - submission_started),
                "queue_settle_seconds_after_last_submit": settled_at - submitted_at,
                "post_p50_seconds": percentile(latencies, 0.50),
                "post_p95_seconds": percentile(latencies, 0.95),
                "post_p99_seconds": percentile(latencies, 0.99),
                "api": api,
                "resource_peaks": resource_peaks(stage_dir),
                "reconcile_duration_p95_p99": None,
                "reconcile_duration_note": (
                    "Not exposed by controller; POST/queue-settle is not reconcile latency."
                ),
            }
            save_json(stage_dir / "summary.json", summary)
            note(
                f"stage {target} PASS: API {api['total_qps']:.1f}/s; "
                f"POST p99 {summary['post_p99_seconds']:.3f}s"
            )
            previous = target

        # Cleanup is exact-ID only. Unknown outcomes or any stage failure keep
        # all CRs and ledgers for operator evidence instead of guessing.
        cleanup_dir = run_dir / "cleanup"
        cleanup_dir.mkdir()
        wait_queue_state(expected_ids, blocker_id, blocker_uid, cleanup_dir, time.monotonic() + 1)
        cleanup_started = True
        cleanup_metrics = api_metrics()
        cleanup_at = time.monotonic()
        for start in range(0, len(expected_ids), 20):
            names = list(expected_ids)[start : start + 20]
            append_jsonl(cleanup_dir / "delete-batches.jsonl", {"ids": names, "started_at": now()})
            output = kctl(
                "-n", NAMESPACE, "delete", "kovabuild", *names, "--wait=false", timeout=60
            )
            append_jsonl(
                cleanup_dir / "delete-batches.jsonl",
                {"ids": names, "output": output, "completed_at": now()},
            )
            if start % 100 == 0:
                observe_blocked_runtime(
                    expected_ids,
                    blocker_id,
                    blocker_uid,
                    pre["pods"],
                    cleanup_dir,
                    Path(pre["docker_root"]),
                )
                cleanup_metrics, cleanup_at = guard_api(cleanup_dir, cleanup_metrics, cleanup_at)
        cleanup_deadline = time.monotonic() + CLEANUP_DEADLINE_SECONDS
        while time.monotonic() < cleanup_deadline:
            builds = kjson("-n", NAMESPACE, "get", "kovabuilds", "-o", "json")["items"]
            names = {item["metadata"]["name"] for item in builds}
            if names - (set(expected_ids) | {blocker_id}):
                fail("an unrecorded KovaBuild appeared during exact cleanup")
            _, intents = queue_ledgers(len(expected_ids), blocker_id, blocker_uid)
            check_runner(blocker_id, blocker_uid)
            if names == {blocker_id} and not intents:
                break
            observe_blocked_runtime(
                expected_ids,
                blocker_id,
                blocker_uid,
                pre["pods"],
                cleanup_dir,
                Path(pre["docker_root"]),
            )
            cleanup_metrics, cleanup_at = guard_api(cleanup_dir, cleanup_metrics, cleanup_at)
            time.sleep(3)
        else:
            fail("queued CR cleanup did not drain within its bounded deadline")
        blocker = kjson("-n", NAMESPACE, "get", "kovabuild", blocker_id, "-o", "json")
        if blocker["metadata"]["uid"] != blocker_uid:
            fail("blocker UID changed before exact deletion")
        append_jsonl(
            cleanup_dir / "delete-batches.jsonl", {"ids": [blocker_id], "started_at": now()}
        )
        output = kctl("-n", NAMESPACE, "delete", "kovabuild", blocker_id, "--wait=false")
        append_jsonl(
            cleanup_dir / "delete-batches.jsonl",
            {"ids": [blocker_id], "output": output, "completed_at": now()},
        )
        while time.monotonic() < cleanup_deadline:
            if (
                not kjson("-n", NAMESPACE, "get", "kovabuilds", "-o", "json")["items"]
                and not kjson(
                    "-n",
                    NAMESPACE,
                    "get",
                    "pods",
                    "-l",
                    "app.kubernetes.io/name=kova-runner",
                    "-o",
                    "json",
                )["items"]
            ):
                queue_ledgers(0)
                break
            resource_sample(pre["pods"], cleanup_dir, Path(pre["docker_root"]))
            cleanup_metrics, cleanup_at = guard_api(cleanup_dir, cleanup_metrics, cleanup_at)
            time.sleep(3)
        else:
            fail("blocker Pod/ledger cleanup did not finish within its bounded deadline")
        save_json(
            run_dir / "result.json",
            {
                "status": "passed",
                "completed_at": now(),
                "stages": STAGES,
                "exact_cr_cleanup": True,
                "cluster_deleted": False,
                "registry_touched": False,
            },
        )
        success = True
        return run_dir
    except BaseException as error:
        save_json(
            run_dir / "result.json",
            {
                "status": "failed",
                "completed_at": now(),
                "error": str(error),
                "known_accepted_ids": list(expected_ids),
                "unknown_outcome_candidates": "see blocker and stage candidate receipts",
                "cleanup_started": cleanup_started,
                "cleanup_may_be_partial": cleanup_started,
                "cluster_deleted": False,
                "registry_touched": False,
            },
        )
        raise
    finally:
        if forward is not None:
            forward.terminate()
            try:
                forward.wait(timeout=10)
            except subprocess.TimeoutExpired:
                forward.kill()
                forward.wait(timeout=5)
        if success:
            note(f"PASS; receipts preserved at {run_dir}")
        else:
            note(f"FAILED; receipts and test-owned CRs retained at {run_dir}; inspect first")


def main() -> None:
    mode = os.environ.get("DEEP_QUEUE_E2E_MODE", "check")
    if mode not in ("check", "run"):
        fail("DEEP_QUEUE_E2E_MODE must be check or run")
    if (
        mode == "run"
        and os.environ.get("DEEP_QUEUE_E2E_ACK") != f"{CLUSTER}/{NAMESPACE}/{RELEASE}-service"
    ):
        fail(f"run mode requires DEEP_QUEUE_E2E_ACK={CLUSTER}/{NAMESPACE}/{RELEASE}-service")
    pre = preflight()
    note(
        f"read-only preflight passed: {CLUSTER}; 2/2 nodes; two Service Pods; empty ledgers"
    )
    if mode == "check":
        note(
            "no writes performed; explicit run mode needs DEEP_QUEUE_E2E_ACK and SERVICE_AUTH_TOKEN"
        )
        return
    token = os.environ.get("SERVICE_AUTH_TOKEN", "")
    if not token:
        fail("SERVICE_AUTH_TOKEN is required in run mode")
    live(pre, token)


if __name__ == "__main__":
    try:
        main()
    except (BenchError, KeyboardInterrupt) as error:
        note(f"error: {error}")
        sys.exit(1)
