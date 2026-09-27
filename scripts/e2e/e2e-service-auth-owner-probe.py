#!/usr/bin/env python3
"""Bounded owner/non-owner GET authorization probe on a fresh TokenReview Kind.

The default ``check`` mode is read-only.  ``run`` creates exactly one
unschedulable, run-scoped KovaBuild through the authenticated Service API,
probes its owner and an unauthorized second principal, then deletes only that
build with a Kubernetes UID precondition.  ``recover`` can finish that exact
cleanup after an interrupted run; an unresolved POST with no matching CR is
left for operator review.  Bearers and untrusted HTTP bodies are never saved.
"""

from __future__ import annotations

import argparse
import fcntl
import importlib.util
import json
import math
import os
import re
import secrets
import signal
import socket
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.request import Request

HERE = Path(__file__).resolve().parent


def load_script(module_name: str, filename: str):
    spec = importlib.util.spec_from_file_location(module_name, HERE / filename)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


fair = load_script("owner_probe_fairness_fixture", "e2e-service-admission-fairness.py")
read = load_script("owner_probe_read_fixture", "e2e-service-auth-read-probe.py")

EVIDENCE_ROOT = Path("/data/forge-artifacts/kova-admission-fairness")
ACK = "kova-admission-fairness/kova/kova-service/auth-owner"
RUN_ID = re.compile(r"owner-get-[0-9]{8}t[0-9]{6}z-[0-9a-f]{8}\Z")
SHA = re.compile(r"[0-9a-f]{40}\Z")
MAX_OWNER_REQUESTS = 60
MAX_NONOWNER_REQUESTS = 30
MAX_QPS = 4.0
MAX_RUN_SECONDS = 300
PHASE = "a1"


def require(condition: bool, reason: str) -> None:
    fair.require(condition, reason)


def now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")  # noqa: UP017


def save(directory: Path, name: str, payload: object) -> None:
    fair.save(directory / name, payload)


def case_for(run_id: str, platform: str) -> dict:
    require(RUN_ID.fullmatch(run_id) is not None, "run ID is not exact")
    require(platform == "linux/amd64", "fixture platform differs")
    requester = fair.principal("a")
    key = f"{run_id}-a1"
    case = {
        "label": PHASE,
        "requester": requester,
        "key": key,
        "id": fair.build_id(requester, key),
        "source_uri": f"oci://kind-registry:5000/kova-sources/{run_id}@{fair.SOURCE_DIGEST}",
        "source_digest": fair.SOURCE_DIGEST,
        "target": f"kind-registry:5000/kova-auth-owner/{run_id}:a1",
        "platform": platform,
    }
    require(fair.failover_io.BUILD_ID.fullmatch(case["id"]) is not None, "build ID is invalid")
    return case


def fresh_preflight(candidate_commit: str) -> tuple[dict, dict]:
    """Refuse quickstart/static auth, reused cursor, foreign work, or image drift."""
    identity = fair.fixture_identity(empty=True, candidate_commit=candidate_commit)
    baseline = read.empty_and_healthy(identity)
    require(baseline["fairness_cursor"] == "", "fixture fairness cursor is not fresh")
    require(identity["candidate_commit"] == candidate_commit, "candidate image commit changed")
    sample_capacity(identity, baseline)
    for who in ("a", "b"):
        require(
            not virtual_get_granted(who, identity),
            "a test principal has virtual Service GET permission; owner bypass cannot be isolated",
        )
    return identity, baseline


def virtual_get_granted(which: str, identity: dict) -> bool:
    """Use the actual ServiceAccount UID and standard TokenReview groups."""
    require(which in fair.SA, "virtual GET identity is not a fixture ServiceAccount")
    uid = identity["service_account_uids"][which]
    require(
        isinstance(uid, str) and fair.UID.fullmatch(uid) is not None,
        "fixture ServiceAccount UID is invalid",
    )
    username = fair.principal(which)
    try:
        result = subprocess.run(
            [
                "kubectl",
                "--kubeconfig",
                str(fair.KUBECONFIG),
                "--request-timeout=15s",
                "-n",
                fair.NAMESPACE,
                "auth",
                "can-i",
                "get",
                "servicebuilds.kova.cofy.dev",
                f"--as={username}",
                f"--as-uid={uid}",
                "--as-group=system:serviceaccounts",
                f"--as-group=system:serviceaccounts:{fair.NAMESPACE}",
                "--as-group=system:authenticated",
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True,
            check=False,
            timeout=20,
        )
    except (OSError, subprocess.TimeoutExpired) as error:
        raise fair.SafetyError(f"virtual GET RBAC check failed: {type(error).__name__}") from None
    require(
        result.returncode in (0, 1)
        and result.stdout.strip() in ("yes", "no")
        and (result.returncode == 0) == (result.stdout.strip() == "yes"),
        "virtual GET RBAC check is inconclusive",
    )
    return result.stdout.strip() == "yes"


def same_identity(identity: dict) -> None:
    current = fair.fixture_identity(empty=False, candidate_commit=identity["candidate_commit"])
    for key in identity:
        if key in ("service_local_image", "service_initial_image_bindings"):
            continue
        require(current.get(key) == identity[key], f"fixture identity drifted: {key}")
    require(
        current["service_config_digest"] == identity["service_config_digest"],
        "Service image config digest changed",
    )


def allocatable_cpu_cores(value: str) -> float:
    try:
        cores = float(value[:-1]) / 1000 if value.endswith("m") else float(value)
    except (AttributeError, TypeError, ValueError) as error:
        raise fair.SafetyError(
            f"node CPU allocatable is malformed: {type(error).__name__}"
        ) from None
    require(math.isfinite(cores) and cores > 0, "node CPU allocatable is invalid")
    return cores


def sample_capacity(identity: dict, baseline: dict) -> dict:
    """Keep CPU, memory, Pod and exact Service Pod-UID protection during the run."""
    same_identity(identity)
    node_names = [item["name"] for item in identity["node_facts"]]
    pods = fair.kjson("get", "pods", "--all-namespaces", "-o", "json").get("items", [])
    require(isinstance(pods, list), "Pod capacity list is malformed")
    counts = dict.fromkeys(node_names, 0)
    for pod in pods:
        node = pod.get("spec", {}).get("nodeName")
        if node in counts and pod.get("metadata", {}).get("deletionTimestamp") is None:
            counts[node] += 1
    capacities = {}
    for node in identity["node_facts"]:
        maximum = int(node["allocatable"]["pods"])
        require(maximum > 0 and counts[node["name"]] / maximum < 0.8, "Kind Pod capacity is unsafe")
        capacities[node["name"]] = {"used": counts[node["name"]], "allocatable": maximum}
    raw = fair.command(
        ["docker", "stats", "--no-stream", "--format", "{{json .}}", *sorted(node_names)],
        timeout=20,
    )
    docker = {}
    for line in raw.splitlines():
        try:
            item = json.loads(line)
            name = item["Name"]
            cpu = float(item["CPUPerc"].removesuffix("%"))
            memory = float(item["MemPerc"].removesuffix("%"))
        except (KeyError, TypeError, ValueError, AttributeError) as error:
            raise fair.SafetyError(f"Docker stats are malformed: {type(error).__name__}") from None
        require(name in counts and name not in docker, "Docker stats node identity changed")
        require(math.isfinite(cpu) and cpu >= 0, "Kind node CPU sample is invalid")
        require(math.isfinite(memory) and memory < 90, "Kind node memory is unsafe")
        node = next(item for item in identity["node_facts"] if item["name"] == name)
        allocated_cores = allocatable_cpu_cores(node["allocatable"]["cpu"])
        cpu_fraction = cpu / (100 * allocated_cores)
        require(cpu_fraction < 0.8, "Kind node CPU is unsafe")
        docker[name] = {
            "cpu_percent": cpu,
            "cpu_allocatable_fraction": round(cpu_fraction, 6),
            "memory_percent": memory,
            "memory_usage": item.get("MemUsage"),
        }
    require(set(docker) == set(node_names), "Docker stats omit an exact Kind node")
    cpu_count = os.cpu_count()
    require(isinstance(cpu_count, int) and cpu_count > 0, "host CPU capacity is unavailable")
    load_1m = os.getloadavg()[0]
    require(math.isfinite(load_1m) and load_1m / cpu_count < 0.8, "host CPU load is unsafe")
    current_pods = fair.kjson(
        "-n",
        fair.NAMESPACE,
        "get",
        "pods",
        "-l",
        "app.kubernetes.io/instance=kova,app.kubernetes.io/component=service",
        "-o",
        "json",
    ).get("items", [])
    service = []
    for pod in current_pods:
        statuses = [
            item
            for item in pod.get("status", {}).get("containerStatuses", [])
            if item.get("name") == "kova-service"
        ]
        require(len(statuses) == 1, "Service container changed")
        service.append(
            {
                "name": pod["metadata"]["name"],
                "uid": pod["metadata"]["uid"],
                "node": pod["spec"].get("nodeName"),
                "image_id": statuses[0]["imageID"],
                "restarts": statuses[0].get("restartCount", 0),
            }
        )
    service.sort(key=lambda item: item["name"])
    require(service == baseline["service_pods"], "Service Pod UID, image or restarts changed")
    return {
        "at": now(),
        "pod_capacity": capacities,
        "docker_nodes": docker,
        "host_load_1m": load_1m,
        "host_cpu_count": cpu_count,
        "service_pods": service,
    }


def observe_owned(state: dict, baseline: dict) -> tuple[dict, dict]:
    require(state.get("uid") is not None, "owned KovaBuild UID is not recorded")
    health = sample_capacity(state["identity"], baseline)
    builds = fair.kjson("get", "kovabuilds", "--all-namespaces", "-o", "json").get("items", [])
    runners = fair.kjson(
        "get", "pods", "--all-namespaces", "-l", "app.kubernetes.io/name=kova-runner", "-o", "json"
    ).get("items", [])
    active = fair.ledger_data(
        fair.kjson(
            "-n", fair.NAMESPACE, "get", "configmap", "kova-service-admission", "-o", "json"
        ),
        "reservations.json",
    )
    queue = fair.ledger_data(
        fair.kjson(
            "-n", fair.NAMESPACE, "get", "configmap", "kova-service-queue-admission", "-o", "json"
        ),
        "queue.json",
    )
    require(isinstance(builds, list) and isinstance(runners, list), "workload list is malformed")
    view = fair.validate_observation(
        builds,
        runners,
        active,
        queue,
        {
            "cases": {PHASE: state["case"]},
            "accepted": {PHASE: state["uid"]},
            "deleting": [PHASE] if state.get("deleting") else [],
            "deleted": [PHASE] if state.get("deleted") else [],
            "identity": state["identity"],
        },
    )
    return view, health


def issue_tokens(identity: dict) -> dict[str, str]:
    return read.issue_tokens(identity)


def create_owned(state: dict, directory: Path, token: str, port: int) -> None:
    case = state["case"]
    require(
        state["uid"] is None and not state["attempting_post"], "POST has already been attempted"
    )
    state["attempting_post"] = True
    save(directory, "state.json", state)
    payload = {
        "source_uri": case["source_uri"],
        "source_digest": case["source_digest"],
        "targets": [{"target": case["target"], "platform": case["platform"]}],
        "format": "oci",
        "concurrency": 1,
        "idempotency_key": case["key"],
    }
    request = Request(
        f"http://127.0.0.1:{port}/v1/builds",
        data=json.dumps(payload, separators=(",", ":")).encode(),
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        method="POST",
    )
    started = time.monotonic()
    try:
        status, headers, body = fair.failover_io.bounded_response(request, timeout=15)
    except fair.failover_io.SafetyError:
        save(directory, "post.json", {"at": now(), "outcome": "unknown", "expected_id": case["id"]})
        raise
    require(token.encode() not in body, "Service reflected a bearer; POST body withheld")
    header_id = next(
        (value for key, value in headers.items() if key.lower() == "x-kova-build-id"), None
    )
    save(
        directory,
        "post.json",
        {
            "at": now(),
            "http_status": status,
            "expected_id": case["id"],
            "header_matches_expected": header_id == case["id"],
            "response_bytes": len(body),
            "elapsed_seconds": round(time.monotonic() - started, 6),
        },
    )
    require(status == 202 and header_id == case["id"], "POST was not newly accepted")
    build = fair.kjson("-n", fair.NAMESPACE, "get", "kovabuild", case["id"], "-o", "json")
    require(
        fair.expected_spec_matches(build, case, state["identity"]["service_account_uids"]["a"]),
        "accepted KovaBuild differs from the exact requester and input",
    )
    uid = build.get("metadata", {}).get("uid", "")
    require(
        isinstance(uid, str) and fair.UID.fullmatch(uid) is not None, "accepted CR UID is invalid"
    )
    state["uid"] = uid
    state["attempting_post"] = False
    save(directory, "state.json", state)


def wait_owned(state: dict, directory: Path, baseline: dict, deadline: float) -> list[dict]:
    samples = []
    while time.monotonic() < deadline:
        view, health = observe_owned(state, baseline)
        samples.append(health)
        if fair.stage_matches(view, active=PHASE, queued=(), cursor="a"):
            save(directory, "owned-ready.json", fair.projection(view))
            save(directory, "owned-ready-health.json", health)
            return samples
        time.sleep(1)
    raise fair.SafetyError("owned build did not reach the exact unschedulable Starting state")


def get_build(which: str, token: str, port: int, case: dict) -> dict:
    request = Request(
        f"http://127.0.0.1:{port}/v1/builds/{case['id']}",
        headers={"Authorization": f"Bearer {token}"},
        method="GET",
    )
    started = time.monotonic()
    status, _, body = fair.failover_io.bounded_response(request, timeout=5)
    elapsed = time.monotonic() - started
    require(token.encode() not in body, "Service reflected a bearer; GET body withheld")
    try:
        payload = json.loads(body)
    except (UnicodeError, ValueError) as error:
        raise fair.SafetyError(f"GET response JSON is malformed: {type(error).__name__}") from None
    if which == "a":
        require(
            status == 200
            and isinstance(payload, dict)
            and payload.get("id") == case["id"]
            and payload.get("requester") == case["requester"]
            and payload.get("source_digest") == case["source_digest"]
            and payload.get("idempotency_key") == case["key"],
            "owner GET did not return only the expected build",
        )
    else:
        require(
            status == 403
            and isinstance(payload, dict)
            and payload.get("code") == "forbidden"
            and payload.get("retryable") is False
            and all(
                marker.encode() not in body
                for marker in (case["id"], case["requester"], case["source_uri"], case["key"])
            ),
            "non-owner GET was not a non-disclosing, definitive denial",
        )
    return {
        "at": now(),
        "principal": which,
        "http_status": status,
        "response_bytes": len(body),
        "elapsed_seconds": round(elapsed, 6),
    }


def request_phase(
    state: dict,
    directory: Path,
    baseline: dict,
    tokens: dict[str, str],
    which: str,
    count: int,
    qps: float,
    deadline: float,
) -> tuple[list[dict], list[dict], float]:
    receipts = []
    health_samples = []
    started = time.monotonic()
    next_earliest = started
    for index in range(count):
        require(time.monotonic() < deadline, "owner auth probe exceeded its five-minute bound")
        if index % 5 == 0:
            view, health = observe_owned(state, baseline)
            require(
                fair.stage_matches(view, active=PHASE, queued=(), cursor="a"),
                "owned fixture changed during GETs",
            )
            save(directory, f"health-{which}-{index // 5 + 1:03d}.json", health)
            health_samples.append(health)
        # Schedule from the completion of the previous request, not from the
        # phase start. A slow health sample must never cause a catch-up burst.
        current = time.monotonic()
        delay = max(0.0, next_earliest - current)
        require(current + delay < deadline, "next GET would exceed the five-minute bound")
        if delay > 0:
            time.sleep(delay)
        require(time.monotonic() < deadline, "owner auth probe exceeded its five-minute bound")
        port = fair.PORT["a" if index % 2 == 0 else "b"]
        try:
            receipt = get_build(which, tokens[which], port, state["case"])
            next_earliest = time.monotonic() + 1.0 / qps
            receipt.update({"index": index + 1, "pod": baseline["service_pods"][index % 2]["name"]})
            save(directory, f"request-{which}-{index + 1:03d}.json", receipt)
            receipts.append(receipt)
        except Exception as error:
            save(
                directory,
                f"request-{which}-{index + 1:03d}.json",
                {
                    "at": now(),
                    "principal": which,
                    "index": index + 1,
                    "outcome": "failed",
                    "error_type": type(error).__name__,
                },
            )
            raise
    return receipts, health_samples, time.monotonic() - started


def review_deltas(before: dict, after: dict, expected_token: int, expected_sar: int) -> dict:
    first = {tuple(sorted(row["labels"].items())): row["value"] for row in before["series"]}
    second = {tuple(sorted(row["labels"].items())): row["value"] for row in after["series"]}
    totals = {"tokenreviews": 0, "subjectaccessreviews": 0}
    rows = []
    for key in sorted(first.keys() | second.keys()):
        start, end = first.get(key, 0), second.get(key, 0)
        require(end >= start, "API review counter reset or disappeared")
        change = end - start
        require(float(change).is_integer(), "API review counter has a fractional delta")
        labels = dict(key)
        resource = labels.get("resource")
        require(resource in totals, "unexpected review resource in metrics")
        totals[resource] += int(change)
        rows.append({"labels": labels, "before": start, "after": end, "delta": int(change)})
    require(
        totals == {"tokenreviews": expected_token, "subjectaccessreviews": expected_sar},
        "cluster-wide review counters were noisy or request review counts differed",
    )
    return {"totals": totals, "series": rows}


def percentile(values: list[float], level: float) -> float:
    return read.percentile(values, level)


def phase_summary(receipts: list[dict], elapsed: float, reviews: dict) -> dict:
    latencies = [row["elapsed_seconds"] for row in receipts]
    return {
        "requests": len(receipts),
        "success_rate": 1.0,
        "elapsed_seconds": round(elapsed, 6),
        "throughput_per_second": round(len(receipts) / elapsed, 6),
        "latency_seconds": {
            "p50": percentile(latencies, 0.5),
            "p95": percentile(latencies, 0.95),
            "p99": percentile(latencies, 0.99),
            "max": max(latencies),
        },
        "api_reviews": reviews,
    }


def cleanup_owned(state: dict, directory: Path, baseline: dict) -> None:
    """Never use delete --all or a name-only deletion."""
    require(state.get("uid") is not None, "cleanup requires a persisted exact CR UID")
    view, health = observe_owned(state, baseline)
    save(directory, "cleanup-before-health.json", health)
    if PHASE not in view["builds"]:
        require(state.get("deleting"), "owned build disappeared without a recorded delete")
    else:
        build = view["builds"][PHASE]
        require(build["metadata"]["uid"] == state["uid"], "owned build UID changed")
        if build["metadata"].get("deletionTimestamp") is None:
            if PHASE in view["active"]:
                active = fair.ledger_data(
                    fair.kjson(
                        "-n",
                        fair.NAMESPACE,
                        "get",
                        "configmap",
                        "kova-service-admission",
                        "-o",
                        "json",
                    ),
                    "reservations.json",
                )
                require(
                    active["active"][state["uid"]].get("inFlight") in (None, []),
                    "active grant has an unresolved runner Create",
                )
            lease = fair.kjson("-n", fair.NAMESPACE, "get", "lease", fair.LEASE, "-o", "json")
            require(
                lease["metadata"]["uid"] == state["identity"]["lease_uid"],
                "admission Lease UID changed",
            )
            state["deleting"] = True
            save(directory, "state.json", state)
            args = argparse.Namespace(
                kubeconfig=fair.KUBECONFIG,
                kubeconfig_sha256=state["identity"]["kubeconfig_sha256"],
                cluster=fair.CLUSTER,
                namespace=fair.NAMESPACE,
                resource="kovabuilds",
                name=state["case"]["id"],
                uid=state["uid"],
                lease_name=fair.LEASE,
                lease_uid=lease["metadata"]["uid"],
                lease_holder=lease["spec"]["holderIdentity"],
                receipt=directory / "delete-owned.json",
            )
            fair.failover_io.uid_delete(args)
    deadline = time.monotonic() + 120
    while time.monotonic() < deadline:
        view, health = observe_owned(state, baseline)
        if not any((view["builds"], view["pods"], view["active"], view["queue"])):
            save(directory, "cleanup-after-health.json", health)
            final = read.empty_and_healthy(state["identity"])
            require(
                final["service_pods"] == baseline["service_pods"]
                and final["fairness_cursor"] == fair.sha256(fair.principal("a")),
                "post-cleanup Service or fairness cursor changed unexpectedly",
            )
            save(directory, "health-final.json", final)
            state["deleted"] = True
            save(directory, "state.json", state)
            return
        time.sleep(1)
    raise fair.SafetyError("exact owned KovaBuild did not finish deletion within two minutes")


def run_probe(args: argparse.Namespace, directory: Path, state: dict, baseline: dict) -> dict:
    tokens = issue_tokens(state["identity"])
    forwards = fair.Forwards(directory)
    health_samples = []
    deadline = time.monotonic() + MAX_RUN_SECONDS
    try:
        forwards.start([pod["name"] for pod in baseline["service_pods"]], "owner-get")
        create_owned(state, directory, tokens["a"], fair.PORT["a"])
        health_samples += wait_owned(
            state, directory, baseline, min(deadline, time.monotonic() + 60)
        )
        before_owner = read.api_metrics(allow_unseen=True)
        save(directory, "apiserver-before-owner.json", before_owner)
        owner, health, owner_elapsed = request_phase(
            state, directory, baseline, tokens, "a", args.owner_requests, args.qps, deadline
        )
        health_samples += health
        after_owner = read.api_metrics(allow_unseen=True)
        save(directory, "apiserver-after-owner.json", after_owner)
        owner_reviews = review_deltas(before_owner, after_owner, len(owner), 0)
        nonowner, health, nonowner_elapsed = request_phase(
            state, directory, baseline, tokens, "b", args.nonowner_requests, args.qps, deadline
        )
        health_samples += health
        after_nonowner = read.api_metrics(allow_unseen=True)
        save(directory, "apiserver-after-nonowner.json", after_nonowner)
        nonowner_reviews = review_deltas(after_owner, after_nonowner, len(nonowner), len(nonowner))
        view, final_health = observe_owned(state, baseline)
        require(
            fair.stage_matches(view, active=PHASE, queued=(), cursor="a"),
            "owned fixture changed after GETs",
        )
        health_samples.append(final_health)
        save(directory, "health-before-cleanup.json", final_health)
        result = {
            "outcome": "measurement-complete-cleanup-pending",
            "completed_at": now(),
            "candidate_commit": state["identity"]["candidate_commit"],
            "build_id": state["case"]["id"],
            "build_uid": state["uid"],
            "owner": phase_summary(owner, owner_elapsed, owner_reviews),
            "nonowner": phase_summary(nonowner, nonowner_elapsed, nonowner_reviews),
            "host_load_1m_peak": max(sample["host_load_1m"] for sample in health_samples),
            "node_cpu_percent_peak": {
                name: max(sample["docker_nodes"][name]["cpu_percent"] for sample in health_samples)
                for name in health_samples[0]["docker_nodes"]
            },
            "node_cpu_allocatable_fraction_peak": {
                name: max(
                    sample["docker_nodes"][name]["cpu_allocatable_fraction"]
                    for sample in health_samples
                )
                for name in health_samples[0]["docker_nodes"]
            },
            "node_memory_percent_peak": {
                name: max(
                    sample["docker_nodes"][name]["memory_percent"] for sample in health_samples
                )
                for name in health_samples[0]["docker_nodes"]
            },
            "pod_capacity_peak": {
                name: max(sample["pod_capacity"][name]["used"] for sample in health_samples)
                for name in health_samples[0]["pod_capacity"]
            },
            "limitations": [
                "Only this isolated two-Pod TokenReview Kind fixture at a bounded GET rate.",
                "API-server review counters are cluster-wide; exact deltas require "
                "no unrelated review traffic.",
                "Unschedulable build is a read-path fixture, not a build-throughput result.",
                "Not a production SLA.",
            ],
        }
        save(directory, "result.json", result)
        return result
    finally:
        forwards.close()
        tokens.clear()
        if state.get("uid") is not None and not state.get("deleted"):
            cleanup_owned(state, directory, baseline)


def load_state(run_id: str) -> tuple[Path, dict]:
    require(RUN_ID.fullmatch(run_id) is not None, "recovery run ID is not exact")
    directory = EVIDENCE_ROOT / run_id
    require(
        directory.is_dir() and not directory.is_symlink(),
        "recovery directory is absent or a symlink",
    )
    path = directory / "state.json"
    require(
        path.is_file() and not path.is_symlink() and path.stat().st_size <= 65536,
        "recovery state is unavailable",
    )
    try:
        state = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, ValueError) as error:
        raise fair.SafetyError(f"recovery state is malformed: {type(error).__name__}") from None
    require(
        isinstance(state, dict)
        and state.get("run_id") == run_id
        and isinstance(state.get("identity"), dict)
        and state["identity"].get("cluster") == fair.CLUSTER
        and state["identity"].get("namespace") == fair.NAMESPACE
        and state.get("case") == case_for(run_id, state["identity"]["platform"]),
        "recovery state identity or planned case changed",
    )
    return directory, state


def recover(run_id: str) -> Path:
    directory, state = load_state(run_id)
    same_identity(state["identity"])
    if state.get("deleted"):
        final = read.empty_and_healthy(state["identity"])
        require(
            final["service_pods"] == state["baseline"]["service_pods"]
            and final["fairness_cursor"] == fair.sha256(fair.principal("a")),
            "recovered fixture changed",
        )
        return directory
    uid = state.get("uid")
    if uid is None:
        require(state.get("attempting_post"), "no accepted or uncertain POST is recorded")
        try:
            build = fair.kjson(
                "-n", fair.NAMESPACE, "get", "kovabuild", state["case"]["id"], "-o", "json"
            )
        except fair.SafetyError:
            raise fair.SafetyError(
                "POST outcome is unresolved; no exact CR found for automatic recovery"
            ) from None
        require(
            fair.expected_spec_matches(
                build, state["case"], state["identity"]["service_account_uids"]["a"]
            ),
            "uncertain POST produced a different KovaBuild",
        )
        uid = build.get("metadata", {}).get("uid", "")
        require(
            isinstance(uid, str) and fair.UID.fullmatch(uid) is not None,
            "recovery CR UID is invalid",
        )
        state["uid"] = uid
        state["attempting_post"] = False
        save(directory, "state.json", state)
    baseline = state["baseline"]
    cleanup_owned(state, directory, baseline)
    save(directory, "recovered.json", {"at": now(), "outcome": "exact-cleanup-complete"})
    return directory


def interrupted(signum: int, _frame: object) -> None:
    raise fair.SafetyError(f"received signal {signum}; run state preserved")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "mode",
        nargs="?",
        choices=("check", "run", "recover"),
        default=os.environ.get("ADMISSION_AUTH_OWNER_MODE", "check"),
    )
    parser.add_argument(
        "--candidate-commit",
        default=os.environ.get("ADMISSION_AUTH_OWNER_CANDIDATE_COMMIT"),
        help="full Git SHA of the deployed image candidate, which may precede this script commit",
    )
    parser.add_argument(
        "--run-id",
        default=os.environ.get("ADMISSION_AUTH_OWNER_RUN_ID"),
        help="exact prior run ID for recover",
    )
    parser.add_argument("--owner-requests", type=int, default=30)
    parser.add_argument("--nonowner-requests", type=int, default=10)
    parser.add_argument("--qps", type=float, default=2.0)
    args = parser.parse_args()
    directory = None
    try:
        require(
            1 <= args.owner_requests <= MAX_OWNER_REQUESTS
            and 1 <= args.nonowner_requests <= MAX_NONOWNER_REQUESTS
            and math.isfinite(args.qps)
            and 0 < args.qps <= MAX_QPS,
            "request count or rate exceeds the bounded envelope",
        )
        if args.mode == "check":
            require(args.run_id is None, "check does not accept a recovery run ID")
            require(
                isinstance(args.candidate_commit, str)
                and SHA.fullmatch(args.candidate_commit) is not None,
                "candidate commit is not exact",
            )
            fresh_preflight(args.candidate_commit)
            read.api_metrics(allow_unseen=True)
            print(f"auth-owner: fresh read-only TokenReview preflight passed: {fair.CLUSTER}")
            return 0
        require(socket.gethostname() == "wayne-hk-kvm", "live probe is restricted to wayne-hk-kvm")
        require(
            os.environ.get("ADMISSION_AUTH_OWNER_ACK") == ACK,
            f"live probe requires ADMISSION_AUTH_OWNER_ACK={ACK}",
        )
        require(not EVIDENCE_ROOT.is_symlink(), "evidence root is a symlink")
        EVIDENCE_ROOT.mkdir(mode=0o700, parents=True, exist_ok=True)
        require(
            EVIDENCE_ROOT.stat().st_uid == os.getuid()
            and EVIDENCE_ROOT.stat().st_mode & 0o077 == 0,
            "evidence root is not private",
        )
        with open(
            EVIDENCE_ROOT / ".run.lock",
            "a+b",
            opener=lambda name, flags: os.open(name, flags, 0o600),
        ) as lock:
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                raise fair.SafetyError("another fairness/auth test owns the fixture") from None
            old_term = signal.signal(signal.SIGTERM, interrupted)
            old_int = signal.signal(signal.SIGINT, interrupted)
            try:
                if args.mode == "recover":
                    require(args.run_id is not None, "recover requires an exact --run-id")
                    directory = recover(args.run_id)
                    print(f"auth-owner: exact recovery cleanup complete; evidence={directory}")
                    return 0
                require(args.run_id is None, "run does not accept a recovery run ID")
                require(
                    isinstance(args.candidate_commit, str)
                    and SHA.fullmatch(args.candidate_commit) is not None,
                    "candidate commit is not exact",
                )
                identity, baseline = fresh_preflight(args.candidate_commit)
                read.api_metrics(allow_unseen=True)
                run_id = (
                    "owner-get-"
                    + datetime.now(timezone.utc).strftime("%Y%m%dt%H%M%Sz")  # noqa: UP017
                    + "-"
                    + secrets.token_hex(4)
                )  # noqa: UP017
                case = case_for(run_id, identity["platform"])
                directory = EVIDENCE_ROOT / run_id
                directory.mkdir(mode=0o700)
                state = {
                    "run_id": run_id,
                    "identity": identity,
                    "baseline": baseline,
                    "case": case,
                    "uid": None,
                    "attempting_post": False,
                    "deleting": False,
                    "deleted": False,
                }
                save(directory, "state.json", state)
                save(directory, "health-initial.json", baseline)
                result = run_probe(args, directory, state, baseline)
                result["outcome"] = "passed"
                result["cleanup_complete"] = state["deleted"]
                save(directory, "result.json", result)
                print(
                    "auth-owner: passed; "
                    f"owner={result['owner']['requests']}; "
                    f"nonowner={result['nonowner']['requests']}; evidence={directory}"
                )
                return 0
            finally:
                signal.signal(signal.SIGTERM, old_term)
                signal.signal(signal.SIGINT, old_int)
    except Exception as error:
        if directory is not None:
            try:
                save(
                    directory,
                    "stop.json",
                    {"at": now(), "outcome": "stopped", "error_type": type(error).__name__},
                )
            except OSError:
                pass
        print(f"auth-owner: STOP: {type(error).__name__}", file=sys.stderr)
        if directory is not None:
            print(f"auth-owner: evidence preserved at {directory}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
