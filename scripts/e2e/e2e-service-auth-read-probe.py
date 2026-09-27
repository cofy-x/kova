#!/usr/bin/env python3
"""Bounded two-principal read-only auth API probe on the fairness Kind fixture.

Check is the default and never issues a token. Run uses TokenRequest only for
short-lived in-memory bearers, and only sends GET /v1/builds?limit=1 to two
UID-pinned Service Pods. It never creates, edits, or deletes Kova workloads.
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
import select
import signal
import socket
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
from urllib.request import Request

HERE = Path(__file__).resolve().parent
SPEC = importlib.util.spec_from_file_location(
    "admission_fairness_probe_fixture", HERE / "e2e-service-admission-fairness.py"
)
assert SPEC and SPEC.loader
fair = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(fair)

ROOT = Path("/data/forge-artifacts/kova-admission-fairness")
ACK = "kova-admission-fairness/kova/kova-service/auth-read"
RUN_ID = re.compile(r"auth-read-[0-9]{8}t[0-9]{6}z-[0-9a-f]{8}\Z")
SHA = re.compile(r"[0-9a-f]{40}\Z")
LABEL = re.compile(r'(\w+)="((?:\\.|[^"\\])*)"')
REVIEW_RESOURCES = {
    ("authentication.k8s.io", "tokenreviews"),
    ("authorization.k8s.io", "subjectaccessreviews"),
}
MAX_METRICS_BYTES = 16 << 20
MAX_RUN_SECONDS = 180
MAX_REQUESTS_PER_PRINCIPAL = 120
MAX_QPS_PER_PRINCIPAL = 4.0


def require(condition: bool, reason: str) -> None:
    fair.require(condition, reason)


def timestamp() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")  # noqa: UP017


def kjson(*args: str) -> dict:
    return fair.kjson(*args)


def empty_and_healthy(identity: dict, pinned: dict | None = None) -> dict:
    """Sample stable fixture identity, workload emptiness, node and Pod capacity."""
    fair.failover_io.assert_exact_kind(fair.KUBECONFIG, fair.CLUSTER, identity["kubeconfig_sha256"])
    nodes = kjson("get", "nodes", "-o", "json")
    node_facts = fair.verify_nodes(nodes)
    require(node_facts == identity["node_facts"], "Kind node identity or capacity drifted")
    active = fair.ledger_data(
        kjson("-n", fair.NAMESPACE, "get", "configmap", "kova-service-admission", "-o", "json"),
        "reservations.json",
    )
    queue = fair.ledger_data(
        kjson(
            "-n", fair.NAMESPACE, "get", "configmap", "kova-service-queue-admission", "-o", "json"
        ),
        "queue.json",
    )
    require(
        active.get("active") == {} and queue.get("intents") == {},
        "a Kova build is active or queued",
    )
    cursor = active.get("lastGrantedRequesterHash", "")
    require(
        cursor in ("", fair.sha256(fair.principal("a")), fair.sha256(fair.principal("b"))),
        "fairness cursor is not from the reviewed two principals",
    )
    require(
        kjson("get", "kovabuilds", "--all-namespaces", "-o", "json").get("items") == [],
        "KovaBuild is present",
    )
    require(
        kjson(
            "get",
            "pods",
            "--all-namespaces",
            "-l",
            "app.kubernetes.io/name=kova-runner",
            "-o",
            "json",
        ).get("items")
        == [],
        "Kova runner Pod is present",
    )
    deployment = kjson("-n", fair.NAMESPACE, "get", "deployment", "kova-service", "-o", "json")
    require(
        deployment.get("metadata", {}).get("uid") == identity["deployment_uid"]
        and fair.stable_hash(deployment.get("spec", {}).get("template"))
        == identity["deployment_template_sha256"]
        and deployment.get("status", {}).get("readyReplicas") == 2
        and deployment.get("status", {}).get("updatedReplicas") == 2,
        "Service Deployment identity or Ready replicas changed",
    )
    pods = kjson(
        "-n",
        fair.NAMESPACE,
        "get",
        "pods",
        "-l",
        "app.kubernetes.io/instance=kova,app.kubernetes.io/component=service",
        "-o",
        "json",
    ).get("items", [])
    require(
        isinstance(pods, list)
        and len(pods) == 2
        and all(fair.one_ready_service_pod(pod, identity["service_image"]) for pod in pods),
        "two Ready candidate Service Pods are not present",
    )
    pod_facts = []
    for pod in pods:
        metadata = pod["metadata"]
        statuses = [
            item for item in pod["status"]["containerStatuses"] if item["name"] == "kova-service"
        ]
        require(len(statuses) == 1, "Service Pod container changed")
        pod_facts.append(
            {
                "name": metadata["name"],
                "uid": metadata["uid"],
                "node": pod["spec"].get("nodeName"),
                "image_id": statuses[0]["imageID"],
                "restarts": statuses[0].get("restartCount", 0),
            }
        )
    pod_facts.sort(key=lambda item: item["name"])
    require(
        len({item["image_id"] for item in pod_facts}) == 1
        and pod_facts[0]["image_id"] == identity["service_image_id"],
        "Service Pod image ID drifted",
    )
    lease = kjson("-n", fair.NAMESPACE, "get", "lease", fair.LEASE, "-o", "json")
    holder = lease.get("spec", {}).get("holderIdentity", "")
    require(
        lease.get("metadata", {}).get("uid") == identity["lease_uid"]
        and any(holder.startswith(item["name"] + "_") for item in pod_facts),
        "admission Lease identity or holder changed",
    )
    all_pods = kjson("get", "pods", "--all-namespaces", "-o", "json").get("items", [])
    require(isinstance(all_pods, list), "Pod capacity list is malformed")
    counts = {item["name"]: 0 for item in node_facts}
    for pod in all_pods:
        node = pod.get("spec", {}).get("nodeName")
        if node in counts and pod.get("metadata", {}).get("deletionTimestamp") is None:
            counts[node] += 1
    capacity = []
    for node in node_facts:
        maximum = int(node["allocatable"]["pods"])
        require(maximum > 0 and counts[node["name"]] / maximum < 0.8, "Kind Pod capacity is unsafe")
        capacity.append(
            {"node": node["name"], "pods": counts[node["name"]], "allocatable": maximum}
        )
    stats = fair.command(
        ["docker", "stats", "--no-stream", "--format", "{{json .}}", *sorted(counts)], timeout=20
    )
    docker_facts = []
    for line in stats.splitlines():
        try:
            item = json.loads(line)
            memory_percent = float(item["MemPerc"].removesuffix("%"))
        except (KeyError, TypeError, ValueError) as error:
            raise fair.SafetyError(
                f"Docker node stats are malformed: {type(error).__name__}"
            ) from None
        require(math.isfinite(memory_percent) and memory_percent < 90, "Kind node memory is unsafe")
        docker_facts.append(
            {
                "name": item.get("Name"),
                "cpu_percent": item.get("CPUPerc"),
                "memory_percent": memory_percent,
                "memory_usage": item.get("MemUsage"),
            }
        )
    docker_facts.sort(key=lambda item: item["name"])
    require([item["name"] for item in docker_facts] == sorted(counts), "Docker node stats differ")
    sample = {
        "at": timestamp(),
        "nodes": node_facts,
        "pod_capacity": capacity,
        "docker_nodes": docker_facts,
        "service_pods": pod_facts,
        "lease_holder": holder,
        "fairness_cursor": cursor,
        "active_count": 0,
        "queue_count": 0,
        "build_count": 0,
        "runner_count": 0,
    }
    if pinned is not None:
        require(
            sample["service_pods"] == pinned["service_pods"],
            "Service Pod identity or restarts changed",
        )
        require(sample["fairness_cursor"] == pinned["fairness_cursor"], "fairness cursor changed")
    return sample


def bounded_metrics_text() -> str:
    """Read API metrics with actual byte and elapsed-time bounds."""
    process = subprocess.Popen(
        [
            "kubectl",
            "--kubeconfig",
            str(fair.KUBECONFIG),
            "--request-timeout=15s",
            "get",
            "--raw",
            "/metrics",
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
    )
    chunks = []
    size = 0
    deadline = time.monotonic() + 30
    try:
        require(process.stdout is not None, "API metrics pipe is unavailable")
        while True:
            remaining = deadline - time.monotonic()
            require(remaining > 0, "API metrics timed out")
            ready, _, _ = select.select([process.stdout], [], [], remaining)
            require(bool(ready), "API metrics timed out")
            chunk = os.read(process.stdout.fileno(), 65536)
            if not chunk:
                break
            size += len(chunk)
            require(size <= MAX_METRICS_BYTES, "API server metrics exceeded 16 MiB")
            chunks.append(chunk)
        require(process.wait(timeout=2) == 0, "API server metrics command failed")
        return b"".join(chunks).decode("utf-8")
    except UnicodeError:
        raise fair.SafetyError("API server metrics are not UTF-8") from None
    finally:
        if process.poll() is None:
            process.kill()
            process.wait(timeout=5)


def api_metrics(*, allow_unseen: bool = False) -> dict:
    """Keep review counters; a fresh fixture may not have emitted them yet."""
    raw = bounded_metrics_text()
    series: dict[tuple[tuple[str, str], ...], float] = {}
    request_family_seen = False
    for line in raw.splitlines():
        if not line.startswith("apiserver_request_total{"):
            continue
        request_family_seen = True
        try:
            label_text, value_text = line.split("}", 1)
            labels = dict(LABEL.findall(label_text))
            if (labels.get("group"), labels.get("resource")) not in REVIEW_RESOURCES:
                continue
            value = float(value_text.strip().split()[0])
        except (ValueError, IndexError) as error:
            raise fair.SafetyError(f"API counter is malformed: {type(error).__name__}") from None
        require(math.isfinite(value) and value >= 0, "API counter is invalid")
        key = tuple(sorted(labels.items()))
        require(key not in series, "API counter series is duplicated")
        series[key] = value
    resources = {(dict(key).get("group"), dict(key).get("resource")) for key in series}
    require(request_family_seen, "API server request counter family is unavailable")
    if not allow_unseen:
        require(resources == REVIEW_RESOURCES, "TokenReview or SAR API counter is unavailable")
    return {
        "at": timestamp(),
        "series": [{"labels": dict(key), "value": value} for key, value in sorted(series.items())],
    }


def metric_deltas(before: dict, after: dict, count: int, elapsed: float) -> dict:
    first = {tuple(sorted(item["labels"].items())): item["value"] for item in before["series"]}
    second = {tuple(sorted(item["labels"].items())): item["value"] for item in after["series"]}
    rows = []
    totals = {"tokenreviews": 0.0, "subjectaccessreviews": 0.0}
    for key in sorted(first.keys() | second.keys()):
        start, end = first.get(key, 0.0), second.get(key, 0.0)
        require(end >= start, "API counter reset or series disappeared during the probe")
        delta = end - start
        labels = dict(key)
        rows.append({"labels": labels, "before": start, "after": end, "delta": delta})
        totals[labels["resource"]] += delta
    require(
        all(value >= count for value in totals.values()), "API counters undercount completed GETs"
    )
    return {
        "series": rows,
        "totals": totals,
        "per_second": {key: value / elapsed for key, value in totals.items()},
    }


def issue_tokens(identity: dict) -> dict[str, str]:
    tokens = {}
    for which, name in fair.SA.items():
        token = fair.kctl("-n", fair.NAMESPACE, "create", "token", name, "--duration=10m").strip()
        require(
            32 <= len(token) <= 8192
            and token.isascii()
            and not any(char.isspace() for char in token),
            "TokenRequest returned an invalid bearer",
        )
        account = kjson("-n", fair.NAMESPACE, "get", "serviceaccount", name, "-o", "json")
        require(
            account["metadata"]["uid"] == identity["service_account_uids"][which],
            "test principal UID changed",
        )
        tokens[which] = token
    return tokens


def get_empty_list(which: str, token: str, port: int) -> dict:
    request = Request(
        f"http://127.0.0.1:{port}/v1/builds?limit=1",
        headers={"Authorization": f"Bearer {token}"},
        method="GET",
    )
    started = time.monotonic()
    status, _, body = fair.failover_io.bounded_response(request, timeout=5)
    elapsed = time.monotonic() - started
    require(token.encode() not in body, "Service reflected a bearer; response withheld")
    try:
        payload = json.loads(body)
    except (UnicodeError, ValueError) as error:
        raise fair.SafetyError(f"Service list JSON is malformed: {type(error).__name__}") from None
    require(
        status == 200
        and isinstance(payload, dict)
        and payload.get("jobs") == []
        and payload.get("continue", "") == "",
        "Service did not return an empty authorized list",
    )
    return {
        "at": timestamp(),
        "principal": which,
        "http_status": status,
        "response_bytes": len(body),
        "elapsed_seconds": round(elapsed, 6),
    }


def percentile(values: list[float], percent: float) -> float:
    require(bool(values), "latency distribution is empty")
    ordered = sorted(values)
    return ordered[math.ceil(percent * len(ordered)) - 1]


def run_requests(
    directory: Path,
    tokens: dict[str, str],
    count: int,
    qps: float,
    pinned: dict,
    identity: dict,
    forwards: fair.Forwards,
) -> tuple[list[dict], float, list[dict]]:
    stopped = threading.Event()
    deadline = time.monotonic() + MAX_RUN_SECONDS
    started = time.monotonic()
    health = []

    def worker(which: str) -> list[dict]:
        rows = []
        for index in range(count):
            require(time.monotonic() < deadline, "read probe exceeded three minutes")
            delay = started + index / qps - time.monotonic()
            if delay > 0 and stopped.wait(delay):
                break
            if stopped.is_set():
                break
            try:
                receipt = get_empty_list(which, tokens[which], fair.PORT[which])
                receipt.update(
                    {
                        "index": index + 1,
                        "pod": pinned["service_pods"][0 if which == "a" else 1]["name"],
                    }
                )
                fair.save(directory / f"request-{which}-{index + 1:03d}.json", receipt)
                rows.append(receipt)
            except Exception as error:
                fair.save(
                    directory / f"request-{which}-{index + 1:03d}.json",
                    {
                        "at": timestamp(),
                        "principal": which,
                        "index": index + 1,
                        "outcome": "failed",
                        "error_type": type(error).__name__,
                    },
                )
                stopped.set()
                raise
        return rows

    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = {which: pool.submit(worker, which) for which in ("a", "b")}
        try:
            sample_index = 0
            while not all(future.done() for future in futures.values()):
                require(time.monotonic() < deadline, "read probe exceeded three minutes")
                if stopped.wait(5):
                    break
                sample_index += 1
                sample = empty_and_healthy(identity, pinned)
                fair.save(directory / f"health-{sample_index:03d}.json", sample)
                health.append(sample)
                require(
                    all(process.poll() is None for process in forwards.processes),
                    "Service port-forward exited",
                )
            for future in futures.values():
                future.result()
        finally:
            stopped.set()
    elapsed = time.monotonic() - started
    rows = [row for future in futures.values() for row in future.result()]
    require(len(rows) == 2 * count, "read probe did not complete its bounded request count")
    return rows, elapsed, health


def run(args: argparse.Namespace, directory: Path, identity: dict, baseline: dict) -> dict:
    tokens = issue_tokens(identity)
    forwards = fair.Forwards(directory)
    try:
        forwards.start([item["name"] for item in baseline["service_pods"]], "auth-read")
        warmups = [get_empty_list(which, tokens[which], fair.PORT[which]) for which in ("a", "b")]
        fair.save(directory / "warmup.json", warmups)
        pinned = empty_and_healthy(identity, baseline)
        fair.save(directory / "health-before.json", pinned)
        before = api_metrics()
        fair.save(directory / "apiserver-before.json", before)
        rows, elapsed, health = run_requests(
            directory,
            tokens,
            args.requests_per_principal,
            args.qps_per_principal,
            pinned,
            identity,
            forwards,
        )
        after = api_metrics()
        fair.save(directory / "apiserver-after.json", after)
        final = empty_and_healthy(identity, pinned)
        fair.save(directory / "health-after.json", final)
        final_identity = fair.fixture_identity(empty=False, candidate_commit=args.candidate_commit)
        for key in identity:
            if key not in ("service_local_image", "service_initial_image_bindings"):
                require(final_identity[key] == identity[key], f"fixture identity drifted: {key}")
        deltas = metric_deltas(before, after, len(rows), elapsed)
        latencies = [row["elapsed_seconds"] for row in rows]
        result = {
            "outcome": "passed",
            "completed_at": timestamp(),
            "candidate_commit": args.candidate_commit,
            "requests": len(rows),
            "per_principal": args.requests_per_principal,
            "success_rate": 1.0,
            "elapsed_seconds": round(elapsed, 6),
            "throughput_per_second": round(len(rows) / elapsed, 6),
            "latency_seconds": {
                "p50": percentile(latencies, 0.50),
                "p95": percentile(latencies, 0.95),
                "p99": percentile(latencies, 0.99),
                "max": max(latencies),
            },
            "api_reviews": deltas,
            "node_memory_percent_peak": {
                name: max(
                    sample["docker_nodes"][index]["memory_percent"]
                    for sample in [baseline, pinned, *health, final]
                )
                for index, name in enumerate(item["name"] for item in baseline["docker_nodes"])
            },
            "pod_capacity_peak": {
                name: max(
                    sample["pod_capacity"][index]["pods"]
                    for sample in [baseline, pinned, *health, final]
                )
                for index, name in enumerate(item["node"] for item in baseline["pod_capacity"])
            },
            "limitations": [
                "Only this two-Pod Kind fixture, empty-list GET path, and bounded rate.",
                "Empty-list GET executes TokenReview and list SAR on each request.",
                "This does not test owner GET's SAR bypass; that needs an owned build.",
                "API server counters are cluster-wide and may include unrelated review traffic.",
                "Not a build-throughput result or production SLA.",
            ],
        }
        fair.save(directory / "result.json", result)
        return result
    finally:
        forwards.close()
        tokens.clear()


def interrupted(signum: int, _frame: object) -> None:
    raise fair.SafetyError(f"received signal {signum}; probe receipts preserved")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "mode",
        nargs="?",
        choices=("check", "run"),
        default=os.environ.get("ADMISSION_AUTH_READ_MODE", "check"),
    )
    parser.add_argument(
        "--candidate-commit",
        default=os.environ.get("ADMISSION_AUTH_READ_CANDIDATE_COMMIT"),
        help="deployed candidate's full Git SHA",
    )
    parser.add_argument("--requests-per-principal", type=int, default=40)
    parser.add_argument("--qps-per-principal", type=float, default=2.0)
    args = parser.parse_args()
    directory = None
    try:
        require(
            isinstance(args.candidate_commit, str)
            and SHA.fullmatch(args.candidate_commit) is not None,
            "candidate SHA is not exact",
        )
        require(
            1 <= args.requests_per_principal <= MAX_REQUESTS_PER_PRINCIPAL
            and math.isfinite(args.qps_per_principal)
            and 0 < args.qps_per_principal <= MAX_QPS_PER_PRINCIPAL,
            "probe rate or request count exceeds its bound",
        )
        identity = fair.fixture_identity(empty=False, candidate_commit=args.candidate_commit)
        baseline = empty_and_healthy(identity)
        if args.mode == "check":
            print(
                "auth-read: read-only preflight passed: "
                + fair.CLUSTER
                + " / "
                + args.candidate_commit[:12]
            )
            return 0
        require(socket.gethostname() == "wayne-hk-kvm", "live probe is restricted to wayne-hk-kvm")
        require(
            os.environ.get("ADMISSION_AUTH_READ_ACK") == ACK,
            f"run requires ADMISSION_AUTH_READ_ACK={ACK}",
        )
        require(not ROOT.is_symlink(), "evidence root is a symlink")
        ROOT.mkdir(mode=0o700, parents=True, exist_ok=True)
        require(
            ROOT.stat().st_uid == os.getuid() and ROOT.stat().st_mode & 0o077 == 0,
            "evidence root is not private",
        )
        with open(
            ROOT / ".run.lock", "a+b", opener=lambda name, flags: os.open(name, flags, 0o600)
        ) as lock:
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                raise fair.SafetyError("another fairness test owns the fixture") from None
            identity = fair.fixture_identity(empty=False, candidate_commit=args.candidate_commit)
            baseline = empty_and_healthy(identity)
            run_id = (
                "auth-read-"
                + datetime.now(timezone.utc).strftime("%Y%m%dt%H%M%Sz")  # noqa: UP017
                + "-"
                + secrets.token_hex(4)
            )  # noqa: UP017
            require(RUN_ID.fullmatch(run_id) is not None, "run ID is invalid")
            directory = ROOT / run_id
            directory.mkdir(mode=0o700)
            fair.save(directory / "identity.json", identity)
            fair.save(directory / "health-initial.json", baseline)
            old_term = signal.signal(signal.SIGTERM, interrupted)
            old_int = signal.signal(signal.SIGINT, interrupted)
            try:
                result = run(args, directory, identity, baseline)
            finally:
                signal.signal(signal.SIGTERM, old_term)
                signal.signal(signal.SIGINT, old_int)
        print(
            f"auth-read: {result['outcome']}; requests={result['requests']}; evidence={directory}"
        )
        return 0
    except Exception as error:
        if directory is not None:
            try:
                fair.save(
                    directory / "stop.json",
                    {
                        "outcome": "stopped",
                        "at": timestamp(),
                        "error_type": type(error).__name__,
                    },
                )
            except OSError:
                pass
        print(f"auth-read: STOP: {type(error).__name__}", file=sys.stderr)
        if directory is not None:
            print(f"auth-read: evidence preserved at {directory}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
