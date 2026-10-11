#!/usr/bin/env python3
"""Bounded, API-server-backed #46 deep-queue acceptance on disposable Kind.

Check mode is read-only. Run mode requires an exact cluster acknowledgement and
keeps all receipts. It never creates a cluster, changes a ledger, or contacts a
registry. One test-owned unschedulable Pending runner holds the active slot;
the genuinely queued builds must have no runner Pods. Genesis v3 retains the
active request's queue charge: supported totals 100/500/1000 mean 99/499/999
waiting builds. The explicitly named short profile uses a fresh cap of 100.
"""

from __future__ import annotations

import base64
import binascii
import fcntl
import hashlib
import hmac
import importlib.util
import json
import math
import os
import re
import secrets
import shutil
import signal
import socket
import stat
import subprocess
import sys
import tarfile
import time
from collections.abc import Callable
from datetime import datetime, timezone
from pathlib import Path
from typing import IO
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import ProxyHandler, Request, build_opener

ROOT = Path(__file__).resolve().parents[2]
sys.dont_write_bytecode = True
_V3_SPEC = importlib.util.spec_from_file_location(
    "deep_queue_v3", Path(__file__).with_name("deep_queue_v3.py")
)
V3 = importlib.util.module_from_spec(_V3_SPEC)
_V3_SPEC.loader.exec_module(V3)
_BURST_SPEC = importlib.util.spec_from_file_location(
    "deep_queue_burst", Path(__file__).with_name("e2e-service-burst.py")
)
BURST = importlib.util.module_from_spec(_BURST_SPEC)
sys.modules[_BURST_SPEC.name] = BURST
_BURST_SPEC.loader.exec_module(BURST)
CLUSTER = "kova-deep-queue"
NAMESPACE = "kova"
CONTROL_NAMESPACE = "kova"
RECEIPT_NAMESPACE = ""
RELEASE = "kova"
KUBECONFIG = ROOT / ".kind" / f"{CLUSTER}.kubeconfig"
STAGES = (100, 500, 1000)
POLL_SECONDS = 5
STEADY_SECONDS = 30
QUIET_SAMPLE_SECONDS = POLL_SECONDS
QUIET_MIN_SECONDS = 300
QUIET_MAX_SECONDS = 600
PORT = 18086
API_PROXY_PORT = 18087
HOST_LOCK = Path("/data/forge-artifacts/kova-deep-queue.run.lock")
SERVICE_CPU_MAX_NANO = 2_000_000_000  # 2 vCPU per Service Pod
SERVICE_MEMORY_MAX = 2 * 1024**3
NODE_MEMORY_AVAILABLE_MIN = 2 * 1024**3
HOST_MEMORY_AVAILABLE_MIN = 8 * 1024**3
DISK_FREE_MIN = 20 * 1024**3
NODE_CPU_FRACTION_MAX = 0.80
API_QPS_MAX = 1200
HTTP_BODY_MAX = 1024 * 1024
OCI_METADATA_BLOB_MAX = 2 * 1024 * 1024
OCI_METADATA_TOTAL_MAX = 16 * 1024 * 1024
SETTLE_DEADLINE_SECONDS = 120
CLEANUP_DEADLINE_SECONDS = 300
RUN_DEADLINE_SECONDS = 900
EMERGENCY_STOP_DEADLINE_SECONDS = 120
SOURCE_DIGEST = "sha256:" + "a" * 64
OPENER = build_opener(ProxyHandler({}))
FIXTURE: dict = {}
CONTRACT: dict = {}
FIXTURE_SHA = ""
EXECUTION_DEADLINE: float | None = None
API_READ_PROXY_ENABLED = False
HTTP_FRONTDOORS: list = []
POST_SEQUENCE = 0


def retained_profile() -> bool:
    return FIXTURE.get("profile") == "supported-capacity-retained"


def configure_fixture() -> dict:
    global \
        FIXTURE, \
        FIXTURE_SHA, \
        CLUSTER, \
        NAMESPACE, \
        CONTROL_NAMESPACE, \
        RECEIPT_NAMESPACE
    global RELEASE, KUBECONFIG, STAGES, PORT, API_PROXY_PORT, RUN_DEADLINE_SECONDS
    raw = os.environ.get("DEEP_QUEUE_E2E_FIXTURE", "")
    if not raw:
        fail("DEEP_QUEUE_E2E_FIXTURE must name the original private fresh-v3 fixture")
    FIXTURE, FIXTURE_SHA = V3.load_fixture(Path(raw))
    CLUSTER, RELEASE = FIXTURE["cluster"], FIXTURE["release"]
    NAMESPACE, CONTROL_NAMESPACE = (
        FIXTURE["runner_namespace"],
        FIXTURE["control_namespace"],
    )
    RECEIPT_NAMESPACE = FIXTURE["receipt_namespace"]
    KUBECONFIG = Path(FIXTURE["kubeconfig"])
    STAGES = (
        (100,) if FIXTURE["profile"] == "representative-short" else (100, 500, 1000)
    )
    RUN_DEADLINE_SECONDS = 1500 if retained_profile() else (900 if len(STAGES) == 1 else 1800)
    PORT, API_PROXY_PORT = FIXTURE["ports"]["service"], FIXTURE["ports"]["api_proxy"]
    quiet_seconds()  # Reject incompatible duration before any host operation.
    return FIXTURE


def quota_limit() -> int:
    return V3.limits(FIXTURE)["maxQueuedJobs"] if FIXTURE else 1000


def source_uri() -> str:
    # No runner can schedule, so this directed admission probe must never
    # inspect/publish source or contact any real/shared registry.
    return f"oci://unused.invalid/deep-queue/{FIXTURE['run_id']}@{SOURCE_DIGEST}"


def kova_fixture_changed() -> bool:
    path = Path(os.environ.get("DEEP_QUEUE_E2E_FIXTURE", ""))
    return (
        not path.is_file()
        or path.is_symlink()
        or hashlib.sha256(path.read_bytes()).hexdigest() != FIXTURE_SHA
    )


def receipt_objects() -> dict[str, dict]:
    inventory = kjson("-n", RECEIPT_NAMESPACE, "get", "configmaps", "-o", "json")
    if inventory.get("metadata", {}).get("continue") or inventory.get(
        "metadata", {}
    ).get("remainingItemCount", 0):
        fail("receipt inventory is incomplete")
    items = inventory.get("items", [])
    if type(items) is not list or len(items) > quota_limit() + 3:
        fail("receipt inventory is malformed or exceeds bounded capacity")
    result = {item["metadata"]["name"]: item for item in items}
    if len(result) != len(items):
        fail("duplicate receipt object names")
    result.pop("kube-root-ca.crt", None)
    return result


def empty_receipts() -> None:
    if receipt_objects():
        fail("fresh receipt namespace contains prior effect receipts")


def qualify_build_receipts(
    builds: dict[str, dict], blocker_id: str, blocker_uid: str
) -> dict:
    grants, intents = queue_ledgers(len(builds) - 1, blocker_id, blocker_uid)
    objects = receipt_objects()
    expected_names = {
        "kova-admission-intent-" + intent["nonce"] for intent in intents.values()
    }
    entry = grants[blocker_uid]
    expected_names |= {
        "kova-grant-intent-" + entry["grantNonce"],
        "kova-pod-create-intent-" + entry["podAttemptNonce"],
    }
    if set(intents) != set(builds) or set(objects) != expected_names:
        fail("v3 receipt inventory differs from the exact charged build set")
    for name, build in builds.items():
        intent = intents[name]
        V3.qualify_queue_receipt(
            objects["kova-admission-intent-" + intent["nonce"]],
            build,
            intent,
            FIXTURE,
            CONTRACT,
        )
    V3.qualify_active_receipts(
        builds[blocker_id],
        check_runner(blocker_id, blocker_uid),
        entry,
        intents[blocker_id],
        objects,
        FIXTURE,
        CONTRACT,
        canonical_pod_digest,
    )
    return objects


def qualify_epoch() -> None:
    if not FIXTURE:
        fail("no original v3 fixture pins")
    if kova_fixture_changed():
        fail("original fixture bytes changed")
    if (
        hashlib.sha256(KUBECONFIG.read_bytes()).hexdigest()
        != FIXTURE["kubeconfig_sha256"]
    ):
        fail("original kubeconfig bytes changed")
    if (
        Path("/proc/sys/kernel/random/boot_id").read_text().strip()
        != FIXTURE["host_boot_id"]
    ):
        fail("host boot identity changed")
    for role in ("control", "runner", "receipt"):
        obj = epoch_named_get("namespace", FIXTURE[role + "_namespace"])
        V3.identity(
            obj, FIXTURE[role + "_namespace"], None, FIXTURE[role + "_namespace_uid"]
        )
        if obj.get("status", {}).get("phase") != "Active":
            fail("original namespace is not Active")
    genesis = epoch_named_get("configmap", V3.GENESIS)
    V3.identity(genesis, V3.GENESIS, NAMESPACE, FIXTURE["genesis"]["genesis_uid"])
    data = V3.decode(
        V3.keys(genesis.get("data"), {"genesis.json"}, "Genesis data differs")[
            "genesis.json"
        ]
    )
    V3.keys(
        data,
        {"contract", "phase", "activeLedgerUID", "queueLedgerUID"},
        "Genesis fields differ",
    )
    if (
        genesis.get("immutable") is not True
        or genesis.get("binaryData")
        or data["phase"] != "Committed"
        or data["contract"] != CONTRACT
        or data["activeLedgerUID"] != FIXTURE["genesis"]["active_ledger_uid"]
        or data["queueLedgerUID"] != FIXTURE["genesis"]["queue_ledger_uid"]
    ):
        fail("original committed Genesis changed")


class BenchError(RuntimeError):
    pass


def fail(message: str) -> None:
    raise BenchError(message)


def note(message: str) -> None:
    print(f"deep-queue-e2e: {message}", file=sys.stderr, flush=True)


def now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")  # noqa: UP017 (Python 3.10)


def quiet_seconds() -> int:
    raw = os.environ.get("DEEP_QUEUE_E2E_QUIET_SECONDS", "0")
    if re.fullmatch(r"0|[1-9][0-9]*", raw) is None:
        fail("DEEP_QUEUE_E2E_QUIET_SECONDS must be 0 or a decimal duration")
    seconds = int(raw)
    if retained_profile() and seconds != 0:
        fail("retained full profile fixes quiet_seconds=0 within its original 1500s budget")
    if seconds != 0 and not (
        QUIET_MIN_SECONDS <= seconds <= QUIET_MAX_SECONDS and seconds % 30 == 0
    ):
        fail("DEEP_QUEUE_E2E_QUIET_SECONDS must be 0 or a 30s multiple from 300 to 600")
    return seconds


def start_execution_budget() -> float:
    """Start one budget before preflight; live must not renew it."""
    global EXECUTION_DEADLINE
    EXECUTION_DEADLINE = time.monotonic() + RUN_DEADLINE_SECONDS
    return EXECUTION_DEADLINE


def budget_timeout(maximum: float) -> float:
    # Pure helper regressions can run without a live execution scope. The
    # authoritative check/run entrypoints always start the shared budget.
    if EXECUTION_DEADLINE is None:
        return maximum
    remaining = EXECUTION_DEADLINE - time.monotonic()
    if remaining <= 0:
        fail("end-to-end preflight/live execution budget expired")
    return min(maximum, remaining)


def live_deadline() -> float:
    if EXECUTION_DEADLINE is None:
        fail("live has no original preflight execution budget")
    budget_timeout(RUN_DEADLINE_SECONDS)
    return EXECUTION_DEADLINE


def pause(seconds: float) -> None:
    time.sleep(budget_timeout(seconds))


def command(
    argv: list[str], *, input_text: str | None = None, timeout: int = 20,
    environment: dict[str, str] | None = None,
) -> str:
    bounded_timeout = budget_timeout(timeout)
    try:
        result = subprocess.run(
            argv,
            input=input_text,
            text=True,
            capture_output=True,
            timeout=bounded_timeout,
            check=False,
            env=environment,
        )
    except (OSError, subprocess.TimeoutExpired) as error:
        fail(f"{argv[0]} did not complete: {type(error).__name__}")
    if result.returncode != 0:
        fail(f"{Path(argv[0]).name} returned nonzero status")
    budget_timeout(timeout)
    return result.stdout


def qualify_template_binary() -> Path:
    path = Path(FIXTURE["pod_template_binary"])
    if not path.is_absolute() or path.resolve() != path or path.is_symlink():
        fail("Pod-template binary path is not canonical")
    def stable(info: os.stat_result) -> tuple:
        # Hashing a newly built file may update atime under Linux relatime.
        return (info.st_dev, info.st_ino, info.st_mode, info.st_uid, info.st_gid,
                info.st_size, info.st_mtime_ns, info.st_ctime_ns)

    before = path.stat()
    if (
        not stat.S_ISREG(before.st_mode)
        or before.st_uid != os.getuid()
        or before.st_mode & 0o022
        or not before.st_mode & 0o100
        or not 0 < before.st_size <= 512 * 1024 * 1024
    ):
        fail("Pod-template binary is not an owned bounded executable")
    digest = hashlib.sha256()
    with os.fdopen(os.open(path, os.O_RDONLY | os.O_NOFOLLOW), "rb") as source:
        if stable(os.fstat(source.fileno())) != stable(before):
            fail("Pod-template binary was replaced before its verification read")
        while block := source.read(1024 * 1024):
            budget_timeout(5)
            digest.update(block)
        if stable(os.fstat(source.fileno())) != stable(before):
            fail("Pod-template binary changed during its verification read")
    if stable(path.stat()) != stable(before) or digest.hexdigest() != FIXTURE["pod_template_binary_sha256"]:
        fail("original candidate Pod-template binary changed")
    return path


def native_template_result(mode: str, raw: bytes, fields: set[str]) -> dict:
    if mode not in ("1", "identity"):
        fail("unsupported native Pod-template operation")
    binary = qualify_template_binary()
    output = command(
        [
            str(binary),
            "-test.run=^TestE2EPodTemplateDigestStdin$",
            "-test.count=1",
            "-test.timeout=5s",
            "-test.v=false",
        ],
        input_text=raw.decode(),
        timeout=5,
        environment={"KOVA_E2E_POD_TEMPLATE_STDIN": mode},
    )
    qualify_template_binary()
    lines = output.splitlines()
    prefix = "KOVA_E2E_POD_TEMPLATE " if mode == "1" else "KOVA_E2E_POD_TEMPLATE_IDENTITY "
    if len(lines) != 2 or not lines[0].startswith(prefix) or lines[1] != "PASS":
        fail("native Pod-template result framing differs")
    result = V3.keys(
        V3.decode(lines[0][len(prefix):]),
        fields,
        "native Pod-template result fields differ",
    )
    if result["candidate_commit"] != FIXTURE["candidate_commit"]:
        fail("native Pod-template binary candidate differs")
    return result


def canonical_pod_digest(pod: dict) -> str:
    """Direct Pod bytes -> pinned native product projection, never stamp trust."""
    raw = V3.encoded(pod)
    if len(raw) > 128 * 1024:
        fail("direct Pod exceeds the native template input bound")
    result = native_template_result(
        "1", raw, {"candidate_commit", "input_sha256", "pod_uid", "pod_template_digest"}
    )
    if (
        result["input_sha256"] != hashlib.sha256(raw).hexdigest()
        or result["pod_uid"] != pod["metadata"]["uid"]
        or type(result["pod_template_digest"]) is not str
        or V3.DIGEST.fullmatch(result["pod_template_digest"]) is None
    ):
        fail("native Pod-template result does not bind this source and direct Pod")
    return result["pod_template_digest"]


def kctl(*args: str, timeout: int = 20) -> str:
    return command(
        [
            "kubectl",
            "--kubeconfig",
            str(KUBECONFIG),
            "--context",
            f"kind-{CLUSTER}",
            "--request-timeout=15s",
            *args,
        ],
        timeout=timeout,
    )


def kjson(*args: str) -> dict:
    result = V3.decode(kctl(*args))
    if not isinstance(result, dict):
        fail(f"kubectl returned non-object JSON for {args[:3]}")
    return result


def save_json(path: Path, value: object) -> None:
    with path.open("w", encoding="utf-8") as output:
        json.dump(value, output, sort_keys=True, indent=2)
        output.write("\n")
        output.flush()
        os.fsync(output.fileno())


def safe_error(error: BaseException, token: str) -> str:
    # Do not persist command stderr, arbitrary exception text or API bodies.
    return (
        type(error).__name__
        if type(error)
        in (BenchError, V3.QualificationError, ValueError, OSError, KeyboardInterrupt)
        else "Exception"
    )


def save_without_token(path: Path, value: object, token: str) -> None:
    if token and token in json.dumps(value, sort_keys=True):
        fail("Kubernetes response reflected the request credential; snapshot withheld")
    save_json(path, value)


def object_sha256(value: object) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


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
    clusters = command(["kind", "get", "clusters"]).splitlines()
    if clusters.count(CLUSTER) != 1:
        fail(f"the exact dedicated Kind cluster {CLUSTER} is unavailable")
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
    if (
        FIXTURE
        and {item["metadata"]["name"]: item["metadata"]["uid"] for item in items}
        != FIXTURE["node_uids"]
    ):
        fail("original Kind node UIDs changed")
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
        f"--namespace={NAMESPACE}",
        "--max-active-jobs=1",
        "--max-active-jobs-per-requester=1",
        "--worker-slots=1",
        f"--max-queued-jobs={quota_limit()}",
        f"--max-queued-jobs-per-requester={quota_limit()}",
        f"--kube-client-qps={100 if retained_profile() else 20}",
        f"--kube-client-burst={200 if retained_profile() else 40}",
        "--runner-node-selector=never=true",
        "--auth-mode=static",
        "--auth-static-principal=kova:e2e",
        "--leader-elect=true",
        "--poll-interval=5s",
        f"--leader-election-namespace={CONTROL_NAMESPACE}",
        "--wait=2h",
        "--max-build-duration=2h",
        "--runner-image-pull-policy=Never",
    }
    if FIXTURE:
        required |= {
            f"--worker-pool-id={FIXTURE['genesis']['worker_pool_id']}",
            f"--runner-image={FIXTURE['role_images']['runner']}",
            f"--admission-genesis-receipt-secret-namespace={CONTROL_NAMESPACE}",
            f"--admission-genesis-receipt-secret-name={FIXTURE['genesis']['secret_name']}",
            f"--admission-genesis-receipt-secret-uid={FIXTURE['genesis']['secret_uid']}",
            "--admission-genesis-receipt-file=/etc/kova/admission-genesis/receipt.json",
            f"--metrics-bind-address=127.0.0.1:{FIXTURE['ports']['metrics_remote']}",
        }
    if not required.issubset(set(args)):
        fail("Service arguments differ from the exact deep-queue benchmark contract")
    flags = [arg.split("=", 1)[0] for arg in args if arg.startswith("--")]
    if len(flags) != len(set(flags)):
        fail("Service arguments contain duplicate flags")
    if FIXTURE:
        mounts = [
            mount
            for mount in containers[0].get("volumeMounts", [])
            if mount.get("mountPath") == "/etc/kova/admission-genesis"
        ]
        volumes = [
            volume
            for volume in spec["template"]["spec"].get("volumes", [])
            if volume.get("name") == "admission-genesis-receipt"
        ]
        if (
            mounts
            != [
                {
                    "name": "admission-genesis-receipt",
                    "mountPath": "/etc/kova/admission-genesis",
                    "readOnly": True,
                }
            ]
            or len(volumes) != 1
            or volumes[0].get("secret")
            != {
                "secretName": FIXTURE["genesis"]["secret_name"],
                "optional": False,
                "items": [{"key": "receipt.json", "path": "receipt.json"}],
                "defaultMode": 420,
            }
        ):
            fail(
                "Service receipt file is not the exact original immutable Secret mount"
            )
    platforms = [
        arg.split("=", 2)[1] for arg in args if arg.startswith("--buildkit-platform-addr=")
    ]
    if len(platforms) != 1 or platforms[0] != "linux/amd64":
        fail("Service has no single supported test platform")
    return "kova:e2e", platforms[0]


def service_pods() -> list[dict]:
    pods = kjson(
        "-n",
        CONTROL_NAMESPACE,
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
    active = epoch_named_get("configmap", V3.ACTIVE)
    queue = epoch_named_get("configmap", V3.QUEUE)
    try:
        active_data = V3.qualify_ledger(active, "active", FIXTURE, CONTRACT)
        queue_data = V3.qualify_ledger(queue, "queue", FIXTURE, CONTRACT)
    except (KeyError, TypeError, ValueError) as error:
        fail(f"admission ledger is unreadable: {type(error).__name__}")
    if (
        active_data.get("version"),
        active_data.get("maxJobs"),
        active_data.get("maxPerRequester"),
        active_data.get("workerSlots"),
    ) != (2, 1, 1, 1):
        fail("active ledger limits differ from the benchmark contract")
    if (
        queue_data.get("version"),
        queue_data.get("globalLimit"),
        queue_data.get("requesterLimit"),
    ) != (2, quota_limit(), quota_limit()):
        fail("queue ledger limits differ from the benchmark contract")
    grants = active_data.get("active")
    intents = queue_data.get("intents")
    if not isinstance(grants, dict) or not isinstance(intents, dict):
        fail("admission ledger entries are not objects")
    if len(intents) > expected_queued + (1 if blocker_id is not None else 0):
        fail("queue ledger has more intents than this stage submitted")
    if blocker_id is None:
        if grants or intents:
            fail("admission ledgers are not empty before the benchmark")
    else:
        entry = grants.get(blocker_uid, {})
        if (
            set(grants) != {blocker_uid}
            or entry.get("buildName") != blocker_id
            or entry.get("requester") != "kova:e2e"
            or entry.get("slots") != 1
            or entry.get("closing", False) is not False
            or entry.get("inFlight", []) != []
            or any(
                type(entry.get(key)) is not str or not V3.DIGEST.fullmatch(entry[key])
                for key in (
                    "grantReceiptDigest",
                    "podReceiptDigest",
                    "podTemplateDigest",
                )
            )
            or any(
                type(entry.get(key)) is not str or not V3.UID.fullmatch(entry[key])
                for key in ("grantReceiptUID", "podReceiptUID")
            )
            or any(
                type(entry.get(key)) is not str or not V3.NONCE.fullmatch(entry[key])
                for key in ("grantNonce", "podAttemptNonce")
            )
        ):
            fail("active ledger is not exactly the qualified v3 blocker grant")
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


def parse_oci_saved_image(
    stream: IO[bytes], reference: str, manifest_digest: str, revision: str
) -> dict:
    """Verify a platform-filtered Docker save without buffering image layers."""
    if not isinstance(reference, str) or not reference:
        fail("Docker OCI archive reference is malformed")
    digest_reference = "@" in reference
    if digest_reference and (
        re.fullmatch(
            r"[a-z0-9](?:[a-zA-Z0-9._:/-]*[a-zA-Z0-9])?@sha256:[0-9a-f]{64}",
            reference,
        )
        is None
        or any(
            component in ("", ".", "..")
            for component in reference.split("@", 1)[0].split("/")
        )
        or reference.rsplit("@", 1)[1] != manifest_digest
    ):
        fail("Docker OCI archive digest reference differs from inspected manifest")
    blobs: dict[str, bytes] = {}
    index_bytes: bytes | None = None
    metadata_bytes = 0
    with tarfile.open(fileobj=stream, mode="r|*") as archive:
        for member in archive:
            if not member.isfile():
                continue
            if member.name == "index.json":
                if index_bytes is not None or member.size > OCI_METADATA_BLOB_MAX:
                    fail("Docker OCI archive has duplicate or oversized index metadata")
                extracted = archive.extractfile(member)
                if extracted is None:
                    fail("Docker OCI archive index is unreadable")
                index_bytes = extracted.read(OCI_METADATA_BLOB_MAX + 1)
                if len(index_bytes) != member.size:
                    fail("Docker OCI archive index is truncated")
                continue
            match = re.fullmatch(r"blobs/sha256/([0-9a-f]{64})", member.name)
            if match is None or member.size > OCI_METADATA_BLOB_MAX:
                continue
            extracted = archive.extractfile(member)
            if extracted is None:
                fail("Docker OCI archive blob is unreadable")
            prefix = extracted.read(min(member.size, 64))
            # Ignore even small layer blobs; only manifest/config JSON is retained.
            if prefix.lstrip()[:1] != b"{":
                continue
            metadata_bytes += member.size
            if metadata_bytes > OCI_METADATA_TOTAL_MAX:
                fail("Docker OCI archive has too much JSON metadata")
            digest = "sha256:" + match.group(1)
            if digest in blobs:
                fail("Docker OCI archive has duplicate blob metadata")
            data = prefix + extracted.read(OCI_METADATA_BLOB_MAX + 1 - len(prefix))
            if len(data) != member.size or "sha256:" + hashlib.sha256(data).hexdigest() != digest:
                fail("Docker OCI archive blob is truncated or digest-mismatched")
            blobs[digest] = data
    if index_bytes is None:
        fail("Docker OCI archive has no index")
    try:
        index = json.loads(index_bytes)
        descriptors = index["manifests"]
        if (
            index.get("schemaVersion") != 2
            or index.get("mediaType") != "application/vnd.oci.image.index.v1+json"
            or not isinstance(descriptors, list)
            or len(descriptors) != 1
        ):
            fail("Docker OCI archive is not an unambiguous platform-filtered index")
        descriptor = descriptors[0]
        if (
            descriptor.get("mediaType") != "application/vnd.oci.image.manifest.v1+json"
            or descriptor.get("digest") != manifest_digest
        ):
            fail("Docker OCI archive index differs from the inspected manifest")
        # A Docker save of a digest-pulled image can omit these optional index
        # hints. A tagged export must retain both; supplied hints must agree.
        if (not digest_reference or "platform" in descriptor) and descriptor.get(
            "platform"
        ) != {"architecture": "amd64", "os": "linux"}:
            fail("Docker OCI archive index differs from the inspected platform")
        annotations = descriptor.get("annotations", {})
        if not isinstance(annotations, dict) or any(
            not isinstance(key, str) or not isinstance(value, str)
            for key, value in annotations.items()
        ):
            fail("Docker OCI archive index annotations are malformed")
        name = "io.containerd.image.name"
        if (not digest_reference or name in annotations) and annotations.get(name) != reference:
            fail("Docker OCI archive index name differs from the inspected image")
        manifest_bytes = blobs[manifest_digest]
        if descriptor.get("size") != len(manifest_bytes):
            fail("Docker OCI archive manifest size differs from its index descriptor")
        manifest = json.loads(manifest_bytes)
        config_descriptor = manifest["config"]
        config_digest = config_descriptor["digest"]
        if (
            manifest.get("schemaVersion") != 2
            or manifest.get("mediaType") != "application/vnd.oci.image.manifest.v1+json"
            or config_descriptor.get("mediaType") != "application/vnd.oci.image.config.v1+json"
            or not isinstance(config_digest, str)
            or not re.fullmatch(r"sha256:[0-9a-f]{64}", config_digest)
        ):
            fail("Docker OCI archive has malformed manifest/config descriptors")
        config_bytes = blobs[config_digest]
        if config_descriptor.get("size") != len(config_bytes):
            fail("Docker OCI archive config size differs from its manifest descriptor")
        config = json.loads(config_bytes)
        if (
            config.get("architecture") != "amd64"
            or config.get("os") != "linux"
            or config.get("config", {}).get("Labels", {}).get("org.opencontainers.image.revision")
            != revision
        ):
            fail("Docker OCI archive config architecture/revision differs from inspected image")
    except (KeyError, TypeError, AttributeError, ValueError) as error:
        fail(f"Docker OCI archive metadata is malformed: {type(error).__name__}")
    return {
        "archive_index_sha256": "sha256:" + hashlib.sha256(index_bytes).hexdigest(),
        "platform_manifest_digest": manifest_digest,
        "config_digest": config_digest,
    }


def saved_image_fact(reference: str, manifest_digest: str, revision: str) -> dict:
    save_timeout = budget_timeout(90)
    try:
        process = subprocess.Popen(
            [
                "timeout",
                "--kill-after=5s",
                f"{save_timeout:.6f}s",
                "docker",
                "image",
                "save",
                "--platform",
                "linux/amd64",
                reference,
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
        )
    except OSError as error:
        fail(f"Docker OCI image save could not start: {type(error).__name__}")
    try:
        assert process.stdout is not None
        result = parse_oci_saved_image(
            process.stdout, reference, manifest_digest, revision
        )
    except BaseException as error:
        if process.poll() is None:
            process.kill()
        process.wait(timeout=5)
        if isinstance(error, BenchError):
            raise
        fail(f"Docker OCI image archive could not be parsed: {type(error).__name__}")
    finally:
        if process.stdout is not None:
            process.stdout.close()
    try:
        exit_code = process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=5)
        fail("Docker OCI image save did not exit after streaming")
    if exit_code != 0:
        fail(f"Docker OCI image save returned {exit_code}")
    budget_timeout(90)
    return result


def local_image_fact(reference: str) -> dict:
    images = json.loads(command(["docker", "image", "inspect", reference], timeout=30))
    if len(images) != 1:
        fail(f"the local Docker image {reference} is unavailable or ambiguous")
    image = images[0]
    revision = (image.get("Config", {}).get("Labels") or {}).get(
        "org.opencontainers.image.revision"
    )
    if not isinstance(revision, str) or not re.fullmatch(
        r"[0-9a-f]{12}|[0-9a-f]{40}", revision
    ):
        fail(f"local Docker image {reference} has no exact Kova revision label")
    image_id = image.get("Id")
    if not isinstance(image_id, str) or not re.fullmatch(r"sha256:[0-9a-f]{64}", image_id):
        fail(f"local Docker image {reference} has no exact SHA-256 image ID")
    platform_images = json.loads(
        command(["docker", "image", "inspect", "--platform", "linux/amd64", reference], timeout=30)
    )
    if len(platform_images) != 1:
        fail(f"local Docker image {reference} has no exact Linux/amd64 platform image")
    platform_image = platform_images[0]
    manifest_digest = platform_image.get("Id")
    if (
        not isinstance(manifest_digest, str)
        or not re.fullmatch(r"sha256:[0-9a-f]{64}", manifest_digest)
        or platform_image.get("Architecture") != "amd64"
        or platform_image.get("Os") != "linux"
        or (platform_image.get("Config", {}).get("Labels") or {}).get(
            "org.opencontainers.image.revision"
        )
        != revision
    ):
        fail(f"local Docker image {reference} has a mismatched Linux/amd64 manifest")
    saved = saved_image_fact(reference, manifest_digest, revision)
    return {
        "reference": reference,
        "image_id": image_id,
        "revision": revision,
        "architecture": image.get("Architecture"),
        **saved,
    }


def kind_runtime_image_fact(node: str, reference: str) -> dict:
    inspection = json.loads(
        command(["docker", "exec", node, "crictl", "inspecti", "-o", "json", reference], timeout=30)
    )
    status = inspection.get("status")
    if not isinstance(status, dict):
        fail(f"Kind node {node} did not return CRI image status for {reference}")
    image_id = status.get("id")
    if not isinstance(image_id, str) or not re.fullmatch(r"sha256:[0-9a-f]{64}", image_id):
        fail(f"Kind node {node} returned an unrecognized CRI image ID for {reference}")
    repo_digests = status.get("repoDigests") or []
    if not isinstance(repo_digests, list) or any(
        not isinstance(item, str) for item in repo_digests
    ):
        fail(f"Kind node {node} returned malformed CRI repo digests for {reference}")
    repo_tags = status.get("repoTags") or []
    if reference not in repo_tags and reference not in repo_digests:
        fail(f"Kind node {node} has no exact CRI reference for {reference}")
    return {"image_id": image_id, "repo_tags": repo_tags, "repo_digests": repo_digests}


def deployed_image_fact(
    pod: dict, container_name: str, expected_reference: str, expected_id: str
) -> dict:
    pod_name = pod.get("metadata", {}).get("name")
    node = pod.get("spec", {}).get("nodeName")
    if not isinstance(node, str) or node not in (
        f"{CLUSTER}-control-plane",
        f"{CLUSTER}-worker",
    ):
        fail(f"Pod {pod_name} is not scheduled on a verified Kind node")
    spec_containers = [
        item
        for item in pod.get("spec", {}).get("containers", [])
        if item.get("name") == container_name
    ]
    if len(spec_containers) != 1 or spec_containers[0].get("image") != expected_reference:
        fail(f"Pod {pod_name} spec does not use the reviewed {container_name} image tag")
    containers = [
        item
        for item in pod.get("status", {}).get("containerStatuses", [])
        if item.get("name") == container_name
    ]
    if (
        len(containers) != 1
        or containers[0].get("image") != expected_reference
        or not isinstance(containers[0].get("imageID"), str)
    ):
        fail(f"Pod {pod_name} has no exact {container_name} image identity")
    pod_image_id = containers[0]["imageID"]
    runtime = kind_runtime_image_fact(node, expected_reference)
    if runtime["image_id"] != expected_id:
        fail(f"Pod {pod_name} runs an image other than the reviewed local Docker image")
    if pod_image_id != runtime["image_id"] and pod_image_id not in runtime["repo_digests"]:
        fail(f"Pod {pod_name} imageID is not in the matching CRI image's exact digests")
    return {
        "pod": pod_name,
        "node": node,
        "pod_image_id": pod_image_id,
        "runtime_image_id": runtime["image_id"],
        "runtime_repo_tags": runtime["repo_tags"],
        "runtime_repo_digests": runtime["repo_digests"],
    }


def host_memory_available() -> int:
    for line in Path("/proc/meminfo").read_text().splitlines():
        if line.startswith("MemAvailable:"):
            return int(line.split()[1]) * 1024
    fail("Linux MemAvailable metric is absent")


def resource_sample(
    expected_pods: list[dict], stage_dir: Path | None, docker_root: Path
) -> dict:
    nodes = good_nodes(kjson("get", "nodes", "-o", "json"))
    current_pods = service_pods()
    original_pods = {pod["metadata"]["name"]: pod["metadata"]["uid"] for pod in expected_pods}
    if {pod["metadata"]["name"]: pod["metadata"]["uid"] for pod in current_pods} != original_pods:
        fail("Service Pod identity changed during the benchmark")
    original_images = {
        pod["metadata"]["name"]: next(
            status["imageID"]
            for status in pod["status"]["containerStatuses"]
            if status["name"] == "kova-service"
        )
        for pod in expected_pods
    }
    current_images = {
        pod["metadata"]["name"]: next(
            status["imageID"]
            for status in pod["status"]["containerStatuses"]
            if status["name"] == "kova-service"
        )
        for pod in current_pods
    }
    if current_images != original_images:
        fail("Service Pod image identity changed during the benchmark")
    services = []
    node_samples = []
    for node in nodes:
        name = node["metadata"]["name"]
        summary = kjson(
            "get", "--raw", f"/api/v1/nodes/{quote(name)}/proxy/stats/summary"
        )
        cpu = summary.get("node", {}).get("cpu", {}).get("usageNanoCores")
        memory = summary.get("node", {}).get("memory", {}).get("workingSetBytes")
        available = summary.get("node", {}).get("memory", {}).get("availableBytes")
        if not all(
            isinstance(value, int) and value >= 0 for value in (cpu, memory, available)
        ):
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
            if (
                ref.get("namespace") != CONTROL_NAMESPACE
                or ref.get("name") not in original_pods
            ):
                continue
            pod_cpu = pod.get("cpu", {}).get("usageNanoCores")
            pod_memory = pod.get("memory", {}).get("workingSetBytes")
            if not all(
                isinstance(value, int) and value >= 0 for value in (pod_cpu, pod_memory)
            ):
                fail(
                    f"Service Pod {ref.get('name')} has incomplete kubelet CPU/memory metrics"
                )
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
    port: int, method: str, path: str, token: str = "", payload: dict | list | None = None
) -> tuple[int, dict, float, dict]:
    bounded_timeout = budget_timeout(15)
    data = json.dumps(payload).encode() if payload is not None else None
    headers = {"Content-Type": "application/json-patch+json"
               if method == "PATCH" and type(payload) is list else "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    req = Request(
        f"http://127.0.0.1:{port}{path}", data=data, headers=headers, method=method
    )
    started = time.monotonic()
    try:
        with OPENER.open(req, timeout=bounded_timeout) as response:
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
    budget_timeout(15)
    if len(raw) > HTTP_BODY_MAX:
        fail(
            f"{method} {path} response exceeds {HTTP_BODY_MAX} bytes; outcome needs review"
        )
    if token and (
        token.encode() in raw
        or any(
            token in key or token in value for key, value in response_headers.items()
        )
    ):
        fail(
            f"{method} {path} response reflected the request credential; receipt withheld"
        )
    response_headers = {key.lower(): value for key, value in response_headers.items()}
    elapsed = time.monotonic() - started
    try:
        public = V3.decode(raw)
    except (ValueError, UnicodeError):
        public = {}
    return (
        status,
        response_headers,
        elapsed,
        {
            "body_length_bytes": len(raw),
            "body_sha256": hashlib.sha256(raw).hexdigest(),
            "queue_capacity_full": (
                status == 429
                and public
                == {
                    "code": "queue_capacity_exceeded",
                    "message": "queue limit is reached",
                    "retryable": True,
                }
            ),
        },
    )


def epoch_named_get(kind: str, name: str) -> dict:
    """Only the six existing epoch reads; no generic API client or caching."""
    if kind == "namespace" and name in {
        FIXTURE.get(role + "_namespace") for role in ("control", "runner", "receipt")
    }:
        path = f"/api/v1/namespaces/{name}"
        args = ("get", "namespace", name, "-o", "json")
    elif kind == "configmap" and name in {V3.GENESIS, V3.ACTIVE, V3.QUEUE}:
        path = f"/api/v1/namespaces/{NAMESPACE}/configmaps/{name}"
        args = ("-n", NAMESPACE, "get", "configmap", name, "-o", "json")
    else:
        fail("epoch GET is outside the closed original identity paths")
    if not API_READ_PROXY_ENABLED:
        return kjson(*args)
    request = Request(f"http://127.0.0.1:{API_PROXY_PORT}{path}", method="GET")
    try:
        with OPENER.open(request, timeout=budget_timeout(15)) as response:
            if response.status != 200:
                fail("original epoch named GET did not return HTTP 200")
            raw = response.read(HTTP_BODY_MAX + 1)
    except (HTTPError, OSError, URLError):
        fail("original epoch named GET is UNKNOWN; no replay or absence inference")
    budget_timeout(15)
    if len(raw) > HTTP_BODY_MAX:
        fail("original epoch named GET exceeds its byte bound")
    try:
        return V3.decode(raw)
    except (ValueError, UnicodeError):
        fail("original epoch named GET JSON is invalid")


def admission_port() -> int:
    """One serial request, alternating the two original independent frontdoors."""
    global POST_SEQUENCE
    if not retained_profile() or not HTTP_FRONTDOORS:
        return PORT
    if len(HTTP_FRONTDOORS) != 2 or any(child.poll() is not None for _, child in HTTP_FRONTDOORS):
        fail("an original HTTP frontdoor exited; do not replay an admission")
    port, _ = HTTP_FRONTDOORS[POST_SEQUENCE % 2]
    POST_SEQUENCE += 1  # An uncertain attempt is spent, never retried elsewhere.
    return port


def start_api_proxy(run_dir: Path) -> object:
    if hashlib.sha256(KUBECONFIG.read_bytes()).hexdigest() != FIXTURE["kubeconfig_sha256"]:
        fail("original kubeconfig changed before API proxy")
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", API_PROXY_PORT))
    with (run_dir / "kubectl-proxy.log").open("w", encoding="utf-8") as log:
        budget_timeout(15)
        child = subprocess.Popen(
            ["kubectl", "--kubeconfig", str(KUBECONFIG), "proxy", "--address=127.0.0.1",
             f"--port={API_PROXY_PORT}", "--api-prefix=/"],
            stdout=log, stderr=subprocess.STDOUT,
        )
    return child


def qualify_api_proxy(child: object) -> None:
    global API_READ_PROXY_ENABLED
    path = f"/api/v1/namespaces/{FIXTURE['runner_namespace']}"
    for _ in range(30):
        if child.poll() is not None:
            fail("original Kubernetes API proxy exited")
        try:
            status = request_json(API_PROXY_PORT, "GET", path)[0]
        except BenchError:
            pause(1)
            continue  # Only pre-ready read transport, never a mutation replay.
        if status != 200:
            fail("original namespace readiness read is UNKNOWN, not absence")
        API_READ_PROXY_ENABLED = True
        qualify_epoch()  # Identity/readback failures are not retried.
        return
    fail("original Kubernetes API proxy did not become ready")


def build_id(key: str) -> str:
    return "idem-" + hashlib.sha256(("kova:e2e\0" + key).encode()).hexdigest()[:20]


def build_api_path(name: str) -> str:
    if not re.fullmatch(r"idem-[0-9a-f]{20}", name):
        fail("refusing to address a KovaBuild outside the generated ID format")
    return f"/apis/kova.cofy.dev/v1alpha1/namespaces/{NAMESPACE}/kovabuilds/{quote(name, safe='')}"


def delete_build_with_uid(name: str, uid: str, kubeconfig_sha256: str) -> dict:
    if not isinstance(uid, str) or not V3.UID.fullmatch(uid):
        fail("refusing KovaBuild deletion without its exact recorded UID")
    if hashlib.sha256(KUBECONFIG.read_bytes()).hexdigest() != kubeconfig_sha256:
        fail("dedicated kubeconfig bytes changed before UID-precondition deletion")
    if FIXTURE:
        qualify_epoch()
        current = kjson("-n", NAMESPACE, "get", "kovabuild", name, "-o", "json")
        V3.identity(current, name, NAMESPACE, uid)
        if current["spec"]["source"] != {"uri": source_uri(), "digest": SOURCE_DIGEST}:
            fail("exact build source changed before UID deletion")
    status, _, elapsed, body_fingerprint = request_json(
        API_PROXY_PORT,
        "DELETE",
        build_api_path(name),
        payload={
            "apiVersion": "meta.k8s.io/v1",
            "kind": "DeleteOptions",
            "preconditions": {"uid": uid},
            "propagationPolicy": "Background",
        },
    )
    receipt = {
        "id": name,
        "uid_precondition": uid,
        "http_status": status,
        "elapsed_seconds": elapsed,
        "body_fingerprint": body_fingerprint,
        "timestamp": now(),
    }
    receipt["accepted"] = status in (200, 202)
    return receipt


def check_runner(blocker_id: str, blocker_uid: str) -> dict:
    listing = kjson(
        "-n",
        NAMESPACE,
        "get",
        "pods",
        "-l",
        "app.kubernetes.io/name=kova-runner",
        "-o",
        "json",
    )
    # kubectl's generic JSON printer wraps selector results in v1/List.
    if listing.get("apiVersion") != "v1" or listing.get("kind") != "List":
        fail("runner inventory is not the typed kubectl List")
    runners = listing["items"]
    if len(runners) != 1 or runners[0]["metadata"]["name"] != f"kova-job-{blocker_id}":
        fail("queued builds gained a runner Pod or blocker Pod is absent")
    # Hash a fresh named GET, not an inferred/defaulted List item TypeMeta.
    original_uid = runners[0]["metadata"].get("uid")
    pod = kjson("-n", NAMESPACE, "get", "pod", f"kova-job-{blocker_id}", "-o", "json")
    if pod.get("apiVersion") != "v1" or pod.get("kind") != "Pod":
        fail("direct runner observation is not a typed Pod")
    V3.identity(pod, f"kova-job-{blocker_id}", NAMESPACE, original_uid)
    if (
        pod["metadata"].get("deletionTimestamp") is not None
        or pod["spec"].get("nodeSelector", {}).get("never") != "true"
        or pod["spec"].get("nodeName")
        or pod["status"].get("phase") != "Pending"
    ):
        fail("blocker runner is not unscheduled and Pending")
    owners = [
        owner
        for owner in pod["metadata"].get("ownerReferences", [])
        if owner.get("controller")
    ]
    if (
        len(owners) != 1
        or owners[0].get("kind") != "KovaBuild"
        or owners[0].get("name") != blocker_id
        or owners[0].get("uid") != blocker_uid
    ):
        fail("blocker runner owner UID does not match the test-owned KovaBuild")
    return pod


def metrics_forwards(pre: dict, deadline: float) -> list:
    kube = BURST.Kube(KUBECONFIG, f"kind-{CLUSTER}")
    forwards = []
    try:
        for name in sorted(pre["service_pods"]):
            forwards.append(
                BURST.Forward(
                    kube,
                    CONTROL_NAMESPACE,
                    name,
                    FIXTURE["ports"]["metrics_remote"],
                    deadline,
                )
            )
        return forwards
    except BaseException:
        close_local_processes(forwards, None, None, deadline)
        raise


def close_local_processes(metrics: list, forward: object, proxy: object,
                          deadline: float | None = None) -> None:
    """Stop only our own local children, with one shared ten-second bound."""
    until = time.monotonic() + 10
    if deadline is not None:
        until = min(until, deadline)
    children = [item.process for item in metrics]
    forwards = forward if type(forward) is list else [forward]
    children += [item for item in [*forwards, proxy] if item is not None]
    for child in children:
        if child.poll() is None:
            child.terminate()
    unresolved = False
    for child in children:
        if child.poll() is None:
            try:
                child.wait(timeout=max(0, until - time.monotonic()))
            except subprocess.TimeoutExpired:
                child.kill()
                try:
                    child.wait(timeout=max(0, until - time.monotonic()))
                except subprocess.TimeoutExpired:
                    unresolved = True
    for item in metrics:
        if item.process.stdout is not None:
            item.process.stdout.close()
    if unresolved:
        fail("owned local child shutdown did not resolve within its shared bound")


def normal_receipt_absence(objects: dict[str, dict], deadline: float) -> None:
    # Absence is checked only after the qualified original CR's one resolved
    # UID-delete. Never use this helper to release uncertain Create capacity.
    for name in objects:
        if time.monotonic() >= deadline:
            fail("normal receipt retirement deadline expired")
        path = f"/api/v1/namespaces/{RECEIPT_NAMESPACE}/configmaps/{name}"
        if request_json(API_PROXY_PORT, "GET", path)[0] != 404:
            fail("original ordinary receipt was not directly confirmed absent")


def full_capacity_recovery(
    pre: dict,
    run_dir: Path,
    expected_ids: dict[str, str],
    expected_uids: dict[str, str],
    blocker_id: str,
    blocker_uid: str,
    token: str,
    deadline: float,
) -> dict:
    directory = run_dir / "full-capacity-recovery"
    directory.mkdir()
    before = wait_queue_state(
        expected_ids,
        blocker_id,
        blocker_uid,
        directory,
        min(deadline, time.monotonic() + 5),
        expected_uids=expected_uids,
    )
    if len(expected_ids) + 1 != quota_limit():
        fail("full-capacity test is not actually full")
    key = run_dir.name + "-over-cap"
    ident = build_id(key)
    payload = {
        "source_uri": source_uri(),
        "source_digest": SOURCE_DIGEST,
        "targets": [
            {
                "target": f"unused.invalid/deep-queue/{FIXTURE['run_id']}:over-cap",
                "platform": pre["platform"],
            }
        ],
        "format": "oci",
        "concurrency": 1,
        "idempotency_key": key,
    }
    append_jsonl(
        directory / "attempts.jsonl",
        {"id": ident, "key": key, "event": "reject_attempt"},
    )
    qualify_epoch()
    status, _, elapsed, body = request_json(
        admission_port(), "POST", "/v1/builds", token=token, payload=payload
    )
    append_jsonl(
        directory / "responses.jsonl",
        {"id": ident, "status": status, "elapsed_seconds": elapsed, **body},
    )
    if status != 429 or body.get("queue_capacity_full") is not True:
        fail("full supported quota did not return the exact capacity rejection")
    if request_json(API_PROXY_PORT, "GET", build_api_path(ident))[0] != 404:
        fail("rejected extra request has a CR")
    after = wait_queue_state(
        expected_ids,
        blocker_id,
        blocker_uid,
        directory,
        min(deadline, time.monotonic() + 5),
        expected_uids=expected_uids,
    )
    if {item["metadata"]["uid"] for item in before["items"]} != {
        item["metadata"]["uid"] for item in after["items"]
    }:
        fail("accepted build identity changed during full rejection")
    # Free one ordinary queued request, not the active blocker or any ledger.
    victim = next(reversed(expected_ids))
    victim_build = next(
        item for item in after["items"] if item["metadata"]["name"] == victim
    )
    _, intents = queue_ledgers(len(expected_ids), blocker_id, blocker_uid)
    original_receipt_name = "kova-admission-intent-" + intents[victim]["nonce"]
    original_receipt = kjson(
        "-n", RECEIPT_NAMESPACE, "get", "configmap", original_receipt_name, "-o", "json"
    )
    V3.qualify_queue_receipt(
        original_receipt, victim_build, intents[victim], FIXTURE, CONTRACT
    )
    deletion = delete_build_with_uid(
        victim, expected_uids[victim], pre["kubeconfig_sha256"]
    )
    append_jsonl(
        directory / "responses.jsonl", {"event": "original_uid_release", **deletion}
    )
    if not deletion["accepted"]:
        fail("original queued UID deletion acknowledgement is unknown")
    release_deadline = min(deadline, time.monotonic() + 120)
    while time.monotonic() < release_deadline:
        _, current_intents = queue_ledgers(len(expected_ids), blocker_id, blocker_uid)
        if (
            request_json(API_PROXY_PORT, "GET", build_api_path(victim))[0] == 404
            and victim not in current_intents
        ):
            normal_receipt_absence(
                {original_receipt_name: original_receipt}, release_deadline
            )
            break
        pause(1)
    else:
        fail("original queued UID/receipt did not release within bounded deadline")
    del expected_ids[victim]
    del expected_uids[victim]
    key = run_dir.name + "-replacement"
    ident = build_id(key)
    payload["idempotency_key"] = key
    payload["targets"][0]["target"] = (
        f"unused.invalid/deep-queue/{FIXTURE['run_id']}:replacement"
    )
    append_jsonl(
        directory / "attempts.jsonl",
        {"id": ident, "key": key, "event": "readmission_attempt"},
    )
    qualify_epoch()
    status, headers, elapsed, body = request_json(
        admission_port(), "POST", "/v1/builds", token=token, payload=payload
    )
    append_jsonl(
        directory / "responses.jsonl",
        {
            "id": ident,
            "event": "readmission",
            "status": status,
            "elapsed_seconds": elapsed,
            **body,
        },
    )
    if status != 202 or headers.get("x-kova-build-id") != ident:
        fail("exact released capacity did not accept one fresh replacement")
    expected_ids[ident] = key
    builds = wait_queue_state(
        expected_ids,
        blocker_id,
        blocker_uid,
        directory,
        min(deadline, time.monotonic() + SETTLE_DEADLINE_SECONDS),
        expected_uids=expected_uids,
    )
    replacement = next(
        item for item in builds["items"] if item["metadata"]["name"] == ident
    )
    expected_uids[ident] = replacement["metadata"]["uid"]
    result = {
        "status": "passed",
        "charged_total": len(expected_ids) + 1,
        "active": 1,
        "queued": len(expected_ids),
        "rejected_id": build_id(run_dir.name + "-over-cap"),
        "released_id": victim,
        "released_uid": victim_build["metadata"]["uid"],
        "replacement_id": ident,
        "replacement_uid": expected_uids[ident],
    }
    save_json(directory / "summary.json", result)
    return result


def leader_observation() -> tuple[dict, str]:
    lease = kjson(
        "-n",
        CONTROL_NAMESPACE,
        "get",
        "lease",
        "kova-service.kova.cofy.dev",
        "-o",
        "json",
    )
    holder = lease.get("spec", {}).get("holderIdentity", "")
    leader = holder.split("_", 1)[0] if type(holder) is str else ""
    pods = service_pods()
    if leader not in {pod["metadata"]["name"] for pod in pods}:
        fail("Lease holder is not one of the exact ready Service Pods")
    return lease, leader


def qualify_service_owner(pod: dict, pre: dict) -> None:
    owners = [
        owner
        for owner in pod["metadata"].get("ownerReferences", [])
        if owner.get("controller") is True
    ]
    if len(owners) != 1 or owners[0].get("kind") != "ReplicaSet":
        fail("Service Pod has no exact controller ReplicaSet")
    rs = kjson(
        "-n", CONTROL_NAMESPACE, "get", "replicaset", owners[0]["name"], "-o", "json"
    )
    V3.identity(rs, owners[0]["name"], CONTROL_NAMESPACE, owners[0]["uid"])
    owners = [
        owner
        for owner in rs["metadata"].get("ownerReferences", [])
        if owner.get("controller") is True
    ]
    if (
        len(owners) != 1
        or owners[0].get("kind") != "Deployment"
        or owners[0].get("name") != f"{RELEASE}-service"
        or owners[0].get("uid") != pre["service_deployment"]["uid"]
    ):
        fail("Service Pod is not owned by the original Deployment")
    deployed_image_fact(
        pod,
        "kova-service",
        pre["local_role_images"]["controller"]["reference"],
        pre["local_role_images"]["controller"]["config_digest"],
    )


def leader_handoff(
    pre: dict,
    run_dir: Path,
    expected_ids: dict[str, str],
    expected_uids: dict[str, str],
    blocker_id: str,
    blocker_uid: str,
    deadline: float,
) -> dict:
    directory = run_dir / "leader-handoff"
    directory.mkdir()
    qualify_epoch()
    wait_queue_state(
        expected_ids,
        blocker_id,
        blocker_uid,
        directory,
        min(deadline, time.monotonic() + 5),
        expected_uids=expected_uids,
    )
    lease, leader = leader_observation()
    if lease["metadata"]["uid"] != pre["leader_lease_uid"]:
        fail("original Lease UID changed")
    if (
        leader != pre["initial_leader"]
        or lease["spec"]["holderIdentity"] != pre["leader_holder"]
    ):
        fail("leader naturally changed before the planned original leader UID deletion")
    pod = kjson("-n", CONTROL_NAMESPACE, "get", "pod", leader, "-o", "json")
    V3.identity(pod, leader, CONTROL_NAMESPACE, pre["service_pods"][leader])
    qualify_service_owner(pod, pre)
    deployment = kjson(
        "-n", CONTROL_NAMESPACE, "get", "deployment", f"{RELEASE}-service", "-o", "json"
    )
    if (
        deployment["metadata"]["uid"] != pre["service_deployment"]["uid"]
        or object_sha256(
            {
                key: value
                for key, value in deployment["spec"].items()
                if key != "replicas"
            }
        )
        != pre["service_deployment"]["spec_except_replicas_sha256"]
    ):
        fail("original Service Deployment changed before handoff")
    latest, latest_leader = leader_observation()
    if (
        latest["metadata"]["uid"] != lease["metadata"]["uid"]
        or latest["spec"]["holderIdentity"] != lease["spec"]["holderIdentity"]
        or latest_leader != leader
    ):
        fail("leader changed before the sole planned UID deletion")
    uid = pod["metadata"]["uid"]
    path = f"/api/v1/namespaces/{CONTROL_NAMESPACE}/pods/{leader}"
    effect = {
        "qualified_original_leader_uid": uid,
        "qualified_original_leader_name": leader,
        "delete_wire_attempted": False,
        "http_ack_code": None,
        "original_leader_direct_absent": False,
        "other_original_uids_last_verified": None,
    }
    pre["leader_handoff_effect"] = effect
    append_jsonl(
        directory / "events.jsonl",
        {
            "event": "leader_delete_attempt",
            "pod": leader,
            "uid": uid,
            "lease_uid": lease["metadata"]["uid"],
        },
    )
    effect["delete_wire_attempted"] = True
    status, _, elapsed, body = request_json(
        API_PROXY_PORT,
        "DELETE",
        path,
        payload={
            "apiVersion": "meta.k8s.io/v1",
            "kind": "DeleteOptions",
            "preconditions": {"uid": uid},
            "propagationPolicy": "Background",
        },
    )
    effect["http_ack_code"] = status
    append_jsonl(
        directory / "events.jsonl",
        {
            "event": "leader_delete_response",
            "http_status": status,
            "elapsed_seconds": elapsed,
            **body,
        },
    )
    if status not in (200, 202):
        fail("leader deletion outcome is unknown")
    started = time.monotonic()
    handoff_deadline = min(deadline, started + 120)
    survivors = {
        name: original
        for name, original in pre["service_pods"].items()
        if name != leader
    }
    while time.monotonic() < handoff_deadline:
        qualify_epoch()
        try:
            current_lease, elected = leader_observation()
            pods = service_pods()
        except BenchError:
            pause(1)
            continue
        if current_lease["metadata"]["uid"] != lease["metadata"]["uid"]:
            fail("Lease UID changed during handoff")
        if current_lease["spec"]["holderIdentity"] == lease["spec"]["holderIdentity"]:
            pause(1)
            continue
        current = {item["metadata"]["name"]: item["metadata"]["uid"] for item in pods}
        if (
            elected == leader
            or leader in current
            or any(
                current.get(name) != original for name, original in survivors.items()
            )
        ):
            fail("planned replacement also changed an unrelated original Service Pod")
        for item in pods:
            qualify_service_owner(item, pre)
        if request_json(API_PROXY_PORT, "GET", path)[0] != 404:
            pause(1)
            continue
        effect["original_leader_direct_absent"] = True
        effect["other_original_uids_last_verified"] = dict(survivors)
        pre["pods"] = pods
        pre["service_pods"] = current
        pre["service_pod_image_ids"] = {
            item["metadata"]["name"]: next(
                c["imageID"]
                for c in item["status"]["containerStatuses"]
                if c["name"] == "kova-service"
            )
            for item in pods
        }
        wait_queue_state(
            expected_ids,
            blocker_id,
            blocker_uid,
            directory,
            min(deadline, time.monotonic() + SETTLE_DEADLINE_SECONDS),
            expected_uids=expected_uids,
        )
        result = {
            "status": "passed",
            "deleted_leader": leader,
            "deleted_uid": uid,
            "lease_uid": lease["metadata"]["uid"],
            "new_holder": current_lease["spec"]["holderIdentity"],
            "new_service_pods": current,
            "handoff_seconds": time.monotonic() - started,
            "active": 1,
            "queued": len(expected_ids),
            "original_blocker_uid": blocker_uid,
        }
        save_json(directory / "summary.json", result)
        return result
    fail(
        "distinct leader/two same-image ready replicas did not converge within 120 seconds"
    )


def wait_queue_state(
    expected_ids: dict[str, str],
    blocker_id: str,
    blocker_uid: str,
    stage_dir: Path,
    deadline: float,
    on_poll: Callable[[], None] | None = None,
    expected_uids: dict[str, str] | None = None,
) -> dict:
    expected = set(expected_ids)
    while time.monotonic() < deadline:
        if on_poll is not None:
            on_poll()
        builds = kjson("-n", NAMESPACE, "get", "kovabuilds", "-o", "json")
        by_name = {item["metadata"]["name"]: item for item in builds.get("items", [])}
        if set(by_name) != expected | {blocker_id}:
            fail("KovaBuild set differs from the exact accepted benchmark IDs")
        if expected_uids and any(
            by_name[name]["metadata"]["uid"] != uid
            for name, uid in expected_uids.items()
        ):
            fail("an accepted KovaBuild UID changed during the benchmark")
        blocker = by_name[blocker_id]
        if (
            blocker["metadata"]["uid"] != blocker_uid
            or blocker.get("status", {}).get("phase") != "Starting"
        ):
            fail("active blocker phase or UID changed")
        if any(
            by_name[name]["spec"]["idempotencyKey"] != expected_ids[name]
            or by_name[name]["spec"]["source"]["uri"] != source_uri()
            for name in expected
        ):
            fail("a queued KovaBuild differs from its exact immutable request")
        phases = {
            name: by_name[name].get("status", {}).get("phase", "") for name in expected
        }
        if any(phase not in ("", "Queued") for phase in phases.values()):
            fail("a queued KovaBuild entered an unexpected phase")
        check_runner(blocker_id, blocker_uid)
        _, intents = queue_ledgers(len(expected), blocker_id, blocker_uid)
        charged = expected | {blocker_id}
        if set(intents) - charged:
            fail("queue ledger contains an unrecorded intent")
        if (
            all(phase == "Queued" for phase in phases.values())
            and set(intents) == charged
        ):
            qualify_build_receipts(by_name, blocker_id, blocker_uid)
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
            save_json(
                stage_dir / "builds-queued.json",
                {
                    "items": [
                        {
                            "name": name,
                            "uid": by_name[name]["metadata"]["uid"],
                            "phase": by_name[name].get("status", {}).get("phase"),
                            "created_at": by_name[name]["metadata"].get(
                                "creationTimestamp"
                            ),
                            "queue_intent_nonce": by_name[name]["metadata"]
                            .get("annotations", {})
                            .get("kova.cofy.dev/queue-intent"),
                        }
                        for name in sorted(expected)
                    ]
                },
            )
            return builds
        pause(2)
    fail("queued CR status/ledger did not converge before the bounded deadline")


def metric_delta(before: dict[str, float], after: dict[str, float], seconds: float) -> dict:
    if not math.isfinite(seconds) or seconds <= 0:
        fail("API measurement window must have a positive finite duration")
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
        "by_verb_group_resource_qps": {
            label: count / seconds for label, count in per_verb_resource.items()
        },
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
    qualify_epoch()
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
    if set(intents) - (set(expected_ids) | {blocker_id}):
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
    global CONTRACT
    if EXECUTION_DEADLINE is None:
        start_execution_budget()
    budget_timeout(RUN_DEADLINE_SECONDS)
    if sys.platform != "linux":
        fail("deep-queue acceptance runs only on an isolated Linux Kind host")
    if os.environ.get("KIND_CLUSTER", CLUSTER) != CLUSTER or os.environ.get(
        "KIND_KUBECONFIG", str(KUBECONFIG)
    ) != str(KUBECONFIG):
        fail(
            "KIND_CLUSTER/KIND_KUBECONFIG must name the fixed disposable deep-queue Kind cluster"
        )
    if not FIXTURE or kova_fixture_changed():
        fail("original v3 fixture file is unavailable or changed")
    native_identity = native_template_result("identity", b"", {"candidate_commit", "os", "architecture"})
    if native_identity["os"] != "linux" or native_identity["architecture"] != "amd64":
        fail("native Pod-template binary platform differs")
    for binary in ("kind", "kubectl", "docker", "helm", "git", "timeout"):
        if shutil.which(binary) is None:
            fail(f"missing required command {binary}")
    kova_commit = command(["git", "-C", str(ROOT), "rev-parse", "HEAD"]).strip()
    if not re.fullmatch(r"[0-9a-f]{40}", kova_commit):
        fail("Kova checkout HEAD is not an exact commit")
    if kova_commit != FIXTURE["candidate_commit"]:
        fail("checkout differs from the original candidate commit")
    if command(
        ["git", "-C", str(ROOT), "status", "--porcelain", "--untracked-files=normal"]
    ).strip():
        fail("Kova checkout is dirty; benchmark candidate must be a clean commit")
    fingerprint = exact_kind_identity()
    system = kjson("get", "namespace", "kube-system", "-o", "json")
    V3.identity(system, "kube-system", None, FIXTURE["kube_system_namespace_uid"])
    nodes = good_nodes(kjson("get", "nodes", "-o", "json"))
    deployment = kjson(
        "-n", CONTROL_NAMESPACE, "get", "deployment", f"{RELEASE}-service", "-o", "json"
    )
    V3.identity(
        deployment,
        f"{RELEASE}-service",
        CONTROL_NAMESPACE,
        FIXTURE["service_deployment_uid"],
    )
    quiet_duration = quiet_seconds()
    principal, platform = single_service_args(deployment)
    pods = service_pods()
    if {pod["metadata"]["name"]: pod["metadata"]["uid"] for pod in pods} != FIXTURE[
        "service_pods"
    ]:
        fail("original Service Pod UIDs differ")
    secret = kjson(
        "-n",
        CONTROL_NAMESPACE,
        "get",
        "secret",
        FIXTURE["genesis"]["secret_name"],
        "-o",
        "json",
    )
    V3.identity(
        secret,
        FIXTURE["genesis"]["secret_name"],
        CONTROL_NAMESPACE,
        FIXTURE["genesis"]["secret_uid"],
    )
    if (
        secret.get("immutable") is not True
        or secret.get("type") != "Opaque"
        or set(secret.get("data", {})) != {"receipt.json"}
    ):
        fail("original installation receipt Secret shape differs")
    raw_receipt = base64.b64decode(secret["data"]["receipt.json"], validate=True)
    if (
        len(raw_receipt) > 8192
        or hashlib.sha256(raw_receipt).hexdigest()
        != FIXTURE["genesis"]["receipt_sha256"]
    ):
        fail("original installation receipt bytes changed")
    CONTRACT = V3.qualify_contract(V3.decode(raw_receipt), FIXTURE)
    qualify_epoch()
    helm_status = json.loads(
        command(
            [
                "helm",
                "status",
                RELEASE,
                "-n",
                CONTROL_NAMESPACE,
                "--kubeconfig",
                str(KUBECONFIG),
                "-o",
                "json",
            ]
        )
    )
    if helm_status.get("info", {}).get("status") != "deployed":
        fail("the dedicated Kova Helm release is not deployed")
    worker = kjson("-n", CONTROL_NAMESPACE, "get", "deployment", RELEASE, "-o", "json")
    V3.identity(worker, RELEASE, CONTROL_NAMESPACE, FIXTURE["worker_deployment_uid"])
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
    runner_arg = next(
        arg for arg in service_container["args"] if arg.startswith("--runner-image=")
    )
    token_env = [
        item
        for item in service_container.get("env", [])
        if item.get("name") == "KOVA_SERVICE_AUTH_TOKEN"
    ]
    if len(token_env) != 1 or token_env[0].get("valueFrom", {}).get("secretKeyRef") != {
        "name": "kova-e2e-token",
        "key": "token",
    }:
        fail("Service static token source differs from the dedicated test Secret")
    if (
        len(worker_containers) != 1
        or worker_containers[0].get("imagePullPolicy") != "Never"
    ):
        fail("worker image must be preloaded with imagePullPolicy Never")
    image_facts = {
        "controller": local_image_fact(service_container["image"]),
        "runner": local_image_fact(runner_arg.split("=", 1)[1]),
        "worker": local_image_fact(worker_containers[0]["image"]),
    }
    if len({fact["revision"] for fact in image_facts.values()}) != 1:
        fail("controller, runner, and worker local image revision labels differ")
    if any(
        fact["revision"] not in (kova_commit, kova_commit[:12])
        for fact in image_facts.values()
    ):
        fail("local role image revisions differ from the clean Kova checkout HEAD")
    if {role: fact["reference"] for role, fact in image_facts.items()} != FIXTURE[
        "role_images"
    ]:
        fail("role images differ from the original manifest references")
    runtime_role_images = {}
    for node in nodes:
        node_name = node["metadata"]["name"]
        runtime_role_images[node_name] = {}
        for role, fact in image_facts.items():
            runtime = kind_runtime_image_fact(node_name, fact["reference"])
            if runtime["image_id"] != fact["config_digest"]:
                fail(f"Kind node {node_name} has a stale {role} image")
            runtime_role_images[node_name][role] = runtime
    worker_selector = worker["spec"]["selector"]["matchLabels"]
    if worker_selector != {
        "app.kubernetes.io/instance": RELEASE,
        "app.kubernetes.io/name": RELEASE,
    }:
        fail("worker Deployment selector differs from the dedicated Helm release")
    worker_pods = kjson(
        "-n",
        CONTROL_NAMESPACE,
        "get",
        "pods",
        "-l",
        f"app.kubernetes.io/instance={RELEASE},app.kubernetes.io/name={RELEASE}",
        "-o",
        "json",
    )["items"]
    if len(worker_pods) != 1 or not any(
        condition.get("type") == "Ready" and condition.get("status") == "True"
        for condition in worker_pods[0].get("status", {}).get("conditions", [])
    ):
        fail("worker Pod is not exactly one Ready Pod")
    if {
        pod["metadata"]["name"]: pod["metadata"]["uid"] for pod in worker_pods
    } != FIXTURE["worker_pods"]:
        fail("original worker Pod UIDs differ")
    deployed_images = {
        "service": [
            deployed_image_fact(
                pod,
                "kova-service",
                image_facts["controller"]["reference"],
                image_facts["controller"]["config_digest"],
            )
            for pod in pods
        ],
        "worker": deployed_image_fact(
            worker_pods[0],
            worker_containers[0]["name"],
            image_facts["worker"]["reference"],
            image_facts["worker"]["config_digest"],
        ),
    }
    if kjson("get", "kovabuilds", "--all-namespaces", "-o", "json")["items"]:
        fail("another KovaBuild exists in the dedicated Kind cluster")
    if kjson(
        "get",
        "pods",
        "--all-namespaces",
        "-l",
        "app.kubernetes.io/name=kova-runner",
        "-o",
        "json",
    )["items"]:
        fail("another runner Pod exists in the dedicated Kind cluster")
    queue_ledgers(0)
    if kjson("-n", NAMESPACE, "get", "pods", "-o", "json")["items"]:
        fail("fresh runner namespace contains pre-existing Pods")
    empty_receipts()
    docker_root = Path(
        command(["docker", "info", "--format", "{{.DockerRootDir}}"]).strip()
    )
    if not docker_root.is_dir():
        fail("Docker data root is unavailable")
    api_metrics()
    # Fail closed before writing if kubelet Pod/node metrics are unavailable
    # or the host is already too busy. This read-only sample writes no file.
    resource_sample(pods, None, docker_root)
    # Check the dedicated Secret before a detached launcher reports success;
    # keep the credential in process memory, never in the preflight receipt.
    load_static_token()
    lease, leader = leader_observation()
    return {
        "cluster": CLUSTER,
        "namespace": NAMESPACE,
        "control_namespace": CONTROL_NAMESPACE,
        "receipt_namespace": RECEIPT_NAMESPACE,
        "profile": FIXTURE["profile"],
        "fixture_sha256": FIXTURE_SHA,
        "v3_contract": CONTRACT,
        "leader_lease_uid": lease["metadata"]["uid"],
        "leader_holder": lease["spec"]["holderIdentity"],
        "initial_leader": leader,
        "kubeconfig_fingerprint": fingerprint,
        "kubeconfig_sha256": hashlib.sha256(KUBECONFIG.read_bytes()).hexdigest(),
        "node_uids": {
            node["metadata"]["name"]: node["metadata"]["uid"] for node in nodes
        },
        "service_deployment": {
            "uid": deployment["metadata"]["uid"],
            "resource_version": deployment["metadata"]["resourceVersion"],
            "labels_sha256": object_sha256(deployment["metadata"].get("labels", {})),
            "spec_except_replicas_sha256": object_sha256(
                {
                    key: value
                    for key, value in deployment["spec"].items()
                    if key != "replicas"
                }
            ),
            "image": service_container["image"],
            "image_container_index": deployment["spec"]["template"]["spec"][
                "containers"
            ].index(service_container),
        },
        "service_pods": {
            pod["metadata"]["name"]: pod["metadata"]["uid"] for pod in pods
        },
        "service_pod_image_ids": {
            pod["metadata"]["name"]: next(
                container["imageID"]
                for container in pod["status"]["containerStatuses"]
                if container["name"] == "kova-service"
            )
            for pod in pods
        },
        "local_role_images": image_facts,
        "kind_runtime_role_images": runtime_role_images,
        "deployed_images": deployed_images,
        "helm_chart": helm_status.get("chart"),
        "kova_commit": kova_commit,
        "principal": principal,
        "platform": platform,
        "quiet_seconds": quiet_duration,
        "docker_root": str(docker_root),
        "pods": pods,
    }


def load_static_token() -> str:
    secret = kjson(
        "-n", CONTROL_NAMESPACE, "get", "secret", "kova-e2e-token", "-o", "json"
    )
    if (
        secret.get("metadata", {}).get("name") != "kova-e2e-token"
        or secret.get("metadata", {}).get("namespace") != CONTROL_NAMESPACE
        or secret.get("type") != "Opaque"
    ):
        fail("dedicated static authentication Secret identity changed")
    if FIXTURE:
        V3.identity(
            secret, "kova-e2e-token", CONTROL_NAMESPACE, FIXTURE["auth_secret_uid"]
        )
    encoded = secret.get("data", {}).get("token")
    if not isinstance(encoded, str):
        fail("dedicated static authentication Secret has no token key")
    try:
        token = base64.b64decode(encoded, validate=True).decode("utf-8")
    except (binascii.Error, UnicodeError):
        fail("dedicated static authentication Secret token is not valid UTF-8/base64")
    if len(token) < 16 or any(ord(char) < 33 or ord(char) > 126 for char in token):
        fail("dedicated static authentication Secret token has invalid length/characters")
    inherited = os.environ.get("SERVICE_AUTH_TOKEN")
    if inherited and not hmac.compare_digest(inherited, token):
        fail("inherited SERVICE_AUTH_TOKEN differs from the dedicated test Secret")
    os.environ.pop("SERVICE_AUTH_TOKEN", None)
    return token


def capture_failure_objects(run_dir: Path, token: str, known_ids: set[str]) -> dict[str, str]:
    snapshots = run_dir / "failure-snapshots"
    snapshots.mkdir(exist_ok=True)
    os.chmod(snapshots, 0o700)
    outcome = {}
    try:
        builds = kjson("-n", NAMESPACE, "get", "kovabuilds", "-o", "json")
        projection = [
            {
                "name": item.get("metadata", {}).get("name"),
                "uid": item.get("metadata", {}).get("uid"),
                "phase": item.get("status", {}).get("phase"),
                "known_run_id": item.get("metadata", {}).get("name") in known_ids,
            }
            for item in builds.get("items", [])
        ]
        save_without_token(snapshots / "builds-projection.json", projection, token)
        outcome["builds"] = "projected"
    except Exception as error:
        outcome["builds"] = f"unavailable: {safe_error(error, token)}"
    for name, configmap in (
        ("active-ledger", "kova-service-admission"),
        ("queue-ledger", "kova-service-queue-admission"),
    ):
        try:
            obj = kjson("-n", NAMESPACE, "get", "configmap", configmap, "-o", "json")
            data = obj.get("data", {})
            projection = {
                "name": obj.get("metadata", {}).get("name"),
                "uid": obj.get("metadata", {}).get("uid"),
                "resource_version": obj.get("metadata", {}).get("resourceVersion"),
                "data_keys": sorted(data),
                "data_sha256": object_sha256(data),
            }
            save_without_token(snapshots / f"{name}-projection.json", projection, token)
            outcome[name] = "projected"
        except Exception as error:
            outcome[name] = f"unavailable: {safe_error(error, token)}"
    return outcome


def service_owned_pod_projection(deployment_uid: str) -> list[dict]:
    def complete_items(kind: str) -> list:
        inventory = kjson("-n", CONTROL_NAMESPACE, "get", kind, "-o", "json")
        metadata = inventory.get("metadata", {})
        items = inventory.get("items")
        if (
            inventory.get("apiVersion") != "v1" or inventory.get("kind") != "List"
            or type(metadata) is not dict or metadata.get("continue")
            or metadata.get("remainingItemCount", 0) != 0 or type(items) is not list
            or len(items) > 32
            or len({(p["metadata"]["name"], p["metadata"]["uid"]) for p in items}) != len(items)
            or any(p["metadata"].get("namespace") != CONTROL_NAMESPACE for p in items)
        ):
            fail("Service owner inventory is malformed, paginated or exceeds its bound")
        return items

    replicasets = complete_items("replicasets")
    owned_rs_uids = {
        item["metadata"]["uid"]
        for item in replicasets
        if any(
            owner.get("kind") == "Deployment" and owner.get("uid") == deployment_uid
            for owner in item["metadata"].get("ownerReferences", [])
        )
    }
    pods = complete_items("pods")
    selected = []
    for pod in pods:
        metadata = pod["metadata"]
        labels = metadata.get("labels", {})
        owners = metadata.get("ownerReferences", [])
        if not (
            labels.get("app.kubernetes.io/instance") == RELEASE
            and labels.get("app.kubernetes.io/component") == "service"
        ) and not any(owner.get("uid") in owned_rs_uids for owner in owners):
            continue
        selected.append(
            {
                "name": metadata["name"],
                "uid": metadata["uid"],
                "phase": pod.get("status", {}).get("phase"),
                "image_ids": {
                    item.get("name"): item.get("imageID")
                    for item in pod.get("status", {}).get("containerStatuses", [])
                },
            }
        )
    return sorted(selected, key=lambda item: item["name"])


def qualify_stop_deployment(obj: dict, pre: dict, replicas: int) -> None:
    original = pre["service_deployment"]
    V3.identity(obj, f"{RELEASE}-service", CONTROL_NAMESPACE, original["uid"])
    metadata, spec = obj["metadata"], obj.get("spec", {})
    if (
        obj.get("apiVersion") != "apps/v1" or obj.get("kind") != "Deployment"
        or metadata.get("deletionTimestamp") is not None
        or type(metadata.get("generation")) is not int or metadata["generation"] < 1
        or type(metadata.get("resourceVersion")) is not str or not metadata["resourceVersion"]
        or type(spec.get("replicas")) is not int or spec["replicas"] != replicas
        or object_sha256(metadata.get("labels", {})) != original["labels_sha256"]
        or object_sha256({key: value for key, value in spec.items() if key != "replicas"})
        != original["spec_except_replicas_sha256"]
    ):
        fail("original Deployment identity/spec differs before/after one planned stop")


def retained_service_stop(pre: dict, directory: Path, deadline: float) -> dict:
    """A predeclared successful-run stop, never UNKNOWN cleanup or a retry."""
    if not retained_profile() or pre.get("retained_stop_effect") is not None:
        fail("retained stop is not predeclared or its sole attempt was spent")
    qualify_epoch()
    deployment = kjson("-n", CONTROL_NAMESPACE, "get", "deployment", f"{RELEASE}-service", "-o", "json")
    qualify_stop_deployment(deployment, pre, 2)
    current_pods = service_pods()
    if {p["metadata"]["name"]: p["metadata"]["uid"] for p in current_pods} != pre["service_pods"]:
        fail("post-handoff original/replacement Service Pod identities changed before stop")
    for pod in current_pods:
        qualify_service_owner(pod, pre)
    observed = deployment["metadata"]
    # Original UID/spec were fixed before the run. This fresh opaque RV is
    # fixed in the intent before one wire attempt; no CAS conflict replay.
    effect = {
        "deployment_uid": observed["uid"], "resource_version": observed["resourceVersion"],
        "from_replicas": 2, "to_replicas": 0, "patch_attempted": True,
        "acknowledged": False, "deployment_zero_observed": False,
        "service_pod_set_empty": False, "direct_absent_original_or_planned_uids": {},
    }
    pre["retained_stop_effect"] = effect
    save_json(directory / "service-stop-intent.json", effect)
    payload = [
        {"op": "test", "path": "/metadata/uid", "value": observed["uid"]},
        {"op": "test", "path": "/metadata/resourceVersion", "value": observed["resourceVersion"]},
        {"op": "test", "path": "/spec/replicas", "value": 2},
        {"op": "replace", "path": "/spec/replicas", "value": 0},
    ]
    path = f"/apis/apps/v1/namespaces/{CONTROL_NAMESPACE}/deployments/{RELEASE}-service"
    status, _, _, body = request_json(API_PROXY_PORT, "PATCH", path, payload=payload)
    effect.update(http_status=status, response_fingerprint=body)
    save_json(directory / "service-stop-effect.json", effect)
    if status != 200:
        fail("one planned UID/RV Deployment stop acknowledgement is UNKNOWN")
    effect["acknowledged"] = True
    known_pods = dict(FIXTURE["service_pods"])
    for name, identity in pre["service_pods"].items():
        if name in known_pods and known_pods[name] != identity:
            fail("planned replacement reused an original Service Pod name")
        known_pods[name] = identity
    while time.monotonic() < deadline:
        qualify_epoch()
        current = kjson("-n", CONTROL_NAMESPACE, "get", "deployment", f"{RELEASE}-service", "-o", "json")
        qualify_stop_deployment(current, pre, 0)
        state = current.get("status", {})
        if (
            state.get("observedGeneration") == current["metadata"].get("generation")
            and all(type(state.get(key, 0)) is int and state.get(key, 0) == 0
                    for key in ("replicas", "readyReplicas", "updatedReplicas", "availableReplicas", "unavailableReplicas"))
            and not service_owned_pod_projection(observed["uid"])
        ):
            effect["deployment_zero_observed"] = True
            effect["service_pod_set_empty"] = True
            for name, identity in known_pods.items():
                path = f"/api/v1/namespaces/{CONTROL_NAMESPACE}/pods/{name}"
                if request_json(API_PROXY_PORT, "GET", path)[0] != 404:
                    fail("an original/planned replacement Service Pod was not directly absent")
                effect["direct_absent_original_or_planned_uids"][name] = identity
            save_json(directory / "service-stop-effect.json", effect)
            return effect
        pause(1)
    fail("planned Service stop did not converge within the original bounded deadline")


def retained_finish(
    pre: dict, run_dir: Path, expected_ids: dict[str, str], expected_uids: dict[str, str],
    blocker_id: str, blocker_uid: str, deadline: float,
) -> dict:
    """Retire only the active blocker, promote one original waiter, retain the rest."""
    directory = run_dir / "retained-success"
    directory.mkdir()
    before = wait_queue_state(expected_ids, blocker_id, blocker_uid, directory,
                              min(deadline, time.monotonic() + 5), expected_uids=expected_uids)
    by_name = {item["metadata"]["name"]: item for item in before["items"]}
    objects = qualify_build_receipts(by_name, blocker_id, blocker_uid)
    grants, intents = queue_ledgers(len(expected_ids), blocker_id, blocker_uid)
    grant = grants[blocker_uid]
    retired_names = {
        "kova-admission-intent-" + intents[blocker_id]["nonce"],
        "kova-grant-intent-" + grant["grantNonce"],
        "kova-pod-create-intent-" + grant["podAttemptNonce"],
    }
    blocker_receipts = {name: objects[name] for name in retired_names}
    retained_queue_receipts = {
        name: obj["metadata"]["uid"] for name, obj in objects.items() if name not in retired_names
    }
    original_runner = check_runner(blocker_id, blocker_uid)
    challenger_id = next(iter(expected_ids))
    challenger_uid = expected_uids[challenger_id]
    # Named immutable CR/spec/receipt qualification precedes the sole DELETE.
    pre["retained_promotion_effect"] = {
        "blocker_id": blocker_id, "blocker_uid": blocker_uid, "delete_attempted": True,
        "original_challenger_id": challenger_id, "original_challenger_uid": challenger_uid,
    }
    save_json(directory / "promotion-intent.json", pre["retained_promotion_effect"])
    deletion = delete_build_with_uid(blocker_id, blocker_uid, pre["kubeconfig_sha256"])
    pre["retained_promotion_effect"]["delete_response"] = deletion
    save_json(directory / "promotion-effect.json", pre["retained_promotion_effect"])
    if not deletion["accepted"]:
        fail("original active blocker UID release acknowledgement is UNKNOWN")
    promotion_started = time.monotonic()
    promotion_deadline = min(deadline, promotion_started + 120)
    while time.monotonic() < promotion_deadline:
        inventory = kjson("-n", NAMESPACE, "get", "kovabuilds", "-o", "json")
        current = {item["metadata"]["name"]: item for item in inventory["items"]}
        if len(current) != len(inventory["items"]) or set(current) - (set(expected_ids) | {blocker_id}):
            fail("unrecorded/duplicate build appeared during retained promotion")
        if not set(expected_ids) <= set(current) or any(
            current[name]["metadata"]["uid"] != expected_uids[name]
            or current[name]["spec"] != by_name[name]["spec"] for name in expected_ids
        ):
            fail("original waiting CR UID/spec changed during retained promotion")
        challenger = current[challenger_id]
        if any(current[name].get("status", {}).get("phase") != "Queued"
               for name in expected_ids if name != challenger_id):
            fail("a nonchallenger queued request changed phase during retained promotion")
        if challenger.get("status", {}).get("phase") not in ("Queued", "Starting"):
            fail("original challenger entered an unexpected phase")
        if blocker_id not in current and challenger.get("status", {}).get("phase") == "Starting":
            current_objects = qualify_build_receipts(current, challenger_id, challenger_uid)
            if any(current_objects.get(name, {}).get("metadata", {}).get("uid") != identity
                   for name, identity in retained_queue_receipts.items()):
                fail("an originally retained queue receipt changed UID")
            if request_json(API_PROXY_PORT, "GET", build_api_path(blocker_id))[0] != 404:
                fail("original released blocker CR was not directly absent")
            old_pod_path = f"/api/v1/namespaces/{NAMESPACE}/pods/{original_runner['metadata']['name']}"
            if request_json(API_PROXY_PORT, "GET", old_pod_path)[0] != 404:
                fail("original released blocker runner was not directly absent")
            normal_receipt_absence(blocker_receipts, promotion_deadline)
            break
        pause(1)
    else:
        fail("original challenger did not promote before the fixed 120s subdeadline")
    promotion = {
        "status": "passed", **pre["retained_promotion_effect"],
        "seconds": time.monotonic() - promotion_started,
        "retired_original_receipt_uids": {name: obj["metadata"]["uid"] for name, obj in blocker_receipts.items()},
    }
    save_json(directory / "promotion.json", promotion)
    # This is success-only disposition. Any prior UNKNOWN exits before scale0.
    stop = retained_service_stop(pre, directory, min(deadline, time.monotonic() + 120))
    final = wait_queue_state(
        {name: key for name, key in expected_ids.items() if name != challenger_id},
        challenger_id, challenger_uid, directory, min(deadline, time.monotonic() + 5),
        expected_uids=expected_uids,
    )
    final_objects = qualify_build_receipts(
        {item["metadata"]["name"]: item for item in final["items"]}, challenger_id, challenger_uid
    )
    if {name: obj["metadata"]["uid"] for name, obj in final_objects.items()} != {
        name: obj["metadata"]["uid"] for name, obj in current_objects.items()
    }:
        fail("retained effect receipt identities changed during planned Service stop")
    return {
        "status": "passed", "original_uid_capacity_promotion": promotion, "planned_service_stop": stop,
        "remaining_charges": len(expected_ids), "active": 1, "genuinely_queued": len(expected_ids) - 1,
        "free_charge_slots": quota_limit() - len(expected_ids), "all_capacity_released": False,
        "bulk_retirement_performed": False, "idle_baseline_claimed": False,
        "frontdoor_service_pods": 0, "runner_execution": False,
        "retained_receipt_uids": {name: obj["metadata"]["uid"] for name, obj in final_objects.items()},
    }


def emergency_stop(pre: dict, run_dir: Path, token: str) -> dict:
    """Stop only this exact Deployment; preserve all builds, ledgers and receipts."""
    outcome = {
        "status": "unconfirmed",
        "started_at": now(),
        "original_service_pods_deleted": False,
    }
    snapshots = run_dir / "failure-snapshots"
    snapshots.mkdir(exist_ok=True)
    os.chmod(snapshots, 0o700)
    try:
        if exact_kind_identity() != pre["kubeconfig_fingerprint"]:
            fail("dedicated Kind identity changed before emergency stop")
        try:
            nodes = kjson("get", "nodes", "-o", "json")["items"]
            outcome["node_uids_changed"] = {
                item["metadata"]["name"]: item["metadata"]["uid"] for item in nodes
            } != pre["node_uids"]
        except Exception as error:
            outcome["node_uid_observation"] = f"unavailable: {safe_error(error, token)}"
        try:
            before_pods = service_owned_pod_projection(pre["service_deployment"]["uid"])
            save_without_token(
                snapshots / "pre-stop-service-pods.json", before_pods, token
            )
            outcome["original_service_pods_changed"] = any(
                item["uid"] != pre["service_pods"].get(item["name"])
                or item["image_ids"].get("kova-service")
                != pre["service_pod_image_ids"].get(item["name"])
                for item in before_pods
            )
        except Exception as error:
            outcome["pre_stop_pods_snapshot"] = (
                f"unavailable: {safe_error(error, token)}"
            )
        for attempt in range(3):
            deployment = kjson(
                "-n", NAMESPACE, "get", "deployment", f"{RELEASE}-service", "-o", "json"
            )
            metadata = deployment.get("metadata", {})
            spec = deployment.get("spec", {})
            if (
                metadata.get("uid") != pre["service_deployment"]["uid"]
                or metadata.get("name") != f"{RELEASE}-service"
                or metadata.get("namespace") != NAMESPACE
                or metadata.get("deletionTimestamp")
                or object_sha256(metadata.get("labels", {}))
                != pre["service_deployment"]["labels_sha256"]
                or object_sha256(
                    {key: value for key, value in spec.items() if key != "replicas"}
                )
                != pre["service_deployment"]["spec_except_replicas_sha256"]
            ):
                fail(
                    "Service Deployment identity or non-replica spec changed before emergency stop"
                )
            replicas = spec.get("replicas")
            if replicas == 0:
                outcome["patch_applied"] = False
                break
            if replicas != 2:
                fail("Service Deployment replica count changed before emergency stop")
            try:
                save_without_token(
                    snapshots / "pre-stop-deployment-projection.json",
                    {
                        "name": metadata["name"],
                        "uid": metadata["uid"],
                        "resource_version": metadata["resourceVersion"],
                        "labels_sha256": object_sha256(metadata.get("labels", {})),
                        "spec_except_replicas_sha256": pre["service_deployment"][
                            "spec_except_replicas_sha256"
                        ],
                        "replicas": replicas,
                    },
                    token,
                )
            except Exception as error:
                outcome["pre_stop_deployment_snapshot"] = (
                    f"unavailable: {safe_error(error, token)}"
                )
            image_index = pre["service_deployment"]["image_container_index"]
            patch = [
                {"op": "test", "path": "/metadata/uid", "value": metadata["uid"]},
                {
                    "op": "test",
                    "path": "/metadata/resourceVersion",
                    "value": metadata["resourceVersion"],
                },
                {"op": "test", "path": "/spec/replicas", "value": 2},
                {
                    "op": "test",
                    "path": f"/spec/template/spec/containers/{image_index}/image",
                    "value": pre["service_deployment"]["image"],
                },
                {"op": "replace", "path": "/spec/replicas", "value": 0},
            ]
            try:
                kctl(
                    "-n",
                    NAMESPACE,
                    "patch",
                    "deployment",
                    f"{RELEASE}-service",
                    "--type=json",
                    "-p",
                    json.dumps(patch),
                )
                outcome["patch_applied"] = True
                break
            except BenchError:
                if attempt == 2:
                    raise
                pause(1)
        deadline = time.monotonic() + EMERGENCY_STOP_DEADLINE_SECONDS
        while time.monotonic() < deadline:
            if exact_kind_identity() != pre["kubeconfig_fingerprint"]:
                fail("dedicated Kind identity changed after emergency stop")
            current = kjson(
                "-n", NAMESPACE, "get", "deployment", f"{RELEASE}-service", "-o", "json"
            )
            current_pods = service_owned_pod_projection(
                pre["service_deployment"]["uid"]
            )
            if (
                current.get("metadata", {}).get("uid")
                != pre["service_deployment"]["uid"]
            ):
                fail("Service Deployment UID changed after emergency stop")
            if current.get("spec", {}).get("replicas") != 0:
                fail("Service Deployment did not retain zero replicas")
            if (
                object_sha256(current.get("metadata", {}).get("labels", {}))
                != pre["service_deployment"]["labels_sha256"]
                or object_sha256(
                    {
                        key: value
                        for key, value in current.get("spec", {}).items()
                        if key != "replicas"
                    }
                )
                != pre["service_deployment"]["spec_except_replicas_sha256"]
            ):
                fail("Service Deployment identity changed after emergency stop")
            if not current_pods:
                outcome.update(
                    {
                        "status": "confirmed",
                        "original_service_pods_deleted": True,
                        "completed_at": now(),
                    }
                )
                try:
                    save_without_token(
                        snapshots / "post-stop-deployment-projection.json",
                        {
                            "uid": current["metadata"]["uid"],
                            "resource_version": current["metadata"]["resourceVersion"],
                            "replicas": 0,
                            "service_owned_pods": [],
                        },
                        token,
                    )
                except Exception as error:
                    outcome["post_stop_snapshot"] = (
                        f"unavailable: {safe_error(error, token)}"
                    )
                return outcome
            pause(2)
        fail("Service Pods did not disappear before the emergency-stop deadline")
    except Exception as error:
        outcome["error"] = safe_error(error, token)
        outcome["completed_at"] = now()
        return outcome


def load_run_identity() -> tuple[Path, dict]:
    requested = os.environ.get("DEEP_QUEUE_E2E_RUN_DIR", "")
    base = ROOT / ".work" / "deep-queue"
    run_dir = Path(requested)
    if (
        not requested
        or not run_dir.is_absolute()
        or run_dir.is_symlink()
        or run_dir.parent.resolve() != base.resolve()
        or not run_dir.is_dir()
    ):
        fail("DEEP_QUEUE_E2E_RUN_DIR must be one existing direct run directory")
    identity_file = run_dir / "identity.json"
    if not identity_file.is_file() or identity_file.is_symlink():
        fail("run directory has no regular identity.json")
    identity = json.loads(identity_file.read_text())
    if (
        identity.get("run_id") != run_dir.name
        or identity.get("cluster") != CLUSTER
        or identity.get("namespace") != NAMESPACE
    ):
        fail("run receipt identity differs from the fixed deep-queue cluster")
    return run_dir, identity


def run_status(run_dir: Path, identity: dict) -> None:
    if exact_kind_identity() != identity["kubeconfig_fingerprint"]:
        fail("dedicated Kind identity differs from the run receipt")
    deployment = kjson("-n", NAMESPACE, "get", "deployment", f"{RELEASE}-service", "-o", "json")
    builds = kjson("-n", NAMESPACE, "get", "kovabuilds", "-o", "json")["items"]
    pods = service_owned_pod_projection(identity["service_deployment"]["uid"])
    result_file = run_dir / "result.json"
    result = json.loads(result_file.read_text()) if result_file.is_file() else None
    print(
        json.dumps(
            {
                "run_dir": str(run_dir),
                "timestamp": now(),
                "deployment_uid_matches": deployment["metadata"]["uid"]
                == identity["service_deployment"]["uid"],
                "service_replicas": deployment["spec"].get("replicas"),
                "service_owned_pods": pods,
                "build_count": len(builds),
                "build_phases": {
                    phase: sum(item.get("status", {}).get("phase") == phase for item in builds)
                    for phase in ("Starting", "Queued", "Succeeded", "Failed")
                },
                "result": {
                    "status": result.get("status"),
                    "emergency_stop": result.get("emergency_stop"),
                    "original_service_pods_deleted": result.get("original_service_pods_deleted"),
                }
                if result
                else None,
            },
            sort_keys=True,
        )
    )


def observe_long_quiet(
    pre: dict,
    run_dir: Path,
    expected_ids: dict[str, str],
    expected_uids: dict[str, str],
    blocker_id: str,
    blocker_uid: str,
    overall_deadline: float,
) -> dict:
    duration = pre["quiet_seconds"]
    if duration == 0:
        return {"status": "disabled", "seconds": 0}
    if time.monotonic() + duration + CLEANUP_DEADLINE_SECONDS >= overall_deadline:
        fail("long quiet observation lacks the full bounded cleanup reserve")
    quiet_dir = run_dir / "stage-1000-quiet"
    quiet_dir.mkdir()
    summaries = []
    window_seconds = duration // 3
    for window in ("early", "middle", "late"):
        window_dir = quiet_dir / window
        window_dir.mkdir()
        api_guard_before = api_metrics()
        api_guard_at = time.monotonic()
        samples = 0

        def guard() -> None:
            nonlocal api_guard_before, api_guard_at, samples
            if time.monotonic() >= overall_deadline:
                fail("long quiet observation exceeded the shared execution deadline")
            observe_blocked_runtime(
                expected_ids,
                blocker_id,
                blocker_uid,
                pre["pods"],
                window_dir,
                Path(pre["docker_root"]),
            )
            api_guard_before, api_guard_at = guard_api(
                window_dir, api_guard_before, api_guard_at, force=True
            )
            samples += 1

        guard()
        before_api = api_metrics()
        save_json(window_dir / "api-before.json", before_api)
        measured_start = time.monotonic()
        for _ in range(window_seconds // QUIET_SAMPLE_SECONDS):
            pause(QUIET_SAMPLE_SECONDS)
            guard()
        after_api = api_metrics()
        measured_seconds = time.monotonic() - measured_start
        save_json(window_dir / "api-after.json", after_api)
        wait_queue_state(
            expected_ids,
            blocker_id,
            blocker_uid,
            window_dir,
            time.monotonic() + 1,
            expected_uids=expected_uids,
        )
        summary = {
            "window": window,
            "timestamp": now(),
            "planned_seconds": window_seconds,
            "measured_seconds": measured_seconds,
            "api": metric_delta(before_api, after_api, measured_seconds),
            "reconcile": {
                "availability": "unavailable: not collected by this benchmark"
            },
            "resource_peaks": resource_peaks(window_dir),
            "observer": {
                "guard_samples": samples,
                "sample_interval_seconds": QUIET_SAMPLE_SECONDS,
                "api_total_includes_benchmark_probes_and_kubernetes_system_traffic": True,
                "api_total_is_not_kova_attributable": True,
            },
        }
        save_json(window_dir / "summary.json", summary)
        summaries.append(summary)
        note(
            f"1000 quiet {window} PASS: {summary['api']['total_qps']:.1f} API/s; "
            f"reconcile {summary['reconcile']['availability']}"
        )
    result = {
        "status": "passed",
        "seconds": duration,
        "measured_seconds": sum(item["measured_seconds"] for item in summaries),
        "windows": summaries,
        "interpretation": (
            "Three sequential measured windows after the original 30s stage; API counters "
            "include this benchmark's probes and Kubernetes system traffic, and cannot "
            "alone establish Kova-attributed idle QPS or a production SLA."
        ),
    }
    save_json(quiet_dir / "summary.json", result)
    return result


def live(pre: dict, token: str) -> Path:
    global API_READ_PROXY_ENABLED, HTTP_FRONTDOORS, POST_SEQUENCE
    API_READ_PROXY_ENABLED, HTTP_FRONTDOORS, POST_SEQUENCE = False, [], 0
    overall_deadline = live_deadline()
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
            "quiet_seconds": pre["quiet_seconds"],
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
    note(f"RUNNING; receipts at {run_dir}")
    expected_ids: dict[str, str] = {}
    expected_uids: dict[str, str] = {}
    forward = None
    proxy = None
    metrics = []
    success = False
    cleanup_started = False
    admission_attempted = False
    checkpoint = "port-forward"
    handled_signals = (signal.SIGINT, signal.SIGTERM, signal.SIGHUP)
    previous_handlers = {signum: signal.getsignal(signum) for signum in handled_signals}

    def interrupted(signum: int, _frame: object) -> None:
        fail(f"received {signal.Signals(signum).name}; emergency stop required")

    for signum in handled_signals:
        signal.signal(signum, interrupted)
    try:
        with socket.socket() as probe:
            probe.bind(("127.0.0.1", PORT))
        pod_name = next(
            name
            for name in sorted(pre["service_pods"])
            if name != pre["initial_leader"]
        )
        with (run_dir / "port-forward.log").open("w", encoding="utf-8") as log:
            budget_timeout(15)
            forward = subprocess.Popen(
                [
                    "kubectl",
                    "--kubeconfig",
                    str(KUBECONFIG),
                    "-n",
                    CONTROL_NAMESPACE,
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
            budget_timeout(15)
            if forward.poll() is not None:
                fail("Service Pod port-forward exited")
            try:
                if request_json(PORT, "GET", "/readyz")[0] == 200:
                    break
            except BenchError:
                pass
            pause(1)
        else:
            fail("Service Pod did not become ready through the local port-forward")

        if retained_profile():
            # Two original HTTP servers, one serial stream. No retries,
            # concurrent ledger mutation, service selectors or Pod adoption.
            primary = forward
            forward = [primary]
            second_name = pre["initial_leader"]
            second_port = FIXTURE["ports"]["service_second"]
            with socket.socket() as probe:
                probe.bind(("127.0.0.1", second_port))
            with (run_dir / "port-forward-second.log").open("w", encoding="utf-8") as log:
                budget_timeout(15)
                secondary = subprocess.Popen(
                    ["kubectl", "--kubeconfig", str(KUBECONFIG), "-n", CONTROL_NAMESPACE,
                     "port-forward", "--address", "127.0.0.1", f"pod/{second_name}", f"{second_port}:8080"],
                    stdout=log, stderr=subprocess.STDOUT,
                )
            forward.append(secondary)
            for _ in range(30):
                if secondary.poll() is not None:
                    fail("original second HTTP Pod port-forward exited")
                try:
                    if request_json(second_port, "GET", "/readyz")[0] == 200:
                        break
                except BenchError:
                    pass
                pause(1)
            else:
                fail("original second HTTP Pod did not become ready")
            HTTP_FRONTDOORS = [(PORT, primary), (second_port, secondary)]
            pre["original_http_frontdoors"] = {
                pod_name: {"uid": pre["service_pods"][pod_name], "port": PORT},
                second_name: {"uid": pre["service_pods"][second_name], "port": second_port},
            }
            save_json(run_dir / "original-http-frontdoors.json", pre["original_http_frontdoors"])
            proxy = start_api_proxy(run_dir)
            qualify_api_proxy(proxy)

        metrics = metrics_forwards(pre, overall_deadline)
        source_uri = (
            f"oci://unused.invalid/deep-queue/{FIXTURE['run_id']}@{SOURCE_DIGEST}"
        )
        target_prefix = f"unused.invalid/deep-queue/{FIXTURE['run_id']}"
        blocker_key = run_id + "-blocker"
        blocker_id = build_id(blocker_key)
        blocker_dir = run_dir / "blocker"
        blocker_dir.mkdir()
        append_jsonl(
            blocker_dir / "candidates.jsonl", {"key": blocker_key, "id": blocker_id}
        )
        blocker_payload = {
            "source_uri": source_uri,
            "source_digest": SOURCE_DIGEST,
            "targets": [
                {"target": f"{target_prefix}:blocker", "platform": pre["platform"]}
            ],
            "format": "oci",
            "concurrency": 1,
            "idempotency_key": blocker_key,
        }
        checkpoint = "blocker"
        qualify_epoch()
        admission_attempted = True
        status, headers, elapsed, body_fingerprint = request_json(
            admission_port(), "POST", "/v1/builds", token, blocker_payload
        )
        observed_id = headers.get("x-kova-build-id", "")
        append_jsonl(
            blocker_dir / "responses.jsonl",
            {
                "id": blocker_id,
                "status": status,
                "body_fingerprint": body_fingerprint,
                "build_id_header_matches_expected": observed_id == blocker_id,
                "build_id_header_length": len(observed_id),
                "build_id_header_sha256": hashlib.sha256(
                    observed_id.encode()
                ).hexdigest(),
                "elapsed_seconds": elapsed,
            },
        )
        if status != 202 or observed_id != blocker_id:
            fail("blocker POST did not accept its exact expected KovaBuild ID")
        blocker_uid = ""
        for _ in range(60):
            builds = kjson("-n", NAMESPACE, "get", "kovabuilds", "-o", "json")["items"]
            if len(builds) > 1 or (
                builds and builds[0]["metadata"]["name"] != blocker_id
            ):
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
                    if (
                        set(intents) == {blocker_id}
                        and runners
                        and blocker.get("status", {}).get("admissionGenesisWitness")
                    ):
                        qualify_build_receipts(
                            {blocker_id: blocker}, blocker_id, blocker_uid
                        )
                        save_without_token(blocker_dir / "build.json", blocker, token)
                        save_without_token(blocker_dir / "runner.json", pod, token)
                        break
            pause(1)
        else:
            fail(
                "blocker did not acquire exactly one active grant and Pending owned runner"
            )

        previous = 1
        for target in STAGES:
            checkpoint = f"total-charge-{target}"
            if time.monotonic() >= overall_deadline:
                fail("overall benchmark duration exceeded one hour")
            stage_dir = run_dir / f"stage-{target:04d}"
            stage_dir.mkdir()
            note(
                f"stage {target} total charges: adding {target - previous} genuinely queued builds"
            )
            guard_metrics = api_metrics()
            guard_at = time.monotonic()
            service_metrics_before = BURST.metric_snapshot(metrics, overall_deadline)
            save_json(stage_dir / "service-metrics-before.json", service_metrics_before)

            def stage_guard(force_api: bool = False) -> None:
                nonlocal guard_metrics, guard_at
                if time.monotonic() >= overall_deadline:
                    fail("overall benchmark duration exceeded one hour")
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
            for index in range(previous, target):
                if (
                    time.monotonic() >= overall_deadline
                    or time.monotonic() - submission_started > 600
                ):
                    fail(f"stage {target} admission exceeded its bounded deadline")
                key = f"{run_id}-queued-{index:04d}"
                ident = build_id(key)
                append_jsonl(
                    stage_dir / "candidates.jsonl",
                    {"index": index, "key": key, "id": ident},
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
                qualify_epoch()
                status, headers, elapsed, body_fingerprint = request_json(
                    admission_port(), "POST", "/v1/builds", token, payload
                )
                observed_id = headers.get("x-kova-build-id", "")
                append_jsonl(
                    stage_dir / "responses.jsonl",
                    {
                        "index": index,
                        "id": ident,
                        "status": status,
                        "body_fingerprint": body_fingerprint,
                        "build_id_header_matches_expected": observed_id == ident,
                        "build_id_header_length": len(observed_id),
                        "build_id_header_sha256": hashlib.sha256(
                            observed_id.encode()
                        ).hexdigest(),
                        "elapsed_seconds": elapsed,
                    },
                )
                if status != 202 or observed_id != ident:
                    fail(
                        f"stage {target} request {index} returned HTTP {status} or wrong ID"
                    )
                expected_ids[ident] = key
                latencies.append(elapsed)
                if index % 10 == 0 or elapsed > 1:
                    stage_guard()
            submitted_at = time.monotonic()
            builds = wait_queue_state(
                expected_ids,
                blocker_id,
                blocker_uid,
                stage_dir,
                min(submitted_at + SETTLE_DEADLINE_SECONDS, overall_deadline),
                stage_guard,
                expected_uids,
            )
            for item in builds["items"]:
                name = item["metadata"]["name"]
                if name not in expected_ids:
                    continue
                uid = item["metadata"]["uid"]
                if not isinstance(uid, str) or not re.fullmatch(r"[0-9a-f-]{36}", uid):
                    fail("an accepted KovaBuild has no exact UID")
                if name in expected_uids and expected_uids[name] != uid:
                    fail("an accepted KovaBuild UID changed between stages")
                expected_uids[name] = uid
            save_json(stage_dir / "accepted-uids.json", expected_uids)
            settled_at = time.monotonic()
            stage_guard(force_api=True)
            before = api_metrics()
            save_json(stage_dir / "api-before.json", before)
            measured_start = time.monotonic()
            for _ in range(STEADY_SECONDS // POLL_SECONDS):
                pause(POLL_SECONDS)
                stage_guard(force_api=True)
            measured_seconds = time.monotonic() - measured_start
            after = api_metrics()
            save_json(stage_dir / "api-after.json", after)
            api = metric_delta(before, after, measured_seconds)
            service_metrics_after = BURST.metric_snapshot(metrics, overall_deadline)
            save_json(stage_dir / "service-metrics-after.json", service_metrics_after)
            service_deltas = BURST.metric_deltas(
                service_metrics_before, service_metrics_after
            )
            if service_deltas["missing"]:
                fail("required bounded Service client metrics are unavailable")
            reconcile_family = "controller_runtime_reconcile_time_seconds"
            limiter_family = "kova_service_kube_limiter_wait_seconds"
            reconcile = V3.histogram_summary(
                V3.histogram_deltas(
                    service_metrics_before, service_metrics_after, reconcile_family
                ),
                reconcile_family,
            )
            limiter = V3.histogram_summary(
                V3.histogram_deltas(
                    service_metrics_before, service_metrics_after, limiter_family
                ),
                limiter_family,
                {"traffic_class": "http", "outcome": "ok"} if retained_profile() else None,
            )
            if reconcile["samples"] == 0 or limiter["waits_over_10ms"] <= 0:
                fail(
                    "stage did not observe reconcile samples and qualified client limiter waits >10ms"
                )
            wait_queue_state(
                expected_ids,
                blocker_id,
                blocker_uid,
                stage_dir,
                time.monotonic() + 1,
                expected_uids=expected_uids,
            )
            summary = {
                "stage": target,
                "timestamp": now(),
                "accepted_this_stage": target - previous,
                "accepted_total": target,
                "total_charged_non_terminal": target,
                "active": 1,
                "genuinely_queued": target - 1,
                "submission_seconds": submitted_at - submission_started,
                "submit_throughput_per_second": (target - previous)
                / (submitted_at - submission_started),
                "queue_settle_seconds_after_last_submit": settled_at - submitted_at,
                "post_p50_seconds": percentile(latencies, 0.50),
                "post_p95_seconds": percentile(latencies, 0.95),
                "post_p99_seconds": percentile(latencies, 0.99),
                "api": api,
                "resource_peaks": resource_peaks(stage_dir),
                "reconcile_histogram": reconcile,
                "client_limiter_histogram": limiter,
                "client_limiter_selection": {"traffic_class": "http", "outcome": "ok"}
                if retained_profile() else "all_classes_legacy_profile",
                "service_metric_deltas": service_deltas,
                "metric_note": "histogram percentile bucket upper bounds; API totals include observer/system traffic",
            }
            save_json(stage_dir / "summary.json", summary)
            note(
                f"stage {target} PASS: API {api['total_qps']:.1f}/s; "
                f"POST p99 {summary['post_p99_seconds']:.3f}s"
            )
            previous = target

        quiet_result = observe_long_quiet(
            pre,
            run_dir,
            expected_ids,
            expected_uids,
            blocker_id,
            blocker_uid,
            overall_deadline,
        )

        # Cleanup is exact-ID only. Unknown outcomes or any stage failure keep
        # all CRs and ledgers for operator evidence instead of guessing.
        cleanup_dir = run_dir / "cleanup"
        cleanup_dir.mkdir()
        if len(expected_uids) != len(expected_ids):
            fail("not every accepted KovaBuild has a recorded UID for cleanup")
        wait_queue_state(
            expected_ids,
            blocker_id,
            blocker_uid,
            cleanup_dir,
            time.monotonic() + 1,
            expected_uids=expected_uids,
        )
        if time.monotonic() >= overall_deadline - 120:
            fail("insufficient shared preflight/live budget remains for exact cleanup")
        if (
            hashlib.sha256(KUBECONFIG.read_bytes()).hexdigest()
            != pre["kubeconfig_sha256"]
        ):
            fail("dedicated kubeconfig bytes changed before API proxy launch")
        cleanup_deadline = min(
            time.monotonic() + CLEANUP_DEADLINE_SECONDS, overall_deadline
        )
        if proxy is None:
            proxy = start_api_proxy(run_dir)
            qualify_api_proxy(proxy)
        cleanup_started = True
        checkpoint = "full-capacity-recovery"
        recovery_result = full_capacity_recovery(
            pre,
            run_dir,
            expected_ids,
            expected_uids,
            blocker_id,
            blocker_uid,
            token,
            cleanup_deadline,
        )
        close_local_processes(metrics, None, None, overall_deadline)
        metrics = []
        checkpoint = "leader-handoff"
        handoff_result = leader_handoff(
            pre,
            run_dir,
            expected_ids,
            expected_uids,
            blocker_id,
            blocker_uid,
            cleanup_deadline,
        )
        if handoff_result["deleted_leader"] == pod_name:
            fail(
                "planned leader changed onto the HTTP-forward survivor; stop without another POST"
            )
        if retained_profile():
            checkpoint = "retained-original-uid-promotion-and-planned-stop"
            retained = retained_finish(
                pre, run_dir, expected_ids, expected_uids, blocker_id, blocker_uid,
                overall_deadline,
            )
            checkpoint = "local-child-shutdown"
            close_local_processes(metrics, forward, proxy, overall_deadline)
            metrics, forward, proxy = [], None, None
            budget_timeout(RUN_DEADLINE_SECONDS)
            save_json(run_dir / "result.json", {
                "status": "passed", "profile": FIXTURE["profile"],
                "scope": "supported_capacity_api_admission_retained_success",
                "completed_at": now(), "stages": STAGES, "quiet_seconds": 0,
                "operator_kube_client_qps_per_pod": 100, "operator_kube_client_burst_per_pod": 200,
                "original_http_frontdoors": pre["original_http_frontdoors"],
                "post_attempts_serial": POST_SEQUENCE,
                "full_capacity_recovery": recovery_result, "leader_handoff": handoff_result,
                "planned_original_leader_uid_delete": pre.get("leader_handoff_effect"),
                "retained_success": retained, "exact_bulk_cr_cleanup": False,
                "unmeasured": ["default 20/40 HTTP performance", "API-server 429 fault injection",
                               "all-Service simultaneous restart", "long quiet/soak", "bulk retirement and idle baseline"],
                "cluster_deleted": False, "registry_touched": False,
            })
            success = True
            return run_dir
        cleanup_metrics = api_metrics()
        cleanup_at = time.monotonic()
        checkpoint = "queued-cleanup"
        challenger_id = next(iter(expected_ids))
        challenger_uid = expected_uids[challenger_id]
        cleanup_ids = [name for name in expected_ids if name != challenger_id]
        for start in range(0, len(cleanup_ids), 20):
            names = cleanup_ids[start : start + 20]
            if time.monotonic() >= cleanup_deadline:
                fail("exact cleanup exceeded its total bounded deadline")
            if exact_kind_identity() != pre["kubeconfig_fingerprint"]:
                fail("dedicated Kind identity changed before a cleanup batch")
            current_builds = kjson("-n", NAMESPACE, "get", "kovabuilds", "-o", "json")[
                "items"
            ]
            current_uids = {
                item["metadata"]["name"]: item["metadata"]["uid"]
                for item in current_builds
            }
            if set(current_uids) - (set(expected_ids) | {blocker_id}):
                fail("an unrecorded KovaBuild appeared during exact cleanup")
            if any(current_uids.get(name) != expected_uids[name] for name in names):
                fail("a queued KovaBuild UID changed before UID-precondition deletion")
            append_jsonl(
                cleanup_dir / "delete-batches.jsonl",
                {"ids": names, "started_at": now()},
            )
            for name in names:
                if time.monotonic() >= cleanup_deadline:
                    fail("exact cleanup exceeded its total bounded deadline")
                receipt = delete_build_with_uid(
                    name, expected_uids[name], pre["kubeconfig_sha256"]
                )
                append_jsonl(
                    cleanup_dir / "delete-responses.jsonl",
                    receipt,
                )
                if not receipt["accepted"]:
                    fail(
                        f"UID-precondition DELETE for {name} returned HTTP "
                        f"{receipt['http_status']}; outcome needs review"
                    )
            observe_blocked_runtime(
                expected_ids,
                blocker_id,
                blocker_uid,
                pre["pods"],
                cleanup_dir,
                Path(pre["docker_root"]),
            )
            cleanup_metrics, cleanup_at = guard_api(
                cleanup_dir, cleanup_metrics, cleanup_at
            )
        while time.monotonic() < cleanup_deadline:
            builds = kjson("-n", NAMESPACE, "get", "kovabuilds", "-o", "json")["items"]
            current_uids = {
                item["metadata"]["name"]: item["metadata"]["uid"] for item in builds
            }
            names = set(current_uids)
            if names - (set(expected_ids) | {blocker_id}):
                fail("an unrecorded KovaBuild appeared during exact cleanup")
            if any(
                current_uids[name] != expected_uids[name]
                for name in names
                if name in expected_uids
            ):
                fail("a queued KovaBuild UID changed during exact cleanup")
            _, intents = queue_ledgers(len(expected_ids), blocker_id, blocker_uid)
            check_runner(blocker_id, blocker_uid)
            if names == {blocker_id, challenger_id} and set(intents) == names:
                break
            observe_blocked_runtime(
                expected_ids,
                blocker_id,
                blocker_uid,
                pre["pods"],
                cleanup_dir,
                Path(pre["docker_root"]),
            )
            cleanup_metrics, cleanup_at = guard_api(
                cleanup_dir, cleanup_metrics, cleanup_at
            )
            pause(3)
        else:
            fail("queued CR cleanup did not drain within its bounded deadline")
        remaining = wait_queue_state(
            {challenger_id: expected_ids[challenger_id]},
            blocker_id,
            blocker_uid,
            cleanup_dir,
            min(cleanup_deadline, time.monotonic() + 5),
            expected_uids={challenger_id: challenger_uid},
        )
        by_name = {item["metadata"]["name"]: item for item in remaining["items"]}
        original_receipts = qualify_build_receipts(by_name, blocker_id, blocker_uid)
        _, remaining_intents = queue_ledgers(1, blocker_id, blocker_uid)
        challenger_receipt = (
            "kova-admission-intent-" + remaining_intents[challenger_id]["nonce"]
        )
        blocker_receipts = {
            name: obj
            for name, obj in original_receipts.items()
            if name != challenger_receipt
        }
        original_runner = check_runner(blocker_id, blocker_uid)
        blocker = by_name[blocker_id]
        if blocker["metadata"]["uid"] != blocker_uid:
            fail("blocker UID changed before exact deletion")
        if time.monotonic() >= cleanup_deadline:
            fail("exact cleanup exceeded its total bounded deadline")
        if exact_kind_identity() != pre["kubeconfig_fingerprint"]:
            fail("dedicated Kind identity changed before blocker deletion")
        append_jsonl(
            cleanup_dir / "delete-batches.jsonl",
            {"ids": [blocker_id], "started_at": now()},
        )
        receipt = delete_build_with_uid(
            blocker_id, blocker_uid, pre["kubeconfig_sha256"]
        )
        append_jsonl(cleanup_dir / "delete-responses.jsonl", receipt)
        if not receipt["accepted"]:
            fail(
                f"UID-precondition DELETE for blocker returned HTTP "
                f"{receipt['http_status']}; outcome needs review"
            )
        checkpoint = "original-challenger-promotion"
        promotion_started = time.monotonic()
        promotion_deadline = min(cleanup_deadline, promotion_started + 120)
        while time.monotonic() < promotion_deadline:
            builds = kjson("-n", NAMESPACE, "get", "kovabuilds", "-o", "json")["items"]
            if any(
                item["metadata"]["name"] not in {blocker_id, challenger_id}
                for item in builds
            ):
                fail("foreign build appeared during exact capacity promotion")
            current = next(
                (item for item in builds if item["metadata"]["name"] == challenger_id),
                None,
            )
            if current is None or current["metadata"]["uid"] != challenger_uid:
                fail("original challenger disappeared or changed UID")
            if (
                len(builds) == 1
                and current.get("status", {}).get("phase") == "Starting"
                and current.get("status", {}).get("admissionGenesisWitness")
            ):
                qualify_build_receipts(
                    {challenger_id: current}, challenger_id, challenger_uid
                )
                if (
                    request_json(API_PROXY_PORT, "GET", build_api_path(blocker_id))[0]
                    != 404
                ):
                    fail(
                        "original blocker was not directly confirmed absent after promotion"
                    )
                old_pod_path = f"/api/v1/namespaces/{NAMESPACE}/pods/{original_runner['metadata']['name']}"
                if request_json(API_PROXY_PORT, "GET", old_pod_path)[0] != 404:
                    fail("original blocker runner was not directly confirmed absent")
                normal_receipt_absence(blocker_receipts, promotion_deadline)
                break
            resource_sample(pre["pods"], cleanup_dir, Path(pre["docker_root"]))
            cleanup_metrics, cleanup_at = guard_api(
                cleanup_dir, cleanup_metrics, cleanup_at
            )
            pause(1)
        else:
            fail(
                "original challenger did not acquire the released active slot within 120 seconds"
            )
        promotion = {
            "status": "passed",
            "original_challenger_id": challenger_id,
            "original_challenger_uid": challenger_uid,
            "released_blocker_uid": blocker_uid,
            "seconds": time.monotonic() - promotion_started,
        }
        save_json(cleanup_dir / "promotion.json", promotion)
        final_receipts = qualify_build_receipts(
            {challenger_id: current}, challenger_id, challenger_uid
        )
        checkpoint = "final-original-uid-cleanup"
        receipt = delete_build_with_uid(
            challenger_id, challenger_uid, pre["kubeconfig_sha256"]
        )
        append_jsonl(cleanup_dir / "delete-responses.jsonl", receipt)
        if not receipt["accepted"]:
            fail("final challenger UID deletion outcome is unknown")
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
                empty_receipts()
                normal_receipt_absence(final_receipts, cleanup_deadline)
                break
            resource_sample(pre["pods"], cleanup_dir, Path(pre["docker_root"]))
            cleanup_metrics, cleanup_at = guard_api(
                cleanup_dir, cleanup_metrics, cleanup_at
            )
            pause(3)
        else:
            fail(
                "blocker Pod/ledger cleanup did not finish within its bounded deadline"
            )
        checkpoint = "local-child-shutdown"
        close_local_processes(metrics, forward, proxy, overall_deadline)
        metrics, forward, proxy = [], None, None
        budget_timeout(RUN_DEADLINE_SECONDS)
        save_json(
            run_dir / "result.json",
            {
                "status": "passed",
                "profile": FIXTURE["profile"],
                "scope": "representative_short_api_admission"
                if len(STAGES) == 1
                else "supported_capacity_api_admission",
                "unmeasured": ["500/1000 outstanding HTTP throughput"]
                if len(STAGES) == 1
                else [],
                "completed_at": now(),
                "stages": STAGES,
                "long_quiet": {
                    "status": quiet_result["status"],
                    "seconds": quiet_result["seconds"],
                    "measured_seconds": quiet_result.get("measured_seconds"),
                },
                "exact_cr_cleanup": True,
                "full_capacity_recovery": recovery_result,
                "leader_handoff": handoff_result,
                "original_uid_capacity_promotion": promotion,
                "emergency_stop": "not-needed",
                "planned_original_leader_uid_delete": pre.get("leader_handoff_effect"),
                "cluster_deleted": False,
                "registry_touched": False,
            },
        )
        success = True
        return run_dir
    except BaseException as error:
        for signum in handled_signals:
            signal.signal(signum, signal.SIG_IGN)
        error_text = safe_error(error, token)
        base_result = {
            "status": "unknown",
            "checkpoint": checkpoint,
            "completed_at": now(),
            "error": error_text,
            "known_accepted_ids": list(expected_ids),
            "known_accepted_uids": expected_uids,
            "unknown_outcome_candidates": "see blocker and stage candidate receipts",
            "cleanup_started": cleanup_started,
            "cleanup_may_be_partial": cleanup_started,
            "http_admission_attempted": admission_attempted,
            "cluster_deleted": False,
            "registry_touched": False,
            "emergency_stop": "not-attempted-retained",
            "planned_original_leader_uid_delete": pre.get("leader_handoff_effect"),
            "planned_service_stop_effect": pre.get("retained_stop_effect"),
            "retained_promotion_effect": pre.get("retained_promotion_effect"),
        }
        try:
            save_json(run_dir / "result.json", base_result)
        except Exception:
            pass
        # Unknown means stop this driver and retain the dedicated fixture.
        # Never infer new cleanup authority or automatically scale a workload.
        stop = {
            "status": "not-attempted-retained",
            "automatic_scale_or_delete_attempted": False,
        }
        try:
            failure_snapshots = capture_failure_objects(
                run_dir, token, set(expected_ids) | {build_id(run_id + "-blocker")}
            )
        except Exception as snapshot_error:
            failure_snapshots = {"error": safe_error(snapshot_error, token)}
        base_result.update(
            {
                "emergency_stop": stop["status"],
                "emergency_stop_detail": stop,
                "failure_snapshots": failure_snapshots,
            }
        )
        note(
            "UNKNOWN: no automatic cleanup/scale; dedicated fixture retained for owner review"
        )
        save_json(run_dir / "result.json", base_result)
        raise BenchError(error_text) from None
    finally:
        for signum, previous_handler in previous_handlers.items():
            signal.signal(signum, previous_handler)
        close_local_processes(metrics, forward, proxy)
        API_READ_PROXY_ENABLED, HTTP_FRONTDOORS = False, []
        if success:
            note(f"PASS; receipts preserved at {run_dir}")
        else:
            note(
                f"FAILED; receipts and test-owned CRs retained at {run_dir}; inspect first"
            )


def main() -> None:
    mode = os.environ.get("DEEP_QUEUE_E2E_MODE", "plan")
    if mode not in ("plan", "check", "run"):
        fail(
            "DEEP_QUEUE_E2E_MODE must be plan, check, or run; legacy stop/status are not v3 authority"
        )
    if mode == "plan" and not os.environ.get("DEEP_QUEUE_E2E_FIXTURE"):
        print(
            json.dumps(
                {
                    "classification": "unprepared_v3_plan",
                    "live_ready": False,
                    "required_input": "DEEP_QUEUE_E2E_FIXTURE",
                    "external_commands": 0,
                    "default_profile": "representative-short",
                    "total_charged": 100,
                    "active": 1,
                    "genuinely_queued": 99,
                    "budget_seconds": 900,
                },
                sort_keys=True,
            )
        )
        return
    configure_fixture()
    if mode == "plan":
        print(
            json.dumps(
                {
                    "classification": "local_contract_only",
                    "live_ready": False,
                    "fixture_sha256": FIXTURE_SHA,
                    "profile": FIXTURE["profile"],
                    "stages_total_charged": STAGES,
                    "active": 1,
                    "genuinely_queued": [stage - 1 for stage in STAGES],
                    "budget_seconds": RUN_DEADLINE_SECONDS,
                    "external_commands": 0,
                    "api_writes": 0,
                },
                sort_keys=True,
            )
        )
        return
    if (
        mode == "run"
        and os.environ.get("DEEP_QUEUE_E2E_ACK") != f"{FIXTURE['run_id']}:{FIXTURE_SHA}"
    ):
        fail(
            f"{mode} mode requires DEEP_QUEUE_E2E_ACK={FIXTURE['run_id']}:{FIXTURE_SHA}"
        )
    if mode == "check":
        start_execution_budget()
        preflight()
        note(
            f"read-only preflight passed: {CLUSTER}; 2/2 nodes; two Service Pods; empty ledgers"
        )
        note("no writes performed; explicit run mode needs DEEP_QUEUE_E2E_ACK")
        return
    if sys.platform != "linux":
        fail("run mode requires an isolated Linux Kind host")
    start_execution_budget()
    if not HOST_LOCK.parent.is_dir():
        fail("managed host lock directory is unavailable")
    lock_fd = os.open(HOST_LOCK, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
    try:
        os.fchmod(lock_fd, 0o600)
        try:
            fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            fail("another deep-queue run holds the host lock")
        pre = preflight()
        note(
            f"locked preflight passed: {CLUSTER}; 2/2 nodes; two Service Pods; empty ledgers"
        )
        token = load_static_token()
        live(pre, token)
    finally:
        os.close(lock_fd)


if __name__ == "__main__":
    try:
        main()
    except (
        BenchError,
        V3.QualificationError,
        BURST.UnknownOutcome,
        BURST.ContractError,
        OSError,
        ValueError,
        KeyboardInterrupt,
    ) as error:
        note(f"error: {safe_error(error, '')}")
        sys.exit(1)
