#!/usr/bin/env python3
"""Real-runner #41 partial-output acceptance on an existing disposable Kind.

Default check mode is read-only. Run mode publishes only unique test tags and
submits one two-target build. It never creates/deletes Kind or registry content.
"""

from __future__ import annotations

import base64
import hashlib
import json
import os
import re
import secrets
import shutil
import signal
import socket
import subprocess
import sys
import threading
import time
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import ProxyHandler, Request, build_opener

from oci_platform_identity import ImageIdentityError, local_platform_image_fact

ROOT = Path(__file__).resolve().parents[2]
CLUSTER = "kova-partial-output-41"
NAMESPACE = "kova"
RELEASE = "kova"
KUBECONFIG = ROOT / ".kind" / f"{CLUSTER}.kubeconfig"
REGISTRY = "kind-registry"
REGISTRY_HOST = "127.0.0.1:5002"
REGISTRY_CLUSTER = "kind-registry:5000"
REGISTRY_IMAGE = (
    "registry:2@sha256:a3d8aaa63ed8681a604f1dea0aa03f100d5895b6a58ace528858a7b332415373"
)
IMAGE_REPOSITORY = "localhost:5002/kova"
RUN_DEADLINE_SECONDS = 20 * 60
MAX_RUNNER_EXPORT_BYTES = 2 * 1024 * 1024 + 4096
MIN_MEMORY_KIB = 8 * 1024 * 1024
MIN_DISK_KIB = 20 * 1024 * 1024
MIN_NODE_MEMORY_BYTES = 2 * 1024**3
HTTP = build_opener(ProxyHandler({}))
PRIVATE_TOKEN = ""
MANIFEST_ACCEPT = ", ".join(
    (
        "application/vnd.oci.image.manifest.v1+json",
        "application/vnd.oci.image.index.v1+json",
        "application/vnd.docker.distribution.manifest.v2+json",
        "application/vnd.docker.distribution.manifest.list.v2+json",
    )
)


class AcceptanceError(RuntimeError):
    pass


def fail(message: str) -> None:
    raise AcceptanceError(message)


def note(message: str) -> None:
    print(f"partial-output-e2e: {message}", file=sys.stderr, flush=True)


def now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")  # noqa: UP017 (Python 3.10)


def command(
    argv: list[str],
    *,
    input_text: str | None = None,
    timeout: int = 30,
    env: dict[str, str] | None = None,
) -> str:
    try:
        result = subprocess.run(
            argv,
            input=input_text,
            text=True,
            capture_output=True,
            timeout=timeout,
            env=env,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired) as error:
        fail(f"{argv[0]} did not complete: {type(error).__name__}")
    if result.returncode != 0:
        message = result.stderr[-400:].strip()
        for token in (PRIVATE_TOKEN, (env or {}).get("KOVA_SERVICE_TOKEN", "")):
            if token:
                message = message.replace(token, "[REDACTED]")
        fail(f"{argv[0]} returned {result.returncode}: {message}")
    return result.stdout


def kctl(*args: str, timeout: int = 30) -> str:
    return command(
        ["kubectl", "--kubeconfig", str(KUBECONFIG), "--request-timeout=15s", *args],
        timeout=timeout,
    )


def kjson(*args: str) -> dict:
    value = json.loads(kctl(*args))
    if not isinstance(value, dict):
        fail("Kubernetes returned non-object JSON")
    return value


def save_json(path: Path, value: object) -> None:
    reject_reflected_token(value)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def append_jsonl(path: Path, value: object) -> None:
    reject_reflected_token(value)
    with path.open("a", encoding="utf-8") as output:
        output.write(json.dumps(value, sort_keys=True) + "\n")


def reject_reflected_token(value: object) -> None:
    if not PRIVATE_TOKEN:
        return
    if isinstance(value, str):
        if PRIVATE_TOKEN in value:
            fail("a receipt reflected the private Service token; write blocked")
    elif isinstance(value, dict):
        for key, item in value.items():
            reject_reflected_token(key)
            reject_reflected_token(item)
    elif isinstance(value, (list, tuple)):
        for item in value:
            reject_reflected_token(item)


def save_text(path: Path, value: str) -> None:
    reject_reflected_token(value)
    path.write_text(value, encoding="utf-8")


def sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def require_tools(mode: str) -> None:
    if sys.platform != "linux":
        fail("this acceptance runs only on a dedicated Linux Kind host")
    if socket.gethostname() != "wayne-hk-kvm":
        fail("this acceptance is pinned to wayne-hk-kvm")
    for tool in ("docker", "kind", "kubectl", "helm", "timeout"):
        if shutil.which(tool) is None:
            fail(f"required tool is absent: {tool}")
    if not os.access(ROOT / "bin/kova", os.X_OK):
        fail("build the current Linux CLI with make kova before preflight")


def check_checkout_revision(revision: str) -> None:
    if not re.fullmatch(r"[0-9a-f]{12}", revision):
        fail("PARTIAL_OUTPUT_EXPECTED_REVISION must be the candidate's exact 12-hex image revision")
    if command(["git", "-C", str(ROOT), "status", "--porcelain"]).strip():
        fail("Kova checkout is dirty; build and validate a committed candidate")
    head = command(["git", "-C", str(ROOT), "rev-parse", "--short=12", "HEAD"]).strip()
    if head != revision:
        fail("expected revision does not match the clean Kova checkout HEAD")
    version = command([str(ROOT / "bin/kova"), "version"]).strip()
    if not re.search(rf"\(commit {revision},", version):
        fail("Linux CLI binary revision does not match the candidate checkout")


def kubeconfig_identity(config_text: str) -> str:
    data = json.loads(
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
            input_text=config_text,
        )
    )
    focused = {
        "context": data["current-context"],
        "cluster": data["clusters"][0]["cluster"],
        "user": data["users"][0]["user"],
    }
    return sha256(json.dumps(focused, sort_keys=True, separators=(",", ":")).encode())


def check_kind_identity() -> str:
    if not KUBECONFIG.is_file() or KUBECONFIG.is_symlink():
        fail(f"missing regular dedicated kubeconfig: {KUBECONFIG}")
    if (
        os.environ.get("KIND_CLUSTER", CLUSTER) != CLUSTER
        or os.environ.get("KIND_KUBECONFIG", f".kind/{CLUSTER}.kubeconfig")
        != f".kind/{CLUSTER}.kubeconfig"
    ):
        fail("Kind cluster/kubeconfig overrides do not match the dedicated fixture")
    if command(["kind", "get", "clusters"]).splitlines() != [CLUSTER]:
        fail(f"the host must contain exactly one Kind cluster named {CLUSTER}")
    actual = KUBECONFIG.read_text(encoding="utf-8")
    expected = command(["kind", "get", "kubeconfig", "--name", CLUSTER])
    actual_id, expected_id = kubeconfig_identity(actual), kubeconfig_identity(expected)
    if actual_id != expected_id:
        fail("dedicated kubeconfig differs from the live Kind credentials/server")
    if (
        command(["kubectl", "--kubeconfig", str(KUBECONFIG), "config", "current-context"]).strip()
        != f"kind-{CLUSTER}"
    ):
        fail("dedicated kubeconfig has an unexpected context")
    return sha256(KUBECONFIG.read_bytes())


def check_nodes() -> list[dict]:
    nodes = kjson("get", "nodes", "-o", "json").get("items", [])
    if len(nodes) != 2:
        fail("the dedicated Kind must have exactly two nodes")
    for node in nodes:
        conditions = {item["type"]: item["status"] for item in node["status"].get("conditions", [])}
        if conditions.get("Ready") != "True" or any(
            conditions.get(kind) != "False"
            for kind in ("MemoryPressure", "DiskPressure", "PIDPressure")
        ):
            fail(f"Kind node {node['metadata']['name']} is not Ready without pressure")
        if int(node["status"]["allocatable"]["pods"]) < 20:
            fail("Kind node Pod capacity is below 20")
        name = node["metadata"]["name"]
        summary = kjson("get", "--raw", f"/api/v1/nodes/{name}/proxy/stats/summary")
        memory = summary.get("node", {}).get("memory", {}).get("availableBytes")
        cpu = summary.get("node", {}).get("cpu", {}).get("usageNanoCores")
        allocatable_cpu = node["status"]["allocatable"].get("cpu", "")
        if not isinstance(memory, int) or memory < MIN_NODE_MEMORY_BYTES:
            fail(f"Kind node {name} has less than 2 GiB available memory")
        if not isinstance(cpu, int) or not isinstance(allocatable_cpu, str):
            fail(f"Kind node {name} has incomplete CPU metrics")
        capacity_nano = (
            int(allocatable_cpu[:-1]) * 1_000_000
            if allocatable_cpu.endswith("m")
            else int(allocatable_cpu) * 1_000_000_000
        )
        if capacity_nano <= 0 or cpu > capacity_nano * 0.9:
            fail(f"Kind node {name} CPU use is over 90% allocatable")
    return nodes


def check_pod_capacity(nodes: list[dict]) -> dict[str, int]:
    pods = kjson("get", "pods", "-A", "-o", "json").get("items", [])
    used: dict[str, int] = {node["metadata"]["name"]: 0 for node in nodes}
    for pod in pods:
        node_name = pod.get("spec", {}).get("nodeName", "")
        if node_name in used and pod.get("status", {}).get("phase") not in ("Succeeded", "Failed"):
            used[node_name] += 1
    free = {
        node["metadata"]["name"]: int(node["status"]["allocatable"]["pods"])
        - used[node["metadata"]["name"]]
        for node in nodes
    }
    if any(value < 8 for value in free.values()):
        fail("Kind node Pod headroom is below eight free slots")
    return free


def assert_empty() -> None:
    if kjson("get", "kovabuilds", "-A", "-o", "json").get("items"):
        fail("another KovaBuild exists; do not overlap tests")
    if kjson("get", "pods", "-A", "-l", "app.kubernetes.io/name=kova-runner", "-o", "json").get(
        "items"
    ):
        fail("a runner Pod exists; do not overlap tests")
    active = kjson("-n", NAMESPACE, "get", "configmap", "kova-service-admission", "-o", "json")
    queue = kjson("-n", NAMESPACE, "get", "configmap", "kova-service-queue-admission", "-o", "json")
    if json.loads(active["data"]["reservations.json"]).get("active") != {}:
        fail("active admission ledger is not empty")
    if json.loads(queue["data"]["queue.json"]).get("intents") != {}:
        fail("queue admission ledger is not empty")


def exact_images(revision: str) -> dict[str, str]:
    if not re.fullmatch(r"[0-9a-f]{12}", revision):
        fail("PARTIAL_OUTPUT_EXPECTED_REVISION must be the candidate's exact 12-hex image revision")
    return {
        role: f"{IMAGE_REPOSITORY}:{role}-{revision}" for role in ("controller", "runner", "worker")
    }


def local_config_id(image: str, revision: str) -> str:
    try:
        return local_platform_image_fact(image, revision, command)["config_digest"]
    except ImageIdentityError as error:
        fail(str(error))


def check_deployment(name: str, image: str, container: str) -> dict:
    deployment = kjson("-n", NAMESPACE, "get", "deployment", name, "-o", "json")
    replicas = deployment["spec"].get("replicas", 1)
    if (
        replicas < 1
        or any(
            deployment["status"].get(field) != replicas
            for field in ("readyReplicas", "updatedReplicas")
        )
        or deployment["status"].get("observedGeneration") != deployment["metadata"]["generation"]
    ):
        fail(f"deployment {name} is not fully Ready")
    selected = [
        item
        for item in deployment["spec"]["template"]["spec"]["containers"]
        if item["name"] == container
    ]
    if len(selected) != 1 or selected[0]["image"] != image:
        fail(f"deployment {name} does not run the exact candidate image {image}")
    return selected[0]


def runtime_image_fact(
    pod: dict, container: str, expected_image: str, expected_config_id: str
) -> dict:
    name = pod.get("metadata", {}).get("name", "unknown")
    node = pod.get("spec", {}).get("nodeName", "")
    if node not in (f"{CLUSTER}-control-plane", f"{CLUSTER}-worker"):
        fail(f"Pod {name} is not scheduled on a verified Kind node")
    specs = [item for item in pod["spec"].get("containers", []) if item.get("name") == container]
    statuses = [
        item for item in pod["status"].get("containerStatuses", []) if item.get("name") == container
    ]
    if (
        len(specs) != 1
        or specs[0].get("image") != expected_image
        or len(statuses) != 1
        or statuses[0].get("image") != expected_image
        or not statuses[0].get("imageID")
    ):
        fail(f"Pod {name} has no exact {container} image identity")
    inspection = json.loads(
        command(
            ["docker", "exec", node, "crictl", "inspecti", "-o", "json", expected_image],
            timeout=30,
        )
    )
    cri = inspection.get("status", {})
    repo_tags = cri.get("repoTags") or []
    repo_digests = cri.get("repoDigests") or []
    image_id = statuses[0]["imageID"].removeprefix("docker-pullable://")
    if (
        repo_tags != [expected_image]
        or cri.get("id") != expected_config_id
        or not isinstance(repo_digests, list)
        or any(
            not isinstance(digest, str) or not re.fullmatch(r"[^@]+@sha256:[0-9a-f]{64}", digest)
            for digest in repo_digests
        )
        or (image_id != expected_config_id and image_id not in repo_digests)
    ):
        fail(f"Pod {name} imageID is not pinned to the reviewed CRI config/repoDigests")
    return {
        "pod": name,
        "uid": pod["metadata"]["uid"],
        "node": node,
        "image": expected_image,
        "image_id": image_id,
        "cri_id": cri["id"],
        "local_config_id": expected_config_id,
        "cri_repo_digests": repo_digests,
    }


def ready_role_pods(
    selector: str, expected_count: int, container: str, image: str, config_id: str
) -> list[dict]:
    pods = kjson("-n", NAMESPACE, "get", "pods", "-l", selector, "-o", "json").get("items", [])
    if len(pods) != expected_count:
        fail(f"role selector {selector} did not return exactly {expected_count} Pods")
    facts = []
    for pod in pods:
        if pod["metadata"].get("deletionTimestamp") or not any(
            item.get("type") == "Ready" and item.get("status") == "True"
            for item in pod["status"].get("conditions", [])
        ):
            fail(f"role Pod {pod['metadata']['name']} is not uniquely Ready")
        facts.append(runtime_image_fact(pod, container, image, config_id))
    return sorted(facts, key=lambda item: item["pod"])


def read_static_token(service: dict) -> str:
    check_static_token_ref(service)
    secret = kjson("-n", NAMESPACE, "get", "secret", "kova-e2e-token", "-o", "json")
    if secret["metadata"]["name"] != "kova-e2e-token" or secret.get("type") != "Opaque":
        fail("test Service token Secret identity differs")
    encoded = secret.get("data", {}).get("token", "")
    try:
        token = base64.b64decode(encoded, validate=True).decode("utf-8")
    except (ValueError, UnicodeError) as error:
        fail(f"test Service token Secret is unreadable: {type(error).__name__}")
    if not token or "\n" in token or "\r" in token:
        fail("test Service token must be nonempty and single-line")
    inherited = os.environ.pop("SERVICE_AUTH_TOKEN", "")
    if inherited and inherited != token:
        fail("inherited Service token differs from the exact test-owned Secret")
    return token


def check_static_token_ref(service: dict) -> None:
    env = service.get("env", [])
    selected = [item for item in env if item.get("name") == "KOVA_SERVICE_AUTH_TOKEN"]
    expected_ref = {"name": "kova-e2e-token", "key": "token"}
    if len(selected) != 1 or selected[0].get("valueFrom", {}).get("secretKeyRef") != expected_ref:
        fail("Service token is not from the exact test-owned Secret key")


def check_registry() -> str:
    registry = json.loads(command(["docker", "inspect", REGISTRY, "--format", "{{json .}}"]))
    binding = registry["NetworkSettings"]["Ports"].get("5000/tcp", [])
    if (
        registry["Config"]["Image"] != REGISTRY_IMAGE
        or registry["State"]["Running"] is not True
        or "kind" not in registry["NetworkSettings"]["Networks"]
        or binding != [{"HostIp": "127.0.0.1", "HostPort": "5002"}]
    ):
        fail("local registry image, Kind network, or loopback port binding differs")
    code, _ = registry_request("GET", "/v2/")
    if code != 200:
        fail(f"local registry API is not healthy (HTTP {code})")
    return registry["Id"]


def registry_request(method: str, path: str) -> tuple[int, dict[str, str]]:
    request = Request(
        f"http://{REGISTRY_HOST}{path}", method=method, headers={"Accept": MANIFEST_ACCEPT}
    )
    try:
        with HTTP.open(request, timeout=10) as response:
            return response.status, dict(response.headers.items())
    except HTTPError as error:
        return error.code, dict(error.headers.items())
    except (OSError, URLError) as error:
        fail(f"registry {method} {path} outcome unknown: {type(error).__name__}")


def manifest_digest(repository: str, tag: str, *, absent: bool = False) -> str:
    if not re.fullmatch(r"[a-z0-9/-]+", repository) or not re.fullmatch(r"[a-z0-9_-]+", tag):
        fail("unsafe registry repository/tag")
    code, headers = registry_request("HEAD", f"/v2/{repository}/manifests/{tag}")
    if absent:
        if code != 404:
            fail(f"{repository}:{tag} is not confirmed absent (HTTP {code})")
        return ""
    digest = next(
        (value for key, value in headers.items() if key.lower() == "docker-content-digest"), ""
    )
    if code != 200 or not re.fullmatch(r"sha256:[0-9a-f]{64}", digest):
        fail(f"{repository}:{tag} has no exact SHA-256 manifest receipt (HTTP {code})")
    return digest


def preflight(revision: str) -> dict:
    check_checkout_revision(revision)
    if (
        os.environ.get("NAMESPACE", NAMESPACE) != NAMESPACE
        or os.environ.get("RELEASE_NAME", RELEASE) != RELEASE
    ):
        fail("namespace/release overrides do not match the isolated kova/kova fixture")
    kube_sha = check_kind_identity()
    nodes = check_nodes()
    pod_headroom = check_pod_capacity(nodes)
    if (
        json.loads(
            command(
                [
                    "helm",
                    "status",
                    RELEASE,
                    "--kubeconfig",
                    str(KUBECONFIG),
                    "-n",
                    NAMESPACE,
                    "-o",
                    "json",
                ]
            )
        )["info"]["status"]
        != "deployed"
    ):
        fail("Kova Helm release is not deployed")
    images = exact_images(revision)
    service = check_deployment("kova-service", images["controller"], "kova-service")
    check_deployment("kova", images["worker"], "buildkitd")
    check_static_token_ref(service)
    args = service.get("args", [])
    required = (
        f"--namespace={NAMESPACE}",
        f"--runner-image={images['runner']}",
        f"--registry-plain-http={REGISTRY_CLUSTER}",
        "--auth-mode=static",
        "--auth-static-principal=kova:e2e",
        "--verification-window=5m",
    )
    if not all(value in args for value in required):
        fail("Service args are not the exact isolated partial-output fixture")
    if "--job-ttl=2h" not in args or "--max-build-duration=2h" not in args:
        fail("Service retention/build deadline differs from the bounded fixture")
    local_config_ids = {role: local_config_id(image, revision) for role, image in images.items()}
    service_replicas = kjson("-n", NAMESPACE, "get", "deployment", "kova-service", "-o", "json")[
        "spec"
    ]["replicas"]
    worker_replicas = kjson("-n", NAMESPACE, "get", "deployment", "kova", "-o", "json")["spec"][
        "replicas"
    ]
    runtime = {
        "service": ready_role_pods(
            "app.kubernetes.io/instance=kova,app.kubernetes.io/component=service",
            service_replicas,
            "kova-service",
            images["controller"],
            local_config_ids["controller"],
        ),
        "worker": ready_role_pods(
            "app.kubernetes.io/instance=kova,app.kubernetes.io/name=kova",
            worker_replicas,
            "buildkitd",
            images["worker"],
            local_config_ids["worker"],
        ),
    }
    crd = kjson("get", "crd", "kovabuilds.kova.cofy.dev", "-o", "json")
    versions = [
        item for item in crd["spec"]["versions"] if item["name"] == "v1alpha1" and item["served"]
    ]
    phase_enum = (
        versions[0]["schema"]["openAPIV3Schema"]["properties"]["status"]["properties"]["phase"].get(
            "enum", []
        )
        if len(versions) == 1
        else []
    )
    if "FailedVerifying" not in phase_enum:
        fail("the installed CRD does not preserve FailedVerifying")
    assert_empty()
    registry_id = check_registry()
    return {
        "kubeconfig_sha256": kube_sha,
        "node_uids": sorted(node["metadata"]["uid"] for node in nodes),
        "registry_id": registry_id,
        "images": images,
        "local_config_ids": local_config_ids,
        "runtime": runtime,
        "pod_headroom": pod_headroom,
    }


def host_guard() -> dict:
    mem = next(
        (
            int(line.split()[1])
            for line in Path("/proc/meminfo").read_text().splitlines()
            if line.startswith("MemAvailable:")
        ),
        0,
    )
    docker_root = Path(command(["docker", "info", "--format", "{{.DockerRootDir}}"]).strip())
    if not docker_root.is_dir():
        fail("Docker data root is unavailable")
    root_free = shutil.disk_usage(ROOT).free // 1024
    docker_free = shutil.disk_usage(docker_root).free // 1024
    if mem < MIN_MEMORY_KIB or root_free < MIN_DISK_KIB or docker_free < MIN_DISK_KIB:
        fail("host memory or workspace/Docker disk is below the 8 GiB/20 GiB guard")
    cpus = os.cpu_count() or 0
    load1 = os.getloadavg()[0]
    if cpus < 1 or load1 > cpus * 0.9:
        fail("host one-minute CPU load exceeds the 90% safety guard")
    return {
        "memory_available_kib": mem,
        "workspace_free_kib": root_free,
        "docker_free_kib": docker_free,
        "host_cpus": cpus,
        "load1": load1,
    }


def make_archive(run_dir: Path, first: str, second: str) -> Path:
    archive = run_dir / "source.zip"
    entries = {
        "a-pass/Dockerfile": b"FROM scratch\nCOPY payload /payload\n",
        "a-pass/metadata.json": json.dumps({"target": first, "platform": "linux/amd64"}).encode()
        + b"\n",
        "a-pass/payload": b"kova partial output receipt acceptance\n",
        "z-fail/Dockerfile": b"FROM scratch\nCOPY missing /must-not-exist\n",
        "z-fail/metadata.json": json.dumps({"target": second, "platform": "linux/amd64"}).encode()
        + b"\n",
    }
    with zipfile.ZipFile(archive, "w", compression=zipfile.ZIP_DEFLATED) as output:
        for path, data in entries.items():
            output.writestr(path, data)
    save_json(
        run_dir / "archive-members.json", {name: sha256(data) for name, data in entries.items()}
    )
    return archive


def project_build(build: dict) -> dict:
    status = build.get("status", {})
    kept = (
        "phase",
        "reason",
        "runnerPodName",
        "startedAt",
        "finishedAt",
        "verificationStartedAt",
        "verificationDeadlineAt",
        "verificationNextAttemptAt",
        "verificationAttempts",
        "verificationLastError",
        "verificationResults",
        "outputs",
    )
    return {
        "name": build["metadata"]["name"],
        "uid": build["metadata"]["uid"],
        "status": {key: status[key] for key in kept if key in status},
    }


def project_node(node: dict) -> dict:
    return {
        "name": node["metadata"]["name"],
        "uid": node["metadata"]["uid"],
        "conditions": [
            {"type": item["type"], "status": item["status"]}
            for item in node["status"].get("conditions", [])
            if item["type"] in ("Ready", "MemoryPressure", "DiskPressure", "PIDPressure")
        ],
        "allocatable_pods": node["status"].get("allocatable", {}).get("pods"),
    }


def project_pod(pod: dict) -> dict:
    return {
        "name": pod["metadata"]["name"],
        "uid": pod["metadata"]["uid"],
        "owner_uids": [item["uid"] for item in pod["metadata"].get("ownerReferences", [])],
        "node": pod["spec"].get("nodeName"),
        "phase": pod["status"].get("phase"),
        "containers": [
            {
                "name": item["name"],
                "image_id": item.get("imageID"),
                "restart_count": item.get("restartCount"),
            }
            for item in pod["status"].get("containerStatuses", [])
        ],
    }


def safe_snapshot(run_dir: Path, stage: str, job_id: str = "", uid: str = "") -> None:
    try:
        builds = kjson("-n", NAMESPACE, "get", "kovabuilds", "-o", "json").get("items", [])
        save_json(
            run_dir / f"{stage}-build-identities.json",
            [{"name": item["metadata"]["name"], "uid": item["metadata"]["uid"]} for item in builds],
        )
        owned = [
            item
            for item in builds
            if item["metadata"]["name"] == job_id and item["metadata"]["uid"] == uid
        ]
        if job_id and uid and len(owned) == 1:
            save_json(run_dir / f"{stage}-build.json", project_build(owned[0]))
        runners = kjson(
            "-n", NAMESPACE, "get", "pods", "-l", "app.kubernetes.io/name=kova-runner", "-o", "json"
        ).get("items", [])
        save_json(
            run_dir / f"{stage}-runner-identities.json",
            [
                project_pod(item)
                if uid
                and uid in [ref["uid"] for ref in item["metadata"].get("ownerReferences", [])]
                else {"name": item["metadata"]["name"], "uid": item["metadata"]["uid"]}
                for item in runners
            ],
        )
        save_json(
            run_dir / f"{stage}-nodes.json",
            [project_node(item) for item in kjson("get", "nodes", "-o", "json").get("items", [])],
        )
        for label, configmap, key, entries in (
            ("active", "kova-service-admission", "reservations.json", "active"),
            ("queue", "kova-service-queue-admission", "queue.json", "intents"),
        ):
            raw = kjson("-n", NAMESPACE, "get", "configmap", configmap, "-o", "json")["data"][key]
            parsed = json.loads(raw)
            values = parsed[entries]
            save_json(
                run_dir / f"{stage}-{label}-ledger.json",
                {
                    "configmap": configmap,
                    "entry_count": len(values),
                    "contains_test_identity": (uid if label == "active" else job_id) in values
                    if uid and job_id
                    else False,
                    "content_sha256": sha256(raw.encode()),
                },
            )
        if job_id and uid:
            events = kjson(
                "-n",
                NAMESPACE,
                "get",
                "events",
                "--field-selector",
                f"involvedObject.name={job_id}",
                "-o",
                "json",
            ).get("items", [])
            save_json(
                run_dir / f"{stage}-events.json",
                [
                    {
                        "uid": item["metadata"]["uid"],
                        "type": item.get("type"),
                        "reason": item.get("reason"),
                        "involved_uid": item["involvedObject"]["uid"],
                    }
                    for item in events
                    if item.get("involvedObject", {}).get("uid") == uid
                ],
            )
    except (AcceptanceError, KeyError, ValueError, IndexError) as error:
        save_text(
            run_dir / f"{stage}-snapshot.error.txt",
            f"{type(error).__name__}: {error}\n",
        )


def capture_runner(
    run_dir: Path, runner_name: str, uid: str, runner_image: str, runner_config_id: str
) -> tuple[bool, str]:
    if not re.fullmatch(r"kova-job-idem-[0-9a-f]{20}", runner_name):
        fail("runner Pod name is not bound to the run-scoped build ID")
    raw = kctl("-n", NAMESPACE, "get", "pod", runner_name, "--ignore-not-found", "-o", "json")
    if not raw.strip():
        return False, ""
    pod = json.loads(raw)
    if pod["metadata"]["name"] != runner_name or not any(
        item.get("controller") is True and item.get("uid") == uid
        for item in pod["metadata"].get("ownerReferences", [])
    ):
        fail("runner Pod is not owned by the exact test KovaBuild UID")
    save_json(run_dir / "latest-runner.json", project_pod(pod))
    if not any(
        item.get("name") == "runner" and item.get("imageID")
        for item in pod["status"].get("containerStatuses", [])
    ):
        return False, pod["metadata"]["uid"]
    save_json(
        run_dir / "runner-runtime-image.json",
        runtime_image_fact(pod, "runner", runner_image, runner_config_id),
    )
    return True, pod["metadata"]["uid"]


def capture_runner_logs(run_dir: Path, runner_name: str) -> None:
    """Best-effort bounded excerpts; structured proof is captured separately."""
    for container in ("source-fetch", "runner"):
        try:
            logs = kctl(
                "-n",
                NAMESPACE,
                "logs",
                f"pod/{runner_name}",
                "-c",
                container,
                "--tail=200",
                timeout=20,
            )
        except AcceptanceError:
            continue
        if len(logs.encode()) > 256 * 1024:
            fail("runner log excerpt exceeds its bounded evidence size")
        save_text(run_dir / f"runner-{container}.log", logs)


def verify_copy_missing_export(raw: str, second: str) -> dict | None:
    """Attribute the second target's failure to this fixture's missing COPY source.

    The exact /missing path is fixture-owned evidence from BuildKit's failure,
    not a match on BuildKit's version-dependent error prose. An export without
    the second result is still in progress; any completed ambiguous result
    fails closed.
    """
    if len(raw.encode()) > MAX_RUNNER_EXPORT_BYTES:
        fail("runner failure export exceeded its bounded evidence size")
    try:
        entries = [json.loads(line) for line in raw.splitlines() if line.strip()]
    except ValueError:
        fail("runner failure export was not valid JSONL")
    if any(not isinstance(item, dict) for item in entries):
        fail("runner failure export contained a non-object entry")
    target = second.replace(":dev", ":dev_nydus_v3")
    if not entries:
        return None
    if len(entries) != 1 or entries[0].get("target") != target:
        fail("runner failure export was not scoped to the exact second Nydus target")
    entry = entries[0]
    logs = entry.get("logs", "")
    if (
        entry.get("success") is not False
        or entry.get("manifest_digest")
        or not isinstance(entry.get("reason"), str)
        or not entry["reason"]
        or not isinstance(logs, str)
        or not logs
    ):
        fail("intentional failure target lacks a failed BuildKit result")
    # The source archive has COPY missing /must-not-exist and no file named
    # missing. A bare `COPY missing ...` progress line cannot satisfy /missing.
    # If a BuildKit release omits the source path from its diagnostic, this
    # acceptance must stop for manual review rather than infer causality.
    path_lines = [
        line
        for line in logs.splitlines()
        if re.search(r"(?<![A-Za-z0-9_])/missing(?![A-Za-z0-9_/-])", line)
    ]
    if not path_lines:
        fail("runner failed, but the fixture-owned /missing COPY source was not diagnosed")
    return {
        "target": target,
        "success": False,
        "source_path": "/missing",
        "reason_sha256": sha256(entry["reason"].encode()),
        "diagnostic_line_sha256": sha256(path_lines[-1].encode()),
        "export_sha256": sha256(raw.encode()),
    }


def capture_copy_failure(run_dir: Path, runner_name: str, runner_uid: str, second: str) -> bool:
    target = second.replace(":dev", ":dev_nydus_v3")
    query = urlencode({"with-fail": "true", "target": target})
    try:
        raw = kctl(
            "-n",
            NAMESPACE,
            "exec",
            f"pod/{runner_name}",
            "-c",
            "runner",
            "--",
            "kovad",
            "transport",
            "--method",
            "POST",
            "--path",
            "/api/v1/export",
            "--query",
            query,
            timeout=30,
        )
    except AcceptanceError as error:
        # A new runner may not yet have opened its result store. Retry while
        # the exact Pod exists; a terminal build without proof fails closed.
        save_text(run_dir / "runner-failure-export-unavailable.txt", f"{error}\n")
        return False
    proof = verify_copy_missing_export(raw, second)
    if proof is None:
        return False
    observed = kjson("-n", NAMESPACE, "get", "pod", runner_name, "-o", "json")
    if observed["metadata"]["uid"] != runner_uid:
        fail("runner Pod changed during failure-export capture")
    proof["runner_pod_uid"] = runner_uid
    save_text(run_dir / "runner-failure-export.jsonl", raw)
    save_json(run_dir / "copy-missing-proof.json", proof)
    return True


def watch_copy_failure(
    run_dir: Path,
    runner_name: str,
    runner_uid: str,
    build_uid: str,
    second: str,
    stop: threading.Event,
    result: dict,
) -> None:
    """Observe the exact runner independently of slow foreground safety snapshots.

    Terminal reconciliation removes the runner Pod, so a serial two-second
    foreground poll can miss an otherwise correct BuildKit failure. Missing
    evidence remains an inconclusive acceptance, never a product failure.
    """
    try:
        while not stop.is_set():
            raw = kctl(
                "-n", NAMESPACE, "get", "pod", runner_name, "--ignore-not-found", "-o", "json"
            )
            if not raw.strip():
                result["end"] = "runner-pod-absent"
                return
            pod = json.loads(raw)
            if pod["metadata"]["uid"] != runner_uid or not any(
                item.get("controller") is True and item.get("uid") == build_uid
                for item in pod["metadata"].get("ownerReferences", [])
            ):
                fail("runner identity changed during failure-export observation")
            if capture_copy_failure(run_dir, runner_name, runner_uid, second):
                result["proven"] = True
                result["end"] = "exact-runner-export"
                return
            stop.wait(0.5)
        result["end"] = "stopped"
    except (AcceptanceError, KeyError, ValueError, IndexError) as error:
        result["error"] = f"{type(error).__name__}: {error}"


def start_port_forward(
    run_dir: Path, resource: str, remote_port: int
) -> tuple[subprocess.Popen, int, object]:
    log = (run_dir / f"port-forward-{resource.replace('/', '-')}.log").open("w+", encoding="utf-8")
    process = subprocess.Popen(
        [
            "kubectl",
            "--kubeconfig",
            str(KUBECONFIG),
            "-n",
            NAMESPACE,
            "port-forward",
            "--address",
            "127.0.0.1",
            resource,
            f":{remote_port}",
        ],
        stdout=log,
        stderr=subprocess.STDOUT,
        start_new_session=True,
    )
    try:
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            if process.poll() is not None:
                fail(f"port-forward {resource} exited before readiness")
            log.flush()
            content = (run_dir / f"port-forward-{resource.replace('/', '-')}.log").read_text(
                encoding="utf-8"
            )
            match = re.search(r"Forwarding from 127\.0\.0\.1:(\d+) ->", content)
            if match:
                return process, int(match.group(1)), log
            time.sleep(0.5)
        fail(f"port-forward {resource} did not become ready")
    except BaseException:
        stop_process(process, log)
        raise


def stop_process(process: subprocess.Popen | None, stream: object | None) -> None:
    if process is not None:
        try:
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=5)
        except (OSError, subprocess.TimeoutExpired):
            # A port-forward/proxy failure must not prevent the exact-UID
            # emergency build stop in the caller's finally block.
            try:
                process.kill()
            except OSError:
                pass
    if stream is not None:
        try:
            stream.close()
        except OSError:
            pass


def check_expected_job(
    build: dict, job_id: str, uid: str, source_uri: str, source_digest: str, targets: list[str]
) -> None:
    metadata, spec = build["metadata"], build["spec"]
    if (
        metadata["name"] != job_id
        or metadata["uid"] != uid
        or metadata["namespace"] != NAMESPACE
        or spec["source"] != {"uri": source_uri, "digest": source_digest}
        or spec.get("idempotencyKey") != run_id_from_target(targets[0])
    ):
        fail("KovaBuild identity/source drifted")
    if [(item["target"], item["platform"]) for item in spec["targets"]] != [
        (item, "linux/amd64") for item in targets
    ]:
        fail("KovaBuild targets drifted")
    options = spec["build"]
    if (
        options.get("format") != "both"
        or options.get("concurrency") != 1
        or options.get("timeout") != 300
        or options.get("failFast", False) is not False
    ):
        fail("KovaBuild execution options drifted")


def run_id_from_target(target: str) -> str:
    match = re.fullmatch(r"kind-registry:5000/kova-examples/(partial-41-[a-z0-9-]+)-a:dev", target)
    if not match:
        fail("target is not a run-scoped partial-41 image")
    return match.group(1)


def verify_failed_receipt(build: dict, first: str, second: str) -> dict[str, str]:
    status = build.get("status", {})
    if (
        status.get("phase") != "Failed"
        or status.get("reason") != "BuildFailed"
        or not status.get("finishedAt")
    ):
        fail("real runner failure did not produce final Failed/BuildFailed")
    started, deadline = status.get("verificationStartedAt"), status.get("verificationDeadlineAt")
    if (
        not started
        or not deadline
        or not (
            0
            < (
                datetime.fromisoformat(deadline.replace("Z", "+00:00"))
                - datetime.fromisoformat(started.replace("Z", "+00:00"))
            ).total_seconds()
            <= 300
        )
    ):
        fail("FailedVerifying had no bounded durable deadline")
    if status.get("verificationAttempts", 0) < 1:
        fail("partial results were never verified")
    results = status.get("verificationResults", [])
    expected = {
        (first, "oci"),
        (first.replace(":dev", ":dev_nydus_v3"), "nydus"),
        (second, "oci"),
        (second.replace(":dev", ":dev_nydus_v3"), "nydus"),
    }
    indexed = {(item["image"], item["format"]): item for item in results}
    if len(results) != 4 or set(indexed) != expected:
        fail("expected four exact per-format verification results")
    outputs = status.get("outputs", [])
    successful = {(first, "oci"), (first.replace(":dev", ":dev_nydus_v3"), "nydus")}
    if len(outputs) != 2 or {(item["image"], item["format"]) for item in outputs} != successful:
        fail("partial outputs are not exactly the successful first target")
    digests: dict[str, str] = {}
    for output in outputs:
        key = (output["image"], output["format"])
        result = indexed[key]
        digest = output["manifestDigest"]
        if (
            result.get("state") != "succeeded"
            or result.get("pushedDigest") != digest
            or output["platform"] != result["platform"]
            or not re.fullmatch(r"sha256:[0-9a-f]{64}", digest)
        ):
            fail("output digest does not equal the runner's exact verified push receipt")
        digests[output["image"]] = digest
    for key in expected - successful:
        if indexed[key].get("state") != "failed" or indexed[key].get("pushedDigest"):
            fail("later intentional failure unexpectedly pushed an output")
    return digests


def verify_public_partial(
    public_job: dict, public_results: dict, build: dict, digests: dict[str, str]
) -> None:
    spec, status = build["spec"], build["status"]
    source = spec["source"]
    job_id = build["metadata"]["name"]
    idempotency_key = spec["idempotencyKey"]
    if (
        public_job.get("id") != job_id
        or public_job.get("status") != "failed"
        or public_job.get("failure_code") != "build_failed"
        or public_job.get("source_uri") != source["uri"]
        or public_job.get("source_digest") != source["digest"]
        or public_job.get("idempotency_key") != idempotency_key
        or public_results.get("id") != job_id
        or public_results.get("source_uri") != source["uri"]
        or public_results.get("source_digest") != source["digest"]
        or public_results.get("idempotency_key") != idempotency_key
    ):
        fail("public job/results identity or source differs from the durable failed build")
    expected = {
        (
            item["format"],
            item["image"],
            item["platform"],
            item["manifestDigest"],
            item["image"].rsplit(":", 1)[0] + "@" + item["manifestDigest"],
        )
        for item in status["outputs"]
    }
    outputs = public_results.get("outputs")
    if not isinstance(outputs, list) or len(outputs) != len(expected):
        fail("public partial output count differs from durable receipts")
    actual = {
        (
            item["format"],
            item["image"],
            item["platform"],
            item["manifest_digest"],
            item["immutable_ref"],
        )
        for item in outputs
    }
    if (
        len(actual) != len(outputs)
        or actual != expected
        or {item["image"]: item["manifest_digest"] for item in outputs} != digests
    ):
        fail("public partial output fields differ from exact durable receipts")


def api_delete_with_uid(port: int, job_id: str, uid: str) -> dict:
    if not re.fullmatch(r"idem-[0-9a-f]{20}", job_id) or not re.fullmatch(r"[0-9a-f-]{36}", uid):
        fail("refusing non-run-scoped or UID-less deletion")
    payload = json.dumps(
        {
            "apiVersion": "meta.k8s.io/v1",
            "kind": "DeleteOptions",
            "preconditions": {"uid": uid},
            "propagationPolicy": "Background",
        }
    ).encode()
    path = f"/apis/kova.cofy.dev/v1alpha1/namespaces/{NAMESPACE}/kovabuilds/{job_id}"
    request = Request(
        f"http://127.0.0.1:{port}{path}",
        data=payload,
        headers={"Content-Type": "application/json"},
        method="DELETE",
    )
    try:
        with HTTP.open(request, timeout=20) as response:
            code, body = response.status, response.read(1024 * 1024 + 1)
    except HTTPError as error:
        code, body = error.code, error.read(1024 * 1024 + 1)
    except (OSError, URLError) as error:
        fail(f"exact CR delete outcome is unknown: {type(error).__name__}")
    if len(body) > 1024 * 1024:
        fail("exact CR delete response exceeded bounded size")
    return {
        "job_id": job_id,
        "uid_precondition": uid,
        "http_status": code,
        "body_sha256": sha256(body),
        "at": now(),
    }


def exact_cleanup(
    run_dir: Path,
    job_id: str,
    uid: str,
    source_uri: str,
    source_digest: str,
    targets: list[str],
    kube_sha: str,
) -> None:
    if sha256(KUBECONFIG.read_bytes()) != kube_sha or check_kind_identity() != kube_sha:
        fail("Kind identity changed before exact CR cleanup")
    current = kjson("-n", NAMESPACE, "get", "kovabuild", job_id, "-o", "json")
    check_expected_job(current, job_id, uid, source_uri, source_digest, targets)
    process: subprocess.Popen | None = None
    port = 0
    # kubectl proxy exposes only this dedicated kubeconfig on loopback; the
    # API-server applies UID preconditions atomically, unlike kubectl delete.
    proxy_log = (run_dir / "kubectl-proxy.log").open("w+", encoding="utf-8")
    try:
        process = subprocess.Popen(
            [
                "kubectl",
                "--kubeconfig",
                str(KUBECONFIG),
                "proxy",
                "--address=127.0.0.1",
                "--port=0",
                "--accept-hosts=^127\\.0\\.0\\.1$",
            ],
            stdout=proxy_log,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            if process.poll() is not None:
                fail("kubectl proxy exited before exact deletion")
            content = (run_dir / "kubectl-proxy.log").read_text(encoding="utf-8")
            match = re.search(r"Starting to serve on 127\.0\.0\.1:(\d+)", content)
            if match:
                port = int(match.group(1))
                break
            time.sleep(0.5)
        if not port:
            fail("kubectl proxy did not choose a loopback port")
        receipt = api_delete_with_uid(port, job_id, uid)
        save_json(run_dir / "exact-cr-delete.json", receipt)
        if receipt["http_status"] not in (200, 202):
            fail(f"exact UID-precondition deletion returned HTTP {receipt['http_status']}")
    finally:
        stop_process(process, proxy_log)
    deadline = time.monotonic() + 120
    while time.monotonic() < deadline:
        remaining = kjson("-n", NAMESPACE, "get", "kovabuilds", "-o", "json").get("items", [])
        if not remaining:
            break
        if len(remaining) != 1 or remaining[0]["metadata"].get("uid") != uid:
            fail("unrelated KovaBuild appeared during exact cleanup")
        time.sleep(2)
    else:
        fail("exact KovaBuild did not disappear after UID-precondition deletion")
    deadline = time.monotonic() + 120
    while time.monotonic() < deadline:
        runners = kjson(
            "get", "pods", "-A", "-l", "app.kubernetes.io/name=kova-runner", "-o", "json"
        ).get("items", [])
        if not runners:
            break
        time.sleep(2)
    else:
        fail("runner Pod remains after exact KovaBuild deletion")
    assert_empty()


def run_acceptance(revision: str, facts: dict) -> None:
    global PRIVATE_TOKEN
    ack = os.environ.get("PARTIAL_OUTPUT_ACK", "")
    if ack != f"{CLUSTER}/{NAMESPACE}/{revision}":
        fail(f"run mode requires PARTIAL_OUTPUT_ACK={CLUSTER}/{NAMESPACE}/{revision}")
    service = check_deployment("kova-service", facts["images"]["controller"], "kova-service")
    token = read_static_token(service)
    inherited_cli = os.environ.pop("KOVA_SERVICE_TOKEN", "")
    if inherited_cli and inherited_cli != token:
        fail("inherited CLI token differs from the exact test-owned Secret")
    PRIVATE_TOKEN = token
    headroom = host_guard()
    os.umask(0o077)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dt%H%M%sz")  # noqa: UP017 (Python 3.10)
    run_id = f"partial-41-{stamp}-{secrets.token_hex(4)}"
    run_dir = ROOT / ".work" / "partial-output" / run_id
    run_dir.mkdir(parents=True, mode=0o700, exist_ok=False)
    os.chmod(run_dir, 0o700)
    first_repo, second_repo = f"kova-examples/{run_id}-a", f"kova-examples/{run_id}-z"
    source_repo = f"kova-sources/{run_id}"
    first, second = f"{REGISTRY_CLUSTER}/{first_repo}:dev", f"{REGISTRY_CLUSTER}/{second_repo}:dev"
    targets = [first, second]
    save_json(
        run_dir / "run.json",
        {
            "run_id": run_id,
            "at": now(),
            "cluster": CLUSTER,
            "namespace": NAMESPACE,
            "revision": revision,
            "images": facts["images"],
            "local_config_ids": facts["local_config_ids"],
            "targets": targets,
            "headroom": headroom,
            "kubeconfig_sha256": facts["kubeconfig_sha256"],
            "registry_id": facts["registry_id"],
        },
    )
    service_forward: subprocess.Popen | None = None
    service_log = None
    job_id = uid = source_uri = source_digest = ""
    terminal = False
    passed = False
    runner_runtime_seen = False
    runner_pod_uid = ""
    runner_logs_captured = False
    runner_watch_stop = threading.Event()
    runner_watch_result: dict = {}
    runner_watch: threading.Thread | None = None
    copy_failure_proven = False
    try:
        safe_snapshot(run_dir, "before")
        for repository, tag in (
            (source_repo, "dev"),
            (first_repo, "dev"),
            (first_repo, "dev_nydus_v3"),
            (second_repo, "dev"),
            (second_repo, "dev_nydus_v3"),
        ):
            manifest_digest(repository, tag, absent=True)
        archive = make_archive(run_dir, first, second)
        save_text(run_dir / "archive-sha256.txt", f"{sha256(archive.read_bytes())}  source.zip\n")
        source_json = json.loads(
            command(
                [
                    str(ROOT / "bin/kova"),
                    "source",
                    "push",
                    "--repository",
                    f"localhost:5002/{source_repo}:dev",
                    "--registry-plain-http",
                    "localhost:5002",
                    str(archive),
                ],
                timeout=180,
            )
        )
        save_json(run_dir / "source-receipt.json", source_json)
        host_uri, source_digest = source_json.get("uri", ""), source_json.get("digest", "")
        match = re.fullmatch(
            rf"oci://localhost:5002/{re.escape(source_repo)}@(sha256:[0-9a-f]{{64}})", host_uri
        )
        if (
            not match
            or not re.fullmatch(r"sha256:[0-9a-f]{64}", source_digest)
            or source_digest != f"sha256:{sha256(archive.read_bytes())}"
        ):
            fail("source publication did not return the exact immutable archive receipt")
        if manifest_digest(source_repo, "dev") != match.group(1):
            fail("source tag does not match its immutable manifest digest")
        source_uri = host_uri.replace("oci://localhost:5002/", f"oci://{REGISTRY_CLUSTER}/", 1)
        if preflight(revision) != facts:
            fail("Kind, release, nodes, or local registry changed during source publication")
        host_guard()
        service_forward, service_port, service_log = start_port_forward(
            run_dir, "svc/kova-service", 8080
        )
        expected_id = "idem-" + sha256(("kova:e2e\0" + run_id).encode())[:20]
        cli_env = dict(os.environ, KOVA_SERVICE_TOKEN=token, KOVA_CTX="")
        job_id = expected_id
        submit = json.loads(
            command(
                [
                    str(ROOT / "bin/kova"),
                    "--service-url",
                    f"http://127.0.0.1:{service_port}",
                    "job",
                    "submit",
                    "--source-digest",
                    source_digest,
                    "--target",
                    first,
                    "--platform",
                    "linux/amd64",
                    "--target",
                    second,
                    "--platform",
                    "linux/amd64",
                    "--format",
                    "both",
                    "--concurrency",
                    "1",
                    "--timeout",
                    "300",
                    "--idempotency-key",
                    run_id,
                    source_uri,
                ],
                timeout=45,
                env=cli_env,
            )
        )
        if submit.get("id") != expected_id or submit.get("status") == "succeeded":
            fail("Service did not return the exact run-scoped idempotent build ID")
        save_json(run_dir / "submit.json", submit)
        build = kjson("-n", NAMESPACE, "get", "kovabuild", job_id, "-o", "json")
        uid = build["metadata"]["uid"]
        check_expected_job(build, job_id, uid, source_uri, source_digest, targets)
        note(f"submitted {job_id}; evidence at {run_dir}")
        deadline = time.monotonic() + RUN_DEADLINE_SECONDS
        last_guard = 0.0
        while time.monotonic() < deadline:
            if service_forward.poll() is not None:
                fail("Service port-forward exited during the build")
            current = kjson("-n", NAMESPACE, "get", "kovabuild", job_id, "-o", "json")
            check_expected_job(current, job_id, uid, source_uri, source_digest, targets)
            phase = current.get("status", {}).get("phase", "")
            append_jsonl(
                run_dir / "status-samples.jsonl",
                {"at": now(), **project_build(current)},
            )
            if phase in ("Succeeded", "Cancelled"):
                save_json(run_dir / "unexpected-terminal.json", project_build(current))
                fail(f"runner failure was incorrectly projected as {phase}")
            if phase not in ("", "Queued", "Starting", "Running", "FailedVerifying", "Failed"):
                fail(f"unexpected controller phase {phase}")
            runner_name = current.get("status", {}).get("runnerPodName", "")
            if runner_name:
                observed_runtime, observed_pod_uid = capture_runner(
                    run_dir,
                    runner_name,
                    uid,
                    facts["images"]["runner"],
                    facts["local_config_ids"]["runner"],
                )
                if runner_pod_uid and observed_pod_uid and observed_pod_uid != runner_pod_uid:
                    fail("runner Pod UID changed during one build")
                runner_pod_uid = observed_pod_uid or runner_pod_uid
                runner_runtime_seen = observed_runtime or runner_runtime_seen
                if observed_runtime and runner_watch is None:
                    runner_watch = threading.Thread(
                        target=watch_copy_failure,
                        args=(
                            run_dir,
                            runner_name,
                            runner_pod_uid,
                            uid,
                            second,
                            runner_watch_stop,
                            runner_watch_result,
                        ),
                        name="partial-output-exact-runner-export",
                        daemon=True,
                    )
                    runner_watch.start()
                if observed_runtime and not runner_logs_captured:
                    capture_runner_logs(run_dir, runner_name)
                    runner_logs_captured = True
            if runner_watch_result.get("error"):
                fail(f"exact runner export observer failed: {runner_watch_result['error']}")
            if phase == "Failed":
                save_json(run_dir / "terminal-build.json", project_build(current))
                terminal = True
                break
            if time.monotonic() - last_guard >= 5:
                if check_kind_identity() != facts["kubeconfig_sha256"]:
                    fail("Kind identity changed during the build")
                guarded_nodes = check_nodes()
                if sorted(node["metadata"]["uid"] for node in guarded_nodes) != facts["node_uids"]:
                    fail("Kind node changed during the build")
                pod_headroom = check_pod_capacity(guarded_nodes)
                host_guard()
                builds = kjson("get", "kovabuilds", "-A", "-o", "json").get("items", [])
                if len(builds) != 1 or builds[0]["metadata"]["uid"] != uid:
                    fail("another KovaBuild appeared during the isolated test")
                if (
                    ready_role_pods(
                        "app.kubernetes.io/instance=kova,app.kubernetes.io/component=service",
                        len(facts["runtime"]["service"]),
                        "kova-service",
                        facts["images"]["controller"],
                        facts["local_config_ids"]["controller"],
                    )
                    != facts["runtime"]["service"]
                ):
                    fail("Service Pod runtime image identity changed")
                if (
                    ready_role_pods(
                        "app.kubernetes.io/instance=kova,app.kubernetes.io/name=kova",
                        len(facts["runtime"]["worker"]),
                        "buildkitd",
                        facts["images"]["worker"],
                        facts["local_config_ids"]["worker"],
                    )
                    != facts["runtime"]["worker"]
                ):
                    fail("worker Pod runtime image identity changed")
                append_jsonl(
                    run_dir / "node-samples.jsonl",
                    {
                        "at": now(),
                        "nodes": [project_node(item) for item in guarded_nodes],
                        "pod_headroom": pod_headroom,
                    },
                )
                last_guard = time.monotonic()
            time.sleep(2)
        else:
            fail("bounded 20-minute runner/verification deadline expired")
        if not terminal:
            fail("no terminal KovaBuild receipt")
        if runner_watch is not None:
            # Give an in-flight exec a bounded chance to complete before the
            # terminal controller removes the Pod, then stop before cleanup.
            runner_watch.join(timeout=8)
            runner_watch_stop.set()
            runner_watch.join(timeout=35)
            if runner_watch.is_alive():
                fail("exact runner export observer did not stop within its bounded deadline")
            if runner_watch_result.get("error"):
                fail(f"exact runner export observer failed: {runner_watch_result['error']}")
            copy_failure_proven = runner_watch_result.get("proven") is True
        if not runner_runtime_seen:
            fail("acceptance evidence incomplete: runner Pod runtime identity was never proven")
        if not copy_failure_proven:
            fail("acceptance evidence incomplete: exact runner /missing export was not captured")
        digests = verify_failed_receipt(current, first, second)
        for image, digest in digests.items():
            repository, tag = image.removeprefix(f"{REGISTRY_CLUSTER}/").split(":", 1)
            if manifest_digest(repository, tag) != digest:
                fail(f"registry tag drifted from exact runner push receipt: {image}")
        manifest_digest(second_repo, "dev", absent=True)
        manifest_digest(second_repo, "dev_nydus_v3", absent=True)
        public_job = json.loads(
            command(
                [
                    str(ROOT / "bin/kova"),
                    "--service-url",
                    f"http://127.0.0.1:{service_port}",
                    "job",
                    "get",
                    job_id,
                ],
                timeout=30,
                env=cli_env,
            )
        )
        public_results = json.loads(
            command(
                [
                    str(ROOT / "bin/kova"),
                    "--service-url",
                    f"http://127.0.0.1:{service_port}",
                    "job",
                    "results",
                    job_id,
                ],
                timeout=30,
                env=cli_env,
            )
        )
        save_json(run_dir / "public-job.json", public_job)
        save_json(run_dir / "public-results.json", public_results)
        verify_public_partial(public_job, public_results, current, digests)
        safe_snapshot(run_dir, "terminal", job_id, uid)
        exact_cleanup(
            run_dir, job_id, uid, source_uri, source_digest, targets, facts["kubeconfig_sha256"]
        )
        safe_snapshot(run_dir, "after-exact-cr-cleanup", job_id, uid)
        passed = True
        note(f"PASS: failed runner retained two exact OCI/Nydus receipts; evidence at {run_dir}")
    finally:
        runner_watch_stop.set()
        if runner_watch is not None and runner_watch.is_alive():
            runner_watch.join(timeout=35)
            if runner_watch.is_alive():
                note("exact runner export observer did not stop before emergency cleanup")
        stop_process(service_forward, service_log)
        if not passed:
            try:
                safe_snapshot(run_dir, "failure", job_id, uid)
            except Exception as error:
                # Evidence storage can fail (for example, disk full). The
                # exact-UID stop still takes priority over the snapshot.
                note(f"failure snapshot unavailable: {type(error).__name__}")
            note(f"FAILED: evidence and all run-scoped registry tags retained at {run_dir}")
            if job_id and not terminal:
                try:
                    if not uid:
                        observed = kctl(
                            "-n",
                            NAMESPACE,
                            "get",
                            "kovabuild",
                            job_id,
                            "--ignore-not-found",
                            "-o",
                            "json",
                        )
                        if observed.strip():
                            candidate = json.loads(observed)
                            candidate_uid = candidate["metadata"]["uid"]
                            check_expected_job(
                                candidate, job_id, candidate_uid, source_uri, source_digest, targets
                            )
                            uid = candidate_uid
                    if uid:
                        exact_cleanup(
                            run_dir,
                            job_id,
                            uid,
                            source_uri,
                            source_digest,
                            targets,
                            facts["kubeconfig_sha256"],
                        )
                        note(
                            f"exact UID-preconditioned emergency stop completed for {job_id}/{uid}"
                        )
                    else:
                        note(f"{job_id} is not present")
                        note("inspect queue admission after uncertain submit")
                except Exception as error:
                    note(f"exact emergency stop unproven: {type(error).__name__}")
                    note(f"inspect {job_id}/{uid or 'unknown-uid'} before another test")
        else:
            note("only the UID-preconditioned test CR was removed")
            note("Kind, registry, tags, and receipts remain")


def main() -> int:
    mode = os.environ.get("PARTIAL_OUTPUT_MODE", "check")
    if mode not in ("check", "run"):
        fail("PARTIAL_OUTPUT_MODE must be check or run")
    for interrupt in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP):
        signal.signal(interrupt, interrupted)
    require_tools(mode)
    revision = os.environ.get("PARTIAL_OUTPUT_EXPECTED_REVISION", "")
    facts = preflight(revision)
    note(f"read-only preflight passed: {CLUSTER}, candidate {revision}, 2/2 nodes")
    note("workloads and admission ledgers are empty")
    if mode == "check":
        note("no writes; run needs PARTIAL_OUTPUT_MODE=run")
        note(f"PARTIAL_OUTPUT_ACK={CLUSTER}/{NAMESPACE}/{revision}")
        note("token is read from the exact test Secret")
        return 0
    run_acceptance(revision, facts)
    return 0


def interrupted(signum: int, _frame: object) -> None:
    # A disconnect must enter the same bounded, UID-preconditioned stop path.
    # Ignore follow-up HUP/TERM during receipt preservation and cleanup.
    for interrupt in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP):
        signal.signal(interrupt, signal.SIG_IGN)
    fail(f"received signal {signum}; preserving evidence and stopping the exact run")


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (AcceptanceError, KeyError, ValueError, IndexError) as error:
        note(f"error: {error}")
        raise SystemExit(1) from None
