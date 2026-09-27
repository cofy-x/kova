#!/usr/bin/env python3
"""Fail-closed real-runner same-tag overwrite acceptance on disposable Kind.

Check mode is read-only. Run mode owns exactly two KovaBuilds, one constrained
registry proxy, and run-scoped source/output tags in a dedicated registry.
It never touches the shared localhost:5002 registry or any cloud cluster.
"""

from __future__ import annotations

import hashlib
import importlib.util
import json
import os
import re
import secrets
import signal
import subprocess
import sys
import threading
import time
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import NamedTuple
from urllib.error import HTTPError, URLError
from urllib.request import Request

ROOT = Path(__file__).resolve().parents[2]
PARTIAL_SCRIPT = Path(__file__).with_name("e2e-service-partial-output.py")
SPEC = importlib.util.spec_from_file_location("kova_partial_acceptance", PARTIAL_SCRIPT)
assert SPEC and SPEC.loader
base = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(base)

# Reuse the partial-output acceptance's identity, resource, token, runner,
# receipt, and exact-UID safety functions with a separate immutable fixture.
CLUSTER = "kova-digest-collision-41"
NAMESPACE = "kova"
REGISTRY = "kind-registry-digest-41"
REGISTRY_HOST = "127.0.0.1:5004"
REGISTRY_CLUSTER = f"{REGISTRY}:5000"
PROXY_NAME = "kova-digest-fault-proxy"
PROXY_HOST = f"{PROXY_NAME}.{NAMESPACE}.svc.cluster.local:5000"
PROXY_IMAGE = "python@sha256:4c47124a8391cb7a9f571164147d154777cf012a4ece5f86097130d7a4478111"
IMAGE_REPOSITORY = "localhost:5004/kova"
RUN_SECONDS = 20 * 60
SHA = re.compile(r"sha256:[0-9a-f]{64}")


class ProxyIdentity(NamedTuple):
    deployment_uid: str
    service_uid: str
    config_uid: str
    code_sha256: str


base.CLUSTER = CLUSTER
base.KUBECONFIG = ROOT / ".kind" / f"{CLUSTER}.kubeconfig"
base.REGISTRY = REGISTRY
base.REGISTRY_HOST = REGISTRY_HOST
base.REGISTRY_PORT = "5004"
base.REGISTRY_CLUSTER = REGISTRY_CLUSTER
base.IMAGE_REPOSITORY = IMAGE_REPOSITORY


def note(message: str) -> None:
    print(f"digest-collision-e2e: {message}", file=sys.stderr, flush=True)


def fail(message: str) -> None:
    raise base.AcceptanceError(message)


def save(run_dir: Path, name: str, value: object) -> None:
    base.save_json(run_dir / name, value)


def expected_targets(run_id: str) -> tuple[str, str]:
    if not re.fullmatch(r"digest-41-[0-9]{8}t[0-9]{6}z-[0-9a-f]{8}", run_id):
        fail("invalid exact run ID")
    return (
        f"{PROXY_HOST}/kova-examples/{run_id}-a:dev",
        f"{PROXY_HOST}/kova-examples/{run_id}-z:dev",
    )


def assert_proxy_absent() -> None:
    for resource in ("configmap", "deployment", "service"):
        found = base.kctl(
            "-n", NAMESPACE, "get", resource, PROXY_NAME, "--ignore-not-found", "-o", "json"
        )
        if found.strip():
            fail(f"test proxy {resource}/{PROXY_NAME} already exists")
    for resource in ("pods", "replicasets"):
        found = base.kjson(
            "-n",
            NAMESPACE,
            "get",
            resource,
            "-l",
            "kova.cofy.dev/e2e=digest-collision-proxy",
            "-o",
            "json",
        ).get("items", [])
        if found:
            fail(f"orphan test proxy {resource} already exist")


def check_fixture(revision: str) -> dict:
    if (
        os.environ.get("KIND_CLUSTER", CLUSTER) != CLUSTER
        or os.environ.get("REGISTRY_NAME", REGISTRY) != REGISTRY
        or os.environ.get("REGISTRY_PORT", "5004") != "5004"
    ):
        fail("Kind or registry override escaped the dedicated #41 fixture")
    if os.environ.get("REGISTRY_HOST", REGISTRY_HOST) not in (REGISTRY_HOST, "localhost:5004"):
        fail("host registry override escaped localhost:5004")
    base.require_tools("check")
    facts = base.preflight(revision)
    service = base.check_deployment("kova-service", facts["images"]["controller"], "kova-service")
    registry_args = [
        item for item in service.get("args", []) if item.startswith("--registry-plain-http=")
    ]
    if registry_args != [
        f"--registry-plain-http={REGISTRY_CLUSTER}",
        f"--registry-plain-http={PROXY_HOST}",
    ]:
        fail("Service verification registries differ from the isolated backend/proxy pair")
    if "--max-active-jobs=2" not in service.get("args", []):
        fail("the isolated fixture must permit exactly two active builds")
    values = json.loads(
        base.command(
            [
                "helm",
                "get",
                "values",
                "kova",
                "--kubeconfig",
                str(base.KUBECONFIG),
                "-n",
                NAMESPACE,
                "-o",
                "json",
            ]
        )
    )
    if values.get("serviceDaemon", {}).get("registryPlainHTTP") != [
        REGISTRY_CLUSTER,
        PROXY_HOST,
    ]:
        fail("Helm values do not contain only the test registry pair")
    buildkit_config = values.get("buildkitdConfig", "")
    if not all(
        f'[registry."{host}"]' in buildkit_config for host in (REGISTRY_CLUSTER, PROXY_HOST)
    ):
        fail("BuildKit config lacks the exact isolated backend/proxy HTTP registries")
    assert_proxy_absent()
    return facts


def make_b_archive(run_dir: Path, target: str, run_id: str) -> Path:
    archive = run_dir / "source-b.zip"
    entries = {
        "a-pass/Dockerfile": b"FROM scratch\nCOPY payload /payload\n",
        "a-pass/metadata.json": json.dumps({"target": target, "platform": "linux/amd64"}).encode()
        + b"\n",
        "a-pass/payload": f"second overwrite build {run_id}\n".encode(),
    }
    with zipfile.ZipFile(archive, "w", compression=zipfile.ZIP_DEFLATED) as output:
        for path, data in entries.items():
            output.writestr(path, data)
    save(
        run_dir,
        "archive-b-members.json",
        {key: base.sha256(value) for key, value in entries.items()},
    )
    return archive


def push_source(run_dir: Path, label: str, archive: Path, run_id: str) -> tuple[str, str]:
    repository = f"kova-sources/{run_id}-{label}"
    base.manifest_digest(repository, "dev", absent=True)
    receipt = json.loads(
        base.command(
            [
                str(ROOT / "bin/kova"),
                "source",
                "push",
                "--repository",
                f"localhost:5004/{repository}:dev",
                "--registry-plain-http",
                "localhost:5004",
                str(archive),
            ],
            timeout=180,
        )
    )
    save(run_dir, f"source-{label}-receipt.json", receipt)
    uri, digest = receipt.get("uri", ""), receipt.get("digest", "")
    match = re.fullmatch(
        rf"oci://localhost:5004/{re.escape(repository)}@(sha256:[0-9a-f]{{64}})", uri
    )
    if (
        not match
        or SHA.fullmatch(digest) is None
        or digest != "sha256:" + base.sha256(archive.read_bytes())
    ):
        fail(f"source {label} publication returned no exact archive/manifest receipt")
    if base.manifest_digest(repository, "dev") != match.group(1):
        fail(f"source {label} tag drifted from immutable manifest")
    return uri.replace("oci://localhost:5004/", f"oci://{REGISTRY_CLUSTER}/", 1), digest


def create_object(value: dict) -> dict:
    kind = value["kind"].lower()
    created = json.loads(
        base.command(
            [
                "kubectl",
                "--kubeconfig",
                str(base.KUBECONFIG),
                "-n",
                NAMESPACE,
                "create",
                "-f",
                "-",
                "-o",
                "json",
            ],
            input_text=json.dumps(value),
            timeout=30,
        )
    )
    if (
        created.get("kind", "").lower() != kind
        or created.get("metadata", {}).get("name") != value["metadata"]["name"]
    ):
        fail(f"created proxy {kind} identity differs")
    return created


def proxy_objects(repository: str, failed_repository: str) -> tuple[dict, dict]:
    deployment = {
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": {
            "name": PROXY_NAME,
            "namespace": NAMESPACE,
            "labels": {"kova.cofy.dev/e2e": "digest-collision-41"},
        },
        "spec": {
            "replicas": 1,
            "strategy": {"type": "Recreate"},
            "selector": {"matchLabels": {"kova.cofy.dev/e2e": "digest-collision-proxy"}},
            "template": {
                "metadata": {
                    "labels": {"kova.cofy.dev/e2e": "digest-collision-proxy"},
                    "annotations": {"kova.cofy.dev/fault-mode-token": "initial"},
                },
                "spec": {
                    "securityContext": {
                        "runAsNonRoot": True,
                        "runAsUser": 65532,
                        "runAsGroup": 65532,
                    },
                    "containers": [
                        {
                            "name": "proxy",
                            "image": PROXY_IMAGE,
                            "imagePullPolicy": "IfNotPresent",
                            "command": ["python3", "-u", "/fault/proxy.py"],
                            "env": [
                                {"name": "COLLISION_REPOSITORY", "value": repository},
                                {"name": "COLLISION_FAILED_REPOSITORY", "value": failed_repository},
                            ],
                            "ports": [{"name": "http", "containerPort": 5000}],
                            "readinessProbe": {
                                "httpGet": {"path": "/v2/", "port": 5000},
                                "periodSeconds": 2,
                            },
                            "resources": {
                                "requests": {"cpu": "20m", "memory": "32Mi"},
                                "limits": {"cpu": "500m", "memory": "128Mi"},
                            },
                            "volumeMounts": [
                                {"name": "fault", "mountPath": "/fault", "readOnly": True}
                            ],
                        }
                    ],
                    "volumes": [{"name": "fault", "configMap": {"name": PROXY_NAME}}],
                },
            },
        },
    }
    service = {
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": {
            "name": PROXY_NAME,
            "namespace": NAMESPACE,
            "labels": {"kova.cofy.dev/e2e": "digest-collision-41"},
        },
        "spec": {
            "type": "ClusterIP",
            "selector": {"kova.cofy.dev/e2e": "digest-collision-proxy"},
            "ports": [{"name": "http", "port": 5000, "targetPort": 5000}],
        },
    }
    return deployment, service


def proxy_pod(deployment_uid: str, mode_token: str) -> dict:
    deployment = base.kjson("-n", NAMESPACE, "get", "deployment", PROXY_NAME, "-o", "json")
    if (
        deployment["metadata"]["uid"] != deployment_uid
        or deployment["spec"].get("replicas") != 1
        or deployment["spec"]["template"]["metadata"]["annotations"].get(
            "kova.cofy.dev/fault-mode-token"
        )
        != mode_token
        or deployment["spec"]["template"]["spec"]["containers"][0].get("image") != PROXY_IMAGE
    ):
        fail("exact proxy Deployment drifted")
    pods = base.kjson(
        "-n",
        NAMESPACE,
        "get",
        "pods",
        "-l",
        "kova.cofy.dev/e2e=digest-collision-proxy",
        "-o",
        "json",
    ).get("items", [])
    if len(pods) != 1:
        fail("proxy has other than one Pod")
    pod = pods[0]
    if (
        pod.get("metadata", {}).get("deletionTimestamp")
        or pod["spec"]["containers"][0].get("image") != PROXY_IMAGE
        or not any(
            item.get("name") == "proxy" and item.get("imageID")
            for item in pod["status"].get("containerStatuses", [])
        )
        or not any(
            item.get("type") == "Ready" and item.get("status") == "True"
            for item in pod["status"].get("conditions", [])
        )
    ):
        fail("exact proxy Pod is not Ready")
    owners = [
        item
        for item in pod["metadata"].get("ownerReferences", [])
        if item.get("controller") is True and item.get("kind") == "ReplicaSet"
    ]
    if len(owners) != 1:
        fail("proxy Pod is not owned by exactly one ReplicaSet")
    replica = base.kjson("-n", NAMESPACE, "get", "replicaset", owners[0]["name"], "-o", "json")
    if replica["metadata"]["uid"] != owners[0]["uid"] or not any(
        item.get("controller") is True
        and item.get("kind") == "Deployment"
        and item.get("uid") == deployment_uid
        for item in replica["metadata"].get("ownerReferences", [])
    ):
        fail("proxy Pod owner chain does not reach the exact Deployment UID")
    return pod


def check_proxy_contract(identity: ProxyIdentity, expected_mode: str) -> dict:
    config = base.kjson("-n", NAMESPACE, "get", "configmap", PROXY_NAME, "-o", "json")
    code = config.get("data", {}).get("proxy.py", "")
    if (
        config["metadata"]["uid"] != identity.config_uid
        or config.get("data", {}).get("mode") != expected_mode
        or base.sha256(code.encode()) != identity.code_sha256
    ):
        fail("exact proxy ConfigMap UID, mode, or code digest drifted")
    service = base.kjson("-n", NAMESPACE, "get", "service", PROXY_NAME, "-o", "json")
    ports = service.get("spec", {}).get("ports", [])
    if (
        service["metadata"]["uid"] != identity.service_uid
        or service["spec"].get("type") != "ClusterIP"
        or service["spec"].get("selector") != {"kova.cofy.dev/e2e": "digest-collision-proxy"}
        or len(ports) != 1
        or any(
            ports[0].get(key) != value
            for key, value in (
                ("name", "http"),
                ("port", 5000),
                ("targetPort", 5000),
                ("protocol", "TCP"),
            )
        )
    ):
        fail("exact proxy Service UID, selector, or port drifted")
    return proxy_pod(identity.deployment_uid, "initial")


def proxy_mode(run_dir: Path, expected: str) -> None:
    process, port, stream = base.start_port_forward(run_dir, f"svc/{PROXY_NAME}", 5000)
    try:
        deadline = time.monotonic() + 90
        while time.monotonic() < deadline:
            request = Request(f"http://127.0.0.1:{port}/fault/mode", method="GET")
            try:
                with base.HTTP.open(request, timeout=3) as response:
                    if response.status == 200 and response.read(256).decode().strip() == expected:
                        return
            except (HTTPError, URLError, OSError):
                pass
            time.sleep(1)
        fail(f"exact proxy did not enter {expected} mode")
    finally:
        base.stop_process(process, stream)


def create_proxy(run_dir: Path, repository: str, failed_repository: str) -> ProxyIdentity:
    assert_proxy_absent()
    code_sha256 = base.sha256(
        Path(__file__).with_name("registry-digest-collision-proxy.py").read_bytes()
    )
    config = json.loads(
        base.command(
            [
                "kubectl",
                "--kubeconfig",
                str(base.KUBECONFIG),
                "-n",
                NAMESPACE,
                "create",
                "configmap",
                PROXY_NAME,
                f"--from-file=proxy.py={Path(__file__).with_name('registry-digest-collision-proxy.py')}",
                "--from-literal=mode=503",
                "-o",
                "json",
            ],
            timeout=30,
        )
    )
    save(
        run_dir,
        "proxy-configmap-created.json",
        {
            "name": config["metadata"]["name"],
            "uid": config["metadata"]["uid"],
            "mode": config["data"]["mode"],
            "code_sha256": base.sha256(config["data"]["proxy.py"].encode()),
        },
    )
    if base.sha256(config["data"]["proxy.py"].encode()) != code_sha256:
        fail("created proxy ConfigMap code differs from the reviewed local file")
    deployment, service = proxy_objects(repository, failed_repository)
    deployed = create_object(deployment)
    served = create_object(service)
    identity = ProxyIdentity(
        deployed["metadata"]["uid"],
        served["metadata"]["uid"],
        config["metadata"]["uid"],
        code_sha256,
    )
    save(
        run_dir,
        "proxy-object-identities.json",
        {
            "configmap_uid": config["metadata"]["uid"],
            "deployment_uid": deployed["metadata"]["uid"],
            "service_uid": served["metadata"]["uid"],
            "code_sha256": code_sha256,
        },
    )
    base.kctl(
        "-n",
        NAMESPACE,
        "rollout",
        "status",
        f"deployment/{PROXY_NAME}",
        "--timeout=180s",
        timeout=190,
    )
    pod = check_proxy_contract(identity, "503")
    save(run_dir, "proxy-initial-pod.json", base.project_pod(pod))
    proxy_mode(run_dir, "503")
    return identity


def set_proxy_mode(run_dir: Path, identity: ProxyIdentity, old_mode: str, new_mode: str) -> None:
    current = check_proxy_contract(identity, old_mode)
    pod_uid = current["metadata"]["uid"]
    patch = json.dumps(
        [
            {"op": "test", "path": "/metadata/uid", "value": identity.config_uid},
            {"op": "test", "path": "/data/mode", "value": old_mode},
            {
                "op": "test",
                "path": "/data/proxy.py",
                "value": Path(__file__)
                .with_name("registry-digest-collision-proxy.py")
                .read_text(encoding="utf-8"),
            },
            {"op": "replace", "path": "/data/mode", "value": new_mode},
        ]
    )
    base.kctl("-n", NAMESPACE, "patch", "configmap", PROXY_NAME, "--type=json", "-p", patch)
    proxy_mode(run_dir, new_mode)
    pod = check_proxy_contract(identity, new_mode)
    if pod["metadata"]["uid"] != pod_uid:
        fail("proxy Pod changed while switching the exact digest fault mode")
    save(
        run_dir,
        f"proxy-mode-{new_mode.split(':', 1)[0]}.json",
        {
            "at": base.now(),
            "deployment_uid": identity.deployment_uid,
            "service_uid": identity.service_uid,
            "configmap_uid": identity.config_uid,
            "code_sha256": identity.code_sha256,
            "pod_uid": pod_uid,
            "mode": new_mode,
        },
    )


def probe_proxy_manifest(
    run_dir: Path,
    identity: ProxyIdentity,
    mode: str,
    phase: str,
    repository: str,
    digest: str,
    expected_status: int,
) -> None:
    if (
        not re.fullmatch(r"[a-z0-9-]+", phase)
        or not re.fullmatch(
            r"kova-examples/digest-41-[0-9]{8}t[0-9]{6}z-[0-9a-f]{8}-a",
            repository,
        )
        or SHA.fullmatch(digest) is None
        or expected_status not in (200, 503)
    ):
        fail("proxy digest probe escaped the exact test repository or status contract")
    pod_uid = check_proxy_contract(identity, mode)["metadata"]["uid"]
    process, port, stream = base.start_port_forward(run_dir, f"svc/{PROXY_NAME}", 5000)
    try:
        request = Request(
            f"http://127.0.0.1:{port}/v2/{repository}/manifests/{digest}",
            method="GET",
            headers={"Host": PROXY_HOST, "Accept": base.MANIFEST_ACCEPT},
        )
        try:
            with base.HTTP.open(request, timeout=10) as response:
                status = response.status
                headers = response.headers
                body = response.read(4 * 1024 * 1024 + 1)
        except HTTPError as error:
            status = error.code
            headers = error.headers
            body = error.read(4 * 1024 * 1024 + 1)
            error.close()
        except (URLError, OSError) as error:
            fail(f"proxy digest GET outcome unknown: {type(error).__name__}")
        if len(body) > 4 * 1024 * 1024 or status != expected_status:
            fail(f"proxy digest GET returned HTTP {status}, expected {expected_status}")
        if expected_status == 503:
            if body != b"test-only digest verification fault":
                fail("proxy digest fault was not emitted by the exact test proxy")
        elif (
            headers.get("Docker-Content-Digest", "") != digest
            or "sha256:" + hashlib.sha256(body).hexdigest() != digest
        ):
            fail("proxy healthy digest response differs from its immutable manifest")
        if check_proxy_contract(identity, mode)["metadata"]["uid"] != pod_uid:
            fail("proxy Pod changed during exact digest probe")
        save(
            run_dir,
            f"proxy-probe-{phase}.json",
            {
                "at": base.now(),
                "mode": mode,
                "pod_uid": pod_uid,
                "repository": repository,
                "digest": digest,
                "http_status": status,
                "body_sha256": base.sha256(body),
            },
        )
    finally:
        base.stop_process(process, stream)


def submit(
    run_dir: Path,
    label: str,
    port: int,
    token: str,
    key: str,
    source_uri: str,
    source_digest: str,
    targets: list[str],
) -> tuple[str, str]:
    job_id = "idem-" + base.sha256(("kova:e2e\0" + key).encode())[:20]
    save(
        run_dir,
        f"{label}-submit-attempt.json",
        {
            "job_id": job_id,
            "idempotency_key": key,
            "source_uri": source_uri,
            "source_digest": source_digest,
            "targets": targets,
            "at": base.now(),
        },
    )
    args = [
        str(ROOT / "bin/kova"),
        "--service-url",
        f"http://127.0.0.1:{port}",
        "job",
        "submit",
        "--source-digest",
        source_digest,
    ]
    for target in targets:
        args.extend(("--target", target, "--platform", "linux/amd64"))
    args.extend(
        (
            "--format",
            "both",
            "--concurrency",
            "1",
            "--timeout",
            "300",
            "--idempotency-key",
            key,
            source_uri,
        )
    )
    response = json.loads(
        base.command(args, timeout=45, env=dict(os.environ, KOVA_SERVICE_TOKEN=token, KOVA_CTX=""))
    )
    if response.get("id") != job_id or response.get("status") in ("succeeded", "failed"):
        fail(f"{label} submission did not return the exact live build ID")
    save(run_dir, f"{label}-submit.json", response)
    build = base.kjson("-n", NAMESPACE, "get", "kovabuild", job_id, "-o", "json")
    uid = build["metadata"]["uid"]
    base.check_expected_job(
        build, job_id, uid, source_uri, source_digest, targets, idempotency_key=key
    )
    save(run_dir, f"{label}-accepted.json", base.project_build(build))
    return job_id, uid


def exact_build(
    job_id: str, uid: str, source_uri: str, source_digest: str, targets: list[str], key: str
) -> dict:
    build = base.kjson("-n", NAMESPACE, "get", "kovabuild", job_id, "-o", "json")
    base.check_expected_job(
        build, job_id, uid, source_uri, source_digest, targets, idempotency_key=key
    )
    return build


def pending_digests(build: dict, first: str, second: str | None) -> dict[str, str]:
    status = build.get("status", {})
    expected_phase = "FailedVerifying" if second is not None else "Verifying"
    if (
        status.get("phase") != expected_phase
        or (second is not None and status.get("reason") != "BuildFailed")
        or status.get("verificationAttempts", 0) < 1
        or status.get("verificationLastError") != "registry manifest verification unavailable"
        or status.get("outputs", [])
        or not status.get("verificationDeadlineAt")
    ):
        fail(
            f"{build['metadata']['name']} is not durably pending on the digest-only registry fault"
        )
    expected = {(first, "oci"), (first.replace(":dev", ":dev_nydus_v3"), "nydus")}
    if second is not None:
        expected |= {(second, "oci"), (second.replace(":dev", ":dev_nydus_v3"), "nydus")}
    results = status.get("verificationResults", [])
    indexed = {(item.get("image"), item.get("format")): item for item in results}
    if len(results) != len(expected) or set(indexed) != expected:
        fail("pending result set is not the exact two-build output contract")
    digests: dict[str, str] = {}
    for image, format_name in expected:
        result = indexed[(image, format_name)]
        if image.startswith(first.rsplit(":", 1)[0]):
            digest = result.get("pushedDigest", "")
            if result.get("state") != "pending" or SHA.fullmatch(digest) is None:
                fail("an exact first-target push receipt was not durably pending")
            digests[image] = digest
        elif result.get("state") != "failed" or result.get("pushedDigest"):
            fail("the intentional later target unexpectedly pushed a digest")
    return digests


def guard_running(facts: dict, identities: dict[str, str]) -> None:
    if base.check_kind_identity() != facts["kubeconfig_sha256"]:
        fail("Kind kubeconfig identity changed during overwrite acceptance")
    nodes = base.check_nodes()
    if sorted(node["metadata"]["uid"] for node in nodes) != facts["node_uids"]:
        fail("Kind node identity changed during overwrite acceptance")
    base.check_pod_capacity(nodes)
    base.host_guard()
    builds = base.kjson("get", "kovabuilds", "-A", "-o", "json").get("items", [])
    actual = {item["metadata"]["name"]: item["metadata"]["uid"] for item in builds}
    if len(actual) != len(builds) or actual != identities:
        fail("another KovaBuild appeared or an exact build identity changed")
    if (
        base.ready_role_pods(
            "app.kubernetes.io/instance=kova,app.kubernetes.io/component=service",
            len(facts["runtime"]["service"]),
            "kova-service",
            facts["images"]["controller"],
            facts["local_config_ids"]["controller"],
        )
        != facts["runtime"]["service"]
        or base.ready_role_pods(
            "app.kubernetes.io/instance=kova,app.kubernetes.io/name=kova",
            len(facts["runtime"]["worker"]),
            "buildkitd",
            facts["images"]["worker"],
            facts["local_config_ids"]["worker"],
        )
        != facts["runtime"]["worker"]
    ):
        fail("Service or worker runtime image identity changed")


def wait_pending(
    run_dir: Path,
    label: str,
    facts: dict,
    identity: tuple[str, str, str, str, list[str], str],
    first: str,
    second: str | None,
    identities: dict[str, str],
    *,
    copy_watch: dict | None = None,
) -> tuple[dict, dict[str, str]]:
    job_id, uid, source_uri, source_digest, targets, key = identity
    deadline = time.monotonic() + RUN_SECONDS
    next_guard = 0.0
    while time.monotonic() < deadline:
        build = exact_build(job_id, uid, source_uri, source_digest, targets, key)
        save(run_dir, f"{label}-latest-build.json", base.project_build(build))
        phase = build.get("status", {}).get("phase", "")
        if phase in ("Failed", "Succeeded", "Cancelled"):
            fail(f"{label} reached {phase} before digest-only fault/overwrite proof")
        if phase not in ("", "Queued", "Starting", "Running", "Verifying", "FailedVerifying"):
            fail(f"{label} entered unexpected phase {phase}")
        runner_name = build.get("status", {}).get("runnerPodName", "")
        if runner_name and copy_watch is not None:
            observed, pod_uid = base.capture_runner(
                run_dir,
                runner_name,
                uid,
                facts["images"]["runner"],
                facts["local_config_ids"]["runner"],
            )
            if copy_watch.get("pod_uid") and pod_uid and copy_watch["pod_uid"] != pod_uid:
                fail("A runner Pod UID changed")
            copy_watch["pod_uid"] = pod_uid or copy_watch.get("pod_uid", "")
            copy_watch["runtime_seen"] = observed or copy_watch.get("runtime_seen", False)
            if observed and second is not None and copy_watch.get("thread") is None:
                stop = threading.Event()
                result: dict = {}
                watcher = threading.Thread(
                    target=base.watch_copy_failure,
                    args=(run_dir, runner_name, copy_watch["pod_uid"], uid, second, stop, result),
                    daemon=True,
                    name="digest-collision-partial-failure-export",
                )
                copy_watch.update({"thread": watcher, "stop": stop, "result": result})
                watcher.start()
        if copy_watch and copy_watch.get("result", {}).get("error"):
            fail(f"A failure-export observer failed: {copy_watch['result']['error']}")
        if phase == ("FailedVerifying" if second else "Verifying"):
            try:
                digests = pending_digests(build, first, second)
            except base.AcceptanceError:
                # Collection may not yet have committed both output receipts.
                if build.get("status", {}).get("verificationAttempts", 0) >= 1:
                    raise
            else:
                save(run_dir, f"{label}-pending.json", base.project_build(build))
                return build, digests
        if time.monotonic() >= next_guard:
            guard_running(facts, identities)
            next_guard = time.monotonic() + 5
        time.sleep(1)
    fail(f"{label} did not reach a bounded durable pending state")


def wait_b_succeeded(
    run_dir: Path,
    facts: dict,
    a_identity: tuple[str, str, str, str, list[str], str],
    b_identity: tuple[str, str, str, str, list[str], str],
    first: str,
    second: str,
    pushed_a: dict[str, str],
) -> tuple[dict, dict[str, str], dict]:
    deadline = time.monotonic() + RUN_SECONDS
    next_guard = 0.0
    runtime_seen = False
    while time.monotonic() < deadline:
        current_a = exact_build(*a_identity)
        if pending_digests(current_a, first, second) != pushed_a:
            fail("A lost its immutable pending receipts before B completed")
        current_b = exact_build(*b_identity)
        phase = current_b.get("status", {}).get("phase", "")
        save(run_dir, "b-latest-build.json", base.project_build(current_b))
        runner_name = current_b.get("status", {}).get("runnerPodName", "")
        if runner_name:
            observed, _ = base.capture_runner(
                run_dir,
                runner_name,
                b_identity[1],
                facts["images"]["runner"],
                facts["local_config_ids"]["runner"],
            )
            runtime_seen = runtime_seen or observed
        if phase == "Succeeded":
            if not runtime_seen:
                fail("B runner Pod runtime image identity was never proven")
            return current_b, verify_succeeded_receipt(current_b, first), current_a
        if phase in ("Failed", "Cancelled") or phase not in (
            "",
            "Queued",
            "Starting",
            "Running",
            "Verifying",
        ):
            fail(f"B did not succeed while only A's digest reads were blocked: {phase}")
        if time.monotonic() >= next_guard:
            guard_running(facts, {a_identity[0]: a_identity[1], b_identity[0]: b_identity[1]})
            next_guard = time.monotonic() + 5
        time.sleep(1)
    fail("B did not succeed within its bounded old-digest fault window")


def immutable_manifest_digest(repository: str, digest: str) -> str:
    if not re.fullmatch(r"kova-examples/digest-41-[0-9]{8}t[0-9]{6}z-[0-9a-f]{8}-a", repository):
        fail("immutable manifest read escaped the exact shared output repository")
    if SHA.fullmatch(digest) is None:
        fail("invalid immutable manifest digest")
    request = Request(
        f"http://{REGISTRY_HOST}/v2/{repository}/manifests/{digest}",
        headers={"Accept": base.MANIFEST_ACCEPT},
        method="GET",
    )
    try:
        with base.HTTP.open(request, timeout=10) as response:
            body = response.read(4 * 1024 * 1024 + 1)
            observed = response.headers.get("Docker-Content-Digest", "")
            if response.status != 200 or len(body) > 4 * 1024 * 1024 or observed != digest:
                fail("old output manifest is not exactly readable by pushed digest")
    except (HTTPError, URLError, OSError) as error:
        fail(f"old output manifest read is unavailable: {type(error).__name__}")
    if "sha256:" + hashlib.sha256(body).hexdigest() != digest:
        fail("old output manifest body hash differs from pushed receipt")
    return digest


def verify_succeeded_receipt(build: dict, first: str) -> dict[str, str]:
    status = build.get("status", {})
    if (
        status.get("phase") != "Succeeded"
        or not status.get("finishedAt")
        or status.get("verificationAttempts", 0) < 1
    ):
        fail("B did not finish as verified Succeeded after fault recovery")
    expected = {first: "oci", first.replace(":dev", ":dev_nydus_v3"): "nydus"}
    results = status.get("verificationResults", [])
    outputs = status.get("outputs", [])
    if len(results) != 2 or len(outputs) != 2:
        fail("B has other than two exact OCI/Nydus verified outputs")
    indexed = {item.get("image"): item for item in results}
    if len(indexed) != 2 or set(indexed) != set(expected):
        fail("B verification result identity drifted")
    digests: dict[str, str] = {}
    for output in outputs:
        image, digest = output.get("image", ""), output.get("manifestDigest", "")
        result = indexed.get(image, {})
        if (
            image not in expected
            or output.get("format") != expected[image]
            or output.get("platform") != "linux/amd64"
            or result.get("state") != "succeeded"
            or result.get("pushedDigest") != digest
            or SHA.fullmatch(digest) is None
        ):
            fail("B output is not bound to its own durable pushed digest")
        digests[image] = digest
    if len(digests) != 2:
        fail("B output images are not unique")
    return digests


def verify_public(
    run_dir: Path, label: str, port: int, token: str, build: dict, digests: dict[str, str]
) -> None:
    job_id = build["metadata"]["name"]
    cli_env = dict(os.environ, KOVA_SERVICE_TOKEN=token, KOVA_CTX="")
    job = json.loads(
        base.command(
            [
                str(ROOT / "bin/kova"),
                "--service-url",
                f"http://127.0.0.1:{port}",
                "job",
                "get",
                job_id,
            ],
            timeout=30,
            env=cli_env,
        )
    )
    results = json.loads(
        base.command(
            [
                str(ROOT / "bin/kova"),
                "--service-url",
                f"http://127.0.0.1:{port}",
                "job",
                "results",
                job_id,
            ],
            timeout=30,
            env=cli_env,
        )
    )
    save(run_dir, f"{label}-public-job.json", job)
    save(run_dir, f"{label}-public-results.json", results)
    if label == "a":
        base.verify_public_partial(job, results, build, digests)
        return
    spec = build["spec"]
    if (
        job.get("id") != job_id
        or job.get("status") != "succeeded"
        or results.get("id") != job_id
        or job.get("source_uri") != spec["source"]["uri"]
        or results.get("source_uri") != spec["source"]["uri"]
        or job.get("source_digest") != spec["source"]["digest"]
        or results.get("source_digest") != spec["source"]["digest"]
        or job.get("idempotency_key") != spec["idempotencyKey"]
        or results.get("idempotency_key") != spec["idempotencyKey"]
    ):
        fail("B public job/results identity differs from its durable KovaBuild")
    outputs = results.get("outputs", [])
    if not isinstance(outputs, list) or len(outputs) != 2:
        fail("B public result has other than two outputs")
    actual = {item.get("image"): item for item in outputs}
    if len(actual) != 2 or set(actual) != set(digests):
        fail("B public output image identity drifted")
    for image, digest in digests.items():
        item = actual[image]
        if (
            item.get("manifest_digest") != digest
            or item.get("immutable_ref") != image.rsplit(":", 1)[0] + "@" + digest
            or item.get("platform") != "linux/amd64"
            or item.get("format") != ("nydus" if image.endswith("_nydus_v3") else "oci")
        ):
            fail("B public output is not bound to its own immutable pushed digest")


def exact_cleanup(
    run_dir: Path,
    facts: dict,
    accepted: dict[str, tuple[str, str, str, str, list[str], str]],
) -> None:
    if base.check_kind_identity() != facts["kubeconfig_sha256"]:
        fail("Kind identity changed before exact cleanup")
    proxy_log = (run_dir / "kubectl-proxy.log").open("w+", encoding="utf-8")
    process = subprocess.Popen(
        [
            "kubectl",
            "--kubeconfig",
            str(base.KUBECONFIG),
            "proxy",
            "--address=127.0.0.1",
            "--port=0",
            "--accept-hosts=^127\\.0\\.0\\.1$",
        ],
        stdout=proxy_log,
        stderr=subprocess.STDOUT,
        start_new_session=True,
    )
    try:
        port = 0
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            if process.poll() is not None:
                fail("kubectl proxy exited before exact cleanup")
            content = (run_dir / "kubectl-proxy.log").read_text(encoding="utf-8")
            match = re.search(r"Starting to serve on 127\.0\.0\.1:(\d+)", content)
            if match:
                port = int(match.group(1))
                break
            time.sleep(0.5)
        if not port:
            fail("kubectl proxy did not provide a loopback port")
        for label, identity in accepted.items():
            job_id, uid, source_uri, source_digest, targets, key = identity
            current = exact_build(job_id, uid, source_uri, source_digest, targets, key)
            save(run_dir, f"{label}-before-delete.json", base.project_build(current))
            receipt = base.api_delete_with_uid(port, job_id, uid)
            save(run_dir, f"{label}-exact-delete.json", receipt)
            if receipt["http_status"] not in (200, 202):
                fail(f"{label} UID-preconditioned delete returned HTTP {receipt['http_status']}")
    finally:
        base.stop_process(process, proxy_log)
    deadline = time.monotonic() + 120
    while time.monotonic() < deadline:
        builds = base.kjson("get", "kovabuilds", "-A", "-o", "json").get("items", [])
        if not builds:
            break
        actual = {item["metadata"]["name"]: item["metadata"]["uid"] for item in builds}
        expected = {identity[0]: identity[1] for identity in accepted.values()}
        if not actual.items() <= expected.items():
            fail("unrelated or UID-drifted KovaBuild appeared during exact cleanup")
        time.sleep(2)
    else:
        fail("exact test KovaBuilds did not disappear after UID-preconditioned delete")
    deadline = time.monotonic() + 120
    while time.monotonic() < deadline:
        runners = base.kjson(
            "get", "pods", "-A", "-l", "app.kubernetes.io/name=kova-runner", "-o", "json"
        ).get("items", [])
        if not runners:
            break
        time.sleep(2)
    else:
        fail("runner Pod remains after exact KovaBuild cleanup")
    base.assert_empty()


def emergency_stop(
    run_dir: Path,
    facts: dict,
    attempts: dict[str, tuple[str, str, str, list[str], str]],
) -> None:
    if not attempts:
        return
    accepted: dict[str, tuple[str, str, str, str, list[str], str]] = {}
    for label, (job_id, source_uri, source_digest, targets, key) in attempts.items():
        raw = base.kctl(
            "-n", NAMESPACE, "get", "kovabuild", job_id, "--ignore-not-found", "-o", "json"
        )
        if not raw.strip():
            note(f"{label} exact attempted CR is absent")
            continue
        build = json.loads(raw)
        uid = build["metadata"]["uid"]
        base.check_expected_job(
            build, job_id, uid, source_uri, source_digest, targets, idempotency_key=key
        )
        accepted[label] = (job_id, uid, source_uri, source_digest, targets, key)
    if accepted:
        exact_cleanup(run_dir, facts, accepted)
        note("UID-preconditioned emergency stop completed for observed test CRs")


def run_acceptance(revision: str, facts: dict) -> None:
    if os.environ.get("DIGEST_COLLISION_ACK", "") != f"{CLUSTER}/{NAMESPACE}/{revision}":
        fail(f"run mode requires DIGEST_COLLISION_ACK={CLUSTER}/{NAMESPACE}/{revision}")
    service = base.check_deployment("kova-service", facts["images"]["controller"], "kova-service")
    token = base.read_static_token(service)
    inherited = os.environ.pop("KOVA_SERVICE_TOKEN", "")
    if inherited and inherited != token:
        fail("inherited CLI token differs from the exact test-owned Secret")
    base.PRIVATE_TOKEN = token
    headroom = base.host_guard()
    os.umask(0o077)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dt%H%M%Sz")  # noqa: UP017 (Python 3.10)
    run_id = f"digest-41-{stamp}-{secrets.token_hex(4)}"
    run_dir = ROOT / ".work" / "digest-collision" / run_id
    run_dir.mkdir(parents=True, mode=0o700, exist_ok=False)
    os.chmod(run_dir, 0o700)
    first, second = expected_targets(run_id)
    first_repo, failed_repo = f"kova-examples/{run_id}-a", f"kova-examples/{run_id}-z"
    save(
        run_dir,
        "run.json",
        {
            "run_id": run_id,
            "started_at": base.now(),
            "cluster": CLUSTER,
            "namespace": NAMESPACE,
            "revision": revision,
            "backend_registry": REGISTRY_HOST,
            "backend_container_id": facts["registry_id"],
            "proxy_host": PROXY_HOST,
            "targets": [first, second],
            "headroom": headroom,
            "kubeconfig_sha256": facts["kubeconfig_sha256"],
            "images": facts["images"],
        },
    )
    attempts: dict[str, tuple[str, str, str, list[str], str]] = {}
    accepted: dict[str, tuple[str, str, str, str, list[str], str]] = {}
    proxy_identity: ProxyIdentity | None = None
    service_forward: subprocess.Popen | None = None
    service_log = None
    copy_watch: dict = {}
    passed = False
    try:
        base.safe_snapshot(run_dir, "before")
        for repository, tag in (
            (first_repo, "dev"),
            (first_repo, "dev_nydus_v3"),
            (failed_repo, "dev"),
            (failed_repo, "dev_nydus_v3"),
            (f"kova-sources/{run_id}-a", "dev"),
            (f"kova-sources/{run_id}-b", "dev"),
        ):
            base.manifest_digest(repository, tag, absent=True)
        proxy_identity = create_proxy(run_dir, first_repo, failed_repo)
        archive_a = base.make_archive(run_dir, first, second)
        source_a, digest_a = push_source(run_dir, "a", archive_a, run_id)
        guard_running(facts, {})
        service_forward, service_port, service_log = base.start_port_forward(
            run_dir, "svc/kova-service", 8080
        )
        key_a = run_id + "-a"
        job_a = "idem-" + base.sha256(("kova:e2e\0" + key_a).encode())[:20]
        attempts["a"] = (job_a, source_a, digest_a, [first, second], key_a)
        job_a, uid_a = submit(
            run_dir, "a", service_port, token, key_a, source_a, digest_a, [first, second]
        )
        accepted["a"] = (job_a, uid_a, source_a, digest_a, [first, second], key_a)
        note(f"A submitted as {job_a}; receipts at {run_dir}")
        _, pushed_a = wait_pending(
            run_dir,
            "a",
            facts,
            accepted["a"],
            first,
            second,
            {job_a: uid_a},
            copy_watch=copy_watch,
        )
        watcher = copy_watch.get("thread")
        if watcher is not None:
            watcher.join(timeout=8)
        if (
            not copy_watch.get("runtime_seen")
            or copy_watch.get("result", {}).get("proven") is not True
        ):
            fail("A partial-output /missing runner proof was not captured before overwrite")
        save(run_dir, "a-pushed-digests.json", pushed_a)
        if base.manifest_digest(first_repo, "dev") != pushed_a[first]:
            fail("A OCI tag did not initially equal its durable pushed digest")
        if (
            base.manifest_digest(first_repo, "dev_nydus_v3")
            != pushed_a[first.replace(":dev", ":dev_nydus_v3")]
        ):
            fail("A Nydus tag did not initially equal its durable pushed digest")
        if len(set(pushed_a.values())) != 2:
            fail("A OCI and Nydus receipts unexpectedly share one manifest digest")
        for image, digest in pushed_a.items():
            probe_proxy_manifest(
                run_dir,
                proxy_identity,
                "503",
                "initial-a-nydus" if image.endswith("_nydus_v3") else "initial-a-oci",
                first_repo,
                digest,
                503,
            )
        only_a_mode = "only:" + ",".join(
            [pushed_a[first], pushed_a[first.replace(":dev", ":dev_nydus_v3")]]
        )
        set_proxy_mode(run_dir, proxy_identity, "503", only_a_mode)
        for image, digest in pushed_a.items():
            probe_proxy_manifest(
                run_dir,
                proxy_identity,
                only_a_mode,
                "narrow-a-nydus" if image.endswith("_nydus_v3") else "narrow-a-oci",
                first_repo,
                digest,
                503,
            )
        if pending_digests(exact_build(*accepted["a"]), first, second) != pushed_a:
            fail("A was no longer pending after narrowing the fault to only its digests")
        archive_b = make_b_archive(run_dir, first, run_id)
        source_b, digest_b = push_source(run_dir, "b", archive_b, run_id)
        guard_running(facts, {job_a: uid_a})
        key_b = run_id + "-b"
        job_b = "idem-" + base.sha256(("kova:e2e\0" + key_b).encode())[:20]
        attempts["b"] = (job_b, source_b, digest_b, [first], key_b)
        job_b, uid_b = submit(run_dir, "b", service_port, token, key_b, source_b, digest_b, [first])
        accepted["b"] = (job_b, uid_b, source_b, digest_b, [first], key_b)
        current_b, pushed_b, current_a = wait_b_succeeded(
            run_dir, facts, accepted["a"], accepted["b"], first, second, pushed_a
        )
        for image, digest in pushed_a.items():
            if pushed_b.get(image) == digest:
                fail("A and B produced the same manifest; overwrite was not demonstrated")
        if pending_digests(current_a, first, second) != pushed_a:
            fail("A lost its durable pushed digests before B tag overwrite")
        for image, digest in pushed_b.items():
            tag = "dev_nydus_v3" if image.endswith("_nydus_v3") else "dev"
            if base.manifest_digest(first_repo, tag) != digest:
                fail(f"B did not overwrite the shared {tag} tag with its own pushed digest")
            probe_proxy_manifest(
                run_dir,
                proxy_identity,
                only_a_mode,
                "overlap-b-nydus" if image.endswith("_nydus_v3") else "overlap-b-oci",
                first_repo,
                digest,
                200,
            )
        for image, digest in pushed_a.items():
            probe_proxy_manifest(
                run_dir,
                proxy_identity,
                only_a_mode,
                "overlap-a-nydus" if image.endswith("_nydus_v3") else "overlap-a-oci",
                first_repo,
                digest,
                503,
            )
        save(
            run_dir,
            "overlap-proof.json",
            {
                "at": base.now(),
                "a_phase": current_a["status"]["phase"],
                "b_phase": current_b["status"]["phase"],
                "a_pushed": pushed_a,
                "b_pushed": pushed_b,
                "shared_tag_head": {
                    "dev": base.manifest_digest(first_repo, "dev"),
                    "dev_nydus_v3": base.manifest_digest(first_repo, "dev_nydus_v3"),
                },
            },
        )
        for digest in pushed_a.values():
            immutable_manifest_digest(first_repo, digest)
        set_proxy_mode(run_dir, proxy_identity, only_a_mode, "healthy")
        for image, digest in pushed_a.items():
            probe_proxy_manifest(
                run_dir,
                proxy_identity,
                "healthy",
                "recovered-a-nydus" if image.endswith("_nydus_v3") else "recovered-a-oci",
                first_repo,
                digest,
                200,
            )
        deadline = time.monotonic() + 180
        next_guard = 0.0
        while time.monotonic() < deadline:
            current_a = exact_build(*accepted["a"])
            current_b = exact_build(*accepted["b"])
            phase_a, phase_b = current_a["status"].get("phase"), current_b["status"].get("phase")
            save(run_dir, "a-latest-build.json", base.project_build(current_a))
            save(run_dir, "b-latest-build.json", base.project_build(current_b))
            if phase_a == "Failed" and phase_b == "Succeeded":
                break
            if phase_a not in ("FailedVerifying", "Failed") or phase_b != "Succeeded":
                fail(f"post-fault KovaBuild phases are abnormal: A={phase_a}, B={phase_b}")
            if time.monotonic() >= next_guard:
                guard_running(facts, {job_a: uid_a, job_b: uid_b})
                next_guard = time.monotonic() + 5
            time.sleep(1)
        else:
            fail("A/B digest verification did not finish within the bounded recovery window")
        save(run_dir, "a-terminal-build.json", base.project_build(current_a))
        save(run_dir, "b-terminal-build.json", base.project_build(current_b))
        final_a = base.verify_failed_receipt(current_a, first, second)
        final_b = verify_succeeded_receipt(current_b, first)
        if final_a != pushed_a or final_b != pushed_b:
            fail("terminal output digest drifted from its build's own pending push receipt")
        for digest in (*final_a.values(), *final_b.values()):
            immutable_manifest_digest(first_repo, digest)
        for image, digest in final_b.items():
            tag = "dev_nydus_v3" if image.endswith("_nydus_v3") else "dev"
            if base.manifest_digest(first_repo, tag) != digest:
                fail("final shared tag no longer points to B's exact pushed manifest")
        base.manifest_digest(failed_repo, "dev", absent=True)
        base.manifest_digest(failed_repo, "dev_nydus_v3", absent=True)
        verify_public(run_dir, "a", service_port, token, current_a, final_a)
        verify_public(run_dir, "b", service_port, token, current_b, final_b)
        base.safe_snapshot(run_dir, "terminal-a", job_a, uid_a)
        base.safe_snapshot(run_dir, "terminal-b", job_b, uid_b)
        exact_cleanup(run_dir, facts, accepted)
        base.safe_snapshot(run_dir, "after-exact-cr-cleanup")
        passed = True
        save(
            run_dir,
            "result.json",
            {
                "status": "passed",
                "finished_at": base.now(),
                "a_job": job_a,
                "b_job": job_b,
                "a_pushed": final_a,
                "b_pushed": final_b,
                "overlap_proven": True,
                "partial_output_proven": True,
                "proxy_restored": "healthy",
                "old_digest_gc_tested": False,
            },
        )
        note(f"PASS: A/B overwrote both tags while A was pending; evidence at {run_dir}")
    finally:
        watcher = copy_watch.get("thread")
        if watcher is not None and watcher.is_alive():
            copy_watch["stop"].set()
            watcher.join(timeout=35)
        base.stop_process(service_forward, service_log)
        if not passed:
            base.safe_snapshot(run_dir, "failure")
            try:
                emergency_stop(run_dir, facts, attempts)
            except Exception as error:
                note(f"exact emergency stop unproven: {type(error).__name__}: {error}")
            note(f"FAILED: retain dedicated Kind, proxy, registry, tags and receipts at {run_dir}")
        else:
            note("only the two UID-preconditioned KovaBuilds were removed")
            note("dedicated Kind, healthy proxy, registry tags and all receipts remain for review")


def main() -> int:
    mode = os.environ.get("DIGEST_COLLISION_MODE", "check")
    if mode not in ("check", "run"):
        fail("DIGEST_COLLISION_MODE must be check or run")
    for interrupt in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP):
        signal.signal(interrupt, base.interrupted)
    revision = os.environ.get("DIGEST_COLLISION_EXPECTED_REVISION", "")
    facts = check_fixture(revision)
    note(f"read-only preflight passed: {CLUSTER}, candidate {revision}, 2/2 Ready nodes")
    if mode == "check":
        note("no writes; live mode needs DIGEST_COLLISION_MODE=run")
        note(f"DIGEST_COLLISION_ACK={CLUSTER}/{NAMESPACE}/{revision}")
        return 0
    run_acceptance(revision, facts)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (base.AcceptanceError, KeyError, ValueError, IndexError) as error:
        note(f"error: {error}")
        raise SystemExit(1) from None
