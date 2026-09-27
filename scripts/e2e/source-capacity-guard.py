#!/usr/bin/env python3
"""Fail-closed identity and exact-stop guard for the isolated #43 Kind test.

No cluster, registry, or image lifecycle is managed here. The only mutation is
an exact KovaBuild DELETE with an API-server-enforced UID precondition after a
run-mode failure. All receipts stay in the caller's private run directory.
"""

from __future__ import annotations

import hashlib
import json
import re
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import ProxyHandler, Request, build_opener

from oci_platform_identity import ImageIdentityError, local_platform_image_fact

ROOT = Path(__file__).resolve().parents[2]
CLUSTER = "kova-source-capacity"
KUBECONFIG = ROOT / ".kind" / f"{CLUSTER}.kubeconfig"
NAMESPACE = "kova"
IMAGE_REPOSITORY = "localhost:5002/kova"
HTTP = build_opener(ProxyHandler({}))
SHA = r"sha256:[0-9a-f]{64}"


class GuardError(RuntimeError):
    pass


def fail(message: str) -> None:
    raise GuardError(message)


def command(args: list[str], *, timeout: int = 30) -> str:
    try:
        result = subprocess.run(args, text=True, capture_output=True, timeout=timeout, check=False)
    except (OSError, subprocess.TimeoutExpired) as error:
        fail(f"{args[0]} outcome unknown: {type(error).__name__}")
    if result.returncode:
        fail(f"{args[0]} failed ({result.returncode}): {result.stderr[-300:].strip()}")
    return result.stdout


def kjson(*args: str) -> dict:
    value = json.loads(
        command(["kubectl", "--kubeconfig", str(KUBECONFIG), "--request-timeout=15s", *args])
    )
    if not isinstance(value, dict):
        fail("Kubernetes response is not an object")
    return value


def save(path: Path, value: object) -> None:
    path.write_text(json.dumps(value, sort_keys=True, indent=2) + "\n", encoding="utf-8")


def sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def candidate_revision() -> str:
    if command(["git", "-C", str(ROOT), "status", "--porcelain"]).strip():
        fail("candidate checkout is dirty")
    revision = command(["git", "-C", str(ROOT), "rev-parse", "--short=12", "HEAD"]).strip()
    if not re.fullmatch(r"[0-9a-f]{12}", revision):
        fail("candidate revision is not an exact 12-hex SHA")
    version = command([str(ROOT / "bin/kova"), "version"]).strip()
    if not re.search(rf"\(commit {revision},", version):
        fail("Linux CLI revision differs from the clean checkout")
    return revision


def image_for(role: str, revision: str) -> str:
    return f"{IMAGE_REPOSITORY}:{role}-{revision}"


def local_config_id(image: str, revision: str) -> str:
    try:
        return local_platform_image_fact(image, revision, command)["config_digest"]
    except ImageIdentityError as error:
        fail(str(error))


def check_deployment(name: str, container: str, image: str) -> tuple[int, dict]:
    deployment = kjson("-n", NAMESPACE, "get", "deployment", name, "-o", "json")
    spec, status = deployment["spec"], deployment["status"]
    replicas = spec.get("replicas", 1)
    if (
        not isinstance(replicas, int)
        or replicas < 1
        or any(status.get(key) != replicas for key in ("readyReplicas", "updatedReplicas"))
        or status.get("observedGeneration") != deployment["metadata"].get("generation")
    ):
        fail(f"deployment {name} is not fully Ready")
    matches = [
        item for item in spec["template"]["spec"]["containers"] if item.get("name") == container
    ]
    if len(matches) != 1 or matches[0].get("image") != image:
        fail(f"deployment {name} does not use the exact candidate image")
    return replicas, matches[0]


def runtime_image_fact(
    pod: dict, container: str, image: str, config_id: str, *, init: bool = False
) -> dict:
    name = pod["metadata"]["name"]
    node = pod.get("spec", {}).get("nodeName", "")
    if node not in (f"{CLUSTER}-control-plane", f"{CLUSTER}-worker"):
        fail(f"Pod {name} is not on the dedicated Kind nodes")
    spec_key = "initContainers" if init else "containers"
    status_key = "initContainerStatuses" if init else "containerStatuses"
    specs = [item for item in pod["spec"].get(spec_key, []) if item.get("name") == container]
    statuses = [item for item in pod["status"].get(status_key, []) if item.get("name") == container]
    if (
        len(specs) != 1
        or specs[0].get("image") != image
        or len(statuses) != 1
        or statuses[0].get("image") != image
        or not statuses[0].get("imageID")
    ):
        fail(f"Pod {name} lacks exact {container} image status")
    inspection = json.loads(
        command(["docker", "exec", node, "crictl", "inspecti", "-o", "json", image])
    )
    cri = inspection.get("status", {})
    digests = cri.get("repoDigests") or []
    image_id = statuses[0]["imageID"].removeprefix("docker-pullable://")
    if (
        cri.get("id") != config_id
        or cri.get("repoTags") != [image]
        or not isinstance(digests, list)
        or any(
            not isinstance(item, str) or not re.fullmatch(r"[^@]+@" + SHA, item) for item in digests
        )
        or (image_id != config_id and image_id not in digests)
    ):
        fail(f"Pod {name} {container} imageID differs from local/CRI candidate config")
    return {
        "pod": name,
        "uid": pod["metadata"]["uid"],
        "node": node,
        "image": image,
        "image_id": image_id,
        "cri_id": cri["id"],
        "cri_repo_digests": digests,
    }


def ready_role_pods(
    selector: str, replicas: int, container: str, image: str, config_id: str
) -> list[dict]:
    pods = kjson("-n", NAMESPACE, "get", "pods", "-l", selector, "-o", "json").get("items", [])
    if len(pods) != replicas:
        fail(f"role selector {selector} returned {len(pods)} Pods, expected {replicas}")
    facts = []
    for pod in pods:
        if pod["metadata"].get("deletionTimestamp") or not any(
            condition.get("type") == "Ready" and condition.get("status") == "True"
            for condition in pod.get("status", {}).get("conditions", [])
        ):
            fail(f"role Pod {pod['metadata']['name']} is not Ready")
        facts.append(runtime_image_fact(pod, container, image, config_id))
    return sorted(facts, key=lambda item: item["pod"])


def kind_facts() -> dict:
    if not KUBECONFIG.is_file() or KUBECONFIG.is_symlink():
        fail("dedicated kubeconfig is missing or is a symlink")
    if command(["kind", "get", "clusters"]).splitlines() != [CLUSTER]:
        fail("host no longer has only the exact dedicated Kind cluster")
    if (
        command(["kubectl", "--kubeconfig", str(KUBECONFIG), "config", "current-context"]).strip()
        != f"kind-{CLUSTER}"
    ):
        fail("dedicated kubeconfig context drifted")
    # The shell preflight compares decoded credentials/server to live Kind.
    # Requiring the same private file bytes on later checks prevents a switch.
    registry = json.loads(command(["docker", "inspect", "kind-registry", "--format", "{{json .}}"]))
    nodes = kjson("get", "nodes", "-o", "json")["items"]
    if len(nodes) != 2:
        fail("dedicated Kind node count drifted")
    return {
        "kubeconfig_sha256": sha(KUBECONFIG.read_bytes()),
        "registry_id": registry["Id"],
        "nodes": {item["metadata"]["name"]: item["metadata"]["uid"] for item in nodes},
    }


def image_preflight() -> dict:
    revision = candidate_revision()
    images = {role: image_for(role, revision) for role in ("controller", "runner", "worker")}
    config_ids = {role: local_config_id(image, revision) for role, image in images.items()}
    service_count, service = check_deployment("kova-service", "kova-service", images["controller"])
    worker_count, _ = check_deployment("kova", "buildkitd", images["worker"])
    if f"--runner-image={images['runner']}" not in service.get("args", []):
        fail("Service runner image argument differs from the exact candidate")
    return {
        "revision": revision,
        "images": images,
        "config_ids": config_ids,
        "kind": kind_facts(),
        "runtime": {
            "service": ready_role_pods(
                "app.kubernetes.io/instance=kova,app.kubernetes.io/component=service",
                service_count,
                "kova-service",
                images["controller"],
                config_ids["controller"],
            ),
            "worker": ready_role_pods(
                "app.kubernetes.io/instance=kova,app.kubernetes.io/name=kova",
                worker_count,
                "buildkitd",
                images["worker"],
                config_ids["worker"],
            ),
        },
    }


def load_contract(run_dir: Path) -> dict:
    contract = json.loads((run_dir / "source-contract.json").read_text(encoding="utf-8"))
    run_id = contract.get("run_id", "")
    job_id = contract.get("expected_job_id", "")
    if not re.fullmatch(r"source-capacity-[0-9]{8}t[0-9]{6}z-[0-9a-f]{8}", run_id):
        fail("run contract has an unsafe run ID")
    expected = "idem-" + sha(b"kova:e2e\0" + run_id.encode())[:20]
    if job_id != expected or not re.fullmatch(r"idem-[0-9a-f]{20}", job_id):
        fail("run contract job ID differs from the deterministic idempotency ID")
    if (
        contract.get("source_uri")
        != f"oci://kind-registry:5000/kova-sources/source-capacity@{contract.get('source_manifest_digest')}"
        or not re.fullmatch(SHA, contract.get("source_manifest_digest", ""))
        or not re.fullmatch(SHA, contract.get("source_digest", ""))
        or contract.get("target") != f"kind-registry:5000/kova-examples/source-capacity:{run_id}"
    ):
        fail("source/target contract is not the exact immutable run")
    run = json.loads((run_dir / "run.json").read_text(encoding="utf-8"))
    source_receipt = json.loads((run_dir / "source-receipt.json").read_text(encoding="utf-8"))
    if (
        run.get("run_id") != run_id
        or run.get("cluster") != CLUSTER
        or run.get("target") != contract["target"]
        or run.get("source_repository") != f"localhost:5002/kova-sources/source-capacity:{run_id}"
        or (run_dir / "expected-job-id.txt").read_text(encoding="utf-8").strip() != job_id
        or source_receipt.get("uri")
        != f"oci://localhost:5002/kova-sources/source-capacity@{contract['source_manifest_digest']}"
        or source_receipt.get("digest") != contract["source_digest"]
    ):
        fail("run record, expected ID, or source push receipt differs from the exact contract")
    return contract


def validate_build(build: dict, contract: dict) -> str:
    metadata, spec = build.get("metadata", {}), build.get("spec", {})
    job_id = contract["expected_job_id"]
    uid = metadata.get("uid", "")
    if (
        metadata.get("name") != job_id
        or metadata.get("namespace") != NAMESPACE
        or not re.fullmatch(r"[0-9a-f]{8}-(?:[0-9a-f]{4}-){3}[0-9a-f]{12}", uid)
        or metadata.get("labels", {}).get("app.kubernetes.io/name") != "kova-build"
        or metadata.get("labels", {}).get("kova.cofy.dev/requester-id") != sha(b"kova:e2e")[:32]
        or spec.get("requester", {}).get("username") != "kova:e2e"
        or spec.get("requester", {}).get("uid", "") != ""
        or spec.get("source")
        != {"uri": contract["source_uri"], "digest": contract["source_digest"]}
        or spec.get("idempotencyKey") != contract["run_id"]
        or spec.get("targets") != [{"target": contract["target"], "platform": "linux/amd64"}]
    ):
        fail("KovaBuild identity, requester, source, or target drifted; no deletion attempted")
    options = spec.get("build", {})
    if (
        options.get("format") != "oci"
        or options.get("concurrency") != 1
        or options.get("timeout") != 900
        or options.get("vars") != ["KOVA_MARKER=capacity"]
        or options.get("failFast", False) is not False
        or options.get("verbose", False) is not False
        or options.get("oomCooldown", "") != ""
    ):
        fail("KovaBuild execution options drifted; no deletion attempted")
    return uid


def check_runner_ownership(pod: dict, contract: dict, uid: str) -> None:
    if (
        pod["metadata"].get("name") != f"kova-job-{contract['expected_job_id']}"
        or pod["metadata"].get("namespace") != NAMESPACE
    ):
        fail("foreign runner Pod is present; no deletion attempted")
    owners = pod["metadata"].get("ownerReferences", [])
    if len(owners) != 1 or not all(
        owner.get("kind") == "KovaBuild"
        and owner.get("name") == contract["expected_job_id"]
        and owner.get("uid") == uid
        and owner.get("controller") is True
        for owner in owners
    ):
        fail("runner Pod is not owned by the exact test KovaBuild UID")


def list_owned_state(contract: dict, uid: str) -> None:
    builds = kjson("get", "kovabuilds", "-A", "-o", "json").get("items", [])
    if (
        len(builds) != 1
        or builds[0]["metadata"].get("name") != contract["expected_job_id"]
        or builds[0]["metadata"].get("uid") != uid
        or builds[0]["metadata"].get("namespace") != NAMESPACE
    ):
        fail("foreign or changed KovaBuild is present; no deletion attempted")
    runners = kjson(
        "get", "pods", "-A", "-l", "app.kubernetes.io/name=kova-runner", "-o", "json"
    ).get("items", [])
    if len(runners) > 1:
        fail("foreign runner Pod is present; no deletion attempted")
    if runners:
        check_runner_ownership(runners[0], contract, uid)


def runner_preflight(run_dir: Path) -> dict:
    contract = load_contract(run_dir)
    baseline = json.loads((run_dir / "candidate-images.json").read_text(encoding="utf-8"))
    if image_preflight() != baseline:
        fail("candidate role image or Kind identity changed before runner verification")
    deadline = time.monotonic() + 180
    while time.monotonic() < deadline:
        build = kjson(
            "-n", NAMESPACE, "get", "kovabuild", contract["expected_job_id"], "-o", "json"
        )
        uid = validate_build(build, contract)
        list_owned_state(contract, uid)
        pods = kjson(
            "-n", NAMESPACE, "get", "pods", "-l", "app.kubernetes.io/name=kova-runner", "-o", "json"
        ).get("items", [])
        if pods:
            pod = pods[0]
            check_runner_ownership(pod, contract, uid)
            image = baseline["images"]["runner"]
            config_id = baseline["config_ids"]["runner"]
            init_ready = any(
                item.get("name") == "source-fetch" and item.get("imageID")
                for item in pod.get("status", {}).get("initContainerStatuses", [])
            )
            main_ready = any(
                item.get("name") == "runner" and item.get("imageID")
                for item in pod.get("status", {}).get("containerStatuses", [])
            )
            if init_ready and main_ready:
                return {
                    "build_uid": uid,
                    "runner_uid": pod["metadata"]["uid"],
                    "source_fetch": runtime_image_fact(
                        pod, "source-fetch", image, config_id, init=True
                    ),
                    "runner": runtime_image_fact(pod, "runner", image, config_id),
                }
        time.sleep(2)
    fail("exact runner Pod did not expose both candidate CRI image identities within 180s")


def delete_options(uid: str) -> bytes:
    return json.dumps(
        {
            "apiVersion": "meta.k8s.io/v1",
            "kind": "DeleteOptions",
            "preconditions": {"uid": uid},
            "propagationPolicy": "Background",
        }
    ).encode()


def exact_delete(run_dir: Path, job_id: str, uid: str) -> dict:
    proxy_log = run_dir / "kubectl-proxy.log"
    process = None
    try:
        with proxy_log.open("w", encoding="utf-8") as output:
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
                stdout=output,
                stderr=subprocess.STDOUT,
            )
        port = None
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            if process.poll() is not None:
                fail("kubectl proxy exited before exact deletion")
            match = re.search(
                r"Starting to serve on 127\.0\.0\.1:(\d+)", proxy_log.read_text(encoding="utf-8")
            )
            if match:
                port = int(match.group(1))
                break
            time.sleep(0.5)
        if not port:
            fail("kubectl proxy did not choose a loopback port")
        request = Request(
            f"http://127.0.0.1:{port}/apis/kova.cofy.dev/v1alpha1/namespaces/{NAMESPACE}/kovabuilds/{job_id}",
            data=delete_options(uid),
            headers={"Content-Type": "application/json"},
            method="DELETE",
        )
        try:
            with HTTP.open(request, timeout=20) as response:
                code, body = response.status, response.read(1024 * 1024 + 1)
        except HTTPError as error:
            code, body = error.code, error.read(1024 * 1024 + 1)
        except (OSError, URLError) as error:
            fail(f"exact UID deletion outcome unknown: {type(error).__name__}")
        if len(body) > 1024 * 1024:
            fail("exact UID deletion response is oversized")
        receipt = {
            "job_id": job_id,
            "uid_precondition": uid,
            "http_status": code,
            "body_sha256": sha(body),
            "at": datetime.now(timezone.utc).isoformat(),  # noqa: UP017 (Python 3.10)
        }
        save(run_dir / "exact-cr-delete.json", receipt)
        if code not in (200, 202):
            fail(f"UID-precondition deletion returned HTTP {code}")
        return receipt
    finally:
        if process is not None:
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)


def admission_empty() -> bool:
    active = kjson("-n", NAMESPACE, "get", "configmap", "kova-service-admission", "-o", "json")
    queue = kjson("-n", NAMESPACE, "get", "configmap", "kova-service-queue-admission", "-o", "json")
    return (
        json.loads(active["data"]["reservations.json"]).get("active") == {}
        and json.loads(queue["data"]["queue.json"]).get("intents") == {}
    )


def exact_stop(run_dir: Path) -> dict:
    contract = load_contract(run_dir)
    save(
        run_dir / "stop-attempt.json",
        {
            "job_id": contract["expected_job_id"],
            "run_id": contract["run_id"],
            "at": datetime.now(timezone.utc).isoformat(),  # noqa: UP017 (Python 3.10)
        },
    )
    baseline = json.loads((run_dir / "candidate-images.json").read_text(encoding="utf-8"))
    if kind_facts() != baseline["kind"]:
        fail("Kind kubeconfig/node/registry identity changed; no deletion attempted")
    job_id = contract["expected_job_id"]
    # A timed-out Service POST can have an unknown outcome. Observe its exact
    # deterministic ID for a bounded window; absence is not proof of no write.
    deadline = time.monotonic() + 60
    build = None
    while time.monotonic() < deadline:
        objects = kjson("get", "kovabuilds", "-A", "-o", "json").get("items", [])
        if any(item["metadata"].get("name") != job_id for item in objects):
            fail("foreign KovaBuild appeared; no deletion attempted")
        if objects:
            if len(objects) != 1:
                fail("multiple KovaBuilds have the expected ID; no deletion attempted")
            build = objects[0]
            break
        time.sleep(3)
    if build is None:
        runners = kjson(
            "get", "pods", "-A", "-l", "app.kubernetes.io/name=kova-runner", "-o", "json"
        ).get("items", [])
        save(
            run_dir / "stop-observed-identities.json",
            {
                "builds": [],
                "runners": [
                    {
                        "name": item["metadata"].get("name"),
                        "namespace": item["metadata"].get("namespace"),
                        "uid": item["metadata"].get("uid"),
                        "owner_uids": [
                            ref.get("uid") for ref in item["metadata"].get("ownerReferences", [])
                        ],
                    }
                    for item in runners
                ],
            },
        )
        result = {
            "status": "submit_outcome_uncertain",
            "job_id": job_id,
            "observed_seconds": 60,
            "note": (
                "No exact CR observed; this is not proof that the timed-out submit "
                "did not create one."
            ),
        }
        save(run_dir / "stop-outcome.json", result)
        fail("submit outcome remains uncertain; inspect exact ID before any new run")
    uid = validate_build(build, contract)
    list_owned_state(contract, uid)
    save(
        run_dir / "pre-stop-build.json",
        {
            "name": job_id,
            "uid": uid,
            "source": build["spec"]["source"],
            "targets": build["spec"]["targets"],
            "phase": build.get("status", {}).get("phase"),
        },
    )
    exact_delete(run_dir, job_id, uid)
    deadline = time.monotonic() + 120
    while time.monotonic() < deadline:
        builds = kjson("get", "kovabuilds", "-A", "-o", "json").get("items", [])
        runners = kjson(
            "get", "pods", "-A", "-l", "app.kubernetes.io/name=kova-runner", "-o", "json"
        ).get("items", [])
        if not builds and not runners and admission_empty():
            result = {
                "status": "exact_uid_stopped",
                "job_id": job_id,
                "uid": uid,
                "at": datetime.now(timezone.utc).isoformat(),  # noqa: UP017 (Python 3.10)
            }
            save(run_dir / "stop-outcome.json", result)
            return result
        if any(
            item["metadata"].get("name") != job_id
            or item["metadata"].get("uid") != uid
            or item["metadata"].get("namespace") != NAMESPACE
            for item in builds
        ):
            fail("foreign KovaBuild appeared during exact stop")
        for pod in runners:
            check_runner_ownership(pod, contract, uid)
        time.sleep(2)
    fail("exact CR/runner/admission cleanup did not converge within 120s")


def main() -> int:
    if len(sys.argv) < 2 or sys.argv[1] not in ("check", "runner", "stop"):
        fail(
            "usage: source-capacity-guard.py check [baseline.json] | runner RUN_DIR | stop RUN_DIR"
        )
    mode = sys.argv[1]
    if mode == "check":
        if len(sys.argv) not in (2, 3):
            fail("check accepts only an optional baseline JSON path")
        current = image_preflight()
        if len(sys.argv) == 3 and current != json.loads(
            Path(sys.argv[2]).read_text(encoding="utf-8")
        ):
            fail("candidate image, role Pod, CRI, or Kind identity changed")
        print(json.dumps(current, sort_keys=True))
    elif mode == "runner":
        if len(sys.argv) != 3:
            fail("runner requires exactly one run directory")
        run_dir = Path(sys.argv[2])
        result = runner_preflight(run_dir)
        save(run_dir / "runner-image-identity.json", result)
        print(json.dumps(result, sort_keys=True))
    else:
        if len(sys.argv) != 3:
            fail("stop requires exactly one run directory")
        print(json.dumps(exact_stop(Path(sys.argv[2])), sort_keys=True))
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (GuardError, KeyError, IndexError, ValueError, OSError, json.JSONDecodeError) as error:
        print(f"source-capacity-guard: {type(error).__name__}: {error}", file=sys.stderr)
        sys.exit(1)
