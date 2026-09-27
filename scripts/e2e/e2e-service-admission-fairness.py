#!/usr/bin/env python3
"""Two authenticated requester fairness and leader-handoff acceptance on Kind.

The default check is read-only. Run and recover require the exact disposable
fixture and an explicit acknowledgement. No mode edits an admission ledger,
deploys Kova, builds an image, or contacts a registry.
"""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
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

import admission_failover_io as failover_io
from oci_platform_identity import ImageIdentityError, local_platform_image_fact

ROOT = Path(__file__).resolve().parents[2]
CLUSTER = "kova-admission-fairness"
NAMESPACE = "kova"
RELEASE = "kova"
KUBECONFIG = ROOT / ".kind" / f"{CLUSTER}.kubeconfig"
EVIDENCE_ROOT = Path("/data/forge-artifacts/kova-admission-fairness")
LEASE = "kova-service.kova.cofy.dev"
ACK = f"{CLUSTER}/{NAMESPACE}/{RELEASE}-service"
SA = {"a": "kova-fair-alice", "b": "kova-fair-bob"}
PORT = {"a": 18110, "b": 18111}
SOURCE_DIGEST = "sha256:" + "a" * 64
RUN_ID = re.compile(r"fairness-[0-9]{8}t[0-9]{6}z-[0-9a-f]{8}\Z")
UID = failover_io.UID
MAX_RUN_SECONDS = 1200
SHA256 = re.compile(r"sha256:[0-9a-f]{64}\Z")


class SafetyError(RuntimeError):
    """An identity or bounded-progress refusal, without credentials or response bodies."""


def require(condition: bool, message: str) -> None:
    if not condition:
        raise SafetyError(message)


def now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")  # noqa: UP017 (Python 3.10)


def command(argv: list[str], *, timeout: int = 20) -> str:
    try:
        result = subprocess.run(argv, capture_output=True, text=True, check=False, timeout=timeout)
    except (OSError, subprocess.TimeoutExpired) as error:
        raise SafetyError(f"{argv[0]} failed to complete: {type(error).__name__}") from None
    # stderr may contain a bearer or an untrusted API response; do not print it.
    require(result.returncode == 0, f"{argv[0]} returned status {result.returncode}")
    return result.stdout


def kctl(*args: str, timeout: int = 20) -> str:
    return command(
        ["kubectl", "--kubeconfig", str(KUBECONFIG), "--request-timeout=15s", *args],
        timeout=timeout,
    )


def kjson(*args: str) -> dict:
    try:
        result = json.loads(kctl(*args))
    except (TypeError, ValueError) as error:
        raise SafetyError(f"Kubernetes returned malformed JSON: {type(error).__name__}") from None
    require(isinstance(result, dict), "Kubernetes returned a non-object JSON response")
    return result


def stable_hash(value: object) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def sha256(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


def save(path: Path, value: object) -> None:
    payload = (json.dumps(value, sort_keys=True, indent=2) + "\n").encode()
    temp = path.with_name(path.name + ".tmp")
    with open(temp, "wb", opener=lambda name, flags: os.open(name, flags, 0o600)) as stream:
        stream.write(payload)
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temp, path)


def principal(which: str) -> str:
    return f"system:serviceaccount:{NAMESPACE}:{SA[which]}"


def build_id(username: str, key: str) -> str:
    digest = hashlib.sha256((username + "\x00" + key).encode()).hexdigest()
    return "idem-" + digest[:20]


def planned_cases(run_id: str, platform: str) -> dict[str, dict]:
    require(RUN_ID.fullmatch(run_id) is not None, "run ID is not bounded")
    cases = {}
    for label in ("a1", "a2", "a3", "a4", "b1", "b2"):
        which = label[0]
        username = principal(which)
        key = f"{run_id}-{label}"
        cases[label] = {
            "label": label,
            "requester": username,
            "key": key,
            "id": build_id(username, key),
            "source_uri": f"oci://kind-registry:5000/kova-sources/{run_id}@{SOURCE_DIGEST}",
            "source_digest": SOURCE_DIGEST,
            "target": f"kind-registry:5000/kova-admission/{run_id}:{label}",
            "platform": platform,
        }
    require(len({item["id"] for item in cases.values()}) == len(cases), "candidate IDs collide")
    return cases


def one_ready_service_pod(pod: dict, image: str) -> bool:
    containers = [
        s
        for s in pod.get("status", {}).get("containerStatuses", [])
        if s.get("name") == "kova-service"
    ]
    return (
        pod.get("metadata", {}).get("deletionTimestamp") is None
        and len(containers) == 1
        and containers[0].get("imageID") not in (None, "")
        and containers[0].get("image") == image
        and any(
            c.get("type") == "Ready" and c.get("status") == "True"
            for c in pod.get("status", {}).get("conditions", [])
        )
    )


def verify_rbac() -> dict[str, str]:
    identities = {}
    submitter_role = kjson("-n", NAMESPACE, "get", "role", "kova-service-submitter", "-o", "json")
    require(
        submitter_role.get("rules")
        == [{"apiGroups": ["kova.cofy.dev"], "resources": ["servicebuilds"], "verbs": ["create"]}],
        "fixture submitter Role does not grant only virtual Service create",
    )
    for which, name in SA.items():
        account = kjson("-n", NAMESPACE, "get", "serviceaccount", name, "-o", "json")
        binding = kjson("-n", NAMESPACE, "get", "rolebinding", f"{name}-submitter", "-o", "json")
        require(
            account.get("metadata", {}).get("name") == name
            and account.get("metadata", {}).get("namespace") == NAMESPACE
            and account.get("metadata", {}).get("labels", {}).get("kova.cofy.dev/e2e")
            == "admission-fairness"
            and account.get("automountServiceAccountToken") is False,
            f"test ServiceAccount {name} differs from fixture",
        )
        uid = account.get("metadata", {}).get("uid", "")
        require(
            isinstance(uid, str) and UID.fullmatch(uid) is not None,
            f"test ServiceAccount {name} UID is invalid",
        )
        subjects = binding.get("subjects")
        require(
            binding.get("metadata", {}).get("namespace") == NAMESPACE
            and binding.get("metadata", {}).get("name") == f"{name}-submitter"
            and binding.get("metadata", {}).get("labels", {}).get("kova.cofy.dev/e2e")
            == "admission-fairness"
            and isinstance(subjects, list)
            and len(subjects) == 1
            and subjects[0].get("kind") == "ServiceAccount"
            and subjects[0].get("name") == name
            and subjects[0].get("namespace") == NAMESPACE
            and subjects[0].get("apiGroup") in (None, "")
            and binding.get("roleRef")
            == {
                "apiGroup": "rbac.authorization.k8s.io",
                "kind": "Role",
                "name": "kova-service-submitter",
            },
            f"test submitter RoleBinding for {name} differs from fixture",
        )
        identities[which] = uid
    service = kjson("-n", NAMESPACE, "get", "serviceaccount", "kova-service", "-o", "json")
    service_uid = service.get("metadata", {}).get("uid", "")
    require(
        service.get("metadata", {}).get("namespace") == NAMESPACE
        and service.get("metadata", {}).get("name") == "kova-service"
        and service.get("automountServiceAccountToken") is True
        and isinstance(service_uid, str)
        and UID.fullmatch(service_uid) is not None,
        "Service TokenReview ServiceAccount differs from the fixture",
    )
    auth_role = kjson("get", "clusterrole", "kova-service-auth-review", "-o", "json")
    require(
        auth_role.get("rules")
        == [
            {
                "apiGroups": ["authentication.k8s.io"],
                "resources": ["tokenreviews"],
                "verbs": ["create"],
            },
            {
                "apiGroups": ["authorization.k8s.io"],
                "resources": ["subjectaccessreviews"],
                "verbs": ["create"],
            },
        ],
        "Service auth-review ClusterRole differs from TokenReview/SAR fixture",
    )
    auth_binding = kjson("get", "clusterrolebinding", "kova-service-auth-review", "-o", "json")
    require(
        auth_binding.get("subjects")
        == [{"kind": "ServiceAccount", "name": "kova-service", "namespace": NAMESPACE}]
        and auth_binding.get("roleRef")
        == {
            "apiGroup": "rbac.authorization.k8s.io",
            "kind": "ClusterRole",
            "name": "kova-service-auth-review",
        },
        "Service auth-review ClusterRoleBinding differs from the fixture",
    )
    identities["service"] = service_uid
    return identities


def verify_nodes(nodes: dict) -> list[dict]:
    items = nodes.get("items", [])
    require(
        {node.get("metadata", {}).get("name") for node in items}
        == {f"{CLUSTER}-control-plane", f"{CLUSTER}-worker"},
        "the fixture is not the exact two-node Kind cluster",
    )
    facts = []
    for node in items:
        conditions = {
            c.get("type"): c.get("status") for c in node.get("status", {}).get("conditions", [])
        }
        require(
            conditions.get("Ready") == "True"
            and all(
                conditions.get(key) == "False"
                for key in ("DiskPressure", "MemoryPressure", "PIDPressure")
            )
            and node.get("status", {}).get("nodeInfo", {}).get("architecture") == "amd64"
            and node.get("metadata", {}).get("labels", {}).get("never") != "true",
            "Kind node is unready, pressured, non-amd64, or schedulable for a blocker",
        )
        facts.append(
            {
                "name": node["metadata"]["name"],
                "uid": node["metadata"]["uid"],
                "allocatable": node["status"].get("allocatable", {}),
            }
        )
    return sorted(facts, key=lambda fact: fact["name"])


def ledger_data(obj: dict, field: str) -> dict:
    try:
        data = json.loads(obj["data"][field])
    except (KeyError, TypeError, ValueError) as error:
        raise SafetyError(f"admission ledger is malformed: {type(error).__name__}") from None
    require(isinstance(data, dict), "admission ledger state is not an object")
    return data


def verify_service_image_binding(pods: list[dict], image: str, config_digest: str) -> list[dict]:
    """Bind both Pod statuses to the reviewed image's Kind CRI config digest."""
    require(SHA256.fullmatch(config_digest) is not None, "candidate config digest is invalid")
    bindings = []
    for pod in pods:
        name = pod.get("metadata", {}).get("name", "")
        node = pod.get("spec", {}).get("nodeName", "")
        require(
            node in (f"{CLUSTER}-control-plane", f"{CLUSTER}-worker"),
            f"Service Pod {name} is not on an exact Kind node",
        )
        specs = [
            item
            for item in pod.get("spec", {}).get("containers", [])
            if item.get("name") == "kova-service"
        ]
        statuses = [
            item
            for item in pod.get("status", {}).get("containerStatuses", [])
            if item.get("name") == "kova-service"
        ]
        require(
            len(specs) == len(statuses) == 1
            and specs[0].get("image") == image
            and specs[0].get("imagePullPolicy") == "Never"
            and statuses[0].get("image") == image,
            f"Service Pod {name} image spec/status differs from the candidate",
        )
        try:
            inspection = json.loads(
                command(
                    ["docker", "exec", node, "crictl", "inspecti", "-o", "json", image],
                    timeout=30,
                )
            )
            cri = inspection["status"]
            repo_digests = cri.get("repoDigests") or []
            image_id = statuses[0]["imageID"].removeprefix("docker-pullable://")
        except (KeyError, TypeError, ValueError, AttributeError) as error:
            raise SafetyError(
                f"Service Pod {name} has malformed image identity: {type(error).__name__}"
            ) from None
        require(
            cri.get("id") == config_digest
            and cri.get("repoTags") == [image]
            and isinstance(repo_digests, list)
            and all(
                isinstance(item, str)
                and re.fullmatch(r"[^@]+@sha256:[0-9a-f]{64}", item) is not None
                for item in repo_digests
            )
            and (image_id == config_digest or image_id in repo_digests),
            f"Service Pod {name} image differs from the reviewed local/CRI candidate",
        )
        bindings.append(
            {
                "pod": name,
                "uid": pod["metadata"]["uid"],
                "node": node,
                "pod_image_id": image_id,
                "cri_config_digest": cri["id"],
                "cri_repo_digests": repo_digests,
            }
        )
    return sorted(bindings, key=lambda binding: binding["pod"])


def fixture_identity(*, empty: bool, expected_config_digest: str | None = None) -> dict:
    require(
        KUBECONFIG.is_file() and not KUBECONFIG.is_symlink(),
        "dedicated kubeconfig is absent or a symlink",
    )
    require(
        command(["kind", "get", "clusters"]).splitlines() == [CLUSTER],
        "this must be the sole Kind cluster",
    )
    kube_sha = hashlib.sha256(KUBECONFIG.read_bytes()).hexdigest()
    failover_io.assert_exact_kind(KUBECONFIG, CLUSTER, kube_sha)
    require(
        kctl("config", "current-context").strip() == f"kind-{CLUSTER}",
        "wrong Kind kubeconfig context",
    )
    head = command(["git", "-C", str(ROOT), "rev-parse", "HEAD"]).strip()
    require(re.fullmatch(r"[0-9a-f]{40}", head) is not None, "candidate commit is unavailable")
    require(
        command(["git", "-C", str(ROOT), "status", "--porcelain"]).strip() == "",
        "candidate checkout is dirty",
    )
    nodes = verify_nodes(kjson("get", "nodes", "-o", "json"))
    deployment = kjson("-n", NAMESPACE, "get", "deployment", f"{RELEASE}-service", "-o", "json")
    spec = deployment.get("spec", {})
    status = deployment.get("status", {})
    require(
        spec.get("replicas") == 2
        and status.get("replicas")
        == status.get("readyReplicas")
        == status.get("updatedReplicas")
        == 2
        and status.get("observedGeneration") == deployment.get("metadata", {}).get("generation"),
        "Service Deployment does not have two converged Ready replicas",
    )
    containers = [
        c
        for c in spec.get("template", {}).get("spec", {}).get("containers", [])
        if c.get("name") == "kova-service"
    ]
    require(len(containers) == 1, "Service Deployment has an unexpected container layout")
    container = containers[0]
    pod_template_spec = spec.get("template", {}).get("spec", {})
    require(
        pod_template_spec.get("serviceAccountName") == "kova-service"
        and pod_template_spec.get("automountServiceAccountToken") is True,
        "Service Pods do not use the reviewed TokenReview ServiceAccount",
    )
    expected_image = f"localhost:5002/kova:controller-{head[:12]}"
    require(
        container.get("image") == expected_image and container.get("imagePullPolicy") == "Never",
        "Service image does not match this committed local candidate",
    )
    args = set(container.get("args", []))
    required = {
        "--namespace=kova",
        "--max-active-jobs=1",
        "--max-active-jobs-per-requester=1",
        "--worker-slots=1",
        "--max-queued-jobs=3",
        "--max-queued-jobs-per-requester=2",
        "--runner-node-selector=never=true",
        "--auth-mode=tokenreview",
        "--leader-elect=true",
        "--leader-election-namespace=kova",
        "--wait=2h",
        "--max-build-duration=2h",
        f"--runner-image=localhost:5002/kova:runner-{head[:12]}",
        "--runner-image-pull-policy=Never",
    }
    require(required.issubset(args), "Service args differ from the exact fairness fixture")
    platforms = [
        arg.split("=", 2)[1] for arg in args if arg.startswith("--buildkit-platform-addr=")
    ]
    require(
        platforms == ["linux/amd64"],
        "fixture must have a single Linux/amd64 platform",
    )
    pods = kjson(
        "-n",
        NAMESPACE,
        "get",
        "pods",
        "-l",
        "app.kubernetes.io/instance=kova,app.kubernetes.io/component=service",
        "-o",
        "json",
    ).get("items", [])
    require(
        len(pods) == 2 and all(one_ready_service_pod(pod, expected_image) for pod in pods),
        "Service Pods are not two Ready same-version instances",
    )
    image_ids = {
        next(
            s["imageID"] for s in pod["status"]["containerStatuses"] if s["name"] == "kova-service"
        )
        for pod in pods
    }
    require(len(image_ids) == 1, "Service Pod image digests differ")
    local_image = None
    if expected_config_digest is None:
        try:
            local_image = local_platform_image_fact(expected_image, head[:12], command)
        except ImageIdentityError as error:
            raise SafetyError(f"local candidate image differs: {error}") from None
        expected_config_digest = local_image["config_digest"]
    image_bindings = verify_service_image_binding(pods, expected_image, expected_config_digest)
    pod_names = sorted(pod["metadata"]["name"] for pod in pods)
    for pod in pods:
        owners = [
            owner
            for owner in pod["metadata"].get("ownerReferences", [])
            if owner.get("controller") is True
        ]
        require(
            len(owners) == 1 and owners[0].get("kind") == "ReplicaSet",
            "Service Pod lacks exact ReplicaSet ownership",
        )
        replica_set = kjson("-n", NAMESPACE, "get", "replicaset", owners[0]["name"], "-o", "json")
        require(
            replica_set["metadata"]["uid"] == owners[0]["uid"], "Service ReplicaSet UID changed"
        )
        require(
            any(
                ref.get("kind") == "Deployment"
                and ref.get("uid") == deployment["metadata"]["uid"]
                and ref.get("controller") is True
                for ref in replica_set["metadata"].get("ownerReferences", [])
            ),
            "Service ReplicaSet is not owned by the fixture Deployment",
        )
    lease = kjson("-n", NAMESPACE, "get", "lease", LEASE, "-o", "json")
    holder = lease.get("spec", {}).get("holderIdentity", "")
    require(
        any(holder.startswith(name + "_") for name in pod_names),
        "Lease holder is not a Ready fixture Service Pod",
    )
    active = kjson("-n", NAMESPACE, "get", "configmap", "kova-service-admission", "-o", "json")
    queue = kjson("-n", NAMESPACE, "get", "configmap", "kova-service-queue-admission", "-o", "json")
    active_state = ledger_data(active, "reservations.json")
    queue_state = ledger_data(queue, "queue.json")
    require(
        (
            active_state.get("version"),
            active_state.get("maxJobs"),
            active_state.get("maxPerRequester"),
            active_state.get("workerSlots"),
        )
        == (1, 1, 1, 1),
        "active ledger limits differ",
    )
    require(
        (
            queue_state.get("version"),
            queue_state.get("globalLimit"),
            queue_state.get("requesterLimit"),
        )
        == (1, 3, 2),
        "queue ledger limits differ",
    )
    require(
        isinstance(active_state.get("active"), dict)
        and isinstance(queue_state.get("intents"), dict),
        "ledger entries are malformed",
    )
    service_accounts = verify_rbac()
    if empty:
        require(
            active_state["active"] == {}
            and queue_state["intents"] == {}
            and not active_state.get("lastGrantedRequesterHash"),
            "fixture ledgers are not freshly empty",
        )
        require(
            kjson("get", "kovabuilds", "--all-namespaces", "-o", "json").get("items") == [],
            "fixture has existing KovaBuilds",
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
            "fixture has existing runner Pods",
        )
    identity = {
        "cluster": CLUSTER,
        "namespace": NAMESPACE,
        "kubeconfig_sha256": kube_sha,
        "candidate_commit": head,
        "deployment_uid": deployment["metadata"]["uid"],
        "deployment_template_sha256": stable_hash(spec["template"]),
        "service_image": expected_image,
        "service_image_id": next(iter(image_ids)),
        "service_config_digest": expected_config_digest,
        "node_facts": nodes,
        "lease_uid": lease["metadata"]["uid"],
        "active_ledger_uid": active["metadata"]["uid"],
        "queue_ledger_uid": queue["metadata"]["uid"],
        "service_account_uids": service_accounts,
        "platform": platforms[0],
    }
    if local_image is not None:
        identity["service_local_image"] = local_image
        identity["service_initial_image_bindings"] = image_bindings
    return identity


def expected_spec_matches(build: dict, case: dict, account_uid: str) -> bool:
    spec = build.get("spec", {})
    source = spec.get("source", {})
    targets = spec.get("targets", [])
    options = spec.get("build", {})
    return (
        build.get("metadata", {}).get("name") == case["id"]
        and build.get("metadata", {}).get("namespace") == NAMESPACE
        and spec.get("idempotencyKey") == case["key"]
        and spec.get("requester") == {"username": case["requester"], "uid": account_uid}
        and source.get("uri") == case["source_uri"]
        and source.get("digest") == case["source_digest"]
        and len(targets) == 1
        and targets[0].get("target") == case["target"]
        and targets[0].get("platform") == case["platform"]
        and options.get("format") == "oci"
        and options.get("concurrency") == 1
    )


def runner_controller_uid(pod: dict) -> str:
    owners = [
        owner
        for owner in pod.get("metadata", {}).get("ownerReferences", [])
        if owner.get("controller") is True
    ]
    require(
        len(owners) == 1
        and owners[0].get("kind") == "KovaBuild"
        and isinstance(owners[0].get("uid"), str)
        and UID.fullmatch(owners[0]["uid"]) is not None,
        "runner Pod has no unique KovaBuild controller owner",
    )
    return owners[0]["uid"]


def validate_observation(
    builds: list[dict], pods: list[dict], active: dict, queue: dict, state: dict
) -> dict:
    """Refuse foreign workload or ledger entries before any test-owned mutation."""
    cases = state["cases"]
    accepted = state["accepted"]
    by_id = {case["id"]: label for label, case in cases.items()}
    observed_builds: dict[str, dict] = {}
    for build in builds:
        metadata = build.get("metadata", {})
        label = by_id.get(metadata.get("name"))
        require(
            label in accepted and metadata.get("namespace") == NAMESPACE,
            "unknown KovaBuild appeared in the fixture",
        )
        require(label not in observed_builds, "duplicate KovaBuild identity observed")
        uid = metadata.get("uid", "")
        require(
            uid == accepted[label] and UID.fullmatch(uid) is not None,
            "accepted KovaBuild UID changed",
        )
        require(
            expected_spec_matches(
                build, cases[label], state["identity"]["service_account_uids"][label[0]]
            ),
            "accepted KovaBuild spec or requester changed",
        )
        phase = build.get("status", {}).get("phase", "")
        require(
            phase in ("", "Queued", "Starting") or label in state["deleting"],
            "test KovaBuild reached an unexpected phase",
        )
        observed_builds[label] = build
    for label in accepted:
        require(
            label in observed_builds or label in state["deleting"] or label in state["deleted"],
            "accepted KovaBuild disappeared without a recorded delete",
        )

    grants = active.get("active")
    intents = queue.get("intents")
    require(
        isinstance(grants, dict) and isinstance(intents, dict), "admission ledgers are malformed"
    )
    require(len(grants) <= 1 and len(intents) <= 3, "admission ledger exceeded fixture caps")
    uid_to_label = {uid: label for label, uid in accepted.items()}
    active_labels: list[str] = []
    grant_details: dict[str, dict] = {}
    for uid, entry in grants.items():
        label = uid_to_label.get(uid)
        require(
            label in observed_builds and isinstance(entry, dict),
            "active grant belongs to an unknown or absent CR",
        )
        require(
            entry.get("buildName") == cases[label]["id"]
            and entry.get("requester") == cases[label]["requester"]
            and entry.get("slots") == 1,
            "active grant identity or slots changed",
        )
        in_flight = entry.get("inFlight") or []
        require(
            isinstance(in_flight, list)
            and len(in_flight) <= 1
            and all(
                isinstance(nonce, str) and re.fullmatch(r"[0-9a-f]{32}", nonce)
                for nonce in in_flight
            ),
            "active grant has an invalid Pod Create nonce",
        )
        active_labels.append(label)
        grant_details[label] = {"in_flight": in_flight, "closing": entry.get("closing", False)}
    queue_labels: list[str] = []
    requester_counts = {"a": 0, "b": 0}
    for build_id_value, entry in intents.items():
        label = by_id.get(build_id_value)
        require(
            label in observed_builds and isinstance(entry, dict),
            "queue intent belongs to an unknown or absent CR",
        )
        require(label in accepted, "rejected request acquired a queue intent")
        nonce = entry.get("nonce")
        require(
            isinstance(nonce, str)
            and re.fullmatch(r"[0-9a-f]{32}", nonce) is not None
            and entry.get("requesterHash") == sha256(cases[label]["requester"])
            and isinstance(entry.get("requestDigest"), str)
            and re.fullmatch(r"[0-9a-f]{64}", entry["requestDigest"]) is not None
            and observed_builds[label]["metadata"]
            .get("annotations", {})
            .get("kova.cofy.dev/queue-intent")
            == nonce,
            "queue intent nonce, requester, or CR annotation changed",
        )
        queue_labels.append(label)
        requester_counts[label[0]] += 1
    require(max(requester_counts.values()) <= 2, "per-requester queue cap was exceeded")

    observed_pods: dict[str, dict] = {}
    for pod in pods:
        metadata = pod.get("metadata", {})
        name = metadata.get("name", "")
        label = next(
            (label for label, case in cases.items() if name == "kova-job-" + case["id"]), None
        )
        require(
            label in observed_builds and metadata.get("namespace") == NAMESPACE,
            "unknown runner Pod appeared",
        )
        require(label not in observed_pods, "duplicate runner Pod appeared")
        require(
            runner_controller_uid(pod) == accepted[label],
            "runner Pod owner changed",
        )
        nonce = metadata.get("annotations", {}).get("kova.cofy.dev/create-attempt", "")
        require(
            isinstance(nonce, str) and re.fullmatch(r"[0-9a-f]{32}", nonce) is not None,
            "runner Pod has no valid Create nonce",
        )
        require(
            pod.get("spec", {}).get("nodeSelector", {}).get("never") == "true"
            and not pod.get("spec", {}).get("nodeName")
            and pod.get("status", {}).get("phase") == "Pending",
            "runner Pod became schedulable or left Pending",
        )
        observed_pods[label] = pod
    require(len(observed_pods) <= 2, "too many runner Pods exist during a one-slot transition")
    require(
        sum(pod["metadata"].get("deletionTimestamp") is None for pod in observed_pods.values())
        <= 1,
        "multiple live runner Pods bypassed the one-slot cap",
    )
    cursor = active.get("lastGrantedRequesterHash", "")
    require(
        cursor in ("", sha256(principal("a")), sha256(principal("b"))),
        "fairness cursor differs from both authenticated requesters",
    )
    return {
        "builds": observed_builds,
        "pods": observed_pods,
        "active": active_labels,
        "grant_details": grant_details,
        "queue": sorted(queue_labels),
        "cursor": cursor,
    }


def stage_matches(
    observation: dict, *, active: str | None, queued: tuple[str, ...], cursor: str | None
) -> bool:
    expected_present = set(queued) | ({active} if active else set())
    if set(observation["builds"]) != expected_present:
        return False
    if observation["active"] != ([active] if active else []):
        return False
    if observation["queue"] != sorted(queued):
        return False
    if set(observation["pods"]) != ({active} if active else set()):
        return False
    if observation["cursor"] != (sha256(principal(cursor)) if cursor else ""):
        return False
    for label, build in observation["builds"].items():
        if build.get("metadata", {}).get("deletionTimestamp") is not None:
            return False
        if build.get("status", {}).get("phase") != ("Starting" if label == active else "Queued"):
            return False
    if active:
        if observation["grant_details"][active] != {"in_flight": [], "closing": False}:
            return False
        pod = observation["pods"][active]
        if pod.get("metadata", {}).get("deletionTimestamp") is not None:
            return False
    return True


def require_creation_order(observation: dict, labels: tuple[str, ...]) -> None:
    """Kubernetes CreationTimestamp is second-granularity for fair FIFO ordering."""
    try:
        timestamps = [
            datetime.fromisoformat(
                observation["builds"][label]["metadata"]["creationTimestamp"].replace("Z", "+00:00")
            )
            for label in labels
        ]
    except (KeyError, AttributeError, TypeError, ValueError) as error:
        raise SafetyError(
            f"candidate creation order is unreadable: {type(error).__name__}"
        ) from None
    require(
        all(older < newer for older, newer in zip(timestamps, timestamps[1:])),
        "candidate creation order did not separate at Kubernetes timestamp precision",
    )


def projection(observation: dict) -> dict:
    return {
        "timestamp": now(),
        "cursor": observation["cursor"],
        "active": observation["active"],
        "grant_details": observation["grant_details"],
        "queue": observation["queue"],
        "builds": {
            label: {
                "name": build["metadata"]["name"],
                "uid": build["metadata"]["uid"],
                "created": build["metadata"].get("creationTimestamp"),
                "deleting": build["metadata"].get("deletionTimestamp"),
                "requester": build["spec"]["requester"]["username"],
                "phase": build.get("status", {}).get("phase", ""),
                "started_at": build.get("status", {}).get("startedAt"),
                "conditions": build.get("status", {}).get("conditions", []),
            }
            for label, build in observation["builds"].items()
        },
        "runners": {
            label: {
                "name": pod["metadata"]["name"],
                "uid": pod["metadata"]["uid"],
                "owner_uid": runner_controller_uid(pod),
                "create_attempt": pod["metadata"]
                .get("annotations", {})
                .get("kova.cofy.dev/create-attempt"),
                "deleting": pod["metadata"].get("deletionTimestamp"),
            }
            for label, pod in observation["pods"].items()
        },
    }


class Forwards:
    def __init__(self, directory: Path) -> None:
        self.directory = directory
        self.processes: list[subprocess.Popen] = []
        self.files = []

    def close(self) -> None:
        for process in self.processes:
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)
        self.processes.clear()
        for output in self.files:
            output.close()
        self.files.clear()

    def start(self, pod_names: list[str], label: str) -> None:
        self.close()
        require(len(pod_names) == 2, "two Ready Service Pods are required for direct port forwards")
        for which, pod in zip(("a", "b"), sorted(pod_names)):
            output = open(
                self.directory / f"{label}-{which}-port-forward.log",
                "xb",
                opener=lambda name, flags: os.open(name, flags, 0o600),
            )
            self.files.append(output)
            process = subprocess.Popen(
                [
                    "kubectl",
                    "--kubeconfig",
                    str(KUBECONFIG),
                    "-n",
                    NAMESPACE,
                    "port-forward",
                    "--address=127.0.0.1",
                    f"pod/{pod}",
                    f"{PORT[which]}:8080",
                ],
                stdout=output,
                stderr=subprocess.STDOUT,
            )
            self.processes.append(process)
        deadline = time.monotonic() + 30
        for which, process in zip(("a", "b"), self.processes):
            while time.monotonic() < deadline:
                require(process.poll() is None, "Service port-forward exited before readiness")
                request = Request(f"http://127.0.0.1:{PORT[which]}/healthz", method="GET")
                try:
                    status, _, _ = failover_io.bounded_response(request, timeout=2)
                    if status == 200:
                        break
                except failover_io.SafetyError:
                    pass
                time.sleep(0.25)
            else:
                raise SafetyError("Service port-forward did not become healthy")


class Campaign:
    def __init__(self, directory: Path, state: dict) -> None:
        self.directory = directory
        self.state = state
        self.forwards = Forwards(directory)
        self.tokens: dict[str, str] = {}
        self.started = time.monotonic()

    def persist(self) -> None:
        save(self.directory / "state.json", self.state)

    def guard_time(self) -> None:
        require(
            time.monotonic() - self.started < MAX_RUN_SECONDS,
            "fairness campaign exceeded its 20-minute bound",
        )

    def identity(self) -> dict:
        self.guard_time()
        current = fixture_identity(
            empty=False, expected_config_digest=self.state["identity"]["service_config_digest"]
        )
        pinned = self.state["identity"]
        for key in (
            "cluster",
            "namespace",
            "kubeconfig_sha256",
            "candidate_commit",
            "deployment_uid",
            "deployment_template_sha256",
            "service_image",
            "service_image_id",
            "service_config_digest",
            "node_facts",
            "lease_uid",
            "active_ledger_uid",
            "queue_ledger_uid",
            "service_account_uids",
            "platform",
        ):
            require(current[key] == pinned[key], f"fixture identity drifted: {key}")
        return current

    def observe(self) -> dict:
        self.identity()
        builds = kjson("get", "kovabuilds", "--all-namespaces", "-o", "json").get("items", [])
        pods = kjson(
            "get",
            "pods",
            "--all-namespaces",
            "-l",
            "app.kubernetes.io/name=kova-runner",
            "-o",
            "json",
        ).get("items", [])
        active = ledger_data(
            kjson("-n", NAMESPACE, "get", "configmap", "kova-service-admission", "-o", "json"),
            "reservations.json",
        )
        queue = ledger_data(
            kjson(
                "-n", NAMESPACE, "get", "configmap", "kova-service-queue-admission", "-o", "json"
            ),
            "queue.json",
        )
        require(isinstance(builds, list) and isinstance(pods, list), "workload lists are malformed")
        return validate_observation(builds, pods, active, queue, self.state)

    def snapshot(self, label: str, observation: dict) -> None:
        require(re.fullmatch(r"[a-z0-9-]+", label) is not None, "invalid receipt label")
        save(self.directory / f"{label}.json", projection(observation))

    def wait_stage(
        self,
        label: str,
        *,
        active: str | None,
        queued: tuple[str, ...],
        cursor: str,
        timeout: int = 120,
    ) -> dict:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            observation = self.observe()
            if stage_matches(observation, active=active, queued=queued, cursor=cursor):
                self.snapshot(label, observation)
                return observation
            time.sleep(1)
        raise SafetyError(f"{label} did not reach the exact fair admission state within {timeout}s")

    def wait_empty(self, label: str, timeout: int = 120) -> None:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            observation = self.observe()
            if not any(
                (
                    observation["builds"],
                    observation["pods"],
                    observation["active"],
                    observation["queue"],
                )
            ):
                self.snapshot(label, observation)
                return
            time.sleep(1)
        raise SafetyError(f"{label} did not leave both ledgers and all workloads empty")

    def issue_tokens(self) -> None:
        self.identity()
        for which, name in SA.items():
            token = kctl(
                "-n", NAMESPACE, "create", "token", name, "--duration=30m", timeout=20
            ).strip()
            require(
                32 <= len(token) <= 8192
                and token.isascii()
                and not any(char.isspace() for char in token),
                "TokenRequest returned an invalid bearer",
            )
            self.tokens[which] = token

    def service_pod_names(self) -> list[str]:
        pods = kjson(
            "-n",
            NAMESPACE,
            "get",
            "pods",
            "-l",
            "app.kubernetes.io/instance=kova,app.kubernetes.io/component=service",
            "-o",
            "json",
        ).get("items", [])
        return sorted(pod["metadata"]["name"] for pod in pods)

    def post(self, label: str, expected_status: int, port: int) -> None:
        self.observe()
        case = self.state["cases"][label]
        require(label not in self.state["accepted"], "case was already accepted")
        require(
            label not in self.state["rejected"] or (label == "b2" and expected_status == 202),
            "unexpected retry of a rejected case",
        )
        receipt = (
            self.directory
            / f"post-{label}-{'accepted' if expected_status == 202 else 'rejected'}.json"
        )
        self.state["attempting"] = label
        self.persist()  # An interrupted POST has an explicit unresolved candidate.
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
            headers={
                "Authorization": f"Bearer {self.tokens[label[0]]}",
                "Content-Type": "application/json",
            },
            method="POST",
        )
        started = time.monotonic()
        try:
            status, headers, body = failover_io.bounded_response(request, timeout=15)
        except failover_io.SafetyError as error:
            save(receipt, {"outcome": "unknown", "id": case["id"], "timestamp": now()})
            raise SafetyError(f"POST {label} outcome is unknown: {type(error).__name__}") from None
        require(
            self.tokens[label[0]].encode() not in body,
            "Service reflected a bearer in its response; body withheld",
        )
        header_id = next(
            (value for key, value in headers.items() if key.lower() == "x-kova-build-id"), None
        )
        code = None
        if status == 429:
            try:
                error = json.loads(body)
                code = error.get("code") if error.get("retryable") is True else None
            except (TypeError, ValueError):
                pass
        save(
            receipt,
            {
                "timestamp": now(),
                "id": case["id"],
                "requester": case["requester"],
                "http_status": status,
                "error_code": code,
                "header_matches_expected": header_id == case["id"],
                "response_bytes": len(body),
                "elapsed_seconds": round(time.monotonic() - started, 6),
            },
        )
        require(
            status == expected_status and header_id == case["id"],
            f"POST {label} returned an unexpected status or Build ID",
        )
        if expected_status == 429:
            require(code == "queue_capacity_exceeded", f"POST {label} returned an unrelated 429")
            self.state["rejected"].append(label)
            self.state["attempting"] = None
            self.persist()
            self.observe()  # The rejected ID must not appear in either ledger or as a CR.
            return
        require(
            expected_status == 202, "test expects only newly accepted or capacity-rejected POSTs"
        )
        build = kjson("-n", NAMESPACE, "get", "kovabuild", case["id"], "-o", "json")
        account_uid = self.state["identity"]["service_account_uids"][label[0]]
        require(
            expected_spec_matches(build, case, account_uid), f"POST {label} created a different CR"
        )
        uid = build.get("metadata", {}).get("uid", "")
        require(
            isinstance(uid, str) and UID.fullmatch(uid) is not None,
            f"POST {label} CR has no exact UID",
        )
        self.state["accepted"][label] = uid
        self.state["attempting"] = None
        self.persist()

    def delete(self, label: str) -> None:
        observation = self.observe()
        require(
            label in observation["builds"], f"cannot prove exact {label} KovaBuild before delete"
        )
        build = observation["builds"][label]
        uid = self.state["accepted"][label]
        require(
            build["metadata"]["uid"] == uid and build["metadata"].get("deletionTimestamp") is None,
            f"{label} is already deleting or changed",
        )
        active = ledger_data(
            kjson("-n", NAMESPACE, "get", "configmap", "kova-service-admission", "-o", "json"),
            "reservations.json",
        )
        if label in observation["active"]:
            require(
                (active["active"][uid].get("inFlight") or []) == [],
                "active grant has unresolved Pod Create nonce",
            )
        lease = kjson("-n", NAMESPACE, "get", "lease", LEASE, "-o", "json")
        require(
            lease["metadata"]["uid"] == self.state["identity"]["lease_uid"],
            "Lease UID changed before delete",
        )
        if label not in self.state["deleting"]:
            self.state["deleting"].append(label)
        self.persist()
        receipt = self.directory / f"delete-{label}.json"
        args = argparse.Namespace(
            kubeconfig=KUBECONFIG,
            kubeconfig_sha256=self.state["identity"]["kubeconfig_sha256"],
            cluster=CLUSTER,
            namespace=NAMESPACE,
            resource="kovabuilds",
            name=self.state["cases"][label]["id"],
            uid=uid,
            lease_name=LEASE,
            lease_uid=lease["metadata"]["uid"],
            lease_holder=lease["spec"]["holderIdentity"],
            receipt=receipt,
        )
        try:
            failover_io.uid_delete(args)
        except (failover_io.SafetyError, OSError, subprocess.TimeoutExpired) as error:
            raise SafetyError(
                f"UID delete for {label} is unproven: {type(error).__name__}"
            ) from None

    def mark_deleted(self, label: str) -> None:
        require(label in self.state["deleting"], "cannot mark an unplanned delete complete")
        if label not in self.state["deleted"]:
            self.state["deleted"].append(label)
        self.persist()

    def handoff(self) -> None:
        self.observe()
        self.forwards.close()
        lease = kjson("-n", NAMESPACE, "get", "lease", LEASE, "-o", "json")
        holder = lease["spec"]["holderIdentity"]
        old_name = holder.split("_", 1)[0]
        pods = kjson(
            "-n",
            NAMESPACE,
            "get",
            "pods",
            "-l",
            "app.kubernetes.io/instance=kova,app.kubernetes.io/component=service",
            "-o",
            "json",
        ).get("items", [])
        old = next((pod for pod in pods if pod["metadata"]["name"] == old_name), None)
        require(
            old is not None and len(pods) == 2,
            "current Lease holder is not one of two Service Pods",
        )
        uid = old["metadata"]["uid"]
        require(
            UID.fullmatch(uid) is not None and old["metadata"].get("deletionTimestamp") is None,
            "leader Pod UID changed or is terminating",
        )
        self.state["leader_delete"] = {
            "name": old_name,
            "uid": uid,
            "holder": holder,
            "planned_at": now(),
        }
        self.persist()
        args = argparse.Namespace(
            kubeconfig=KUBECONFIG,
            kubeconfig_sha256=self.state["identity"]["kubeconfig_sha256"],
            cluster=CLUSTER,
            namespace=NAMESPACE,
            resource="pods",
            name=old_name,
            uid=uid,
            lease_name=LEASE,
            lease_uid=self.state["identity"]["lease_uid"],
            lease_holder=holder,
            receipt=self.directory / "delete-leader-pod.json",
        )
        try:
            failover_io.uid_delete(args)
        except (failover_io.SafetyError, OSError, subprocess.TimeoutExpired) as error:
            raise SafetyError(
                f"leader Pod UID delete is unproven: {type(error).__name__}"
            ) from None
        deadline = time.monotonic() + 180
        while time.monotonic() < deadline:
            try:
                self.identity()
                current = kjson("-n", NAMESPACE, "get", "lease", LEASE, "-o", "json")
                names = self.service_pod_names()
                new_holder = current["spec"].get("holderIdentity", "")
                if (
                    new_holder != holder
                    and old_name not in names
                    and any(new_holder.startswith(name + "_") for name in names)
                ):
                    self.state["leader_delete"]["new_holder"] = new_holder
                    self.state["leader_delete"]["completed_at"] = now()
                    self.persist()
                    save(self.directory / "handoff.json", self.state["leader_delete"])
                    self.forwards.start(names, "after-handoff")
                    return
            except SafetyError:
                pass  # Deployment replacement is transient; the deadline remains bounded.
            time.sleep(2)
        raise SafetyError("distinct Ready Service leader did not appear within 180s")

    def promotion(self, released: str, promoted: str, queued: tuple[str, ...], cursor: str) -> None:
        started = time.monotonic()
        self.delete(released)
        observation = self.wait_stage(
            f"promoted-{promoted}", active=promoted, queued=queued, cursor=cursor
        )
        self.mark_deleted(released)
        build = observation["builds"][promoted]
        queued_seconds = None
        try:
            created = datetime.fromisoformat(
                build["metadata"]["creationTimestamp"].replace("Z", "+00:00")
            )
            queued_seconds = round((datetime.now(timezone.utc) - created).total_seconds(), 3)  # noqa: UP017 (Python 3.10)
        except (KeyError, TypeError, ValueError):
            pass
        timing = {
            "released": released,
            "promoted": promoted,
            "observed_at": now(),
            "release_to_grant_seconds": round(time.monotonic() - started, 3),
            "created_to_grant_observation_seconds": queued_seconds,
        }
        self.state["promotions"].append(timing)
        self.persist()
        save(self.directory / f"timing-{promoted}.json", timing)

    def run(self) -> None:
        self.issue_tokens()
        self.forwards.start(self.service_pod_names(), "initial")
        self.post("a1", 202, PORT["a"])
        self.wait_stage("a1-active", active="a1", queued=(), cursor="a")
        time.sleep(1.1)
        self.post("a2", 202, PORT["b"])
        time.sleep(1.1)
        self.post("a3", 202, PORT["a"])
        self.wait_stage("alice-queue-full", active="a1", queued=("a2", "a3"), cursor="a")
        self.post("a4", 429, PORT["b"])
        time.sleep(1.1)
        self.post("b1", 202, PORT["b"])
        full = self.wait_stage(
            "global-queue-full", active="a1", queued=("a2", "a3", "b1"), cursor="a"
        )
        require_creation_order(full, ("a1", "a2", "a3", "b1"))
        self.post("b2", 429, PORT["a"])
        self.handoff()
        self.wait_stage("after-handoff", active="a1", queued=("a2", "a3", "b1"), cursor="a")
        self.promotion("a1", "b1", ("a2", "a3"), "b")
        self.post("b2", 202, PORT["a"])
        self.wait_stage("bob-queue-restored", active="b1", queued=("a2", "a3", "b2"), cursor="b")
        self.promotion("b1", "a2", ("a3", "b2"), "a")
        self.promotion("a2", "b2", ("a3",), "b")
        self.promotion("b2", "a3", (), "a")
        self.delete("a3")
        self.wait_empty("after-cleanup")
        self.mark_deleted("a3")
        self.state["outcome"] = "passed"
        self.state["completed_at"] = now()
        self.persist()

    def recover(self) -> None:
        self.identity()
        require(
            self.state.get("attempting") is None,
            "a POST outcome is unresolved; inspect its exact CR and queue intent manually",
        )
        observation = self.observe()
        queued = sorted(observation["queue"])
        active = observation["active"]
        for label in (
            queued
            + [
                label
                for label in observation["builds"]
                if label not in queued and label not in active
            ]
            + active
        ):
            observation = self.observe()
            if label not in observation["builds"]:
                continue
            build = observation["builds"][label]
            if build["metadata"].get("deletionTimestamp") is None:
                self.delete(label)
            elif label not in self.state["deleting"]:
                self.state["deleting"].append(label)
                self.persist()
        self.wait_empty("recovery-empty")
        for label in self.state["accepted"]:
            if label not in self.state["deleted"]:
                require(label in self.state["deleting"], "a disappeared CR has no recorded delete")
                self.mark_deleted(label)
        self.state["outcome"] = "recovered"
        self.state["completed_at"] = now()
        self.persist()


def load_run(directory: Path) -> dict:
    require(
        directory.parent == EVIDENCE_ROOT and RUN_ID.fullmatch(directory.name) is not None,
        "recovery path is not an exact fairness run",
    )
    require(
        directory.is_dir() and not directory.is_symlink(),
        "recovery run directory is unavailable or a symlink",
    )
    path = directory / "state.json"
    require(
        path.is_file() and not path.is_symlink() and path.stat().st_size <= 65536,
        "recovery state is absent or unbounded",
    )
    try:
        state = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, ValueError) as error:
        raise SafetyError(f"recovery state is unreadable: {type(error).__name__}") from None
    require(
        isinstance(state, dict) and state.get("run_id") == directory.name,
        "recovery state run ID differs",
    )
    identity = state.get("identity", {})
    require(
        isinstance(identity, dict)
        and identity.get("cluster") == CLUSTER
        and identity.get("namespace") == NAMESPACE,
        "recovery state targets another cluster",
    )
    require(
        isinstance(identity.get("service_config_digest"), str)
        and SHA256.fullmatch(identity["service_config_digest"]) is not None,
        "recovery state has no pinned candidate image config digest",
    )
    local_image = identity.get("service_local_image", {})
    require(
        isinstance(local_image, dict)
        and local_image.get("config_digest") == identity["service_config_digest"]
        and isinstance(local_image.get("platform_manifest_digest"), str)
        and SHA256.fullmatch(local_image["platform_manifest_digest"]) is not None,
        "recovery state has no reviewed local image chain",
    )
    require(
        state.get("cases") == planned_cases(directory.name, identity.get("platform", "")),
        "recovery candidate contracts differ",
    )
    accepted = state.get("accepted", {})
    require(
        isinstance(accepted, dict) and set(accepted).issubset({"a1", "a2", "a3", "b1", "b2"}),
        "recovery accepted set is invalid",
    )
    require(
        all(isinstance(uid, str) and UID.fullmatch(uid) is not None for uid in accepted.values()),
        "recovery accepted UID is invalid",
    )
    require(
        all(
            isinstance(state.get(field), list)
            for field in ("rejected", "deleting", "deleted", "promotions")
        ),
        "recovery state fields are malformed",
    )
    require(
        set(state["deleting"]).issubset(accepted)
        and set(state["deleted"]).issubset(state["deleting"]),
        "recovery delete journal is invalid",
    )
    return state


def handler(signum: int, _frame: object) -> None:
    raise SafetyError(f"received signal {signum}; receipts preserved for exact recovery")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "mode",
        choices=("check", "run", "recover"),
        nargs="?",
        default=os.environ.get("ADMISSION_FAIRNESS_MODE", "check"),
    )
    parser.add_argument(
        "--run-dir",
        type=Path,
        default=(
            Path(os.environ["ADMISSION_FAIRNESS_RUN_DIR"])
            if os.environ.get("ADMISSION_FAIRNESS_RUN_DIR")
            else None
        ),
    )
    args = parser.parse_args()
    try:
        require(args.mode in ("check", "run", "recover"), "ADMISSION_FAIRNESS_MODE is invalid")
        if args.mode == "check":
            require(args.run_dir is None, "check mode does not use a run directory")
            identity = fixture_identity(empty=True)
            print(
                "admission-fairness: read-only preflight passed: "
                + identity["cluster"]
                + " / "
                + identity["candidate_commit"][:12]
            )
            return 0
        require(
            socket.gethostname() == "wayne-hk-kvm",
            "live fairness acceptance is restricted to wayne-hk-kvm",
        )
        require(
            os.environ.get("ADMISSION_FAIRNESS_ACK") == ACK,
            f"live mode requires ADMISSION_FAIRNESS_ACK={ACK}",
        )
        require(not EVIDENCE_ROOT.is_symlink(), "evidence root is a symlink")
        EVIDENCE_ROOT.mkdir(mode=0o700, parents=True, exist_ok=True)
        require(
            EVIDENCE_ROOT.stat().st_uid == os.getuid()
            and EVIDENCE_ROOT.stat().st_mode & 0o077 == 0,
            "evidence root is not private to the KVM user",
        )
        with open(
            EVIDENCE_ROOT / ".run.lock",
            "a+b",
            opener=lambda name, flags: os.open(name, flags, 0o600),
        ) as lock:
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                raise SafetyError(
                    "another fairness acceptance or recovery owns the fixture"
                ) from None
            if args.mode == "run":
                require(
                    args.run_dir is None, "run mode generates its own unique evidence directory"
                )
                identity = fixture_identity(empty=True)
                run_id = (
                    "fairness-"
                    + datetime.now(timezone.utc).strftime("%Y%m%dt%H%M%Sz")  # noqa: UP017 (Python 3.10)
                    + "-"
                    + secrets.token_hex(4)
                )
                directory = EVIDENCE_ROOT / run_id
                directory.mkdir(mode=0o700)
                state = {
                    "run_id": run_id,
                    "identity": identity,
                    "cases": planned_cases(run_id, identity["platform"]),
                    "accepted": {},
                    "rejected": [],
                    "deleting": [],
                    "deleted": [],
                    "promotions": [],
                    "attempting": None,
                    "outcome": "running",
                    "started_at": now(),
                }
                campaign = Campaign(directory, state)
                campaign.persist()
            else:
                require(args.run_dir is not None, "recover mode requires --run-dir")
                directory = args.run_dir.absolute()
                state = load_run(directory)
                campaign = Campaign(directory, state)
            previous_term = signal.signal(signal.SIGTERM, handler)
            previous_int = signal.signal(signal.SIGINT, handler)
            try:
                if args.mode == "run":
                    campaign.run()
                else:
                    campaign.recover()
            finally:
                campaign.forwards.close()
                signal.signal(signal.SIGTERM, previous_term)
                signal.signal(signal.SIGINT, previous_int)
            print(f"admission-fairness: {state['outcome']}; evidence: {directory}")
            return 0
    except (SafetyError, failover_io.SafetyError, OSError, subprocess.TimeoutExpired) as error:
        if "campaign" in locals():
            try:
                campaign.state["outcome"] = "stopped"
                campaign.state["stopped_at"] = now()
                campaign.state["stop_reason"] = f"{type(error).__name__}: {error}"
                campaign.persist()
            except (OSError, TypeError, ValueError):
                pass
        print(f"admission-fairness: STOP: {type(error).__name__}: {error}", file=sys.stderr)
        if "directory" in locals():
            print(f"admission-fairness: evidence preserved at {directory}", file=sys.stderr)
        return 1
    except Exception as error:
        if "campaign" in locals():
            try:
                campaign.state["outcome"] = "stopped"
                campaign.state["stopped_at"] = now()
                campaign.state["stop_reason"] = f"unexpected {type(error).__name__}"
                campaign.persist()
            except (OSError, TypeError, ValueError):
                pass
        print(f"admission-fairness: STOP: unexpected {type(error).__name__}", file=sys.stderr)
        if "directory" in locals():
            print(f"admission-fairness: evidence preserved at {directory}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
