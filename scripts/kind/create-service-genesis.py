#!/usr/bin/env python3
"""Create one fresh Kind-only Service admission installation, without adoption.

This is an external local-fixture installer, not part of the Service runtime.
Every write is create-only. Unknown outcomes retain the private evidence
directory; rerunning against an existing namespace is deliberately refused.
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
import re
import secrets
import subprocess
import sys
import time
from pathlib import Path
from urllib.parse import urlsplit


class Stop(RuntimeError):
    pass


def require(ok: bool, message: str) -> None:
    if not ok:
        raise Stop(message)


def dns_label(value: str) -> bool:
    return len(value) <= 63 and re.fullmatch(r"[a-z0-9](?:[a-z0-9-]*[a-z0-9])?", value) is not None


def runner_manifest_reference(value: str) -> bool:
    """Require an explicit registry, including a Kind DNS label with a port."""
    if len(value) > 512:
        return False
    match = re.fullmatch(
        r"([^/]+)/[a-z0-9]+(?:[._/-][a-z0-9]+)*@sha256:[0-9a-f]{64}", value
    )
    if match is None:
        return False
    authority = match.group(1)
    host, separator, port = authority.partition(":")
    if separator and (
        re.fullmatch(r"[1-9][0-9]{0,4}", port) is None or int(port) > 65535
    ):
        return False
    return (
        len(host) <= 253
        and all(dns_label(label) for label in host.split("."))
        and (host == "localhost" or "." in host or bool(separator))
    )


def strict_object(pairs: list[tuple[str, object]]) -> dict:
    result = {}
    for key, value in pairs:
        require(key not in result, "duplicate JSON receipt field")
        result[key] = value
    return result


def local_endpoint(value: str) -> bool:
    endpoint = urlsplit(value)
    return (
        endpoint.scheme == "https"
        and endpoint.hostname == "127.0.0.1"
        and not (endpoint.username or endpoint.password or endpoint.query or endpoint.fragment)
        and endpoint.path in ("", "/")
    )


def expected_contract(args: argparse.Namespace, namespace_uid: str, receipt_namespace_uid: str, generation: str) -> dict:
    return {
        "version": 3,
        "namespaceUID": namespace_uid,
        "receiptNamespace": args.receipt_namespace,
        "receiptNamespaceUID": receipt_namespace_uid,
        "workerPoolID": args.worker_pool_id,
        "runnerImage": args.runner_image,
        "generation": generation,
        "activeLedgerName": "kova-service-admission",
        "activeLedgerSchema": 2,
        "queueLedgerName": "kova-service-queue-admission",
        "queueLedgerSchema": 2,
        "limits": {
            "maxActiveJobs": args.max_active_jobs,
            "maxActiveJobsPerRequester": args.max_active_jobs_per_requester,
            "workerSlots": args.worker_slots,
            "maxQueuedJobs": args.max_queued_jobs,
            "maxQueuedJobsPerRequester": args.max_queued_jobs_per_requester,
        },
    }


def check_secret_data(secret: dict, expected: dict, proposed_data: dict | None = None) -> None:
    data = secret.get("data")
    require(
        secret.get("immutable") is True
        and secret.get("type") == "Opaque"
        and not secret.get("stringData"),
        "receipt Secret is not immutable Opaque data",
    )
    require(
        isinstance(data, dict)
        and set(data) == {"receipt.json"}
        and isinstance(data["receipt.json"], str),
        "receipt Secret data keys differ",
    )
    raw = base64.b64decode(data["receipt.json"], validate=True)
    require(0 < len(raw) <= 8192, "receipt bytes exceed limit")
    parsed = json.loads(raw, object_pairs_hook=strict_object)
    require(
        parsed == expected and raw == json.dumps(expected, separators=(",", ":")).encode(),
        "receipt bytes do not match the original installation contract",
    )
    require(
        proposed_data is None or data == proposed_data,
        "receipt Secret bytes changed from the create proposal",
    )


def private_json(path: Path, value: object) -> None:
    raw = (json.dumps(value, sort_keys=True, indent=2) + "\n").encode()
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, "wb") as output:
        output.write(raw)
        output.flush()
        os.fsync(output.fileno())


def object_uid(obj: dict, kind: str, name: str, namespace: str | None = None) -> str:
    metadata = obj.get("metadata", {})
    uid = metadata.get("uid")
    require(
        obj.get("kind") == kind
        and metadata.get("name") == name
        and (namespace is None or metadata.get("namespace") == namespace)
        and isinstance(uid, str)
        and 0 < len(uid) <= 256
        and not metadata.get("deletionTimestamp"),
        f"{kind} create/read identity is incomplete or changed",
    )
    return uid


def validate_options(args: argparse.Namespace) -> None:
    require(
        args.context.startswith("kind-") and len(args.context) > 5,
        "only an explicit Kind context is supported",
    )
    require(
        isinstance(args.kube_system_uid, str) and 0 < len(args.kube_system_uid) <= 256,
        "explicit kube-system UID is required",
    )
    require(
        dns_label(args.runner_namespace) and dns_label(args.service_namespace)
        and dns_label(args.receipt_namespace),
        "invalid namespace name",
    )
    require(
        len({args.runner_namespace, args.service_namespace, args.receipt_namespace}) == 3,
        "runner, Service, and receipt namespaces must be pairwise distinct",
    )
    require(dns_label(args.release), "invalid release name")
    require(
        re.fullmatch(r"[A-Za-z0-9._:/@+\-]{1,128}", args.worker_pool_id) is not None,
        "an explicit stable worker pool identity is required",
    )
    require(
        runner_manifest_reference(args.runner_image),
        "runner image must be an explicit repository@sha256 OCI manifest digest",
    )
    require(
        args.acknowledge == f"{args.context}/{args.service_namespace}/{args.runner_namespace}/{args.receipt_namespace}",
        "fresh-install acknowledgement must match context/service-namespace/runner-namespace/receipt-namespace",
    )
    require(1 <= args.max_active_jobs <= 128, "unsupported active limit")
    require(
        1 <= args.max_active_jobs_per_requester <= args.max_active_jobs,
        "unsupported requester active limit",
    )
    require(1 <= args.worker_slots <= 65535, "unsupported worker slots")
    require(1 <= args.max_queued_jobs <= 1000, "unsupported queue limit")
    require(
        1 <= args.max_queued_jobs_per_requester <= args.max_queued_jobs,
        "unsupported requester queue limit",
    )
    for value, label in ((args.kubeconfig, "kubeconfig"), (args.kova_cli, "Kova CLI")):
        path = Path(value)
        require(
            path.is_absolute() and path.is_file() and not path.is_symlink(),
            f"{label} must be an absolute regular file",
        )
    require(os.access(args.kova_cli, os.X_OK), "Kova CLI is not executable")
    output = Path(args.output_directory)
    require(
        output.is_absolute() and not output.exists() and not output.is_symlink(),
        "output directory must be a new absolute path",
    )


class Installer:
    def __init__(self, args: argparse.Namespace):
        validate_options(args)
        self.args = args
        self.directory = Path(args.output_directory)
        self.deadline = time.monotonic() + 180
        self.kubeconfig_sha = hashlib.sha256(Path(args.kubeconfig).read_bytes()).hexdigest()
        self.cli_sha = hashlib.sha256(Path(args.kova_cli).read_bytes()).hexdigest()
        self.sequence = 0
        self.node_id = ""
        self.api_port: int | None = None

    def execute(self, argv: list[str], *, payload: str | None = None) -> str:
        require(
            hashlib.sha256(Path(self.args.kubeconfig).read_bytes()).hexdigest()
            == self.kubeconfig_sha,
            "kubeconfig changed during installation",
        )
        require(
            hashlib.sha256(Path(self.args.kova_cli).read_bytes()).hexdigest() == self.cli_sha,
            "Kova CLI changed during installation",
        )
        remaining = self.deadline - time.monotonic()
        require(remaining > 0, "installation deadline expired; inspect retained evidence")
        try:
            result = subprocess.run(
                argv,
                input=payload,
                text=True,
                capture_output=True,
                timeout=min(35, remaining),
                check=False,
            )
        except (OSError, subprocess.TimeoutExpired) as error:
            raise Stop(
                f"unknown command outcome ({type(error).__name__}); do not retry or delete by name"
            ) from None
        require(
            result.returncode == 0,
            "command failed; preserve evidence and inspect exact objects without retry/adoption",
        )
        require(
            hashlib.sha256(Path(self.args.kubeconfig).read_bytes()).hexdigest()
            == self.kubeconfig_sha,
            "kubeconfig changed while command ran; outcome requires inspection",
        )
        require(
            hashlib.sha256(Path(self.args.kova_cli).read_bytes()).hexdigest() == self.cli_sha,
            "Kova CLI changed while command ran; outcome requires inspection",
        )
        require(len(result.stdout.encode()) <= 1024 * 1024, "installation response exceeds limit")
        return result.stdout

    def kubectl(self, *args: str, payload: str | None = None) -> str:
        return self.execute(
            [
                "kubectl",
                "--kubeconfig",
                self.args.kubeconfig,
                "--context",
                self.args.context,
                "--request-timeout=10s",
                *args,
            ],
            payload=payload,
        )

    def read(self, kind: str, name: str, namespace: str | None = None) -> dict:
        scope = [] if namespace is None else ["-n", namespace]
        return json.loads(self.kubectl(*scope, "get", kind, name, "-o", "json"))

    def create(self, label: str, manifest: dict) -> dict:
        self.check_cluster()
        self.sequence += 1
        private_json(self.directory / f"{self.sequence:02d}-{label}-request.json", manifest)
        created = json.loads(
            self.kubectl("create", "-f", "-", "-o", "json", payload=json.dumps(manifest))
        )
        private_json(self.directory / f"{self.sequence:02d}-{label}-response.json", created)
        return created

    def check_cluster(self) -> None:
        self.check_kind_node()
        system = self.read("namespace", "kube-system")
        require(
            object_uid(system, "Namespace", "kube-system") == self.args.kube_system_uid,
            "kube-system UID changed or targets another cluster",
        )
        require(
            system.get("status", {}).get("phase") == "Active", "kube-system namespace is not Active"
        )

    def check_kind_node(self) -> None:
        cluster = self.args.context.removeprefix("kind-")
        control_plane = cluster + "-control-plane"
        nodes = self.execute(["kind", "get", "nodes", "--name", cluster]).splitlines()
        require(control_plane in nodes, "local Kind control-plane is absent")
        template = (
            '{"id":{{json .Id}},"running":{{json .State.Running}},'
            '"cluster":{{json (index .Config.Labels "io.x-k8s.kind.cluster")}},'
            '"role":{{json (index .Config.Labels "io.x-k8s.kind.role")}},'
            '"ports":{{json (index .NetworkSettings.Ports "6443/tcp")}}}'
        )
        node = json.loads(self.execute(["docker", "inspect", "--format", template, control_plane]))
        require(
            node.get("running") is True
            and node.get("cluster") == cluster
            and node.get("role") == "control-plane"
            and re.fullmatch(r"[0-9a-f]{64}", node.get("id", "")) is not None,
            "local Kind control-plane identity differs",
        )
        require(
            node.get("ports") == [{"HostIp": "127.0.0.1", "HostPort": str(self.api_port)}],
            "API port is not the local Kind control-plane loopback binding",
        )
        require(not self.node_id or self.node_id == node["id"], "Kind control-plane was replaced")
        self.node_id = node["id"]

    def proposal(self, subcommand: str, common: list[str], extra: list[str] | None = None) -> dict:
        return json.loads(
            self.execute(
                [
                    self.args.kova_cli,
                    "--kubeconfig",
                    self.args.kubeconfig,
                    "--namespace",
                    self.args.runner_namespace,
                    "admission-genesis",
                    subcommand,
                    *common,
                    *(extra or []),
                ]
            )
        )

    def run(self) -> Path:
        args = self.args
        # The CLI reads current-context from this exact file; kubectl is also
        # explicitly pinned. Never mutate the caller's context or kubeconfig.
        current = self.kubectl("config", "current-context").strip()
        require(
            current == args.context,
            "kubeconfig current-context differs from the explicitly pinned Kind context",
        )
        endpoint = self.kubectl(
            "config", "view", "--minify", "-o", "jsonpath={.clusters[0].cluster.server}"
        ).strip()
        require(local_endpoint(endpoint), "Kind installer requires a loopback HTTPS API endpoint")
        self.api_port = urlsplit(endpoint).port
        require(self.api_port is not None, "Kind API endpoint has no explicit port")
        proxy = self.kubectl(
            "config", "view", "--minify", "-o", "jsonpath={.clusters[0].cluster.proxy-url}"
        ).strip()
        require(not proxy, "Kind installer refuses kubeconfig proxy routing")
        self.check_cluster()
        service = self.read("namespace", args.service_namespace)
        service_uid = object_uid(service, "Namespace", args.service_namespace)
        require(
            service.get("status", {}).get("phase") == "Active", "Service namespace is not Active"
        )
        existing = self.kubectl(
            "get", "namespace", args.runner_namespace, "--ignore-not-found", "-o", "json"
        ).strip()
        require(
            not existing, "runner namespace already exists; installation never adopts or resets it"
        )
        existing_receipts = self.kubectl(
            "get", "namespace", args.receipt_namespace, "--ignore-not-found", "-o", "json"
        ).strip()
        require(
            not existing_receipts,
            "receipt namespace already exists; installation never adopts or resets it",
        )
        selector = f"app.kubernetes.io/instance={args.release},app.kubernetes.io/component=service"
        pods = json.loads(
            self.kubectl("-n", args.service_namespace, "get", "pods", "-l", selector, "-o", "json")
        )
        require(
            pods.get("items") == [], "old Service Pods remain; external stop-and-drain is required"
        )
        deployments = json.loads(
            self.kubectl(
                "-n", args.service_namespace, "get", "deployments", "-l", selector, "-o", "json"
            )
        )
        require(isinstance(deployments.get("items"), list), "Deployment list is incomplete")
        require(
            all(d.get("spec", {}).get("replicas") == 0 for d in deployments["items"]),
            "old Service Deployment is still enabled",
        )
        self.directory.mkdir(mode=0o700)
        generation = secrets.token_hex(16)
        private_json(
            self.directory / "installation-intent.json",
            {
                "schema": "kova-kind-genesis-installation-v1",
                "context": args.context,
                "serviceNamespace": args.service_namespace,
                "serviceNamespaceUID": service_uid,
                "runnerNamespace": args.runner_namespace,
                "receiptNamespace": args.receipt_namespace,
                "workerPoolID": args.worker_pool_id,
                "runnerImage": args.runner_image,
                "generation": generation,
                "kubeconfigSHA256": self.kubeconfig_sha,
                "cliSHA256": self.cli_sha,
                "kubeSystemUID": args.kube_system_uid,
                "controlPlaneID": self.node_id,
            },
        )
        ns = self.create(
            "namespace",
            {"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": args.runner_namespace}},
        )
        namespace_uid = object_uid(ns, "Namespace", args.runner_namespace)
        observed = self.read("namespace", args.runner_namespace)
        require(
            object_uid(observed, "Namespace", args.runner_namespace) == namespace_uid,
            "new Namespace UID changed",
        )
        require(
            observed.get("status", {}).get("phase") == "Active",
            "new Namespace is not Active; retain original identity and inspect",
        )
        receipt_ns = self.create(
            "receipt-namespace",
            {"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": args.receipt_namespace}},
        )
        receipt_namespace_uid = object_uid(receipt_ns, "Namespace", args.receipt_namespace)
        observed_receipts = self.read("namespace", args.receipt_namespace)
        require(
            object_uid(observed_receipts, "Namespace", args.receipt_namespace)
            == receipt_namespace_uid
            and observed_receipts.get("status", {}).get("phase") == "Active",
            "new receipt Namespace changed or is not Active; retain original identity and inspect",
        )
        common = [
            "--namespace-uid", namespace_uid,
            "--receipt-namespace", args.receipt_namespace,
            "--receipt-namespace-uid", receipt_namespace_uid,
            "--worker-pool-id", args.worker_pool_id,
            "--runner-image", args.runner_image,
            "--generation", generation,
        ]
        for name in (
            "max-active-jobs",
            "max-active-jobs-per-requester",
            "worker-slots",
            "max-queued-jobs",
            "max-queued-jobs-per-requester",
        ):
            common.extend([f"--{name}", str(getattr(args, name.replace("-", "_")))])
        genesis_name = "kova-service-admission-genesis"
        manifest = self.proposal("render-genesis", common)
        require(
            manifest.get("kind") == "ConfigMap"
            and manifest.get("metadata", {}).get("name") == genesis_name
            and manifest["metadata"].get("namespace") == args.runner_namespace,
            "CLI proposed a different Genesis object",
        )
        contract = expected_contract(args, namespace_uid, receipt_namespace_uid, generation)
        genesis_data = {
            "contract": contract,
            "phase": "Initializing",
            "activeLedgerUID": "",
            "queueLedgerUID": "",
        }
        require(
            set(manifest.get("data", {})) == {"genesis.json"}
            and not manifest.get("binaryData")
            and not manifest.get("immutable"),
            "Genesis proposal has extra or immutable data",
        )
        require(
            json.loads(manifest["data"]["genesis.json"], object_pairs_hook=strict_object)
            == genesis_data,
            "Genesis proposal differs from original contract",
        )
        genesis = self.create("genesis", manifest)
        genesis_uid = object_uid(genesis, "ConfigMap", genesis_name, args.runner_namespace)
        require(
            genesis.get("data") == manifest["data"]
            and not genesis.get("binaryData")
            and not genesis.get("immutable"),
            "Genesis create response changed the proposal",
        )
        secret_name = "kova-admission-" + generation[:16]
        secret = self.proposal(
            "export-receipt-secret",
            common,
            [
                "--genesis-uid",
                genesis_uid,
                "--service-namespace",
                args.service_namespace,
                "--secret-name",
                secret_name,
            ],
        )
        require(
            secret.get("kind") == "Secret"
            and secret.get("metadata", {}).get("name") == secret_name
            and secret["metadata"].get("namespace") == args.service_namespace
            and secret.get("immutable") is True,
            "CLI proposed a different or mutable receipt Secret",
        )
        expected_receipt = {
            "namespace": args.runner_namespace,
            "genesisName": genesis_name,
            "genesisUID": genesis_uid,
            "contract": contract,
        }
        check_secret_data(secret, expected_receipt)
        created = self.create("receipt-secret", secret)
        secret_uid = object_uid(created, "Secret", secret_name, args.service_namespace)
        check_secret_data(created, expected_receipt, secret["data"])
        observed = self.read("secret", secret_name, args.service_namespace)
        require(
            object_uid(observed, "Secret", secret_name, args.service_namespace) == secret_uid
            and observed.get("immutable") is True
            and observed.get("data") == created.get("data"),
            "receipt Secret readback differs",
        )
        check_secret_data(observed, expected_receipt, secret["data"])
        self.check_cluster()
        require(
            object_uid(
                self.read("namespace", args.service_namespace), "Namespace", args.service_namespace
            )
            == service_uid,
            "Service namespace changed",
        )
        require(
            object_uid(
                self.read("namespace", args.runner_namespace), "Namespace", args.runner_namespace
            )
            == namespace_uid,
            "runner namespace changed",
        )
        require(
            object_uid(
                self.read("namespace", args.receipt_namespace), "Namespace", args.receipt_namespace
            )
            == receipt_namespace_uid,
            "receipt namespace changed",
        )
        private_json(
            self.directory / "identities.json",
            {
                "namespaceUID": namespace_uid,
                "receiptNamespaceUID": receipt_namespace_uid,
                "genesisUID": genesis_uid,
                "secretUID": secret_uid,
                "generation": generation,
                "status": "EXTERNALLY_CREATED_INITIALIZING",
            },
        )
        values = {
            "images": {
                "runner": {
                    "repository": args.runner_image.split("@", 1)[0],
                    "tag": "",
                    "digest": args.runner_image.split("@", 1)[1],
                },
            },
            "serviceDaemon": {
                "runnerNamespace": args.runner_namespace,
                "workerPoolID": args.worker_pool_id,
                "admissionGenesis": {
                    "enabled": True,
                    "recoveryReceiptNamespace": args.receipt_namespace,
                    "receiptSecret": {"name": secret_name, "uid": secret_uid},
                },
                "maxActiveJobs": args.max_active_jobs,
                "maxActiveJobsPerRequester": args.max_active_jobs_per_requester,
                "workerSlots": args.worker_slots,
                "maxQueuedJobs": args.max_queued_jobs,
                "maxQueuedJobsPerRequester": args.max_queued_jobs_per_requester,
            }
        }
        target = self.directory / "values.json"
        private_json(target, values)
        return target


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    for name in (
        "kubeconfig",
        "context",
        "kube-system-uid",
        "kova-cli",
        "runner-namespace",
        "receipt-namespace",
        "worker-pool-id",
        "runner-image",
        "service-namespace",
        "release",
        "output-directory",
        "acknowledge",
    ):
        result.add_argument(f"--{name}", required=True)
    for name in (
        "max-active-jobs",
        "max-active-jobs-per-requester",
        "worker-slots",
        "max-queued-jobs",
        "max-queued-jobs-per-requester",
    ):
        result.add_argument(f"--{name}", required=True, type=int)
    return result


def main() -> int:
    args = parser().parse_args()
    try:
        values = Installer(args).run()
    except (Stop, OSError, ValueError) as error:
        print(f"Genesis installation STOP: {type(error).__name__}: {error}", file=sys.stderr)
        print(
            f"Retain any evidence and objects for inspection: {args.output_directory}",
            file=sys.stderr,
        )
        return 1
    print(values)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
