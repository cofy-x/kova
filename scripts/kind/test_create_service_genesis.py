#!/usr/bin/env python3
"""Offline checks for the create-only, externally owned Kind installer."""

import base64
import importlib.util
import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

SPEC = importlib.util.spec_from_file_location(
    "installer", Path(__file__).with_name("create-service-genesis.py")
)
assert SPEC and SPEC.loader
installer = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(installer)


class FakeInstaller(installer.Installer):
    def __init__(self, args):
        super().__init__(args)
        self.objects = {
            ("namespace", "service"): {
                "kind": "Namespace",
                "metadata": {"name": "service", "uid": "service-original"},
                "status": {"phase": "Active"},
            },
            ("namespace", "kube-system"): {
                "kind": "Namespace",
                "metadata": {"name": "kube-system", "uid": "system-original"},
                "status": {"phase": "Active"},
            },
        }
        self.pods = []
        self.deployments = []
        self.writes = []
        self.unknown_create = None
        self.replace_secret = False
        self.wrong_proposal = False
        self.endpoint = "https://127.0.0.1:6443"
        self.proxy = ""
        self.mutate_secret = False
        self.wrong_receipt = False
        self.kind_node_valid = True

    def check_kind_node(self):
        installer.require(self.kind_node_valid, "API port is not the local Kind node")
        self.node_id = "a" * 64

    def kubectl(self, *args, payload=None):
        if args == ("config", "current-context"):
            return self.args.context
        if args[:3] == ("config", "view", "--minify"):
            return self.proxy if args[-1].endswith("proxy-url}") else self.endpoint
        if len(args) == 6 and args[0:2] == ("get", "namespace") and args[2] in (
            self.args.runner_namespace, self.args.receipt_namespace
        ) and args[3:] == ("--ignore-not-found", "-o", "json"):
            value = self.objects.get(("namespace", args[2]))
            return json.dumps(value) if value else ""
        if "pods" in args:
            return json.dumps({"items": self.pods})
        if "deployments" in args:
            return json.dumps({"items": self.deployments})
        if args[0] == "create":
            obj = json.loads(payload)
            self.writes.append(obj)
            kind = obj["kind"].lower()
            if kind == self.unknown_create:
                raise installer.Stop("unknown create result")
            obj["metadata"]["uid"] = (
                obj["metadata"]["name"] + "-original" if kind == "namespace" else kind + "-original"
            )
            if kind == "secret" and self.mutate_secret:
                obj["data"]["receipt.json"] = "e30="
            if kind == "namespace":
                obj["status"] = {"phase": "Active"}
            self.objects[(kind, obj["metadata"]["name"])] = obj
            return json.dumps(obj)
        raise AssertionError(args)

    def read(self, kind, name, namespace=None):
        obj = json.loads(json.dumps(self.objects[(kind, name)]))
        if kind == "secret" and self.replace_secret:
            obj["metadata"]["uid"] = "secret-replaced"
        return obj

    def proposal(self, subcommand, common, extra=None):
        contract = installer.expected_contract(
            self.args,
            common[common.index("--namespace-uid") + 1],
            common[common.index("--receipt-namespace-uid") + 1],
            common[common.index("--generation") + 1],
        )
        if subcommand == "render-genesis":
            return {
                "apiVersion": "v1",
                "kind": "ConfigMap",
                "metadata": {
                    "namespace": self.args.runner_namespace,
                    "name": "wrong" if self.wrong_proposal else "kova-service-admission-genesis",
                },
                "data": {
                    "genesis.json": json.dumps(
                        {
                            "contract": contract,
                            "phase": "Initializing",
                            "activeLedgerUID": "",
                            "queueLedgerUID": "",
                        }
                    )
                },
            }
        receipt = {
            "namespace": self.args.runner_namespace,
            "genesisName": "kova-service-admission-genesis",
            "genesisUID": "configmap-original",
            "contract": contract,
        }
        if self.wrong_receipt:
            receipt["genesisUID"] = "different-genesis"
        return {
            "apiVersion": "v1",
            "kind": "Secret",
            "metadata": {
                "namespace": self.args.service_namespace,
                "name": extra[extra.index("--secret-name") + 1],
            },
            "immutable": True,
            "type": "Opaque",
            "data": {
                "receipt.json": base64.b64encode(
                    json.dumps(receipt, separators=(",", ":")).encode()
                ).decode()
            },
        }


class InstallerTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        root = Path(self.tmp.name)
        cli = root / "kova"
        cli.write_text("test-only inert file")
        cli.chmod(0o700)
        kubeconfig = root / "config"
        kubeconfig.write_text("test-only inert config")
        self.args = installer.parser().parse_args(
            [
                "--kubeconfig",
                str(kubeconfig),
                "--kova-cli",
                str(cli),
                "--context",
                "kind-test",
                "--kube-system-uid",
                "system-original",
                "--runner-namespace",
                "jobs-new",
                "--receipt-namespace",
                "receipts-new",
                "--worker-pool-id",
                "kind-fixture/test-pool",
                "--runner-image",
                "localhost:5002/kova@sha256:" + "a" * 64,
                "--service-namespace",
                "service",
                "--release",
                "kova",
                "--output-directory",
                str(root / "evidence"),
                "--acknowledge",
                "kind-test/service/jobs-new/receipts-new",
                "--max-active-jobs",
                "20",
                "--max-active-jobs-per-requester",
                "4",
                "--worker-slots",
                "20",
                "--max-queued-jobs",
                "1000",
                "--max-queued-jobs-per-requester",
                "100",
            ]
        )

    def test_fresh_install_creates_only_four_exact_objects(self):
        fake = FakeInstaller(self.args)
        values_path = fake.run()
        values = json.loads(values_path.read_text())["serviceDaemon"]
        self.assertEqual([obj["kind"] for obj in fake.writes], ["Namespace", "Namespace", "ConfigMap", "Secret"])
        self.assertEqual(values["admissionGenesis"]["recoveryReceiptNamespace"], "receipts-new")
        self.assertEqual(values["admissionGenesis"]["receiptSecret"]["uid"], "secret-original")
        self.assertEqual(values["runnerNamespace"], "jobs-new")
        self.assertEqual(values["workerPoolID"], "kind-fixture/test-pool")
        self.assertEqual(json.loads(values_path.read_text())["images"]["runner"], {
            "repository": "localhost:5002/kova", "tag": "", "digest": "sha256:" + "a" * 64,
        })
        self.assertEqual(values["maxQueuedJobs"], 1000)
        self.assertEqual(os.stat(values_path).st_mode & 0o777, 0o600)
        self.assertEqual(os.stat(values_path.parent).st_mode & 0o777, 0o700)
        for artifact in values_path.parent.iterdir():
            self.assertEqual(os.stat(artifact).st_mode & 0o777, 0o600)

    def test_execution_identity_rejected_before_any_write(self):
        for field, value in (
            ("worker_pool_id", ""), ("worker_pool_id", "pool with spaces"),
            ("runner_image", "localhost:5002/kova:runner-dev"),
            ("runner_image", "sha256:" + "a" * 64),
            ("runner_image", "localhost:5002/kova@sha256:" + "A" * 64),
        ):
            with self.subTest(field=field, value=value):
                prior = getattr(self.args, field)
                setattr(self.args, field, value)
                with self.assertRaises(installer.Stop):
                    FakeInstaller(self.args)
                self.assertFalse(Path(self.args.output_directory).exists())
                setattr(self.args, field, prior)

    def test_existing_namespace_never_adopted(self):
        fake = FakeInstaller(self.args)
        fake.objects[("namespace", "jobs-new")] = {"metadata": {"uid": "old"}}
        with self.assertRaises(installer.Stop):
            fake.run()
        self.assertEqual(fake.writes, [])
        self.assertFalse(fake.directory.exists())

    def test_existing_receipt_namespace_never_adopted(self):
        fake = FakeInstaller(self.args)
        fake.objects[("namespace", "receipts-new")] = {"metadata": {"uid": "old"}}
        with self.assertRaises(installer.Stop):
            fake.run()
        self.assertEqual(fake.writes, [])
        self.assertFalse(fake.directory.exists())

    def test_old_service_pods_and_deployment_block_before_create(self):
        for field, value in (
            ("pods", [{"metadata": {"deletionTimestamp": "still-running"}}]),
            ("deployments", [{"spec": {"replicas": 1}}]),
        ):
            with self.subTest(field=field):
                fake = FakeInstaller(self.args)
                setattr(fake, field, value)
                with self.assertRaises(installer.Stop):
                    fake.run()
                self.assertEqual(fake.writes, [])

    def test_unknown_create_stops_without_retry_or_cleanup(self):
        fake = FakeInstaller(self.args)
        fake.unknown_create = "namespace"
        with self.assertRaises(installer.Stop):
            fake.run()
        self.assertEqual(len(fake.writes), 1)
        self.assertTrue((fake.directory / "01-namespace-request.json").exists())
        self.assertFalse((fake.directory / "values.json").exists())

    def test_secret_replacement_never_yields_installable_values(self):
        fake = FakeInstaller(self.args)
        fake.replace_secret = True
        with self.assertRaises(installer.Stop):
            fake.run()
        self.assertEqual(len(fake.writes), 4)
        self.assertFalse((fake.directory / "values.json").exists())

    def test_wrong_rendered_object_not_created(self):
        fake = FakeInstaller(self.args)
        fake.wrong_proposal = True
        with self.assertRaises(installer.Stop):
            fake.run()
        self.assertEqual(len(fake.writes), 2)

    def test_remote_endpoint_or_proxy_rejected_before_create(self):
        for key, value in (
            ("endpoint", "https://api.example.com:6443"),
            ("endpoint", "http://127.0.0.1:6443"),
            ("endpoint", "https://127.0.0.2:6443"),
            ("endpoint", "https://[::1]:6443"),
            ("endpoint", "https://localhost:6443"),
            ("proxy", "https://proxy.example.com"),
        ):
            with self.subTest(key=key, value=value):
                fake = FakeInstaller(self.args)
                setattr(fake, key, value)
                with self.assertRaises(installer.Stop):
                    fake.run()
                self.assertEqual(fake.writes, [])

    def test_cluster_uid_replacement_rejected_before_create(self):
        fake = FakeInstaller(self.args)
        fake.objects[("namespace", "kube-system")]["metadata"]["uid"] = "another-cluster"
        with self.assertRaises(installer.Stop):
            fake.run()
        self.assertEqual(fake.writes, [])

    def test_loopback_tunnel_is_not_a_local_kind_node(self):
        fake = FakeInstaller(self.args)
        fake.kind_node_valid = False
        with self.assertRaises(installer.Stop):
            fake.run()
        self.assertEqual(fake.writes, [])

    def test_wrong_receipt_proposal_never_written(self):
        fake = FakeInstaller(self.args)
        fake.wrong_receipt = True
        with self.assertRaises(installer.Stop):
            fake.run()
        self.assertEqual(len(fake.writes), 3)
        self.assertFalse((fake.directory / "values.json").exists())

    def test_mutating_receipt_response_never_yields_values(self):
        fake = FakeInstaller(self.args)
        fake.mutate_secret = True
        with self.assertRaises(installer.Stop):
            fake.run()
        self.assertEqual(len(fake.writes), 4)
        self.assertFalse((fake.directory / "values.json").exists())

    def test_explicit_ack_context_and_namespace_are_required(self):
        for key, value in (
            ("acknowledge", "wrong"),
            ("context", "production"),
            ("runner_namespace", "service"),
            ("receipt_namespace", "jobs-new"),
            ("max_queued_jobs", 1001),
        ):
            with self.subTest(key=key), patch.object(self.args, key, value):
                with self.assertRaises(installer.Stop):
                    FakeInstaller(self.args)

    def test_existing_evidence_and_changed_input_fail_before_command(self):
        fake = FakeInstaller(self.args)
        Path(self.args.kubeconfig).write_text("changed")
        with patch.object(installer.subprocess, "run") as run, self.assertRaises(installer.Stop):
            fake.execute(["not-run"])
        run.assert_not_called()
        fake.directory.mkdir()
        with self.assertRaises(installer.Stop):
            FakeInstaller(self.args)


if __name__ == "__main__":
    unittest.main()
