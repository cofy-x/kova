"""Cluster-free safety checks for the disposable real-runner #41 acceptance."""

from __future__ import annotations

import hashlib
import http.client
import importlib.util
import io
import json
import os
import sys
import tempfile
import threading
import unittest
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from unittest.mock import patch
from urllib.error import HTTPError

HERE = Path(__file__).parent
sys.path.insert(0, str(HERE))
SPEC = importlib.util.spec_from_file_location(
    "e2e_service_digest_collision", HERE / "e2e-service-digest-collision.py"
)
assert SPEC and SPEC.loader
acceptance = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(acceptance)
RUN = "digest-41-20260927t000000z-deadbeef"
FIRST, SECOND = acceptance.expected_targets(RUN)
REPOSITORY = f"kova-examples/{RUN}-a"
FAILED_REPOSITORY = f"kova-examples/{RUN}-z"
VERIFIER = acceptance.VerifierIdentity(
    "kova-service-test", "11111111-1111-4111-8111-111111111111", "10.244.1.7", "deployment-uid"
)


def pending_build(*, failed: bool, prefix: str) -> dict:
    started = datetime(2026, 9, 27, tzinfo=timezone.utc)  # noqa: UP017 (Python 3.10)
    results = [
        {
            "image": FIRST,
            "format": "oci",
            "platform": "linux/amd64",
            "state": "pending",
            "pushedDigest": "sha256:" + prefix * 64,
        },
        {
            "image": FIRST.replace(":dev", ":dev_nydus_v3"),
            "format": "nydus",
            "platform": "linux/amd64",
            "state": "pending",
            "pushedDigest": "sha256:" + ("b" if prefix == "a" else "d") * 64,
        },
    ]
    if failed:
        results.extend(
            [
                {"image": SECOND, "format": "oci", "state": "failed"},
                {
                    "image": SECOND.replace(":dev", ":dev_nydus_v3"),
                    "format": "nydus",
                    "state": "failed",
                },
            ]
        )
    return {
        "metadata": {"name": "idem-" + prefix * 20},
        "status": {
            "phase": "FailedVerifying" if failed else "Verifying",
            "reason": "BuildFailed" if failed else "",
            "verificationAttempts": 1,
            "verificationLastError": "registry manifest verification unavailable",
            "verificationDeadlineAt": (started + timedelta(minutes=5)).isoformat(),
            "verificationResults": results,
            "outputs": [],
        },
    }


class ReceiptSafetyTest(unittest.TestCase):
    def test_wait_pending_tolerates_inflight_verification_status(self) -> None:
        transient = pending_build(failed=True, prefix="a")
        transient["status"].pop("verificationLastError")
        settled = pending_build(failed=True, prefix="a")
        for build in (transient, settled):
            build["metadata"]["uid"] = "build-uid"
        identity = (
            transient["metadata"]["name"],
            "build-uid",
            "source-uri",
            "source-digest",
            [FIRST, SECOND],
            "idempotency-key",
        )
        with (
            tempfile.TemporaryDirectory() as temporary,
            patch.object(acceptance, "exact_build", side_effect=[transient, settled]),
            patch.object(acceptance, "guard_running"),
            patch.object(acceptance.time, "sleep"),
        ):
            build, digests = acceptance.wait_pending(
                Path(temporary), "a", {}, identity, FIRST, SECOND, {}
            )
        self.assertEqual(build, settled)
        self.assertEqual(len(digests), 2)

    def test_wait_pending_rejects_stale_incomplete_verification_status(self) -> None:
        stale = pending_build(failed=True, prefix="a")
        stale["metadata"]["uid"] = "build-uid"
        stale["status"].pop("verificationLastError")
        identity = (
            stale["metadata"]["name"],
            "build-uid",
            "source-uri",
            "source-digest",
            [FIRST, SECOND],
            "idempotency-key",
        )
        with (
            tempfile.TemporaryDirectory() as temporary,
            patch.object(acceptance, "exact_build", return_value=stale),
            patch.object(acceptance, "guard_running"),
            patch.object(acceptance.time, "sleep"),
            patch.object(acceptance.time, "monotonic", side_effect=[0, 1, 2, 3, 4, 5, 33, 34]),
        ):
            with self.assertRaisesRegex(
                acceptance.base.AcceptanceError, "not durably pending"
            ):
                acceptance.wait_pending(
                    Path(temporary), "a", {}, identity, FIRST, SECOND, {}
                )

    def test_first_and_second_pending_receipts_are_distinct(self) -> None:
        first = pending_build(failed=True, prefix="a")
        second = pending_build(failed=False, prefix="c")
        pushed_a = acceptance.pending_digests(first, FIRST, SECOND)
        pushed_b = acceptance.pending_digests(second, FIRST, None)
        self.assertEqual(set(pushed_a), set(pushed_b))
        self.assertTrue(all(pushed_a[image] != pushed_b[image] for image in pushed_a))

    def test_pending_receipt_fails_closed_without_own_pushed_digest(self) -> None:
        build = pending_build(failed=True, prefix="a")
        build["status"]["verificationResults"][0]["pushedDigest"] = ""
        with self.assertRaises(acceptance.base.AcceptanceError):
            acceptance.pending_digests(build, FIRST, SECOND)
        build = pending_build(failed=True, prefix="a")
        build["status"]["phase"] = "Failed"
        with self.assertRaises(acceptance.base.AcceptanceError):
            acceptance.pending_digests(build, FIRST, SECOND)

    def test_successful_second_build_keeps_its_own_two_digests(self) -> None:
        build = pending_build(failed=False, prefix="c")
        build["status"].update(
            {"phase": "Succeeded", "finishedAt": "2026-09-27T00:01:00Z", "verificationAttempts": 2}
        )
        for item in build["status"]["verificationResults"]:
            item["state"] = "succeeded"
        build["status"]["outputs"] = [
            {
                "image": item["image"],
                "format": item["format"],
                "platform": item["platform"],
                "manifestDigest": item["pushedDigest"],
            }
            for item in build["status"]["verificationResults"]
        ]
        self.assertEqual(
            acceptance.verify_succeeded_receipt(build, FIRST),
            {
                item["image"]: item["pushedDigest"]
                for item in build["status"]["verificationResults"]
            },
        )
        build["status"]["outputs"][0]["manifestDigest"] = "sha256:" + "e" * 64
        with self.assertRaises(acceptance.base.AcceptanceError):
            acceptance.verify_succeeded_receipt(build, FIRST)

    def test_second_archive_changes_the_same_target_payload(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            archive = acceptance.make_b_archive(Path(temporary), FIRST, RUN)
            import zipfile

            with zipfile.ZipFile(archive) as source:
                self.assertEqual(len(source.namelist()), 3)
                self.assertEqual(json.loads(source.read("a-pass/metadata.json"))["target"], FIRST)
                self.assertIn(b"second overwrite build", source.read("a-pass/payload"))


class ProxyIdentityTest(unittest.TestCase):
    def test_proxy_contract_pins_both_repositories_and_verifier_env(self) -> None:
        identity = acceptance.ProxyIdentity(
            "proxy-deployment-uid",
            "proxy-service-uid",
            "proxy-config-uid",
            "code-sha",
            VERIFIER,
            REPOSITORY,
            FAILED_REPOSITORY,
        )
        env = [
            {"name": "COLLISION_REPOSITORY", "value": REPOSITORY},
            {"name": "COLLISION_FAILED_REPOSITORY", "value": FAILED_REPOSITORY},
            {"name": "COLLISION_VERIFIER_POD_UID", "value": VERIFIER.pod_uid},
            {"name": "COLLISION_VERIFIER_POD_IP", "value": VERIFIER.pod_ip},
        ]
        container = {"name": "proxy", "image": acceptance.PROXY_IMAGE, "env": env}
        deployment = {
            "metadata": {"uid": identity.deployment_uid},
            "spec": {
                "replicas": 1,
                "template": {
                    "metadata": {"annotations": {"kova.cofy.dev/fault-mode-token": "initial"}},
                    "spec": {"containers": [container]},
                },
            },
        }
        pod = {
            "metadata": {
                "uid": "proxy-pod-uid",
                "ownerReferences": [
                    {
                        "name": "proxy-rs",
                        "uid": "proxy-rs-uid",
                        "kind": "ReplicaSet",
                        "controller": True,
                    }
                ],
            },
            "spec": {"containers": [dict(container, env=[dict(item) for item in env])]},
            "status": {
                "containerStatuses": [
                    {"name": "proxy", "imageID": "sha256:test", "restartCount": 0}
                ],
                "conditions": [{"type": "Ready", "status": "True"}],
            },
        }
        replica = {
            "metadata": {
                "uid": "proxy-rs-uid",
                "ownerReferences": [
                    {"uid": identity.deployment_uid, "kind": "Deployment", "controller": True}
                ],
            }
        }

        def selected(*args: str) -> dict:
            if "deployment" in args:
                return deployment
            if "replicaset" in args:
                return replica
            return {"items": [pod]}

        with patch.object(acceptance.base, "kjson", side_effect=selected):
            self.assertEqual(
                acceptance.proxy_pod(identity, "initial")["metadata"]["uid"], "proxy-pod-uid"
            )
            for name in ("COLLISION_REPOSITORY", "COLLISION_FAILED_REPOSITORY"):
                chosen = next(item for item in env if item["name"] == name)
                original = chosen["value"]
                chosen["value"] = "kova-examples/other"
                with self.assertRaises(acceptance.base.AcceptanceError):
                    acceptance.proxy_pod(identity, "initial")
                chosen["value"] = original
            pod["spec"]["containers"][0]["env"][3]["value"] = "10.244.1.8"
            with self.assertRaises(acceptance.base.AcceptanceError):
                acceptance.proxy_pod(identity, "initial")

    def test_verifier_source_requires_exact_owned_pod_uid_and_unique_ip(self) -> None:
        pod = {
            "metadata": {
                "name": VERIFIER.pod_name,
                "namespace": acceptance.NAMESPACE,
                "uid": VERIFIER.pod_uid,
                "ownerReferences": [
                    {
                        "name": "service-rs",
                        "uid": "rs-uid",
                        "kind": "ReplicaSet",
                        "controller": True,
                    }
                ],
            },
            "spec": {"hostNetwork": False},
            "status": {
                "phase": "Running",
                "podIP": VERIFIER.pod_ip,
                "podIPs": [{"ip": VERIFIER.pod_ip}],
                "conditions": [{"type": "Ready", "status": "True"}],
                "containerStatuses": [{"name": "kova-service", "restartCount": 0}],
            },
        }
        deployment = {"metadata": {"name": "kova-service", "uid": VERIFIER.deployment_uid}}
        replica = {
            "metadata": {
                "uid": "rs-uid",
                "ownerReferences": [
                    {
                        "name": "kova-service",
                        "uid": VERIFIER.deployment_uid,
                        "kind": "Deployment",
                        "controller": True,
                    }
                ],
            }
        }

        def selected(*args: str) -> dict:
            if "deployment" in args:
                return deployment
            if "replicaset" in args:
                return replica
            if "pods" in args:
                return {"items": [pod]}
            return pod

        with patch.object(acceptance.base, "kjson", side_effect=selected):
            service_fact = {"pod": VERIFIER.pod_name, "uid": VERIFIER.pod_uid}
            self.assertEqual(acceptance.verifier_pod_identity(service_fact), VERIFIER)
            pod["status"]["podIP"] = "10.244.1.8"
            pod["status"]["podIPs"] = [{"ip": "10.244.1.8"}]
            with self.assertRaises(acceptance.base.AcceptanceError):
                acceptance.verifier_pod_identity(service_fact, VERIFIER)
            pod["status"]["podIP"] = VERIFIER.pod_ip
            pod["status"]["podIPs"] = [{"ip": VERIFIER.pod_ip}]
            pod["metadata"]["uid"] = "22222222-2222-4222-8222-222222222222"
            with self.assertRaises(acceptance.base.AcceptanceError):
                acceptance.verifier_pod_identity(service_fact, VERIFIER)
            pod["metadata"]["uid"] = VERIFIER.pod_uid
            with patch.object(
                acceptance.base,
                "kjson",
                side_effect=lambda *args: (
                    {"items": [pod, dict(pod)]} if "pods" in args else selected(*args)
                ),
            ):
                with self.assertRaises(acceptance.base.AcceptanceError):
                    acceptance.verifier_pod_identity(service_fact, VERIFIER)

    def test_orphan_proxy_pod_or_replicaset_blocks_preflight(self) -> None:
        with patch.object(acceptance.base, "kctl", return_value=""):
            for orphan in ("pods", "replicasets"):

                def selected(*args: str) -> dict:
                    return (
                        {"items": [{"metadata": {"uid": "orphan"}}]}
                        if orphan in args
                        else {"items": []}
                    )

                with (
                    self.subTest(orphan=orphan),
                    patch.object(acceptance.base, "kjson", side_effect=selected),
                    self.assertRaises(acceptance.base.AcceptanceError),
                ):
                    acceptance.assert_proxy_absent()

    def test_proxy_contract_pins_service_and_configmap(self) -> None:
        code = (HERE / "registry-digest-collision-proxy.py").read_text(encoding="utf-8")
        identity = acceptance.ProxyIdentity(
            "deployment-uid",
            "service-uid",
            "config-uid",
            acceptance.base.sha256(code.encode()),
            VERIFIER,
            REPOSITORY,
            FAILED_REPOSITORY,
        )
        config = {
            "metadata": {"uid": identity.config_uid},
            "data": {"mode": "503", "proxy.py": code},
        }
        service = {
            "metadata": {"uid": identity.service_uid},
            "spec": {
                "type": "ClusterIP",
                "selector": {"kova.cofy.dev/e2e": "digest-collision-proxy"},
                "ports": [{"name": "http", "port": 5000, "targetPort": 5000, "protocol": "TCP"}],
            },
        }

        def get_object(*args: str) -> dict:
            return config if "configmap" in args else service

        with (
            patch.object(acceptance.base, "kjson", side_effect=get_object),
            patch.object(acceptance, "verifier_pod_identity", return_value=VERIFIER),
            patch.object(acceptance, "proxy_pod", return_value={"metadata": {"uid": "pod-uid"}}),
        ):
            self.assertEqual(
                acceptance.check_proxy_contract(identity, "503")["metadata"]["uid"], "pod-uid"
            )
            with self.assertRaises(acceptance.base.AcceptanceError):
                acceptance.check_proxy_contract(identity._replace(proxy_pod_uid="other"), "503")
            config["data"]["proxy.py"] = "changed"
            with self.assertRaises(acceptance.base.AcceptanceError):
                acceptance.check_proxy_contract(identity, "503")
            config["data"]["proxy.py"] = code
            service["spec"]["selector"] = {"kova.cofy.dev/e2e": "other"}
            with self.assertRaises(acceptance.base.AcceptanceError):
                acceptance.check_proxy_contract(identity, "503")
            service["spec"]["selector"] = {"kova.cofy.dev/e2e": "digest-collision-proxy"}
            service["metadata"]["uid"] = "different"
            with self.assertRaises(acceptance.base.AcceptanceError):
                acceptance.check_proxy_contract(identity, "503")

    def test_direct_proxy_probe_requires_exact_fault_and_manifest(self) -> None:
        repository = f"kova-examples/{RUN}-a"
        manifest = b'{"schemaVersion":2,"config":{"digest":"sha256:example"}}'
        digest = "sha256:" + hashlib.sha256(manifest).hexdigest()
        identity = acceptance.ProxyIdentity(
            "deployment", "service", "config", "code", VERIFIER, REPOSITORY, FAILED_REPOSITORY
        )

        class Response:
            status = 200
            headers = {"Docker-Content-Digest": digest}

            def __enter__(self) -> Response:
                return self

            def __exit__(self, *_args: object) -> None:
                return None

            def read(self, _length: int) -> bytes:
                return manifest

        with (
            tempfile.TemporaryDirectory() as temporary,
            patch.object(
                acceptance, "check_proxy_contract", return_value={"metadata": {"uid": "pod-uid"}}
            ),
            patch.object(acceptance.base, "start_port_forward", return_value=(None, 5000, None)),
            patch.object(acceptance.base, "stop_process"),
            patch.object(
                acceptance.base.HTTP, "open", side_effect=[Response(), Response()]
            ) as opened,
        ):
            run_dir = Path(temporary)
            acceptance.probe_proxy_manifest(
                run_dir, identity, "503", "a-forwarded", repository, digest, 200
            )
            acceptance.probe_proxy_manifest(
                run_dir, identity, "healthy", "a-healthy", repository, digest, 200
            )
            self.assertEqual(
                json.loads((run_dir / "proxy-probe-a-forwarded.json").read_text())["http_status"],
                200,
            )
            self.assertEqual(
                json.loads((run_dir / "proxy-probe-a-healthy.json").read_text())["http_status"], 200
            )
            for call in opened.call_args_list:
                self.assertEqual(call.args[0].get_header("Host"), acceptance.PROXY_HOST)
            with self.assertRaises(acceptance.base.AcceptanceError):
                acceptance.probe_proxy_manifest(
                    run_dir, identity, "healthy", "outside", "kova-examples/unrelated", digest, 200
                )

    def test_direct_proxy_probe_rejects_unattributed_503(self) -> None:
        repository = f"kova-examples/{RUN}-a"
        digest = "sha256:" + "a" * 64
        fault = HTTPError(
            f"http://127.0.0.1:5000/v2/{repository}/manifests/{digest}",
            503,
            "unattributed",
            {},
            io.BytesIO(b"backend unavailable"),
        )
        identity = acceptance.ProxyIdentity(
            "deployment", "service", "config", "code", VERIFIER, REPOSITORY, FAILED_REPOSITORY
        )
        with (
            tempfile.TemporaryDirectory() as temporary,
            patch.object(
                acceptance, "check_proxy_contract", return_value={"metadata": {"uid": "pod-uid"}}
            ),
            patch.object(acceptance.base, "start_port_forward", return_value=(None, 5000, None)),
            patch.object(acceptance.base, "stop_process"),
            patch.object(acceptance.base.HTTP, "open", side_effect=fault),
            self.assertRaises(acceptance.base.AcceptanceError),
        ):
            acceptance.probe_proxy_manifest(
                Path(temporary), identity, "503", "wrong-fault", repository, digest, 200
            )


class RegistryProxyTest(unittest.TestCase):
    def test_only_exact_repository_digest_get_faults_and_writes_pass(self) -> None:
        repository = f"kova-examples/{RUN}-a"
        failed_repository = f"kova-examples/{RUN}-z"
        proxy_path = HERE / "registry-digest-collision-proxy.py"
        with tempfile.TemporaryDirectory() as temporary:
            mode_file = Path(temporary) / "mode"
            mode_file.write_text("503\n")
            manifest = b'{"schemaVersion":2,"config":{"digest":"sha256:example"}}'
            digest = "sha256:" + hashlib.sha256(manifest).hexdigest()

            class Backend(BaseHTTPRequestHandler):
                protocol_version = "HTTP/1.1"

                def reply(self, code: int, body: bytes = b"", **headers: str) -> None:
                    self.send_response(code)
                    for key, value in headers.items():
                        self.send_header(key, value)
                    self.send_header("Content-Length", str(len(body)))
                    self.end_headers()
                    if self.command != "HEAD":
                        self.wfile.write(body)

                def do_POST(self) -> None:  # noqa: N802
                    self.reply(
                        202,
                        Location=f"http://127.0.0.1:{self.server.server_port}/v2/{repository}/blobs/uploads/12345678-1234-1234-1234-123456789abc",
                    )

                def do_PUT(self) -> None:  # noqa: N802
                    length = int(self.headers.get("Content-Length", "0"))
                    body = self.rfile.read(length)
                    if "/blobs/uploads/" in self.path:
                        self.reply(201, **{"Docker-Content-Digest": "sha256:" + "f" * 64})
                        return
                    if body != manifest:
                        self.reply(400)
                        return
                    self.reply(201, **{"Docker-Content-Digest": digest})

                def do_PATCH(self) -> None:  # noqa: N802
                    length = int(self.headers.get("Content-Length", "0"))
                    if self.rfile.read(length) != b"small-layer":
                        self.reply(400)
                        return
                    self.reply(202)

                def do_HEAD(self) -> None:  # noqa: N802
                    self.reply(200, manifest, **{"Docker-Content-Digest": digest})

                def do_GET(self) -> None:  # noqa: N802
                    self.reply(200, manifest, **{"Docker-Content-Digest": digest})

                def log_message(self, *_args: object) -> None:
                    pass

            backend = ThreadingHTTPServer(("127.0.0.1", 0), Backend)
            backend_thread = threading.Thread(target=backend.serve_forever, daemon=True)
            backend_thread.start()
            try:
                with patch.dict(
                    os.environ,
                    {
                        "COLLISION_REPOSITORY": repository,
                        "COLLISION_FAILED_REPOSITORY": failed_repository,
                        "COLLISION_VERIFIER_POD_UID": VERIFIER.pod_uid,
                        "COLLISION_VERIFIER_POD_IP": VERIFIER.pod_ip,
                    },
                ):
                    spec = importlib.util.spec_from_file_location(
                        "collision_registry_proxy", proxy_path
                    )
                    assert spec and spec.loader
                    proxy = importlib.util.module_from_spec(spec)
                    spec.loader.exec_module(proxy)
                proxy.BACKEND_HOST = "127.0.0.1"
                proxy.BACKEND_PORT = backend.server_port
                proxy.MODE_FILE = mode_file
                self.assertTrue(proxy.valid_repositories())
                self.assertTrue(proxy.valid_verifier_identity())

                class PeerServer(ThreadingHTTPServer):
                    peer_ip = "127.0.0.1"

                    def finish_request(self, request: object, client_address: tuple) -> None:
                        super().finish_request(request, (self.peer_ip, client_address[1]))

                server = PeerServer(("127.0.0.1", 0), proxy.Handler)
                thread = threading.Thread(target=server.serve_forever, daemon=True)
                thread.start()
                try:

                    def request(
                        method: str, path: str, body: bytes = b"", *, forged_ip: str = ""
                    ) -> tuple[int, bytes, dict]:
                        conn = http.client.HTTPConnection(
                            "127.0.0.1", server.server_port, timeout=3
                        )
                        try:
                            headers = {"Host": proxy.EXPECTED_HOST}
                            if forged_ip:
                                headers["X-Forwarded-For"] = forged_ip
                            conn.request(method, path, body=body, headers=headers)
                            response = conn.getresponse()
                            return response.status, response.read(), dict(response.getheaders())
                        finally:
                            conn.close()

                    status, _, headers = request("POST", f"/v2/{repository}/blobs/uploads/")
                    self.assertEqual(status, 202)
                    self.assertIn(proxy.EXPECTED_HOST, headers["Location"])
                    upload_path = headers["Location"].split(proxy.EXPECTED_HOST, 1)[1]
                    self.assertEqual(request("PATCH", upload_path, b"small-layer")[0], 202)
                    self.assertEqual(
                        request("PUT", upload_path + "?digest=sha256:" + "f" * 64)[0], 201
                    )
                    self.assertEqual(request("GET", "/v2/")[0], 200)
                    self.assertEqual(
                        request("PUT", f"/v2/{repository}/manifests/dev", manifest)[0], 201
                    )
                    status, body, headers = request("HEAD", f"/v2/{repository}/manifests/dev")
                    self.assertEqual(
                        (status, body, headers["Content-Length"]),
                        (200, b"", str(len(manifest))),
                    )
                    self.assertEqual(
                        request(
                            "GET",
                            f"/v2/{repository}/manifests/{digest}",
                            forged_ip=VERIFIER.pod_ip,
                        )[0],
                        200,
                    )
                    server.peer_ip = VERIFIER.pod_ip
                    status, body, _ = request("GET", f"/v2/{repository}/manifests/{digest}")
                    self.assertEqual((status, body), (503, b"test-only digest verification fault"))
                    server.peer_ip = "127.0.0.1"
                    self.assertEqual(request("GET", f"/v2/other/manifests/{digest}")[0], 404)
                    self.assertEqual(
                        request("DELETE", f"/v2/{repository}/manifests/{digest}")[0], 405
                    )
                    mode_file.write_text("only:" + digest + ",sha256:" + "a" * 64 + "\n")
                    server.peer_ip = VERIFIER.pod_ip
                    self.assertEqual(request("GET", f"/v2/{repository}/manifests/{digest}")[0], 503)
                    self.assertEqual(
                        request("GET", f"/v2/{repository}/manifests/sha256:{'c' * 64}")[0],
                        200,
                    )
                    mode_file.write_text("healthy\n")
                    status, body, headers = request("GET", f"/v2/{repository}/manifests/{digest}")
                    self.assertEqual(
                        (status, body, headers["Docker-Content-Digest"]), (200, manifest, digest)
                    )
                    server.peer_ip = "127.0.0.1"
                    status, body, _ = request("GET", "/fault/observations")
                    self.assertEqual(status, 200)
                    observed = json.loads(body)
                    self.assertEqual(observed["repository"], repository)
                    self.assertEqual(observed["failed_repository"], failed_repository)
                    self.assertEqual(observed["verifier_pod_uid"], VERIFIER.pod_uid)
                    self.assertEqual(observed["verifier_pod_ip"], VERIFIER.pod_ip)
                    self.assertFalse(observed["overflow"])
                    self.assertTrue(
                        any(
                            event["peer_ip"] == VERIFIER.pod_ip
                            and event["path"] == f"/v2/{repository}/manifests/{digest}"
                            and event["method"] == "GET"
                            and event["repository"] == repository
                            and event["digest"] == digest
                            and event["action"] == "fault"
                            and event["status"] == 503
                            for event in observed["events"]
                        )
                    )
                    self.assertTrue(
                        any(
                            event["peer_ip"] == "127.0.0.1"
                            and event["digest"] == digest
                            and event["action"] == "forwarded"
                            and event["status"] == 200
                            for event in observed["events"]
                        )
                    )
                finally:
                    server.shutdown()
                    server.server_close()
                    thread.join(timeout=3)
            finally:
                backend.shutdown()
                backend.server_close()
                backend_thread.join(timeout=3)


if __name__ == "__main__":
    unittest.main()
