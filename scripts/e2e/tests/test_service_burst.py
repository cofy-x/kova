"""Pure-local #51 burst-coordinator contract and unknown-outcome tests."""

from __future__ import annotations

import argparse
import http.server
import importlib.util
import io
import json
import subprocess
import sys
import tempfile
import threading
import time
import unittest
import zipfile
from pathlib import Path
from unittest.mock import MagicMock, patch
from urllib.request import Request

SCRIPT = Path(__file__).resolve().parents[1] / "e2e-service-burst.py"
SPEC = importlib.util.spec_from_file_location("service_burst", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
burst = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(burst)

RUN = "i51-abcdefgh"
UUIDS = [f"00000000-0000-0000-0000-{i:012d}" for i in range(1, 23)]
DIGEST = "sha256:" + "a" * 64
REPOSITORY = f"kind-registry-{RUN}:5000/issue51-{RUN}"


def plan() -> dict:
    return {
        "schema_version": 2,
        "run_id": RUN,
        "candidate": {
            "version": "v1.0.0-rc.1",
            "kova_commit": "a" * 40,
            "cli_sha256": "c" * 64,
            "chart_sha256": "d" * 64,
            "values_sha256": "e" * 64,
            "source_template_sha256": "f" * 64,
            "base_image": "example/base@" + DIGEST,
            "role_images": {
                "controller": "example/controller@" + DIGEST,
                "runner": "example/runner@sha256:" + "b" * 64,
                "worker": "example/worker@sha256:" + "c" * 64,
            },
        },
        "fixture": {
            "host": "isolated-builder-1",
            "host_boot_id": UUIDS[0],
            "context": f"kind-kova-issue51-{RUN}",
            "kubeconfig_sha256": "1" * 64,
            "kube_system_namespace_uid": UUIDS[1],
            "control_namespace": f"kova-control-{RUN}",
            "control_namespace_uid": UUIDS[2],
            "runner_namespace": f"kova-runner-{RUN}",
            "runner_namespace_uid": UUIDS[3],
            "receipt_namespace": f"kova-receipts-{RUN}",
            "receipt_namespace_uid": UUIDS[20],
            "service_deployment_uid": UUIDS[4],
            "worker_deployment_uid": UUIDS[5],
            "worker_service_uid": UUIDS[19],
            "worker_configmap_uid": UUIDS[21],
            "worker_config_sha256": "9" * 64,
            "service_deployment_generation": 1,
            "worker_deployment_generation": 1,
            "service_deployment_template_sha256": "6" * 64,
            "worker_deployment_template_sha256": "7" * 64,
            "service_pod_uids": UUIDS[6:8],
            "worker_pod_uids": UUIDS[8:11],
            "registry_repository": REPOSITORY,
            "registry_instance_sha256": "2" * 64,
            "registry_image": "registry:2@" + DIGEST,
            "registry_image_id": "sha256:" + "d" * 64,
            "registry_volume_created_at": "2026-10-02T00:00:00Z",
            "registry_volume_mountpoint": "/var/lib/docker/volumes/issue51/_data",
            "registry_network_id": "8" * 64,
            "repository_owner_run_id": RUN,
        },
        "limits": {
            "service_replicas": 2,
            "worker_replicas": 3,
            "max_active_jobs": 12,
            "max_active_jobs_per_requester": 12,
            "worker_slots": 12,
            "max_queued_jobs": 1000,
            "max_queued_jobs_per_requester": 1000,
            "controller_concurrency": 8,
            "kube_client_qps": 20,
            "kube_client_burst": 40,
            "leader_qps": 5,
            "leader_burst": 10,
            "readiness_qps": 5,
            "readiness_burst": 10,
        },
        "measurement": {
            "modes": ["cached", "uncached"],
            "levels": [8, 12],
            "warmup_jobs": 1,
            "measured_jobs": 40,
            "format": "oci",
            "platform": "linux/amd64",
            "payload_mib": 32,
            "cache_policy": "one_shared_cached_key_unique_uncached_keys",
        },
        "budget_seconds": {
            "injection": 1500,
            "cleanup": 300,
            "source_prepare": 300,
            "warmup": 120,
            "each_wave": 240,
        },
        "telemetry": {
            "counter_schema_sha256": "3" * 64,
            "required_counters": list(burst.REQUIRED_COUNTERS),
        },
    }


def sources(jobs: list[dict]) -> dict:
    entries = []
    for i, job in enumerate(jobs):
        shared = job["mode"] != "uncached"
        entries.append(
            {
                **{
                    key: job[key]
                    for key in ("mode", "concurrency", "ordinal", "source_tag", "output_tag")
                },
                "source_uri": "oci://"
                + job["source_tag"].rsplit(":", 1)[0]
                + "@sha256:"
                + f"{i + 1:064x}",
                "source_digest": "sha256:" + f"{i + 100:064x}",
                "cache_cohort": (
                    "shared-cached" if shared else f"uncached-{job['concurrency']}-{job['ordinal']}"
                ),
                "payload_member": "image/payload.bin",
                "payload_sha256": "4" * 64 if shared else f"{i + 200:064x}",
            }
        )
    return {"schema_version": 1, "run_id": RUN, "jobs": entries}


def runtime(plan_sha: str, sources_sha: str) -> dict:
    return {
        "schema_version": 2,
        "run_id": RUN,
        "plan_sha256": plan_sha,
        "sources_sha256": sources_sha,
        "service_deployment_name": "kova-service",
        "worker_deployment_name": "kova-worker",
        "worker_service_name": "kova-worker",
        "worker_service_port": 9094,
        "worker_configmap_name": "kova-config",
        "service_pods": [{"name": f"service-{i}", "uid": uid} for i, uid in enumerate(UUIDS[6:8])],
        "worker_pods": [{"name": f"worker-{i}", "uid": uid} for i, uid in enumerate(UUIDS[8:11])],
        "registry_container_name": f"kind-registry-{RUN}",
        "registry_volume_name": f"kova-registry-{RUN}",
        "registry_network_name": "kind",
        "registry_url": "http://127.0.0.1:25000",
        "requester": "kova:e2e",
        "requester_uid": "",
        "service_port": 8080,
        "metrics_port": 8081,
        "genesis": {
            "secret_namespace": f"kova-control-{RUN}",
            "secret_name": "admission-receipt",
            "secret_uid": UUIDS[11],
            "receipt_sha256": "5" * 64,
            "genesis_uid": UUIDS[12],
            "active_ledger_uid": UUIDS[13],
            "queue_ledger_uid": UUIDS[14],
            "worker_pool_id": f"worker-pool-{RUN}",
        },
    }


def build(entry: dict, rt: dict, uid: str = UUIDS[15]) -> dict:
    return {
        "apiVersion": "kova.cofy.dev/v1alpha1",
        "kind": "KovaBuild",
        "metadata": {
            "name": burst.expected_job_id(rt["requester"], entry["key"]),
            "namespace": f"kova-runner-{RUN}",
            "uid": uid,
        },
        "spec": {
            "source": {"uri": entry["source_uri"], "digest": entry["source_digest"]},
            "idempotencyKey": entry["key"],
            "requester": {"username": rt["requester"], "uid": rt["requester_uid"]},
            "targets": [{"target": entry["output_tag"], "platform": "linux/amd64"}],
            "build": {"format": "oci", "concurrency": 1, "timeout": 300},
        },
        "status": {"phase": "Queued"},
    }


class MemoryJournal:
    def __init__(self):
        self.events = []
        self.files = {}

    def append(self, event: str, **fields: object) -> None:
        self.events.append((event, fields))

    def save_json(self, name: str, value: object) -> str:
        self.files[name] = value
        return burst.sha(burst.canonical(value))

    def close(self) -> None:
        pass


class FakeKube:
    def __init__(self, object_: dict):
        self.object = object_
        self.gets = 0

    def get(self, _deadline: float, _kind: str, _name: str, _namespace: str) -> dict:
        self.gets += 1
        return self.object


class FakeForward:
    def __init__(self):
        self.port = 12345
        self.pod = "service-0"
        self.process = type("Process", (), {"poll": lambda self: None})()

    def close(self) -> None:
        pass


class CleanupKube:
    def __init__(self, entry: dict, rt: dict, uid: str):
        self.build = build(entry, rt, uid)
        self.rt = rt
        self.reads = 0
        self.before_delete = True

    def get(self, _deadline: float, kind: str, name: str, _namespace: str) -> dict:
        if kind == "kovabuild":
            return self.build
        if kind == "configmap":
            field = "active_ledger_uid" if name == burst.ACTIVE_NAME else "queue_ledger_uid"
            data = (
                {
                    "reservations.json": json.dumps(
                        {"active": {}, "maxJobs": 12, "maxPerRequester": 12, "workerSlots": 12}
                    )
                }
                if name == burst.ACTIVE_NAME
                else {
                    "queue.json": json.dumps(
                        {"intents": {}, "globalLimit": 1000, "requesterLimit": 1000}
                    )
                }
            )
            return {"metadata": {"uid": self.rt["genesis"][field]}, "data": data}
        raise AssertionError(kind)

    def list(
        self, _deadline: float, _kind: str, _namespace: str, _selector: str | None = None
    ) -> list[dict]:
        self.reads += 1
        if _kind == "kovabuild" and self.before_delete:
            self.before_delete = False
            return [self.build]
        return []


class FakeProxy:
    def __init__(self, _kube, _deadline):
        self.port = 12346

    def close(self):
        pass


def observation() -> dict:
    return {
        "schemaVersion": 1,
        "availability": "observed",
        "vertexCount": 3,
        "vertexIntervalCount": 3,
        "cachedVertexIntervalCount": 1,
        "vertexUnionNanoseconds": 15000000000,
        "exportAvailability": "observed",
        "exportUnionNanoseconds": 9000000000,
        "pushAvailability": "observed",
        "pushUnionNanoseconds": 7000000000,
        "exportPushOverlapNanoseconds": 6000000000,
        "nydusAvailability": "unavailable",
        "workerSessionsAvailability": "unavailable",
    }


def history(target: str, ref: str = "actual-server-ref", *, completed: bool = True) -> dict:
    record = {
        "Ref": ref,
        "CreatedAt": {"seconds": 1791511200, "nanos": 123456789},
        "Exporters": [{"Type": "image", "Attrs": {"name": target}}],
    }
    if completed:
        record["CompletedAt"] = {"seconds": 1791511202, "nanos": 987654321}
        record["ExporterResponse"] = {"containerimage.digest": DIGEST}
    return {"type": 1, "record": record} if completed else {"record": record}


class BurstObservationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.plan = plan()
        self.runtime = runtime("0" * 64, "1" * 64)
        self.jobs = burst.job_matrix(RUN, REPOSITORY)
        self.target = self.jobs[0]["output_tag"]

    def test_real_nanosecond_phase_unions_are_not_subtracted_or_zero_filled(self):
        value = observation()
        projected = burst.build_observation(value)
        self.assertEqual(projected, value)
        self.assertTrue(burst.phase_observation_complete(projected))
        self.assertNotIn("buildkit_execute_ms", projected)
        for field in (
            "vertexUnionNanoseconds",
            "pushUnionNanoseconds",
            "exportPushOverlapNanoseconds",
        ):
            changed = {key: val for key, val in value.items() if key != field}
            self.assertFalse(burst.phase_observation_complete(burst.build_observation(changed)))
        self.assertFalse(burst.phase_observation_complete(burst.build_observation(None)))

    def test_malformed_optional_observation_is_unavailable_without_artifact_failure(self):
        mutations = [
            {"schemaVersion": True},
            {"vertexCount": True},
            {"pushUnionNanoseconds": -1},
            {"pushUnionNanoseconds": 1.5},
            {"exportPushOverlapNanoseconds": 8000000000},
            {"workerSessionsAvailability": "observed"},
            {"raw_trace": "secret"},
        ]
        for mutation in mutations:
            with self.subTest(mutation=mutation):
                value = burst.build_observation({**observation(), **mutation})
                self.assertEqual(value["availability"], "unavailable")
                self.assertEqual(value["reason"], "invalid_observation")

    def test_phase_receipt_is_joined_to_exact_internal_output_digest(self):
        entry = {"output_tag": self.target}
        result = {
            "image": self.target,
            "format": "oci",
            "platform": "linux/amd64",
            "state": "succeeded",
            "pushedDigest": DIGEST,
            "buildObservation": observation(),
        }
        build = {"status": {"verificationResults": [result]}}
        self.assertEqual(burst.output_observation(build, entry, DIGEST), observation())
        for field, value in (
            ("image", "unowned"),
            ("pushedDigest", "sha256:" + "b" * 64),
            ("state", "pending"),
        ):
            with self.subTest(field=field):
                result[field] = value
                with self.assertRaises(burst.UnknownOutcome):
                    burst.output_observation(build, entry, DIGEST)
                result[field] = (
                    self.target
                    if field == "image"
                    else DIGEST
                    if field == "pushedDigest"
                    else "succeeded"
                )

    def test_server_history_omitted_started_enum_and_submicrosecond_times(self):
        active = history(self.target, completed=False)
        rows = burst.project_history(burst.canonical(active) + b"\n", {self.target})
        self.assertEqual(rows[0]["solve_ref"], "actual-server-ref")
        self.assertEqual(rows[0]["created_ns"], 1791511200123456789)
        self.assertIsNone(rows[0]["completed_ns"])
        self.assertIsNone(burst.peak_solve_occupancy(rows))

    def test_started_complete_snapshot_race_retains_original_server_ref(self):
        raw = (
            b"\n".join(
                [
                    burst.canonical(history(self.target, completed=False)),
                    burst.canonical(history(self.target)),
                ]
            )
            + b"\n"
        )
        rows = burst.project_history(raw, {self.target})
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["completed_ns"], 1791511202987654321)
        self.assertEqual(burst.peak_solve_occupancy(rows), 1)

    def test_history_fail_closed_on_foreign_malformed_changed_and_limit(self):
        for changed in (
            history("foreign"),
            {"type": 2, "record": history(self.target)["record"]},
            {"type": True, "record": history(self.target)["record"]},
            {
                "type": 1,
                "record": {**history(self.target)["record"], "CreatedAt": {"seconds": True}},
            },
            {"type": 1, "record": {**history(self.target)["record"], "ExporterResponse": {}}},
        ):
            with self.subTest(changed=changed), self.assertRaises(burst.UnknownOutcome):
                burst.project_history(burst.canonical(changed), {self.target})
        first = history(self.target)
        second = history(self.target)
        second["record"]["CompletedAt"]["seconds"] += 1
        with self.assertRaises(burst.UnknownOutcome):
            burst.project_history(
                burst.canonical(first) + b"\n" + burst.canonical(second), {self.target}
            )
        with self.assertRaises(burst.UnknownOutcome):
            burst.project_history(b"x" * ((1 << 20) + 1), {self.target})
        with self.assertRaises(burst.UnknownOutcome):
            burst.project_history(b"{}\n" * 83, {self.target})

    def test_per_worker_overlap_is_half_open_and_not_cpu_or_transport_session(self):
        rows = [
            {"created_ns": 0, "completed_ns": 10},
            {"created_ns": 5, "completed_ns": 15},
            {"created_ns": 15, "completed_ns": 16},
            {"created_ns": 15, "completed_ns": 15},
        ]
        self.assertEqual(burst.peak_solve_occupancy(rows), 2)

    def test_41_exact_uid_target_digest_worker_joins_are_required(self):
        journal = MemoryJournal()
        observer = burst.WorkerSolveObserver(None, self.plan, self.runtime, journal)
        uid = UUIDS[8]
        for worker_uid in UUIDS[8:11]:
            observer.identities[worker_uid] = {
                "worker_id": "actual-worker-" + worker_uid,
                "pod_uid": worker_uid,
            }
        known = {}
        for index, job in enumerate(self.jobs):
            job_id = "exact-build-" + str(index)
            known[job_id] = {
                "uid": "exact-uid-" + str(index),
                "entry": job,
                "output_manifest_digest": DIGEST,
            }
            row = burst.project_history(
                burst.canonical(history(job["output_tag"], str(index))), observer.targets
            )[0]
            observer.records[(uid, row["solve_ref"])] = row
        result = observer.summary(known)
        self.assertTrue(result["complete_for_attribution"])
        self.assertEqual(len(result["joined_builds"]), 41)
        self.assertEqual(result["transport_session_ids"], "unavailable")
        self.assertEqual(result["pure_cpu_execution_time"], "unavailable")
        observer.records.pop((uid, "0"))
        self.assertFalse(observer.summary(known)["complete_for_attribution"])
        self.assertFalse(observer.summary({})["complete_for_attribution"])
        observer.records[(uid, "1")]["pushed_digest"] = "sha256:" + "b" * 64
        with self.assertRaises(burst.UnknownOutcome):
            observer.summary(known)

    def test_two_real_solves_for_one_output_are_ambiguous_not_one_command(self):
        observer = burst.WorkerSolveObserver(None, self.plan, self.runtime, MemoryJournal())
        uid = UUIDS[8]
        observer.identities[uid] = {"worker_id": "actual-worker", "pod_uid": uid}
        for ref in ("accepted-a", "accepted-b"):
            observer.records[(uid, ref)] = burst.project_history(
                burst.canonical(history(self.target, ref)), observer.targets
            )[0]
        result = observer.summary(
            {"job": {"uid": UUIDS[15], "entry": self.jobs[0], "output_manifest_digest": DIGEST}}
        )
        self.assertFalse(result["complete_for_attribution"])
        self.assertEqual(result["missing_or_ambiguous_builds"], ["job"])

    def test_read_only_debug_commands_cannot_solve_prune_or_rebind_container(self):
        pin = self.runtime["worker_pods"][0]
        pod = {
            "metadata": {"name": pin["name"], "uid": pin["uid"]},
            "spec": {
                "containers": [
                    {
                        "name": "buildkitd",
                        "image": self.plan["candidate"]["role_images"]["worker"],
                        "volumeMounts": [{"name": "buildkit-config", "mountPath": "/etc/buildkit"}],
                        "args": [
                            "--config",
                            "/etc/buildkit/buildkitd.toml",
                            "--addr",
                            "tcp://0.0.0.0:9094",
                            "--oci-worker-no-process-sandbox",
                        ],
                    }
                ],
                "volumes": [{"name": "buildkit-config", "configMap": {"name": "kova-config"}}],
            },
            "status": {
                "phase": "Running",
                "conditions": [{"type": "Ready", "status": "True"}],
                "containerStatuses": [
                    {
                        "name": "buildkitd",
                        "restartCount": 0,
                        "containerID": "containerd://actual-container",
                        "imageID": DIGEST,
                        "state": {"running": {"startedAt": "2026-10-09T00:00:00Z"}},
                    }
                ],
            },
        }
        kube = FakeKube(pod)
        kube.argv = lambda *parts: ["kubectl", *parts]
        observer = burst.WorkerSolveObserver(kube, self.plan, self.runtime, MemoryJournal())
        with patch.object(burst, "command", return_value=b"") as called:
            observer.read(pin, "histories", time.monotonic() + 1)
        argv = called.call_args.args[0]
        self.assertEqual(argv[-4:], ["debug", "histories", "--format", "{{json .}}"])
        self.assertNotIn("build", argv)
        self.assertNotIn("prune", argv)
        observer.identities[pin["uid"]] = observer.pod_identity(pin, time.monotonic() + 1)
        pod["status"]["containerStatuses"][0]["containerID"] = "containerd://replacement"
        with self.assertRaises(burst.UnknownOutcome):
            observer.read(pin, "histories", time.monotonic() + 1)
        with self.assertRaises(burst.UnknownOutcome):
            observer.read(pin, "prune", time.monotonic() + 1)

    def test_history_retention_has_capacity_for_all_41_solves_on_one_worker(self):
        burst.check_history_retention('[history]\n maxAge = "1h"\n maxEntries = 64\n[worker.oci]\n')
        for config in (
            "",
            '[history]\n maxAge="1h"\n maxEntries=40\n',
            '[history]\n maxAge="1m"\n maxEntries=64\n',
            '[history]\n maxAge="1h"\n maxEntries=64\n[history]\n',
            'history.maxEntries=1\n[history]\n maxAge="1h"\n maxEntries=64\n',
        ):
            with self.subTest(config=config), self.assertRaises(burst.UnknownOutcome):
                burst.check_history_retention(config)

    def test_background_reader_type_error_is_unknown_not_silent_success(self):
        sampler = burst.Sampler(
            None, self.plan, self.runtime, MemoryJournal(), time.monotonic() + 1
        )
        with patch.object(burst, "sample_resources", side_effect=TypeError("malformed response")):
            sampler.start()
            sampler.thread.join(timeout=1)
        with self.assertRaises(burst.UnknownOutcome):
            sampler.check()

    def test_real_reader_baseline_checks_all_worker_ids_and_original_incarnations(self):
        config = '[history]\nmaxAge="1h"\nmaxEntries=64\n'
        self.plan["fixture"]["worker_config_sha256"] = burst.sha(config.encode())
        configmap = {
            "metadata": {"uid": self.plan["fixture"]["worker_configmap_uid"]},
            "data": {"buildkitd.toml": config},
        }

        class ConfigKube:
            def get(self, *args):
                return configmap

        observer = burst.WorkerSolveObserver(ConfigKube(), self.plan, self.runtime, MemoryJournal())

        def identity(pin, _deadline):
            return {
                "pod": pin["name"],
                "pod_uid": pin["uid"],
                "container_id": "containerd://" + pin["uid"],
                "image_id": DIGEST,
                "container_started_at": "2026-10-09T00:00:00Z",
            }

        def read(pin, operation, _deadline):
            if operation == "histories":
                return b""
            return burst.canonical(
                [
                    {
                        "ID": "worker-" + pin["uid"],
                        "BuildkitVersion": {
                            "package": "github.com/moby/buildkit",
                            "version": "v0.31.2",
                            "revision": burst.BUILDKIT_REVISION,
                        },
                    }
                ]
            )

        with (
            patch.object(observer, "pod_identity", side_effect=identity),
            patch.object(observer, "read", side_effect=read),
        ):
            observed = observer.sample(time.monotonic() + 1, baseline=True)
            self.assertEqual(len(observed["workers"]), 3)
            self.assertEqual(len(observer.identities), 3)

        def nonempty(pin, operation, deadline):
            return (
                burst.canonical(history(self.target))
                if operation == "histories"
                else read(pin, operation, deadline)
            )

        with (
            patch.object(observer, "pod_identity", side_effect=identity),
            patch.object(observer, "read", side_effect=nonempty),
            self.assertRaises(burst.UnknownOutcome),
        ):
            observer.sample(time.monotonic() + 1, baseline=True)


class BurstContractTests(unittest.TestCase):
    def setUp(self) -> None:
        self.plan = plan()
        self.jobs = burst.validate_plan(self.plan)
        self.source_doc = sources(self.jobs)
        self.entries = json.loads(
            json.dumps(burst.validate_sources(self.source_doc, self.plan, self.jobs))
        )
        for entry, job in zip(self.entries, self.jobs):
            entry["key"] = job["key"]
        self.runtime = runtime(
            burst.sha(burst.canonical(self.plan)), burst.sha(burst.canonical(self.source_doc))
        )

    def test_exact_1_8_12_8_12_matrix_and_genesis_identity(self) -> None:
        self.assertEqual(
            [
                len(self.entries[:1]),
                len(self.entries[1:9]),
                len(self.entries[9:21]),
                len(self.entries[21:29]),
                len(self.entries[29:41]),
            ],
            [1, 8, 12, 8, 12],
        )
        burst.validate_runtime(
            self.runtime, self.plan, self.runtime["plan_sha256"], self.runtime["sources_sha256"]
        )
        changed = json.loads(json.dumps(self.runtime))
        changed["genesis"]["secret_uid"] = UUIDS[12]
        with self.assertRaises(burst.ContractError):
            burst.validate_runtime(
                changed, self.plan, self.runtime["plan_sha256"], self.runtime["sources_sha256"]
            )
        changed = json.loads(json.dumps(self.plan))
        changed["fixture"]["runner_namespace"] = changed["fixture"]["control_namespace"]
        with self.assertRaises(burst.ContractError):
            burst.validate_plan(changed)
        changed = json.loads(json.dumps(self.runtime))
        changed["genesis"]["secret_namespace"] = self.plan["fixture"]["runner_namespace"]
        with self.assertRaises(burst.ContractError):
            burst.validate_runtime(
                changed, self.plan, self.runtime["plan_sha256"], self.runtime["sources_sha256"]
            )

    def test_source_cohort_or_manifest_alias_is_rejected(self) -> None:
        changed = json.loads(json.dumps(self.source_doc))
        changed["jobs"][21]["payload_sha256"] = "4" * 64
        with self.assertRaises(burst.ContractError):
            burst.validate_sources(changed, self.plan, self.jobs)
        changed = json.loads(json.dumps(self.source_doc))
        changed["jobs"][1]["source_uri"] = changed["jobs"][0]["source_uri"]
        with self.assertRaises(burst.ContractError):
            burst.validate_sources(changed, self.plan, self.jobs)

    def test_source_zip_embedded_target_and_payload_digest(self) -> None:
        entry = {
            "payload_member": "image/payload.bin",
            "payload_sha256": burst.sha(b"data"),
            "output_tag": "registry.local/target:one",
        }
        output = io.BytesIO()
        with zipfile.ZipFile(output, "w") as archive:
            archive.writestr(
                "image/metadata.json",
                json.dumps({"target": entry["output_tag"], "platform": "linux/amd64"}),
            )
            archive.writestr(entry["payload_member"], b"data")
        output.seek(0)
        burst.inspect_source_zip(output, entry, 4)
        entry["output_tag"] = "registry.local/other:two"
        output.seek(0)
        with self.assertRaises(burst.UnknownOutcome):
            burst.inspect_source_zip(output, entry, 4)

    def test_ambiguous_post_reads_exact_id_once_and_never_retries(self) -> None:
        entry = self.entries[0]
        journal = MemoryJournal()
        kube = FakeKube(build(entry, self.runtime))
        calls = []

        def uncertain(_request, _deadline):
            calls.append("post")
            raise burst.UnknownOutcome("timeout")

        with patch.object(burst, "http_json", side_effect=uncertain):
            with self.assertRaises(burst.UnknownOutcome):
                burst.post_one(
                    entry,
                    0,
                    threading.Barrier(1),
                    [FakeForward()],
                    "long-private-token-for-test",
                    kube,
                    self.plan,
                    self.runtime,
                    journal,
                    time.monotonic() + 60,
                )
        self.assertEqual(calls, ["post"])
        self.assertEqual(kube.gets, 1)
        self.assertIn("submit_unknown_exact_read", [event for event, _ in journal.events])
        self.assertNotIn("submit_accepted", [event for event, _ in journal.events])

    def test_202_with_wrong_public_identity_reads_exact_id_once_and_quarantines(self) -> None:
        entry = self.entries[0]
        journal = MemoryJournal()
        kube = FakeKube(build(entry, self.runtime))
        with patch.object(
            burst,
            "http_json",
            return_value=(202, {"id": "foreign-job", "source_uri": entry["source_uri"]}),
        ):
            with self.assertRaises(burst.UnknownOutcome):
                burst.post_one(
                    entry,
                    0,
                    threading.Barrier(1),
                    [FakeForward()],
                    "long-private-token-for-test",
                    kube,
                    self.plan,
                    self.runtime,
                    journal,
                    time.monotonic() + 60,
                )
        self.assertEqual(kube.gets, 1)
        self.assertIn("submit_unknown_exact_read", [event for event, _ in journal.events])
        self.assertNotIn("submit_accepted", [event for event, _ in journal.events])

    def test_check_mode_is_local_and_cannot_claim_live_ready(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            files = {}
            for field in (
                "cli",
                "chart",
                "values",
                "source-template",
                "counter-schema",
                "kubeconfig",
            ):
                path = base / field
                path.write_bytes(field.encode())
                files[field] = path
            files["cli"].chmod(0o700)
            frozen = json.loads(json.dumps(self.plan))
            for field, target in (
                ("cli", "cli_sha256"),
                ("chart", "chart_sha256"),
                ("values", "values_sha256"),
                ("source-template", "source_template_sha256"),
            ):
                frozen["candidate"][target] = burst.hash_file(files[field])
            frozen["telemetry"]["counter_schema_sha256"] = burst.hash_file(files["counter-schema"])
            frozen["fixture"]["kubeconfig_sha256"] = burst.hash_file(files["kubeconfig"])
            manifest = self.source_doc
            pinned_runtime = runtime(
                burst.sha(burst.canonical(frozen)), burst.sha(burst.canonical(manifest))
            )
            for label, value in (
                ("plan", frozen),
                ("sources", manifest),
                ("runtime", pinned_runtime),
            ):
                path = base / f"{label}.json"
                path.write_bytes(burst.canonical(value))
                files[label] = path
            argv = ["e2e-service-burst.py", "check"]
            for label, path in files.items():
                argv.extend((f"--{label}", str(path)))
            output = io.StringIO()
            with patch.object(sys, "argv", argv), patch("sys.stdout", output):
                self.assertEqual(burst.main(), 0)
            self.assertEqual(json.loads(output.getvalue())["live_ready"], False)

    def test_metric_reset_is_unknown_not_negative_latency(self) -> None:
        before = [{"pod": "service-0", "series": ["kova_service_kube_wire_requests_total 2"]}]
        after = [{"pod": "service-0", "series": ["kova_service_kube_wire_requests_total 1"]}]
        with self.assertRaises(burst.UnknownOutcome):
            burst.metric_deltas(before, after)

    def test_cleanup_uses_only_server_uid_precondition_and_two_empty_reads(self) -> None:
        entry = self.entries[0]
        uid = UUIDS[15]
        job_id = burst.expected_job_id(self.runtime["requester"], entry["key"])
        known = {job_id: {"entry": entry, "uid": uid}}
        kube = CleanupKube(entry, self.runtime, uid)
        journal = MemoryJournal()
        deleted = []

        def deletion(request, _deadline, _maximum):
            deleted.append(json.loads(request.data))
            self.assertEqual(request.get_method(), "DELETE")
            self.assertIn(job_id, request.full_url)
            self.assertIn("/namespaces/kova-runner-", request.full_url)
            return 200, burst.canonical(kube.build)

        with (
            patch.object(burst, "fixture_preflight") as preflight,
            patch.object(burst, "Proxy", FakeProxy),
            patch.object(burst, "http_bytes", side_effect=deletion),
            patch.object(burst.time, "sleep"),
        ):
            burst.exact_cleanup(
                kube, self.plan, self.runtime, known, journal, time.monotonic() + 60
            )
        self.assertEqual(deleted[0]["preconditions"], {"uid": uid})
        self.assertEqual(kube.reads, 6)
        self.assertEqual([call.kwargs["empty"] for call in preflight.call_args_list], [False, True])
        self.assertEqual([event for event, _ in journal.events].count("post_cleanup_empty_read"), 2)

    def test_delete_success_requires_exact_object_or_qualified_status(self) -> None:
        entry = self.entries[0]
        item = {"entry": entry, "uid": UUIDS[15]}
        original = build(entry, self.runtime)
        original["metadata"]["deletionTimestamp"] = "2026-10-09T00:00:00Z"
        burst.qualify_delete_response(200, burst.canonical(original), item, self.runtime, self.plan)
        status = {
            "apiVersion": "v1",
            "kind": "Status",
            "status": "Success",
            "details": {
                "uid": item["uid"],
                "name": original["metadata"]["name"],
                "group": "kova.cofy.dev",
                "kind": "kovabuilds",
            },
        }
        for http_code in (200, 202):
            for body_code in (0, http_code):
                with self.subTest(http_code=http_code, body_code=body_code):
                    burst.qualify_delete_response(
                        http_code,
                        burst.canonical({**status, "code": body_code}),
                        item,
                        self.runtime,
                        self.plan,
                    )
        invalid = [b"", b"{}", b"[]", b"<html>ok</html>", b'{"kind":"Status","kind":"Status"}']
        for field, value in (
            ("apiVersion", "other/v1"),
            ("kind", "ConfigMap"),
            ("status", "Failure"),
            ("reason", "NotFound"),
            ("code", 201),
            ("code", True),
        ):
            invalid.append(burst.canonical({**status, field: value}))
        for field in ("uid", "name", "group", "kind"):
            invalid.append(
                burst.canonical({**status, "details": {**status["details"], field: "foreign"}})
            )
        for field, value in (("uid", UUIDS[16]), ("name", "foreign"), ("namespace", "foreign")):
            invalid.append(
                burst.canonical({**original, "metadata": {**original["metadata"], field: value}})
            )
        invalid.append(burst.canonical({**original, "metadata": []}))
        known = {
            burst.expected_job_id(self.runtime["requester"], candidate["key"]): {
                "entry": candidate,
                "uid": UUIDS[15 + index],
            }
            for index, candidate in enumerate(self.entries[:2])
        }
        first = known[sorted(known)[0]]
        for raw in invalid:
            with (
                self.subTest(raw=raw),
                patch.object(burst, "fixture_preflight"),
                patch.object(burst, "campaign_objects_preflight"),
                patch.object(burst, "Proxy", FakeProxy),
                patch.object(burst, "http_bytes", return_value=(200, raw)) as http,
                self.assertRaises(burst.UnknownOutcome),
            ):
                burst.exact_cleanup(
                    FakeKube(build(first["entry"], self.runtime, first["uid"])),
                    self.plan,
                    self.runtime,
                    known,
                    MemoryJournal(),
                    time.monotonic() + 60,
                )
            self.assertEqual(
                http.call_count, 1, "unknown first response must block the next delete"
            )

    def test_timing_read_failure_quarantines_but_missing_optional_stage_is_incomplete(self) -> None:
        observer = MagicMock()
        observer.summary.return_value = {"complete_for_attribution": True}

        def submitted(entries, *_args):
            return [
                {
                    "id": burst.expected_job_id(self.runtime["requester"], entry["key"]),
                    "entry": entry,
                    "uid": UUIDS[15],
                    "phase_observation_complete": True,
                    "output_manifest_digest": DIGEST,
                    "build_observation": observation(),
                }
                for entry in entries
            ]

        replacements = {
            "prepare_outputs": MagicMock(),
            "Kube": MagicMock(),
            "check_registry_container": MagicMock(),
            "fixture_preflight": MagicMock(),
            "WorkerSolveObserver": MagicMock(return_value=observer),
            "check_registry_catalog": MagicMock(),
            "verify_source": MagicMock(return_value={}),
            "Forward": MagicMock(side_effect=lambda *_args: FakeForward()),
            "http_json": MagicMock(return_value=(200, {"api_version": "v1", "commit": "a" * 40})),
            "Sampler": MagicMock(),
            "campaign_objects_preflight": MagicMock(),
            "verify_source_tag": MagicMock(),
            "snapshot_metrics": MagicMock(),
            "submit_wave": MagicMock(side_effect=submitted),
            "poll_wave": MagicMock(),
            "summarize_wave": MagicMock(return_value={}),
            "metric_deltas": MagicMock(return_value={"missing": []}),
            "exact_cleanup": MagicMock(),
        }
        cases = [
            (burst.UnknownOutcome("identity changed"), 3, "quarantined", None),
            (OSError("unreadable"), 3, "quarantined", None),
            (ValueError("malformed"), 3, "quarantined", None),
            ({"complete_for_attribution": False}, 4, "run_finished", None),
            (burst.UnknownOutcome("identity changed"), 3, "quarantined", "identified failure"),
            ({"complete_for_attribution": False}, 1, "run_finished", "identified failure"),
        ]
        with tempfile.TemporaryDirectory() as tmp:
            token = Path(tmp) / "token"
            token.write_text("private-test-bearer-123456789", encoding="utf-8")
            token.chmod(0o600)
            args = argparse.Namespace(
                ack=(
                    f"{RUN}/{self.plan['fixture']['control_namespace']}/"
                    f"{self.plan['fixture']['runner_namespace']}/{'0' * 12}"
                ),
                token_file=token,
                out_dir=Path(tmp),
                kubeconfig=Path(tmp) / "kubeconfig",
            )
            for diagnostic, expected_exit, final_event, build_failure in cases:
                replacements["exact_cleanup"].reset_mock()
                replacements["poll_wave"].side_effect = (
                    burst.MeasuredFailure(build_failure) if build_failure else None
                )
                journal = MemoryJournal()
                timing = (
                    MagicMock(side_effect=diagnostic)
                    if isinstance(diagnostic, Exception)
                    else MagicMock(return_value=diagnostic)
                )
                with (
                    self.subTest(diagnostic=diagnostic),
                    patch.object(burst.sys, "platform", "linux"),
                    patch.multiple(burst, **replacements),
                    patch.object(burst, "Journal", return_value=journal),
                    patch.object(burst, "timing_diagnostics", timing),
                ):
                    self.assertEqual(
                        burst.run_campaign(
                            args,
                            self.plan,
                            self.runtime,
                            self.entries,
                            "0" * 64,
                            "1" * 64,
                            "2" * 64,
                        ),
                        expected_exit,
                    )
                self.assertEqual(journal.events[-1][0], final_event)
                if expected_exit == 3:
                    replacements["exact_cleanup"].assert_not_called()
                    self.assertNotIn("cleanup_complete", [event for event, _ in journal.events])
                else:
                    replacements["exact_cleanup"].assert_called_once()

    def test_cleanup_refuses_replacement_uid_before_delete(self) -> None:
        entry = self.entries[0]
        job_id = burst.expected_job_id(self.runtime["requester"], entry["key"])
        known = {job_id: {"entry": entry, "uid": UUIDS[15]}}
        kube = CleanupKube(entry, self.runtime, UUIDS[16])
        with (
            patch.object(burst, "fixture_preflight"),
            patch.object(burst, "Proxy", FakeProxy),
            patch.object(burst, "http_bytes") as http,
        ):
            with self.assertRaises(burst.UnknownOutcome):
                burst.exact_cleanup(
                    kube, self.plan, self.runtime, known, MemoryJournal(), time.monotonic() + 60
                )
        http.assert_not_called()

    def test_unknown_never_authorizes_scripted_cleanup(self) -> None:
        self.assertFalse(burst.may_cleanup("UNKNOWN", True))
        self.assertFalse(burst.may_cleanup("COMPLETE", False))
        self.assertTrue(burst.may_cleanup("INCOMPLETE", True))

    def test_cleanup_refuses_unlabelled_runner_pod_before_delete(self) -> None:
        entry = self.entries[0]
        job_id = burst.expected_job_id(self.runtime["requester"], entry["key"])
        known = {job_id: {"entry": entry, "uid": UUIDS[15]}}
        kube = CleanupKube(entry, self.runtime, UUIDS[15])
        original_list = kube.list

        def listed(deadline, kind, namespace, selector=None):
            if kind == "pod":
                return [{"metadata": {"name": "foreign", "namespace": namespace, "uid": UUIDS[16]}}]
            return original_list(deadline, kind, namespace, selector)

        kube.list = listed
        with patch.object(burst, "fixture_preflight"), patch.object(burst, "http_bytes") as http:
            with self.assertRaises(burst.UnknownOutcome):
                burst.exact_cleanup(
                    kube, self.plan, self.runtime, known, MemoryJournal(), time.monotonic() + 60
                )
        http.assert_not_called()

    def test_cleanup_refuses_foreign_build_before_delete(self) -> None:
        entry = self.entries[0]
        job_id = burst.expected_job_id(self.runtime["requester"], entry["key"])
        known = {job_id: {"entry": entry, "uid": UUIDS[15]}}
        kube = CleanupKube(entry, self.runtime, UUIDS[15])
        original_list = kube.list

        def listed(deadline, kind, namespace, selector=None):
            if kind == "kovabuild":
                return [
                    kube.build,
                    {"metadata": {"name": "foreign", "namespace": namespace, "uid": UUIDS[16]}},
                ]
            return original_list(deadline, kind, namespace, selector)

        kube.list = listed
        with patch.object(burst, "fixture_preflight"), patch.object(burst, "http_bytes") as http:
            with self.assertRaises(burst.UnknownOutcome):
                burst.exact_cleanup(
                    kube, self.plan, self.runtime, known, MemoryJournal(), time.monotonic() + 60
                )
        http.assert_not_called()

    def test_worker_buildkit_topology_only_reaches_three_pinned_pods(self) -> None:
        namespace = self.plan["fixture"]["control_namespace"]
        service_uid = self.plan["fixture"]["worker_service_uid"]
        selector = {"app.kubernetes.io/component": "worker"}
        service = {
            "metadata": {"name": "kova-worker", "namespace": namespace, "uid": service_uid},
            "spec": {
                "type": "ClusterIP",
                "clusterIP": "None",
                "clusterIPs": ["None"],
                "internalTrafficPolicy": "Cluster",
                "ipFamilies": ["IPv4"],
                "ipFamilyPolicy": "SingleStack",
                "sessionAffinity": "None",
                "selector": selector,
                "ports": [{"name": "tcp", "port": 9094, "protocol": "TCP", "targetPort": "tcp"}],
            },
        }
        pods = []
        for pin in self.runtime["service_pods"]:
            pods.append({"metadata": {**pin, "labels": {"app.kubernetes.io/component": "service"}}})
        endpoints = []
        for index, pin in enumerate(self.runtime["worker_pods"], 1):
            ip = f"10.1.0.{index}"
            pods.append(
                {
                    "metadata": {**pin, "labels": selector},
                    "spec": {"containers": [{"ports": [{"name": "tcp", "containerPort": 9094}]}]},
                    "status": {"podIP": ip, "podIPs": [{"ip": ip}]},
                }
            )
            endpoints.append(
                {
                    "addresses": [ip],
                    "conditions": {"ready": True},
                    "targetRef": {"kind": "Pod", "namespace": namespace, **pin},
                }
            )
        slices = [
            {
                "metadata": {
                    "namespace": namespace,
                    "labels": {"kubernetes.io/service-name": "kova-worker"},
                    "ownerReferences": [
                        {
                            "apiVersion": "v1",
                            "kind": "Service",
                            "name": "kova-worker",
                            "uid": service_uid,
                            "controller": True,
                        }
                    ],
                },
                "addressType": "IPv4",
                "ports": [{"name": "tcp", "protocol": "TCP", "port": 9094}],
                "endpoints": endpoints,
            }
        ]

        class TopologyKube:
            def get(self, _deadline, kind, _name, _namespace):
                if kind != "service":
                    raise AssertionError(kind)
                return service

            def list(self, _deadline, kind, _namespace):
                if kind != "endpointslice":
                    raise AssertionError(kind)
                return slices

        kube = TopologyKube()
        deadline = time.monotonic() + 60
        burst.buildkit_topology_preflight(kube, self.plan, self.runtime, deadline, selector, pods)
        endpoints[0]["targetRef"]["uid"] = UUIDS[20]
        with self.assertRaises(burst.UnknownOutcome):
            burst.buildkit_topology_preflight(
                kube, self.plan, self.runtime, deadline, selector, pods
            )
        endpoints[0]["targetRef"]["uid"] = self.runtime["worker_pods"][0]["uid"]
        service["spec"]["externalIPs"] = ["192.0.2.1"]
        with self.assertRaises(burst.UnknownOutcome):
            burst.buildkit_topology_preflight(
                kube, self.plan, self.runtime, deadline, selector, pods
            )

    def test_service_pod_cannot_override_cluster_dns_for_worker_address(self) -> None:
        pod = {"spec": {"dnsPolicy": "ClusterFirst", "containers": [{}]}}
        burst.canonical_service_dns(pod)
        pod["spec"]["hostAliases"] = [{"ip": "192.0.2.1", "hostnames": ["kova-worker"]}]
        with self.assertRaises(burst.UnknownOutcome):
            burst.canonical_service_dns(pod)
        del pod["spec"]["hostAliases"]
        pod["spec"]["containers"][0]["volumeMounts"] = [{"mountPath": "/etc/resolv.conf"}]
        with self.assertRaises(burst.UnknownOutcome):
            burst.canonical_service_dns(pod)

    def test_role_command_override_cannot_bypass_pinned_buildkit_flag(self) -> None:
        service = {"spec": {"containers": [{"name": "kova-service", "args": ["--x=y"]}]}}
        burst.canonical_role_entrypoint(service, "service", 9094)
        service["spec"]["containers"][0]["command"] = ["/bin/other"]
        with self.assertRaises(burst.UnknownOutcome):
            burst.canonical_role_entrypoint(service, "service", 9094)
        worker = {
            "spec": {
                "containers": [
                    {
                        "name": "buildkitd",
                        "args": [
                            "--config",
                            "/etc/buildkit/buildkitd.toml",
                            "--addr",
                            "tcp://0.0.0.0:9094",
                            "--oci-worker-no-process-sandbox",
                        ],
                    }
                ]
            }
        }
        burst.canonical_role_entrypoint(worker, "worker", 9094)
        worker["spec"]["containers"][0]["args"][3] = "tcp://0.0.0.0:9095"
        with self.assertRaises(burst.UnknownOutcome):
            burst.canonical_role_entrypoint(worker, "worker", 9094)

    def test_kubernetes_inventory_rejects_partial_page(self) -> None:
        kube = burst.Kube(Path("/unused/kubeconfig"), "kind-test")
        with patch.object(
            kube, "json", return_value={"items": [], "metadata": {"continue": "next-page"}}
        ):
            with self.assertRaises(burst.UnknownOutcome):
                kube.list(time.monotonic() + 60, "endpointslice", "control")

    def test_wave_edge_rejects_foreign_runner_pod_without_sampling_delay(self) -> None:
        entry = self.entries[0]
        job_id = burst.expected_job_id(self.runtime["requester"], entry["key"])
        known = {job_id: {"entry": entry, "uid": UUIDS[15]}}

        class Listed:
            def list(self, _deadline, kind, namespace, _selector=None):
                if kind == "kovabuild":
                    return [build(entry, self_runtime, UUIDS[15])]
                return [{"metadata": {"name": "foreign", "namespace": namespace, "uid": UUIDS[16]}}]

        self_runtime = self.runtime
        with self.assertRaises(burst.UnknownOutcome):
            burst.campaign_objects_preflight(
                Listed(), self.plan, self.runtime, known, time.monotonic() + 60
            )

    def test_sampler_deadline_failure_cannot_appear_healthy(self) -> None:
        sampler = burst.Sampler(
            object(), self.plan, self.runtime, MemoryJournal(), time.monotonic() + 1
        )
        with patch.object(burst, "sample_resources", side_effect=burst.MeasuredFailure("deadline")):
            sampler.start()
            sampler.thread.join(timeout=1)
        with self.assertRaises(burst.UnknownOutcome):
            sampler.check()

    def test_registry_rejects_extra_port_or_shared_storage_source(self) -> None:
        frozen = json.loads(json.dumps(self.plan))
        cid, other_id = "a" * 64, "b" * 64
        frozen["fixture"]["registry_instance_sha256"] = burst.sha(cid.encode())
        name = self.runtime["registry_container_name"]
        volume_name = self.runtime["registry_volume_name"]
        mountpoint = frozen["fixture"]["registry_volume_mountpoint"]
        binding = [{"HostIp": "127.0.0.1", "HostPort": "25000"}]
        container = {
            "Id": cid,
            "Image": frozen["fixture"]["registry_image_id"],
            "Name": "/" + name,
            "State": {"Running": True},
            "Config": {
                "Image": frozen["fixture"]["registry_image"],
                "Entrypoint": ["/entrypoint.sh"],
                "Cmd": ["/etc/docker/registry/config.yml"],
                "Labels": {burst.OWNER_LABEL: RUN},
                "Env": ["PATH=/usr/bin"],
            },
            "Path": "/entrypoint.sh",
            "Args": ["/etc/docker/registry/config.yml"],
            "HostConfig": {"NetworkMode": "kind", "PortBindings": {"5000/tcp": binding}},
            "NetworkSettings": {
                "Ports": {"5000/tcp": binding},
                "Networks": {
                    "kind": {
                        "NetworkID": frozen["fixture"]["registry_network_id"],
                        "Aliases": [name],
                    }
                },
            },
            "Mounts": [
                {
                    "Type": "volume",
                    "Driver": "local",
                    "Name": volume_name,
                    "Source": mountpoint,
                    "Destination": "/var/lib/registry",
                    "RW": True,
                }
            ],
        }
        volume = {
            "Name": volume_name,
            "Driver": "local",
            "Scope": "local",
            "Options": {},
            "CreatedAt": frozen["fixture"]["registry_volume_created_at"],
            "Mountpoint": mountpoint,
            "Labels": {burst.OWNER_LABEL: RUN},
        }
        other = {"Id": other_id, "Mounts": [], "HostConfig": {"PortBindings": {}}}

        def inspect(argv, _deadline, _maximum=burst.MAX_JSON):
            if argv == ["docker", "inspect", name]:
                return json.dumps([container]).encode()
            if argv == ["docker", "volume", "inspect", volume_name]:
                return json.dumps([volume]).encode()
            if argv == ["docker", "ps", "-aq", "--no-trunc"]:
                return f"{cid}\n{other_id}\n".encode()
            if argv == ["docker", "inspect", other_id]:
                return json.dumps([other]).encode()
            raise AssertionError(argv)

        with patch.object(burst, "command", side_effect=inspect):
            burst.check_registry_container(frozen, self.runtime, time.monotonic() + 5)
            container["HostConfig"]["PortBindings"]["8080/tcp"] = binding
            with self.assertRaises(burst.UnknownOutcome):
                burst.check_registry_container(frozen, self.runtime, time.monotonic() + 5)
            container["HostConfig"]["PortBindings"].pop("8080/tcp")
            other["Mounts"] = [{"Type": "bind", "Source": mountpoint}]
            with self.assertRaises(burst.UnknownOutcome):
                burst.check_registry_container(frozen, self.runtime, time.monotonic() + 5)

    def test_http_redirect_does_not_make_second_request_or_forward_bearer(self) -> None:
        received = []

        class Handler(http.server.BaseHTTPRequestHandler):
            def do_POST(self):
                received.append((self.path, self.headers.get("Authorization")))
                self.send_response(302)
                self.send_header("Location", "/target")
                self.send_header("Content-Length", "0")
                self.end_headers()

            def do_GET(self):
                received.append((self.path, self.headers.get("Authorization")))
                self.send_response(200)
                self.send_header("Content-Length", "2")
                self.end_headers()
                self.wfile.write(b"{}")

            def log_message(self, *_args):
                pass

        server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            url = f"http://127.0.0.1:{server.server_port}/submit"
            request = Request(
                url,
                data=b"{}",
                method="POST",
                headers={"Authorization": "Bearer long-test-bearer"},
            )
            code, _ = burst.http_bytes(request, time.monotonic() + 2, 128)
            self.assertEqual(code, 302)
            self.assertEqual(received, [("/submit", "Bearer long-test-bearer")])
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)

    def test_http_body_drip_and_startup_partial_line_respect_deadline(self) -> None:
        stop = threading.Event()

        class Handler(http.server.BaseHTTPRequestHandler):
            def do_GET(self):
                self.send_response(200)
                self.send_header("Content-Length", "100000")
                self.end_headers()
                while not stop.wait(0.02):
                    try:
                        self.wfile.write(b"x")
                        self.wfile.flush()
                    except OSError:
                        break

            def log_message(self, *_args):
                pass

        server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        started = time.monotonic()
        try:
            with self.assertRaises(burst.UnknownOutcome):
                burst.http_bytes(
                    Request(f"http://127.0.0.1:{server.server_port}/drip"),
                    time.monotonic() + 0.15,
                    200000,
                )
            self.assertLess(time.monotonic() - started, 0.8)
        finally:
            stop.set()
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)
        process = subprocess.Popen(
            [
                sys.executable,
                "-c",
                "import sys,time;sys.stdout.write('F');sys.stdout.flush();time.sleep(2)",
            ],
            stdout=subprocess.PIPE,
        )
        started = time.monotonic()
        try:
            with self.assertRaises(burst.UnknownOutcome):
                list(burst.process_lines(process, time.monotonic() + 0.15))
            self.assertLess(time.monotonic() - started, 0.8)
        finally:
            process.kill()
            process.wait(timeout=2)
            assert process.stdout is not None
            process.stdout.close()


if __name__ == "__main__":
    unittest.main()
