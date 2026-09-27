"""Pure-local safety and metric checks for the bounded auth read probe."""

from __future__ import annotations

import importlib.util
import json
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

SCRIPT = Path(__file__).resolve().parents[1] / "e2e-service-auth-read-probe.py"
sys.path.insert(0, str(SCRIPT.parent))
SPEC = importlib.util.spec_from_file_location("auth_read_probe", SCRIPT)
PROBE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(PROBE)

ALICE_CURSOR = PROBE.fair.sha256(PROBE.fair.principal("a"))


def fixture_objects(cursor: str = ALICE_CURSOR, active: dict | None = None) -> tuple[dict, dict]:
    names = [f"{PROBE.fair.CLUSTER}-control-plane", f"{PROBE.fair.CLUSTER}-worker"]
    node_facts = [
        {"name": name, "uid": f"node-{index}", "allocatable": {"pods": "110"}}
        for index, name in enumerate(names)
    ]
    pods = []
    for index, name in enumerate(("kova-service-aaa", "kova-service-bbb")):
        pods.append(
            {
                "metadata": {"name": name, "uid": f"pod-{index}"},
                "spec": {"nodeName": names[index]},
                "status": {
                    "conditions": [{"type": "Ready", "status": "True"}],
                    "containerStatuses": [
                        {
                            "name": "kova-service",
                            "image": "candidate",
                            "imageID": "sha256:image",
                            "restartCount": 0,
                        }
                    ],
                },
            }
        )
    identity = {
        "kubeconfig_sha256": "a" * 64,
        "node_facts": node_facts,
        "deployment_uid": "deployment-uid",
        "deployment_template_sha256": PROBE.fair.stable_hash({"revision": "candidate"}),
        "service_image": "candidate",
        "service_image_id": "sha256:image",
        "lease_uid": "lease-uid",
    }
    objects = {
        ("get", "nodes", "-o", "json"): {"items": []},
        ("-n", "kova", "get", "configmap", "kova-service-admission", "-o", "json"): {
            "data": {
                "reservations.json": json.dumps(
                    {
                        "active": {} if active is None else active,
                        "lastGrantedRequesterHash": cursor,
                    }
                )
            }
        },
        ("-n", "kova", "get", "configmap", "kova-service-queue-admission", "-o", "json"): {
            "data": {"queue.json": json.dumps({"intents": {}})}
        },
        ("get", "kovabuilds", "--all-namespaces", "-o", "json"): {"items": []},
        (
            "get",
            "pods",
            "--all-namespaces",
            "-l",
            "app.kubernetes.io/name=kova-runner",
            "-o",
            "json",
        ): {"items": []},
        ("-n", "kova", "get", "deployment", "kova-service", "-o", "json"): {
            "metadata": {"uid": "deployment-uid"},
            "spec": {"template": {"revision": "candidate"}},
            "status": {"readyReplicas": 2, "updatedReplicas": 2},
        },
        (
            "-n",
            "kova",
            "get",
            "pods",
            "-l",
            "app.kubernetes.io/instance=kova,app.kubernetes.io/component=service",
            "-o",
            "json",
        ): {"items": pods},
        ("-n", "kova", "get", "lease", PROBE.fair.LEASE, "-o", "json"): {
            "metadata": {"uid": "lease-uid"},
            "spec": {"holderIdentity": "kova-service-aaa_123"},
        },
        ("get", "pods", "--all-namespaces", "-o", "json"): {"items": pods},
    }
    return identity, objects


class AuthReadProbeTest(unittest.TestCase):
    def test_health_accepts_prior_fairness_cursor_but_requires_empty_ledgers(self) -> None:
        identity, objects = fixture_objects()
        stats = "\n".join(
            json.dumps(
                {"Name": name, "CPUPerc": "2%", "MemPerc": "5%", "MemUsage": "100MiB / 2GiB"}
            )
            for name in (f"{PROBE.fair.CLUSTER}-control-plane", f"{PROBE.fair.CLUSTER}-worker")
        )
        with (
            patch.object(PROBE.fair.failover_io, "assert_exact_kind"),
            patch.object(PROBE.fair, "verify_nodes", return_value=identity["node_facts"]),
            patch.object(PROBE.fair, "one_ready_service_pod", return_value=True),
            patch.object(PROBE.fair, "command", return_value=stats),
            patch.object(PROBE, "kjson", side_effect=lambda *args: objects[args]),
        ):
            sample = PROBE.empty_and_healthy(identity)
            self.assertEqual(sample["fairness_cursor"], ALICE_CURSOR)
            self.assertEqual(sample["build_count"], 0)
            self.assertEqual(len(sample["service_pods"]), 2)
            PROBE.empty_and_healthy(identity, sample)
            objects[("-n", "kova", "get", "configmap", "kova-service-admission", "-o", "json")][
                "data"
            ]["reservations.json"] = json.dumps(
                {"active": {"other": {}}, "lastGrantedRequesterHash": ALICE_CURSOR}
            )
            with self.assertRaisesRegex(PROBE.fair.SafetyError, "active or queued"):
                PROBE.empty_and_healthy(identity, sample)

    def test_metrics_preserve_exact_series_and_reject_counter_reset(self) -> None:
        before = {
            "series": [
                {
                    "labels": {
                        "group": "authentication.k8s.io",
                        "resource": "tokenreviews",
                        "code": "201",
                    },
                    "value": 10,
                },
                {
                    "labels": {
                        "group": "authorization.k8s.io",
                        "resource": "subjectaccessreviews",
                        "code": "201",
                    },
                    "value": 20,
                },
            ]
        }
        after = {
            "series": [
                {"labels": before["series"][0]["labels"], "value": 14},
                {"labels": before["series"][1]["labels"], "value": 24},
            ]
        }
        result = PROBE.metric_deltas(before, after, 4, 2.0)
        self.assertEqual(result["per_second"]["tokenreviews"], 2.0)
        self.assertEqual(result["totals"]["subjectaccessreviews"], 4.0)
        after["series"][0]["value"] = 9
        with self.assertRaisesRegex(PROBE.fair.SafetyError, "counter reset"):
            PROBE.metric_deltas(before, after, 4, 2.0)

    def test_metric_parser_requires_both_review_resources(self) -> None:
        raw = "\n".join(
            [
                'apiserver_request_total{group="authentication.k8s.io",'
                'resource="tokenreviews",verb="POST",code="201"} 12',
                'apiserver_request_total{group="authorization.k8s.io",'
                'resource="subjectaccessreviews",verb="POST",code="201"} 9',
                'apiserver_request_total{group="",resource="pods",verb="GET",code="200"} 100',
            ]
        )
        with patch.object(PROBE, "bounded_metrics_text", return_value=raw):
            sample = PROBE.api_metrics()
        self.assertEqual(len(sample["series"]), 2)
        with patch.object(PROBE, "bounded_metrics_text", return_value=raw.splitlines()[0]):
            with self.assertRaisesRegex(PROBE.fair.SafetyError, "unavailable"):
                PROBE.api_metrics()

    def test_metric_parser_allows_unseen_reviews_only_on_fresh_fixture(self) -> None:
        raw = 'apiserver_request_total{group="",resource="pods",verb="GET",code="200"} 100'
        with patch.object(PROBE, "bounded_metrics_text", return_value=raw):
            self.assertEqual(PROBE.api_metrics(allow_unseen=True)["series"], [])
            with self.assertRaisesRegex(PROBE.fair.SafetyError, "unavailable"):
                PROBE.api_metrics()
        with patch.object(PROBE, "bounded_metrics_text", return_value="unrelated_metric 1"):
            with self.assertRaisesRegex(PROBE.fair.SafetyError, "counter family"):
                PROBE.api_metrics(allow_unseen=True)

    def test_empty_get_never_returns_or_persists_bearer(self) -> None:
        def response(request, *, timeout):
            self.assertEqual(request.get_method(), "GET")
            self.assertEqual(request.full_url, "http://127.0.0.1:18110/v1/builds?limit=1")
            self.assertEqual(request.get_header("Authorization"), "Bearer secret-token")
            self.assertEqual(timeout, 5)
            return 200, {}, b'{"jobs":[],"continue":""}'

        with patch.object(PROBE.fair.failover_io, "bounded_response", side_effect=response):
            receipt = PROBE.get_empty_list("a", "secret-token", 18110)
        self.assertNotIn("secret-token", json.dumps(receipt))
        self.assertEqual(receipt["http_status"], 200)

    def test_nearest_rank_tail(self) -> None:
        self.assertEqual(PROBE.percentile(list(range(1, 101)), 0.95), 95)
        self.assertEqual(PROBE.percentile(list(range(1, 101)), 0.99), 99)


if __name__ == "__main__":
    unittest.main()
