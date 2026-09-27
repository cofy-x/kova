"""Pure-local safety and ordering checks for two-principal Kind acceptance."""

from __future__ import annotations

import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

SCRIPT = Path(__file__).resolve().parents[1] / "e2e-service-admission-fairness.py"
sys.path.insert(0, str(SCRIPT.parent))
SPEC = importlib.util.spec_from_file_location("admission_fairness", SCRIPT)
FAIR = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(FAIR)

RUN_ID = "fairness-20260927t120000z-0123abcd"
SA_UID_A = "11111111-1111-1111-1111-111111111111"
SA_UID_B = "22222222-2222-2222-2222-222222222222"
CONFIG_DIGEST = "sha256:" + "c" * 64
MANIFEST_DIGEST = "sha256:" + "d" * 64
BUILD_UIDS = {
    "a1": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa1",
    "a2": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa2",
    "a3": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa3",
    "b1": "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbb1",
    "b2": "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbb2",
}


def state() -> dict:
    return {
        "run_id": RUN_ID,
        "identity": {
            "cluster": FAIR.CLUSTER,
            "namespace": FAIR.NAMESPACE,
            "platform": "linux/amd64",
            "service_account_uids": {"a": SA_UID_A, "b": SA_UID_B},
            "service_config_digest": CONFIG_DIGEST,
            "service_local_image": {
                "config_digest": CONFIG_DIGEST,
                "platform_manifest_digest": MANIFEST_DIGEST,
            },
        },
        "cases": FAIR.planned_cases(RUN_ID, "linux/amd64"),
        "accepted": {label: BUILD_UIDS[label] for label in ("a1", "a2", "a3", "b1")},
        "rejected": ["a4", "b2"],
        "deleting": [],
        "deleted": [],
        "promotions": [],
        "attempting": None,
    }


def build(label: str, run_state: dict, *, queued: bool) -> dict:
    case = run_state["cases"][label]
    annotations = {"kova.cofy.dev/queue-intent": "a" * 32} if queued else {}
    return {
        "metadata": {
            "name": case["id"],
            "namespace": FAIR.NAMESPACE,
            "uid": BUILD_UIDS[label],
            "annotations": annotations,
            "creationTimestamp": "2026-09-27T12:00:00Z",
        },
        "spec": {
            "requester": {
                "username": case["requester"],
                "uid": SA_UID_A if label[0] == "a" else SA_UID_B,
            },
            "idempotencyKey": case["key"],
            "source": {"uri": case["source_uri"], "digest": case["source_digest"]},
            "targets": [{"target": case["target"], "platform": case["platform"]}],
            "build": {"format": "oci", "concurrency": 1},
        },
        "status": {"phase": "Queued" if queued else "Starting"},
    }


def runner(label: str, run_state: dict) -> dict:
    return {
        "metadata": {
            "name": "kova-job-" + run_state["cases"][label]["id"],
            "namespace": FAIR.NAMESPACE,
            "uid": "33333333-3333-3333-3333-333333333333",
            "ownerReferences": [
                {"kind": "KovaBuild", "uid": BUILD_UIDS[label], "controller": True}
            ],
            "annotations": {"kova.cofy.dev/create-attempt": "b" * 32},
        },
        "spec": {"nodeSelector": {"never": "true"}},
        "status": {"phase": "Pending"},
    }


def observation(run_state: dict) -> tuple[list[dict], list[dict], dict, dict]:
    builds = [build("a1", run_state, queued=False)] + [
        build(label, run_state, queued=True) for label in ("a2", "a3", "b1")
    ]
    pods = [runner("a1", run_state)]
    active = {
        "active": {
            BUILD_UIDS["a1"]: {
                "buildName": run_state["cases"]["a1"]["id"],
                "requester": FAIR.principal("a"),
                "slots": 1,
                "inFlight": [],
            }
        },
        "lastGrantedRequesterHash": FAIR.sha256(FAIR.principal("a")),
    }
    intents = {}
    for label in ("a2", "a3", "b1"):
        case = run_state["cases"][label]
        intents[case["id"]] = {
            "nonce": "a" * 32,
            "requesterHash": FAIR.sha256(case["requester"]),
            "requestDigest": "b" * 64,
        }
    return builds, pods, active, {"intents": intents}


class FairnessSafetyTest(unittest.TestCase):
    def test_auth_fixture_requires_real_tokenreview_and_virtual_submitter_rbac(self) -> None:
        objects = {
            ("-n", "kova", "get", "role", "kova-service-submitter", "-o", "json"): {
                "rules": [
                    {
                        "apiGroups": ["kova.cofy.dev"],
                        "resources": ["servicebuilds"],
                        "verbs": ["create"],
                    }
                ]
            },
            ("get", "clusterrole", "kova-service-auth-review", "-o", "json"): {
                "rules": [
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
                ]
            },
            ("get", "clusterrolebinding", "kova-service-auth-review", "-o", "json"): {
                "subjects": [
                    {"kind": "ServiceAccount", "name": "kova-service", "namespace": "kova"}
                ],
                "roleRef": {
                    "apiGroup": "rbac.authorization.k8s.io",
                    "kind": "ClusterRole",
                    "name": "kova-service-auth-review",
                },
            },
        }
        for which, name in FAIR.SA.items():
            uid = SA_UID_A if which == "a" else SA_UID_B
            objects[("-n", "kova", "get", "serviceaccount", name, "-o", "json")] = {
                "metadata": {
                    "name": name,
                    "namespace": "kova",
                    "uid": uid,
                    "labels": {"kova.cofy.dev/e2e": "admission-fairness"},
                },
                "automountServiceAccountToken": False,
            }
            objects[("-n", "kova", "get", "rolebinding", f"{name}-submitter", "-o", "json")] = {
                "metadata": {
                    "name": f"{name}-submitter",
                    "namespace": "kova",
                    "labels": {"kova.cofy.dev/e2e": "admission-fairness"},
                },
                "subjects": [{"kind": "ServiceAccount", "name": name, "namespace": "kova"}],
                "roleRef": {
                    "apiGroup": "rbac.authorization.k8s.io",
                    "kind": "Role",
                    "name": "kova-service-submitter",
                },
            }
        objects[("-n", "kova", "get", "serviceaccount", "kova-service", "-o", "json")] = {
            "metadata": {"name": "kova-service", "namespace": "kova", "uid": SA_UID_A},
            "automountServiceAccountToken": True,
        }
        with patch.object(FAIR, "kjson", side_effect=lambda *args: objects[args]):
            self.assertEqual(FAIR.verify_rbac()["b"], SA_UID_B)
        objects[("get", "clusterrole", "kova-service-auth-review", "-o", "json")]["rules"][0][
            "resources"
        ] = ["subjectaccessreviews"]
        with patch.object(FAIR, "kjson", side_effect=lambda *args: objects[args]):
            with self.assertRaises(FAIR.SafetyError):
                FAIR.verify_rbac()

    def test_service_pod_image_must_match_reviewed_local_config_and_cri(self) -> None:
        image = "localhost:5002/kova:controller-" + "a" * 12
        pod = {
            "metadata": {"name": "kova-service-a", "uid": SA_UID_A},
            "spec": {
                "nodeName": FAIR.CLUSTER + "-worker",
                "containers": [
                    {"name": "kova-service", "image": image, "imagePullPolicy": "Never"}
                ],
            },
            "status": {
                "containerStatuses": [
                    {"name": "kova-service", "image": image, "imageID": CONFIG_DIGEST}
                ]
            },
        }
        cri = {"status": {"id": CONFIG_DIGEST, "repoTags": [image], "repoDigests": []}}
        with patch.object(FAIR, "command", return_value=json.dumps(cri)):
            fact = FAIR.verify_service_image_binding([pod], image, CONFIG_DIGEST)
        self.assertEqual(fact[0]["cri_config_digest"], CONFIG_DIGEST)
        for key, value in (
            ("id", MANIFEST_DIGEST),
            ("repoTags", [image, "another:tag"]),
        ):
            with self.subTest(key=key):
                drifted = {"status": {**cri["status"], key: value}}
                with patch.object(FAIR, "command", return_value=json.dumps(drifted)):
                    with self.assertRaises(FAIR.SafetyError):
                        FAIR.verify_service_image_binding([pod], image, CONFIG_DIGEST)

    def test_live_fixture_identifies_two_distinct_requesters(self) -> None:
        cases = FAIR.planned_cases(RUN_ID, "linux/amd64")
        self.assertNotEqual(cases["a1"]["requester"], cases["b1"]["requester"])
        self.assertNotEqual(cases["a1"]["id"], cases["b1"]["id"])
        self.assertEqual(len({case["id"] for case in cases.values()}), 6)
        with self.assertRaises(FAIR.SafetyError):
            FAIR.planned_cases("fairness-20260927t1728000000000000z-0123abcd", "linux/amd64")

    def test_valid_one_slot_snapshot_and_fair_stage(self) -> None:
        run_state = state()
        observed = FAIR.validate_observation(*observation(run_state), run_state)
        self.assertTrue(
            FAIR.stage_matches(observed, active="a1", queued=("a2", "a3", "b1"), cursor="a")
        )
        self.assertFalse(FAIR.stage_matches(observed, active="b1", queued=("a2", "a3"), cursor="b"))
        snapshot = FAIR.projection(observed)
        self.assertEqual(snapshot["active"], ["a1"])
        self.assertNotIn("Authorization", json.dumps(snapshot))

    def test_older_alice_backlog_is_proven_before_bob_arrives(self) -> None:
        run_state = state()
        builds, pods, active, queue = observation(run_state)
        for offset, item in enumerate(builds):
            item["metadata"]["creationTimestamp"] = f"2026-09-27T12:00:0{offset}Z"
        observed = FAIR.validate_observation(builds, pods, active, queue, run_state)
        FAIR.require_creation_order(observed, ("a1", "a2", "a3", "b1"))
        builds[2]["metadata"]["creationTimestamp"] = builds[1]["metadata"]["creationTimestamp"]
        observed = FAIR.validate_observation(builds, pods, active, queue, run_state)
        with self.assertRaises(FAIR.SafetyError):
            FAIR.require_creation_order(observed, ("a1", "a2", "a3", "b1"))

    def test_foreign_or_tampered_workload_fails_closed(self) -> None:
        run_state = state()
        for mutation in ("foreign", "requester", "schedulable", "cursor", "extra_intent", "nonce"):
            with self.subTest(mutation=mutation):
                builds, pods, active, queue = observation(run_state)
                if mutation == "foreign":
                    builds.append({"metadata": {"name": "foreign", "namespace": FAIR.NAMESPACE}})
                elif mutation == "requester":
                    builds[1]["spec"]["requester"]["username"] = FAIR.principal("b")
                elif mutation == "schedulable":
                    pods[0]["spec"]["nodeName"] = "unexpected-node"
                elif mutation == "cursor":
                    active["lastGrantedRequesterHash"] = "c" * 64
                elif mutation == "extra_intent":
                    queue["intents"]["unknown"] = {"nonce": "a" * 32}
                else:
                    queue["intents"][run_state["cases"]["a2"]["id"]]["nonce"] = "c" * 32
                with self.assertRaises(FAIR.SafetyError):
                    FAIR.validate_observation(builds, pods, active, queue, run_state)

    def test_unresolved_pod_nonce_never_counts_as_stable_grant(self) -> None:
        run_state = state()
        builds, pods, active, queue = observation(run_state)
        active["active"][BUILD_UIDS["a1"]]["inFlight"] = ["c" * 32]
        observed = FAIR.validate_observation(builds, pods, active, queue, run_state)
        self.assertFalse(
            FAIR.stage_matches(observed, active="a1", queued=("a2", "a3", "b1"), cursor="a")
        )

    def test_bob_grant_after_handoff_beats_older_alice_backlog(self) -> None:
        run_state = state()
        run_state["deleting"] = ["a1"]
        builds, _, active, queue = observation(run_state)
        builds = [
            item for item in builds if item["metadata"]["name"] != run_state["cases"]["a1"]["id"]
        ]
        builds[-1]["status"]["phase"] = "Starting"
        queue["intents"].pop(run_state["cases"]["b1"]["id"])
        active["active"] = {
            BUILD_UIDS["b1"]: {
                "buildName": run_state["cases"]["b1"]["id"],
                "requester": FAIR.principal("b"),
                "slots": 1,
                "inFlight": [],
            }
        }
        active["lastGrantedRequesterHash"] = FAIR.sha256(FAIR.principal("b"))
        observed = FAIR.validate_observation(
            builds, [runner("b1", run_state)], active, queue, run_state
        )
        self.assertTrue(FAIR.stage_matches(observed, active="b1", queued=("a2", "a3"), cursor="b"))
        self.assertFalse(FAIR.stage_matches(observed, active="a2", queued=("a3", "b1"), cursor="a"))

    def test_recovery_refuses_changed_contract_and_ambiguous_post(self) -> None:
        run_state = state()
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            directory = root / RUN_ID
            directory.mkdir()
            FAIR.save(directory / "state.json", run_state)
            with patch.object(FAIR, "EVIDENCE_ROOT", root):
                self.assertEqual(FAIR.load_run(directory)["run_id"], RUN_ID)
                run_state["cases"]["a1"]["target"] = "foreign-target"
                FAIR.save(directory / "state.json", run_state)
                with self.assertRaises(FAIR.SafetyError):
                    FAIR.load_run(directory)
            run_state = state()
            run_state["attempting"] = "a1"
            campaign = FAIR.Campaign(directory, run_state)
            with patch.object(campaign, "identity"), patch.object(campaign, "observe") as seen:
                with self.assertRaises(FAIR.SafetyError):
                    campaign.recover()
                seen.assert_not_called()

    def test_token_and_http_body_are_not_saved(self) -> None:
        run_state = state()
        run_state["accepted"] = {}
        run_state["rejected"] = ["b2"]
        case = run_state["cases"]["b2"]
        with tempfile.TemporaryDirectory() as temp:
            campaign = FAIR.Campaign(Path(temp), run_state)
            campaign.tokens = {"b": "test-secret-token-that-must-not-persist"}
            with (
                patch.object(campaign, "observe"),
                patch.object(
                    FAIR.failover_io,
                    "bounded_response",
                    return_value=(202, {"X-Kova-Build-ID": case["id"]}, b"accepted"),
                ),
                patch.object(FAIR, "kjson", return_value=build("b2", run_state, queued=True)),
            ):
                campaign.post("b2", 202, FAIR.PORT["a"])
            self.assertEqual(run_state["accepted"]["b2"], BUILD_UIDS["b2"])
            for path in Path(temp).iterdir():
                self.assertNotIn("test-secret-token", path.read_text())


if __name__ == "__main__":
    unittest.main()
