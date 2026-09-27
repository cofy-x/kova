#!/usr/bin/env python3
"""Validate exact terminal and resource evidence for Service OCI oversize rejection."""

from __future__ import annotations

import importlib.util
import json
import re
import sys
from pathlib import Path

DIRECTORY = Path(__file__).resolve().parent
sys.path.insert(0, str(DIRECTORY))
SPEC = importlib.util.spec_from_file_location(
    "source_capacity_guard", DIRECTORY / "source-capacity-guard.py"
)
assert SPEC and SPEC.loader
guard = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(guard)

ERROR = "source archive exceeds 512 MiB compressed size limit"
PRESSURE = ("DiskPressure", "MemoryPressure", "PIDPressure")
PERCENT = re.compile(r"[0-9]+(?:\.[0-9]+)?%\Z")


class EvidenceError(ValueError):
    pass


def require(ok: bool, message: str) -> None:
    if not ok:
        raise EvidenceError(message)


def read_object(path: Path) -> dict:
    value = json.loads(path.read_text(encoding="utf-8"))
    require(isinstance(value, dict), f"{path.name} is not a JSON object")
    return value


def read_jsonl(path: Path) -> list[dict]:
    values = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]
    require(
        bool(values) and all(isinstance(item, dict) for item in values),
        f"{path.name} lacks object samples",
    )
    return values


def check_nodes(samples: list[dict], cluster: str) -> int:
    names = {f"{cluster}-control-plane", f"{cluster}-worker"}
    for item in samples:
        nodes = item.get("nodes")
        require(isinstance(item.get("at"), str) and item["at"], "node sample lacks time")
        require(isinstance(nodes, list) and len(nodes) == 2, "node sample is incomplete")
        require({node.get("name") for node in nodes} == names, "node identity differs")
        for node in nodes:
            conditions = node.get("conditions")
            require(
                isinstance(conditions, list) and len(conditions) == 4,
                "node conditions are incomplete",
            )
            statuses = {entry.get("type"): entry.get("status") for entry in conditions}
            require(len(statuses) == 4 and statuses.get("Ready") == "True", "node is not Ready")
            require(
                all(statuses.get(kind) == "False" for kind in PRESSURE), "node pressure present"
            )
    return len(samples)


def check_docker(samples: list[dict], cluster: str) -> dict:
    names = {f"{cluster}-control-plane", f"{cluster}-worker"}
    groups: dict[str, set[str]] = {}
    cpu_peak, memory_peak, pid_peak = 0.0, 0.0, 0
    for sample in samples:
        at, name = sample.get("at"), sample.get("name")
        require(isinstance(at, str) and at and name in names, "Docker sample identity differs")
        require(name not in groups.setdefault(at, set()), "Docker sample repeats a node")
        groups[at].add(name)
        cpu, memory = sample.get("cpu_percent"), sample.get("memory_percent")
        require(
            isinstance(cpu, str) and PERCENT.fullmatch(cpu) is not None, "CPU sample is invalid"
        )
        require(
            isinstance(memory, str) and PERCENT.fullmatch(memory) is not None,
            "memory sample is invalid",
        )
        require(
            isinstance(sample.get("memory_usage"), str) and sample["memory_usage"],
            "memory usage is absent",
        )
        require(
            isinstance(sample.get("block_io"), str) and sample["block_io"], "block I/O is absent"
        )
        pids = str(sample.get("pids", ""))
        require(pids.isdigit(), "PID sample is invalid")
        cpu_peak = max(cpu_peak, float(cpu[:-1]))
        memory_peak = max(memory_peak, float(memory[:-1]))
        pid_peak = max(pid_peak, int(pids))
    require(all(group == names for group in groups.values()), "Docker epoch omitted a Kind node")
    return {
        "epochs": len(groups),
        "peak_cpu_percent": cpu_peak,
        "peak_memory_percent": memory_peak,
        "peak_pids": pid_peak,
    }


def check_runner_samples(samples: list[dict], job_id: str, runner_uid: str) -> int:
    observed = [sample for sample in samples if sample.get("missing") is not True]
    require(bool(observed), "resource sampler never observed the runner")
    for sample in observed:
        require(
            sample.get("name") == f"kova-job-{job_id}" and sample.get("uid") == runner_uid,
            "runner sample identity differs",
        )
        require(
            sample.get("reason") != "Evicted"
            and sample.get("phase") in {"Pending", "Running", "Failed"},
            "runner phase is unsafe",
        )
        if sample.get("phase") == "Failed":
            require(
                any(
                    item.get("name") == "source-fetch"
                    and (item.get("state", {}).get("terminated") or {}).get("exitCode", 0) > 0
                    and (item.get("state", {}).get("terminated") or {}).get("reason") != "OOMKilled"
                    for item in sample.get("initContainerStatuses", [])
                ),
                "runner failed without the expected source-fetch exit",
            )
        for item in sample.get("initContainerStatuses", []) + sample.get("containerStatuses", []):
            require(item.get("restartCount") == 0, "runner or source-fetch restarted")
    return len(observed)


def check_source_log(capture: dict, logs: str) -> int:
    require(capture.get("capture_complete") is True, "source-fetch log capture incomplete")
    require(
        capture.get("command_exit_code") == 0 and capture.get("forwarded_signal") in (None, 0),
        "source-fetch log command failed or was interrupted",
    )
    require(capture.get("stdout", {}).get("truncated") is False, "source-fetch log was truncated")
    require(
        capture.get("stderr", {}).get("truncated") is False,
        "source-fetch log stderr was truncated",
    )
    size = len(logs.encode())
    require(ERROR in logs and size <= 1 << 20, "bounded source-fetch log lacks size-limit error")
    require(
        capture.get("stdout", {}).get("retained_bytes") == size,
        "log capture byte receipt differs",
    )
    return size


def check_optional_init_failure(pod: dict) -> bool:
    statuses = [
        item
        for item in pod.get("status", {}).get("initContainerStatuses", [])
        if item.get("name") == "source-fetch"
    ]
    require(len(statuses) <= 1, "source-fetch status is duplicated")
    if not statuses:
        return False
    status = statuses[0]
    require(status.get("restartCount") == 0, "source-fetch restarted")
    terminated = status.get("state", {}).get("terminated")
    if terminated is None:
        return False
    require(
        isinstance(terminated, dict)
        and terminated.get("exitCode", 0) != 0
        and terminated.get("reason") != "OOMKilled",
        "source-fetch termination did not prove a non-OOM failure",
    )
    return True


def validate(run_dir: Path) -> dict:
    contract = guard.load_contract(run_dir)
    run_id, job_id = contract["run_id"], contract["expected_job_id"]
    publication = read_object(run_dir / "source-publication.json")
    require(publication.get("archive_bytes") == (512 << 20) + 1, "source is not 512 MiB + 1 byte")
    require(publication.get("allocated_bytes", 1 << 30) <= 16 << 20, "source is not sparse")
    require(publication.get("source_digest") == contract["source_digest"], "content digest differs")
    require(
        publication.get("source_manifest_digest") == contract["source_manifest_digest"],
        "manifest digest differs",
    )
    require(
        publication.get("tag") == run_id and publication.get("target") == contract["target"],
        "publisher target differs",
    )

    terminal = read_object(run_dir / "terminal.json")
    require(
        terminal.get("id") == job_id and terminal.get("status") == "failed",
        "job did not fail with exact ID",
    )
    require(
        terminal.get("failure_code") == "invalid_source", "Service did not classify InvalidSource"
    )
    require(
        terminal.get("error") == "immutable source validation failed",
        "Service returned an unexpected public error",
    )
    require(
        terminal.get("source_digest") == contract["source_digest"], "Service source digest differs"
    )
    require(terminal.get("source_uri") == contract["source_uri"], "Service source URI differs")
    require(terminal.get("idempotency_key") == run_id, "Service idempotency key differs")

    build = read_object(run_dir / "kovabuild.json")
    uid = guard.validate_build(build, contract)
    status = build.get("status", {})
    require(
        status.get("phase") == "Failed" and status.get("reason") == "InvalidSource",
        "KovaBuild failure phase or reason differs",
    )
    require(
        isinstance(status.get("message"), str) and status["message"],
        "KovaBuild has no failure message",
    )
    require(not status.get("outputs"), "oversized source created an output receipt")

    baseline = read_object(run_dir / "candidate-images.json")
    first = read_object(run_dir / "runner-pod-first.json")
    last = read_object(run_dir / "runner-pod-last.json")
    identity = read_object(run_dir / "runner-pod-image.json")
    for pod in (first, identity, last):
        guard.check_runner_ownership(pod, contract, uid)
    require(
        first["metadata"]["uid"] == identity["metadata"]["uid"] == last["metadata"]["uid"],
        "runner Pod UID changed",
    )
    terminal_init_seen = check_optional_init_failure(last)
    for pod in (first, identity, last):
        for container in pod.get("status", {}).get("containerStatuses", []):
            require(
                container.get("name") == "runner"
                and not container.get("state", {}).get("running")
                and not container.get("state", {}).get("terminated"),
                "runner main container started",
            )
    runtime = guard.runtime_image_fact(
        identity,
        "source-fetch",
        baseline["images"]["runner"],
        baseline["config_ids"]["runner"],
        init=True,
    )

    runner_samples = check_runner_samples(
        read_jsonl(run_dir / "runner-pod-samples.jsonl"), job_id, last["metadata"]["uid"]
    )
    capture = read_object(run_dir / "source-fetch.capture.json")
    logs = (run_dir / "source-fetch.log").read_text(encoding="utf-8")
    log_bytes = check_source_log(capture, logs)
    errors = run_dir / "sampler-errors.txt"
    require(not errors.exists() or errors.stat().st_size == 0, "resource sampler reported an error")
    nodes = check_nodes(read_jsonl(run_dir / "node-health.jsonl"), guard.CLUSTER)
    final_nodes = read_object(run_dir / "nodes-final.json")
    check_nodes(
        [
            {
                "at": "final",
                "nodes": [
                    {
                        "name": node["metadata"]["name"],
                        "conditions": [
                            {"type": item["type"], "status": item["status"]}
                            for item in node["status"].get("conditions", [])
                            if item["type"] in ("Ready", *PRESSURE)
                        ],
                    }
                    for node in final_nodes.get("items", [])
                ],
            }
        ],
        guard.CLUSTER,
    )
    docker = check_docker(read_jsonl(run_dir / "node-docker-stats.jsonl"), guard.CLUSTER)
    for name in ("output-before", "output-after-publication", "output-final"):
        require(
            (run_dir / f"{name}.status").read_text().strip() == "404",
            f"{name} output tag was present",
        )
    for name in ("source-after", "source-final"):
        require(
            (run_dir / f"{name}.status").read_text().strip() == "200",
            f"{name} source tag is absent",
        )
    return {
        "status": "passed",
        "job_id": job_id,
        "build_uid": uid,
        "runner_uid": identity["metadata"]["uid"],
        "runner_image_id": runtime["cri_id"],
        "terminated_init_observed": terminal_init_seen,
        "node_samples": nodes,
        "runner_samples": runner_samples,
        "docker": docker,
        "source_fetch_log_bytes": log_bytes,
    }


def main() -> None:
    if len(sys.argv) != 2:
        raise EvidenceError("usage: source-oci-oversize-evidence.py RUN_DIR")
    print(json.dumps(validate(Path(sys.argv[1])), sort_keys=True))


if __name__ == "__main__":
    try:
        main()
    except (EvidenceError, guard.GuardError, OSError, ValueError, KeyError, TypeError) as error:
        print(f"source-oci-oversize-evidence: {type(error).__name__}: {error}", file=sys.stderr)
        raise SystemExit(1) from None
