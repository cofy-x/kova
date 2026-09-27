#!/usr/bin/env python3
"""Validate complete, bounded evidence from the isolated source-capacity run."""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path

SHA = re.compile(r"sha256:[0-9a-f]{64}\Z")
PERCENT = re.compile(r"[0-9]+(?:\.[0-9]+)?%\Z")
PRESSURE = ("DiskPressure", "MemoryPressure", "PIDPressure")
BAD_WAITING = {
    "CrashLoopBackOff",
    "ImagePullBackOff",
    "ErrImagePull",
    "CreateContainerError",
    "RunContainerError",
    "InvalidImageName",
    "OOMKilled",
}


class EvidenceError(ValueError):
    pass


def require(condition: bool, message: str) -> None:
    if not condition:
        raise EvidenceError(message)


def load_json(path: Path) -> dict:
    value = json.loads(path.read_text(encoding="utf-8"))
    require(isinstance(value, dict), f"{path.name} is not a JSON object")
    return value


def load_jsonl(path: Path) -> list[dict]:
    lines = path.read_text(encoding="utf-8").splitlines()
    require(bool(lines), f"{path.name} has no samples")
    values = [json.loads(line) for line in lines]
    require(
        all(isinstance(value, dict) for value in values), f"{path.name} has a non-object sample"
    )
    return values


def validate_result(run_dir: Path) -> str:
    contract = load_json(run_dir / "source-contract.json")
    result = load_json(run_dir / "results.json")
    target = contract.get("target")
    require(isinstance(target, str) and ":" in target, "source contract has no target")
    source_digest = contract.get("source_digest")
    require(
        isinstance(source_digest, str) and SHA.fullmatch(source_digest) is not None,
        "source contract has no digest",
    )
    outputs = result.get("outputs")
    require(result.get("source_digest") == source_digest, "result source digest differs")
    require(isinstance(outputs, list) and len(outputs) == 1, "result has no single OCI output")
    output = outputs[0]
    require(isinstance(output, dict), "result output is not an object")
    digest = output.get("manifest_digest")
    require(
        isinstance(digest, str) and SHA.fullmatch(digest) is not None,
        "result has no manifest digest",
    )
    repository = target.rsplit(":", 1)[0]
    require(
        output.get("image") == target
        and output.get("format") == "oci"
        and output.get("platform") == "linux/amd64"
        and output.get("immutable_ref") == f"{repository}@{digest}",
        "result OCI target or immutable reference differs from the exact repository and digest",
    )
    return digest


def validate_node_samples(run_dir: Path, cluster: str) -> int:
    expected = {f"{cluster}-control-plane", f"{cluster}-worker"}
    samples = load_jsonl(run_dir / "node-health.jsonl")
    for sample in samples:
        nodes = sample.get("nodes")
        require(isinstance(sample.get("at"), str), "node sample lacks a timestamp")
        require(isinstance(nodes, list) and len(nodes) == 2, "node sample is not complete")
        require(
            all(isinstance(node, dict) for node in nodes), "node sample contains a non-object node"
        )
        require({node.get("name") for node in nodes} == expected, "node sample identity differs")
        for node in nodes:
            conditions = node.get("conditions")
            require(isinstance(conditions, list), "node sample lacks conditions")
            require(all(isinstance(item, dict) for item in conditions), "node condition is invalid")
            by_type = {condition.get("type"): condition.get("status") for condition in conditions}
            require(len(by_type) == len(conditions), "node sample repeats a condition")
            require(by_type.get("Ready") == "True", "node was not Ready during the build")
            require(
                all(by_type.get(kind) == "False" for kind in PRESSURE),
                "node pressure was present or unobservable during the build",
            )
    return len(samples)


def validate_docker_samples(run_dir: Path, cluster: str) -> int:
    expected = {f"{cluster}-control-plane", f"{cluster}-worker"}
    groups: dict[str, set[str]] = {}
    for sample in load_jsonl(run_dir / "node-docker-stats.jsonl"):
        at, name = sample.get("at"), sample.get("name")
        require(isinstance(at, str) and at, "Docker sample lacks a timestamp")
        require(name in expected, "Docker sample has an unexpected container")
        require(name not in groups.setdefault(at, set()), "Docker sample repeats a node")
        groups[at].add(name)
        require(
            all(
                isinstance(sample.get(key), str) and sample[key]
                for key in ("cpu_percent", "memory_usage", "memory_percent", "block_io")
            )
            and isinstance(sample.get("pids"), (str, int)),
            "Docker sample lacks resource fields",
        )
        require(
            PERCENT.fullmatch(sample["cpu_percent"]) is not None
            and PERCENT.fullmatch(sample["memory_percent"]) is not None
            and re.fullmatch(r"[0-9]+", str(sample["pids"])) is not None,
            "Docker sample has invalid CPU, memory, or PID measurements",
        )
    require(all(names == expected for names in groups.values()), "Docker sample is missing a node")
    return len(groups)


def validate_runner_samples(run_dir: Path, cluster: str, job_id: str) -> int:
    runner = load_json(run_dir / "runner-image-identity.json")
    uid = runner.get("runner_uid")
    require(isinstance(uid, str) and uid, "runner identity receipt has no UID")
    observed = 0
    for sample in load_jsonl(run_dir / "runner-pod-samples.jsonl"):
        require(isinstance(sample.get("at"), str), "runner sample lacks a timestamp")
        if sample.get("missing") is True:
            continue
        observed += 1
        require(
            sample.get("name") == f"kova-job-{job_id}"
            and sample.get("uid") == uid
            and sample.get("node") in {f"{cluster}-control-plane", f"{cluster}-worker"},
            "runner sample has the wrong Pod identity",
        )
        require(
            sample.get("phase") in {"Pending", "Running", "Succeeded"}
            and sample.get("reason") != "Evicted",
            "runner failed or was evicted during sampling",
        )
        for field in ("initContainerStatuses", "containerStatuses"):
            statuses = sample.get(field)
            require(isinstance(statuses, list), f"runner sample lacks {field}")
            for status in statuses:
                require(isinstance(status, dict), "runner container status is invalid")
                require(status.get("restartCount") == 0, "runner container restarted")
                state = status.get("state") or {}
                require(isinstance(state, dict), "runner container state is invalid")
                terminated = state.get("terminated") or {}
                waiting = state.get("waiting") or {}
                require(
                    isinstance(terminated, dict) and isinstance(waiting, dict),
                    "runner container transition is invalid",
                )
                require(
                    terminated.get("reason") != "OOMKilled"
                    and terminated.get("exitCode", 0) == 0
                    and waiting.get("reason") not in BAD_WAITING,
                    "runner container failed during sampling",
                )
    require(observed > 0, "sampler never observed the owned runner Pod")
    return observed


def validate_logs(run_dir: Path) -> str:
    for name in ("runner-follow", "runner-logs"):
        receipt_path = run_dir / f"{name}.capture.json"
        log_path = run_dir / ("runner-follow.log" if name == "runner-follow" else "runner-logs.txt")
        if not receipt_path.is_file() or not log_path.is_file():
            continue
        receipt = load_json(receipt_path)
        stdout = receipt.get("stdout") or {}
        interrupted_follow = (
            name == "runner-follow"
            and receipt.get("forwarded_signal") == 15
            and receipt.get("command_exit_code") == -15
        )
        child_ok = receipt.get("command_exit_code") == 0 or interrupted_follow
        size = log_path.stat().st_size
        if (
            receipt.get("capture_complete") is True
            and child_ok
            and isinstance(stdout.get("total_bytes"), int)
            and stdout["total_bytes"] > 0
            and stdout.get("retained_bytes") == size
            and 0 < size <= 1 << 20
        ):
            return name
    raise EvidenceError("no complete, nonempty bounded runner log capture")


def validate_monitoring(run_dir: Path) -> dict:
    run = load_json(run_dir / "run.json")
    cluster, job_id = run.get("cluster"), (run_dir / "expected-job-id.txt").read_text().strip()
    require(cluster == "kova-source-capacity", "monitoring run has the wrong cluster")
    require(re.fullmatch(r"idem-[0-9a-f]{20}", job_id) is not None, "monitoring job ID differs")
    errors = run_dir / "sampler-errors.txt"
    require(not errors.exists() or errors.stat().st_size == 0, "resource sampler reported an error")
    return {
        "node_samples": validate_node_samples(run_dir, cluster),
        "docker_sample_epochs": validate_docker_samples(run_dir, cluster),
        "runner_samples": validate_runner_samples(run_dir, cluster, job_id),
        "runner_log_capture": validate_logs(run_dir),
    }


def main() -> int:
    if len(sys.argv) != 3 or sys.argv[1] not in {"result", "monitoring"}:
        raise EvidenceError("usage: source-capacity-evidence.py result|monitoring RUN_DIR")
    run_dir = Path(sys.argv[2])
    if sys.argv[1] == "result":
        print(validate_result(run_dir))
    else:
        print(json.dumps(validate_monitoring(run_dir), sort_keys=True))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (EvidenceError, OSError, ValueError, KeyError, TypeError) as error:
        print(f"source-capacity-evidence: {type(error).__name__}: {error}", file=sys.stderr)
        raise SystemExit(1) from None
