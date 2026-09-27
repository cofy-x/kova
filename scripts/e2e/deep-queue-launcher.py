#!/usr/bin/env python3
"""Detached, run-scoped launcher for the isolated deep-queue Kind benchmark.

The benchmark reads its test credential only from its verified K8s Secret.
This launcher never accepts a token argument or writes one to a receipt.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import secrets
import socket
import stat
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
BENCH = ROOT / "scripts" / "e2e" / "e2e-service-admission-deep-queue.py"
CONTROL_ROOT = Path("/data/forge-artifacts/kova-deep-queue")
TOOLS = Path("/data/forge-tools/seed-loop/bin")
ACK = "kova-deep-queue/kova/kova-service"
RUN_ROOT = ROOT / ".work" / "deep-queue"


def fail(message: str) -> None:
    raise RuntimeError(message)


def save_private(path: Path, value: dict) -> None:
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, "w", encoding="utf-8") as output:
        json.dump(value, output, sort_keys=True, indent=2)
        output.write("\n")
        output.flush()
        os.fsync(output.fileno())


def runtime_environment() -> dict[str, str]:
    if sys.platform != "linux" or socket.gethostname().split(".", 1)[0] != "wayne-hk-kvm":
        fail("launcher runs only on the assigned isolated Linux KVM host")
    for name in ("kind", "kubectl", "helm"):
        path = TOOLS / name
        if not path.is_file() or not os.access(path, os.X_OK):
            fail(f"pinned {name} binary is unavailable at {path}")
    if not Path("/usr/bin/nohup").is_file() or not Path("/usr/bin/python3").is_file():
        fail("pinned nohup/python3 runtime is unavailable")
    environment = dict(os.environ)
    environment["PATH"] = f"{TOOLS}:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
    environment.pop("SERVICE_AUTH_TOKEN", None)
    return environment


def process_start(pid: int) -> tuple[str, str] | None:
    try:
        value = Path(f"/proc/{pid}/stat").read_text()
    except FileNotFoundError:
        return None
    tail = value[value.rfind(")") + 2 :].split()
    if len(tail) < 20:
        fail("process status is malformed")
    return tail[0], tail[19]


def process_alive(pid: int, start_ticks: str) -> bool:
    current = process_start(pid)
    return current is not None and current[0] != "Z" and current[1] == start_ticks


def control_directory(raw: str) -> Path:
    path = Path(raw)
    if (
        not path.is_absolute()
        or CONTROL_ROOT.is_symlink()
        or path.is_symlink()
        or not path.is_dir()
        or path.parent.resolve() != CONTROL_ROOT.resolve()
    ):
        fail("control directory must be one direct, existing Kova deep-queue run")
    info = path.stat()
    if info.st_uid != os.getuid() or stat.S_IMODE(info.st_mode) != 0o700:
        fail("control directory owner or permissions differ from the private contract")
    return path


def pid_fact(path: Path) -> tuple[int, str]:
    marker = path / "controller.pid.json"
    if not marker.is_file() or marker.is_symlink() or stat.S_IMODE(marker.stat().st_mode) != 0o600:
        fail("private controller PID receipt is missing or has unsafe permissions")
    data = json.loads(marker.read_text())
    pid = data.get("pid")
    start_ticks = data.get("start_ticks")
    if not isinstance(pid, int) or pid < 2 or not isinstance(start_ticks, str):
        fail("controller PID receipt is malformed")
    return pid, start_ticks


def find_run_dir(path: Path) -> Path | None:
    log = path / "controller.log"
    if not log.is_file():
        return None
    pattern = re.compile(r"RUNNING; receipts at (/\S+)")
    found = None
    with log.open(encoding="utf-8", errors="replace") as source:
        for line in source:
            match = pattern.search(line)
            if match:
                found = Path(match.group(1))
    if found is None:
        return None
    if found.is_symlink() or found.parent.resolve() != RUN_ROOT.resolve():
        fail("controller logged a run directory outside the exact receipt root")
    return found


def result_summary(run_dir: Path | None) -> dict | None:
    if run_dir is None or not (run_dir / "result.json").is_file():
        return None
    result = json.loads((run_dir / "result.json").read_text())
    return {
        "status": result.get("status"),
        "emergency_stop": result.get("emergency_stop"),
        "original_service_pods_deleted": result.get("original_service_pods_deleted"),
        "exact_cr_cleanup": result.get("exact_cr_cleanup"),
    }


def start() -> None:
    environment = runtime_environment()
    if not CONTROL_ROOT.exists():
        CONTROL_ROOT.mkdir(mode=0o700)
    if CONTROL_ROOT.is_symlink() or not CONTROL_ROOT.is_dir():
        fail("managed control root is not a regular directory")
    if (
        CONTROL_ROOT.stat().st_uid != os.getuid()
        or stat.S_IMODE(CONTROL_ROOT.stat().st_mode) != 0o700
    ):
        fail("managed control root owner or mode is not private")
    check_env = dict(environment, DEEP_QUEUE_E2E_MODE="check")
    check = subprocess.run(
        ["/usr/bin/python3", str(BENCH)],
        cwd=ROOT,
        env=check_env,
        text=True,
        capture_output=True,
        timeout=120,
        check=False,
    )
    if check.returncode != 0:
        fail(f"read-only benchmark preflight failed: {check.stderr[-500:].strip()}")
    name = (
        "control-"
        + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")  # noqa: UP017 (Python 3.10)
        + "-"
        + secrets.token_hex(4)
    )
    path = CONTROL_ROOT / name
    path.mkdir(mode=0o700)
    os.chmod(path, 0o700)
    save_private(
        path / "launch.json",
        {
            "host": socket.gethostname(),
            "candidate_root": str(ROOT),
            "candidate_commit": subprocess.check_output(
                ["git", "-C", str(ROOT), "rev-parse", "HEAD"], text=True
            ).strip(),
            "cluster": "kova-deep-queue",
            "tool_root": str(TOOLS),
            "tool_sha256": {
                name: hashlib.sha256((TOOLS / name).read_bytes()).hexdigest()
                for name in ("kind", "kubectl", "helm")
            },
            "kind_config_sha256": hashlib.sha256(
                (ROOT / "deploy" / "quickstart-kind-cluster.yaml").read_bytes()
            ).hexdigest(),
            "log": str(path / "controller.log"),
            "token_source": "verified-kind-secret-in-memory",
        },
    )
    log_fd = os.open(
        path / "controller.log", os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600
    )
    run_env = dict(environment, DEEP_QUEUE_E2E_MODE="run", DEEP_QUEUE_E2E_ACK=ACK)
    with os.fdopen(log_fd, "w", encoding="utf-8") as log:
        process = subprocess.Popen(
            ["/usr/bin/nohup", "/usr/bin/python3", str(BENCH)],
            cwd=ROOT,
            env=run_env,
            stdin=subprocess.DEVNULL,
            stdout=log,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
    started = process_start(process.pid)
    save_private(
        path / "controller.pid.json",
        {
            "pid": process.pid,
            "start_ticks": started[1] if started else "0",
            "started_at": datetime.now(timezone.utc).isoformat(),  # noqa: UP017 (Python 3.10)
        },
    )
    print(
        json.dumps(
            {"control_dir": str(path), "pid": process.pid, "log": str(path / "controller.log")},
            sort_keys=True,
        )
    )


def status(path: Path) -> None:
    pid, start_ticks = pid_fact(path)
    run_dir = find_run_dir(path)
    summary = {
        "control_dir": str(path),
        "pid": pid,
        "controller_alive": process_alive(pid, start_ticks),
        "log": str(path / "controller.log"),
        "run_dir": str(run_dir) if run_dir else None,
        "result": result_summary(run_dir),
    }
    if run_dir is not None and summary["controller_alive"]:
        environment = runtime_environment()
        environment.update(DEEP_QUEUE_E2E_MODE="status", DEEP_QUEUE_E2E_RUN_DIR=str(run_dir))
        cluster = subprocess.run(
            ["/usr/bin/python3", str(BENCH)],
            cwd=ROOT,
            env=environment,
            text=True,
            capture_output=True,
            timeout=60,
            check=False,
        )
        summary["cluster"] = (
            json.loads(cluster.stdout) if cluster.returncode == 0 else "unavailable"
        )
    print(json.dumps(summary, sort_keys=True))


def stop(path: Path) -> None:
    pid, start_ticks = pid_fact(path)
    if process_alive(pid, start_ticks):
        os.kill(pid, 15)
        deadline = time.monotonic() + 240
        while time.monotonic() < deadline and process_alive(pid, start_ticks):
            time.sleep(2)
        if process_alive(pid, start_ticks):
            fail("controller has not exited after SIGTERM; emergency stop remains unconfirmed")
    run_dir = find_run_dir(path)
    result = result_summary(run_dir)
    if result and (result.get("status") == "passed" or result.get("emergency_stop") == "confirmed"):
        print(
            json.dumps(
                {"stop": "confirmed", "run_dir": str(run_dir), "result": result}, sort_keys=True
            )
        )
        return
    if run_dir is None:
        fail("controller exited before a run receipt was announced; inspect its private log")
    environment = runtime_environment()
    environment.update(
        DEEP_QUEUE_E2E_MODE="stop", DEEP_QUEUE_E2E_ACK=ACK, DEEP_QUEUE_E2E_RUN_DIR=str(run_dir)
    )
    manual = subprocess.run(
        ["/usr/bin/python3", str(BENCH)],
        cwd=ROOT,
        env=environment,
        text=True,
        capture_output=True,
        timeout=240,
        check=False,
    )
    if manual.returncode != 0:
        fail("exact manual emergency stop is unconfirmed; inspect manual-stop.json and cluster")
    print(
        json.dumps({"stop": "confirmed", "run_dir": str(run_dir), "manual": True}, sort_keys=True)
    )


def main() -> None:
    if len(sys.argv) == 2 and sys.argv[1] == "start":
        start()
    elif len(sys.argv) == 3 and sys.argv[1] in ("status", "stop"):
        path = control_directory(sys.argv[2])
        if sys.argv[1] == "status":
            status(path)
        else:
            stop(path)
    else:
        fail("usage: deep-queue-launcher.py start | status <control-dir> | stop <control-dir>")


if __name__ == "__main__":
    try:
        main()
    except (RuntimeError, OSError, ValueError, subprocess.TimeoutExpired) as error:
        print(f"deep-queue-launcher: {error}", file=sys.stderr)
        sys.exit(1)
