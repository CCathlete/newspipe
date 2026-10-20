#!/usr/bin/env python3
"""newspipe launcher: up, run, down, status.

One entry point for the four operations that used to be hand-driven across
two repositories. Section order follows layer order: DOMAIN value types,
then APPLICATION port and use cases, then INFRASTRUCTURE adapters, then the
CONTROL entrypoint at the bottom.
"""

from __future__ import annotations

import argparse
import http.client
import os
import shutil
import signal
import socket
import subprocess
import sys
import threading
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import IO, Generic, Literal, Sequence, TypeAlias, TypeVar

# ── DOMAIN ─────────────────────────────────────────────────────────────

# TypeVar definitions stay bare: annotating them is a linter error.
# ARCH.PYTHON exemption — the TypeVar itself is the declaration.
T = TypeVar("T")
E_co = TypeVar("E_co", bound=Exception, covariant=True)


@dataclass(slots=True, frozen=True)
class Ok(Generic[T]):
    """Success monad — carries the value."""

    value: T


@dataclass(slots=True, frozen=True)
class Err(Generic[E_co]):
    """Failure monad — carries the exception."""

    error: E_co


PipelineName: TypeAlias = Literal["ingestion", "enrichment"]
PIPELINES: Sequence[PipelineName] = ("ingestion", "enrichment")
STATE_SEPARATOR: str = "\t"

REPO_ROOT: Path = Path(__file__).resolve().parent.parent
INFRA_ROOT: Path = Path.home() / "Repos" / "pipeline_infra"
LOGBACK_FILE: Path = REPO_ROOT / "conf" / "logback.xml"
STATE_FILE: Path = Path("/tmp/newspipe-launcher.state")
KAFKA_HOST: str = "localhost"
KAFKA_PORT: int = 9092
RUSTFS_HOST: str = "localhost"
RUSTFS_PORT: int = 9000
RUSTFS_PATH: str = "/health"
TOPIC: str = "raw_rss"
ENRICHMENT_GROUP: str = "enrichment-group"
KAFKA_OFFSETS_TOOL: str = "/opt/kafka/bin/kafka-get-offsets.sh"
KAFKA_GROUPS_TOOL: str = "/opt/kafka/bin/kafka-consumer-groups.sh"
SHUTDOWN_GRACE_SECONDS: float = 10.0


@dataclass(slots=True, frozen=True)
class ReadinessReport:
    """The three dependencies the launcher waits for."""

    present: Sequence[str]
    missing: Sequence[str]

    @property
    def ready(self) -> bool:
        """True when no dependency is missing."""
        return len(self.missing) == 0


@dataclass(slots=True, frozen=True)
class GroupOffset:
    """Committed offset and lag of the enrichment consumer group."""

    committed: int
    log_end: int
    lag: int


@dataclass(slots=True, frozen=True)
class StatusReport:
    """Everything the status verb prints."""

    present: Sequence[str]
    missing: Sequence[str]
    topic_messages: int | None
    group: GroupOffset | None
    last_poll: dict[PipelineName, str]


# ── APPLICATION ────────────────────────────────────────────────────────


class OpsPort(ABC):
    """Outbound port: everything the use cases need from the outside world."""

    @abstractmethod
    def provision(self) -> Ok[None] | Err[Exception]:
        raise NotImplementedError

    @abstractmethod
    def readiness(self) -> ReadinessReport:
        raise NotImplementedError

    @abstractmethod
    def start(self, names: Sequence[PipelineName]) -> Ok[Sequence[int]] | Err[Exception]:
        raise NotImplementedError

    @abstractmethod
    def wait(self) -> int:
        raise NotImplementedError

    @abstractmethod
    def stop(self) -> Ok[int] | Err[Exception]:
        raise NotImplementedError

    @abstractmethod
    def status(self) -> StatusReport:
        raise NotImplementedError


def do_up(ops: OpsPort) -> Ok[ReadinessReport] | Err[Exception]:
    """Apply the infrastructure, then report readiness."""
    applied: Ok[None] | Err[Exception] = ops.provision()
    if isinstance(applied, Err):
        return applied
    report: ReadinessReport = ops.readiness()
    if not report.ready:
        return Err(RuntimeError("not ready: " + ", ".join(report.missing)))
    return Ok(report)


def do_run(ops: OpsPort, names: Sequence[PipelineName]) -> Ok[Sequence[int]] | Err[Exception]:
    """Start the selected pipelines as children of this launcher."""
    return ops.start(names)


def do_wait(ops: OpsPort) -> int:
    """Block until every started child exits."""
    return ops.wait()


def do_down(ops: OpsPort) -> Ok[int] | Err[Exception]:
    """Stop the recorded pipeline children and nothing else."""
    return ops.stop()


def do_status(ops: OpsPort) -> Ok[StatusReport] | Err[Exception]:
    """Read the current state; only an infrastructure surprise can fail."""
    return Ok(ops.status())


# ── INFRASTRUCTURE ─────────────────────────────────────────────────────


def _parse_int(token: str) -> int | None:
    """Parse a decimal token, or None when it is not one."""
    stripped: str = token.strip()
    if not stripped.isdigit():
        return None
    return int(stripped)


def _parse_pid(token: str) -> int | None:
    """Parse a recorded pid, or None when the token is not a live pid shape."""
    parsed: int | None = _parse_int(token)
    if parsed is None or parsed <= 0:
        return None
    return parsed


def _pipeline_name(token: str) -> PipelineName | None:
    """Map a state token to a pipeline name, or None when unknown."""
    if token == "ingestion":
        return "ingestion"
    if token == "enrichment":
        return "enrichment"
    return None


def _alive(pid: int) -> bool:
    """True when the pid still exists."""
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _is_pipeline_process(pid: int) -> bool:
    """True when the pid still belongs to an sbt or JVM pipeline process.

    The launcher records pids at start time; by the time down runs, the
    operating system may have reused one for an unrelated process. The
    command line check keeps down from signalling a stranger.
    """
    cmdline_path: Path = Path(f"/proc/{pid}/cmdline")
    try:
        cmdline: str = cmdline_path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return False
    return "sbt" in cmdline or "java" in cmdline


def _kafka_reachable() -> bool:
    """True when the Kafka broker accepts a TCP connection."""
    try:
        conn: socket.socket = socket.create_connection((KAFKA_HOST, KAFKA_PORT), timeout=2)
    except OSError:
        return False
    conn.close()
    return True


def _rustfs_reachable() -> bool:
    """True when the RustFS health endpoint answers 200."""
    conn: http.client.HTTPConnection = http.client.HTTPConnection(RUSTFS_HOST, RUSTFS_PORT, timeout=2)
    try:
        conn.request("GET", RUSTFS_PATH)
        response: http.client.HTTPResponse = conn.getresponse()
    except (OSError, http.client.HTTPException):
        conn.close()
        return False
    status: int = response.status
    conn.close()
    return status == 200


def _reachability() -> tuple[Sequence[str], Sequence[str]]:
    """Split the three dependencies into reachable and unreachable names."""
    present: list[str] = []
    missing: list[str] = []
    if _kafka_reachable():
        present.append("kafka")
    else:
        missing.append("kafka")
    if _rustfs_reachable():
        present.append("rustfs")
    else:
        missing.append("rustfs")
    if shutil.which("opencode") is not None:
        present.append("opencode")
    else:
        missing.append("opencode")
    return tuple(present), tuple(missing)


def _read_state() -> dict[int, tuple[PipelineName, str]]:
    """Read the launcher state file: pid, pipeline, newest observed poll line."""
    if not STATE_FILE.is_file():
        return {}
    entries: dict[int, tuple[PipelineName, str]] = {}
    raw_line: str
    for raw_line in STATE_FILE.read_text(encoding="utf-8").splitlines():
        parts: list[str] = raw_line.split(STATE_SEPARATOR, 2)
        if len(parts) != 3:
            continue
        pid: int | None = _parse_pid(parts[0])
        if pid is None:
            continue
        name: PipelineName | None = _pipeline_name(parts[1])
        if name is None:
            continue
        entries[pid] = (name, parts[2])
    return entries


def _write_state(entries: dict[int, tuple[PipelineName, str]]) -> Ok[None] | Err[Exception]:
    """Replace the state file with the given entries."""
    rows: list[str] = []
    pid: int
    for pid in sorted(entries):
        name, poll = entries[pid]
        rows.append(f"{pid}{STATE_SEPARATOR}{name}{STATE_SEPARATOR}{poll}")
    body: str = "".join(row + "\n" for row in rows)
    try:
        STATE_FILE.write_text(body, encoding="utf-8")
    except OSError as caught:
        return Err(caught)
    return Ok(None)


def _clear_state() -> Ok[None] | Err[Exception]:
    """Remove the state file when it exists."""
    try:
        STATE_FILE.unlink(missing_ok=True)
    except OSError as caught:
        return Err(caught)
    return Ok(None)


def _terminate_pids(pids: Sequence[int]) -> int:
    """Signal each recorded process group, then escalate after the grace period."""
    stopped: int = 0
    pid: int
    for pid in pids:
        if not _alive(pid) or not _is_pipeline_process(pid):
            continue
        try:
            os.killpg(pid, signal.SIGTERM)
        except (ProcessLookupError, PermissionError):
            continue
        stopped += 1
    deadline: float = time.monotonic() + SHUTDOWN_GRACE_SECONDS
    while time.monotonic() < deadline and any(_alive(entry) for entry in pids):
        time.sleep(0.25)
    for pid in pids:
        if _alive(pid) and _is_pipeline_process(pid):
            try:
                os.killpg(pid, signal.SIGKILL)
            except (ProcessLookupError, PermissionError):
                continue
    return stopped


def _run_tool(cmd: list[str], timeout: float) -> subprocess.CompletedProcess[str] | None:
    """Run a read-only inspection command, or None when it cannot run."""
    try:
        return subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            check=False,
            timeout=timeout,
        )
    except (OSError, subprocess.TimeoutExpired):
        return None


def _topic_messages() -> int | None:
    """Summed message count of the raw topic, or None when it cannot be read."""
    completed: subprocess.CompletedProcess[str] | None = _run_tool(
        [
            "docker",
            "exec",
            "kafka",
            KAFKA_OFFSETS_TOOL,
            "--bootstrap-server",
            f"{KAFKA_HOST}:{KAFKA_PORT}",
            "--topic",
            TOPIC,
        ],
        timeout=30.0,
    )
    if completed is None or completed.returncode != 0:
        return None
    total: int = 0
    seen: bool = False
    line: str
    for line in completed.stdout.splitlines():
        parts: list[str] = line.rsplit(":", 1)
        if len(parts) != 2:
            continue
        count: int | None = _parse_int(parts[1])
        if count is None:
            continue
        total += count
        seen = True
    if not seen:
        return None
    return total


def _group_offset() -> GroupOffset | None:
    """Committed offset and lag of the enrichment group, or None when unreadable."""
    completed: subprocess.CompletedProcess[str] | None = _run_tool(
        [
            "docker",
            "exec",
            "kafka",
            KAFKA_GROUPS_TOOL,
            "--bootstrap-server",
            f"{KAFKA_HOST}:{KAFKA_PORT}",
            "--describe",
            "--group",
            ENRICHMENT_GROUP,
        ],
        timeout=30.0,
    )
    if completed is None or completed.returncode != 0:
        return None
    line: str
    for line in completed.stdout.splitlines():
        tokens: list[str] = line.split()
        if len(tokens) < 6:
            continue
        if tokens[0] != ENRICHMENT_GROUP or tokens[1] != TOPIC:
            continue
        committed: int | None = _parse_int(tokens[3])
        log_end: int | None = _parse_int(tokens[4])
        lag: int | None = _parse_int(tokens[5])
        if committed is None or log_end is None or lag is None:
            continue
        return GroupOffset(committed=committed, log_end=log_end, lag=lag)
    return None


def _selection(value: str) -> Sequence[PipelineName]:
    """Map the run argument to the pipelines it selects."""
    if value == "both":
        return PIPELINES
    if value == "ingestion":
        return ("ingestion",)
    if value == "enrichment":
        return ("enrichment",)
    return ()


class SubprocessOps(OpsPort):
    """Adapters over terraform, the dependency probes and the pipeline JVMs."""

    def __init__(self) -> None:
        self._lock: threading.Lock = threading.Lock()
        self._started: list[subprocess.Popen[bytes]] = []

    def provision(self) -> Ok[None] | Err[Exception]:
        """terraform apply against the pipeline_infra repository."""
        completed: subprocess.CompletedProcess[str]
        try:
            completed = subprocess.run(
                ["terraform", "apply", "-auto-approve"],
                cwd=INFRA_ROOT,
                capture_output=True,
                text=True,
                check=False,
            )
        except OSError as caught:
            return Err(caught)
        if completed.returncode != 0:
            detail: str = (completed.stderr.strip() or completed.stdout.strip())[-400:]
            return Err(RuntimeError(f"terraform apply exited {completed.returncode}: {detail}"))
        return Ok(None)

    def readiness(self) -> ReadinessReport:
        """Ask the three dependencies whether they are up."""
        present: Sequence[str]
        missing: Sequence[str]
        present, missing = _reachability()
        return ReadinessReport(present=present, missing=missing)

    def start(self, names: Sequence[PipelineName]) -> Ok[Sequence[int]] | Err[Exception]:
        """Start one sbt child per selected pipeline and record the pids."""
        env: dict[str, str] = dict(os.environ)
        existing_options: str = env.get("JAVA_TOOL_OPTIONS", "")
        env["JAVA_TOOL_OPTIONS"] = (
            f"{existing_options} -Dlogback.configurationFile={LOGBACK_FILE}"
        ).strip()
        with self._lock:
            entries: dict[int, tuple[PipelineName, str]] = {
                pid: value
                for pid, value in _read_state().items()
                if _alive(pid) and _is_pipeline_process(pid)
            }
        pids: list[int] = []
        name: PipelineName
        for name in names:
            proc: subprocess.Popen[bytes]
            try:
                proc = subprocess.Popen(
                    ["sbt", "run"],
                    cwd=REPO_ROOT / "pipelines" / name,
                    env=env,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    start_new_session=True,
                )
            except OSError as caught:
                _terminate_pids(tuple(pids))
                return Err(caught)
            self._started.append(proc)
            pids.append(proc.pid)
            entries[proc.pid] = (name, "")
            stream: IO[bytes] | None = proc.stdout
            if stream is not None:
                relay = threading.Thread(
                    target=self._relay,
                    args=(proc.pid, name, stream),
                    daemon=True,
                )
                relay.start()
        with self._lock:
            written: Ok[None] | Err[Exception] = _write_state(entries)
        if isinstance(written, Err):
            _terminate_pids(tuple(pids))
            return written
        return Ok(tuple(pids))

    def _relay(self, pid: int, name: PipelineName, stream: IO[bytes]) -> None:
        """Print one child line and keep the newest poll line in the state file."""
        for raw_line in iter(stream.readline, b""):
            text: str = raw_line.decode("utf-8", errors="replace").rstrip("\n")
            print(f"[{name}] {text}", flush=True)
            if "poll merged" not in text:
                continue
            with self._lock:
                entries: dict[int, tuple[PipelineName, str]] = _read_state()
                entries[pid] = (name, text)
                written: Ok[None] | Err[Exception] = _write_state(entries)
            if isinstance(written, Err):
                print(f"state update failed: {written.error}", file=sys.stderr)

    def wait(self) -> int:
        """Block until every child this invocation started has exited."""
        code: int = 0
        proc: subprocess.Popen[bytes]
        for proc in self._started:
            code = proc.wait()
        return code

    def stop(self) -> Ok[int] | Err[Exception]:
        """Stop every recorded child and clear the state file."""
        entries: dict[int, tuple[PipelineName, str]] = _read_state()
        stopped: int = _terminate_pids(tuple(entries))
        cleared: Ok[None] | Err[Exception] = _clear_state()
        if isinstance(cleared, Err):
            return cleared
        return Ok(stopped)

    def status(self) -> StatusReport:
        """Read infra reachability, topic count, group lag and newest poll lines."""
        present: Sequence[str]
        missing: Sequence[str]
        present, missing = _reachability()
        return StatusReport(
            present=present,
            missing=missing,
            topic_messages=_topic_messages(),
            group=_group_offset(),
            last_poll=_last_poll(),
        )


def _last_poll() -> dict[PipelineName, str]:
    """Newest observed poll line per pipeline whose child is still running."""
    polls: dict[PipelineName, str] = {}
    entries: dict[int, tuple[PipelineName, str]] = _read_state()
    pid: int
    for pid in entries:
        name, poll = entries[pid]
        if not poll:
            continue
        if _alive(pid) and _is_pipeline_process(pid):
            polls[name] = poll
    return polls


# ── CONTROL ────────────────────────────────────────────────────────────


def build_parser() -> argparse.ArgumentParser:
    """Argument parser exposing exactly the four verbs."""
    parser: argparse.ArgumentParser = argparse.ArgumentParser(
        prog="newspipe",
        description="Start, stop and inspect the newspipe pipelines.",
    )
    subparsers: argparse._SubParsersAction[argparse.ArgumentParser] | None = (
        parser.add_subparsers(dest="verb", required=True)
    )
    if subparsers is None:
        raise RuntimeError("argument parser did not provide subparsers")
    subparsers.add_parser("up", help="apply the infrastructure and wait for readiness")
    run_parser: argparse.ArgumentParser = subparsers.add_parser(
        "run", help="start the selected pipeline children"
    )
    run_parser.add_argument(
        "selection",
        choices=("ingestion", "enrichment", "both"),
        help="which pipeline to start",
    )
    subparsers.add_parser("down", help="stop the pipeline children this launcher started")
    subparsers.add_parser("status", help="print infra reachability, offsets and newest poll lines")
    return parser


def main(argv: list[str]) -> int:
    """Dispatch one verb and return its exit code."""
    parsed: argparse.Namespace = build_parser().parse_args(argv)
    verb: str = str(parsed.verb)
    ops: SubprocessOps = SubprocessOps()
    if verb == "up":
        outcome: Ok[ReadinessReport] | Err[Exception] = do_up(ops)
        if isinstance(outcome, Err):
            print(f"up failed: {outcome.error}", file=sys.stderr)
            return 1
        print("ready: " + ", ".join(outcome.value.present))
        return 0
    if verb == "run":
        selection: str = str(parsed.selection)
        names: Sequence[PipelineName] = _selection(selection)
        if not names:
            print("run failed: no pipeline selected", file=sys.stderr)
            return 1
        started: Ok[Sequence[int]] | Err[Exception] = do_run(ops, names)
        if isinstance(started, Err):
            print(f"run failed: {started.error}", file=sys.stderr)
            return 1
        pid: int
        for pid in started.value:
            print(f"started pid={pid}", flush=True)
        try:
            code: int = do_wait(ops)
        except KeyboardInterrupt:
            stopped: Ok[int] | Err[Exception] = do_down(ops)
            if isinstance(stopped, Err):
                print(f"interrupted, stop failed: {stopped.error}", file=sys.stderr)
                return 1
            print("interrupted: children stopped", file=sys.stderr)
            return 0
        return 0 if code == 0 else 1
    if verb == "down":
        stopped_result: Ok[int] | Err[Exception] = do_down(ops)
        if isinstance(stopped_result, Err):
            print(f"down failed: {stopped_result.error}", file=sys.stderr)
            return 1
        print(f"stopped {stopped_result.value} pipeline process(es)")
        return 0
    report_result: Ok[StatusReport] | Err[Exception] = do_status(ops)
    if isinstance(report_result, Err):
        print(f"status failed: {report_result.error}", file=sys.stderr)
        return 1
    report: StatusReport = report_result.value
    print("infra present: " + (", ".join(report.present) if report.present else "none"))
    if report.missing:
        print("infra missing: " + ", ".join(report.missing))
    if report.topic_messages is None:
        print(f"{TOPIC}: count unavailable")
    else:
        print(f"{TOPIC}: {report.topic_messages} message(s)")
    if report.group is None:
        print(f"{ENRICHMENT_GROUP}: no committed offset")
    else:
        print(
            f"{ENRICHMENT_GROUP}: committed={report.group.committed} "
            f"end={report.group.log_end} lag={report.group.lag}"
        )
    if not report.last_poll:
        print("poll: no running pipeline observed")
    else:
        polled: PipelineName
        for polled in report.last_poll:
            print(f"{polled}: {report.last_poll[polled]}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
