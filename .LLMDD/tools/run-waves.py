#!/usr/bin/env python3
"""run-waves.py - Execute CUs in waves with live coloured output and credit tracking.

Platform and shell agnostic replacement for run-waves.fish.
Requires: Python 3.8+, opencode CLI on PATH, git repo.
"""

import argparse
import json
import os
import re
import subprocess
import sys
import threading
from datetime import datetime
from pathlib import Path
import asyncio
import concurrent.futures
import typing
from dataclasses import dataclass
from typing import Generic, TypeVar, Any


# ── Defaults ───────────────────────────────────────────────────────

DEFAULT_CODER_MODEL = "opencode/muse-spark-1.3-contributor-free"
DEFAULT_LEAD_MODEL = "opencode/big-pickle"
DEFAULT_CU_TIMEOUT = 300
DEFAULT_LEAD_TIMEOUT = 600
DEFAULT_MAX_FIX_ITERATIONS = 3

# ── Fallback defaults ───────────────────────────────────────────────
DEFAULT_CODER_FALLBACKS = [
    "opencode/big-pickle",
]

# ── Monadic Result Type ────────────────────────────────────────────

T = TypeVar("T")
E_co = TypeVar("E_co", bound=Exception, covariant=True)


@dataclass(frozen=True)
class Ok(Generic[T]):
    """Success monad — contains a value."""
    value: T


@dataclass(frozen=True)
class Err(Generic[E_co]):
    """Failure monad — contains an exception."""
    error: E_co


@dataclass(frozen=True)
class CoderOutput:
    """Result of a single coder run."""
    rc: int
    files_changed: list[str]
    usage: "TokenUsage"
    model_used: str


# ── I/O Boundary — Monadic Result Wrappers (only try/except in the tool) ──

def _popen(cmd: list[str], cwd: Path) -> Ok[subprocess.Popen] | Err[Exception]:
    """Spawn subprocess. Returns Ok(proc) or Err(FileNotFoundError|OSError)."""
    try:
        return Ok(subprocess.Popen(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, cwd=cwd
        ))
    except (FileNotFoundError, OSError) as e:
        return Err(e)


def _open_file(path: Path, mode: str = "r") -> Ok[typing.IO] | Err[Exception]:
    """Open a file. Returns Ok(fh) or Err(OSError|IOError)."""
    try:
        return Ok(open(path, mode))
    except (OSError, IOError) as e:
        return Err(e)


def _git_run(*args: str, cwd: Path) -> Ok[str] | Err[Exception]:
    """Run a git command. Returns Ok(stdout) or Err."""
    try:
        r = subprocess.run(
            ["git", *args], capture_output=True, text=True, cwd=cwd, timeout=30
        )
        return Ok(r.stdout.strip())
    except (subprocess.TimeoutExpired, subprocess.CalledProcessError, OSError) as e:
        return Err(e)


def _read_file_text(path: Path) -> Ok[str] | Err[Exception]:
    """Read file as text. Returns Ok(text) or Err."""
    try:
        return Ok(path.read_text(errors="replace"))
    except (OSError, IOError) as e:
        return Err(e)


def _write_file_text(path: Path, text: str) -> Ok[None] | Err[Exception]:
    """Write text to file. Returns Ok(None) or Err."""
    try:
        path.write_text(text)
        return Ok(None)
    except (OSError, IOError) as e:
        return Err(e)


# ── Future-safe Monadic Wrapper ───────────────────────────────────

async def future_result(awaitable: typing.Awaitable) -> Ok | Err:
    """Await an awaitable and wrap its result in Ok/Err.

    This is the BRIDGE between asyncio Futures (which raise exceptions
    on await when the wrapped callable fails) and the Result monad.
    It contains the ONE try/except in the async orchestration layer.
    Every caller uses match/case on the returned Ok|Err.
    """
    try:
        return Ok(await awaitable)
    except Exception as e:
        return Err(e)


# ── Refusal / Error Detection ───────────────────────────────────────

_REFUSAL_PATTERNS = [
    # Hard refusals
    "I'm sorry, but I'm currently unable to assist",
    "I'm sorry, but I cannot",
    "I'm sorry, but I can't",
    "I'm not able to assist with that",
    "I'm unable to assist with",
    "I cannot assist with that",
    "I can't assist with that",
    "I'm not able to help with",
    "I cannot help with that",
    "I'm sorry, but I don't have access",
    "I don't have the tools needed",
    "I currently don't have the tools",
    "don't have the tools needed",
    "don't have the tools to",
    "lack the tools",
    "without the necessary tools",
    "don't have access to the necessary tool",
    "I'm not permitted to",
    "I'm not allowed to",
    "This request violates",
    "I must decline",
    "I'm afraid I cannot",
    # Partial refusals / confusion
    "I couldn't find the file",
    "I'm sorry, but I couldn't find",
    "I'm unable to find",
    "I cannot find",
    "I can't find",
    "unable to find the specified",
    "Could you please double-check",
    "I'm not sure what you mean",
    "I don't understand what you're asking",
    "I'm not sure I understand",
    "I'm confused about",
    "I'm having trouble understanding",
    "I'm not clear on what",
    "I'm not sure how to",
    "I don't know how to",
    "I'm unable to locate",
    "I can't locate",
    "I'm not finding",
    "must match exactly",
    "text must match",
    # Capability denials
    "I don't have the capability to access or modify",
    "I currently don't have the capability",
    "I don't have the ability to",
    "I'm not able to access",
    "I cannot access",
    "I can't access",
    "I'm unable to access",
    "I don't have direct access",
    "I don't have file system access",
    "I'm here to help, but I currently",
    "I don't have the tools",
    "I'm not equipped to",
    "I lack the capability",
]

_ERROR_PATTERNS = [
    # Process-level fatal signals only (raw stderr, never JSON event text).
    # Coder tool errors, compiler output, and opencode warnings MUST NOT
    # fail a CU — only a dead process or an explicit opencode error event.
    "Traceback (most recent call last)",
    "Exception in thread",
    "panic:",
    "FATAL:",
]

# ── Model Fallback Chain ────────────────────────────────────────────

class ModelFallbackChain:
    """Manages fallback order for model attempts."""
    
    def __init__(self, primary_model: str, fallback_models: list[str] | None = None):
        self.chain = [primary_model]
        if fallback_models:
            self.chain.extend(fallback_models)
    
    def __iter__(self):
        return iter(self.chain)
    
    def __len__(self):
        return len(self.chain)
    
    def __repr__(self):
        return " → ".join(self.chain)


def _detect_refusal(log_file: Path) -> bool:
    """Check if the log contains refusal patterns."""
    try:
        text = log_file.read_text(errors="replace")
        for pattern in _REFUSAL_PATTERNS:
            if pattern.lower() in text.lower():
                return True
    except Exception:
        pass
    return False


def _detect_error(log_file: Path) -> bool:
    """Check if the log contains error patterns (but not tool output)."""
    try:
        lines = log_file.read_text(errors="replace").splitlines()
        for line in lines:
            if "toolResult" in line or "tool_result" in line:
                continue
            for pattern in _ERROR_PATTERNS:
                if pattern in line:
                    return True
    except Exception:
        pass
    return False


# ── Token / Cost tracking ─────────────────────────────────────────

def _extract_step_tokens(ev: dict) -> tuple[dict, float, str]:
    """Extract (tokens_dict, cost, reason) from a step_finish event."""
    part = ev.get("part", {})
    tok = part.get("tokens", ev.get("tokens", {}))
    cost = part.get("cost", ev.get("cost", 0))
    reason = part.get("reason", ev.get("reason", ""))
    return tok, cost, reason


class TokenUsage:
    """Accumulated token and cost data from opencode step_finish events."""
    __slots__ = ("input", "output", "reasoning", "cache_read", "cache_write",
                 "total", "cost", "session_id", "model")

    def __init__(self):
        self.input = 0
        self.output = 0
        self.reasoning = 0
        self.cache_read = 0
        self.cache_write = 0
        self.total = 0
        self.cost = 0.0
        self.session_id = ""
        self.model = ""

    def add_from_step_finish(self, ev: dict):
        tok, cost, _reason = _extract_step_tokens(ev)
        self.input += tok.get("input", 0)
        self.output += tok.get("output", 0)
        self.reasoning += tok.get("reasoning", 0)
        cache = tok.get("cache", {})
        self.cache_read += cache.get("read", 0)
        self.cache_write += cache.get("write", 0)
        self.total += tok.get("total", 0)
        self.cost += cost

    def is_empty(self):
        return self.total == 0 and self.cost == 0.0

    def dict(self):
        return {
            "input": self.input, "output": self.output,
            "reasoning": self.reasoning,
            "cache_read": self.cache_read, "cache_write": self.cache_write,
            "total": self.total, "cost": self.cost,
            "session_id": self.session_id, "model": self.model,
        }


class WaveCredits:
    def __init__(self, wave_num: int):
        self.wave_num = wave_num
        self.coders: dict[str, TokenUsage] = {}

    def wave_totals(self) -> TokenUsage:
        totals = TokenUsage()
        for u in self.coders.values():
            totals.input += u.input
            totals.output += u.output
            totals.reasoning += u.reasoning
            totals.cache_read += u.cache_read
            totals.cache_write += u.cache_write
            totals.total += u.total
            totals.cost += u.cost
        return totals


class CreditTracker:
    def __init__(self):
        self.waves: list[WaveCredits] = []
        self._accumulated = TokenUsage()

    def start_wave(self, wave_num: int) -> WaveCredits:
        wc = WaveCredits(wave_num)
        self.waves.append(wc)
        return wc

    def accumulate(self, usage: TokenUsage):
        self._accumulated.input += usage.input
        self._accumulated.output += usage.output
        self._accumulated.reasoning += usage.reasoning
        self._accumulated.cache_read += usage.cache_read
        self._accumulated.cache_write += usage.cache_write
        self._accumulated.total += usage.total
        self._accumulated.cost += usage.cost

    @property
    def accumulated(self) -> TokenUsage:
        return self._accumulated


# ── ANSI Colours ───────────────────────────────────────────────────

class C:
    RESET   = "\033[0m"
    BOLD    = "\033[1m"
    DIM     = "\033[2m"
    ULINE   = "\033[4m"
    RED     = "\033[31m"
    GREEN   = "\033[32m"
    YELLOW  = "\033[33m"
    BLUE    = "\033[34m"
    MAGENTA = "\033[35m"
    CYAN    = "\033[36m"
    WHITE   = "\033[37m"
    BR_RED    = "\033[91m"
    BR_GREEN  = "\033[92m"
    BR_YELLOW = "\033[93m"
    BR_BLUE   = "\033[94m"
    BR_MAG    = "\033[95m"
    BR_CYAN   = "\033[96m"
    BR_WHITE  = "\033[97m"
    BG_RED   = "\033[41m"
    BG_GREEN = "\033[42m"
    BG_BLUE  = "\033[44m"

    @classmethod
    def disable(cls):
        for attr in dir(cls):
            if attr.isupper() and attr != "RESET":
                setattr(cls, attr, "")
        cls.RESET = ""


CODER_COLOURS = [
    (C.BR_CYAN,   "CYN"),
    (C.BR_GREEN,  "GRN"),
    (C.BR_YELLOW, "YLW"),
    (C.BR_MAG,    "MAG"),
    (C.BR_BLUE,   "BLU"),
    (C.BR_RED,    "RED"),
    (C.YELLOW,     "yel"),
    (C.CYAN,       "cyn"),
]


def _no_colour_support():
    if os.environ.get("NO_COLOR"):
        return True
    if not sys.stdout.isatty():
        return True
    return False

if _no_colour_support():
    C.disable()


# ── Formatting helpers ─────────────────────────────────────────────

def _fmt_tokens(n: int) -> str:
    if n >= 1_000_000:
        return f"{n / 1_000_000:.1f}M"
    if n >= 1_000:
        return f"{n / 1_000:.1f}K"
    return str(n)

def _fmt_cost(c: float) -> str:
    if c == 0.0:
        return "$0.00"
    if c < 0.01:
        return f"${c:.4f}"
    return f"${c:.2f}"

def _banner_line(width=63):
    return f"{C.BR_CYAN}{'═' * width}{C.RESET}"

def _info(msg):
    print(f"  {C.DIM}{msg}{C.RESET}")

def _success(msg):
    print(f"  {C.BR_GREEN}✓{C.RESET} {msg}")

def _fail(msg):
    print(f"  {C.BR_RED}✗{C.RESET} {msg}")

def _warn(msg):
    print(f"  {C.BR_YELLOW}⚠{C.RESET} {msg}")

def _header(msg):
    print(f"  {C.BOLD}{C.BR_WHITE}{msg}{C.RESET}")


def _print_credit_table(cu_usages: dict[str, TokenUsage], label: str,
                         colour: str = ""):
    """Print credit usage table — cost is the primary column."""
    if not cu_usages:
        return

    print(f"    {colour}{C.BOLD}{label}{C.RESET}")
    hdr = (f"    {C.DIM}{'CU':<22} {'Cost':>8}  "
           f"{'In':>8} {'Out':>8} {'Reason':>8} {'CacheR':>8} {'Model':<25}{C.RESET}")
    print(hdr)
    print(f"    {C.DIM}{'─' * 97}{C.RESET}")

    wt = TokenUsage()
    for cu_id, u in cu_usages.items():
        if u.is_empty():
            continue
        cost_str = _fmt_cost(u.cost)
        cost_col = C.BR_YELLOW if u.cost > 0 else C.DIM
        model_str = u.model if u.model else "unknown"
        # Shorten model name for display
        if "/" in model_str:
            model_str = model_str.split("/")[-1]
        if len(model_str) > 24:
            model_str = model_str[:21] + "..."
        print(f"    {colour}{cu_id:<22}{C.RESET}"
              f" {cost_col}{cost_str:>8}{C.RESET}  "
              f"{_fmt_tokens(u.input):>8}"
              f" {_fmt_tokens(u.output):>8}"
              f" {_fmt_tokens(u.reasoning):>8}"
              f" {_fmt_tokens(u.cache_read):>8}"
              f" {C.DIM}{model_str:<25}{C.RESET}")
        wt.input += u.input
        wt.output += u.output
        wt.reasoning += u.reasoning
        wt.cache_read += u.cache_read
        wt.cache_write += u.cache_write
        wt.total += u.total
        wt.cost += u.cost

    total_cost_col = C.BR_YELLOW if wt.cost > 0 else C.DIM
    print(f"    {C.DIM}{'─' * 97}{C.RESET}")
    print(f"    {C.BOLD}{'TOTAL':<22}{C.RESET}"
          f" {total_cost_col}{_fmt_cost(wt.cost):>8}{C.RESET}  "
          f"{_fmt_tokens(wt.input):>8}"
          f" {_fmt_tokens(wt.output):>8}"
          f" {_fmt_tokens(wt.reasoning):>8}"
          f" {_fmt_tokens(wt.cache_read):>8}")
    print()


def _print_accumulated(tracker: CreditTracker):
    """Print accumulated totals — cost is the hero number."""
    a = tracker.accumulated
    if a.is_empty():
        return
    print(f"    {C.BOLD}{C.BR_WHITE}ACCUMULATED TOTALS{C.RESET}")
    print(f"    {C.DIM}{'─' * 50}{C.RESET}")
    cost_col = C.BR_YELLOW if a.cost > 0 else C.DIM
    print(f"    {C.BOLD}{'Total cost:':<25}{C.RESET} "
          f"{cost_col}{C.BOLD}{_fmt_cost(a.cost)}{C.RESET}")
    print(f"    {'Input tokens:':<25} {C.BOLD}{_fmt_tokens(a.input)}{C.RESET}")
    print(f"    {'Output tokens:':<25} {C.BOLD}{_fmt_tokens(a.output)}{C.RESET}")
    print(f"    {'Reasoning tokens:':<25} {C.BOLD}{_fmt_tokens(a.reasoning)}{C.RESET}")
    print(f"    {'Cache read:':<25} {C.BOLD}{_fmt_tokens(a.cache_read)}{C.RESET}")
    print()


# ── CLI ────────────────────────────────────────────────────────────

def parse_args(argv=None):
    p = argparse.ArgumentParser(
        description="Execute CUs in waves with live coloured output and credit tracking.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("--itr", required=True, help="Path to compiled ITR directory")
    p.add_argument("--app", required=True, help="Application name")
    p.add_argument("--waves", default=None, help="Wave definition JSON file")
    p.add_argument("--coder-model", default=DEFAULT_CODER_MODEL)
    p.add_argument("--coder-fallbacks", nargs="*", default=DEFAULT_CODER_FALLBACKS,
                   help="Fallback models if primary refuses/errors")
    p.add_argument("--lead-model", default=DEFAULT_LEAD_MODEL)
    p.add_argument("--max-fix-iterations", type=int, default=DEFAULT_MAX_FIX_ITERATIONS)
    p.add_argument("--cu-timeout", type=int, default=DEFAULT_CU_TIMEOUT,
                   help="Kill coder if no activity for N seconds (activity-based timeout)")
    p.add_argument("--lead-timeout", type=int, default=DEFAULT_LEAD_TIMEOUT,
                   help="Kill lead if no activity for N seconds (activity-based timeout)")
    p.add_argument("--project", default=None,
                   help="Project root directory (default: auto-detect from .opencode/agents/)")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--no-per-cu-lead", action="store_true",
                   help="Disable per-CU code lead (run wave-level lead instead)")
    p.add_argument("--no-color", action="store_true")
    return p.parse_args(argv)


# ── CU ID → file resolution ───────────────────────────────────────

_CUID_RE = re.compile(r"^#\s*CU-ID:\s*(.+)$")

_COMPONENT_FILES = ("COORDINATES.itr", "REQUIREMENTS.itr",
                    "IMPLEMENTATION_STEPS.itr", "ACCEPTANCE.itr")


def _file_cu_id(f: Path) -> str | None:
    """Return the CU-ID header of a frame file, or None."""
    try:
        with open(f) as fh:
            for line in fh:
                m = _CUID_RE.match(line)
                if m:
                    return m.group(1).strip()
    except Exception:
        pass
    return None


def _cu_folder_ref(subdir: Path) -> Path | None:
    """Reference for a CU subdirectory: the folder itself when it holds
    component files, else the CU-ID-bearing frame inside, else None."""
    if not subdir.is_dir():
        return None
    if any((subdir / name).is_file() for name in _COMPONENT_FILES):
        return subdir
    for f in sorted(subdir.iterdir()):
        if f.suffix == ".itr" and f.is_file() and _file_cu_id(f):
            return f
    return None


def _cu_read_instructions(cu_ref: Path) -> str:
    """How the agent reads this CU: folder components or single frame."""
    if cu_ref.is_dir():
        return (f"Read the CU folder: {cu_ref}\n"
                f"  - COORDINATES.itr = DTR area this CU affects/creates\n"
                f"  - REQUIREMENTS.itr = what part of the feature to build\n"
                f"  - IMPLEMENTATION_STEPS.itr = your stepwise instructions, follow them exactly\n"
                f"  - ACCEPTANCE.itr = your tests, all must pass")
    return f"Read the CU frame file: {cu_ref}"


def build_cu_map(itr_path: Path) -> dict[str, Path]:
    cu_map = {}
    # Legacy root-level frames first, then per-CU subdirectories (win).
    for f in sorted(itr_path.iterdir()):
        if f.suffix == ".itr" and f.is_file():
            cu_id = _file_cu_id(f)
            if cu_id:
                cu_map[cu_id] = f
    for sub in sorted(p for p in itr_path.iterdir() if p.is_dir()):
        ref = _cu_folder_ref(sub)
        if ref is not None:
            cu_id = sub.name if ref.is_dir() else (_file_cu_id(ref) or sub.name)
            cu_map[cu_id] = ref
    return cu_map


def resolve_cu_file(cu_id: str, cu_map: dict[str, Path], itr_path: Path) -> Path | None:
    if cu_id in cu_map:
        return cu_map[cu_id]
    direct = itr_path / f"{cu_id}.itr"
    if direct.is_file():
        return direct
    subdir = itr_path / cu_id
    ref = _cu_folder_ref(subdir)
    if ref is not None:
        return ref
    nested = subdir / f"{cu_id}.itr"
    return nested if nested.is_file() else None


# ── Wave detection ─────────────────────────────────────────────────

def detect_waves(itr_path: Path, waves_file: str | None) -> list[dict]:
    if waves_file:
        p = Path(waves_file)
        if p.is_file():
            return json.loads(p.read_text())
        _warn(f"Waves file not found: {waves_file}")
    cu_map = build_cu_map(itr_path)
    cu_ids = sorted(cu_map.keys())
    return [{"wave": 1, "cus": cu_ids}] if cu_ids else []


# ── Git helpers ────────────────────────────────────────────────────

def _git(*args, cwd: Path | None = None) -> str:
    try:
        r = subprocess.run(
            ["git"] + list(args),
            capture_output=True, text=True, cwd=cwd or Path.cwd(), timeout=30,
        )
        return r.stdout.strip()
    except Exception:
        return ""

def git_diff_names(work_dir: Path, ref: str) -> list[str]:
    names = _git("diff", "--name-only", ref, cwd=work_dir)
    return names.splitlines() if names else []

def git_stash_ref(work_dir: Path) -> str:
    return _git("rev-parse", "HEAD", cwd=work_dir)


# ── Live-streaming with JSON parsing ──────────────────────────────

def _stream_process_json(cmd: list[str], log_fh, colour: str, prefix: str,
                         timeout: int, work_dir: Path,
                         token_usage: TokenUsage) -> Ok[int] | Err[Exception]:
    """Stream JSON events with activity-based timeout.
    Returns Ok(exit_code) or Err(FileNotFoundError|OSError|TimeoutError).
    """
    match _popen(cmd, cwd=work_dir):
        case Ok(proc):
            pass
        case Err(e):
            _fail(f"Command not found: {cmd[0]}")
            return Err(e)

    _done = threading.Event()
    last_activity = [datetime.now()]


    def _reader():
        assert proc.stdout is not None
        for raw in proc.stdout:
            line = raw.decode("utf-8", errors="replace").rstrip("\n")
            log_fh.write(line + "\n")
            log_fh.flush()

            # Any output from the process counts as activity
            last_activity[0] = datetime.now()

            try:
                ev = json.loads(line)
                ev_type = ev.get("type", "")

                sid = ev.get("sessionID", "")
                if sid and not token_usage.session_id:
                    token_usage.session_id = sid

                if ev_type == "model_start":
                    model_info = ev.get("model", {})
                    if isinstance(model_info, dict):
                        token_usage.model = model_info.get("id", "")
                    elif isinstance(model_info, str):
                        token_usage.model = model_info

                if ev_type == "step_finish":
                    token_usage.add_from_step_finish(ev)

                display = _format_event(ev, colour)
                if display:
                    dlines = display.split("\n")
                    print(f"  {colour}{prefix}{C.RESET} {C.DIM}│{C.RESET} {dlines[0]}",
                          flush=True)
                    for cont in dlines[1:]:
                        print(f"  {colour}{prefix}{C.RESET} {C.DIM}│{C.RESET} {cont}",
                              flush=True)

            except (json.JSONDecodeError, ValueError):
                print(f"  {colour}{prefix}{C.RESET} {C.DIM}│{C.RESET} {line}",
                      flush=True)

        _done.set()

    t = threading.Thread(target=_reader, daemon=True)
    t.start()

    while not _done.is_set():
        idle_secs = (datetime.now() - last_activity[0]).total_seconds()

        # Kill if no activity for full timeout
        if idle_secs > timeout:
            proc.kill()
            t.join(timeout=5)
            _fail(f"Timed out — no activity for {timeout}s")
            log_fh.write(f"\n[TIMED OUT — no activity for {timeout}s]\n")
            return Err(TimeoutError(f"No activity for {timeout}s"))
        _done.wait(timeout=0.5)

    proc.wait()
    return Ok(proc.returncode)


def _format_event(ev: dict, colour: str) -> str | None:
    t = ev.get("type", "")

    if t == "step_start":
        return f"{C.DIM}▸ step start{C.RESET}"

    if t == "step_finish":
        tok, cost, reason = _extract_step_tokens(ev)
        parts = []
        if cost > 0:
            parts.append(f"cost:{_fmt_cost(cost)}")
        if tok.get("input"):
            parts.append(f"in:{_fmt_tokens(tok['input'])}")
        if tok.get("output"):
            parts.append(f"out:{_fmt_tokens(tok['output'])}")
        if tok.get("reasoning"):
            parts.append(f"reason:{_fmt_tokens(tok['reasoning'])}")
        cache = tok.get("cache", {})
        if cache.get("read"):
            parts.append(f"cR:{_fmt_tokens(cache['read'])}")
        if reason:
            parts.append(f"({reason})")
        token_str = " ".join(parts) if parts else "no data"
        return f"{C.BR_GREEN}▸ done{C.RESET} {C.DIM}{token_str}{C.RESET}"

    if t == "model_start":
        model = ev.get("model", {})
        if isinstance(model, dict):
            name = model.get("id", "unknown")
            provider = model.get("providerID", "")
            return f"{C.DIM}▸ model: {provider}/{name}{C.RESET}"
        return f"{C.DIM}▸ model start{C.RESET}"

    if t == "model_finish":
        return None

    if t == "text":
        part = ev.get("part", {})
        text = part.get("text", "")
        if not text:
            return None
        # Show what the agent actually says, in full, one line per line.
        # Truncation here is what made runs unreadable — cap high.
        if len(text) > 4000:
            text = text[:3997] + "..."
        return "\n".join(f"{C.BR_WHITE}📝 {ln}{C.RESET}" for ln in text.split("\n"))

    if t == "tool_invocation_start":
        part = ev.get("part", {})
        tool = part.get("toolName", "unknown")
        return f"{C.BR_YELLOW}🔧 {tool}{C.RESET}"

    if t == "tool_result":
        part = ev.get("part", {})
        tool = part.get("toolName", "unknown")
        result = part.get("result", "")
        if isinstance(result, str) and len(result) > 80:
            result = result[:77] + "..."
        elif isinstance(result, dict):
            result = json.dumps(result, ensure_ascii=False)
            if len(result) > 80:
                result = result[:77] + "..."
        return f"{C.DIM}  ↳ {tool}: {result}{C.RESET}"

    if t == "permission_request":
        part = ev.get("part", {})
        action = part.get("action", "")
        resource = part.get("resource", "")
        return f"{C.BR_MAG}🔐 permission: {action} {resource}{C.RESET}"

    return None


# ── Coder / Lead execution ─────────────────────────────────────────

def _usage_line(usage: TokenUsage, colour: str = "") -> str:
    """Format a single-line usage summary: cost is prominent."""
    if usage.is_empty():
        return ""
    cost_str = _fmt_cost(usage.cost)
    cost_col = C.BR_YELLOW if usage.cost > 0 else C.DIM
    return (f"    {cost_col}{C.BOLD}cost: {cost_str}{C.RESET}"
            f" {C.DIM}| in={_fmt_tokens(usage.input)}"
            f" out={_fmt_tokens(usage.output)}"
            f" reason={_fmt_tokens(usage.reasoning)}"
            f" cacheR={_fmt_tokens(usage.cache_read)}{C.RESET}")


def run_coder(cu_file: Path, cu_id: str, fallback_chain: ModelFallbackChain,
              feedback_dir: Path, log_dir: Path, timeout: int,
              project_dir: Path, coder_colour: tuple[str, str],
              stash_ref: str = "") -> Ok[CoderOutput] | Err[Exception]:
    """Run coder with model fallback. Returns Ok(CoderOutput) or Err.

    If stash_ref is provided (git HEAD hash captured before parallel dispatch),
    file tracking uses `git diff --name-only <stash_ref>` to isolate changes
    made during this CU's execution from other parallel CUs.
    """
    colour_code, colour_label = coder_colour
    prefix = f"[{cu_id}]"
    combined_usage = TokenUsage()
    model_used = ""
    files_changed: list[str] = []
    stream_rc = 1

    prompt = (
        f"Implement ONLY CU {cu_id}. Rules:\n"
        f"1. {_cu_read_instructions(cu_file)}\n"
        f"2. Implement ONLY the changes described in that CU\n"
        f"3. Modify ONLY the files mentioned in that CU\n"
        f"4. Do NOT read any other CU folders or frame files\n"
        f"5. Do NOT implement any other CUs\n"
        f"6. Write feedback to {feedback_dir / f'{cu_id}.feedback.txt'}\n"
        f"7. Include COMMIT_MESSAGE field in feedback\n"
        f"8. Do NOT commit \u2014 the code lead will commit\n\n"
        f"Your scope is EXACTLY one CU: {cu_id}. Nothing else."
    )

    print(f"  {colour_code}{C.BOLD}\u25b8 {colour_code}{C.BOLD}CU {cu_id}{C.RESET}"
          f" {C.DIM}\u2192 {cu_file.name}{C.RESET}  {C.DIM}({colour_label}){C.RESET}")
    print(f"    {C.DIM}{'\u2500' * 55}{C.RESET}")

    for model_idx, model in enumerate(fallback_chain):
        log_file = log_dir / f"{cu_id}.attempt-{model_idx}.log"
        attempt_usage = TokenUsage()

        if model_idx > 0:
            _warn(f"Fallback attempt {model_idx}: trying {C.BOLD}{model}{C.RESET}")
            print(f"    {C.DIM}Trying fallback model: {model}{C.RESET}")

        cmd = [
            "opencode", "run",
            "--dir", str(project_dir),
            "--model", model,
            "--agent", "llmdd/coder",
            "--format", "json",
            "--auto",
            prompt,
        ]

        # Open log file via monadic wrapper
        match _open_file(log_file, "w"):
            case Ok(log_fh):
                match _stream_process_json(cmd, log_fh, colour_code, prefix,
                                           timeout, project_dir, attempt_usage):
                    case Ok(rc):
                        stream_rc = rc
                    case Err(e):
                        log_fh.close()
                        return Err(e)
                log_fh.close()
            case Err(e):
                return Err(e)

        # Check for issues via monadic file read.
        # An opencode `"type": "error"` event is authoritative. Plain-text
        # patterns only apply to non-JSON lines (raw process stderr) so
        # coder tool errors and compiler output can never fail a CU.
        match _read_file_text(log_file):
            case Ok(text):
                is_refusal = any(p.lower() in text.lower() for p in _REFUSAL_PATTERNS)
                is_error_lines = False
                for line in text.splitlines():
                    try:
                        ev = json.loads(line)
                        if isinstance(ev, dict) and ev.get("type") == "error":
                            is_error_lines = True
                            break
                        continue
                    except (json.JSONDecodeError, ValueError):
                        pass
                    if "toolResult" in line or "tool_result" in line:
                        continue
                    for p in _ERROR_PATTERNS:
                        if p in line:
                            is_error_lines = True
                            break
                    if is_error_lines:
                        break
            case Err(_):
                is_refusal = False
                is_error_lines = False

        is_error = is_error_lines if stream_rc == 0 else False
        is_timeout = stream_rc == 124

        # Combine usage
        combined_usage.input += attempt_usage.input
        combined_usage.output += attempt_usage.output
        combined_usage.reasoning += attempt_usage.reasoning
        combined_usage.cache_read += attempt_usage.cache_read
        combined_usage.cache_write += attempt_usage.cache_write
        combined_usage.total += attempt_usage.total
        combined_usage.cost += attempt_usage.cost
        if attempt_usage.session_id:
            combined_usage.session_id = attempt_usage.session_id
        combined_usage.model = model

        # Git diff from project root — use stash_ref to isolate per-CU changes
        # when running in parallel (avoids capturing changes from other CUs)
        diff_args = ["diff", "--name-only"]
        if stash_ref:
            diff_args.append(stash_ref)
        match _git_run(*diff_args, cwd=project_dir):
            case Ok(diff_out):
                files_changed = diff_out.splitlines() if diff_out else []
            case Err(_):
                files_changed = []

        # Determine if this attempt succeeded
        succeeded = stream_rc == 0 and not is_refusal and not is_error

        if succeeded:
            model_used = model
            if model_idx > 0:
                _success(f"CU {cu_id} completed with fallback {C.BOLD}{model}{C.RESET}"
                        f" {C.DIM}({len(files_changed)} files){C.RESET}")
            else:
                _success(f"CU {cu_id} completed {C.DIM}({len(files_changed)} files){C.RESET}")
            break
        else:
            reason = "refused" if is_refusal else ("error" if is_error else ("timeout" if is_timeout else f"exit {stream_rc}"))
            _warn(f"Model {C.BOLD}{model}{C.RESET} failed: {reason}")
            if model_idx == len(fallback_chain) - 1:
                model_used = model
                _fail(f"CU {cu_id} failed on all {len(fallback_chain)} model(s)")

                # Never clobber evidence: the coder may have written feedback
                # before failing. Only write the escalation stub when the
                # coder left no feedback file behind.
                coder_feedback = feedback_dir / f"{cu_id}.feedback.txt"
                if coder_feedback.is_file():
                    _warn(f"Keeping coder feedback at {coder_feedback}")
                    break

                feedback_content = f"""CU_ID={cu_id}
CODER_NAME=wave-runner
DATE={datetime.now().isoformat()}
CLARITY_RATING=1
AMBIGUOUS_LINES=all
MISSING_CONTEXT=all models failed
TOO_MUCH_DETAIL=0
ARCHITECTURE_DEVIATION=CU not implemented
ARCHITECTURE_DEVIATION.SEVERITY=CRITICAL
TIME_TAKEN_MINUTES=0
AI_CREDITS_USED={combined_usage.cost:.4f}
COMMIT_MESSAGE=N/A - CU not implemented
STATUS=ESCALATION
ESCALATION_REASON=all models failed to implement CU
ESCALATION_DETAIL=all {len(fallback_chain)} model(s) failed
VERIFICATION_RESULT=NOT_RUN
VERIFICATION_DETAILS=CU not implemented
"""
                match _write_file_text(feedback_dir / f"{cu_id}.feedback.txt", feedback_content):
                    case Ok(_):
                        pass
                    case Err(e):
                        _warn(f"Failed to write escalation feedback: {e}")
                break

    line = _usage_line(combined_usage)
    if line:
        print(line)

    if files_changed:
        for fc in files_changed:
            print(f"    {colour_code}\u251c{C.RESET} {fc}")

    if model_used:
        log_model_info = log_dir / f"{cu_id}.model-used.txt"
        match _write_file_text(log_model_info, f"model={model_used}\nfallback_level={model_idx}\n"):
            case Ok(_):
                pass
            case Err(e):
                _warn(f"Failed to write model info: {e}")

    print()
    return Ok(CoderOutput(
        rc=stream_rc,
        files_changed=files_changed,
        usage=combined_usage,
        model_used=model_used,
    ))


def run_code_lead(wave: int, num_waves: int, model: str, max_iterations: int,
                  feedback_dir: Path, log_dir: Path, timeout: int,
                  project_dir: Path, wave_cus: list[str]) -> tuple[int, TokenUsage]:
    log_file = log_dir / f"lead-wave-{wave}.log"
    colour = C.BR_MAG
    prefix = "[LEAD]"
    usage = TokenUsage()

    is_final = (wave == num_waves)

    if is_final:
        commit_instruction = (
            f"\n7. Read COMMIT_MESSAGE fields from all feedback files across all waves\n"
            f"8. Consolidate into a single commit message\n"
            f"9. Stage all changes and commit with the consolidated message\n"
        )
    else:
        commit_instruction = (
            f"\n7. Prepare a COMMIT_MESSAGE for this wave in {feedback_dir}/wave-{wave}-commit.txt\n"
            f"8. Do NOT commit \u2014 the final wave lead will commit\n"
        )

    prompt = (
        f"You are the code lead for wave {wave}.\n\n"
        f"1. Read feedback files for this wave\u2019s CUs: {', '.join(wave_cus)}\n"
        f"2. For any CU with STATUS=ESCALATION: read the CU frame file and implement it yourself\n"
        f"3. Write a feedback file for each escalated CU you implement\n"
        f"4. Run verification tests for all implemented CUs\n"
        f"5. Fix any test failures\n"
        f"6. Write verification report to {feedback_dir}/wave-{wave}-verification.txt\n"
        f"{commit_instruction}\n"
        f"Max fix iterations: {max_iterations}"
    )

    cmd = [
        "opencode", "run",
        "--dir", str(project_dir),
        "--model", model,
        "--agent", "verifier",
        "--format", "json",
        "--auto",
        prompt,
    ]

    print(f"  {colour}{C.BOLD}▸{C.RESET} {colour}{C.BOLD}Code Lead — Wave {wave} review{C.RESET}")
    print(f"    {C.DIM}{'─' * 55}{C.RESET}")

    match _open_file(log_file, "w"):
        case Ok(log_fh):
            match _stream_process_json(cmd, log_fh, colour, prefix,
                                      timeout, project_dir, usage):
                case Ok(rc):
                    pass
                case Err(e):
                    log_fh.close()
                    _fail(f"Lead review failed: {e}")
                    return 1, usage
            log_fh.close()
        case Err(e):
            _fail(f"Cannot open log: {e}")
            return 1, usage

    if rc == 0:
        _success(f"Lead review completed")
    else:
        _fail(f"Lead review failed (exit {rc})")

    line = _usage_line(usage)
    if line:
        print(line)
    print()
    return rc, usage




def run_per_cu_lead(cu_id: str, cu_file: Path, model: str, max_iterations: int,
                    feedback_dir: Path, log_dir: Path, timeout: int,
                    project_dir: Path) -> Ok[tuple[int, TokenUsage]] | Err[Exception]:
    """Run code lead for a single escalated CU immediately.
    Returns Ok((rc, usage)) or Err."""
    log_file = log_dir / f"lead-{cu_id}.log"
    colour = C.BR_MAG
    prefix = f"[LEAD-{cu_id}]"
    usage = TokenUsage()

    prompt = (
        f"You are the code lead. CU {cu_id} was escalated by the coder.\n\n"
        f"1. {_cu_read_instructions(cu_file)}\n"
        f"2. Read the escalation feedback at {feedback_dir / f'{cu_id}.feedback.txt'}\n"
        f"3. Implement the CU yourself (the coder failed)\n"
        f"4. Write updated feedback to {feedback_dir / f'{cu_id}.feedback.txt'}\n"
        f"5. Do NOT commit \u2014 the final wave lead will commit\n\n"
        f"Max fix iterations: {max_iterations}"
    )

    cmd = [
        "opencode", "run",
        "--dir", str(project_dir),
        "--model", model,
        "--agent", "verifier",
        "--format", "json",
        "--auto",
        prompt,
    ]

    print(f"  {colour}{C.BOLD}\u25b8 {colour}{C.BOLD}Lead \u2014 fixing {cu_id}{C.RESET}")
    print(f"    {C.DIM}{'\u2500' * 55}{C.RESET}")

    match _open_file(log_file, "w"):
        case Ok(log_fh):
            match _stream_process_json(cmd, log_fh, colour, prefix,
                                      timeout, project_dir, usage):
                case Ok(rc):
                    pass
                case Err(e):
                    log_fh.close()
                    return Err(e)
            log_fh.close()
        case Err(e):
            return Err(e)

    match rc:
        case 0:
            _success(f"Lead fixed {cu_id}")
        case _:
            _fail(f"Lead failed on {cu_id} (exit {rc})")

    line = _usage_line(usage)
    if line:
        print(line)
    print()
    return Ok((rc, usage))

def _is_escalated(cu_id: str, feedback_dir: Path) -> bool:
    """Check if a specific CU has ESCALATION status."""
    fb = feedback_dir / f"{cu_id}.feedback.txt"
    match _read_file_text(fb):
        case Ok(text):
            return "STATUS=ESCALATION" in text
        case Err(_):
            return False


# ── Escalation check ───────────────────────────────────────────────

def check_escalations(wave_cus: list[str], feedback_dir: Path) -> int:
    count = 0
    for cu_id in wave_cus:
        fb = feedback_dir / f"{cu_id}.feedback.txt"
        if fb.is_file():
            text = fb.read_text(errors="replace")
            if "STATUS=ESCALATION" in text:
                count += 1
                _warn(f"Escalation: {cu_id}")
    return count


# ── Convergence tracking ───────────────────────────────────────────

def load_convergence(path: Path) -> dict:
    if path.is_file():
        return json.loads(path.read_text())
    return {"waves": [], "status": "RUNNING"}

def save_convergence(path: Path, data: dict):
    path.write_text(json.dumps(data, indent=2) + "\n")

def update_convergence(path: Path, wave_num: int, wave_cus: list[str],
                       escalations: int, coder_ok: bool, lead_ok: bool,
                       files_changed: dict[str, list[str]],
                       credits: dict[str, dict]):
    data = load_convergence(path)
    data["waves"].append({
        "wave": wave_num, "cus": wave_cus,
        "escalations": escalations,
        "coder_status": 0 if coder_ok else 1,
        "lead_status": 0 if lead_ok else 1,
        "files_changed": files_changed,
        "credits": credits,
    })
    data["status"] = "RUNNING"
    save_convergence(path, data)


# ── Parallel wave execution ──────────────────────────────────────

async def run_wave_parallel(
    cu_files: list[tuple[str, Path]],
    wc: WaveCredits,
    tracker: CreditTracker,
    feedback_dir: Path,
    log_dir: Path,
    project_dir: Path,
    coder_model: str,
    coder_fallbacks: list[str],
    cu_timeout: int,
    lead_model: str,
    lead_timeout: int,
    max_fix_iterations: int,
    no_per_cu_lead: bool,
) -> tuple[dict[str, list[str]], bool]:
    """Run all CUs in a wave in parallel via ThreadPoolExecutor.

    Returns (wave_files_changed, all_passed).
    No try/except in this function — all error handling via Result monad + match/case.
    """
    wave_files_changed: dict[str, list[str]] = {}
    all_passed = True
    loop = asyncio.get_event_loop()

    # Capture git HEAD reference ONCE before parallel dispatch
    match _git_run("rev-parse", "HEAD", cwd=project_dir):
        case Ok(stash_ref):
            pass
        case Err(e):
            _fail(f"Cannot get git HEAD: {e}")
            return {}, False

    with concurrent.futures.ThreadPoolExecutor(max_workers=len(cu_files)) as pool:
        # Dispatch all coders in parallel
        coder_futures: list[tuple[str, Path, asyncio.Future]] = []
        for idx, (cu_id, cu_file) in enumerate(cu_files):
            coder_colour = CODER_COLOURS[idx % len(CODER_COLOURS)]
            fallback_chain = ModelFallbackChain(coder_model, coder_fallbacks)

            future = loop.run_in_executor(
                pool,
                run_coder,
                cu_file, cu_id, fallback_chain,
                feedback_dir, log_dir, cu_timeout,
                project_dir, coder_colour,
                stash_ref,
            )
            coder_futures.append((cu_id, cu_file, future))

        # Await all coders with monadic wrapping
        for cu_id, cu_file, future in coder_futures:
            match await future_result(future):
                case Ok(CoderOutput(rc=rc, files_changed=files, usage=usage, model_used=model)):
                    wave_files_changed[cu_id] = files
                    wc.coders[cu_id] = usage
                    tracker.accumulate(usage)

                    if rc != 0:
                        all_passed = False

                case Err(e):
                    _fail(f"CU {cu_id} crashed: {e}")
                    wave_files_changed[cu_id] = []
                    all_passed = False

    # Per-CU lead checks (sequential after all coders done — avoids file conflicts)
    escalated_cus: list[str] = []
    for cu_id, cu_file, _ in coder_futures:
        if no_per_cu_lead:
            continue
        match _read_file_text(feedback_dir / f"{cu_id}.feedback.txt"):
            case Ok(text) if "STATUS=ESCALATION" in text:
                escalated_cus.append(cu_id)
                _warn(f"Escalation detected: {cu_id}")
                match run_per_cu_lead(
                    cu_id, cu_file, lead_model, max_fix_iterations,
                    feedback_dir, log_dir, lead_timeout, project_dir,
                ):
                    case Ok((lead_rc, lead_usage)):
                        tracker.accumulate(lead_usage)
                        wc.coders[f"{cu_id}-lead"] = lead_usage
                    case Err(e):
                        _fail(f"Lead failed on {cu_id}: {e}")
            case _:
                pass

    # After all per-CU leads, recompute files changed from pre-wave ref.
    # This captures files committed by leads (coders' files_changed is empty
    # when the lead implements the CU instead of the coder).
    if escalated_cus:
        match _git_run("diff", "--name-only", stash_ref, cwd=project_dir):
            case Ok(diff_out):
                all_changed = diff_out.splitlines() if diff_out else []
                for cu_id in escalated_cus:
                    wave_files_changed[cu_id] = all_changed
            case Err(_):
                pass

    return wave_files_changed, all_passed


# ── Async entry point ────────────────────────────────────────────

async def main_async(args: argparse.Namespace) -> None:
    """Async entry point — drives wave execution.
    No try/except in this function — all error handling via match/case on Result monad.
    """
    itr_path = Path(args.itr).resolve()
    if not itr_path.is_dir():
        _fail(f"ITR path does not exist: {itr_path}")
        sys.exit(1)

    app_name = args.app
    # Run outputs live INSIDE the ITR folder, never scattered next to it.
    work_dir = itr_path

    # Resolve project root
    if args.project:
        project_dir = Path(args.project).resolve()
    else:
        project_dir = None
        for start in [Path.cwd(), work_dir.resolve()]:
            search = start
            for _ in range(10):
                if (search / ".opencode" / "agents").is_dir():
                    project_dir = search
                    break
                parent = search.parent
                if parent == search:
                    break
                search = parent
            if project_dir:
                break
        if project_dir is None:
            _fail("Cannot find project root (.opencode/agents/ not found)")
            _info("Use --project to specify the project root")
            sys.exit(1)

    feedback_dir = work_dir / f"{app_name}.feedback"
    log_dir = work_dir / f"{app_name}.logs"
    convergence_file = work_dir / f"{app_name}.convergence.json"

    # A dry-run prints the plan and touches nothing on disk.
    if not args.dry_run:
        feedback_dir.mkdir(parents=True, exist_ok=True)
        log_dir.mkdir(parents=True, exist_ok=True)

    cu_map = build_cu_map(itr_path)

    print()
    print(_banner_line())
    print(f"  {C.BOLD}{C.BR_CYAN}LLMDD Wave Executor{C.RESET}")
    print(_banner_line())
    print()
    _info(f"ITR:           {C.BOLD}{itr_path}{C.RESET}")
    _info(f"Project:       {C.BOLD}{project_dir}{C.RESET}")
    _info(f"App:           {C.BOLD}{app_name}{C.RESET}")
    _info(f"CUs found:     {C.BOLD}{len(cu_map)}{C.RESET}")
    _info(f"Coder model:   {args.coder_model}")
    _info(f"Lead model:    {args.lead_model}")
    _info(f"CU timeout:    {args.cu_timeout}s")
    _info(f"Lead timeout:  {args.lead_timeout}s")
    print()

    _header("Loading waves...")
    waves = detect_waves(itr_path, args.waves)
    if not args.dry_run:
        log_dir.joinpath("waves.json").write_text(json.dumps(waves, indent=2) + "\n")

    num_waves = len(waves)
    _success(f"Found {num_waves} waves")
    print()

    if not args.dry_run:
        save_convergence(convergence_file, {"waves": [], "status": "RUNNING"})
    tracker = CreditTracker()

    for wave_def in waves:
        wave_num = wave_def["wave"]
        wave_cus = wave_def["cus"]
        description = wave_def.get("description", "")
        wc = tracker.start_wave(wave_num)

        print(_banner_line())
        print(f"  {C.BOLD}{C.BR_CYAN}Wave {wave_num}/{num_waves}{C.RESET}"
              + (f"  {C.DIM}— {description}{C.RESET}" if description else ""))
        print(_banner_line())
        _info(f"CUs: {', '.join(wave_cus)}")
        print()

        cu_files = []
        for cu_id in wave_cus:
            cu_file = resolve_cu_file(cu_id, cu_map, itr_path)
            if cu_file:
                cu_files.append((cu_id, cu_file))
            else:
                _warn(f"Not found: {cu_id}")

        if not cu_files:
            _warn("No CUs found, skipping")
            print()
            continue

        if args.dry_run:
            print(f"  {C.DIM}[dry-run] Would execute:{C.RESET}")
            for cu_id, f in cu_files:
                print(f"    {C.DIM}·{C.RESET} {C.BR_CYAN}{cu_id}{C.RESET} {C.DIM}→ {f.name}{C.RESET}")
            print()
            continue

        # Capture pre-wave HEAD ref for accurate file tracking after wave lead commits
        match _git_run("rev-parse", "HEAD", cwd=project_dir):
            case Ok(ref):
                pre_wave_ref = ref
            case Err(_):
                _warn(f"Wave {wave_num}: could not capture HEAD ref; file tracking may be inaccurate")
                pre_wave_ref = None

        _header(f"Running {len(cu_files)} coder(s) in parallel...")
        print()

        wave_files_changed, all_passed = await run_wave_parallel(
            cu_files, wc, tracker,
            feedback_dir, log_dir, project_dir,
            args.coder_model, args.coder_fallbacks,
            args.cu_timeout,
            args.lead_model, args.lead_timeout,
            args.max_fix_iterations,
            args.no_per_cu_lead,
        )

        # Wave file summary
        print(f"  {C.BOLD}Wave {wave_num} files changed:{C.RESET}")
        for cu_id, files in wave_files_changed.items():
            if files:
                for f in files:
                    print(f"    {C.DIM}├{C.RESET} {C.BR_CYAN}{cu_id}{C.RESET} {C.DIM}→{C.RESET} {f}")
        total_files = set()
        for files in wave_files_changed.values():
            total_files.update(files)
        if total_files:
            _info(f"{C.BOLD}{len(total_files)}{C.RESET} unique file(s) modified this wave")
        else:
            _info("No files modified")
        print()

        # Wave credit summary
        _print_credit_table(wc.coders, f"Wave {wave_num} Credits", C.BR_CYAN)
        _print_accumulated(tracker)

        # Code lead review
        lead_rc, lead_usage = run_code_lead(
            wave_num, num_waves, args.lead_model, args.max_fix_iterations,
            feedback_dir, log_dir, args.lead_timeout,
            project_dir, wave_cus,
        )
        tracker.accumulate(lead_usage)
        lead_ok = lead_rc == 0

        # After wave lead, merge files committed by wave lead into file tracking
        if pre_wave_ref:
            match _git_run("diff", "--name-only", pre_wave_ref, cwd=project_dir):
                case Ok(diff_out):
                    lead_files = set(diff_out.splitlines() if diff_out else [])
                    if lead_files:
                        for cu_id in wave_files_changed:
                            existing = set(wave_files_changed[cu_id])
                            wave_files_changed[cu_id] = list(existing | lead_files)
                case Err(_):
                    pass

        escalations = check_escalations(wave_cus, feedback_dir)
        if escalations > 0:
            _warn(f"Wave {wave_num}: {escalations} escalation(s)")
        else:
            _success(f"Wave {wave_num} completed")
        print()

        credits_data = {cu_id: u.dict() for cu_id, u in wc.coders.items()}
        credits_data["lead"] = lead_usage.dict()
        update_convergence(
            convergence_file, wave_num, wave_cus,
            escalations, all_passed, lead_ok,
            wave_files_changed, credits_data,
        )

    # Final summary — dry-run reports the plan from memory and writes nothing.
    if args.dry_run:
        print(_banner_line())
        print(f"  {C.BOLD}Summary (dry-run, nothing written){C.RESET}")
        print(_banner_line())
        _info(f"Waves:       {C.BOLD}{num_waves}{C.RESET}")
        print()
        _print_accumulated(tracker)
        print(_banner_line())
        return

    convergence = load_convergence(convergence_file)
    total_escalations = sum(w["escalations"] for w in convergence["waves"])
    failed_waves = sum(
        1 for w in convergence["waves"]
        if w["coder_status"] != 0 or w["lead_status"] != 0
    )
    unique_all = set()
    for w in convergence["waves"]:
        for files in w.get("files_changed", {}).values():
            unique_all.update(files)

    if total_escalations == 0 and failed_waves == 0:
        convergence["status"] = "CONVERGED"
    else:
        convergence["status"] = "NEEDS_ATTENTION"
    save_convergence(convergence_file, convergence)

    status_colour = C.BR_GREEN if convergence["status"] == "CONVERGED" else C.BR_RED

    print(_banner_line())
    print(f"  {C.BOLD}Summary{C.RESET}")
    print(_banner_line())
    _info(f"Waves:       {C.BOLD}{num_waves}{C.RESET}")
    _info(f"Escalations: {total_escalations}")
    _info(f"Files:       {C.BOLD}{len(unique_all)}{C.RESET}")
    _info(f"Status:      {status_colour}{C.BOLD}{convergence['status']}{C.RESET}")
    print()
    _print_accumulated(tracker)
    print(_banner_line())


def main(argv=None):
    """Entry point — parses args and delegates to async main."""
    args = parse_args(argv)
    if args.no_color:
        C.disable()
    sys.exit(asyncio.run(main_async(args)))


if __name__ == "__main__":
    main()
