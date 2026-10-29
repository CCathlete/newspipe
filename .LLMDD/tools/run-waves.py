#!/usr/bin/env python3
"""run-waves.py - Execute CUs in waves with live coloured output and credit tracking.

Platform and shell agnostic replacement for run-waves.fish.
Requires: Python 3.10+, opencode CLI on PATH, git repo.
"""

import argparse
import asyncio
import concurrent.futures
import json
import os
import re
import subprocess
import sys
import threading
import typing
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Final, Generic, Iterator, Literal, Optional, Sequence, TypeAlias, TypedDict, TypeVar, cast

from cu_scope import (NoTouch, has_enforceable_patterns, is_touched,
    parse_no_touch, resolve_patterns, write_paths)


# ── Defaults ───────────────────────────────────────────────────────

DEFAULT_CODER_MODEL: str = "nvidia/nvidia/nemotron-3.5-lightning-30b-a3b"
DEFAULT_LEAD_MODEL: str = "nvidia/nvidia/nemotron-3.5-lightning-30b-a3b"
DEFAULT_CU_TIMEOUT: int = 300
DEFAULT_LEAD_TIMEOUT: int = 300
DEFAULT_MAX_FIX_ITERATIONS: int = 3

# ── Fallback defaults ───────────────────────────────────────────────
# Coder fallbacks: Nvidia models tried in order when the primary coder
# fails (refusal, error, or activity timeout). The first two are measured
# on this host with stdin isolated (gpt-oss ~5s, laguna ~18s) and are the
# frequent fallbacks. The rest are ordered by known latency, slowest last:
# diffusiongemma (unverified solo, ~2s by reputation), deepseek ~260s,
# then glm-5.3-flash as the last resort -- on 2026-10-29 it hung past 90s
# even solo with stdin isolated. Nvidia-only by policy: no cross-provider
# entries. Dead models removed after live probes: kimi-k3 (EOL
# 2026-10-15), magistral-small-2506 (HTTP 404), mistral-7b-instruct-v0.3
# (not found).
DEFAULT_CODER_FALLBACKS: list[str] = [
    "nvidia/openai/gpt-oss-20b",
    "nvidia/poolside/laguna-xs-2.1",
    "nvidia/google/diffusiongemma-26b-a4b-it",
    "nvidia/deepseek-ai/deepseek-v4.1-flash",
    "nvidia/z-ai/glm-5.3-flash",
]
# Lead fallbacks: larger Nvidia models tried in order when the primary
# lead fails (activity timeout or non-zero exit). Ordered by observed
# latency (gpt-oss ~8s, nemotron-120b ~11s, glimmer ~60s, glm-5.3 ~240s).
# Dead models removed after live probes: mistral-medium-3 (HTTP 404).
DEFAULT_LEAD_FALLBACKS: list[str] = [
    "nvidia/openai/gpt-oss-20b",
    "nvidia/nvidia/nemotron-3.5-super-vl-120b-a12b",
    "nvidia/meta/muse-glimmer-30b",
    "nvidia/z-ai/glm-5.3",
]

# ── Monadic Result Type ────────────────────────────────────────────

# TypeVar definitions stay bare: annotating them is a linter error.
# ARCH.PYTHON exemption — the TypeVar itself is the declaration.
T = TypeVar("T")
E_co = TypeVar("E_co", bound=Exception, covariant=True)


@dataclass(slots=True, frozen=True)
class Ok(Generic[T]):
    """Success monad — contains a value."""
    value: T


@dataclass(slots=True, frozen=True)
class Err(Generic[E_co]):
    """Failure monad — contains an exception."""
    error: E_co


@dataclass(slots=True, frozen=True)
class CoderOutput:
    """Result of a single coder run."""
    rc: int
    files_changed: list[str]
    usage: "TokenUsage"
    model_used: str


# ── JSON Custom Types (no Any, no bare generics) ───────────────────

class TokenCache(TypedDict, total=False):
    read: int
    write: int
class StepTokens(TypedDict, total=False):
    input: int
    output: int
    reasoning: int
    cache: TokenCache
    total: int
EventResult: TypeAlias = str | dict[str, str | int | float | bool | list[str]]
EventModel: TypeAlias = str | dict[str, str]
class EventPart(TypedDict, total=False):
    text: str
    tool: str
    toolName: str
    result: EventResult
    tokens: StepTokens
    cost: float
    reason: str
    action: str
    resource: str
    model: EventModel
    id: str
class JsonEvent(TypedDict, total=False):
    type: str
    part: EventPart
    tokens: StepTokens
    cost: float
    reason: str
    sessionID: str
    model: EventModel
class TokenReport(TypedDict):
    input: int
    output: int
    reasoning: int
    cache_read: int
    cache_write: int
    total: int
    cost: float
    session_id: str
    model: str
class WaveDef(TypedDict, total=False):
    wave: int
    cus: list[str]
    description: str
class WaveRecord(TypedDict):
    wave: int
    cus: list[str]
    escalations: int
    coder_status: int
    lead_status: int
    files_changed: dict[str, list[str]]
    credits: dict[str, TokenReport]
class ConvergenceData(TypedDict):
    waves: list[WaveRecord]
    status: str


# ── I/O Boundary — Monadic Result Wrappers (only try/except in the tool) ──

def _popen(cmd: list[str], cwd: Path) -> Ok[subprocess.Popen[bytes]] | Err[Exception]:
    """Spawn subprocess. Returns Ok(proc) or Err(FileNotFoundError|OSError)."""
    # MONADIC-BOUNDARY: foreign process spawn; OS failure becomes Err.
    caught: Exception
    try:
        return Ok(subprocess.Popen(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, cwd=cwd
        ))
    except (FileNotFoundError, OSError) as caught:
        return Err(caught)


def _open_file(path: Path, mode: Literal["r", "w"] = "r") -> Ok[typing.TextIO] | Err[Exception]:
    """Open a file. Returns Ok(fh) or Err(OSError|IOError)."""
    # MONADIC-BOUNDARY: foreign filesystem open; OS failure becomes Err.
    caught: Exception
    try:
        return Ok(open(path, mode))
    except (OSError, IOError) as caught:
        return Err(caught)


def _git_run(*args: str, cwd: Path) -> Ok[str] | Err[Exception]:
    """Run a git command. Returns Ok(stdout) or Err."""
    # MONADIC-BOUNDARY: foreign git process; spawn and timeout failure becomes Err.
    completed: subprocess.CompletedProcess[str]
    caught: Exception
    try:
        completed = subprocess.run(
            ["git", *args], capture_output=True, text=True, cwd=cwd, timeout=30
        )
        return Ok(completed.stdout.strip())
    except (subprocess.TimeoutExpired, subprocess.CalledProcessError, OSError) as caught:
        return Err(caught)


def _read_file_text(path: Path) -> Ok[str] | Err[Exception]:
    """Read file as text. Returns Ok(text) or Err."""
    # MONADIC-BOUNDARY: foreign filesystem read; OS failure becomes Err.
    caught: Exception
    try:
        return Ok(path.read_text(errors="replace"))
    except (OSError, IOError) as caught:
        return Err(caught)


def _write_file_text(path: Path, text: str) -> Ok[None] | Err[Exception]:
    """Write text to file. Returns Ok(None) or Err."""
    # MONADIC-BOUNDARY: foreign filesystem write; OS failure becomes Err.
    caught: Exception
    try:
        path.write_text(text)
        return Ok(None)
    except (OSError, IOError) as caught:
        return Err(caught)


# ── Future-safe Monadic Wrapper ───────────────────────────────────

async def future_result(awaitable: typing.Awaitable[Ok[T] | Err[Exception]]) -> Ok[T] | Err[Exception]:
    """Await an awaitable that already yields a Result and pass it through.

    This is the BRIDGE between asyncio Futures (which raise exceptions
    on await when the wrapped callable fails) and the Result monad.
    It contains the ONE try/except in the async orchestration layer.
    Every caller uses match/case on the returned Ok|Err.

    The wrapped callable already returns Ok|Err (run_coder,
    _run_coder_correction), so the awaited value is returned unchanged;
    wrapping it again would produce Ok(Ok(...)) and break every success
    pattern. A raised exception still becomes Err.
    """
    # MONADIC-BOUNDARY: asyncio Future await raises by design; the raise becomes Err here and never leaks past this bridge.
    caught: Exception
    try:
        return await awaitable
    except Exception as caught:
        return Err(caught)


# ── Refusal / Error Detection ───────────────────────────────────────

_REFUSAL_PATTERNS: list[str] = [
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

_ERROR_PATTERNS: list[str] = [
    # Process-level fatal signals only (raw stderr, never JSON event text).
    # Coder tool errors, compiler output, and opencode warnings MUST NOT
    # fail a CU — only a dead process or an explicit opencode error event.
    "Traceback (most recent call last)",
    "Exception in thread",
    "panic:",
    "FATAL:",
]

# ── Model Fallback Chain ────────────────────────────────────────────

@dataclass(slots=True, frozen=True)
class ModelFallbackChain:
    """Manages fallback order for model attempts."""
    primary_model: str
    fallback_models: Sequence[str] = ()
    chain: Sequence[str] = ()

    def __post_init__(self) -> None:
        merged: list[str] = [self.primary_model, *self.fallback_models]
        object.__setattr__(self, "chain", tuple(merged))

    def __iter__(self) -> Iterator[str]:
        return iter(self.chain)

    def __len__(self) -> int:
        return len(self.chain)

    def __repr__(self) -> str:
        return " → ".join(self.chain)


def _detect_refusal(log_file: Path) -> bool:
    """Check if the log contains refusal patterns."""
    read: Ok[str] | Err[Exception] = _read_file_text(log_file)
    if isinstance(read, Err):
        return False
    text: str = read.value
    lowered: str = text.lower()
    pattern: str
    for pattern in _REFUSAL_PATTERNS:
        if pattern.lower() in lowered:
            return True
    return False


def _detect_error(log_file: Path) -> bool:
    """Check if the log contains error patterns (but not tool output)."""
    read: Ok[str] | Err[Exception] = _read_file_text(log_file)
    if isinstance(read, Err):
        return False
    text: str = read.value
    line: str
    pattern: str
    for line in text.splitlines():
        if "toolResult" in line or "tool_result" in line:
            continue
        for pattern in _ERROR_PATTERNS:
            if pattern in line:
                return True
    return False


# ── Token / Cost tracking ─────────────────────────────────────────

def _as_part(raw: object) -> EventPart:
    """Narrow foreign JSON into an EventPart; non-dicts become empty."""
    if isinstance(raw, dict):
        narrowed_part: EventPart = cast(EventPart, raw)
        return narrowed_part
    return {}


def _as_tokens(raw: object) -> StepTokens:
    """Narrow foreign JSON into StepTokens; non-dicts become empty."""
    if isinstance(raw, dict):
        narrowed_tok: StepTokens = cast(StepTokens, raw)
        return narrowed_tok
    return {}


def _extract_step_tokens(ev: JsonEvent) -> tuple[StepTokens, float, str]:
    """Extract (tokens_dict, cost, reason) from a step_finish event."""
    part: EventPart = _as_part(ev.get("part", {}))
    tok: StepTokens = _as_tokens(part.get("tokens", ev.get("tokens", {})))
    cost_raw: object = part.get("cost", ev.get("cost", 0))
    cost: float = float(cost_raw) if isinstance(cost_raw, (int, float)) else 0.0
    reason_raw: object = part.get("reason", ev.get("reason", ""))
    reason: str = reason_raw if isinstance(reason_raw, str) else ""
    return tok, cost, reason


@dataclass(slots=True)
# MAJOR-latitude justification: TokenUsage is a mutable accumulator updated
# at dozens of call sites; frozen would force a full rewrite for zero
# behavior gain. Slotted plus annotated fields satisfies the preferred shape.
class TokenUsage:
    """Accumulated token and cost data from opencode step_finish events."""
    input: int = 0
    output: int = 0
    reasoning: int = 0
    cache_read: int = 0
    cache_write: int = 0
    total: int = 0
    cost: float = 0.0
    session_id: str = ""
    model: str = ""

    def add_from_step_finish(self, ev: JsonEvent) -> None:
        tok: StepTokens
        cost: float
        _reason: str
        tok, cost, _reason = _extract_step_tokens(ev)
        tok_input: int = tok.get("input", 0)
        tok_output: int = tok.get("output", 0)
        tok_reasoning: int = tok.get("reasoning", 0)
        cache: TokenCache = tok.get("cache", {})
        cache_read: int = cache.get("read", 0)
        cache_write: int = cache.get("write", 0)
        tok_total: int = tok.get("total", 0)
        self.input += tok_input
        self.output += tok_output
        self.reasoning += tok_reasoning
        self.cache_read += cache_read
        self.cache_write += cache_write
        self.total += tok_total
        self.cost += cost

    def is_empty(self) -> bool:
        return self.total == 0 and self.cost == 0.0

    def dict(self) -> TokenReport:
        return {
            "input": self.input, "output": self.output,
            "reasoning": self.reasoning,
            "cache_read": self.cache_read, "cache_write": self.cache_write,
            "total": self.total, "cost": self.cost,
            "session_id": self.session_id, "model": self.model,
        }


@dataclass(slots=True)
# MAJOR-latitude justification: same accumulator rationale as TokenUsage.
class WaveCredits:
    wave_num: int
    coders: dict[str, TokenUsage] = field(default_factory=dict)

    def wave_totals(self) -> TokenUsage:
        totals: TokenUsage = TokenUsage()
        u: TokenUsage
        for u in self.coders.values():
            totals.input += u.input
            totals.output += u.output
            totals.reasoning += u.reasoning
            totals.cache_read += u.cache_read
            totals.cache_write += u.cache_write
            totals.total += u.total
            totals.cost += u.cost
        return totals


@dataclass(slots=True)
# MAJOR-latitude justification: same accumulator rationale as TokenUsage.
class CreditTracker:
    waves: list[WaveCredits] = field(default_factory=list)
    _accumulated: TokenUsage = field(default_factory=TokenUsage)

    def start_wave(self, wave_num: int) -> WaveCredits:
        wc: WaveCredits = WaveCredits(wave_num)
        self.waves.append(wc)
        return wc

    def accumulate(self, usage: TokenUsage) -> None:
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

# MAJOR-latitude justification: C is a dynamic namespace reset in place by
# C.disable via setattr; a frozen dataclass cannot support that shape, so the
# annotated plain class is retained.
class C:
    RESET: str     = "\033[0m"
    BOLD: str      = "\033[1m"
    DIM: str       = "\033[2m"
    ULINE: str     = "\033[4m"
    RED: str       = "\033[31m"
    GREEN: str     = "\033[32m"
    YELLOW: str    = "\033[33m"
    BLUE: str      = "\033[34m"
    MAGENTA: str   = "\033[35m"
    CYAN: str      = "\033[36m"
    WHITE: str     = "\033[37m"
    BR_RED: str    = "\033[91m"
    BR_GREEN: str  = "\033[92m"
    BR_YELLOW: str = "\033[93m"
    BR_BLUE: str   = "\033[94m"
    BR_MAG: str    = "\033[95m"
    BR_CYAN: str   = "\033[96m"
    BR_WHITE: str  = "\033[97m"
    BG_RED: str   = "\033[41m"
    BG_GREEN: str = "\033[42m"
    BG_BLUE: str  = "\033[44m"

    @classmethod
    def disable(cls) -> None:
        attr: str
        for attr in dir(cls):
            if attr.isupper() and attr != "RESET":
                setattr(cls, attr, "")
        cls.RESET = ""


CODER_COLOURS: list[tuple[str, str]] = [
    (C.BR_CYAN,   "CYN"),
    (C.BR_GREEN,  "GRN"),
    (C.BR_YELLOW, "YLW"),
    (C.BR_MAG,    "MAG"),
    (C.BR_BLUE,   "BLU"),
    (C.BR_RED,    "RED"),
    (C.YELLOW,     "yel"),
    (C.CYAN,       "cyn"),
]


def _no_colour_support() -> bool:
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

def _banner_line(width: int = 63) -> str:
    return f"{C.BR_CYAN}{'═' * width}{C.RESET}"

def _info(msg: str) -> None:
    print(f"  {C.DIM}{msg}{C.RESET}")

def _success(msg: str) -> None:
    print(f"  {C.BR_GREEN}✓{C.RESET} {msg}")

def _fail(msg: str) -> None:
    print(f"  {C.BR_RED}✗{C.RESET} {msg}")

def _warn(msg: str) -> None:
    print(f"  {C.BR_YELLOW}⚠{C.RESET} {msg}")

def _header(msg: str) -> None:
    print(f"  {C.BOLD}{C.BR_WHITE}{msg}{C.RESET}")


def _print_credit_table(cu_usages: dict[str, TokenUsage], label: str,
                         colour: str = "") -> None:
    """Print credit usage table — cost is the primary column."""
    if not cu_usages:
        return

    print(f"    {colour}{C.BOLD}{label}{C.RESET}")
    hdr: str = (f"    {C.DIM}{'CU':<22} {'Cost':>8}  "
           f"{'In':>8} {'Out':>8} {'Reason':>8} {'CacheR':>8} {'Model':<25}{C.RESET}")
    print(hdr)
    print(f"    {C.DIM}{'─' * 97}{C.RESET}")

    wt: TokenUsage = TokenUsage()
    cu_id: str
    u: TokenUsage
    for cu_id, u in cu_usages.items():
        if u.is_empty():
            continue
        cost_str: str = _fmt_cost(u.cost)
        cost_col: str = C.BR_YELLOW if u.cost > 0 else C.DIM
        model_str: str = u.model if u.model else "unknown"
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

    total_cost_col: str = C.BR_YELLOW if wt.cost > 0 else C.DIM
    print(f"    {C.DIM}{'─' * 97}{C.RESET}")
    print(f"    {C.BOLD}{'TOTAL':<22}{C.RESET}"
          f" {total_cost_col}{_fmt_cost(wt.cost):>8}{C.RESET}  "
          f"{_fmt_tokens(wt.input):>8}"
          f" {_fmt_tokens(wt.output):>8}"
          f" {_fmt_tokens(wt.reasoning):>8}"
          f" {_fmt_tokens(wt.cache_read):>8}")
    print()


def _print_accumulated(tracker: CreditTracker) -> None:
    """Print accumulated totals — cost is the hero number."""
    a: TokenUsage = tracker.accumulated
    if a.is_empty():
        return
    print(f"    {C.BOLD}{C.BR_WHITE}ACCUMULATED TOTALS{C.RESET}")
    print(f"    {C.DIM}{'─' * 50}{C.RESET}")
    cost_col: str = C.BR_YELLOW if a.cost > 0 else C.DIM
    print(f"    {C.BOLD}{'Total cost:':<25}{C.RESET} "
          f"{cost_col}{C.BOLD}{_fmt_cost(a.cost)}{C.RESET}")
    print(f"    {'Input tokens:':<25} {C.BOLD}{_fmt_tokens(a.input)}{C.RESET}")
    print(f"    {'Output tokens:':<25} {C.BOLD}{_fmt_tokens(a.output)}{C.RESET}")
    print(f"    {'Reasoning tokens:':<25} {C.BOLD}{_fmt_tokens(a.reasoning)}{C.RESET}")
    print(f"    {'Cache read:':<25} {C.BOLD}{_fmt_tokens(a.cache_read)}{C.RESET}")
    print()


# ── CLI ────────────────────────────────────────────────────────────

def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p: argparse.ArgumentParser = argparse.ArgumentParser(
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
    p.add_argument("--lead-fallbacks", nargs="*", default=DEFAULT_LEAD_FALLBACKS,
                   help="Fallback models for lead review, tried in order on timeout or failure")
    p.add_argument("--project", default=None,
                   help="Project root directory (default: auto-detect from .opencode/agents/)")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--no-per-cu-lead", action="store_true",
                   help="Disable per-CU code lead (run wave-level lead instead)")
    p.add_argument("--no-color", action="store_true")
    return p.parse_args(argv)


# ── CU ID → file resolution ───────────────────────────────────────

_CUID_RE: re.Pattern[str] = re.compile(r"^#\s*CU-ID:\s*(.+)$")

_COMPONENT_FILES: Sequence[str] = ("COORDINATES.itr", "REQUIREMENTS.itr",
                    "IMPLEMENTATION_STEPS.itr", "ACCEPTANCE.itr")


def _cu_no_touch(cu_ref: Path) -> NoTouch:
    """Return the no-touch set declared by one CU frame folder or file."""
    steps: Path = cu_ref / "IMPLEMENTATION_STEPS.itr" if cu_ref.is_dir() else cu_ref
    opened: Ok[typing.TextIO] | Err[Exception] = _open_file(steps)
    if isinstance(opened, Err):
        return parse_no_touch("")
    handle: typing.TextIO = opened.value
    text: str = handle.read()
    handle.close()
    return parse_no_touch(text)


def _dirty_paths(project_dir: Path) -> frozenset[str]:
    """Return paths currently dirty (tracked and untracked), empty when git cannot answer."""
    dirty: frozenset[str]
    match _git_run("status", "--porcelain", "--untracked-files=all", cwd=project_dir):
        case Ok(out):
            lines: list[str] = out.splitlines()
            # _git_run strips the whole output, so when the first porcelain
            # line has its X column set to a space (worktree-only change,
            # " M path") that space is eaten and the path would shift one
            # character left. A native line whose X column is a letter is
            # either XY (letter at index 1) or X+space+space (separator at
            # index 2), so letter + one space + non-space can only be a
            # stripped line — restore the lost column before the index-3 parse.
            if (lines and len(lines[0]) > 2 and lines[0][0].isalpha()
                    and lines[0][1] == " " and lines[0][2] != " "):
                lines[0] = " " + lines[0]
            dirty = frozenset(line[3:].strip() for line in lines if len(line) > 3)
        case Err(_):
            dirty = frozenset()
    return dirty


def _wave_write_index(cu_files: Sequence[tuple[str, Path]]) -> dict[str, Sequence[str]]:
    """Map each CU id to the write targets its frame declares in prose."""
    index: dict[str, Sequence[str]] = {}
    cu_id: str
    directory: Path
    for cu_id, directory in cu_files:
        steps_file: Path = directory / "IMPLEMENTATION_STEPS.itr"
        opened: Ok[typing.TextIO] | Err[Exception] = _open_file(steps_file)
        if isinstance(opened, Err):
            continue
        handle: typing.TextIO = opened.value
        text: str = handle.read()
        handle.close()
        index[cu_id] = write_paths(text)
    return index


def _wave_dirty_clashes(cu_files: Sequence[tuple[str, Path]],
                        baseline_dirty: frozenset[str]) -> list[tuple[str, str]]:
    """Return (owner, path) pairs whose write target was dirty before the run."""
    owners: dict[str, Sequence[str]] = _wave_write_index(cu_files)
    return sorted({
        (owner, path)
        for owner, paths in owners.items()
        for path in paths
        if path in baseline_dirty
    })


def _file_cu_id(f: Path) -> str | None:
    """Return the CU-ID header of a frame file, or None."""
    opened: Ok[typing.TextIO] | Err[Exception] = _open_file(f)
    if isinstance(opened, Err):
        return None
    log_fh: typing.TextIO = opened.value
    line: str
    for line in log_fh:
        found: Optional[re.Match[str]] = _CUID_RE.match(line)
        if found is not None:
            log_fh.close()
            return found.group(1).strip()
    log_fh.close()
    return None


def _cu_folder_ref(subdir: Path) -> Path | None:
    """Reference for a CU subdirectory: the folder itself when it holds
    component files, else the CU-ID-bearing frame inside, else None."""
    if not subdir.is_dir():
        return None
    name: str
    for name in _COMPONENT_FILES:
        if (subdir / name).is_file():
            return subdir
    f: Path
    for f in sorted(subdir.iterdir()):
        if f.suffix == ".itr" and f.is_file() and _file_cu_id(f):
            return f
    return None


def _iter_subdirs(itr_path: Path) -> list[Path]:
    """List subdirectory entries of an ITR folder."""
    found: list[Path] = []
    entry: Path
    for entry in itr_path.iterdir():
        if entry.is_dir():
            found.append(entry)
    return found


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
    cu_map: dict[str, Path] = {}
    # Legacy root-level frames first, then per-CU subdirectories (win).
    f: Path
    cu_id: str | None
    for f in sorted(itr_path.iterdir()):
        if f.suffix == ".itr" and f.is_file():
            cu_id = _file_cu_id(f)
            if cu_id:
                cu_map[cu_id] = f
    sub: Path
    ref: Path | None
    for sub in sorted(_iter_subdirs(itr_path)):
        ref = _cu_folder_ref(sub)
        if ref is not None:
            cu_id = sub.name if ref.is_dir() else (_file_cu_id(ref) or sub.name)
            cu_map[cu_id] = ref
    return cu_map


def resolve_cu_file(cu_id: str, cu_map: dict[str, Path], itr_path: Path) -> Path | None:
    if cu_id in cu_map:
        return cu_map[cu_id]
    direct: Path = itr_path / f"{cu_id}.itr"
    if direct.is_file():
        return direct
    subdir: Path = itr_path / cu_id
    ref: Path | None = _cu_folder_ref(subdir)
    if ref is not None:
        return ref
    nested: Path = subdir / f"{cu_id}.itr"
    return nested if nested.is_file() else None


# ── Wave detection ─────────────────────────────────────────────────

def detect_waves(itr_path: Path, waves_file: str | None) -> list[WaveDef]:
    if waves_file:
        p: Path = Path(waves_file)
        if p.is_file():
            loaded: list[WaveDef] = json.loads(p.read_text())
            return loaded
        _warn(f"Waves file not found: {waves_file}")
    cu_map: dict[str, Path] = build_cu_map(itr_path)
    cu_ids: list[str] = sorted(cu_map.keys())
    return [{"wave": 1, "cus": cu_ids}] if cu_ids else []


# ── Git helpers ────────────────────────────────────────────────────

def _git(*args: str, cwd: Path | None = None) -> str:
    """Run git and return stdout; empty string when git is unavailable."""
    base: Path = cwd if cwd is not None else Path.cwd()
    ran: Ok[str] | Err[Exception] = _git_run(*args, cwd=base)
    if isinstance(ran, Err):
        return ""
    return ran.value

def git_diff_names(work_dir: Path, ref: str) -> list[str]:
    names: str = _git("diff", "--name-only", ref, cwd=work_dir)
    return names.splitlines() if names else []

def git_stash_ref(work_dir: Path) -> str:
    return _git("rev-parse", "HEAD", cwd=work_dir)


# ── Per-CU change attribution ──────────────────────────────────────

WRITE_TOOLS: Final[frozenset[str]] = frozenset({"write", "edit", "patch"})
_SHELL_WRITE: Final[re.Pattern[str]] = re.compile(
    r"(?:>>?|tee|cp|mv|touch|mkdir\s+-p)\s+([^\s;|&<>'\"`]+)")
_QUOTED_REPO_PATH: Final[re.Pattern[str]] = re.compile(
    r"['\"]((?:\.LLMDD|src|scripts|docs|conf|project|itr-buffer|dtr-builder|"
    r"itr-compiler|commit-tool)/[\w./-]+)['\"]")


@dataclass(frozen=True, slots=True)
class CoderWrites:
    cu_id: str
    paths: Sequence[str]


@dataclass(frozen=True, slots=True)
class WaveWrites:
    per_cu: Sequence[CoderWrites]
    unattributed: Sequence[str]


def _text_of(raw: object) -> str:
    return raw if isinstance(raw, str) else ""


def _tool_input(ev: JsonEvent) -> EventPart:
    part: EventPart = _as_part(ev.get("part", {}))
    state: EventPart = _as_part(part.get("state", {}))
    return _as_part(state.get("input", {}))


def _shell_candidates(command: str) -> Sequence[str]:
    found: list[str] = [match.group(1) for match in _SHELL_WRITE.finditer(command)]
    found.extend(_QUOTED_REPO_PATH.findall(command))
    return tuple(found)


def _tool_candidates(ev: JsonEvent) -> Sequence[str]:
    part: EventPart = _as_part(ev.get("part", {}))
    if _text_of(part.get("type")) != "tool":
        return ()
    tool: str = _text_of(part.get("tool"))
    if tool in WRITE_TOOLS:
        path: str = _text_of(_tool_input(ev).get("path"))
        return (path,) if path else ()
    if tool == "shell":
        return _shell_candidates(_text_of(_tool_input(ev).get("command")))
    return ()


def _repo_relative(project_dir: Path, raw: str) -> str | None:
    candidate: Path = Path(raw.strip("'\"`"))
    if not candidate.is_absolute():
        candidate = project_dir / candidate
    try:
        return candidate.resolve().relative_to(project_dir.resolve()).as_posix()
    except (OSError, ValueError):
        return None


def coder_writes(log_prefix: str, log_dir: Path, project_dir: Path) -> CoderWrites:
    found: list[str] = []
    log_file: Path
    for pattern in (f"{log_prefix}.attempt-*.log", f"{log_prefix}.correction-*.log"):
        for log_file in sorted(log_dir.glob(pattern)):
            opened: Ok[typing.TextIO] | Err[Exception] = _open_file(log_file)
            if isinstance(opened, Err):
                continue
            handle: typing.TextIO = opened.value
            line: str
            for line in handle:
                parsed: Ok[JsonEvent] | Err[ValueError] = _parse_event_line(line.strip())
                if isinstance(parsed, Err):
                    continue
                raw: str
                for raw in _tool_candidates(parsed.value):
                    relative: str | None = _repo_relative(project_dir, raw)
                    if relative is not None:
                        found.append(relative)
            handle.close()
    return CoderWrites(cu_id=log_prefix, paths=tuple(dict.fromkeys(found)))


def wave_writes(project_dir: Path, base_ref: str,
                baseline_dirty: frozenset[str]) -> Sequence[str]:
    changed: list[str] = []
    match _git_run("diff", "--name-only", base_ref, cwd=project_dir):
        case Ok(out):
            changed.extend(out.splitlines())
        case Err(_):
            pass
    match _git_run("ls-files", "--others", "--exclude-standard", cwd=project_dir):
        case Ok(out):
            changed.extend(out.splitlines())
        case Err(_):
            pass
    unique: frozenset[str] = frozenset(path for path in changed if path)
    return tuple(sorted(unique - baseline_dirty))


def attribute_writes(wave_paths: Sequence[str],
                     per_cu: Sequence[CoderWrites]) -> WaveWrites:
    claimed: set[str] = set()
    writes: CoderWrites
    for writes in per_cu:
        claimed.update(writes.paths)
    return WaveWrites(
        per_cu=tuple(per_cu),
        unattributed=tuple(path for path in wave_paths if path not in claimed),
    )


# ── Live-streaming with JSON parsing ──────────────────────────────

def _parse_event_line(line: str) -> Ok[JsonEvent] | Err[ValueError]:
    """Parse one streamed log line as a JSON event."""
    # MONADIC-BOUNDARY: JSON decode of a foreign process stream; a non-object line becomes Err.
    parsed: object = None
    caught: ValueError
    try:
        parsed = json.loads(line)
    except (json.JSONDecodeError, ValueError) as caught:
        return Err(caught)
    if isinstance(parsed, dict):
        event: JsonEvent = cast(JsonEvent, parsed)
        return Ok(event)
    return Err(ValueError("event line is not a JSON object"))


def _stream_process_json(cmd: list[str], log_fh: typing.TextIO, colour: str, prefix: str,
                         timeout: int, work_dir: Path,
                         token_usage: TokenUsage) -> Ok[int] | Err[Exception]:
    """Stream JSON events with activity-based timeout.
    Returns Ok(exit_code) or Err(FileNotFoundError|OSError|TimeoutError).
    """
    proc: subprocess.Popen[bytes]
    e: Exception
    match _popen(cmd, cwd=work_dir):
        case Ok(proc):
            pass
        case Err(e):
            _fail(f"Command not found: {cmd[0]}")
            return Err(e)

    _done: threading.Event = threading.Event()
    last_activity: list[datetime] = [datetime.now()]


    def _reader() -> None:
        assert proc.stdout is not None
        raw: bytes
        line: str
        for raw in proc.stdout:
            line = raw.decode("utf-8", errors="replace").rstrip("\n")
            log_fh.write(line + "\n")
            log_fh.flush()

            # Any output from the process counts as activity
            last_activity[0] = datetime.now()

            parsed: Ok[JsonEvent] | Err[ValueError] = _parse_event_line(line)
            if isinstance(parsed, Err):
                print(f"  {colour}{prefix}{C.RESET} {C.DIM}│{C.RESET} {line}",
                      flush=True)
            else:
                ev: JsonEvent = parsed.value
                ev_type: str = ev.get("type", "")

                sid: str = ev.get("sessionID", "")
                if sid and not token_usage.session_id:
                    token_usage.session_id = sid

                if ev_type == "model_start":
                    model_info: EventModel = ev.get("model", {})
                    if isinstance(model_info, dict):
                        token_usage.model = model_info.get("id", "")
                    elif isinstance(model_info, str):
                        token_usage.model = model_info

                if ev_type == "step_finish":
                    token_usage.add_from_step_finish(ev)

                display: str | None = _format_event(ev, colour)
                if display:
                    dlines: list[str] = display.split("\n")
                    print(f"  {colour}{prefix}{C.RESET} {C.DIM}│{C.RESET} {dlines[0]}",
                          flush=True)
                    cont: str
                    for cont in dlines[1:]:
                        print(f"  {colour}{prefix}{C.RESET} {C.DIM}│{C.RESET} {cont}",
                              flush=True)

        _done.set()

    t: threading.Thread = threading.Thread(target=_reader, daemon=True)
    t.start()

    while not _done.is_set():
        idle_secs: float = (datetime.now() - last_activity[0]).total_seconds()

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


def _format_event(ev: JsonEvent, colour: str) -> str | None:
    t: str = ev.get("type", "")

    if t == "step_start":
        return f"{C.DIM}▸ step start{C.RESET}"

    if t == "step_finish":
        tok: StepTokens
        cost: float
        reason: str
        tok, cost, reason = _extract_step_tokens(ev)
        parts: list[str] = []
        if cost > 0:
            parts.append(f"cost:{_fmt_cost(cost)}")
        if tok.get("input"):
            parts.append(f"in:{_fmt_tokens(tok['input'])}")
        if tok.get("output"):
            parts.append(f"out:{_fmt_tokens(tok['output'])}")
        if tok.get("reasoning"):
            parts.append(f"reason:{_fmt_tokens(tok['reasoning'])}")
        cache: TokenCache = tok.get("cache", {})
        if cache.get("read"):
            parts.append(f"cR:{_fmt_tokens(cache['read'])}")
        if reason:
            parts.append(f"({reason})")
        token_str: str = " ".join(parts) if parts else "no data"
        return f"{C.BR_GREEN}▸ done{C.RESET} {C.DIM}{token_str}{C.RESET}"

    if t == "model_start":
        model: EventModel = ev.get("model", {})
        if isinstance(model, dict):
            name: str = model.get("id", "unknown")
            provider: str = model.get("providerID", "")
            return f"{C.DIM}▸ model: {provider}/{name}{C.RESET}"
        return f"{C.DIM}▸ model start{C.RESET}"

    if t == "model_finish":
        return None

    if t == "text":
        part: EventPart = ev.get("part", {})
        text: str = part.get("text", "")
        if not text:
            return None
        # Show what the agent actually says, in full, one line per line.
        # Truncation here is what made runs unreadable — cap high.
        if len(text) > 4000:
            text = text[:3997] + "..."
        prefixed: list[str] = []
        ln: str
        for ln in text.split("\n"):
            prefixed.append(f"{C.BR_WHITE}📝 {ln}{C.RESET}")
        return "\n".join(prefixed)

    if t == "tool_invocation_start":
        part = ev.get("part", {})
        tool: str = part.get("toolName", "unknown")
        return f"{C.BR_YELLOW}🔧 {tool}{C.RESET}"

    if t == "tool_result":
        part = ev.get("part", {})
        tool = part.get("toolName", "unknown")
        result: EventResult = part.get("result", "")
        if isinstance(result, str) and len(result) > 80:
            result = result[:77] + "..."
        elif isinstance(result, dict):
            result = json.dumps(result, ensure_ascii=False)
            if len(result) > 80:
                result = result[:77] + "..."
        return f"{C.DIM}  ↳ {tool}: {result}{C.RESET}"

    if t == "permission_request":
        part = ev.get("part", {})
        action: str = part.get("action", "")
        resource: str = part.get("resource", "")
        return f"{C.BR_MAG}🔐 permission: {action} {resource}{C.RESET}"

    return None


# ── Coder / Lead execution ─────────────────────────────────────────

def _usage_line(usage: TokenUsage, colour: str = "") -> str:
    """Format a single-line usage summary: cost is prominent."""
    if usage.is_empty():
        return ""
    cost_str: str = _fmt_cost(usage.cost)
    cost_col: str = C.BR_YELLOW if usage.cost > 0 else C.DIM
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
    colour_code: str
    colour_label: str
    colour_code, colour_label = coder_colour
    prefix: str = f"[{cu_id}]"
    combined_usage: TokenUsage = TokenUsage()
    model_used: str = ""
    files_changed: list[str] = []
    stream_rc: int = 1

    prompt: str = (
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

    model_idx: int
    model: str
    for model_idx, model in enumerate(fallback_chain):
        log_file: Path = log_dir / f"{cu_id}.attempt-{model_idx}.log"
        attempt_usage: TokenUsage = TokenUsage()

        if model_idx > 0:
            _warn(f"Fallback attempt {model_idx}: trying {C.BOLD}{model}{C.RESET}")
            print(f"    {C.DIM}Trying fallback model: {model}{C.RESET}")

        cmd: list[str] = [
            "opencode", "run",
            "--model", model,
            "--agent", "llmdd/coder",
            "--format", "json",
            "--auto",
            prompt,
        ]

        # Open log file via monadic wrapper
        log_fh: typing.TextIO
        rc: int
        e: Exception
        match _open_file(log_file, "w"):
            case Ok(log_fh):
                match _stream_process_json(cmd, log_fh, colour_code, prefix,
                                           timeout, project_dir, attempt_usage):
                    case Ok(rc):
                        stream_rc = rc
                    case Err(e):
                        log_fh.close()
                        if isinstance(e, TimeoutError) and model_idx < len(fallback_chain) - 1:
                            _warn(f"Model {C.BOLD}{model}{C.RESET} timed out after {timeout}s \u2014 rotating to next fallback")
                            stream_rc = 124
                        else:
                            return Err(e)
                log_fh.close()
            case Err(e):
                return Err(e)

        # Check for issues via monadic file read.
        # An opencode `"type": "error"` event is authoritative. Plain-text
        # patterns only apply to non-JSON lines (raw process stderr) so
        # coder tool errors and compiler output can never fail a CU.
        text: str
        match _read_file_text(log_file):
            case Ok(text):
                lowered_text: str = text.lower()
                is_refusal: bool = False
                p: str
                for p in _REFUSAL_PATTERNS:
                    if p.lower() in lowered_text:
                        is_refusal = True
                        break
                is_error_lines: bool = False
                line: str
                ev: JsonEvent
                for line in text.splitlines():
                    parsed_line: Ok[JsonEvent] | Err[ValueError] = _parse_event_line(line)
                    if isinstance(parsed_line, Err):
                        pass
                    else:
                        ev = parsed_line.value
                        if ev.get("type") == "error":
                            is_error_lines = True
                            break
                        continue
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

        is_error: bool = is_error_lines if stream_rc == 0 else False
        is_timeout: bool = stream_rc == 124

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

        # The CU's change set comes from this agent's own tool calls, never
        # from the shared working tree: a sibling's writes are not ours, and
        # a file this agent created without git add is still counted.
        files_changed = list(coder_writes(cu_id, log_dir, project_dir).paths)

        # Determine if this attempt succeeded
        succeeded: bool = stream_rc == 0 and not is_refusal and not is_error

        if succeeded:
            model_used = model
            if model_idx > 0:
                _success(f"CU {cu_id} completed with fallback {C.BOLD}{model}{C.RESET}"
                        f" {C.DIM}({len(files_changed)} files){C.RESET}")
            else:
                _success(f"CU {cu_id} completed {C.DIM}({len(files_changed)} files){C.RESET}")
            break
        else:
            reason: str = "refused" if is_refusal else ("error" if is_error else ("timeout" if is_timeout else f"exit {stream_rc}"))
            _warn(f"Model {C.BOLD}{model}{C.RESET} failed: {reason}")
            if model_idx == len(fallback_chain) - 1:
                model_used = model
                _fail(f"CU {cu_id} failed on all {len(fallback_chain)} model(s)")

                # Never clobber evidence: the coder may have written feedback
                # before failing. Only write the escalation stub when the
                # coder left no feedback file behind.
                coder_feedback: Path = feedback_dir / f"{cu_id}.feedback.txt"
                if coder_feedback.is_file():
                    _warn(f"Keeping coder feedback at {coder_feedback}")
                    break

                feedback_content: str = f"""CU_ID={cu_id}
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

    line2: str = _usage_line(combined_usage)
    if line2:
        print(line2)

    if files_changed:
        fc: str
        for fc in files_changed:
            print(f"    {colour_code}\u251c{C.RESET} {fc}")

    if model_used:
        log_model_info: Path = log_dir / f"{cu_id}.model-used.txt"
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
                  project_dir: Path, wave_cus: list[str],
                  fallback_models: Sequence[str] = (),
                  unattributed_context: str = "") -> tuple[int, TokenUsage]:
    colour: str = C.BR_MAG
    prefix: str = "[LEAD]"
    usage: TokenUsage = TokenUsage()

    is_final: bool = (wave == num_waves)

    if is_final:
        commit_instruction: str = (
            f"\n8. Read COMMIT_MESSAGE fields from all feedback files across all waves\n"
            f"9. Consolidate into a single commit message\n"
            f"10. Stage all changes and commit with the consolidated message\n"
        )
    else:
        commit_instruction = (
            f"\n8. Prepare a COMMIT_MESSAGE for this wave in {feedback_dir}/wave-{wave}-commit.txt\n"
            f"9. Do NOT commit \u2014 the final wave lead will commit\n"
        )

    if unattributed_context:
        u: str = (
            f"\nUNATTRIBUTED CHANGES (no owning CU):\n"
            f"{unattributed_context}\n\n"
            f"These changes were made by no CU in this wave. Routes to the wave lead per Rule 3.\n\n"
        )
    else:
        u = ""

    prompt: str = (
        f"You are the code lead for wave {wave}.\n\n"
        f"1. Read feedback files for this wave\u2019s CUs: {', '.join(wave_cus)}\n"
        f"2. For any CU with STATUS=ESCALATION: read the CU frame file and implement it yourself\n"
        f"3. Write a feedback file for each escalated CU you implement\n"
        f"4. {u}"
        f"5. Run verification tests for all implemented CUs\n"
        f"6. Fix any test failures\n"
        f"7. Write verification report to {feedback_dir}/wave-{wave}-verification.txt\n"
        f"{commit_instruction}\n"
        f"Max fix iterations: {max_iterations}"
    )

    print(f"  {colour}{C.BOLD}\u25b8{C.RESET} {colour}{C.BOLD}Code Lead \u2014 Wave {wave} review{C.RESET}")
    print(f"    {C.DIM}{'\u2500' * 55}{C.RESET}")

    chain: ModelFallbackChain = ModelFallbackChain(model, tuple(fallback_models))
    rc: int = 1
    attempt: int
    attempt_model: str
    for attempt, attempt_model in enumerate(chain):
        log_file: Path = log_dir / f"lead-wave-{wave}.attempt-{attempt}.log"
        if attempt > 0:
            _warn(f"Lead fallback attempt {attempt}: trying {C.BOLD}{attempt_model}{C.RESET}")

        cmd: list[str] = [
            "opencode", "run",
            "--model", attempt_model,
            "--agent", "verifier",
            "--format", "json",
            "--auto",
            prompt,
        ]

        log_fh: typing.TextIO
        e: Exception
        match _open_file(log_file, "w"):
            case Ok(log_fh):
                match _stream_process_json(cmd, log_fh, colour, prefix,
                                           timeout, project_dir, usage):
                    case Ok(rc):
                        pass
                    case Err(e):
                        log_fh.close()
                        if isinstance(e, TimeoutError) and attempt < len(chain) - 1:
                            _warn(f"Lead model {C.BOLD}{attempt_model}{C.RESET} timed out after {timeout}s \u2014 rotating to next fallback")
                            continue
                        _fail(f"Lead review failed: {e}")
                        return 1, usage
                log_fh.close()
            case Err(e):
                _fail(f"Cannot open log: {e}")
                return 1, usage

        if rc == 0:
            if attempt > 0:
                _success(f"Lead review completed with fallback {C.BOLD}{attempt_model}{C.RESET}")
            else:
                _success(f"Lead review completed")
            break
        if attempt < len(chain) - 1:
            _warn(f"Lead model {C.BOLD}{attempt_model}{C.RESET} exited {rc} \u2014 rotating to next fallback")
    else:
        _fail(f"Lead review failed on all {len(chain)} model(s) (last exit {rc})")

    line: str = _usage_line(usage)
    if line:
        print(line)
    print()
    return rc, usage


def run_per_cu_lead(cu_id: str, cu_file: Path, model: str, max_iterations: int,
                     feedback_dir: Path, log_dir: Path, timeout: int,
                     project_dir: Path,
                     fallback_models: Sequence[str] = (),
                     breach_context: str = "") -> Ok[tuple[int, TokenUsage]] | Err[Exception]:
    colour: str = C.BR_MAG
    prefix: str = f"[LEAD-{cu_id}]"
    usage: TokenUsage = TokenUsage()

    if breach_context:
        bc: str = (
            f"\nBOUNDARY VIOLATIONS (why escalated):\n"
            f"{breach_context}\n\n"
        )
    else:
        bc = ""

    prompt: str = (
        f"You are the code lead. CU {cu_id} was escalated by the coder.\n\n"
        f"1. {_cu_read_instructions(cu_file)}\n"
        f"2. Read the escalation feedback at {feedback_dir / f'{cu_id}.feedback.txt'}\n"
        f"{bc}"
        f"3. Implement the CU yourself (the coder failed). Stay inside the CU scope.\n"
        f"4. Write updated feedback to {feedback_dir / f'{cu_id}.feedback.txt'}\n"
        f"5. Do NOT commit \u2014 the final wave lead will commit\n\n"
        f"Max fix iterations: {max_iterations}"
    )

    print(f"  {colour}{C.BOLD}\u25b8 {colour}{C.BOLD}Lead \u2014 fixing {cu_id}{C.RESET}")
    print(f"    {C.DIM}{'\u2500' * 55}{C.RESET}")

    chain: ModelFallbackChain = ModelFallbackChain(model, tuple(fallback_models))
    rc: int = 1
    attempt: int
    attempt_model: str
    for attempt, attempt_model in enumerate(chain):
        log_file: Path = log_dir / f"lead-{cu_id}.attempt-{attempt}.log"
        if attempt > 0:
            _warn(f"Lead fallback attempt {attempt}: trying {C.BOLD}{attempt_model}{C.RESET}")

        cmd: list[str] = [
            "opencode", "run",
            "--model", attempt_model,
            "--agent", "verifier",
            "--format", "json",
            "--auto",
            prompt,
        ]

        log_fh: typing.TextIO
        e: Exception
        match _open_file(log_file, "w"):
            case Ok(log_fh):
                match _stream_process_json(cmd, log_fh, colour, prefix,
                                           timeout, project_dir, usage):
                    case Ok(rc):
                        pass
                    case Err(e):
                        log_fh.close()
                        if isinstance(e, TimeoutError) and attempt < len(chain) - 1:
                            _warn(f"Lead model {C.BOLD}{attempt_model}{C.RESET} timed out after {timeout}s \u2014 rotating to next fallback")
                            continue
                        return Err(e)
                log_fh.close()
            case Err(e):
                return Err(e)

        if rc == 0:
            break
        if attempt < len(chain) - 1:
            _warn(f"Lead model {C.BOLD}{attempt_model}{C.RESET} exited {rc} \u2014 rotating to next fallback")

    match rc:
        case 0:
            if attempt > 0:
                _success(f"Lead fixed {cu_id} with fallback {C.BOLD}{attempt_model}{C.RESET}")
            else:
                _success(f"Lead fixed {cu_id}")
        case _:
            _fail(f"Lead failed on {cu_id} (exit {rc})")

    line: str = _usage_line(usage)
    if line:
        print(line)
    print()
    return Ok((rc, usage))


def _is_escalated(cu_id: str, feedback_dir: Path) -> bool:
    """Check if a specific CU has ESCALATION status."""
    fb: Path = feedback_dir / f"{cu_id}.feedback.txt"
    text: str
    match _read_file_text(fb):
        case Ok(text):
            return "STATUS=ESCALATION" in text
        case Err(_):
            return False


# ── Escalation check ───────────────────────────────────────────────

def check_escalations(wave_cus: list[str], feedback_dir: Path) -> int:
    count: int = 0
    cu_id: str
    for cu_id in wave_cus:
        fb: Path = feedback_dir / f"{cu_id}.feedback.txt"
        if fb.is_file():
            text: str = fb.read_text(errors="replace")
            if "STATUS=ESCALATION" in text:
                count += 1
                _warn(f"Escalation: {cu_id}")
    return count


# ── Convergence tracking ───────────────────────────────────────────

def load_convergence(path: Path) -> ConvergenceData:
    if path.is_file():
        data: ConvergenceData = json.loads(path.read_text())
        return data
    return {"waves": [], "status": "RUNNING"}

def save_convergence(path: Path, data: ConvergenceData) -> None:
    path.write_text(json.dumps(data, indent=2) + "\n")

def update_convergence(path: Path, wave_num: int, wave_cus: list[str],
                       escalations: int, coder_ok: bool, lead_ok: bool,
                       files_changed: dict[str, list[str]],
                       credits: dict[str, TokenReport]) -> None:
    data: ConvergenceData = load_convergence(path)
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


# ── CU boundary enforcement ─────────────────────────────────────────

@dataclass(frozen=True, slots=True)
class BoundaryBreach:
    """One boundary violation: which scope, which path, which rule."""
    scope: str
    path: str
    rule: str


def _resolved(declared: NoTouch, project_dir: Path) -> NoTouch:
    """Resolve declared patterns through symlinks against the repo root."""
    return NoTouch(
        entries=declared.entries,
        patterns=resolve_patterns(declared.patterns, project_dir),
        unparsed=declared.unparsed,
    )


def _owned_breaches(writes: CoderWrites,
                    guards: dict[str, NoTouch]) -> Sequence[BoundaryBreach]:
    """Rule 1: a CU that writes a path its own frame forbids fails."""
    declared: NoTouch = guards.get(writes.cu_id, parse_no_touch(""))
    if not has_enforceable_patterns(declared):
        return (BoundaryBreach(scope=writes.cu_id, path="", rule="unenforceable"),)
    found: list[BoundaryBreach] = []
    path: str
    for path in writes.paths:
        if is_touched(declared.patterns, path):
            found.append(BoundaryBreach(scope=writes.cu_id, path=path, rule="no-touch"))
    return tuple(found)


def _collision_breaches(per_cu: Sequence[CoderWrites]) -> Sequence[BoundaryBreach]:
    """Rule 2: two CU scopes writing one path in a wave both fail."""
    owners: dict[str, list[str]] = {}
    writes: CoderWrites
    for writes in per_cu:
        path: str
        for path in writes.paths:
            owners.setdefault(path, []).append(writes.cu_id)
    found: list[BoundaryBreach] = []
    path2: str
    ids: list[str]
    for path2, ids in sorted(owners.items()):
        if len(ids) < 2:
            continue
        cu: str
        for cu in ids:
            found.append(BoundaryBreach(scope=cu, path=path2, rule="shared-write"))
    return tuple(found)


def _unattributed_breaches(unattributed: Sequence[str],
                           guards: dict[str, NoTouch]) -> Sequence[BoundaryBreach]:
    """Rule 3: a change no CU claimed is still held to every guard."""
    found: list[BoundaryBreach] = []
    path: str
    for path in unattributed:
        owners: list[str] = [
            cu for cu, declared in guards.items()
            if is_touched(declared.patterns, path)]
        if owners:
            found.append(BoundaryBreach(
                scope="wave", path=path,
                rule="unattributed:" + ",".join(sorted(owners))))
    return tuple(found)


def boundary_breaches(writes: WaveWrites,
                      guards: dict[str, NoTouch]) -> Sequence[BoundaryBreach]:
    """Apply all three rules to one measured wave."""
    found: list[BoundaryBreach] = []
    per: CoderWrites
    for per in writes.per_cu:
        found.extend(_owned_breaches(per, guards))
    found.extend(_collision_breaches(writes.per_cu))
    found.extend(_unattributed_breaches(writes.unattributed, guards))
    return tuple(found)


def report_breaches(breaches: Sequence[BoundaryBreach], feedback_dir: Path) -> None:
    """Print every breach and leave the same record on disk."""
    breach: BoundaryBreach
    for breach in breaches:
        _fail(f"CU boundary {breach.rule}: {breach.scope} -> {breach.path}")
    text: str = "\n".join(
        f"SCOPE={b.scope}\nRULE={b.rule}\nPATH={b.path}\n" for b in breaches)
    match _write_file_text(feedback_dir / "boundary-breaches.txt", text):
        case Ok(_):
            pass
        case Err(e):
            _warn(f"Cannot write boundary report: {e}")


def _runner_relative_roots(project_dir: Path, dirs: Sequence[Path]) -> tuple[str, ...]:
    """Repo-relative roots of runner bookkeeping directories."""
    root: Path = project_dir.resolve()
    found: list[str] = []
    directory: Path
    for directory in dirs:
        try:
            found.append(directory.resolve().relative_to(root).as_posix())
        except (OSError, ValueError):
            continue
    return tuple(found)


def _without_runner_paths(per_cu: Sequence[CoderWrites],
                          roots: Sequence[str]) -> tuple[CoderWrites, ...]:
    """Drop runner bookkeeping paths from per-CU write attribution.

    Coders mkdir the feedback directory and write their feedback file into
    it. Those are protocol artifacts, not CU write targets, so they must
    not trip the shared-write rule when several CUs report in one wave.
    """
    kept: list[CoderWrites] = []
    writes: CoderWrites
    path: str
    for writes in per_cu:
        paths: tuple[str, ...] = tuple(
            path for path in writes.paths
            if not any(path == root or path.startswith(root + "/") for root in roots))
        kept.append(CoderWrites(cu_id=writes.cu_id, paths=paths))
    return tuple(kept)


def enforce_boundary(per_cu: Sequence[CoderWrites], project_dir: Path,
                     base_ref: str, baseline_dirty: frozenset[str],
                     guards: dict[str, NoTouch],
                     feedback_dir: Path,
                     log_dir: Path) -> tuple[Sequence[BoundaryBreach], Sequence[str]]:
    """Measure the wave, attribute it to CUs, and apply every rule."""
    scoped: tuple[CoderWrites, ...] = _without_runner_paths(
        per_cu, _runner_relative_roots(project_dir, (feedback_dir, log_dir)))
    wave_paths: Sequence[str] = wave_writes(project_dir, base_ref, baseline_dirty)
    attributed: WaveWrites = attribute_writes(wave_paths, scoped)
    breaches: Sequence[BoundaryBreach] = boundary_breaches(attributed, guards)
    if breaches:
        report_breaches(breaches, feedback_dir)
    claimed: set[str] = set()
    writes: CoderWrites
    for writes in scoped:
        claimed.update(writes.paths)
    return breaches, tuple(sorted(claimed | set(wave_paths)))


def _paths_against(paths: Sequence[str], guards: dict[str, NoTouch],
                   scope: str) -> Sequence[BoundaryBreach]:
    """Hold one agent's writes to a set of CU guards."""
    found: list[BoundaryBreach] = []
    path: str
    for path in paths:
        owners: list[str] = [
            cu for cu, declared in guards.items()
            if is_touched(declared.patterns, path)]
        if owners:
            found.append(BoundaryBreach(
                scope=scope, path=path,
                rule="lead:" + ",".join(sorted(owners))))
    return tuple(found)


def _unattributed_detail(feedback_dir: Path) -> str:
    """Rule 3 detail: wave-scoped breach lines from the last report."""
    text: str
    match _read_file_text(feedback_dir / "boundary-breaches.txt"):
        case Ok(text):
            blocks: list[str] = []
            block: str
            for block in text.strip().split("\n\n"):
                if "SCOPE=wave" not in block:
                    continue
                fields: list[str] = [
                    line.strip() for line in block.strip().splitlines()
                    if line.strip()]
                blocks.append(" ".join(fields))
            return "\n".join(blocks)
        case Err(_):
            return ""


def _escalated_in(wave_cus: Sequence[str], feedback_dir: Path) -> set[str]:
    """CU ids whose feedback marks STATUS=ESCALATION."""
    found: set[str] = set()
    cu: str
    text: str
    for cu in wave_cus:
        match _read_file_text(feedback_dir / f"{cu}.feedback.txt"):
            case Ok(text) if "STATUS=ESCALATION" in text:
                found.add(cu)
            case _:
                pass
    return found


# ── CU boundary correction pass ───────────────────────────────────

def _breach_detail_for_cu(breaches: Sequence[BoundaryBreach], cu_id: str) -> str:
    lines: list[str] = []
    b: BoundaryBreach
    for b in breaches:
        if b.scope == cu_id and b.rule in ("no-touch", "shared-write"):
            lines.append(f"SCOPE={b.scope} RULE={b.rule} PATH={b.path}")
            continue
        if b.rule == "shared-write" and b.scope == cu_id:
            lines.append(f"SCOPE={b.scope} RULE={b.rule} PATH={b.path}")
    return "\n".join(lines)


def _run_coder_correction(cu_file: Path, cu_id: str, model: str,
                          breaches: Sequence[BoundaryBreach],
                          feedback_dir: Path, log_dir: Path,
                          timeout: int, project_dir: Path,
                          colour_code: str, prefix: str) -> Ok[CoderOutput] | Err[Exception]:
    detail: str = _breach_detail_for_cu(breaches, cu_id)
    prompt: str = (
        f"CORRECTION PASS for CU {cu_id}.\n"
        f"You went out of scope. Fix ONLY these violations:\n"
        f"{detail}\n\n"
        f"Rules:\n"
        f"1. {_cu_read_instructions(cu_file)}\n"
        f"2. Fix ONLY the listed paths. Revert or move out-of-scope changes.\n"
        f"3. Do NOT change in-scope files unnecessarily.\n"
        f"4. Do NOT read other CU folders.\n"
        f"5. Write feedback to {feedback_dir / f'{cu_id}.feedback.txt'} if needed.\n"
        f"6. Do NOT commit.\n"
        f"Your scope is EXACTLY one CU: {cu_id}.\n"
    )
    combined_usage: TokenUsage = TokenUsage()
    files_changed: list[str] = []
    attempt: int
    for attempt in range(3):
        log_file: Path = log_dir / f"{cu_id}.correction-{attempt}.log"
        attempt_usage: TokenUsage = TokenUsage()
        cmd: list[str] = [
            "opencode", "run",
            "--model", model,
            "--agent", "llmdd/coder",
            "--format", "json",
            "--auto",
            prompt,
        ]
        log_fh: typing.TextIO
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
        combined_usage.input += attempt_usage.input
        combined_usage.output += attempt_usage.output
        combined_usage.reasoning += attempt_usage.reasoning
        combined_usage.cache_read += attempt_usage.cache_read
        combined_usage.cache_write += attempt_usage.cache_write
        combined_usage.total += attempt_usage.total
        combined_usage.cost += attempt_usage.cost
        if attempt_usage.session_id:
            combined_usage.session_id = attempt_usage.session_id
        files_changed = list(coder_writes(cu_id, log_dir, project_dir).paths)
        text: str
        match _read_file_text(log_file):
            case Ok(text):
                lowered_text: str = text.lower()
                is_refusal: bool = False
                p: str
                for p in _REFUSAL_PATTERNS:
                    if p.lower() in lowered_text:
                        is_refusal = True
                        break
                is_error_lines: bool = False
                line: str
                ev: JsonEvent
                for line in text.splitlines():
                    parsed_line: Ok[JsonEvent] | Err[ValueError] = _parse_event_line(line)
                    if isinstance(parsed_line, Err):
                        continue
                    ev = parsed_line.value
                    if ev.get("type") == "error":
                        is_error_lines = True
                        break
            case Err(_):
                is_refusal = False
                is_error_lines = False
        is_error: bool = is_error_lines if stream_rc == 0 else False
        succeeded: bool = stream_rc == 0 and not is_refusal and not is_error
        if succeeded:
            break
    return Ok(CoderOutput(rc=stream_rc, files_changed=files_changed,
                          usage=combined_usage, model_used=model))


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
    lead_fallbacks: list[str],
    max_fix_iterations: int,
    no_per_cu_lead: bool,
    baseline_dirty: frozenset[str],
) -> tuple[dict[str, list[str]], bool, Sequence[str]]:
    """Run all CUs in a wave in parallel via ThreadPoolExecutor.

    Returns (wave_files_changed, all_passed, written).
    No try/except in this function — all error handling via Result monad + match/case.
    """
    wave_files_changed: dict[str, list[str]] = {}
    all_passed: bool = True
    loop: asyncio.AbstractEventLoop = asyncio.get_event_loop()

    # Capture git HEAD reference ONCE before parallel dispatch
    stash_ref: str
    e: Exception
    match _git_run("rev-parse", "HEAD", cwd=project_dir):
        case Ok(stash_ref):
            pass
        case Err(e):
            _fail(f"Cannot get git HEAD: {e}")
            return {}, False, ()

    pool: concurrent.futures.ThreadPoolExecutor
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(cu_files)) as pool:
        # Dispatch all coders in parallel
        coder_futures: list[tuple[str, Path, asyncio.Future[Ok[CoderOutput] | Err[Exception]]]] = []
        idx: int
        cu_id: str
        cu_file: Path
        for idx, (cu_id, cu_file) in enumerate(cu_files):
            coder_colour: tuple[str, str] = CODER_COLOURS[idx % len(CODER_COLOURS)]
            fallback_chain: ModelFallbackChain = ModelFallbackChain(coder_model, tuple(coder_fallbacks))

            future: asyncio.Future[Ok[CoderOutput] | Err[Exception]] = loop.run_in_executor(
                pool,
                run_coder,
                cu_file, cu_id, fallback_chain,
                feedback_dir, log_dir, cu_timeout,
                project_dir, coder_colour,
                stash_ref,
            )
            coder_futures.append((cu_id, cu_file, future))

        # Await all coders with monadic wrapping
        rc: int
        files: list[str]
        usage: TokenUsage
        model: str
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

    guards: dict[str, NoTouch] = {}
    guard_cu: str
    guard_file: Path
    for guard_cu, guard_file in cu_files:
        guards[guard_cu] = _resolved(_cu_no_touch(guard_file), project_dir)

    per_cu_writes: list[CoderWrites] = []
    write_cu: str
    write_file: Path
    for write_cu, write_file in cu_files:
        per_cu_writes.append(coder_writes(write_cu, log_dir, project_dir))

    breaches: Sequence[BoundaryBreach]
    written: Sequence[str]
    breaches, written = enforce_boundary(
        per_cu_writes, project_dir, stash_ref, baseline_dirty, guards, feedback_dir, log_dir)

    breached_cus: set[str] = set()
    b: BoundaryBreach
    for b in breaches:
        if b.scope != "wave" and b.rule in ("no-touch", "shared-write"):
            breached_cus.add(b.scope)
    model_by_cu: dict[str, str] = {}
    cu_id_c: str
    cu_file_c2: Path
    fut_c: asyncio.Future[Ok[CoderOutput] | Err[Exception]]
    for cu_id_c, cu_file_c2, fut_c in coder_futures:
        res_m: Ok[CoderOutput] | Err[Exception] = await future_result(fut_c)
        co: CoderOutput
        match res_m:
            case Ok(co):
                if co.model_used:
                    model_by_cu[cu_id_c] = co.model_used
            case Err(_):
                pass
    colour_by_cu: dict[str, str] = {}
    cidx: int
    cu_id_c2: str
    _cu_file_c2: Path
    for cidx, (cu_id_c2, _cu_file_c2) in enumerate(cu_files):
        cc: str
        _cp: str
        cc, _cp = CODER_COLOURS[cidx % len(CODER_COLOURS)]
        colour_by_cu[cu_id_c2] = cc
    correction_failed: set[str] = set()
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as cpool:
        cu_corr: str
        for cu_corr in sorted(breached_cus):
            if cu_corr not in model_by_cu or not model_by_cu[cu_corr]:
                continue
            cu_file_c3: Path | None = None
            cid3: str
            cfile3: Path
            for cid3, cfile3 in cu_files:
                if cid3 == cu_corr:
                    cu_file_c3 = cfile3
                    break
            if cu_file_c3 is None:
                continue
            attempts_made: int = 0
            attempt_c: int
            for attempt_c in range(3):
                attempts_made = attempt_c + 1
                per_cu_writes_c: list[CoderWrites] = []
                wc_id_c: str
                wc_file_c: Path
                for wc_id_c, wc_file_c in cu_files:
                    per_cu_writes_c.append(coder_writes(wc_id_c, log_dir, project_dir))
                breaches_c: Sequence[BoundaryBreach]
                written_c: Sequence[str]
                breaches_c, written_c = enforce_boundary(
                    per_cu_writes_c, project_dir, stash_ref, baseline_dirty, guards, feedback_dir, log_dir)
                if not breaches_c:
                    break
                still: bool = False
                bb: BoundaryBreach
                for bb in breaches_c:
                    if bb.scope == cu_corr and bb.rule in ("no-touch", "shared-write"):
                        still = True
                        break
                if not still:
                    break
                fut_cc: asyncio.Future[Ok[CoderOutput] | Err[Exception]] = loop.run_in_executor(
                    cpool, _run_coder_correction, cu_file_c3, cu_corr,
                    model_by_cu[cu_corr], breaches_c, feedback_dir, log_dir,
                    1800, project_dir, colour_by_cu.get(cu_corr, C.BR_CYN), f"[{cu_corr}]")
                res_cc: Ok[CoderOutput] | Err[Exception] = await future_result(fut_cc)
                e_c: Exception
                match res_cc:
                    case Ok(_):
                        pass
                    case Err(e_c):
                        _fail(f"Correction failed for {cu_corr}: {e_c}")
                        break
            if attempts_made >= 3:
                per_cu_writes_c2: list[CoderWrites] = []
                wc_id_c2: str
                wc_file_c2: Path
                for wc_id_c2, wc_file_c2 in cu_files:
                    per_cu_writes_c2.append(coder_writes(wc_id_c2, log_dir, project_dir))
                breaches_c2: Sequence[BoundaryBreach]
                written_c2: Sequence[str]
                breaches_c2, written_c2 = enforce_boundary(
                    per_cu_writes_c2, project_dir, stash_ref, baseline_dirty, guards, feedback_dir, log_dir)
                still2: bool = False
                bb2: BoundaryBreach
                for bb2 in breaches_c2:
                    if bb2.scope == cu_corr and bb2.rule in ("no-touch", "shared-write"):
                        still2 = True
                        break
                if still2:
                    correction_failed.add(cu_corr)
    per_cu_writes = []
    wc_id: str
    wc_file: Path
    for wc_id, wc_file in cu_files:
        per_cu_writes.append(coder_writes(wc_id, log_dir, project_dir))
    breaches, written = enforce_boundary(
        per_cu_writes, project_dir, stash_ref, baseline_dirty, guards, feedback_dir, log_dir)

    if breaches:
        all_passed = False

    # Per-CU lead checks (sequential after all coders done — avoids file conflicts)
    escalated_cus: list[str] = []
    _leftover: asyncio.Future[Ok[CoderOutput] | Err[Exception]]
    text: str
    lead_rc: int
    lead_usage: TokenUsage
    breach_detail_by_cu: dict[str, str] = {}
    for cu_id in sorted(set([b.scope for b in breaches if b.scope not in ("wave",)])):
        breach_detail_by_cu[cu_id] = _breach_detail_for_cu(breaches, cu_id)
    cf: str
    for cf in sorted(correction_failed):
        if cf not in breach_detail_by_cu:
            breach_detail_by_cu[cf] = _breach_detail_for_cu(breaches, cf)
    for cu_id, cu_file, _leftover in coder_futures:
        if no_per_cu_lead:
            continue
        is_esc: bool = False
        match _read_file_text(feedback_dir / f"{cu_id}.feedback.txt"):
            case Ok(text) if "STATUS=ESCALATION" in text:
                is_esc = True
            case _:
                pass
        if not is_esc and cu_id in correction_failed:
            is_esc = True
        if is_esc:
            if cu_id not in escalated_cus:
                escalated_cus.append(cu_id)
            _warn(f"Escalation detected: {cu_id}")
            match run_per_cu_lead(
                cu_id, cu_file, lead_model, max_fix_iterations,
                feedback_dir, log_dir, lead_timeout, project_dir,
                lead_fallbacks,
                breach_context=breach_detail_by_cu.get(cu_id, ""),
            ):
                case Ok((lead_rc, lead_usage)):
                    tracker.accumulate(lead_usage)
                    wc.coders[f"{cu_id}-lead"] = lead_usage
                case Err(e):
                    _fail(f"Lead failed on {cu_id}: {e}")

    guard_breaches: list[BoundaryBreach] = []
    escalated_set: set[str] = set(escalated_cus)
    lead_cu: str
    lead_ref: Path
    lead_left: asyncio.Future[Ok[CoderOutput] | Err[Exception]]
    for lead_cu, lead_ref, lead_left in coder_futures:
        if lead_cu not in escalated_set:
            continue
        raw_lead: CoderWrites = coder_writes(f"lead-{lead_cu}", log_dir, project_dir)
        owned: Sequence[BoundaryBreach] = _owned_breaches(
            CoderWrites(cu_id=lead_cu, paths=raw_lead.paths), guards)
        for breach in owned:
            if breach.rule == "no-touch":
                guard_breaches.append(
                    BoundaryBreach(scope=breach.scope, path=breach.path, rule="lead"))
            else:
                guard_breaches.append(breach)
    if guard_breaches:
        report_breaches(tuple(guard_breaches), feedback_dir)
        all_passed = False

    # After all per-CU leads, recompute files changed from pre-wave ref.
    # This captures files committed by leads (coders' files_changed is empty
    # when the lead implements the CU instead of the coder).
    diff_out: str
    if escalated_cus:
        match _git_run("diff", "--name-only", stash_ref, cwd=project_dir):
            case Ok(diff_out):
                all_changed: list[str] = diff_out.splitlines() if diff_out else []
                for cu_id in escalated_cus:
                    wave_files_changed[cu_id] = all_changed
            case Err(_):
                pass

    return wave_files_changed, all_passed, written


# ── Async entry point ────────────────────────────────────────────

async def main_async(args: argparse.Namespace) -> int:
    """Async entry point — drives wave execution.
    No try/except in this function — all error handling via match/case on Result monad.
    """
    itr_path: Path = Path(args.itr).resolve()
    if not itr_path.is_dir():
        _fail(f"ITR path does not exist: {itr_path}")
        sys.exit(1)

    app_name: str = args.app
    # Run outputs live INSIDE the ITR folder, never scattered next to it.
    work_dir: Path = itr_path

    # Resolve project root
    project_dir: Path | None
    if args.project:
        project_dir = Path(args.project).resolve()
    else:
        project_dir = None
        start: Path
        for start in [Path.cwd(), work_dir.resolve()]:
            search: Path = start
            depth: int
            for depth in range(10):
                if (search / ".opencode" / "agents").is_dir():
                    project_dir = search
                    break
                parent: Path = search.parent
                if parent == search:
                    break
                search = parent
            if project_dir:
                break
        if project_dir is None:
            _fail("Cannot find project root (.opencode/agents/ not found)")
            _info("Use --project to specify the project root")
            sys.exit(1)

    feedback_dir: Path = work_dir / f"{app_name}.feedback"
    log_dir: Path = work_dir / f"{app_name}.logs"
    convergence_file: Path = work_dir / f"{app_name}.convergence.json"

    # A dry-run prints the plan and touches nothing on disk.
    if not args.dry_run:
        feedback_dir.mkdir(parents=True, exist_ok=True)
        log_dir.mkdir(parents=True, exist_ok=True)

    cu_map: dict[str, Path] = build_cu_map(itr_path)

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
    _info(f"Coder fallbacks: {', '.join(args.coder_fallbacks) if args.coder_fallbacks else '(none)'}")
    _info(f"Lead model:    {args.lead_model}")
    _info(f"Lead fallbacks: {', '.join(args.lead_fallbacks) if args.lead_fallbacks else '(none)'}")
    _info(f"CU timeout:    {args.cu_timeout}s")
    _info(f"Lead timeout:  {args.lead_timeout}s")
    print()

    _header("Loading waves...")
    waves: list[WaveDef] = detect_waves(itr_path, args.waves)
    if not args.dry_run:
        log_dir.joinpath("waves.json").write_text(json.dumps(waves, indent=2) + "\n")

    num_waves: int = len(waves)
    _success(f"Found {num_waves} waves")
    print()

    if not args.dry_run:
        save_convergence(convergence_file, {"waves": [], "status": "RUNNING"})
    tracker: CreditTracker = CreditTracker()
    run_written: set[str] = set()

    wave_def: WaveDef
    for wave_def in waves:
        wave_num: int = wave_def["wave"]
        wave_cus: list[str] = wave_def["cus"]
        description: str = wave_def.get("description", "")
        wc: WaveCredits = tracker.start_wave(wave_num)

        print(_banner_line())
        print(f"  {C.BOLD}{C.BR_CYAN}Wave {wave_num}/{num_waves}{C.RESET}"
              + (f"  {C.DIM}— {description}{C.RESET}" if description else ""))
        print(_banner_line())
        _info(f"CUs: {', '.join(wave_cus)}")
        print()

        baseline_dirty: frozenset[str] = frozenset(
            _dirty_paths(project_dir) - run_written)

        cu_files: list[tuple[str, Path]] = []
        cu_id: str
        for cu_id in wave_cus:
            cu_file: Path | None = resolve_cu_file(cu_id, cu_map, itr_path)
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
            f: Path
            for cu_id, f in cu_files:
                print(f"    {C.DIM}·{C.RESET} {C.BR_CYAN}{cu_id}{C.RESET} {C.DIM}→ {f.name}{C.RESET}")
            print()
            continue

        clashes: list[tuple[str, str]] = _wave_dirty_clashes(cu_files, baseline_dirty)
        if clashes:
            owner: str
            path: str
            for owner, path in clashes:
                _fail(f"wave {wave_num}: CU {owner} declares {path} was already dirty before this run")
            sys.exit(1)

        # Capture pre-wave HEAD ref for accurate file tracking after wave lead commits
        ref: str
        pre_wave_ref: str | None
        match _git_run("rev-parse", "HEAD", cwd=project_dir):
            case Ok(ref):
                pre_wave_ref = ref
            case Err(_):
                _warn(f"Wave {wave_num}: could not capture HEAD ref; file tracking may be inaccurate")
                pre_wave_ref = None

        _header(f"Running {len(cu_files)} coder(s) in parallel...")
        print()

        wave_files_changed: dict[str, list[str]]
        all_passed: bool
        written: Sequence[str]
        run_written: set[str]
        wave_files_changed, all_passed, written = await run_wave_parallel(
            cu_files, wc, tracker,
            feedback_dir, log_dir, project_dir,
            args.coder_model, args.coder_fallbacks,
            args.cu_timeout,
            args.lead_model, args.lead_timeout,
            args.lead_fallbacks,
            args.max_fix_iterations,
            args.no_per_cu_lead,
            baseline_dirty,
        )
        run_written.update(written)

        # Wave file summary
        print(f"  {C.BOLD}Wave {wave_num} files changed:{C.RESET}")
        files: list[str]
        for cu_id, files in wave_files_changed.items():
            if files:
                fname: str
            for fname in files:
                print(f"    {C.DIM}├{C.RESET} {C.BR_CYAN}{cu_id}{C.RESET} {C.DIM}→{C.RESET} {fname}")
        total_files: set[str] = set()
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
        lead_rc: int
        lead_usage: TokenUsage
        lead_rc, lead_usage = run_code_lead(
            wave_num, num_waves, args.lead_model, args.max_fix_iterations,
            feedback_dir, log_dir, args.lead_timeout,
            project_dir, wave_cus, args.lead_fallbacks,
            unattributed_context=_unattributed_detail(feedback_dir),
        )
        tracker.accumulate(lead_usage)
        lead_ok: bool = lead_rc == 0

        escalated_ids: set[str] = _escalated_in(wave_cus, feedback_dir)
        wave_lead_paths: Sequence[str] = coder_writes(
            f"lead-wave-{wave_num}", log_dir, project_dir).paths
        lead_guards: dict[str, NoTouch] = {}
        guard_name: str
        guard_path: Path
        for guard_name, guard_path in cu_files:
            if guard_name not in escalated_ids:
                lead_guards[guard_name] = _resolved(
                    _cu_no_touch(guard_path), project_dir)
        lead_breaches: Sequence[BoundaryBreach] = _paths_against(
            wave_lead_paths, lead_guards, f"lead-wave-{wave_num}")
        if lead_breaches:
            report_breaches(lead_breaches, feedback_dir)
            all_passed = False

        # After wave lead, merge files committed by wave lead into file tracking
        diff_out: str
        if pre_wave_ref:
            match _git_run("diff", "--name-only", pre_wave_ref, cwd=project_dir):
                case Ok(diff_out):
                    lead_files: set[str] = set(diff_out.splitlines() if diff_out else [])
                    if lead_files:
                        for cu_id in wave_files_changed:
                            existing: set[str] = set(wave_files_changed[cu_id])
                            wave_files_changed[cu_id] = list(existing | lead_files)
                case Err(_):
                    pass

        escalations: int = check_escalations(wave_cus, feedback_dir)
        if escalations > 0:
            _warn(f"Wave {wave_num}: {escalations} escalation(s)")
        else:
            _success(f"Wave {wave_num} completed")
        print()

        credits_data: dict[str, TokenReport] = {}
        cu_credit_id: str
        cu_usage: TokenUsage
        for cu_credit_id, cu_usage in wc.coders.items():
            credits_data[cu_credit_id] = cu_usage.dict()
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
        return 0

    convergence: ConvergenceData = load_convergence(convergence_file)
    total_escalations: int = 0
    failed_waves: int = 0
    w: WaveRecord
    for w in convergence["waves"]:
        total_escalations += w["escalations"]
        if w["coder_status"] != 0 or w["lead_status"] != 0:
            failed_waves += 1
    unique_all: set[str] = set()
    for w in convergence["waves"]:
        for files in w.get("files_changed", {}).values():
            unique_all.update(files)

    if total_escalations == 0 and failed_waves == 0:
        convergence["status"] = "CONVERGED"
    else:
        convergence["status"] = "NEEDS_ATTENTION"
    save_convergence(convergence_file, convergence)

    status_colour: str = C.BR_GREEN if convergence["status"] == "CONVERGED" else C.BR_RED

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

    if convergence["status"] == "CONVERGED":
        return 0
    return 1


def main(argv: list[str] | None = None) -> None:
    """Entry point — parses args and delegates to async main."""
    args: argparse.Namespace = parse_args(argv)
    if args.no_color:
        C.disable()
    sys.exit(asyncio.run(main_async(args)))


if __name__ == "__main__":
    main()
