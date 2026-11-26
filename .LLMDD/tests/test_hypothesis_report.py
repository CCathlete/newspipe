"""Tests for the deterministic hypothesis report echo checker."""
from __future__ import annotations

import json
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Sequence

ROOT: Path = Path(__file__).resolve().parents[2]
TOOL: Path = ROOT / ".LLMDD" / "tools" / "check-hypothesis-report.py"


@dataclass(frozen=True, slots=True)
class Case:
    """One table-driven checker case."""

    name: str
    verdict: str
    with_report: bool
    echo: str
    expected_ok: bool
    expected_reason: str


def block(stage: int, decision: str) -> str:
    return json.dumps(
        {"stage": stage, "decision": decision, "hypothesis_id": "H-CASE"},
        separators=(",", ":"),
        sort_keys=True,
    )


CURRENT: str = block(2, "RED")
PRIOR: str = block(1, "RED")

CASES: Sequence[Case] = (
    Case("verbatim echo passes", "RED_LIGHT", True, CURRENT, True, ""),
    Case("missing report fails", "RED_LIGHT", False, CURRENT, False, "report-file-missing"),
    Case("non-verbatim echo fails", "RED_LIGHT", True, "different echo text", False, "verbatim-echo-missing"),
    Case("missing echo fails", "RED_LIGHT", True, "", False, "verbatim-echo-missing"),
    Case("mismatched verdict fails", "GREEN_LIGHT", True, CURRENT, False, "verdict-mismatch"),
)


def run_case(case: Case, tmp_path: Path) -> tuple[int, str]:
    report: Path = tmp_path / "case.ledger-report.jsonl"
    conversation: Path = tmp_path / "case.hypothesis-conversation.md"
    if case.with_report:
        report.write_text(PRIOR + "\n" + CURRENT + "\n", encoding="utf-8")
    conversation.write_text(case.echo, encoding="utf-8")
    completed: subprocess.CompletedProcess[str] = subprocess.run(
        [sys.executable, str(TOOL), "--report", str(report), "--conversation", str(conversation),
         "--stage", "2", "--verdict", case.verdict],
        capture_output=True, text=True, check=False,
    )
    return completed.returncode, completed.stdout + completed.stderr


def test_checker_table(tmp_path: Path) -> None:
    import shutil
    case: Case
    for case in CASES:
        # Clean up any leftover report file from a previous case
        report: Path = tmp_path / "case.ledger-report.jsonl"
        if report.exists():
            report.unlink()
        code: int
        output: str
        code, output = run_case(case, tmp_path)
        assert (code == 0) is case.expected_ok, case.name + " " + output
        if not case.expected_ok:
            assert case.expected_reason in output, case.name + " " + output