"""check-hypothesis-report.py u2014 deterministic gate for the per-stage ledger echo."""
from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Final, Sequence


@dataclass(frozen=True, slots=True)
class Failure:
    """A named, non-exceptional check failure."""

    reason: str


@dataclass(frozen=True, slots=True)
class Block:
    """One parsed ledger-report line."""

    stage: int
    decision: str
    line: str


VERDICT_TO_LABEL: Final[dict[str, str]] = {
    "GREEN_LIGHT": "GREEN",
    "RED_LIGHT": "RED",
    "ADVISOR_ACTION_REQUIRED": "BLOCKED",
}


def parse_block(line: str) -> Block | Failure:
    parsed: object
    try:
        parsed = json.loads(line)
    except json.JSONDecodeError as error:
        return Failure("block-malformed-json: " + error.msg)
    if not isinstance(parsed, dict):
        return Failure("block-not-object")
    stage: object = parsed.get("stage")
    decision: object = parsed.get("decision")
    if not isinstance(stage, int) or isinstance(stage, bool):
        return Failure("block-stage-invalid")
    if not isinstance(decision, str):
        return Failure("block-decision-invalid")
    return Block(stage=stage, decision=decision, line=line)


def check(report_path: Path, conversation_path: Path, stage: int, verdict: str) -> Failure | None:
    if not report_path.is_file():
        return Failure("report-file-missing: " + str(report_path))
    lines: Sequence[str] = tuple(
        line for line in report_path.read_text(encoding="utf-8").splitlines() if line.strip()
    )
    parsed: Sequence[Block | Failure] = tuple(parse_block(line) for line in lines)
    item: Block | Failure
    for item in parsed:
        if isinstance(item, Failure):
            return item
    blocks: Sequence[Block] = tuple(entry for entry in parsed if isinstance(entry, Block))
    matching: Sequence[Block] = tuple(entry for entry in blocks if entry.stage == stage)
    if len(matching) != 1:
        return Failure("stage-block-count: expected=1 actual=" + str(len(matching)))
    block: Block = matching[0]
    expected: str | None = VERDICT_TO_LABEL.get(verdict)
    if expected is None:
        return Failure("verdict-unknown: " + verdict)
    if block.decision != expected:
        return Failure("verdict-mismatch: report=" + block.decision + " verdict=" + expected)
    if not conversation_path.is_file():
        return Failure("conversation-file-missing: " + str(conversation_path))
    echo: str = conversation_path.read_text(encoding="utf-8")
    if block.line not in echo:
        return Failure("verbatim-echo-missing: stage=" + str(stage))
    return None


def main(argv: Sequence[str] | None = None) -> int:
    parser: argparse.ArgumentParser = argparse.ArgumentParser(
        description="Verify the current hypothesis stage report was echoed verbatim."
    )
    parser.add_argument("--report", required=True)
    parser.add_argument("--conversation", required=True)
    parser.add_argument("--stage", required=True, type=int)
    parser.add_argument("--verdict", required=True)
    args: argparse.Namespace = parser.parse_args(argv)
    failure: Failure | None = check(
        Path(args.report), Path(args.conversation), args.stage, str(args.verdict)
    )
    if failure is not None:
        print("CHECK_FAILED " + failure.reason)
        return 1
    print("CHECK_OK stage=" + str(args.stage))
    return 0


if __name__ == "__main__":
    sys.exit(main())