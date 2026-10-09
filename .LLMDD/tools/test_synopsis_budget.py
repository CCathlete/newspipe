#!/usr/bin/env python3
"""Feat-1 T6b: pin the runner retry-budget contract.

Parses MAX_ATTEMPTS=<int> out of a SYNOPSIS.itr path given as argv[1],
asserts it equals the retry cap the runner would use, prints BUDGET_OK <n>.

Usage:
    python3 .LLMDD/tools/test_synopsis_budget.py <path-to-SYNOPSIS.itr>

Full runner enforcement of the budget is out of scope for feat-1; this test
pins the contract (budget comes from SYNOPSIS.itr).
"""
import re
import sys

RETRY_CAP = 3


def main() -> int:
    if len(sys.argv) != 2:
        print(f"usage: {sys.argv[0]} <path-to-SYNOPSIS.itr>", file=sys.stderr)
        return 2
    path = sys.argv[1]
    try:
        with open(path, "r", encoding="utf-8") as fh:
            text = fh.read()
    except OSError as exc:
        print(f"BUDGET_FAIL cannot read {path}: {exc}", file=sys.stderr)
        return 1
    match = re.search(r"^MAX_ATTEMPTS=(\d+)\s*$", text, re.MULTILINE)
    if not match:
        print(f"BUDGET_FAIL no MAX_ATTEMPTS=<int> in {path}", file=sys.stderr)
        return 1
    budget = int(match.group(1))
    if budget != RETRY_CAP:
        print(
            f"BUDGET_FAIL budget {budget} != runner retry cap {RETRY_CAP}",
            file=sys.stderr,
        )
        return 1
    print(f"BUDGET_OK {budget}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
