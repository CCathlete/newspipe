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
from dataclasses import dataclass
from pathlib import Path
from typing import Generic, Optional, TypeVar

# TypeVar definitions stay bare: annotating them is a linter error.
# ARCH.PYTHON exemption — the TypeVar itself is the declaration.
T = TypeVar("T")
E = TypeVar("E", bound=Exception)


@dataclass(slots=True, frozen=True)
class Ok(Generic[T]):
    """Success monad u2014 contains a value."""
    value: T


@dataclass(slots=True, frozen=True)
class Err(Generic[E]):
    """Failure monad u2014 contains an exception."""
    error: E


RETRY_CAP: int = 3
BUDGET_RE: re.Pattern[str] = re.compile(r"^MAX_ATTEMPTS=(\d+)\s*$", re.MULTILINE)


def _read_text(path: Path) -> Ok[str] | Err[OSError]:
    """Read a file as text. Boundary translator: OSError becomes Err."""
    # MONADIC-BOUNDARY: foreign filesystem read; OS failure becomes Err.
    caught: OSError
    try:
        return Ok(path.read_text(encoding="utf-8"))
    except OSError as caught:
        return Err(caught)


def _parse_budget(text: str) -> Ok[int] | Err[ValueError]:
    """Parse MAX_ATTEMPTS out of SYNOPSIS text. A missing value becomes Err."""
    found: Optional[re.Match[str]] = BUDGET_RE.search(text)
    if found is None:
        return Err(ValueError("no MAX_ATTEMPTS=<int>"))
    return Ok(int(found.group(1)))


def main() -> int:
    argv: list[str] = sys.argv
    if len(argv) != 2:
        prog: str = argv[0] if argv else "test_synopsis_budget.py"
        print(f"usage: {prog} <path-to-SYNOPSIS.itr>", file=sys.stderr)
        return 2
    path: Path = Path(argv[1])
    text: str
    read_failure: OSError
    match _read_text(path):
        case Ok(value=text):
            pass
        case Err(error=read_failure):
            print(f"BUDGET_FAIL cannot read {path}: {read_failure}", file=sys.stderr)
            return 1
    budget: int
    match _parse_budget(text):
        case Ok(value=budget):
            pass
        case Err():
            print(f"BUDGET_FAIL no MAX_ATTEMPTS=<int> in {path}", file=sys.stderr)
            return 1
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
