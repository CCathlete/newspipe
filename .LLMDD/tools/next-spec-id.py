"""Global per-repo spec-id allocator (feat-6).

Allocates bug and feature serials from a plain-text counter plus every git
ref, so ids stay unique per repository across branches and clones.

CLI:
    next-spec-id.py --kind bug|feature [--dry-run] [--prefix bugfix-feature]
    next-spec-id.py --check

Exit 0: clean, or an id printed. Exit 1: check violation. Exit 2: usage
error. See .LLMDD/tools/check-advisor-approval.py for the sibling shape.
"""
from __future__ import annotations

import argparse
import re
import subprocess
import sys
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import TypeAlias

Serial: TypeAlias = int
SpecKind: TypeAlias = str
CounterMap: TypeAlias = dict[str, Serial]
KindMax: TypeAlias = dict[str, Serial]
Findings: TypeAlias = list[str]

COUNTER_REL: str = ".LLMDD/spec-driven/ids.txt"
SPEC_DIRS: dict[str, str] = {
    "bug": ".LLMDD/spec-driven/bugs",
    "feature": ".LLMDD/spec-driven/features",
}
FEATURE_PREFIXES: Sequence[str] = ("feat-", "bugfix-feature-")
BUG_PREFIX: str = "bug-"
REF_NAMESPACES: Sequence[str] = ("refs/heads/", "refs/remotes/")
SKIP_DIRS: Sequence[str] = ("templates",)
BUG_RE: re.Pattern[str] = re.compile(r"^bug-(\d+)-")
FEATURE_RE: re.Pattern[str] = re.compile(r"^(?:feat|bugfix-feature)-(\d+)-")
COUNTER_RE: re.Pattern[str] = re.compile(r"^(bug|feature)=(\d+)\s*$")


@dataclass(slots=True, frozen=True)
class Usage:
    """Usage problem that ends the run with exit 2."""

    message: str


@dataclass(slots=True, frozen=True)
class Violation:
    """Check failure that ends the run with exit 1."""

    message: str


@dataclass(slots=True, frozen=True)
class RefSpec:
    """One spec directory as seen at one git ref."""

    ref: str
    directory: str
    names: Sequence[str]


@dataclass(slots=True, frozen=True)
class Repo:
    """Repository root plus the resolved spec tree."""

    root: Path

    @property
    def counter(self) -> Path:
        return self.root / COUNTER_REL

    def specs(self, kind: SpecKind) -> Path:
        return self.root / SPEC_DIRS[kind]


def repo_root() -> Path:
    """Repository root is two parents above .LLMDD/tools/."""
    return Path(__file__).resolve().parent.parent.parent


def spec_names(path: Path) -> Sequence[str]:
    """Spec filenames in one directory, empty when it is absent."""
    if not path.is_dir():
        return ()
    return tuple(sorted(entry.name for entry in path.iterdir() if entry.is_file()))


def tree_names(repo: Repo, kind: SpecKind) -> Sequence[str]:
    """Spec filenames from the working tree."""
    return spec_names(repo.specs(kind))


def ref_names(repo: Repo, ref: str, kind: SpecKind) -> Sequence[str]:
    """Spec filenames at one ref, empty when the path is absent there."""
    try:
        done: subprocess.CompletedProcess[str] = subprocess.run(
            ["git", "-C", str(repo.root), "ls-tree", "--name-only", ref, "--", SPEC_DIRS[kind]],
            capture_output=True,
            text=True,
            check=False,
        )
    except OSError:
        return ()
    if done.returncode != 0:
        return ()
    entries: list[str] = []
    line: str
    for line in done.stdout.splitlines():
        entry: str = line.strip().rsplit("/", 1)[-1]
        if entry.endswith(".md"):
            entries.append(entry)
    return tuple(entries)


def all_refs(repo: Repo) -> Sequence[str]:
    """Every branch ref in the clone, local and remote-tracking."""
    try:
        done: subprocess.CompletedProcess[str] = subprocess.run(
            ["git", "-C", str(repo.root), "for-each-ref", "--format=%(refname)", *REF_NAMESPACES],
            capture_output=True,
            text=True,
            check=False,
        )
    except OSError:
        return ()
    if done.returncode != 0:
        return ()
    return tuple(line.strip() for line in done.stdout.splitlines() if line.strip())


def ref_serials(repo: Repo) -> KindMax:
    """Highest serial per kind across every ref."""
    highest: KindMax = {"bug": 0, "feature": 0}
    ref: str
    for ref in all_refs(repo):
        kind: SpecKind
        for kind in ("bug", "feature"):
            names: Sequence[str] = ref_names(repo, ref, kind)
            pattern: re.Pattern[str] = BUG_RE if kind == "bug" else FEATURE_RE
            name: str
            for name in names:
                found: re.Match[str] | None = pattern.match(name)
                if found is not None:
                    highest[kind] = max(highest[kind], int(found.group(1)))
        text: str = ref_counter_text(repo, ref)
        key: str
        value: str
        for key, value in parse_counter(text):
            if key in highest:
                highest[key] = max(highest[key], int(value))
    return highest


def ref_counter_text(repo: Repo, ref: str) -> str:
    """Counter file text at one ref, empty when absent."""
    try:
        done: subprocess.CompletedProcess[str] = subprocess.run(
            ["git", "-C", str(repo.root), "show", ref + ":" + COUNTER_REL],
            capture_output=True,
            text=True,
            check=False,
        )
    except OSError:
        return ""
    if done.returncode != 0:
        return ""
    return done.stdout


def tree_serials(names: Sequence[str], kind: SpecKind) -> KindMax:
    """Highest serial per kind from one set of spec filenames."""
    highest: KindMax = {"bug": 0, "feature": 0}
    pattern: re.Pattern[str] = BUG_RE if kind == "bug" else FEATURE_RE
    name: str
    for name in names:
        found: re.Match[str] | None = pattern.match(name)
        if found is not None:
            highest[kind] = max(highest[kind], int(found.group(1)))
    return highest


def merge_max(left: KindMax, right: KindMax) -> KindMax:
    """Element-wise maximum of two serial maps."""
    return {key: max(left[key], right[key]) for key in left}


def parse_counter(text: str) -> Sequence[tuple[str, str]]:
    """Parse `kind=n` lines; unknown or malformed lines are ignored."""
    rows: list[tuple[str, str]] = []
    line: str
    for line in text.splitlines():
        found: re.Match[str] | None = COUNTER_RE.match(line.strip())
        if found is not None:
            rows.append((found.group(1), found.group(2)))
    return tuple(rows)


def read_counter(path: Path) -> CounterMap:
    """Counter map from disk; a missing file seeds to empty."""
    if not path.is_file():
        return {}
    try:
        text: str = path.read_text(encoding="utf-8")
    except OSError:
        return {}
    return {key: int(value) for key, value in parse_counter(text)}


def write_counter(path: Path, values: CounterMap) -> None:
    """Write the counter file with stable key order."""
    path.parent.mkdir(parents=True, exist_ok=True)
    lines: str = "".join(key + "=" + str(values[key]) + "\n" for key in sorted(values))
    path.write_text(lines, encoding="utf-8")


def next_serial(repo: Repo, kind: SpecKind) -> Serial:
    """Highest serial used anywhere in this repository, plus one."""
    highest: KindMax = merge_max(ref_serials(repo), tree_serials(tree_names(repo, kind), kind))
    other: SpecKind = "feature" if kind == "bug" else "bug"
    highest = merge_max(highest, tree_serials(tree_names(repo, other), other))
    return highest[kind] + 1


def render_id(n: Serial, kind: SpecKind, prefix: str | None) -> str:
    """Render the spec id for this run."""
    lead: str = prefix if prefix else ("feat-" if kind == "feature" else BUG_PREFIX)
    return lead + str(n) + "-"


def check(repo: Repo) -> Findings:
    """Collect every numbering violation in the working tree."""
    findings: Findings = []
    kinds: tuple[str, str] = ("bug", "feature")
    kind: SpecKind
    for kind in kinds:
        names: Sequence[str] = tree_names(repo, kind)
        pattern: re.Pattern[str] = BUG_RE if kind == "bug" else FEATURE_RE
        seen: dict[Serial, str] = {}
        name: str
        for name in names:
            found: re.Match[str] | None = pattern.match(name)
            if found is None:
                continue
            serial: Serial = int(found.group(1))
            if serial in seen:
                findings.append("duplicate " + kind + " serial " + str(serial) + ": " + seen[serial] + " and " + name)
            else:
                seen[serial] = name
    stored: CounterMap = read_counter(repo.counter)
    kind = "bug"
    for kind in kinds:
        if kind in stored:
            behind: Serial = next_serial(repo, kind) - 1
            if stored[kind] < behind:
                findings.append("counter " + kind + "=" + str(stored[kind]) + " is behind serial " + str(behind))
    roots: tuple[str, str] = (".LLMDD/DTRS", ".LLMDD/ITRS")
    root: str
    for root in roots:
        directory: Path = repo.root / root
        if not directory.is_dir():
            continue
        entry: Path
        for entry in sorted(directory.iterdir()):
            if not entry.is_dir():
                continue
            if entry.name in SKIP_DIRS:
                continue
            spec: Path = repo.root / ".LLMDD/spec-driven/features" / (entry.name + ".md")
            bug_spec: Path = repo.root / ".LLMDD/spec-driven/bugs" / (entry.name + ".md")
            if not spec.is_file() and not bug_spec.is_file():
                findings.append("orphan artifact " + root + "/" + entry.name + " has no matching spec file")
    return findings


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser: argparse.ArgumentParser = argparse.ArgumentParser(
        description="Allocate the next bug or feature id for this repository."
    )
    parser.add_argument("--kind", choices=["bug", "feature"], help="Which sequence to draw from.")
    parser.add_argument("--dry-run", action="store_true", help="Print the id without writing the counter.")
    parser.add_argument("--prefix", help="Id prefix for this run, for example bugfix-feature-.")
    parser.add_argument("--check", action="store_true", help="Report numbering violations and exit non-zero.")
    args: argparse.Namespace = parser.parse_args(argv)
    return args


def main(argv: list[str] | None = None) -> int:
    args: argparse.Namespace = parse_args(argv)
    repo: Repo = Repo(root=repo_root())
    if args.check:
        findings: Findings = check(repo)
        if findings:
            for finding in findings:
                sys.stderr.write("VIOLATION: " + finding + "\n")
            return 1
        return 0
    kind: SpecKind = str(args.kind)
    if args.kind is None:
        sys.stderr.write("usage: --kind bug|feature is required unless --check is given\n")
        return 2
    prefix: str | None = args.prefix
    n: Serial = next_serial(repo, kind)
    stored: CounterMap = read_counter(repo.counter)
    if args.dry_run:
        sys.stdout.write(render_id(n, kind, prefix) + "\n")
        return 0
    values: CounterMap = dict(stored)
    values[kind] = max(n, values.get(kind, 0))
    write_counter(repo.counter, values)
    sys.stdout.write(render_id(n, kind, prefix) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
