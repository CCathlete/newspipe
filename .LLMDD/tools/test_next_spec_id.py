"""Fixture test for next-spec-id.py (feat-6).

Builds a throwaway repository under /tmp/opencode with bug-5 and feat-5,
cuts two sibling branches, and asserts the allocator hands out different
ids. No pytest; test_synopsis_budget.py is the standalone-script precedent.
"""
from __future__ import annotations

import shutil
import subprocess
import sys
from pathlib import Path

KIND_BUG: str = "bug"
KIND_FEATURE: str = "feature"
COUNTER_REL: str = ".LLMDD/spec-driven/ids.txt"
EXIT_OK: int = 0
EXIT_VIOLATION: int = 1
BASE: str = "refs/heads/main"
FIRST: str = "refs/heads/branch-a"
SECOND: str = "refs/heads/branch-b"
DUPLICATE_SPEC: str = ".LLMDD/spec-driven/bugs/bug-5-shadow.md"


def run(command: list[str], cwd: Path) -> str:
    """Run one command and return stdout, letting a failure raise."""
    done: subprocess.CompletedProcess[str] = subprocess.run(
        command, cwd=cwd, capture_output=True, text=True, check=True
    )
    return done.stdout.strip()


def stage_allocator(repo: Path) -> Path:
    """Copy the allocator into the fixture repo so __file__ gives its root."""
    tools: Path = repo / ".LLMDD/tools"
    tools.mkdir(parents=True, exist_ok=True)
    source: Path = Path(__file__).resolve().parent / "next-spec-id.py"
    target: Path = tools / "next-spec-id.py"
    shutil.copyfile(source, target)
    return target


def allocator(repo: Path) -> list[str]:
    """The allocator under test, staged inside the fixture repository."""
    return [sys.executable, str(stage_allocator(repo))]


def git(repo: Path, *args: str) -> str:
    """Run one git command inside the fixture repository."""
    return run(["git", *args], repo)


def make_fixture(base: Path) -> Path:
    """Create the fixture repository and return its path."""
    root: Path = base / "spec-id-fixture"
    if root.exists():
        shutil.rmtree(root)
    root.mkdir(parents=True)
    git(root, "init", "--quiet", "--initial-branch=main")
    git(root, "config", "user.email", "fixture@example.com")
    git(root, "config", "user.name", "Fixture")
    bugs: Path = root / ".LLMDD/spec-driven/bugs"
    features: Path = root / ".LLMDD/spec-driven/features"
    bugs.mkdir(parents=True)
    features.mkdir(parents=True)
    (bugs / "bug-5-fixture.md").write_text("# bug-5: fixture\n", encoding="utf-8")
    (features / "feat-5-fixture.md").write_text("# feat-5: fixture\n", encoding="utf-8")
    git(root, "add", "-A")
    git(root, "commit", "--quiet", "-m", "fixture base")
    git(root, "branch", "branch-a")
    git(root, "branch", "branch-b")
    return root


def allocate(repo: Path, kind: str, ref: str) -> str:
    """Allocate one id while checked out at ref."""
    git(repo, "checkout", "--quiet", ref.replace("refs/heads/", ""))
    out: str = run([*allocator(repo), "--kind", kind], repo)
    return out


def expect(condition: bool, label: str) -> None:
    """Record one assertion, keeping all failures visible."""
    if not condition:
        raise AssertionError("failed: " + label)


def main() -> int:
    base: Path = Path("/tmp/opencode")
    repo: Path = make_fixture(base)
    first: str = allocate(repo, KIND_BUG, FIRST)
    expect(first.startswith("bug-"), "branch-a id carries the bug prefix, got " + first)
    git(repo, "add", "-A")
    git(repo, "commit", "--quiet", "-m", "bump for branch-a")
    second: str = allocate(repo, KIND_BUG, SECOND)
    git(repo, "checkout", "--quiet", "main")
    expect(second != first, "sibling branches share the id " + second)
    both: str = run([*allocator(repo), "--check"], repo)
    expect(both == "", "clean repository must report no finding, got " + both)
    shadow: Path = repo / DUPLICATE_SPEC
    shadow.write_text("# bug-5: shadow\n", encoding="utf-8")
    result: subprocess.CompletedProcess[str] = subprocess.run(
        [*allocator(repo), "--check"], cwd=repo, capture_output=True, text=True, check=False
    )
    expect(result.returncode == EXIT_VIOLATION, "duplicate serial must exit 1, got " + str(result.returncode))
    shadow.unlink()
    dry: str = run([*allocator(repo), "--kind", KIND_FEATURE, "--dry-run"], repo)
    expect(dry.startswith("feat-"), "dry-run prints a feature id, got " + dry)
    sys.stdout.write("PASS\n")
    return EXIT_OK


if __name__ == "__main__":
    raise SystemExit(main())
