"""Regression suite for CU boundary measurement and enforcement."""
import importlib.util
import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path
from types import ModuleType
from typing import Sequence

import unittest

ROOT: Path = Path(__file__).resolve().parents[2]
TOOLS: Path = ROOT / ".LLMDD" / "tools"
sys.path.insert(0, str(TOOLS))

from cu_scope import (  # noqa: E402
    has_enforceable_patterns, is_touched, parse_no_touch, resolve_patterns,
    write_paths)

RUNNER_PATH: Path = TOOLS / "run-waves.py"
_SPEC = importlib.util.spec_from_file_location("run_waves_tool", RUNNER_PATH)
if _SPEC is None or _SPEC.loader is None:
    raise RuntimeError("cannot load the wave runner module")
RUNNER: ModuleType = importlib.util.module_from_spec(_SPEC)
sys.modules["run_waves_tool"] = RUNNER
_SPEC.loader.exec_module(RUNNER)


def _event(tool: str, payload: dict[str, str]) -> str:
    """Render one tool_use line shaped like the opencode JSON stream."""
    part: dict[str, object] = {
        "type": "tool", "tool": tool, "state": {"input": payload}}
    return json.dumps({"type": "tool_use", "part": part})


class TestScopeParsing(unittest.TestCase):
    def test_semicolon_list(self) -> None:
        parsed = parse_no_touch(
            "Do NOT touch: a/b.py; c/d.py\nStop when green.\n")
        self.assertEqual(parsed.patterns, ("a/b.py", "c/d.py"))
        self.assertEqual(parsed.unparsed, ())

    def test_wrapped_list(self) -> None:
        parsed = parse_no_touch(
            "Do NOT touch: .opencode/agents/verifier.md,\n"
            "        any Scala source, any Python tool.\n"
            "Stop when green.\n")
        self.assertIn("**/*.scala", parsed.patterns)
        self.assertIn(".LLMDD/tools/*.py", parsed.patterns)

    def test_directory_expands(self) -> None:
        parsed = parse_no_touch("Do NOT touch: .LLMDD/agents\nStop when x\n")
        self.assertEqual(parsed.patterns, (".LLMDD/agents/**",))
        self.assertTrue(is_touched(parsed.patterns, ".LLMDD/agents/coder.md"))

    def test_dot_slash_keeps_dotfile(self) -> None:
        parsed = parse_no_touch("Do NOT touch: ./.LLMDD/agents\nStop when x\n")
        self.assertEqual(parsed.patterns, (".LLMDD/agents/**",))

    def test_unparsed_is_surfaced(self) -> None:
        parsed = parse_no_touch(
            "Do NOT touch: the sys-prompt tensor, any Scala source\nStop when x\n")
        self.assertEqual(parsed.unparsed, ("the sys-prompt tensor",))
        self.assertIn("**/*.scala", parsed.patterns)

    def test_empty_is_unenforceable(self) -> None:
        self.assertFalse(has_enforceable_patterns(parse_no_touch("")))
        self.assertFalse(has_enforceable_patterns(parse_no_touch(
            "Do NOT touch: the sys-prompt tensor\nStop when x\n")))

    def test_star_does_not_cross_separator(self) -> None:
        parsed = parse_no_touch("Do NOT touch: any Python tool\nStop when x\n")
        self.assertTrue(is_touched(parsed.patterns, ".LLMDD/tools/x.py"))
        self.assertFalse(is_touched(parsed.patterns, ".LLMDD/tools/sub/x.py"))

    def test_symlink_resolution(self) -> None:
        parsed = parse_no_touch(
            "Do NOT touch: .opencode/agents/llmdd/coder.md\nStop when x\n")
        resolved: Sequence[str] = resolve_patterns(parsed.patterns, ROOT)
        self.assertIn(".LLMDD/agents/coder.md", resolved)

    def test_write_index_is_step_scoped(self) -> None:
        body = (
            "1. Add the helper to .LLMDD/tools/thing.py\n\n"
            "```python\nx = 1\n```\n\n"
            "Prove: `python3 -m py_compile .LLMDD/tools/other.py`\n\n"
            "2. Prove docs/notes.md is intact\n\n"
            "```sh\nls docs/notes.md\n```\n\n"
            "Prove: `ls docs/notes.md`\n"
            "Do NOT touch: .LLMDD/tools/guarded.py\n"
            "Stop when green.\n")
        found: Sequence[str] = write_paths(body)
        self.assertIn(".LLMDD/tools/thing.py", found)
        self.assertNotIn(".LLMDD/tools/other.py", found)
        self.assertNotIn(".LLMDD/tools/guarded.py", found)


class TestAttribution(unittest.TestCase):
    def test_coder_writes_reads_tool_events(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root: Path = Path(raw)
            log_dir: Path = root / "logs"
            log_dir.mkdir()
            lines: list[str] = [
                _event("write", {"path": str(root / ".LLMDD" / "tools" / "a.py")}),
                _event("edit", {"path": str(root / "src" / "b.py")}),
                _event("shell", {"command": "echo x > src/c.py"}),
                _event("read", {"path": str(root / "src" / "ignored.py")}),
                "not json at all",
            ]
            (log_dir / "cu-001.attempt-0.log").write_text("\n".join(lines) + "\n")
            writes = RUNNER.coder_writes("cu-001", log_dir, root)
            self.assertEqual(writes.cu_id, "cu-001")
            self.assertIn(".LLMDD/tools/a.py", writes.paths)
            self.assertIn("src/b.py", writes.paths)
            self.assertIn("src/c.py", writes.paths)
            self.assertNotIn("src/ignored.py", writes.paths)

    def test_coder_writes_reads_every_attempt(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root: Path = Path(raw)
            log_dir: Path = root / "logs"
            log_dir.mkdir()
            (log_dir / "cu-002.attempt-0.log").write_text(
                _event("write", {"path": str(root / "src" / "one.py")}) + "\n")
            (log_dir / "cu-002.attempt-1.log").write_text(
                _event("edit", {"path": str(root / "src" / "two.py")}) + "\n")
            writes = RUNNER.coder_writes("cu-002", log_dir, root)
            self.assertIn("src/one.py", writes.paths)
            self.assertIn("src/two.py", writes.paths)

    def test_attribute_writes_splits_claimed_and_residual(self) -> None:
        per_cu = [
            RUNNER.CoderWrites(cu_id="cu-001", paths=("src/a.py",)),
            RUNNER.CoderWrites(cu_id="cu-002", paths=("src/b.py",)),
        ]
        split = RUNNER.attribute_writes(
            ("src/a.py", "src/b.py", "src/c.py"), per_cu)
        self.assertEqual(split.unattributed, ("src/c.py",))

    def test_wave_writes_includes_untracked_and_drops_baseline(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root: Path = Path(raw)
            git: list[str] = ["git", "-C", str(root)]
            subprocess.run(git + ["init", "-q"], check=True)
            (root / "tracked.txt").write_text("one\n")
            subprocess.run(git + ["add", "tracked.txt"], check=True)
            subprocess.run(
                git + ["-c", "user.email=a@b.c", "-c", "user.name=t",
                       "commit", "-q", "-m", "base"], check=True)
            (root / "tracked.txt").write_text("two\n")
            (root / "created.txt").write_text("new\n")
            found: Sequence[str] = RUNNER.wave_writes(
                root, "HEAD", frozenset({"dirty.txt"}))
            self.assertIn("tracked.txt", found)
            self.assertIn("created.txt", found)
            filtered: Sequence[str] = RUNNER.wave_writes(
                root, "HEAD", frozenset({"tracked.txt"}))
            self.assertNotIn("tracked.txt", filtered)
            self.assertIn("created.txt", filtered)


class TestBoundaryRules(unittest.TestCase):
    def _guards(self) -> dict[str, object]:
        return {
            "cu-001": parse_no_touch(
                "Do NOT touch: .LLMDD/agents\nStop when x\n"),
            "cu-002": parse_no_touch(
                "Do NOT touch: src/other.py\nStop when x\n"),
        }

    def test_own_no_touch_breach(self) -> None:
        writes = RUNNER.WaveWrites(
            per_cu=(RUNNER.CoderWrites(
                cu_id="cu-001", paths=(".LLMDD/agents/coder.md",)),),
            unattributed=(),
        )
        breached = RUNNER.boundary_breaches(writes, self._guards())
        self.assertEqual(len(breached), 1)
        self.assertEqual(breached[0].rule, "no-touch")
        self.assertEqual(breached[0].path, ".LLMDD/agents/coder.md")

    def test_sibling_write_is_not_a_breach(self) -> None:
        writes = RUNNER.WaveWrites(
            per_cu=(RUNNER.CoderWrites(
                cu_id="cu-001", paths=("src/owned.py",)),),
            unattributed=(),
        )
        breached = RUNNER.boundary_breaches(writes, self._guards())
        self.assertEqual(breached, ())

    def test_shared_write_breach(self) -> None:
        writes = RUNNER.WaveWrites(
            per_cu=(
                RUNNER.CoderWrites(cu_id="cu-001", paths=("src/shared.py",)),
                RUNNER.CoderWrites(cu_id="cu-002", paths=("src/shared.py",)),
            ),
            unattributed=(),
        )
        breached = RUNNER.boundary_breaches(writes, self._guards())
        self.assertEqual(
            sorted(item.rule for item in breached),
            ["shared-write", "shared-write"])

    def test_unattributed_breach_names_the_guard(self) -> None:
        writes = RUNNER.WaveWrites(
            per_cu=(RUNNER.CoderWrites(cu_id="cu-001", paths=()),),
            unattributed=("src/other.py",),
        )
        breached = RUNNER.boundary_breaches(writes, self._guards())
        self.assertEqual(len(breached), 1)
        self.assertEqual(breached[0].scope, "wave")
        self.assertIn("cu-002", breached[0].rule)

    def test_unenforceable_fails_closed(self) -> None:
        writes = RUNNER.WaveWrites(
            per_cu=(RUNNER.CoderWrites(cu_id="cu-009", paths=("src/a.py",)),),
            unattributed=(),
        )
        breached = RUNNER.boundary_breaches(writes, {"cu-009": parse_no_touch("")})
        self.assertEqual(len(breached), 1)
        self.assertEqual(breached[0].rule, "unenforceable")


class TestCorrectionPass(unittest.TestCase):
    def test_breach_detail_is_scope_correct(self) -> None:
        breaches = (
            RUNNER.BoundaryBreach(scope="cu-001", path="src/other.py", rule="shared-write"),
            RUNNER.BoundaryBreach(scope="cu-002", path="src/shared.py", rule="shared-write"),
            RUNNER.BoundaryBreach(scope="cu-002", path=".LLMDD/agents/coder.md", rule="no-touch"),
        )
        detail1 = RUNNER._breach_detail_for_cu(breaches, "cu-001")
        self.assertIn("src/other.py", detail1)
        self.assertNotIn(".LLMDD/agents/coder.md", detail1)
        detail2 = RUNNER._breach_detail_for_cu(breaches, "cu-002")
        self.assertIn(".LLMDD/agents/coder.md", detail2)

    def test_correction_logs_are_attributed(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root: Path = Path(raw)
            log_dir: Path = root / "logs"
            log_dir.mkdir()
            (log_dir / "cu-001.attempt-0.log").write_text(_event("write", {"path": str(root / "src" / "a.py")}) + "\n")
            (log_dir / "cu-001.correction-0.log").write_text(_event("edit", {"path": str(root / "src" / "b.py")}) + "\n")
            writes = RUNNER.coder_writes("cu-001", log_dir, root)
            self.assertIn("src/a.py", writes.paths)
            self.assertIn("src/b.py", writes.paths)


class TestCUProductionGate(unittest.TestCase):
    """Hard gates: force a correct CU against DTR/seam/impl/tests/proof/tracking."""

    def setUp(self) -> None:
        if os.environ.get("CU_BOUNDARY_SKIP_GATE") == "1":
            self.skipTest("gate skipped")
        self.root = ROOT
        self.itr_root = self.root / ".LLMDD" / "ITRS" / "feat-9-enforce-cu-boundary-redirect"
        self.batch_path = self.root / ".LLMDD" / "ITRS" / "feat-9-enforce-cu-boundary-redirect.batch.json"
        self.dtr_path = self.root / ".LLMDD" / "DTRS" / "feat-9-enforce-cu-boundary-redirect" / "repo.dtr"
        self.spec_path = self.root / ".LLMDD" / "spec-driven" / "features" / "feat-9-enforce-cu-boundary-redirect.md"

    def _load_batch(self) -> list[dict]:
        data = json.loads(self.batch_path.read_text())
        self.assertIsInstance(data, list)
        return data

    def _cu_content(self, batch: list[dict], cu_id: str) -> str:
        for c in batch:
            if c.get("cu-id") == cu_id:
                self.assertEqual(c.get("cu-type"), "regular")
                coords = c.get("dtr-coordinates", [])
                self.assertIsInstance(coords, list)
                self.assertGreater(len(coords), 0)
                content = c.get("content", "")
                self.assertIsInstance(content, str)
                self.assertGreater(len(content), 200)
                return content
        self.fail(f"missing CU {cu_id}")

    def test_exact_dtr_coordinates_point_to_updated_dtr(self) -> None:
        batch = self._load_batch()
        content1 = self._cu_content(batch, "cu-001")
        content2 = self._cu_content(batch, "cu-002")
        self.assertIn("FILE..LLMDD/tools/run-waves.py", content1)
        self.assertIn("CODEX..LLMDD/tools/run-waves.py=FUNCTION:def _breach_detail_for_cu", content1)
        self.assertIn("CODEX..LLMDD/tools/run-waves.py=FUNCTION:def run_per_cu_lead", content1)
        self.assertIn("CODEX..LLMDD/tools/run-waves.py=FUNCTION:def run_code_lead", content1)
        self.assertIn("CODEX..LLMDD/tools/run-waves.py=FUNCTION:async def run_wave_parallel", content1)
        self.assertIn("FILE..LLMDD/tests/test_cu_boundary.py", content2)
        self.assertTrue(self.dtr_path.is_file(), "DTR must exist for this feature")
        dtr = self.dtr_path.read_text()
        self.assertIn("FILE..LLMDD/tools/run-waves.py", dtr)
        self.assertIn("CODEX..LLMDD/tools/run-waves.py=FUNCTION:def _breach_detail_for_cu", dtr)
        self.assertIn("CODEX..LLMDD/tools/run-waves.py=FUNCTION:def run_per_cu_lead", dtr)
        self.assertIn("CODEX..LLMDD/tools/run-waves.py=FUNCTION:def run_code_lead", dtr)
        self.assertIn("CODEX..LLMDD/tools/run-waves.py=FUNCTION:async def run_wave_parallel", dtr)
        self.assertIn("FILE..LLMDD/tests/test_cu_boundary.py", dtr)
        mtime_cu = self.batch_path.stat().st_mtime
        mtime_dtr = self.dtr_path.stat().st_mtime
        self.assertGreaterEqual(mtime_dtr, mtime_cu, "DTR must be updated (>= batch) or rebuilt after coordinate changes")

    def test_correct_production_seam_and_complete_implementation(self) -> None:
        batch = self._load_batch()
        content1 = self._cu_content(batch, "cu-001")
        content2 = self._cu_content(batch, "cu-002")
        for required in ("IMPLEMENTATION_STEPS", "REQUIREMENTS", "COORDINATES", "ACCEPTANCE"):
            self.assertIn(required + ":", content1)
            self.assertIn(required + ":", content2)
        for fn in ("_breach_detail_for_cu", "_run_coder_correction", "run_wave_parallel", "run_per_cu_lead", "run_code_lead"):
            self.assertIn(fn, content1)
        self.assertIn("```python", content1)
        self.assertIn("```python", content2)
        self.assertIn("Prove:", content1)
        self.assertIn("Prove:", content2)
        self.assertIn("Do NOT touch:", content1)
        self.assertIn("Do NOT touch:", content2)
        self.assertIn("Stop when:", content1)
        self.assertIn("Stop when:", content2)
        self.assertIn("no try/except", content1.lower())
        self.assertIn("same model", content1.lower())
        self.assertIn("correction", content1.lower())
        self.assertIn("unattributed", content1.lower())

    def test_exact_test_behaviour_mapping_and_executable_proof(self) -> None:
        batch = self._load_batch()
        content2 = self._cu_content(batch, "cu-002")
        self.assertIn("TestCorrectionPass", content2)
        self.assertIn("test_breach_detail_is_scope_correct", content2)
        self.assertIn("test_correction_logs_are_attributed", content2)
        self.assertIn("python3 -m unittest", content2)
        self.assertIn("discover", content2)
        self.assertIn(".LLMDD/tests", content2)
        env = os.environ.copy()
        env["CU_BOUNDARY_SKIP_GATE"] = "1"
        proc = subprocess.run([sys.executable, "-m", "unittest", "discover", "-s", ".LLMDD/tests", "-p", "test_cu_boundary.py", "-v"], cwd=self.root, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, env=env)
        self.assertEqual(proc.returncode, 0, proc.stdout[-4000:].decode(errors="replace"))

    def test_guaranteed_tracked_final_source(self) -> None:
        proc = subprocess.run(["git", "-C", str(self.root), "ls-files", ".LLMDD/tests/test_cu_boundary.py", ".LLMDD/tools/run-waves.py", ".LLMDD/ITRS/feat-9-enforce-cu-boundary-redirect.batch.json", ".LLMDD/spec-driven/features/feat-9-enforce-cu-boundary-redirect.md"], stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        self.assertEqual(proc.returncode, 0)
        out = proc.stdout.decode()
        self.assertIn(".LLMDD/tests/test_cu_boundary.py", out)
        self.assertIn(".LLMDD/tools/run-waves.py", out)
        self.assertIn(".LLMDD/ITRS/feat-9-enforce-cu-boundary-redirect.batch.json", out)
        self.assertIn(".LLMDD/spec-driven/features/feat-9-enforce-cu-boundary-redirect.md", out)
        proc2 = subprocess.run(["git", "-C", str(self.root), "diff", "--name-only"], stdout=subprocess.PIPE)
        self.assertEqual(proc2.stdout.strip(), b"", "no unstaged changes")


if __name__ == "__main__":
    unittest.main()
