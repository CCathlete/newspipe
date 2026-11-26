"""Regression tests for bug-14: lead repairs must not fail a wave."""
import importlib.util
import json
import sys
import tempfile
import types
import unittest
from pathlib import Path
from typing import Callable, Sequence, cast

root: Path = Path(__file__).resolve().parents[3]
tools: Path = root / ".LLMDD" / "tools"
if str(tools) not in sys.path:
    sys.path.insert(0, str(tools))


def load_runner() -> types.ModuleType:
    spec: "importlib.util.ModuleSpec | None" = importlib.util.spec_from_file_location(
        "llmdd_run_waves_regression", tools / "run-waves.py")
    if spec is None or spec.loader is None:
        raise RuntimeError("cannot load run-waves.py")
    module: types.ModuleType = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


runner: types.ModuleType = load_runner()
final_status: "Callable[[Sequence[dict[str, object]]], str]" = cast(
    "Callable[[Sequence[dict[str, object]]], str]", runner.final_status)


def wave_with(coder_status: int, lead_status: int, escalations: int,
              lead_repairs: list[str]) -> "dict[str, object]":
    record: "dict[str, object]" = {
        "wave": 1, "cus": ["cu-001"], "escalations": escalations,
        "coder_status": coder_status, "lead_status": lead_status,
        "lead_repairs": lead_repairs,
        "files_changed": {}, "credits": {},
    }
    return record


class ConvergenceStatusTest(unittest.TestCase):
    def test_bug14_lead_repair_converges(self) -> None:
        waves: "list[dict[str, object]]" = [wave_with(
            0, 0, 0, ["hypothesis-engine/src/main/scala/hypothesisengine/infrastructure/SqliteLedger.scala"])]
        self.assertEqual(final_status(waves), "CONVERGED")

    def test_real_coder_failure_still_fails(self) -> None:
        waves: "list[dict[str, object]]" = [wave_with(1, 0, 0, [])]
        self.assertEqual(final_status(waves), "NEEDS_ATTENTION")

    def test_real_escalation_still_fails(self) -> None:
        waves: "list[dict[str, object]]" = [wave_with(0, 0, 1, [])]
        self.assertEqual(final_status(waves), "NEEDS_ATTENTION")

    def test_lead_process_failure_still_fails(self) -> None:
        waves: "list[dict[str, object]]" = [wave_with(0, 1, 0, [])]
        self.assertEqual(final_status(waves), "NEEDS_ATTENTION")

    def test_update_convergence_records_repairs_without_failing(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path: Path = Path(tmp) / "c.json"
            runner.update_convergence(path, 1, ["cu-001"], 0, True, True,
                                      ["src/main/example.py"], {}, {})
            data: "dict[str, object]" = json.loads(path.read_text())
            waves: "list[dict[str, object]]" = cast(
                "list[dict[str, object]]", data["waves"])
            record: "dict[str, object]" = waves[0]
            self.assertEqual(record["coder_status"], 0)
            self.assertEqual(record["lead_status"], 0)
            self.assertEqual(record["lead_repairs"], ["src/main/example.py"])

    def test_lead_branch_no_longer_flips_all_passed(self) -> None:
        source: str = (tools / "run-waves.py").read_text()
        start: int = source.find("if lead_breaches:")
        self.assertGreaterEqual(start, 0)
        self.assertNotIn("all_passed = False", source[start:start + 400])


if __name__ == "__main__":
    unittest.main()