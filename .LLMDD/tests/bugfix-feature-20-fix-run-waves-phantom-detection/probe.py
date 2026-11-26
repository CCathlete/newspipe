"""Probes for bugfix-feature-20.

Keys "1".."6" are the baseline hypothesis-gate probes (all true before the fix,
false after it). Keys "t1".."t5" are the post-fix acceptance checks named by the
feature spec (false before the fix, true after it).
"""
import importlib.util
import json
import sys
import types
from typing import Callable, Sequence, cast
from pathlib import Path

root: Path = Path(__file__).resolve().parents[3]
runner_path: Path = root / ".LLMDD/tools/run-waves.py"
runner: str = runner_path.read_text()
fix_convergence: Path = (
    root / ".LLMDD/ITRS/feat-19-weight-evidence-by-coverage-fix/repo.convergence.json"
)
test_path: Path = (
    root / ".LLMDD/tests/bugfix-feature-20-fix-run-waves-phantom-detection/"
    "test_convergence_status.py"
)


def _load_runner() -> "types.ModuleType | None":
    tools: Path = root / ".LLMDD" / "tools"
    if str(tools) not in sys.path:
        sys.path.insert(0, str(tools))
    spec: "importlib.util.ModuleSpec | None" = importlib.util.spec_from_file_location(
        "llmdd_run_waves_acceptance", runner_path)
    if spec is None or spec.loader is None:
        return None
    module: types.ModuleType = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


# ── baseline gate probes ────────────────────────────────────────────

def lead_branch_flips_all_passed() -> bool:
    marker: str = "if lead_breaches:"
    start: int = runner.find(marker)
    if start < 0:
        return False
    return "all_passed = False" in runner[start:start + 400]


def artifact_shows_phantom() -> bool:
    if not fix_convergence.is_file():
        return False
    text: str = fix_convergence.read_text()
    return (
        '"status": "NEEDS_ATTENTION"' in text
        and '"coder_status": 1' in text
        and '"lead_status": 0' in text
        and '"escalations": 0' in text
    )


checks: dict[str, tuple[bool, str]] = {
    "1": (lead_branch_flips_all_passed(),
          "lead boundary branch folds a repair into all_passed (=> coder_status)"),
    "2": ("escalations, all_passed, lead_ok," in runner,
          "update_convergence receives all_passed as the coder_ok argument"),
    "3": ("lead_repairs" not in runner,
          "no lead_repairs field exists to record repairs distinctly"),
    "4": (not any(("def " + name) in runner
                  for name in ("final_status", "compute_status", "wave_status")),
          "final-status decision is not a pure, testable function"),
    "5": (artifact_shows_phantom(),
          "bug-14 artifact: NEEDS_ATTENTION with coder=1 lead=0 esc=0 (reproducible)"),
    "6": (not test_path.exists(),
          "no convergence-status regression test yet (creation coordinate)"),
}


# ── post-fix acceptance probes ──────────────────────────────────────

def _lead_branch_clean() -> bool:
    marker: str = "if lead_breaches:"
    start: int = runner.find(marker)
    return start >= 0 and "all_passed = False" not in runner[start:start + 400]


def _final_status_of(coder: int, lead: int, escalations: int) -> "str | None":
    module: "types.ModuleType | None" = _load_runner()
    if module is None:
        return None
    fn: "Callable[[Sequence[dict[str, object]]], str] | None" = getattr(
        module, "final_status", None)
    if fn is None:
        return None
    wave: "dict[str, object]" = {
        "wave": 1, "cus": ["cu-001"], "escalations": escalations,
        "coder_status": coder, "lead_status": lead,
        "lead_repairs": ["src/main/example.py"],
        "files_changed": {}, "credits": {},
    }
    return cast(str, fn([wave]))


checks["t1"] = (_lead_branch_clean(),
                "post-fix: lead boundary branch records repairs without flipping all_passed")
checks["t2"] = ("lead_repairs" in runner and '"lead_repairs":' in runner,
                "post-fix: lead_repairs is written into the wave record")
checks["t3"] = ("def final_status(" in runner,
                "post-fix: final_status is a pure, testable function")
checks["t4"] = (_final_status_of(0, 0, 0) == "CONVERGED",
                "post-fix: bug-14 wave (coder=0 lead=0 esc=0 + lead repair) converges")
checks["t5"] = (_final_status_of(1, 0, 0) == "NEEDS_ATTENTION",
                "post-fix: a real coder failure still yields NEEDS_ATTENTION")


def emit(passed: bool, detail: str) -> None:
    print(json.dumps({"passed": passed, "detail": detail}))


def main_probe() -> None:
    key: str = sys.argv[1]
    passed: bool
    detail: str
    passed, detail = checks[key]
    emit(passed, detail)


if __name__ == "__main__":
    main_probe()
