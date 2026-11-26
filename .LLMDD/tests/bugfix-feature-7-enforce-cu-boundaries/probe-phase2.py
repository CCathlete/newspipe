"""Prep-runnable probes for bugfix-feature-7 Phase 2. Each check confirms one
baseline gap at its declared DTR coordinate; the prior-written exclusion is
net-new work."""
import json
import sys
from pathlib import Path

root: Path = Path(__file__).resolve().parents[3]
runner: str = (root / ".LLMDD/tools/run-waves.py").read_text()
test: str = (root / ".LLMDD/tests/test_cu_boundary.py").read_text()

checks: dict[str, tuple[bool, str]] = {
    "1": ("prior_written" not in runner.split("def wave_writes")[1].split("def attribute_writes")[0],
          "wave_writes has no prior_written param"),
    "2": ("prior_written" not in runner.split("def enforce_boundary")[1].split("def _paths_against")[0],
          "enforce_boundary has no prior_written param"),
    "3": ("prior_written" not in runner.split("async def run_wave_parallel")[1].split("async def run_per_cu_lead")[0],
          "run_wave_parallel has no prior_written param"),
    "4": ("frozenset(run_written)" not in runner,
          "main_async call site never passes frozenset(run_written)"),
    "5": (runner.count("run_written: set[str]") == 2,
          "redundant run_written re-annotation present (mypy no-redef)"),
    "6": ("prior_written" not in test,
          "test_cu_boundary.py has no prior_written regression"),
}

arg: str = sys.argv[1] if len(sys.argv) > 1 else "1"
passed, detail = checks[arg]
print(json.dumps({"passed": passed, "detail": "check=" + arg + " " + detail}))
