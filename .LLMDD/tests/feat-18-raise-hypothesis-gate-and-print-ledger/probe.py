"""Prep-runnable probes for feat-18. Each check confirms one baseline defect at
its declared DTR coordinate; the feature is the net-new work that removes it."""
import json
import sys
from pathlib import Path

root: Path = Path(__file__).resolve().parents[3]
skill: str = (root / ".LLMDD/skills/prepare-implementation/SKILL.md").read_text()
examination: str = (root / "hypothesis-engine/src/main/scala/hypothesisengine/application/Examination.scala").read_text()
model: str = (root / "hypothesis-engine/src/main/scala/hypothesisengine/domain/Model.scala").read_text()
schema: str = (root / "hypothesis-engine/src/main/resources/ledger-schema.sql").read_text()
main: str = (root / "hypothesis-engine/src/main/scala/hypothesisengine/control/Main.scala").read_text()
checker_exists: bool = (root / ".LLMDD/tools/check-hypothesis-report.py").exists()

checks: dict[str, tuple[bool, str]] = {
    "1": ('"gate_probability_for_correctness": 0.6' in skill,
          'authoring default 0.6 present=' + str('"gate_probability_for_correctness": 0.6' in skill)),
    "2": (("0.85" not in examination) and ("floor" not in examination.lower())
          and ("hypothesis.gate" in examination),
          'no 0.85 floor and gate read from hypothesis=' + str("hypothesis.gate" in examination)),
    "3": (("justification" not in model) and ("justification" not in schema),
          'justification absent from domain and schema=' + str("justification" not in model and "justification" not in schema)),
    "4": (("LedgerStageReport" not in main) and ("ledger-report" not in main)
          and ("GREEN_LIGHT" in main),
          'one-line verdict only, no report=' + str("GREEN_LIGHT" in main and "LedgerStageReport" not in main)),
    "5": (not checker_exists, 'checker tool absent=' + str(not checker_exists)),
}

arg: str = sys.argv[1] if len(sys.argv) > 1 else "1"
passed, detail = checks[arg]
print(json.dumps({"passed": passed, "detail": "check=" + arg + " " + detail}))
