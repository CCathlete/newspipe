"""Baseline probes for feat-21: all keys are true before the hypothesize skill exists."""
import json
import sys
from pathlib import Path

root: Path = Path(__file__).resolve().parents[3]
prep: Path = root / ".LLMDD/skills/prepare-implementation/SKILL.md"
new_skill: Path = root / ".LLMDD/skills/hypothesize/SKILL.md"
checker: Path = root / ".LLMDD/tools/check-hypothesis-report.py"
engine_main: Path = root / "hypothesis-engine/src/main/scala/hypothesisengine/control/Main.scala"
spec: Path = root / ".LLMDD/spec-driven/features/feat-21-hypothesize-skill.md"
skills_link: Path = root / ".opencode/skills"

checks: dict[str, tuple[bool, str]] = {
    "1": (not new_skill.exists(),
          "creation coordinate: hypothesize/SKILL.md does not exist yet"),
    "2": (prep.is_file() and all(s in prep.read_text() for s in (
          "runMain hypothesisengine.control.Main", "cleanLedger",
          "check-hypothesis-report.py", "GREEN_LIGHT", "hypothesis-verification.txt")),
          "prepare-implementation step 3 still embeds the full engine procedure"),
    "3": (checker.is_file() and all(s in checker.read_text() for s in (
          "GREEN_LIGHT", "RED_LIGHT", "ADVISOR_ACTION_REQUIRED", "VERDICT_TO_LABEL")),
          "check-hypothesis-report.py owns the verbatim-echo gate and verdict labels"),
    "4": (engine_main.is_file() and "object Main" in engine_main.read_text()
          and "package hypothesisengine.control" in engine_main.read_text(),
          "hypothesis engine entrypoint present and unchanged"),
    "5": (skills_link.is_symlink() and skills_link.resolve().name == "skills",
          ".opencode/skills symlink makes new .LLMDD/skills/* auto-discoverable"),
    "6": (spec.is_file() and all(s in spec.read_text() for s in (
          "diagnosis", "validation", "GREEN_LIGHT", "stop boundary")),
          "feature spec declares both modes over one loop with a stop boundary"),
}

if __name__ == "__main__":
    key: str = sys.argv[1]
    passed: bool
    detail: str
    passed, detail = checks[key]
    print(json.dumps({"passed": passed, "detail": detail}))
