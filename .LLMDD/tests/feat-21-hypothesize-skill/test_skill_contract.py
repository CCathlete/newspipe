"""Contract tests for feat-21: the hypothesize skill and its delegation."""
import unittest
from pathlib import Path
from typing import Sequence

root: Path = Path(__file__).resolve().parents[3]
hyp: Path = root / ".LLMDD/skills/hypothesize/SKILL.md"
prep: Path = root / ".LLMDD/skills/prepare-implementation/SKILL.md"


def text_of(path: Path) -> str:
    return path.read_text(encoding="utf-8") if path.is_file() else ""


hyp_text: str = text_of(hyp)
prep_text: str = text_of(prep)

ENGINE_MARKERS: Sequence[str] = (
    "hypotheses.json", "experiments.jsonl", "cleanLedger",
    "runMain hypothesisengine.control.Main", "check-hypothesis-report.py",
    "hypothesis-conversation.md", "hypothesis-verification.txt",
    "GREEN_LIGHT", "RED_LIGHT", "ADVISOR_ACTION_REQUIRED",
)
DTR_MARKERS: Sequence[str] = (
    ".LLMDD/DTRS/<subject>/<app>.dtr", "META.GENERATOR=dtr-builder",
    "META.TIMESTAMP", "FILE.", "CODEX.", "TYPE.", "dtr_coordinates",
    "gate_probability_for_correctness",
)
MODE_MARKERS: Sequence[str] = (
    "name: hypothesize", "why is this not working", "is this suggested solution correct",
    "diagnosis", "validation", "GREEN_LIGHT", "## Stop boundary",
)
EXCLUSION_MARKERS: Sequence[str] = (
    "batch CU", "compile ITR", "run-waves.txt", "operational wave command",
)


class SkillContractTest(unittest.TestCase):
    def test_frontmatter_and_modes(self) -> None:
        for marker in MODE_MARKERS:
            with self.subTest(marker=marker):
                self.assertIn(marker, hyp_text)

    def test_canonical_engine_procedure_present(self) -> None:
        for marker in ENGINE_MARKERS:
            with self.subTest(marker=marker):
                self.assertIn(marker, hyp_text)

    def test_dtr_policy_present(self) -> None:
        for marker in DTR_MARKERS:
            with self.subTest(marker=marker):
                self.assertIn(marker, hyp_text)

    def test_stop_boundary_present(self) -> None:
        for marker in EXCLUSION_MARKERS:
            with self.subTest(marker=marker):
                self.assertIn(marker, hyp_text)

    def test_delegation_in_prepare_implementation(self) -> None:
        self.assertIn("@hypothesize", prep_text)
        self.assertIn("Validate the feature hypotheses", prep_text)

    def test_engine_body_not_duplicated(self) -> None:
        for marker in ("runMain hypothesisengine.control.Main", "cleanLedger", "experiments.jsonl"):
            with self.subTest(marker=marker):
                self.assertNotIn(marker, prep_text)

    def test_prepare_chain_steps_kept(self) -> None:
        for marker in ("Commit pending specs", "Build DTR", "Draft the batch CU", "Compile ITR"):
            with self.subTest(marker=marker):
                self.assertIn(marker, prep_text)


if __name__ == "__main__":
    unittest.main()