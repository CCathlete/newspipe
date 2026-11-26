"""Prep-runnable probes for feat-19. Each check confirms one baseline fact at
its declared DTR coordinate; the feature is the net-new work that changes it."""
import json
import sys
from pathlib import Path

root: Path = Path(__file__).resolve().parents[3]
examination: str = (root / "hypothesis-engine/src/main/scala/hypothesisengine/application/Examination.scala").read_text()
main: str = (root / "hypothesis-engine/src/main/scala/hypothesisengine/control/Main.scala").read_text()
model: str = (root / "hypothesis-engine/src/main/scala/hypothesisengine/domain/Model.scala").read_text()
ports: str = (root / "hypothesis-engine/src/main/scala/hypothesisengine/application/Ports.scala").read_text()
ledger: str = (root / "hypothesis-engine/src/main/scala/hypothesisengine/infrastructure/SqliteLedger.scala").read_text()
schema: str = (root / "hypothesis-engine/src/main/resources/ledger-schema.sql").read_text()

checks: dict[str, tuple[bool, str]] = {
    "1": ("BayesianEvaluator" in examination and "concentration" in examination,
          "probe-count Beta-Binomial evaluator present=" + str("concentration" in examination)),
    "2": ("hypothesis.coordinates.lift(index)" in examination,
          "positional coordinate fallback already used at baseline=" + str("hypothesis.coordinates.lift(index)" in examination)),
    "3": ("final case class ExperimentDefinition(" in model
          and "dtr_coordinates" not in model.split("final case class ExperimentDefinition(")[1].split("final case class")[0],
          "ExperimentDefinition lacks dtr_coordinates at baseline"),
    "4": ('args.length < 2' in main and '"init"' not in main and '"cleanup"' not in main
          and 'hypothesis-engine.ledger.path' in main,
          "Main is positional-only with no init/cleanup subcommand; ledger path is a sysprop"),
    "5": (all(name not in ports for name in ("def initialize(", "def deleteHypothesisData(", "def deleteAllData(")),
          "Ledger port lacks initialize/deleteHypothesisData/deleteAllData"),
    "6": (schema.count("CREATE TABLE") == 7 and "IF NOT EXISTS" in schema,
          "ledger schema has 7 idempotent CREATE TABLE IF NOT EXISTS statements=" + str(schema.count("CREATE TABLE"))),
    "7": ("override def resetStageData" in ledger and "resetStageData(hypothesisId: HypothesisId): Unit = ()" in ports,
          "resetStageData is a no-op at baseline (cleanup is net-new)"),
    "8": (all(field in main for field in ("probability_for_correctness", "\"posterior\"", "\"gap\"", "\"history\"")),
          "report JSON contract fields present in ReportJson encoder"),
    "9": ("ExaminationGate" in examination and "gateFloor" in examination and "0.85" in examination,
          "gate floor 0.85 and ExaminationGate present at baseline"),
}


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
