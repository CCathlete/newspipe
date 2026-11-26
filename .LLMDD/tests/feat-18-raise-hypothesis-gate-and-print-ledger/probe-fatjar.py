"""Prep-runnable probes for feat-18 Phase 2. Each check confirms one baseline
gap at its declared DTR coordinate; the fat-jar phase is the net-new work."""
import json
import sys
from pathlib import Path

root: Path = Path(__file__).resolve().parents[3]
plugins: str = (root / "hypothesis-engine/project/plugins.sbt").read_text()
build: str = (root / "hypothesis-engine/build.sbt").read_text()
main: str = (root / "hypothesis-engine/src/main/scala/hypothesisengine/control/Main.scala").read_text()
gitignore: str = (root / ".gitignore").read_text()
wf_dir: Path = root / ".github/workflows"
workflows: str = ""
if wf_dir.is_dir():
    workflows = "\n".join(p.read_text() for p in wf_dir.rglob("*") if p.is_file())

checks: dict[str, tuple[bool, str]] = {
    "1": ("sbt-assembly" not in plugins,
          "sbt-assembly absent from plugins.sbt=" + str("sbt-assembly" not in plugins)),
    "2": (("mainClass" not in build) and ("assemblyJarName" not in build),
          "mainClass/assemblyJarName absent from build.sbt=" + str("mainClass" not in build and "assemblyJarName" not in build)),
    "3": ("hypothesis-engine.ledger.path" not in main,
          "ledger-path property absent from Main.scala=" + str("hypothesis-engine.ledger.path" not in main)),
    "4": ("hypothesis-engine/hypothesis-engine" not in gitignore and "hypothesis-engine.jar" not in gitignore,
          "no jar ignore rule=" + str("hypothesis-engine.jar" not in gitignore)),
    "5": (("assembly" not in workflows) and ("hypothesis-engine" not in workflows),
          "no engine assembly/release workflow=" + str("assembly" not in workflows)),
    "6": (("assembly" not in build) and ("Compile / mainClass" not in build),
          "no assembly task or jar output in build.sbt=" + str("assembly" not in build)),
}

arg: str = sys.argv[1] if len(sys.argv) > 1 else "1"
passed, detail = checks[arg]
print(json.dumps({"passed": passed, "detail": "check=" + arg + " " + detail}))
