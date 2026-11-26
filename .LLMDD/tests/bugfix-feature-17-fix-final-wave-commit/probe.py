"""Bugfix-17 prep probe: defect is located at the exact DTR coordinate in the baseline"""
import json
from pathlib import Path

_root: Path = Path(__file__).resolve().parents[3]
_src: str = (_root / ".LLMDD" / "tools" / "run-waves.py").read_text()


def _label_commits(labels: list[int]) -> list[int]:
    last: int = labels[-1]
    return [w for w in labels if w == last]


def _count_commits(labels: list[int]) -> list[int]:
    n: int = len(labels)
    return [w for w in labels if w == n]


def _emit(passed: bool, detail: str) -> None:
    print(json.dumps({"passed": passed, "detail": detail}))

_has_bug: bool = "is_final: bool = (wave == num_waves)" in _src
_has_count: bool = "num_waves: int = len(waves)" in _src
_emit(_has_bug and _has_count,
      "defect_present=" + str(_has_bug) + " count_binding=" + str(_has_count))
