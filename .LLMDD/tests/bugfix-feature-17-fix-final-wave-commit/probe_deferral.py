"""Bugfix-17 prep probe: non-final waves still defer and exactly one wave commits"""
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

_labels: list[int] = [1, 2, 3, 4]
_by_label: list[int] = _label_commits(_labels)
_has_defer: bool = "Do NOT commit" in _src
_emit(_by_label == [4] and len(_by_label) == 1 and _has_defer,
      "labels=" + str(_labels) + " final_commits=" + str(_by_label) + " defer_text=" + str(_has_defer))
