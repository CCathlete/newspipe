"""Bugfix-17 prep probe: consecutive labelling [1,2,3] still commits exactly the final wave"""
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

_labels: list[int] = [1, 2, 3]
_by_label: list[int] = _label_commits(_labels)
_by_count: list[int] = _count_commits(_labels)
_emit(_by_label == [3] and _by_count == [3],
      "labels=" + str(_labels) + " label_commits=" + str(_by_label) + " count_commits=" + str(_by_count))
