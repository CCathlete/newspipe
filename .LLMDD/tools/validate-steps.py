"""validate-steps.py — mechanical dummy-proof gate for CU steps.

Usage:
    python3 .LLMDD/tools/validate-steps.py <batch.json>
    python3 .LLMDD/tools/validate-steps.py <frame-root/>

Exits 0 when every regular CU passes, 1 otherwise. See
.LLMDD/skills/validate-implementation-steps/SKILL.md for the F-rules.

This tool itself complies with ARCH.PYTHON.*: every first binding carries
an explicit annotation, no typing.Any appears, and failures are returned
as a list (monadic style) instead of raised/caught with try-except.
"""
import json
import re
import sys
from pathlib import Path
from typing import Optional, Sequence, TypedDict

from fnmatch import fnmatch

from cu_scope import NoTouch, parse_no_touch, write_paths

class BatchCu(TypedDict, total=False):
    cu_id: str
    content: str
    cu_type: str


def _normalize_cu_keys(raw: object) -> BatchCu:
    """Map wire keys (cu-id) to identifier keys; ValueError on wrong-typed values."""
    if not isinstance(raw, dict):
        raise ValueError(f"CU item is not an object: {raw!r}")
    normalized: BatchCu = {}
    cu_id_raw: object = raw.get("cu-id", "")
    if not isinstance(cu_id_raw, str):
        raise ValueError(f"CU item has non-string cu-id: {cu_id_raw!r}")
    normalized["cu_id"] = cu_id_raw
    content_raw: object = raw.get("content", "")
    if not isinstance(content_raw, str):
        raise ValueError(f"CU item has non-string content: {content_raw!r}")
    normalized["content"] = content_raw
    cu_type_raw: object = raw.get("cu-type", "regular")
    if not isinstance(cu_type_raw, str):
        raise ValueError(f"CU item has non-string cu-type: {cu_type_raw!r}")
    normalized["cu_type"] = cu_type_raw
    return normalized

BANNED: list[str] = ["handle", "extend", "improve", "enhance", "refactor",
          "wire up", "take care of", "manage"]
PLACEHOLDER: re.Pattern[str] = re.compile(r"TBD|TODO|FIXME|XXX|etc\.")
EG: re.Pattern[str] = re.compile(r"e\.g\.")
FENCE: re.Pattern[str] = re.compile(r"```(\w+)\n(.*?)```", re.S)
STEP: re.Pattern[str] = re.compile(r"(?m)^(\d+)\.\s")
PATH: re.Pattern[str] = re.compile(r"(src/|conf/|project/|\.LLMDD/|itr-buffer/|dtr-builder/|itr-compiler/|commit-tool/|scripts/|docs/)[\w./-]+|\bbuild\.sbt\b|\bREADME\.md\b")
PROVE: re.Pattern[str] = re.compile(r"Prove:\s*`([^`]+)`")


def check(cu_id: str, text: str) -> list[str]:
    fails: list[str] = []
    body: str
    if "# IMPLEMENTATION_STEPS:" in text:
        body = text.split("# IMPLEMENTATION_STEPS:", 1)[-1]
    else:
        body = text
    parts: list[str] = STEP.split(body)[1:]
    nums: list[str] = parts[0::2]
    chunks: list[str] = parts[1::2]
    if len(nums) < 1:
        fails.append("F1: no numbered steps")
    n: str
    ch: str
    verb: str
    for n, ch in zip(nums, chunks):
        if not PATH.search(ch):
            fails.append(f"F1: step {n} has no exact file path")
        if re.search(r"[Cc]reate|[Cc]hange|[Aa]dd|[Ww]rite", ch) and "```" not in ch:
            fails.append(f"F2: step {n} changes a file without a fenced code block")
        for verb in BANNED:
            if re.search(rf"\b{verb}\b", ch, re.I) and not ("```" in ch and PATH.search(ch)):
                fails.append(f"F5: step {n} uses vague verb '{verb}' without fence+path")
    if len(PROVE.findall(body)) < len(nums):
        fails.append(f"F3: {len(PROVE.findall(body))} Prove lines for {len(nums)} steps")
    if "Do NOT touch:" not in body:
        fails.append("F4: missing Do NOT touch list")
    if "Stop when" not in body:
        fails.append("F4: missing Stop when condition")
    declared: NoTouch = parse_no_touch(body)
    if not declared.entries:
        fails.append("F7: no-touch list is absent or empty")
    bad: str
    for bad in declared.unparsed:
        fails.append(f"F7: no-touch entry is neither a path nor a known clause: {bad}")
    owned: Sequence[str] = write_paths(body)
    guarded: str
    for guarded in declared.patterns:
        clash: str = next((w for w in owned if fnmatch(w, guarded)), "")
        if clash:
            fails.append(f"F7: no-touch list contains this CU own write target: {clash}")
    code_free: str = FENCE.sub("", body)
    if PLACEHOLDER.search(code_free) or EG.search(code_free):
        fails.append("F6: placeholder token outside code (TBD/TODO/FIXME/XXX/e.g./etc.)")
    if re.search(r"TODO|FIXME|XXX", body):
        fails.append("F6: TODO/FIXME/XXX token (even in code)")
    fence_pair: tuple[str, str]
    fence_text: str = ""
    placeholder_in_fence: bool = False
    for fence_pair in FENCE.findall(body):
        fence_text = fence_pair[1]
        if "\u2026" in fence_text or "..." in fence_text:
            placeholder_in_fence = True
    if placeholder_in_fence:
        fails.append("F2: fenced block contains \u2026/... placeholder instead of exact code")
    return fails


def main() -> None:
    src: Path = Path(sys.argv[1])
    targets: dict[str, str] = {}
    raw: str
    parsed: list[BatchCu] = []
    cu: BatchCu
    content: str
    cu_id: str
    m: Optional[re.Match[str]]
    steps_file: Path
    if src.suffix == ".json":
        raw = src.read_text()
        loaded: object = json.loads(raw)
        items: list[BatchCu] = []
        raw_items: list[object] = []
        if isinstance(loaded, dict):
            cus_raw: object = loaded.get("cus", [])
            if isinstance(cus_raw, list):
                raw_items = cus_raw
        elif isinstance(loaded, list):
            raw_items = loaded
        item: object
        for item in raw_items:
            items.append(_normalize_cu_keys(item))
        for cu in items:
            if cu.get("cu_type", "regular") in ("arch", "legend"):
                continue
            content = cu.get("content", "")
            m = re.search(r"IMPLEMENTATION_STEPS:(.*?)ACCEPTANCE:", content, re.S)
            cu_id = cu.get("cu_id", "")
            targets[cu_id] = m.group(1) if m else ""
    else:
        for steps_file in sorted(src.glob("cu-*/IMPLEMENTATION_STEPS.itr")):
            targets[steps_file.parent.name] = steps_file.read_text()
    failed: bool = False
    cid: str
    text: str
    errs: list[str] = []
    e: str
    for cid, text in targets.items():
        errs = check(cid, text)
        print(("FAIL " if errs else "PASS ") + cid)
        for e in errs:
            print("  - " + e)
            failed = True
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
