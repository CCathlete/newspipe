"""validate-steps.py — mechanical dummy-proof gate for CU steps.

Usage:
    python3 .LLMDD/tools/validate-steps.py <batch.json>
    python3 .LLMDD/tools/validate-steps.py <frame-root/>

Exits 0 when every regular CU passes, 1 otherwise. See
.LLMDD/skills/validate-implementation-steps/SKILL.md for the F-rules.
"""
import json
import re
import sys
from pathlib import Path

BANNED = ["handle", "extend", "improve", "enhance", "refactor",
          "wire up", "take care of", "manage"]
PLACEHOLDER = re.compile(r"TBD|TODO|FIXME|XXX|etc\.")
EG = re.compile(r"e\.g\.")
FENCE = re.compile(r"```(\w+)\n(.*?)```", re.S)
STEP = re.compile(r"(?m)^(\d+)\.\s")
PATH = re.compile(r"(src/|conf/|project/)[\w./-]+|\bbuild\.sbt\b")
PROVE = re.compile(r"Prove:\s*`([^`]+)`")


def check(cu_id: str, text: str) -> list[str]:
    fails: list[str] = []
    if "# IMPLEMENTATION_STEPS:" in text:
        body = text.split("# IMPLEMENTATION_STEPS:", 1)[-1]
    else:
        body = text
    parts = STEP.split(body)[1:]
    nums, chunks = parts[0::2], parts[1::2]
    if len(nums) < 1:
        fails.append("F1: no numbered steps")
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
    code_free = FENCE.sub("", body)
    if PLACEHOLDER.search(code_free) or EG.search(code_free):
        fails.append("F6: placeholder token outside code (TBD/TODO/FIXME/XXX/e.g./etc.)")
    if re.search(r"TODO|FIXME|XXX", body):
        fails.append("F6: TODO/FIXME/XXX token (even in code)")
    if any("…" in b or "..." in b for _, b in FENCE.findall(body)):
        fails.append("F2: fenced block contains …/... placeholder instead of exact code")
    return fails


def main() -> None:
    src = Path(sys.argv[1])
    targets: dict[str, str] = {}
    if src.suffix == ".json":
        for cu in json.loads(src.read_text()):
            if cu.get("cu-type", "regular") in ("arch", "legend"):
                continue
            content = cu.get("content", "")
            m = re.search(r"IMPLEMENTATION_STEPS:(.*?)ACCEPTANCE:", content, re.S)
            targets[cu["cu-id"]] = m.group(1) if m else ""
    else:
        for steps_file in sorted(src.glob("cu-*/IMPLEMENTATION_STEPS.itr")):
            targets[steps_file.parent.name] = steps_file.read_text()
    failed = False
    for cid, text in targets.items():
        errs = check(cid, text)
        print(("FAIL " if errs else "PASS ") + cid)
        for e in errs:
            print("  - " + e)
            failed = True
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
