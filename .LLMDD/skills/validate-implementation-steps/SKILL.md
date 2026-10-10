---
name: validate-implementation-steps
description: Validate that a CU's IMPLEMENTATION_STEPS are dummy-proof — exact file paths, injectable fenced code, proving commands, no-touch list, stop condition, no vague prose. Use on batch CU files or compiled cu folders before handing frames to a Coder; fail the batch when any check fails.
---

# validate-implementation-steps

A Coder must be able to implement from the steps without asking
questions. This skill is the gate: mechanical checks (runnable script
below) plus a judgment pass. Both must be green.

## When to use

- After drafting or recompiling a batch CU file, before STEP5.
- After any hand-fix of `IMPLEMENTATION_STEPS.itr` files.
- In code-lead review when a Coder escalates with questions — the steps
  failed this gate.

## The dummy-proof bar

Every regular CU's steps must contain:

1. **Numbered steps** (`1.`, `2.`, …), each naming at least one exact file
   path (`src/main/scala/…`, `src/test/…`, `conf/…`, `project/…`,
   `build.sbt`).
2. **Injectable code**: every step that creates or changes a file carries
   a complete fenced block (```scala / ```hocon / ```xml / ```properties /
   ```text) with exact content — signatures, imports, literals, versions.
   Sketches with `…` placeholders fail.
3. **Proving command per step**: a `Prove: \`…\`` line with the exact
   command (`sbt compile`, `sbt "testOnly …"`, `cat …`) and what it prints.
4. **Do-NOT-touch list**: an explicit `Do NOT touch:` line naming
   neighboring modules/files the CU must leave alone.
5. **Stop condition**: a `Stop when …` line stating exactly when the CU is
   done and what belongs to other CUs.
6. **No vague verbs**: `handle`, `extend`, `improve`, `enhance`,
   `refactor`, `wire up`, `take care of`, `manage` fail unless the same
   step also contains a fenced code block AND an exact file path.
7. **No placeholders**: `TBD`, `TODO`, `FIXME`, `XXX`, `e.g.`, `etc.`
   fail unconditionally — write the concrete value instead.

## Mechanical check

Save as `.LLMDD/tools/validate-steps.py` (or run inline) and execute
against the batch file or a frame root:

```sh
python3 .LLMDD/tools/validate-steps.py .LLMDD/ITRS/<batch>.batch.json
python3 .LLMDD/tools/validate-steps.py .LLMDD/ITRS/<feature-name>/
```

Checker rules (FAIL = batch rejected, fix and recompile):

- F1: no numbered steps, or any step without an exact
  `src/|conf/|project/` path or root `build.sbt`.
- F2: number of fenced code blocks < number of file-creating steps
  (a step creating/changing a file without a fence fails).
- F3: number of `Prove: \`…\`` lines < number of numbered steps.
- F4: missing `Do NOT touch:` line or missing `Stop when` line.
- F5: banned vague verb on a line whose step chunk has no fence+path.
- F6: any `TBD|TODO|FIXME|XXX|e.g.|etc.` token (code fences excluded
  from this rule except for `TODO|FIXME|XXX`, which fail everywhere).

Reference implementation (keep in sync with the rules above):

```python
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
```

## Judgment pass (human/agent review)

The script cannot judge everything. The reviewer additionally confirms:

- J1: pasted code would compile — imports resolve, package names match
  paths, no undefined symbols across the CU's own files.
- J2: versions, model names, ports, column names are literal values, not
  "latest" or "appropriate".
- J3: cross-CU contracts name the exact API (e.g. `BronzeUnit.idFor`
  public in cu-001, consumed in cu-005) and dependency order is stated.
- J4: each `Prove:` command's expected output is stated, not just the command.

## Rules

- FAIL on any F-rule or J-rule violation: fix the batch file and
  recompile with `--force` (never hand-patch frames to pass the gate).
- Keep this skill's checker and the F-rules in sync — editing one means
  editing the other.
- Record the validation run (command + PASS list) in the CU feedback or
  commit message.

## Output

A PASS verdict for every regular CU, qualifying the frames for STEP5.
