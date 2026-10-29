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
   (Repo-own work uses repo-rooted paths instead:
   `.LLMDD/…`, `itr-buffer/…`, `dtr-builder/…`, `itr-compiler/…`.)
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

Reference implementation (keep in sync with the rules above; itself complies with `ARCH.PYTHON.*` — explicit annotations, no `Any`, failures returned as a list):

```python
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
from typing import Optional, TypedDict

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
- J5: Python code (when `APP.LANGUAGE=Python`) obeys `ARCH.PYTHON.*` —
  every first binding carries an explicit annotation, no `typing.Any` /
  bare generics, slotted+frozen dataclasses for value objects, monadic
  error handling over `try-except`. Injectable Python fences violating
  `EXPLICIT_DECLARATION` or `NO_ANY` fail like any J-rule. Annotation
  presence alone does not pass: the reviewer runs
  `uvx mypy --strict <touched-files>` with zero errors required
  (feat-4 lesson: 77 errors invisible to tests and to annotation audits —
  annotated TypeVars, annotated functional `TypedDict(...)`, same-scope
  re-annotations). Sanctioned forms that are both rule-clean and
  linter-clean: bare `T = TypeVar(...)` definitions (annotating them is a
  linter error; tensor exemption recorded), class-based `TypedDict`s
  (functional form cannot carry `TypeAlias`), `cast` only at
  foreign-boundary narrowings.

## Rules

- FAIL on any F-rule or J-rule violation: fix the batch file and
  recompile with `--force` (never hand-patch frames to pass the gate).
- Keep this skill's checker and the F-rules in sync — editing one means
  editing the other.
- Record the validation run (command + PASS list) in the CU feedback or
  commit message. For Python work, record the `mypy --strict` run the same
  way (command + zero-error verdict).

## Output

A PASS verdict for every regular CU, qualifying the frames for STEP5.

## Book tracking (MANDATORY)

Before committing in the current repo, if the current repo is `llm-driven-design` and `~/Repos/llmdd-book` exists:
1. Add a summary of changes to the llmdd-book (typically a new subsection under the current-version section of `docs/building-llm-driven-design.md`)
2. `git -C ~/Repos/llmdd-book add docs/building-llm-driven-design.md`
3. `git -C ~/Repos/llmdd-book commit -m "docs: <summary>"`
4. Never push from either repo
