---
name: bug
description: Create a new spec-driven bug file under .LLMDD/spec-driven/bugs/ with a serial bug-<n>-<name> id allocated by .LLMDD/tools/next-spec-id.py and unique per repository across branches, bug detail, resolution status, and (when resolved) fix summary plus proving tests. Use whenever a defect is reported in LLMDD v9.
---

# bug

Creates one Markdown spec per bug. Bugs are tracked as first-class specs
alongside features — each bug flows through reproduction, fix (via
`compile-itr` + `run-waves` if code changes are needed), and proof by test.

## Naming

`bug-<n>-<short-name>.md`, for example `bug-6-force-flag-skips-frames.md`.

- `<n>` comes from the allocator, never from a directory scan. Run
  `python3 .LLMDD/tools/next-spec-id.py --kind bug` from the repo root and
  use the id it prints; the tool reads the counter and every git ref, so
  two branches never receive the same serial. Never reuse a number.
- `<short-name>` is kebab-case, symptom-oriented, at most six words.

## File location

`.LLMDD/spec-driven/bugs/bug-<n>-<short-name>.md`

## Template

```markdown
# bug-<n>: <Title>

- Status: Open | Resolved
- Created: <YYYY-MM-DD>
- Resolved: <YYYY-MM-DD or "-">
- Bug ID: bug-<n>-<short-name>

## Bug detail

Symptoms, reproduction steps (commands + expected vs actual), environment,
and suspected cause. Enough for another engineer to reproduce blind.

## Resolution status

Whether the bug is resolved or not. Move Open → Resolved only when the
fix is merged AND the proving tests below pass. Record the fixing
commit(s) here. There is no third state — to cancel a bug, delete the
file.

## Fix summary (Resolved only)

What changed and why. Link the fixing commit(s). Leave as "Pending" while
Open.

## Tests proving resolution (Resolved only)

Executable tests demonstrating the bug is fixed: test file paths, commands
to run, expected results, and the failing-before / passing-after evidence.
Leave as "Pending" while Open.
```

## Rules

- One file per bug; a bug covering two symptoms is two files.
- Status is a single value from the allowed set — `Resolved` requires both
  a merged fix and green proving tests, no exceptions. Never introduce a
  third status to describe a bug that stopped being one.
- A cancelled or invalid bug is **deleted**, not annotated. `Open` and
  `Resolved` are the whole enum. When deleting, also strip references to
  that id from every other bug, feature record and book section — a
  deleted file with live references is worse than either legal state. The
  serial stays burned; the allocator never reissues it, so the sequence
  may skip a number and `--check` still exits 0.
- Run the reproduction **before writing the file**, not at fix time. A bug
  whose reproduction has never been executed is an assertion wearing a
  report's clothes. If a probe genuinely cannot be run, say so in Bug
  detail and mark the cause unknown rather than implying it was verified.
- Never file a runtime or configuration claim you inferred. Claims about
  what a tool loads, parses, enforces or exposes must be observed from the
  running system — read the installed schema, query the endpoint, inspect
  the live tool set. A plausible guess about a framework's behaviour is
  the most common way a filed bug turns out not to be a bug at all.
- `Resolved` is not immutable. If later evidence contradicts a closure,
  amend the file with a dated correction naming what changed, and restate
  which tests changed verdict. Do not silently leave the stale claim.
- Reproduction steps must be verified before the fix starts; unverified
  reports stay Open with cause marked unknown.
- The bug ID (serial int) is assigned at creation and never changes.
- If the fix needs code changes, implement via the normal pipeline
  (`compile-itr` → `run-waves`). Linkage flows fix → bugs only: the fix
  feature lists this bug's ID, never the reverse — a bug file must not
  point at its fix.

## Output

The new spec file. Triage and fixing proceed against it; closure requires
the Fix summary and Tests sections filled in.


## Skill validation

Before creating the spec, run the deterministic validator to ensure all steps are followed:

```bash
python3 .LLMDD/tools/skill-check.py bug --print-prompt
```

The Advisor must execute every step from that prompt in order and report a checklist at the end.

## Book tracking (MANDATORY)

Before committing in the current repo, if the current repo is `llm-driven-design` and `~/Repos/llmdd-book` exists:
1. Add a summary of changes to the llmdd-book (typically a new subsection under the current-version section of `docs/building-llm-driven-design.md`)
2. `git -C ~/Repos/llmdd-book add docs/building-llm-driven-design.md`
3. `git -C ~/Repos/llmdd-book commit -m "docs: <summary>"`
4. Never push from either repo
