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
commit(s) here.

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
  a merged fix and green proving tests, no exceptions.
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
