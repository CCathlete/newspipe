---
name: bug
description: Create a new spec-driven bug file under .LLMDD/spec-driven/bugs/ with a serial bug-<n>-<name> id, bug detail, resolution status, and (when resolved) fix summary plus proving tests. Use whenever a defect is reported in LLMDD v9.
---

# bug

Creates one Markdown spec per bug. Bugs are tracked as first-class specs
alongside features — each bug flows through reproduction, fix (via
`compile-itr` + `run-waves` if code changes are needed), and proof by test.

## Naming

`bug-<n>-<short-name>.md`, e.g. `bug-3-force-flag-skips-frames.md`.

- `<n>` is a serial integer: scan `.LLMDD/spec-driven/bugs/` for the
  highest existing `bug-N-*` and use `N+1`. Never reuse a number.
- `<short-name>` is kebab-case, symptom-oriented, ≤6 words.

## File location

`.LLMDD/spec-driven/bugs/bug-<n>-<short-name>.md`

## Template

```markdown
# bug-<n>: <Title>

- Status: Open | Resolved
- Created: <YYYY-MM-DD>
- Resolved: <YYYY-MM-DD or "-">
- Bug ID: bug-<n>-<short-name>
- Related features: <feat-N ids or "-">

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
  (`compile-itr` → `run-waves`) and reference the feature/CUs in
  Related features.
```

## Output

The new spec file. Triage and fixing proceed against it; closure requires
the Fix summary and Tests sections filled in.
