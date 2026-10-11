---
name: feature
description: Create a new spec-driven feature file under .LLMDD/spec-driven/features/ with a serial feat-<n>-<name> id (bugfix-feature-<n>-<name> for bug/bugs fixes), motivation synopsis, implementation status, acceptance criteria and tests. Use whenever a new feature is scoped in LLMDD v9.
---

# feature

Creates one Markdown spec per feature. Features (not ad-hoc tasks) are the
development steps in LLMDD v9 — each feature flows through Advisor design,
`compile-itr`, and `run-waves`.

## Naming

`feat-<n>-<short-name>.md`, e.g. `feat-1-add-dtr-pointer-to-cu.md`.

- `<n>` is a serial integer: scan `.LLMDD/spec-driven/features/` for the
  highest existing `feat-N-*` and use `N+1`. Never reuse a number.
- `<short-name>` is kebab-case, imperative, ≤6 words.

When the feature is a bug/bugs fix, prefix the name with `bugfix-`:
`bugfix-feature-<n>-<short-name>.md`,
e.g. `bugfix-feature-2-fix-compiler-parsing-and-layout.md`. The serial `<n>`
shares the same sequence (highest `feat-N-*` or `bugfix-feature-N-*` + 1),
and the Feature ID is the full prefixed name
(`bugfix-feature-<n>-<short-name>`).

## File location

`.LLMDD/spec-driven/features/feat-<n>-<short-name>.md` — or
`.LLMDD/spec-driven/features/bugfix-feature-<n>-<short-name>.md` for a
bug/bugs fix.

## Template

```markdown
# feat-<n>: <Title>
(for a bug/bugs fix: # bugfix-feature-<n>: <Title>)

- Status: Proposed | In Progress | Implemented
- Created: <YYYY-MM-DD>
- Feature ID: feat-<n>-<short-name>
(for a bug/bugs fix: bugfix-feature-<n>-<short-name>)

## Synopsis

Motivation: why this feature exists, what problem it solves, who it serves.
Scope: what is in and out.

## Implementation status

Whether the feature is implemented or not. Move Proposed → In Progress when
CUs are assigned, → Implemented when all waves are done and E2E verify is
green. Record the implementing commit(s) and ITR directory here.

## Acceptance criteria

Numbered, testable conditions that must hold for the feature to count as
done (e.g. `1. dtr-builder emits TYPE entries for nested classes`).

## Tests

Executable tests proving each criterion: test file paths, commands to run,
and expected results. Written by the Advisor, run by the code lead in
E2E verify.
```

## Rules

- One file per feature; never bundle two features in one spec.
- Status is a single value from the allowed set — keep it current; a stale
  status is a spec bug.
- Acceptance criteria must be verifiable by the listed tests, one-to-one
  where possible.
- The feature ID (serial int) is assigned at creation and never changes,
  even if the title is reworded.
- A bug/bugs fix feature must use the `bugfix-feature-<n>-<short-name>` name
  and ID, and must list the fixed bug ID(s) (e.g. `bug-1-...`) in its
  Synopsis. Linkage flows fix → bugs only: never add a reverse pointer in
  the bug file(s).

## Output

The new spec file. Design discussion continues against it; implementation
starts only after the Designer approves the criteria and tests.
