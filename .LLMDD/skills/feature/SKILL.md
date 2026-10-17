---
name: feature
description: Create a new spec-driven feature file under .LLMDD/spec-driven/features/ with a serial feat-<n>-<name> id (bugfix-feature-<n>-<name> for bug or bugs fixes) allocated by .LLMDD/tools/next-spec-id.py and unique per repository across branches, motivation synopsis, implementation status, acceptance criteria and tests. Use whenever a new feature is scoped in LLMDD v9.
---

# feature

Creates one Markdown spec per feature. Features (not ad-hoc tasks) are the
development steps in LLMDD v9 — each feature flows through Advisor design,
`compile-itr`, and `run-waves`.

## Naming

`feat-<n>-<short-name>.md`, for example `feat-6-add-dtr-pointer.md`.

- `<n>` comes from the allocator, never from a directory scan. Run
  `python3 .LLMDD/tools/next-spec-id.py --kind feature` from the repo root
  and use the id it prints. For a bug or bugs fix, also pass
  `--prefix bugfix-feature-` and the tool prints
  `bugfix-feature-<n>-`. The tool reads the counter and every git ref, so
  two branches never receive the same serial. Never reuse a number.
- `<short-name>` is kebab-case, imperative, at most six words.

The bugfix feature serial shares the feature sequence, exactly as the
rules section below already requires.

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
E2E verify. For Python implementation work, include a mypy strict probe
(e.g. `uvx mypy --strict <touched-files>`, zero errors expected) — annotation
presence does not imply type agreement.
```

## Rules

- One file per feature; never bundle two features in one spec.
- Status is a single value from the allowed set — keep it current; a stale
  status is a spec bug.
- Acceptance criteria must be verifiable by the listed tests, one-to-one
  where possible.
- Python features must list the `mypy --strict` zero-error probe among their
  tests (see Tests); a Python feature without it is under-tested.
- The feature ID (serial int) is assigned at creation and never changes,
  even if the title is reworded.
- A bug/bugs fix feature must use the `bugfix-feature-<n>-<short-name>` name
  and ID, and must list the fixed bug ID(s) (e.g. `bug-1-...`) in its
  Synopsis. Linkage flows fix → bugs only: never add a reverse pointer in
  the bug file(s).

## Output

The new spec file. Design discussion continues against it; implementation
starts only after the Designer approves the criteria and tests.
