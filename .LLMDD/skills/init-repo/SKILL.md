---
name: init-repo
description: Initialise a copied .LLMDD directory for a new repo by cleaning ITRS, DTRS, spec-driven/features and spec-driven/bugs back to empty state. Use ONLY when setting up LLMDD in a new repo from a copied .LLMDD folder.
---

# init-repo

Resets a copied `.LLMDD/` directory to a clean initial state so a new repo
starts with no inherited features, bugs, ITRs, or DTRs. Only the four
content directories are cleaned; everything else (`tools/`, `instructions/`,
`agents/`, `prompts/`, `ITRS/templates/`, all `README.md` files) is
preserved.

## When to use

- The `.LLMDD/` directory was copied from another repo and must be
  initialised before WORKFLOW STEP1 (OBTAIN_BASELINE_DTR).
- Never use on the source repo or mid-feature — this deletes real specs.

## Prerequisites

- Run from the new repo root.
- `.LLMDD/` exists and is a copy (verify with `git status` — the new repo
  should show `.LLMDD/` as new/untracked, not as modifications to the old
  repo).

## Commands (run from repo root)

```sh
# 1. Verify this is a fresh copy target
ls .LLMDD/ && git status --short | head -n 20

# 2. Clean the four content directories, preserving README.md and templates/
find .LLMDD/ITRS -mindepth 1 -maxdepth 1 ! -name 'README.md' ! -name 'templates' -exec rm -rf {} +
find .LLMDD/DTRS -mindepth 1 -maxdepth 1 ! -name 'README.md' -exec rm -rf {} +
find .LLMDD/spec-driven/features -mindepth 1 -maxdepth 1 ! -name 'README.md' -exec rm -rf {} +
find .LLMDD/spec-driven/bugs -mindepth 1 -maxdepth 1 ! -name 'README.md' -exec rm -rf {} +
```

## Rules

- Delete only inside these four directories: `.LLMDD/ITRS/`,
  `.LLMDD/DTRS/`, `.LLMDD/spec-driven/features/`,
  `.LLMDD/spec-driven/bugs/`.
- Never delete `.LLMDD/ITRS/templates/` or any `README.md`.
- Never delete or modify `.LLMDD/tools/`, `.LLMDD/instructions/`,
  `.LLMDD/agents/`, `.LLMDD/prompts/`, `.LLMDD/skills/`.
- Never delete the `.LLMDD/` directory itself.
- Serial numbering restarts: the first new feature is `feat-1-*`, the first
  new bug is `bug-1-*`.

## Verify

1. `ls .LLMDD/ITRS/` shows only `README.md` and `templates/`.
2. `ls .LLMDD/DTRS/` shows only `README.md`.
3. `ls .LLMDD/spec-driven/features/` and `ls .LLMDD/spec-driven/bugs/`
   show only `README.md`.
4. `git status --short` shows deletions limited to the four directories
   above — nothing under `tools/`, `templates/`, or `README.md` files.

## Output

An empty `.LLMDD/` workspace ready for STEP1. Hand off to `build-dtr` for
the baseline DTR.
