# feat-6: Global Per-Repo Spec Numbering

- Status: Implemented
- Created: 2026-10-17
- Feature ID: feat-6-global-spec-id-numbering

## Synopsis

Motivation: bug and feature ids are allocated by scanning the working tree
(`.LLMDD/spec-driven/bugs/`, `.LLMDD/spec-driven/features/`). The working
tree is branch-scoped, so two branches cut from the same state of dev both
compute `max+1` and both claim the same serial. Because the colliding
files carry different names (`bug-4-a.md` vs `bug-4-b.md`), git merges them
with no conflict and the repo ends up with two bug-4s — a silent defect,
not a merge failure. It bites on every branch switch: create a bug on
branch A, switch to a branch cut earlier, create "bug-4" again.

Scope in: one repo-agnostic allocator tool under `.LLMDD/tools/`, a plain
text counter file, rewiring the two spec skills onto the tool, the tensor
rule that makes allocation tool-only, and `init-repo` resetting the
counter. Scope out: approval tokens (they are per-change expiring grants
validated by id, not a sequence), migrating existing ids, and changing
the `bug-<n>-` / `feat-<n>-` / `bugfix-feature-<n>-` id shapes.

The fix is placement, not storage: a counter committed to a branch is
itself branch-scoped and reproduces the collision. sqlite is strictly
worse than text — a binary counter file cannot be hand-merged when two
branches bump it, so one side's allocation is lost. Hence a text counter,
with the allocator reading every git ref rather than the checkout.

## Implementation status

Implemented in commit d96e2a0 (wave 1 cu-001; wave 2 cu-002 and cu-003).
Both waves converged with zero escalations; all three CU feedback files
report STATUS=COMPLETED, VERIFICATION_RESULT=PASSED, ESCALATION_REASON=none.
ITR directory: `.LLMDD/ITRS/feat-6-global-spec-id-numbering/`.

Delivered: `.LLMDD/tools/next-spec-id.py` (ref-aware allocator with
`--kind`, `--prefix`, `--dry-run`, `--check`) and
`.LLMDD/tools/test_next_spec_id.py`; both spec skills rewired onto the
allocator; the counter seeded at `.LLMDD/spec-driven/ids.txt`
(`bug=5`, `feature=6`); `RULE.NUM.GLOBAL_SPEC_IDS` in the tensor;
`init-repo` resets the counter; `.LLMDD/tools/README.md` documents both
tools.

No linked bugs: this feature was scoped directly, not raised as a defect,
so no file under `.LLMDD/spec-driven/bugs/` is closed by it.

## Acceptance criteria

1. `.LLMDD/tools/next-spec-id.py --kind bug` in this repo prints `bug-6`;
   `--kind feature` prints `feat-7`.
2. The maximum is taken across the working tree and every ref under
   `refs/heads` and `refs/remotes`, so two branches cut from the same
   commit receive different numbers.
3. The counter is plain text at `.LLMDD/spec-driven/ids.txt` with one
   `kind=<n>` line for `bug` and `feature`; `bugfix-feature` shares the
   `feature` sequence; a missing counter is seeded from existing spec
   filenames, so no repo needs migration.
4. The tool is repo-agnostic: the root is derived from `__file__`, no
   other path input is taken, and no repository-specific bug or feature
   name is assumed.
5. `--dry-run` prints the id without writing; `--check` never writes.
6. `--check` exits 1 naming the offending files when two bug serials
   collide, when two feature serials collide (counting `bugfix-feature-*`
   in the feature sequence), when a `.LLMDD/DTRS/<name>` or
   `.LLMDD/ITRS/<name>` directory has no matching spec file, or when the
   counter is lower than a serial present in the tree.
7. `skills/bug` and `skills/feature` allocate only through the tool and
   no longer instruct a directory scan; this repo carries a seeded
   `ids.txt`.
8. The tensor carries `RULE.NUM.GLOBAL_SPEC_IDS`, and `init-repo` deletes
   `ids.txt` so a fresh repo restarts at 1.

## Tests

1. `python3 .LLMDD/tools/test_next_spec_id.py` — standalone fixture test
   (no pytest; `test_synopsis_budget.py` is the precedent). Builds a
   throwaway repo under `/tmp/opencode/` with bug-5 and feat-5 committed,
   cuts two sibling branches, and asserts they get different ids;
   asserts `--check` is clean; asserts a hand-made duplicate makes
   `--check` exit 1. Expected: prints PASS, exits 0.
2. `uvx mypy --strict .LLMDD/tools/next-spec-id.py .LLMDD/tools/test_next_spec_id.py`
   — expected: zero errors.
3. `python3 .LLMDD/tools/next-spec-id.py --check` — expected: exit 0, no
   output.
4. `python3 .LLMDD/tools/next-spec-id.py --kind feature --dry-run` —
   expected: prints `feat-7-`; `git status --short` shows `ids.txt`
   unchanged.
5. `grep -c "highest existing" .LLMDD/skills/bug/SKILL.md
   .LLMDD/skills/feature/SKILL.md` — expected: both counts 0.
