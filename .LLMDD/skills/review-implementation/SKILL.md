---
name: review-implementation
description: Review a completed wave run against its requirements — inspect the implementation diff, wave logs and feedback, re-run acceptance tests independently, and when everything is green mark linked bugs Resolved and features Implemented, then commit. Use after run-waves.py finishes all waves for a feature.
---

# review-implementation

Post-implementation review for WORKFLOW STEP5 (IMPLEMENT_CUS) into STEP6/7
(VERIFY / E2E VERIFY). The agent verifies a finished wave run for one
feature (`<feature-name>` = the feature ID) and, only on a full pass,
closes the loop in the spec-driven tracker: linked bug files under
`.LLMDD/spec-driven/bugs/` go to Resolved, the feature file under
`.LLMDD/spec-driven/features/` goes to Implemented. Then the agent
commits, and records the validation in the llmdd-book repo with a
separate commit (step 8 — mandatory, not optional). One invocation = one
feature. Never batch two features in one run.

## When to use

- The Designer reports "waves are done" / asks to "review" / "validate"
  a finished `run-waves.py` run for `<feature-name>`.
- `repo.convergence.json` under `.LLMDD/ITRS/<feature-name>/` reports a
  terminal state for all waves.
- If `<feature-name>` is ambiguous, ask the Designer before starting.

## Prerequisites

- Compiled frames at `.LLMDD/ITRS/<feature-name>/` (`ARCH.itr`,
  `LEGEND.itr`, `SYNOPSIS.itr`, `cu-NNN/` folders, `waves.json`,
  `run-waves.txt`).
- Runner runtime state: `.LLMDD/ITRS/<feature-name>/repo.feedback/`
  (per-CU `*.feedback.txt`, `wave-N-verification.txt`),
  `repo.logs/`, `repo.convergence.json`. These paths are gitignored —
  read them, never commit them.
- Feature spec at `.LLMDD/spec-driven/features/<feature-name>.md`
  (acceptance criteria `T1..Tn`); linked bug files under
  `.LLMDD/spec-driven/bugs/`.

## Procedure (stop on first red gate)

### 1. Confirm the run finished

- Read `repo.convergence.json`: every wave lists its CUs with
  `coder_status: 0` / `lead_status: 0`, `escalations: 0`, and the top
  level `status` is `CONVERGED`.
- Read each `repo.feedback/cu-NNN.feedback.txt`: `STATUS=COMPLETED`,
  `VERIFICATION_RESULT=PASSED`, `ESCALATION_REASON=none`. Note any
  `ARCHITECTURE_DEVIATION` or "pre-existing edit" remarks for step 4.
- Non-converged waves or open escalations → red gate: stop, report,
  hand back to the Coder / code lead. Never validate a partial run.

### 2. Review the implementation diff

```sh
git log --oneline -8; git status -sb
git show --stat HEAD
```

- The wave commit(s) touch only the feature's intended files. Any
  change to a `Do NOT touch` path from the CU frames (neighbor agents,
  `opencode.json`, unrelated sources/tools) is a red gate.
- `git status --porcelain` is clean apart from expected untracked
  runtime state. Stray modifications → red gate.

### 3. Review wave logs and feedback

- Read `repo.feedback/wave-N-verification.txt` per wave: convergence
  claimed on which iteration, which probes ran, cross-CU issues.
- Skim `repo.logs/` for the wave under review (coder attempt logs,
  `lead-wave-N.log`): look for timeouts, fallback rotations, retried
  attempts, or silent kills that the verification report does not
  mention. Unexplained anomalies → investigate before proceeding.

### 4. Compare implementation to requirements

For each regular CU, diff its `REQUIREMENTS.itr` /
`COORDINATES.itr` / `IMPLEMENTATION_STEPS.itr` against the working
tree:

- Exact placement: inserted lines/rules/sections at the specified
  location, byte-identical to the CU fence text, no duplicates
  (`grep -c` == 1 for each added marker).
- Hardlink / twin integrity where the CU claims it
  (`ls -i` identical inodes, `cmp` clean).
- Feedback "verified pre-existing edit" claims re-checked against
  the CU spec — identical end state still counts, but say so in the
  verdict.
- Mismatch (wrong file, wrong placement, duplicated block, broken
  twin) → red gate.

### 5. Re-run acceptance tests independently

Never trust feedback transcripts alone. Re-run every spec test
(`T1..Tn` from the feature file) plus the CU `ACCEPTANCE.itr` probes
with live commands from the repo root, and confirm green:

```sh
python3 .LLMDD/tools/validate-steps.py .LLMDD/ITRS/<feature-name>.batch.json
python3 .LLMDD/tools/test_synopsis_budget.py .LLMDD/ITRS/<feature-name>/SYNOPSIS.itr
```

- Static probes (frontmatter audits, `grep -c`/`grep -n` markers) —
  re-run all.
- Negative / denial probes — re-run live where the spec demands a
  denial (exit codes observed, not quoted).
- Read-path probes — re-run and confirm `git status --porcelain`
  shows no modifications from the read path.
- Language proofs where applicable (`mypy --strict`, focused test
  suites) — re-run; a missing tool in this environment downgrades to
  an explicit observation, not a silent pass.
- Any red probe → red gate: stop, report findings with file and line
  references, do not close specs.

### 6. Close the specs (only on full green)

Edit the tracker files (spec edits only — never implementation
files in this step):

- Each linked bug file (`.LLMDD/spec-driven/bugs/<bug-id>.md`):
  `Status: Resolved`, `Resolved: <YYYY-MM-DD>`, fill `Resolution
  status` (fixed in `<wave-commit>`), `Fix summary` (what changed,
  file references), `Tests proving resolution` (the re-run `T1..Tn`
  results from step 5).
- The feature file
  (`.LLMDD/spec-driven/features/<feature-name>.md`):
  `Status: Implemented`, `Implementation status` updated (CUs merged,
  waves converged, bugs closed).
- Record honest gaps as follow-ups in the verdict (e.g. static-only
  denial proof where the spec wanted a live e2e, narrow allowlists
  that may cause friction) instead of silently passing them.

### 7. Commit

```sh
git add .LLMDD/spec-driven/features .LLMDD/spec-driven/bugs
git status --short
git commit -m "Review: validate <feature-name> waves — mark <bug-id> resolved"
```

- Stage only the spec/bug paths (never `-A`); implementation files
  were committed by the wave run, runtime state stays gitignored.
- Verify the commit lists only intended files. Never push (the
  Designer pushes; a pre-push hook blocks agents anyway).

### 8. Book summary (llmdd-book — mandatory)

Every validation commit in this repo is accompanied by a summary in the
`llmdd-book` repo (`~/Repos/llmdd-book`,
`docs/building-llm-driven-design.md`, new subsection under the
current-version section: verdict, re-run evidence, gaps/follow-ups,
workspace/skill/lessons notes). Commit it separately with a `docs:`
message. Never push from either repo — the Designer pushes.

```sh
git -C ~/Repos/llmdd-book add docs/building-llm-driven-design.md
git -C ~/Repos/llmdd-book commit -m "docs: <subsection> <feature-name> validation verdict"
```

## Rules

- Evidence before synthesis: every verdict cites a live command
  result from this session, not a feedback transcript.
- A red gate stops the chain — fix and re-run from that step, never
  skip ahead, never close specs on red.
- Never hand-edit compiled frames (fix the batch, recompile) or
  baselines (re-run the builder) during validation; findings go back
  to the Coder / code lead as a new wave cycle.
- Never `git add -A`; never push.

## Verify

1. Convergence file shows all waves converged, zero escalations.
2. Wave diff touches only intended files; tree clean otherwise.
3. Per-CU implementation matches frame requirements byte-exactly.
4. All spec `T1..Tn` plus `validate-steps.py` plus synopsis budget
   re-run green in this session.
5. Bug file(s) Resolved with summary + proving tests; feature file
   Implemented.
6. Final commit contains spec/bug paths only, and nothing else.
7. Book subsection added and committed in llmdd-book with a `docs:`
   message; nothing pushed from either repo.

## Output

A validation verdict (PASS with re-run evidence, or FAIL with
findings) plus, on PASS, the spec-closing commit and the llmdd-book
summary commit — the feature is done, the tracker says so, and the book
records it.
