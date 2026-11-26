---
name: waves-done-broken
description: Advisor handoff for a wave run that finished and had its ITR artifacts rebuilt but is still broken — diagnose the root design defect and draft corrected CU content for Designer approval. Use when the Designer reports that waves ran, artifacts were rebuilt, and the feature is still broken.
---

# waves-done-broken

Design-level re-entry for WORKFLOW STEP8 (ITERATE) after a failed wave cycle.
The Designer reports that a feature's waves finished running, the ITR artifacts
were rebuilt (batch recompiled and frames regenerated), and the feature is
still broken. This is deliberately **not** another `review-implementation` pass:
breakage that survives a rebuild means the implementation-level loop is
exhausted and the fault is most likely in the design — the spec, the CU
decomposition, or the CU content itself. The Advisor diagnoses that root cause
and drafts corrected CU content for the Designer to review, approve and
compile. One invocation = one feature (`<feature-name>`). Never batch two
features in one run.

The Advisor never implements the fix, never edits compiled frames, and never
re-runs the waves. Those stay with the Coder, the itr-compiler and the Designer.

## When to use

- The Designer says "waves are done, we rebuilt the artifacts, but it's still
  broken" for `<feature-name>`.
- A `review-implementation` pass returned FAIL and a rebuild cycle has since
  completed without restoring green.
- If `<feature-name>` is ambiguous, ask the Designer before starting.

## Prerequisites

- Compiled frames at `.LLMDD/ITRS/<feature-name>/` (`ARCH.itr`, `LEGEND.itr`,
  `SYNOPSIS.itr`, `cu-NNN/` folders, `waves.json`, `run-waves.txt`).
- Batch source `.LLMDD/ITRS/<feature-name>.batch.json` and any prior fix batch
  (`.fix.batch.json`, `.fix-N.batch.json`). Never overwrite a prior cycle.
- Runner runtime state (gitignored — read, never commit):
  `.LLMDD/ITRS/<feature-name>/repo.convergence.json`, `repo.feedback/`,
  `repo.logs/`.
- Feature spec `.LLMDD/spec-driven/features/<feature-name>.md` (acceptance
  criteria `T1..Tn`) and linked bug files under `.LLMDD/spec-driven/bugs/`.
- Baseline DTR at `.LLMDD/DTRS/<feature-name>/`. A fresh scan is **not**
  required by this skill. If the evidence shows the DTR coordinates no longer
  match the working tree, record that and ask the Designer before re-running
  `dtr-builder` (that is a separate STEP1 cycle).

## Procedure (stop on first red gate)

### 1. Confirm the reported state

- Read `repo.convergence.json`: waves finished, coder/lead statuses terminal,
  escalations resolved. Read `repo.feedback/cu-NNN.feedback.txt` and
  `wave-N-verification.txt` for the rebuild outcome.
- Read `git log --oneline -8`: locate the rebuild commit(s) and confirm the
  rebuilt artifacts are the ones currently in the tree.
- Re-run the failing acceptance probes live from the repo root — do not quote
  a feedback transcript. Confirm the break is reproducible.
- Red gate: if the waves did not actually finish, the artifacts were not
  rebuilt, or the probes are in fact green, report the mismatch and stop. Do
  not diagnose a phantom failure.

### 2. Localize each break against the spec

For every failing `T_i`, find the CU whose `ACCEPTANCE.itr` claims it and read
that CU's `REQUIREMENTS.itr`, `COORDINATES.itr` and `IMPLEMENTATION_STEPS.itr`
alongside the actual diff:

- Map `T_i` → CU id → exact DTR coordinates → exact file paths/lines.
- Classify the break:
  - **Design** — wrong or stale DTR coordinate, missing/ambiguous CU step,
    contradictory or incomplete spec, dependency/wave ordering fault.
  - **Implementation** — the design is sound but the code diverged from a
    correct CU step.
- Cite file and line evidence for each classification.

### 3. Decide design defect vs implementation defect

- If every break is implementation-level, stop: hand the evidence back to the
  Coder / code lead. This skill does not draft CUs for a correct design.
- If any break traces to decomposition, coordinates or CU content, state the
  root-cause hypothesis explicitly ("still broken because …") and continue.

### 4. Draft corrected CU content

Write `.LLMDD/ITRS/<feature-name>.fix.batch.json` — or `.fix-N.batch.json` if
that path already exists (preserve every prior cycle for the audit trail):

- JSON array; first two entries are always `arch` + `legend`; regular CUs are
  `cu-001`, `cu-002`, ….
- Each regular CU has `cu-id`, `dtr-coordinates` (exact keys verified against
  the DTR in Prerequisites; absent key = creation) and `content`.
- `content` carries all four labeled sections: `REQUIREMENTS`, `COORDINATES`,
  `IMPLEMENTATION_STEPS`, `ACCEPTANCE`. Steps need exact file paths, complete
  fenced code (no `…`), a `Prove: \`…\`` line per step, an explicit `Do NOT
  touch:` list, and a `Stop when …` line.
- Specify dependency order: CUs touching the same file go in different waves,
  dependencies flow forward.
- Every CU is implicitly `SEVERITY:CRITICAL`; only downgrade with an explicit
  reason in the content (`ARCH.SEVERITY`).

Gate the draft with `validate-steps.py`; all CUs must PASS before handoff:

```sh
python3 .LLMDD/tools/validate-steps.py .LLMDD/ITRS/<feature-name>.fix.batch.json
```

Never compile it — the Designer owns `itr-compiler` and wave planning.

### 5. Commit the diagnosis and draft

Record the root-cause diagnosis in
`.LLMDD/ITRS/<feature-name>/broken-wave-diagnosis.txt` (break → CU → evidence
→ hypothesis), then stage only the diagnosis and the draft batch:

```sh
git add .LLMDD/ITRS/<feature-name>.fix*.batch.json .LLMDD/ITRS/<feature-name>/broken-wave-diagnosis.txt
git status --short
git commit -m "Redesign draft: <feature-name> still broken after rebuild — corrected CUs"
```

Never `git add -A`; runtime state stays gitignored; never push.

### 6. Hand off for approval

Present to the Designer: the root-cause diagnosis, the draft batch path and
the CU dependency order. The Designer reviews and approves, then compiles via
`itr-compiler`, re-plans waves and hands the Coder a new wave cycle. Do not
mark the feature Implemented, do not close bugs, do not run `run-waves.py`.

### 7. Book summary (llmdd-book — mandatory)

Add a subsection under the current-version section of
`~/Repos/llmdd-book/docs/building-llm-driven-design.md` (what was still broken,
the root-cause hypothesis, the CU changes) and commit it separately with a
`docs:` message. Never push from either repo.

## Rules

- Analyze-only: no application code, no frame edits, no wave runs. CU content
  is design output for Designer review; the itr-compiler produces frames.
- Evidence before synthesis: every classification and hypothesis cites a live
  command from this session, not a feedback transcript.
- A red gate stops the chain — report and re-run from that step, never skip.
- Never overwrite a prior cycle's batch or diagnosis; append a new `.fix-N`.
- Never close specs or mark the feature Implemented — that is
  `review-implementation` after a green rebuild, not this skill.
- The standing design grant covers `.LLMDD/ITRS/*`; any application-code write
  still needs a scoped approval token
  (`.LLMDD/tools/check-advisor-approval.py`).
- Never `git add -A`; never push.

## Verify

1. Reported state confirmed live: waves finished, artifacts rebuilt, break
   reproducible.
2. Every failing `T_i` mapped to a CU, coordinates and file/line evidence.
3. Root-cause hypothesis stated; implementation-only breaks handed back.
4. Draft batch written to a new `.fix*.batch.json` and passes
   `validate-steps.py` for every CU.
5. Diagnosis + draft committed; no frames, code or runtime state staged.
6. Handoff names the diagnosis, draft path and dependency order; specs left
   open.
7. Book subsection added and committed with a `docs:` message; nothing pushed.

## Output

A diagnosis (root cause with file/line evidence) plus the corrected CU draft
for Designer approval — no implementation, no compiled frames, no wave run,
no spec closure.
