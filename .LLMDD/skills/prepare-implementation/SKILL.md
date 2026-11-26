---
name: prepare-implementation
description: Run the full pre-implementation chain for one feature — commit pending specs, build DTR, validate the feature hypotheses with the Hypothesis Engine (hard gate), draft a dummy-proof batch CU, compile ITR, plan waves, write the wave command to a txt file, print it, and commit. Use when the Designer says implementation is ready to be prepared.
---

# prepare-implementation

End-to-end prep for WORKFLOW STEP5 (IMPLEMENT_CUS). Chains the
single-purpose skills in order: spec commit → `build-dtr` →
hypothesis validation (hard gate) → batch CU → `compile-itr` →
waves plan → dry-run fix loop → `run-waves.txt` → print → commit.
The agent does everything up to
(not including) executing `run-waves.py` — the Designer runs that
by pasting the printed command.

## When to use

- The Designer has approved a feature spec (and any linked bug specs)
  and asks to "prepare implementation" / "prepare waves" / "get ready
  for coders".
- One invocation = one feature (`<feature-name>` = the feature ID,
  e.g. `feat-4-infra-port-folder-rename`,
  `bugfix-feature-6-enrichment-consumer-poll-contract`). Never batch
  two features in one run.
- Delegation: @hypothesize is used for hypothesis reasoning and validation
  tasks, as documented in the @hypothesize skill.

End result must be successful dry run of wave command. If you didn't reach the end result you fix until you reach a successful dry run of wave command.

DO NOT RUN WAVE COMMAND WITHOUT --dry-run

## Prerequisites

- Feature spec exists at
  `.LLMDD/spec-driven/features/<feature-name>.md` with agreed
  acceptance criteria; linked bug files exist under
  `.LLMDD/spec-driven/bugs/` where applicable.
- If `<feature-name>` is ambiguous, ask the Designer before starting.

## Procedure (stop on first red gate)

### 1. Commit pending specs

```sh
git status --short -- .LLMDD/spec-driven/features .LLMDD/spec-driven/bugs
```

- Empty output → specs clean, proceed.
- Non-empty → stage and commit only the spec paths (never `-A`):

```sh
git add .LLMDD/spec-driven/features .LLMDD/spec-driven/bugs
git commit -m "Spec: stage feature/bug specs for <feature-name>"
```

Verify with `git status --short -- <those paths>` (must be empty).

### 2. Build DTR (`build-dtr`)

```sh
.LLMDD/tools/dtr-builder --out .LLMDD/DTRS/<feature-name>/<app>.dtr --root <path-to-code>
```

- Reuse the feature's existing DTR path when one exists; otherwise
  create `.LLMDD/DTRS/<feature-name>/`. One baseline per cycle —
  re-running overwrites.
- Never hand-edit the `.dtr`. Confirm exit 0 and the summary counts,
  and that the `ARCH=` line matches project constraints.

### 3. Validate the feature hypotheses (hard gate)

Before any batch CU is written, run the feature's hypotheses through the
Hypothesis Engine by invoking `@hypothesize <feature-name>`. That skill now
owns the complete reasoning and validation procedure: hypothesis and
experiment authoring, DTR coordinate mapping, engine invocation, verbatim
ledger echo, the deterministic checker gate, verdict interpretation and its
fix loop. This step remains a hard gate: no batch CU is drafted until
`hypothesize` returns `GREEN_LIGHT`, recorded in
`.LLMDD/ITRS/<feature-name>/hypothesis-verification.txt`. Keep the hypothesis
inputs, ledger report, conversation record and verdict line under
`.LLMDD/ITRS/<feature-name>/` where `hypothesize` writes them; they are
committed in step 10. Acceptance tests that need the new code belong in the
verify phase, not here. Do not duplicate the engine procedure in this file.

### 4. Draft the batch CU (dummy-proof)

   3. Reset the engine ledger for the first stage, then examine (run from the repo
    root). Run `clean-up-probe` once before the first stage of a hypothesis. On a
    re-run after a RED or ADVISOR_ACTION_REQUIRED stage, do NOT run
    `clean-up` again: the engine clears that stage's evidence and preserves
    the decision history so each report can show the full progression.

    ```sh
    (cd hypothesis-engine && sbt -batch clean-up-probe)
    (cd hypothesis-engine && sbt -batch "run_main hypothesisengine.control.Main probe-command ../.LLMDD/ITRS/<feature-name>/<feature-name>.hypotheses.json ../.LLMDD/ITRS/<feature-name>/<feature-name>.probe-results")
    ```

    4. Echo the ledger report block before any revision or advance. The engine
    prints one JSON block to stdout and appends the byte-identical block to
    `.LLMDD/ITRS/<feature-name>/<feature-name>.ledger-report.jsonl`. Paste that
    block verbatim into the conversation and append the same block to
    `.LLMDD/ITRS/<feature-name>/<feature-name>.hypothesis-conversation.md`.
    You MUST NOT revise the hypothesis, re-run the engine, or advance to
    top-level step 4 until the current stage's block appears verbatim in both
    the conversation and the conversation record. Run the deterministic checker
    as a hard gate:

    ```sh
    python3 .LLMDD/tools/check-hypothesis-report.py \
      --report .LLMDD/ITRS/<feature-name>/<feature-name>.ledger-report.jsonl \
      --conversation .LLMDD/ITRS/<feature-name>/<feature-name>.hypothesis-conversation.md \
      --stage <stage-number-from-the-report-block> \
      --verdict <GREEN_LIGHT|RED_LIGHT|ADVISOR_ACTION_REQUIRED>
    ```

    A `CHECK_FAILED` exit stops the workflow: supply the missing verbatim echo
    and re-run the checker before interpreting the verdict.

    5. Interpret the verdict verbatim:

    - `GREEN_LIGHT posterior=<value> gate=<value>` u2192 pass; report the GREEN_LIGHT block in
      the conversation first, record the line in
      `.LLMDD/ITRS/<feature-name>/hypothesis-verification.txt` and proceed to
      top-level step 4 (`Draft the batch CU`).
    - `RED_LIGHT posterior=<value> gate=<value> breakpoints=<DTR coords> fix=<text>` u2192 fail. The breakpoints
      are DTR coordinates: revise the spec/DTR (or the hypotheses) at those
      coordinates and re-run from sub-step 3. Do not draft CUs on a red light.
    - `ADVISOR_ACTION_REQUIRED missing=<names> corrections=<text>` u2192 fail. Supply the named
      ledger data (a gate of at least 0.85, a non-empty justification, or fix
      malformed hypotheses/experiments JSON) and re-run from sub-step 3.
    - Non-zero exit, crash, or no verdict line u2192 fail. Treat as an engine defect;
      stop and report it.

    Store the hypothesis/experiment inputs, the report file, the conversation
    record and the verdict line with the feature's ITR artifacts; commit them in
    step 10. Never proceed to top-level step 4 on a red gate u2014 the fix loop has no
    iteration cap.
Write `.LLMDD/ITRS/<feature-name>.batch.json` (JSON array; YAML
accepted with `--yaml-content`). First two entries are always
`arch` + `legend`; regular CUs are `cu-001`, `cu-002`, …:

```json
[
  {"cu-id": "arch", "cu-type": "arch", "dtr-coordinates": [], "content": "Architecture rules..."},
  {"cu-id": "legend", "cu-type": "legend", "dtr-coordinates": [], "content": "Legend..."},
  {"cu-id": "cu-001", "dtr-coordinates": ["FILE.a/b.py"], "content": "REQUIREMENTS:\n...\n\nCOORDINATES:\n...\n\nIMPLEMENTATION_STEPS:\n...\n\nACCEPTANCE:\n..."}
]
```

Every regular CU content carries all four labeled sections with
implementation-grade detail (see `compile-itr` + 
`review-implementation-steps`):

- **REQUIREMENTS** — slice of the feature this CU delivers.
- **COORDINATES** — exact DTR keys touched, verified against the
  feature DTR from step 2 (absent key = creation).
- **IMPLEMENTATION_STEPS** — numbered steps, each with exact file
  path(s), complete fenced code (no `…`/`...`), a
  `Prove: \`…\`` line per step, an explicit `Do NOT touch:` list,
  and a `Stop when …` line.
- **ACCEPTANCE** — spec test IDs with exact commands + expected results.

Gate before compiling:

```sh
python3 .LLMDD/tools/validate-steps.py .LLMDD/ITRS/<feature-name>.batch.json
```

All CUs must PASS. FAIL → fix the batch, re-run. Never compile a
failing batch.

### 5. Compile ITR (`compile-itr`)

```sh
.LLMDD/tools/itr-compiler --compile --json-content .LLMDD/ITRS/<feature-name>.batch.json --out-folder .LLMDD/ITRS/<feature-name>/ --dtr .LLMDD/DTRS/<feature-name>/<app>.dtr
```

- Fresh folder requires `arch` + `legend` or the compiler hard-fails.
- Non-zero exit = hard fail: fix the batch, recompile (with
  `--force` for intentional recompiles). Never hand-edit frames.
- Run the `verify-cu-structure` checks: root holds only `ARCH.itr`,
  `LEGEND.itr` (plus `SYNOPSIS.itr` / companions); every `cu-NNN/`
  holds exactly `COORDINATES.itr`, `REQUIREMENTS.itr`,
  `IMPLEMENTATION_STEPS.itr`, `ACCEPTANCE.itr`; no blobs, no
  `arch/`/`legend/` subfolders. Repair per that skill when needed.

### 6. Create waves (`run-waves` planning)

Author `.LLMDD/ITRS/<feature-name>/waves.json` (regular CUs only —
`arch`/`legend` excluded), packing as many parallel CUs per wave as
ordering allows:

```json
[
  {"wave": 1, "description": "...", "cus": ["cu-001", "cu-002"]},
  {"wave": 2, "description": "...", "cus": ["cu-003"]}
]
```

Rules: same-file CUs in different waves (dependency order);
dependencies flow forward; verification/E2E last.

### 7. Dry-run the waves and fix until green

Run the dry-run (the agent runs this itself — never the operational
command, which is the Designer's job):

```sh
python3 .LLMDD/tools/run-waves.py --itr .LLMDD/ITRS/<feature-name> --app <app> --waves .LLMDD/ITRS/<feature-name>/waves.json --dry-run
```

Green means all three: exit code 0, wave count and CU assignments
match `waves.json`, every CU resolves to a frame file. On any
failure, diagnose and fix, then re-run — repeat until green:

- Unresolvable CU → frame problem: go back to step 5 (recompile
  with `--force`, re-run `verify-cu-structure`).
- Wrong wave/CU listing or ordering complaint → plan problem: fix
  `waves.json` per the step 6 rules and re-run the dry-run.
- Runner usage error (bad flags, wrong `--app`) → fix the command.

Never proceed to steps 8–10 on a red dry-run. The fix loop has no
iteration cap — a valid dry-run is mandatory before handoff.

### 8. Write the wave command file

Write the exact operational command (no `--dry-run`, no
placeholders, `<feature-name>` and `<app>` filled in) as the sole
content of `.LLMDD/ITRS/<feature-name>/run-waves.txt`:

```sh
python3 .LLMDD/tools/run-waves.py --itr .LLMDD/ITRS/<feature-name> --app <app> --waves .LLMDD/ITRS/<feature-name>/waves.json
```

Include non-default flags (`--coder-model`, `--lead-model`, …) on
the same line when the run needs them.

### 9. Print the wave command

Print the identical one-line command from step 8 as the handoff for
the Designer to paste into a terminal (fish). Print only after the
step 7 dry-run is green. Never print it beforehand.

### 10. Commit

```sh
git add .LLMDD/ITRS/<feature-name>/<feature-name>.hypotheses.json .LLMDD/ITRS/<feature-name>/<feature-name>.probe-results .LLMDD/ITRS/<feature-name>/hypothesis-verification.txt .LLMDD/ITRS/<feature-name>.batch.json .LLMDD/ITRS/<feature-name>/ARCH.itr .LLMDD/ITRS/<feature-name>/LEGEND.itr .LLMDD/ITRS/<feature-name>/cu-*/ .LLMDD/ITRS/<feature-name>/waves.json .LLMDD/ITRS/<feature-name>/run-waves.txt .LLMDD/DTRS/<feature-name>/
git status --short
git commit -m "Prep: DTR plus ITR frames plus waves for <feature-name> (cu-001..cu-<NNN>)"
```

- Exclude runner runtime state (`<app>.feedback/`, `<app>.logs/`,
  `<app>.convergence.json`) — gitignored, never committed.
- Verify the commit lists only intended files. Never push (the
  Designer pushes; a pre-push hook blocks agents anyway).

## Rules

- Strict order: each step gates the next. A red gate stops the
  chain — fix and re-run from that step, never skip ahead.
- The step 3 hypothesis gate is hard: never draft or compile a batch CU
  until the engine returns `GREEN_LIGHT` for the feature's hypotheses.
- Never hand-edit a baseline DTR (re-run the builder) or a compiled
  frame (fix the batch, recompile).
- Never `git add -A`; stage only the spec, DTR, batch, frame,
  waves, and txt paths for this feature.
- The agent prepares and prints the wave command but never executes
  it.

## Verify

1. Spec paths clean after step 1.
2. DTR exists at `.LLMDD/DTRS/<feature-name>/` with `META.GENERATOR`
   and `FILE.`/`CODEX.`/`TYPE.` entries.
3. Hypothesis gate: engine emitted `GREEN_LIGHT`; inputs + verdict line
   committed with the feature's ITR artifacts.
4. `validate-steps.py` PASS for every regular CU.
5. `verify-cu-structure` four checks pass; compiler exit 0.
6. Waves dry-run (step 7 loop) exit 0 with expected wave/CU listing.
7. `run-waves.txt` content == printed command == operational command
   minus nothing.
8. Final commit contains hypothesis inputs + verdict + batch + frames +
   `waves.json` +
   `run-waves.txt` (+ DTR), and nothing else.

## Output

Committed prep for `<feature-name>` plus the printed wave command —
the Designer pastes it to start STEP5.

## Book tracking (MANDATORY)

Before committing in the current repo, if the current repo is `llm-driven-design` and `~/Repos/llmdd-book` exists:
1. Add a summary of changes to the llmdd-book (typically a new subsection under the current-version section of `docs/building-llm-driven-design.md`)
2. `git -C ~/Repos/llmdd-book add docs/building-llm-driven-design.md`
3. `git -C ~/Repos/llmdd-book commit -m "docs: <summary>"`
4. Never push from either repo
