---
name: prepare-implementation
description: Run the full pre-implementation chain for one feature — commit pending specs, build DTR, draft a dummy-proof batch CU, compile ITR, plan waves, write the wave command to a txt file, print it, and commit. Use when the Designer says implementation is ready to be prepared.
---

# prepare-implementation

End-to-end prep for WORKFLOW STEP5 (IMPLEMENT_CUS). Chains the
single-purpose skills in order: spec commit → `build-dtr` →
batch CU → `compile-itr` → waves plan → dry-run fix loop →
`run-waves.txt` → print → commit. The agent does everything up to
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

### 3. Draft the batch CU (dummy-proof)

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

### 4. Compile ITR (`compile-itr`)

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

### 5. Create waves (`run-waves` planning)

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

### 6. Dry-run the waves and fix until green

Run the dry-run (the agent runs this itself — never the operational
command, which is the Designer's job):

```sh
python3 .LLMDD/tools/run-waves.py --itr .LLMDD/ITRS/<feature-name> --app <app> --waves .LLMDD/ITRS/<feature-name>/waves.json --dry-run
```

Green means all three: exit code 0, wave count and CU assignments
match `waves.json`, every CU resolves to a frame file. On any
failure, diagnose and fix, then re-run — repeat until green:

- Unresolvable CU → frame problem: go back to step 4 (recompile
  with `--force`, re-run `verify-cu-structure`).
- Wrong wave/CU listing or ordering complaint → plan problem: fix
  `waves.json` per the step 5 rules and re-run the dry-run.
- Runner usage error (bad flags, wrong `--app`) → fix the command.

Never proceed to steps 7–9 on a red dry-run. The fix loop has no
iteration cap — a valid dry-run is mandatory before handoff.

### 7. Write the wave command file

Write the exact operational command (no `--dry-run`, no
placeholders, `<feature-name>` and `<app>` filled in) as the sole
content of `.LLMDD/ITRS/<feature-name>/run-waves.txt`:

```sh
python3 .LLMDD/tools/run-waves.py --itr .LLMDD/ITRS/<feature-name> --app <app> --waves .LLMDD/ITRS/<feature-name>/waves.json
```

Include non-default flags (`--coder-model`, `--lead-model`, …) on
the same line when the run needs them.

### 8. Print the wave command

Print the identical one-line command from step 7 as the handoff for
the Designer to paste into a terminal (fish). Print only after the
step 6 dry-run is green. Never print it beforehand.

### 9. Commit

```sh
git add .LLMDD/ITRS/<feature-name>.batch.json .LLMDD/ITRS/<feature-name>/ARCH.itr .LLMDD/ITRS/<feature-name>/LEGEND.itr .LLMDD/ITRS/<feature-name>/cu-*/ .LLMDD/ITRS/<feature-name>/waves.json .LLMDD/ITRS/<feature-name>/run-waves.txt .LLMDD/DTRS/<feature-name>/
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
3. `validate-steps.py` PASS for every regular CU.
4. `verify-cu-structure` four checks pass; compiler exit 0.
5. Waves dry-run (step 6 loop) exit 0 with expected wave/CU listing.
6. `run-waves.txt` content == printed command == operational command
   minus nothing.
7. Final commit contains batch + frames + `waves.json` +
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
