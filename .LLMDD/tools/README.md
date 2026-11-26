# .LLMDD/tools

Repo-agnostic tooling carried by the .LLMDD standard. Every tool derives
the repository root from its own location, so the folder works unchanged
in any repository that carries a .LLMDD folder.

## Tool map

| Tool | Purpose | Phase |
|------|---------|-------|
| `next-spec-id.py` | Allocate unique bug/feature serials | spec |
| `check-advisor-approval.py` | Enforce the Advisor approval gate | governance |
| `dtr-builder` | Extract a baseline DTR from source | DTR (STEP1/STEP8) |
| `itr-compiler` | Compile an approved CU batch into ITR frames | compile (STEP4) |
| `validate-steps.py` | Mechanical dummy-proof gate on CU steps | compile (STEP4) |
| `verify-cu-structure` | *(skill, not a tool)* check compiled frame layout | compile (STEP4) |
| `hypothesis-engine.jar` | Ledger-backed hypothesis evaluator | hypothesis gate (STEP3) |
| `check-hypothesis-report.py` | Enforce the verbatim ledger-echo gate | hypothesis gate (STEP3) |
| `skill-check.py` | Deterministic skill executor/enforcer | authoring |
| `run-waves.py` | Execute compiled CUs in parallel waves | implement (STEP5) |
| `cu_scope.py` | Internal helper imported by `validate-steps.py` | internal |
| `test_next_spec_id.py`, `test_synopsis_budget.py` | Fixture/standalone contract checks | internal |
| `wave-runner-core.README.md` | Detailed wave-runner reference | reference |

The tools follow the pipeline order in
`.LLMDD/prompts/llm-driven-design-sys-prompt.itr.md`; see each section below.

## next-spec-id.py

Allocates the next bug or feature serial for this repository. It reads
the plain-text counter at `.LLMDD/spec-driven/ids.txt` plus every git ref
under `refs/heads` and `refs/remotes`, so two branches cut from the same
state of dev never receive the same number.

```sh
python3 .LLMDD/tools/next-spec-id.py --kind bug
python3 .LLMDD/tools/next-spec-id.py --kind feature --prefix bugfix-feature-
python3 .LLMDD/tools/next-spec-id.py --kind feature --dry-run
python3 .LLMDD/tools/next-spec-id.py --check
```

`--kind` prints the id and bumps the counter. `--prefix` chooses the id
lead, so a bug or bugs fix prints `bugfix-feature-<n>-` from the feature
sequence. `--dry-run` prints without writing. `--check` never writes and
fails on a duplicate serial, on an artifact directory with no matching
spec file, or on a counter behind a serial present in the tree.

Exit codes follow the convention shared with `check-advisor-approval.py`:
0 clean or id printed, 1 check violation, 2 usage error.

## check-advisor-approval.py

Validates a Designer-issued approval token against a proposed change. The
Advisor must call this **before any other action** in a response that changes
files (RULE.ADV.APPROVAL_GATE_CONTRACT); prompt text never grants approval.

```sh
python3 .LLMDD/tools/check-advisor-approval.py --approval <id> \
  --files <changed-path> ... --commands "<write-command>" ...
```

`--approval` is a token id under `.LLMDD/approvals/` (or a direct path).
`--files` lists changed tracked paths; `--commands` lists write commands.
A token entry ending in ` *` matches that command and any arguments; file
entries ending in `/*` cover a directory and its contents, and `!`-prefixed
entries are prohibitions that always win. Exit 0 = `ALLOWED`, 1 = `DENIED`
(reason on stderr), 2 = usage error.

## dtr-builder

Extracts a baseline DTR (`FILE.`/`CODEX.`/`TYPE.`/`REL.` entries) from a source
tree. It is a native binary (`dtr-builder` / `.bat`).

```sh
# Extract mode: point --root at ONE source subtree
.LLMDD/tools/dtr-builder \
  --out .LLMDD/DTRS/<feature>/<app>.dtr \
  --root <path-to-source-subtree> \
  --filter 'venv/' --filter '.env' --filter 'node_modules/' \
  --no-dotenv

# Baseline skeleton mode (greenfield)
.LLMDD/tools/dtr-builder --create-baseline-dtr \
  --root <path> --language <lang> --out <path>.dtr
```

Other flags: `--max-chunk-size <n>` (default 1MB, or `DTR_MAX_CHUNK_SIZE`),
`--filter <glob>` (repeatable), `--no-dotenv` (skip `.env` discovery),
`--version`. Single chunk writes `<path>`; multiple chunks write
`<path>-001.ext`, `-002.ext`, ...

### Current practice: thin DTRs from multiple roots, not one big inclusive DTR

Build **small, focused DTRs**, each from a single `--root` that covers the code
a feature actually touches. When work spans several areas, run the builder
once per root to produce several thin DTRs rather than scanning the whole repo
into one inclusive DTR. A giant DTR is noisy, slow to produce, and dilutes the
coordinates the hypothesis gate maps evidence onto. A creation coordinate
(a key absent from the DTR) is legitimate; every other `dtr_coordinates` entry
must be an exact key from a thin DTR you built.

### Glob support is limited — skip libraries explicitly

The `--filter` matcher is **not** a general glob engine. It understands only
three forms: `*.ext` (suffix match on the filename), a trailing `/` (substring
match on the path), and an exact filename / path-segment. It does **not**
support `**`, `?`, brace expansion or other shell-glob features. The built-in
blocklist covers `.git/`, `node_modules/`, `target/`, `build/`, `dist/`,
`.next/`, `.cache/`, common binaries and images, and `*.pyc`/`*.pyo`, but it
does **not** cover `venv`, `.venv`, `.env`, `site-packages`, `__pycache__` or
many other generated/library trees, and its matching is only as good as those
three forms.

So **always pass explicit `--filter` entries for every virtualenv, `.env`,
`node_modules`, `__pycache__`, `site-packages`, and other vendored library or
dependency tree you do not want scanned** — the builder will not reliably skip
them on its own. Use `--no-dotenv` as well when you do not want `.env` contents
loaded into the environment. Verify the result with
`grep -c '^FILE\.' <dtr>` and by checking that no dependency paths appear.

Never hand-edit a generated DTR; rebuild it with the builder.

## itr-compiler

Compiles a Designer-approved batch (CU content) plus a baseline DTR into
per-CU ITR frames under an output folder.

```sh
.LLMDD/tools/itr-compiler --compile \
  --json-content .LLMDD/ITRS/<feature>.batch.json \
  --out-folder .LLMDD/ITRS/<feature>/ \
  --dtr .LLMDD/DTRS/<feature>/<app>.dtr
```

`--compile` is required. Input is one of `--json-content <path>` (batch),
`--yaml-content <path>` (batch) or `--raw-content <string>` with `--cu-id <id>`
(single CU). `--out-folder` defaults to `out/`; `--force` overwrites existing
CU files. Output is `ARCH.itr`, `LEGEND.itr` and one `cu-NNN/` folder per CU
(its four section files), with DTR coordinates attached. Run
`validate-steps.py` on the batch **before** compiling; run the
`verify-cu-structure` skill checks after.

## validate-steps.py

Mechanical dummy-proof gate on CU implementation steps. Run it on the batch
or on a compiled frame root; exit 0 passes, 1 fails:

```sh
python3 .LLMDD/tools/validate-steps.py .LLMDD/ITRS/<feature>.batch.json
python3 .LLMDD/tools/validate-steps.py .LLMDD/ITRS/<feature>/
```

Every regular CU must satisfy the F-rules: F1 numbered steps with an exact
repo-rooted file path; F2 a fenced code block for every file-changing step and
no `...`/`…` placeholder in fences; F3 at least one `Prove:` line per step;
F4 a `Do NOT touch:` list and a `Stop when:` condition; F5 no vague verbs
("handle", "improve", ...) without a fence+path; F6 no `TBD`/`TODO`/`FIXME`/
`XXX`/`e.g.`/`etc.` placeholders. Note F1 splits on numbered lines even
inside fences, so embed skill/doc bodies with `- Step N:` bullets rather than
`1.` lines. See `.LLMDD/skills/validate-implementation-steps/SKILL.md`.

## check-hypothesis-report.py

Hard gate proving the engine's JSON report block was echoed verbatim into the
conversation record. Used by `@hypothesize` and `prepare-implementation` step 3.

```sh
python3 .LLMDD/tools/check-hypothesis-report.py \
  --report .LLMDD/ITRS/<feature>/<feature>.ledger-report.jsonl \
  --conversation .LLMDD/ITRS/<feature>/<feature>.hypothesis-conversation.md \
  --stage 1 --verdict GREEN_LIGHT
```

`--stage` is the stage number; `--verdict` is `GREEN_LIGHT` (mapped to ledger
`GREEN`), `RED_LIGHT` (`RED`) or `ADVISOR_ACTION_REQUIRED` (`BLOCKED`). Pass
means `CHECK_OK stage=<n>` and exit 0; a missing/non-identical echo prints
`CHECK_FAILED <reason>` and fails closed. The engine prints one JSON block and
appends the byte-identical block to the ledger report file; echo it verbatim
into both the conversation and the conversation record before calling this.

## hypothesis-engine.jar

Fat jar for the Hypothesis Engine — the ledger-backed stage evaluator used by
the `@hypothesize` skill and by `prepare-implementation` step 3. Built from the
Scala project in `hypothesis-engine/`; it embeds the sqlite JDBC driver and the
ledger schema, so no classpath or driver setup is needed. The jar is
gitignored (built artifact), so rebuild it locally after changing engine
source:

```sh
cd hypothesis-engine && sbt -batch assembly
# writes ../.LLMDD/tools/hypothesis-engine.jar
```

Verify the shipped CLI:

```sh
javap -p -cp .LLMDD/tools/hypothesis-engine.jar 'hypothesisengine.control.Main$' \
  | grep -E 'runInit|runCleanup'
```

### Commands

```sh
# Examine: evaluate hypotheses against experiments (the normal gate run)
java -jar .LLMDD/tools/hypothesis-engine.jar \
  <hypotheses.json> <experiments.jsonl>

# Initialise an empty ledger at a path
java -jar .LLMDD/tools/hypothesis-engine.jar init [path]
# -> INIT_OK path=<absolute path>

# Clean ledger data for one hypothesis, or all of it
java -jar .LLMDD/tools/hypothesis-engine.jar cleanup <hypothesis-id>
java -jar .LLMDD/tools/hypothesis-engine.jar cleanup --all
# -> CLEANUP_OK hypothesis=<id> / CLEANUP_ALL_OK
```

With no subcommand the jar runs the examine path. The ledger location defaults
to `ledger.db` and can be overridden with `-Dhypothesis-engine.ledger.path=...`.
`sbt -batch cleanLedger` is a separate build task that resets the source
project's `ledger.db` for a fresh start; it is not a jar subcommand.

The verdict report is printed as one JSON block and appended byte-identically
to `<feature>.ledger-report.jsonl`. `check-hypothesis-report.py` (same folder)
enforces the verbatim-echo hard gate; see `.LLMDD/skills/hypothesize/SKILL.md`
for the full procedure, artifact paths and stop boundary.

## skill-check.py

Deterministic skill executor/enforcer. Resolves a skill by name under
`.LLMDD/skills/<name>/SKILL.md` and emits its prompt, plan or enforcement
checklist.

```sh
python3 .LLMDD/tools/skill-check.py hypothesize --print-prompt
python3 .LLMDD/tools/skill-check.py prepare-implementation --plan
python3 .LLMDD/tools/skill-check.py <skill> --enforce   # checklist
python3 .LLMDD/tools/skill-check.py <skill> --json
```

`--print-prompt` prints the execution prompt with a `## Skill File:` header —
use it to confirm a skill resolves to the expected SKILL.md. A focused check:
`python3 .LLMDD/tools/skill-check.py <skill> --print-prompt | grep 'Skill File'`.

## run-waves.py

Executes compiled CUs in parallel waves with a code lead reviewing each wave.
This is the STEP5 implementation runner; the Advisor only ever runs it with
`--dry-run` — the operational command belongs to the Designer.

```sh
python3 .LLMDD/tools/run-waves.py \
  --itr .LLMDD/ITRS/<feature> --app <app> \
  --waves .LLMDD/ITRS/<feature>/waves.json --dry-run
```

Required: `--itr` (compiled ITR directory), `--app` (application name).
Optional: `--waves` (wave definition JSON; auto-detected if omitted),
`--coder-model`, `--coder-fallbacks`, `--lead-model`, `--lead-fallbacks`,
`--max-fix-iterations`, `--cu-timeout`, `--lead-timeout` (activity-based),
`--project` (root, auto-detected from `.opencode/agents/`), `--no-per-cu-lead`,
`--no-color`, and `--dry-run` (print the plan, execute nothing).

Runtime state (`repo.feedback/`, `repo.logs/`, `repo.convergence.json`) is
written under the ITR folder and is gitignored — read it, never commit it.
A green `--dry-run` is the last prep gate; the wave command written to
`run-waves.txt` is for the Designer to paste, not for the Advisor to run.
See `wave-runner-core.README.md` in this folder for the full option table and
wave lifecycle.

## Internal helpers (not normally invoked directly)

- `cu_scope.py` — shared vocabulary imported by `validate-steps.py` to parse
  `Do NOT touch:` lists into matchable glob patterns and to extract declared
  write paths. Not a standalone CLI.
- `test_next_spec_id.py` — fixture test for `next-spec-id.py`; builds a
  throwaway repo under `/tmp/opencode` and checks ids stay unique across
  sibling branches.
- `test_synopsis_budget.py` — standalone contract check that parses
  `MAX_ATTEMPTS=<int>` from a `SYNOPSIS.itr` path and prints `BUDGET_OK <n>`.
- `wave-runner-core.README.md` — detailed reference for the wave runner.
