---
name: compile-itr
description: Compile a Designer-approved batch CU file into per-CU .itr frames under .LLMDD/ITRS/<feature-name>/ using the .LLMDD/tools/itr-compiler binary. Use at WORKFLOW STEP4 (COMPILE_ITR), after CU content is approved and before CUs are assigned to Coders.
---

# compile-itr

Compiles approved CU content into per-CU `.itr` frame files. The compiler is
the prebuilt binary at `.LLMDD/tools/itr-compiler` — it is the only supported
interface (no source builds, no hand-written frames).

## When to use

WORKFLOW STEP4 (COMPILE_ITR): the Designer has approved a batch CU file
(drafted with the Advisor in STEP3). Compile it, verify the frames, then hand
the frame directory to the Coder.

## Prerequisites

- A batch CU file (JSON or YAML) with approved content.
- A feature name. Output goes to `.LLMDD/ITRS/<feature-name>/`.
- Baseline DTR path (optional, pass through when available).

## Batch file formats

CU IDs use zero-padded sequence numbers (`cu-001`, `cu-002`, …).
`cu-type: arch` / `cu-type: legend` produce `ARCH.itr` / `LEGEND.itr`.

JSON (`--json-content` — array of objects):

```json
[
  {"cu-id": "arch", "cu-type": "arch", "dtr-coordinates": [], "content": "Architecture rules..."},
  {"cu-id": "legend", "cu-type": "legend", "dtr-coordinates": [], "content": "Legend..."},
  {"cu-id": "cu-001", "dtr-coordinates": ["TYPE.com.app.domain.Model"], "content": "Implement..."}
]
```

YAML (`--yaml-content` — mapping of id to fields):

```yaml
arch:
  cu-type: arch
  dtr-coordinates: []
  content: "Architecture rules..."
legend:
  cu-type: legend
  dtr-coordinates: []
  content: "Legend..."
cu-001:
  dtr-coordinates: [TYPE.com.app.domain.Model]
  content: "Implement..."
```

## CU content bar (dummy-proof)

A batch is not compilable until every regular CU's content carries four
labeled sections with implementation-grade detail — the Coder must be able
to implement without asking questions:

- **REQUIREMENTS** — what part of the feature this CU adds, in one paragraph.
- **COORDINATES** — the exact DTR keys this CU touches (verified against the
  feature DTR, not guessed). Creation semantics: a key that does not exist
  yet means the CU creates it.
- **IMPLEMENTATION_STEPS** — numbered steps, each with: exact file path(s),
  what to add/change (code sketches or signatures, not prose like "extend
  validation"), what NOT to touch (neighboring modules, signatures relied
  on by others), and the command proving the step (`sbt compile`, focused
  `testOnly`, etc.). End with an explicit stop condition ("stop when X is
  green; Y and Z belong to other CUs").
- **ACCEPTANCE** — test IDs mapped to the feature spec, each with the exact
  command and expected result.

The Designer rejects the batch (no compile) when any CU lacks file paths,
code-level specifics, or verification commands in its steps, or when any
coordinate does not resolve in the feature DTR. Vague verbs
("handle", "extend", "improve") without a file + snippet are a reject.

## Commands (run from repo root)

```sh
# JSON batch
.LLMDD/tools/itr-compiler --compile --json-content <batch.json> --out-folder .LLMDD/ITRS/<feature-name>/ [--dtr <baseline.dtr>]

# YAML batch
.LLMDD/tools/itr-compiler --compile --yaml-content <batch.yaml> --out-folder .LLMDD/ITRS/<feature-name>/ [--dtr <baseline.dtr>]

# Single CU (no batch file)
.LLMDD/tools/itr-compiler --compile --raw-content "<text>" --cu-id <id> --out-folder .LLMDD/ITRS/<feature-name>/
```

## Rules

- **ARCH + LEGEND are mandatory.** The first compile into a fresh folder must
  include `arch` and `legend` CUs, otherwise the compiler hard-fails
  (`Missing required parts`, exit 1). Later batches may add regular CUs
  incrementally to the same folder once `ARCH.itr`/`LEGEND.itr` exist.
- **`--force` semantics (verified):** without `--force`, existing frame files
  are silently skipped — exit code stays 0 and the CU is still reported ✔,
  but the old file is untouched. For an intentional recompile, pass `--force`
  or compile into an empty directory.
- **Non-zero exit = hard fail.** Do not proceed to implementation; fix the
  batch file and recompile.
- **Prefer batch files.** Single-CU mode does not record DTR coordinates in
  the frame — use JSON/YAML batches for anything needing traceability.
- **`--jsonl-content` is not supported** by the bundled binary (compiles 0
  CUs). Use JSON or YAML.
- **Never hand-edit compiled frames.** Fix the batch file and recompile
  (with `--force`) instead.

## Verify

1. Batch gate: every regular CU has all four content sections; every
   coordinate resolves in the feature DTR (`python3 -c` check over batch +
   `.dtr` keys — zero missing or no compile).
2. Exit code is 0.
3. Expected layout: `ARCH.itr`, `LEGEND.itr` (+ `SYNOPSIS.itr`) at root, one
   `cu-<id>/` subdirectory per regular CU. Component-carrying CUs hold only
   the four component files — no per-CU `.itr` blob anywhere, no `.itr` at
   the root besides globals. (Component-less legacy batches keep one
   `<id>.itr` inside their folder.)
4. Spot-check one frame header inside its folder:
  `# CU-ID`, `# CU-TYPE`, `# TIMESTAMP`, `# DTR-COORDINATES`, then content.

## Output

A directory `.LLMDD/ITRS/<feature-name>/` with globals at the root and one
`cu-<id>/` folder per regular CU. Hand this directory to the Coder for
STEP5 (IMPLEMENT_CUS).

## Rebuilding the binary

`.LLMDD/tools/itr-compiler` is a build artifact (gitignored), not source.
After changing `itr-compiler/src`, rebuild from the module dir with
`cs launch sbt -- assembly` (or `sbt assembly` where installed) and copy
the resulting `itr-compiler/itr-compiler` over `.LLMDD/tools/itr-compiler`.
Verify with `sbt test` before rebuilding.
