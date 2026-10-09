---
name: build-dtr
description: Produce a baseline DTR with the .LLMDD/tools/dtr-builder binary — extract from an existing codebase or generate a greenfield skeleton. Use at WORKFLOW STEP1 (OBTAIN_BASELINE_DTR) and STEP8 (re-scan after implementation).
---

# build-dtr

Produces the baseline Design Tensor (DTR): a flat `KEY=VALUE` file with
`ARCH`, `META`, `FILE`, `CODEX`, `TYPE`, `REL` sections. It is the coordinate
system every CU traces into. The builder is the prebuilt binary at
`.LLMDD/tools/dtr-builder` — the only supported interface.

## When to use

- WORKFLOW STEP1 (OBTAIN_BASELINE_DTR): before any design discussion.
- WORKFLOW STEP8 (ITERATE): re-scan the updated codebase for the next cycle.

## Modes

Existing codebase — extract:

```sh
.LLMDD/tools/dtr-builder --out .LLMDD/DTRS/<feature-name>/<app>.dtr --root <path-to-code>
```

Greenfield — skeleton from seed template (hexagonal
domain/application/infrastructure/control layout; app name is derived from
the `--root` basename, there is no `--app` flag):

```sh
.LLMDD/tools/dtr-builder --create-baseline-dtr --language <lang> --root <path> --out .LLMDD/DTRS/<feature-name>/<app>.dtr
```

`--root` defaults to the current directory; `--out` is required in extract
mode. Always store DTRs under `.LLMDD/DTRS/<feature-name>/` (one folder per
feature, holding that feature's DTRs).

## Options

- `--max-chunk-size <n>` — chunk threshold in bytes (default 1MB, or
  `DTR_MAX_CHUNK_SIZE` env var). Large codebases chunk automatically:
  `<path>-001.ext`, `<path>-002.ext`, …
- `--filter <glob>` — additional blocklist pattern (repeatable).
- `--no-dotenv` — skip `.env` file discovery.
- `--version` / `--help` — version / full usage.

## Rules

- One baseline per cycle; re-running overwrites the previous output.
- Never hand-edit a baseline DTR — re-run the builder instead.
- Confirm the `ARCH=` line matches the project constraints before handing
  the DTR to the Advisor.

## Verify

1. Exit code is 0 and the summary reports files/types/relations analyzed.
2. The `.dtr` file exists (plus `-NNN.ext` chunks for large trees).
3. Spot-check sections: `META.GENERATOR=dtr-builder`, at least one `FILE.` /
  `CODEX.` / `TYPE.` entry.

## Output

A `.dtr` file under `.LLMDD/DTRS/<feature-name>/`. Hand it to the Advisor for
STEP2 (ANALYZE_AND_DECOMPOSE) — CU `dtr-coordinates` must reference keys
from this DTR.
