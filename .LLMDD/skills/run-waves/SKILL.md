---
name: run-waves
description: Implement compiled CUs in parallel waves with code-lead review using .LLMDD/tools/run-waves.py. Use at WORKFLOW STEP5 (IMPLEMENT_CUS), after compile-itr produces .LLMDD/ITRS/<feature-name>/. The Advisor plans the waves file for maximum parallelism.
---

# run-waves

Executes compiled CUs from their `cu-<id>/` folders in waves: each wave launches one parallel Coder
per CU, waits for all to finish, then the code lead reviews and fixes
escalations before the next wave starts. Final wave: the lead commits
everything. The runner is `python3 .LLMDD/tools/run-waves.py` (Python 3.8+,
opencode CLI on PATH, git repo).

## When to use

WORKFLOW STEP5 (IMPLEMENT_CUS): `.LLMDD/ITRS/<feature-name>/` is compiled
and verified. Plan waves, dry-run, execute.

## Wave planning (Advisor)

The Advisor authors the waves file and places it in the ITR root:
`.LLMDD/ITRS/<feature-name>/waves.json`. The Advisor never executes the
runner. Goal: **as many parallel CUs per wave as possible**, subject to:

- **Same file → different waves.** CUs editing the same file must be
  serialized across waves in dependency order — a wave's CUs run in
  parallel and would clobber each other.
- **Dependencies flow forward.** A CU goes in a later wave than the CUs it
  depends on.
- **Verification/E2E last.** Verification CUs run after the CUs they verify;
  the E2E CU runs alone in the final wave.
- **Exclude `arch`/`legend`.** `ARCH.itr`/`LEGEND.itr` are context, not work
  — waves list regular CUs only.
- **Pack aggressively.** Independent CUs touching different files belong in
  the same wave, even if that means 6+ parallel Coders.

Waves file (`.LLMDD/ITRS/<feature-name>/waves.json`):

```json
[
  {"wave": 1, "description": "Independent domain models, full parallel (3 coders)", "cus": ["cu-001", "cu-002", "cu-003"]},
  {"wave": 2, "description": "Services depending on wave 1", "cus": ["cu-004", "cu-005"]},
  {"wave": 3, "description": "E2E cross-file verification", "cus": ["cu-006"]}
]
```

## Dry-run (Advisor)

The Advisor runs the dry-run itself and verifies the plan — the Designer is
only handed the operational command once the dry-run is valid:

```sh
python3 .LLMDD/tools/run-waves.py --itr .LLMDD/ITRS/<feature-name> --app <app> --waves .LLMDD/ITRS/<feature-name>/waves.json --dry-run
```

Valid means: exit 0, wave count and CU assignments match `waves.json`, every
CU resolves to a frame file. On success, commit `waves.json`, then hand off.

## Execution (Designer, in a terminal)

The Designer — never the agent — runs the operational command from the repo
root (given verbatim by the agent after a valid dry-run):

```sh
python3 .LLMDD/tools/run-waves.py --itr .LLMDD/ITRS/<feature-name> --app <app> --waves .LLMDD/ITRS/<feature-name>/waves.json
```

Omitting `--waves` auto-detects a **single wave with all CUs** — maximum
parallelism but zero ordering. Only safe when every CU is independent;
otherwise the Advisor must write an explicit waves file.

## Options

| Option | Default | Notes |
|--------|---------|-------|
| `--coder-model` | `opencode/muse-spark-1.3-contributor-free` | fast, parallel execution |
| `--lead-model` | `opencode/big-pickle` | smarter, reviews and fixes |
| `--coder-fallbacks` | `opencode/big-pickle` | repeatable |
| `--max-fix-iterations` | `3` | lead repairs per wave |
| `--cu-timeout` | `300s` | activity timeout per CU |
| `--lead-timeout` | `600s` | activity timeout for lead |
| `--project` | auto-detect | project root (via `.opencode/agents/`) |
| `--dry-run` | off | print plan, execute nothing |

## Rules

- The Designer dry-runs before every execution; confirm wave count and CU assignments.
- Never put same-file CUs in one wave.
- Monitor per-CU feedback during execution; the lead picks up `ESCALATION`
  status (handled by the runner's review step, not by re-running waves).
- One final commit per task is produced by the lead — do not commit
  mid-wave.

## Verify

1. Dry-run lists the expected waves and CUs, exit 0, and writes nothing.
2. Run outputs (`<app>.feedback/`, `<app>.logs/`, `<app>.convergence.json`)
   live INSIDE the ITR folder and are gitignored runtime state — never
   committed, never scattered beside it.
3. No `ESCALATION` left unhandled in feedback before moving to E2E verify.

## Handoff to Designer

After a valid dry-run and committing `waves.json`, the agent prints the
exact operational fish command for the Designer to paste (feature and app
names filled in — no placeholders, no `--dry-run`):

```fish
python3 .LLMDD/tools/run-waves.py --itr .LLMDD/ITRS/<feature-name> --app <app> --waves .LLMDD/ITRS/<feature-name>/waves.json
```

The execute command is given only after the dry-run is verified and
`waves.json` is committed. Never print it beforehand.

## Output

Implemented code, committed by the code lead. Per-CU feedback is consumed
during the run (untracked). Feeds
STEP6 (VERIFY) and STEP7 (E2E_VERIFY).
