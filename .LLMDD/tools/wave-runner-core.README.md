# Wave Command

Execute CUs in parallel waves with code lead review.

## Usage

```bash
python3 .LLMDD/tools/run-waves.py --itr <itr-path> --app <app-name> [--waves <waves-json>]
```

## Options

| Option | Description | Default |
|--------|-------------|---------|
| `--itr <path>` | Path to compiled ITR directory | Required |
| `--app <name>` | Application name | Required |
| `--waves <json>` | Wave definition file | Auto-detect |
| `--coder-model <m>` | Model for coders | groq/openai/gpt-oss-120B |
| `--lead-model <m>` | Model for code lead | nvidia/nvidia/nemotron-3.5-lightning-30b-a3B |
| `--coder-fallbacks <m>` | Fallback models if primary fails | opencode/big-pickle |
| `--max-fix-iterations <n>` | Max fix iterations per wave | 3 |
| `--cu-timeout <s>` | Activity timeout per CU | 180 |
| `--lead-timeout <s>` | Activity timeout for lead | 600 |
| `--dry-run` | Show what would be executed | false |

## Wave Structure

Each wave:
1. Launches parallel coders (one per CU)
2. Waits for all coders to finish
3. Launches code lead to review and fix escalations
4. Moves to next wave

Final wave: code lead commits everything.

## Wave Definition Files

- `llmdd-v8-waves.json` — LLMDD v8 implementation (12 CUs, 4 waves)

## CU Decomposition (llmdd-v8)

| Wave | CUs | Parallelism | Description |
|------|-----|-------------|-------------|
| 1 | cu-001, cu-002, cu-003 | 3 serial | System tensor edits (same file) |
| 2 | cu-004–cu-009 | **6 parallel** | Agent files + documentation |
| 3 | cu-010, cu-012 | 2 parallel | Verification tests |
| 4 | cu-011 | 1 | E2E pipeline test |

## Examples

```bash
# Dry run
python3 .LLMDD/tools/run-waves.py \
  --itr .LLMDD/ITRS/<feature-name> \
  --app <app> \
  --waves <waves-json> \
  --dry-run

# Execute
python3 .LLMDD/tools/run-waves.py \
  --itr .LLMDD/ITRS/<feature-name> \
  --app <app> \
  --waves <waves-json>
```

## Output

- `<app>.feedback/` — Per-CU feedback files
- `<app>.logs/` — Execution logs
- `<app>.convergence.json` — Convergence state

## Models

- **Coders**: opencode/muse-spark-1.3-contributor-free (verified working, fast, parallel execution)
- **Code Lead**: opencode/big-pickle (smarter, reviews and fixes)
- **Fallback**: opencode/big-pickle
