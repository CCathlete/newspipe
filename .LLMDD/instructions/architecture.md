# Architecture

## The Team: Designer - Advisor - Coder - Code Lead

LLMDD models every development task as a team of four roles, not one:

| Role      | Node | Who      | Description |
|-----------|------|----------|-------------|
| Designer  | DSG  | Human    | Obtains baseline DTR (via dtr-builder), brainstorms with Advisor, approves CU content, runs itr-compiler, assigns CUs to Coders. |
| Advisor   | ADV  | AI agent | Receives baseline DTR, analyzes codebase structure, brainstorms with Designer. Never writes code — only drafts CU content for Designer approval. |
| Coder     | COD  | AI agent | Receives compiled CU frames, implements them in dependency order, writes per-CU feedback. Never changes design. |
| Code Lead | VER | AI agent | Monitors Coder feedback, picks up escalations, runs e2e tests, repairs cross-CU issues. Smarter model. |

The Designer works in the role of a tech lead: they obtain a baseline DTR
(either by extracting from existing code or creating a greenfield skeleton),
brief the Advisor, and together they decompose work into Computational Units
(CUs). Once approved, the Designer compiles CU content via `itr-compiler` and
assigns the resulting CU frames to Coders.

```
┌──────────┐   DTR    ┌──────────┐  CU content  ┌──────────────┐   frames
│ Designer │ ───────→ │ Advisor  │ ────────────→ │ ITR Compiler │ ──────┬────→ ┌──────────┐
│  (human) │ ←─────── │ (AI)     │ brainstorm    │   (tool)     │       │     │  Coder   │
└──────────┘ discuss  └──────────┘               └──────────────┘       │     │  (AI)    │
    │                                                                    │     └────┬─────┘
    │ runs dtr-builder                                                   │          │ per-CU
    ▼                                                                    │          │ feedback
  Codebase                                                         ┌─────┘          ▼
                                                                   │         ┌──────────────┐
                                                                   └────────→│  Code Lead   │
                                                                             │  (VER, AI)   │
                                                                             │ monitors,    │
                                                                             │ escalations, │
                                                                             │ e2e verify   │
                                                                             │ (parallel    │
                                                                             │  with COD)   │
                                                                             └──────────────┘
```

## LLMDD Tensor Pipeline

```
X (dtr-builder) → DTR (baseline) → ADV (advisor) ↔ DSG (designer) → CU content → ITR COMPILER → ITR (frames) → COD (coder) + VER (code lead) → OUT (code + feedback) → DTR (next iteration)
```

The pipeline loops: output feeds back into the design tensor for iterative
refinement.

## Node roles

Each node in the pipeline has strict rules:

| Node | Rules |
|------|-------|
| **X (dtr-builder)** | Walks source tree, extracts files/types/relations, produces flat DTR. Two modes: --root (extract) and --create-baseline (greenfield). |
| **DTR (Baseline Design Tensor)** | Flat KEY=VALUE file with sections: ARCH, META, FILE, CODEX, TYPE, REL. Serves as coordinate system for CUs. |
| **ADV (Advisor)** | Analyze only, no implementation. Brainstorms with Designer, drafts CU content. |
| **DSG (Designer)** | Decides on CU decomposition, approves CU content, runs itr-compiler, assigns CUs to Coders. No implementation. |
| **ITR COMPILER** | Tool that takes baseline DTR + CU content (JSON/YAML/JSONL/raw) and produces per-CU .itr frame files in `<app>.itr/` directory. |
| **ITR (Implementation Tensor)** | Directory of compiled CU frame files (cu-001.itr, cu-002.itr, ...) plus LEGEND.itr and ARCH.itr. |
| **COD (Coder)** | Implements CUs in dependency order. Writes per-CU feedback. Commits once per task. No design changes. |
| VER (Code Lead) | Verifier role. Monitors Coder feedback. Picks up escalations and performs fixes. Runs e2e tests. Repairs cross-CU issues until convergence. Writes verification report. |
| **OUT (Output)** | Produced code + per-CU FEEDBACK files. Feeds back into the next DTR cycle. |

## Tensor format

Every line is a complete semantic unit: `NAMESPACE.KEY=VALUE` or `KEY=VALUE`.
No brackets, no nesting. A misgenerated line kills only itself — no cascade.

```
SYS=LLMDDv8.0
MODE=DTR_BASELINE_X_CU_COMPILE
ARCH=HEX,DI,DIP,NO_CROSS_LAYER,PORT_FLOW_OUT_IN,HARD_FAIL
WORKFLOW.STEP1=OBTAIN_BASELINE_DTR
```

## Implementation Tensor (ITR) structure

An ITR is a **directory** at `itr-buffer/<app>.itr/` containing compiled
CU frame files:

```
<app>.itr/
├── LEGEND.itr          # Symbol definitions
├── ARCH.itr            # App-specific architecture config
├── cu-001.itr          # Compiled frame for CU 001
├── cu-002.itr          # Compiled frame for CU 002
└── ...

# Feedback lives in a parallel directory:
<app>.feedback/
├── cu-001.feedback.txt # Coder feedback for CU 001
├── cu-002.feedback.txt # Coder feedback for CU 002
└── ...
```

Each CU frame is compiled by `itr-compiler` from CU content + baseline DTR.

## Workflow

1. **OBTAIN_BASELINE_DTR** — Designer runs `dtr-builder` (extract or create)
2. **ANALYZE_AND_DECOMPOSE** — Advisor analyzes DTR, brainstorms with Designer
3. **DRAFT_CU_CONTENT** — Advisor drafts CU content, Designer approves
4. **COMPILE_ITR** — Designer runs `itr-compiler` to produce CU frame files
5. **IMPLEMENT_CUS** — Coder implements CUs in order, writes per-CU feedback, commits
6. **VERIFY** — Code lead monitors feedback, picks up escalations
7. **E2E_VERIFY** — Code lead runs e2e tests, repairs until convergence
8. **ITERATE** — Re-scan with dtr-builder, repeat

## Layer layout demands

Every app follows the hexagonal layer partition below. Folder-name casing
follows the implementation language: snake_case for Python, camelCase for
Scala/TS/JS, etc. (a Scala app uses `useCases`, `dependencyInjection`,
`entryPoint`, `valueObjects`; a Python app uses `use_cases`,
`dependency_injection`, `entry_point`, `value_objects`).

- **application** divides into `services`, `ports`, `useCases` — nothing else.
- **infrastructure** divides into one folder per application port (each port
  gets its adapter folder), plus an `Env` adapter singleton that loads config
  and env variables (system env takes precedence over loaded config).
- **domain** contains `models` and `valueObjects` only.
- **control** divides into `dependencyInjection` and `entryPoint` — nothing else.
- **No layer root holds files.** `application`, `infrastructure`, `domain`,
  and `control` contain only their nested folders; every source file lives in
  a nested folder. A file placed directly in a layer root is a constraint
  violation (`HARD_FAIL`).

## Constraints

- Hexagonal architecture (`HEX`)
- Dependency injection (`DI`)
- Dependency inversion (`DIP`)
- No cross-layer dependencies (`NO_CROSS_LAYER`)
- Port flow: outbound → inbound (`PORT_FLOW_OUT_IN`)
- DotEnv walk-up discovery (`DOTENV_WALKUP`)
- Per-CU feedback (`CODER_FEEDBACK`)
- Severity taxonomy: CRITICAL / MAJOR / MINOR / TRIVIAL (`SEVERITY`)
- One ITR directory per app, tracked in git (`ITR_LIFECYCLE`)
- Hard fail on constraint violation (`HARD_FAIL`)
- Fixed layer partition: application → services/ports/useCases,
  infrastructure → one folder per port + Env singleton,
  domain → models/valueObjects only,
  control → dependencyInjection/entryPoint (`LAYER_LAYOUT`)
- No files in layer roots, only in nested folders (`NO_LAYER_ROOT_FILES`)
- Infra Env adapter singleton loads config + env variables (`ENV_ADAPTER`)
- Python explicit declaration: every variable annotated at first binding (`PYTHON_EXPLICIT_DECLARATION`, MUST/CRITICAL)
- Python no-Any: `typing.Any` and bare generics forbidden, define custom types (`PYTHON_NO_ANY`, MUST/CRITICAL)
- Python slotted+frozen dataclasses preferred (`PYTHON_FROZEN_SLOTTED_DATACLASS`, SHOULD/MAJOR)
- Python monadic error handling preferred over try-except (`PYTHON_MONADIC_ERROR`, SHOULD/MAJOR)

## Python discipline (Python implementation code, `APP.LANGUAGE=Python`)

Non-Python tooling (e.g. Scala dtr-builder/itr-compiler) is exempt.

1. **Explicit declaration before definition (MUST/CRITICAL).** Every
   variable carries an explicit type annotation at its first binding:
   `name: Type = value`, or `name: Type` declared before `name = value`.
   Covers locals, attributes, function args and returns, loop targets.
   Bare `x = ...` on first binding is a `HARD_FAIL` violation. TypeVar definitions stay bare (`T = TypeVar(...)`) — annotating them is a linter error; the TypeVar itself is the declaration.
2. **No `Any` (MUST/CRITICAL).** `typing.Any` is forbidden. Bare
   `dict`/`list`/`tuple`/`set` without parameters count as implicit
   `Any` and are forbidden. Create unambiguous custom types instead:
   `NewType`, dataclass, `Enum`, `Literal`, `Union`, `TypeAlias`,
   `Protocol`. `object` as an escape hatch is forbidden.
3. **Slotted, frozen dataclasses preferred (SHOULD/MAJOR).** Use
   `@dataclass(slots=True, frozen=True)` over plain classes and mutable
   dataclasses. Domain value objects default to slotted+frozen.
   Deviation needs justification in CU content or per-CU feedback with
   Designer acknowledgement.
4. **Monadic containers over try-except (SHOULD/MAJOR).** Use
   `Result[T,E]`/`Either`/`Option`-style wrappers and `Optional`
   chaining for expected and domain failures. `try-except` is reserved
   for truly exceptional boundary errors (imports, interpreter,
   OS-level). Business-logic control flow via exceptions is forbidden.
   Deviation needs justification as above.

`EXPLICIT_DECLARATION` and `NO_ANY` violations are CRITICAL
(`HARD_FAIL`); `SLOTTED_FROZEN_DATACLASS` and `MONADIC_ERROR`
deviations are MAJOR by default.

See the system tensor at `.LLMDD/prompts/llm-driven-design-sys-prompt.itr.md` for
the complete specification.
