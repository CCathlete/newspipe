# LLM-Driven Design (LLMDD)

This project explores AI-assisted software design through structured system
tensors. Every design decision, architecture constraint, and implementation
step is encoded in `.itr` (implementation tensor) files under `system_tensors/`.

## The Triad

Every development task is a team of four:

| Role     | Who             | Responsibility |
|----------|-----------------|----------------|
| Designer | Human           | Obtains baseline DTR, brainstorms, approves CU content, runs itr-compiler, assigns CUs |
| Advisor  | AI agent        | Analyzes baseline DTR, discusses design, drafts CU content |
| Coder    | AI agent        | Receives compiled CU frames, implements in order, writes per-CU feedback |
| Code Lead | VER | AI agent | Monitors Coder feedback, handles ESCALATION status, picks up escalations, runs e2e tests, repairs cross-CU issues |

The Designer opens an **Advisor session** for design work, a **Coder session** for implementation, and a **Verifier session** for verification and e2e testing. See `.opencode/agents/advisor.md`, `.opencode/agents/coder.md`, and `.opencode/agents/verifier.md`.

## Workflow

The LLMDD pipeline is defined in `system_tensors/llm-driven-design-sys-prompt.itr`:

1. **Obtain Baseline** — Designer runs `dtr-builder` to produce a baseline DTR (extract from existing code or --create-baseline for greenfield)
2. **Analyze & Decompose** — Advisor analyzes the DTR and brainstorms with Designer, decomposing work into Computational Units (CUs)
3. **Draft CU Content** — On green light, Advisor drafts CU content (JSON/YAML format) with CU-ID, DTR coordinates, and implementation content
4. **Compile ITR** — Designer runs `itr-compiler` to compile CU content + baseline DTR into per-CU `.itr` frame files
5. **Implement** — Coder receives compiled CU frames, implements in dependency order, writes per-CU feedback, and commits
6. **Verify** — Code lead monitors feedback, picks up ESCALATION items
7. **E2E Verify** — Code lead runs e2e tests, repairs until convergence
8. **Iterate** — Re-scan with dtr-builder, repeat

## Architecture rules

- Hexagonal architecture, dependency injection, dependency inversion
- No cross-layer dependencies
- Port flow: outbound → inbound
- Layer order: domain > application > infrastructure > control

## Knowledge base

Project-specific knowledge is stored in `.opencode/instructions/`:
- `memory.md` — persistent session context
- `architecture.md` — triad, pipeline, constraints, tensor format

## OpenCode agents

Four primary agents are defined in `.opencode/agents/`:

- **advisor** — design partner; reads only, never edits; thinks and drafts
- **coder** — implementation engine; reads and writes, never designs
- **verifier** — code lead; monitors feedback, picks up escalations, runs e2e tests; smarter model

Use `opencode --agent advisor` or `opencode --agent coder` to start a
session in the respective role. Use `opencode --agent verifier` for
verification and e2e testing sessions.

## Source of truth

The master specification is `system_tensors/llm-driven-design-sys-prompt.itr`.
The `.opencode/agents/` files are convenience wrappers. Any agent or chat
assistant that reads the system tensor at session start understands the LLMDD
standard.

## Pre-push hook

A global pre-push hook blocks AI assistants from pushing to remote (installed
at `~/.config/git/hooks/pre-push`). Only interactive terminal users may push.

## Commit protocol

Every commit in this repo must be accompanied by a summary of the changes
tracked in the `~/Repos/llmdd-book` repo:

1. After committing here, record what changed in the book — typically a new
   subsection under the current-version section of
   `docs/building-llm-driven-design.md` (workspace, skills, features, ITR
   layout, lessons learned).
2. Commit the book change separately in `~/Repos/llmdd-book` with a `docs:`
   message.
3. Never push from either repo — the Designer pushes.
