# bug-5: Advisor writes without approval

- Status: Resolved
- Created: 2026-10-16
- Resolved: 2026-10-18
- Bug ID: bug-5-advisor-writes-without-approval

## Bug detail

Symptoms: the Advisor role is specified as analyze-only / never writes
application code (`.opencode/agents/llmdd/advisor.md`: `edit: deny`;
`RULE.ADV=ANALYZE_ONLY,NO_IMPLEMENTATION` in
`.LLMDD/prompts/llm-driven-design-sys-prompt.itr.md`), yet nothing
deterministically enforces the "ask Designer for explicit, specific approval
before obtaining code access" requirement. An Advisor session can perform
code changes without that approval, silently violating the separation
of roles (Designer approves CU content, Coder implements).

Reproduction (from repo root, read-only):

```sh
sed -n '1,10p' .opencode/agents/llmdd/advisor.md
# EXPECTED: a deterministic gate — e.g. Advisor has no write-capable tools
#   at all (no edit, no bash/file-writes), or a permission-escalation hook
#   that blocks on missing Designer approval token.
# ACTUAL: frontmatter grants `bash: allow` alongside `edit: deny`, and the
#   no-implementation rule lives only in prompt text (agent md + system
#   tensor). Bash is a write-capable channel (`echo x > file`, `apply_patch`,
#   `python3 -c "open(...,'w')..."`), so the constraint is advisory, not
#   enforced. No approval-token check exists anywhere in the repo.
grep -rn "approval" .opencode/agents/llmdd/advisor.md .LLMDD/prompts/llm-driven-design-sys-prompt.itr.md | head
# EXPECTED: a machine-checkable approval requirement.
# ACTUAL: no hits — approval is an unwritten convention, not a gate.
```

Environment: repo checkout at `.opencode/agents/llmdd/advisor.md`,
`.LLMDD/prompts/llm-driven-design-sys-prompt.itr.md` lines ~193-195.

Suspected cause: the read-only posture was implemented as prompt
instruction (`ANALYZE_ONLY`, `NO_IMPLEMENTATION`, `edit: deny`) rather
than as capability removal + deterministic enforcement. `bash: allow`
(needed for read-only inspection) re-opens the write path, and there is
no approval-grant mechanism (explicit/specific Designer sign-off) for an
Advisor session to request or the harness to verify.

Impact: breaks the core LLMDD guarantee that design sessions are
side-effect-free until the Designer approves CU content and hands
compiled frames to a Coder. Any write performed by an Advisor bypasses
DTR-traceability, severity baselining, and Coder/Verifier review.

Resolved on 2026-10-18 by Designer decision. The approval mechanism this
bug asked for was built and merged; the residual gap is that the
mechanism is honoured by prompt adherence rather than mechanically
enforced. That residual is carried forward as bug-7 and bug-8, which
remain Open and are the enforcement work. Closing this bug records
that the mechanism exists, works in practice, and is no longer blocking -
it does NOT assert that the Advisor is technically prevented from writing.

## Fix summary (Resolved only)

Shipped by bugfix-feature-5-enforce-advisor-read-only-approval. Fixing
commits: `d220b97` (waves 1+2, cu-001 + cu-002), `028e045` (prep,
cu-003 + cu-004), `d6d1c2e` (wave 2, cu-004), spec in `eaaba1f`.

What changed:

- A scoped approval-token schema at `.LLMDD/approvals/<id>.json`
  requiring `id`, `files`, `commands`, `reason`, `granted_by`, `expires`.
  Scope is enforced by both file and command lists, so a token granting
  a read grant cannot widen into a write grant.
- `.LLMDD/tools/check-advisor-approval.py`, a typed validator with an
  explicit exit contract (0 allow, 1 deny, 2 usage). Obeys the repo's own
  ARCH.PYTHON.* rules: slotted frozen dataclass, TypeAlias not Any,
  explicit declarations, mypy-strict clean.
- `RULE.ADV.APPROVAL_REQUIRED` added to the system tensor
  (llm-driven-design-sys-prompt.itr.md:196) with machine-checkable
  semantics.
- A widened read-only bash allowlist in the Advisor agent frontmatter,
  plus an "Approval gate" section stating that prompt text never grants
  approval and silence is denial.
- Two standing tokens granted by the Designer: `llmdd-workspace` (design
  artifacts and .LLMDD tools) and `git-global` (recording only, empty
  file list so it can never widen what may be edited).

## Tests proving resolution (Resolved only)

Partially green. Stated exactly, including what does not pass.

(1) PASSING - approval requirement has machine-checkable semantics.
    `grep -n "RULE.ADV.APPROVAL_REQUIRED" .LLMDD/prompts/llm-driven-design-sys-prompt.itr.md`
    returns the rule at line 196 naming the token path, the validating
    script, and the exit-code contract. This is the original bug's
    suggested test (3), and it now passes.

(2) PASSING (live) - the gate is actually exercised in practice. On
    2026-10-18 an Advisor session authored three bug specs only after
    validating its token first:
    `python3 .LLMDD/tools/check-advisor-approval.py --approval llmdd-workspace --files ...`
    returned `ALLOWED: llmdd-workspace`, exit 0. In the same session the
    Advisor declined a branch deletion as out of token scope. The
    mechanism is wired into behaviour, not just written down.

(3) NOT PASSING - negative e2e: a write without a token is NOT denied
    deterministically. Nothing intercepts the write; denial depends on the
    model choosing to run the gate script and honour its exit code. This
    is the original bug's suggested test (2), and it fails.

(4) PASSING - static assertion that the Advisor is granted no
    write-capable tool. An Advisor session's live tool set is
    `execute, glob, grep, question, read, shell, skill, subagent,
    webfetch, websearch` — there is no edit, write or patch tool to
    grant. The edit channel is closed by capability removal, which is
    stronger than the declared `edit: deny`.

    (An earlier draft of this entry recorded this as failing, on the
    claim that `edit: deny` was "not loaded as a permission". That claim
    was wrong and was withdrawn on 2026-10-18: `advisor.md` is loaded as
    the agent definition, and `permission` is a valid `AgentConfig` key
    whose `PermissionConfig` supports `edit` and `bash` with values
    "ask" | "allow" | "deny".)

    The residual write channel is `bash`, which can write via
    `cat > file`, `python3` or `git commit` despite the allowlist. That
    is tracked by bug-7 (autoaccept) and bug-8 (advisory gate), not here.

(5) NOT PASSING - an agent-level `"*": ask` never surfaces, because the
    user-global config sets `session.permissions: "autoaccept"` (bug-7).

Tests (3) and (5) become achievable only when bug-7 and bug-8 are fixed - chiefly by moving the gate out-of-band of the model as
a real `pre-commit` hook. Until then this closure rests on prompt
adherence plus a detective control (the change is visible in `git diff`
to the code lead and the Designer). That is a deliberate, accepted
residual for now, not an oversight.
