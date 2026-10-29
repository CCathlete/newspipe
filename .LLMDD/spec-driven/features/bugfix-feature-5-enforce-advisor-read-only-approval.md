# bugfix-feature-5: Enforce advisor read-only approval

- Status: Implemented
- Created: 2026-10-16
- Feature ID: bugfix-feature-5-enforce-advisor-read-only-approval

## Synopsis

Motivation: bug-5 (`bug-5-advisor-writes-without-approval`) — the Advisor
role is specified as analyze-only / never writes application code, but the
constraint lives only in prompt text plus `edit: deny`, while frontmatter
still grants blanket `bash: allow`. Bash is a write-capable channel, so an
Advisor session can perform code changes without explicit, specific
Designer approval, silently breaking the guarantee that design
sessions are side-effect-free until the Designer approves CU content. This
feature adds deterministic enforcement, not prose: least-privilege Advisor
capabilities by default (read-only inspection plus scratch creation —
directories and text files outside tracked code), plus a scoped approval
token for the exceptional code-change case. Every code change (any
modification to a git-tracked file) requires a token file
`.LLMDD/approvals/<id>.json` naming files plus commands plus reason plus
expiry, and a gate script `.LLMDD/tools/check-advisor-approval.py` that
exits non-zero without a valid, unexpired, in-scope token. It serves the
Designer (side-effect-free design sessions, scoped auditable grants) and
downstream Coder/Verifier review (no untraced writes).

Scope: In: `.opencode/agents/llmdd/advisor.md` permission frontmatter
(read-only bash allowlist — `ls`, `cat`, `rg`, `grep`, `head`, `tail`,
`find`, `wc`, `file`, `stat`, full `git` — plus `mkdir`/`touch` for
scratch creation, default deny/ask), the approval-token schema
(`.LLMDD/approvals/<id>.json`: files, commands, reason, granted-by,
expiry), the gate script (`.LLMDD/tools/check-advisor-approval.py`,
exit non-zero without a valid in-scope token), and the matching
`RULE.ADV.*` / system-tensor wording in
`.LLMDD/prompts/llm-driven-design-sys-prompt.itr.md` where the
enforcement semantics change. In: static audit + gate-script unit checks
+ negative e2e proving the gate. Out: `coder.md` / `verifier.md` grants,
`dtr-builder`, `itr-compiler`, `run-waves`, skill-doc rewrites except
where the approval mechanism itself is documented. Fixes
`bug-5-advisor-writes-without-approval`. Code means git-tracked files;
scratch means untracked paths (`itr-buffer/`, `/tmp/opencode/`).

## Implementation status

Implemented. All four CUs (`cu-001` read-only frontmatter, `cu-002`
`RULE.ADV.APPROVAL_REQUIRED` + Approval-gate prose, `cu-003` approval-token
schema + gate script, `cu-004` widened frontmatter) are merged: `d220b97`
(waves 1+2, cu-001 + cu-002), `028e045` (prep, cu-003 + cu-004), `d6d1c2e`
(wave 2, cu-004), spec widened in `eaaba1f`. ITR directory
`.LLMDD/ITRS/bugfix-feature-5-enforce-advisor-read-only-approval/`.
Closes `bug-5-advisor-writes-without-approval` (Resolved 2026-10-18).

Independent re-verification on 2026-10-18 (the tests were re-run against
the current tree, not read back from the Coder feedback):

| Test | Criterion | Result |
| --- | --- | --- |
| T1 | 1 | PASS - 0 blanket `bash: allow`, 13 `: allow` rules, 1 trailing `"*": ask`, `edit: deny` present |
| T2 | 2 | PASS - all 6 gate probes correct (below) |
| T3 | 3 | **FAIL** - enforcement is not capability-level |
| T4 | 4 | PASS - read-only flow leaves `git status` byte-identical |

T2 probe results (fresh tokens under `/tmp/opencode/t2`): valid in-scope
exit 0 `ALLOWED: t-valid`; expired exit 1 `token t-exp expired 2020-01-01`;
malformed exit 1 `not valid JSON`; missing exit 1 `token file not found`;
file out of scope exit 1 `file out of scope: src/app.py`; command out of
scope exit 1 `command out of scope: rm -rf /`.

### Criterion 3 is not met

Criterion 3 requires that ignoring the advisory prose still leaves code
writes blocked. That is false today, and the failure is not in the
delivered artefacts - it is in whether those artefacts are loaded and
honoured. Three independent checks:

- `.opencode/agents/*.md` resolves to `verifier.md` only. `advisor.md` is
  a hardlink twin (inode 75534, verified identical to
  `.LLMDD/agents/advisor.md`) but sits one directory too deep, at
  `.opencode/agents/llmdd/advisor.md`, which the agent scanner does not
  walk. No `agents` key in `.opencode/opencode.json`, and no `opencode.json`
  at repo root.
- `~/.config/opencode/cli.json` sets `session.permissions: "autoaccept"`,
  so the declared `"*": ask` cannot surface even if it were loaded.
- Nothing invokes `check-advisor-approval.py` automatically - no
  pre-commit hook, no wrapper. It is a procedure, not a control.

So the shipped mechanism is real and correct in form, and it is being
followed in practice, but its enforcement is advisory: it holds because
the Advisor reads the tensor and chooses to run the gate, not because
anything intercepts a write. Carried by
`bug-7-session-permissions-autoaccept-nullifies-ask` and
`bug-8-approval-gate-advisory-not-enforced` (both Open). The out-of-band
fix is a real `pre-commit` hook, which sits out-of-band of the model
(it fires from git, not from the agent). An earlier draft justified this
by reference to an existing global pre-push hook; no such hook exists —
see the correction in `bug-8`. The principal defence in force today is
detective, not preventive: an unapproved write is visible in `git diff` to
the code lead and the Designer.

Correction (2026-10-18): an earlier draft asserted that `advisor.md` was
never loaded and that its `permission:` block was therefore inert. Both
claims were wrong and are withdrawn. `advisor.md` IS loaded as the agent
definition - an Advisor session receives its body verbatim while
`.opencode/instructions/AGENTS.md` does not contain it - and `permission`
is a valid `AgentConfig` key whose `PermissionConfig` supports `edit` and
`bash` with values "ask" | "allow" | "deny". The residual is narrower
than first recorded: the **edit channel is closed outright**, because an
Advisor session is granted no edit, write or patch tool at all
(`execute, glob, grep, question, read, shell, skill, subagent, webfetch,
websearch`). What remains open is the `bash` write channel, gated only by
`autoaccept`, plus the token check that nothing invokes.

Note on T3 as originally executed: the Coder's T3 probe stripped the agent
body to a temp copy and observed that `edit: deny` survived in the
frontmatter. That proves the file says `edit: deny`; it does not prove
the harness enforces it. The test is circular and should be replaced by a
behavioural probe - issue an `edit` call from an Advisor session with no
token in scope and assert the harness returns a permission error. That
replacement is part of the bug-7/8 work, not of this feature.

## Acceptance criteria

1. Default Advisor session has no write-capable grant on code: `edit: deny`
   holds and blanket `bash: allow` is gone — bash is a read-only allowlist
   (`ls`, `cat`, `rg`, `grep`, `head`, `tail`, `find`, `wc`, `file`,
   `stat`, `git`) plus `mkdir`/`touch` for scratch creation only, with a
   trailing `"*": ask` or `"*": deny` default for everything else.
2. Every code change requires a scoped approval token that is
   deterministically enforced: `.LLMDD/approvals/<id>.json` names files
   plus commands plus reason plus granted-by plus expiry; the gate script
   `.LLMDD/tools/check-advisor-approval.py` validates schema, expiry, and
   that each changed tracked file and invoked command is within token
   scope, exiting non-zero (write denied) otherwise. No token, expired
   token, or out-of-scope path/command is denied deterministically — never
   by prose alone.
3. The enforcement is capability-level, not prompt-only: removing or
   ignoring the advisory prose still leaves code writes blocked without a
   valid in-scope token (harness `edit: deny` + gate-script denial).
4. Read-only Advisor workflow is preserved, including scratch writes: DTR
   analysis, repo inspection, directory creation, and draft-text/CU-content
   writes to untracked scratch paths work with no approval token, and
   `git status` shows no tracked-file modifications from that path.

## Tests

- `T1` (criterion 1, bug-5 repro): parse
  `.opencode/agents/llmdd/advisor.md` frontmatter; assert `edit: deny`,
  assert no blanket `bash: allow`, assert bash allowlist contains the
  read-only set (`ls`, `cat`, `rg`, `grep`, `head`, `tail`, `find`, `wc`,
  `file`, `stat`, `git`) plus `mkdir`/`touch` and a trailing `"*": ask` or
  `"*": deny` default. Fails on the pre-fix file (blanket allow present).
- `T2` (criterion 2): gate-script unit checks — no token file exits
  non-zero; expired token exits non-zero; token whose `files` omit a
  changed tracked file exits non-zero; token whose `commands` omit the
  invoked write command exits non-zero; valid unexpired in-scope token
  exits zero. Plus negative e2e: an Advisor-attributed code modification
  with no token is denied (gate script exit non-zero on the resulting
  diff), and succeeds only with an in-scope token.
- `T3` (criterion 3): with advisory prose stripped/ignored (prompt-injection
  style probe asserting "you may write"), a code-write attempt is still
  denied without a valid in-scope token — harness `edit: deny` plus gate
  script both refuse, proving the gate is not prompt-dependent.
- `T4` (criterion 4): run the documented read-only Advisor flow (inspect DTR
  fixture, list repo, `mkdir` scratch dir, write draft text to untracked
  scratch, draft CU content to stdout) with no token; assert exit 0 and
  `git status --porcelain` shows no tracked-file modifications.
