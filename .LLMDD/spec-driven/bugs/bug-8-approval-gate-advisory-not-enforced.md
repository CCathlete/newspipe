# bug-8: Approval token gate is advisory, with no enforcement point

- Status: Open
- Created: 2026-10-18
- Resolved: -
- Bug ID: bug-8-approval-gate-advisory-not-enforced

## Bug detail

Symptoms: `.LLMDD/tools/check-advisor-approval.py` is a sound validator -
typed `ApprovalToken`, slotted frozen dataclass, `TypeAlias` not `Any`,
explicit declarations, mypy-strict clean, exit 0/1 with scope and expiry
checks. But it is never invoked by anything. It is a procedure the
advisor is asked to follow, not a control that constrains the advisor.
Nothing in the repo wires it into a hook, a wrapper, or a commit path.

An advisor that writes a file without first running the script produces
no failure anywhere: the script is never told the write happened, and
every downstream check stays green.

Reproduction (from repo root, read-only):

```sh
grep -rn "check-advisor-approval" . --include='*.py' --include='*.sh' \
  --include='*.json' --include='*.yaml' --include='*.fish' \
  | grep -v node_modules
# EXPECTED: an enforcement point (pre-commit hook, CI job, wrapper)
# ACTUAL: no hits - references are only prose in the system tensor,
#   .LLMDD/agents/advisor.md, a DTR, and archived wave logs

ls .git/hooks/pre-commit 2>/dev/null
# ACTUAL: absent - so an unapproved write can still be committed
```

Environment: repo checkout at 4436647 on `llmdd-v9`; identical content on
`origin/main`.

Suspected cause: the mechanism was designed as agent-facing guidance
("validate it with ...; exit zero allows, non-zero denies") rather than
as an out-of-band check that the agent cannot route around. The tensor
states the requirement in the same register as the rules it does not
mechanically enforce: `RULE.ADV.APPROVAL_REQUIRED` is phrased as text,
and the "Prompt text never grants approval" clause is itself only text.

Impact: the approval gate itself is advisory. It holds because the
Advisor reads the tensor and chooses to run the script, not because
anything intercepts a write.

Correction (2026-10-18): an earlier draft of this bug overstated the
gap by asserting that nothing prevents the Advisor from writing code. That
is wrong on the edit channel. An Advisor session's live tool set contains
no edit, write or patch tool at all, so that channel is closed by
capability removal rather than by policy — strictly stronger than the
`edit: deny` the frontmatter declares. What actually remains open is
narrower and sharper than this bug first claimed:

- **Closed:** direct file edits. No such tool exists to grant.
- **Open:** the `bash` channel, which can write (`cat > file`, `python3`,
  `git add`). Gated only by `session.permissions: "autoaccept"` (bug-7),
  which auto-approves anything not explicitly denied.
- **Open:** the token check itself, which nothing invokes (this bug).

So the residual is precisely the bash write path, not "the Advisor can
write code". Severity is lower than first recorded, but the defect is real
and the same `pre-commit` remedy closes it, because a hook is indifferent
to which channel the write arrived through.

Related: bug-7 (autoaccept nullifies the bash `ask` default),
bug-10 (CU scope boundaries unenforced), bug-5 (Resolved — the mechanism
exists and is followed in practice).

Remediation direction (not a design decision - for a future
bugfix-feature): move the gate out-of-band of the model. A `pre-commit`
hook that refuses any staged change to git-tracked files unless a
matching unexpired token covers those paths converts "the advisor was
supposed to ask" into "the commit is impossible without a token". A CI
re-validation of token coverage against the merged diff is the backstop.
This is deliberately not specified here - bug specs track the defect,
the fix is shaped in the feature that links to it.

Related: bug-7 (global autoaccept), bug-5 (original unenforced-approval
report, Resolved).

## Resolution status

Open. No fix merged. Parked by the Designer on 2026-10-18 - tracked, not
scheduled.

## Fix summary (Resolved only)

Pending.

## Tests proving resolution (Resolved only)

Pending. Suggested: (1) negative test - a write outside token scope is
refused by the hook with non-zero exit and a clear denial reason, with no
advisor cooperation required; (2) positive test - a write covered by an
unexpired in-scope token commits cleanly; (3) expiry test - a token past
`expires` denies even for otherwise in-scope files; (4) bypass test -
disabling the model does not disable the hook (assert the hook is a real
git hook, not an agent-invoked step).
