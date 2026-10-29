# bug-7: Global autoaccept nullifies every agent "ask" rule

- Status: Open
- Created: 2026-10-18
- Resolved: -
- Bug ID: bug-7-session-permissions-autoaccept-nullifies-ask

## Bug detail

Symptoms: `~/.config/opencode/cli.json` sets
`session.permissions: "autoaccept"`, which auto-accepts permission
prompts for every session regardless of agent. Any `"*": ask` fallback
declared in an agent's bash allowlist therefore never reaches the user,
which defeats the default-ask rule in the Advisor frontmatter (which is
loaded and parsed correctly - that part works).

The setting is user-global, so it silently overrides the per-repo intent
expressed in agent frontmatter. A repository that believes it is
guarded by default-ask is not guarded on this machine.

Reproduction (from repo root, read-only):

```sh
cat ~/.config/opencode/cli.json
# ACTUAL: { ..., "session": { ..., "permissions": "autoaccept" } }
# EXPECTED: "ask"/"prompt" (or the key absent, so agent rules apply)

grep -rn '"\*": ask' .LLMDD/agents/
# ACTUAL: .LLMDD/agents/advisor.md:20 declares the rule
#   - that rule is unreachable while autoaccept is set
```

Environment: `~/.config/opencode/cli.json` on the Designer workstation;
opencode v2.0.30, Linux.

Suspected cause: `autoaccept` was set as a convenience for interactive
work and never revisited when the per-agent permission model was
introduced. Because the setting lives in the user-global config, it
outranks repository config, so no repo-level change can override it.

Impact: the `ask` tier of the permission model is dead on this machine.
Approval becomes advisory by configuration, not merely by omission -
compounding bug-8. Every role is affected, not
just the Advisor.

Related: bug-8 (advisory-only approval gate), bug-5 (original
unenforced-approval report, Resolved).

## Resolution status

Open. No fix merged. Parked by the Designer on 2026-10-18 - tracked, not
scheduled.

## Fix summary (Resolved only)

Pending.

## Tests proving resolution (Resolved only)

Pending. Suggested: (1) config assertion that `session.permissions` is not
set to an auto-accepting value, or a startup lint that warns when a
global auto-accept shadows an agent `"*": ask`; (2) negative e2e test -
an Advisor session invoking a non-allowlisted command surfaces a prompt to
the user, and proceeding requires an explicit interactive choice;
(3) regression guard asserting every agent's `ask` fallbacks are
reachable under the effective (global + project merged) permission config.
