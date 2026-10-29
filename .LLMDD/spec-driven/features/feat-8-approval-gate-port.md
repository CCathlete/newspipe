# feat-8: Approval gate port

- Status: Proposed
- Created: 2026-10-19
- Feature ID: feat-8-approval-gate-port

## Synopsis

Motivation: `RULE.ADV.APPROVAL_GATE_CONTRACT` states that the Advisor
approval gate has three pieces and that only the third is
harness-specific. Investigation of opencode v2.0.32 on 2026-10-19
established that the v2 plugin API exposes **no interception point** for
tool calls or responses — `chat.message`, `tool.execute.before` and
`permission.ask` are v1-only and do not exist in v2 — while
`AgentV2Info.permissions` is a static `(action, resource, effect)`
ruleset with no predicate and no session state. So on that harness the
ordering requirement ("validate before any response") is not expressible
mechanically.

That finding is *why this feature exists in this shape*. Building an
adapter requires knowing what the host can intercept, which differs per
harness and changes between versions. Building the port does not. The
port is the harness-neutral half, and it is worth building on its own
because it is the reusable half: whatever the answer for opencode turns
out to be, and whatever it is for Claude Code or anything else, the
contract, the shared decision logic and the proof that an adapter honours
them are the same everywhere.

Per `ARCH.DIP` ("high-level modules own the interfaces, low-level modules
implement them") and `ARCH.PORT_FLOW` ("outbound ports defined in
application layer, inbound adapters in infrastructure layer"), the port
lives in the toolchain and the adapter is a separate feature. This feature
delivers the port only. No adapter for any harness is in scope — the
opencode adapter is explicitly deferred to its own feature, because its
shape depends on the harness-interception question that is still open.

Scope: In — the normative port contract in the tensor; a harness-neutral
gate process implementing the shared decision logic; a line protocol so an
adapter in any language can drive it; a conformance suite any adapter must
pass; and documentation stating what a harness must be able to do to host
the port. Out — every adapter, including opencode's; any change to
`check-advisor-approval.py`'s token schema or exit-code contract, which
this feature consumes unchanged; and `bug-10` (CU scope boundaries),
which is unrelated.

### The separation

The port answers one question: **what must a host be able to do for the
gate to exist?** The adapter answers, per harness: **how do we make it do
that?** Conflating them is what stalled this work — an adapter cannot be
specified until the host's capabilities are known, and the capabilities
differ by version. Splitting them lets the port ship against the
capability list rather than against any one harness's behaviour.

### What the port specifies

**1. Host capability requirements.** A harness can host the gate if and
only if it can do all four:

| # | Capability | Why it is required |
| --- | --- | --- |
| C1 | signal the start of a response, naming the agent | arms the gate; without it there is no "before" |
| C2 | intercept an action and return allow or deny | this is the enforcement |
| C3 | identify the action well enough to match a command pattern | the validator must be recognisable |
| C4 | invoke an external process and read its exit code | the validator is a process |

A harness meeting C1–C4 can implement the gate fully. One missing C2
means it cannot enforce, only prompt. One missing C4 means it cannot use
the shared validator. This list is the deliverable that makes "can this
harness host the gate?" answerable without writing any adapter code.

**2. The decision logic**, harness-neutral and owned here so adapters
cannot drift:

```
state per session:
  UNVALIDATED  — no validation recorded yet
  VALIDATED    — a validation returned allow
  REFUSED      — a validation returned deny

on response begin for a gated agent:  state := UNVALIDATED
on proposed action:
    if action is the validation command:  run it, record result, allow
    else if state is VALIDATED:           allow
    else:                                deny
```

The ordering requirement lives here, not in prompt text: in `UNVALIDATED`,
the *only* action that can succeed is the validation itself. That is what
makes the check first — not because the Advisor was told to check first,
but because nothing else is permitted until it has.

**3. The line protocol**, so the logic is reachable from any language.
An adapter that shells out can speak it without linking anything. This is
the same portability trick as the existing exit-code contract, applied to
state: stdin commands, stdout verdicts, no shared runtime.

**4. The conformance suite**, written once and run against every adapter.
An adapter is "done" when it passes the suite the first adapter passed.
This is what makes the port a port rather than a document — without it,
"implements the contract" is an opinion.

**5. The documentation** a harness author needs: the capability list, the
protocol, and the four conformance cases, with a worked example for a
harness that meets all four and a note on what to do when it does not.

## Implementation status

Proposed. No CUs drafted; the decomposition below is a proposal.

### Proposed decomposition

- `cu-001` — the normative port contract in the tensor: the four host
  capability requirements, the state model, and the protocol. No code.
  Everything else is written against this, so it lands first.
- `cu-002` — the gate process implementing the decision logic, with unit
  tests covering every state transition including the refusals. Pure,
  harness-free, fast.
- `cu-003` — the conformance suite: the four cases (pre-validation action
  denied, validation action allowed, post-validation action allowed, a
  denied validation leaving state REFUSED), written against the protocol
  so any adapter can run it.
- `cu-004` — harness-author documentation, including the worked example
  and the degraded-mode note.

Independent of the opencode adapter and of `bug-10`. `cu-001` blocks the
rest; `cu-002` and `cu-003` are independent of each other once the
protocol is fixed.

## Acceptance criteria

1. The tensor carries the port contract: the four host capability
   requirements C1–C4, the three-state model, the rule that in
   `UNVALIDATED` only the validation command may proceed, and the
   protocol. Written so a reader can answer "can my harness host this?"
   without reading code.
2. A harness-neutral gate process implements the state model, accepts
   protocol commands on stdin, and returns allow or deny on stdout. It has
   no dependency on any harness, on opencode, or on the model.
3. The gate process reuses `.LLMDD/tools/check-advisor-approval.py`
   unchanged, preserving its exit-code contract (0 allow, 1 deny, 2
   usage). No token schema change.
4. A conformance suite exists and passes against a reference driver: the
   first proposed action in `UNVALIDATED` is denied; the validation
   command is allowed; after a passing validation subsequent actions are
   allowed; after a failing validation the state stays `REFUSED` and
   actions continue to be denied.
5. Documentation states C1–C4, the protocol, the conformance cases, a
   worked example, and what a harness missing a capability must do instead
   (degraded mode, honestly labelled rather than silently partial).

## Tests proving resolution

- `T1` (criterion 1) — grep: `RULE.ADV.APPROVAL_GATE_CONTRACT` appears
  exactly once in the tensor and names C1–C4 and the three states.
- `T2` (criterion 2, 4) — unit tests for the state machine, every
  transition named, including: action before validation → deny; validation
  command → allowed and runs the validator; exit 0 → `VALIDATED`;
  exit 1 → `REFUSED` and stays denied; exit 2 → usage error surfaced, not
  treated as allow; a second validation re-runs the validator rather than
  short-circuiting on stale state.
- `T3` (criterion 3) — the gate process invokes
  `check-advisor-approval.py` with an in-scope token and observes exit 0;
  with an out-of-scope file exit 1; with a missing token file exit 1;
  with a usage error exit 2. Asserts no change to the validator's own
  behaviour.
- `T4` (criterion 4) — the conformance suite passes against a reference
  driver, and the suite is written against the protocol rather than against
  any harness, so a second driver can be added without changing it.
- `T5` (all) — `mypy --strict` clean on the new Python, per the standing
  proof; no `Any`, explicit declarations, no try/except for expected
  failures; no dependency on opencode packages anywhere in the gate.
