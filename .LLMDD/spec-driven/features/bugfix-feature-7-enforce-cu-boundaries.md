# bugfix-feature-7: Enforce CU boundaries

- Status: Implemented
- Created: 2026-10-18
- Feature ID: bugfix-feature-7-enforce-cu-boundaries

## Synopsis

Motivation: a CU declares its blast radius twice — the paths it writes
(`IMPLEMENTATION_STEPS`, enforced as F1) and the `Do NOT touch:` list —
and neither declaration is checked against what the Coder actually
changes. The boundary is prose that no gate parses and no runner compares,
so a drifting Coder produces a green wave. `bug-10` is Open against exactly
this, and it is the only bug this feature covers.

Earlier drafts of this spec bundled `bug-4`, then `bug-7` and `bug-8`
alongside `bug-10`. Those were removed by Designer decision on
2026-10-18/19: `bug-4` is an unrelated Scala refactor and remains Open, and
`bug-7`/`bug-8` belong to the approval-gate line of work (`feat-8` and a
future adapter feature), not here. What remains is the CU boundary problem,
which was always the only coherent member of that group.

Scope: In — the three gaps below, in `.LLMDD/tools/validate-steps.py` and
`.LLMDD/tools/run-waves.py`. Out — a declared per-CU allowlist (see the
cost warning), any change to the CU content format, every existing compiled
frame, `bug-7`, `bug-8`, `bug-4`, and the approval-gate work in `feat-8`.

### The three gaps

All three were verified against the tree at `397334b`, and the reproduction
in `bug-10` was executed rather than asserted.

**Gap 1 — the validator checks that a no-touch list exists, never what is
in it.** `validate-steps.py` F4 is two substring tests:

```python
if "Do NOT touch:" not in body:
    fails.append("F4: missing Do NOT touch list")
```

The list is never parsed, never normalised, and never intersected against
the paths the steps declare. There is no allowlist concept: scope is
inferred from prose paths inside step bodies. Verified probe: a CU that
lists its own write target in the no-touch list prints `PASS cu-bad`,
exit 0.

**Gap 2 — the exclusions that matter are unmatchable prose.** The shipped
`bugfix-feature-5` frames lean on exactly the clauses a simple Coder is
most likely to violate:

```
cu-001  Do NOT touch: ...coder.md, ...verifier.md, opencode.json,
        the sys-prompt tensor, any Scala source, any Python tool.
cu-003  Do NOT touch: ...advisor.md (both twins), coder.md, verifier.md,
        opencode.json, the sys-prompt tensor, any Scala source,
        any ITR frame or feedback file.
```

`any Scala source`, `any Python tool` and `any ITR frame or feedback
file` are not paths. No matcher can test them, so the clauses carrying the
most weight are the ones enforcement cannot reach.

**Gap 3 — the runner measures drift and discards the measurement.**
`run-waves.py` populates `files_changed` from the diff at line 1127 and
threads it to reporting at 1186, 1202, 1428, 1458 and 1501 — and never
compares it to anything. `grep -cE "no_touch|scope_files|allowed_paths"`
returns 0. The only place CU scope reaches a Coder is a prompt string at
line 1018.

### Drift has already been waved through

- `cu-001` feedback: the sys-prompt tensor "carries a pre-existing
  modification not made or touched by this session (cu-002 scope)" — a
  file explicitly on cu-001's own no-touch list, dirty while cu-001 ran.
  The Coder noticed and narrated it; nothing failed.
- `cu-002` feedback: "both cu-002 edits were already present in the
  working tree from a prior interrupted attempt". State from an aborted
  attempt leaked into a later run with no gate noticing.

Both waves reported `STATUS=COMPLETED` with verification green.

### Cost warning: the full fix is a breaking format change

Making scope a declared structure — an explicit per-CU allowlist — is
**breaking**: every existing batch and compiled frame lacks one, so either
all frames are recompiled or the validator infers an allowlist for legacy
content and rejects only when inference fails. That trade should be decided
in its own feature, not here.

This feature deliberately takes the slice that needs no format change and
no recompile: parse the no-touch list that already exists, make its prose
clauses matchable, and compare it against the `files_changed` the runner
already computes. Highest leverage first, same principle as leading with
the measurement that already exists rather than the one you wish existed.

## Implementation status

Proposed. No CUs drafted; the decomposition below is a proposal.

### Proposed decomposition

- `cu-001` (design time) — parse `Do NOT touch:` into a path set; convert
  the prose clauses to globs so they are matchable; fail a CU whose list is
  empty, unparseable, or intersects its own declared write paths, naming
  both sides.
- `cu-002` (run time) — in `run-waves.py`, compare `files_changed` against
  the parsed no-touch set and fail the CU non-zero naming the offending
  path and the CU it belongs to. No change to the CU content format, so no
  frame recompile.
- `cu-003` (pre-flight) — refuse to start a CU when a path in another
  active CU's declared write paths is already dirty in the working tree.
  This is the `cu-001`/`cu-002` contamination above, caught before the
  implementation step rather than narrated afterwards.

`cu-001` blocks the other two. `cu-002` and `cu-003` are independent of
each other.

## Acceptance criteria

1. `validate-steps.py` parses the `Do NOT touch:` list into a normalised
   path set. A CU is rejected when the list is absent, empty, or contains
   an entry that is not a path or a glob. Failure names the CU and the
   offending entry.
2. The prose clauses that appear in shipped frames — `any Scala source`,
   `any Python tool`, `any ITR frame or feedback file` — are matchable
   globs, and a path falling under one is recognised as covered.
3. `validate-steps.py` fails a CU whose no-touch set intersects the paths
   its own steps declare as writes, naming both sides.
4. `run-waves.py` fails a CU non-zero when `files_changed` contains a path
   in that CU's no-touch set, naming the path. Conforming CUs are
   unaffected and `files_changed` is still reported as before.
5. A CU refuses to start when a path belonging to another active CU is
   already dirty in the working tree, naming the path and the owning CU.

## Tests proving resolution

- `T1` (criteria 1, 2) — parser unit tests: a well-formed list parses to
  the expected set; an empty list is rejected; an entry that is neither
  path nor glob is rejected naming it; each shipped prose clause resolves
  to a glob that matches a representative path and does not match an
  unrelated one.
- `T2` (criterion 3) — a CU whose no-touch list names its own write target
  fails validation, naming both sides. This is the `PASS cu-bad` probe from
  `bug-10`, inverted: the same fixture must now exit non-zero.
- `T3` (criterion 4) — negative: a run whose diff touches a no-touch path
  fails the CU non-zero, naming the path; positive: a conforming run still
  passes and reports `files_changed` unchanged; negative: a no-touch glob
  (`any Scala source`) is matched, not just an exact path.
- `T4` (criterion 5) — a CU starting with a sibling CU's declared path
  already dirty fails before implementation begins, naming both.
- `T5` (all) — `mypy --strict` clean on both tools, per the standing proof;
  no `Any`, explicit annotations at first binding, monadic error handling
  for expected failures; every existing compiled frame still validates
  unchanged, proving no format change was introduced.

## Reopened 2026-10-22 (audit of the shipped runtime slice)

The previous implementation (fa50684) compiled and ran cu-001..cu-003, but the run-time boundary check is unsound (probes in /tmp/opencode/probe_fidelity.py).

### Over-fire
- R1 Sibling contamination: every CU diffed against the same pre-wave HEAD, so wave-wide changes were attributed to the CU currently running.
- R2 Pre-existing dirt: baseline_dirty captured once before the loop, and untracked files ignored; dirt on disk was blamed on later CUs.

### Under-fire
- R3 New files invisible (git diff only).
- R4 Directory entries (.LLMDD/agents) never match (fnmatch whole string).
- R5 Symlink entries never match.
- R6 Unparsed no-touch clauses dropped silently; semicolon lists collapse to unparsed; empty patterns allowed the gate to pass vacuously.
- R7 Wrapped continuation lines lost their tail.
- R8 Absent no-touch list disables the gate entirely.
- R9 Flat frames never gated.
- R10 Leads ungated.
- R11 Pre-flight weak (no per-wave baseline, no untracked, no same-wave write-target collision).
- R12 No tests.

### Fix design
Measure the CU's change set from the CU's own opencode JSON tool_use events (write/edit/patch + shell), union wave diff and untracked files minus the pre-wave baseline, and attribute each changed path to the claiming CU. Enforce: own no-touch, cross-CU write collision (shared attributed path), and unattributed changes against every no-touch in the wave. Fail closed when a list yields no enforceable pattern. Resolve symlinks, expand directories, split on commas and semicolons, continue wrapped lines. Gate leads and recompute baseline per wave. Add a regression suite.

### Fix batch (waves)
- wave 1: cu-004 (cu_scope hardening), cu-005 (run-waves attribution)
- wave 2: cu-006 (run-waves enforcement)
Waves plan in .LLMDD/ITRS/bugfix-feature-7-enforce-cu-boundaries/waves.json after compilation.
