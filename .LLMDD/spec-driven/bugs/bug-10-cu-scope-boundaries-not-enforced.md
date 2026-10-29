# bug-10: CU scope boundaries are declared but never enforced

- Status: Open
- Created: 2026-10-18
- Resolved: -
- Bug ID: bug-10-cu-scope-boundaries-not-enforced

## Bug detail

Symptoms: a CU declares its blast radius twice — the paths it writes
(F1, "exact file path") and the `Do NOT touch:` list — but neither
declaration is ever checked against what the Coder actually changes. The
boundary is prose that no gate parses and no runner compares. A Coder
that drifts outside its scope produces a green wave.

Three independent gaps, each individually sufficient to let drift through.

**Gap 1 — the validator checks that a no-touch list exists, never what is
in it.** `.LLMDD/tools/validate-steps.py` F4 is two substring tests:

```python
if "Do NOT touch:" not in body:
    fails.append("F4: missing Do NOT touch list")
```

The list is never parsed, never normalised, and never intersected against
the paths the steps declare. An empty list, a list naming the very file
the CU writes, or a list of pure prose all pass F4 identically. The
validator also has no allowlist concept: scope is inferred from prose
paths inside step bodies rather than from an explicit declaration.

**Gap 2 — the no-touch lists in practice are unmatchable prose.** The
shipped frames for bugfix-feature-5 lean on exactly the exclusions a
simple Coder is most likely to violate:

```
cu-001  Do NOT touch: ...coder.md, ...verifier.md, opencode.json,
        the sys-prompt tensor, any Scala source, any Python tool.
cu-003  Do NOT touch: ...advisor.md (both twins), coder.md, verifier.md,
        opencode.json, the sys-prompt tensor, any Scala source,
        any ITR frame or feedback file.
```

"any Scala source", "any Python tool" and "any ITR frame or feedback
file" are not paths. No mechanical matcher can test them, so the clauses
that carry the most weight are the ones enforcement cannot reach.

**Gap 3 — the runner measures drift and discards the measurement.**
`.LLMDD/tools/run-waves.py` already collects the changed-file list per CU
(`files_changed`, populated from the diff at line 1127) and threads it
through to reporting (lines 1186, 1202, 1428, 1458, 1501). It is never
compared to anything — there is no intersection, no subset assertion, no
deny path. Grepping the runner for `no_touch`, `scope_files`,
`allowed_paths` or any declared-scope reference returns nothing. The one
place CU scope is stated to a Coder is a prompt string at line 1018,
`f"Your scope is EXACTLY one CU: {cu_id}. Nothing else."` — advisory text,
the same class of control as bug-8.

Reproduction (from repo root, read-only):

````sh

# Gap 1 - a CU whose no-touch list names its own write target passes F4.
cat > /tmp/opencode/cu-bad-steps.txt <<'CU'
1. Edit .LLMDD/tools/x.py

```python
x = 1
```

Prove: `python3 -m py_compile .LLMDD/tools/x.py`

Do NOT touch: .LLMDD/tools/x.py, any Scala source
Stop when green.
CU
python3 -c "import json,pathlib; s=pathlib.Path('/tmp/opencode/cu-bad-steps.txt').read_text(); print(json.dumps({'cus':[{'cu-id':'cu-bad','content':'# IMPLEMENTATION_STEPS:\n'+s+'# ACCEPTANCE:\n1. x is 1\n'}]}))" > /tmp/opencode/batch.json
python3 .LLMDD/tools/validate-steps.py /tmp/opencode/batch.json
# EXPECTED: FAIL - the CU lists its own write target as untouchable
# ACTUAL:   PASS cu-bad, exit 0 - F4 tests substring presence, never parses

# Gap 2 - the real frames' most important exclusions are not paths
grep -h "Do NOT touch:" .LLMDD/ITRS/bugfix-feature-5-*/cu-*/IMPLEMENTATION_STEPS.itr \
  | grep -oE "any [A-Za-z ]+"
# ACTUAL: "any Scala source", "any Python tool",
#   "any ITR frame or feedback file" - unmatchable

# Gap 3 - the runner records changed files and never checks them
grep -n "files_changed" .LLMDD/tools/run-waves.py | tail -3
# ACTUAL: populated at 1127, forwarded to reporting; no comparison anywhere
grep -cE "no_touch|scope_files|allowed_paths" .LLMDD/tools/run-waves.py
# ACTUAL: 0
````

Evidence that drift has already occurred under this gate:

- `cu-001` feedback: the sys-prompt tensor "carries a pre-existing
  modification not made or touched by this session (cu-002 scope)". A
  file explicitly on cu-001's no-touch list was dirty in the working tree
  while cu-001 ran. The Coder noticed and narrated it; nothing failed.
- `cu-002` feedback: "both cu-002 edits were already present in the
  working tree from a prior interrupted attempt (attempt-0/attempt-1
  logs)". State from an aborted attempt leaked into a later run with no
  gate noticing.

Both waves reported STATUS=COMPLETED and verification green.

All three reproductions above were executed against this checkout on
2026-10-18; Gap 1 prints `PASS cu-bad` and exits 0, confirming a
self-contradictory no-touch list is accepted. (First draft of the Gap 1
probe omitted the `IMPLEMENTATION_STEPS:` / `ACCEPTANCE:` markers the
batch path requires, so the validator read an empty body and failed for
the wrong reason - a reminder that a reproduction must be run before it is
recorded, not merely written.)

Environment: repo checkout at 397334b on `llmdd-v9`; identical content on
`origin/main`. Python 3.13, Linux.

Suspected cause: scope was treated as a documentation requirement rather
than a constraint to be computed. The validator was designed to police
step *shape* (F1 paths, F2 fences, F3 proofs, F5 vague verbs, F6
placeholders) and scope slipped through as one more prose requirement. The
no-touch list is free-text because nothing has ever needed to match it.
Meanwhile the runner collected `files_changed` for reporting before
anything needed it for enforcement, and the reporting use case locked in
the shape.

Impact: the CU is the unit of coordination in the whole design — it
carries DTR coordinates, severity, acceptance tests and the no-touch
boundary that keeps parallel waves from colliding. A boundary that is not
checked is not a boundary. With N CUs in a wave running in parallel
against one working tree, an undetected out-of-scope edit is attributed to
the wrong CU, can silently satisfy another CU's acceptance test, or
corrupt a sibling's diff before either is committed. This is the drift
risk that grows as waves get wider and CUs get smaller — exactly the
regime the global spec-id work (feat-6) pushed toward.

Note: `bug-9` is a burned serial. The allocator is a high-water mark, so a
verification call issued it and no file was created. `--check` exits 0
(ahead-of-tree is safe), but the sequence skips 9.

Remediation direction (not a design decision — for a future
bugfix-feature): make scope a declared, parsed structure rather than
prose. (1) Give each CU an explicit machine-readable allowlist — the set
of paths it may write — replacing path inference from step prose.
(2) Parse the `Do NOT touch:` list into the same path vocabulary and
assert at design time that allowlist and no-touch are disjoint, which
catches self-contradictory CUs before compilation. (3) Convert the
prose clauses ("any Scala source") into glob patterns so they become
matchable. (4) At run time, compare the `files_changed` the runner
already computes against the declared allowlist and fail the CU on any
file outside it — this is the smallest change with the largest effect,
because the measurement already exists. (5) Reject when the working tree
is dirty in another CU's declared paths at CU start, which is the
cu-001/cu-002 contamination above.

## Resolution status

Open. No fix merged.

## Fix summary (Resolved only)

Pending.

## Tests proving resolution (Resolved only)

Pending. Suggested: (1) negative - a CU whose no-touch list contains its
own allowlist path fails validation with both sides named; (2) negative -
an empty or prose-only no-touch list fails validation rather than passing
substring presence; (3) negative - a Coder run whose diff touches a file
outside the declared allowlist fails the CU non-zero, naming the offending
path and the token/CU that permitted it; (4) negative - a CU starting
with a sibling CU's declared path already dirty in the working tree fails
before implementation begins; (5) positive - a conforming run still
passes, with `files_changed` reported as before, so the check is not
merely stricter; (6) static - the runner contains at least one assertion
comparing `files_changed` to a declared scope, so the measurement cannot
be removed without failing.
