---
name: hypothesize
description: Standalone hypothesis reasoning and validation with the Hypothesis Engine. Use for "why is this not working" diagnosis or "is this suggested solution correct" validation questions — the reasoning and validation segment only, not the rest of prepare-implementation.
---

# hypothesize

Standalone hypothesis reasoning + validation for two question types over one
loop. Invoked as `@hypothesize <subject>` where `<subject>` is a short
kebab-case name for the question under examination. The skill owns only the
reasoning + validation segment; it ends at the recorded verdict.

## When to use

- Diagnosis: "why is this not working?" — find and confirm a cause.
- Validation: "is this suggested solution correct?" — confirm or refute a claim.
- Not for drafting CUs, compiling ITR, planning waves or running Coder sessions;
  those remain in `@prepare-implementation`.

## Modes

`diagnosis`: the hypothesis states a candidate cause at DTR coordinates.
`GREEN_LIGHT` means that cause is supported; a `RED_LIGHT` names the
breakpoints where the cause claim fails and the hypothesis is revised there.

`validation`: the hypothesis states the proposed solution's correctness claim.
`GREEN_LIGHT` means the claim is confirmed; a `RED_LIGHT` names the coordinates
where the claim fails and the claim is revised there.

Both modes share one loop, the same experiment authoring, the same engine
invocation, the same verbatim echo, the same hard gate and the same verdict
fix-loop. Only the hypothesis statement template and the meaning of a green
result differ.

Hypothesis statement templates:
- diagnosis: `The observed failure at <coordinates> is caused by <candidate cause>.`
- validation: `The proposed change at <coordinates> satisfies <claim> without violating <constraint>.`

## DTR input policy

Reuse the existing baseline DTR at `.LLMDD/DTRS/<subject>/<app>.dtr`. Build it
with `.LLMDD/tools/dtr-builder --out <path> --root <path-to-code>` when it is
absent or stale. A DTR is stale when any of these hold: the file is missing;
it has no `META.GENERATOR=dtr-builder` entry; it lacks `FILE.`, `CODEX.` or
`TYPE.` entries; or its `META.TIMESTAMP` is older than the newest source file
under the scanned root. Never hand-edit a DTR. Every `dtr_coordinates` entry
must be an exact key from this DTR; a key absent from the DTR denotes a
creation coordinate.

## Loop (both modes, stop on first red gate)

- Step 1: Write `.LLMDD/ITRS/<subject>/<subject>.hypotheses.json` with `id`,
   `statement` (from the mode template), `dtr_coordinates`,
   `gate_probability_for_correctness` at least 0.85, `prior` and a non-empty
   `justification`. A blank justification fails closed before evaluation.
- Step 2: Write `.LLMDD/ITRS/<subject>/<subject>.experiments.jsonl`, one JSON object
   per line with `experiment_id`, `hypothesis_id`, `runtime`, `command` as an
   argv list, `working_directory`, `timeout_seconds` and
   `test_artifact_version`. Set an explicit `dtr_coordinates` list on every
   experiment so evidence is mapped rather than left to positional fallback.
   Each probe prints `{"passed": true, "detail": "reason for the result"}` on stdout and does
   not write the ledger.
- Step 3: Run `sbt -batch cleanLedger` once before the first stage. On a re-run after
   a `RED_LIGHT` or `ADVISOR_ACTION_REQUIRED` stage, do NOT clean again: the
   engine clears that stage's evidence and preserves decision history so each
   report shows the full progression. Run the engine from `hypothesis-engine/`
   with `sbt -batch "runMain hypothesisengine.control.Main <hypotheses.json> <experiments.jsonl>"`.
   If sbt cannot see the sqlite JDBC driver, launch the same entrypoint directly
   with `java -cp "$(sbt -batch 'export Compile / fullClasspath')" hypothesisengine.control.Main <hypotheses.json> <experiments.jsonl>`.
- Step 4: Echo the ledger report block verbatim, both into the conversation and by
   appending the byte-identical block to
   `.LLMDD/ITRS/<subject>/<subject>.hypothesis-conversation.md`. Do not revise
   the hypothesis, re-run the engine or advance until that block appears in
   both places.
- Step 5: Run `python3 .LLMDD/tools/check-hypothesis-report.py --report <report.jsonl> --conversation <conversation.md> --stage <n> --verdict <verdict>` as a hard gate. A `CHECK_FAILED` exit stops the loop: supply the missing verbatim echo and re-run the checker.
- Step 6: Interpret the verdict verbatim. `GREEN_LIGHT posterior=<value> gate=<value>` passes. `RED_LIGHT posterior=<value> gate=<value> breakpoints=<coords> fix=<text>` fails: revise the hypothesis or DTR at those coordinates and re-run from step 3. `ADVISOR_ACTION_REQUIRED missing=<names> corrections=<text>` fails: supply the named ledger data and re-run from step 3. A non-zero exit, crash or missing verdict line is an engine defect: stop and report it.
- Step 7: Record the verdict line in `.LLMDD/ITRS/<subject>/hypothesis-verification.txt`. Store the hypotheses, experiments, ledger report, conversation record and verdict with the subject's ITR artifacts.

## Stop boundary

End at the recorded verdict. Do not draft a batch CU, compile ITR, plan or
dry-run waves, write `run-waves.txt`, or execute the operational wave command.
Those actions belong to `@prepare-implementation`.

## Artifact paths

Keep these paths unchanged so the engine and `check-hypothesis-report.py` need
no edits: `.LLMDD/ITRS/<subject>/<subject>.hypotheses.json`,
`.experiments.jsonl`, `.ledger-report.jsonl`, `.hypothesis-conversation.md`
and `hypothesis-verification.txt`.
