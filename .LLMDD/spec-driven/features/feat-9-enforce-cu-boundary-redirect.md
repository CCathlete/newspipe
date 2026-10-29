# feat-9: Enforce CU boundary correction pass

- Status: Implemented
- Created: 2026-10-22
- Implemented: 2026-10-24
- Feature ID: feat-9-enforce-cu-boundary-redirect

## Synopsis

Motivation: the post-hoc enforcement in `run-waves.py` detects boundary
violations (`no-touch`, `shared-write`, `unattributed`, `unenforceable`)
and writes `boundary-breaches.txt`, but detection alone does not put a
drifting coder back inside its CU. The Designer's requirement is explicit:
do **not** terminate the coder session and do **not** route a scope
violation to the code lead by default. Prompt the original coder to align
itself, then continue the run.

The mechanism is a **correction pass**: a second `opencode run --agent
llmdd/coder` invocation against the same CU folder, using the same model
that produced the breach, with a prompt listing only the violating paths.
Correction passes are not model fallbacks — the fallback chain exists for
"the model failed to respond", and a scope violation is not a failure to
respond. Up to three correction attempts per breached CU, re-measured
after each. Escalation to the per-CU lead happens only when the cap is
exhausted. Unattributed changes have no owning CU, so they go to the wave
lead with explicit instructions.

Scope: In — hardening the correction pass, injecting breach context into
the per-CU and wave lead prompts, routing unattributed breaches, and the
regression tests that prove all of it. Out — changing the attribution
model, changing the four enforcement rules, using fallback models for
scope corrections, and any change to the CU content format.

## Implementation status

Partially implemented ahead of the pipeline: the correction pass and
correction-log attribution were written directly into
`.LLMDD/tools/run-waves.py` (`4be0d3a`, `dcb364d`) before this feature
had frames. That code works but carries defects this feature fixes:

- `_breach_detail_for_cu` contains a dead `and False` branch, an
  unreachable `elif`, and a redundant fallback loop; its `shared-write`
  test does not check that `cu_id` is one of the owners, so one CU can be
  handed another pair's collision.
- The correction driver in `run_wave_parallel` uses `try/except` around
  `fut.result` and `await fut_c`, which violates the no-try/except rule
  stated in that function's own docstring and `ARCH.PYTHON.MONADIC_ERROR`.
- It re-measures every CU's attribution inside the per-CU attempt loop
  instead of measuring only the CU being corrected.
- Colour selection is derived from dict key order, so the same CU can
  change colour between runs.
- Exhausting the three attempts does not mark the CU for lead escalation;
  the spec requires escalation at the cap.
- Breach detail is never injected into `run_per_cu_lead` or
  `run_code_lead`, so a lead asked to repair an escalated CU cannot see
  why it breached without opening `boundary-breaches.txt`.
- No tests cover the correction path.

CUs assigned at `cu-001` (runner hardening + wiring) and `cu-002`
(regression tests). ITR directory:
`.LLMDD/ITRS/feat-9-enforce-cu-boundary-redirect/`.

Wave 1 (`cu-001`) was run and delivered the hardened correction pass. Two
latent defects carried in the direct edit were fixed by that CU: a bare
`ThreadPoolExecutor` reference with only `concurrent.futures` imported
(`NameError` on the first breached wave) and a three-name unpack of the
two-element `cu_files` tuple (`ValueError` on every correction attempt).
The CU flagged one gap it could not close — the `run_code_lead` call site
in `main_async` was outside its coordinates — so `_unattributed_detail`
now reads the wave-scoped breach lines and passes them as
`unattributed_context` at that call site. The regression tests were added
under `TestCorrectionPass`; the production gates live under
`TestCUProductionGate` and skip when `CU_BOUNDARY_SKIP_GATE=1`.

## Errors found during review and wave execution

The following errors were discovered after the feature was prepared/marked implemented:

### 1) Unqualified `ThreadPoolExecutor` causes `NameError` (bug-12)
- **Location**: `.LLMDD/tools/run-waves.py` in `run_wave_parallel` correction pass
- **Error**: `NameError: name 'ThreadPoolExecutor' is not defined`
- **Cause**: Used `ThreadPoolExecutor(max_workers=1)` unqualified while only `import concurrent.futures` exists (no `from concurrent.futures import ThreadPoolExecutor`)
- **Trigger**: Only when a boundary breach occurs and the correction loop executes
- **Impact**: Wave runner crashes mid-wave on first breach
- **Fix**: Use `concurrent.futures.ThreadPoolExecutor(...)` or add the import

### 2) Three-name unpack of two-element `cu_files` tuple (latent, fixed by cu-001)
- **Location**: Correction pass iteration over `cu_files`
- **Error**: `ValueError` on unpacking `for cid3, cfile3, _f3 in cu_files:` where `cu_files` is `list[tuple[str, Path]]`
- **Trigger**: During correction attempts
- **Status**: Fixed in cu-001

### 3) Review process missed runtime bugs (bug-13)
- **Issue**: Implementation review trusted CU self-verification and structural gates
- **Gaps**:
  - No integration testing of breach/correction code paths (only ast.parse, unit tests, structural gates)
  - No symbol/import sanity checks (grep for usage vs imports)
  - Gates validated structure/DTR/coordinates but not runtime behavior
  - Conflated "CUs committed" with "feature works end-to-end"
- **Impact**: feat-9 marked Implemented despite broken runtime behavior
- **Lesson**: Must exercise new branches with synthetic scenarios and trace injected code

### 4) Attributed writes confusion during mid-run changes
- **Observation**: Files modified/created mid-wave run (e.g. `.LLMDD/ITRS/feat-9-enforce-cu-boundary-redirect.batch.json`, `.LLMDD/tests/test_cu_boundary.py`) were correctly flagged as `unattributed:cu-001` by Rule 3
- **Cause**: `stash_ref` captured at wave start; changes made after capture appear as unattributed
- **Note**: This is correct boundary enforcement behavior, not a bug in enforcement logic

## Acceptance criteria

1. A CU with an owned (`no-touch`) or `shared-write` breach is corrected
   by a second coder invocation on the same model, never by a fallback
   model, and the coder's own feedback is not replaced.
2. `_breach_detail_for_cu` returns exactly the breaches whose `scope` is
   the CU, plus `shared-write` breaches where the CU is one of the owners.
   It never returns another CU's collision and contains no dead branches.
3. Correction runs at most three times per breached CU; each attempt
   writes `cu-<id>.correction-<n>.log` and is included by `coder_writes`.
4. After each attempt the runner re-measures only the corrected CU. When
   its breaches clear, correction stops for that CU.
5. When the cap is exhausted with breaches remaining, that CU is added to
   the escalation set handed to the per-CU lead, with its breach lines
   injected into the lead prompt.
6. `unattributed` breaches are never sent to a coder. They are aggregated
   and injected into the wave-lead prompt together with the guarded CU ids
   named by the rule.
7. `run_wave_parallel` contains no `try/except`; results are handled with
   `match`/`case` on `Ok`/`Err`.
8. `boundary-breaches.txt` is still written on every breach so the run
   leaves an auditable record.

## Tests

- `.LLMDD/tests/test_cu_boundary.py` — extend with:
  - `test_breach_detail_is_scope_correct` — a shared-write between
    `cu-002`/`cu-003` is not returned for `cu-001`; owned breaches are.
  - `test_correction_uses_same_model_twice` — a fake runner records the
    models used; both invocations receive the original model and no
    fallback id appears.
  - `test_correction_stops_after_three_attempts` — cap respected.
  - `test_correction_logs_are_attributed` — `coder_writes` picks up
    `cu-001.correction-0.log`.
  - `test_unattributed_routes_to_wave_lead` — unattributed breach lines
    appear in the wave-lead context, and no coder invocation is recorded.
  - `test_lead_prompt_contains_breach_detail` — the per-CU lead prompt
    built for an escalated CU contains the violating path.
- Command: `python3 -m unittest discover -s .LLMDD/tests -v`
- Expected: all tests pass, exit 0.
- Python probe: `uvx mypy --strict .LLMDD/tools/run-waves.py` if the tool is
  available in the environment; record "not installed" otherwise and rely
  on `ast.parse` plus the behavioural tests.
