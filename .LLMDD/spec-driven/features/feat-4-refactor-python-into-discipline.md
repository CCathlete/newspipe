# feat-4: Refactor Python Code Into Discipline

- Status: Implemented
- Created: 2026-10-11
- Feature ID: feat-4-refactor-python-into-discipline

## Synopsis

Motivation: the repo just promoted four Python rules to first-class
architecture constraints (`ARCH.PYTHON.*` in the system tensor), but the
repo's own Python code predates them. The wave runner, the budget test, and
the validator must obey the rules they enforce — otherwise every future CU
ships with a standing CRITICAL deviation.

Scope (in): full `ARCH.PYTHON.*` compliance for
`.LLMDD/tools/run-waves.py` (1525 lines) and
`.LLMDD/tools/test_synopsis_budget.py` (46 lines), with zero behavior
change. `.LLMDD/tools/validate-steps.py` is already compliant (done in the
discipline commit) and serves as the pattern reference. Scope (out):
`dtr-builder/src/test/resources/fixtures/src/util.py` is a dtr-builder
extraction fixture — changing it would corrupt fixture expectations, so it
is explicitly excluded; non-Python tooling (Scala) is exempt per
`ARCH.PYTHON.SCOPE`.

## Implementation status

Proposed. On approval: compile the batch at
`.LLMDD/ITRS/feat-4-refactor-python-into-discipline/batch.json` into that
ITR directory, run the waves in `.LLMDD/ITRS/feat-4-refactor-python-into-discipline/waves.json` in order (wave 1: cu-001 + cu-002, wave 2: cu-003, wave 3: cu-004). Implemented 2026-10-11 by the pipeline operator following the compiled frames in order; all CUs COMPLETED, T1-T7 green, per-CU feedback in the ITR feedback directory. Prerequisite gate fixes already applied: validate-steps accepts repo-rooted paths and `{"synopsis", "cus"}` envelope batches (mechanical gate passes 4/4 on this batch).

## Acceptance criteria

1. No `typing.Any` / `Any` type usage remains in `run-waves.py` or
   `test_synopsis_budget.py`; no bare `dict`/`list`/`tuple`/`set`
   annotations remain (custom `TypedDict`/`TypeAlias`/`dataclass` types
   used instead).
2. Every first binding in both files carries an explicit type annotation
   (`name: Type = value`, pre-declared loop/match/except targets,
   annotated signatures); reviewer confirms by diff inspection.
3. All dataclasses are slotted; value-semantics holders (`Ok`, `Err`,
   `CoderOutput`, `ModelFallbackChain`) are frozen; remaining mutable
   accumulators (`TokenUsage`, `WaveCredits`, `CreditTracker`) are slotted
   with the frozen-deviation justification recorded; class `C` keeps its
   shape with annotated constants and the `disable` justification recorded.
4. `try`/`except` remains only at the documented monadic boundary sites
   (five I/O wrappers + `future_result` bridge + `_read_text` /
   `_parse_event_line` translators), each carrying a `MONADIC-BOUNDARY`
   marker; all other handlers (`_detect_refusal`, `_detect_error`,
   `_file_cu_id`, `_git`, per-line JSON parsing) are match/case on
   `Ok`/`Err`.
5. Behavior is preserved: `test_synopsis_budget.py` exit codes/messages
   unchanged, `run-waves.py --dry-run` plan output unchanged in shape,
   `validate-steps.py` output byte-identical on the feat-1 ITR.
6. Only the two in-scope files change (`git diff --stat` shows nothing
   else).

## Tests

- T1: `python3 -m compileall -q .LLMDD/tools/run-waves.py .LLMDD/tools/test_synopsis_budget.py` exits 0.
- T2: `grep -rn "Any" .LLMDD/tools/run-waves.py .LLMDD/tools/test_synopsis_budget.py` prints only prose mentions of the word Any, no `typing.Any` import or `Any` annotation; `grep -nE ":\s*(dict|list|tuple|set)(\s|=|,|\)|$)"` on both files prints nothing.
- T3: `grep -n "slots=True" .LLMDD/tools/run-waves.py` shows every `@dataclass`; `grep -n "frozen=True"` shows `Ok`, `Err`, `CoderOutput`, `ModelFallbackChain`.
- T4: `grep -n "try:" .LLMDD/tools/run-waves.py .LLMDD/tools/test_synopsis_budget.py` lists exactly the allowlisted boundary sites, each with a `MONADIC-BOUNDARY` marker comment.
- T5: `python3 .LLMDD/tools/test_synopsis_budget.py .LLMDD/ITRS/feat-1-per-cu-itr-directories/SYNOPSIS.itr` prints `BUDGET_OK 3`, exit 0.
- T6: `python3 .LLMDD/tools/run-waves.py --itr .LLMDD/ITRS/feat-1-per-cu-itr-directories --app feat-1 --dry-run` exits 0 with the same plan shape as before the refactor.
- T7: `python3 .LLMDD/tools/validate-steps.py .LLMDD/ITRS/feat-1-per-cu-itr-directories/` output is byte-identical to the pre-refactor run.
- R1 (review): reviewer walks the `git diff` and confirms criterion 2 (annotation completeness) site by site.
