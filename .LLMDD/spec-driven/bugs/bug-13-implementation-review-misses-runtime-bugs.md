# bug-13: Review process misses runtime bugs - implementation review lacks integration testing of breach/correction paths

- Status: Resolved
- Created: 2026-10-24
- Resolved: 2026-10-24
- Bug ID: bug-13-implementation-review-misses-runtime-bugs

## Bug detail

During review of feat-9 (enforce CU boundary correction pass), the bug in `run-waves.py` where `ThreadPoolExecutor` was used unqualified (causing `NameError` in the correction pass) was not caught. The review process failed to detect this runtime defect despite the code being modified.

## Root cause

1. **Over-reliance on self-verification**: The CU (cu-001) was marked `STATUS=COMPLETED` with `VERIFICATION_RESULT=PASSED` by the coder. The review trusted this without independently tracing the injected code.
2. **No integration testing of new code paths**: The `NameError` only occurs when a boundary breach exists and the correction pass executes. `ast.parse`, unit tests, and structural production gates (`TestCUProductionGate`) all passed but never exercised this path.
3. **Gates are too narrow**: `TestCUProductionGate` validates structure (DTR coordinates, file layout, test mappings, tracked files) but does not validate runtime behavior of new control flow. It cannot catch import/symbol errors in branches never executed.
4. **Missing basic sanity checks**: A trivial grep for symbol usage vs imports would have caught `ThreadPoolExecutor` used unqualified while only `import concurrent.futures` exists. This check was not performed.
5. **Conflation of "implemented" vs "working"**: feat-9 was marked as Implemented because CUs were written/committed, not because the modified runner was verified end-to-end with actual breach scenarios.

## Expected behavior

Implementation review must:
- Trace injected code line by line, especially new control flow paths
- Perform basic symbol/import sanity checks (grep for used symbols vs imports)
- Exercise integration scenarios that trigger new branches (e.g., create synthetic breach and run through correction path)
- Not blindly trust CU self-verification; independently verify with tests/smoke runs
- Distinguish between "structure complete" and "runtime correct"

## Impact

feat-9 was marked Implemented but the runner crashes on first breach/correction attempt. The feature cannot be used as designed until bug-12 is fixed. This represents a process failure in implementation review capabilities.

## Fix summary

Enhanced `.LLMDD/tools/skill-check.py` to enforce deterministic execution of all skills including `review-implementation`:
- Extracts steps, commands, files from skill definitions
- Enforces mandatory requirements per skill
- Provides strict execution protocol with step tracking (STATUS: IN_PROGRESS/OK/FAIL with evidence)
- Forces reading entire skill file, executing each step completely, verifying outputs
- Added `--print-prompt`, `--plan`, `--enforce` modes for deterministic execution

This ensures implementation reviews follow the skill steps exactly and catch issues like missing imports, untested code paths, etc. Fix committed in bugfix-feature-14.
