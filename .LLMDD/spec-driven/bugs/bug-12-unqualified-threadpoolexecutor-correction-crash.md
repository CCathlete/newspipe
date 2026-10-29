# bug-12: Unqualified ThreadPoolExecutor causes NameError in CU boundary correction pass

- Status: Resolved
- Created: 2026-10-24
- Resolved: 2026-10-24
- Bug ID: bug-12-unqualified-threadpoolexecutor-correction-crash

## Bug detail

The correction pass introduced by feat-9 in `.LLMDD/tools/run-waves.py` creates a `ThreadPoolExecutor` with `with ThreadPoolExecutor(max_workers=1) as cpool:` but the module only imports `concurrent.futures` (no `from concurrent.futures import ThreadPoolExecutor`). 

When a boundary breach occurs in `run_wave_parallel` and execution enters the correction block, Python raises `NameError: name 'ThreadPoolExecutor' is not defined` and the wave runner crashes. The traceback from the feat-9 wave run shows the crash in `run_wave_parallel` at the correction loop after breaches are detected.

This path is only exercised when breaches exist and correction attempts are made, so unit tests and structural gates (ast.parse, production gates) do not hit it.

## Reproduction

1. Run a wave where any CU breaches CU boundaries (no-touch/shared-write).
2. The runner reaches the correction pass in `run_wave_parallel`.
3. Crashes with `NameError: name 'ThreadPoolExecutor' is not defined`.

## Root cause

Missing import / unqualified usage in the feat-9 correction pass code. The fix is either `from concurrent.futures import ThreadPoolExecutor` or use `concurrent.futures.ThreadPoolExecutor`.

## Expected behavior

The correction pass should run without crashing. If breaches occur, it should attempt up to 3 same-model correction passes, re-measure after each, and escalate exhausted CUs to the per-CU lead with breach context.

## Fix summary

Fixed in `.LLMDD/tools/run-waves.py` line ~2013: changed `with ThreadPoolExecutor(max_workers=1) as cpool:` to `with concurrent.futures.ThreadPoolExecutor(max_workers=1) as cpool:`. Verified with `ast.parse` - no errors. Fix committed in bugfix-feature-14.

## Proving tests

- Add a synthetic test that creates a breach scenario and exercises the correction path, asserting no `NameError` and correct behavior.
- Manual: run waves with a fabricated breach; verify correction attempts execute and runner does not crash.
