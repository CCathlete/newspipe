# feat-1: Per-CU ITR directories with enforced components

- Status: Implemented
- Created: 2026-10-09
- Feature ID: feat-1-per-cu-itr-directories

## Synopsis

Motivation: the current compiler emits one flat `<id>.itr` frame per CU plus
global `ARCH.itr`/`LEGEND.itr`. That single frame mixes concerns (which DTR
area is affected, what the CU must build, how to build it, how to verify
it), so Coders improvise structure and the compiler cannot reject
under-specified CUs.

This feature gives every CU its own subdirectory in the ITR folder with four
enforced components, adds a root-level `SYNOPSIS.itr`, and makes compilation
fail with a relevant error when any rule is not met.

Scope: `itr-compiler` enforcement rules and ITR layout only. Per-CU
overrides of global files are created manually by the Advisor and are
explicitly NOT compiler-enforced.

## Implementation status

Implemented. Waves 1–4 done, 0 escalations; implementing commit `de32e82`
(`feat(feat-1): per-CU ITR directories`), ITR directory
`.LLMDD/ITRS/feat-1-per-cu-itr-directories/`. Verified: `sbt test` 87/87
(21 suites); live-binary probes for T1 (folders + SYNOPSIS, no blobs),
T2 (missing components fail naming CU + pieces), T3/T7 (missing summary
fails), legacy compat (in-folder blob, root clean); cross-layer import
audit clean. E2E verify green.

## Acceptance criteria

1. Compiled ITR directory contains one subdirectory per regular CU, named
   after the CU id (e.g. `cu-001/`).
2. Each CU subdirectory contains `COORDINATES.itr` stating the DTR area
   affected by the CU; a CU may reference an area that does not exist yet,
   which means the CU creates it.
3. Each CU subdirectory contains `REQUIREMENTS.itr` stating what part of the
   feature implementation this CU adds.
4. Each CU subdirectory contains `IMPLEMENTATION_STEPS.itr` with the actual
   code change/creation instructions for the Coder, divided into steps; this
   file is gitignored.
5. Each CU subdirectory contains `ACCEPTANCE.itr` with per-CU acceptance
   criteria and per-CU acceptance (including regression) tests.
6. A Coder that does not meet its CU acceptance tests retries until reaching
   `MAX_ATTEMPTS` (defined in root `SYNOPSIS.itr`) before escalating to the
   code lead.
7. `ARCH.itr` and all other existing global ITR components stay in the ITR
   folder root and apply to all CUs.
8. A CU may carry a manually created per-CU override file (written by the
   Advisor into the CU folder); the compiler must accept it without
   enforcing its content.
9. The ITR root contains `SYNOPSIS.itr` naming which features this ITR
   implements, defining the `MAX_ATTEMPTS` retry parameter, and carrying
   an implementation summary stating what the ITR builds.
10. Compilation fails with a relevant error message when any of criteria
    1–5, 7, 9, or 11 is not met (missing component, missing subdirectory,
    missing `SYNOPSIS.itr`/`MAX_ATTEMPTS`/implementation summary, stray
    flat frame).
11. The writer emits no per-CU `.itr` blob for a CU carrying components —
    only its four component files. A component-less legacy CU keeps exactly
    one `<cu-id>.itr` frame inside its own folder. No `.itr` file for a
    regular CU ever lands at the ITR root; root holds globals
    (`ARCH.itr`, `LEGEND.itr`, `SYNOPSIS.itr`) only.

## Tests

Executable tests proving each criterion (Advisor-written, run by the code
lead in E2E verify):

- `T1` (criteria 1–5, 9, 11): compile a batch with `arch`, `legend`, and two
  regular CUs via `.LLMDD/tools/itr-compiler --compile --json-content
  <batch> --out-folder <itr>/`; assert `<itr>/cu-<id>/` exists per CU and
  each holds `COORDINATES.itr`, `REQUIREMENTS.itr`,
  `IMPLEMENTATION_STEPS.itr`, `ACCEPTANCE.itr`, and `<itr>/SYNOPSIS.itr`
  exists with a `MAX_ATTEMPTS` entry. Command: `test -f` checks + `grep
  MAX_ATTEMPTS <itr>/SYNOPSIS.itr`. Also assert no `<itr>/cu-<id>.itr`
  flat files and no `cu-<id>/cu-<id>.itr` blobs exist for
  component-carrying CUs.
- `T2` (criterion 10, missing component): compile a batch whose CU content
  omits one required component; assert exit code is non-zero and stderr
  names the missing component.
- `T3` (criterion 10, missing SYNOPSIS data): compile without
  `MAX_ATTEMPTS` defined; assert non-zero exit naming `MAX_ATTEMPTS`.
- `T4` (criterion 8): place a hand-written override file in a CU folder,
  recompile with `--force`; assert exit 0 and the override file is
  preserved byte-for-byte.
- `T5` (criterion 4): assert `IMPLEMENTATION_STEPS.itr` is ignored by git
  (`git check-ignore <itr>/cu-001/IMPLEMENTATION_STEPS.itr` exits 0).
- `T6` (criteria 2, 6): compile a CU whose coordinates reference a
  not-yet-existing DTR area (assert exit 0 — creation semantics); and a
  runner-level test asserting the retry-then-escalate behavior honors
  `MAX_ATTEMPTS` from `SYNOPSIS.itr`.
- `T7` (criteria 9–10): compile with the implementation summary missing or
  empty in the synopsis data; assert non-zero exit naming the summary.
