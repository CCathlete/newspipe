# bugfix-feature-3: Accept bare label aliases

- Status: Proposed
- Created: 2026-10-10
- Feature ID: bugfix-feature-3-accept-bare-label-aliases

## Synopsis

Motivation: bug-3 (`bug-3-silent-blob-on-bare-section-labels`) — a batch
whose CU `content` carries the four sections with bare labels
(`REQUIREMENTS` without trailing colon) compiles with exit 0 into a silent
legacy blob (`cu-<id>/cu-<id>.itr`), with no warning that the sections were
unrecognized, while the partially-bare case fails loudly. The `compile-itr`
skill's "CU content bar" names the sections without showing the colon
requirement, so Advisor-drafted batches naturally use bare labels and go
green into the wrong layout. Per Designer decision, the fix accepts bare
labels as aliases (rather than failing them).

Scope: `itr-compiler` inline-section splitting (`JSONFormat.splitInlineSections`
plus the shared `CU.sectionLabels` it matches against) and the validation
agreement over the same label set. In: bare labels split into components
exactly like colon labels (colon forms keep match priority), mixed
bare/colon batches split fully, partial-label batches keep failing loudly
naming CU id + missing sections. Out: `FileSystem.write` branches,
`Compile` orchestration, `YAMLFormat` (its inline splitting is a separate
documented follow-up), `dtr-builder`, `run-waves`, skill-doc rewrites except
where the batch format itself changes.

## Implementation status

Proposed. No CUs assigned, no fix merged. Intended to close
`bug-3-silent-blob-on-bare-section-labels` once waves are done and E2E
verify is green. ITR directory
`.LLMDD/ITRS/bugfix-feature-3-accept-bare-label-aliases/`.

## Acceptance criteria

1. A regular CU whose `content` carries the four sections with bare labels
   compiles with exit 0 to a `cu-<id>/` folder holding `COORDINATES.itr`,
   `REQUIREMENTS.itr`, `IMPLEMENTATION_STEPS.itr`, `ACCEPTANCE.itr`, with no
   `cu-<id>/cu-<id>.itr` blob.
2. Mixed batches (some sections bare, some colon) split fully to the same
   four-file folder; colon-label behavior is unchanged.
3. A partially-labeled CU (fewer than four recognizable sections, bare or
   colon) in a new-schema batch still fails with non-zero exit naming the CU
   id plus the missing section(s); no blob left behind.
4. `sbt test` stays green (no regressions in existing parser/writer suites).

## Tests

- `T1` (criterion 1, bug-3 repro 1): compile the all-bare batch from bug-3
  via `.LLMDD/tools/itr-compiler --compile --json-content <batch>
  --out-folder <tmp>/`; assert exit 0 and
  `<out>/cu-001/{COORDINATES,REQUIREMENTS,IMPLEMENTATION_STEPS,ACCEPTANCE}.itr`
  exist with no `<out>/cu-001/cu-001.itr` blob.
- `T2` (criterion 2, bug-3 repro 2 + mixed): the all-colon batch stays green
  with four component files; a mixed bare/colon batch yields the same
  four-file folder.
- `T3` (criterion 3): a batch whose CU carries only two sections fails
  non-zero, stderr names the CU id and the two missing sections, no blob
  exists for the offender.
- `T4` (criterion 4): `sbt test` from `itr-compiler/`; expected all suites
  green — including a new parser-level test over bare-label batches
  asserting four populated components.
