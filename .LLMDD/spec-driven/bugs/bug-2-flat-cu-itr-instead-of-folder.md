# bug-2: Compiler emits cu-*.itr blobs instead of CU folder with component files

- Status: Resolved
- Created: 2026-10-10
- Resolved: 2026-10-10
- Bug ID: bug-2-flat-cu-itr-instead-of-folder

## Bug detail

Symptoms: compiling a normal Advisor-drafted batch produces one
`cu-<id>/cu-<id>.itr` blob per regular CU instead of a `cu-<id>/` folder
holding the four separate component files (`COORDINATES.itr`,
`REQUIREMENTS.itr`, `IMPLEMENTATION_STEPS.itr`, `ACCEPTANCE.itr`).

Reproduction (from repo root, needs only the prebuilt binary):

```sh
rm -rf /tmp/itr_repro_feat1
.LLMDD/tools/itr-compiler --compile --json-content .LLMDD/ITRS/feat-1-per-cu-itr-directories/batch.json --out-folder /tmp/itr_repro_feat1
# EXPECTED: per feat-1 criteria 1-5/11 and the checked-in example ITR,
#   /tmp/itr_repro_feat1/cu-001/COORDINATES.itr
#   /tmp/itr_repro_feat1/cu-001/REQUIREMENTS.itr
#   /tmp/itr_repro_feat1/cu-001/IMPLEMENTATION_STEPS.itr
#   /tmp/itr_repro_feat1/cu-001/ACCEPTANCE.itr
#   and no cu-001/cu-001.itr blob anywhere.
# ACTUAL (exit 0): Compiled 7 CU(s), all regular CUs land as single blobs:
#   /tmp/itr_repro_feat1/cu-001/cu-001.itr
#   /tmp/itr_repro_feat1/cu-002/cu-002.itr
#   /tmp/itr_repro_feat1/cu-003/cu-003.itr
#   /tmp/itr_repro_feat1/cu-004/cu-004.itr
#   /tmp/itr_repro_feat1/cu-005/cu-005.itr
find /tmp/itr_repro_feat1 -type f | sort
```

Minimal repro with the same shape (no `components` key, sections inline in
`content` — exactly what the `compile-itr` skill's "CU content bar" tells the
Advisor to draft):

```sh
cat > /tmp/batch_inline.json <<'EOF'
[{"cu-id":"arch","cu-type":"arch","dtr-coordinates":[],"content":"arch rules"},{"cu-id":"legend","cu-type":"legend","dtr-coordinates":[],"content":"legend text"},{"cu-id":"cu-001","dtr-coordinates":[],"content":"REQUIREMENTS: build X. COORDINATES: TYPE.Foo. IMPLEMENTATION_STEPS: (1) edit a/b. ACCEPTANCE: test T1 green."}]
EOF
rm -rf /tmp/itr_inline && .LLMDD/tools/itr-compiler --compile --json-content /tmp/batch_inline.json --out-folder /tmp/itr_inline
# EXPECTED: /tmp/itr_inline/cu-001/{COORDINATES,REQUIREMENTS,IMPLEMENTATION_STEPS,ACCEPTANCE}.itr
# ACTUAL: /tmp/itr_inline/cu-001/cu-001.itr only
```

The checked-in example `.LLMDD/ITRS/feat-1-per-cu-itr-directories/cu-001/`
shows the intended layout (four component files, no blob), so a fresh
compile of its own `batch.json` no longer reproduces it.

Environment: `.LLMDD/tools/itr-compiler` (and identical
`itr-compiler/itr-compiler`, 6092538 bytes, built Oct 9), Linux, OpenJDK 21.

Suspected cause: `JSONFormat.parse()`
(`itr-compiler/src/main/scala/itrcompiler/infrastructure/parsers/JSONFormat.scala`)
populates `CU.components` only from an explicit `"components": {...}`
object (`parseComponentsForCU` / `parseComponentsObject`). Advisor batches
carry the four sections inline in the `content` string (the `compile-itr`
skill documents only `cu-id` / `dtr-coordinates` / `content`, no
`components` key), so `components` stays empty. `FileSystem.write()`
branches on `components.nonEmpty`: empty takes the single-blob branch
(`<cu-id>/<cu-id>.itr`). Nothing ever splits the inline
`REQUIREMENTS:` / `COORDINATES:` / `IMPLEMENTATION_STEPS:` / `ACCEPTANCE:`
sections into components. Unit/integration tests pass because
`Feat1LayoutTest` constructs `CU(..., components = fullComponents(...))`
directly, bypassing the parser — the end-to-end batch-file path is
uncovered.

## Resolution status

Resolved. Fix merged in commit `078c65b`
(`Fix: itr-compiler batch parsing + CU layout (bugfix-feature-2, waves 1-3
consolidated)`, cu-002); proving tests below green on 2026-10-10.

## Fix summary (Resolved only)

`JSONFormat.splitInlineSections` (called from `parse()` for `RegularCU`s)
cuts the four colon-labeled sections (`REQUIREMENTS:`, `COORDINATES:`,
`IMPLEMENTATION_STEPS:`, `ACCEPTANCE:` from `CU.sectionLabels`) out of the
CU's `content` into `CU.components` after content extraction; an explicit
`"components"` object wins when present, and all-four-absent keeps the
legacy single-frame path. `FileSystem.write` needed no change — its
`components.nonEmpty` branch was already correct; the parser simply never
fed it. Fixing commit: `078c65b` (cu-002).

## Tests proving resolution (Resolved only)

Failing-before (2026-10-10, Oct 9 binary 6092538 bytes): recompiling
`.LLMDD/ITRS/feat-1-per-cu-itr-directories/batch.json` yielded
`cu-001/cu-001.itr` … `cu-005/cu-005.itr` (5 blobs, 0 component files).

Passing-after (2026-10-10, rebuilt binary 6097863 bytes,
md5 `a86cb864290e5dcb2f8573f78bcb56fd`):

1. `.LLMDD/tools/itr-compiler --compile --json-content
   .LLMDD/ITRS/feat-1-per-cu-itr-directories/batch.json --out-folder
   /tmp/itr_repro_feat1` → `Compiled 7 CU(s)`, exit 0;
   `find /tmp/itr_repro_feat1 -name "cu-*.itr" | wc -l` = 0, each of
   `cu-001`…`cu-005` holds exactly `COORDINATES.itr`, `REQUIREMENTS.itr`,
   `IMPLEMENTATION_STEPS.itr`, `ACCEPTANCE.itr`, root holds `ARCH.itr` +
   `LEGEND.itr` only.
2. Minimal inline-section batch (`/tmp/batch_inline.json` from Bug detail) →
   `/tmp/itr_inline/cu-001/` holds the four component files, no
   `cu-001/cu-001.itr`.
3. `sbt test` from `itr-compiler/`: 22 suites, 93 tests, 0 failed
   (includes the new splitter/key-order/layout regression tests).
