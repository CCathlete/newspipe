# bug-1: JSON batch key order rejected

- Status: Resolved
- Created: 2026-10-09
- Resolved: 2026-10-10
- Bug ID: bug-1-json-batch-key-order-rejected

## Bug detail

Symptoms: `itr-compiler --compile --json-content <batch>` fails with
`Missing required parts: ArchCU, LegendCU` (exit 1) even though the batch
contains valid `arch` and `legend` entries. Reordering the keys inside each
batch object — with byte-identical values — compiles cleanly (exit 0).

The parser only accepts one key order per CU object:
`cu-id`, optional `cu-type`, `dtr-coordinates`, `content`. Any other order
(e.g. `content` first, or alphabetical via `sort_keys`) matches zero CUs,
so validation reports the parts as missing. The error message therefore
misdirects: it blames absent ARCH/LEGEND content instead of the actual
cause (unparsed objects).

Reproduction (from repo root; needs only `java`, no sbt):

```sh
# 1. content-first key order (valid JSON, RFC 8259 objects are unordered)
cat > /tmp/batch_content_first.json <<'EOF'
[
  {"content": "arch rules", "cu-id": "arch", "cu-type": "arch", "dtr-coordinates": []},
  {"content": "legend text", "cu-id": "legend", "cu-type": "legend", "dtr-coordinates": []},
  {"content": "REQUIREMENTS\nX", "cu-id": "cu-001", "dtr-coordinates": []}
]
EOF
./itr-compiler/itr-compiler --compile --json-content /tmp/batch_content_first.json --out-folder /tmp/itr_a/
# EXPECTED: Compiled 3 CU(s), exit 0
# ACTUAL: IllegalStateException: Missing required parts: ArchCU, LegendCU, exit 1

# 2. same values, cu-id-first order
cat > /tmp/batch_id_first.json <<'EOF'
[
  {"cu-id": "arch", "cu-type": "arch", "dtr-coordinates": [], "content": "arch rules"},
  {"cu-id": "legend", "cu-type": "legend", "dtr-coordinates": [], "content": "legend text"},
  {"cu-id": "cu-001", "dtr-coordinates": [], "content": "REQUIREMENTS\nX"}
]
EOF
./itr-compiler/itr-compiler --compile --json-content /tmp/batch_id_first.json --out-folder /tmp/itr_b/
# EXPECTED and ACTUAL: Compiled 3 CU(s), exit 0
```

Environment: repo-root `itr-compiler/itr-compiler` assembly, OpenJDK 21
(Temurin-21.0.12+8); same result from the copy shipped at
`newspipe/.LLMDD/tools/itr-compiler`. Formatting controlled: pretty-printed
vs compact makes no difference, only key order does.

Suspected cause:
`itr-compiler/src/main/scala/itrcompiler/infrastructure/parsers/JSONFormat.scala`,
`parse()`, the `cuLocator` regex (line ~80-81) hard-codes the sequence
`"cu-id" ... "cu-type" ... "dtr-coordinates" ... "content"`. Non-matching
objects are silently skipped (no warning), yielding an empty CU list that
`RequiredPartsValidation` then reports as missing parts. The class docstring
even documents the fixed order (`[{cu-id, [cu-type,] dtr-coordinates:[str],
content:str}]`) and notes the parser is a stopgap ("consider a full JSON
library").

Impact: any batch round-tripped through JSON tooling that reorders keys
(`sort_keys`, formatters, editors) breaks the build with a misleading error;
batch authors must obey an undocumented ordering rule.

## Resolution status

Resolved. Fix merged in commit `078c65b`
(`Fix: itr-compiler batch parsing + CU layout (bugfix-feature-2, waves 1-3
consolidated)`, cu-001); proving tests below green on 2026-10-10.

## Fix summary (Resolved only)

`JSONFormat.parse` no longer uses the order-hardcoded `cuLocator` regex: a
per-object scan locates each CU object extent and extracts `cu-id`,
optional `cu-type`, `dtr-coordinates`, and `content` independently of key
order (iterative `parseJsonString` content extraction kept, no regex
backtracking). Class docstring updated. Fixing commit: `078c65b` (cu-001).

## Tests proving resolution (Resolved only)

Failing-before (Oct 9 binary): content-first batch →
`IllegalStateException: Missing required parts: ArchCU, LegendCU`, exit 1;
identical-values cu-id-first batch → exit 0.

Passing-after (2026-10-10, rebuilt binary 6097863 bytes,
md5 `a86cb864290e5dcb2f8573f78bcb56fd`):

1. Both CLI repro commands from Bug detail re-run → `Compiled 3 CU(s)`,
   exit 0 for both, identical output file sets
   (`diff` of `find` listings clean).
2. `sbt "testOnly itrcompiler.infrastructure.parsers.JSONFormatTest
   itrcompiler.infrastructure.parsers.YAMLFormatTest"` from `itr-compiler/`:
   12 tests, 0 failed — including `should parse equal CUBatch across key
   orders (key-order)`.
