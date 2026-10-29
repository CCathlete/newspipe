# bug-3: Silent blob fallback on bare section labels

- Status: Resolved
- Created: 2026-10-10
- Resolved: 2026-10-18
- Bug ID: bug-3-silent-blob-on-bare-section-labels

## Bug detail

Symptoms: a batch whose CU `content` carries the four sections with
**bare** labels (`REQUIREMENTS` without a trailing colon) compiles with
exit 0 and silently emits the legacy single-blob layout
(`cu-<id>/cu-<id>.itr`) instead of the four component files — no warning
that the sections were not recognized. The `compile-itr` skill's "CU
content bar" names the sections without showing the colon requirement,
so an Advisor following the skill naturally drafts bare labels and gets
blobs with a green exit code.

Reproduction (from repo root; needs only `java`, uses the repo-root
assembly `itr-compiler/itr-compiler`, 6097863 bytes, built Oct 10):

```sh
# 1. all-bare labels: SILENT legacy blob, exit 0
cat > /tmp/batch_allbare.json <<'EOF'
[{"cu-id":"arch","cu-type":"arch","dtr-coordinates":[],"content":"arch rules"},{"cu-id":"legend","cu-type":"legend","dtr-coordinates":[],"content":"legend text"},{"cu-id":"cu-001","dtr-coordinates":[],"content":"REQUIREMENTS\nBuild X.\nCOORDINATES\nTYPE.Foo.\nIMPLEMENTATION_STEPS\n1. Edit a/b.\nACCEPTANCE\nTest T1 green."}]
EOF
rm -rf /tmp/itr_allbare && ./itr-compiler/itr-compiler --compile --json-content /tmp/batch_allbare.json --out-folder /tmp/itr_allbare
# EXPECTED: four component files (or a loud error naming the bad labels)
# ACTUAL: Compiled 3 CU(s), exit 0, only /tmp/itr_allbare/cu-001/cu-001.itr

# 2. same content with colon labels: components, exit 0
cat > /tmp/batch_allcolon.json <<'EOF'
[{"cu-id":"arch","cu-type":"arch","dtr-coordinates":[],"content":"arch rules"},{"cu-id":"legend","cu-type":"legend","dtr-coordinates":[],"content":"legend text"},{"cu-id":"cu-001","dtr-coordinates":[],"content":"REQUIREMENTS:\nBuild X.\nCOORDINATES:\nTYPE.Foo.\nIMPLEMENTATION_STEPS:\n1. Edit a/b.\nACCEPTANCE:\nTest T1 green."}]
EOF
rm -rf /tmp/itr_allcolon && ./itr-compiler/itr-compiler --compile --json-content /tmp/batch_allcolon.json --out-folder /tmp/itr_allcolon
# EXPECTED and ACTUAL: Compiled 3 CU(s), exit 0,
#   cu-001/{COORDINATES,REQUIREMENTS,IMPLEMENTATION_STEPS,ACCEPTANCE}.itr

# 3. mixed labels (two bare, two colon): LOUD failure, exit 1
# (bare REQUIREMENTS/COORDINATES + colon IMPLEMENTATION_STEPS:/ACCEPTANCE:)
# ACTUAL: IllegalStateException: ITR enforcement failed:
#   CU 'cu-001' is missing component 'coordinates'
#   CU 'cu-001' is missing component 'requirements'
```

So partial recognition fails loudly (good) while zero recognition
succeeds silently (bad): the worst combination — one missing colon
anywhere still fails, but all colons missing looks green.

Environment: repo-root `itr-compiler/itr-compiler` assembly, OpenJDK 21
(Temurin-21.0.12+8).

Suspected cause:
`itrcompiler/infrastructure/parsers/JSONFormat.scala`,
`splitInlineSections` matches only the colon labels from
`CU.sectionLabels` (`REQUIREMENTS:` etc.). Zero matches returns
`Map.empty`, and `FileSystem.write` branches on `components.nonEmpty`:
empty takes the legacy single-blob branch with no diagnostic. The
enforcement that catches the partial case never runs for the empty
case. Related: bug-2 (blobs instead of folders) — its colon-label
minimal repro now emits components with this binary, so bug-2's
suspected cause looks stale; this bare-label hole remains.

Impact: Advisor-drafted batches that follow the skill prose but omit
colons compile green into the wrong layout; the error surfaces far
downstream (Coder gets blobs, `verify-cu-structure`/`run-waves`
expect folders).

## Resolution status

Resolved on 2026-10-18 by the Designer, on verification that the fix
merged in `93d2cc8` is present and its proving tests pass. Verified in
this repo rather than inferred from the commit message.

## Fix summary (Resolved only)

Fixing commit: `93d2cc8` ("bugfix-feature-3/cu-001: accept bare
section-label aliases in inline splitting (bug-3)"). Spec:
`bugfix-feature-3-accept-bare-label-aliases`.

`CU.sectionLabels` (domain) gained a bare, colon-less alias per component
key, with the colon form listed first and canonical.
`JSONFormat.splitInlineSections` now matches colon-first per key
(`groupBy key` + `collectFirst`), so a bare prefix can never shadow a
colon label, and `contentSectionKeys` agrees with it by construction over
the same list. All-bare and mixed bare/colon batches compile to four-file
CU folders instead of silently falling back to a legacy blob. An explicit
`components` object still wins, all-four-absent still takes the legacy
single-frame path, and partially-labelled batches keep failing loudly
naming the CU id plus the missing sections. No changes to
`FileSystem.scala`, `Compile.scala` or `YAMLFormat.scala`.

## Tests proving resolution (Resolved only)

`sbt 'testOnly itrcompiler.infrastructure.parsers.JSONFormatTest'` run in
this repo on 2026-10-18: **11 tests run, 11 succeeded, 0 failed, 0
aborted, 0 pending.** All three bug-3 repro tests are green:

- "should split all four inline sections from bare labels (bug-3 repro 1)"
  - the original silent-blob repro now yields four component files
- "should split mixed bare and colon labels fully (bug-3 repro 3)"
  - the mixed case still succeeds fully
- "should prefer the colon label over its bare prefix (bare never shadows
  colon)" - the colon-first precedence invariant

The three tests that were green before the fix and had to stay green also
pass: all-four-present split, one-section-missing split, and
"should leave components empty when no inline sections are present" - the
last confirming the legacy single-frame path is preserved for genuinely
unlabelled content.

Caveat on the original repro commands: they invoke the repo-root
assembly `./itr-compiler/itr-compiler`, whose mtime (2026-10-10 16:49)
predates the fix commit (2026-10-10 17:01) by twelve minutes, and the
assembly is gitignored. Running those commands as written exercises the
stale binary and would still show the old behaviour. The test-level
evidence above is the authoritative proof for this closure; the assembly
should be rebuilt before anyone re-runs the CLI repro verbatim.
