# bugfix-feature-5: split-standalone-pipelines

- Status: Implemented
- Created: 2026-10-11
- Feature ID: bugfix-feature-5-split-standalone-pipelines

## Synopsis

Fixes bug-4-dedup-before-sink-write-loses-data (fast path).

The single pipeline blocks bronze writes behind sequential per-article
`opencode` calls, so one slow enrichment stalls everything and a sink
failure loses already-paid enrichments. This feature splits newspipe into
two fully separate standalone pipelines under `pipelines/`, each with its
own `build.sbt` and duplicated model/port sources (no shared modules):

- `pipelines/ingestion/` (fast path): RSS fetch, Kafka publish, raw-only
  bronze append. `seen` is marked only after a successful write, so a
  failed batch retries on the next poll (bug-4 fix for this path).
- `pipelines/enrichment/` (slow path): Kafka raw-topic consumer,
  `opencode` enrichment, Delta merge backfill by `id`. Failures stay
  unenriched and are repolled via uncommitted offsets.

Package names stay `newspipe.*` in both trees. Class renames are out of
scope; behavior of each stage matches the current code.

Scope (in):
- Both scaffolds (build.sbt, build.properties, application.conf subset),
  duplicated domain models, valueObjects, and ports (enrichment Ports
  gains `RawConsumer` plus `BronzeMergeSink` traits)
- Ingestion: trimmed service (no enrich step), subset Env, container, Main,
  service spec proving retry-then-dedup
- Enrichment: `KafkaRawConsumer`, `DeltaMergeSink` (MERGE on id),
  `EnrichmentService`, subset Env, container, Main, merge spec
- Checklist split record

Scope (out):
- Class renames; shared/common module; silver/gold; dashboard
- Removal of the legacy root tree (stays until both pipelines are E2E green)

## Implementation status

Implemented. Eight CUs across five waves; landed in 6185bf5, with the
legacy root tree retired in e672049. Closed 2026-10-19.

Run-record gap, recorded rather than hidden: `newspipe.convergence.json`
reports NEEDS_ATTENTION because waves 3 and 4 carry `lead_status: 1`, and
cu-004 and cu-007 have no feedback file and no wave-3/wave-4 verification
report (only waves 1, 2 and 5 produced one). Both CUs' deliverables were
nevertheless verified present in the tree at close -- IngestionServiceSpec
and MergeBackfillSpec are the two they own, added by 6185bf5 -- and their
acceptance probes, `sbt test` with workdir pipelines/ingestion and
pipelines/enrichment, were re-run live and passed 37/37 and 41/41. The
lead-process failure cost the record, not the code.

## Acceptance criteria

1. `pipelines/ingestion/build.sbt` and `pipelines/enrichment/build.sbt`
   exist; `sbt compile` exits 0 inside each directory standalone
2. Ingestion `runOnce` fetches, publishes, writes raw-only rows, marks
   `seen` only after write; a failed write retries next poll; `sbt test`
   green in `pipelines/ingestion`
3. Enrichment consumes the raw topic, merges enriched rows by `id`, and
   excludes enrichment failures from the bronze merge while still
   committing the batch; `sbt test` green in `pipelines/enrichment`
   Amended 2026-10-19. The criterion previously read "skips failures
   without committing them away", which its own proof contradicts:
   `MergeBackfillSpec` asserts the failed item is absent from the merged
   list (line 48) and that `commit()` ran (line 49). The tested behaviour
   is the correct one -- a failed LLM call must not reach bronze, and
   acking the batch is what stops a poison-pill item looping forever,
   which is why bugfix-feature-6 bounds batches to 20 records. The prose
   was wrong, not the code.
4. No cross-pipeline imports: each tree references only its own files
5. The legacy root `src/` and root `build.sbt` were retired as the last
   step of the split rather than kept green
   Amended 2026-10-19. The original wording required them "untouched and
   still green", but commit e672049 ("remove legacy root Scala tree after
   pipelines split (bugfix-feature-5 E2E green)"), part of this feature's
   own close, deleted both. The split is only complete when one copy of
   each tree remains, so the criterion was superseded by the very
   decision it was written to guard.
6. Checklist records the split

## Tests

- `sbt compile` inside each pipeline directory (criterion 1)
- Ingestion service spec: raw-only write, dedup second run, failing sink
  retries then dedups (criteria 2, bug-4 proof)
- Enrichment merge spec: stubbed service skips failures; Delta merge
  updates one row without duplicating (criterion 3)
- `grep -rn 'pipelines/ingestion' pipelines/enrichment/` prints nothing
  and vice versa (criterion 4)
- Root `sbt compile` still exits 0 (criterion 5)
