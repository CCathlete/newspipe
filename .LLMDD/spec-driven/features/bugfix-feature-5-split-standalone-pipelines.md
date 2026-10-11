# bugfix-feature-5: split-standalone-pipelines

- Status: Proposed
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

Proposed. No CUs assigned yet.

## Acceptance criteria

1. `pipelines/ingestion/build.sbt` and `pipelines/enrichment/build.sbt`
   exist; `sbt compile` exits 0 inside each directory standalone
2. Ingestion `runOnce` fetches, publishes, writes raw-only rows, marks
   `seen` only after write; a failed write retries next poll; `sbt test`
   green in `pipelines/ingestion`
3. Enrichment consumes the raw topic, merges enriched rows by `id`,
   skips failures without committing them away; `sbt test` green in
   `pipelines/enrichment`
4. No cross-pipeline imports: each tree references only its own files
5. Legacy root `src/` plus root `build.sbt` untouched and still green
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
