# feat-16: Scan and validate RSS source manifests

- Status: Proposed
- Created: 2026-11-26
- Feature ID: feat-16-scan-and-validate-rss-sources

## Synopsis

Motivation: NewsPipe is a topic/domain-driven information collector. Its
long-term purpose is geopolitical information collection; the immediate need is
industry intelligence for banking and financial markets in Europe and the US.
Website scraping and Kafka infrastructure have been removed; OpenCode is the AI
harness, LLMDD drives design/implementation, and collected content is persisted
in the existing Bronze layer. The candidate feed inventory is assembled
externally from curated RSS repositories and directories and may contain
hundreds or thousands of entries. NewsPipe needs a deterministic, inexpensive
module that probes this inventory and tells the existing collection pipeline
which feeds are actually usable, without rediscovering everything on each run.

In scope: declarative candidate manifest ingestion (YAML or JSON) with
provenance and multiple candidate URLs per publisher; deterministic HTTP
probing, redirect/error/timeout handling, content-type inconsistency handling,
RSS/Atom identification, autodiscovery, structural and content validation;
validation statuses and diagnostics; persistence of results; exposure of a
usable-feed result to the collection pipeline; execution only within the
NewsPipe process lifecycle.

Out of scope: publisher/repository or internet-wide source discovery; changes
to the relevance-classification pipeline; Bronze/Silver/Gold redesign; topic
extraction/clustering, embeddings, semantic compression or retention
optimisation; new Kafka/queue infrastructure; independent background services
or schedulers.

## AI/tooling constraints

- This module is deterministic: no OpenCode invocation and no LLM call in
  manifest ingestion, probing, feed identification, validation or result
  handling.
- NewsPipe-wide, any AI-assisted enrichment must use non-`opencode/*` provider
  models (current chain: `nvidia/nvidia/nemotron-3.5-lightning-30b-a3b`,
  fallback `nvidia/poolside/laguna-xs-2.1`) and must submit multiple RSS items
  from one poll in a single OpenCode call, mapping the response back per item.
- Enrichment batching itself is not implemented here; it needs its own feature
  spec. This section records the constraint so the RSS design introduces no
  per-item model calls.

## Implementation status

Proposed. No baseline DTR, CU draft or ITR exists for this feature yet. Design
starts from a fresh `dtr-builder` baseline of the relevant existing structure
(`pipelines/ingestion`, its `conf/` inputs and the `scripts/newspipe` launcher);
record the DTR path and the ITR directory
(`.LLMDD/ITRS/feat-16-scan-and-validate-rss-sources/`) here once produced. Move
to In Progress when CUs are assigned; Implemented only after all waves finish
and E2E verification is green. Record implementing commit(s) here at that point.

## Acceptance criteria

1. A baseline DTR for the relevant existing structure is produced by
   `dtr-builder` and stored under
   `.LLMDD/DTRS/feat-16-scan-and-validate-rss-sources/`; it contains ARCH, FILE,
   CODEX, TYPE and REL sections, and the design records which existing
   components are reused and where the scanning/validation boundary sits.
2. A deterministic YAML or JSON manifest of at least 1000 candidates loads
   successfully; publisher identity, provenance and multiple candidate URLs per
   publisher are preserved; invalid URLs are retained as candidate data; a
   malformed manifest fails with a located diagnostic rather than an unhandled
   exception.
3. Probing handles HTTP errors, redirects, timeouts, connection failures and
   inconsistent content types as data; one failing candidate never aborts the
   batch; an identical endpoint probed more than once in a run produces at most
   one network probe.
4. RSS 2.0, Atom 1.0 and RSS 1.0/RDF documents are identified as feeds,
   ordinary HTML is rejected, and an HTML page carrying RSS/Atom
   autodiscovery metadata yields the advertised endpoint.
5. Validation classifies each candidate as usable, temporarily unavailable,
   invalid or requiring investigation, with a reason code; it checks successful
   parse, valid RSS/Atom structure, at least one usable entry, useful metadata,
   and publication dates/identifiers where the format supplies them. Empty or
   unusable entries are not usable, while a structurally valid but stale feed
   is not classified invalid.
6. Results (status, reason code, timestamp and diagnostic detail) are persisted
   through an existing NewsPipe persistence mechanism with no new database or
   queue; an unchanged re-run reuses a prior usable result and skips redundant
   probing unless re-probing is explicitly requested.
7. The existing collection pipeline consumes only feeds marked usable; the
   relevance-classification implementation is unchanged (no diff under its
   source paths).
8. Validation runs only while NewsPipe is executing (initial manifest
   processing or normal execution); a documented invocation completes and
   exits, leaving no daemon, background scheduler or task running afterwards.
9. Probing, parsing, feed identification, validation and error classification
   contain no LLM/AI calls. If an ambiguity resolver is introduced, it has an
   explicit typed input/output contract, validates outputs, and falls back to a
   deterministic result.
10. A 1000-candidate manifest over local stub endpoints completes with bounded
    concurrency, bounded retries and every candidate accounted for in the
    result set.
11. Compiled CUs and wave artifacts are recorded under
    `.LLMDD/ITRS/feat-16-scan-and-validate-rss-sources/`, and both
    `pipelines/ingestion` and `pipelines/enrichment` compile and pass their
    test suites.

## Tests

Planned paths (Advisor writes executable tests at CU-draft time; code lead runs
them in E2E verify, using local fixtures and stub servers — no live internet):

- `pipelines/ingestion/src/test/scala/newspipe/SourceManifestSpec.scala` —
  `sbt "testOnly newspipe.SourceManifestSpec"`; asserts criterion 2 including a
  1000-candidate fixture, provenance, multi-URL publishers, invalid URLs and a
  located malformed-manifest error.
- `pipelines/ingestion/src/test/scala/newspipe/FeedProbeSpec.scala` —
  `sbt "testOnly newspipe.FeedProbeSpec"`; local stub server covers 3xx, 4xx,
  5xx, timeout, connection refusal, wrong content-type and probe deduplication
  (criterion 3).
- `pipelines/ingestion/src/test/scala/newspipe/FeedDetectorSpec.scala` —
  `sbt "testOnly newspipe.FeedDetectorSpec"`; RSS2/Atom/RDF fixture files, an
  HTML negative fixture and autodiscovery HTML fixture (criterion 4).
- `pipelines/ingestion/src/test/scala/newspipe/FeedValidatorSpec.scala` —
  `sbt "testOnly newspipe.FeedValidatorSpec"`; empty feed, malformed XML,
  stale-but-valid feed, missing date/id and meaningful-entry cases produce the
  specified statuses and reason codes (criterion 5).
- `pipelines/ingestion/src/test/scala/newspipe/ValidationResultsSpec.scala` —
  `sbt "testOnly newspipe.ValidationResultsSpec"`; persistence round-trip and
  rerun-without-reprobe assertions (criterion 6).
- `pipelines/ingestion/src/test/scala/newspipe/SourceValidationIntegrationSpec.scala` —
  `sbt "testOnly newspipe.SourceValidationIntegrationSpec"`; manifest to usable
  feed list to existing ingestion path, with a fake collection adapter
  asserting non-usable feeds never reach it (criteria 7 and 8).
- `pipelines/ingestion/src/test/scala/newspipe/SourceValidationScaleSpec.scala` —
  `sbt "testOnly newspipe.SourceValidationScaleSpec"`; 1000 local stub
  candidates, bounded probe count/retries, all statuses recorded (criterion 10).
- Static checks: `git diff --name-only` shows no relevance-classification
  source changes (criterion 7); scan/validation source contains no OpenCode or
  LLM client import (criterion 9); no new scheduler/daemon entry point
  (criterion 8).
- Full regression: `sbt compile` and `sbt test` in `pipelines/ingestion` and in
  `pipelines/enrichment` (criterion 11). No Python implementation is planned;
  if the launcher is touched, add `uvx mypy --strict scripts/newspipe` with
  zero errors.
