# feat-1: Repair startup and ingestion pipeline defects

- Status: Proposed
- Created: 2026-10-09
- Feature ID: feat-1-repair-startup-and-ingestion-pipeline

## Synopsis

Motivation: the repo does not run end to end. Code review (read-only,
verified by import and runtime smoke checks, all reverted without code
changes) found defects that each independently break startup or prevent any
record from reaching the lakehouse.

Defects diagnosed:

1. `input_files/` (seed_urls, traversal_policies, relevance_policies) is
   missing and git-ignored, so `control.main:main` raises `ValueError` on
   startup. No tracked samples or `.env.example` exist.
2. `IngestionPipeline` buffers by `chunk_id` (unique per chunk), so
   `len(group) >= buffer_size (5)` can never trigger — zero records are ever
   flushed to the lakehouse. Needs grouping by `source_url` plus a `flush_all`
   path for tail records.
3. Bare `case _: raise` in `ingest_if_relevant` re-raises with no active
   exception (`RuntimeError`) on any record-creation failure.
4. `DiscoveryService.run` quiescent branch never breaks out of `while True`;
   bytes slicing in log calls; missing `IOSuccess(Failure)` match arms.
5. `IngestionService._handle_ingestion` uses `data["source_url"]` (KeyError
   risk), drops decode/pipeline failures without committing (poison-pill
   redelivery loop).
6. `StreamScraper` missing `IOSuccess(Failure)` arms; `CrawlerResult` protocol
   lacks `url`; chunk input not coerced to `str`.
7. `LinguisticService` constructs `BronzeRecord` with non-existent
   `control_action` / `gram_type` fields (silently dropped).
8. `LakehouseConnector._convert_embedding` pattern-matches
   `value in [None, Never]` (comparison against a type object) instead of
   checking `Nothing`.
9. Notebook `silver_layer.py`/`.ipynb` imports non-existent
   `DataPlatformContainer`.
10. `pyproject.toml` depends on `dotenv` shim instead of `python-dotenv`;
    `LitellmClient` `max_tokens: 5` risks truncating `NOT RELEVANT`.

Scope: in — fixes for items 1–10 with proving tests. Out — infra repo
(Kafka/MinIO/Spark/LiteLLM provisioning), silver-layer SQL tuning, new
crawler sources.

## Implementation status

Proposed. Awaiting Designer approval, then Advisor CU design, `compile-itr`,
and `run-waves`. No code changed yet (prior uncommitted fix attempt was fully
reverted; working tree clean).

## Acceptance criteria

1. Fresh checkout plus documented setup steps reaches `main()` config build
   without `ValueError` (samples + `.env.example` + setup docs exist).
2. Five same-source relevant chunks via `IngestionPipeline` (buffer_size 5)
   produce exactly 5 lakehouse writes; a sixth single-source chunk plus
   `flush_all()` produces 1 more write.
3. Record-creation failure surfaces the original error, never bare-raise
   `RuntimeError: No active exception to reraise`.
4. Discovery consumer loop terminates after quiescent threshold with no
   in-flight tasks (no infinite idle loop).
5. Undecodable Kafka records are logged, committed (skipped), and do not block
   subsequent records.
6. All modules import under `PYTHONPATH=src`; `compileall` passes on `src/`.
7. README documents prerequisites, install, configuration, run, layout, and
   troubleshooting.

## Tests

- `PYTHONPATH=src .venv/bin/python -m compileall -q src` → exit 0.
- Pipeline flush test (throwaway script, fakes for `AIPort`/`StoragePort`):
  feed 5 same-source chunks, assert 5 written; feed 1 other-source chunk,
  assert 0 new writes until `flush_all()`, then 1.
- Startup config test: `_get_seed_urls` + `_get_active_config` against the
  shipped samples validate into `TraversalRules` / `RelevancePolicy`.
- Poison-pill test: undecodable record → logged + offset committed, next
  record still processed.
- Quiescence test: discovery loop source shows loop exit after threshold
  (static `break` presence plus idle-simulation run).
- Manual: `cp input_files/*.example.json` + `cp .env.example .env`, then
  `uv run newspipe-ingest` gets past config loading (service backends still
  required for full E2E).
