# feat-1: Scala RSS opencode bronze pipeline

- Status: Implemented
- Created: 2026-10-10
- Feature ID: feat-1-scala-rss-opencode-bronze-pipeline

## Synopsis

Motivation: the current newspipe (`src/`, Python) scrapes websites with
crawl4ai and calls an LLM over HTTP (LiteLLM) to filter chunks. The
Designer wants a refactor with four properties: (1) written in Scala,
(2) pulls geopolitics RSS feeds instead of scraping websites, (3) calls
the `opencode` CLI instead of LLM HTTP calls, (4) summarises each RSS
item and tags it (countries, geo-area, further dashboard tags) and
writes data + metadata (summary, tags) to the data lake's bronze layer
as one unit.

Scope (in): Scala 3 + sbt project layout (hexagonal: domain >
application > infrastructure > control); RSS pull of geopolitics
feeds (replacing crawl4ai `StreamScraper`); per-article `opencode run
--format json` enrichment producing summary + tags (replacing
`LitellmClient.is_relevant` / `embed_text`); bronze unit = one Delta
Lake row per article holding raw fields plus summary/tags; Kafka + Spark
kept between stages (per Designer decision).

Scope (out): dashboard itself (only tags feeding it later); silver/gold
layers; feed curation UI; removal of the legacy Python pipeline (stays
until the Scala pipeline is E2E green).

## Implementation status

Implemented. All 7 CUs (cu-001..cu-007) implemented across 4 waves and E2E
verified green: `sbt clean test` 5/5 and `sbt run` boots (wave-4 report,
`.LLMDD/ITRS/scala-rss-opencode-bronze-pipeline/newspipe.feedback/wave-4-verification.txt`;
convergence CONVERGED). Baseline DTR of the Python codebase (design input):
`.LLMDD/DTRS/newspipe-scala-baseline/newspipe-python.dtr`.

## Acceptance criteria

1. `build.sbt` pins Scala 3 + sbt and the project compiles with `sbt compile`.
2. RSS ingestion pulls all configured geopolitics feeds and yields
   items with guid, feed URL, title, link, publish date, description;
   no crawl4ai/website scraping remains on the Scala path.
3. Enrichment shells out to `opencode run --format json` once per
   article and parses the concatenated `type=text` event payloads into
   a JSON object with summary, countries, geo-area, and extra tags.
4. No direct LLM HTTP calls (no LiteLLM client) exist on the Scala path.
5. Each bronze unit written to the Delta Lake bronze path contains the
   article data AND its enrichment metadata (summary, countries,
   geo-area, tags) as a single row/record.
6. Raw RSS items flow through the kept Kafka topics and the bronze
   write goes through Spark (Kafka + Spark preserved between stages).
7. Hexagonal layering holds: domain defines ports with no
   infrastructure imports; dependency direction is
   domain > application > infrastructure > control.

## Tests

- `sbt compile` passes (criterion 1).
- RSS test: `src/test/scala/newspipe/.../RssClientSpec.scala` parses a
  fixture feed and asserts item fields; plus a live-fetch dry run
  against one real geopolitics feed asserting non-empty items
  (criterion 2).
- Enrichment test: unit test feeds a canned `opencode run --format
  json` event stream (real shape observed 2026-10-10: NDJSON, text in
  `{"type":"text","part":{"text":"..."}}`) and asserts parsed
  summary/countries/geo-area/tags; `grep -ri litellm
  src/main/scala` returns nothing (criteria 3, 4).
- Bronze test: write two enriched units to a temp Delta path, read
  back with Spark/Delta and assert every row has both raw columns
  (title, content/link) and metadata columns (summary, countries,
  geo_area, tags) with row count 2 (criterion 5).
- Pipeline test: end-to-end dry run RSS → Kafka → enrich (stubbed CLI
  or real `opencode run`) → Delta temp path asserting topic flow and
  Spark write (criterion 6).
- Architecture test: `grep -rn "^import"
  src/main/scala/newspipe/domain/` asserts no `infrastructure`,
  `kafka`, `spark`, `delta`, or `rome` imports (criterion 7).
