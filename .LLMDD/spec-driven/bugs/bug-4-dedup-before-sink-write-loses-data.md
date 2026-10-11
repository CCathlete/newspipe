# bug-4: dedup-before-sink-write-loses-data

- Status: Open
- Created: 2026-10-11
- Resolved: -
- Bug ID: bug-4-dedup-before-sink-write-loses-data
- Related features: -

## Bug detail

Symptoms: if the bronze sink write fails (Spark session broken, Delta
unavailable), the affected articles are silently skipped on every later
poll — never retried, never written, with only a warn/error log as trace.
The raw Kafka topic holds items that never reach bronze.

Reproduction (unit level, no infra needed):

```scala
val svc = new IngestionService(rss, enrich, failingSink, raw)
svc.runOnce(now) // throws from sink.write AFTER enrichments ran
svc.runOnce(now) // returns 0: items already in `seen`, never retried
```

Root cause: `IngestionService.runOnce` marks IDs seen
(`items.filter(it => seen.add(BronzeUnit.idFor(it)))`) *before*
`enrich`/`sink.write`. A sink failure therefore discards already-paid
enrichments and poisons `seen`, so the next poll filters the same items
out. Related: `AppContainer` builds the Spark session as an eager `val`,
so a Spark failure at boot kills `Main` before fetch/enrich ever run;
enrichment only survives Spark failures that occur after boot, and even
then its results are lost with the failed batch.

Environment: repo checkout, `src/main/scala/newspipe/application/services/IngestionService.scala`
(`runOnce`), `src/main/scala/newspipe/control/dependencyInjection/AppContainer.scala`.

## Resolution status

Open. No fix merged.

## Fix summary (Resolved only)

Pending. Suggested direction: move the `seen.add` to after a successful
`sink.write` (mark only written IDs), so a failed batch is retried on the
next poll; consider lazy/session-recovering Spark construction in
`AppContainer` so a transient Spark failure does not kill `Main`.

## Tests proving resolution (Resolved only)

Pending. Suggested: failing-sink test asserting the second `runOnce`
returns the item count again (retry), then a recovering sink asserting the
third `runOnce` writes and a fourth returns 0 (dedup intact); existing
`PipelineSpec` dedup test keeps passing.
