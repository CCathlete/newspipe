# bug-4: dedup-before-sink-write-loses-data

- Status: Resolved
- Created: 2026-10-11
- Resolved: 2026-10-19
- Bug ID: bug-4-dedup-before-sink-write-loses-data
- Related features: bugfix-feature-5-split-standalone-pipelines

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

Resolved 2026-10-19 by bugfix-feature-5-split-standalone-pipelines
(commit 6185bf5).

## Fix summary (Resolved only)

`IngestionService.runOnce` now marks IDs only after the write succeeds:
it computes `fresh`, calls `sink.write(units)`, and only then runs
`units.foreach(u => seen.add(u.id))`. A write that throws leaves `seen`
untouched, so the next poll refetches and retries the same articles
instead of filtering them out; a successful write still dedups them.
The suggested `AppContainer` session-recovery work was NOT done -- a Spark
failure at boot still kills `Main` -- so the retry half of this bug is
fixed and the eager-session half is deliberately left open.

## Tests proving resolution (Resolved only)

Re-run live 2026-10-19 in `pipelines/ingestion`, 37/37 green:
- `IngestionServiceSpec.scala:34` -- "retries the batch after a sink
  failure instead of losing it": the second `runOnce` returns the item
  count again after a failed write
- `IngestionServiceSpec.scala:15` -- "publishes raw and writes raw-only
  bronze rows, dedups second run": the dedup half still holds

NOT proven: the recovering-sink / eager-`AppContainer` case, which remains
out of scope per the fix summary above.
