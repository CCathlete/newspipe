# bug-5: enrichment-batch-exceeds-max-poll-interval

- Status: Open
- Created: 2026-10-11
- Resolved: -
- Bug ID: bug-5-enrichment-batch-exceeds-max-poll-interval
- Related features: -

## Bug detail

Symptoms: the enrichment pipeline never converges on a real batch. The
consumer joins the group, fetches dozens of items, then the batch takes
longer than the default `max.poll.interval.ms` (5 min) because enrichment
is one sequential `opencode run` per article (~1-2 min each here, 32+
articles per poll). The broker kicks the member mid-batch; the terminal
`commitSync` then fails, `runOnce` throws, and the next poll re-fetches
and re-enriches the same records. Observed e2e 2026-10-11: join at
18:03:31, group-leave lines at exactly 18:08:31 (+5 min), zero merges
after 7+ min with the process still grinding through CLI calls.

Reproduction: point the enrichment pipeline at a raw topic holding 30+
records and run one poll iteration; the member is kicked before
`commit()`.

Suspected cause: `KafkaRawConsumer.props` sets `enable.auto.commit=false`
but never raises `max.poll.interval.ms`, while `EnrichmentService.runOnce`
does fetch-enrich-merge-commit as one synchronous batch with no interim
polls. Throughput (sequential CLI calls) is by design for now; the defect
is the commit contract not surviving a slow-but-healthy batch.

Environment: repo checkout,
`pipelines/enrichment/src/main/scala/newspipe/infrastructure/kafka/KafkaRawConsumer.scala`,
`pipelines/enrichment/src/main/scala/newspipe/application/services/EnrichmentService.scala`.

## Resolution status

Open. No fix merged.

## Fix summary (Resolved only)

Pending. Suggested direction: set `max.poll.interval.ms` to cover a full
batch (e.g. 3600000) and/or `max.poll.records` to bound batch cost, so a
slow batch commits instead of looping; longer term, parallelize per-item
enrichment or commit incrementally with interim polls.

## Tests proving resolution (Resolved only)

Pending. Suggested: stubbed slow-enrich batch test asserting commit is
reached, plus e2e run showing `poll merged N` with N > 0 on a 30+ record
topic.
