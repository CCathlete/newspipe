# bugfix-feature-6: enrichment-consumer-poll-contract

- Status: Implemented
- Created: 2026-10-11
- Feature ID: bugfix-feature-6-enrichment-consumer-poll-contract

## Synopsis

Fixes bug-5-enrichment-batch-exceeds-max-poll-interval.

A healthy enrichment batch (20+ sequential `opencode` calls) outlasts the
default 5-minute `max.poll.interval.ms`, so the broker kicks the member
and the terminal commit fails into a retry loop. This feature sizes the
consumer contract to the batch cost: `max.poll.interval.ms=3600000` (one
hour, above the worst case of 20 records at the 120s CLI timeout each)
and `max.poll.records=20` to bound batch cost, keeping manual commits.

Scope (in):
- Two consumer props in the enrichment pipeline
- Props spec proving the contract; full suite green

Scope (out):
- Per-item parallelization; incremental commits; ingestion changes

## Implementation status

Implemented in f118337. Both CUs (cu-001, cu-002) implemented across 2 waves, 0 escalations; `ConsumerPropsSpec` green and full `sbt test` 6/6 green (lead review plus independent verify 2026-10-11). ITR directory: .LLMDD/ITRS/bugfix-feature-6-enrichment-consumer-poll-contract/.

## Acceptance criteria

1. `KafkaRawConsumer.props` carries `max.poll.interval.ms=3600000` and
   `max.poll.records=20` with manual commits retained
2. `ConsumerPropsSpec` passes; full `sbt test` green in
   `pipelines/enrichment` (existing merge/service suites unaffected)

## Tests

- `ConsumerPropsSpec` asserts all four prop values (criterion 1, 2)
- `sbt test` with workdir `pipelines/enrichment` green (criterion 2)
