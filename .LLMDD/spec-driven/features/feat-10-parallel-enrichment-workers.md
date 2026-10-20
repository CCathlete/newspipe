# feat-10: Parallel enrichment workers with retry

- Status: Implemented
- Created: 2026-10-20
- Feature ID: feat-10-parallel-enrichment-workers

## Synopsis

**Motivation.** Enrichment works through its batch one item at a time —
`EnrichmentService.runOnce` is a serial `flatMap` over the polled items, each
one launching a blocking `opencode run` with a 120 s budget. At that rate the
consumer cannot keep up: on 2026-10-20 it was 5746 messages behind. The same
day a second and worse failure surfaced. `conf/enrichment.conf` pinned
`opencode/muse-spark-1.3-contributor-free`, a model opencode no longer serves,
so every call returned `provider.no-route` at once. Because `runOnce` commits
the offset whether or not enrichment succeeded, running the pipeline in that
state would have walked the whole backlog, failed on every item, and committed
past them without a trace. A broken model is not slow enrichment, it is silent
data loss.

**Scope.** In: an externally configurable worker count and attempt count,
bounded parallel workers inside a poll batch, retry-then-skip per item, a
corrected model default, a startup check that the configured model actually
answers, and a poll line that reports counts. Out: Kafka partitioning and
consumer scale-out — `raw_rss` has one partition, so a second instance would
sit idle — the commit-on-failure policy (unchanged: an item that exhausts its
attempts is skipped and committed past), the ingestion pipeline, and any
retry across polls.

**Already true, worth stating.** The model and timeout were never hardcoded.
`Env.scala` reads `.env` first, then the shell, then `conf/enrichment.conf`;
`.env` simply did not exist, so the broken conf default was in force. This
feature corrects the default rather than adding the mechanism. A local `.env`
pinning a working model was written during scoping and is gitignored, so the
pipeline is usable again whether or not this feature has landed.

## Implementation status

Implemented. The batch compiled into cu-001..cu-003, ran as three serial
waves, and landed as 70a9b68 (9 files). The code lead finished green:
42/42 tests, `sbt compile` clean, `newspipe status` and `newspipe down`
unchanged. cu-002 escalated once exactly as the frame prescribed:
`MergeBackfillSpec` did not compile against the new constructor, the Coder
reported it instead of editing it, and the code lead repaired the test
contract (`new EnrichmentService(consumer, port, sink, 1, 1)` asserting
`.merged`). All three CUs closed COMPLETED / PASSED with no open escalation.
The runner then crashed in its own summary step (`run-waves.py:1784`,
iterating the values of a `{path: count}` map as if they were paths), which
is why `newspipe.convergence.json` still reads RUNNING; that is an upstream
tool bug, not a feature defect.

The Advisor re-ran every closure probe that does not need `sbt`: criteria 1,
2, 6 and 7 green, `EnrichmentService.scala` byte-identical to its CU fence,
`validate-steps` PASS x3. Criteria 3, 4, 5 and the test suite rest on the
wave evidence — the Advisor does not run `sbt`.

Known gap, tracked separately: bug-9 (opencode calls hang when stdin is an
open non-TTY pipe). From an interactive terminal the pipeline works; under
non-interactive stdin every opencode call blocks until its timeout and the
readiness gate would refuse a healthy model. The live verification ran
behind a temporary out-of-repo stdin shim, deleted after the run. feat-10
does not fix bug-9.

## Acceptance criteria

1. The model named in `conf/enrichment.conf` is one opencode serves:
   `opencode run --format json --model opencode/minimax-m3.1-flash-free "reply
   with exactly: OK"` exits 0 and prints no `"type":"error"`.
2. Worker count and attempt count come from configuration, not code.
   `conf/enrichment.conf` carries `opencode.workers` (default 4) and
   `opencode.max-attempts` (default 3), and `NEWSPIPE_OPENCODE_WORKERS` and
   `NEWSPIPE_OPENCODE_MAX_ATTEMPTS` override them from `.env` or the shell,
   under the same precedence `NEWSPIPE_OPENCODE_MODEL` already uses.
3. Workers overlap. While a batch of at least four items is being enriched,
   `pgrep -fc "opencode run"` reports 4 or more.
4. Attempts are bounded. With the model pointed at one opencode does not
   serve, each item is attempted exactly `opencode.max-attempts` times and
   then skipped, and the skip is logged once, naming the item and the number
   of attempts.
5. An item that exhausts its attempts does not block the queue: the poll
   returns, the offset commits, and the next poll proceeds normally.
6. The poll line reports counts — merged, enriched and failed — so
   throughput is observable without reading logs per item.
7. Startup refuses a model that does not answer. With the model pointed at an
   unserved one, the readiness gate reports not-ready naming the model and the
   pipeline exits non-zero before polling, instead of consuming and dropping
   the backlog. A new probe joins the existing `opencode` binary probe; the
   binary probe is not repurposed.
8. No regression: `sbt compile` is clean, the existing tests pass, the Delta
   merge still writes, and `newspipe status` and `newspipe down` behave as
   before.

## Tests

Written by the Advisor, run by the code lead.

- Compile (criterion 8): `cd pipelines/enrichment && sbt compile` — no errors.
- Unit (criterion 8): `cd pipelines/enrichment && sbt test` — all green.
- Model validity (criterion 1): the `opencode run` command above — exit 0,
  no `"type":"error"` in stdout.
- Concurrency (criterion 3): with enrichment running against a backlog,
  sample `pgrep -fc "opencode run"` several times during a batch — expect 4
  or more.
- Retry then skip (criterion 4): set
  `NEWSPIPE_OPENCODE_MODEL=opencode/muse-spark-1.3-contributor-free` in
  `.env`, start enrichment, expect three attempts per item and one skip line
  per item naming the count; restore `.env` afterwards. The dead model makes a
  convenient controlled failure.
- Readiness (criterion 7): the same bogus model — expect `startup aborted`
  naming the model and a non-zero exit before any poll line.
- Throughput (criterion 6): `newspipe run enrichment` on the working model —
  the poll line carries merged, enriched and failed, and the backlog shrinks
  faster than it did serially.
- Teardown: `newspipe down`.

The Advisor does not run `sbt`; the compile and unit probes are the code
lead's to run, as in feat-9.

## Do NOT touch

`pipelines/ingestion/**`, `conf/ingestion.conf`, `conf/feeds.conf`,
`conf/logback.xml`, `scripts/newspipe`, `~/Repos/pipeline_infra/**`, and
`.LLMDD/**` apart from this spec. `.env` is the operator's local file — it is
read, never written, by any CU.
