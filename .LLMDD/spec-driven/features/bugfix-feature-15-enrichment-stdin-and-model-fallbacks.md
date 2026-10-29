# bugfix-feature-15: Enrichment stdin isolation and model fallbacks

- Status: Proposed
- Created: 2026-10-29
- Feature ID: bugfix-feature-15-enrichment-stdin-and-model-fallbacks

## Synopsis

Fixes bug-9-opencode-hangs-on-piped-stdin and
bug-14-readiness-gate-ignores-wall-clock.

**Motivation.** `newspipe run both` looks healthy on ingestion and silent on
enrichment. The enrichment JVM is not dead and its logic is not failing; the
startup readiness gate is parked in `OpenCodeModelReadinessProbe.check` at
`Process.waitFor`. Two defects combine to hide that:

1. `sbt run` gives the forked JVM a pipe for stdin (`run / fork := true` with
   `connectInput` enabled), and both `OpenCodeModelReadinessProbe` and
   `OpenCodeCliClient` spawn `opencode run` without redirecting stdin.
   `opencode run` blocks on that open, non-EOF pipe until its own timeout.
   Verified on 2026-10-29: `/proc/<opencode>/fd/0 -> pipe:[...]` even when sbt
   was launched under a pty, and `timeout 40 sh -c 'sleep 60 | opencode run
   ...'` exits 124. A direct `opencode run </dev/null` returns immediately.
2. `ReadinessGate.await` advances a simulated `now` by `interval` after each
   sleep instead of reading the clock, so a probe that blocks for 30 s is
   counted as 2 s. `maxWait = 60.seconds` therefore takes roughly 15 minutes
   to expire, and the expected `startup aborted` line never appears while the
   operator watches.

A third, independent blocker is configuration: the tracked default and the
local `.env` both name `opencode/minimax-m3.1-flash-free`, which opencode no
longer serves (`provider.no-route`). Even with stdin isolated, the gate would
abort. Enrichment must use the same free, non-opencode provider models the
wave runner uses, with fallbacks, instead of an `opencode/*-free` model that
can disappear without notice.

**Scope (in).**
- Redirect child stdin to `/dev/null` in the model readiness probe and the
  enrichment client; add `stdin=subprocess.DEVNULL` to the launcher and
  `connectInput := false` to the enrichment build.
- Make `ReadinessGate` enforce `maxWait` against real elapsed time.
- Switch the enrichment model to the wave-tool provider: primary
  `nvidia/nvidia/nemotron-3.5-lightning-30b-a3b` (measured answering in ~1 s)
  with fallback `nvidia/poolside/laguna-xs-2.1` (measured ~18 s). Both are
  `nvidia/*`, no `opencode/*` entry remains.
- Add a configurable fallback chain resolved through the existing
  environment > `.env` > `conf/enrichment.conf` precedence; rotate models
  within `opencode.max-attempts`; readiness passes when at least one model in
  the chain answers.
- Regression tests for stdin isolation, wall-clock gating, fallback rotation
  and configuration precedence; live end-to-end verification.

**Scope (out).**
- Ingestion, Kafka partitions, `max.poll.records` / `max.poll.interval.ms`,
  the commit-past-failure policy, `~/Repos/pipeline_infra`, and the wave
  runner's own model defaults.
- Any `opencode/*` model in the enrichment chain.
- Writing or committing `.env`. It is the operator's local file; the E2E
  check backs it up and restores it. After this feature lands the Designer
  must remove or update the stale `NEWSPIPE_OPENCODE_MODEL` override in
  `.env`, because it still pins the dead model on this host.

## Implementation status

Proposed. Not implemented.

## Acceptance criteria

1. `OpenCodeModelReadinessProbe` and `OpenCodeCliClient` launch every child
   with stdin connected to `/dev/null`, independent of the parent's stdin
   shape. A regression test using a fake `opencode` that reads stdin proves a
   call returns instead of blocking when the JVM's own stdin is an open pipe.
2. `scripts/newspipe` starts sbt with `stdin=subprocess.DEVNULL`, and
   `pipelines/enrichment/build.sbt` sets `run / connectInput := false`
   alongside `run / fork := true`.
3. `ReadinessGate.await` uses real elapsed time: with a probe whose `check()`
   advances the clock by 30 s and `maxWait = 60 s`, the gate stops after two
   checks, not after the thirty simulated ticks the current code performs.
4. `conf/enrichment.conf` configures `opencode.model =
   "nvidia/nvidia/nemotron-3.5-lightning-30b-a3b"` and
   `opencode.fallback-models = ["nvidia/poolside/laguna-xs-2.1"]`; no
   `opencode/` model remains anywhere in that file.
5. `EnvConfig.opencodeFallbackModels` resolves as a list through sys.env >
   `.env` > `conf/enrichment.conf`; `NEWSPIPE_OPENCODE_FALLBACK_MODELS` is a
   comma-separated override, and blank entries are dropped.
6. `EnrichmentService` rotates through the configured chain within
   `opencode.max-attempts`: a primary failure followed by a fallback success
   enriches the item, reports it in `PollReport.enriched`, and records the
   call order primary-then-fallback.
7. Startup readiness succeeds when at least one configured model answers and
   fails naming every configured model when none do.
8. End to end on this host: with Kafka and RustFS reachable,
   `newspipe run enrichment` emits a `poll merged N enriched units (N
   enriched, 0 failed)` line with `N > 0`, `newspipe status` shows a committed
   offset greater than zero for `enrichment-group`, and the log carries no
   `opencode run timed out` line.
9. No regression: `sbt compile` is clean, the full `sbt test` suite is green
   in `pipelines/enrichment`, ingestion is untouched, and
   `newspipe status` / `newspipe down` behave as before.

## Tests

Written by the Advisor, run by the code lead.

- Stdin isolation (criterion 1):
  `pipelines/enrichment/src/test/scala/newspipe/StdinIsolationSpec.scala`
  writes a fake `opencode` script that runs `cat >/dev/null` before printing
  an answer. Calling `OpenCodeCliClient.enrich` and
  `OpenCodeModelReadinessProbe.check` with a 5 s timeout must return
  `Right`/`Ready`. With the stdin redirect removed the fake script blocks and
  the calls time out, which is the failing-before evidence.
- Gate wall clock (criterion 3):
  `pipelines/enrichment/src/test/scala/newspipe/ReadinessGateSpec.scala`
  adds a fake-clock probe test; expected `checks == 2`, `slept == 1`.
- Configuration (criteria 4, 5):
  `pipelines/enrichment/src/test/scala/newspipe/EnvPrecedenceSpec.scala`
  asserts the conf default list, the comma-separated override, and blank
  trimming; `grep -c 'opencode/' conf/enrichment.conf` prints `0`.
- Fallback rotation (criterion 6):
  `pipelines/enrichment/src/test/scala/newspipe/ModelFallbackSpec.scala`
  uses a fake `EnrichmentPort` and asserts `List("primary", "fallback")`
  call order and `report.enriched == 1`.
- Readiness any-live (criterion 7): `StdinIsolationSpec` drives the probe with
  two fake models, one failing and one answering, and expects `Ready`; the
  all-failing case expects `NotReady` naming both models.
- Type probe (criterion 2, launcher is Python):
  `uvx mypy --strict scripts/newspipe` exits 0 with zero errors.
- Unit and compile (criterion 9): `cd pipelines/enrichment && sbt compile`
  and `cd pipelines/enrichment && sbt test` both exit 0, all suites green.
- End to end (criterion 8): with `./scripts/newspipe status` reporting
  `kafka`, `rustfs` and `opencode` present, back up `.env`, set
  `NEWSPIPE_OPENCODE_MODEL=nvidia/nvidia/nemotron-3.5-lightning-30b-a3b`,
  `NEWSPIPE_OPENCODE_FALLBACK_MODELS=nvidia/poolside/laguna-xs-2.1` and
  `NEWSPIPE_POLL_INTERVAL_SECONDS=60`, then run
  `./scripts/newspipe run enrichment` for one poll. Assert the poll line has
  `0 failed`, `./scripts/newspipe status` reports a non-zero committed offset,
  no `timed out` line appears, then run `./scripts/newspipe down` and restore
  the `.env` backup.

## Do NOT touch

`pipelines/ingestion/**`, `conf/ingestion.conf`, `conf/feeds.conf`,
`conf/logback.xml`, `~/Repos/pipeline_infra/**`, `.LLMDD/**` apart from this
spec and this feature's own batch/frame artifacts, and `.env` as a tracked
file (it is gitignored; the E2E check backs it up and restores it).
