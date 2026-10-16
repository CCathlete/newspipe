# bug-6: opencode-timeout-not-enforced

- Status: Resolved
- Created: 2026-10-11
- Resolved: 2026-10-16
- Bug ID: bug-6-opencode-timeout-not-enforced
- Related features: -

## Bug detail

Symptoms: an `opencode run` subprocess outlived the designed 120s timeout
by 10x+ while its parent stayed blocked. Observed e2e 2026-10-11: article
"Twelve killed..." spawned `opencode` at 19:42; the app logged "timed out
after 120 s" and skipped it, yet the same PID was still alive and burning
CPU at 20:14 (32 min); a second call ran 12+ min with the parent blocked
and no skip logged. Killing sbt/JVMs also strands these grandchildren as
orphans that keep spending CPU/RAM (two concurrent orphans observed
enriching different articles).

Reproduction: point enrichment at any slow model response and watch the
child PID survive past `timeout-seconds` while the parent blocks.

Suspected cause: `proc.waitFor(secs, ...)` plus
`proc.destroyForcibly()` in `OpenCodeCliClient.enrich` does not end the
`opencode` wrapper (or its runtime children) on this box; the blocked
parent and the orphaned burn suggest the timeout path neither frees the
thread promptly nor reaps the process tree.

Environment: repo checkout,
`pipelines/enrichment/src/main/scala/newspipe/infrastructure/llm-data-enricher/OpenCodeCliClient.scala`
(`enrich`), opencode CLI with model
`opencode/muse-spark-1.3-contributor-free`.

## Resolution status

Resolved in 2ac9fe5. Both CUs implemented across 2 waves, 0 escalations,
convergence.json CONVERGED; independent verify 2026-10-16: `sbt
"testOnly newspipe.OpenCodeTimeoutSpec"` 2/2 green, full `sbt test` 8/8
green (4 suites) with workdir `pipelines/enrichment`.

## Fix summary (Resolved only)

`OpenCodeCliClient.enrich` now checks `waitFor` before touching stdout
and calls the new `OpenCodeCliClient.killTree` helper on timeout, which
destroys all descendants plus the wrapper, closes the pipes, and reaps
with a bounded 5 s wait; the timeout throw carries elapsed seconds plus
item link, surfaced by the existing `EnrichmentService` warn line.
Prompt, parse, merge, service, and consumer logic unchanged (14
insertions, 5 deletions, all inside `enrich` plus the helper).

## Tests proving resolution (Resolved only)

- `OpenCodeTimeoutSpec` (`pipelines/enrichment/src/test/scala/newspipe/OpenCodeTimeoutSpec.scala`):
  slow-stub tree (`sh -c 'sleep 120'`) leaves no surviving descendant
  and is reaped in bounded time — failing-before: orphaned `opencode`
  grandchildren observed alive 32 min past timeout (see Bug detail);
  passing-after: both tests green 2026-10-16.
- Full `sbt test` with workdir `pipelines/enrichment`: 8/8 green,
  including pre-existing parser, merge, and props suites.
