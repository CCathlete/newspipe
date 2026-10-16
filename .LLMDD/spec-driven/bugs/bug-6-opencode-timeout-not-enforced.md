# bug-6: opencode-timeout-not-enforced

- Status: Open
- Created: 2026-10-11
- Resolved: -
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

Open. No fix merged.

## Fix summary (Resolved only)

Pending. Suggested direction: destroy the whole process tree
(`ProcessHandle.descendants` plus wrapper) on timeout, bound total
per-item elapsed time independent of `waitFor`, and log per-item elapsed
so slow calls are visible before they strand.

## Tests proving resolution (Resolved only)

Pending. Suggested: stubbed-process test proving no child survives N
seconds past the timeout and the parent thread is freed; e2e log shows a
skip line for every over-timeout call with no orphan left behind.
