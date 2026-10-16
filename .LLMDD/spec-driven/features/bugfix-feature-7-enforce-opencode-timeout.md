# bugfix-feature-7: enforce-opencode-timeout

- Status: Implemented
- Created: 2026-10-16
- Feature ID: bugfix-feature-7-enforce-opencode-timeout

## Synopsis

Fixes bug-6-opencode-timeout-not-enforced.

`OpenCodeCliClient.enrich` launches `opencode run` per article with a
`timeout: FiniteDuration`, but the timeout is not enforced: `waitFor`
expiry only calls `destroyForcibly()` on the wrapper process (leaving
the `opencode` runtime children alive as CPU-burning orphans), and
`readAllBytes()` on stdout runs before the timeout check, so the
parent thread stays blocked past the timeout instead of skipping the
item promptly.

Scope (in):
- Timeout path in `OpenCodeCliClient.enrich`: kill the whole process
  tree on expiry, stop blocking on stdout past the timeout, bound
  per-item elapsed time, log per-item elapsed and every skip
- Timeout spec proving no child survives and the caller is freed;
  full suite green

Scope (out):
- Per-item parallelization; consumer poll contract (bugfix-feature-6);
  ingestion changes

## Implementation status

Implemented in 2ac9fe5. Both CUs (cu-001, cu-002) implemented across 2
waves, 0 escalations, convergence.json CONVERGED; independent verify
2026-10-16: `OpenCodeTimeoutSpec` 2/2 green, full `sbt test` 8/8 green
(4 suites) with workdir `pipelines/enrichment`. ITR directory:
.LLMDD/ITRS/bugfix-feature-7-enforce-opencode-timeout/.

## Acceptance criteria

1. On `opencode run` exceeding `timeout`, the full process tree
   (wrapper plus all descendants) is destroyed — no child survives N
   seconds past the timeout
2. Total per-item elapsed time is bounded independent of `waitFor`;
   the caller is freed promptly and the item is skipped, never
   blocking indefinitely on stdout
3. Every over-timeout call logs a skip line with its elapsed time;
   slow calls are visible before they strand
4. Existing enrich behavior (prompt, parse, error paths) unchanged;
   full `sbt test` green in `pipelines/enrichment`

## Tests

- `OpenCodeTimeoutSpec` (`pipelines/enrichment/src/test/scala/newspipe/OpenCodeTimeoutSpec.scala`):
  slow-stub process exceeding the timeout leaves no surviving
  descendant past N seconds and the call returns within the bound
  (criteria 1, 2); skip/elapsed logging asserted (criterion 3)
- `sbt test` with workdir `pipelines/enrichment` green, including
  existing `OpenCodeParserSpec` (criterion 4)
