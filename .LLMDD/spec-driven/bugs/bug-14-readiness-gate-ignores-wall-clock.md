# bug-14: Readiness gate ignores wall-clock time

- Status: Open
- Created: 2026-10-29
- Resolved: -
- Bug ID: bug-14-readiness-gate-ignores-wall-clock

## Bug detail

`ReadinessGate.await` claims to bound startup at `maxWait`, but it never reads
the clock. It starts `now` at the caller's `start`, and after each failed
probe it does:

```scala
sleep(interval)
now = now.plusMillis(interval.toMillis)
```

The increment assumes every probe plus its sleep took exactly `interval`.
When a probe blocks for longer than `interval`, the loop under-counts the
elapsed time and keeps probing far past the stated deadline.

### Reproduction (verified 2026-10-29)

The enrichment readiness gate is configured with `maxWait = 60.seconds` and
`interval = 2.seconds`. Its fourth probe, `OpenCodeModelReadinessProbe`, calls
`Process.waitFor(30.seconds)` and blocks for the full 30 s per check because
the child is stuck on stdin (bug-9). The gate therefore performs about
`60 / 2 = 30` checks, each costing 30 s, for roughly 15 minutes.

Observed on this repository:

    cd pipelines/enrichment
    JAVA_TOOL_OPTIONS=-Dlogback.configurationFile=/home/kcat/Repos/newspipe/conf/logback.xml sbt run

- Expected: a `startup aborted, not ready: ...` line about 60 s after boot.
- Actual: no abort line after more than 5 minutes; the process is still
  probing. `jstack <forked-jvm-pid>` shows the main thread parked in
  `OpenCodeModelReadinessProbe.check` under `ReadinessGate.await`.

The same arithmetic applies to any slow probe (object-store read timeout,
model answer timeout), not just the stdin case.

### Environment

Linux; `/home/kcat/Repos/newspipe`; branch `feature/v2-prototype`;
enrichment `ReadinessGate(..., maxWait = 60.seconds, interval = 2.seconds)`.

### Suspected cause

`ReadinessGate.scala` lines 20-33. The deadline is computed from `start`, but
the current time is simulated by adding `interval` per loop iteration instead
of reading `Instant.now()` or a monotonic clock. Cause confirmed by the thread
dump and by timing the loop; no inference from a framework contract.

## Resolution status

Not resolved. Open: no merged fix, no proving tests. The reproduction above
is verified, so the cause is confirmed rather than suspected.

## Fix summary (Resolved only)

Pending.

## Tests proving resolution (Resolved only)

Pending.
