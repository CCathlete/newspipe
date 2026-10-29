# bug-9: opencode run blocks until timeout when stdin is an open pipe

- Status: Open
- Created: 2026-10-20
- Resolved: -
- Bug ID: bug-9-opencode-hangs-on-piped-stdin

## Bug detail

Every `opencode run` child launched from the enrichment JVM hangs whenever
its stdin is an open non-TTY pipe that never reaches EOF, and returns only
when its timeout expires. Two symptoms follow:

1. The startup readiness gate false-refuses a healthy model. The model probe
   waits 30 s, receives nothing, reports `model '<name>' did not answer
   within 30 s`, and the pipeline aborts before polling. The model answers
   fine — the process is blocked on stdin, not on the model.
2. If the gate is passed some other way, every enrichment worker call burns
   its full 120 s budget, so items fail and the offset commits past them.

From an interactive terminal — stdin a TTY — the identical code works: the
probe answers in about 2 s. That asymmetry is why the defect survived
testing: anything driven from a shell with a terminal behaves.

### Reproduction (verified 2026-10-20 on this repository, before any fix)

A. Piped stdin, no EOF — hangs (the failure):

    timeout 12 sh -c 'sleep 20 | opencode run --format json \
      --model opencode/minimax-m3.1-flash-free "reply with exactly: OK"'

    expected: JSON answer within about 2 s
    actual:   exit 124 (killed at the 12 s timeout), empty stdout

B. Same command with stdin from /dev/null — works:

    opencode run --format json --model opencode/minimax-m3.1-flash-free \
      "reply with exactly: OK" </dev/null

    expected: exit 0, no "type":"error"
    actual:   exit 0 in 1.5 s, no error

C. TTY stdin — works (what a human at a terminal gets):

    script -qec "opencode run --format json --model opencode/minimax-m3.1-flash-free 'reply with exactly: OK'" /dev/null

    expected: exit 0 in about 2 s
    actual:   exit 0 in 2 s, JSON step output

Only the stdin shape varies across A to C; model and prompt are identical.

### Environment

Linux; `/home/kcat/Repos/newspipe`; opencode CLI current version; conf
model `opencode/minimax-m3.1-flash-free`, a model that answers. This bug is
not model-specific: any served model reproduces it.

### Suspected cause

Inheritance chain, each link verified by inspection:

1. `opencode run` reads stdin when it is an open non-TTY pipe without EOF
   (proven by A against B above).
2. `pipelines/enrichment/build.sbt:39-40` sets `run / fork := true` and
   `Test / fork := true` with no `connectInput := false`; the sbt default
   `connectInput := true` wires the forked JVM stdin to sbt stdin.
3. `scripts/newspipe` spawns sbt with `subprocess.Popen(..., stdout=PIPE,
   start_new_session=True)` and no `stdin=` argument, so the launcher
   inherits its parent stdin.
4. The JVM `ProcessBuilder` children — the readiness probe and every
   enrichment worker call — inherit that stdin unchanged; nothing in the
   pipeline redirects it.

So non-interactive stdin (agent shell, CI, cron, any pipe without EOF) flows
launcher to sbt to JVM to opencode, and every call blocks. The inheritance
path predates the readiness probe: `OpenCodeCliClient` has always built its
`ProcessBuilder` without redirecting stdin; the probe only made the hang
visible at startup as an abort.

The live verification that discovered this ran the pipeline behind a
temporary PATH shim wrapping opencode with stdin redirected from /dev/null.
The shim lived outside the repository and was deleted afterwards, so the
repository as committed carries no mitigation, and the discovery runs
numbers do not carry over to non-interactive shells.

Natural mitigations, any one sufficient: `connectInput := false` in
`pipelines/enrichment/build.sbt`, `stdin=subprocess.DEVNULL` in the
launcher Popen call, or redirecting stdin inside the `ProcessBuilder`
construction.

## Correction 2026-10-29 — a TTY does not save the sbt launch path

The original report said the identical code works from an interactive
terminal and that only non-TTY stdin fails. That is true for a **direct**
`opencode run` invocation, but it is false for the way the pipelines are
launched. Through `sbt run`, the forked JVM's stdin is a pipe regardless of
the parent's stdin shape, and `opencode` inherits that pipe.

Verified on this repository on 2026-10-29:

1. `script -qec 'bash -c "cd pipelines/enrichment && sbt run"' /dev/null`
   launches the pipeline under a pty. Inspecting the live probe child anyway
   gives `/proc/<opencode>/fd/0 -> pipe:[100865]`, not `/dev/pts/N`.
2. With stdin from `/dev/null` (`sbt run </dev/null`), the child still gets an
   open pipe, and the readiness gate remains parked in
   `OpenCodeModelReadinessProbe.check` at `Process.waitFor`.
3. `jstack` on the forked JVM shows the main thread under
   `ReadinessGate.await` -> `OpenCodeModelReadinessProbe.check`.

So the user-visible failure is not limited to agents, CI or cron: an
interactive `newspipe run both` hangs the same way. The original reproduction
A (`sleep 20 | opencode run ...`) remains valid evidence of the mechanism;
the TTY asymmetry in reproduction C only applies before sbt forks the JVM.

Consequence for any fix: validating it from an interactive terminal is not
sufficient. The regression test must attach the child's stdin to an open,
never-closing pipe and assert the call returns.

## Resolution status

Not resolved. Open: no merged fix, no proving tests. The original
reproduction and the 2026-10-29 pty correction above are both verified, so the
cause is confirmed rather than suspected.

## Fix summary (Resolved only)

Pending.

## Tests proving resolution (Resolved only)

Pending.
