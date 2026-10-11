# bug-3: runmain-provided-spark-classpath

- Status: Open
- Created: 2026-10-11
- Resolved: -
- Bug ID: bug-3-runmain-provided-spark-classpath
- Related features: -

## Bug detail

Symptoms: `sbt "runMain newspipe.control.entryPoint.Main"` dies at boot
with `java.lang.NoClassDefFoundError: org/apache/spark/sql/SparkSession$`
thrown from `AppContainer.<init>`, while plain `sbt run` boots and polls
normally.

Reproduction (from repo root):

```sh
sbt "runMain newspipe.control.entryPoint.Main"
# ACTUAL: NoClassDefFoundError SparkSession$ at AppContainer.scala:15
sbt run
# ACTUAL: boots, polls feeds, publishes to Kafka
```

Environment: repo checkout, sbt 1.10.7, Java 21, spark-sql 3.5.1.

Suspected cause: `spark-sql` is `Provided` scope in `build.sbt`, so it
sits on `Compile/fullClasspath` but not on the runtime classpath. The
existing workaround (`Compile / run := Defaults.runTask(Compile /
fullClasspath, ...)`) only rewires the `run` task; `runMain` resolves a
different classpath and still misses the Provided jars.

## Resolution status

Open. No fix merged.

## Fix summary (Resolved only)

Pending.

## Tests proving resolution (Resolved only)

Pending. Suggested: `sbt "runMain newspipe.control.entryPoint.Main"`
boots past AppContainer construction (same behavior as `sbt run`), plus
`sbt compile` green with no scope regressions for the test suites.
