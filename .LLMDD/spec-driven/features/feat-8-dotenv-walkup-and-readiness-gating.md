# feat-8: DotEnv walk-up and readiness gating

- Status: Proposed
- Created: 2026-10-17
- Feature ID: feat-8-dotenv-walkup-and-readiness-gating

## Synopsis

Motivation: newspipe carries a standing `HARD_FAIL` on `ARCH.DOTENV`. That
tensor rule is mandatory and has four clauses — walk-up discovery, system-env
precedence, `${VAR}` expansion with a depth limit of 10, and quoting rules
where single quotes are literal. `Env.scala` in both pipelines implements
none of them: it reads `sys.env`, falls back to Typesafe Config, and does no
`.env` discovery, no expansion, and no documented precedence beyond that. Every
deployment-specific value therefore has to be exported by hand in the shell,
which is why a one-command "run the infra and the pipeline" story has nothing
to read configuration from.

Readiness is the second half. Both `Main` loops enter an unconditional
`while true` and swallow every poll failure with
`catch case e: Exception => logger.error("poll iteration failed", e)`, so
starting before Kafka and the object store are up produces an unbounded error
log rather than a fast, actionable failure.

Scope (in):
- A `ConfLoader` that locates the external `conf/` directory by walking up from
  the working directory until it finds it, with no environment override, and
  merges `feeds.conf` under the per-pipeline file. The classpath
  `application.conf` in each tree is deleted, so user configuration is no
  longer part of the compiled artifact
- `DotEnv` loader implementing all four `ARCH.DOTENV` clauses, in
  `infrastructure/env/` (the mandated Env adapter folder) in both pipelines
- `Env.load()` precedence `sys.env` > `.env` > `application.conf`
- Readiness ports in `application/ports/` and adapters in
  `infrastructure/<port-folder>/`, one folder per port per `LAYER_LAYOUT`:
  Kafka (broker reachable, raw topic present), object store (endpoint
  reachable, bucket present), and `opencode` on PATH for enrichment only.
  The object store is RustFS at `http://localhost:9000`, per
  `pipeline_infra` feat-1; the bucket is `lakehouse`
- `ReadinessGate` service returning typed `NotReady(reasons)` / `Ready`,
  called by both `Main`s before the first poll under a configurable timeout
- Spark S3 wiring: `hadoop-aws` dependency and `fs.s3a.*` settings applied in
  each `AppContainer` SparkSession builder, sourced from the dotenv values.
  The concrete keys the `.env` is expected to supply:

  ```
  NEWSPIPE_S3_ENDPOINT=http://localhost:9000
  NEWSPIPE_S3_PATH_STYLE=true
  NEWSPIPE_S3_ACCESS_KEY=minioadmin
  NEWSPIPE_S3_SECRET_KEY=minioadminpassword
  NEWSPIPE_BRONZE_BUCKET=lakehouse
  ```

  Path-style addressing is required: S3-compatible stores on a single host
  answer virtual-host-style bucket names only via DNS, which `localhost`
  cannot serve. Credentials match `RUSTFS_ACCESS_KEY` / `RUSTFS_SECRET_KEY`
  in `pipeline_infra/variables.tf`. Spark must be given these through
  `spark.hadoop.fs.s3a.*` before the first Delta read or write; left
  unconfigured, `s3a://` resolves to Spark's built-in default of
  `s3a.amazonaws.com`

Scope (out):
- The single launcher executable and its infra up/down lifecycle (separate
  feature)
- Any process-environment override for the `conf/` location. The process
  environment remains the highest-precedence tier for individual *values*, as
  `ARCH.DOTENV.MERGE` requires, but where the directory lives is walk-up only
- Feed configuration — `feeds = []` today, which means nothing has ever been
  ingested
- Any change to `~/Repos/pipeline_infra`; the S3 gateway decision lives there
- Silver/gold, Trino, Hive Metastore

## Post-implementation findings

Recorded 2026-10-19 after waves 1-7 ran. The feature is NOT closable.
These are findings from live runs, not from reading the code.

### Corrected: DotEnv discovery inversion (waves 1-6, fixed in wave 7)

`DotEnv.discover` walked to the filesystem root accumulating the LAST
`.env` it met, so it resolved the furthest file rather than the nearest,
violating ARCH.DOTENV.DISCOVERY. The cause was a self-contradictory test
in the cu-001 frame: it wrote a nearer `.env` in the starting directory
and a farther one above it, then asserted the FARTHER value. Satisfying
that assertion inverted the implementation and broke the correct test
alongside it. Fixed by cu-013/cu-014; suites re-run at 31/31 and 35/35.

### Resolved 2026-10-19 (wave 8): the object-store probe authenticated wrongly

cu-009's `HttpObjectStoreReadinessProbe` sets
`Authorization: Bearer <accessKey>:<secretKey>`. There is no Bearer
scheme for S3; S3 uses SigV4. Measured against RustFS on this host:

```
signed head-bucket, existing bucket     -> success
signed head-bucket, nonexistent bucket  -> error
unsigned HEAD /lakehouse               -> 403
unsigned HEAD /no-such-bkt             -> 403     (also 403!)
unsigned HEAD + fabricated Bearer header -> 400
GET /health                             -> 200     (does not inspect buckets)
```

Two consequences, both measured rather than assumed:

1. The fabricated header turns a healthy bucket into `400`, so the probe
   reports "bucket missing" against a bucket that is present and the gate
   blocks startup when the object store is UP.

2. Dropping the header and accepting `403` is NOT a sufficient fix. RustFS
   answers `403` for a nonexistent bucket too, so that classification
   would report ready when the bucket is absent. Distinguishing the two
   cases requires a SigV4-signed request.

### The data path has never executed

This is the headline finding and it predates feat-8. No row has ever been
written to the lakehouse. `DeltaBronzeSpec` writes to a local temp
directory, and `s3a://` was never exercised by any test before this
feature. So the unit suites being green says nothing about whether the
RSS -> Kafka -> s3a -> Delta path works at all; every prior feature closed
on compile and unit evidence alone.

Because the broken gate blocked startup, the data path stayed unexercised
even after feat-8 landed. Fixing the probe unblocks it; it does not prove
it.

RESOLVED 2026-10-19 by wave 8. With the probe corrected and RustFS and Kafka
up, `sbt run` from `pipelines/ingestion` passed the gate and completed two
polls, writing 579 and then 17 bronze units with Delta commits under
`s3a://lakehouse/bronze/news/_delta_log/`. The RSS -> Kafka -> s3a -> Delta
path therefore works, and this is the first evidence in the repository that
it does. The finding stands unretracted for every feature that closed before
this one, all of which closed on compile and unit evidence alone.

### Open and unproven: hadoop.home.dir

`sbt run` logs `java.io.FileNotFoundException: HADOOP_HOME and
hadoop.home.dir are unset`, because hadoop-aws is on the classpath without
a Hadoop installation. This is an INFO-level message from
`org.apache.hadoop.util.Shell` and no failure has been attributed to it.

It is deliberately NOT fixed speculatively. The corrective CU makes the
pipeline run end to end and asserts a row lands in the bucket; if this
warning is load-bearing the assertion will fail and name it. Fixing a
warning before demonstrating it breaks anything is how you end up with two
unexplained changes instead of one.

RESOLVED 2026-10-19. The wave-8 end-to-end run logged the warning and then
completed both polls successfully, so it is not load-bearing. Deferring it
until the run could answer the question is what produced this answer.

### Scope of the probe's claim after the fix

Acceptance for the probe must state what it actually verifies. After the
fix it proves the object store is live, because `/health` is the store's
own liveness endpoint. It does NOT prove the bucket exists, because that
needs SigV4 and is therefore left to the first write to surface. A gate
that claims more than it checks is worse than a gate with a documented
limit.

## Implementation status

Sixteen CUs across eight waves, all converged with zero escalations. Waves 1-6
delivered the feature as designed. Wave 7 corrected an `ARCH.DOTENV.DISCOVERY`
inversion that entered through a self-contradictory cu-001 test. Wave 8
corrected the object-store probe and was the first run in this repository to
write a row to the lakehouse. Remains Proposed until the wave-8 commit lands.

## Acceptance criteria

1. `DotEnv.discover` walks up from the starting directory toward the
   filesystem root, returns the first `.env` found, and returns an empty map
   when none exists.
2. `${VAR}` inside a value expands, nested expansion resolves, and a circular
   or self reference raises a hard error rather than yielding an empty value.
3. Expansion depth is bounded at 10; exceeding it is a hard error.
4. Single-quoted values are literal; unquoted and double-quoted values expand.
5. `Env.load()` resolves in the order `sys.env` > `.env` > the `conf/` files,
   with the winning source observable per key. `ConfigFactory.load()` is no
   longer used, because it resolves only against the classpath.
6. `ReadinessGate` returns `Ready` only when every dependency probe passes, and
   `NotReady(reasons)` naming each failing dependency otherwise.
7. At timeout with a dependency still down, the gate returns `NotReady` rather
   than blocking indefinitely.
8. Neither `Main` enters the poll loop while any dependency is unready, and
   the failure names the culprit.
9. No AWS SDK is used and no new runtime dependency beyond `hadoop-aws` is
   added to either `build.sbt`. Kafka is probed with a raw TCP socket; the
   object store is probed with an unauthenticated HTTP `GET` against its own
   `/health` liveness endpoint.
   Revised 2026-10-19. This criterion previously required an HTTP `HEAD`
   against the bucket path. That probe carried a fabricated
   `Authorization: Bearer <accessKey>:<secretKey>` header; S3 has no Bearer
   scheme, so RustFS answered `400` against a healthy bucket and the gate
   blocked startup while the store was UP.
10. WITHDRAWN 2026-10-19, superseded by the narrowed claim below. The original
    criterion required the gate to create the bronze bucket if absent and to
    verify it before reporting ready. Distinguishing an absent bucket from a
    present one needs a SigV4-signed request: RustFS answers `403` for a
    nonexistent bucket exactly as it does for an unsigned request, so no
    unsigned probe can separate the two cases. The gate now claims only what
    it checks -- the store is live -- and a missing bucket surfaces at the
    first write, which names it. The bucket is `lakehouse` and the endpoint is
    `http://localhost:9000`; both are read from configuration rather than
    hardcoded, so a different store is reachable without a code change.
11. Both pipelines remain standalone: `grep` proves neither tree references
    the other, mirroring bugfix-feature-5 criterion 4.
12. `sbt compile` and `sbt test` exit 0 in both `pipelines/ingestion` and
    `pipelines/enrichment`.
13. Neither tree contains `src/main/resources/application.conf`; both read
    their configuration from `conf/` at the repository root.
14. `ConfLoader` resolves the same `conf/` directory whether the process is
    launched from the repository root or from inside `pipelines/ingestion`,
    because `run / fork := true` makes the working directory the launch
    directory.

## Tests

- `pipelines/ingestion/src/test/scala/newspipe/DotEnvSpec.scala` and the
  byte-identical `pipelines/enrichment/src/test/scala/newspipe/DotEnvSpec.scala`
  — temporary directory chains proving walk-up order (criteria 1), `${VAR}`
  and nested expansion (2), depth-10 bound and circular/self hard error
  (2, 3), single-quote literal against bare and double-quoted expansion (4),
  and empty-map-when-absent (1)
- `EnvPrecedenceSpec.scala` in both trees — `sys.env` beats `.env` beats the
  `conf/` files for the same key, with the winning source asserted
  (criterion 5)
- `ConfLoaderSpec.scala` in both trees — a temporary directory chain proves
  walk-up finds `conf/` from a nested working directory, that a missing
  directory yields a named failure rather than an empty configuration, and that
  a key present in both `feeds.conf` and the per-pipeline file resolves to the
  per-pipeline value (criteria 13, 14)
- `test ! -f pipelines/ingestion/src/main/resources/application.conf` and the
  same for `pipelines/enrichment` — both print nothing (criterion 13)
- `cd pipelines/ingestion && sbt run` started from the pipeline directory
  resolves `conf/` at the repository root, proven by the run reaching the poll
  loop rather than exiting on a missing configuration (criterion 14)
- `ReadinessGateSpec.scala` in both trees — all stubs ready returns `Ready`;
  one stub failing returns `NotReady` naming it; a never-ready stub under a
  short timeout returns `NotReady` and does not hang (criteria 6, 7)
- `sbt test` from `pipelines/ingestion` and from `pipelines/enrichment`,
  both reporting zero failures (criterion 12)
- `sbt compile` from both pipeline directories (criteria 9, 12)
- `grep -rn 'pipelines/enrichment' pipelines/ingestion/` prints nothing, and
  `grep -rn 'pipelines/ingestion' pipelines/enrichment/` prints nothing
  (criterion 11)
- Manual smoke: with the object store and Kafka down, `./sbt run` in
  `pipelines/ingestion` exits non-zero within the gate timeout naming Kafka
  and the object store, instead of looping (criterion 8)
- `ObjectStoreReadinessProbeSpec.scala` in both trees -- drives a JDK HTTP
  server on an ephemeral port, so it contacts no real object store and no
  network; asserts `Ready` on 200, `NotReady` naming the endpoint on 5xx,
  `NotReady` when the connection is refused, the specific messages for an
  empty endpoint and an empty bucket, and that no `Authorization` header is
  sent at all (criteria 9, 10 as revised)
- Manual smoke, RUN 2026-10-19: with RustFS and Kafka up, `sbt run` from
  `pipelines/ingestion` passed the gate and completed two polls writing 579
  and 17 bronze units, with Delta commits under
  `s3a://lakehouse/bronze/news/_delta_log/`. This is the first test in this
  repository to exercise the object-store path at all; `DeltaBronzeSpec`
  writes to a local temp directory and no prior test had touched `s3a://`.
  A read-back through `spark.read.format("delta").load(...)` was not run;
  the Delta commit log confirms the write landed.
