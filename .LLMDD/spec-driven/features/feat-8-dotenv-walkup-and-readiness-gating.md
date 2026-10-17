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
- Feed configuration — `feeds = []` today, which means nothing has ever been
  ingested
- Any change to `~/Repos/pipeline_infra`; the S3 gateway decision lives there
- Silver/gold, Trino, Hive Metastore

## Implementation status

Proposed. No CUs assigned yet.

## Acceptance criteria

1. `DotEnv.discover` walks up from the starting directory toward the
   filesystem root, returns the first `.env` found, and returns an empty map
   when none exists.
2. `${VAR}` inside a value expands, nested expansion resolves, and a circular
   or self reference raises a hard error rather than yielding an empty value.
3. Expansion depth is bounded at 10; exceeding it is a hard error.
4. Single-quoted values are literal; unquoted and double-quoted values expand.
5. `Env.load()` resolves in the order `sys.env` > `.env` > `application.conf`,
   with the winning source observable per key.
6. `ReadinessGate` returns `Ready` only when every dependency probe passes, and
   `NotReady(reasons)` naming each failing dependency otherwise.
7. At timeout with a dependency still down, the gate returns `NotReady` rather
   than blocking indefinitely.
8. Neither `Main` enters the poll loop while any dependency is unready, and
   the failure names the culprit.
9. Readiness probes use a raw socket and an HTTP `HEAD`; no AWS SDK and no new
   runtime dependency beyond `hadoop-aws` is added to either `build.sbt`.
10. The bronze bucket is created if absent before the gate reports ready.
    The bucket is `lakehouse` and the endpoint is `http://localhost:9000`;
    both are read from configuration rather than hardcoded, so a different
    store is reachable without a code change.
11. Both pipelines remain standalone: `grep` proves neither tree references
    the other, mirroring bugfix-feature-5 criterion 4.
12. `sbt compile` and `sbt test` exit 0 in both `pipelines/ingestion` and
    `pipelines/enrichment`.

## Tests

- `pipelines/ingestion/src/test/scala/newspipe/DotEnvSpec.scala` and the
  byte-identical `pipelines/enrichment/src/test/scala/newspipe/DotEnvSpec.scala`
  — temporary directory chains proving walk-up order (criteria 1), `${VAR}`
  and nested expansion (2), depth-10 bound and circular/self hard error
  (2, 3), single-quote literal against bare and double-quoted expansion (4),
  and empty-map-when-absent (1)
- `EnvPrecedenceSpec.scala` in both trees — `sys.env` beats `.env` beats
  `application.conf` for the same key, with the winning source asserted
  (criterion 5)
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
- Manual smoke: with RustFS and Kafka up, a `.env` supplying the five
  `NEWSPIPE_S3_*` keys above, one poll writes a row readable back through
  `spark.read.format("delta").load("s3a://lakehouse/bronze/news")`
  (criteria 9, 10). This is the first test in this repository that would
  exercise the object-store path at all; `DeltaBronzeSpec` writes to a
  local temp directory and no prior test has touched `s3a://`.
