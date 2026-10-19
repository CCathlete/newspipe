# feat-9: Single launcher and legible logs

- Status: Proposed
- Created: 2026-10-19
- Feature ID: feat-9-single-launcher-and-legible-logs

## Synopsis

Motivation: running newspipe is a manual, multi-terminal, multi-repository
procedure, and once it is running the output cannot be read.

Three problems, each measured on 2026-10-19 rather than assumed:

1. **No launcher exists.** `find . -maxdepth 2 -name '*.sh'` at the repository
   root prints nothing, so a full start is hand-driven: `terraform apply` in
   `~/Repos/pipeline_infra`, then `cd pipelines/ingestion && sbt run`, then
   `cd pipelines/enrichment && sbt run`, each in its own terminal. feat-8
   deliberately scoped this out as "the single launcher executable and its
   infra up/down lifecycle (separate feature)".

2. **The output is unreadable because there is no logging configuration at
   all.** `find pipelines -name 'logback*.xml'` prints nothing in either tree
   while `logback-classic` is on both dependency lists, so logback falls back
   to `BasicConfigurator` and the root logger runs at DEBUG. Kafka's
   coordinator heartbeat therefore writes DEBUG lines every three seconds,
   while the only two lines carrying information -- `poll merged N enriched
   units` (INFO) and `enrichment failed for <link>` (WARN) -- sit buried
   underneath. Diagnosing "enrichment is not enriching" that day required
   reading past hundreds of heartbeat lines.

3. **Basic diagnostics require hand-written commands.** Establishing whether
   the topic held data at all, and where the consumer group sat, took three
   separate `docker exec kafka ...` invocations (console-consumer,
   consumer-groups describe, and GetOffsetShell, which failed outright
   because its class moved in Kafka 3.7).

Scope (in):

- `conf/logback.xml` at the repository root's `conf/`, alongside the existing
  `feeds.conf`, `ingestion.conf` and `enrichment.conf`, so logging levels
  become external user configuration consistent with the rule feat-8
  established. Neither pipeline tree gains `src/main/resources/logback.xml`.
- A Python launcher at `scripts/newspipe.py` with four verbs:
  - `up` -- run `terraform apply` against the pipeline_infra repository, then
    poll the same three dependencies `ReadinessGate` checks (Kafka TCP,
    RustFS `/health`, `opencode` on PATH) and either report ready or name each
    dependency still down and exit non-zero.
  - `run {ingestion|enrichment|both}` -- start the selected pipeline(s) as
    child processes with the external logback configuration wired in.
  - `down` -- terminate the pipeline children it started, and nothing else. It
    never invokes terraform.
  - `status` -- report infra reachability, the `raw_rss` message count, the
    `enrichment-group` committed offset and lag, and the newest `poll merged`
    line per running pipeline, all without the user having to `docker exec`.
- A `mypy --strict` zero-error probe over the launcher, run as
  `uvx mypy --strict` (no standalone `mypy` is installed on this host;
  `uvx` is).

Wiring mechanism, decided at design time so no CU has to rediscover it: both
pipelines set `run / fork := true`, so a `-D` passed to the sbt JVM never
reaches the application JVM, and editing either `build.sbt` to append
`run / javaOptions` would violate the no-pipeline-edit constraint above. The
launcher therefore exports `JAVA_TOOL_OPTIONS` containing
`-Dlogback.configurationFile=<absolute path to conf/logback.xml>` in the
environment it spawns sbt with. Verified on this host: a JVM launched with
that variable reports `logback.configurationFile = <path>` in
`-XshowSettings:properties`. The one cosmetic cost is a `Picked up
JAVA_TOOL_OPTIONS: ...` line on **stderr**, which does not enter the stdout
stream the launcher parses.

New paths outside the current standing approval tokens: `scripts/newspipe.py` and
any launcher unit tests. Implementation will need an approval token naming
them; `pipelines/**` stays untouched.

Note for compile time: this feature is Python, so `APP.LANGUAGE=Python` must
be set when compiling its ITR or the `ARCH.PYTHON.*` discipline (explicit
declarations, no `Any`, slotted+frozen dataclasses, monadic errors) will not
be attached to the frames.

Scope (out):

- **Infra teardown.** `terraform destroy` cannot run today: every volume and
  the shared network in `pipeline_infra/data_platform.tf` carry
  `prevent_destroy = true`, so a destroy errors before it touches anything.
  Removing those guards is pipeline_infra's decision and repository, not this
  feature's. `down` stops pipelines only.
- Any edit to `~/Repos/pipeline_infra` -- a separate repository, separately
  versioned.
- Any edit under `pipelines/**`. The logback file reaches the forked run JVM
  through the environment, not through a build change.
- Log shipping, aggregation, rotation, or a log file on disk; stdout only.
- Introducing a `.env`. feat-8's DotEnv machinery stays available, but this
  feature does not require one.
- Silver/gold, Trino, Hive Metastore.

## Implementation status

Proposed. No CUs assigned yet.

## Acceptance criteria

1. `conf/logback.xml` exists at the repository root and is the only logging
   configuration: `find pipelines -name 'logback*.xml' -not -path '*/target/*'`
   prints nothing.
2. Started through the launcher, a pipeline's stdout carries zero lines
   matching `HEARTBEAT` (no Kafka DEBUG), while `newspipe` records remain at
   INFO.
3. Started through the launcher, a completed poll cycle surfaces
   `poll merged N enriched units` on stdout, so the informative line is
   reachable without grep.
4. `python3 scripts/newspipe.py --help` exits 0 and lists exactly `up`, `run`,
   `down`, `status`.
5. `run ingestion` starts only the ingestion pipeline, `run enrichment` only
   the enrichment pipeline, and `run both` starts exactly two pipeline child
   processes; no verb starts a pipeline it was not asked for.
6. `down` terminates the pipeline children it started and leaves the Kafka and
   RustFS containers running (`docker ps` still lists both).
7. `down` invokes no terraform process and exits 0, despite `terraform destroy`
   being impossible on this host because of the `prevent_destroy` guards.
8. `up` runs `terraform apply` against the pipeline_infra path and reports
   ready only when the Kafka TCP probe, the RustFS `/health` probe and
   `opencode` on PATH all pass; when one fails it names that dependency and
   exits non-zero rather than proceeding.
9. `status` prints, with no `docker exec` needed by the user: the `raw_rss`
   message count, the `enrichment-group` committed offset and lag, and the
   newest `poll merged` line for each running pipeline.
10. `uvx mypy --strict scripts/newspipe.py` exits 0 with zero errors.
11. No file under `pipelines/**` is created, modified or deleted: after
    implementation and after a launcher run, `git status --porcelain pipelines/`
    is empty.

## Tests

- Config presence (criterion 1): `test -f conf/logback.xml` exits 0, and
  `find pipelines -name 'logback*.xml' -not -path '*/target/*'` prints nothing.
- Verb surface (criterion 4): `python3 scripts/newspipe.py --help` exits 0 with
  `up`, `run`, `down`, `status` in its stdout; an unrecognised verb exits
  non-zero.
- Type probe (criterion 10): `uvx mypy --strict scripts/newspipe.py` exits 0 with
  zero errors.
- Log legibility (criteria 2, 3): with Kafka and RustFS up, reset the
  enrichment group near the topic end so one cycle is short --
  `docker exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server
  localhost:9092 --group enrichment-group --topic raw_rss --reset-offsets
  --shift-by -3 --execute` -- then run `python3 scripts/newspipe.py run enrichment`
  for one cycle. Assert stdout has `grep -c HEARTBEAT` == 0 and contains a line
  matching `poll merged [0-9]+ enriched units`.
- Verb selection (criterion 5): invoke each verb in turn and assert the
  launcher-reported child pids number 1, 1 and 2 respectively.
- `down` containment (criteria 6, 7): after `down`, assert the reported
  children are gone, `docker ps --format '{{.Names}}'` still lists `kafka` and
  `rustfs`, no `terraform` process appeared, and the exit code is 0.
- `up` readiness, negative probe (criterion 8): `docker stop rustfs`, then
  `python3 scripts/newspipe.py up` must exit non-zero and name `rustfs`; restore
  with `docker start rustfs`. With all three dependencies up, `up` exits 0.
- `status` content (criterion 9): `python3 scripts/newspipe.py status` stdout
  contains the `raw_rss` count, the `enrichment-group` offset and lag, and a
  `poll merged` line.
- Source integrity (criterion 11): `git status --porcelain pipelines/` empty
  after implementation and after a launcher run.

