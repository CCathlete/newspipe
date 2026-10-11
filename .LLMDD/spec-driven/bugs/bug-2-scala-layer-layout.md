# bug-2: scala-layer-layout

- Status: Open
- Created: 2026-10-11
- Resolved: -
- Bug ID: bug-2-scala-layer-layout
- Related features: -

## Bug detail

Symptoms: the newspipe Scala tree violates the layer layout demands in
`.opencode/instructions/llmdd/architecture.md` (`LAYER_LAYOUT`,
`NO_LAYER_ROOT_FILES`, `ENV_ADAPTER`) in four ways. bugfix-feature-2
(checklist plus build.sbt comments, commit 49ae49b) documented the layout
but changed zero production files, so the violations stand.

Reproduction (from repo root, read-only):

```sh
find src/main/scala/newspipe -maxdepth 1 -type f | sort
# ACTUAL: application/IngestionService.scala, control/Main.scala,
#   domain/models.scala, domain/ports.scala,
#   infrastructure/DeltaBronzeSink.scala, infrastructure/KafkaRawPublisher.scala,
#   infrastructure/OpenCodeCliClient.scala, infrastructure/RomeRssClient.scala
# EXPECTED per demands: no files in any layer root; domain holds models/valueObjects
#   only; application holds services/ports/useCases; infrastructure holds one folder
#   per port plus Env singleton; control holds dependencyInjection/entryPoint.
ls src/main/scala/newspipe/infrastructure/env 2>&1
# ACTUAL: no such directory — no Env adapter exists. Config is loaded inline in
#   control/Main.scala via ConfigFactory with no sys.env override path.
```

Environment: repo checkout, `src/main/scala/newspipe/` (8 main Scala files).

Suspected cause: the feat-1 CUs scaffolded one file per class directly in
the layer roots, predating enforcement of the layout demands.

## Resolution status

Open. No fix merged.

## Fix summary (Resolved only)

Pending.

## Tests proving resolution (Resolved only)

Pending. Suggested: `find src/main/scala/newspipe -maxdepth 2 -type f`
asserting zero files at depth 1 under each layer root, package assertions
per demand, `sbt compile` plus `sbt test` green after the moves.
