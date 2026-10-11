# bug-2: scala-layer-layout

- Status: Resolved
- Created: 2026-10-11
- Resolved: 2026-10-11
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

Resolved in 1fe3e25.

## Fix summary (Resolved only)

V2 layer partition per bugfix-feature-3-scala-layer-layout (cu-001..cu-008): models split plus GeoArea/Urgency opaque types, ports moved to application/ports, one infra folder per port plus env/Env singleton with eight NEWSPIPE_* overrides, AppContainer DI plus thin entryPoint/Main. Fix commit: 1fe3e25.

## Tests proving resolution (Resolved only)

Proven by commit 1fe3e25: `find src/main/scala/newspipe/domain src/main/scala/newspipe/application src/main/scala/newspipe/infrastructure src/main/scala/newspipe/control -maxdepth 1 -type f | wc -l` prints 0 (was 8 files); `sbt compile` exits 0; `sbt test` passes 5/5 (failing-before: 8 files in layer roots, no env/ directory; passing-after: v2 partition, all suites green).
