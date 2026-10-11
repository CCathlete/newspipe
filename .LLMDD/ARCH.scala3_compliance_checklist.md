# Scala 3 compliance checklist
- Feature: bugfix-feature-2-arch-scala3-compliance
- Layout restructure: bugfix-feature-3-scala-layer-layout
- Rename: feat-4-infra-port-folder-rename (raw to message-queue, enrichment to llm-data-enricher)
- Split: bugfix-feature-5-split-standalone-pipelines (pipelines/ingestion fast path, pipelines/enrichment slow path)
- Fixes: bug-1-arch-scala3-compliance, bug-2-scala-layer-layout, bug-4-dedup-before-sink-write-loses-data
- Scala: 3.3.4, sbt 1.10.7, Java 21

## 1. given resolution under Scala 3
Verdict: PASS.
Evidence: givens live in object KafkaCodecs in src/main/scala/newspipe/infrastructure/message-queue/KafkaRawPublisher.scala as Encoder[Instant] plus Encoder[RssItem]. Call site imports them explicitly with import KafkaCodecs.given in publish. No package-object givens remain on the Scala path. No implicit keyword remains in src/main/scala.
Criterion: scalafixAll reports no given-in-module ambiguity.

## 2. Trait form for domain ports
Verdict: MOVED per LAYER_LAYOUT.
Evidence: ports RssSource, EnrichmentPort, BronzeSink, RawPublisher are traits in src/main/scala/newspipe/application/ports/Ports.scala. Domain holds models plus valueObjects only. Dependency inversion unchanged: services and infrastructure depend on the port traits.
Criterion: Ports.scala lists four traits; domain grep shows no infrastructure imports.

## 3. Cross-version dependency shim
Verdict: DOCUMENTED.
Evidence: build.sbt loads spark-sql 3.5.1 Provided plus delta-spark 3.1.0 through CrossVersion.for3Use2_13. Upstream publishes Scala 2.13 artifacts only. ExclusionRule removes scala-xml_3 so scala-xml_2.13 satisfies both Spark and ScalaTest. build.sbt comment lines point here.
Criterion: sbt compile exits 0 with shim in place.

## 4. Scala 3 boundary audit
Verdict: PASS under v2 partition, mirrored per pipeline.
Evidence: domain/models plus domain/valueObjects import stdlib plus java.time only. application/services/IngestionService.scala imports application.ports plus domain.models plus logging only. infrastructure port folders (rss, message-queue, llm-data-enricher, bronze) plus env/ own all external imports. control/dependencyInjection/AppContainer.scala owns construction; control/entryPoint/Main.scala reads config only via Env. No .scala file sits directly in any layer root. pipelines/ingestion and pipelines/enrichment mirror the same partition with duplicated sources and no shared modules: ingestion drops the enrich step and marks seen only after write; enrichment adds RawConsumer plus BronzeMergeSink ports with KafkaRawConsumer plus DeltaMergeSink adapters.
Criterion: find maxdepth 1 under each layer root prints zero files; sbt compile green in root plus both pipeline directories.

## 5. then-do control syntax consistency
Verdict: PASS.
Evidence: control flow uses Scala 3 form at src/main/scala/newspipe/control/entryPoint/Main.scala (while true do) with try/catch case, plus src/main/scala/newspipe/application/services/IngestionService.scala (match with case) and if units.isEmpty then. No while-paren form found in src/main/scala.
Criterion: search for paren-while returns zero hits.
