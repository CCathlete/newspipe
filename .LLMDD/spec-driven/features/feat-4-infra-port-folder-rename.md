# feat-4: infra-port-folder-rename

- Status: Proposed
- Created: 2026-10-11
- Feature ID: feat-4-infra-port-folder-rename

## Synopsis

Motivation: the v2 infrastructure partition names two port folders by
mechanism instead of intent. `raw` is a Kafka message queue; `enrichment`
is LLM-data enrichment via the opencode CLI. Renaming the folders makes
the partition self-describing for readers and future CUs.

Scope (in):
- `infrastructure/raw/` to `infrastructure/message-queue/` (package
  `newspipe.infrastructure.messagequeue`; hyphens are illegal in Scala
  package identifiers, so the package drops the hyphen)
- `infrastructure/enrichment/` to `infrastructure/llm-data-enricher/`
  (package `newspipe.infrastructure.llmdataenricher`)
- Import updates in `AppContainer` and `OpenCodeParserSpec`
- Checklist evidence path update
- Class names (`KafkaRawPublisher`, `OpenCodeCliClient`) unchanged

Scope (out):
- Behavior changes; new ports; class renames; other folders

## Implementation status

Proposed. No CUs assigned yet.

## Acceptance criteria

1. `infrastructure/raw/` no longer exists; `message-queue/KafkaRawPublisher.scala`
   carries package `newspipe.infrastructure.messagequeue`
2. `infrastructure/enrichment/` no longer exists;
   `llm-data-enricher/OpenCodeCliClient.scala` carries package
   `newspipe.infrastructure.llmdataenricher`
3. No references to `infrastructure.raw` or `infrastructure.enrichment`
   remain under `src/`
4. `sbt compile` exits 0 and `sbt test` passes all suites
5. Checklist evidence paths name the new folders

## Tests

- `ls src/main/scala/newspipe/infrastructure/` shows bronze, env,
  llm-data-enricher, message-queue, rss (criteria 1, 2)
- `grep -rn 'infrastructure\.raw\|infrastructure\.enrichment' src/` prints
  nothing (criterion 3)
- `sbt compile` passes and `sbt test` passes all suites (criterion 4)
- `grep -n 'message-queue' .LLMDD/ARCH.scala3_compliance_checklist.md`
  prints at least one line (criterion 5)
