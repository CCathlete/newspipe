# bugfix-feature-3: scala-layer-layout

- Status: Proposed
- Created: 2026-10-11
- Feature ID: bugfix-feature-3-scala-layer-layout

## Synopsis

Fixes bug-2-scala-layer-layout.

The feat-1 Scala scaffold placed one file per class directly in the layer
roots and loads config inline in Main, violating LAYER_LAYOUT,
NO_LAYER_ROOT_FILES, and ENV_ADAPTER. This feature relocates every main
source file into the demanded partition, moves the four port traits to
application/ports, types GeoArea/Urgency as opaque value objects, and puts
all configuration behind a new infrastructure/env/Env singleton with a
dependency-injection container in control. bugfix-feature-2 stays as the
docs-only audit; this feature is the behavior-preserving code reorganization.

Scope (in):
- domain/models/ (RssItem, Enrichment, BronzeUnit split) plus
  domain/valueObjects/ (GeoArea, Urgency opaque types)
- application/ports/ (four traits moved from domain) plus
  application/services/ (IngestionService moved)
- infrastructure/rss, enrichment, bronze, raw (one folder per port) plus
  infrastructure/env/ (Env singleton: application.conf plus sys.env overrides)
- control/dependencyInjection/ (AppContainer) plus control/entryPoint/ (thin Main)
- Test import updates; checklist sections 2 and 4 refreshed to v2 verdicts

Scope (out):
- Behavior changes (same feeds, topics, bronze schema, config keys)
- Silver/gold layers, dashboard, feed curation UI, Python pipeline removal

## Implementation status

Proposed. No CUs assigned yet.

## Acceptance criteria

1. Zero `.scala` files directly in any of the four layer roots under
   `src/main/scala/newspipe/` (domain, application, infrastructure, control)
2. `GeoArea`/`Urgency` opaque types compile and are the field types in
   `Enrichment` and `BronzeUnit`
3. The four port traits resolve from `newspipe.application.ports`
4. `Env.load()` reads `application.conf` with all eight `NEWSPIPE_*`
   overrides honored
5. `Main` reads configuration only via `Env`
6. `sbt compile` exits 0 and `sbt test` passes all suites
7. Bronze Delta schema unchanged (same 15 columns)

## Tests

- `find src/main/scala/newspipe -maxdepth 2 -type f` shows zero depth-1 files (criterion 1)
- `sbt compile` passes (criteria 2, 3, 4, 5)
- `sbt test` passes all suites (criterion 6)
- Delta spec asserts the same column set as before the moves (criterion 7)
- `NEWSPIPE_BRONZE_PATH=/tmp/x sbt "runMain newspipe.control.entryPoint.Main"` reads the override (criterion 4)
