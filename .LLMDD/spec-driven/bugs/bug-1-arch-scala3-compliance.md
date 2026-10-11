# bug-1: arch-scala3-compliance

- Status: Open
- Created: 2026-10-10
- Bug ID: bug-1-arch-scala3-compliance
- Related features: -

## Bug detail

The newspipe codebase runs on Scala 3.3.4 (`build.sbt:3`) but the architectural
pattern was not explicitly designed or validated for Scala 3 compliance. Several
areas reflect Scala 2–oriented habits that should be audited and updated for
Scala 3 best practices:

1. **Implicit/given instance placement** — `KafkaRawPublisher.scala:14-15` defines
   `given Encoder[Instant]` and `given Encoder[RssItem]` in a package object.
   Scala 3 treats givens differently (they areno longer automatic implicit
   resolution candidates outside their scope in the same way). These should be
   reviewed to ensure they don't cause implicit resolution ambiguities or
   "given instances in modules" anti-patterns.

2. **Trait vs abstract class for ports** — The domain ports (`RssSource`,
   `EnrichmentPort`, `BronzeSink`, `RawPublisher`) use `trait`. In Scala 3,
   `abstract class` with `opaque type` or self-types can provide stronger
   encapsulation. This is a style concern, not a blocker, but should be
   documented.

3. **Cross-version dependency management** — Spark 3.5.1/Delta 3.1.0 ship Scala
   2.13 artifacts pulled via `CrossVersion.for3Use2_13`. While this works, the
   project should document why these aren't published as native Scala 3 artifacts
   and whether a future migration could remove the cross-version shim.

4. **No Scala 3 boundary audit** — There is no architectural checklist or
   compliance report verifying that the hexagonal architecture (domain > application >
   infrastructure > control), port flow (outbound → inbound), and dependency inversion
   constraints are all satisfied under Scala 3's stricter type system. A manual or
   automated audit is missing.

5. **`then`/`do` control syntax compatibility** — The code uses Scala 3 control
   syntax (`while true do`, `if ... then ... else ...`). While this works, the
   architecture should verify that all CUs (Computational Units) and future
   code additions respect the same control-flow conventions to avoid mixed-style
   modules.

The project is functionally on Scala 3, but the architectural foundation lacks
Scala 3–specific validation. Without an audit, implicit/given patterns and
cross-version dependency choices could accumulate technical debt.

## Resolution status

Open

## Fix summary (Resolved only)

Pending resolution — requires architecture audit and Scala 3 compliance review.

## Tests proving resolution (Resolved only)

Pending — after the architecture audit, proving tests should validate:
- `sbt scalafixAll` or equivalent rules pass for Scala 3 implicit/given
- Cross-version dependency configuration is audited and documented
- No cross-layer dependency violations under Scala 3 type checking
- Control-flow syntax consistency across all source files
