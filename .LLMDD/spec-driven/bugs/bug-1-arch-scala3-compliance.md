# bug-1: arch-scala3-compliance

- Status: Resolved
- Created: 2026-10-10
- Resolved: 2026-10-19
- Bug ID: bug-1-arch-scala3-compliance
- Related features: bugfix-feature-2-arch-scala3-compliance

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

Resolved 2026-10-19 by bugfix-feature-2-arch-scala3-compliance
(commit 49ae49b), its two CUs converged with zero escalations.

## Fix summary (Resolved only)

The audit now exists: `.LLMDD/ARCH.scala3_compliance_checklist.md`, five
sections, each naming evidence and a criterion. All five items raised here
closed against it -- givens moved out of package objects into an object with
an explicit `given` import at the call site, traits retained for the ports
with the reasoning recorded, the cross-version shim documented in both
build.sbt files against Spark 3.5.1 / Delta 3.1.0 publishing Scala 2.13
artifacts only, a full boundary audit, and control-syntax consistency.

## Tests proving resolution (Resolved only)

Re-run live 2026-10-19:
- `grep -rn '\bimplicit\b' pipelines/*/src` -> 0 hits; `^package object` -> 0
- `grep -rn '^import' .../domain/` matched against infrastructure/kafka/
  spark/delta/rome -> 0 hits (no cross-layer imports)
- `grep -rn 'while (' pipelines/*/src` -> 0 hits; both trees use `while true do`
- Both build.sbt files cite `ARCH.scala3_compliance_checklist.md`; the
  checklist cites this feature
- `sbt compile test` green in both trees: ingestion 37/37, enrichment 41/41,
  exit 0

UNVERIFIED: `sbt scalafixAll` itself was not re-run at close -- scalafix is
an sbt task and no further sbt invocation was permitted after an earlier
one destabilised the host. This is the one leg of the original proof that
rests on the audit rather than on a fresh run.
