# bugfix-feature-2: arch-scala3-compliance

- Status: Implemented
- Created: 2026-10-10
- Feature ID: bugfix-feature-2-arch-scala3-compliance

## Synopsis

Fixes bug-1-arch-scala3-compliance.

The newspipe codebase runs on Scala 3.3.4 but the architectural pattern was not
explicitly designed or validated for Scala 3 compliance. This feature adds the
Scala 3 boundary audit, compliance checklist, and cross-version dependency
documentation required to close architecture gaps identified in the codebase
transition to Scala 3.

Scope (in):
- Add `.LLMDD/ARCH.scala3_compliance_checklist.md` with five audited items:
  1. Implicit/given resolution under Scala 3
  2. Trait vs abstract class for domain ports
  3. Cross-version dependency shim (Spark/Delta) rationale
  4. Scala 3 boundary audit (hexagonal layering, port flow, DI constraints)
  5. then/do control syntax consistency across all modules
- Update build.sbt comments to cross-reference the checklist
- Ensure `sbt compile` passes after checklist addition

Scope (out):
- Dashboard itself (only tags feeding it later)
- Silver/gold layers
- Feed curation UI
- Removal of the legacy Python pipeline (stays until Scala pipeline is E2E green)

## Implementation status

Implemented. Two CUs across two waves, CONVERGED, zero escalations;
landed in 49ae49b. Closed 2026-10-19.

Criterion 4 (`scalafixAll` green) was NOT re-run at close and is recorded
as UNVERIFIED. The closing session ran `sbt compile test` in both trees
(ingestion 37/37, enrichment 41/41, exit 0), but scalafix is itself an
sbt task and the Designer directed that no further sbt invocations run
after an earlier invocation destabilised the host. The remaining six
criteria were re-verified live: the checklist carries all 5 sections (1),
compile exits 0 (2), the domain tree has zero infrastructure/kafka/spark/
delta/rome imports (3), no `while (` survives in either tree (5), both
build.sbt files cite `ARCH.scala3_compliance_checklist.md` (6), and the
checklist cites this feature (7).

## Acceptance criteria

1. `.LLMDD/ARCH.scala3_compliance_checklist.md` exists with all 5 sections populated
   (implicit/given, trait vs abstract class, cross-version shim, boundary audit, control syntax)
2. `sbt compile` exits 0 after checklist and build.sbt updates
3. No cross-layer import violations: `grep -rn "^import" src/main/scala/newspipe/domain/` shows no
   infrastructure, kafka, spark, delta, or rome imports
4. given/using patterns resolve without scalafix ambiguity (scalafixAll green)
5. Control-flow syntax is consistent: no mixed `while true do` vs `while (true)` variants
6. Cross-version dependency shim documented in build.sbt comments referencing
   ARCH.scala3_compliance_checklist.md
7. Architecture audit checklist references bugfix-feature-2-arch-scala3-compliance.md

## Tests

- `sbt compile` passes (criterion 1)
- scalafixAll exits 0 with no given-in-module ambiguities
- `grep -rn "^import" src/main/scala/newspipe/domain/` shows no infra/kafka/spark/delta/rome imports
- Project-wide search for control-flow syntax patterns confirms consistent then/do usage
- build.sbt comments cross-reference ARCH.scala3_compliance_checklist.md
- .LLMDD/ARCH.scala3_compliance_checklist.md all 5 sections populated
