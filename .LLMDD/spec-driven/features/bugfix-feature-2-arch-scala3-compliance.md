# bugfix-feature-2: arch-scala3-compliance

- Status: Proposed
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

Open. Requires creation of architecture compliance checklist and build.sbt
updates. After implementation, all 5 checklist items should be populated, `sbt
compile` should exit 0, and the hexagonal layering constraint (domain >
application > infrastructure > control) should be verified under Scala 3 type
checking.

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
