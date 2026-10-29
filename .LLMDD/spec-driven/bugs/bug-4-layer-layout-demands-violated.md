# bug-4: Layer layout demands violated

- Status: Open
- Created: 2026-10-10
- Resolved: -
- Bug ID: bug-4-layer-layout-demands-violated

## Bug detail

Symptoms: `itr-compiler` source violates the layer layout demands recorded
in `.LLMDD/instructions/architecture.md` (`LAYER_LAYOUT`, `NO_LAYER_ROOT_FILES`,
`ENV_ADAPTER`) in five ways. (`dtr-builder` shares the same shape; this bug
covers `itr-compiler`, the app under active feature work.)

Reproduction (from repo root, read-only):

```sh
find itr-compiler/src/main -type f -name "*.scala" | sort
ls itr-compiler/src/main/scala/itrcompiler/ itr-compiler/src/main/scala/itrcompiler/*/
# EXPECTED per demands: application/{services,ports,useCases},
#   infrastructure/{one folder per port + Env adapter}, domain/{models,valueObjects},
#   control/{dependencyInjection,entryPoint}, camelCase folders (Scala), no files in any layer root.
# ACTUAL:
#   1. application/{ports,services,use_cases} — snake_case, expected camelCase useCases.
#   2. infrastructure/{database,environment,filesystem,parsers} — 4 folders for 3 ports
#      (DTRLoad, CUWrite, ContentRead); filesystem implements all three ports, database has
#      no port, parsers are helpers. Expected one folder per port.
#   3. domain/{models} only — ValueObject.scala sits inside models/; expected models + valueObjects.
#   4. control/{cli,dependency_injection,entry_point} — extra cli/ folder (CliParser.scala);
#      expected dependencyInjection + entryPoint only, camelCase.
#   5. infrastructure/environment/Environment.scala is a singleton reading sys.env plus an
#      in-memory overrides map, but loads no .env/config file — despite its docstring claiming
#      "loaded .env values". Expected Env adapter loading config and env variables.
```

Layer roots themselves are clean (no files directly in any of the four
roots) — that demand already holds.

Environment: repo checkout, `itr-compiler/src/main/scala/itrcompiler/`
(26 Scala files); same shape in `dtr-builder/src/main/scala/dtrbuilder/`.

Suspected cause: the code predates the demands — the partition grew
organically (shared `filesystem` adapter, helper folders `parsers`/`database`,
`cli` parsing kept separate, `ValueObject` never split out, `.env` loading
never implemented). Nothing enforces the layout (no compiler/dtr-builder
check), so drift was silent.

Impact: new CUs drafted against the demands (e.g. `bugfix-feature-2`, whose
coordinates reference the new-structure keys) target files/keys that do not
exist yet; every structural CU pays a relocation tax until the layout is fixed.

## Resolution status

Open. No fix merged.

## Fix summary (Resolved only)

Pending.

## Tests proving resolution (Resolved only)

Pending. Suggested: a layout audit command (`find <app>/src/main -type d`
asserting the exact folder set per layer, `find <app>/src/main/scala/<app>/<layer> -maxdepth 1 -type f`
asserting empty for all four layers, grep asserting `sys.env` + `.env` loading
in the Env adapter) plus `sbt test` green after the file moves.
