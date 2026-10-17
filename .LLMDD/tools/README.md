# .LLMDD/tools

Repo-agnostic tooling carried by the .LLMDD standard. Every tool derives
the repository root from its own location, so the folder works unchanged
in any repository that carries a .LLMDD folder.

## next-spec-id.py

Allocates the next bug or feature serial for this repository. It reads
the plain-text counter at `.LLMDD/spec-driven/ids.txt` plus every git ref
under `refs/heads` and `refs/remotes`, so two branches cut from the same
state of dev never receive the same number.

```sh
python3 .LLMDD/tools/next-spec-id.py --kind bug
python3 .LLMDD/tools/next-spec-id.py --kind feature --prefix bugfix-feature-
python3 .LLMDD/tools/next-spec-id.py --kind feature --dry-run
python3 .LLMDD/tools/next-spec-id.py --check
```

`--kind` prints the id and bumps the counter. `--prefix` chooses the id
lead, so a bug or bugs fix prints `bugfix-feature-<n>-` from the feature
sequence. `--dry-run` prints without writing. `--check` never writes and
fails on a duplicate serial, on an artifact directory with no matching
spec file, or on a counter behind a serial present in the tree.

Exit codes follow the convention shared with `check-advisor-approval.py`:
0 clean or id printed, 1 check violation, 2 usage error.

## check-advisor-approval.py

Validates a Designer-issued approval token against a proposed change.
Exit 0 allows, 1 denies, 2 is a usage error.
