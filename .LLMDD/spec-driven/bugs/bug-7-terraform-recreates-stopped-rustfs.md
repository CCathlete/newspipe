# bug-7: terraform apply recreates the stopped rustfs container

- Status: Open
- Created: 2026-10-20
- Resolved: -
- Bug ID: bug-7-terraform-recreates-stopped-rustfs

## Bug detail

Stopping the object store is supposed to prove that a readiness check
fails closed. It does not, because the infra apply that runs first in the
same command puts the store back.

Symptom: `up` reports ready while the store is provably down.

Reproduction:

```sh
docker stop rustfs
python3 scripts/newspipe.py up
echo "exit=$?"
docker start rustfs
```

Expected: non-zero exit, and output naming `rustfs` as the dependency still
missing, because `/health` cannot answer while the container is stopped.

Actual: exit 0 with `ready: kafka, rustfs, opencode`. The readiness gate is
not what fails here — the container is running again by the time the gate
runs, so the gate correctly sees it healthy. The probe itself behaves: with
the endpoint verifiably dead the gate reports not-ready, and it passes only
after the container is back.

Evidence, two independent attempts, both capturing the same actual result:
`/tmp/newspipe-feat9-up-fail.log` and `/tmp/newspipe-feat9-up-fail2.log`,
each a single line reading `ready: kafka, rustfs, opencode`. The run that
produced them also confirmed, immediately before, that `/health` answered
200 only after the apply had already healed the container.

Not re-run during validation: reproducing it needs `docker stop rustfs`
plus `terraform apply`, an infrastructure mutation outside the Advisor's
grant, so this report rests on the captured wave artifacts above. Their
contents were re-read during validation, not quoted from a transcript.

Environment: 2026-10-19, Linux host, `~/Repos/pipeline_infra` applied
through the docker provider; `resource "docker_container" "rustfs"` at
`data_platform.tf:250` declares no explicit `must_run` or restart policy.

Suspected cause: the docker provider restores a container that exists in
config but is stopped, and `terraform apply` therefore heals `rustfs`
before the readiness probe in the same process runs. Whether that is the
provider's `must_run` default, a recreate driven by config drift, or
`rustfs_init` bringing it back is the first thing to confirm in
`pipeline_infra`; the symptom alone does not distinguish them.

Why a newspipe change cannot fix this: the launcher's order is fixed by
design — apply the infrastructure, then probe readiness. Probing first
would report not-ready for a stack that is about to come up, which is the
opposite of what `up` is for. The fix belongs in the infrastructure
repository, where the container's restart behaviour is declared, and that
repository is outside every CU of the launcher work.

## Resolution status

Open. No fix has landed in `~/Repos/pipeline_infra`. Until one does, the
negative half of the launcher's readiness criterion cannot be proven on
this host; the positive half is proven and is recorded separately in the
launcher feature spec.

## Fix summary (Resolved only)

Pending.

## Tests proving resolution (Resolved only)

Pending. The proving test is the reproduction above with the expected
result instead of the actual one: `docker stop rustfs`, then
`python3 scripts/newspipe.py up` must exit non-zero and name `rustfs`,
followed by `docker start rustfs` to restore the stack.
