# bug-7: terraform apply recreates the stopped rustfs container

- Status: Resolved
- Created: 2026-10-20
- Resolved: 2026-10-20
- Bug ID: bug-7-terraform-recreates-stopped-rustfs

## Bug detail

Closed as not-a-bug. The observed behaviour is real; calling it a defect was
the error, and this file stays only so the finding is not silently erased.

What was observed: stopping the object store and then running the launcher's
`up` verb reports ready instead of failing.

```sh
docker stop rustfs
python3 scripts/newspipe up
docker start rustfs
```

Actual on 2026-10-19: exit 0, `ready: kafka, rustfs, opencode`, twice
(`/tmp/newspipe-feat9-up-fail.log`, `/tmp/newspipe-feat9-up-fail2.log`).

Why that is correct rather than broken: `up` means apply the infrastructure,
then probe whether it is healthy. The apply restores a container that is
declared in the infrastructure, so by probe time the store is up and the
report of "ready" is true. The test was designed so that it could never
fail — an apply that repairs what you stopped is doing its job, so
"stop it, then apply" is not a way to make a readiness check fail.

Confirmed mechanism, for whoever reads this later: `terraform state show
docker_container.rustfs` holds `must_run = true`, the docker provider's
default, which is why the apply restarts a container that was stopped by
hand. `~/Repos/pipeline_infra` sets no `must_run` of its own.

The launcher's ordering is unchanged and correct: `provision()` at
`scripts/newspipe:434` is the only place terraform is invoked, and it is
reachable only from `do_up`. `status` never applies anything.

## Resolution status

Resolved — closed as not-a-bug, not as fixed. No code change in either
repository, and none is warranted. A test that cannot fail is a bad test,
not a product defect.

## Fix summary (Resolved only)

No fix. Nothing changed. The report came from a test design that cannot
produce a failure, so there was nothing for a fix to address.

## Tests proving resolution (Resolved only)

Not applicable — there was no defect to prove fixed. The only honest
evidence is that the behaviour described above is expected: an apply
restoring a stopped dependency is what `up` is for, and the readiness gate
itself was verified separately to fail closed when the endpoint is actually
unreachable.
