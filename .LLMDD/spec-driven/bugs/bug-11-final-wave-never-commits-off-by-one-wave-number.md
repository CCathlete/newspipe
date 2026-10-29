# bug-11: Final wave never commits when waves are not numbered 1..N

- Status: Open
- Created: 2026-10-19
- Resolved: -
- Bug ID: bug-11-final-wave-never-commits-off-by-one-wave-number

## Bug detail

Symptom: a `run-waves.py` run whose wave numbers are not the consecutive
sequence `1..N` completes and reports `CONVERGED`, but leaves every
implementation change uncommitted in the working tree. No wave is ever told
to commit, because no wave satisfies the "am I final?" test.

Cause: `is_final` compares a wave's *label* against the *number of waves*,
which is the same integer only when labels run `1..N` consecutively.

```python
# .LLMDD/tools/run-waves.py:1628
num_waves: int = len(waves)

# .LLMDD/tools/run-waves.py:1216
is_final: bool = (wave == num_waves)
```

`is_final` is meant to answer "is this the last wave of this run?". It
actually answers "does this wave's number happen to equal how many waves
there are?". The `else` branch then instructs the wave lead:

> 8. Do NOT commit - the final wave lead will commit

...and the `for wave_def in waves:` loop is the only place `run_code_lead` is
invoked, with no post-loop commit pass. So when the last wave is misjudged,
the instruction to commit is issued to nobody and the work stays dirty.

Reproduction (read-only, no wave run needed):

```python
# expressions lifted verbatim from run-waves.py:1628 and :1216
def simulate(waves):
    num_waves = len(waves)              # line 1628
    return [(w["wave"], (w["wave"] == num_waves)) for w in waves]  # line 1216

for waves in ([{"wave":1},{"wave":2},{"wave":3}],   # consecutive
              [{"wave":8}],                          # corrective, single
              [{"wave":7},{"wave":8}],               # corrective, pair
              [{"wave":1},{"wave":2},{"wave":8}]):   # mixed
    n, res = simulate(waves)
    print(n, res, "commits:", [w for w, f in res if f] or "NONE")
```

Observed:

```
consecutive 1..3       len=3  -> commits: [3]
corrective single 8    len=1  -> commits: NONE
corrective 7,8         len=2  -> commits: NONE
mixed 1,2,8            len=3  -> commits: NONE
```

Only the consecutive case commits. Every other labelling loses the commit.

Environment: `llm-driven-design` at `9e325ad`; identical at
`newspipe` `.LLMDD/tools/run-waves.py:1216` (synced copy).

Live occurrence, not theoretical: `newspipe` feat-8, wave 8
(`cu-015`, `cu-016`, object-store readiness probe). `waves.json` held a
single wave numbered 8, so `num_waves == 1` and `8 != 1`. The wave-8 lead
finished with, verbatim from `lead-wave-8.attempt-0.log`:

> 8. **Not committed** - Per instructions, final wave lead will commit

and wrote `wave-8-commit.txt` instead. Six files were left uncommitted: four
modified (`AppContainer.scala` and `ObjectStoreReadinessProbe.scala` in each
tree) plus two new untracked
(`ObjectStoreReadinessProbeSpec.scala` in each tree). `newspipe.convergence.json`
reported `"status": "CONVERGED"` with `coder_status: 0`, `lead_status: 0`,
`escalations: 0` throughout. The run was reported as successful by every
signal the runner emits.

The same defect hit wave 7 of that feature (`waves.json` numbered 7,
`num_waves == 1`). Its commit `ec1c91f` was made by hand, and its message is
Designer prose rather than the text the lead had staged in
`wave-7-commit.txt` - the signature of a manual commit.

Impact: a run that reports success can leave its entire implementation
uncommitted. The failure is silent at every level the runner surfaces:
convergence is `CONVERGED`, every CU is `STATUS=COMPLETED` with
`VERIFICATION_RESULT=PASSED`, and no warning mentions the missing commit.
Detection requires `git status` against HEAD. Because corrective waves are
exactly the case where labels diverge from `1..N`, the defect is triggered
most often exactly when a feature is being repaired and a commit matters
most.

Note the design intent is sound and should be preserved: non-final waves
deliberately do not commit, so the last wave produces one consolidated
commit per run. The defect is confined to identifying the last wave.

Suggested fix: compare against the last wave's label rather than the count.

```python
num_waves: int = len(waves)
final_wave: int = waves[-1]["wave"]     # last LABEL, not len()
...
is_final: bool = (wave == final_wave)
```

This preserves multi-wave behaviour (`[7,8]` -> wave 8 commits, wave 7 does
not) and changes nothing for intermediate waves, which already do not
commit.

Separate, related gap (not this bug): because non-final waves do not commit
by design, any run interrupted part-way leaves its work uncommitted. That is
a durability property of the one-commit-per-run model, not a defect in the
final-wave test, and closing it would mean committing per wave.

Related: none.

## Resolution status

Open. No fix merged. Reported by the Designer from the `newspipe` consumer
repo on 2026-10-19, where it surfaced as an uncommitted working tree after a
converged wave run.

## Fix summary (Resolved only)

Pending.

## Tests proving resolution (Resolved only)

Pending. Suggested: (1) table-driven unit test over wave labellings -
`[1,2,3]`, `[8]`, `[7,8]`, `[1,2,8]` - asserting exactly the last labelled
wave is `is_final`; (2) end-to-end `--dry-run` over a single corrective wave
numbered above `len(waves)`, asserting the emitted lead prompt contains the
commit instruction and not the "Do NOT commit" deferral; (3) a negative
guard asserting a mid-sequence wave still defers, so the fix cannot be
implemented by simply always committing.
