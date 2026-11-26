# LLMDD tool tests

Run every tool regression test with:

```sh
cd /home/kcat/Repos/llm-driven-design
python3 -m unittest discover -s .LLMDD/tests -v
```

`test_cu_boundary.py` proves the CU boundary contract: no-touch parsing,
segment-aware matching, symlink resolution, the precise write index,
per-CU attribution from the coder event log, untracked file coverage,
baseline subtraction, and the three enforcement rules.
