# CI sample gate

Issue #433 asks GitHub and GitLab to avoid drifting on sample validation. The shared entry point is:

```bash
make ci
```

For a faster local signal while iterating:

```bash
make sample-gate MAX_FAILURES=0
```

The sample gate uses the committed fixtures in `qtop_py/contrib` and runs focused PBS parser tests:

- `tests/plugins/test_pbs.py`

The committed sample inventory currently covers PBS, OAR, and SGE-style fixtures. SLURM is documented in `ROADMAP.rst`,
so this gate does not invent an unverified SLURM fixture.

Artifacts are written to `artifacts/qtop-sample-gate/`:

- `summary.txt`
- `pytest.log`

The failure policy is explicit: with `MAX_FAILURES=0`, missing sample files or failing focused tests fail the job.

The `fortifications` target also runs in `make ci`. It checks for unexpected generated/binary file changes, unusual Unicode/control characters in changed text files, and reports the current `eval(` inventory. Existing `eval(` usage is reported but does not fail by default; set `FORTIFY_FAIL_ON_EVAL=1` when the project is ready to make that check strict.
