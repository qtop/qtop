# CI sample gate

`make sample-gate` is the shared fast validation entry point for local runs,
GitHub Actions, and GitLab CI. It renders scheduler command traces through qtop
and stores a JSON manifest plus rendered output under `artifacts/sample-gate`.

## Sample sources

- Slurm samples are bundled in `tests/plugins/slurm_samples`.
- PBS samples can be supplied from `../qtop-test-repo/qtop5/results` or by
  overriding `PBS_SAMPLES_DIR`.
- SGE is wired into the same gate through `SAMPLE_SCHEDULERS`, but the current
  repository does not bundle runnable SGE command traces.

## Limits and failure policy

The default gate validates up to six samples per scheduler and allows zero
failures:

```sh
make sample-gate
```

CI can tune the same command without duplicating shell logic:

```sh
make sample-gate SAMPLE_LIMIT=12 SAMPLE_MAX_FAILURES=0
```

Missing optional external sample directories are recorded as `skipped` in the
manifest. Render failures are recorded as `failed`; CI fails when failures exceed
`SAMPLE_MAX_FAILURES`.

## CI shape

GitHub Actions and GitLab CI both call the Makefile target. This keeps the CI
definition thin and makes the same check available to contributors before they
push. The local proof of concept is `tools/validate_samples.py`, which uses only
the Python standard library so it can run in early cluster-like environments.
