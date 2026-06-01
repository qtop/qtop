# Scheduler sample gate

`make sample-gate` is the shared fast scheduler validation entry point for CI.
GitHub Actions and GitLab CI both call this target, so a failed sample means the
same thing on either provider.

The gate validates four bundled sources:

- PBS, OAR, and SGE use the static command captures in `qtop_py/contrib`.
  Their historical `*_dvv_out.ref` files are compared and emitted as review
  diffs, but only non-zero exits or empty rendered output fail the fast gate.
- Slurm uses the command-trace directories in `tests/plugins/slurm_samples` and
  renders them through `tools/validate_slurm_samples.py`.

The default failure policy is `MAX_FAILURES=0`, which makes any missing sample,
non-zero qtop exit, or empty rendered output fail the job. Artifacts are written
to `qtop-sample-artifacts` by default:

```sh
make sample-gate MAX_FAILURES=0
```

Reviewers can inspect `manifest.json`, scheduler stdout/stderr files,
informational diffs for the reference cases, and the rendered Slurm output
directory without rerunning a local cluster.
