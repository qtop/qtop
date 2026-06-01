# Sample CI Gate

`make test-samples` is the shared entry point for the lightweight scheduler
sample gate. GitHub Actions and GitLab CI both call this target so the two CI
systems do not drift.

`make fortify` is the shared pre-test review gate. It compares the current
branch against `FORTIFY_BASE` (`origin/develop` by default) and fails on:

- non-ASCII or control characters added by the contribution diff, including
  bidirectional text controls;
- new `eval()` calls added by the contribution diff;
- changed generated, compressed, binary, or fixture-like paths that should be
  reviewed manually instead of silently accepted by automation.

The gate always runs the built-in PBS, SGE, and OAR samples from
`qtop_py/contrib`, then runs the in-repository Slurm command-trace samples from
`tests/plugins/slurm_samples` and writes rendered output to `SLURM_OUTPUT_DIR`
(`/tmp/qtop-slurm-rendered` by default). When the external PBS archive is
available at `PBS_SAMPLES_DIR`, the same target also runs
`tools/validate_pbs_samples.py` with `PBS_SAMPLE_LIMIT` and writes output to
`PBS_OUTPUT_DIR` (`/tmp/qtop-pbs-rendered` by default).

PBS samples are intentionally optional in the default CI job because the large
historical trace archive is not vendored in this repository. Set
`SKIP_MISSING_PBS_SAMPLES=0` in a CI job that mounts the archive to make PBS
coverage mandatory.

Failure policy:

- `make fortify` runs before sample rendering in GitHub Actions and GitLab CI.
- `SAMPLE_MAX_FAILURES=0` is passed to the fast Slurm pytest gate.
- Built-in PBS, SGE, and OAR samples fail the job when qtop cannot render them
  or when they produce empty output.
- Slurm rendering fails the job when no valid samples are found or qtop cannot
  render a sample.
- PBS rendering fails the job when the archive is required and missing, when a
  golden sample is missing, or when fewer than `PBS_SAMPLE_LIMIT` samples render.
- Rendered outputs are uploaded as CI artifacts instead of being committed to
  this repository, keeping the repo aligned with `CONTRIBUTING.md`.

The structure mirrors a common CI pattern used by projects that keep one local
command as the contract and call it from more than one CI system. For example,
Ruff keeps contributor-facing commands in its repository automation and invokes
them from GitHub workflows rather than duplicating all logic in workflow YAML.
