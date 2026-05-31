# CI sample gate

This note documents the shared CI entry point added for challenge #433.

## Entry points

- `make ci` is the command called by both GitHub Actions and GitLab CI.
- `make fortify` checks the current diff against `origin/develop` for unusual control or non-ASCII characters and unexpected generated/binary files.
- `make test` runs the unit test suite.
- `make sample-gate` runs the fast in-repository scheduler sample gate.
- `make test-slurm-samples` validates bundled Slurm command traces and writes rendered output to `SLURM_OUTPUT_DIR`.
- `make test-pbs-samples` remains the larger archived PBS sweep and expects an external `qtop-test-repo` checkout, so it is not part of the default CI gate.

## Sample source and failure policy

The default CI sample gate uses the checked-in Slurm command traces under `tests/plugins/slurm_samples`.
Each sample directory must include both `squeue.txt` and `sinfo.txt`.

The gate has a zero-failure policy: if pytest fails, a Slurm sample is missing, qtop cannot render a sample, or the renderer exits with a non-zero status, `make ci` fails.

GitHub Actions uploads the rendered Slurm output from `artifacts/qtop-slurm-rendered`.
GitLab CI uses the same command and publishes the same artifact path, so a reviewer sees the same failure surface in both systems.

## External structure precedent

The structure follows the common OSS pattern used by projects such as `tox`: define one local command that developers and CI both run, then keep platform-specific CI files as thin wrappers around that command. In this repository the command is `make ci`, which keeps GitHub and GitLab from drifting.
