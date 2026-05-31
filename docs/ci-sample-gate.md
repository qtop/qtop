# CI sample gate

The shared CI entry point is:

```bash
make ci
```

GitHub Actions and GitLab CI both call this Makefile target so local, GitHub, and GitLab validation do not drift.

## Sample source

The fast PR gate uses only repository-local fixtures:

- PBS: `qtop_py/contrib/pbsnodes_a.txt`, `qtop_py/contrib/qstat.txt`, and `qtop_py/contrib/qstat_q.txt`
- OAR: `qtop_py/contrib/oarnodes*.txt` and `qtop_py/contrib/oarstat.txt`
- SGE: `qtop_py/contrib/qstat.F.xml.stdout`
- Slurm: `tests/plugins/slurm_samples/*`

The larger archived PBS sweep remains available separately through:

```bash
make test-pbs-samples PBS_SAMPLES_DIR=../qtop-test-repo/qtop5/results PBS_SAMPLE_LIMIT=100
```

## Limits and failure policy

The PR gate is intentionally small enough to run on every pull request. It renders bundled PBS, OAR, SGE, and Slurm samples, checks that qtop produces the expected report sections, writes rendered outputs and logs to `artifacts/sample-gate/`, and records `manifest.json`.

The default policy is zero tolerated failures:

```bash
make sample-gate SAMPLE_GATE_MAX_FAILURES=0
```

Use a non-zero `SAMPLE_GATE_MAX_FAILURES` only while debugging locally.

## Compatibility

GitHub and GitLab run the same `make ci` command on a modern Python image and on an AlmaLinux 8 / Python 3.6 job. The AlmaLinux job keeps the legacy cluster runtime visible while the modern job covers the active development environment.

## Independent structure reference

The provider-neutral Makefile-as-CI-entrypoint pattern is also used by open-source CI helper projects such as `w5s/makefile-ci` (https://github.com/w5s/makefile-ci), which documents the same idea of one local Makefile workflow reused by remote CI systems.
