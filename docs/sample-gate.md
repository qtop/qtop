# qtop sample gate

This gate gives GitHub Actions and GitLab CI one shared command for fast scheduler regression checks:

```bash
make sample-validate SAMPLE_SCHEDULERS=slurm SAMPLE_MAX_FAILURES=0
```

The default CI gate renders the checked-in Slurm command traces from `tests/plugins/slurm_samples` into `artifacts/qtop-sample-gate/slurm`. It writes a `manifest.json` for successful renders, `failures.json` when failures are allowed, and a top-level `sample-gate.json` that records each scheduler run.

PBS traces stay optional because the large historical corpus lives outside this repository, matching `CONTRIBUTING.md` guidance to keep heavy artifacts out of `qtop`. When a checkout has that corpus next to this repository, use:

```bash
make sample-validate SAMPLE_SCHEDULERS=slurm,pbs PBS_SAMPLES_DIR=../qtop-test-repo/qtop5/results PBS_SAMPLE_LIMIT=10 SAMPLE_MAX_FAILURES=0
```

Failure policy:

- `SAMPLE_MAX_FAILURES=0` is the pull-request default.
- `SLURM_SAMPLE_LIMIT=0` means all local Slurm samples.
- `PBS_SAMPLE_LIMIT=10` validates the curated golden PBS set before filling the remaining limit.
- Missing PBS samples are skipped only by the shared wrapper, so ordinary CI can run without vendoring the external trace repository.

The same entry point is used by `.github/workflows/pytest.yml` and `.gitlab-ci.yml`, so the commands do not drift. The GitLab pipeline also includes an AlmaLinux 8 Python 3.6 style job because older clusters still need that runtime covered.

The SGE/OAR gate is intentionally not wired yet: this checkout has small parser fixtures but not a complete command-trace directory equivalent to the Slurm traces. The wrapper is structured so another scheduler can be added as soon as a reproducible fixture source is available.

This layout follows the same low-tech CI pattern used by projects such as `qpoint-io/qtap`, whose development docs expose `make ci` as the umbrella target for repository checks. The important part is the same here: keep the detailed checks in repository scripts, and make each CI provider call those scripts instead of copying command bodies into provider-specific YAML.
