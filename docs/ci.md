# CI and Sample Gates

qtop keeps the default regression gate lightweight: Python unit tests plus
curated scheduler sample replays that render archived command traces.

## Fast local gate

Run the same gate used by GitHub Actions and GitLab CI:

```sh
make ci
```

This expands to:

```sh
make fortifications
make test
make test-contrib-samples
make test-slurm-samples
```

Rendered sample output is written under `build/qtop-contrib-rendered/` and
`build/qtop-slurm-rendered/`. The sample validators set qtop's runtime home
under the output directory, so logs and per-user config lookups stay inside the
build tree instead of depending on the contributor's `$HOME`.

The `fortifications` target is a lightweight repository-health check. It fails
when generated artifacts are tracked or when UTF-8 text files contain control or
bidirectional override characters that should not appear in source.

## Scheduler sample gates

The fast sample gate covers bundled PBS and SGE contrib snapshots plus bundled
Slurm command traces:

```sh
make sample-gate
```

Bundled PBS and SGE snapshots live in `qtop_py/contrib/`:

```sh
make test-contrib-samples
```

Slurm samples are stored in `tests/plugins/slurm_samples/`. Each sample is a
directory with both `sinfo.txt` and `squeue.txt`:

```sh
make test-slurm-samples
```

PBS replay coverage uses the external qtop sample repository:

```sh
make test-pbs-samples PBS_SAMPLES_DIR=/path/to/qtop-test-repo/qtop5/results
```

The default PBS gate renders the curated golden samples first, then continues
through the remaining sample directories until `PBS_SAMPLE_LIMIT` is reached.
All sample validators default to `MAX_FAILURES=0`, so any sample failure makes
the gate fail. For exploratory runs, override the threshold:

```sh
make sample-gate MAX_FAILURES=1
```

## Updating samples

When adding a scheduler sample:

1. Keep the input files small and anonymized.
2. Add or update a focused parser/unit test when the sample captures a specific
   edge case.
3. Run `make ci` before opening the pull request.
4. Include the command output and any rendered artifact details in the pull
   request description.

CI uploads rendered sample output as an artifact on every run, including
failures, so maintainers can inspect the last rendered state without storing
generated files in the repository.
