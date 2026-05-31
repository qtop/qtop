# CI sample gates

Challenge #6 asks qtop to run the same validation path locally and in CI without
vendoring large scheduler trace archives into this repository. The shared
entrypoint is:

```bash
make ci
```

That target runs unit tests, all available scheduler sample gates, and the
lightweight source hardening check. GitHub Actions and GitLab CI both call the
same target so failures mean the same thing in either runner.

## PBS samples

PBS historical traces live outside this repository. Point `PBS_SAMPLES_DIR` at a
checkout of the trace archive and run:

```bash
make test-pbs-samples PBS_SAMPLES_DIR=../qtop-test-repo/qtop5/results PBS_SAMPLE_LIMIT=100
```

`tools/validate_pbs_samples.py` writes rendered `.ans` files plus
`manifest.json` and `failures.json` under `sample-artifacts/pbs-rendered` by
default. The first entries are the curated golden samples referenced by the PBS
regression notes, then the helper fills the remaining limit from the archive.

CI uses `make test-pbs-samples-if-present`, which skips only when the external
archive is absent. Jobs with the archive mounted should call
`make test-pbs-samples` directly so missing samples fail fast.

## Slurm samples

Bundled Slurm command-trace samples are kept small enough for the main
repository:

```bash
make test-slurm-samples
```

The validator renders each sample into `sample-artifacts/slurm-rendered` and
writes `failures.json`. `MAX_FAILURES=0` is the default, so any sample regression
fails the gate while still leaving artifacts for review.

## Adding samples

Add compact scheduler traces under the matching `tests/plugins/*_samples`
directory when they are small, self-contained, and safe to keep in git. Large or
historical captures should stay in the external artifact repository and be
referenced from documentation instead.

For a new sample gate:

1. Add a validator under `tools/` that writes rendered output and
   `failures.json`.
2. Add a Makefile target that accepts `MAX_FAILURES` and output directory
   variables.
3. Wire the target into `make test-samples`.
4. Confirm GitHub and GitLab still call only `make ci`.

## Interpreting failures

The CI artifact bundle is the first place to look. `manifest.json` lists samples
that rendered successfully; `failures.json` lists the samples that failed and
their captured error text. Re-run the failing target locally with the same
environment variables before changing parser behavior.
