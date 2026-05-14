# PBS sample regression notes

The archived PBS traces from `fgeorgatos/qtop-test-repo/qtop5/results` can now be replayed with:

```sh
make test-pbs-samples PBS_SAMPLE_ROOT=../qtop-test-repo/qtop5/results PBS_SAMPLE_LIMIT=100
```

Use `PBS_SAMPLE_LIMIT=0` to render every available sample. The helper writes:

* `manifest.csv` with sample name, worker-node count, status and output path
* `rendered/*.ansi.txt` with the ANSI-coloured qtop render for each passing sample
* `runs/<sample>/` with the raw qtop run files

`make test` runs unit tests and, when `PBS_SAMPLE_ROOT` exists, also runs the PBS sample regression. This keeps the external archive optional for normal development while making the historical replay easy to wire into CI.

The PR evidence lives under `docs/pbs-sample-regression/`:

* `rendered-100/` contains 100 ANSI-coloured qtop renders
* `rendered-100-manifest.csv` lists the 100 samples provided as PR evidence
* `validation-all-manifest.csv` records the full local validation run, where all 447 archived PBS samples passed
