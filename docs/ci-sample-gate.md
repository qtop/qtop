# CI sample gate

The shared CI entry point is:

```sh
make ci
```

It runs the pytest suite and then validates that the bundled PBS, OAR, and SGE sample scheduler outputs still render the expected qtop report sections. The sample gate writes raw output, filtered output, expected historical output, stderr, and optional diffs under `artifacts/sample-gate/` so reviewers can inspect rendering drift without making CI depend on byte-for-byte terminal output.

The gate is intentionally small and fast. It uses the existing sample files in `qtop_py/contrib/`, requires the stable report headings to render, strips known volatile lines such as working directory and log-file paths, and stores a diff against the checked-in references when the rendered terminal output drifts.

Useful commands:

```sh
make test
make sample-gate
make clean-artifacts
```

Configuration:

```sh
PYTHON=python3.10 make ci
MAX_FAILURES=1 make sample-gate
ARTIFACT_DIR=/tmp/qtop-artifacts make sample-gate
```

GitHub Actions and GitLab CI both call `make ci`, so future sample or coverage changes should be added behind that Makefile target to avoid CI drift.
