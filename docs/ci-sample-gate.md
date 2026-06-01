# CI sample gate

The shared CI entry point is:

```sh
make ci
```

It runs the pytest suite and then validates the bundled PBS, OAR, and SGE sample scheduler outputs against their reference output files. The sample gate writes raw and filtered qtop output under `artifacts/sample-gate/` so reviewers can inspect the exact rendered output when a CI run fails.

The gate is intentionally small and fast. It uses the existing sample files in `qtop_py/contrib/`, strips known volatile lines such as working directory and log-file paths, and diffs the remaining output against the checked-in references.

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
