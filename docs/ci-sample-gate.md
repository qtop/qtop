# CI sample gate

`make ci-sample-gate` is the shared validation entry point for GitHub Actions,
GitLab CI, and local contributors. It runs two checks:

1. `make fortify` compares the branch with `origin/develop` and rejects added
   control or bidirectional Unicode characters plus unexpected binary or
   generated-looking file changes.
2. `make sample-gate` runs qtop against the bundled PBS, OAR, and SGE sample
   inputs under `qtop_py/contrib`.

The sample gate writes logs and rendered qtop output to
`artifacts/qtop-sample-gate/`. CI uploads that directory so reviewers can inspect
the exact command output before reproducing the run locally.

The failure threshold is explicit and defaults to zero:

```sh
make SAMPLE_GATE_MAX_FAILURES=0 ci-sample-gate
```

Schedulers can be narrowed while debugging:

```sh
make SAMPLE_GATE_SCHEDULERS=pbs sample-gate
```

GitHub Actions runs the shared gate on modern Python and on an AlmaLinux 8
container with Python 3.6. GitLab mirrors the same Makefile command and artifact
paths so failures mean the same thing in both systems.

The Makefile layout follows the common self-documenting target pattern used by
projects such as `kubernetes/minikube`, where CI jobs call stable project-owned
targets instead of duplicating long shell command sequences in each provider.
