# Scheduler sample gate

The fast scheduler sample gate gives local development, GitHub Actions, and
GitLab CI one shared entry point:

```bash
make sample-gate SAMPLE_GATE_MAX_FAILURES=0
```

The gate runs:

1. bundled PBS parser unit fixtures;
2. the bundled SGE XML command-trace fixture;
3. bundled Slurm parser fixtures;
4. rendered output checks for bundled Slurm command traces.

Logs, rendered Slurm output, and `manifest.json` are written under
`artifacts/sample-gate/`. Hosted CI uploads that directory so reviewers can
inspect output before rerunning locally.

The qtop CLI renderer depends on POSIX `SIGPIPE`. On Windows, the shared gate
records the Slurm render step as skipped while still running parser fixtures.
The Linux CI jobs always execute the render step.

## Failure policy

`SAMPLE_GATE_MAX_FAILURES=0` makes every failed step fail the gate. The
underlying Python entry point also accepts `--max-failures 0`, which is useful
outside Make:

```bash
python3 tools/sample_gate.py --output artifacts/sample-gate --max-failures 0
```

## Archived PBS corpus

The large PBS corpus stays outside this lightweight repository. When it is
available locally, include a curated archived render sweep:

```bash
python3 tools/sample_gate.py \
  --output artifacts/sample-gate \
  --max-failures 0 \
  --pbs-samples ../qtop-test-repo/qtop5/results \
  --pbs-limit 10
```

The external sample source is `fgeorgatos/qtop-test-repo/qtop5/results`.
`--pbs-limit 10` caps the archived sweep, while the bundled fixtures always run.

## Fortifications

The shared Makefile also exposes a lightweight changed-file check:

```bash
make fortifications
```

It rejects bidirectional Unicode control characters and requests manual review
for generated, compressed, or binary-looking changed paths. In an extracted
archive without git metadata, pass paths explicitly:

```bash
python3 tools/fortifications.py tools/sample_gate.py docs/sample-gate.md
```

## Scope

This change intentionally does not remove the remaining inherited `eval`
calls. Replacing those calls requires separate behavior-focused tests and
review. Keeping that refactor separate makes this CI contribution easier to
validate and backport.

## Generic structure

The reusable pattern is intentionally small:

1. one dependency-light Python orchestrator;
2. one Makefile target used locally and by both hosted CI systems;
3. a machine-readable manifest plus plain-text logs;
4. optional larger external corpora that do not burden ordinary PR checks.

This structure can be copied into another Python OSS project without requiring
qtop-specific CI plugins.
