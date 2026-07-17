# Sample Gate Configuration

## Overview

The qtop CI pipeline uses **sample gates** to validate scheduler command output parsing across multiple backends. A sample gate runs qtop against committed sample outputs (sinfo, squeue, PBS node files, etc.) and verifies that the tool produces correct results without requiring a live HPC cluster.

## Supported Schedulers

The sample gate framework supports five scheduler backends:

| Scheduler | Identifier | Notes |
|-----------|-----------|-------|
| PBS/Torque | `pbs` | Parsed via `pbsnodes`-style output |
| Sun Grid Engine | `sge` | Parsed via `qhost -xml`-style output |
| SLURM | `slurm` | Parsed via `sinfo`/`squeue`-style output |
| OAR | `oar` | Parsed via `oarnodes`-style output |
| Demo | `demo` | Built-in demo data for CI testing |

## Configuration

Sample gate behaviour is controlled via environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `SAMPLE_GATE_SCHEDULERS` | `pbs,sge,slurm,oar,demo` | Comma-separated list of schedulers to validate |
| `SAMPLE_GATE_MAX_FAILURES` | `0` | Maximum acceptable per-sample failures before the gate fails |
| `SAMPLE_GATE_ARTIFACT_DIR` | `artifacts/sample-gate` | Directory where rendered output and logs are stored |

### Running Locally

```bash
# Run all sample gates with default settings
make sample-gate

# Run a subset of backends
SAMPLE_GATE_SCHEDULERS=slurm,demo make sample-gate

# Allow up to 1 failure before exiting
SAMPLE_GATE_MAX_FAILURES=1 make sample-gate

# Specify a custom artifact output directory
SAMPLE_GATE_ARTIFACT_DIR=tmp/my-artifacts make sample-gate
```

### CI Integration

Sample gate artifacts are published from both GitHub Actions and GitLab CI to the configured artifact directory. These artifacts include:

- Rendered text output for each scheduler backend
- ANSI-colour terminal output
- Machine-readable error/failure logs

Reviewers can inspect these artifacts to understand what qtop produces **before** running it locally.

## Sample Sources and Limits

### Committed Samples (CI-only)

The repository contains committed scheduler output samples that serve as the canonical validation corpus:

- **SLURM samples**: `tests/plugins/slurm_samples/` - multiple cluster configurations (basic, mixed, large_cluster, multi_partition)
- **PBS samples**: tested via `test_pbs_sample_regressions.py` against historical PBS corpus

### Archived PBS Samples (local/external)

Larger archived PBS sample sweeps require the external corpus:

```bash
# When the PBS sample corpus is available:
PBS_SAMPLES_DIR=/path/to/qtop-test-repo/qtop5/results PBS_SAMPLE_LIMIT=447 make test-pbs-samples
```

The default limit of 447 samples balances thorough coverage with CI time constraints.

### SLURM Sample Rendering

Committed SLURM samples are also rendered as terminal output for visual review:

```bash
SLURM_SAMPLES_DIR=tests/plugins/slurm_samples SLURM_OUTPUT_DIR=/tmp/qtop-slurm-rendered make test-slurm-samples
```

## Failure Policy

- By default (`SAMPLE_GATE_MAX_FAILURES=0`), **any** sample failure causes the gate to fail
- The gate runs all backends even when failures are encountered, collecting a full error report
- Diagnostic information is written to the artifact directory regardless of pass/fail status
- A per-backend summary shows pass/fail counts at the end of the run

## Artifacts Published

After a successful CI run, the following artifacts are available:

| Artifact Name | Contents | CI Platform |
|---------------|----------|-------------|
| `sample-gate-{python-version}` | Rendered output for all backends | GitHub Actions |
| `sample-gate-python3.6-almalinux8` | Python 3.6 compatibility gate output | GitHub Actions |
| `artifacts/sample-gate/` | Same as above | GitLab CI |
| `artifacts/sample-gate-py36/` | Python 3.6 gate output | GitLab CI |

## Reproducing CI Checks

To reproduce the exact CI validation path locally:

```bash
# Full CI pipeline (tests, sample gates, linting, formatting checks)
make ci

# CI plus explicit fortifications (eval checks, diff health, binary file detection)
make fortifications

# Python 3.6 compatibility gate
make compat-py36
```
