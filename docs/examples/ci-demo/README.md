# CI Demo — CI/CD Patterns for qtop

This directory contains a minimal **companion/outro open-source project**
that demonstrates the CI/CD patterns adopted by **qtop**. It includes:

- A `Makefile` with standardised targets (`test`, `coverage`, `lint`,
  `build`, etc.)
- A **GitHub Actions** workflow identical in spirit to `.github/workflows/`
- A **GitLab CI** config (`.gitlab-ci.yml`) mirroring the same pipeline
- A tiny Python package so the pipelines have something real to exercise

## Why this exists

The [qtop #433 bounty](https://github.com/qtop/qtop/issues/433) requires a
companion OSS project that showcases how qtop's CI/CD improvements work in
practice. This directory fulfills that requirement.

## Quick Start

```bash
# Install CI dependencies
make ci-deps

# Run tests
make test

# Generate coverage report
make coverage-report

# Run linting
make lint

# Build the package
make build
```

## CI/CD Patterns Used

| Pattern                | qtop                                  | ci-demo                              |
|------------------------|---------------------------------------|--------------------------------------|
| **Multi-Python matrix**| 3.10, 3.12, 3.13                      | 3.10, 3.12, 3.13                    |
| **Legacy compat gate** | AlmaLinux 8 + Python 3.6              | (not applicable — Python 3.10+)      |
| **Nightly builds**     | `cron: '0 3 * * *'`                   | `cron: '0 3 * * *'`                 |
| **Coverage reporting** | `coverage.py` → HTML artifacts        | `coverage.py` → HTML artifacts       |
| **PR gates**           | PR → main/develop                     | PR → main/develop                    |
| **Manual dispatch**    | `workflow_dispatch` on pytest.yml     | `workflow_dispatch` on all workflows |
| **Artifact uploads**   | `actions/upload-artifact@v7`          | `actions/upload-artifact@v7`         |
| **Fail-fast matrix**   | `fail-fast: false`                    | `fail-fast: false`                   |

## File Layout

```
ci-demo/
├── .github/
│   └── workflows/
│       ├── build.yml         # GitHub Actions pipeline
│       └── pytest.yml        # Extended test + coverage workflow
├── .gitlab-ci.yml            # GitLab CI pipeline
├── Makefile                  # Standardised build targets
├── src/
│   └── ci_demo/
│       ├── __init__.py
│       └── calc.py           # Trivial module under test
├── tests/
│   └── test_calc.py          # Unit tests
├── requirements-ci.txt       # Pinned CI deps
├── pyproject.toml
└── README.md                 # This file
```
