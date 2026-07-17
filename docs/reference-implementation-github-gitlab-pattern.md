# Reference Implementation: Cross-Platform CI Pattern for GitHub & GitLab

## Overview

This document describes a reusable CI/CD pattern that ensures **GitHub Actions** and **GitLab CI** remain in sync while running the **same validation targets**. The pattern is implemented in qtop but can be adapted to any Python project.

## The Pattern

### 1. Shared Makefile Targets

Both CI platforms call platform-specific Makefile aliases that delegate to a shared validation target:

```makefile
ci: test backend-validation lint ruff-check format-check ## Shared local/CI validation path

github-ci: ci ## GitHub Actions entry point
gitlab-ci: ci ## GitLab CI entry point
```

**Benefits:**
- Single source of truth for what "passing CI" means
- Platform-specific configuration lives in the respective YAML files
- Developers can run `make ci` locally and get the same result

### 2. Containerized Compatibility Gate

For HPC environments that still use legacy operating systems:

```yaml
# .gitlab-ci.yml
almalinux-python36:
  image: almalinux:8
  script:
    - dnf -y install python36 make findutils
    - make compat-py36 PYTHON=python3.6
```

This pattern ensures that Python 3.6 compatibility is actively tested rather than assumed.

### 3. Shared Artifact Paths

Both CI platforms use identical artifact paths:

- `artifacts/sample-gate/` — sample validation output
- `artifacts/sample-gate-py36/` — Python 3.6 compatibility output
- `dist/` — built package distributions

**Why it matters:** Identical paths mean that CI migration or parallel runs produce comparable results, and local `make` invocations produce files in the same locations CI would.

### 4. Explicit Fortification Step

```makefile
fortifications: ## Check diff health and reject eval() call sites
    $(PYTHON) tools/fortifications.py --base-ref $(FORTIFY_BASE_REF)
```

Both CI YAMLs run this as an explicit step before the shared `ci` target, ensuring that:
- No `eval()` calls are introduced
- No control/bidi Unicode characters appear in diffs
- No generated/binary files are accidentally committed
- Diff health is checked against the target branch

### 5. Permission Hardening

```yaml
permissions:
  contents: read
```

GitHub Actions are scoped to read-only contents by default, following the principle of least privilege.

### 6. Action Version Pinning

All third-party GitHub Actions are pinned to full commit SHAs:

```yaml
- uses: actions/checkout@9f698171ed81b15d1823a05fc7211befd50c8ae0 # v6.0.3
- uses: actions/setup-python@a309ff8b426b58ec0e2a45f0f869d46889d02405 # v6.2.0
```

Pinning to SHAs prevents supply-chain attacks from tag mutability while the comment preserves human readability.

## Alternative Implementations

This pattern is generic enough to apply to any Python project. For example:

- **PoC repo**: A small project could use the same `Makefile` + dual-CI structure
- **scikit-learn**: Uses a similar `Makefile`-driven CI pattern with `make test`, `make lint`, etc.
- **Apache Airflow**: Uses shared CI scripts (`ci/`) called from both GitHub Actions and self-hosted CI

The key insight is that **the Makefile is the contract** — if both CI platforms call it, they stay in sync.
