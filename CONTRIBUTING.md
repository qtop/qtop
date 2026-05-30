# Contributing to qtop

Please follow common conventions for Open Source projects; see the Electron framework guidelines as a reference:

- For source code contributions, either a Developer Certificate of Origin (DCO) [1][2] or a Contributor License Agreement (CLA) [3] is acceptable.
- For bug reports, consult [4] and use your best judgement.
- For improvements or fixes, open a new issue or comment on a relevant open issue.

You may contribute in the following ways:

* Write code — follow [Conventional Commits][5] or the [Electron PR guidelines][6]
* Review pull requests
* Maintain and improve the qtop website or documentation
* Help with outreach and onboard new contributors
* Write or lead collaboration proposals, including grants or other community efforts

---

## Development workflow

### Requirements

* Python ≥ 3.9 (tested up to 3.12; EL8/AlmaLinux 8 is covered via `python3.9`)
* `make`, `ruff`, `pytest`, `pytest-cov`, `build`

Install development dependencies:

```sh
pip install pytest pytest-cov ruff build
```

### Makefile targets

Run `make` (no arguments) to list all available targets:

| Target            | Description                                              |
|-------------------|----------------------------------------------------------|
| `make test`       | Run unit tests with pytest                               |
| `make coverage`   | Run tests and emit XML + terminal coverage report        |
| `make lint`       | Lint with ruff                                           |
| `make lint-fix`   | Lint and auto-fix with ruff                              |
| `make format-check` | Check formatting with ruff format                      |
| `make format-fix` | Apply ruff formatting                                    |
| `make sample-validate` | Run PBS/OAR/SGE scheduler sample gate           |
| `make fortify`    | Run security and codebase health checks                  |
| `make dist`       | Build sdist + wheel                                      |
| `make version`    | Print current version                                    |
| `make release`    | Interactive: confirm → lint → test → dist                |
| `make ci`         | Full CI gate (lint + test + coverage + sample + fortify) |

### Sample validation gate

`make sample-validate` runs qtop over the reference input files in
`qtop_py/contrib/` (PBS, OAR, SGE) and diffs the output against stored
reference files (`*_dvv_out.ref`).  Any diff exits non-zero.

**Failure policy**: `MAX_FAILURES=0` — zero acceptable differences.
Override with `make sample-validate MAX_FAILURES=1` during local development
only; CI always uses `MAX_FAILURES=0`.

**Sample source**: `qtop_py/contrib/` — anonymised, static scheduler outputs
captured from real clusters.  Do not replace them without updating the
corresponding `.ref` files.

**Reproducing locally**:

```sh
pip install -e .
make sample-validate
# Artifacts written to sample-artifacts/sample-run.log
```

### Fortifications (`make fortify`)

`scripts/fortify.sh` checks:

1. No weird unicode / control characters introduced in the diff vs `origin/main`
2. No bidirectional text markers (Trojan Source attack vectors) in source files
3. Active `eval()` call count (informational; lambda-based ones are documented below)
4. No compiled Python artefacts (`.pyc`, `__pycache__`) tracked in git
5. No trailing whitespace

This check runs in both GitHub Actions and GitLab CI on every PR.

### `eval()` status

The following `eval()` calls have been replaced with `ast.literal_eval` or
direct type coercion as of this branch:

* `yaml_parser.py` — YAML list / quoted-string parsing
* `qtop.py` — config boolean fields (`transpose_wn_matrices`, etc.)
* `qtop.py` — CLI `--option` boolean values
* `plugins/pbs.py`, `plugins/sge.py` — integer job counts

Two `eval()` calls remain and are tracked with `# TODO` comments:

* `qtop.py` — user-defined `lambda` in `remapping` config key
* `qtop.py` — user-defined `lambda` in `sorting.user_sort` config key

These require a callable-registry refactor and are out of scope for the
current CI/CD improvement cycle.  A contributor picking up that work should
replace them with a fixed set of named sort/remap functions.

---

## CI/CD architecture

Both GitHub Actions and GitLab CI call the **same Makefile targets**, so a
failure means the same thing on both platforms:

```
GitHub Actions (.github/workflows/pytest.yml)  ──┐
                                                   ├─► make test / make sample-validate / make fortify
GitLab CI      (.gitlab-ci.yml)               ──┘
```

This pattern is validated by large OSS projects that maintain multi-platform
CI, for example **GitLab CE/EE** itself
(`gitlab.com/gitlab-org/gitlab-foss`), which delegates build steps to
Makefile targets so GitHub mirrors and fork CI stay in sync.

### GitHub Actions

* All action references are pinned to **full commit SHAs** to prevent
  supply-chain substitution attacks.
* Top-level `permissions: contents: read` limits the token scope.
* Python matrix: 3.9, 3.10, 3.11, 3.12.
* Separate AlmaLinux 8 container job (`almalinux:8` + `python3.9`) covers EL8
  compatibility.
* Sample-run log uploaded as a workflow artifact so reviewers can inspect the
  expected scheduler output without running qtop locally.

### GitLab CI

* Mirrors the same stages: `lint → test → sample-validate → fortify → dist`.
* Uses the same container images as GitHub Actions.
* Coverage report uploaded as a Cobertura artifact for inline MR display.
* Sample-run artifact uploaded with `when: always` so it is visible even on
  failure.

---

## Screenshots / demo output

The animated demo GIF at `qtop_py/contrib/qtop_demo.gif` shows expected
rendered output for the bundled sample data.  CI uploads `sample-run.log`
as an artifact on every run so reviewers can verify output without a cluster.

---

[1]: https://wiki.linuxfoundation.org/dco
[2]: https://developercertificate.org/
[3]: https://en.wikipedia.org/wiki/Contributor_License_Agreement
[4]: https://contributing.md/
[5]: https://www.conventionalcommits.org/en/v1.0.0/
[6]: https://www.electronjs.org/docs/latest/development/pull-requests#step-5-commit
