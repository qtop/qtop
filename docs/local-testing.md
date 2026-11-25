# Local tests on Linux and Windows

The full suite targets Linux x86-64 CPython 3.12. Native Windows is not a
supported full-suite environment: POSIX permissions, process groups and PTYs
need Linux semantics. Windows developers run the same checks through WSL.
POSIX-specific tests skip on native Windows; that does not certify the rest of
the repository for Windows. No Windows CI lane is claimed.

## First setup

Install WSL Ubuntu, then install its small OS toolchain:

```sh
sudo apt-get update
sudo apt-get install -y git make python3-venv golang-go nodejs
```

Go must be 1.26 or newer, or have Go's automatic toolchain download enabled.
Install [uv](https://docs.astral.sh/uv/) (or `pipx install nox`). Use a
Linux-native clone inside WSL, not the Windows checkout under `/mnt/c`:
Windows mounts lack the POSIX permissions and timing the suite checks.

## Daily use

From the checkout:

```sh
uvx nox
uvx nox -- test-fast
```

[noxfile.py](../noxfile.py) creates a Python 3.12 environment (uv fetches
Python 3.12 if it is missing), installs `requirements-ci.txt` and runs the
requested Make targets in one Make invocation; the default is `pre-pr` then
`schema-ci`. Later runs reuse the environment and re-check the pins; `-R` skips
that check, and `-N` rebuilds the environment. `test-fast` runs a
parser/model/UI subset for iteration; it does not replace the full gate. Test
and coverage gates retain separate main/web JUnit reports under
`artifacts/tests/` and show the 20 slowest tests; override `PYTEST_ARGS`,
`PYTEST_REPORT_DIR` or `PYTEST_XDIST_AUTO_NUM_WORKERS` in the environment.
Make runs in the checkout itself, so do not edit sources during a run.

To run without nox, in any Python 3.12 environment with `requirements-ci.txt`:

```sh
make pre-pr schema-ci PYTHON=python
```

Python 3.6/RHEL8 verification remains a separate CI lane. See [CI
structure](ci-structure.md) for legacy contracts, coverage and soak tests.

## When tests run and how many

For the pinned Linux x86-64 Python 3.12 environment at revision `1134990`
(0.9.20261115), with the full-run reports and collection checks reviewed on
2026-11-14:

| Target | Pytest cases per invocation | Coverage enforced |
|---|---:|---|
| `test-fast` | 664 parser/model/UI cases | No |
| `test` or `ci` | 1,569: 1,565 parallel, then 4 web cases serially | No |
| `coverage`, `coverage-xml` or `pre-pr` | Same 1,569 cases, once | 95% statements, 91% branches |
| `property-test` | 23 cases, already included in the full suite | No |
| `test-packaging` | 7 build-and-install cases that `test` and `coverage` skip; build jobs and nightly lanes run them | No |
| `nightly-properties` | Same 23 cases with the extended profile | No |

Counts describe this revision and installed optional tooling, not fixed quotas.
The observed pytest 9.2 JUnit summary counter also includes 29 unittest
subtests: `main.xml` reports 1,594 tests and contains 1,565 `<testcase>`
elements; `web.xml` adds four. The pytest total is therefore 1,569 cases, while the JUnit
summary counters add up to 1,598 outcomes. Do not treat those as extra collected
pytest cases.

Hypothesis uses up to 100 examples for each generated property normally and
1,000 in the nightly profile; examples are not extra pytest cases. `pre-pr`
also runs 13 scheduler samples and both 5,000-case fuzzers (seed 488), plus
lint and type checks. Those checks are separate from the pytest totals.

`nox` defaults to `pre-pr` followed by `schema-ci`: one full pytest run plus
the separate Go schema gate. Within one Make invocation, shared prerequisites
run once; for example,
`make pre-pr coverage-xml` reuses the coverage run. `make ci coverage` runs the
suite twice because `test` and `coverage` are distinct targets.

GitHub runs the full suite in both modern-Python build lanes on PRs targeting
`main`/`develop` and pushes to `main`, `develop` or `releases/**`. Its separate
coverage workflow reruns the full Python 3.12 suite on those PRs and pushes to
`main`/`develop`, or manual dispatch. Thus an ordinary PR has two Python 3.12
suite runs (plain and coverage) and one Python 3.15 run. Optional Atheris is
not installed on 3.15. Historical 0.9.20261113 measurements were 1,491 cases
with one skipped module on 3.15 and 1,468 cases with two skipped optional
modules on 3.10. Those historical counts are not current-revision results.

Publishing a GitHub release runs one full Python 3.12 suite through
`github-ci`, followed by artifact build/install checks. The manual pytest
workflow also runs one full Python 3.12 suite when dispatched.

GitLab's modern-Python job runs the plain suite; its coverage job runs it again
for merge requests, external PRs, `develop` and the default branch. Nightly
matrices run a suite per supported interpreter/distro lane; the separate
extended-property job runs once per nightly pipeline. Legacy lanes use the
stdlib compatibility and scheduler-sample checks rather than the full suite.
Always use the emitted pytest/JUnit counts for the exact run being reviewed.


Make owns coverage floors and the default worker count. The runner forwards an
explicit `--workers` override only when requested; otherwise Make uses its
default or the environment. Standalone `tools.check_coverage` calls must supply
`--statements` and `--branches`, as the Make targets already do.
