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

Go must be 1.25 or newer, or have Go's automatic toolchain download enabled.
From a Windows PowerShell checkout:

```powershell
.\tools\test-local.ps1 --bootstrap
```

From Linux or a WSL shell in the source checkout:

```sh
python3 -m tools.local_tests --bootstrap
```

Bootstrap installs uv 0.13.8 into a separate tool environment, obtains managed
Python 3.12, then installs `requirements-ci.txt` into `~/.venvs/qtop-py312`. It
requires network access. No project runtime dependencies are added. Re-run
`--bootstrap` after CI pins change. It does not change shell profiles.

## Daily use

```powershell
.\tools\test-local.ps1
.\tools\test-local.ps1 test-fast
```

```sh
python3 -m tools.local_tests
python3 -m tools.local_tests test-fast
```

The default runs `pre-pr` then `schema-ci`, with four workers. `test-fast`
runs a parser/model/UI subset for iteration; it does not replace the full
gate. Test and coverage gates retain separate main/web JUnit reports under
`artifacts/tests/` and show the 20 slowest tests. Override `PYTEST_ARGS` or
`PYTEST_REPORT_DIR` when invoking Make directly; both CI providers retain
those reports. Every invocation makes a fresh Linux-native snapshot of HEAD
plus staged, unstaged and untracked non-ignored files. Deletions are
included; ignored files stay excluded. This avoids testing a stale secondary
clone or depending on Windows mount timing. Do not edit the source during
snapshot creation. The source Git checkout is never reset or cleaned.
Existing virtual environments are reused.

A unique `~/.local/state/qtop-test-reports/run-*` directory retains reports and
a manifest with revision, working-tree digest/status, target list and baseline
SHA. The Linux temporary copy is removed after the run. Use `--reports` to
choose another location. On Windows mounts without WSL metadata, private
report-directory permissions cannot be enforced; enable metadata or use the
default Linux-native report directory. The runner fails explicitly instead of
weakening private output checks.

To run directly from an existing Linux-native checkout:

```sh
source ~/.venvs/qtop-py312/bin/activate
export PYTEST_XDIST_AUTO_NUM_WORKERS=4
make pre-pr schema-ci
```

Python 3.6/RHEL8 verification remains a separate CI lane. See [CI
structure](ci-structure.md) for legacy contracts, coverage and soak tests.

## When tests run and how many

For the pinned Linux x86-64 Python 3.12 environment at 0.9.20261113:

| Target | Pytest cases per invocation | Coverage enforced |
|---|---:|---|
| `test-fast` | 638 parser/model/UI cases | No |
| `test` or `ci` | 1,504: 1,500 parallel, then 4 web cases serially | No |
| `coverage`, `coverage-xml` or `pre-pr` | Same 1,504 cases, once | 95% statements, 91% branches |
| `property-test` | 23 cases, already included in the full suite | No |
| `nightly-properties` | Same 23 cases with the extended profile | No |

Counts describe this revision and installed optional tooling, not fixed quotas.
The observed pytest 9.2 JUnit summary counter also includes 29 unittest
subtests: `main.xml` says 1,529 tests but serializes 1,500 `<testcase>` elements;
`web.xml` adds four. The pytest total is therefore 1,504 cases, while the JUnit
summary counters add up to 1,533 outcomes. Do not treat those as extra collected
pytest cases.

Hypothesis uses up to 100 examples for each generated property normally and
1,000 in the nightly profile; examples are not extra pytest cases. `pre-pr`
also runs 13 scheduler samples and both 5,000-case fuzzers (seed 488), plus
lint and type checks. Those checks are separate from the pytest totals.

The Windows launcher and `python3 -m tools.local_tests` default to `pre-pr`
followed by `schema-ci`: one full pytest run plus the separate Go schema gate.
Passing multiple targets to the launcher starts a separate Make invocation for
each, so requesting both `pre-pr` and `coverage-xml` repeats the full suite.
Within one Make invocation, shared prerequisites run once; for example,
`make pre-pr coverage-xml` reuses the coverage run. `make ci coverage` runs the
suite twice because `test` and `coverage` are distinct targets.

GitHub runs the full suite in both modern-Python build lanes on PRs targeting
`main`/`develop` and pushes to `main`, `develop` or `releases/**`. Its separate
coverage workflow reruns the full Python 3.12 suite on those PRs and pushes to
`main`/`develop`, or manual dispatch. Thus an ordinary PR has two Python 3.12
suite runs (plain and coverage) and one Python 3.15 run. Optional Atheris is
not installed on 3.15, so that lane currently passes 1,491 cases and skips one
module. Python 3.10 passes 1,468 cases and skips two optional-tooling modules.

Publishing a GitHub release runs one full Python 3.12 suite through
`github-ci`, followed by artifact build/install checks. The manual pytest
workflow also runs one full Python 3.12 suite when dispatched.

GitLab's modern-Python job runs the plain suite; its coverage job runs it again
for merge requests, external PRs, `develop` and the default branch. Nightly
matrices run a suite per supported interpreter/distro lane; the separate
extended-property job runs once per nightly pipeline. Legacy lanes use the
stdlib compatibility and scheduler-sample checks rather than the full suite.
Always use the emitted pytest/JUnit counts for the exact run being reviewed.
