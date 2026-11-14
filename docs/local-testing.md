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
.\tools\test-local.ps1 test
```

```sh
python3 -m tools.local_tests
python3 -m tools.local_tests --workers 2 test schema-ci
```

The default runs `pre-pr` then `schema-ci`, with four workers. Test and
coverage gates retain separate main/web JUnit reports under `artifacts/tests/`
and show the 20 slowest tests. Override `PYTEST_ARGS` or `PYTEST_REPORT_DIR`
when invoking Make directly; both CI providers retain those reports. Every
invocation makes a fresh Linux-native snapshot of HEAD plus staged, unstaged
and untracked non-ignored files. Deletions are included; ignored files stay
excluded. This avoids testing a stale secondary clone or depending on Windows
mount timing. Do not edit the source during snapshot creation. The source Git
checkout is never reset or cleaned. Existing virtual environments are reused.

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