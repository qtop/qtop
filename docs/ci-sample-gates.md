# CI sample gates

Challenge #433 asks for one reproducible validation path that local users,
GitHub Actions, and GitLab CI can all run without drifting. The shared entry
point is:

```bash
make ci
```

This target runs unit tests, the fast committed scheduler sample gate, and the
fortification checks. GitHub Actions and GitLab CI both call the same Makefile
target.

CI-only Python dependencies are pinned centrally in `requirements-ci.txt`.
Provider YAML files call `make ci-deps` instead of carrying separate `pip
install` command lists.

## Fast scheduler gate

```bash
make sample-gate
```

The gate uses committed sample sources only, so it is suitable for every pull
request:

| Scheduler | Source | Limit | Policy |
| --- | --- | --- | --- |
| PBS | `qtop_py/contrib` | One committed fixture set from `pbsnodes_a.txt`, `qstat.txt`, and `qstat_q.txt`. | Render qtop and require stable summary/section markers from the committed PBS sample. |
| SGE | `qtop_py/contrib` | One committed fixture set from `qstat.F.xml.stdout`. | Render qtop and require stable summary/section markers from the committed SGE sample. |
| SLURM | `tests/plugins/slurm_samples` | Every committed Slurm command-trace sample in that directory. | Render every committed Slurm command-trace sample and require qtop to exit successfully. |

The default failure threshold is strict:

```bash
make sample-gate SAMPLE_GATE_MAX_FAILURES=0
```

For triage runs, reviewers can temporarily allow a limited number of failures:

```bash
make sample-gate SAMPLE_GATE_MAX_FAILURES=1
```

Rendered output, stderr logs, command lines, and SVG text screenshots are
written under `artifacts/sample-gate/`. CI uploads that directory even when the
gate fails, so reviewers can inspect expected output before rerunning locally.
The default CI path does not depend on external sample repositories or network
fetches.

## Python 3.6 / AlmaLinux 8

Clusters still running older RHEL-like environments can use the dependency-light
compatibility gate:

```bash
make compat-py36 PYTHON=python3.6
```

The GitHub and GitLab AlmaLinux 8 jobs install Python 3.6 when available, run a
source compile check, then run the same sample gate. They do not require pytest.

## Fortifications

```bash
make fortifications
```

The fortification target checks the pull-request diff for control or bidi
characters, generated or binary-looking paths that require manual review, and
Python `eval()` call sites. The GitHub workflow also pins third-party actions
to full commit SHAs and uses `permissions: contents: read`.

The older OAR fixture is intentionally left out of this PR gate because #433
asks for PBS/SGE/SLURM and the current `develop` command-line restrictions
reject that legacy anonymized sample command before it reaches rendering.

## Coverage roadmap

Coverage should stay behind the same Makefile abstraction when it is enabled:
add a pinned coverage dependency to `requirements-ci.txt`, add a `coverage`
target that composes the existing `test` target, then switch the desired
provider entry point from `make github-ci`/`make gitlab-ci` to the coverage
target or a `ci` composition that includes it. That keeps coverage behavior
local-first and prevents GitHub/GitLab YAML drift.

## Independent structure reference

The structure mirrors the common OSS pattern of keeping CI providers thin and
delegating real behavior to repository-owned Makefile targets. One small public
proof-of-concept of the same shape is:

https://github.com/nicolatrozzi/qtop-ci-gate-poc

That repository demonstrates the provider-independent pattern only; qtop keeps
the real scheduler-specific policy in this repository.
