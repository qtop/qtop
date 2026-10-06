## AI-assisted contributions

AI tools are welcome when they are used constructively for software quality,
documentation, testing, portability, maintainability, or user experience.
The contributor remains responsible for the result and must be able to explain
the change and validate it. Mass-generated, low-understanding, and autonomous
bot-driven pull requests may be rejected.

If AI materially contributed to a change, disclose the tool and version in the
pull request description. A short, factual disclosure is enough.

## Before opening a pull request

- Use `develop` as both the source and target branch.
- Keep the runtime dependency footprint small. qtop is used on HPC systems
  where internet access, extra packages, and administrator-managed Python
  stacks may not be available. Keep CI and developer-only dependencies pinned
  and out of the runtime path.
- For a bug report, include a concise reproduction, expected behavior, actual
  behavior, and relevant environment details.
- For a feature or fix, open an issue first or explain in an existing issue
  why the change belongs there.
- Follow the project's DCO requirement for source changes; see [1] and [2].
  The DCO is currently enforced for qtop contributions.
- Keep generated artifacts out of this repository. Put review artifacts in the
  `qtop-artifacts` repository instead.

When a change affects runtime behavior, include evidence that it still works
on a Python 3.6/RHEL 8-compatible environment or explain the closest
available equivalent. For documentation-only changes, run the relevant
formatting or link checks and state which runtime checks do not apply.

## Validation and review evidence

Before requesting review, run the checks that cover the change. For a normal
source change, the usual starting points are:

```text
python -m pytest -q
make sample-gate SAMPLE_GATE_SCHEDULERS=pbs,sge,slurm,oar,demo SAMPLE_GATE_MAX_FAILURES=0
```

Also demonstrate a coloured demo run when the change affects rendering or
terminal behavior:

```text
./qtop -b demo -FGTw
```

Attach concise output or a screenshot to the pull request when it helps the
reviewer verify the result. Do not claim checks that were not run; record
platform limitations and failures clearly.

## Contribution paths

You can contribute by:

- implementing a focused fix or feature;
- adding regression tests, documentation, or portability improvements;
- reviewing pull requests and reporting actionable feedback;
- maintaining qtop repositories and helping new contributors understand the
  codebase; or
- preparing collaboration, grant, fundraising, or community proposals.

## Proof of humanity

Contributors should be ready to explain their changes and, when requested,
demonstrate the relevant behavior in a live run. Optional profile markers may
be used only when they are already verifiable from the contributor's GitHub
profile; they are not a substitute for tests, reviewable code, or a clear
explanation. Never send personal identity documents or other sensitive data as
proof.

[1] https://wiki.linuxfoundation.org/dco

[2] https://developercertificate.org/

[3] https://en.wikipedia.org/wiki/Contributor_License_Agreement

[4] https://www.conventionalcommits.org/en/v1.0.0/ or https://www.electronjs.org/docs/latest/development/pull-requests#step-5-commit
