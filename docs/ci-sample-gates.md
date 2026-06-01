# CI sample gates

Issue #433 asks for a small validation path that can be reproduced locally and
then reused by CI. The local entry point is:

```bash
make sample-gate SAMPLE_GATE_SCHEDULERS=pbs,sge,slurm SAMPLE_GATE_MAX_FAILURES=0
```

Sources:

- PBS: `qtop_py/contrib`
- SGE: `qtop_py/contrib`
- Slurm: every directory under `tests/plugins/slurm_samples`

The PBS and SGE cases reuse the historical contrib wrapper flags (`-raF` for
PBS and `-Fadvv` for SGE) while running through the shared Python gate. The
legacy `qtop_py/contrib/func_tests.sh` wrapper delegates to the same target, so
manual and automated sample checks exercise one implementation.

Policy:

- `SAMPLE_GATE_MAX_FAILURES=0` is the default and means any failed scheduler
  case fails the gate.
- Rendered qtop output, ANSI-stripped normalized output, SVG terminal
  screenshots, stderr, command lines, and `summary.json` are written under
  `artifacts/sample-gate/`, including for non-zero and timeout failures.
- Each qtop subprocess receives an isolated `HOME` under its artifact
  directory, so log creation does not depend on a writable runner home.
- Generated artifacts stay out of the repository. The `artifacts/` path is
  ignored and can be uploaded by a later CI wiring change.

`make ci` currently composes the unit tests, sample gate, fortifications, and
branch whitespace check. Provider-specific GitHub/GitLab wiring can call this
same target in a follow-up change without redefining the validation contract.
