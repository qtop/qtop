Please simply follow any common conventions for Open Source projects, f.i. see Electron framework:
- For source code contributions either a Developer Certificate of Origin (DCO) [1] [2] or a Contributor License Agreement (CLA) [3] may be acceptable.
- For bug reports, please consult the information in [4] to use with your best judgement.
- For improvements or fixes, open a new issue or leave a comment on a relevant issue that is already open.

You may contribute in the following ways:
* Write code; f.i. you may follow guidelines in [5]
* Review pull requests
* Maintain and improve a qtop website or documentation
* Help with outreach and onboard new contributors
* Write and/or lead collaborations proposals, including grants or help with other fundraising or community efforts

[1] https://wiki.linuxfoundation.org/dco

[2] https://developercertificate.org/

[3] https://en.wikipedia.org/wiki/Contributor_License_Agreement

[4] https://contributing.md/ -> How Do I Submit a Good Bug Report?

[5] https://www.conventionalcommits.org/en/v1.0.0/ or https://www.electronjs.org/docs/latest/development/pull-requests#step-5-commit

## Local checks

Use the Makefile as the shared entry point for local development and CI:

```sh
make help
make ci
```

`make ci` runs the fortification diff check, unit tests with coverage, and the fast scheduler sample gate. GitHub Actions and GitLab CI call the same targets so their behavior does not drift.

## Scheduler sample gate

The default fast sample gate runs the committed PBS and SGE scheduler samples from `qtop_py/contrib`:

```sh
make sample-gate
```

The gate renders qtop output through `scripts/sample_gate.py`, removes volatile lines containing paths or log timestamps, records a diff against the checked-in references, and requires the rendered output to contain the expected summary/accounting sections:

- `qtop_py/contrib/pbs_dvv_out.ref`
- `qtop_py/contrib/sger_dvv_out.ref`

Artifacts are written to `artifacts/sample-gate` as stdout, stderr, and unified diff files. The default policy is `SAMPLE_GATE_MAX_FAILURES=0`, so any command failure or missing required output section fails CI. Reference output drift is recorded for review; make it a hard failure with:

```sh
python scripts/sample_gate.py --schedulers pbs,sge --strict-reference
```

Run additional sample coverage with:

```sh
make sample-gate SAMPLE_GATE_SCHEDULERS=pbs,sge,oar
```

SLURM is not part of this gate yet because qtop does not currently ship a SLURM scheduler plugin or reference sample.

## Fortification check

`make fortifications` runs `git diff --check`, flags generated or binary-looking changes for manual review, detects control/bidirectional Unicode characters in added lines, and rejects new `eval(` usage. Existing `eval(` calls are tracked as legacy debt; do not add more.
