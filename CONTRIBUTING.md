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

Development checks:

* `make test` runs the unit tests.
* `make sample-validate MAX_FAILURES=0` runs qtop against checked-in scheduler samples and writes raw output, stderr, normalized output, and `summary.json` under `sample-artifacts/`.
* `make fortify` rejects active `eval()` calls, bidirectional text markers, and committed Python bytecode.
* `make ci` is the shared GitHub Actions and GitLab CI entry point.

Scheduler sample gate:

The sample gate uses fixtures already committed in `qtop_py/contrib/`: PBS uses `pbsnodes_a.txt`, `qstat_q.txt`, and `qstat.txt`; SGE uses `qstat.F.xml.stdout`; OAR uses `oarnodes_s_Y.txt`, `oarnodes_Y.txt`, and `oarstat.txt`. The gate runs qtop with `python -m qtop_py.cli`, strips volatile timestamps, working directories, ANSI color, and log paths from the normalized artifact, and fails when a scheduler command exits non-zero, produces no normalized output, or misses expected qtop sections. Set `MAX_FAILURES=0` in CI so any scheduler regression fails the pipeline.

qtop currently has PBS, OAR, SGE, and demo scheduler implementations. There is no SLURM plugin or sample fixture in this repository yet; add that plugin and fixture before enabling a SLURM sample job.

[1] https://wiki.linuxfoundation.org/dco

[2] https://developercertificate.org/

[3] https://en.wikipedia.org/wiki/Contributor_License_Agreement

[4] https://contributing.md/ -> How Do I Submit a Good Bug Report?

[5] https://www.conventionalcommits.org/en/v1.0.0/ or https://www.electronjs.org/docs/latest/development/pull-requests#step-5-commit
