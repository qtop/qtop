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

## Validation

Before opening a pull request, run the shared validation target:

```sh
make ci
```

This runs the unit tests, validates the committed PBS/OAR/SGE scheduler samples
against their reference output, and checks the diff for review hazards such as
new `eval()` usage, control/bidi characters, or generated-looking binary paths.

The sample gate uses the files in `qtop_py/contrib` as its source of truth. It
normalizes run-specific lines such as the working directory and log path, then
fails if any selected scheduler output differs from the committed reference.
Captured stdout, stderr, and diffs are written under `artifacts/sample-gate/`.

[1] https://wiki.linuxfoundation.org/dco

[2] https://developercertificate.org/

[3] https://en.wikipedia.org/wiki/Contributor_License_Agreement

[4] https://contributing.md/ -> How Do I Submit a Good Bug Report?

[5] https://www.conventionalcommits.org/en/v1.0.0/ or https://www.electronjs.org/docs/latest/development/pull-requests#step-5-commit
