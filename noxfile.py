## Copyright (c) 2026 qtop contributors
## SPDX-License-Identifier: MIT
"""Run qtop's Make gates in a managed Python 3.12 environment with the pinned CI requirements.

`uvx nox` runs make pre-pr schema-ci; `uvx nox -- test-fast` runs any Make targets, in one invocation.
"""

import os

import nox

nox.options.default_venv_backend = "uv|virtualenv"
nox.options.reuse_venv = "yes"  # later runs re-check the pins in the same environment; -N rebuilds it
nox.options.sessions = ["gates"]


@nox.session(python="3.12")
def gates(session: nox.Session) -> None:
    """Install requirements-ci.txt, then run the requested Make targets (default: pre-pr schema-ci)."""
    session.install("-r", "requirements-ci.txt")
    targets = session.posargs or ["pre-pr", "schema-ci"]
    if any(target.startswith("-") or "=" in target for target in targets):
        session.error("pass Make target names only, e.g. nox -- test-fast")
    session.run("make", *targets, "PYTHON=" + os.path.join(session.bin, "python"), external=True)
