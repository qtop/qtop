## Copyright (c) 2026 qtop contributors
## SPDX-License-Identifier: MIT
"""Run qtop's Make gates in a managed Python 3.12.3 environment with the pinned CI requirements.

`uvx nox@2026.10.20` runs make pre-pr schema-ci; `uvx nox@2026.10.20 -- test-fast` runs any Make targets, in one invocation.
nox is pinned here and in docs/local-testing.md, not in requirements-ci.txt, which it installs.
"""

import os
from typing import TYPE_CHECKING

import nox

if TYPE_CHECKING:
    from typing import Final

# CI's modern-python lanes run 3.12.3; QTOP_NOX_PYTHON=3.13 (say) tries another interpreter.
NOX_PYTHON: "Final" = os.environ.get("QTOP_NOX_PYTHON", "3.12.3")

nox.needs_version = ">=2026.10.20"  # the uvx nox@ pin of docs/local-testing.md; Dependabot does not update either
nox.options.default_venv_backend = "uv|virtualenv"
nox.options.reuse_venv = "yes"  # later runs re-check the pins in the same environment; -N rebuilds it
nox.options.sessions = ["gates"]


@nox.session(python=NOX_PYTHON)
def gates(session: nox.Session) -> None:
    """Install requirements-ci.txt, then run the requested Make targets (default: pre-pr schema-ci)."""
    session.install("-r", "requirements-ci.txt")
    targets = session.posargs or ["pre-pr", "schema-ci"]
    if any(target.startswith("-") or "=" in target for target in targets):
        session.error("pass Make target names only, e.g. nox -- test-fast")
    session.run("make", *targets, "PYTHON=" + os.path.join(session.bin, "python"), external=True)
