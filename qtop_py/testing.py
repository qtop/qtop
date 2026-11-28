## Copyright (c) 2026 qtop contributors
## SPDX-License-Identifier: MIT
"""Contract checks for a scheduler backend: those qtop runs on its own plugins, for any package.

A backend shipped in another package (through the qtop.backends entry point) can prove it meets
qtop's contract with its own sample traces, without this repository:

    from qtop_py.testing import check_backend, check_sample

    def test_my_backend(tmp_path):
        check_backend(MyBatchSystem)
        check_sample(MyBatchSystem, "tests/samples/basic", str(tmp_path))

Each check raises AssertionError saying what to fix. Neither needs pytest.
"""

import json
import re
from typing import TYPE_CHECKING, Any, Mapping, Optional, Set, Type

from qtop_py import configuration
from qtop_py.core_api import BackendOptions, GenericBatchSystem, collect, command_lines, validate_cluster_state
from qtop_py.core_sample import anonymize_state
from qtop_py.schedulers import metadata
from qtop_py.snapshot import Snapshot

if TYPE_CHECKING:
    from qtop_py.core_state_types import ClusterState

## Names that -A already produced (u_anon_user_0, ...): a sample made of them may legitimately reproduce them.
_ANONYMIZED_USER = re.compile(r".?_anon_user_[0-9]+")


def check_backend(backend: Type[GenericBatchSystem]) -> None:
    """Assert backend declares what qtop needs: a name, its commands, a signature command and a consistent extension."""
    name = backend.get_mnemonic()
    assert isinstance(name, str) and re.fullmatch(r"[a-z][a-z0-9_]*", name), "get_mnemonic() must return a lower-case name, not %r" % (name,)
    assert backend.default_commands, "declare default_commands on %s: trace key -> 'output file, command'" % name
    command_lines(backend, backend.default_commands, "/savepath", BackendOptions(SOURCEDIR="/traces"))  # raises ValueError naming a malformed entry
    assert isinstance(backend.signature_command, str) and backend.signature_command.strip(), "declare a signature_command on %s for -b auto" % name
    spec = backend.extensions
    if spec is None:
        assert backend.node_job_fields == (), "%s lists node_job_fields without an ExtensionSpec, so -A would not mask them" % name
        return
    assert any(spec is known for known in metadata.extensions().specs), "register %s (a qtop_py/plugins module or the qtop.backends entry point)" % name
    assert backend.node_job_fields == spec.node_job_fields, "take %s's node_job_fields from its ExtensionSpec" % name
    if spec.diagnostics:
        assert spec.diagnostics[0].endswith("_diagnostics"), "name %s's diagnostics key <name>_diagnostics" % name


def check_sample(backend: Type[GenericBatchSystem], trace_dir: str, workdir: str, config: Optional[Mapping[str, Any]] = None) -> "ClusterState":
    """Build a state from the traces in trace_dir and assert it validates, replays losslessly and -A hides its users.

    config defaults to the backends' declared defaults; workdir must exist. Returns the state for further checks.
    """
    config = configuration.with_backend_defaults(dict(config or {}))
    state = collect(backend, workdir, BackendOptions(SOURCEDIR=trace_dir), config=config).state
    assert Snapshot.from_cluster_state(state).to_cluster_state() == state, "the %s state does not survive a snapshot round trip" % trace_dir
    masked = validate_cluster_state(anonymize_state(state)[0])
    text = json.dumps(masked)
    users: Set[str] = {job["user"] for job in state["jobs"] if not _ANONYMIZED_USER.fullmatch(job["user"])}
    leaked = sorted(user for user in users if json.dumps(user) in text)
    assert not leaked, "-A left user names in the %s state: %s" % (trace_dir, leaked)
    return state
