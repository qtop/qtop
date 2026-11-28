## Copyright (c) 2026 qtop contributors
## SPDX-License-Identifier: MIT
"""qtop_py.testing: the contract checks a backend in another package runs, and what they report."""

from pathlib import Path

import pytest

from qtop_py import testing
from qtop_py.core_api import GenericBatchSystem
from qtop_py.plugins.flux import FluxBatchSystem
from qtop_py.schedulers.extensions import ExtensionSpec

FLUX_SAMPLE = str(Path(__file__).resolve().parent / "plugins" / "flux_samples" / "basic")


def backend(name="toy", **attributes):
    """Return a GenericBatchSystem subclass named name with the given class attributes."""
    defaults = {"default_commands": {"jobs_file": "%(savepath)s/toy%(pid)s.txt, toystat"}, "signature_command": "toystat"}
    return type("Toy", (GenericBatchSystem,), dict(defaults, get_mnemonic=staticmethod(lambda: name), **attributes))


def test_a_well_formed_backend_passes():
    testing.check_backend(backend())


@pytest.mark.parametrize(
    "broken, message",
    [
        (backend(name="Toy-1"), "lower-case name"),
        (backend(default_commands={}), "declare default_commands"),
        (backend(signature_command=" "), "signature_command"),
        (backend(node_job_fields=("toy_jobs",)), "without an ExtensionSpec"),
        (backend(extensions=ExtensionSpec("Toy")), "register toy"),
    ],
    ids=["name", "commands", "signature", "job-fields", "unregistered-spec"],
)
def test_a_broken_backend_is_told_what_to_fix(broken, message):
    with pytest.raises(AssertionError, match=message):
        testing.check_backend(broken)


def test_a_malformed_command_names_its_key():
    with pytest.raises(ValueError, match="jobs_file"):
        testing.check_backend(backend(default_commands={"jobs_file": "no separator"}))


def test_a_registered_spec_must_match_and_name_its_diagnostics(monkeypatch):
    spec = ExtensionSpec("Toy", diagnostics=("toy_counts", ("rows",)), node_job_fields=("toy_jobs",))
    known = type("Known", (), {"specs": (spec,)})  # metadata.extensions() returns an object with .specs
    monkeypatch.setattr(testing.metadata, "extensions", known)
    with pytest.raises(AssertionError, match="node_job_fields from its ExtensionSpec"):
        testing.check_backend(backend(extensions=spec))
    with pytest.raises(AssertionError, match="<name>_diagnostics"):
        testing.check_backend(backend(extensions=spec, node_job_fields=("toy_jobs",)))


def test_a_sample_is_built_replayed_and_masked(tmp_path, monkeypatch):
    state = testing.check_sample(FluxBatchSystem, FLUX_SAMPLE, str(tmp_path))
    assert len(state["nodes"]) == 8 and {job["user"] for job in state["jobs"]} == {"fluxuser"}
    monkeypatch.setattr(testing, "anonymize_state", lambda state: (state, {}))  # a masker that leaks
    with pytest.raises(AssertionError, match=r"-A left user names .*\['fluxuser'\]"):
        testing.check_sample(FluxBatchSystem, FLUX_SAMPLE, str(tmp_path))
