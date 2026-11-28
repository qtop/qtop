## Copyright (c) 2026 qtop contributors
## SPDX-License-Identifier: MIT
"""Contract every registered scheduler backend must meet, whatever its scheduler.

Parametrized over discover_qtop_batch_systems(), so a new plugin is checked as soon as it is
registered: configuration and gate entries exist, its extension fields are declared where
validation and -A masking find them, and each committed sample survives export, anonymization
and replay. See "Adding a scheduler backend" in docs/documentation.md.
"""

import json
import re
import sys
from pathlib import Path

import pytest

from qtop_py import configuration, testing
from qtop_py import core_yaml_parser as yaml_parser
from qtop_py.core_cluster_state import validate_cluster_state
from qtop_py.schedulers import metadata, registry
from qtop_py.snapshot import Snapshot

ROOT = Path(__file__).resolve().parents[1]
BACKENDS = registry.discover_qtop_batch_systems()
CONFIG = configuration.with_backend_defaults(yaml_parser.parse(str(ROOT / "qtopconf.yaml")))

sys.path.insert(0, str(ROOT / "tools"))
from validate_scheduler_samples import discover_cases  # noqa: E402

SAMPLE_CASES = discover_cases(sorted(BACKENDS), ROOT / "tests/plugins/slurm_samples")


@pytest.mark.parametrize("mnemonic", sorted(BACKENDS))
def test_backend_is_configured_and_gated(mnemonic):
    assert mnemonic in CONFIG["schedulers"], "declare default_commands on the %s backend class" % mnemonic
    assert mnemonic in CONFIG["signature_commands"], "declare a signature_command on the %s backend class for -b auto" % mnemonic
    gate = re.search(r"^SAMPLE_GATE_SCHEDULERS \?= (\S+)$", (ROOT / "Makefile").read_text(), re.M)
    assert gate and (gate.group(1) == "all" or mnemonic in gate.group(1).split(",")), "SAMPLE_GATE_SCHEDULERS must be all or list %s" % mnemonic
    assert any(case["scheduler"] == mnemonic for case in SAMPLE_CASES), "add tests/plugins/%s_samples/<case>/sample-gate.json" % mnemonic


@pytest.mark.parametrize("mnemonic", sorted(BACKENDS))
def test_backend_meets_the_contract_other_packages_check_too(mnemonic):
    """qtop_py.testing is what a backend in another package runs; qtop's own plugins pass it as well."""
    testing.check_backend(BACKENDS[mnemonic])


@pytest.mark.parametrize("case", [case for case in SAMPLE_CASES if "-s" in case["args"]], ids=lambda case: case["name"])
def test_backend_sample_passes_the_library_check(case, tmp_path):
    testing.check_sample(BACKENDS[case["scheduler"]], str(case["source"]), str(tmp_path))


def test_extension_specs_do_not_overlap():
    seen = {}
    for spec in metadata.extensions().specs:
        keys = list(spec.node_job_fields) + [field for identity in spec.job_identities for field in identity[:2]]
        keys += [spec.diagnostics[0]] if spec.diagnostics else []
        for key in keys:
            assert key not in seen, "%s and %s both claim %s" % (seen[key], spec.label, key)
            seen[key] = spec.label


def _export(runtime, case, *extra):
    """Export the case's sample with qtop -E inside the test process; return the exported state."""
    source = ["-s", str(case["source"])] if case.get("source") else []
    result, output = runtime("-b", case["scheduler"], *(source + list(extra)))
    assert result == 0
    (export,) = output.glob("qtop_json_*.json")
    return json.loads(export.read_text())


@pytest.mark.parametrize("case", [case for case in SAMPLE_CASES if case["scheduler"] not in ("demo", "demo72")], ids=lambda case: case["name"])
def test_backend_sample_survives_export_anonymization_and_replay(case, runtime):
    state = _export(runtime, case)
    validated = validate_cluster_state(state)
    assert Snapshot.from_cluster_state(validated).to_cluster_state() == validated

    masked_text = json.dumps(_export(runtime, case, "-A"))
    # Some committed samples are themselves anonymized; re-masking those names may reproduce them.
    users = {job["user"] for job in state["jobs"] if not re.fullmatch(r".?_anon_user_[0-9]+", job["user"])}
    leaked = [user for user in users if json.dumps(user) in masked_text]
    assert not leaked, "-A left user names in the %s export: %s" % (case["name"], leaked)
