## Copyright (c) 2026 qtop contributors
## SPDX-License-Identifier: MIT
"""Contract every registered scheduler backend must meet, whatever its scheduler.

Parametrized over discover_qtop_batch_systems(), so a new plugin is checked as soon as it is
registered: configuration and gate entries exist, its extension fields are declared where
validation and -A masking find them, and each committed sample survives export, anonymization
and replay. See "Adding a scheduler backend" in docs/documentation.md.
"""

import json
import os
import re
import subprocess
import sys
from pathlib import Path

import pytest

from qtop_py import core_yaml_parser as yaml_parser
from qtop_py.core_cluster_state import validate_cluster_state
from qtop_py.schedulers import metadata, registry
from qtop_py.snapshot import Snapshot

ROOT = Path(__file__).resolve().parents[1]
BACKENDS = registry.discover_qtop_batch_systems()
CONFIG = yaml_parser.parse(str(ROOT / "qtopconf.yaml"))

sys.path.insert(0, str(ROOT / "tools"))
from validate_scheduler_samples import discover_cases  # noqa: E402

SAMPLE_CASES = discover_cases(sorted(BACKENDS), ROOT / "tests/plugins/slurm_samples")


@pytest.mark.parametrize("mnemonic", sorted(BACKENDS))
def test_backend_is_configured_and_gated(mnemonic):
    assert mnemonic in CONFIG["schedulers"], "add %s commands under schedulers: in qtopconf.yaml" % mnemonic
    assert mnemonic in CONFIG["signature_commands"], "add %s under signature_commands: for auto-detection" % mnemonic
    gate = re.search(r"^SAMPLE_GATE_SCHEDULERS \?= (\S+)$", (ROOT / "Makefile").read_text(), re.M)
    assert gate and mnemonic in gate.group(1).split(","), "add %s to SAMPLE_GATE_SCHEDULERS in the Makefile" % mnemonic
    assert any(case["scheduler"] == mnemonic for case in SAMPLE_CASES), "add a %s sample to tools/validate_scheduler_samples.py" % mnemonic


@pytest.mark.parametrize("mnemonic", sorted(BACKENDS))
def test_backend_extension_fields_are_declared_once(mnemonic):
    backend = BACKENDS[mnemonic]
    spec = backend.extensions
    if spec is None:
        assert backend.node_job_fields == (), "%s lists node job fields without an ExtensionSpec, so -A would not mask them" % mnemonic
        return
    assert any(spec is known for known in metadata.EXTENSIONS), "list %r in metadata.EXTENSIONS" % spec
    assert backend.node_job_fields == spec.node_job_fields


def test_extension_specs_do_not_overlap():
    seen = {}
    for spec in metadata.EXTENSIONS:
        keys = list(spec.node_job_fields) + [field for identity in spec.job_identities for field in identity[:2]]
        keys += [spec.diagnostics[0]] if spec.diagnostics else []
        for key in keys:
            assert key not in seen, "%s and %s both claim %s" % (seen[key], spec.label, key)
            seen[key] = spec.label


def _export(case, tmp_path, *extra):
    output = tmp_path / ("out" + "".join(extra))
    command = [sys.executable, "-m", "qtop_py.qtop", "-b", case["scheduler"], "-E", "-O", "-c", "OFF", "-o", "savepath=%s" % output]
    if case.get("source"):
        command[5:5] = ["-s", str(case["source"])]
    env = dict(os.environ, HOME=str(tmp_path), PYTHONPATH=str(ROOT))
    env.pop("QTOP_SCHEDULER", None)
    result = subprocess.run(command + list(extra), cwd=str(ROOT), env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True, timeout=60)
    assert result.returncode == 0, result.stderr
    (export,) = output.glob("qtop_json_*.json")
    return json.loads(export.read_text())


@pytest.mark.parametrize("case", [case for case in SAMPLE_CASES if case["scheduler"] != "demo"], ids=lambda case: case["name"])
def test_backend_sample_survives_export_anonymization_and_replay(case, tmp_path):
    state = _export(case, tmp_path)
    validated = validate_cluster_state(state)
    assert Snapshot.from_cluster_state(validated).to_cluster_state() == validated

    masked_text = json.dumps(_export(case, tmp_path, "-A"))
    # Some committed samples are themselves anonymized; re-masking those names may reproduce them.
    users = {job["user"] for job in state["jobs"] if not re.fullmatch(r".?_anon_user_[0-9]+", job["user"])}
    leaked = [user for user in users if json.dumps(user) in masked_text]
    assert not leaked, "-A left user names in the %s export: %s" % (case["name"], leaked)
