## Copyright (c) 2026 qtop contributors
## SPDX-License-Identifier: MIT
"""Critical MVU boundaries: explicit transitions and isolated query/presentation."""

import copy
import sys
from pathlib import Path

from qtop_py import configuration, qtop, utils, yaml_parser
from qtop_py.mvu import MVU_Model, MVU_query, MVU_update, MVU_view
from qtop_py.plugins.slurm import SlurmBatchSystem
from qtop_py.snapshot_document import Document
from qtop_py.ui.viewport import Viewport


def MVU_test_model(tmp_path, monkeypatch):
    root = Path(__file__).resolve().parents[1]
    monkeypatch.setattr(sys, "argv", ["qtop", "-b", "slurm", "-F", "-o", "savepath=" + str(tmp_path)])
    args = utils.parse_qtop_cmdline_args()
    args.COLOR, args.REMAP = "OFF", False
    config, users, states = configuration.prepare_config(yaml_parser.parse(str(root / "qtopconf.yaml")), args)
    source = root / "tests/plugins/slurm_samples/basic"
    backend = SlurmBatchSystem({"squeue_file": str(source / "squeue.txt"), "sinfo_file": str(source / "sinfo.txt")}, config, args)
    document = Document.from_cluster_state(backend.get_cluster_state(args))
    viewport = Viewport(0, 0)
    viewport.set_term_size(40, 120)
    return MVU_Model(
        config,
        args,
        viewport,
        backend=SlurmBatchSystem,
        display_overrides={"force_names": True},
        document=document,
        user_to_color=users,
        nodestate_to_color=states,
        package_path=str(root),
        help_file="helpfile.txt",
    )


def test_MVU_update_returns_owned_state_and_effects_without_io(tmp_path, monkeypatch):
    model = MVU_test_model(tmp_path, monkeypatch)
    original_overrides = model.display_overrides.copy()
    original_viewport = vars(model.viewport).copy()
    next_model, effects = MVU_update(model, "t")
    assert next_model is not model
    assert next_model.display_overrides["transpose_wn_matrices"] is not model.config["transpose_wn_matrices"]
    assert effects == (("notice", "Transposing matrix..."),)
    assert model.display_overrides == original_overrides and vars(model.viewport) == original_viewport
    assert MVU_update(model, "unknown") == (model, ())
    assert MVU_update(model, "q")[1] == (("quit", None),)
    help_model, _ = MVU_update(model, "?")
    assert help_model.display_overrides["output_fp"] == model.help_file
    restored_model, _ = MVU_update(help_model, "?")
    assert "output_fp" not in restored_model.display_overrides
    assert model.display_overrides == original_overrides


def test_MVU_query_and_view_need_no_runtime_globals_or_mutable_snapshot(tmp_path, monkeypatch, capsys):
    model = MVU_test_model(tmp_path, monkeypatch)
    original = model.document.to_cluster_state()
    original_nodes = copy.deepcopy(model.document.worker_nodes)
    original_viewport = vars(model.viewport).copy()
    original_config = copy.deepcopy(model.config)
    for name in ("args", "config", "dynamic_config", "scheduler", "document", "cluster", "viewport", "user_to_color", "transposed_sections"):
        monkeypatch.delattr(qtop, name, raising=False)
    view_data = MVU_query(model)
    MVU_view(view_data, str(tmp_path), "sample.tar", "qtop.log")
    assert "Job accounting summary" in capsys.readouterr().out
    assert model.document.to_cluster_state() == original
    assert model.document.worker_nodes == original_nodes
    assert model.config == original_config
    assert vars(model.viewport) == original_viewport
    assert model.args.REMAP is False
