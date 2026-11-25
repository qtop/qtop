## Copyright (c) 2026 qtop contributors
## SPDX-License-Identifier: MIT
"""Slurm topology/block (NVL72 NVLink domains): the optional trace, slurm_block, the per-block notes, -A and replay."""

import json
import logging
from pathlib import Path

import pytest

from qtop_py import core_api, core_collect
from qtop_py.core_cluster_state import validate_cluster_state
from qtop_py.core_errors import OperationalError
from qtop_py.core_sample import anonymize_state
from qtop_py.core_source_trace import TraceMasker
from qtop_py.plugins.slurm import SlurmBatchSystem
from qtop_py.schedulers.metadata import node_is_idle

NVL72 = Path(__file__).resolve().parent / "slurm_samples" / "nvl72"
NVL72_NOTES = [
    "INFO: Topology blocks (Slurm topology/block, e.g. NVLink domains):",
    "INFO: nvl72_01: 18 nodes (4 free, 14 busy), gpu 16/72 idle, jobs across blocks: 301",
    "INFO: nvl72_02: 18 nodes (11 free, 6 busy), gpu 52/72 idle, jobs across blocks: 301",
]


def nvl72_state(tmp_path, topology=None):
    """Replay the synthetic NVL72 capture; topology replaces its topology.txt (None keeps it, "" drops it)."""
    source = tmp_path / "nvl72"
    source.mkdir()
    for trace in NVL72.iterdir():
        if trace.name != "topology.txt" or topology is None:
            (source / trace.name).write_text(trace.read_text())
    if topology:
        (source / "topology.txt").write_text(topology)
    return core_api.collect_state(SlurmBatchSystem, str(tmp_path), core_api.BackendOptions(SOURCEDIR=str(source)))


ENABLED = {"experimental_nvlink_blocks": True}


def block_notes(state, config=None):
    config = ENABLED if config is None else config
    return [line for line in SlurmBatchSystem.display_messages("resources", None, state["nodes"], config) if not line.startswith("INFO: Accelerators") and "gpu0" not in line]


def test_nvl72_replay_assigns_each_node_its_block(tmp_path):
    state = nvl72_state(tmp_path)
    blocks = {node["domainname"]: node["slurm_block"] for node in state["nodes"]}
    assert len(blocks) == 36
    assert {name for name, block in blocks.items() if block == "nvl72_01"} == {"nvl%03d" % number for number in range(1, 19)}
    assert {name for name, block in blocks.items() if block == "nvl72_02"} == {"nvl%03d" % number for number in range(19, 37)}


def test_nvl72_notes_show_free_nodes_idle_gpus_and_jobs_spanning_blocks(tmp_path):
    state = nvl72_state(tmp_path)
    assert block_notes(state) == NVL72_NOTES
    assert block_notes(state, {}) == []  # experimental: off unless enabled
    assert block_notes(state, {"experimental_nvlink_blocks": False}) == []
    assert block_notes(state, dict(ENABLED, accelerator_map="off")) == NVL72_NOTES


def test_aggregated_blocks_tree_switches_and_bad_node_lists_are_ignored(tmp_path, caplog):
    topology = (
        "BlockName=nvl72_01 BlockIndex=0 Nodes=nvl[001-018] BlockSize=18\n"
        "BlockName=nvl72_02 BlockIndex=1 Nodes=nvl[036-019] BlockSize=18\n"
        "BlockName=spare BlockIndex=2 BlockSize=18\n"
        "AggregatedBlock=nvl72_01,nvl72_02 BlockIndex=3 Nodes=nvl[001-036] BlockSize=36\n"
        "SwitchName=s1 Level=0 LinkSpeed=1 Nodes=nvl[019-036]\n"
    )
    with caplog.at_level(logging.WARNING):
        state = nvl72_state(tmp_path, topology)
    assert {node.get("slurm_block") for node in state["nodes"]} == {"nvl72_01", None}
    assert "invalid node list: nvl72_02" in caplog.text
    # 301's nodes outside any block are not a second block, so it no longer spans blocks
    assert block_notes(state)[1:] == ["INFO: nvl72_01: 18 nodes (4 free, 14 busy), gpu 16/72 idle"]


@pytest.mark.parametrize("topology", ("", "No topology information available\n", "SwitchName=s1 Level=0 LinkSpeed=1 Nodes=nvl[001-036]\n"))
def test_a_missing_or_non_block_topology_shows_no_blocks(tmp_path, topology):
    state = nvl72_state(tmp_path, topology)
    assert not any("slurm_block" in node for node in state["nodes"])
    assert block_notes(state) == []


def test_a_failed_topology_command_degrades_with_its_own_warning(tmp_path, caplog):
    commands = {"topology_file": (str(tmp_path / "topology.txt"), "scontrol show topology")}
    with caplog.at_level(logging.WARNING):
        files = core_collect.trace_files(commands, SlurmBatchSystem, core_api.BackendOptions(), {"topology_file": OperationalError("scontrol failed")})
    assert files == {}
    assert "scontrol failed Slurm topology blocks are not shown." in caplog.text
    assert "CPU placement" not in caplog.text


def test_topology_is_optional_live_and_skipped_by_slurm_summary():
    assert "topology_file" in SlurmBatchSystem.optional_trace_keys
    assert SlurmBatchSystem.optional_trace_aliases["topology_file"] == "topology.txt"
    assert SlurmBatchSystem.default_commands["topology_file"].endswith(", scontrol show topology")
    summary = core_api.BackendOptions(SLURM_SUMMARY=True)
    assert SlurmBatchSystem.collection_command("topology_file", "scontrol show topology", summary) is None
    assert SlurmBatchSystem.collection_command("topology_file", "scontrol show topology", core_api.BackendOptions(ALL_PARTITIONS=True)) == "scontrol show topology"


@pytest.mark.parametrize("value", ("", 18, ["nvl72_01"]))
def test_imported_blocks_are_validated(tmp_path, value):
    state = nvl72_state(tmp_path)
    state["nodes"][0]["slurm_block"] = value
    with pytest.raises(ValueError, match="slurm_block"):
        validate_cluster_state(state)


def test_a_block_alone_does_not_make_a_node_busy(tmp_path):
    state = nvl72_state(tmp_path)
    free = [node for node in state["nodes"] if node["domainname"] == "nvl011"][0]
    assert free["slurm_block"] == "nvl72_01" and node_is_idle(free)


def test_anonymized_states_mask_block_names_and_keep_the_grouping(tmp_path):
    state = nvl72_state(tmp_path)
    masked, mappings = anonymize_state(state)
    assert "nvl72" not in json.dumps(masked)
    assert mappings["block"] == {"nvl72_01": "b_anon_block_0", "nvl72_02": "b_anon_block_1"}
    assert [node["slurm_block"] for node in masked["nodes"]] == ["b_anon_block_0"] * 18 + ["b_anon_block_1"] * 18
    notes = block_notes(validate_cluster_state(masked))
    assert notes[1].startswith("INFO: b_anon_block_0: 18 nodes (4 free, 14 busy), gpu 16/72 idle, jobs across blocks: 1000")


def test_masked_topology_trace_uses_the_state_mappings():
    mask = TraceMasker({"block": {"nvl72_01": "b_anon_block_0"}, "node": {"nvl001": "n_anon_wn_0", "nvl002": "n_anon_wn_1"}})
    backend = SlurmBatchSystem({}, {}, core_api.BackendOptions())
    name, text = backend.mask_source_trace(
        "topology_file",
        "BlockName=nvl72_01 BlockIndex=0 Nodes=nvl[001-002] BlockSize=18\nAggregatedBlock=private_agg BlockIndex=1 BlockSize=36\n",
        mask,
    )
    assert name == "topology.txt"
    assert text == "BlockName=b_anon_block_0 BlockIndex=0 Nodes=n_anon_wn_0,n_anon_wn_1 BlockSize=18\nAggregatedBlock=block_trace_1 BlockIndex=1 BlockSize=36\n"
    for unsupported in ("SwitchName=s1 Level=0 LinkSpeed=1 Nodes=nvl001\n", "BlockName=b BlockIndex=0 Nodes=nvl[2-1] BlockSize=1\n"):
        with pytest.raises(ValueError):
            backend.mask_source_trace("topology_file", unsupported, mask)
