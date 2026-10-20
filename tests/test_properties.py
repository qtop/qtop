## SPDX-License-Identifier: MIT
"""Bounded, shrinkable checks for qtop's data and parser contracts."""

import copy
import os
import sys
import tarfile
import tempfile
from pathlib import Path
from types import SimpleNamespace

import pytest

# Modern tools are optional on the legacy runtime lanes, mandatory on 3.12+.
if sys.version_info < (3, 12):
    pytest.skip("property tests run in the modern tooling lane", allow_module_level=True)

from hypothesis import given, settings
from hypothesis import strategies as st

from qtop_py import sample, yaml_parser
from qtop_py.cluster_state import build_cluster_state, cluster_state_totals, validate_cluster_state
from qtop_py.plugins.pbs import PBSBatchSystem, PBSStatExtractor
from qtop_py.plugins.slurm import SlurmBatchSystem
from qtop_py.qtop import Document
from qtop_py.ui.viewport import Viewport

settings.register_profile("ci", max_examples=100, derandomize=True, deadline=None)
settings.register_profile("nightly", max_examples=1000, deadline=None)
settings.load_profile(os.environ.get("HYPOTHESIS_PROFILE", "ci"))

NAMES = st.from_regex(r"[a-z][a-z0-9]{0,8}", fullmatch=True)


@st.composite
def cluster_states(draw):
    users = draw(st.lists(NAMES, min_size=1, max_size=4, unique=True))
    queues = draw(st.lists(NAMES, min_size=1, max_size=3, unique=True))
    ids = [str(number) for number in draw(st.lists(st.integers(1, 9999), max_size=8, unique=True))]
    jobs = [{"id": job, "user": draw(st.sampled_from(users)), "queue": draw(st.sampled_from(queues)), "state": draw(st.sampled_from(("R", "Q", "F")))} for job in ids]
    nodes = []
    for index in range(draw(st.integers(0, 5))):
        cores = draw(st.integers(1, 8))
        assignments = draw(st.dictionaries(st.integers(0, cores - 1).map(str), st.sampled_from(ids))) if ids else {}
        nodes.append(
            {
                "domainname": "node%s" % index,
                "state": "-",
                "np": str(cores),
                "qname": draw(st.lists(st.one_of(st.none(), st.sampled_from(queues)), max_size=3)),
                "core_job_map": assignments,
            }
        )
    return {
        "jobs": jobs,
        "nodes": nodes,
        "queues": [
            {
                "name": queue,
                "limit": "0",
                "state": "E",
                "running": sum(job["queue"] == queue and job["state"] == "R" for job in jobs),
                "queued": sum(job["queue"] == queue and job["state"] == "Q" for job in jobs),
            }
            for queue in queues
        ],
    }


@given(cluster_states())
def test_document_round_trip_preserves_cluster_state(state):
    assert validate_cluster_state(state) is state
    assert Document.from_cluster_state(state).to_cluster_state() == state
    assert cluster_state_totals(state) == {
        "running_jobs": sum(job["state"] == "R" for job in state["jobs"]),
        "queued_jobs": sum(job["state"] == "Q" for job in state["jobs"]),
    }


@given(cluster_states())
def test_anonymization_preserves_identity_and_core_links(state):
    original = copy.deepcopy(state)
    masked, mappings = sample.anonymize_state(state)
    assert state == original
    for mapping in mappings.values():
        assert len(set(mapping.values())) == len(mapping)
    for before, after in zip(state["jobs"], masked["jobs"]):
        assert after["id"] == mappings["job"][before["id"]]
        assert after["user"] == mappings["user"][before["user"]]
        assert after["queue"] == mappings["queue"][before["queue"]]
        assert after["state"] == before["state"]
    for before, after in zip(state["nodes"], masked["nodes"]):
        assert after["np"] == before["np"]
        assert after["core_job_map"] == {core: mappings["job"][job] for core, job in before["core_job_map"].items()}
    assert cluster_state_totals(masked) == cluster_state_totals(state)
    assert sample.anonymize_state(state)[0] == masked


@given(cluster_states())
def test_portable_snapshot_round_trip(state):
    masked, _mappings = sample.anonymize_state(state)
    with tempfile.TemporaryDirectory() as directory:
        path = str(Path(directory) / "sample.tar")
        with tarfile.open(path, "w") as archive:
            sample.save_snapshot(archive, masked, {"extract_info": None}, SimpleNamespace(ANONYMIZE=True, COLOR="OFF"), "slurm", "test", [])
        assert sample.load_snapshot(path)["state"] == masked


@given(st.integers(0, 90), st.integers(0, 8), st.integers(1, 3))
def test_slurm_ranges_preserve_padding_cartesian_order_and_deduplicate(start, length, racks):
    end = start + length
    hosts = SlurmBatchSystem.expand_nodelist("rack[1-%s]n[%02d-%02d],rack1n%02d" % (racks, start, end, start))
    assert hosts == ["rack%sn%02d" % (rack, node) for rack in range(1, racks + 1) for node in range(start, end + 1)]


@given(st.integers(0, 999))
def test_slurm_rejects_reversed_and_incomplete_ranges(start):
    assert SlurmBatchSystem.expand_nodelist("node[%s-%s]" % (start + 1, start)) == []
    assert SlurmBatchSystem.expand_nodelist("node[%s" % start) == []


@given(st.integers(0, 100), st.integers(0, 10), st.integers(1, 9999))
def test_pbs_core_ranges_are_inclusive(start, length, job):
    expected = [(str(core), str(job)) for core in range(start, start + length + 1)]
    assert list(PBSBatchSystem.get_corejob_from_range("%s-%s" % (start, start + length), str(job))) == expected


@given(
    st.integers(0, 200),
    st.integers(0, 200),
    st.integers(1, 50),
    st.integers(1, 50),
    st.lists(
        st.sampled_from(("scroll_up", "scroll_down", "scroll_left", "scroll_right", "scroll_top", "scroll_bottom", "scroll_far_left", "scroll_far_right", "reset_display")),
        max_size=50,
    ),
)
def test_viewport_movements_remain_within_bounds(height, width, rows, columns, movements):
    view = Viewport()
    view.max_height, view.max_width = height, width
    view.set_term_size(rows, columns)
    for movement in movements:
        getattr(view, movement)()
        assert 0 <= view.v_start <= max(height - rows, 0)
        assert 0 <= view.h_start <= max(width - columns, 0)


YAML_VALUES = st.recursive(
    st.one_of(st.none(), st.booleans(), st.integers(), st.text(max_size=10)),
    lambda children: st.one_of(st.lists(children, max_size=4), st.dictionaries(st.sampled_from(("-", "state", "items", "count")), children, max_size=4)),
    max_leaves=20,
)


@given(st.dictionaries(st.sampled_from(("items", "state", "count")), YAML_VALUES, max_size=3))
def test_yaml_normalization_is_idempotent_and_does_not_mutate_input(value):
    original = copy.deepcopy(value)
    normalized = yaml_parser.convert_dash_key_in_dict(value)
    assert value == original
    assert yaml_parser.convert_dash_key_in_dict(normalized) == normalized


def test_yaml_normalization_preserves_siblings_and_literal_state_dash():
    value = {"count": 3, "items": {"-": [{"nested": {"-": ["value"]}}]}, "mixed": {"-": ["value"], "description": "keep"}, "state": {"-": ["Q", "R"]}}
    result = yaml_parser.convert_dash_key_in_dict(value)
    assert result["items"] == [{"nested": ["value"]}]
    assert result["mixed"] == value["mixed"]
    assert result["state"] == value["state"]


@given(cluster_states(), st.sampled_from(("jobs", "nodes", "queues")))
def test_imported_state_rejects_non_record_collections(state, key):
    state[key] = [None]
    with pytest.raises(TypeError):
        validate_cluster_state(state)


@pytest.mark.parametrize("field, invalid", (("np", -1), ("np", True), ("qname", "queue"), ("core_job_map", {"0": None})))
def test_imported_state_rejects_invalid_node_fields(field, invalid):
    state = {"jobs": [], "queues": [], "nodes": [{"domainname": "node1", "state": "-", "np": 1, "qname": [], "core_job_map": {}}]}
    state["nodes"][0][field] = invalid
    with pytest.raises((TypeError, ValueError)):
        validate_cluster_state(state)


def test_build_rejects_misaligned_job_columns():
    with pytest.raises(ValueError, match="equal lengths"):
        build_cluster_state(["42"], [], ["R"], ["debug"], [], [])


@pytest.mark.parametrize("change", ("scheduler", "state", "options"))
def test_snapshot_import_rejects_malformed_nested_metadata(tmp_path, change):
    snapshot = {"format_version": 1, "scheduler": "slurm", "state": {"jobs": [], "nodes": [], "queues": []}, "config": {}, "options": {}}
    snapshot[change] = {"scheduler": 42, "state": {"jobs": [None], "nodes": [], "queues": []}, "options": {"CLASSIC": "yes"}}[change]
    path = str(tmp_path / "bad.tar")
    with tarfile.open(path, "w") as archive:
        sample.add_json(archive, sample.SNAPSHOT_NAME, snapshot)
    with pytest.raises((TypeError, ValueError)):
        sample.load_snapshot(path)


def test_pbs_json_queue_anonymization_supplies_its_category(tmp_path):
    path = tmp_path / "qstat_q.json"
    path.write_text('{"Queue":{"private_queue":{"enabled":true,"state_count":"Transit:0 Queued:2 Held:0 Waiting:0 Running:1 Exiting:0"}}}')
    rows = PBSStatExtractor({}, SimpleNamespace(ANONYMIZE=True)).extract_qstatq(str(path))
    assert rows[0]["queue_name"] == "p_anon_q_0"
    assert rows[0]["run"] == "1" and rows[0]["queued"] == "2"
