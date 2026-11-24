## Copyright (c) 2026 qtop contributors
## SPDX-License-Identifier: MIT
import copy
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from qtop_py.core_cluster_state import validate_cluster_state
from qtop_py.core_sample import anonymize_state
from qtop_py.plugins.slurm import SlurmBatchSystem
from qtop_py.plugins.slurm_json import SUPPORTED_PARSERS, SlurmJSONBatchSystem, UnsupportedSlurmJSON

SAMPLES = Path(__file__).parent / "slurm_json_samples"


@pytest.fixture(params=SUPPORTED_PARSERS, ids=SUPPORTED_PARSERS)
def json_files(request, tmp_path):
    family = request.param if (SAMPLES / request.param).is_dir() else "v0.0.43"
    paths = {}
    for key in ("jobs", "nodes", "partitions"):
        data = json.loads((SAMPLES / family / ("slurm_" + key + ".json")).read_text())
        data["meta"]["plugin"]["data_parser"] = "data_parser/" + request.param
        path = tmp_path / ("slurm_" + key + ".json")
        path.write_text(json.dumps(data))
        paths[key + "_json"] = str(path)
    return paths


def backend(paths, anonymize=False):
    return SlurmJSONBatchSystem(paths, {}, SimpleNamespace(ANONYMIZE=anonymize, SOURCEDIR=True))


def mutate(paths, key, change):
    path = Path(paths[key + "_json"])
    data = json.loads(path.read_text())
    change(data)
    path.write_text(json.dumps(data))


def test_versions_preserve_inventory_identities_allocations_and_gpu(json_files, tmp_path):
    system = backend(json_files)
    state = validate_cluster_state(system.get_cluster_state(system.options))
    jobs = {job["id"]: job for job in state["jobs"]}
    assert set(jobs) == {"80+1", "80+0", "42_7"}
    assert jobs["42_7"]["user"] == "bob"
    assert jobs["42_7"]["slurm_partitions"] == ["debug", "batch"]
    assert jobs["80+1"]["slurm_het_job_offset"] == "1"
    queues = {row["name"]: row for row in state["queues"]}
    assert queues["batch"]["running"] == 1 and queues["batch"]["queued"] == 1
    assert queues["batch"]["state"] == "DRAIN"
    assert queues["empty"]["state"] == "INACTIVE" and queues["empty"]["running"] == 0
    nodes = {row["domainname"]: row for row in state["nodes"]}
    assert len(nodes) == 3 and nodes["node03"]["state"] == "d"
    assert nodes["node03"]["qname"] == []
    assert nodes["node01"]["np"] == "8"
    assert nodes["node01"]["qname"] == ["debug", "batch"]
    assert nodes["node01"]["core_job_map"] == {4: "80+1", 5: "80+1", 0: "80+0", 1: "80+0"}
    assert nodes["node02"]["core_job_map"] == {index: "80+1" for index in range(4)}
    assert nodes["node01"]["cpu_allocation_source"] == nodes["node02"]["cpu_allocation_source"] == "slurm"
    resource = nodes["node02"]["slurm_gpu_bindings"][0]
    assert resource == {"job_id": "80+1", "type": "a100_1g.5gb", "count": 1, "device_indices": [1], "mig_profile": "1g.5gb", "parent_gpu_uuid": None}
    assert nodes["node01"]["slurm_gpu_configured"][0]["type"] == "a100"
    assert nodes["node01"]["slurm_gpu_used"][0]["device_indices"] == [3]
    legacy = tmp_path / "scontrol.txt"
    legacy.write_text("JobId=80+1\n Nodes=node01 CPU_IDs=4-5\n Nodes=node02 CPU_IDs=0-3\nJobId=80+0\n Nodes=node01 CPU_IDs=0-1\n")
    old = SlurmBatchSystem({"scontrol_file": str(legacy)}, {}, system.options)._read_cpu_allocations()
    assert old == {job: {host: tuple(values[0]) for host, values in hosts.items()} for job, hosts in system._allocations.items()}


def test_grouped_pending_arrays_use_existing_bounded_expansion(json_files):
    def change(data):
        job = data["jobs"][1]
        job["array_task_id"] = {"set": False, "number": 0, "infinite": False}
        job["array_task_string"] = "1-5:2%2"

    mutate(json_files, "jobs", change)
    state = backend(json_files).get_cluster_state(SimpleNamespace(ANONYMIZE=False))
    assert [job["id"] for job in state["jobs"]] == ["80+1", "42_1", "42_3", "42_5", "80+0"]


def test_empty_clusters_have_valid_empty_lists(json_files):
    for kind in ("jobs", "nodes", "partitions"):
        mutate(json_files, kind, lambda data, kind=kind: data.__setitem__(kind, []))
    state = backend(json_files).get_cluster_state(SimpleNamespace(ANONYMIZE=False))
    assert state == {"jobs": [], "nodes": [], "queues": []}


@pytest.mark.parametrize("damage", ["version", "errors", "missing-list", "duplicate", "bad-resource", "bad-gpu", "bad-topology", "missing-allocation"])
def test_malformed_or_unsupported_responses_are_not_silently_accepted(json_files, damage):
    def change(data):
        if damage == "version":
            data["meta"]["plugin"]["data_parser"] = "v0.0.999"
        elif damage == "errors":
            data["errors"] = [{"error": "RPC failed"}]
        elif damage == "missing-list":
            del data["jobs"]
        elif damage == "duplicate":
            data["jobs"].append(copy.deepcopy(data["jobs"][0]))
        elif damage == "bad-resource":
            data["jobs"][0]["job_resources"] = []
        elif damage == "bad-gpu":
            data["jobs"][0]["gres_detail"] = ["gpu:a100:2(IDX:3)", ""]
        elif damage == "bad-topology":
            data["jobs"][0]["job_resources"] = {"nodes": {"allocation": []}, "allocated_nodes": []}
        else:
            data["jobs"][0]["job_resources"] = {}

    mutate(json_files, "jobs", change)
    with pytest.raises(ValueError):
        backend(json_files)


def test_empty_output_is_capability_failure_but_empty_json_cluster_is_not(json_files):
    Path(json_files["jobs_json"]).write_text("")
    with pytest.raises(UnsupportedSlurmJSON, match="slurm-json-squeue"):
        backend(json_files)


def test_anonymization_preserves_references_without_names_or_gpu_types(json_files):
    system = backend(json_files)
    original = system.get_cluster_state(system.options)
    masked, _ = anonymize_state(original)
    validate_cluster_state(masked)
    assert not any(value in json.dumps(masked) for value in ("alice", "bob", "node01", "a100", '"debug"'))
    assert original["jobs"][0]["user"] == "alice"


@pytest.mark.parametrize("all_partitions", [False, True], ids=["visible", "all"])
@pytest.mark.parametrize("squeue", [False, True], ids=["scontrol", "squeue"])
def test_live_commands_respect_all_and_jobs_override(all_partitions, squeue):
    options = SimpleNamespace(ALL_PARTITIONS=all_partitions, SLURM_JSON_SQUEUE=squeue, SOURCEDIR=None)
    for key in ("partitions_json", "nodes_json", "jobs_json"):
        command = SlurmJSONBatchSystem.collection_command(key, "scontrol --json show " + key[:-5], options)
        assert (" --all" in command) is all_partitions
        assert command.startswith("squeue --json --states=all") is (squeue and key == "jobs_json")
    options.SOURCEDIR = "/saved"
    assert " --all" not in SlurmJSONBatchSystem.collection_command("nodes_json", "scontrol --json show nodes", options)


@pytest.mark.parametrize("value,expected", [(0, 0), (65534, 65534), (65535, 65535), ({"set": True, "number": 3, "infinite": False}, 3)])
def test_json_integers_preserve_real_ids(value, expected):
    from qtop_py.plugins.slurm_json import integer

    assert integer(value) == expected


@pytest.mark.parametrize("value", [None, -1, True, 1.5, "2", {"set": False, "number": 3}, {"set": True, "infinite": True, "number": 3}])
def test_invalid_or_unset_required_json_integers_fail(value):
    from qtop_py.plugins.slurm_json import integer

    with pytest.raises(ValueError):
        integer(value)


@pytest.mark.parametrize("payload", ["[]", '{"meta":null}', '{"meta":{"plugin":{}}}', '{"jobs":[],"jobs":[{}]}', '{"meta":{"plugin":{"data_parser":"v0.0.43"}},"jobs":{}}'])
def test_response_structure_and_duplicate_fields_fail_closed(tmp_path, payload):
    from qtop_py.plugins.slurm_json import read_response

    path = tmp_path / "payload.json"
    path.write_text(payload)
    with pytest.raises(ValueError):
        read_response(str(path), "jobs")


@pytest.mark.parametrize("field,value", [("cpus", True), ("cpus", 65537), ("partitions", [""]), ("state", []), ("gres", 1), ("gres", "gpu:a100:2(IDX:0)")])
def test_node_inventory_errors_are_rejected(json_files, field, value):
    mutate(json_files, "nodes", lambda data: data["nodes"][0].__setitem__(field, value))
    with pytest.raises(ValueError):
        backend(json_files)


@pytest.mark.parametrize("field,value", [("cpus", -1), ("partition", None), ("nodes", "node["), ("user_name", None), ("tres_per_node", []), ("array_job_id", 5)])
def test_job_fields_cannot_drop_allocations(json_files, field, value):
    mutate(json_files, "jobs", lambda data: data["jobs"][0].__setitem__(field, value))
    with pytest.raises(ValueError):
        backend(json_files)


@pytest.mark.parametrize(
    "damage", ["duplicate-node", "missing-node", "schema-mismatch", "bad-topology", "unknown-core-state", "duplicate-cores", "outside-topology", "wrong-total", "incomplete-nodes"]
)
def test_allocation_inconsistencies_reject_the_whole_collection(json_files, damage):
    if damage == "duplicate-node":
        mutate(json_files, "nodes", lambda data: data["nodes"].append(data["nodes"][0]))
    elif damage == "missing-node":
        mutate(json_files, "nodes", lambda data: data["nodes"].pop(0))
    elif damage == "schema-mismatch":
        mutate(json_files, "jobs", lambda data: data["meta"]["plugin"].__setitem__("data_parser", "v0.0.41" if data["meta"]["plugin"]["data_parser"].endswith("40") else "v0.0.40"))
    elif damage == "bad-topology":
        mutate(json_files, "nodes", lambda data: data["nodes"][0].__setitem__("threads", 1000000000))
    else:

        def change(data):
            resource = data["jobs"][0]["job_resources"]
            old = "allocated_nodes" in resource
            allocations = resource["allocated_nodes"] if old else resource["nodes"]["allocation"]
            if damage == "wrong-total":
                if old:
                    resource["allocated_cores"] = 10
                else:
                    resource["cpus"] = 10
            elif damage == "incomplete-nodes":
                allocations.pop()
            elif damage == "unknown-core-state":
                if old:
                    allocations[0]["sockets"]["1"]["cores"]["0"] = "invented"
                else:
                    allocations[0]["sockets"][0]["cores"][0]["status"] = ["INVENTED"]
            elif damage == "duplicate-cores":
                if old:
                    allocations.append(allocations[0])
                else:
                    socket = next(socket for socket in allocations[0]["sockets"] if any("ALLOCATED" in core["status"] for core in socket["cores"]))
                    socket["cores"].append(next(core for core in socket["cores"] if "ALLOCATED" in core["status"]))
            elif old:
                allocations[0]["sockets"] = {"99": {"cores": {"0": "allocated"}}}
            else:
                next(socket for socket in allocations[0]["sockets"] if any("ALLOCATED" in core["status"] for core in socket["cores"]))["index"] = 99

        mutate(json_files, "jobs", change)
    with pytest.raises(ValueError):
        backend(json_files)


def test_partial_smt_is_never_labelled_exact(json_files):
    def change(data):
        job = data["jobs"][0]
        job["nodes"], job["cpus"], job["gres_detail"] = "node01", 1, []
        resource = job["job_resources"]
        if "allocated_nodes" in resource:
            resource["allocated_nodes"] = resource["allocated_nodes"][:1]
            resource["allocated_cores"] = 1
        else:
            resource["nodes"]["allocation"] = resource["nodes"]["allocation"][:1]
            resource["nodes"]["allocation"][0]["cpus"]["count"] = resource["cpus"] = 1

    mutate(json_files, "jobs", change)
    system = backend(json_files)
    node = system.get_cluster_state(system.options)["nodes"][0]
    assert node["cpu_allocation_source"] == "estimated"
    assert node["core_job_map"][4] == "80+1"
    assert 5 not in node["core_job_map"]


def test_unavailable_details_are_explicitly_estimated_and_flags_are_retained(json_files):
    def change(data):
        data["jobs"][0]["job_resources"] = None
        data["jobs"][0]["job_state"] = ["RUNNING", "COMPLETING"]

    mutate(json_files, "jobs", change)
    system = backend(json_files)
    state = system.get_cluster_state(SimpleNamespace(ANONYMIZE=True))
    assert state["jobs"][0]["state"] == "RUNNING+COMPLETING"
    assert state["nodes"][0]["cpu_allocation_source"] == "estimated"
    assert "alice" not in str(state)


def test_source_json_is_omitted_instead_of_leaking_unknown_fields(json_files):
    with pytest.raises(ValueError, match="portable"):
        backend(json_files).mask_source_trace("jobs_json", "unreviewed names", None)


def test_null_optional_inventory_does_not_invent_gpu_counts(json_files):
    def change(data):
        data["nodes"][2].update(partitions=None, gres=None, gres_used=None)

    mutate(json_files, "nodes", change)
    system = backend(json_files)
    node = system.get_cluster_state(system.options)["nodes"][2]
    assert node["qname"] == []
    assert "slurm_gpu_configured" not in node and "slurm_gpu_used" not in node


def test_nonintegral_core_indices_cannot_be_truncated(json_files):
    def change(data):
        resource = data["jobs"][0]["job_resources"]
        if "allocated_nodes" in resource:
            resource["allocated_nodes"][0]["sockets"]["1"]["cores"] = {"0.5": "allocated"}
        else:
            next(core for socket in resource["nodes"]["allocation"][0]["sockets"] for core in socket["cores"] if "ALLOCATED" in core["status"])["index"] = 0.5

    mutate(json_files, "jobs", change)
    with pytest.raises(ValueError):
        backend(json_files)


@pytest.mark.parametrize(
    "state,expected",
    [
        ("RUNNING+COMPLETING", "CG"),
        ("PENDING+CONFIGURING", "CF"),
        ("RUNNING+STAGE_OUT+COMPLETING", "CG"),
        ("RUNNING+LAUNCH_FAILED", "R"),
        ("SUSPENDED", "S"),
        ("FUTURE_STATE", "FUTURE_STATE"),
    ],
)
def test_slurm_state_flag_precedence_matches_legacy_display(state, expected):
    assert SlurmJSONBatchSystem.display_job_state(state) == expected


def test_squeue_override_keeps_a_configured_parser_version():
    options = SimpleNamespace(SLURM_JSON_SQUEUE=True, ALL_PARTITIONS=True, SOURCEDIR=None)
    assert SlurmJSONBatchSystem.collection_command("jobs_json", "scontrol --json=v0.0.43 show jobs", options) == "squeue --json=v0.0.43 --states=all --all"


def test_nonresponding_nodes_are_not_displayed_as_healthy(json_files):
    mutate(json_files, "nodes", lambda data: data["nodes"][2].__setitem__("state", ["IDLE", "NOT_RESPONDING"]))
    system = backend(json_files)
    assert system.get_cluster_state(system.options)["nodes"][2]["state"] == "d"


def test_exact_cpu_assignments_take_priority_over_estimates(json_files):
    mutate(json_files, "jobs", lambda data: data["jobs"][0].__setitem__("job_resources", None))
    system = backend(json_files)
    node = system.get_cluster_state(system.options)["nodes"][0]
    assert node["core_job_map"][0] == node["core_job_map"][1] == "80+0"
    assert node["cpu_allocation_source"] == "estimated"
    assert sum(value == "80+1" for value in node["core_job_map"].values()) == 3


def test_missing_node_cannot_lose_a_reported_gpu_binding(json_files):
    mutate(json_files, "jobs", lambda data: data["jobs"][0].__setitem__("job_resources", None))
    mutate(json_files, "nodes", lambda data: data["nodes"].pop(0))
    with pytest.raises(ValueError, match="missing node"):
        backend(json_files)


@pytest.mark.parametrize("estimated", (False, True), ids=("overlapping-exact", "estimated-overcapacity"))
def test_unrepresentable_cpu_allocations_cannot_be_silently_dropped(json_files, estimated):
    def change(data):
        job = copy.deepcopy(data["jobs"][0])
        job.update(job_id=90, het_job_id=0)
        if estimated:
            job.update(job_resources=None, cpus=200)
        data["jobs"].append(job)

    mutate(json_files, "jobs", change)
    system = backend(json_files)
    with pytest.raises(ValueError, match="overlap or exceed"):
        system.get_cluster_state(system.options)


@pytest.mark.parametrize("version", ["v0.0.44", "v0.0.45", "v0.0.46"])
def test_version_specific_fixtures_preserve_replay_and_allocation_flags(version, tmp_path):
    from qtop_py.snapshot import Snapshot

    files = {key + "_json": str(SAMPLES / version / ("slurm_" + key + ".json")) for key in ("jobs", "nodes", "partitions")}
    system = backend(files)
    for job in system._jobs:
        assert job["JobId"] in {"80+0", "80+1", "42_7"}
    original = system.get_cluster_state(system.options)
    reference = backend({key + "_json": str(SAMPLES / "v0.0.43" / ("slurm_" + key + ".json")) for key in ("jobs", "nodes", "partitions")})
    assert original == reference.get_cluster_state(reference.options)
    path = tmp_path / "state.json"
    Snapshot.from_cluster_state(original).save(str(path))
    restored = Snapshot.from_cluster_state(json.loads(path.read_text()))
    assert json.loads(json.dumps(restored.to_cluster_state())) == json.loads(json.dumps(original))
    masked, _ = anonymize_state(original)
    assert Snapshot.from_cluster_state(masked).total_queued_jobs == restored.total_queued_jobs == 1
