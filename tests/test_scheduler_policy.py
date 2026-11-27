## Copyright (c) 2026 qtop contributors
## SPDX-License-Identifier: MIT
"""A new plugin supplies policy without editing the core runtime."""

import io
import tarfile
from pathlib import Path
from types import SimpleNamespace

import pytest

from qtop_py import core_process as process_io
from qtop_py import qtop, user_details
from qtop_py.core_errors import OperationalError
from qtop_py.core_queue_nodes import queue_node_counts
from qtop_py.core_serialiser import GenericBatchSystem
from qtop_py.mvu import MVU_Model
from qtop_py.schedulers import collector, registry
from qtop_py.ui import mvu_view


class CustomBackend(GenericBatchSystem):
    optional_trace_keys = ("detail_file",)
    optional_trace_aliases = {"detail_file": "native-details.txt"}
    node_job_fields = ("custom_job_ids",)
    detection_priority = -10
    blocked_queue_note = " (custom queue policy)"

    @staticmethod
    def get_mnemonic():
        return "custom"

    @staticmethod
    def collection_command(key, command, options):
        return command + " --custom-policy"

    @staticmethod
    def queue_is_blocked(state):
        return state == "custom-closed"

    @staticmethod
    def display_messages(section, snapshot, nodes, config):
        return ["INFO: custom " + section]


def test_plugin_controls_collection_optional_inputs_and_queue_policy(monkeypatch, tmp_path, capsys):
    monkeypatch.setattr(qtop, "PLUGIN_BATCH_SYSTEMS", (CustomBackend,))
    options = SimpleNamespace(SOURCEDIR=str(tmp_path), workdir=str(tmp_path))
    config = {"schedulers": {"custom": {"main_file": "%(savepath)s/main.txt, native", "detail_file": "%(savepath)s/detail.txt, details"}}}
    (tmp_path / "main.txt").write_text("native trace")
    (tmp_path / "native-details.txt").write_text("native optional detail")
    commands = collector.finalize_filepaths_schedulercommands(options, config, CustomBackend)
    assert commands["main_file"][1] == "native --custom-policy"
    paths = collector.get_input_filenames(commands, config, options, CustomBackend)
    assert paths["detail_file"] == str(tmp_path / "native-details.txt")
    assert qtop.queue_is_blocked("custom-closed", "custom")
    assert not qtop.queue_is_blocked("D", "custom")
    assert qtop.node_job_ids({"core_job_map": {0: "1"}, "custom_job_ids": ["2"]}) == {"1", "2"}
    nodes = CustomBackend.ensure_worker_nodes_have_qnames([{"core_job_map": {}, "custom_job_ids": ["2"]}], ["2"], ["batch"])
    assert nodes[0]["qname"] == ["batch"]
    monkeypatch.setattr(CustomBackend, "native_queue_membership", True)
    idle = dict(domainname="node1", qname=["batch"], state="-", core_job_map={})
    assert queue_node_counts([idle], ["batch"], CustomBackend) == [("batch", 1, 1)]
    model = MVU_Model(config, options, None, backend=CustomBackend)
    display = mvu_view.TextDisplay(None, config, None, None, SimpleNamespace(worker_nodes=[]), options, MVU_model=model)
    display.display_backend_messages()
    assert capsys.readouterr().out.splitlines() == [
        "Scheduler messages:",
        "INFO: custom snapshot",
        "INFO: custom accounting",
        "INFO: custom placement",
        "INFO: custom resources",
        "",
    ]


def test_plugin_controls_optional_live_failure_and_detection(monkeypatch, tmp_path, caplog):
    monkeypatch.setattr(registry, "PLUGIN_BATCH_SYSTEMS", (GenericCompetitor, CustomBackend))
    monkeypatch.setattr(registry.shutil, "which", lambda command: "/usr/bin/" + command)
    assert registry.auto_get_avail_batch_system({"signature_commands": {"competitor": "other", "custom": "native"}}, registry.discover_qtop_batch_systems()) == "custom"

    def fail(*args, **kwargs):
        raise qtop.OperationalError("optional client unavailable")

    monkeypatch.setattr(process_io, "execute_shell_batch_commands", fail)
    assert collector.get_input_filenames({"detail_file": (str(tmp_path / "missing"), "details")}, {"savepath": str(tmp_path)}, SimpleNamespace(SOURCEDIR=None), CustomBackend) == {}
    assert "optional client unavailable" in caplog.text


class GenericCompetitor(GenericBatchSystem):
    @staticmethod
    def get_mnemonic():
        return "competitor"


@pytest.mark.parametrize("runtime_state", ["absent", "unrelated"])
def test_collection_uses_explicit_inputs_without_runtime_initialization(monkeypatch, tmp_path, runtime_state):
    for name in ("args", "config", "scheduler"):
        if runtime_state == "absent":
            monkeypatch.delattr(qtop, name, raising=False)
        else:
            monkeypatch.setattr(qtop, name, object(), raising=False)
    options = SimpleNamespace(SOURCEDIR=str(tmp_path), workdir=str(tmp_path))
    config = {"schedulers": {"custom": {"main_file": "%(savepath)s/main.txt, native"}}}
    trace = tmp_path / "main.txt"
    trace.write_text("independent collection")
    assert collector.fetch_scheduler_files(options, config, CustomBackend) == {"main_file": str(trace)}
    monkeypatch.setattr(registry.shutil, "which", lambda command: "/bin/native")
    assert registry.auto_get_avail_batch_system({"signature_commands": {"custom": "native"}}, {"custom": CustomBackend}) == "custom"
    assert user_details.get_detail_of_name([], {"extract_info": None}, SimpleNamespace(GET_GECOS=False)) == {}


@pytest.mark.parametrize(
    "entries, cause",
    [
        (None, "expected a mapping"),
        ({"main_file": "no separator"}, "main_file: expected"),
        ({"main_file": "%(home)s/main.txt, native"}, "KeyError"),
        ({"main_file": 7}, "AttributeError"),
    ],
    ids=["missing", "no-separator", "unknown-placeholder", "not-text"],
)
def test_malformed_command_entries_are_configuration_errors(tmp_path, entries, cause):
    options = SimpleNamespace(SOURCEDIR=str(tmp_path), workdir=str(tmp_path))
    config = {"schedulers": {} if entries is None else {"custom": entries}}
    with pytest.raises(OperationalError, match="Invalid scheduler command configuration for custom") as error:
        collector.fetch_scheduler_files(options, config, CustomBackend)
    assert cause in str(error.value)


def test_backend_bugs_in_collection_command_are_not_configuration_errors(monkeypatch, tmp_path):
    def broken(key, command, options):
        raise KeyError("backend bug")

    monkeypatch.setattr(CustomBackend, "collection_command", staticmethod(broken))
    options = SimpleNamespace(SOURCEDIR=str(tmp_path), workdir=str(tmp_path))
    with pytest.raises(KeyError, match="backend bug"):
        collector.fetch_scheduler_files(options, {"schedulers": {"custom": {"main_file": "%(savepath)s/m, native"}}}, CustomBackend)


def test_archive_collection_uses_explicit_backend_and_caches_selected_set(monkeypatch, tmp_path):
    monkeypatch.setattr(qtop, "scheduler", "unrelated", raising=False)
    output = tmp_path / "replay"
    output.mkdir(mode=0o700)  # as qtop's own mkdtemp workspace
    archive = tmp_path / "native.tar"
    with tarfile.open(str(archive), "w") as tar:
        entry = tarfile.TarInfo("capture/main.txt")
        entry.size = len(b"native trace")
        tar.addfile(entry, io.BytesIO(b"native trace"))
    options = SimpleNamespace(SOURCEDIR=str(output), workdir=str(output), source_archive=str(archive))
    config = {"schedulers": {"custom": {"main_file": "%(savepath)s/main.txt, native", "detail_file": "%(savepath)s/detail.txt, details"}}}
    paths = collector.fetch_scheduler_files(options, config, CustomBackend)
    assert Path(paths["main_file"]).read_text() == "native trace"
    assert "detail_file" not in paths
    archive.unlink()
    assert collector.fetch_scheduler_files(options, config, CustomBackend) == paths
    paths.clear()
    assert options.archive_trace_paths


@pytest.mark.parametrize("native_available", [False, True])
def test_detection_only_considers_explicit_registered_backends(monkeypatch, native_available):
    monkeypatch.setattr(registry.shutil, "which", lambda command: "/bin/" + command if command == "echo" or native_available else None)
    config = {"signature_commands": {"unregistered": "echo", "custom": "native"}}
    if native_available:
        assert registry.auto_get_avail_batch_system(config, {"custom": CustomBackend}) == "custom"
    else:
        with pytest.raises(qtop.SchedulerNotSpecified):
            registry.auto_get_avail_batch_system(config, {"custom": CustomBackend})


@pytest.mark.parametrize(
    "nodes, levels",
    (
        ([], []),
        ([{"cpu_allocation_source": "estimated"}], ["WARNING"]),
        ([{"cpu_allocation_source": "slurm"}], ["INFO"]),
        ([{"cpu_allocation_source": "estimated", "slurm_unmapped_cpus": 2}, {"cpu_allocation_source": "slurm"}], ["WARNING", "INFO", "WARNING"]),
    ),
)
def test_slurm_placement_message_severity(nodes, levels):
    messages = qtop.scheduler_backend("slurm").display_messages("placement", SimpleNamespace(), nodes, {})
    assert [message.split(":", 1)[0] for message in messages] == levels


@pytest.mark.parametrize(
    "sample, anonymize, load_sample, expected",
    [(0, False, None, True), (1, False, None, True), (0, True, None, True), (1, True, None, False), (1, True, "replay.tar", True)],
    ids=["plain", "sample", "anonymize", "masked-sample", "replayed-sample"],
)
def test_user_detail_options_are_off_only_while_saving_a_masked_sample(sample, anonymize, load_sample, expected):
    options = SimpleNamespace(GET_GECOS=True, SHOW_GROUPS=True, SAMPLE=sample, ANONYMIZE=anonymize, load_sample=load_sample)
    assert user_details.user_detail_option(options, "GET_GECOS") is expected
    assert user_details.user_detail_option(options, "SHOW_GROUPS") is expected
    assert options.GET_GECOS is options.SHOW_GROUPS is True  # derived, never written back
    assert user_details.user_detail_option(SimpleNamespace(), "SHOW_GROUPS") is False
