## SPDX-License-Identifier: MIT
"""A new plugin supplies policy without editing the core runtime."""

import io
import tarfile
from pathlib import Path
from types import SimpleNamespace

import pytest

from qtop_py import qtop
from qtop_py.queue_nodes import queue_node_counts
from qtop_py.serialiser import GenericBatchSystem


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
    def display_messages(section, document, nodes, config):
        return ["custom " + section]


def test_plugin_controls_collection_optional_inputs_and_queue_policy(monkeypatch, tmp_path, capsys):
    monkeypatch.setattr(qtop, "PLUGIN_BATCH_SYSTEMS", (CustomBackend,))
    monkeypatch.setattr(qtop, "scheduler", "custom", raising=False)
    options = SimpleNamespace(SOURCEDIR=str(tmp_path), workdir=str(tmp_path))
    monkeypatch.setattr(qtop, "args", options, raising=False)
    config = {"schedulers": {"custom": {"main_file": "%(savepath)s/main.txt, native", "detail_file": "%(savepath)s/detail.txt, details"}}}
    (tmp_path / "main.txt").write_text("native trace")
    (tmp_path / "native-details.txt").write_text("native optional detail")
    commands = qtop.finalize_filepaths_schedulercommands(options, config, CustomBackend)
    assert commands["main_file"][1] == "native --custom-policy"
    paths = qtop.get_input_filenames(commands, config, options, CustomBackend)
    assert paths["detail_file"] == str(tmp_path / "native-details.txt")
    assert qtop.queue_is_blocked("custom-closed", "custom")
    assert not qtop.queue_is_blocked("D", "custom")
    assert qtop.node_job_ids({"core_job_map": {0: "1"}, "custom_job_ids": ["2"]}) == {"1", "2"}
    nodes = CustomBackend.ensure_worker_nodes_have_qnames([{"core_job_map": {}, "custom_job_ids": ["2"]}], ["2"], ["batch"])
    assert nodes[0]["qname"] == ["batch"]
    monkeypatch.setattr(CustomBackend, "native_queue_membership", True)
    idle = dict(domainname="node1", qname=["batch"], state="-", core_job_map={})
    assert queue_node_counts([idle], ["batch"], CustomBackend) == [("batch", 1, 1)]
    display = qtop.TextDisplay(None, config, None, None, SimpleNamespace(worker_nodes=[]), options)
    display.display_backend_messages("snapshot")
    assert capsys.readouterr().out == "custom snapshot\n"


def test_plugin_controls_optional_live_failure_and_detection(monkeypatch, tmp_path, caplog):
    monkeypatch.setattr(qtop, "PLUGIN_BATCH_SYSTEMS", (GenericCompetitor, CustomBackend))
    monkeypatch.setattr(qtop, "scheduler", "custom", raising=False)
    monkeypatch.setattr(qtop, "args", SimpleNamespace(SOURCEDIR=None), raising=False)
    monkeypatch.setattr(qtop.shutil, "which", lambda command: "/usr/bin/" + command)
    assert qtop.auto_get_avail_batch_system({"signature_commands": {"competitor": "other", "custom": "native"}}, qtop.discover_qtop_batch_systems()) == "custom"

    def fail(*args, **kwargs):
        raise qtop.OperationalError("optional client unavailable")

    monkeypatch.setattr(qtop, "execute_shell_batch_commands", fail)
    assert qtop.get_input_filenames({"detail_file": (str(tmp_path / "missing"), "details")}, {"savepath": str(tmp_path)}, SimpleNamespace(SOURCEDIR=None), CustomBackend) == {}
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
    assert qtop.fetch_scheduler_files(options, config, CustomBackend) == {"main_file": str(trace)}
    monkeypatch.setattr(qtop.shutil, "which", lambda command: "/bin/native")
    assert qtop.auto_get_avail_batch_system({"signature_commands": {"custom": "native"}}, {"custom": CustomBackend}) == "custom"
    assert qtop.get_detail_of_name([], {"extract_info": None}, SimpleNamespace(GET_GECOS=False)) == {}


def test_archive_collection_uses_explicit_backend_and_caches_selected_set(monkeypatch, tmp_path):
    monkeypatch.setattr(qtop, "scheduler", "unrelated", raising=False)
    output = tmp_path / "replay"
    output.mkdir()
    archive = tmp_path / "native.tar"
    with tarfile.open(str(archive), "w") as tar:
        entry = tarfile.TarInfo("capture/main.txt")
        entry.size = len(b"native trace")
        tar.addfile(entry, io.BytesIO(b"native trace"))
    options = SimpleNamespace(SOURCEDIR=str(output), workdir=str(output), source_archive=str(archive))
    config = {"schedulers": {"custom": {"main_file": "%(savepath)s/main.txt, native", "detail_file": "%(savepath)s/detail.txt, details"}}}
    paths = qtop.fetch_scheduler_files(options, config, CustomBackend)
    assert Path(paths["main_file"]).read_text() == "native trace"
    assert "detail_file" not in paths
    archive.unlink()
    assert qtop.fetch_scheduler_files(options, config, CustomBackend) == paths
    paths.clear()
    assert options.archive_trace_paths


@pytest.mark.parametrize("native_available", [False, True])
def test_detection_only_considers_explicit_registered_backends(monkeypatch, native_available):
    monkeypatch.setattr(qtop.shutil, "which", lambda command: "/bin/" + command if command == "echo" or native_available else None)
    config = {"signature_commands": {"unregistered": "echo", "custom": "native"}}
    if native_available:
        assert qtop.auto_get_avail_batch_system(config, {"custom": CustomBackend}) == "custom"
    else:
        with pytest.raises(qtop.SchedulerNotSpecified):
            qtop.auto_get_avail_batch_system(config, {"custom": CustomBackend})
