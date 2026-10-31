## SPDX-License-Identifier: MIT
"""A new plugin supplies policy without editing the core runtime."""

from types import SimpleNamespace

from qtop_py import qtop
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
    commands = qtop.finalize_filepaths_schedulercommands(options, config)
    assert commands["main_file"][1] == "native --custom-policy"
    paths = qtop.get_input_filenames(commands, config)
    assert paths["detail_file"] == str(tmp_path / "native-details.txt")
    assert qtop.queue_is_blocked("custom-closed", "custom")
    assert not qtop.queue_is_blocked("D", "custom")
    assert qtop.node_job_ids({"core_job_map": {0: "1"}, "custom_job_ids": ["2"]}) == {"1", "2"}
    nodes = CustomBackend.ensure_worker_nodes_have_qnames([{"core_job_map": {}, "custom_job_ids": ["2"]}], ["2"], ["batch"])
    assert nodes[0]["qname"] == ["batch"]
    display = qtop.TextDisplay(None, config, None, None, SimpleNamespace(worker_nodes=[]), options)
    display.display_backend_messages("snapshot")
    assert capsys.readouterr().out == "custom snapshot\n"


def test_plugin_controls_optional_live_failure_and_detection(monkeypatch, tmp_path, caplog):
    monkeypatch.setattr(qtop, "PLUGIN_BATCH_SYSTEMS", (GenericCompetitor, CustomBackend))
    monkeypatch.setattr(qtop, "scheduler", "custom", raising=False)
    monkeypatch.setattr(qtop, "args", SimpleNamespace(SOURCEDIR=None), raising=False)
    monkeypatch.setattr(qtop.shutil, "which", lambda command: "/usr/bin/" + command)
    assert qtop.auto_get_avail_batch_system({"signature_commands": {"competitor": "other", "custom": "native"}}) == "custom"

    def fail(*args, **kwargs):
        raise qtop.OperationalError("optional client unavailable")

    monkeypatch.setattr(qtop, "execute_shell_batch_commands", fail)
    assert qtop.get_input_filenames({"detail_file": (str(tmp_path / "missing"), "details")}, {"savepath": str(tmp_path)}) == {}
    assert "optional client unavailable" in caplog.text


class GenericCompetitor(GenericBatchSystem):
    @staticmethod
    def get_mnemonic():
        return "competitor"
