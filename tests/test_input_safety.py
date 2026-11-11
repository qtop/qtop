## Copyright (c) 2026 qtop contributors
## SPDX-License-Identifier: MIT
"""Adversarial data stays inert; resource failures are explicit and bounded."""

import io
import json
import logging
import os
import shlex
import subprocess
import sys
import tarfile
from pathlib import Path
from types import SimpleNamespace
from xml.etree.ElementTree import ParseError

import pytest

from qtop_py import configuration, process, qtop, safety, sample, yaml_parser
from qtop_py.plugins.pbs import PBSStatExtractor
from qtop_py.plugins.sge import SGEStatExtractor
from qtop_py.snapshot import Snapshot


@pytest.mark.parametrize("hostile", ["x\x1b[2J", "x\x1b]52;c;secret\x07", "x\r\nWARNING: forged", "x\x9b2J", "x\u202e", "x\u061c", "x\udcff", "x\\n"])
def test_display_identifiers_are_inert_and_export_remains_original(hostile, monkeypatch):
    state = {
        "jobs": [{"id": "1", "user": hostile, "state": "R", "queue": hostile}],
        "nodes": [{"domainname": hostile, "state": "-", "np": 1, "core_job_map": {0: "1"}, "qname": [hostile]}],
        "queues": [{"name": hostile, "state": "E", "limit": "--", "running": 1, "queued": 0}],
    }
    snapshot = Snapshot.from_cluster_state(state)
    display_name = next(iter(snapshot.queues_dict))
    assert display_name == snapshot.jobs_dict["1"].user_name == snapshot.worker_nodes[0]["domainname"]
    assert not safety._CONTROLS.search(display_name)
    assert snapshot.to_cluster_state() == state
    monkeypatch.setattr(qtop, "args", SimpleNamespace(COLOR="ON"), raising=False)
    coloured = qtop.colorize(display_name, color_func="Red", mapping={"NoPattern": "Red"})
    assert coloured.startswith("\x1b[")
    assert display_name in coloured


def test_escaped_identifiers_do_not_collide_and_unicode_remains_readable():
    assert safety.visible_text("x\n") != safety.visible_text("x\\n")
    assert safety.visible_text("\u00c9lodie") == "\u00c9lodie"
    assert safety.visible_data(1) == 1


@pytest.mark.parametrize("message", ["input\nWARNING: forged", "input\rhidden", "input\x1b[2J", "input\u202e"])
def test_log_formatter_neutralizes_records_without_mutating_them(message):
    record = logging.LogRecord("test", logging.INFO, __file__, 1, "untrusted: %s", (message,), None)
    rendered = safety.SafeLogFormatter("%(levelname)s: %(message)s").format(record)
    assert not safety._CONTROLS.search(rendered)
    assert record.getMessage() == "untrusted: " + message
    assert rendered.startswith("INFO: untrusted: ")


def test_log_formatter_sanitizes_exceptions_and_preserves_copyable_commands():
    try:
        raise ValueError("bad\n\x1b[2J")
    except ValueError:
        record = logging.LogRecord("test", logging.ERROR, __file__, 1, "command: %s", ("C:\\work\\trace",), sys.exc_info())
    text = safety.SafeLogFormatter().format(record)
    assert not safety._CONTROLS.search(text)
    assert "C:\\work\\trace" in text
    assert "ValueError" in text


@pytest.mark.parametrize("encoding", ["utf-8", "utf-16", "utf-32"])
def test_json_limits_allow_quoted_delimiters_and_unicode_encodings(encoding):
    data = {"text": "[" * 100, "unicode": "\u00c9lodie"}
    assert safety.load_json(io.BytesIO(json.dumps(data).encode(encoding))) == data


@pytest.mark.parametrize("value", ["[[[0]]]", '{"a":{"b":{}}}'])
def test_json_nesting_is_rejected_before_deserialization(monkeypatch, value):
    monkeypatch.setattr(safety, "MAX_DEPTH", 2)
    with pytest.raises(safety.ResourceLimitError, match="nesting"):
        safety.load_json(io.StringIO(value))


def test_json_item_and_byte_limits(monkeypatch):
    monkeypatch.setattr(safety, "MAX_ITEMS", 2)
    with pytest.raises(safety.ResourceLimitError, match="item"):
        safety.load_json(io.StringIO("[0,0,0]"))
    with pytest.raises(safety.ResourceLimitError, match="size"):
        safety.load_json(io.StringIO('"\u00e9\u00e9"'), limit=5)
    assert safety.load_json(io.BytesIO(b"[0]"), limit=3) == [0]


@pytest.mark.parametrize("parser", [configuration.literal, yaml_parser.literal_or_original])
def test_literal_limits_cannot_fall_back_to_unparsed_strings(monkeypatch, parser):
    monkeypatch.setattr(safety, "MAX_DEPTH", 2)
    with pytest.raises(safety.ResourceLimitError):
        parser("[[[0]]]")
    monkeypatch.setattr(safety, "MAX_LITERAL_BYTES", 4)
    with pytest.raises(safety.ResourceLimitError):
        parser("12345")
    assert parser("name") == "name"


@pytest.mark.parametrize("kind", ["bytes", "line", "depth"])
def test_yaml_input_limits_at_file_boundary(tmp_path, monkeypatch, kind):
    content = "key: value\n"
    if kind == "bytes":
        monkeypatch.setattr(safety, "MAX_INPUT_BYTES", 5)
    elif kind == "line":
        monkeypatch.setattr(safety, "MAX_LITERAL_BYTES", 5)
    else:
        monkeypatch.setattr(safety, "MAX_DEPTH", 1)
        content = "key:\n    nested: value\n"
    path = tmp_path / "input.yaml"
    path.write_text(content)
    with pytest.raises(safety.ResourceLimitError):
        yaml_parser.parse(str(path))


def test_pbs_json_cannot_bypass_shared_size_limit(tmp_path, monkeypatch):
    monkeypatch.setattr(safety, "MAX_INPUT_BYTES", 20)
    path = tmp_path / "qstat.json"
    path.write_text(json.dumps({"Jobs": {}, "padding": "x" * 100}))
    with pytest.raises(safety.ResourceLimitError):
        PBSStatExtractor({}, SimpleNamespace(ANONYMIZE=False)).extract_qstat(str(path))


@pytest.mark.parametrize("kind", ["bytes", "depth", "elements"])
def test_xml_limits_are_enforced_during_parsing(tmp_path, monkeypatch, kind):
    text = "<root><child/><child/></root>"
    if kind == "bytes":
        monkeypatch.setattr(safety, "MAX_INPUT_BYTES", 8)
    elif kind == "depth":
        monkeypatch.setattr(safety, "MAX_DEPTH", 1)
    else:
        monkeypatch.setattr(safety, "MAX_ITEMS", 2)
    path = tmp_path / "input.xml"
    path.write_text(text)
    extractor = SGEStatExtractor({}, SimpleNamespace(ANONYMIZE=False), {})
    with pytest.raises(ParseError, match="XML input exceeds"):
        extractor.get_xml_tree(str(path))


def make_archive(path, mode, members):
    with tarfile.open(str(path), mode) as archive:
        for name, data in members:
            member = tarfile.TarInfo(name)
            member.size = len(data)
            archive.addfile(member, io.BytesIO(data))


@pytest.mark.parametrize("mode", ["w", "w:gz", "w:bz2", "w:xz"])
def test_snapshot_limits_apply_to_decompressed_data(tmp_path, monkeypatch, mode):
    path = tmp_path / "bomb.tar"
    make_archive(path, mode, [("padding", b"0" * 65536), (sample.SNAPSHOT_NAME, b"{}")])
    monkeypatch.setattr(safety, "MAX_ARCHIVE_BYTES", 4096)
    with pytest.raises(safety.ResourceLimitError, match="decompressed"):
        sample.load_snapshot(str(path))


def test_snapshot_archive_closes_source_on_invalid_header(monkeypatch):
    source = io.BytesIO(b"invalid tar header")
    monkeypatch.setattr(safety, "open", lambda *args: source, raising=False)
    with pytest.raises(tarfile.ReadError):
        with safety.snapshot_archive("input.tar"):
            pytest.fail("Invalid archive was accepted")
    assert source.closed


@pytest.mark.parametrize("first", [b"{}", b"null"])
def test_snapshot_member_limit_and_duplicate_rejection(tmp_path, monkeypatch, first):
    path = tmp_path / "sample.tar"
    make_archive(path, "w", [("a", b""), ("b", b""), (sample.SNAPSHOT_NAME, b"{}")])
    monkeypatch.setattr(safety, "MAX_ARCHIVE_MEMBERS", 2)
    with pytest.raises(safety.ResourceLimitError, match="member"):
        sample.load_snapshot(str(path))
    make_archive(path, "w", [(sample.SNAPSHOT_NAME, first), (sample.SNAPSHOT_NAME, b"{}")])
    with pytest.raises(ValueError, match="duplicate"):
        sample.load_snapshot(str(path))


@pytest.mark.parametrize("channel", ["stdout", "stderr"])
def test_command_output_overflow_preserves_previous_trace_and_cleans_tempfiles(tmp_path, monkeypatch, channel):
    monkeypatch.setattr(process, "MAX_STDOUT_BYTES", 4096)
    monkeypatch.setattr(process, "MAX_STDERR_BYTES", 4096)
    path = tmp_path / "trace.txt"
    path.write_bytes(b"previous snapshot")
    script = "import sys; sys.%s.buffer.write(b'x'*65536)" % channel
    command = " ".join(shlex.quote(arg) for arg in (sys.executable, "-c", script))
    with pytest.raises(qtop.OperationalError, match="output exceeds"):
        qtop.execute_shell_batch_commands({"trace": command}, {"trace": str(path)}, "trace", str(tmp_path))
    assert path.read_bytes() == b"previous snapshot"
    assert list(tmp_path.iterdir()) == [path]


def test_bounded_command_drains_both_pipes_without_deadlock():
    script = "import sys; sys.stderr.buffer.write(b'e'*131072); sys.stdout.buffer.write(b'o'*131072)"
    command = subprocess.Popen([sys.executable, "-c", script], stdout=subprocess.PIPE, stderr=subprocess.PIPE, start_new_session=(os.name == "posix"))
    output, errors = process.communicate_with_timeout(command, 5)
    assert output == b"o" * 131072 and errors == b"e" * 131072
    assert command.returncode == 0


@pytest.mark.parametrize(
    "key,value",
    [("experimental_web_port", 0), ("experimental_web_port", 65536), ("experimental_web_port", True), ("experimental_web_host", ""), ("experimental_web_host", "127.0.0.1\n")],
)
def test_invalid_experimental_web_configuration_is_rejected(key, value):
    config = yaml_parser.parse(str(Path(__file__).resolve().parents[1] / "qtopconf.yaml"))
    config[key] = value
    with pytest.raises(ValueError, match=key):
        configuration.validate_config(config)


@pytest.mark.parametrize("host,port", [("127.0.0.1", 8080), ("0.0.0.0", 8181), ("localhost", "9000")])
def test_experimental_web_settings_accept_explicit_operator_choices(host, port):
    config = yaml_parser.parse(str(Path(__file__).resolve().parents[1] / "qtopconf.yaml"))
    config.update(experimental_web_host=host, experimental_web_port=port)
    configuration.validate_config(config)


def test_literal_node_budget_and_non_string_fallback(monkeypatch):
    value = "[" + ",".join("0" for _ in range(100001)) + "]"
    with pytest.raises(safety.ResourceLimitError, match="item"):
        safety.literal_eval(value)
    assert configuration.literal(3) == 3
    assert yaml_parser.literal_or_original("[1, 2]") == [1, 2]


def test_command_reader_failure_reaps_client():
    class BrokenSink(object):
        def write(self, data):
            raise OSError("disk full")

    command = subprocess.Popen(
        [sys.executable, "-c", "print('output', flush=True); import time; time.sleep(5)"], stdout=subprocess.PIPE, stderr=subprocess.PIPE, start_new_session=(os.name == "posix")
    )
    with pytest.raises(OSError, match="disk full"):
        process.communicate_with_timeout(command, 3, stdout=BrokenSink())
    assert command.poll() is not None


def test_nesting_scanner_handles_long_escaped_and_unterminated_strings():
    text = '"' + '\\"' * 10000
    with pytest.raises(json.JSONDecodeError):
        safety.load_json(io.StringIO(text))
    value = {"text": "\\a\"'[]{}()," * 1000}
    assert safety.load_json(io.StringIO(json.dumps(value))) == value


def test_raw_and_visible_escapes_only_the_display_copy():
    state = {"nodes": [{"domainname": "evil\x1b[2J\\x", "core_job_map": {0: "1" + chr(0x202E)}, "np": 2}], "id\x07": ["a\\b"]}
    raw, safe = safety.raw_and_visible(state)
    assert raw == state and raw is not state and raw["nodes"][0] is not state["nodes"][0]
    assert safe == safety.visible_data(state)
    assert safe["nodes"][0]["domainname"] == "evil\\x1b[2J\\\\x" and "id\\x07" in safe
    raw["nodes"][0]["np"] = 3
    assert safe["nodes"][0]["np"] == 2 and state["nodes"][0]["np"] == 2
