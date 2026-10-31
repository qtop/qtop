## SPDX-License-Identifier: MIT

import io
import tarfile
from pathlib import Path

import pytest

from qtop_py import trace_archive

CONFIGURED = {key + "_file": "%(savepath)s/" + key + "%(pid)s.txt, ignored" for key in ("squeue", "sinfo", "scontrol")}


def make_archive(path, entries, mode="w"):
    with tarfile.open(str(path), mode) as archive:
        for name, text in entries:
            data = text.encode()
            member = tarfile.TarInfo(name)
            member.size = len(data)
            archive.addfile(member, io.BytesIO(data))


@pytest.mark.parametrize("reverse", (False, True))
@pytest.mark.parametrize("mode", ("w", "w:gz"))
def test_archive_selection_is_coherent_complete_and_order_independent(tmp_path, reverse, mode):
    entries = [
        ("frames/squeue_10.txt", "jobs 10"),
        ("frames/sinfo_10.txt", "nodes 10"),
        ("frames/scontrol_10.txt", "details 10"),
        ("frames/squeue_20.txt", "jobs 20"),
        ("frames/sinfo_20.txt", "nodes 20"),
        ("other/squeue_99.txt", "incomplete"),
    ]
    archive = tmp_path / "input.tar"
    make_archive(archive, list(reversed(entries)) if reverse else entries, mode)
    output = tmp_path / "output"
    output.mkdir()
    paths = trace_archive.load_trace_archive(str(archive), str(output), CONFIGURED, ("scontrol_file",))
    assert {key: Path(path).read_text() for key, path in paths.items()} == {"squeue_file": "jobs 10", "sinfo_file": "nodes 10", "scontrol_file": "details 10"}
    assert sorted(p.name for p in output.iterdir()) == ["scontrol.txt", "sinfo.txt", "squeue.txt"]


@pytest.mark.parametrize("reverse", (False, True))
def test_equal_completeness_uses_lexicographically_last_cohort(tmp_path, reverse):
    entries = [(folder + "/" + name + "_10.txt", folder) for folder in ("a", "z") for name in ("squeue", "sinfo")]
    make_archive(tmp_path / "input.tar", list(reversed(entries)) if reverse else entries)
    (tmp_path / "output").mkdir()
    paths = trace_archive.load_trace_archive(str(tmp_path / "input.tar"), str(tmp_path / "output"), CONFIGURED, ("scontrol_file",))
    assert all(Path(path).read_text() == "z" for path in paths.values())


@pytest.mark.parametrize(
    "entries",
    (
        [("squeue_10.txt", "one"), ("sinfo_20.txt", "two")],
        [("a/squeue.txt", "one"), ("b/sinfo.txt", "two")],
        [("squeue.txt", "one"), ("squeue.txt", "duplicate"), ("sinfo.txt", "two")],
        [("../squeue.txt", "one"), ("../sinfo.txt", "two")],
        [("/squeue.txt", "one"), ("/sinfo.txt", "two")],
        [("C:/squeue.txt", "one"), ("C:/sinfo.txt", "two")],
    ),
)
def test_archive_never_mixes_incomplete_sets_or_uses_unsafe_members(tmp_path, entries):
    make_archive(tmp_path / "input.tar", entries)
    (tmp_path / "output").mkdir()
    with pytest.raises(ValueError, match="no unambiguous complete"):
        trace_archive.load_trace_archive(str(tmp_path / "input.tar"), str(tmp_path / "output"), CONFIGURED, ("scontrol_file",))
    assert list((tmp_path / "output").iterdir()) == []


@pytest.mark.parametrize("kind", (tarfile.SYMTYPE, tarfile.LNKTYPE, tarfile.FIFOTYPE))
def test_archive_does_not_follow_links_or_special_files(tmp_path, kind):
    make_archive(tmp_path / "input.tar", [("sinfo.txt", "nodes")])
    with tarfile.open(str(tmp_path / "input.tar"), "a") as archive:
        member = tarfile.TarInfo("squeue.txt")
        member.type, member.linkname = kind, "sinfo.txt"
        archive.addfile(member)
    (tmp_path / "output").mkdir()
    with pytest.raises(ValueError, match="no unambiguous complete"):
        trace_archive.load_trace_archive(str(tmp_path / "input.tar"), str(tmp_path / "output"), CONFIGURED, ("scontrol_file",))


def test_archive_limits_are_checked_before_materialization(tmp_path, monkeypatch):
    make_archive(tmp_path / "input.tar", [("squeue.txt", "jobs"), ("sinfo.txt", "nodes")])
    (tmp_path / "output").mkdir()
    monkeypatch.setattr(trace_archive, "MAX_TRACE_BYTES", 2)
    with pytest.raises(ValueError, match="size limits"):
        trace_archive.load_trace_archive(str(tmp_path / "input.tar"), str(tmp_path / "output"), CONFIGURED, ("scontrol_file",))
    assert list((tmp_path / "output").iterdir()) == []
