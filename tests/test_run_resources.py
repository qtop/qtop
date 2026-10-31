## SPDX-License-Identifier: MIT
"""Run resource ownership, including failure and repeated refresh paths."""

import os
import sys
import tarfile
import tempfile
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from qtop_py.runtime import RunResources


@pytest.mark.parametrize("failure", [None, ValueError, KeyboardInterrupt])
def test_frame_closes_only_owned_output_and_restores_cwd(tmp_path, failure):
    original, stderr = sys.stdout, sys.stderr
    directory = os.getcwd()
    streams = []
    paths = []
    web = Mock()

    def run():
        with RunResources() as resources:
            resources.web = web
            os.chdir(str(tmp_path))
            with resources.frame(str(tmp_path), "test") as path:
                paths.append(path)
                streams.append(sys.stdout)
                print("frame content")
                if failure:
                    raise failure("interrupted frame")

    if failure:
        with pytest.raises(failure, match="interrupted frame"):
            run()
    else:
        run()
    assert sys.stdout is original and not original.closed
    assert sys.stderr is stderr and not stderr.closed
    assert streams[0].closed
    assert os.getcwd() == directory
    assert Path(paths[0]).read_text() == "frame content\n"
    web.stop.assert_called_once_with()


def test_capture_is_complete_and_closed_before_next_refresh_overwrites_traces(tmp_path, capsys):
    trace = tmp_path / "trace.txt"
    log = tmp_path / "qtop.log"
    log.write_text("diagnostic log")
    archives = []
    with RunResources() as resources:
        resources.options = SimpleNamespace(SAMPLE=1, ANONYMIZE=False, raw_companion=False)
        resources.logfile = str(log)
        for number in (1, 2):
            trace.write_text("trace %s" % number)
            with resources.frame(str(tmp_path), str(number)):
                resources.start_capture(str(tmp_path), "capture-%s.tar" % number, {"trace": str(trace)}, {}, str(tmp_path))
                archives.append(resources.archive)
                print("frame %s" % number)
            assert archives[-1].closed
            assert resources.archive is None
    for number in (1, 2):
        with tarfile.open(str(tmp_path / ("capture-%s.tar" % number))) as archive:
            assert archive.extractfile("trace.txt").read() == ("trace %s" % number).encode()
            frame = next(name for name in archive.getnames() if name.startswith("qtop_fullview_"))
            assert archive.extractfile(frame).read() == ("frame %s\n" % number).encode()
            assert archive.extractfile("qtop.log").read() == b"diagnostic log"
    assert capsys.readouterr().err.splitlines() == ["Sample files saved in " + str(tmp_path / ("capture-%s.tar" % number)) for number in (1, 2)]


@pytest.mark.parametrize("primary_failure", [False, True])
@pytest.mark.parametrize("failed_cleanup", ["web", "archive"])
def test_cleanup_failures_do_not_skip_other_resources_or_hide_primary_error(tmp_path, caplog, primary_failure, failed_cleanup):
    original = sys.stdout
    resources = RunResources()
    workspace = tempfile.TemporaryDirectory(dir=str(tmp_path))
    resources.options = SimpleNamespace(ANONYMIZE=True, source_workspace=workspace)
    web, archive = Mock(), Mock()
    resources.web, resources.archive = web, archive
    target = web.stop if failed_cleanup == "web" else archive.close
    target.side_effect = RuntimeError("cleanup failure")
    expected = ValueError if primary_failure else RuntimeError
    with pytest.raises(expected, match="primary failure" if primary_failure else "cleanup failure"):
        with resources:
            os.chdir(workspace.name)
            if primary_failure:
                raise ValueError("primary failure")
    assert sys.stdout is original and not original.closed
    assert os.getcwd() == resources.initial_cwd
    assert not os.path.exists(workspace.name)
    web.stop.assert_called_once_with()
    archive.close.assert_called_once_with()
    resources.__exit__(None, None, None)
    web.stop.assert_called_once_with()
    archive.close.assert_called_once_with()
    if primary_failure:
        assert "Cleanup failed" in caplog.text


def test_owner_discards_only_completed_intermediate_frames(tmp_path):
    with RunResources() as resources:
        with resources.frame(str(tmp_path), "intermediate") as intermediate:
            print("intermediate")
        resources.discard_frame()
        assert not os.path.exists(intermediate)
        resources.discard_frame()
        with resources.frame(str(tmp_path), "final") as final:
            print("final")
    assert Path(final).read_text() == "final\n"


def test_frame_allocation_closes_descriptor_when_opening_stream_fails(tmp_path, monkeypatch):
    import qtop_py.runtime as runtime

    handle, path = tempfile.mkstemp(dir=str(tmp_path))
    monkeypatch.setattr(runtime.fileutils, "get_new_temp_file", lambda *args, **kwargs: (handle, path))

    def fail(*args, **kwargs):
        raise OSError("cannot open frame stream")

    monkeypatch.setattr(runtime.os, "fdopen", fail)
    with pytest.raises(OSError, match="cannot open frame stream"):
        with RunResources() as resources:
            with resources.frame(str(tmp_path), "failed"):
                pytest.fail("Frame must not start")
    with pytest.raises(OSError):
        os.fstat(handle)
