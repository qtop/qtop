## Copyright (c) 2026 qtop contributors
## SPDX-License-Identifier: MIT
"""Own run resources independently of collection and rendering policy."""

import contextlib
import datetime
import glob
import logging
import os
import re
import sys
import tarfile
import tempfile
from collections import OrderedDict
from types import TracebackType
from typing import TYPE_CHECKING, Any, Callable, Dict, Generator, Iterable, Iterator, List, Mapping, NamedTuple, Optional, Tuple, Type

from qtop_py import fileutils, sample
from qtop_py.constants import QTOPCONF_YAML
from qtop_py.input_parsing import get_date_obj_from_str
from qtop_py.snapshot import Snapshot
from qtop_py.ui.viewport import terminal_rows

if TYPE_CHECKING:
    from qtop_py.web import Web


RunPaths = NamedTuple("RunPaths", [("initial_cwd", str), ("curpath", str), ("qtoppath", str), ("help_fp", str)])


class RunResources(object):
    """The files, streams and web server one qtop run owns; closing it releases them in order."""

    def __init__(self) -> None:
        self.stdout = sys.stdout
        self.initial_cwd = os.getcwd()
        self.options: Any = None
        self.web: Optional[Web] = None  # function-scope annotations are never evaluated (PEP 526)
        self.logfile: Optional[str] = None
        self.archive: Optional[tarfile.TarFile] = None
        self.frame_path: Optional[str] = None
        self.trace_paths: Dict[str, str] = {}
        self.capture_paths: Dict[str, str] = OrderedDict()
        self.closed = False
        self.poller: Any = None
        self.poller_key: Any = None

    def __enter__(self) -> "RunResources":
        return self

    def __exit__(self, exc_type: Optional[Type[BaseException]], exc_value: Optional[BaseException], traceback: Optional[TracebackType]) -> None:
        if self.closed:
            return
        self.closed = True
        self._cleanup(
            (self.stop_collection, self.restore_stdout, self.stop_web, self.close_capture, self.restore_cwd, self.cleanup_workspace, self.report_captures), exc_type is not None
        )

    def _cleanup(self, callbacks: Iterable[Callable[[], None]], failed: bool) -> None:
        failures: List[BaseException] = []
        for cleanup in callbacks:
            try:
                cleanup()
            except (Exception, KeyboardInterrupt, SystemExit) as error:  # noqa: BLE001 - finish every cleanup, then propagate without masking the primary error.
                failures.append(error)
                if failed:
                    logging.warning("Cleanup failed: %s", error)
        if not failed and failures:
            raise failures[0]

    @contextlib.contextmanager
    def frame(self, savepath: str, timestr: str) -> Generator[str, None, None]:
        """Redirect stdout into a new frame file for one render; archive it, or close the capture on failure."""
        handle, path = fileutils.get_new_temp_file(savepath, prefix="qtop_fullview_%s_" % timestr, suffix=".out")
        self.frame_path = path
        with contextlib.ExitStack() as allocation:
            allocation.callback(os.close, handle)
            stream = os.fdopen(handle, "w")
            allocation.pop_all()
        finished = False
        try:
            with stream, contextlib.redirect_stdout(stream):
                yield path
            finished = True
        finally:
            self._cleanup((self.archive_frame, self.close_capture), not finished)

    def archive_frame(self) -> None:
        """Add the current frame file to the support archive, if one is open."""
        if self.archive is not None and self.frame_path is not None:
            fileutils.add_to_sample([self.frame_path], self.archive)

    def discard_frame(self) -> None:
        """Delete the current frame file."""
        path, self.frame_path = self.frame_path, None
        if path is not None:
            os.unlink(path)

    def start_capture(self, savepath: str, filename: str, trace_paths: Dict[str, str], config: Mapping[str, Any], package_dir: str) -> None:
        """Open the support archive for -L (and the raw companion archive, if asked), replacing any open one."""
        self.close_capture()
        self.trace_paths = trace_paths.copy()
        options = self.options
        if options is None:
            raise RuntimeError("Run options must be bound before capture")
        if not options.SAMPLE:
            return
        archive = fileutils.init_sample_file(options, savepath, filename, trace_paths, QTOPCONF_YAML, package_dir)
        if archive is None:  # only when SAMPLE < 1, excluded above
            return
        self.archive = archive
        self.capture_paths[str(archive.name)] = "Sample files saved in"
        if options.SAMPLE >= 2 and not options.ANONYMIZE:
            sample.add_json(archive, "qtop-effective-config.json", config)
        if options.raw_companion:
            companion = fileutils.save_raw_companion(savepath, trace_paths)
            self.capture_paths[companion] = "PRIVATE unmasked scheduler traces:"

    def close_capture(self) -> None:
        """Add the log and scheduler traces to the support archive unless anonymising, then close it."""
        archive, self.archive = self.archive, None
        if archive is None:
            return
        try:
            options = self.options
            if options is None:
                raise RuntimeError("Run options must be bound before capture")
            if not options.ANONYMIZE:
                if self.logfile is not None:
                    fileutils.add_to_sample([self.logfile], archive)
                fileutils.add_to_sample([path for path in self.trace_paths.values() if os.path.isfile(path)], archive)
        finally:
            archive.close()

    def stop_collection(self) -> None:
        """Stop collection before its captured files or workspace can be released."""
        if self.poller is not None:
            self.poller.close()
            self.poller = None

    def restore_stdout(self) -> None:
        """Close any redirected stdout and restore the original one."""
        if sys.stdout is not self.stdout:
            with contextlib.suppress(OSError):
                sys.stdout.close()
            sys.stdout = self.stdout

    def stop_web(self) -> None:
        """Stop the web server, if one was started."""
        if self.web is not None:
            self.web.stop()

    def restore_cwd(self) -> None:
        """Return to the directory qtop started in."""
        os.chdir(self.initial_cwd)

    def cleanup_workspace(self) -> None:
        """Remove the temporary source workspace, if any."""
        options = self.options
        workspace = getattr(options, "source_workspace", None)
        if options is not None and workspace is not None:
            workspace.cleanup()
            options.source_workspace = None

    def report_captures(self) -> None:
        """Print where the support and raw-trace archives were written."""
        for path, label in self.capture_paths.items():
            sys.stderr.write("%s %s\n" % (label, path))


def get_output_size(max_line_len: int, output_fp: str, max_height: int = 0, term_columns: int = 0) -> Tuple[int, int]:
    """Returns the char dimensions of the entirety of the qtop output file."""
    ansi_escape = re.compile(r"\x1b[^m]*m")  # matches ANSI escape characters

    if not max_height or not max_line_len:
        with open(output_fp, "r", encoding="utf-8", errors="replace") as output_file:
            output_lines = list(terminal_rows(output_file, term_columns))
        if not output_lines:
            raise ValueError("There is no output from qtop *whatsoever*. Weird.")

        if not max_height:
            max_height = len(output_lines)

        if not max_line_len:
            max_line_len = max(len(ansi_escape.sub("", line.strip())) for line in output_lines)

    logging.debug("Total nr of lines: %s", max_height)
    logging.debug("Max line length: %s", max_line_len)

    return max_height, max_line_len


def init_dirs(args: Any, _savepath: str) -> Any:
    """Resolve the -s source (directory or trace archive) and the save path; return the updated options."""
    args.SOURCEDIR = os.path.realpath(args.SOURCEDIR) if args.SOURCEDIR else None
    if args.SOURCEDIR and os.path.isfile(args.SOURCEDIR):
        if tarfile.is_tarfile(args.SOURCEDIR) or args.SOURCEDIR.lower().endswith((".tar", ".tar.gz", ".tgz", ".tar.bz2", ".tbz2", ".tar.xz", ".txz")):
            args.source_archive = args.SOURCEDIR
            args.source_workspace = tempfile.TemporaryDirectory(prefix="qtop-traces-", dir=_savepath)
            args.SOURCEDIR = args.source_workspace.name
        else:
            args.SOURCEDIR = os.path.dirname(args.SOURCEDIR)
    logging.debug("User-defined source directory: %s", args.SOURCEDIR)
    args.workdir = args.SOURCEDIR or _savepath
    logging.debug("Working directory is now: %s", args.workdir)
    # Sonar path finding: -s or savepath is the caller's chosen working directory, by design; see docs/sonar-review.md
    os.chdir(args.workdir)
    return args


def pick_frames_to_replay(_savepath: str, args: Any, config: Dict[str, Any]) -> Tuple[Iterator[str], Any]:
    """Return the saved frames in savepath that -R asks to replay."""
    if args.REPLAY[0] == 0:  # add default arg, if no replay start time is set in the cmdline
        time_delta = fileutils.get_timedelta(fileutils.parse_time_input(config["replay_last"]))
        some_time_ago = datetime.datetime.now() - time_delta
        args.REPLAY[0] = some_time_ago.strftime("%Y%m%dT%H%M%S")
    if len(args.REPLAY) == 1:  # add default arg, if no replay duration is set in the cmdline
        args.REPLAY.append("2m")

    time_delta = fileutils.get_timedelta(fileutils.parse_time_input(args.REPLAY[1]))
    watch_start_datetime_obj = get_date_obj_from_str(args.REPLAY[0], datetime.datetime.now())
    REC_FP_ALL = _savepath + "/*_partview*.out"
    rec_files = glob.iglob(REC_FP_ALL)
    useful_frames: List[str] = []

    for rec_file in rec_files:
        rec_file_last_modified_date = datetime.datetime.strptime(rec_file.rsplit("/", 1)[-1].split("_")[2], "%Y%m%dT%H%M%S")
        if datetime.timedelta(seconds=0) < rec_file_last_modified_date - watch_start_datetime_obj < time_delta:
            useful_frames.append(rec_file)

    return iter(useful_frames[::-1]), args.REPLAY


def MVU_export_snapshot(snapshot: Snapshot, args: Any, savepath: str, timestr: str, web: "Web") -> None:
    """Write the snapshot as JSON for -E or the web view, and point the web server at it."""
    if not (args.EXPORT or args.WEB):
        return
    with tempfile.NamedTemporaryFile(delete=False, prefix="qtop_json_%s_" % timestr, suffix=".json", dir=savepath) as json_file:
        json_filename = json_file.name
    saved = False
    try:
        snapshot.save(json_filename)
        saved = True
    finally:
        if not saved:
            with contextlib.suppress(OSError):
                os.unlink(json_filename)
    if args.WEB:
        web.set_filename(json_filename)
