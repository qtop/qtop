## Copyright (c) 2026 qtop contributors
## SPDX-License-Identifier: MIT
"""Own run resources independently of collection and rendering policy."""

import contextlib
import logging
import os
import sys
from collections import OrderedDict
from types import TracebackType
from typing import TYPE_CHECKING, Any, Callable, Dict, Generator, Iterable, List, Mapping, Optional, Type

if TYPE_CHECKING:
    import tarfile

    from qtop_py.web import Web

from qtop_py import fileutils, sample
from qtop_py.constants import QTOPCONF_YAML


class RunResources(object):
    def __init__(self) -> None:
        self.stdout = sys.stdout
        self.initial_cwd = os.getcwd()
        self.options: Any = None
        self.web: Optional["Web"] = None
        self.logfile: Optional[str] = None
        self.archive: Optional["tarfile.TarFile"] = None
        self.frame_path: Optional[str] = None
        self.trace_paths: Dict[str, str] = {}
        self.capture_paths: Dict[str, str] = OrderedDict()
        self.closed = False

    def __enter__(self) -> "RunResources":
        return self

    def __exit__(self, exc_type: Optional[Type[BaseException]], exc_value: Optional[BaseException], traceback: Optional[TracebackType]) -> None:
        if self.closed:
            return
        self.closed = True
        self._cleanup((self.restore_stdout, self.stop_web, self.close_capture, self.restore_cwd, self.cleanup_workspace, self.report_captures), exc_type is not None)

    def _cleanup(self, callbacks: Iterable[Callable[[], None]], failed: bool) -> None:
        failures: List[BaseException] = []
        for cleanup in callbacks:
            try:
                cleanup()
            except (Exception, KeyboardInterrupt, SystemExit) as error:
                failures.append(error)
                if failed:
                    logging.warning("Cleanup failed: %s", error)
        if not failed and failures:
            raise failures[0]

    @contextlib.contextmanager
    def frame(self, savepath: str, timestr: str) -> Generator[str, None, None]:
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
        if self.archive is not None:
            fileutils.add_to_sample([self.frame_path], self.archive)

    def discard_frame(self) -> None:
        path, self.frame_path = self.frame_path, None
        if path is not None:
            os.unlink(path)

    def start_capture(self, savepath: str, filename: str, trace_paths: Dict[str, str], config: Mapping[str, Any], package_dir: str) -> None:
        self.close_capture()
        self.trace_paths = trace_paths.copy()
        options = self.options
        if options is None:
            raise RuntimeError("Run options must be bound before capture")
        if not options.SAMPLE:
            return
        self.archive = fileutils.init_sample_file(options, savepath, filename, trace_paths, QTOPCONF_YAML, package_dir)
        self.capture_paths[str(self.archive.name)] = "Sample files saved in"
        if options.SAMPLE >= 2 and not options.ANONYMIZE:
            sample.add_json(self.archive, "qtop-effective-config.json", config)
        if options.raw_companion:
            companion = fileutils.save_raw_companion(savepath, trace_paths)
            self.capture_paths[companion] = "PRIVATE unmasked scheduler traces:"

    def close_capture(self) -> None:
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

    def restore_stdout(self) -> None:
        if sys.stdout is not self.stdout:
            with contextlib.suppress(OSError):
                sys.stdout.close()
            sys.stdout = self.stdout

    def stop_web(self) -> None:
        if self.web is not None:
            self.web.stop()

    def restore_cwd(self) -> None:
        os.chdir(self.initial_cwd)

    def cleanup_workspace(self) -> None:
        options = self.options
        workspace = getattr(options, "source_workspace", None)
        if options is not None and workspace is not None:
            workspace.cleanup()
            options.source_workspace = None

    def report_captures(self) -> None:
        for path, label in self.capture_paths.items():
            sys.stderr.write("%s %s\n" % (label, path))
