## SPDX-License-Identifier: MIT
"""Own run resources independently of collection and rendering policy."""

import contextlib
import logging
import os
import sys
from collections import OrderedDict

from qtop_py import fileutils, sample
from qtop_py.constants import QTOPCONF_YAML


class RunResources(object):
    def __init__(self):
        self.stdout = sys.stdout
        self.initial_cwd = os.getcwd()
        self.options = None
        self.web = None
        self.logfile = None
        self.archive = None
        self.frame_path = None
        self.trace_paths = {}
        self.capture_paths = OrderedDict()
        self.closed = False

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if self.closed:
            return False
        self.closed = True
        self._cleanup((self.restore_stdout, self.stop_web, self.close_capture, self.restore_cwd, self.cleanup_workspace, self.report_captures), exc_type is not None)
        return False

    def _cleanup(self, callbacks, failed):
        failures = []
        for cleanup in callbacks:
            try:
                cleanup()
            except BaseException as error:
                failures.append(error)
                if failed:
                    logging.warning("Cleanup failed: %s", error)
        if not failed and failures:
            raise failures[0]

    @contextlib.contextmanager
    def frame(self, savepath, timestr):
        handle, path = fileutils.get_new_temp_file(savepath, prefix="qtop_fullview_%s_" % timestr, suffix=".out")
        self.frame_path = path
        try:
            stream = os.fdopen(handle, "w")
        except BaseException:
            os.close(handle)
            raise
        try:
            with stream, contextlib.redirect_stdout(stream):
                yield path
        finally:
            self._cleanup(
                (lambda: fileutils.add_to_sample([path], self.archive) if self.archive is not None else None, self.close_capture),
                sys.exc_info()[0] is not None,
            )

    def discard_frame(self):
        path, self.frame_path = self.frame_path, None
        if path is not None:
            os.unlink(path)

    def start_capture(self, savepath, filename, trace_paths, config, package_dir):
        self.close_capture()
        self.trace_paths = trace_paths.copy()
        options = self.options
        if options is None:
            raise RuntimeError("Run options must be bound before capture")
        if not options.SAMPLE:
            return
        self.archive = fileutils.init_sample_file(options, savepath, filename, trace_paths, QTOPCONF_YAML, package_dir)
        self.capture_paths[self.archive.name] = "Sample files saved in"
        if options.SAMPLE >= 2 and not options.ANONYMIZE:
            sample.add_json(self.archive, "qtop-effective-config.json", config)
        if options.raw_companion:
            companion = fileutils.save_raw_companion(savepath, trace_paths)
            self.capture_paths[companion] = "PRIVATE unmasked scheduler traces:"

    def close_capture(self):
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

    def restore_stdout(self):
        if sys.stdout is not self.stdout:
            with contextlib.suppress(OSError):
                sys.stdout.close()
            sys.stdout = self.stdout

    def stop_web(self):
        if self.web is not None:
            self.web.stop()

    def restore_cwd(self):
        os.chdir(self.initial_cwd)

    def cleanup_workspace(self):
        options = self.options
        workspace = getattr(options, "source_workspace", None)
        if options is not None and workspace is not None:
            workspace.cleanup()
            options.source_workspace = None

    def report_captures(self):
        for path, label in self.capture_paths.items():
            sys.stderr.write("%s %s\n" % (label, path))
