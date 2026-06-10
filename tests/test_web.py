##
## qtop is a tool to monitor queuing systems - https://github.com/qtop/qtop
##
## Copyright (c) 2016 Fotis Georgatos
## Copyright (c) 2016 Sotiris Fragkiskos
## Copyright (c) 2023 Hewlett Packard Enterprise Development LP
##
## SPDX-License-Identifier: MIT
##

from qtop_py.web import Web


class TestWebInit:
    def test_init_sets_web_dir(self):
        web = Web("/home/user/project")
        assert web.web_dir == "/home/user/project/web"

    def test_init_started_is_false(self):
        web = Web("/tmp")
        assert web.started is False

    def test_init_filename_is_none(self):
        web = Web("/tmp")
        assert web.filename is None


class TestWebSetFilename:
    def test_set_filename_before_start_is_noop(self):
        web = Web("/tmp")
        web.set_filename("/some/file.json")
        assert web.filename is None

    def test_stop_before_start_is_noop(self):
        web = Web("/tmp")
        web.stop()
        assert web.started is False
