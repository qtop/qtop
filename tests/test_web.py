## Copyright (c) 2026 qtop contributors
## SPDX-License-Identifier: MIT

import multiprocessing
import socket
import time
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import ProxyHandler, Request, build_opener

import pytest

from qtop_py import web as web_module


@pytest.mark.parametrize("host", ["127.0.0.1", "0.0.0.0"])
def test_spawned_web_worker_serves_filename_with_spaces_and_stops(tmp_path, monkeypatch, host):
    context = multiprocessing.get_context("spawn")
    monkeypatch.setattr(web_module, "Process", context.Process)
    monkeypatch.setattr(web_module, "Queue", context.Queue)
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        port = listener.getsockname()[1]
    source = tmp_path / "state with spaces.json"
    source.write_bytes(b'{"jobs": []}\n')
    web = web_module.Web(str(tmp_path), port=port, host=host)
    opener = build_opener(ProxyHandler({}))
    try:
        web.start()
        web.set_filename(str(source))
        deadline = time.monotonic() + 8
        while True:
            try:
                with opener.open("http://127.0.0.1:%s/api.json" % port, timeout=0.5) as response:
                    assert response.read() == source.read_bytes()
                    break
            except URLError:
                if time.monotonic() >= deadline or web.server.exitcode is not None:
                    raise
                time.sleep(0.02)
    finally:
        web.stop()
    assert web.server.exitcode == 0
    assert not web.started
    web.stop()


def test_web_preserves_local_assets_and_falls_back_to_bundled_assets(tmp_path):
    bundled = web_module.Web(str(tmp_path))
    assert (Path(bundled.web_dir) / "index.html").is_file()
    custom = tmp_path / "web"
    custom.mkdir()
    assert Path(web_module.Web(str(tmp_path)).web_dir) == custom


def test_web_serves_only_the_page_and_api_json(tmp_path):
    custom = tmp_path / "web"
    (custom / "sub").mkdir(parents=True)
    (custom / "index.html").write_text("custom page")
    (custom / "notes.txt").write_text("notes")
    (custom / "sub" / "deep.txt").write_text("nested")
    (tmp_path / "outside.txt").write_text("outside")
    (custom / "linked").symlink_to(tmp_path / "outside.txt")
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        port = listener.getsockname()[1]
    web = web_module.Web(str(tmp_path), port=port)
    opener = build_opener(ProxyHandler({}))

    def status(path, method="GET"):
        try:
            with opener.open(Request("http://127.0.0.1:%s%s" % (port, path), method=method), timeout=2) as response:
                return response.status, response.read()
        except HTTPError as error:
            return error.code, b""

    try:
        web.start()
        deadline = time.monotonic() + 8
        while True:
            try:
                assert status("/") == (200, b"custom page")
                break
            except URLError:
                if time.monotonic() >= deadline:
                    raise
                time.sleep(0.02)
        assert status("/index.html") == (200, b"custom page")
        assert status("/", "HEAD")[0] == 200
        for path in ("/notes.txt", "/linked", "/sub/", "/sub/deep.txt", "/api.json", "/../outside.txt"):
            assert status(path)[0] == 404, path
            assert status(path, "HEAD")[0] == 404, path
    finally:
        web.stop()
