# SPDX-License-Identifier: MIT

import multiprocessing
import socket
import time
from pathlib import Path
from urllib.error import URLError
from urllib.request import ProxyHandler, build_opener

from qtop_py import web as web_module


def test_spawned_web_worker_serves_filename_with_spaces_and_stops(tmp_path, monkeypatch):
    context = multiprocessing.get_context("spawn")
    monkeypatch.setattr(web_module, "Process", context.Process)
    monkeypatch.setattr(web_module, "Queue", context.Queue)
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        port = listener.getsockname()[1]
    source = tmp_path / "state with spaces.json"
    source.write_bytes(b'{"jobs": []}\n')
    web = web_module.Web(str(tmp_path), port=port)
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
