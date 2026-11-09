##
## qtop is a tool to monitor queuing systems - https://github.com/qtop/qtop
##
## Copyright (c) 2016 Fotis Georgatos
## Copyright (c) 2016 Sotiris Fragkiskos
## Copyright (c) 2023 Hewlett Packard Enterprise Development LP
##
## SPDX-License-Identifier: MIT
##

import os
from multiprocessing import Process, Queue
from typing import TYPE_CHECKING, Any, Optional

if TYPE_CHECKING:
    import multiprocessing.queues


class Web(object):
    def __init__(self, initial_cwd: str, port: int = 8080, host: str = "127.0.0.1") -> None:
        self.web_dir = os.path.join(initial_cwd, "web")
        if not os.path.isdir(self.web_dir):
            self.web_dir = os.path.join(os.path.dirname(__file__), "web")
        self.port, self.host = port, host
        self.started = False
        self.filename: Optional[str] = None

    def start(self) -> None:
        self.q: "multiprocessing.queues.Queue[Any]" = Queue()
        self.server = Process(target=self._http_server, args=(self.web_dir, self.port, self.q, self.host))
        self.server.start()
        self.started = True

    @staticmethod
    def _http_server(web_dir: str, port: int, q: "multiprocessing.queues.Queue[Any]", host: str = "127.0.0.1") -> None:
        # Imported only in the server process: http.server alone is ~19 ms on every plain run.
        from http.server import SimpleHTTPRequestHandler
        from socketserver import TCPServer
        from threading import Thread

        os.chdir(web_dir)

        class SharedVars(object):
            def __init__(self) -> None:
                self.filename = ""

        shared = SharedVars()

        class MyHandler(SimpleHTTPRequestHandler):
            """Serve only the page and api.json (PySCG-0055); never other files under web_dir."""

            PAGES = ("/", "/index.html")

            def log_message(self, format: str, *args: Any) -> None:
                """quiet!"""

            def do_HEAD(self) -> None:
                if self.path in self.PAGES:
                    self.path = "/index.html"
                    SimpleHTTPRequestHandler.do_HEAD(self)
                else:
                    self.send_error(404)

            def do_GET(self) -> None:
                # Handle the case of api.json...
                if self.path == "/api.json" and shared.filename:
                    with open(shared.filename, "rb") as f:
                        read_data = f.read()

                    self.send_response(200)
                    self.send_header("Content-Type", "application/json")
                    self.send_header("Content-Length", str(len(read_data)))
                    self.end_headers()
                    self.wfile.write(read_data)
                elif self.path in self.PAGES:
                    self.path = "/index.html"
                    SimpleHTTPRequestHandler.do_GET(self)
                else:
                    self.send_error(404)

        class MyTCPServer(TCPServer):
            # Allow restarting the server while previous connections finish closing.
            allow_reuse_address = True

        httpd = MyTCPServer((host, port), MyHandler)
        thread = Thread(target=httpd.serve_forever, args=())
        thread.start()

        # This process accepts stop and setfilename commands.
        while True:
            command = q.get()

            # The "stop" command terminates the server and exits the loop
            if command == "stop":
                httpd.shutdown()
                httpd.server_close()
                thread.join()
                break

            # The "setfilename" command sets the file where "api.json"
            # points to.
            if command.startswith("setfilename "):
                # Setting the shared filename that will
                # be picked up by server's thread
                shared.filename = command.partition(" ")[2]

    def set_filename(self, filename: str) -> None:
        if self.started:
            self.q.put("setfilename " + filename)

    def stop(self) -> None:
        if self.started:
            self.q.put("stop")
            self.server.join(timeout=5)
            if self.server.is_alive():
                self.server.terminate()
                self.server.join(timeout=5)
            self.q.cancel_join_thread()
            self.q.close()
            self.started = False
