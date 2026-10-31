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
from http.server import SimpleHTTPRequestHandler
from multiprocessing import Process, Queue
from socketserver import TCPServer
from threading import Thread


class Web(object):
    def __init__(self, initial_cwd, port=8080):
        self.web_dir = os.path.join(initial_cwd, "web")
        if not os.path.isdir(self.web_dir):
            self.web_dir = os.path.join(os.path.dirname(__file__), "web")
        self.port = port
        self.started = False
        self.filename = None

    def start(self):
        self.q = Queue()
        self.server = Process(target=self._http_server, args=(self.web_dir, self.port, self.q))
        self.server.start()
        self.started = True

    @staticmethod
    def _http_server(web_dir, port, q):
        os.chdir(web_dir)

        class SharedVars(object):
            def __init__(self):
                self.filename = ""

        shared = SharedVars()

        class MyHandler(SimpleHTTPRequestHandler):
            """Overriding the default behaviour just for api.json"""

            def log_message(self, format, *args):
                """quiet!"""
                pass

            def do_GET(self):
                # Handle the case of api.json...
                if self.path == "/api.json" and shared.filename:
                    with open(shared.filename, "rb") as f:
                        read_data = f.read()

                    self.send_response(200)
                    self.send_header("Content-Type", "application/json")
                    self.send_header("Content-Length", str(len(read_data)))
                    self.end_headers()
                    self.wfile.write(read_data)
                else:
                    # Default behaviour
                    SimpleHTTPRequestHandler.do_GET(self)

        class MyTCPServer(TCPServer):
            # Allow restarting the server while previous connections finish closing.
            allow_reuse_address = True

        httpd = MyTCPServer(("", port), MyHandler)
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
            elif command.startswith("setfilename "):
                # Setting the shared filename that will
                # be picked up by server's thread
                shared.filename = command.partition(" ")[2]

    def set_filename(self, filename):
        if self.started:
            self.q.put("setfilename " + filename)

    def stop(self):
        if self.started:
            self.q.put("stop")
            self.server.join(timeout=5)
            if self.server.is_alive():
                self.server.terminate()
                self.server.join(timeout=5)
            self.q.cancel_join_thread()
            self.q.close()
            self.started = False
