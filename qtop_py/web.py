from SimpleHTTPServer import SimpleHTTPRequestHandler
import SocketServer
from multiprocessing import Process, Queue
from threading import Thread
import os


class Web(object):
    def __init__(self, initial_cwd):
        self.web_dir = initial_cwd + "/web"
        self.started = False
        self.filename = None

    def start(self):
        self.started = True
        self.q = Queue()

        # This will run in another process which will communicate through
        # the `q` Queue.
        def http_server(q):
            os.chdir(self.web_dir)

            class SharedVars(object):
                def __init__(self):
                    self.filename = ""

            shared = SharedVars()

            class MyHandler(SimpleHTTPRequestHandler):
                ''' Overriding the default behaviour just for api.json'''

                def log_message(self, format, *args):
                    '''quiet!'''
                    pass

                def do_GET(self):
                    # Handle the case of api.json...
                    if self.path == "/api.json" and shared.filename:
                        with open(shared.filename, 'r') as f:
                            read_data = f.read()

                        self.send_response(200)
                        self.send_header("Content-type",
                                         "Content-Type: application/json")
                        self.send_header("Content-length", len(read_data))
                        self.end_headers()
                        self.wfile.write(read_data)
                    else:
                        # Default behaviour
                        SimpleHTTPRequestHandler.do_GET(self)

            class MyTCPServer(SocketServer.TCPServer):
                # Undocumented. Getting rid of "address already in use" errors
                allow_reuse_address = True

            # Hardcoded... who cares right now.
            PORT = 8080
            httpd = MyTCPServer(("", PORT), MyHandler)
            thread = Thread(target=httpd.serve_forever, args=())
            thread.start()

            # This is the main loop of this process. I supports two commands...
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
                    shared.filename = command.split()[1]

        self.server = Process(target=http_server, args=(self.q, ))
        self.server.start()

    def set_filename(self, filename):
        if self.started:
            self.q.put("setfilename " + filename)

    def stop(self):
        if self.started:
            self.q.put("stop")
            self.server.join()
