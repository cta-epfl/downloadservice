import os
import tempfile
import subprocess
from wsgidav.wsgidav_app import WsgiDAVApp
from cheroot import wsgi


def serve():
    with tempfile.TemporaryDirectory() as tmpdir:
        path = os.path.join(tmpdir, 'lst/users/anonymous/example-files/tmpdir')
        os.makedirs(path)
        subprocess.check_call([
            "dd", "if=/dev/random",
            f"of={tmpdir}/md5sum-lst.txt",
            "bs=1M", "count=10"
        ])
        config = {
            "host": "127.0.0.1",
            "port": 31102,
            "provider_mapping": {
                "/": tmpdir,
            },
            "simple_dc": {
                "user_mapping": {
                    "*": True
                },
            },
            "logging": {
                "enable": True,
            },
            "verbose": 5,
        }
        app = WsgiDAVApp(config)

        server_args = {
            "bind_addr": (config["host"], config["port"]),
            "wsgi_app": app,
            "timeout": 30,
        }
        httpserver = wsgi.Server(**server_args)

        httpserver.shutdown_timeout = 0  # Speed-up tests teardown

        httpserver.start()


def __main__():
    serve()
