import copy
import hashlib
import os
import psutil
import pytest
import re
import signal
import subprocess
import tempfile
import time
from threading import Thread

from contextlib import contextmanager
from wsgidav.wsgidav_app import WsgiDAVApp
from cheroot import wsgi

__this_dir__ = os.path.join(os.path.abspath(os.path.dirname(__file__)))

webdav_server_host = "127.0.0.1"
webdav_server_port = 31102


def hash_file(filename):
    """"Returns the SHA-1 hash of the file provided"""
    h = hashlib.sha1()

    with open(filename, 'rb') as file:
        chunk = 0
        while chunk != b'':
            chunk = file.read(1024**2)
            h.update(chunk)

    return h.hexdigest()


def generate_random_file(filename, size):
    with open(filename, 'wb') as fout:
        fout.write(os.urandom(size))


@pytest.fixture(scope="session")
def app():
    with tempfile.TemporaryDirectory() as tmpdir:
        from downloadservice.app import app

        os.system(f"openssl req -newkey rsa:2048 -new -nodes -x509 -days 3650 \
                -keyout {tmpdir}/key.pem -out {tmpdir}/cert.pem -batch")

        app.config.update({
            "TESTING": True,
            "CTADS_DISABLE_ALL_AUTH": True,
            "DEBUG": True,
            "CTADS_CABUNDLE": f"{tmpdir}/cert.pem",
            "CTADS_CLIENTCERT": f"{tmpdir}/cert.pem",
            'CTADS_UPSTREAM_ENDPOINT':
                f'http://{webdav_server_host}:{str(webdav_server_port)}/',
            'CTADS_UPSTREAM_BASEPATH': '',
            "SERVER_NAME": 'app',
        })

        yield app


@contextmanager
def webdav_server():
    """Set up and tear down a Cheroot server instance."""

    with tempfile.TemporaryDirectory() as tmpdir:
        path = os.path.join(tmpdir, 'lst/users/anonymous/example-files/tmpdir')
        os.makedirs(path)

        config = {
            "host": webdav_server_host,
            "port": webdav_server_port,
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

        with httpserver._run_in_thread() as thread:
            yield locals()


def kill_child_processes(parent_pid, sig=signal.SIGINT):
    try:
        parent = psutil.Process(parent_pid)
        children = parent.children(recursive=True)
        for process in children:
            process.send_signal(sig)
    except psutil.NoSuchProcess:
        return


@pytest.fixture
def start_service(pytestconfig):
    with tempfile.TemporaryDirectory() as tmpdir:

        os.system(f"openssl req -newkey rsa:2048 -new -nodes -x509 -days 3650 \
                -keyout {tmpdir}/key.pem -out {tmpdir}/cert.pem -batch")

        rootdir = pytestconfig.rootdir

        env = copy.deepcopy(dict(os.environ))
        print(("rootdir", str(rootdir)))
        env['PYTHONPATH'] = str(rootdir) + ":" + str(rootdir) + "/tests:" + \
            str(rootdir) + '/bin:' + \
            __this_dir__ + ":" + os.path.join(__this_dir__, "../bin:") + \
            env.get('PYTHONPATH', "")

        env['CTADS_DISABLE_ALL_AUTH'] = 'True'
        env['CTADS_CABUNDLE'] = "cabundle.pem"
        env["CTADS_CABUNDLE"] = f"{tmpdir}/cert.pem"
        env["CTADS_CLIENTCERT"] = f"{tmpdir}/cert.pem"
        env['CTADS_UPSTREAM_ENDPOINT'] = \
            f'http://{webdav_server_host}:{str(webdav_server_port)}/'
        env['CTADS_UPSTREAM_BASEPATH'] = ''

        print(("pythonpath", env['PYTHONPATH']))

        cmd = ["python", "downloadservice/cli.py"]

        print(f"\033[33mcommand: {cmd}\033[0m")

        p = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            shell=False,
            env=env,
        )

        url_store = [None]

        def follow_output():
            url_store[0] = None
            for line in iter(p.stdout):
                line = line.decode()

                NC = '\033[0m'
                if 'ERROR' in line:
                    C = '\033[31m'
                else:
                    C = '\033[34m'

                print(f"{C}following server: {line.rstrip()}{NC}")
                m = re.search(r"Running on (.*?:5000)", line)
                if m:
                    # alternatively get from configenv
                    url_store[0] = m.group(1).strip()
                    print(f"{C}following server: found url:{url_store[0]}")

                if re.search(r"\* Debugger PIN:.*?", line):
                    url_store[0] = url_store[0].replace("0.0.0.0", "127.0.0.1")
                    print(f"{C}following server: ready, url {url_store[0]}")

        thread = Thread(target=follow_output, args=())
        thread.start()

        started_waiting = time.time()
        while url_store[0] is None:
            print("waiting for server to start since",
                  time.time() - started_waiting)
            time.sleep(0.2)
        time.sleep(0.5)

        service = url_store[0]

        yield dict(
            url=service,
            pid=p.pid
        )

        kill_child_processes(p.pid, signal.SIGINT)
        os.kill(p.pid, signal.SIGINT)
