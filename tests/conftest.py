import os
import copy
import re
import subprocess
from threading import Thread
import time
import pytest
import signal
import psutil

__this_dir__ = os.path.join(os.path.abspath(os.path.dirname(__file__)))


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

    os.system("openssl req -newkey rsa:2048 -new -nodes -x509 -days 3650 -keyout certificates/key.pem -out certificates/cert.pem -batch")

    rootdir = pytestconfig.rootdir

    env = copy.deepcopy(dict(os.environ))
    print(("rootdir", str(rootdir)))
    env['PYTHONPATH'] = str(rootdir) + ":" + str(rootdir) + "/tests:" + \
        str(rootdir) + '/bin:' + \
        __this_dir__ + ":" + os.path.join(__this_dir__, "../bin:") + \
        env.get('PYTHONPATH', "")

    env['CTADS_DISABLE_ALL_AUTH'] = 'True'
    env['CTADS_CABUNDLE'] = "cabundle.pem"
    env["CTADS_CABUNDLE"] = "./certificates/cert.pem"
    env["CTADS_CLIENTCERT"] = "./certificates/cert.pem"
    env['CTADS_UPSTREAM_ENDPOINT'] = 'http://127.0.0.1:31102/'
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

            if re.search("\* Debugger PIN:.*?", line):
                url_store[0] = url_store[0].replace("0.0.0.0", "127.0.0.1")
                print(f"{C}following server: server ready, url {url_store[0]}")

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
