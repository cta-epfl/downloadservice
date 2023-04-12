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

    rootdir = pytestconfig.rootdir
    
    env = copy.deepcopy(dict(os.environ))
    print(("rootdir", str(rootdir)))
    env['PYTHONPATH'] = str(rootdir) + ":" + str(rootdir) + "/tests:" + \
                        str(rootdir) + '/bin:' + \
                        __this_dir__ + ":" + os.path.join(__this_dir__, "../bin:") + \
                        env.get('PYTHONPATH', "")

    env['CTADS_DISABLE_ALL_AUTH'] = 'True'
    env['CTADS_CABUNDLE'] = "cabundle.pem"

    print(("pythonpath", env['PYTHONPATH']))

    cmd = [ "python", "downloadservice/__init__.py"]
        
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

            print(f"{C}following server: {line.rstrip()}{NC}" )
            m = re.search(r"Running on (.*?:5000)", line)
            if m:
                url_store[0] = m.group(1).strip()  # alternatively get from configenv
                print(f"{C}following server: found url:{url_store[0]}")

            if re.search("\* Debugger PIN:.*?", line):
                url_store[0] = url_store[0].replace("0.0.0.0", "127.0.0.1")
                print(f"{C}following server: server ready, url {url_store[0]}")

    thread = Thread(target=follow_output, args=())
    thread.start()

    started_waiting = time.time()
    while url_store[0] is None:
        print("waiting for server to start since", time.time() - started_waiting)
        time.sleep(0.2)
    time.sleep(0.5)

    service = url_store[0]

    yield dict(
        url=service,
        pid=p.pid
    )

    kill_child_processes(p.pid, signal.SIGINT)
    os.kill(p.pid, signal.SIGINT)
