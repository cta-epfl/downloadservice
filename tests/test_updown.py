import subprocess
from typing import Any
import pytest
import tempfile
from flask import url_for

from contextlib import contextmanager
from wsgidav.wsgidav_app import WsgiDAVApp
from cheroot import wsgi
import threading
import time
import os


@pytest.fixture(scope="session")
def app():
    from downloadservice.app import app

    os.system("openssl req -newkey rsa:2048 -new -nodes -x509 -days 3650 -keyout certificates/key.pem -out certificates/cert.pem -batch")

    app.config.update({
        "TESTING": True,
        "CTADS_DISABLE_ALL_AUTH": True,
        "DEBUG": True,
        "CTADS_CABUNDLE": "./certificates/cert.pem",
        "CTADS_CLIENTCERT": "./certificates/cert.pem",
        'CTADS_UPSTREAM_ENDPOINT': 'http://127.0.0.1:31102/',
        'CTADS_UPSTREAM_BASEPATH': '',
        "SERVER_NAME": 'app',
    })

    return app


@contextmanager
def webdav_server():
    """Set up and tear down a Cheroot server instance."""
    try:
        config = {
            "host": "127.0.0.1",
            "port": 31102,
            "provider_mapping": {
                "/": "./test_data",
            },
            "simple_dc": {
                "user_mapping": {
                    "*": True
                },
            },
            "verbose": 1,
        }
        app = WsgiDAVApp(config)

        server_args = {
            "bind_addr": (config["host"], config["port"]),
            "wsgi_app": app,
        }
        httpserver = wsgi.Server(**server_args)
    except OSError:
        pass

    httpserver.shutdown_timeout = 0  # Speed-up tests teardown

    threading.Thread(target=httpserver.safe_start).start()  # spawn it
    while not httpserver.ready:  # wait until fully initialized and bound
        time.sleep(0.1)

    yield httpserver

    httpserver.stop()  # destroy it


@pytest.mark.timeout(60)
def test_health(app: Any, client: Any):
    with webdav_server():
        with app.app_context():
            print(url_for('health'))
            r = client.get(url_for('health'))
            print(r.json, url_for('health'))
            assert r.status_code == 200


@pytest.mark.timeout(60)
def test_list(app: Any, client: Any):
    with webdav_server():
        with app.app_context():
            r = client.get(url_for('list', path="lst"))
            assert r.status_code == 200
            print(r.json)


@pytest.mark.timeout(60)
def test_fetch(app: Any, client: Any):
    with webdav_server():
        with app.app_context():
            r = client.get(url_for('fetch', path="md5sum-lst.txt"))
            assert r.status_code == 200
            print(r.json)


@pytest.mark.timeout(60)
def test_apiclient_list(start_service):
    with webdav_server():
        import ctadata
        r = ctadata.list_dir("", downloadservice=start_service['url'])

        print(r)

        ctadata.APIClient.downloadservice = start_service['url']

        with pytest.raises(ctadata.api.StorageException):
            ctadata.list_dir("blablabalfake")

        r = ctadata.list_dir("lst")

        n_dir = 0
        n_file = 0

        for entry in r:
            print(entry)
            if entry['type'] == 'directory':
                n_dir += 1
                print(ctadata.list_dir(entry['href']))
            else:
                n_file += 1

            if n_dir > 2 and n_file > 3:
                break


@pytest.mark.timeout(60)
def test_apiclient_fetch(start_service, caplog):
    with webdav_server():
        import ctadata
        ctadata.APIClient.downloadservice = start_service['url']

        r = ctadata.list_dir("lst")

        for entry in r:
            print(entry)
            if entry['type'] == 'file' and int(entry['size']) > 10000:
                ctadata.fetch_and_save_file(
                    entry['href'], chunk_size=1024*1024*10)
                assert 'in 4 chunks' in caplog.text
                assert 'in 9 chunks' not in caplog.text
                ctadata.fetch_and_save_file(
                    entry['href'], chunk_size=1024*1024*5)
                assert 'in 9 chunks' in caplog.text
                break


@pytest.mark.timeout(60)
def test_apiclient_upload_single_file(start_service, caplog):
    with webdav_server():
        import ctadata
        ctadata.APIClient.downloadservice = start_service['url']

        with tempfile.TemporaryDirectory() as tmpdir:
            subprocess.check_call([
                "dd", "if=/dev/random", f"of={tmpdir}/local-file-example", "bs=1M", "count=1"
            ])

            r = ctadata.upload_file(f'{tmpdir}/local-file-example',
                                    'example-files/example-file')

            print(r)

            ctadata.fetch_and_save_file(r['path'], 'restored-file-example')


@pytest.mark.timeout(60)
def test_apiclient_upload_invalid_path(start_service, caplog):
    with webdav_server():
        import ctadata
        ctadata.APIClient.downloadservice = start_service['url']

        with tempfile.TemporaryDirectory() as tmpdir:
            subprocess.check_call(
                ["dd", "if=/dev/random", f"of={tmpdir}/local-file-example", "bs=1M", "count=1"])

            with pytest.raises(ctadata.api.StorageException):
                ctadata.upload_file(
                    f'{tmpdir}/local-file-example', '../example-file')


@pytest.mark.timeout(60)
def test_apiclient_upload_wrong(start_service, caplog):
    with webdav_server():
        import ctadata
        ctadata.APIClient.downloadservice = start_service['url']

        with tempfile.TemporaryDirectory() as tmpdir:
            subprocess.check_call(
                ["dd", "if=/dev/random", f"of={tmpdir}/local-file-example", "bs=1M", "count=1"])

            with pytest.raises(ctadata.api.StorageException):
                ctadata.upload_file(f'{tmpdir}/local-file-example',
                                    'example-files/example-file/../')


@pytest.mark.timeout(60)
def test_apiclient_upload_dir(start_service, caplog):
    with webdav_server():
        import ctadata
        ctadata.APIClient.downloadservice = start_service['url']

        with tempfile.TemporaryDirectory() as tmpdir:
            for i in range(10):
                print(f"{tmpdir}/local-file-example-{i}")
                subprocess.check_call([
                    "dd", "if=/dev/random", f"of={tmpdir}/local-file-example-{i}",
                    "bs=1M", "count=1"
                ])

            ctadata.upload_dir(tmpdir, 'example-files/tmpdir')


@pytest.mark.xfail(reason="dav not implemented yet")
@pytest.mark.timeout(60)
def test_dav_list(start_service):
    with webdav_server():
        from webdav4.client import Client

        client = Client(start_service['url'] + "/dav/lst")
        client.exists("bla")

        client.ls("", detail=True)
        client.upload_file("test")
