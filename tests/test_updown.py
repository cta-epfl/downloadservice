import subprocess
from typing import Any
import pytest
import tempfile
from flask import url_for

from conftest import webdav_server


@pytest.mark.timeout(30)
def test_health(app: Any, client: Any):
    with webdav_server():
        with app.app_context():
            print(url_for('health'))
            r = client.get(url_for('health'))
            print(r.json, url_for('health'))
            assert r.status_code == 200


@pytest.mark.timeout(30)
def test_list(app: Any, client: Any):
    with webdav_server():
        with app.app_context():
            r = client.get(url_for('list', path="lst"))
            assert r.status_code == 200
            print(r.json)


@pytest.mark.timeout(30)
def test_fetch(app: Any, client: Any):
    with webdav_server() as server:
        subprocess.check_call([
            "dd", "if=/dev/random",
            f"of={server['config']['provider_mapping']['/']}/md5sum-lst.txt",
            "bs=1M", "count=10"
        ])
        with app.app_context():
            r = client.get(url_for('fetch', path="md5sum-lst.txt"))
            assert r.status_code == 200
            print(r.json)


@pytest.mark.timeout(30)
def test_webdav_list(app: Any, client: Any):
    with webdav_server():
        with app.app_context():
            r = client.open(url_for('webdav', path="lst"), method='PROPFIND')
            assert r.status_code in [200, 207]


@pytest.mark.timeout(30)
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


@pytest.mark.timeout(30)
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


@pytest.mark.timeout(30)
def test_apiclient_upload_single_file(start_service, caplog):
    with webdav_server():
        import ctadata
        ctadata.APIClient.downloadservice = start_service['url']

        with tempfile.TemporaryDirectory() as tmpdir:
            subprocess.check_call([
                "dd", "if=/dev/random", f"of={tmpdir}/local-file-example",
                "bs=1M", "count=100"
            ])

            r = ctadata.upload_file(f'{tmpdir}/local-file-example',
                                    'example-files/example-file')
            print(r)

            ctadata.fetch_and_save_file(
                r['path'], f'{tmpdir}/restored-file-example')


@pytest.mark.timeout(30)
def test_apiclient_upload_invalid_path(start_service, caplog):
    with webdav_server():
        import ctadata
        ctadata.APIClient.downloadservice = start_service['url']

        with tempfile.TemporaryDirectory() as tmpdir:
            subprocess.check_call(
                ["dd", "if=/dev/random", f"of={tmpdir}/local-file-example",
                 "bs=1M", "count=1"])

            with pytest.raises(ctadata.api.StorageException):
                ctadata.upload_file(
                    f'{tmpdir}/local-file-example', '../example-file')


@pytest.mark.timeout(30)
def test_apiclient_upload_wrong(start_service, caplog):
    with webdav_server():
        import ctadata
        ctadata.APIClient.downloadservice = start_service['url']

        with tempfile.TemporaryDirectory() as tmpdir:
            subprocess.check_call(
                ["dd", "if=/dev/random", f"of={tmpdir}/local-file-example",
                 "bs=1M", "count=1"])

            with pytest.raises(ctadata.api.StorageException):
                ctadata.upload_file(f'{tmpdir}/local-file-example',
                                    'example-files/example-file/../')


@pytest.mark.timeout(30)
def test_apiclient_upload_dir(start_service, caplog):
    with webdav_server():
        import ctadata
        ctadata.APIClient.downloadservice = start_service['url']

        with tempfile.TemporaryDirectory() as tmpdir:
            for i in range(10):
                print(f"{tmpdir}/local-file-example-{i}")
                subprocess.check_call([
                    "dd", "if=/dev/random",
                    f"of={tmpdir}/local-file-example-{i}", "bs=1M", "count=1"
                ])

            ctadata.upload_dir(tmpdir, 'example-files/tmpdir')


@pytest.mark.timeout(30)
def test_webdav4_client_list(start_service):
    with webdav_server():
        from webdav4.client import Client

        client = Client(start_service['url'] + "/webdav/lst")
        client.ls("", detail=True)


@pytest.mark.timeout(30)
def test_webdav4_client_upload(start_service):
    with webdav_server():
        from webdav4.client import Client

        client = Client(start_service['url'] + "/webdav/lst")

        with tempfile.TemporaryDirectory() as tmpdir:
            subprocess.check_call([
                "dd", "if=/dev/random", f"of={tmpdir}/local-file-example",
                "bs=1M", "count=100"
            ])

            file_uri = 'uploaded-file'

            def on_success(res):
                pass

            def on_upload(res):
                # raise Exception(res)
                client.download_file(
                    file_uri,
                    f'{tmpdir}/restored-file-example',
                    callback=on_success)

            client.upload_file(
                f'{tmpdir}/local-file-example',
                file_uri,
                chunk_size=1024**2)  # ,
            # callback=on_upload)
