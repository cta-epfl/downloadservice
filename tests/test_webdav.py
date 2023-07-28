import subprocess
import pytest
import tempfile
from webdav4.client import Client, HTTPError
from conftest import webdav_server, generate_random_file


@pytest.mark.timeout(30)
def test_webdav4_client_list(start_service):
    with webdav_server():
        client = Client(start_service['url'] + "/webdav/lst")
        client.ls("", detail=True)


@pytest.mark.timeout(30)
def test_webdav4_client_upload_denied(start_service):
    with webdav_server():
        client = Client(start_service['url'] + "/webdav/lst")

        with tempfile.TemporaryDirectory() as tmpdir:
            local_file = f"{tmpdir}/local-file"
            generate_random_file(local_file, 1*(1024**2))

            with pytest.raises(HTTPError):
                client.upload_file(
                    local_file,
                    'uploaded-file',
                    chunk_size=1024**2,
                )


@pytest.mark.timeout(30)
def test_webdav4_client_upload_valid(start_service):
    with webdav_server():
        client = Client(start_service['url'] + "/webdav/lst")

        with tempfile.TemporaryDirectory() as tmpdir:
            local_file = f"{tmpdir}/local-file"
            generate_random_file(local_file, 100*(1024**2))

            client.upload_file(
                local_file,
                'users/anonymous/uploaded-file',
                chunk_size=1024**2,
            )


@pytest.mark.timeout(30)
def test_webdav4_client_download(start_service):
    with webdav_server() as server:
        subprocess.check_call([
            "dd", "if=/dev/random",
            f"of={server['config']['provider_mapping']['/']}/lst/file.txt",
            "bs=1M", "count=10"
        ])

        client = Client(start_service['url'] + "/webdav/lst")

        with tempfile.TemporaryDirectory() as tmpdir:
            client.download_file(
                'file.txt',
                f'{tmpdir}/restored-file-example',
            )


@pytest.mark.timeout(30)
def test_webdav4_client_mkdir(start_service):
    with webdav_server():
        client = Client(start_service['url'] + "/webdav/lst")
        client.mkdir('users/anonymous/test')
