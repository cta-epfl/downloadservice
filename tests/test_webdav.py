import pytest
import tempfile
from webdav4.client import Client, HTTPError, ForbiddenOperation
from conftest import upstream_webdav_server, generate_random_file, hash_file


@pytest.mark.timeout(30)
def test_webdav4_client_list(testing_download_service):
    with upstream_webdav_server():
        client = Client(testing_download_service['url'] + "/webdav/lst")
        res = client.ls("", detail=True)
        assert len(res) == 1
        assert res[0]['href'] == "/webdav/lst/users/"
        assert res[0]['type'] == "directory"


@pytest.mark.timeout(30)
def test_webdav4_client_upload_denied_1(testing_download_service):
    with upstream_webdav_server():
        client = Client(testing_download_service['url'] + "/webdav/lst")

        with tempfile.TemporaryDirectory() as tmpdir:
            local_file = f"{tmpdir}/local-file"
            generate_random_file(local_file, 1*(1024**2))

            with pytest.raises(HTTPError):
                remote_file = "upload-file"
                try:
                    client.upload_file(
                        local_file, remote_file, chunk_size=1024**2)
                except HTTPError as e:
                    print(e.__str__())
                    assert "received 403 (Missing rights to write in : " +\
                        f"lst/{remote_file}, you are only allowed to write " +\
                        "in lst/users/anonymous/)" == \
                        e.__str__()
                    raise e


@pytest.mark.timeout(30)
def test_webdav4_client_upload_denied_2(testing_download_service):
    with upstream_webdav_server():
        client = Client(testing_download_service['url'] + "/webdav/lst")

        with tempfile.TemporaryDirectory() as tmpdir:
            local_file = f"{tmpdir}/local-file"
            generate_random_file(local_file, 1*(1024**2))

            with pytest.raises(HTTPError):
                remote_file = "users/anonymous/../test"
                parsed_remote_file = "users/test"
                try:
                    client.upload_file(
                        local_file, remote_file, chunk_size=1024**2)
                except HTTPError as e:
                    print(e.__str__())
                    assert "received 403 (Missing rights to write in : " +\
                        f"lst/{parsed_remote_file}, you are only allowed to" +\
                        " write in lst/users/anonymous/)" == \
                        e.__str__()
                    raise e


@pytest.mark.timeout(30)
def test_webdav4_client_upload_valid(testing_download_service):
    with upstream_webdav_server() as (server_dir, _):
        client = Client(testing_download_service['url'] + "/webdav/lst")

        with tempfile.TemporaryDirectory() as tmpdir:
            local_file = f"{tmpdir}/local-file"
            generate_random_file(local_file, 100*(1024**2))

            remote_ressource = 'users/anonymous/uploaded-file'
            # TODO: Add a test to ensure that large uploaded file are chunked
            client.upload_file(local_file, remote_ressource,
                               chunk_size=1024**2)

            remote_file = f"{server_dir}/lst/{remote_ressource}"
            assert hash_file(local_file) == hash_file(remote_file)


@pytest.mark.timeout(30)
def test_webdav4_client_download(testing_download_service):
    with upstream_webdav_server() as (server_dir, _):
        filename = "remote-file"
        remote_file = f"{server_dir}/lst/{filename}"
        generate_random_file(remote_file, 10 * (1024**2))

        client = Client(testing_download_service['url'] + "/webdav/lst")

        with tempfile.TemporaryDirectory() as tmpdir:
            downloaded_file = f'{tmpdir}/restored-file-example'
            client.download_file(filename, downloaded_file)

            assert hash_file(remote_file) == hash_file(downloaded_file)


@pytest.mark.timeout(30)
def test_webdav4_client_mkdir_valid(testing_download_service):
    with upstream_webdav_server():
        client = Client(testing_download_service['url'] + "/webdav/lst")
        client.mkdir('users/anonymous/test')


@pytest.mark.timeout(30)
def test_webdav4_client_mkdir_denied(testing_download_service):
    with upstream_webdav_server():
        client = Client(testing_download_service['url'] + "/webdav/lst")

        with pytest.raises(ForbiddenOperation):
            try:
                client.mkdir('users/test/test')
            except ForbiddenOperation as e:
                print(e.__str__())
                assert "the server does not allow creation in the namespace"\
                    in e.__str__()
                raise e
