from conftest import (
    upstream_webdav_server,
    hash_file,
    generate_random_file,
)
import logging
import os
import tempfile
import pytest
import ctadata


@pytest.mark.timeout(30)
def test_apiclient_list(testing_download_service):
    with upstream_webdav_server():
        r = ctadata.list_dir(
            "", downloadservice=testing_download_service['url'])
        print(r)

        ctadata.APIClient.downloadservice = testing_download_service['url']

        with pytest.raises(ctadata.api.StorageException):
            ctadata.list_dir("fake-temp")

        r = ctadata.list_dir("lst/")

        expected = ['lst/', 'lst/users/']
        assert set([entry['href'] for entry in r]) == set(expected)


@pytest.mark.timeout(30)
def test_apiclient_webdav4_client(testing_download_service):
    with upstream_webdav_server():
        ctadata.APIClient.downloadservice = testing_download_service['url']
        ctadata.APIClient.token = "faketoken"

        client = ctadata.webdav4_client()
        res = client.ls("lst", detail=True)
        assert len(res) == 1
        assert res[0]['href'] == "/webdav/lst/users/"
        assert res[0]['type'] == "directory"


@pytest.mark.timeout(30)
def test_apiclient_fetch(testing_download_service, caplog):
    caplog.set_level(logging.DEBUG)
    with upstream_webdav_server() as (server_dir, _):
        remote_file = f"{server_dir}/lst/remote-file"
        generate_random_file(remote_file, 10 * (1024**2))
        hash_remote_file = hash_file(remote_file)

        ctadata.APIClient.downloadservice = testing_download_service['url']

        r = ctadata.list_dir("lst")

        expected = ['lst/', 'lst/users/', 'lst/remote-file']
        assert set([entry['href'] for entry in r]) == set(expected)

        with tempfile.TemporaryDirectory() as tmpdir:
            for entry in r:
                if entry['type'] == 'file' and int(entry['size']) > 10000:
                    local_filename_1 = f"{tmpdir}/local-copy-1"
                    ctadata.fetch_and_save_file(
                        entry['href'], chunk_size=1024*1024*2,
                        save_to_fn=local_filename_1)
                    assert 'in 4 chunks' in caplog.text
                    assert 'in 9 chunks' not in caplog.text
                    assert hash_remote_file == hash_file(local_filename_1)

                    local_filename_2 = f"{tmpdir}/local-copy-2"
                    ctadata.fetch_and_save_file(
                        entry['href'], chunk_size=1024*1024*1,
                        save_to_fn=local_filename_2)
                    assert 'in 9 chunks' in caplog.text
                    assert hash_remote_file == hash_file(local_filename_2)


@pytest.mark.timeout(30)
def test_apiclient_upload_single_file(testing_download_service):
    with upstream_webdav_server() as (server_dir, _):
        ctadata.APIClient.downloadservice = testing_download_service['url']

        with tempfile.TemporaryDirectory() as tmpdir:
            origin_file = f"{tmpdir}/local-file"
            generate_random_file(origin_file, 100 * (1024**2))

            remote_path = 'example-files/example-file'
            r = ctadata.upload_file(origin_file, remote_path)

            restored_file = f"{tmpdir}/restored-file"
            ctadata.fetch_and_save_file(r['path'], restored_file)

            assert hash_file(origin_file) == hash_file(restored_file)

            remote_file = f"{server_dir}/lst/users/anonymous/{remote_path}"
            assert os.path.isfile(remote_file)


@pytest.mark.timeout(240)
def test_apiclient_upload_single_large_file(testing_download_service):
    with upstream_webdav_server() as (server_dir, _):
        ctadata.APIClient.downloadservice = testing_download_service['url']

        with tempfile.TemporaryDirectory() as tmpdir:
            origin_file = f"{tmpdir}/local-file"
            generate_random_file(origin_file, 1474801635)

            remote_path = 'example-files/example-file'
            r = ctadata.upload_file(origin_file, remote_path)

            restored_file = f"{tmpdir}/restored-file"
            ctadata.fetch_and_save_file(r['path'], restored_file)

            assert hash_file(origin_file) == hash_file(restored_file)

            remote_file = f"{server_dir}/lst/users/anonymous/{remote_path}"
            assert os.path.isfile(remote_file)


@pytest.mark.timeout(30)
def test_apiclient_upload_invalid_path(testing_download_service):
    with upstream_webdav_server():
        ctadata.APIClient.downloadservice = testing_download_service['url']

        with tempfile.TemporaryDirectory() as tmpdir:
            origin_file = f"{tmpdir}/local-file"
            generate_random_file(origin_file, 1 * (1024**2))

            with pytest.raises(ctadata.api.StorageException):
                ctadata.upload_file(origin_file, '../example-file')


@pytest.mark.timeout(30)
def test_apiclient_upload_wrong(testing_download_service):
    with upstream_webdav_server() as (server_dir, _):
        ctadata.APIClient.downloadservice = testing_download_service['url']

        with tempfile.TemporaryDirectory() as tmpdir:
            origin_file = f"{tmpdir}/local-file-example"
            generate_random_file(origin_file, 1 * (1024**2))

            remote_path = 'example-files/example-file/../'
            r = ctadata.upload_file(origin_file, remote_path)

            restored_file = f"{tmpdir}/restored-file"
            ctadata.fetch_and_save_file(r['path'], restored_file)

            assert hash_file(origin_file) == hash_file(restored_file)

            remote_file = f"{server_dir}/lst/users/anonymous/" +\
                "example-files/local-file-example"
            assert os.path.isfile(remote_file)


@pytest.mark.timeout(30)
def test_apiclient_upload_dir(testing_download_service):
    with upstream_webdav_server():
        ctadata.APIClient.downloadservice = testing_download_service['url']

        with tempfile.TemporaryDirectory() as tmpdir:
            for i in range(10):
                print(local_file_i := f"{tmpdir}/local-file-example-{i}")
                generate_random_file(local_file_i, 1 * (1024**2))

            ctadata.upload_dir(tmpdir, 'example-files/tmpdir')
