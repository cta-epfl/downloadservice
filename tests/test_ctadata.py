import pytest
import tempfile

from conftest import webdav_server, hash_file, generate_random_file
import ctadata


@pytest.mark.timeout(30)
def test_apiclient_list(start_service):
    with webdav_server():
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
        ctadata.APIClient.downloadservice = start_service['url']

        with tempfile.TemporaryDirectory() as tmpdir:
            origin_file = f"{tmpdir}/local-file"
            generate_random_file(origin_file, 100 * (1024**2))

            r = ctadata.upload_file(origin_file, 'example-files/example-file')

            restored_file = f"{tmpdir}/restored-file"
            ctadata.fetch_and_save_file(r['path'], restored_file)

            assert hash_file(origin_file) == hash_file(restored_file)


@pytest.mark.timeout(30)
def test_apiclient_upload_invalid_path(start_service, caplog):
    with webdav_server():
        ctadata.APIClient.downloadservice = start_service['url']

        with tempfile.TemporaryDirectory() as tmpdir:
            origin_file = f"{tmpdir}/local-file"
            generate_random_file(origin_file, 1 * (1024**2))

            with pytest.raises(ctadata.api.StorageException):
                ctadata.upload_file(origin_file, '../example-file')


@pytest.mark.timeout(30)
def test_apiclient_upload_wrong(start_service, caplog):
    with webdav_server():
        ctadata.APIClient.downloadservice = start_service['url']

        with tempfile.TemporaryDirectory() as tmpdir:
            local_file = f"{tmpdir}/local-file-example"
            generate_random_file(local_file, 1 * (1024**2))

            with pytest.raises(ctadata.api.StorageException):
                ctadata.upload_file(
                    local_file, 'example-files/example-file/../')


@pytest.mark.timeout(30)
def test_apiclient_upload_dir(start_service):
    with webdav_server():
        ctadata.APIClient.downloadservice = start_service['url']

        with tempfile.TemporaryDirectory() as tmpdir:
            for i in range(10):
                print(local_file_i := f"{tmpdir}/local-file-example-{i}")
                generate_random_file(local_file_i, 1 * (1024**2))

            ctadata.upload_dir(tmpdir, 'example-files/tmpdir')
