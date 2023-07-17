import json
import subprocess
from typing import Any
import pytest
import tempfile
from flask import url_for
from unittest import mock


@pytest.fixture(scope="session")
def app():
    from downloadservice.app import app

    app.config.update({
        "TESTING": True,
        "CTADS_DISABLE_ALL_AUTH": True,
        "DEBUG": True,
        "CTADS_CABUNDLE": "./certificats/cert.pem",
        "CTADS_CLIENTCERT": "./certificats/cert.pem",
        "SERVER_NAME": 'app'
    })

    return app


# This method will be used by the mock to replace requests.get

class MockResponse:
    def __init__(self, data, status_code):
        self.content = data.encode()
        self.status_code = status_code

    def __enter__(self):
        return self

    def __exit__(self, type, value, tb):
        return self

    def iter_content(self, chunk_size):
        s = self.content
        while len(s) > chunk_size:
            k = chunk_size
            while (ord(s[k]) & 0xc0) == 0x80:
                k -= 1
            yield s[:k]
            s = s[k:]
        yield s

    def json(self):
        return self.data


def mocked_health_request(*args, **kwargs):
    if args[0] == 'PROPFIND' and args[1] == "https://dcache.cta.cscs.ch:2880/pnfs/cta.cscs.ch/lst":
        return MockResponse("OK", 200)
    return MockResponse("KO", 500)


@mock.patch('downloadservice.app.requests.Session.request', side_effect=mocked_health_request)
def test_health(app: Any, client: Any):
    with app.app_context():
        print(url_for('health'))
        r = client.get(url_for('health'))
        assert r.status_code == 200
        print(r.json)


def mocked_list_request(*args, **kwargs):
    if args[0] == 'PROPFIND' and args[1] == "https://dcache.cta.cscs.ch:2880/pnfs/cta.cscs.ch/lst":
        return MockResponse('<?xml version="1.0" encoding="utf-8" ?><multistatus xmlns="DAV:"><response><href>http://www.example.com/container/</href><propstat><prop xmlns:R="http://ns.example.com/boxschema/"><R:bigbox/><R:author/><creationdate/><displayname/><resourcetype/><supportedlock/></prop><status>HTTP/1.1 200 OK</status></propstat></response><response><href>http://www.example.com/container/front.html</href><propstat><prop xmlns:R="http://ns.example.com/boxschema/"><R:bigbox/><creationdate/><displayname/><getcontentlength/><getcontenttype/><getetag/><getlastmodified/><resourcetype/><supportedlock/></prop><status>HTTP/1.1 200 OK</status></propstat></response></multistatus>', 200)
    return MockResponse("KO", 500)


@mock.patch('downloadservice.app.requests.Session.request', side_effect=mocked_list_request)
def test_list(app: Any, client: Any):
    with app.app_context():
        r = client.get(url_for('list', path="lst"))
        assert r.status_code == 200
        print(r.json)


def mocked_fetch_request(*args, **kwargs):
    if args[0] == 'PROPFIND' and args[1] == "https://dcache.cta.cscs.ch:2880/pnfs/cta.cscs.ch/lst":
        return MockResponse('<?xml version="1.0" encoding="utf-8" ?><multistatus xmlns="DAV:"><response><href>http://www.example.com/container/</href><propstat><prop xmlns:R="http://ns.example.com/boxschema/"><R:bigbox/><R:author/><creationdate/><displayname/><resourcetype/><supportedlock/></prop><status>HTTP/1.1 200 OK</status></propstat></response><response><href>http://www.example.com/container/front.html</href><propstat><prop xmlns:R="http://ns.example.com/boxschema/"><R:bigbox/><creationdate/><displayname/><getcontentlength/><getcontenttype/><getetag/><getlastmodified/><resourcetype/><supportedlock/></prop><status>HTTP/1.1 200 OK</status></propstat></response></multistatus>', 200)
    return MockResponse("KO", 500)


@mock.patch('downloadservice.app.requests.Session.request', side_effect=mocked_fetch_request)
def test_fetch(app: Any, client: Any):
    with app.app_context():
        r = client.get(url_for('fetch', path="md5sum-lst.txt"))
        assert r.status_code == 200
        print(r.json)


@mock.patch('downloadservice.app.requests.Session.request', side_effect=mocked_list_request)
def test_apiclient_list(start_service):
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


@mock.patch('downloadservice.app.requests.Session.request', side_effect=mocked_fetch_request)
def test_apiclient_fetch(start_service, caplog):
    import ctadata
    ctadata.APIClient.downloadservice = start_service['url']

    r = ctadata.list_dir("lst")

    for entry in r:
        print(entry)
        if entry['type'] == 'file' and int(entry['size']) > 10000:
            ctadata.fetch_and_save_file(entry['href'], chunk_size=1024*1024*10)
            assert 'in 4 chunks' in caplog.text
            assert 'in 9 chunks' not in caplog.text
            ctadata.fetch_and_save_file(entry['href'], chunk_size=1024*1024*5)
            assert 'in 9 chunks' in caplog.text
            break


@mock.patch('downloadservice.app.requests.Session.request', side_effect=mocked_health_request)
def test_apiclient_upload(start_service, caplog):
    import ctadata
    ctadata.APIClient.downloadservice = start_service['url']

    subprocess.check_call([
        "dd", "if=/dev/random", "of=local-file-example", "bs=1M", "count=1000"
    ])

    r = ctadata.upload_file('local-file-example', 'example-files/example-file')
    print(r)

    ctadata.fetch_and_save_file(r['path'], 'restored-file-example')


@mock.patch('downloadservice.app.requests.Session.request', side_effect=mocked_health_request)
def test_apiclient_upload_invalid_path(start_service, caplog):
    import ctadata
    ctadata.APIClient.downloadservice = start_service['url']

    subprocess.check_call(
        ["dd", "if=/dev/random", "of=local-file-example", "bs=1M", "count=1"])

    with pytest.raises(ctadata.api.StorageException):
        ctadata.upload_file('local-file-example', '../example-file')


@mock.patch('downloadservice.app.requests.Session.request', side_effect=mocked_health_request)
def test_apiclient_upload_wrong(start_service, caplog):
    import ctadata
    ctadata.APIClient.downloadservice = start_service['url']

    subprocess.check_call(
        ["dd", "if=/dev/random", "of=local-file-example", "bs=1M", "count=1"])

    with pytest.raises(ctadata.api.StorageException):
        ctadata.upload_file('local-file-example',
                            'example-files/example-file/../')


@mock.patch('downloadservice.app.requests.Session.request', side_effect=mocked_health_request)
def test_apiclient_upload_dir(start_service, caplog):
    import ctadata
    ctadata.APIClient.downloadservice = start_service['url']

    with tempfile.TemporaryDirectory() as tmpdir:
        for i in range(10):
            subprocess.check_call([
                "dd", "if=/dev/random", f"of={tmpdir}/local-file-example-{i}",
                "bs=1M", "count=1"
            ])

        ctadata.upload_dir(tmpdir, 'example-files/tmpdir')


@pytest.mark.xfail(reason="dav not implemented yet")
def test_dav_list(start_service):
    from webdav4.client import Client

    client = Client(start_service['url'] + "/dav/lst")
    client.exists("bla")

    client.ls("", detail=True)
    client.upload_file("test")
