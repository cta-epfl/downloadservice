from typing import Any
import pytest
from flask import url_for
import xmltodict
import tempfile
from conftest import upstream_webdav_server, generate_random_file, hash_file


@pytest.mark.timeout(30)
def test_health(app: Any, client: Any):
    with upstream_webdav_server():
        with app.app_context():
            print(url_for('health'))
            r = client.get(url_for('health'))
            print(r.json, url_for('health'))
            assert r.status_code == 200


@pytest.mark.timeout(30)
def test_list(app: Any, client: Any):
    with upstream_webdav_server():
        with app.app_context():
            r = client.get(url_for('list', path="lst"))
            assert r.status_code == 200
            print(r.json)


@pytest.mark.timeout(30)
def test_download(app: Any, client: Any):
    with upstream_webdav_server() as (server_dir, _):
        filename = "md5sum-lst.txt"
        remote_file = f"{server_dir}/{filename}"
        generate_random_file(remote_file, 1 * (1024**2))

        with app.app_context():
            r = client.get(url_for('fetch', path=filename))
            assert r.status_code == 200

            with tempfile.TemporaryDirectory() as tmpdir:
                downloaded_file = f"{tmpdir}/generated-file"
                with open(downloaded_file, 'wb') as fout:
                    print(type(r))
                    print(dir(r))
                    print(dir(r.stream))
                    for buf in r.iter_encoded():
                        fout.write(buf)

                assert hash_file(remote_file) == hash_file(downloaded_file)


@pytest.mark.timeout(30)
def test_webdav_list(app: Any, client: Any):
    with upstream_webdav_server():
        with app.app_context():
            r = client.open(url_for('webdav', path="lst"), method='PROPFIND')
            assert r.status_code in [200, 207]

            data = r.get_data()
            xml_res = xmltodict.parse(data)

            assert len(xml_res['ns0:multistatus']['ns0:response']) == 5

            expected = ['/webdav/lst/',
                        '/webdav/lst/users/',
                        '/webdav/lst/users/anonymous/',
                        '/webdav/lst/users/anonymous/example-files/',
                        '/webdav/lst/users/anonymous/example-files/tmpdir/']

            for elem in xml_res['ns0:multistatus']['ns0:response']:
                assert elem['ns0:href'] in expected
