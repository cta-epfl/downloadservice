from typing import Any
import pytest
from flask import url_for

from conftest import webdav_server, generate_random_file


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
def test_download(app: Any, client: Any):
    with webdav_server() as server:
        filename = "md5sum-lst.txt"
        generate_random_file(
            server['config']['provider_mapping']['/']+"/"+filename,
            1 * (1024**2))

        with app.app_context():
            r = client.get(url_for('fetch', path=filename))
            assert r.status_code == 200
            print(r.json)


@pytest.mark.timeout(30)
def test_webdav_list(app: Any, client: Any):
    with webdav_server():
        with app.app_context():
            r = client.open(url_for('webdav', path="lst"), method='PROPFIND')
            assert r.status_code in [200, 207]
