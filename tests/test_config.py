from conftest import upstream_webdav_server, tmp_certificate
from flask import url_for
import pytest
from typing import Any


@pytest.mark.timeout(10)
def test_valid_owncert_config(app: Any, client: Any):
    with upstream_webdav_server():
        with app.app_context():
            ca_bundle, certificate = tmp_certificate(1)
            open(app.config['CTADS_CABUNDLE'], 'w').write(ca_bundle)
            r = client.post(url_for('upload_cert'), json={
                'certificate': certificate})
            assert r.status_code == 200


@pytest.mark.timeout(10)
def test_invalid_owncert_config(app: Any, client: Any):
    with upstream_webdav_server():
        with app.app_context():
            _, certificate = tmp_certificate(1)
            r = client.post(url_for('upload_cert'), json={
                'certificate': certificate})
            assert r.status_code == 400 and \
                r.text == 'invalid certificate verification chain'


@pytest.mark.timeout(10)
def test_expired_owncert_config(app: Any, client: Any):
    with upstream_webdav_server():
        with app.app_context():
            ca_bundle, certificate = tmp_certificate(-1)
            r = client.post(url_for('upload_cert'), json={
                            'certificate': certificate})
            assert r.status_code == 400 and \
                r.text == 'certificate expired'


@ pytest.mark.timeout(10)
def test_fake_owncert_config(app: Any, client: Any):
    with upstream_webdav_server() as (server_dir, _):
        with app.app_context():
            certificate = 'fake certificate string'
            r = client.post(url_for('upload_cert'), json={
                'certificate': certificate})
            assert r.status_code == 400 and \
                r.text.startswith('invalid certificate : ')


@ pytest.mark.timeout(10)
def test_valid_maincert_config(app: Any, client: Any):
    with upstream_webdav_server():
        with app.app_context():
            ca_bundle, certificate = tmp_certificate(1)
            r = client.post(
                url_for('upload_main_cert'),
                json={
                    'certificate': certificate,
                    'cabundle': ca_bundle
                }
            )
            assert r.status_code == 200


@ pytest.mark.timeout(10)
def test_selfsigned_maincert_config(app: Any, client: Any):
    with upstream_webdav_server():
        with app.app_context():
            _, certificate = tmp_certificate(1)
            r = client.post(
                url_for('upload_main_cert'),
                json={
                    'certificate': certificate,
                }
            )
            assert r.status_code == 400 and \
                r.text == 'invalid certificate verification chain'


@ pytest.mark.timeout(10)
def test_invalid_maincert_config(app: Any, client: Any):
    with upstream_webdav_server():
        with app.app_context():
            ca_bundle, certificate = tmp_certificate(1)
            r = client.post(
                url_for('upload_main_cert'),
                json={
                    'certificate': certificate,
                    'cabundle': ca_bundle
                }
            )
            assert r.status_code == 200


@ pytest.mark.timeout(10)
def test_original_maincert_config(app: Any, client: Any):
    with upstream_webdav_server():
        with app.app_context():
            ca_bundle, certificate = tmp_certificate(365)
            r = client.post(
                url_for('upload_main_cert'),
                json={
                    'certificate': certificate,
                    'cabundle': ca_bundle
                }
            )
            assert r.status_code == 400 and r.text == \
                'certificate validity too long, please generate a ' +\
                'short-lived (max 7 day) proxy certificate for uploading. ' +\
                'Please see https://ctaodc.ch/ for more details.'
