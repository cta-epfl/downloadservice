from conftest import upstream_webdav_server, ca_certificate, sign_certificate
from flask import url_for
import pytest
from typing import Any


@pytest.mark.timeout(30)
def test_valid_owncert_config(app: Any, client: Any):
    with upstream_webdav_server():
        certificate = sign_certificate(app.ca, 1)
        r = client.post(
            url_for('upload_cert'),
            json={'certificate': certificate},
            headers={'authorization': 'Bearer fake'}
        )
        assert r.status_code == 200


@pytest.mark.timeout(30)
def test_invalid_owncert_config(app: Any, client: Any):
    with upstream_webdav_server():
        with ca_certificate() as alt_ca:
            certificate = sign_certificate(alt_ca, 1)
            r = client.post(
                url_for('upload_cert'),
                json={'certificate': certificate},
                headers={'authorization': 'Bearer fake'}
            )
            assert r.status_code == 400 and \
                r.text == 'invalid certificate verification chain'


@pytest.mark.timeout(30)
def test_expired_owncert_config(app: Any, client: Any):
    with upstream_webdav_server():
        certificate = sign_certificate(app.ca, -1)
        r = client.post(
            url_for('upload_cert'),
            json={'certificate': certificate},
            headers={'authorization': 'Bearer fake'}
        )
        assert r.status_code == 400 and \
            r.text.startswith('invalid certificate : ')


@pytest.mark.timeout(30)
def test_fake_owncert_config(app: Any, client: Any):
    with upstream_webdav_server() as (server_dir, _):
        certificate = 'fake certificate string'
        r = client.post(
            url_for('upload_cert'),
            json={'certificate': certificate},
            headers={'authorization': 'Bearer fake'}
        )
        assert r.status_code == 400 and \
            r.text.startswith('invalid certificate : ')


@pytest.mark.timeout(30)
def test_invalid_chain_maincert_config(app: Any, client: Any):
    with upstream_webdav_server():
        with ca_certificate() as alt_ca:
            certificate = sign_certificate(alt_ca, 1)
            r = client.post(
                url_for('upload_main_cert'),
                json={'certificate': certificate},
                headers={'authorization': 'Bearer fake'}
            )
            assert r.status_code == 400 and \
                r.text == 'invalid certificate verification chain'


@pytest.mark.timeout(30)
def test_new_cert_maincert_config(app: Any, client: Any):
    with upstream_webdav_server():
        certificate = sign_certificate(app.ca, 1)
        r = client.post(
            url_for('upload_main_cert'),
            json={'certificate': certificate},
            headers={'authorization': 'Bearer fake'}
        )
        assert r.status_code == 200


@pytest.mark.timeout(30)
def test_original_maincert_config(app: Any, client: Any):
    with upstream_webdav_server():
        certificate = sign_certificate(app.ca, 365)
        r = client.post(
            url_for('upload_main_cert'),
            json={'certificate': certificate},
            headers={'authorization': 'Bearer fake'}
        )
        assert r.status_code == 400 and r.text == \
            'certificate validity too long, please generate a ' +\
            'short-lived (max 7 day) proxy certificate for uploading. ' +\
            'Please see https://ctaodc.ch/ for more details.'


@pytest.mark.timeout(30)
def test_valid_maincert_config(app: Any, client: Any):
    with upstream_webdav_server():
        with ca_certificate() as alt_ca:
            certificate = sign_certificate(alt_ca, 1)
            r = client.post(
                url_for('upload_main_cert'),
                json={
                    'certificate': certificate,
                    'cabundle': open(alt_ca['crt_file'], 'r').read()
                },
                headers={'authorization': 'Bearer fake'}
            )
            assert r.status_code == 200
