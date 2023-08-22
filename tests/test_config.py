from conftest import upstream_webdav_server
from flask import url_for
import os
import pytest
import tempfile
from typing import Any


def tmp_certificate(duration):
    with tempfile.TemporaryDirectory() as tmpdir:
        # cert key
        key_file = tmpdir+'/private.key'
        os.system('openssl genrsa -out ' + key_file)
        # Cert signing request
        csr_file = tmpdir+'/request.csr'
        os.system('openssl req -new -batch -key '+key_file+' -out '+csr_file)
        return os.popen('openssl x509 -req -days '+str(duration)+' -in ' +
                        csr_file+' -signkey '+key_file).read()


@pytest.mark.timeout(30)
def test_valid_owncert_config(app: Any, client: Any):
    with upstream_webdav_server():
        with app.app_context():
            certificate = tmp_certificate(1)
            r = client.post(url_for('upload_cert'), json={
                            'certificate': certificate})
            assert r.status_code == 200


@pytest.mark.timeout(30)
def test_invalid_owncert_config(app: Any, client: Any):
    with upstream_webdav_server():
        with app.app_context():
            certificate = 'fake certificate string'
            r = client.post(url_for('upload_cert'), json={
                            'certificate': certificate})
            assert r.status_code == 400 and r.text == 'invalid certificate'


@pytest.mark.timeout(30)
def test_valid_maincert_config(app: Any, client: Any):
    with upstream_webdav_server():
        with app.app_context():
            certificate = tmp_certificate(1)
            r = client.post(
                url_for('upload_main_cert'),
                json={
                    'certificate': certificate,
                    'cabundle': certificate
                }
            )
            assert r.status_code == 200


@pytest.mark.timeout(30)
def test_invalid_maincert_config(app: Any, client: Any):
    with upstream_webdav_server():
        with app.app_context():
            certificate = tmp_certificate(1)
            r = client.post(
                url_for('upload_main_cert'),
                json={
                    'certificate': certificate,
                    'cabundle': certificate
                }
            )
            assert r.status_code == 200


@pytest.mark.timeout(30)
def test_original_maincert_config(app: Any, client: Any):
    with upstream_webdav_server():
        with app.app_context():
            certificate = tmp_certificate(365)
            r = client.post(
                url_for('upload_main_cert'),
                json={
                    'certificate': certificate,
                    'cabundle': certificate
                }
            )
            assert r.status_code == 400 and r.text == \
                'certificate validity too long (max 1 day)'
