from conftest import upstream_webdav_server, generate_random_file, hash_file
from flask import url_for
import os
import pytest
import tempfile
from typing import Any
from contextlib import contextmanager

@contextmanager
def tmp_certificate(duration):
    with tempfile.TemporaryDirectory() as tmpdir:
        # cert key
        key_file = tmpdir+'/private.key'
        os.system('openssl genrsa -out ' + key_file)
        # Cert signing request
        csr_file = tmpdir+'/request.csr'
        os.system('openssl req -new -batch -key '+key_file+' -out '+csr_file)
        # Certificate
        cert_file = tmpdir+'/certificate.crt'
        os.system('openssl x509 -req -days '+str(duration)+' -in '+csr_file+' -signkey '+key_file+' -out '+cert_file)
        yield cert_file


@pytest.mark.timeout(30)
def test_maincert_config(app: Any, client: Any):
    with upstream_webdav_server():
        with app.app_context():
            with tmp_certificate(1) as cert_file:
                with open(cert_file, 'r') as f:
                    certificate = f.read()
                    r = client.post(url_for('upload_main_cert'), json={'certificate':certificate})
                    assert r.status_code == 200

@pytest.mark.timeout(30)
def test_maincert_config_origin_cert(app: Any, client: Any):
    with upstream_webdav_server():
        with app.app_context():
            with tmp_certificate(365) as cert_file:
                with open(cert_file, 'r') as f:
                    certificate = f.read()
                    r = client.post(
                        url_for('upload_main_cert'), 
                        json={
                            'certificate':certificate,
                            'cabundle':certificate
                        }
                    )
                    assert r.status_code == 400
