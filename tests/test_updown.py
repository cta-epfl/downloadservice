from typing import Any
import pytest
from flask import Flask, url_for



@pytest.fixture(scope="session")
def app():
    import downloadservice

    downloadservice.app.config.update({
        "TESTING": True,
        "CTADS_DISABLE_ALL_AUTH": True,
        "DEBUG": True,
        "CTADS_CABUNDLE": "cabundle.pem",
    })

    print("config now:", downloadservice.app.config)

    return downloadservice.app


def test_health(client: Any):
    r = client.get(url_for('health'))
    assert r.status_code == 200
    print(r.json)


def test_list(client: Any):
    r = client.get(url_for('list', basepath="pnfs/cta.cscs.ch/lst"))
    assert r.status_code == 200
    print(r.json)


def test_fetch(client: Any):
    r = client.get(url_for('fetch', subpath="pnfs/cta.cscs.ch/lst/md5sum-lst.txt"))
    assert r.status_code == 200
    print(r.json)


def test_apiclient_list(start_service):
    import ctadata

    print("start_service", start_service)

    ctadata.default_downloadservice = start_service['url']
    
    r = ctadata.list_dir("")
    print(r)