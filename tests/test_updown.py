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


def test_list(client: Any):
    r = client.get(url_for('fetch'))
    assert r.status_code == 200
    print(r.json)

    r = client.get(url_for('fetch', basepath="lst"))
    assert r.status_code == 200
    print(r.json)