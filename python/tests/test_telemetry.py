import pytest
import requests

import lancedb
from lancedb.utils.events import _Events
from lancedb.utils.general import threaded_request


def mock_request(
    method, url, retry=3, timeout=30, thread=True, code=-1, verbose=True, **kwargs
):
    assert method.lower() == "post"
    json = kwargs.get("json", {})
    # TODO: don't hardcode these here. Instead create a module level json scehma in lancedb.utils.events for better evolvability
    batch_keys = ["api_key", "distinct_id", "batch"]
    event_keys = ["event", "properties", "timestamp", "distinct_id"]
    property_keys = ["cli", "install", "platforms", "version", "session_id", "blud"]

    assert all([key in json for key in batch_keys])
    assert all([key in json["batch"][0] for key in event_keys])
    assert all([key in json["batch"][0]["properties"] for key in property_keys])


def mock_register_event(name: str, **kwargs):
    if _Events._instance is None:
        _Events._instance = _Events()
        _Events._instance.enabled = True
        _Events._instance.rate_limit = 0

    _Events._instance(name, **kwargs)


# notice our test uses the custom fixture instead of monkeypatch directly
def test_get_access_token_success(monkeypatch) -> None:
    monkeypatch.setattr(lancedb.table, "register_event", mock_register_event) # Force enable resitering events and strip exception handling
    monkeypatch.setattr(lancedb.utils.events, "threaded_request", mock_request)

    db = lancedb.connect("db")
    db.create_table(
        "test",
        data=[
            {"vector": [3.1, 4.1], "item": "foo", "price": 10.0},
            {"vector": [5.9, 26.5], "item": "bar", "price": 20.0},
        ],
        mode="overwrite",
    )
