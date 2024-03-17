import json

import pytest

import lancedb
from lancedb.utils.events import _Events


@pytest.fixture(autouse=True)
def request_log_path(tmp_path):
    return tmp_path / "request.json"


def mock_register_event(name: str, **kwargs):
    if _Events._instance is None:
        _Events._instance = _Events()

    _Events._instance.enabled = True
    _Events._instance.rate_limit = 0
    _Events._instance(name, **kwargs)


def test_event_reporting(monkeypatch, request_log_path, tmp_path) -> None:
    def mock_request(**kwargs):
        json_data = kwargs.get("json", {})
        with open(request_log_path, "w") as f:
            json.dump(json_data, f)

    monkeypatch.setattr(
        lancedb.table, "register_event", mock_register_event
    )  # Force enable registering events and strip exception handling
    monkeypatch.setattr(lancedb.utils.events, "threaded_request", mock_request)

    db = lancedb.connect(tmp_path)
    db.create_table(
        "test",
        data=[
            {"vector": [3.1, 4.1], "item": "foo", "price": 10.0},
            {"vector": [5.9, 26.5], "item": "bar", "price": 20.0},
        ],
        mode="overwrite",
    )

    assert request_log_path.exists()  # test if event was registered

    with open(request_log_path, "r") as f:
        json_data = json.load(f)

    # TODO: don't hardcode these here. Instead create a module level json scehma in
    # lancedb.utils.events for better evolvability
    batch_keys = ["api_key", "distinct_id", "batch"]
    event_keys = ["event", "properties", "timestamp", "distinct_id"]
    property_keys = ["cli", "install", "platforms", "version", "session_id"]

    assert all([key in json_data for key in batch_keys])
    assert all([key in json_data["batch"][0] for key in event_keys])
    assert all([key in json_data["batch"][0]["properties"] for key in property_keys])

    # cleanup & reset
    monkeypatch.undo()
    _Events._instance = None
