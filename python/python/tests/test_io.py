# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors


import os

import lancedb
import pytest

# AWS:
# You need to setup AWS credentials an a base path to run this test. Example
#    AWS_PROFILE=default TEST_S3_BASE_URL=s3://my_bucket/dataset pytest tests/test_io.py
#
# Azure:
# You need to setup Azure credentials an a base path to run this test. Example
#   export AZURE_STORAGE_ACCOUNT_NAME="<account>"
#   export AZURE_STORAGE_ACCOUNT_KEY="<key>"
#   export REMOTE_BASE_URL=az://my_blob/dataset
#   pytest tests/test_io.py


@pytest.fixture(autouse=True, scope="module")
def setup():
    yield

    if remote_url := os.environ.get("REMOTE_BASE_URL"):
        db = lancedb.connect(remote_url)

        for table in db.table_names():
            db.drop_table(table)


@pytest.mark.skipif(
    (os.environ.get("REMOTE_BASE_URL") is None),
    reason="please setup remote base url",
)
def test_remote_io():
    db = lancedb.connect(os.environ.get("REMOTE_BASE_URL"))
    assert db.table_names() == []

    table = db.create_table(
        "test",
        data=[
            {"vector": [3.1, 4.1], "item": "foo", "price": 10.0},
            {"vector": [5.9, 26.5], "item": "bar", "price": 20.0},
        ],
    )
    rs = table.search([100, 100]).limit(1).to_pandas()
    assert len(rs) == 1
    assert rs["item"].iloc[0] == "bar"

    rs = table.search([100, 100]).where("price < 15").limit(2).to_pandas()
    assert len(rs) == 1
    assert rs["item"].iloc[0] == "foo"

    assert db.table_names() == ["test"]
    assert "test" in db
    assert len(db) == 1

    assert db.open_table("test").name == db["test"].name
