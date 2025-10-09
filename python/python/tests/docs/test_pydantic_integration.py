# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

# --8<-- [start:imports]
import sys

import lancedb
import pytest
from lancedb.pydantic import Vector, LanceModel
# --8<-- [end:imports]


def test_pydantic_model(tmp_path):
    # --8<-- [start:base_model]
    class PersonModel(LanceModel):
        name: str
        age: int
        vector: Vector(2)

    # --8<-- [end:base_model]

    # --8<-- [start:set_url]
    url = "./example"
    # --8<-- [end:set_url]
    url = tmp_path

    # --8<-- [start:base_example]
    db = lancedb.connect(url)
    table = db.create_table("person", schema=PersonModel)
    table.add(
        [
            PersonModel(name="bob", age=1, vector=[1.0, 2.0]),
            PersonModel(name="alice", age=2, vector=[3.0, 4.0]),
        ]
    )
    assert table.count_rows() == 2
    person = table.search([0.0, 0.0]).limit(1).to_pydantic(PersonModel)
    assert person[0].name == "bob"
    # --8<-- [end:base_example]


@pytest.mark.skipif(
    sys.version_info < (3, 11),
    reason="StrEnum where only introduced in python 3.11",
)
def test_pydantic_str_enum(tmp_path):
    from enum import StrEnum

    # --8<-- [start:base_model]
    class Status(StrEnum):
        Pending = "pending"
        Running = "running"
        Finished = "finished"

    class JobModel(LanceModel):
        name: str
        status: Status

    # --8<-- [end:base_model]

    # --8<-- [start:set_url]
    url = "./example"
    # --8<-- [end:set_url]
    url = tmp_path

    # --8<-- [start:base_example]
    db = lancedb.connect(url)
    table = db.create_table("person", schema=JobModel)
    table.add(
        [
            JobModel(name="insert_articles", status=Status.Running),
            JobModel(name="insert_podcasts", status=Status.Finished),
        ]
    )
    assert table.count_rows() == 2
    job = (
        table.search().where("name = 'insert_articles'").limit(1).to_pydantic(JobModel)
    )
    assert job[0].status == Status.Running
    # --8<-- [end:base_example]
