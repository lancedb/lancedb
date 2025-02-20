# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

# --8<-- [start:imports]
import lancedb
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
