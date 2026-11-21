# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors


def test_versioning_checkpoint_delta(tmp_db):
    db = tmp_db

    # --8<-- [start:versioning_checkpoint_delta]
    table = db.create_table(
        "checkpoint_delta_example",
        [
            {"id": 1, "author": "Richard", "quote": "Wubba Lubba Dub Dub!"},
            {"id": 2, "author": "Morty", "quote": "Rick, what's going on?"},
        ],
        storage_options={"new_table_enable_stable_row_ids": "true"},
        mode="overwrite",
    )
    table.tags.create("baseline", table.version)
    table.add([{"id": 3, "author": "Summer", "quote": "Whatever, Grandpa"}])
    table.update(where="id = 2", values={"quote": "Aww geez, Rick!"})
    table.delete("id = 1")

    delta = table.delta("baseline")
    print("inserted:", delta.inserted_rows().read_all()["id"].to_pylist())
    print("updated:", delta.updated_rows().read_all()["id"].to_pylist())
    print("deleted:", delta.deleted_row_ids().read_all()["_rowid"].to_pylist())
    print(
        "transactions:",
        [transaction["operation"] for transaction in delta.list_transactions()],
    )
    # --8<-- [end:versioning_checkpoint_delta]

    assert table.version == 4
    assert delta.inserted_rows().read_all()["id"].to_pylist() == [3]
    assert delta.updated_rows().read_all()["id"].to_pylist() == [2]
    assert delta.deleted_row_ids().read_all().num_rows == 1
    assert [transaction["operation"] for transaction in delta.list_transactions()] == [
        "Append",
        "Update",
        "Delete",
    ]
