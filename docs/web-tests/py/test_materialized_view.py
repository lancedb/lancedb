# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

"""Examples for docs/web/feature-engineering/materialized-views.mdx."""


def test_materialized_view_create_and_refresh(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)

    # --8<-- [start:materialized_view_create]
    import lancedb

    db = lancedb.connect(
        "./.lancedb",
        storage_options={"new_table_enable_stable_row_ids": "true"},
    )
    table = db.create_table(
        "people", [{"name": "ada", "age": 36}, {"name": "kid", "age": 7}]
    )

    view = db.create_materialized_view(
        "adults",
        "people",
        select=["name", ("shout", "upper(name)")],
        where="age >= 18",
    )
    print(view.table.count_rows())
    # 1
    # --8<-- [end:materialized_view_create]
    assert view.table.count_rows() == 1

    # --8<-- [start:materialized_view_refresh]
    table.add([{"name": "bea", "age": 41}])

    result = view.refresh()
    print(result.rows_written)
    # 1
    # --8<-- [end:materialized_view_refresh]
    assert result.rows_written == 1
    assert sorted(row["shout"] for row in view.table.to_arrow().to_pylist()) == [
        "ADA",
        "BEA",
    ]


def test_materialized_view_with_no_data(tmp_path):
    import lancedb

    db = lancedb.connect(
        tmp_path, storage_options={"new_table_enable_stable_row_ids": "true"}
    )
    db.create_table("people", [{"name": "ada", "age": 36}])
    view = db.create_materialized_view(
        "adults", "people", where="age >= 18", with_no_data=True
    )
    assert view.table.count_rows() == 0
    assert view.refresh().rows_written == 1
