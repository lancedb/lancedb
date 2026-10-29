# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

"""Examples for docs/web/feature-engineering/materialized-views.mdx."""


def test_materialized_view_create_and_refresh(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)

    # --8<-- [start:materialized_view_create]
    import lancedb

    db = lancedb.connect("./.lancedb")
    table = db.create_table(
        "people",
        [{"name": "ada", "age": 36}, {"name": "kid", "age": 7}],
        storage_options={"new_table_enable_stable_row_ids": "true"},
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

    view.refresh()
    print(view.table.count_rows())
    # 2
    # --8<-- [end:materialized_view_refresh]
    assert view.table.count_rows() == 2
    assert sorted(row["shout"] for row in view.table.to_arrow().to_pylist()) == [
        "ADA",
        "BEA",
    ]


def test_materialized_view_with_no_data(tmp_path):
    import lancedb

    db = lancedb.connect(tmp_path)
    db.create_table(
        "people",
        [{"name": "ada", "age": 36}, {"name": "kid", "age": 7}],
        storage_options={"new_table_enable_stable_row_ids": "true"},
    )
    view = db.create_materialized_view(
        "adults", "people", where="age >= 18", with_no_data=True
    )
    assert view.table.count_rows() == 0
    assert view.refresh().rows_written == 1
    assert view.table.count_rows() == 1


def test_materialized_view_refresh_to_source_version(tmp_path):
    import lancedb

    db = lancedb.connect(tmp_path)
    table = db.create_table(
        "people",
        [{"name": "ada", "age": 36}],
        storage_options={"new_table_enable_stable_row_ids": "true"},
    )
    view = db.create_materialized_view("adults", "people", where="age >= 18")
    first = table.version
    table.add([{"name": "bea", "age": 41}])
    view.refresh()
    assert view.table.count_rows() == 2

    result = view.refresh(source_version=first)
    assert (result.source_version, result.rows_written) == (first, 1)
    assert view.table.count_rows() == 1
