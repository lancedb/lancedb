# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors


def test_read_consistency():
    # --8<-- [start:read_consistency]
    from datetime import timedelta
    from tempfile import TemporaryDirectory

    import lancedb

    with TemporaryDirectory(prefix="lancedb-consistency-") as uri:
        writer = lancedb.connect(uri)
        table = writer.create_table("documents", [{"id": 1, "text": "first"}])

        default_reader = lancedb.connect(uri).open_table("documents")
        latest_reader = lancedb.connect(
            uri, read_consistency_interval=timedelta(0)
        ).open_table("documents")

        table.add([{"id": 2, "text": "second"}])

        default_count = default_reader.count_rows()
        latest_count = latest_reader.count_rows()
        print(f"Default reader: {default_count} rows")  # 1
        print(f"Zero-interval reader: {latest_count} rows")  # 2

        default_reader.checkout_latest()
        refreshed_count = default_reader.count_rows()
        print(f"After explicit refresh: {refreshed_count} rows")  # 2
    # --8<-- [end:read_consistency]

    assert default_count == 1
    assert latest_count == 2
    assert refreshed_count == 2
