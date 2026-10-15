# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors


def test_sample_dataset():
    # --8<-- [start:sample_dataset]
    from tempfile import TemporaryDirectory

    import lancedb
    from lancedb.permutation import Permutation, permutation_builder

    with TemporaryDirectory(prefix="lancedb-sampling-") as uri:
        db = lancedb.connect(uri)
        table = db.create_table("records", [{"id": i} for i in range(10)])
        splits = (
            permutation_builder(table).split_random(counts=[3, 7], seed=42).execute()
        )
        sample = Permutation.from_tables(table, splits, split=0).with_format("arrow")
        selected_ids = []
        for batch in sample.iter(batch_size=3):
            selected_ids.extend(batch.column("id").to_pylist())
        print(f"Selected {len(selected_ids)} rows: {selected_ids}")
    # --8<-- [end:sample_dataset]

    assert len(selected_ids) == 3
    assert len(set(selected_ids)) == 3
    assert all(0 <= value < 10 for value in selected_ids)
