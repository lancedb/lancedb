# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

import pyarrow as pa
import pytest

from lancedb import DBConnection, Table, connect
from lancedb.permutation import Permutation, permutation_builder


def test_split_random_ratios(mem_db):
    """Test random splitting with ratios."""
    tbl = mem_db.create_table(
        "test_table", pa.table({"x": range(100), "y": range(100)})
    )
    permutation_tbl = permutation_builder(tbl).split_random(ratios=[0.3, 0.7]).execute()

    # Check that the table was created and has data
    assert permutation_tbl.count_rows() == 100

    # Check that split_id column exists and has correct values
    data = permutation_tbl.search(None).to_arrow().to_pydict()
    split_ids = data["split_id"]
    assert set(split_ids) == {0, 1}

    # Check approximate split sizes (allowing for rounding)
    split_0_count = split_ids.count(0)
    split_1_count = split_ids.count(1)
    assert 25 <= split_0_count <= 35  # ~30% ± tolerance
    assert 65 <= split_1_count <= 75  # ~70% ± tolerance


def test_split_random_counts(mem_db):
    """Test random splitting with absolute counts."""
    tbl = mem_db.create_table(
        "test_table", pa.table({"x": range(100), "y": range(100)})
    )
    permutation_tbl = permutation_builder(tbl).split_random(counts=[20, 30]).execute()

    # Check that we have exactly the requested counts
    assert permutation_tbl.count_rows() == 50

    data = permutation_tbl.search(None).to_arrow().to_pydict()
    split_ids = data["split_id"]
    assert split_ids.count(0) == 20
    assert split_ids.count(1) == 30


def test_split_random_fixed(mem_db):
    """Test random splitting with fixed number of splits."""
    tbl = mem_db.create_table(
        "test_table", pa.table({"x": range(100), "y": range(100)})
    )
    permutation_tbl = permutation_builder(tbl).split_random(fixed=4).execute()

    # Check that we have 4 splits with 25 rows each
    assert permutation_tbl.count_rows() == 100

    data = permutation_tbl.search(None).to_arrow().to_pydict()
    split_ids = data["split_id"]
    assert set(split_ids) == {0, 1, 2, 3}

    for split_id in range(4):
        assert split_ids.count(split_id) == 25


def test_split_random_with_seed(mem_db):
    """Test that seeded random splits are reproducible."""
    tbl = mem_db.create_table("test_table", pa.table({"x": range(50), "y": range(50)}))

    # Create two identical permutations with same seed
    perm1 = permutation_builder(tbl).split_random(ratios=[0.6, 0.4], seed=42).execute()

    perm2 = permutation_builder(tbl).split_random(ratios=[0.6, 0.4], seed=42).execute()

    # Results should be identical
    data1 = perm1.search(None).to_arrow().to_pydict()
    data2 = perm2.search(None).to_arrow().to_pydict()

    assert data1["row_id"] == data2["row_id"]
    assert data1["split_id"] == data2["split_id"]


def test_split_hash(mem_db):
    """Test hash-based splitting."""
    tbl = mem_db.create_table(
        "test_table",
        pa.table(
            {
                "id": range(100),
                "category": (["A", "B", "C"] * 34)[:100],  # Repeating pattern
                "value": range(100),
            }
        ),
    )

    permutation_tbl = (
        permutation_builder(tbl)
        .split_hash(["category"], [1, 1], discard_weight=0)
        .execute()
    )

    # Should have all 100 rows (no discard)
    assert permutation_tbl.count_rows() == 100

    data = permutation_tbl.search(None).to_arrow().to_pydict()
    split_ids = data["split_id"]
    assert set(split_ids) == {0, 1}

    # Verify that each split has roughly 50 rows (allowing for hash variance)
    split_0_count = split_ids.count(0)
    split_1_count = split_ids.count(1)
    assert 30 <= split_0_count <= 70  # ~50 ± 20 tolerance for hash distribution
    assert 30 <= split_1_count <= 70  # ~50 ± 20 tolerance for hash distribution

    # Hash splits should be deterministic - same category should go to same split
    # Let's verify by creating another permutation and checking consistency
    perm2 = (
        permutation_builder(tbl)
        .split_hash(["category"], [1, 1], discard_weight=0)
        .execute()
    )

    data2 = perm2.search(None).to_arrow().to_pydict()
    assert data["split_id"] == data2["split_id"]  # Should be identical


def test_split_hash_with_discard(mem_db):
    """Test hash-based splitting with discard weight."""
    tbl = mem_db.create_table(
        "test_table",
        pa.table({"id": range(100), "category": ["A", "B"] * 50, "value": range(100)}),
    )

    permutation_tbl = (
        permutation_builder(tbl)
        .split_hash(["category"], [1, 1], discard_weight=2)  # Should discard ~50%
        .execute()
    )

    # Should have fewer than 100 rows due to discard
    row_count = permutation_tbl.count_rows()
    assert row_count < 100
    assert row_count > 0  # But not empty


def test_split_sequential(mem_db):
    """Test sequential splitting."""
    tbl = mem_db.create_table(
        "test_table", pa.table({"x": range(100), "y": range(100)})
    )

    permutation_tbl = (
        permutation_builder(tbl).split_sequential(counts=[30, 40]).execute()
    )

    assert permutation_tbl.count_rows() == 70

    data = permutation_tbl.search(None).to_arrow().to_pydict()
    row_ids = data["row_id"]
    split_ids = data["split_id"]

    # Sequential should maintain order
    assert row_ids == sorted(row_ids)

    # First 30 should be split 0, next 40 should be split 1
    assert split_ids[:30] == [0] * 30
    assert split_ids[30:] == [1] * 40


def test_split_calculated(mem_db):
    """Test calculated splitting."""
    tbl = mem_db.create_table(
        "test_table", pa.table({"id": range(100), "value": range(100)})
    )

    permutation_tbl = (
        permutation_builder(tbl)
        .split_calculated("id % 3")  # Split based on id modulo 3
        .execute()
    )

    assert permutation_tbl.count_rows() == 100

    data = permutation_tbl.search(None).to_arrow().to_pydict()
    row_ids = data["row_id"]
    split_ids = data["split_id"]

    # Verify the calculation: each row's split_id should equal row_id % 3
    for i, (row_id, split_id) in enumerate(zip(row_ids, split_ids)):
        assert split_id == row_id % 3


def test_split_error_cases(mem_db):
    """Test error handling for invalid split parameters."""
    tbl = mem_db.create_table("test_table", pa.table({"x": range(10), "y": range(10)}))

    # Test split_random with no parameters
    with pytest.raises(Exception):
        permutation_builder(tbl).split_random().execute()

    # Test split_random with multiple parameters
    with pytest.raises(Exception):
        permutation_builder(tbl).split_random(
            ratios=[0.5, 0.5], counts=[5, 5]
        ).execute()

    # Test split_sequential with no parameters
    with pytest.raises(Exception):
        permutation_builder(tbl).split_sequential().execute()

    # Test split_sequential with multiple parameters
    with pytest.raises(Exception):
        permutation_builder(tbl).split_sequential(ratios=[0.5, 0.5], fixed=2).execute()


def test_shuffle_no_seed(mem_db):
    """Test shuffling without a seed."""
    tbl = mem_db.create_table(
        "test_table", pa.table({"id": range(100), "value": range(100)})
    )

    # Create a permutation with shuffling (no seed)
    permutation_tbl = permutation_builder(tbl).shuffle().execute()

    assert permutation_tbl.count_rows() == 100

    data = permutation_tbl.search(None).to_arrow().to_pydict()
    row_ids = data["row_id"]

    # Row IDs should not be in sequential order due to shuffling
    # This is probabilistic but with 100 rows, it's extremely unlikely they'd stay
    # in order
    assert row_ids != list(range(100))


def test_shuffle_with_seed(mem_db):
    """Test that shuffling with a seed is reproducible."""
    tbl = mem_db.create_table(
        "test_table", pa.table({"id": range(50), "value": range(50)})
    )

    # Create two identical permutations with same shuffle seed
    perm1 = permutation_builder(tbl).shuffle(seed=42).execute()

    perm2 = permutation_builder(tbl).shuffle(seed=42).execute()

    # Results should be identical due to same seed
    data1 = perm1.search(None).to_arrow().to_pydict()
    data2 = perm2.search(None).to_arrow().to_pydict()

    assert data1["row_id"] == data2["row_id"]
    assert data1["split_id"] == data2["split_id"]


def test_shuffle_with_clump_size(mem_db):
    """Test shuffling with clump size."""
    tbl = mem_db.create_table(
        "test_table", pa.table({"id": range(100), "value": range(100)})
    )

    # Create a permutation with shuffling using clumps
    permutation_tbl = (
        permutation_builder(tbl)
        .shuffle(clump_size=10)  # 10-row clumps
        .execute()
    )

    assert permutation_tbl.count_rows() == 100

    data = permutation_tbl.search(None).to_arrow().to_pydict()
    row_ids = data["row_id"]

    for i in range(10):
        start = row_ids[i * 10]
        assert row_ids[i * 10 : (i + 1) * 10] == list(range(start, start + 10))


def test_shuffle_different_seeds(mem_db):
    """Test that different seeds produce different shuffle orders."""
    tbl = mem_db.create_table(
        "test_table", pa.table({"id": range(50), "value": range(50)})
    )

    # Create two permutations with different shuffle seeds
    perm1 = permutation_builder(tbl).split_random(fixed=2).shuffle(seed=42).execute()

    perm2 = permutation_builder(tbl).split_random(fixed=2).shuffle(seed=123).execute()

    # Results should be different due to different seeds
    data1 = perm1.search(None).to_arrow().to_pydict()
    data2 = perm2.search(None).to_arrow().to_pydict()

    # Row order should be different
    assert data1["row_id"] != data2["row_id"]


def test_shuffle_combined_with_splits(mem_db):
    """Test shuffling combined with different split strategies."""
    tbl = mem_db.create_table(
        "test_table",
        pa.table(
            {
                "id": range(100),
                "category": (["A", "B", "C"] * 34)[:100],
                "value": range(100),
            }
        ),
    )

    # Test shuffle with random splits
    perm_random = (
        permutation_builder(tbl)
        .split_random(ratios=[0.6, 0.4], seed=42)
        .shuffle(seed=123, clump_size=None)
        .execute()
    )

    # Test shuffle with hash splits
    perm_hash = (
        permutation_builder(tbl)
        .split_hash(["category"], [1, 1], discard_weight=0)
        .shuffle(seed=456, clump_size=5)
        .execute()
    )

    # Test shuffle with sequential splits
    perm_sequential = (
        permutation_builder(tbl)
        .split_sequential(counts=[40, 35])
        .shuffle(seed=789, clump_size=None)
        .execute()
    )

    # Verify all permutations work and have expected properties
    assert perm_random.count_rows() == 100
    assert perm_hash.count_rows() == 100
    assert perm_sequential.count_rows() == 75

    # Verify shuffle affected the order
    data_random = perm_random.search(None).to_arrow().to_pydict()
    data_sequential = perm_sequential.search(None).to_arrow().to_pydict()

    assert data_random["row_id"] != list(range(100))
    assert data_sequential["row_id"] != list(range(75))


def test_no_shuffle_maintains_order(mem_db):
    """Test that not calling shuffle maintains the original order."""
    tbl = mem_db.create_table(
        "test_table", pa.table({"id": range(50), "value": range(50)})
    )

    # Create permutation without shuffle (should maintain some order)
    permutation_tbl = (
        permutation_builder(tbl)
        .split_sequential(counts=[25, 25])  # Sequential maintains order
        .execute()
    )

    assert permutation_tbl.count_rows() == 50

    data = permutation_tbl.search(None).to_arrow().to_pydict()
    row_ids = data["row_id"]

    # With sequential splits and no shuffle, should maintain order
    assert row_ids == list(range(50))


def test_filter_basic(mem_db):
    """Test basic filtering functionality."""
    tbl = mem_db.create_table(
        "test_table", pa.table({"id": range(100), "value": range(100, 200)})
    )

    # Filter to only include rows where id < 50
    permutation_tbl = permutation_builder(tbl).filter("id < 50").execute()

    assert permutation_tbl.count_rows() == 50

    data = permutation_tbl.search(None).to_arrow().to_pydict()
    row_ids = data["row_id"]

    # All row_ids should be less than 50
    assert all(row_id < 50 for row_id in row_ids)


def test_filter_with_splits(mem_db):
    """Test filtering combined with split strategies."""
    tbl = mem_db.create_table(
        "test_table",
        pa.table(
            {
                "id": range(100),
                "category": (["A", "B", "C"] * 34)[:100],
                "value": range(100),
            }
        ),
    )

    # Filter to only category A and B, then split
    permutation_tbl = (
        permutation_builder(tbl)
        .filter("category IN ('A', 'B')")
        .split_random(ratios=[0.5, 0.5])
        .execute()
    )

    # Should have fewer than 100 rows due to filtering
    row_count = permutation_tbl.count_rows()
    assert row_count == 67

    data = permutation_tbl.search(None).to_arrow().to_pydict()
    categories = data["category"]

    # All categories should be A or B
    assert all(cat in ["A", "B"] for cat in categories)


def test_filter_with_shuffle(mem_db):
    """Test filtering combined with shuffling."""
    tbl = mem_db.create_table(
        "test_table",
        pa.table(
            {
                "id": range(100),
                "category": (["A", "B", "C", "D"] * 25)[:100],
                "value": range(100),
            }
        ),
    )

    # Filter and shuffle
    permutation_tbl = (
        permutation_builder(tbl)
        .filter("category IN ('A', 'C')")
        .shuffle(seed=42)
        .execute()
    )

    row_count = permutation_tbl.count_rows()
    assert row_count == 50  # Should have 50 rows (A and C categories)

    data = permutation_tbl.search(None).to_arrow().to_pydict()
    row_ids = data["row_id"]

    assert row_ids != sorted(row_ids)


def test_filter_empty_result(mem_db):
    """Test filtering that results in empty set."""
    tbl = mem_db.create_table(
        "test_table", pa.table({"id": range(10), "value": range(10)})
    )

    # Filter that matches nothing
    permutation_tbl = (
        permutation_builder(tbl)
        .filter("value > 100")  # No values > 100 in our data
        .execute()
    )

    assert permutation_tbl.count_rows() == 0


@pytest.fixture
def mem_db() -> DBConnection:
    return connect("memory:///")


@pytest.fixture
def some_table(mem_db: DBConnection) -> Table:
    data = pa.table(
        {
            "id": range(10000),
            "value": range(10000),
        }
    )
    return mem_db.create_table("some_table", data)


@pytest.fixture
def some_permutation(some_table: Table) -> Permutation:
    perm = (
        permutation_builder(some_table)
        .split_random(ratios=[0.95, 0.05], seed=42)
        .shuffle(seed=42)
        .execute()
    )
    return Permutation.from_tables(some_table, perm)


def test_num_rows(some_permutation: Permutation):
    assert some_permutation.num_rows == 9500


def test_num_columns(some_permutation: Permutation):
    assert some_permutation.num_columns == 2


def test_column_names(some_permutation: Permutation):
    assert some_permutation.column_names == ["id", "value"]


def test_shape(some_permutation: Permutation):
    assert some_permutation.shape == (9500, 2)


def test_schema(some_permutation: Permutation):
    assert some_permutation.schema == pa.schema(
        [("id", pa.int64()), ("value", pa.int64())]
    )


def test_limit_offset(some_permutation: Permutation):
    assert some_permutation.with_take(100).num_rows == 100
    assert some_permutation.with_skip(100).num_rows == 9400
    assert some_permutation.with_take(100).with_skip(100).num_rows == 100

    with pytest.raises(Exception):
        some_permutation.with_take(1000000).num_rows
    with pytest.raises(Exception):
        some_permutation.with_skip(1000000).num_rows
    with pytest.raises(Exception):
        some_permutation.with_take(5000).with_skip(5000).num_rows
    with pytest.raises(Exception):
        some_permutation.with_skip(5000).with_take(5000).num_rows


def test_remove_columns(some_permutation: Permutation):
    assert some_permutation.remove_columns(["value"]).schema == pa.schema(
        [("id", pa.int64())]
    )
    # Should not modify the original permutation
    assert some_permutation.schema.names == ["id", "value"]
    # Cannot remove all columns
    with pytest.raises(ValueError, match="Cannot remove all columns"):
        some_permutation.remove_columns(["id", "value"])


def test_rename_column(some_permutation: Permutation):
    assert some_permutation.rename_column("value", "new_value").schema == pa.schema(
        [("id", pa.int64()), ("new_value", pa.int64())]
    )
    # Should not modify the original permutation
    assert some_permutation.schema.names == ["id", "value"]
    # Cannot rename to an existing column
    with pytest.raises(
        ValueError,
        match="a column with that name already exists",
    ):
        some_permutation.rename_column("value", "id")
    # Cannot rename a non-existent column
    with pytest.raises(
        ValueError,
        match="does not exist",
    ):
        some_permutation.rename_column("non_existent", "new_value")


def test_rename_columns(some_permutation: Permutation):
    assert some_permutation.rename_columns({"value": "new_value"}).schema == pa.schema(
        [("id", pa.int64()), ("new_value", pa.int64())]
    )
    # Should not modify the original permutation
    assert some_permutation.schema.names == ["id", "value"]
    # Cannot rename to an existing column
    with pytest.raises(ValueError, match="a column with that name already exists"):
        some_permutation.rename_columns({"value": "id"})


def test_select_columns(some_permutation: Permutation):
    assert some_permutation.select_columns(["id"]).schema == pa.schema(
        [("id", pa.int64())]
    )
    # Should not modify the original permutation
    assert some_permutation.schema.names == ["id", "value"]
    # Cannot select a non-existent column
    with pytest.raises(ValueError, match="does not exist"):
        some_permutation.select_columns(["non_existent"])
    # Empty selection is not allowed
    with pytest.raises(ValueError, match="select at least one column"):
        some_permutation.select_columns([])


def test_iter_basic(some_permutation: Permutation):
    """Test basic iteration with custom batch size."""
    batch_size = 100
    batches = list(some_permutation.iter(batch_size, skip_last_batch=False))

    # Check that we got the expected number of batches
    expected_batches = (9500 + batch_size - 1) // batch_size  # ceiling division
    assert len(batches) == expected_batches

    # Check that all batches are dicts (default python format)
    assert all(isinstance(batch, dict) for batch in batches)

    # Check that batches have the correct structure
    for batch in batches:
        assert "id" in batch
        assert "value" in batch
        assert isinstance(batch["id"], list)
        assert isinstance(batch["value"], list)

    # Check that all batches except the last have the correct size
    for batch in batches[:-1]:
        assert len(batch["id"]) == batch_size
        assert len(batch["value"]) == batch_size

    # Last batch might be smaller
    assert len(batches[-1]["id"]) <= batch_size


def test_iter_skip_last_batch(some_permutation: Permutation):
    """Test iteration with skip_last_batch=True."""
    batch_size = 300
    batches_with_skip = list(some_permutation.iter(batch_size, skip_last_batch=True))
    batches_without_skip = list(
        some_permutation.iter(batch_size, skip_last_batch=False)
    )

    # With skip_last_batch=True, we should have fewer batches if the last one is partial
    num_full_batches = 9500 // batch_size
    assert len(batches_with_skip) == num_full_batches

    # Without skip_last_batch, we should have one more batch if there's a remainder
    if 9500 % batch_size != 0:
        assert len(batches_without_skip) == num_full_batches + 1
        # Last batch should be smaller
        assert len(batches_without_skip[-1]["id"]) == 9500 % batch_size

    # All batches with skip_last_batch should be full size
    for batch in batches_with_skip:
        assert len(batch["id"]) == batch_size


def test_iter_different_batch_sizes(some_permutation: Permutation):
    """Test iteration with different batch sizes."""

    # Test with small batch size
    small_batches = list(some_permutation.iter(100, skip_last_batch=False))
    assert len(small_batches) == 95  # 9500 / 100

    # Test with large batch size
    large_batches = list(some_permutation.iter(1000, skip_last_batch=False))
    assert len(large_batches) == 10  # ceiling(9500 / 1000)

    # Test with batch size equal to total rows
    single_batch = list(some_permutation.iter(9500, skip_last_batch=False))
    assert len(single_batch) == 1
    assert len(single_batch[0]["id"]) == 9500

    # Note: Very large batch sizes (>= total rows) are currently not supported
    # by the underlying Rust implementation due to how it batches data from
    # the base table. This is an expected limitation.

    # Test with batch size larger than total rows
    oversized_batch = list(some_permutation.iter(10000, skip_last_batch=False))
    assert len(oversized_batch) == 1
    assert len(oversized_batch[0]["id"]) == 9500


def test_dunder_iter(some_permutation: Permutation):
    """Test the __iter__ method."""
    # __iter__ should use DEFAULT_BATCH_SIZE (10) and skip_last_batch=True
    batches = list(some_permutation)

    # With DEFAULT_BATCH_SIZE=100 and skip_last_batch=True, we should get 950 batches
    assert len(batches) == 95  # 9500 / 100

    # All batches should be full size
    for batch in batches:
        assert len(batch["id"]) == 100
        assert len(batch["value"]) == 100

    some_permutation = some_permutation.with_batch_size(1000)
    batches = list(some_permutation)
    assert len(batches) == 9  # floor(9500 / 1000) since skip_last_batch=True
    for batch in batches:
        assert len(batch["id"]) == 1000
        assert len(batch["value"]) == 1000


def test_iter_with_different_formats(some_permutation: Permutation):
    """Test iteration with different output formats."""
    batch_size = 100

    # Test with arrow format
    arrow_perm = some_permutation.with_format("arrow")
    arrow_batches = list(arrow_perm.iter(batch_size, skip_last_batch=False))
    assert all(isinstance(batch, pa.RecordBatch) for batch in arrow_batches)

    # Test with python format (default)
    python_perm = some_permutation.with_format("python")
    python_batches = list(python_perm.iter(batch_size, skip_last_batch=False))
    assert all(isinstance(batch, dict) for batch in python_batches)

    # Test with pandas format
    pandas_perm = some_permutation.with_format("pandas")
    pandas_batches = list(pandas_perm.iter(batch_size, skip_last_batch=False))
    # Import pandas to check the type
    import pandas as pd

    assert all(isinstance(batch, pd.DataFrame) for batch in pandas_batches)


def test_iter_with_column_selection(some_permutation: Permutation):
    """Test iteration after column selection."""
    # Select only the id column
    id_only = some_permutation.select_columns(["id"])
    batches = list(id_only.iter(100, skip_last_batch=False))

    # Check that batches only contain the id column
    for batch in batches:
        assert "id" in batch
        assert "value" not in batch


def test_iter_with_column_rename(some_permutation: Permutation):
    """Test iteration after renaming columns."""
    renamed = some_permutation.rename_column("value", "data")
    batches = list(renamed.iter(100, skip_last_batch=False))

    # Check that batches have the renamed column
    for batch in batches:
        assert "id" in batch
        assert "data" in batch
        assert "value" not in batch


def test_iter_with_limit_offset(some_permutation: Permutation):
    """Test iteration with limit and offset."""
    # Test with offset
    offset_perm = some_permutation.with_skip(100)
    offset_batches = list(offset_perm.iter(100, skip_last_batch=False))
    # Should have 9400 rows (9500 - 100)
    expected_batches = (9400 + 100 - 1) // 100
    assert len(offset_batches) == expected_batches

    # Test with limit
    limit_perm = some_permutation.with_take(500)
    limit_batches = list(limit_perm.iter(100, skip_last_batch=False))
    # Should have 5 batches (500 / 100)
    assert len(limit_batches) == 5

    # Test with both limit and offset
    limited_perm = some_permutation.with_skip(100).with_take(300)
    limited_batches = list(limited_perm.iter(100, skip_last_batch=False))
    # Should have 3 batches (300 / 100)
    assert len(limited_batches) == 3


def test_iter_empty_permutation(mem_db):
    """Test iteration over an empty permutation."""
    # Create a table and filter it to be empty
    tbl = mem_db.create_table(
        "test_table", pa.table({"id": range(10), "value": range(10)})
    )
    permutation_tbl = permutation_builder(tbl).filter("value > 100").execute()
    perm = Permutation.from_tables(tbl, permutation_tbl)

    # Currently, the Rust implementation raises an error for empty permutations
    # rather than returning an empty iterator. This is expected behavior.
    with pytest.raises(ValueError, match="Permutation was empty"):
        list(perm.iter(10, skip_last_batch=False))


def test_iter_single_row(mem_db):
    """Test iteration over a permutation with a single row."""
    tbl = mem_db.create_table("test_table", pa.table({"id": [42], "value": [100]}))
    permutation_tbl = permutation_builder(tbl).execute()
    perm = Permutation.from_tables(tbl, permutation_tbl)

    # With skip_last_batch=False, should get one batch
    batches = list(perm.iter(10, skip_last_batch=False))
    assert len(batches) == 1
    assert len(batches[0]["id"]) == 1

    # With skip_last_batch=True, should skip the single row (since it's < batch_size)
    batches_skip = list(perm.iter(10, skip_last_batch=True))
    assert len(batches_skip) == 0
