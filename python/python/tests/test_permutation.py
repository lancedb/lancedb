# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

import pyarrow as pa
import pytest

from lancedb.permutation import permutation_builder


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
