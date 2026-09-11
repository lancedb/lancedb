# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

import lancedb


def test_session_cache_configuration(tmp_path):
    """Test Session cache configuration and basic functionality."""
    # Create session with small cache limits for testing
    index_cache_size = 1024 * 1024  # 1MB
    metadata_cache_size = 512 * 1024  # 512KB

    session = lancedb.Session(
        index_cache_size_bytes=index_cache_size,
        metadata_cache_size_bytes=metadata_cache_size,
    )

    # Record initial cache state
    initial_cache_size = session.size_bytes
    initial_cache_items = session.approx_num_items

    # Test session works with database connection
    db = lancedb.connect(tmp_path, session=session)

    # Create and use a table to exercise the session
    data = [{"id": i, "text": f"item {i}"} for i in range(100)]
    table = db.create_table("test", data)
    results = list(table.to_arrow().to_pylist())

    assert len(results) == 100

    # Verify cache usage increased after operations
    final_cache_size = session.size_bytes
    final_cache_items = session.approx_num_items

    assert final_cache_size > initial_cache_size  # Cache should have grown
    assert final_cache_items >= initial_cache_items  # Items should not decrease
    assert initial_cache_size < index_cache_size + metadata_cache_size


def test_clear_metadata_cache_releases_cached_metadata(tmp_path):
    """A session holds cached metadata for the life of every connection
    using it, with no way to hand that memory back. Clearing must actually
    shrink it."""
    session = lancedb.Session()
    db = lancedb.connect(tmp_path, session=session)
    table = db.create_table("cached", [{"id": i} for i in range(100)])
    table.to_arrow()

    before = session.size_bytes
    assert before > 0, "expected reading the table to populate the session cache"

    session.clear_metadata_cache()

    assert session.size_bytes < before


def test_clear_metadata_cache_is_a_no_op_on_an_untouched_session():
    """Edge case: clearing a session that never cached anything, and
    clearing twice in a row, must both be harmless rather than failing."""
    session = lancedb.Session()
    baseline = session.size_bytes

    session.clear_metadata_cache()
    session.clear_metadata_cache()

    # A fresh session already reports the fixed overhead of its own cache
    # structures, so the guarantee is that clearing never grows it and
    # leaves nothing cached -- not that it reaches zero.
    assert session.size_bytes <= baseline
    assert session.approx_num_items == 0
