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

    assert initial_cache_size > 0  # Some initial overhead
    assert initial_cache_size < index_cache_size + metadata_cache_size  # Within limits
    assert initial_cache_items >= 0  # Non-negative

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
