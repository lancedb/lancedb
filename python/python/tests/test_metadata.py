# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

import pytest
import tempfile
import lancedb as db
import pyarrow as pa


def sample_data():
    """Create sample data for testing."""
    return pa.table({
        "vector": [[1.0, 2.0], [3.0, 4.0], [5.0, 6.0]],
        "text": ["hello", "world", "test"],
        "id": [1, 2, 3]
    })


@pytest.mark.asyncio
async def test_metadata_accessors_exist():
    """Test that metadata accessor objects can be created."""
    with tempfile.TemporaryDirectory() as tmpdir:
        conn = await db.connect_async(tmpdir)
        
        data = sample_data()
        table = await conn.create_table("test_table", data)
        
        # Test that accessor objects are created correctly
        table_metadata = table.metadata
        schema_metadata = table.schema_metadata
        config = table.config
        
        # Check that the objects are the correct types
        assert hasattr(table_metadata, 'get')
        assert hasattr(table_metadata, 'update')
        assert hasattr(schema_metadata, 'get')
        assert hasattr(schema_metadata, 'update')
        assert hasattr(config, 'get')
        assert hasattr(config, 'update')


@pytest.mark.asyncio
async def test_metadata_get_panics_with_todo():
    """Test that metadata get methods panic with todo! message since Lance support is not yet implemented."""
    with tempfile.TemporaryDirectory() as tmpdir:
        conn = await db.connect_async(tmpdir)
        
        data = sample_data()
        table = await conn.create_table("test_table", data)
        
        # Test that calling get() on local tables panics with todo! message
        # Since we're using todo!() in the local implementation, these should fail
        
        with pytest.raises(Exception) as exc_info:
            await table.metadata.get()
        
        # Should panic from Rust with our todo! message
        assert "panicked" in str(exc_info.value)
        
        with pytest.raises(Exception) as exc_info:
            await table.schema_metadata.get()
        
        assert "panicked" in str(exc_info.value)
        
        with pytest.raises(Exception) as exc_info:
            await table.config.get()
        
        assert "panicked" in str(exc_info.value)


@pytest.mark.asyncio  
async def test_metadata_update_panics_with_todo():
    """Test that metadata update methods panic with todo! message since Lance support is not yet implemented."""
    with tempfile.TemporaryDirectory() as tmpdir:
        conn = await db.connect_async(tmpdir)
        
        data = sample_data()
        table = await conn.create_table("test_table", data)
        
        # Test that calling update() on local tables panics with todo! message
        with pytest.raises(Exception) as exc_info:
            await table.metadata.update({"key": "value"})
        
        assert "panicked" in str(exc_info.value)
        
        with pytest.raises(Exception) as exc_info:
            await table.schema_metadata.update({"key": "value"})
        
        assert "panicked" in str(exc_info.value)
        
        with pytest.raises(Exception) as exc_info:
            await table.config.update({"key": "value"})
        
        assert "panicked" in str(exc_info.value)


@pytest.mark.asyncio
async def test_metadata_update_replace_parameter():
    """Test that the replace parameter is properly handled."""
    with tempfile.TemporaryDirectory() as tmpdir:
        conn = await db.connect_async(tmpdir)
        
        data = sample_data()
        table = await conn.create_table("test_table", data)
        
        # Test that calling update() with replace=True also panics (with todo!)
        with pytest.raises(Exception) as exc_info:
            await table.metadata.update({"key": "value"}, replace=True)
        
        assert "panicked" in str(exc_info.value)
        
        # Test with replace=False (default)
        with pytest.raises(Exception) as exc_info:
            await table.metadata.update({"key": "value"}, replace=False)
        
        assert "panicked" in str(exc_info.value)


@pytest.mark.asyncio  
async def test_metadata_update_with_none_values():
    """Test that None values in metadata updates are handled properly."""
    with tempfile.TemporaryDirectory() as tmpdir:
        conn = await db.connect_async(tmpdir)
        
        data = sample_data()
        table = await conn.create_table("test_table", data)
        
        # Test that calling update() with None values (for deletion) panics with todo!
        with pytest.raises(Exception) as exc_info:
            await table.metadata.update({"key_to_delete": None, "key_to_set": "value"})
        
        assert "panicked" in str(exc_info.value)