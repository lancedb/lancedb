// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Utility functions for MemWAL operations.

use lance_io::object_store::ObjectStoreParams;
use object_store::path::Path;
use uuid::Uuid;

// ============================================================================
// Watchable Cell
// ============================================================================

/// A cell that can be written to once and read by multiple readers.
///
/// Used for durability notifications where multiple callers may need to await the same result.
#[derive(Clone, Debug)]
pub struct WatchableOnceCell<T: Clone + std::fmt::Debug> {
    rx: tokio::sync::watch::Receiver<Option<T>>,
    tx: tokio::sync::watch::Sender<Option<T>>,
}

/// Reader handle for a WatchableOnceCell.
///
/// Can be cloned and shared across tasks to await the same value.
#[derive(Clone, Debug)]
pub struct WatchableOnceCellReader<T: Clone + std::fmt::Debug> {
    rx: tokio::sync::watch::Receiver<Option<T>>,
}

impl<T: Clone + std::fmt::Debug> WatchableOnceCell<T> {
    /// Create a new empty cell.
    pub fn new() -> Self {
        let (tx, rx) = tokio::sync::watch::channel(None);
        Self { rx, tx }
    }

    /// Write a value to the cell.
    ///
    /// Only the first write takes effect; subsequent writes are ignored.
    pub fn write(&self, val: T) {
        self.tx.send_if_modified(|v| {
            if v.is_some() {
                return false;
            }
            v.replace(val);
            true
        });
    }

    /// Get a reader handle for this cell.
    pub fn reader(&self) -> WatchableOnceCellReader<T> {
        WatchableOnceCellReader {
            rx: self.rx.clone(),
        }
    }
}

impl<T: Clone + std::fmt::Debug> Default for WatchableOnceCell<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: Clone + std::fmt::Debug> WatchableOnceCellReader<T> {
    /// Read the current value without waiting.
    ///
    /// Returns `None` if no value has been written yet.
    pub fn read(&self) -> Option<T> {
        self.rx.borrow().clone()
    }

    /// Wait for a value to be written.
    ///
    /// Returns immediately if a value is already present. Returns
    /// `None` if the underlying watch channel was closed before a
    /// value was published — typically because the producing task
    /// died (panicked or returned `Err`). Earlier this case used to
    /// panic with `expect("watch channel closed")`, which turned a
    /// dead-flusher consequence into a worker panic and made the
    /// merge_insert call fail with a `RustPanic` instead of a
    /// recoverable error.
    pub async fn await_value(&mut self) -> Option<T> {
        self.rx.wait_for(|v| v.is_some()).await.ok()?;
        self.rx.borrow().clone()
    }
}

/// Bit-reverse a 64-bit integer.
///
/// Used for file naming to distribute files evenly across object store keyspace,
/// optimizing S3 throughput by spreading sequential writes across internal partitions.
///
/// # Example
/// ```ignore
/// // 5 in binary: 000...101
/// // Reversed:    101...000
/// assert_eq!(bit_reverse_u64(5), 0xa000000000000000);
/// ```
pub fn bit_reverse_u64(n: u64) -> u64 {
    n.reverse_bits()
}

/// Generate a bit-reversed filename for a given ID.
///
/// # Arguments
/// * `id` - The sequential ID to convert
/// * `ext` - File extension (e.g., "binpb", "lance")
///
/// # Returns
/// A string like "1010000000000000000000000000000000000000000000000000000000000000.binpb"
/// for id=5, ext="binpb"
pub fn bit_reversed_filename(id: u64, ext: &str) -> String {
    format!("{:064b}.{}", bit_reverse_u64(id), ext)
}

/// Parse a bit-reversed filename back to the original ID.
///
/// # Arguments
/// * `filename` - The filename without path (e.g., "1010...0000.binpb")
///
/// # Returns
/// The original ID, or None if parsing fails
pub fn parse_bit_reversed_filename(filename: &str) -> Option<u64> {
    let stem = filename.split('.').next()?;
    if stem.len() != 64 || !stem.chars().all(|c| c == '0' || c == '1') {
        return None;
    }
    let reversed = u64::from_str_radix(stem, 2).ok()?;
    Some(bit_reverse_u64(reversed))
}

/// Adapt the store params a base dataset was opened with for use on a URI
/// *derived* from it (an SSTable under `_mem_wal/`).
///
/// The deprecated `object_store` binding pins a store to one location: given
/// `Some((store, url))`, both `ObjectStore::from_uri_and_params` and
/// `DatasetBuilder::build_object_store` take the path from `url` and ignore the
/// URI they were asked to open. Carried onto a generation URI it would silently
/// redirect the open — and, on the flush path, the write — at the base table
/// itself. Drop it so the generation URI resolves its own store; everything
/// else (storage options, wrapper, credentials, block size) still carries over.
///
/// Only the base's *own* URI may reuse the params verbatim.
pub(crate) fn derived_store_params(params: &ObjectStoreParams) -> ObjectStoreParams {
    #[allow(deprecated)]
    ObjectStoreParams {
        object_store: None,
        ..params.clone()
    }
}

/// Path to the MemWAL root directory.
///
/// Returns: `{base_path}/_mem_wal/`
pub fn mem_wal_path(base_path: &Path) -> Path {
    base_path.clone().join("_mem_wal")
}

/// Base path for a shard within the MemWAL directory.
///
/// Returns: `{base_path}/_mem_wal/{shard_id}/`
pub fn shard_base_path(base_path: &Path, shard_id: &Uuid) -> Path {
    mem_wal_path(base_path).join(shard_id.as_hyphenated().to_string())
}

/// Path to the WAL directory for a shard.
///
/// Returns: `{base_path}/_mem_wal/{shard_id}/wal/`
pub fn shard_wal_path(base_path: &Path, shard_id: &Uuid) -> Path {
    shard_base_path(base_path, shard_id).join("wal")
}

/// Path to the manifest directory for a shard.
///
/// Returns: `{base_path}/_mem_wal/{shard_id}/manifest/`
pub fn shard_manifest_path(base_path: &Path, shard_id: &Uuid) -> Path {
    shard_base_path(base_path, shard_id).join("manifest")
}

/// Path to an SSTable directory.
///
/// Returns: `{base_path}/_mem_wal/{shard_id}/{random_hash}_gen_{generation}/`
pub fn sstable_path(base_path: &Path, shard_id: &Uuid, random_hash: &str, generation: u64) -> Path {
    shard_base_path(base_path, shard_id).join(format!("{}_gen_{}", random_hash, generation))
}

/// Subdirectory of an SSTable holding its standalone primary-key
/// dedup index (a sidecar BTree, not registered in the manifest). Both the
/// flush writer and the block-list probe join this onto the generation path.
pub const PK_INDEX_DIR: &str = "_pk_index";

/// Path to an SSTable's standalone primary-key dedup index.
pub fn pk_index_path(gen_path: &Path) -> Path {
    gen_path.clone().join(PK_INDEX_DIR)
}

/// Generate an 8-character random hex string for SSTable directories.
pub fn generate_random_hash() -> String {
    let bytes: [u8; 4] = rand::random();
    format!(
        "{:02x}{:02x}{:02x}{:02x}",
        bytes[0], bytes[1], bytes[2], bytes[3]
    )
}

/// WAL entry filename.
///
/// Returns bit-reversed filename with .arrow extension (Arrow IPC format).
pub fn wal_entry_filename(wal_entry_position: u64) -> String {
    bit_reversed_filename(wal_entry_position, "arrow")
}

/// Shard manifest filename.
///
/// Returns bit-reversed filename with .binpb extension.
pub fn manifest_filename(version: u64) -> String {
    bit_reversed_filename(version, "binpb")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bit_reverse_u64() {
        // 0 should remain 0
        assert_eq!(bit_reverse_u64(0), 0);

        // 1 (least significant bit) becomes most significant
        assert_eq!(bit_reverse_u64(1), 0x8000000000000000);

        // 5 = 101 in binary, reversed = 101 followed by 61 zeros
        assert_eq!(bit_reverse_u64(5), 0xa000000000000000);

        // Double reversal should give original
        for i in [0u64, 1, 2, 5, 100, 1000, u64::MAX / 2, u64::MAX] {
            assert_eq!(bit_reverse_u64(bit_reverse_u64(i)), i);
        }
    }

    #[test]
    fn test_bit_reversed_filename() {
        let filename = bit_reversed_filename(1, "binpb");
        assert_eq!(
            filename,
            "1000000000000000000000000000000000000000000000000000000000000000.binpb"
        );

        let filename = bit_reversed_filename(5, "lance");
        assert_eq!(
            filename,
            "1010000000000000000000000000000000000000000000000000000000000000.lance"
        );
    }

    #[test]
    fn test_parse_bit_reversed_filename() {
        // Round-trip test
        for id in [1u64, 5, 100, 1000, u64::MAX / 2] {
            let filename = bit_reversed_filename(id, "binpb");
            let parsed = parse_bit_reversed_filename(&filename);
            assert_eq!(parsed, Some(id), "Failed round-trip for id={}", id);
        }

        // Invalid inputs
        assert_eq!(parse_bit_reversed_filename("invalid"), None);
        assert_eq!(parse_bit_reversed_filename("123.binpb"), None);
        assert_eq!(
            parse_bit_reversed_filename(
                "10100000000000000000000000000000000000000000000000000000000000002.binpb"
            ),
            None
        );
    }

    #[test]
    fn test_mem_wal_path() {
        let base_path = Path::from("my/dataset");
        assert_eq!(mem_wal_path(&base_path).as_ref(), "my/dataset/_mem_wal");

        let empty_base = Path::from("");
        assert_eq!(mem_wal_path(&empty_base).as_ref(), "_mem_wal");
    }

    #[test]
    fn test_shard_paths() {
        let base_path = Path::from("my/dataset");
        let shard_id = Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();

        assert_eq!(
            shard_base_path(&base_path, &shard_id).as_ref(),
            "my/dataset/_mem_wal/550e8400-e29b-41d4-a716-446655440000"
        );

        assert_eq!(
            shard_wal_path(&base_path, &shard_id).as_ref(),
            "my/dataset/_mem_wal/550e8400-e29b-41d4-a716-446655440000/wal"
        );

        assert_eq!(
            shard_manifest_path(&base_path, &shard_id).as_ref(),
            "my/dataset/_mem_wal/550e8400-e29b-41d4-a716-446655440000/manifest"
        );

        assert_eq!(
            sstable_path(&base_path, &shard_id, "a1b2c3d4", 5).as_ref(),
            "my/dataset/_mem_wal/550e8400-e29b-41d4-a716-446655440000/a1b2c3d4_gen_5"
        );

        // Test with empty base path
        let empty_base = Path::from("");
        assert_eq!(
            shard_wal_path(&empty_base, &shard_id).as_ref(),
            "_mem_wal/550e8400-e29b-41d4-a716-446655440000/wal"
        );
    }

    #[test]
    fn test_generate_random_hash() {
        let hash = generate_random_hash();
        assert_eq!(hash.len(), 8);
        assert!(hash.chars().all(|c| c.is_ascii_hexdigit()));

        // Should generate different values (with very high probability)
        let hash2 = generate_random_hash();
        assert_ne!(hash, hash2);
    }

    #[tokio::test]
    async fn test_watchable_once_cell_write_once() {
        let cell = WatchableOnceCell::new();
        let reader = cell.reader();

        assert_eq!(reader.read(), None);

        cell.write(42);
        assert_eq!(reader.read(), Some(42));

        // Second write is ignored
        cell.write(100);
        assert_eq!(reader.read(), Some(42));
    }

    #[tokio::test]
    async fn test_watchable_once_cell_await() {
        let cell = WatchableOnceCell::new();
        let mut reader = cell.reader();

        let handle = tokio::spawn(async move { reader.await_value().await });

        // Brief delay to ensure the task is waiting
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;

        cell.write(123);

        let result = handle.await.unwrap();
        assert_eq!(result, Some(123));
    }

    #[tokio::test]
    async fn test_watchable_once_cell_multiple_readers() {
        let cell = WatchableOnceCell::new();
        let mut reader1 = cell.reader();
        let mut reader2 = cell.reader();

        let h1 = tokio::spawn(async move { reader1.await_value().await });
        let h2 = tokio::spawn(async move { reader2.await_value().await });

        tokio::time::sleep(std::time::Duration::from_millis(10)).await;

        cell.write(456);

        assert_eq!(h1.await.unwrap(), Some(456));
        assert_eq!(h2.await.unwrap(), Some(456));
    }

    #[tokio::test]
    async fn test_watchable_once_cell_returns_none_on_close() {
        let cell: WatchableOnceCell<i32> = WatchableOnceCell::new();
        let mut reader = cell.reader();

        // Drop the cell (and thereby the sender) without ever writing.
        // Earlier this caused `await_value` to panic with
        // "watch channel closed"; now it returns None so callers can
        // surface the dead-producer condition as a recoverable error.
        let handle = tokio::spawn(async move { reader.await_value().await });
        drop(cell);
        assert_eq!(handle.await.unwrap(), None);
    }

    /// The path-bound store binding is the only thing dropped — credentials and
    /// storage options must still reach the generation's store.
    #[test]
    fn test_derived_store_params_drops_only_the_path_bound_store() {
        let accessor = lance_io::object_store::StorageOptionsAccessor::with_static_options(
            std::collections::HashMap::from([("access_key_id".to_string(), "key".to_string())]),
        );
        #[allow(deprecated)]
        let params = ObjectStoreParams {
            object_store: Some((
                std::sync::Arc::new(object_store::memory::InMemory::new()),
                url::Url::parse("memory:///base").unwrap(),
            )),
            block_size: Some(1234),
            storage_options_accessor: Some(std::sync::Arc::new(accessor)),
            ..Default::default()
        };

        let derived = derived_store_params(&params);

        #[allow(deprecated)]
        {
            assert!(
                derived.object_store.is_none(),
                "a store pinned to the base path must not be reused for a generation URI"
            );
        }
        assert_eq!(derived.block_size, Some(1234));
        assert_eq!(
            derived
                .storage_options()
                .and_then(|o| o.get("access_key_id")),
            Some(&"key".to_string()),
        );
    }
}
