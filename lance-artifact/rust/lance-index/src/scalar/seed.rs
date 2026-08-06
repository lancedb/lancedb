// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Index seed writers — compact per-fragment summaries embedded in data files.
//!
//! A seed writer observes column values as they are written to a data file,
//! accumulates compact statistics in memory, and serializes them to a byte
//! buffer that is embedded in the data file footer as a global buffer.
//!
//! The buffer can later be read back during index updates to reconstruct index
//! statistics without re-scanning the column data.

use arrow_array::ArrayRef;
use bytes::Bytes;
use lance_core::Result;

/// Schema metadata key prefix for all seed buffers: `"lance.seed.<column_name>"`.
pub const SEED_META_KEY_PREFIX: &str = "lance.seed.";

/// A hook registered during data file writes that observes column values batch
/// by batch, accumulates compact statistics in memory, and serializes them to
/// a byte buffer that is embedded in the data file footer as a global buffer.
///
/// The buffer can later be read back during index updates to reconstruct index
/// statistics without re-scanning the column data.
pub trait IndexSeedWriter: Send + std::fmt::Debug {
    /// The column this writer is interested in.
    fn column_name(&self) -> &str;

    /// Observe a slice of column values as they are written to the current fragment.
    /// Called once per batch.
    fn observe_batch(&mut self, values: &ArrayRef) -> Result<()>;

    /// Serialize accumulated state to bytes and reset for the next fragment.
    /// Returns `None` if no data was observed (empty fragment).
    fn finish(&mut self) -> Result<Option<Bytes>>;

    /// Schema metadata key used to record that a seed buffer was written.
    /// Convention: `"lance.seed.<column_name>"`.
    fn schema_metadata_key(&self) -> String;

    /// Create a string to store in the file's schema metadata. This will normally
    /// contain the buffer index (provided by the caller after `add_global_buffer`)
    /// as well as any other information needed to validate or understand the seed
    /// (e.g. `rows_per_zone` for zone map seeds).
    fn schema_metadata_value(&self, buf_index: u32) -> String;
}

/// A pre-harvested seed buffer from a single fragment's data file.
#[derive(Debug, Clone)]
pub struct FragmentSeed {
    pub fragment_id: u64,
    pub bytes: Bytes,
    /// The raw value that was stored in the data file's schema metadata under
    /// the seed key (i.e. the output of [`IndexSeedWriter::schema_metadata_value`]).
    /// Plugins can inspect this to validate that the seed is compatible with the
    /// current index configuration before consuming `bytes`.
    pub metadata_value: String,
}
