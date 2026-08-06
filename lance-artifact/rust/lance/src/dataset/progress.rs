// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::sync::Arc;

use async_trait::async_trait;
use lance_table::format::Fragment;

use crate::Result;

/// Progress of writing a [Fragment].
///
/// When start writing a [`Fragment`], WriteProgress::begin() will be called before
/// writing any data.
///
/// When stop writing a [`Fragment`], WriteProgress::complete() will be called after.
///
/// This might be called concurrently when writing multiple [`Fragment`]s. Therefore,
/// the methods require non-exclusive access to `self`.
///
/// This is an experimental API and may change in the future.
#[async_trait]
pub trait WriteFragmentProgress: std::fmt::Debug + Sync + Send {
    /// Indicate the beginning of writing a [Fragment], with the in-flight multipart ID.
    async fn begin(&self, fragment: &Fragment) -> Result<()>;

    /// Complete writing a [Fragment].
    async fn complete(&self, fragment: &Fragment) -> Result<()>;
}

/// Statistics reported to the write progress callback set via
/// [`InsertBuilder::progress`](crate::dataset::InsertBuilder::progress) or
/// [`WriteParams::write_progress`](crate::dataset::WriteParams::write_progress).
#[derive(Debug, Clone, Default)]
pub struct WriteStats {
    /// Cumulative bytes handed to the writer so far.
    ///
    /// For local storage this closely tracks bytes flushed to disk. For cloud
    /// object stores (S3, GCS, Azure) this reflects bytes handed to the
    /// multipart-upload buffer; actual network I/O may lag slightly.
    pub bytes_written: u64,
    /// Cumulative rows written so far.
    pub rows_written: u64,
    /// Number of files (fragments) whose writes have completed so far.
    pub files_written: u32,
}

/// An opaque wrapper around a write-progress closure.
///
/// Stored inside [`WriteParams::write_progress`](crate::dataset::WriteParams::write_progress).
/// Construct via [`InsertBuilder::progress`](crate::dataset::InsertBuilder::progress) or
/// directly with [`WriteProgressFn::new`].
#[derive(Clone)]
pub struct WriteProgressFn(Arc<dyn Fn(WriteStats) + Send + Sync>);

impl WriteProgressFn {
    pub fn new(f: impl Fn(WriteStats) + Send + Sync + 'static) -> Self {
        Self(Arc::new(f))
    }

    pub(crate) fn call(&self, stats: WriteStats) {
        (self.0)(stats);
    }
}

impl std::fmt::Debug for WriteProgressFn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WriteProgressFn").finish_non_exhaustive()
    }
}

/// By default, Progress tracker is Noop.
#[derive(Debug, Clone, Default)]
pub struct NoopFragmentWriteProgress {}

impl NoopFragmentWriteProgress {
    pub fn new() -> Self {
        Self {}
    }
}

#[async_trait]
impl WriteFragmentProgress for NoopFragmentWriteProgress {
    #[inline]
    async fn begin(&self, _fragment: &Fragment) -> Result<()> {
        Ok(())
    }

    #[inline]
    async fn complete(&self, _fragment: &Fragment) -> Result<()> {
        Ok(())
    }
}
