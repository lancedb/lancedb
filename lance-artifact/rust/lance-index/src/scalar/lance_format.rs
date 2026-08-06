// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Utilities for serializing and deserializing scalar indices in the lance format

use super::{IndexFile, IndexReader, IndexStore, IndexWriter};
use arrow_array::RecordBatch;
use arrow_schema::Schema;
use async_trait::async_trait;
use bytes::Bytes;
use futures::TryStreamExt;
use lance_core::deepsize::DeepSizeOf;
use lance_core::{Error, Result, cache::LanceCache};
use lance_encoding::decoder::{DecoderPlugins, FilterExpression};
use lance_file::reader::{FileReader as CurrentFileReader, FileReaderOptions};
use lance_file::version::ConcreteFileVersion;
use lance_file::version::LanceFileVersion;
use lance_file::versions;
use lance_file::versions::v1::reader::FileReader as V1FileReader;
use lance_file::writer as current_writer;
use lance_io::scheduler::{ScanScheduler, SchedulerConfig};
use lance_io::utils::CachedFileSize;
use lance_io::{ReadBatchParams, object_store::ObjectStore};
use lance_table::format::SelfDescribingFileReader;
use lance_table::format::list_index_files_with_sizes;
use object_store::path::Path;
use std::cmp::min;
use std::collections::HashMap;
use std::pin::Pin;
use std::{any::Any, sync::Arc};

/// An index store that serializes scalar indices using the lance format
///
/// Scalar indices are made up of named collections of record batches.  This
/// struct relies on there being a dedicated directory for the index and stores
/// each collection in a file in the lance format.
#[derive(Debug, Clone)]
pub struct LanceIndexStore {
    object_store: Arc<ObjectStore>,
    index_dir: Path,
    metadata_cache: Arc<LanceCache>,
    scheduler: Arc<ScanScheduler>,
    /// Cached file sizes (filename -> size in bytes)
    /// When set, used to avoid HEAD calls when opening files
    file_sizes: HashMap<String, u64>,
    format_version: LanceFileVersion,
    /// Base I/O priority for all requests this store submits to `scheduler`.
    io_priority: u64,
}

impl DeepSizeOf for LanceIndexStore {
    fn deep_size_of_children(&self, context: &mut lance_core::deepsize::Context) -> usize {
        // Exclude the shared, session-scoped `metadata_cache` (accounted once by
        // `Session::deep_size_of_children`): it is not this store's own footprint, and
        // sizing it here makes every opened index that holds a store report the whole
        // cache as its own bytes — inflating and N-times double-counting it.
        self.object_store.deep_size_of_children(context)
            + self.index_dir.as_ref().deep_size_of_children(context)
    }
}

impl LanceIndexStore {
    /// Create a new index store at the given directory
    pub fn new(
        object_store: Arc<ObjectStore>,
        index_dir: Path,
        metadata_cache: Arc<LanceCache>,
    ) -> Self {
        Self::with_format_version(
            object_store,
            index_dir,
            metadata_cache,
            LanceFileVersion::V2_0,
        )
    }

    /// Create a new index store at the given directory with a specific format version
    pub fn with_format_version(
        object_store: Arc<ObjectStore>,
        index_dir: Path,
        metadata_cache: Arc<LanceCache>,
        format_version: LanceFileVersion,
    ) -> Self {
        let scheduler = ScanScheduler::new(
            object_store.clone(),
            SchedulerConfig::max_bandwidth(&object_store),
        );
        Self {
            object_store,
            index_dir,
            metadata_cache,
            scheduler,
            file_sizes: HashMap::new(),
            format_version,
            io_priority: 0,
        }
    }

    /// Set cached file sizes to avoid HEAD calls when opening files.
    ///
    /// The map should contain relative paths (e.g., "index.idx") as keys
    /// and file sizes in bytes as values.
    pub fn with_file_sizes(mut self, file_sizes: HashMap<String, u64>) -> Self {
        self.file_sizes = file_sizes;
        self
    }

    /// The base I/O priority all this store's requests are submitted at.
    pub fn io_priority(&self) -> u64 {
        self.io_priority
    }

    fn index_file_path(&self, name: &str) -> Result<Path> {
        let relative_path = Path::parse(name).map_err(|err| {
            Error::invalid_input(format!("invalid index file path {name:?}: {err}"))
        })?;
        if self.index_dir.is_root() {
            return Ok(relative_path);
        }
        if relative_path.is_root() {
            return Ok(self.index_dir.clone());
        }
        Path::parse(format!(
            "{}/{}",
            self.index_dir.as_ref(),
            relative_path.as_ref()
        ))
        .map_err(|err| Error::invalid_input(format!("invalid index file path {name:?}: {err}")))
    }
}

struct LanceIndexWriter {
    path: String,
    inner: current_writer::FileWriter,
}

#[async_trait]
impl IndexWriter for LanceIndexWriter {
    async fn write_record_batch(&mut self, batch: RecordBatch) -> Result<u64> {
        let offset = self.inner.tell().await?;
        self.inner.write_batch(&batch).await?;
        Ok(offset)
    }

    async fn add_global_buffer(&mut self, data: Bytes) -> Result<u32> {
        self.inner.add_global_buffer(data).await
    }

    async fn finish(&mut self) -> Result<IndexFile> {
        let summary = self.inner.finish().await?;
        Ok(IndexFile {
            path: self.path.clone(),
            size_bytes: summary.size_bytes,
        })
    }

    async fn finish_with_metadata(
        &mut self,
        metadata: HashMap<String, String>,
    ) -> Result<IndexFile> {
        metadata.into_iter().for_each(|(k, v)| {
            self.inner.add_schema_metadata(k, v);
        });
        let summary = self.inner.finish().await?;
        Ok(IndexFile {
            path: self.path.clone(),
            size_bytes: summary.size_bytes,
        })
    }
}

/// Newtype wrapper to allow implementing IndexReader for V1FileReader (a foreign type)
struct V1IndexReader(V1FileReader);

#[async_trait]
impl IndexReader for V1IndexReader {
    async fn read_record_batch(&self, offset: u64, _batch_size: u64) -> Result<RecordBatch> {
        self.0
            .read_batch(offset as i32, ReadBatchParams::RangeFull, self.0.schema())
            .await
    }

    async fn read_range(
        &self,
        range: std::ops::Range<usize>,
        projection: Option<&[&str]>,
    ) -> Result<RecordBatch> {
        let projection = match projection {
            Some(projection) => self.0.schema().project(projection)?,
            None => self.0.schema().clone(),
        };
        self.0.read_range(range, &projection).await
    }

    async fn num_batches(&self, _batch_size: u64) -> u32 {
        self.0.num_batches() as u32
    }

    fn num_rows(&self) -> usize {
        self.0.len()
    }

    fn schema(&self) -> &lance_core::datatypes::Schema {
        V1FileReader::schema(&self.0)
    }
}

/// Newtype wrapper to allow implementing IndexReader for CurrentFileReader (a foreign type)
struct CurrentIndexReader(CurrentFileReader);

#[async_trait]
impl IndexReader for CurrentIndexReader {
    async fn read_record_batch(&self, offset: u64, batch_size: u64) -> Result<RecordBatch> {
        let start = offset * batch_size;
        let end = start + batch_size;
        let end = end.min(self.0.num_rows());
        self.read_range(start as usize..end as usize, None).await
    }

    async fn read_global_buffer(&self, n: u32) -> Result<Bytes> {
        CurrentFileReader::read_global_buffer(&self.0, n).await
    }

    async fn read_range(
        &self,
        range: std::ops::Range<usize>,
        projection: Option<&[&str]>,
    ) -> Result<RecordBatch> {
        if range.is_empty() {
            return Ok(RecordBatch::new_empty(Arc::new(
                self.0.schema().as_ref().into(),
            )));
        }
        let projection = if let Some(projection) = projection {
            versions::reader_projection_from_column_names(
                self.0.metadata().version(),
                self.0.schema(),
                projection,
            )?
        } else {
            versions::reader_projection_from_whole_schema(
                self.0.schema(),
                self.0.metadata().version(),
            )
        };
        let batches = self
            .0
            .read_stream_projected(
                ReadBatchParams::Range(range),
                u32::MAX,
                u32::MAX,
                projection,
                FilterExpression::no_filter(),
            )
            .await?
            .try_collect::<Vec<_>>()
            .await?;
        assert_eq!(batches.len(), 1);
        Ok(batches[0].clone())
    }

    async fn read_ranges(
        &self,
        ranges: &[std::ops::Range<usize>],
        projection: Option<&[&str]>,
    ) -> Result<RecordBatch> {
        let empty_batch = || {
            Ok(RecordBatch::new_empty(Arc::new(
                self.0.schema().as_ref().into(),
            )))
        };
        if ranges.is_empty() {
            return empty_batch();
        }
        let projection = if let Some(projection) = projection {
            versions::reader_projection_from_column_names(
                self.0.metadata().version(),
                self.0.schema(),
                projection,
            )?
        } else {
            versions::reader_projection_from_whole_schema(
                self.0.schema(),
                self.0.metadata().version(),
            )
        };
        // `DecodeBatchScheduler::schedule_ranges` requires sorted,
        // non-overlapping ranges; sort internally and permute the
        // result back to caller order so callers don't have to know.
        let mut order: Vec<usize> = (0..ranges.len()).collect();
        order.sort_by_key(|&i| ranges[i].start);
        let already_sorted = order.iter().enumerate().all(|(i, &j)| i == j);
        let sorted_ranges: Arc<[std::ops::Range<u64>]> = order
            .iter()
            .map(|&i| ranges[i].start as u64..ranges[i].end as u64)
            .collect();
        let total_rows: u64 = sorted_ranges.iter().map(|r| r.end - r.start).sum();
        let batches = self
            .0
            .read_stream_projected(
                ReadBatchParams::Ranges(sorted_ranges),
                (total_rows as u32).max(1),
                16,
                projection,
                FilterExpression::no_filter(),
            )
            .await?
            .try_collect::<Vec<_>>()
            .await?;
        let merged = match batches.len() {
            0 => return empty_batch(),
            1 => batches.into_iter().next().unwrap(),
            _ => {
                let schema = batches[0].schema();
                arrow_select::concat::concat_batches(&schema, &batches)?
            }
        };
        if already_sorted {
            return Ok(merged);
        }
        let sorted_sizes: Vec<u32> = order
            .iter()
            .map(|&i| (ranges[i].end - ranges[i].start) as u32)
            .collect();
        let mut sorted_offsets = Vec::with_capacity(sorted_sizes.len());
        let mut acc = 0u32;
        for &s in &sorted_sizes {
            sorted_offsets.push(acc);
            acc += s;
        }
        let mut sorted_pos = vec![0usize; ranges.len()];
        for (sp, &oi) in order.iter().enumerate() {
            sorted_pos[oi] = sp;
        }
        let mut take_indices = Vec::with_capacity(total_rows as usize);
        for &sp in &sorted_pos {
            for k in 0..sorted_sizes[sp] {
                take_indices.push(sorted_offsets[sp] + k);
            }
        }
        let take_arr = arrow_array::UInt32Array::from(take_indices);
        Ok(arrow_select::take::take_record_batch(&merged, &take_arr)?)
    }

    async fn read_range_stream(
        &self,
        range: std::ops::Range<usize>,
        projection: Option<&[&str]>,
    ) -> Result<Pin<Box<dyn lance_io::stream::RecordBatchStream>>> {
        if range.is_empty() {
            return Ok(Box::pin(lance_io::stream::RecordBatchStreamAdapter::new(
                Arc::new(self.0.schema().as_ref().into()),
                futures::stream::empty(),
            )));
        }
        let projection = if let Some(projection) = projection {
            versions::reader_projection_from_column_names(
                self.0.metadata().version(),
                self.0.schema(),
                projection,
            )?
        } else {
            versions::reader_projection_from_whole_schema(
                self.0.schema(),
                self.0.metadata().version(),
            )
        };
        self.0
            .read_stream_projected(
                ReadBatchParams::Range(range),
                4096,
                2,
                projection,
                FilterExpression::no_filter(),
            )
            .await
    }

    // V2 format has removed the row group concept,
    // so here we assume each batch is with 4096 rows.
    async fn num_batches(&self, batch_size: u64) -> u32 {
        CurrentFileReader::num_rows(&self.0).div_ceil(batch_size) as u32
    }

    fn num_rows(&self) -> usize {
        CurrentFileReader::num_rows(&self.0) as usize
    }

    fn schema(&self) -> &lance_core::datatypes::Schema {
        CurrentFileReader::schema(&self.0)
    }

    fn file_size_bytes(&self) -> Option<u64> {
        // The manifest records each index file's size and passes it to the reader
        // at open, so it's already in metadata here (no extra I/O).
        Some(self.0.metadata().file_size())
    }
}

#[async_trait]
impl IndexStore for LanceIndexStore {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn clone_arc(&self) -> Arc<dyn IndexStore> {
        Arc::new(self.clone())
    }

    fn io_parallelism(&self) -> usize {
        self.object_store.io_parallelism()
    }

    async fn new_index_file(
        &self,
        name: &str,
        schema: Arc<Schema>,
    ) -> Result<Box<dyn IndexWriter>> {
        let path = self.index_file_path(name)?;
        let schema = schema.as_ref().try_into()?;
        let writer = self.object_store.create(&path).await?;
        let writer = versions::create_writer(
            ConcreteFileVersion::from(self.format_version),
            writer,
            schema,
            current_writer::FileWriterOptions::default(),
        )?;
        Ok(Box::new(LanceIndexWriter {
            path: name.to_string(),
            inner: writer,
        }))
    }

    fn with_io_priority(&self, io_priority: u64) -> Arc<dyn IndexStore> {
        // The `scheduler` is shared (`Arc`), so this clone is cheap and the new
        // priority only affects requests this clone submits.
        Arc::new(Self {
            io_priority,
            ..self.clone()
        })
    }

    async fn open_index_file(&self, name: &str) -> Result<Arc<dyn IndexReader>> {
        let path = self.index_file_path(name)?;
        // Use cached file size if available, otherwise unknown (requires HEAD call)
        let cached_size = self
            .file_sizes
            .get(name)
            .map(|&size| CachedFileSize::new(size))
            .unwrap_or_else(CachedFileSize::unknown);
        let file_scheduler = self
            .scheduler
            .open_file_with_priority(&path, self.io_priority, &cached_size)
            .await?;
        match CurrentFileReader::try_open(
            file_scheduler,
            None,
            Arc::<DecoderPlugins>::default(),
            &self.metadata_cache,
            FileReaderOptions::default(),
        )
        .await
        {
            Ok(reader) => Ok(Arc::new(CurrentIndexReader(reader))),
            Err(e) => {
                // If the error is a version conflict we can try to read the file with v1 reader
                if let Error::VersionConflict { .. } = e {
                    let path = self.index_file_path(name)?;
                    let file_reader = V1FileReader::try_new_self_described(
                        &self.object_store,
                        &path,
                        Some(&self.metadata_cache),
                    )
                    .await?;
                    Ok(Arc::new(V1IndexReader(file_reader)))
                } else {
                    Err(e)
                }
            }
        }
    }

    async fn copy_index_file(&self, name: &str, dest_store: &dyn IndexStore) -> Result<IndexFile> {
        self.copy_index_file_to(name, name, dest_store).await
    }

    async fn copy_index_file_to(
        &self,
        name: &str,
        new_name: &str,
        dest_store: &dyn IndexStore,
    ) -> Result<IndexFile> {
        let path = self.index_file_path(name)?;

        let other_store = dest_store.as_any().downcast_ref::<Self>();
        match other_store {
            Some(dest_store) if dest_store.object_store.scheme() == self.object_store.scheme() => {
                // If both this store and the destination are lance stores we can use object_store's copy
                // This does blindly assume that both stores are using the same underlying object_store
                // but there is no easy way to verify this and it happens to always be true at the moment
                let dest_path = dest_store.index_file_path(new_name)?;
                self.object_store.copy(&path, &dest_path).await?;
                let size_bytes = match self.file_sizes.get(name) {
                    Some(size_bytes) => *size_bytes,
                    None => self.object_store.size(&path).await?,
                };
                Ok(IndexFile {
                    path: new_name.to_string(),
                    size_bytes,
                })
            }
            _ => {
                let reader = self.open_index_file(name).await?;
                let mut writer = dest_store
                    .new_index_file(new_name, Arc::new(reader.schema().into()))
                    .await?;

                for offset in (0..reader.num_rows()).step_by(4096) {
                    let next_offset = min(offset + 4096, reader.num_rows());
                    let batch = reader.read_range(offset..next_offset, None).await?;
                    writer.write_record_batch(batch).await?;
                }
                writer.finish().await
            }
        }
    }

    async fn rename_index_file(&self, name: &str, new_name: &str) -> Result<IndexFile> {
        let path = self.index_file_path(name)?;
        let new_path = self.index_file_path(new_name)?;
        self.object_store.copy(&path, &new_path).await?;
        self.object_store.delete(&path).await?;
        let size_bytes = match self.file_sizes.get(name) {
            Some(size_bytes) => *size_bytes,
            None => self.object_store.size(&new_path).await?,
        };
        Ok(IndexFile {
            path: new_name.to_string(),
            size_bytes,
        })
    }

    async fn delete_index_file(&self, name: &str) -> Result<()> {
        let path = self.index_file_path(name)?;
        self.object_store.delete(&path).await
    }

    async fn list_files_with_sizes(&self) -> Result<Vec<IndexFile>> {
        let files = list_index_files_with_sizes(&self.object_store, &self.index_dir).await?;
        Ok(files
            .into_iter()
            .map(|f| IndexFile {
                path: f.path,
                size_bytes: f.size_bytes,
            })
            .collect())
    }
}

#[cfg(test)]
mod tests {

    use std::{collections::HashMap, ops::Bound};

    use crate::metrics::NoOpMetricsCollector;
    use crate::pbold;
    use crate::scalar::bitmap::BitmapIndexPlugin;
    use crate::scalar::btree::{BTreeIndexPlugin, BTreeParameters};
    use crate::scalar::label_list::LabelListIndexPlugin;
    use crate::scalar::registry::{BasicTrainer, ScalarIndexPlugin, VALUE_COLUMN_NAME};
    use crate::scalar::{
        LabelListQuery, SargableQuery, ScalarIndex, SearchResult,
        bitmap::BitmapIndex,
        btree::{DEFAULT_BTREE_BATCH_SIZE, train_btree_index},
    };

    use super::*;
    use arrow::{buffer::ScalarBuffer, datatypes::UInt8Type};
    use arrow_array::{
        ListArray, RecordBatchIterator, RecordBatchReader, StringArray, UInt64Array,
        cast::AsArray,
        types::{Int32Type, UInt64Type},
    };
    use arrow_schema::Schema as ArrowSchema;
    use arrow_schema::{DataType, Field, TimeUnit};
    use arrow_select::take::TakeOptions;
    use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
    use datafusion_common::ScalarValue;
    use futures::FutureExt;
    use lance_core::ROW_ID;
    use lance_core::utils::row_addr_remap::RowAddrRemap;
    use lance_core::utils::tempfile::TempDir;
    use lance_datagen::{ArrayGeneratorExt, BatchCount, ByteCount, RowCount, array, gen_batch};
    use lance_select::{RowAddrTreeMap, RowSetOps};

    fn test_store(tempdir: &TempDir) -> Arc<dyn IndexStore> {
        let test_path = tempdir.obj_path();
        let (object_store, test_path) = ObjectStore::from_uri(test_path.as_ref())
            .now_or_never()
            .unwrap()
            .unwrap();
        let cache = Arc::new(lance_core::cache::LanceCache::with_capacity(
            128 * 1024 * 1024,
        ));
        Arc::new(LanceIndexStore::new(object_store, test_path, cache))
    }

    #[tokio::test]
    async fn test_store_deep_size_excludes_metadata_cache() {
        // The metadata cache is session-scoped and shared; a store must not count
        // it as its own footprint, so growing the cache must not change store size.
        struct BlobKey;
        impl lance_core::cache::CacheKey for BlobKey {
            type ValueType = Vec<u8>;
            fn key(&self) -> std::borrow::Cow<'_, str> {
                std::borrow::Cow::Borrowed("blob")
            }
            fn type_name() -> &'static str {
                "Vec<u8>"
            }
            fn stable_type_id() -> &'static str {
                "lance.scalar.lance-format.Blob"
            }
            fn schema() -> lance_core::cache::CacheKeySchema {
                lance_core::cache::CacheKeySchema::new("lance.scalar.lance-format.blob-key", 1)
            }
            fn write_key(&self, builder: &mut lance_core::cache::KeyBuilder) {
                builder.write_variant(0);
            }
        }

        let index_dir = TempDir::default();
        let test_path = index_dir.obj_path();
        let (object_store, test_path) = ObjectStore::from_uri(test_path.as_ref())
            .now_or_never()
            .unwrap()
            .unwrap();
        let cache = Arc::new(lance_core::cache::LanceCache::with_capacity(
            128 * 1024 * 1024,
        ));
        let store = LanceIndexStore::new(object_store, test_path, cache.clone());

        let before = store.deep_size_of();
        cache
            .insert_with_key(&BlobKey, Arc::new(vec![0u8; 4 * 1024 * 1024]))
            .await;
        // Force moka to commit the write so a cache-inclusive size would grow.
        let _ = cache.size_bytes().await;
        let after = store.deep_size_of();

        assert_eq!(
            before, after,
            "store deep size must exclude the shared metadata cache"
        );
    }

    async fn train_index(
        index_store: &Arc<dyn IndexStore>,
        data: impl RecordBatchReader + Send + Sync + 'static,
        custom_batch_size: Option<u64>,
    ) {
        let batch_size = custom_batch_size.unwrap_or(DEFAULT_BTREE_BATCH_SIZE);
        let params = BTreeParameters {
            zone_size: Some(batch_size),
            range_id: None,
        };
        let params = serde_json::to_string(&params).unwrap();
        let btree_plugin = BTreeIndexPlugin;
        let data = lance_datafusion::utils::reader_to_stream(Box::new(data));
        let request = btree_plugin
            .new_training_request(
                &params,
                &Field::new(VALUE_COLUMN_NAME, DataType::Int32, false),
            )
            .unwrap();
        btree_plugin
            .train_index(
                data,
                index_store.as_ref(),
                request,
                None,
                crate::progress::noop_progress(),
            )
            .await
            .unwrap();
    }

    fn default_details<T: prost::Message + prost::Name + std::default::Default>() -> prost_types::Any
    {
        prost_types::Any::from_msg(&T::default()).unwrap()
    }

    #[tokio::test]
    async fn test_global_buffer_round_trip() {
        let tempdir = TempDir::default();
        let index_store = test_store(&tempdir);

        let mut writer = index_store
            .new_index_file("global-buffer.lance", Arc::new(Schema::empty()))
            .await
            .unwrap();
        let expected = bytes::Bytes::from_static(b"scalar-global-buffer");
        let buffer_idx = writer.add_global_buffer(expected.clone()).await.unwrap();
        let write_summary = writer.finish().await.unwrap();
        let files = index_store.list_files_with_sizes().await.unwrap();
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].path, "global-buffer.lance");
        assert_eq!(write_summary.size_bytes, files[0].size_bytes);

        let reader = index_store
            .open_index_file("global-buffer.lance")
            .await
            .unwrap();
        let actual = reader.read_global_buffer(buffer_idx).await.unwrap();

        assert_eq!(actual, expected);
    }

    #[tokio::test]
    async fn test_basic_btree() {
        let tempdir = TempDir::default();
        let index_store = test_store(&tempdir);
        let data = gen_batch()
            .col(VALUE_COLUMN_NAME, array::step::<Int32Type>())
            .col(ROW_ID, array::step::<UInt64Type>())
            .into_reader_rows(RowCount::from(4096), BatchCount::from(100));
        train_index(&index_store, data, None).await;
        let index = BTreeIndexPlugin
            .load_index(
                index_store,
                &default_details::<pbold::BTreeIndexDetails>(),
                None,
                &LanceCache::no_cache(),
            )
            .await
            .unwrap();

        let result = index
            .search(
                &SargableQuery::Equals(ScalarValue::Int32(Some(10000))),
                &NoOpMetricsCollector,
            )
            .await
            .unwrap();

        assert!(result.is_exact());
        let row_ids = result.row_addrs().true_rows();
        assert_eq!(Some(1), row_ids.len());
        assert!(row_ids.contains(10000));

        let result = index
            .search(
                &SargableQuery::Range(
                    Bound::Unbounded,
                    Bound::Excluded(ScalarValue::Int32(Some(-100))),
                ),
                &NoOpMetricsCollector,
            )
            .await
            .unwrap();

        assert!(result.is_exact());
        let row_addrs = result.row_addrs().true_rows();

        assert_eq!(Some(0), row_addrs.len());

        let result = index
            .search(
                &SargableQuery::Range(
                    Bound::Unbounded,
                    Bound::Excluded(ScalarValue::Int32(Some(100))),
                ),
                &NoOpMetricsCollector,
            )
            .await
            .unwrap();

        assert!(result.is_exact());
        let row_addrs = result.row_addrs().true_rows();

        assert_eq!(Some(100), row_addrs.len());
    }

    #[tokio::test]
    async fn test_btree_update() {
        let index_dir = TempDir::default();
        let index_store = test_store(&index_dir);
        let data = gen_batch()
            .col(VALUE_COLUMN_NAME, array::step::<Int32Type>())
            .col(ROW_ID, array::step::<UInt64Type>())
            .into_reader_rows(RowCount::from(4096), BatchCount::from(100));
        train_index(&index_store, data, None).await;
        let index = BTreeIndexPlugin
            .load_index(
                index_store,
                &default_details::<pbold::BTreeIndexDetails>(),
                None,
                &LanceCache::no_cache(),
            )
            .await
            .unwrap();

        let data = gen_batch()
            .col(
                VALUE_COLUMN_NAME,
                array::step_custom::<Int32Type>(4096 * 100, 1),
            )
            .col(ROW_ID, array::step_custom::<UInt64Type>(4096 * 100, 1))
            .into_reader_rows(RowCount::from(4096), BatchCount::from(100));

        let updated_index_dir = TempDir::default();
        let updated_index_store = test_store(&updated_index_dir);
        index
            .update(
                lance_datafusion::utils::reader_to_stream(Box::new(data)),
                updated_index_store.as_ref(),
                None,
            )
            .await
            .unwrap();
        let updated_index = BTreeIndexPlugin
            .load_index(
                updated_index_store,
                &default_details::<pbold::BTreeIndexDetails>(),
                None,
                &LanceCache::no_cache(),
            )
            .await
            .unwrap();

        let result = updated_index
            .search(
                &SargableQuery::Equals(ScalarValue::Int32(Some(10000))),
                &NoOpMetricsCollector,
            )
            .await
            .unwrap();

        assert!(result.is_exact());
        let row_addrs = result.row_addrs().true_rows();

        assert_eq!(Some(1), row_addrs.len());
        assert!(row_addrs.contains(10000));

        let result = updated_index
            .search(
                &SargableQuery::Equals(ScalarValue::Int32(Some(500_000))),
                &NoOpMetricsCollector,
            )
            .await
            .unwrap();

        assert!(result.is_exact());
        let row_addrs = result.row_addrs().true_rows();

        assert_eq!(Some(1), row_addrs.len());
        assert!(row_addrs.contains(500_000));
    }

    async fn check(index: &Arc<dyn ScalarIndex>, query: SargableQuery, expected: &[u64]) {
        let results = index.search(&query, &NoOpMetricsCollector).await.unwrap();
        assert!(results.is_exact());
        let expected_arr = RowAddrTreeMap::from_iter(expected);
        assert_eq!(&results.row_addrs().true_rows(), &expected_arr);
    }

    #[tokio::test]
    async fn test_btree_with_gaps() {
        let tempdir = TempDir::default();
        let index_store = test_store(&tempdir);
        let batch_one = gen_batch()
            .col(
                VALUE_COLUMN_NAME,
                array::cycle::<Int32Type>(vec![0, 1, 4, 5]),
            )
            .col(ROW_ID, array::cycle::<UInt64Type>(vec![0, 1, 2, 3]))
            .into_batch_rows(RowCount::from(4));
        let batch_two = gen_batch()
            .col(
                VALUE_COLUMN_NAME,
                array::cycle::<Int32Type>(vec![10, 11, 11, 15]),
            )
            .col(ROW_ID, array::cycle::<UInt64Type>(vec![40, 50, 60, 70]))
            .into_batch_rows(RowCount::from(4));
        let batch_three = gen_batch()
            .col(
                VALUE_COLUMN_NAME,
                array::cycle::<Int32Type>(vec![15, 15, 15, 15]),
            )
            .col(ROW_ID, array::cycle::<UInt64Type>(vec![400, 500, 600, 700]))
            .into_batch_rows(RowCount::from(4));
        let batch_four = gen_batch()
            .col(
                VALUE_COLUMN_NAME,
                array::cycle::<Int32Type>(vec![15, 16, 20, 20]),
            )
            .col(
                ROW_ID,
                array::cycle::<UInt64Type>(vec![4000, 5000, 6000, 7000]),
            )
            .into_batch_rows(RowCount::from(4));
        let batches = vec![batch_one, batch_two, batch_three, batch_four];
        let schema = Arc::new(Schema::new(vec![
            Field::new(VALUE_COLUMN_NAME, DataType::Int32, false),
            Field::new(ROW_ID, DataType::UInt64, false),
        ]));
        let data = RecordBatchIterator::new(batches, schema);
        train_index(&index_store, data, Some(4)).await;
        let index = BTreeIndexPlugin
            .load_index(
                index_store,
                &default_details::<pbold::BTreeIndexDetails>(),
                None,
                &LanceCache::no_cache(),
            )
            .await
            .unwrap();

        // The above should create four pages
        //
        // 0 - 5
        // 10 - 15
        // 15 - 15
        // 15 - 20
        //
        // This will help us test various indexing corner cases

        // No results (off the left side)
        check(
            &index,
            SargableQuery::Equals(ScalarValue::Int32(Some(-3))),
            &[],
        )
        .await;

        check(
            &index,
            SargableQuery::Range(
                Bound::Unbounded,
                Bound::Included(ScalarValue::Int32(Some(-3))),
            ),
            &[],
        )
        .await;

        check(
            &index,
            SargableQuery::Range(
                Bound::Included(ScalarValue::Int32(Some(-10))),
                Bound::Included(ScalarValue::Int32(Some(-3))),
            ),
            &[],
        )
        .await;

        // Hitting the middle of a bucket
        check(
            &index,
            SargableQuery::Equals(ScalarValue::Int32(Some(4))),
            &[2],
        )
        .await;

        // Hitting a gap between two buckets
        check(
            &index,
            SargableQuery::Equals(ScalarValue::Int32(Some(7))),
            &[],
        )
        .await;

        // Hitting the lowest of the overlapping buckets
        check(
            &index,
            SargableQuery::Equals(ScalarValue::Int32(Some(11))),
            &[50, 60],
        )
        .await;

        // Hitting the 15 shared on all three buckets
        check(
            &index,
            SargableQuery::Equals(ScalarValue::Int32(Some(15))),
            &[70, 400, 500, 600, 700, 4000],
        )
        .await;

        // Hitting the upper part of the three overlapping buckets
        check(
            &index,
            SargableQuery::Equals(ScalarValue::Int32(Some(20))),
            &[6000, 7000],
        )
        .await;

        // Ranges that capture multiple buckets
        check(
            &index,
            SargableQuery::Range(
                Bound::Unbounded,
                Bound::Included(ScalarValue::Int32(Some(11))),
            ),
            &[0, 1, 2, 3, 40, 50, 60],
        )
        .await;

        check(
            &index,
            SargableQuery::Range(
                Bound::Unbounded,
                Bound::Excluded(ScalarValue::Int32(Some(11))),
            ),
            &[0, 1, 2, 3, 40],
        )
        .await;

        check(
            &index,
            SargableQuery::Range(
                Bound::Included(ScalarValue::Int32(Some(4))),
                Bound::Unbounded,
            ),
            &[
                2, 3, 40, 50, 60, 70, 400, 500, 600, 700, 4000, 5000, 6000, 7000,
            ],
        )
        .await;

        check(
            &index,
            SargableQuery::Range(
                Bound::Included(ScalarValue::Int32(Some(4))),
                Bound::Included(ScalarValue::Int32(Some(11))),
            ),
            &[2, 3, 40, 50, 60],
        )
        .await;

        check(
            &index,
            SargableQuery::Range(
                Bound::Included(ScalarValue::Int32(Some(4))),
                Bound::Excluded(ScalarValue::Int32(Some(11))),
            ),
            &[2, 3, 40],
        )
        .await;

        check(
            &index,
            SargableQuery::Range(
                Bound::Excluded(ScalarValue::Int32(Some(4))),
                Bound::Unbounded,
            ),
            &[
                3, 40, 50, 60, 70, 400, 500, 600, 700, 4000, 5000, 6000, 7000,
            ],
        )
        .await;

        check(
            &index,
            SargableQuery::Range(
                Bound::Excluded(ScalarValue::Int32(Some(4))),
                Bound::Included(ScalarValue::Int32(Some(11))),
            ),
            &[3, 40, 50, 60],
        )
        .await;

        check(
            &index,
            SargableQuery::Range(
                Bound::Excluded(ScalarValue::Int32(Some(4))),
                Bound::Excluded(ScalarValue::Int32(Some(11))),
            ),
            &[3, 40],
        )
        .await;

        check(
            &index,
            SargableQuery::Range(
                Bound::Excluded(ScalarValue::Int32(Some(-50))),
                Bound::Excluded(ScalarValue::Int32(Some(1000))),
            ),
            &[
                0, 1, 2, 3, 40, 50, 60, 70, 400, 500, 600, 700, 4000, 5000, 6000, 7000,
            ],
        )
        .await;
    }

    #[tokio::test]
    async fn test_btree_types() {
        for data_type in &[
            DataType::Boolean,
            DataType::Int32,
            DataType::Utf8,
            DataType::Float32,
            DataType::Date32,
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            DataType::Date64,
            DataType::Date32,
            DataType::Time64(TimeUnit::Nanosecond),
            DataType::Time32(TimeUnit::Second),
            DataType::FixedSizeBinary(16),
            // Not supported today, error from datafusion:
            // Min/max accumulator not implemented for Duration(Nanosecond)
            // DataType::Duration(TimeUnit::Nanosecond),
        ] {
            let tempdir = TempDir::default();
            let index_store = test_store(&tempdir);
            let data: RecordBatch = gen_batch()
                .col(VALUE_COLUMN_NAME, array::rand_type(data_type))
                .col(ROW_ID, array::step::<UInt64Type>())
                .into_batch_rows(RowCount::from(4096 * 3))
                .unwrap();

            let sample_value = ScalarValue::try_from_array(data.column(0), 0).unwrap();
            let sample_row_id = data.column(1).as_primitive::<UInt64Type>().value(0);

            let sort_indices = arrow::compute::sort_to_indices(data.column(0), None, None).unwrap();
            let sorted_values = arrow_select::take::take(
                data.column(0),
                &sort_indices,
                Some(TakeOptions {
                    check_bounds: false,
                }),
            )
            .unwrap();
            let sorted_row_ids = arrow_select::take::take(
                data.column(1),
                &sort_indices,
                Some(TakeOptions {
                    check_bounds: false,
                }),
            )
            .unwrap();
            let sorted_batch =
                RecordBatch::try_new(data.schema().clone(), vec![sorted_values, sorted_row_ids])
                    .unwrap();

            let batch_one = sorted_batch.slice(0, 4096);
            let batch_two = sorted_batch.slice(4096, 4096);
            let batch_three = sorted_batch.slice(8192, 4096);
            let training_data = RecordBatchIterator::new(
                vec![batch_one, batch_two, batch_three].into_iter().map(Ok),
                data.schema().clone(),
            );

            train_index(&index_store, training_data, None).await;
            let index = BTreeIndexPlugin
                .load_index(
                    index_store,
                    &default_details::<pbold::BTreeIndexDetails>(),
                    None,
                    &LanceCache::no_cache(),
                )
                .await
                .unwrap();

            let result = index
                .search(&SargableQuery::Equals(sample_value), &NoOpMetricsCollector)
                .await
                .unwrap();

            assert!(result.is_exact());
            let row_addrs = result.row_addrs().true_rows();

            // The random data may have had duplicates so there might be more than 1 result
            // but even for boolean we shouldn't match the entire thing
            assert!(!row_addrs.is_empty());
            assert!(row_addrs.len().unwrap() < data.num_rows() as u64);
            assert!(row_addrs.contains(sample_row_id));
        }
    }

    #[tokio::test]
    async fn btree_entire_null_page() {
        let tempdir = TempDir::default();
        let index_store = test_store(&tempdir);
        let batch = gen_batch()
            .col(
                VALUE_COLUMN_NAME,
                array::rand_utf8(ByteCount::from(0), false).with_nulls(&[true]),
            )
            .col(ROW_ID, array::step::<UInt64Type>())
            .into_batch_rows(RowCount::from(4096));
        assert_eq!(
            batch.as_ref().unwrap()[VALUE_COLUMN_NAME].null_count(),
            4096
        );
        let batches = vec![batch];
        let schema = Arc::new(Schema::new(vec![
            Field::new(VALUE_COLUMN_NAME, DataType::Utf8, true),
            Field::new(ROW_ID, DataType::UInt64, false),
        ]));
        let data = RecordBatchIterator::new(batches, schema);
        let data = lance_datafusion::utils::reader_to_stream(Box::new(data));

        train_btree_index(
            data,
            index_store.as_ref(),
            DEFAULT_BTREE_BATCH_SIZE,
            None,
            None,
        )
        .await
        .unwrap();

        let index = BTreeIndexPlugin
            .load_index(
                index_store,
                &default_details::<pbold::BTreeIndexDetails>(),
                None,
                &LanceCache::no_cache(),
            )
            .await
            .unwrap();

        let result = index
            .search(
                &SargableQuery::Equals(ScalarValue::Utf8(Some("foo".to_string()))),
                &NoOpMetricsCollector,
            )
            .await
            .unwrap();

        assert!(result.is_exact());
        let row_addrs = result.row_addrs().true_rows();

        assert!(row_addrs.is_empty());

        let result = index
            .search(&SargableQuery::IsNull(), &NoOpMetricsCollector)
            .await
            .unwrap();
        assert!(result.is_exact());
        let row_addrs = result.row_addrs().true_rows();
        assert_eq!(row_addrs.len(), Some(4096));
    }

    async fn train_bitmap(
        index_store: &Arc<dyn IndexStore>,
        data: impl RecordBatchReader + Send + Sync + 'static,
    ) {
        // Sort the data by value column (nulls first) to match the production
        // scanner behavior (TrainingOrdering::Values).
        let schema = data.schema();
        let batches: Vec<_> = data
            .into_iter()
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap();
        let combined = arrow::compute::concat_batches(&schema, &batches).unwrap();
        let options = arrow::compute::SortOptions {
            descending: false,
            nulls_first: true,
        };
        let indices =
            arrow::compute::sort_to_indices(combined.column(0), Some(options), None).unwrap();
        let sorted_columns: Vec<_> = combined
            .columns()
            .iter()
            .map(|col| arrow::compute::take(col.as_ref(), &indices, None).unwrap())
            .collect();
        let sorted_batch = RecordBatch::try_new(schema.clone(), sorted_columns).unwrap();
        let stream = Box::pin(RecordBatchStreamAdapter::new(
            schema,
            futures::stream::once(async move { Ok(sorted_batch) }),
        ));

        let request = BitmapIndexPlugin
            .new_training_request("{}", &Field::new(VALUE_COLUMN_NAME, DataType::Int32, false))
            .unwrap();
        BitmapIndexPlugin
            .train_index(
                stream,
                index_store.as_ref(),
                request,
                None,
                crate::progress::noop_progress(),
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_bitmap_working() {
        let tempdir = TempDir::default();
        let index_store = test_store(&tempdir);

        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new(VALUE_COLUMN_NAME, DataType::Utf8, true),
            Field::new(ROW_ID, DataType::UInt64, false),
        ]));

        let batch1 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec![Some("abcd"), None, Some("abcd")])),
                Arc::new(UInt64Array::from(vec![1, 2, 3])),
            ],
        )
        .unwrap();

        let batch2 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec![
                    Some("apple"),
                    Some("hello"),
                    Some("abcd"),
                ])),
                Arc::new(UInt64Array::from(vec![4, 5, 6])),
            ],
        )
        .unwrap();

        let batches = vec![batch1, batch2];
        let data = RecordBatchIterator::new(batches.into_iter().map(Ok), schema);
        train_bitmap(&index_store, data).await;

        let index = BitmapIndex::load(index_store, None, &LanceCache::no_cache())
            .await
            .unwrap();

        let result = index
            .search(
                &SargableQuery::Equals(ScalarValue::Utf8(None)),
                &NoOpMetricsCollector,
            )
            .await
            .unwrap();

        assert!(result.is_exact());
        let row_addrs = result.row_addrs().true_rows();
        assert_eq!(Some(1), row_addrs.len());
        assert!(row_addrs.contains(2));

        let result = index
            .search(
                &SargableQuery::Equals(ScalarValue::Utf8(Some("abcd".to_string()))),
                &NoOpMetricsCollector,
            )
            .await
            .unwrap();

        assert!(result.is_exact());
        let row_addrs = result.row_addrs().true_rows();
        assert_eq!(Some(3), row_addrs.len());
        assert!(row_addrs.contains(1));
        assert!(row_addrs.contains(3));
        assert!(row_addrs.contains(6));
    }

    #[tokio::test]
    async fn test_basic_bitmap() {
        let tempdir = TempDir::default();
        let index_store = test_store(&tempdir);
        let data = gen_batch()
            .col(VALUE_COLUMN_NAME, array::step::<Int32Type>())
            .col(ROW_ID, array::step::<UInt64Type>())
            .into_reader_rows(RowCount::from(4096), BatchCount::from(100));
        train_bitmap(&index_store, data).await;
        let index = BitmapIndex::load(index_store, None, &LanceCache::no_cache())
            .await
            .unwrap();

        let result = index
            .search(
                &SargableQuery::Equals(ScalarValue::Int32(Some(10000))),
                &NoOpMetricsCollector,
            )
            .await
            .unwrap();

        assert!(result.is_exact());
        let row_addrs = result.row_addrs().true_rows();
        assert_eq!(Some(1), row_addrs.len());
        assert!(row_addrs.contains(10000));

        let result = index
            .search(
                &SargableQuery::Range(
                    Bound::Unbounded,
                    Bound::Excluded(ScalarValue::Int32(Some(-100))),
                ),
                &NoOpMetricsCollector,
            )
            .await
            .unwrap();

        assert!(result.is_exact());
        let row_addrs = result.row_addrs().true_rows();
        assert!(row_addrs.is_empty());

        let result = index
            .search(
                &SargableQuery::Range(
                    Bound::Unbounded,
                    Bound::Excluded(ScalarValue::Int32(Some(100))),
                ),
                &NoOpMetricsCollector,
            )
            .await
            .unwrap();

        assert!(result.is_exact());
        let row_addrs = result.row_addrs().true_rows();
        assert_eq!(Some(100), row_addrs.len());
    }

    async fn check_bitmap(index: &BitmapIndex, query: SargableQuery, expected: &[u64]) {
        let results = index.search(&query, &NoOpMetricsCollector).await.unwrap();
        assert!(results.is_exact());
        let expected_arr = RowAddrTreeMap::from_iter(expected);
        assert_eq!(&results.row_addrs().true_rows(), &expected_arr);
    }

    #[tokio::test]
    async fn test_bitmap_with_gaps() {
        let tempdir = TempDir::default();
        let index_store = test_store(&tempdir);
        let batch_one = gen_batch()
            .col(
                VALUE_COLUMN_NAME,
                array::cycle::<Int32Type>(vec![0, 1, 4, 5]),
            )
            .col(ROW_ID, array::cycle::<UInt64Type>(vec![0, 1, 2, 3]))
            .into_batch_rows(RowCount::from(4));
        let batch_two = gen_batch()
            .col(
                VALUE_COLUMN_NAME,
                array::cycle::<Int32Type>(vec![10, 11, 11, 15]),
            )
            .col(ROW_ID, array::cycle::<UInt64Type>(vec![40, 50, 60, 70]))
            .into_batch_rows(RowCount::from(4));
        let batch_three = gen_batch()
            .col(
                VALUE_COLUMN_NAME,
                array::cycle::<Int32Type>(vec![15, 15, 15, 15]),
            )
            .col(ROW_ID, array::cycle::<UInt64Type>(vec![400, 500, 600, 700]))
            .into_batch_rows(RowCount::from(4));
        let batch_four = gen_batch()
            .col(
                VALUE_COLUMN_NAME,
                array::cycle::<Int32Type>(vec![15, 16, 20, 20]),
            )
            .col(
                ROW_ID,
                array::cycle::<UInt64Type>(vec![4000, 5000, 6000, 7000]),
            )
            .into_batch_rows(RowCount::from(4));
        let batches = vec![batch_one, batch_two, batch_three, batch_four];
        let schema = Arc::new(Schema::new(vec![
            Field::new(VALUE_COLUMN_NAME, DataType::Int32, false),
            Field::new(ROW_ID, DataType::UInt64, false),
        ]));
        let data = RecordBatchIterator::new(batches, schema);
        train_bitmap(&index_store, data).await;
        let index = BitmapIndex::load(index_store, None, &LanceCache::no_cache())
            .await
            .unwrap();

        // The above should create four pages
        //
        // 0 - 5
        // 10 - 15
        // 15 - 15
        // 15 - 20
        //
        // This will help us test various indexing corner cases

        // No results (off the left side)
        check_bitmap(
            &index,
            SargableQuery::Equals(ScalarValue::Int32(Some(-3))),
            &[],
        )
        .await;

        check_bitmap(
            &index,
            SargableQuery::Range(
                Bound::Unbounded,
                Bound::Included(ScalarValue::Int32(Some(-3))),
            ),
            &[],
        )
        .await;

        check_bitmap(
            &index,
            SargableQuery::Range(
                Bound::Included(ScalarValue::Int32(Some(-10))),
                Bound::Included(ScalarValue::Int32(Some(-3))),
            ),
            &[],
        )
        .await;

        // Hitting the middle of a bucket
        check_bitmap(
            &index,
            SargableQuery::Equals(ScalarValue::Int32(Some(4))),
            &[2],
        )
        .await;

        // Hitting a gap between two buckets
        check_bitmap(
            &index,
            SargableQuery::Equals(ScalarValue::Int32(Some(7))),
            &[],
        )
        .await;

        // Hitting the lowest of the overlapping buckets
        check_bitmap(
            &index,
            SargableQuery::Equals(ScalarValue::Int32(Some(11))),
            &[50, 60],
        )
        .await;

        // Hitting the 15 shared on all three buckets
        check_bitmap(
            &index,
            SargableQuery::Equals(ScalarValue::Int32(Some(15))),
            &[70, 400, 500, 600, 700, 4000],
        )
        .await;

        // Hitting the upper part of the three overlapping buckets
        check_bitmap(
            &index,
            SargableQuery::Equals(ScalarValue::Int32(Some(20))),
            &[6000, 7000],
        )
        .await;

        // Ranges that capture multiple buckets
        check_bitmap(
            &index,
            SargableQuery::Range(
                Bound::Unbounded,
                Bound::Included(ScalarValue::Int32(Some(11))),
            ),
            &[0, 1, 2, 3, 40, 50, 60],
        )
        .await;

        check_bitmap(
            &index,
            SargableQuery::Range(
                Bound::Unbounded,
                Bound::Excluded(ScalarValue::Int32(Some(11))),
            ),
            &[0, 1, 2, 3, 40],
        )
        .await;

        check_bitmap(
            &index,
            SargableQuery::Range(
                Bound::Included(ScalarValue::Int32(Some(4))),
                Bound::Unbounded,
            ),
            &[
                2, 3, 40, 50, 60, 70, 400, 500, 600, 700, 4000, 5000, 6000, 7000,
            ],
        )
        .await;

        check_bitmap(
            &index,
            SargableQuery::Range(
                Bound::Included(ScalarValue::Int32(Some(4))),
                Bound::Included(ScalarValue::Int32(Some(11))),
            ),
            &[2, 3, 40, 50, 60],
        )
        .await;

        check_bitmap(
            &index,
            SargableQuery::Range(
                Bound::Included(ScalarValue::Int32(Some(4))),
                Bound::Excluded(ScalarValue::Int32(Some(11))),
            ),
            &[2, 3, 40],
        )
        .await;

        check_bitmap(
            &index,
            SargableQuery::Range(
                Bound::Excluded(ScalarValue::Int32(Some(4))),
                Bound::Unbounded,
            ),
            &[
                3, 40, 50, 60, 70, 400, 500, 600, 700, 4000, 5000, 6000, 7000,
            ],
        )
        .await;

        check_bitmap(
            &index,
            SargableQuery::Range(
                Bound::Excluded(ScalarValue::Int32(Some(4))),
                Bound::Included(ScalarValue::Int32(Some(11))),
            ),
            &[3, 40, 50, 60],
        )
        .await;

        check_bitmap(
            &index,
            SargableQuery::Range(
                Bound::Excluded(ScalarValue::Int32(Some(4))),
                Bound::Excluded(ScalarValue::Int32(Some(11))),
            ),
            &[3, 40],
        )
        .await;

        check_bitmap(
            &index,
            SargableQuery::Range(
                Bound::Excluded(ScalarValue::Int32(Some(-50))),
                Bound::Excluded(ScalarValue::Int32(Some(1000))),
            ),
            &[
                0, 1, 2, 3, 40, 50, 60, 70, 400, 500, 600, 700, 4000, 5000, 6000, 7000,
            ],
        )
        .await;
    }

    #[tokio::test]
    async fn test_bitmap_update() {
        let index_dir = TempDir::default();
        let index_store = test_store(&index_dir);
        let data = gen_batch()
            .col(VALUE_COLUMN_NAME, array::step::<Int32Type>())
            .col(ROW_ID, array::step::<UInt64Type>())
            .into_reader_rows(RowCount::from(4096), BatchCount::from(1));
        train_bitmap(&index_store, data).await;
        let index = BitmapIndex::load(index_store, None, &LanceCache::no_cache())
            .await
            .unwrap();

        let data = gen_batch()
            .col(VALUE_COLUMN_NAME, array::step_custom::<Int32Type>(4096, 1))
            .col(ROW_ID, array::step_custom::<UInt64Type>(4096, 1))
            .into_reader_rows(RowCount::from(4096), BatchCount::from(1));

        let updated_index_dir = TempDir::default();
        let updated_index_store = test_store(&updated_index_dir);
        index
            .update(
                lance_datafusion::utils::reader_to_stream(Box::new(data)),
                updated_index_store.as_ref(),
                None,
            )
            .await
            .unwrap();
        let updated_index = BitmapIndex::load(updated_index_store, None, &LanceCache::no_cache())
            .await
            .unwrap();

        let result = updated_index
            .search(
                &SargableQuery::Equals(ScalarValue::Int32(Some(5000))),
                &NoOpMetricsCollector,
            )
            .await
            .unwrap();

        assert!(result.is_exact());
        let row_addrs = result.row_addrs().true_rows();
        assert_eq!(Some(1), row_addrs.len());
        assert!(row_addrs.contains(5000));
    }

    #[tokio::test]
    async fn test_bitmap_remap() {
        let index_dir = TempDir::default();
        let index_store = test_store(&index_dir);
        let data = gen_batch()
            .col(VALUE_COLUMN_NAME, array::step::<Int32Type>())
            .col(ROW_ID, array::step::<UInt64Type>())
            .into_reader_rows(RowCount::from(50), BatchCount::from(1));
        train_bitmap(&index_store, data).await;
        let index = BitmapIndex::load(index_store, None, &LanceCache::no_cache())
            .await
            .unwrap();

        let mapping = (0..50)
            .map(|i| {
                let map_result = if i == 5 {
                    Some(65)
                } else if i == 7 {
                    None
                } else {
                    Some(i)
                };
                (i, map_result)
            })
            .collect::<HashMap<_, _>>();

        let remapped_dir = TempDir::default();
        let remapped_store = test_store(&remapped_dir);
        index
            .remap(&RowAddrRemap::direct(mapping), remapped_store.as_ref())
            .await
            .unwrap();
        let remapped_index = BitmapIndex::load(remapped_store, None, &LanceCache::no_cache())
            .await
            .unwrap();

        // Remapped to new value
        assert!(
            remapped_index
                .search(
                    &SargableQuery::Equals(ScalarValue::Int32(Some(5))),
                    &NoOpMetricsCollector
                )
                .await
                .unwrap()
                .row_addrs()
                .selected(65)
        );
        // Deleted
        assert!(
            remapped_index
                .search(
                    &SargableQuery::Equals(ScalarValue::Int32(Some(7))),
                    &NoOpMetricsCollector
                )
                .await
                .unwrap()
                .row_addrs()
                .is_empty()
        );
        // Not remapped
        assert!(
            remapped_index
                .search(
                    &SargableQuery::Equals(ScalarValue::Int32(Some(3))),
                    &NoOpMetricsCollector
                )
                .await
                .unwrap()
                .row_addrs()
                .selected(3)
        );
    }

    async fn train_tag(
        index_store: &Arc<dyn IndexStore>,
        data: impl RecordBatchReader + Send + Sync + 'static,
    ) {
        let data = lance_datafusion::utils::reader_to_stream(Box::new(data));
        let request = LabelListIndexPlugin
            .new_training_request(
                "{}",
                &Field::new(
                    VALUE_COLUMN_NAME,
                    DataType::List(Arc::new(Field::new("item", DataType::UInt8, false))),
                    false,
                ),
            )
            .unwrap();
        LabelListIndexPlugin
            .train_index(
                data,
                index_store.as_ref(),
                request,
                None,
                crate::progress::noop_progress(),
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_label_list_index() {
        let tempdir = TempDir::default();
        let index_store = test_store(&tempdir);
        let data = gen_batch()
            .col(
                VALUE_COLUMN_NAME,
                array::rand_type(&DataType::List(Arc::new(Field::new(
                    "item",
                    DataType::UInt8,
                    false,
                )))),
            )
            .col(ROW_ID, array::step::<UInt64Type>())
            .into_batch_rows(RowCount::from(40960))
            .unwrap();

        let batch_reader = RecordBatchIterator::new(vec![Ok(data.clone())], data.schema());

        // This is probably enough data that we can be assured each tag is used at least once
        train_tag(&index_store, batch_reader).await;

        // We scan through each list, if it was a match we run match_fn to check
        // if the match was correct if it was not a match we run no_match_fn to check
        // if the no-match was correct
        type MatchFn = Box<dyn Fn(&ScalarBuffer<u8>) -> bool>;
        let check = |query: LabelListQuery, match_fn: MatchFn, no_match_fn: MatchFn| {
            let index_store = index_store.clone();
            let data = data.clone();
            async move {
                let index = LabelListIndexPlugin
                    .load_index(
                        index_store,
                        &default_details::<pbold::LabelListIndexDetails>(),
                        None,
                        &LanceCache::no_cache(),
                    )
                    .await
                    .unwrap();
                let result = index.search(&query, &NoOpMetricsCollector).await.unwrap();
                assert!(result.is_exact());
                let row_addrs = result.row_addrs().true_rows();

                let row_addrs_set = row_addrs
                    .row_addrs()
                    .unwrap()
                    .map(u64::from)
                    .collect::<std::collections::HashSet<_>>();

                for (list, row_id) in data
                    .column(0)
                    .as_list::<i32>()
                    .iter()
                    .zip(data.column(1).as_primitive::<UInt64Type>())
                {
                    let list = list.unwrap();
                    let row_id = row_id.unwrap();
                    let vals = list.as_primitive::<UInt8Type>().values();
                    if row_addrs_set.contains(&row_id) {
                        assert!(match_fn(vals));
                    } else {
                        assert!(no_match_fn(vals));
                    }
                }
            }
        };

        // Simple check for 1 value (doesn't matter intersection vs union)
        check(
            LabelListQuery::HasAnyLabel(vec![ScalarValue::UInt8(Some(1))]),
            Box::new(|vals| vals.contains(&1)),
            Box::new(|vals| !vals.contains(&1)),
        )
        .await;
        check(
            LabelListQuery::HasAllLabels(vec![ScalarValue::UInt8(Some(1))]),
            Box::new(|vals| vals.contains(&1)),
            Box::new(|vals| !vals.contains(&1)),
        )
        .await;
        // Set intersection
        check(
            LabelListQuery::HasAllLabels(vec![
                ScalarValue::UInt8(Some(1)),
                ScalarValue::UInt8(Some(2)),
            ]),
            // Match must have 1 and 2
            Box::new(|vals| vals.contains(&1) && vals.contains(&2)),
            // No-match must either not have 1 or not have 2
            Box::new(|vals| !vals.contains(&1) || !vals.contains(&2)),
        )
        .await;
        // Set union
        check(
            LabelListQuery::HasAnyLabel(vec![
                ScalarValue::UInt8(Some(1)),
                ScalarValue::UInt8(Some(2)),
            ]),
            // Match either have 1 or have 2
            Box::new(|vals| vals.contains(&1) || vals.contains(&2)),
            // No-match must not have 1 and not have 2
            Box::new(|vals| !vals.contains(&1) && !vals.contains(&2)),
        )
        .await;
    }

    #[tokio::test]
    async fn test_label_list_null_handling() {
        let tempdir = TempDir::default();
        let index_store = test_store(&tempdir);

        // Create test data with null items within lists:
        // Row 0: [1, 2] - no nulls
        // Row 1: [3, null] - has a null item
        // Row 2: [4] - no nulls
        let list_array = ListArray::from_iter_primitive::<UInt8Type, _, _>(vec![
            Some(vec![Some(1), Some(2)]),
            Some(vec![Some(3), None]),
            Some(vec![Some(4)]),
        ]);
        let row_ids = UInt64Array::from_iter_values(0..3);
        // Create schema with nullable list items to match the ListArray
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                VALUE_COLUMN_NAME,
                DataType::List(Arc::new(Field::new("item", DataType::UInt8, true))),
                true,
            ),
            Field::new(ROW_ID, DataType::UInt64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(list_array), Arc::new(row_ids)],
        )
        .unwrap();

        let batch_reader = RecordBatchIterator::new(vec![Ok(batch)], schema);
        train_tag(&index_store, batch_reader).await;

        let index = LabelListIndexPlugin
            .load_index(
                index_store,
                &default_details::<pbold::LabelListIndexDetails>(),
                None,
                &LanceCache::no_cache(),
            )
            .await
            .unwrap();

        // Test: Search for lists containing value 1
        // Row 0: [1, 2] - contains 1 → TRUE
        // Row 1: [3, null] - null elements are ignored → FALSE
        // Row 2: [4] - doesn't contain 1 → FALSE
        let query = LabelListQuery::HasAnyLabel(vec![ScalarValue::UInt8(Some(1))]);
        let result = index.search(&query, &NoOpMetricsCollector).await.unwrap();

        match result {
            SearchResult::Exact(row_ids) => {
                let actual_rows: Vec<u64> = row_ids
                    .true_rows()
                    .row_addrs()
                    .unwrap()
                    .map(u64::from)
                    .collect();
                assert_eq!(
                    actual_rows,
                    vec![0],
                    "Should find row 0 where list contains 1"
                );

                assert!(
                    row_ids.null_rows().is_empty(),
                    "null_row_ids should be empty when null elements are ignored"
                );
            }
            _ => panic!("Expected Exact search result"),
        }
    }

    #[tokio::test]
    async fn test_label_list_bitmap_only_layout_is_compatible() {
        let tempdir = TempDir::default();
        let index_store = test_store(&tempdir);

        // Simulate an older released layout that only had the bitmap lookup file.
        let values = arrow_array::UInt8Array::from(vec![1, 2]);
        let row_ids = UInt64Array::from(vec![0, 2]);
        let schema = Arc::new(Schema::new(vec![
            Field::new(VALUE_COLUMN_NAME, DataType::UInt8, true),
            Field::new(ROW_ID, DataType::UInt64, false),
        ]));
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(values), Arc::new(row_ids)])
            .unwrap();

        BitmapIndexPlugin::train_bitmap_index(
            lance_datafusion::utils::reader_to_stream(Box::new(RecordBatchIterator::new(
                vec![Ok(batch)],
                schema,
            ))),
            index_store.as_ref(),
        )
        .await
        .unwrap();

        let index = LabelListIndexPlugin
            .load_index(
                index_store,
                &default_details::<pbold::LabelListIndexDetails>(),
                None,
                &LanceCache::no_cache(),
            )
            .await
            .unwrap();

        let query = LabelListQuery::HasAnyLabel(vec![ScalarValue::UInt8(Some(1))]);
        let result = index.search(&query, &NoOpMetricsCollector).await.unwrap();

        match result {
            SearchResult::Exact(row_ids) => {
                assert!(row_ids.null_rows().is_empty());
                let actual_rows: Vec<u64> = row_ids
                    .true_rows()
                    .row_addrs()
                    .unwrap()
                    .map(u64::from)
                    .collect();
                assert_eq!(actual_rows, vec![0]);
            }
            _ => panic!("Expected Exact search result"),
        }
    }
}
