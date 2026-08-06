// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::sync::Arc;

use arrow_array::{RecordBatch, RecordBatchReader};
use arrow_schema::ArrowError;
use futures::TryStreamExt;
use lance_core::{cache::LanceCache, datatypes::Schema, utils::tempfile::TempObjFile};
use lance_encoding::decoder::{DecoderPlugins, FilterExpression};
use lance_io::{
    ReadBatchParams,
    object_store::ObjectStore,
    scheduler::{ScanScheduler, SchedulerConfig},
    utils::CachedFileSize,
};

use crate::reader::{FileReader, FileReaderOptions};
use crate::version::ConcreteFileVersion;
use crate::{versions, writer::FileWriterOptions};

pub struct FsFixture {
    pub tmp_path: TempObjFile,
    pub object_store: Arc<ObjectStore>,
    pub scheduler: Arc<ScanScheduler>,
}

impl Default for FsFixture {
    fn default() -> Self {
        let tmp_path = TempObjFile::default();
        let object_store = Arc::new(ObjectStore::local());
        let scheduler =
            ScanScheduler::new(object_store.clone(), SchedulerConfig::default_for_testing());
        Self {
            object_store,
            tmp_path,
            scheduler,
        }
    }
}

pub struct WrittenFile {
    pub schema: Arc<Schema>,
    pub data: Vec<RecordBatch>,
    pub field_id_mapping: Vec<(u32, u32)>,
}

pub async fn write_lance_file(
    data: impl RecordBatchReader,
    fs: &FsFixture,
    version: ConcreteFileVersion,
    options: FileWriterOptions,
) -> WrittenFile {
    let writer = fs.object_store.create(&fs.tmp_path).await.unwrap();

    let lance_schema = lance_core::datatypes::Schema::try_from(data.schema().as_ref()).unwrap();

    let mut file_writer =
        versions::create_writer(version, writer, lance_schema.clone(), options).unwrap();

    let data = data
        .collect::<std::result::Result<Vec<_>, ArrowError>>()
        .unwrap();

    for batch in &data {
        file_writer.write_batch(batch).await.unwrap();
    }
    let field_id_mapping = file_writer.field_id_to_column_indices().to_vec();
    file_writer.add_schema_metadata("foo", "bar");
    file_writer.finish().await.unwrap();
    WrittenFile {
        schema: Arc::new(lance_schema),
        data,
        field_id_mapping,
    }
}

pub fn test_cache() -> Arc<LanceCache> {
    Arc::new(LanceCache::with_capacity(128 * 1024 * 1024))
}

pub async fn read_lance_file(
    fs: &FsFixture,
    decoder_middleware: Arc<DecoderPlugins>,
    filter: FilterExpression,
) -> Vec<RecordBatch> {
    let file_scheduler = fs
        .scheduler
        .open_file(&fs.tmp_path, &CachedFileSize::unknown())
        .await
        .unwrap();
    let file_reader = FileReader::try_open(
        file_scheduler,
        None,
        decoder_middleware,
        &test_cache(),
        FileReaderOptions::default(),
    )
    .await
    .unwrap();

    let schema = file_reader.schema();
    assert_eq!(schema.metadata.get("foo").unwrap(), "bar");

    let batch_stream = file_reader
        .read_stream(ReadBatchParams::RangeFull, 1024, 16, filter)
        .await
        .unwrap();

    batch_stream.try_collect().await.unwrap()
}

pub async fn count_lance_file(
    fs: &FsFixture,
    decoder_middleware: Arc<DecoderPlugins>,
    filter: FilterExpression,
) -> usize {
    read_lance_file(fs, decoder_middleware, filter)
        .await
        .iter()
        .map(|b| b.num_rows())
        .sum()
}
