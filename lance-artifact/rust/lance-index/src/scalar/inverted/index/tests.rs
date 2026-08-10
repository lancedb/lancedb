// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use crate::scalar::inverted::document_tokenizer::DocType;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use futures::stream;
use lance_core::cache::{LanceCache, QuickCacheBackend};
use lance_core::utils::tempfile::TempObjDir;
use lance_io::object_store::ObjectStore;

use crate::metrics::{LocalMetricsCollector, NoOpMetricsCollector};
use crate::prefilter::NoFilter;
use crate::scalar::ScalarIndex;
use crate::scalar::inverted::builder::{
    InnerBuilder, InvertedIndexBuilder, PositionRecorder, doc_file_path, inverted_list_schema,
    inverted_list_schema_for_version_with_block_size,
    inverted_list_schema_for_version_with_block_size_and_impacts, posting_file_path,
    token_file_path,
};
use crate::scalar::inverted::encoding::{
    compress_positions, compress_posting_list_with_tail_codec,
    decompress_posting_list_with_tail_codec, encode_position_stream_block_into,
};
use crate::scalar::inverted::query::{FtsSearchParams, Operator};
use crate::scalar::lance_format::LanceIndexStore;
use arrow::array::{
    AsArray, GenericListBuilder, GenericStringBuilder, Int32Builder, LargeBinaryBuilder,
    ListBuilder, UInt32Builder,
};
use arrow::datatypes::{Float32Type, UInt32Type};
use arrow_array::{ArrayRef, Float32Array, RecordBatch, StringArray, UInt32Array, UInt64Array};
use arrow_schema::{DataType, Field, Schema};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use crate::scalar::inverted::tokenizer::document_tokenizer::TextTokenizer;
use lance_tokenizer::{Language, SimpleTokenizer, StopWordFilter, TextAnalyzer};

use super::*;

async fn write_single_partition_index(
    store: Arc<LanceIndexStore>,
    params: InvertedIndexParams,
    token_set_format: TokenSetFormat,
    token: &str,
    row_id: u64,
) -> Result<Arc<InvertedIndex>> {
    let block_size = params.posting_block_size();
    let format_version = params.resolved_format_version();
    let mut partition = InnerBuilder::new_with_format_version_and_block_size(
        0,
        false,
        token_set_format,
        format_version,
        block_size,
    );
    partition.tokens.add(token.to_owned());
    let mut posting_list = PostingListBuilder::new_with_posting_tail_codec_and_block_size(
        false,
        format_version.posting_tail_codec(),
        block_size,
    );
    posting_list.add(0, PositionRecorder::Count(1));
    partition.posting_lists.push(posting_list);
    partition.docs.append(row_id, 1);
    partition.write(store.as_ref()).await?;

    let metadata = HashMap::from([
        (
            "partitions".to_owned(),
            serde_json::to_string(&vec![0_u64]).unwrap(),
        ),
        ("params".to_owned(), serde_json::to_string(&params).unwrap()),
        (
            TOKEN_SET_FORMAT_KEY.to_owned(),
            token_set_format.to_string(),
        ),
        (
            POSTING_TAIL_CODEC_KEY.to_owned(),
            format_version.posting_tail_codec().as_str().to_owned(),
        ),
        (
            FTS_FORMAT_VERSION_KEY.to_owned(),
            format_version.index_version().to_string(),
        ),
        (POSTING_BLOCK_SIZE_KEY.to_owned(), block_size.to_string()),
    ]);
    let mut writer = store
        .new_index_file(METADATA_FILE, Arc::new(arrow_schema::Schema::empty()))
        .await?;
    writer.finish_with_metadata(metadata).await?;

    InvertedIndex::load(store, None, &LanceCache::no_cache()).await
}

fn empty_doc_stream() -> SendableRecordBatchStream {
    let schema = Arc::new(Schema::new(vec![
        Field::new("doc", DataType::Utf8, true),
        Field::new(ROW_ID, DataType::UInt64, false),
    ]));
    Box::pin(RecordBatchStreamAdapter::new(
        schema,
        stream::iter(Vec::<datafusion::error::Result<RecordBatch>>::new()),
    ))
}

async fn write_test_metadata(
    store: &Arc<LanceIndexStore>,
    partition_ids: Vec<u64>,
    params: InvertedIndexParams,
) {
    let format_version = params.resolved_format_version();
    let metadata = HashMap::from([
        (
            "partitions".to_owned(),
            serde_json::to_string(&partition_ids).unwrap(),
        ),
        ("params".to_owned(), serde_json::to_string(&params).unwrap()),
        (
            TOKEN_SET_FORMAT_KEY.to_owned(),
            TokenSetFormat::default().to_string(),
        ),
        (
            POSTING_TAIL_CODEC_KEY.to_owned(),
            format_version.posting_tail_codec().as_str().to_owned(),
        ),
        (
            FTS_FORMAT_VERSION_KEY.to_owned(),
            format_version.index_version().to_string(),
        ),
        (
            POSTING_BLOCK_SIZE_KEY.to_owned(),
            params.posting_block_size().to_string(),
        ),
    ]);
    let mut writer = store
        .new_index_file(METADATA_FILE, Arc::new(arrow_schema::Schema::empty()))
        .await
        .unwrap();
    writer.finish_with_metadata(metadata).await.unwrap();
}

mod flat_search;
mod format_and_builder;
mod grouping;
mod lifecycle;
mod position_cache;
mod prewarm_cache;
mod query;
mod scoring;
mod stats;
