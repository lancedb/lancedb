// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::vec;

use crate::dataset::ROW_ID;
use crate::dataset::builder::DatasetBuilder;
use crate::dataset::tests::dataset_migrations::scan_dataset;
use crate::dataset::tests::dataset_transactions::{assert_results, execute_sql};
use crate::index::vector::VectorIndexParams;
use crate::session::Session;
use crate::{Dataset, Error, Result};
use lance_arrow::FixedSizeListArrayExt;

use crate::dataset::write::{WriteMode, WriteParams};
use crate::index::DatasetIndexExt;
use arrow::array::{AsArray, GenericListBuilder, GenericStringBuilder};
use arrow::datatypes::UInt64Type;
use arrow_array::RecordBatch;
use arrow_array::{Array, GenericStringArray, LargeListArray, ListArray, StructArray, UInt64Array};
use arrow_array::{
    ArrayRef, Float32Array, Int32Array, RecordBatchIterator, StringArray,
    builder::StringDictionaryBuilder,
    types::{Float32Type, Int32Type, Int64Type},
};
use arrow_schema::{
    DataType, Field as ArrowField, Field, Fields as ArrowFields, Schema as ArrowSchema,
};
use lance_arrow::ARROW_EXT_NAME_KEY;
use lance_core::cache::LanceCache;
use lance_core::utils::tempfile::TempStrDir;
use lance_datafusion::exec::ExecutionSummaryCounts;
use lance_datagen::{BatchCount, Dimension, RowCount, array, gen_batch};
use lance_file::reader::{FileReader, FileReaderOptions};
use lance_file::version::LanceFileVersion;
use lance_index::metrics::{
    COMPOUND_ADDRESS_RESOLUTION_BATCHES_METRIC, COMPOUND_ADDRESSES_RESOLVED_METRIC,
    COMPOUND_PEAK_ADDRESS_RESOLUTION_BATCH_SIZE_METRIC, COMPOUND_PEAK_BUFFERED_CANDIDATES_METRIC,
    COMPOUND_SCORE_FLOOR_OVERFLOWS_METRIC,
};
use lance_index::optimize::OptimizeOptions;
use lance_index::scalar::FullTextSearchQuery;
use lance_index::scalar::inverted::{
    DocumentGranularity, InvertedListFormatVersion, SCORE_COL,
    query::{BooleanQuery, BoostQuery, MatchQuery, Occur, Operator, PhraseQuery},
    tokenizer::InvertedIndexParams,
};
use lance_index::{FtsPrewarmOptions, PrewarmOptions};
use lance_index::{IndexType, scalar::ScalarIndexParams, vector::DIST_COL};
use lance_io::scheduler::{ScanScheduler, SchedulerConfig};
use lance_io::utils::CachedFileSize;
use lance_linalg::distance::MetricType;

use datafusion::common::{assert_contains, assert_not_contains};
use futures::{StreamExt, TryStreamExt};
use itertools::Itertools;
use lance_arrow::json::ARROW_JSON_EXT_NAME;
use lance_index::scalar::inverted::query::{FtsQuery, MultiMatchQuery};
use lance_testing::datagen::generate_random_array;
use rand::Rng;
use rstest::rstest;

#[rstest]
#[tokio::test]
async fn test_create_index(
    #[values(LanceFileVersion::Legacy, LanceFileVersion::Stable)]
    data_storage_version: LanceFileVersion,
) {
    let test_uri = TempStrDir::default();

    let dimension = 16;
    let schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
        "embeddings",
        DataType::FixedSizeList(
            Arc::new(ArrowField::new("item", DataType::Float32, true)),
            dimension,
        ),
        false,
    )]));

    let float_arr = generate_random_array(512 * dimension as usize);
    let vectors = Arc::new(
        <arrow_array::FixedSizeListArray as FixedSizeListArrayExt>::try_new_from_values(
            float_arr, dimension,
        )
        .unwrap(),
    );
    let batches = vec![RecordBatch::try_new(schema.clone(), vec![vectors.clone()]).unwrap()];

    let reader = RecordBatchIterator::new(batches.into_iter().map(Ok), schema.clone());

    let mut dataset = Dataset::write(
        reader,
        &test_uri,
        Some(WriteParams {
            data_storage_version: Some(data_storage_version),
            ..Default::default()
        }),
    )
    .await
    .unwrap();
    dataset.validate().await.unwrap();

    // Make sure valid arguments should create index successfully
    let params = VectorIndexParams::ivf_pq(10, 8, 2, MetricType::L2, 50);
    let index_meta = dataset
        .create_index(&["embeddings"], IndexType::Vector, None, &params, true)
        .await
        .unwrap();
    dataset.validate().await.unwrap();

    // Verify the returned metadata
    assert_eq!(index_meta.name, "embeddings_idx");
    // The version should match the table version it was created from.
    let expected = dataset.manifest.version - 1;
    assert_eq!(index_meta.dataset_version, expected);
    let fragment_bitmap = index_meta.fragment_bitmap.as_ref().unwrap();
    assert_eq!(fragment_bitmap.len(), 1);
    assert!(fragment_bitmap.contains(0));

    // Append should inherit index
    let write_params = WriteParams {
        mode: WriteMode::Append,
        data_storage_version: Some(data_storage_version),
        ..Default::default()
    };
    let batches = vec![RecordBatch::try_new(schema.clone(), vec![vectors.clone()]).unwrap()];
    let reader = RecordBatchIterator::new(batches.into_iter().map(Ok), schema.clone());
    let dataset = Dataset::write(reader, &test_uri, Some(write_params))
        .await
        .unwrap();
    let indices = dataset.load_indices().await.unwrap();
    let actual = indices.first().unwrap().dataset_version;
    let expected = dataset.manifest.version - 2;
    assert_eq!(actual, expected);
    dataset.validate().await.unwrap();
    // Fragment bitmap should show the original fragments, and not include
    // the newly appended fragment.
    let fragment_bitmap = indices.first().unwrap().fragment_bitmap.as_ref().unwrap();
    assert_eq!(fragment_bitmap.len(), 1);
    assert!(fragment_bitmap.contains(0));

    let actual_statistics: serde_json::Value =
        serde_json::from_str(&dataset.index_statistics("embeddings_idx").await.unwrap()).unwrap();
    let actual_statistics = actual_statistics.as_object().unwrap();
    assert_eq!(actual_statistics["index_type"].as_str().unwrap(), "IVF_PQ");

    let deltas = actual_statistics["indices"].as_array().unwrap();
    assert_eq!(deltas.len(), 1);
    assert_eq!(deltas[0]["metric_type"].as_str().unwrap(), "l2");
    assert_eq!(deltas[0]["num_partitions"].as_i64().unwrap(), 10);

    assert!(dataset.index_statistics("non-existent_idx").await.is_err());
    assert!(dataset.index_statistics("").await.is_err());

    // Overwrite should invalidate index
    let write_params = WriteParams {
        mode: WriteMode::Overwrite,
        data_storage_version: Some(data_storage_version),
        ..Default::default()
    };
    let batches = vec![RecordBatch::try_new(schema.clone(), vec![vectors]).unwrap()];
    let reader = RecordBatchIterator::new(batches.into_iter().map(Ok), schema.clone());
    let dataset = Dataset::write(reader, &test_uri, Some(write_params))
        .await
        .unwrap();
    assert!(dataset.manifest.index_section.is_none());
    assert!(dataset.load_indices().await.unwrap().is_empty());
    dataset.validate().await.unwrap();

    let fragment_bitmap = indices.first().unwrap().fragment_bitmap.as_ref().unwrap();
    assert_eq!(fragment_bitmap.len(), 1);
    assert!(fragment_bitmap.contains(0));
}

#[rstest]
#[tokio::test]
async fn test_create_scalar_index(
    #[values(LanceFileVersion::Legacy, LanceFileVersion::Stable)]
    data_storage_version: LanceFileVersion,
    #[values(false, true)] use_stable_row_id: bool,
) {
    let test_uri = TempStrDir::default();

    let data = gen_batch().col("int", array::step::<Int32Type>());
    // Write 64Ki rows.  We should get 16 4Ki pages
    let mut dataset = Dataset::write(
        data.into_reader_rows(RowCount::from(16 * 1024), BatchCount::from(4)),
        &test_uri,
        Some(WriteParams {
            data_storage_version: Some(data_storage_version),
            enable_stable_row_ids: use_stable_row_id,
            ..Default::default()
        }),
    )
    .await
    .unwrap();

    let index_name = "my_index".to_string();

    dataset
        .create_index(
            &["int"],
            IndexType::Scalar,
            Some(index_name.clone()),
            &ScalarIndexParams::default(),
            false,
        )
        .await
        .unwrap();

    let indices = dataset.load_indices_by_name(&index_name).await.unwrap();

    assert_eq!(indices.len(), 1);
    assert_eq!(indices[0].dataset_version, 1);
    assert_eq!(indices[0].fields, vec![0]);
    assert_eq!(indices[0].name, index_name);

    dataset.index_statistics(&index_name).await.unwrap();
}

async fn create_bad_file(data_storage_version: LanceFileVersion) -> Result<Dataset> {
    let test_uri = TempStrDir::default();

    let schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
        "a.b.c",
        DataType::Int32,
        false,
    )]));

    let batches: Vec<RecordBatch> = (0..20)
        .map(|i| {
            RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(Int32Array::from_iter_values(i * 20..(i + 1) * 20))],
            )
            .unwrap()
        })
        .collect();
    let reader = RecordBatchIterator::new(batches.into_iter().map(Ok), schema.clone());
    Dataset::write(
        reader,
        &test_uri,
        Some(WriteParams {
            data_storage_version: Some(data_storage_version),
            ..Default::default()
        }),
    )
    .await
}

#[tokio::test]
async fn test_create_fts_index_with_empty_table() {
    let test_uri = TempStrDir::default();

    let schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
        "text",
        DataType::Utf8,
        false,
    )]));

    let batches: Vec<RecordBatch> = vec![];
    let reader = RecordBatchIterator::new(batches.into_iter().map(Ok), schema.clone());
    let mut dataset = Dataset::write(reader, &test_uri, None)
        .await
        .expect("write dataset");

    let params = InvertedIndexParams::default();
    dataset
        .create_index(&["text"], IndexType::Inverted, None, &params, true)
        .await
        .unwrap();

    let batch = dataset
        .scan()
        .full_text_search(FullTextSearchQuery::new("lance".to_owned()))
        .unwrap()
        .try_into_batch()
        .await
        .unwrap();
    assert_eq!(batch.num_rows(), 0);
}

#[rstest]
#[tokio::test]
async fn test_create_int8_index(
    #[values(LanceFileVersion::Legacy, LanceFileVersion::Stable)]
    data_storage_version: LanceFileVersion,
) {
    use lance_testing::datagen::generate_random_int8_array;

    let test_uri = TempStrDir::default();

    let dimension = 16;
    let schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
        "embeddings",
        DataType::FixedSizeList(
            Arc::new(ArrowField::new("item", DataType::Int8, true)),
            dimension,
        ),
        false,
    )]));

    let int8_arr = generate_random_int8_array(512 * dimension as usize);
    let vectors = Arc::new(
        <arrow_array::FixedSizeListArray as FixedSizeListArrayExt>::try_new_from_values(
            int8_arr, dimension,
        )
        .unwrap(),
    );
    let batches = vec![RecordBatch::try_new(schema.clone(), vec![vectors.clone()]).unwrap()];

    let reader = RecordBatchIterator::new(batches.into_iter().map(Ok), schema.clone());

    let mut dataset = Dataset::write(
        reader,
        &test_uri,
        Some(WriteParams {
            data_storage_version: Some(data_storage_version),
            ..Default::default()
        }),
    )
    .await
    .unwrap();
    dataset.validate().await.unwrap();

    // Make sure valid arguments should create index successfully
    let params = VectorIndexParams::ivf_pq(10, 8, 2, MetricType::L2, 50);
    let index_meta = dataset
        .create_index(&["embeddings"], IndexType::Vector, None, &params, true)
        .await
        .unwrap();
    dataset.validate().await.unwrap();

    // Verify the returned metadata
    assert_eq!(index_meta.name, "embeddings_idx");
    // The version should match the table version it was created from.
    let expected = dataset.manifest.version - 1;
    assert_eq!(index_meta.dataset_version, expected);
    let fragment_bitmap = index_meta.fragment_bitmap.as_ref().unwrap();
    assert_eq!(fragment_bitmap.len(), 1);
    assert!(fragment_bitmap.contains(0));

    // Append should inherit index
    let write_params = WriteParams {
        mode: WriteMode::Append,
        data_storage_version: Some(data_storage_version),
        ..Default::default()
    };
    let batches = vec![RecordBatch::try_new(schema.clone(), vec![vectors.clone()]).unwrap()];
    let reader = RecordBatchIterator::new(batches.into_iter().map(Ok), schema.clone());
    let dataset = Dataset::write(reader, &test_uri, Some(write_params))
        .await
        .unwrap();
    let indices = dataset.load_indices().await.unwrap();
    let actual = indices.first().unwrap().dataset_version;
    let expected = dataset.manifest.version - 2;
    assert_eq!(actual, expected);
    dataset.validate().await.unwrap();
    // Fragment bitmap should show the original fragments, and not include
    // the newly appended fragment.
    let fragment_bitmap = indices.first().unwrap().fragment_bitmap.as_ref().unwrap();
    assert_eq!(fragment_bitmap.len(), 1);
    assert!(fragment_bitmap.contains(0));

    let actual_statistics: serde_json::Value =
        serde_json::from_str(&dataset.index_statistics("embeddings_idx").await.unwrap()).unwrap();
    let actual_statistics = actual_statistics.as_object().unwrap();
    assert_eq!(actual_statistics["index_type"].as_str().unwrap(), "IVF_PQ");

    let deltas = actual_statistics["indices"].as_array().unwrap();
    assert_eq!(deltas.len(), 1);
    assert_eq!(deltas[0]["metric_type"].as_str().unwrap(), "l2");
    assert_eq!(deltas[0]["num_partitions"].as_i64().unwrap(), 10);

    assert!(dataset.index_statistics("non-existent_idx").await.is_err());
    assert!(dataset.index_statistics("").await.is_err());

    // Overwrite should invalidate index
    let write_params = WriteParams {
        mode: WriteMode::Overwrite,
        data_storage_version: Some(data_storage_version),
        ..Default::default()
    };
    let batches = vec![RecordBatch::try_new(schema.clone(), vec![vectors]).unwrap()];
    let reader = RecordBatchIterator::new(batches.into_iter().map(Ok), schema.clone());
    let dataset = Dataset::write(reader, &test_uri, Some(write_params))
        .await
        .unwrap();
    assert!(dataset.manifest.index_section.is_none());
    assert!(dataset.load_indices().await.unwrap().is_empty());
    dataset.validate().await.unwrap();

    let fragment_bitmap = indices.first().unwrap().fragment_bitmap.as_ref().unwrap();
    assert_eq!(fragment_bitmap.len(), 1);
    assert!(fragment_bitmap.contains(0));
}

#[tokio::test]
async fn test_create_fts_index_with_empty_strings() {
    let test_uri = TempStrDir::default();

    let schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
        "text",
        DataType::Utf8,
        false,
    )]));

    let batches: Vec<RecordBatch> = vec![
        RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(StringArray::from(vec!["", "", ""]))],
        )
        .unwrap(),
    ];
    let reader = RecordBatchIterator::new(batches.into_iter().map(Ok), schema.clone());
    let mut dataset = Dataset::write(reader, &test_uri, None)
        .await
        .expect("write dataset");

    let params = InvertedIndexParams::default();
    dataset
        .create_index(&["text"], IndexType::Inverted, None, &params, true)
        .await
        .unwrap();

    let batch = dataset
        .scan()
        .full_text_search(FullTextSearchQuery::new("lance".to_owned()))
        .unwrap()
        .try_into_batch()
        .await
        .unwrap();
    assert_eq!(batch.num_rows(), 0);
}

#[rstest]
#[tokio::test]
async fn test_bad_field_name(
    #[values(LanceFileVersion::Legacy, LanceFileVersion::Stable)]
    data_storage_version: LanceFileVersion,
) {
    // don't allow `.` in the field name
    assert!(create_bad_file(data_storage_version).await.is_err());
}

#[tokio::test]
async fn test_open_dataset_not_found() {
    let result = Dataset::open(".").await;
    assert!(matches!(result.unwrap_err(), Error::DatasetNotFound { .. }));
}

#[rstest]
#[tokio::test]
async fn test_search_empty(
    #[values(LanceFileVersion::Legacy, LanceFileVersion::Stable)]
    data_storage_version: LanceFileVersion,
) {
    // Create a table
    let schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
        "vec",
        DataType::FixedSizeList(
            Arc::new(ArrowField::new("item", DataType::Float32, true)),
            128,
        ),
        false,
    )]));

    let test_uri = TempStrDir::default();

    let vectors = Arc::new(
        <arrow_array::FixedSizeListArray as FixedSizeListArrayExt>::try_new_from_values(
            Float32Array::from_iter_values(vec![]),
            128,
        )
        .unwrap(),
    );

    let data = RecordBatch::try_new(schema.clone(), vec![vectors]);
    let reader = RecordBatchIterator::new(vec![data.unwrap()].into_iter().map(Ok), schema);
    let dataset = Dataset::write(
        reader,
        &test_uri,
        Some(WriteParams {
            data_storage_version: Some(data_storage_version),
            ..Default::default()
        }),
    )
    .await
    .unwrap();

    let mut stream = dataset
        .scan()
        .nearest(
            "vec",
            &Float32Array::from_iter_values((0..128).map(|_| 0.1)),
            1,
        )
        .unwrap()
        .try_into_stream()
        .await
        .unwrap();

    while let Some(batch) = stream.next().await {
        let schema = batch.unwrap().schema();
        assert_eq!(schema.fields.len(), 2);
        assert_eq!(
            schema.field_with_name("vec").unwrap(),
            &ArrowField::new(
                "vec",
                DataType::FixedSizeList(
                    Arc::new(ArrowField::new("item", DataType::Float32, true)),
                    128
                ),
                false,
            )
        );
        assert_eq!(
            schema.field_with_name(DIST_COL).unwrap(),
            &ArrowField::new(DIST_COL, DataType::Float32, true)
        );
    }
}

#[rstest]
#[tokio::test]
async fn test_search_empty_after_delete(
    #[values(LanceFileVersion::Legacy, LanceFileVersion::Stable)]
    data_storage_version: LanceFileVersion,
    #[values(false, true)] use_stable_row_id: bool,
) {
    // Create a table
    let test_uri = TempStrDir::default();

    let data = gen_batch().col("vec", array::rand_vec::<Float32Type>(Dimension::from(32)));
    let reader = data.into_reader_rows(RowCount::from(500), BatchCount::from(1));
    let mut dataset = Dataset::write(
        reader,
        &test_uri,
        Some(WriteParams {
            data_storage_version: Some(data_storage_version),
            enable_stable_row_ids: use_stable_row_id,
            ..Default::default()
        }),
    )
    .await
    .unwrap();

    let params = VectorIndexParams::ivf_pq(1, 8, 1, MetricType::L2, 50);
    dataset
        .create_index(&["vec"], IndexType::Vector, None, &params, true)
        .await
        .unwrap();

    dataset.delete("true").await.unwrap();

    // This behavior will be re-introduced once we work on empty vector index handling.
    // https://github.com/lance-format/lance/issues/4034
    // let indices = dataset.load_indices().await.unwrap();
    // // With the new retention behavior, indices are kept even when all fragments are deleted
    // // This allows the index configuration to persist through data changes
    // assert_eq!(indices.len(), 1);

    // // Verify the index has an empty effective fragment bitmap
    // let index = &indices[0];
    // let effective_bitmap = index
    //     .effective_fragment_bitmap(&dataset.fragment_bitmap)
    //     .unwrap();
    // assert!(effective_bitmap.is_empty());

    let mut stream = dataset
        .scan()
        .nearest(
            "vec",
            &Float32Array::from_iter_values((0..32).map(|_| 0.1)),
            1,
        )
        .unwrap()
        .try_into_stream()
        .await
        .unwrap();

    while let Some(batch) = stream.next().await {
        let schema = batch.unwrap().schema();
        assert_eq!(schema.fields.len(), 2);
        assert_eq!(
            schema.field_with_name("vec").unwrap(),
            &ArrowField::new(
                "vec",
                DataType::FixedSizeList(
                    Arc::new(ArrowField::new("item", DataType::Float32, true)),
                    32
                ),
                false,
            )
        );
        assert_eq!(
            schema.field_with_name(DIST_COL).unwrap(),
            &ArrowField::new(DIST_COL, DataType::Float32, true)
        );
    }

    // predicate with redundant whitespace
    dataset.delete(" True").await.unwrap();

    let mut stream = dataset
        .scan()
        .nearest(
            "vec",
            &Float32Array::from_iter_values((0..32).map(|_| 0.1)),
            1,
        )
        .unwrap()
        .try_into_stream()
        .await
        .unwrap();

    while let Some(batch) = stream.next().await {
        let batch = batch.unwrap();
        let schema = batch.schema();
        assert_eq!(schema.fields.len(), 2);
        assert_eq!(
            schema.field_with_name("vec").unwrap(),
            &ArrowField::new(
                "vec",
                DataType::FixedSizeList(
                    Arc::new(ArrowField::new("item", DataType::Float32, true)),
                    32
                ),
                false,
            )
        );
        assert_eq!(
            schema.field_with_name(DIST_COL).unwrap(),
            &ArrowField::new(DIST_COL, DataType::Float32, true)
        );
        assert_eq!(batch.num_rows(), 0, "Expected no results after delete");
    }
}

#[rstest]
#[tokio::test]
async fn test_num_small_files(
    #[values(LanceFileVersion::Legacy, LanceFileVersion::Stable)]
    data_storage_version: LanceFileVersion,
) {
    let test_uri = TempStrDir::default();
    let dimensions = 16;
    let column_name = "vec";
    let field = ArrowField::new(
        column_name,
        DataType::FixedSizeList(
            Arc::new(ArrowField::new("item", DataType::Float32, true)),
            dimensions,
        ),
        false,
    );

    let schema = Arc::new(ArrowSchema::new(vec![field]));

    let float_arr = generate_random_array(512 * dimensions as usize);
    let vectors =
        arrow_array::FixedSizeListArray::try_new_from_values(float_arr, dimensions).unwrap();

    let record_batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(vectors)]).unwrap();

    let reader = RecordBatchIterator::new(vec![record_batch].into_iter().map(Ok), schema.clone());

    let dataset = Dataset::write(
        reader,
        &test_uri,
        Some(WriteParams {
            data_storage_version: Some(data_storage_version),
            ..Default::default()
        }),
    )
    .await
    .unwrap();
    dataset.validate().await.unwrap();

    assert!(dataset.num_small_files(1024).await > 0);
    assert!(dataset.num_small_files(512).await == 0);
}

#[tokio::test]
async fn test_read_struct_of_dictionary_arrays() {
    let test_uri = TempStrDir::default();

    let arrow_schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
        "s",
        DataType::Struct(ArrowFields::from(vec![ArrowField::new(
            "d",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            true,
        )])),
        true,
    )]));

    let mut batches: Vec<RecordBatch> = Vec::new();
    for _ in 1..2 {
        let mut dict_builder = StringDictionaryBuilder::<Int32Type>::new();
        dict_builder.append("a").unwrap();
        dict_builder.append("b").unwrap();
        dict_builder.append("c").unwrap();
        dict_builder.append("d").unwrap();

        let struct_array = Arc::new(StructArray::from(vec![(
            Arc::new(ArrowField::new(
                "d",
                DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
                true,
            )),
            Arc::new(dict_builder.finish()) as ArrayRef,
        )]));

        let batch = RecordBatch::try_new(arrow_schema.clone(), vec![struct_array.clone()]).unwrap();
        batches.push(batch);
    }

    let batch_reader =
        RecordBatchIterator::new(batches.clone().into_iter().map(Ok), arrow_schema.clone());
    Dataset::write(batch_reader, &test_uri, Some(WriteParams::default()))
        .await
        .unwrap();

    let result = scan_dataset(&test_uri).await.unwrap();

    assert_eq!(batches, result);
}

#[tokio::test]
async fn test_fts_fuzzy_query() {
    let params = InvertedIndexParams::default();
    let text_col = GenericStringArray::<i32>::from(vec![
        "fa", "fo", "fob", "focus", "foo", "food", "foul", // # spellchecker:disable-line
    ]);
    let batch = RecordBatch::try_new(
        arrow_schema::Schema::new(vec![arrow_schema::Field::new(
            "text",
            text_col.data_type().to_owned(),
            false,
        )])
        .into(),
        vec![Arc::new(text_col) as ArrayRef],
    )
    .unwrap();
    let schema = batch.schema();
    let batches = RecordBatchIterator::new(vec![batch].into_iter().map(Ok), schema);
    let test_uri = TempStrDir::default();
    let mut dataset = Dataset::write(batches, &test_uri, None).await.unwrap();
    dataset
        .create_index(&["text"], IndexType::Inverted, None, &params, true)
        .await
        .unwrap();
    let results = dataset
        .scan()
        .full_text_search(FullTextSearchQuery::new_fuzzy("foo".to_owned(), Some(1)))
        .unwrap()
        .try_into_batch()
        .await
        .unwrap();
    assert_eq!(results.num_rows(), 4);
    let texts = results["text"]
        .as_string::<i32>()
        .iter()
        .map(|s| s.unwrap().to_owned())
        .collect::<HashSet<_>>();
    assert_eq!(
        texts,
        vec![
            "foo".to_owned(),  // 0 edits
            "fo".to_owned(),   // 1 deletion        # spellchecker:disable-line
            "fob".to_owned(),  // 1 substitution    # spellchecker:disable-line
            "food".to_owned(), // 1 insertion       # spellchecker:disable-line
        ]
        .into_iter()
        .collect::<HashSet<_>>()
    );
}

#[tokio::test]
async fn test_fts_on_multiple_columns() {
    let params = InvertedIndexParams::default();
    let title_col =
        GenericStringArray::<i32>::from(vec!["title common", "title hello", "title lance"]);
    let content_col = GenericStringArray::<i32>::from(vec![
        "content world",
        "content database",
        "content common",
    ]);
    let batch = RecordBatch::try_new(
        arrow_schema::Schema::new(vec![
            arrow_schema::Field::new("title", title_col.data_type().to_owned(), false),
            arrow_schema::Field::new("content", title_col.data_type().to_owned(), false),
        ])
        .into(),
        vec![
            Arc::new(title_col) as ArrayRef,
            Arc::new(content_col) as ArrayRef,
        ],
    )
    .unwrap();
    let schema = batch.schema();
    let batches = RecordBatchIterator::new(vec![batch].into_iter().map(Ok), schema);
    let test_uri = TempStrDir::default();
    let mut dataset = Dataset::write(batches, &test_uri, None).await.unwrap();
    dataset
        .create_index(&["title"], IndexType::Inverted, None, &params, true)
        .await
        .unwrap();
    dataset
        .create_index(&["content"], IndexType::Inverted, None, &params, true)
        .await
        .unwrap();

    let results = dataset
        .scan()
        .full_text_search(FullTextSearchQuery::new("title".to_owned()))
        .unwrap()
        .try_into_batch()
        .await
        .unwrap();
    assert_eq!(results.num_rows(), 3);

    let results = dataset
        .scan()
        .full_text_search(FullTextSearchQuery::new("content".to_owned()))
        .unwrap()
        .try_into_batch()
        .await
        .unwrap();
    assert_eq!(results.num_rows(), 3);

    let results = dataset
        .scan()
        .full_text_search(FullTextSearchQuery::new("common".to_owned()))
        .unwrap()
        .try_into_batch()
        .await
        .unwrap();
    assert_eq!(results.num_rows(), 2);

    let results = dataset
        .scan()
        .full_text_search(
            FullTextSearchQuery::new("common".to_owned())
                .with_column("title".to_owned())
                .unwrap(),
        )
        .unwrap()
        .try_into_batch()
        .await
        .unwrap();
    assert_eq!(results.num_rows(), 1);

    let results = dataset
        .scan()
        .full_text_search(
            FullTextSearchQuery::new("common".to_owned())
                .with_column("content".to_owned())
                .unwrap(),
        )
        .unwrap()
        .try_into_batch()
        .await
        .unwrap();
    assert_eq!(results.num_rows(), 1);
}

async fn create_fragmented_fts_index(dataset: &mut Dataset, column: &str, with_position: bool) {
    create_fragmented_fts_index_with_order(dataset, column, with_position, false).await;
}

async fn create_fragmented_fts_index_with_order(
    dataset: &mut Dataset,
    column: &str,
    with_position: bool,
    reverse_segments: bool,
) {
    let index_name = format!("{column}_idx");
    let columns = [column];
    let params = InvertedIndexParams::default().with_position(with_position);
    let fragment_ids = dataset
        .get_fragments()
        .iter()
        .map(|fragment| fragment.id() as u32)
        .collect::<Vec<_>>();
    let mut segments = Vec::with_capacity(fragment_ids.len());
    for fragment_id in &fragment_ids {
        let mut builder = dataset
            .create_index_builder(&columns, IndexType::Inverted, &params)
            .name(index_name.clone())
            .fragments(vec![*fragment_id]);
        segments.push(builder.execute_uncommitted().await.unwrap());
    }
    if reverse_segments {
        segments.reverse();
    }
    dataset
        .commit_existing_index_segments(&index_name, column, segments)
        .await
        .unwrap();

    let segments =
        crate::index::scalar::inverted::load_segments(dataset, column, DocumentGranularity::Row)
            .await
            .unwrap()
            .unwrap();
    assert_eq!(segments.len(), fragment_ids.len());
}

fn compound_multimatch_query() -> FtsQuery {
    MultiMatchQuery::try_new(
        "common".to_owned(),
        vec!["title".to_owned(), "body".to_owned()],
    )
    .unwrap()
    .try_with_boosts(vec![10.0, 1.0])
    .unwrap()
    .into()
}

fn compound_match_query(term: &str, column: &str, boost: f32) -> FtsQuery {
    MatchQuery::new(term.to_owned())
        .with_column(Some(column.to_owned()))
        .with_boost(boost)
        .into()
}

async fn compound_fts_results(
    dataset: &Dataset,
    query: FtsQuery,
    limit: Option<i64>,
) -> Vec<(u64, f32)> {
    let mut scan = dataset.scan();
    scan.with_row_id()
        .full_text_search(FullTextSearchQuery::new_query(query))
        .unwrap();
    if let Some(limit) = limit {
        scan.limit(Some(limit), None).unwrap();
    }
    let batch = scan.try_into_batch().await.unwrap();
    let row_ids = batch[ROW_ID].as_primitive::<UInt64Type>().values();
    let scores = batch[SCORE_COL].as_primitive::<Float32Type>().values();
    row_ids
        .iter()
        .copied()
        .zip(scores.iter().copied())
        .collect()
}

async fn assert_compound_fts_top_k(dataset: &Dataset, query: FtsQuery, limit: usize) {
    let exhaustive = compound_fts_results(dataset, query.clone(), None).await;
    assert!(
        exhaustive.len() > limit,
        "the exhaustive result must contain candidates beyond k"
    );
    let limited = compound_fts_results(dataset, query, Some(limit as i64)).await;
    assert_eq!(limited, exhaustive[..limit]);
}

fn expected_must_score_sum(left: Vec<(u64, f32)>, right: Vec<(u64, f32)>) -> Vec<(u64, f32)> {
    let right = right.into_iter().collect::<HashMap<_, _>>();
    let mut expected = left
        .into_iter()
        .filter_map(|(row_id, left_score)| {
            right
                .get(&row_id)
                .map(|right_score| (row_id, left_score + right_score))
        })
        .collect::<Vec<_>>();
    expected.sort_unstable_by(|(left_row_id, left_score), (right_row_id, right_score)| {
        right_score
            .total_cmp(left_score)
            .then_with(|| left_row_id.cmp(right_row_id))
    });
    expected
}

#[tokio::test]
async fn test_boolean_must_scores_sum_across_execution_paths() {
    let batch = arrow_array::record_batch!(
        (
            "title",
            Utf8,
            [
                "alpha beta delta",
                "alpha alpha beta delta delta",
                "alpha delta",
                "beta delta",
                "alpha beta beta delta delta delta"
            ]
        ),
        (
            "body",
            Utf8,
            ["gamma", "gamma gamma", "gamma", "gamma", "other"]
        )
    )
    .unwrap();
    let schema = batch.schema();
    let mut dataset = Dataset::write(
        RecordBatchIterator::new(vec![batch].into_iter().map(Ok), schema),
        "memory://",
        Some(WriteParams {
            max_rows_per_file: 3,
            ..Default::default()
        }),
    )
    .await
    .unwrap();
    assert_eq!(dataset.get_fragments().len(), 2);
    create_fragmented_fts_index(&mut dataset, "title", false).await;
    create_fragmented_fts_index(&mut dataset, "body", false).await;
    const LIMIT: usize = 2;

    let match_query = |term: &str, column: &str, boost: f32| -> FtsQuery {
        MatchQuery::new(term.to_owned())
            .with_column(Some(column.to_owned()))
            .with_boost(boost)
            .into()
    };

    let same_column_left = match_query("alpha", "title", 2.0);
    let same_column_right = match_query("beta", "title", 3.0);
    let expected = expected_must_score_sum(
        compound_fts_results(&dataset, same_column_left.clone(), None).await,
        compound_fts_results(&dataset, same_column_right.clone(), None).await,
    );
    assert!(expected.len() > LIMIT);
    let same_column_query: FtsQuery = BooleanQuery::new([
        (Occur::Must, same_column_left.clone()),
        (Occur::Must, same_column_right.clone()),
    ])
    .into();
    let actual =
        compound_fts_results(&dataset, same_column_query.clone(), Some(LIMIT as i64)).await;
    assert_eq!(actual, expected[..LIMIT]);
    let reversed_same_column_query: FtsQuery = BooleanQuery::new([
        (Occur::Must, same_column_right),
        (Occur::Must, same_column_left),
    ])
    .into();
    assert_eq!(
        compound_fts_results(&dataset, reversed_same_column_query, Some(LIMIT as i64)).await,
        expected[..LIMIT]
    );

    let nested_left = match_query("alpha", "title", 2.0);
    let nested_middle = match_query("beta", "title", 3.0);
    let nested_right = match_query("delta", "title", 5.0);
    let expected = expected_must_score_sum(
        expected_must_score_sum(
            compound_fts_results(&dataset, nested_left.clone(), None).await,
            compound_fts_results(&dataset, nested_middle.clone(), None).await,
        ),
        compound_fts_results(&dataset, nested_right.clone(), None).await,
    );
    assert!(expected.len() > LIMIT);
    let nested_pair: FtsQuery =
        BooleanQuery::new([(Occur::Must, nested_left), (Occur::Must, nested_middle)]).into();
    let nested_query: FtsQuery =
        BooleanQuery::new([(Occur::Must, nested_pair), (Occur::Must, nested_right)]).into();
    assert_eq!(
        compound_fts_results(&dataset, nested_query, Some(LIMIT as i64)).await,
        expected[..LIMIT]
    );
    let reversed_nested_pair: FtsQuery = BooleanQuery::new([
        (Occur::Must, match_query("beta", "title", 3.0)),
        (Occur::Must, match_query("alpha", "title", 2.0)),
    ])
    .into();
    let reversed_nested_query: FtsQuery = BooleanQuery::new([
        (Occur::Must, match_query("delta", "title", 5.0)),
        (Occur::Must, reversed_nested_pair),
    ])
    .into();
    assert_eq!(
        compound_fts_results(&dataset, reversed_nested_query, Some(LIMIT as i64)).await,
        expected[..LIMIT]
    );

    let mut scanner = dataset.scan();
    scanner
        .full_text_search(FullTextSearchQuery::new_query(same_column_query))
        .unwrap();
    scanner.limit(Some(LIMIT as i64), None).unwrap();
    let plan = scanner.explain_plan(false).await.unwrap();
    assert!(
        plan.contains("CompoundFtsScorer"),
        "same-column MUST should exercise the composable scorer:\n{plan}"
    );

    let cross_column_left = match_query("alpha", "title", 2.0);
    let cross_column_right = match_query("gamma", "body", 3.0);
    let expected = expected_must_score_sum(
        compound_fts_results(&dataset, cross_column_left.clone(), None).await,
        compound_fts_results(&dataset, cross_column_right.clone(), None).await,
    );
    assert!(expected.len() > LIMIT);
    let cross_column_query: FtsQuery = BooleanQuery::new([
        (Occur::Must, cross_column_left.clone()),
        (Occur::Must, cross_column_right.clone()),
    ])
    .into();
    let actual =
        compound_fts_results(&dataset, cross_column_query.clone(), Some(LIMIT as i64)).await;
    assert_eq!(actual, expected[..LIMIT]);
    let reversed_cross_column_query: FtsQuery = BooleanQuery::new([
        (Occur::Must, cross_column_right),
        (Occur::Must, cross_column_left),
    ])
    .into();
    assert_eq!(
        compound_fts_results(&dataset, reversed_cross_column_query, Some(LIMIT as i64)).await,
        expected[..LIMIT]
    );

    let mut scanner = dataset.scan();
    scanner
        .full_text_search(FullTextSearchQuery::new_query(cross_column_query))
        .unwrap();
    scanner.limit(Some(LIMIT as i64), None).unwrap();
    let plan = scanner.explain_plan(false).await.unwrap();
    assert!(
        plan.contains("HashJoinExec"),
        "cross-column MUST should exercise the exact fallback:\n{plan}"
    );
}

#[tokio::test]
async fn test_nested_multimatch_limit_propagation() {
    let batch = arrow_array::record_batch!(
        (
            "title",
            Utf8,
            [
                "common",
                "common filler filler filler filler filler filler filler",
                "irrelevant",
                "common tie",
                "common tie",
                "irrelevant"
            ]
        ),
        (
            "body",
            Utf8,
            [
                "penalty",
                "special",
                "common",
                "neutral",
                "neutral",
                "common filler filler filler penalty"
            ]
        )
    )
    .unwrap();
    let schema = batch.schema();
    let mut dataset = Dataset::write(
        RecordBatchIterator::new(vec![batch].into_iter().map(Ok), schema),
        "memory://",
        Some(WriteParams {
            max_rows_per_file: 2,
            ..Default::default()
        }),
    )
    .await
    .unwrap();
    assert_eq!(dataset.get_fragments().len(), 3);
    create_fragmented_fts_index(&mut dataset, "title", false).await;
    create_fragmented_fts_index(&mut dataset, "body", false).await;

    let must_query: FtsQuery = BooleanQuery::new([
        (Occur::Must, compound_multimatch_query()),
        (
            Occur::Should,
            compound_match_query("special", "body", 100.0),
        ),
    ])
    .into();
    let must_results = compound_fts_results(&dataset, must_query.clone(), None).await;
    assert!(
        must_results
            .windows(2)
            .any(|rows| rows[0].1 == rows[1].1 && rows[0].0 < rows[1].0),
        "the exhaustive result should include a deterministic score tie"
    );
    assert_compound_fts_top_k(&dataset, must_query, 2).await;

    let should_query: FtsQuery = BooleanQuery::new([
        (Occur::Should, compound_multimatch_query()),
        (
            Occur::Should,
            compound_match_query("special", "body", 100.0),
        ),
    ])
    .into();
    assert_compound_fts_top_k(&dataset, should_query.clone(), 2).await;
    let mut fallback_scanner = dataset.scan();
    fallback_scanner
        .with_row_id()
        .full_text_search(FullTextSearchQuery::new_query(should_query))
        .unwrap();
    fallback_scanner.limit(Some(2), None).unwrap();
    let fallback_plan = fallback_scanner.explain_plan(false).await.unwrap();
    assert!(
        fallback_plan.contains("BooleanQuery"),
        "cross-column compound FTS should retain its exact fallback:\n{fallback_plan}"
    );
    assert!(
        !fallback_plan.contains("CompoundFtsScorer"),
        "cross-column compound FTS is not yet supported by the scorer tree:\n{fallback_plan}"
    );

    let boost_query: FtsQuery = BoostQuery::new(
        compound_multimatch_query(),
        compound_match_query("penalty", "body", 100.0),
        Some(1.0),
    )
    .into();
    assert_compound_fts_top_k(&dataset, boost_query, 2).await;

    assert_compound_fts_top_k(&dataset, compound_multimatch_query(), 1).await;
}

#[tokio::test]
async fn test_same_column_compound_scorer_is_exact_and_bounded() {
    let batch = arrow_array::record_batch!((
        "text",
        Utf8,
        [
            "common",
            "common filler filler filler",
            "irrelevant",
            "common tie",
            "common tie",
            "common filler"
        ]
    ))
    .unwrap();
    let schema = batch.schema();
    let mut dataset = Dataset::write(
        RecordBatchIterator::new(vec![batch].into_iter().map(Ok), schema),
        "memory://",
        Some(WriteParams {
            max_rows_per_file: 2,
            ..Default::default()
        }),
    )
    .await
    .unwrap();
    create_fragmented_fts_index(&mut dataset, "text", true).await;

    let match_query = |term: &str| {
        MatchQuery::new(term.to_owned())
            .with_column(Some("text".to_owned()))
            .into()
    };
    let query: FtsQuery = BooleanQuery::new([
        (Occur::Must, match_query("common")),
        (
            Occur::Should,
            BoostQuery::new(match_query("tie"), match_query("filler"), Some(0.5)).into(),
        ),
        (
            Occur::Should,
            PhraseQuery::new("common tie".to_owned())
                .with_column(Some("text".to_owned()))
                .into(),
        ),
        (Occur::MustNot, match_query("irrelevant")),
    ])
    .into();

    assert_compound_fts_top_k(&dataset, query.clone(), 2).await;

    let mut scanner = dataset.scan();
    scanner
        .with_row_id()
        .full_text_search(FullTextSearchQuery::new_query(query))
        .unwrap();
    scanner.limit(Some(2), None).unwrap();
    let plan = scanner.explain_plan(false).await.unwrap();
    assert!(
        plan.contains("CompoundFtsScorer"),
        "same-column compound FTS should use the scorer tree:\n{plan}"
    );
    assert!(
        !plan.contains("HashJoinExec"),
        "same-column compound FTS should not materialize intermediate joins:\n{plan}"
    );

    let same_column_multimatch: FtsQuery = MultiMatchQuery::try_new(
        "common".to_owned(),
        vec!["text".to_owned(), "text".to_owned()],
    )
    .unwrap()
    .try_with_boosts(vec![1.0, 0.5])
    .unwrap()
    .into();
    assert_compound_fts_top_k(&dataset, same_column_multimatch.clone(), 2).await;
    let mut scanner = dataset.scan();
    scanner
        .with_row_id()
        .full_text_search(FullTextSearchQuery::new_query(same_column_multimatch))
        .unwrap();
    scanner.limit(Some(2), None).unwrap();
    let plan = scanner.explain_plan(false).await.unwrap();
    assert!(
        plan.contains("CompoundFtsScorer"),
        "bounded same-column MultiMatch should use posting-backed scorers:\n{plan}"
    );
}

#[tokio::test]
async fn test_compound_tie_uses_resolved_row_id() {
    let batch = arrow_array::record_batch!(("text", Utf8, vec!["common"; 384])).unwrap();
    let schema = batch.schema();
    let mut dataset = Dataset::write(
        RecordBatchIterator::new(vec![batch].into_iter().map(Ok), schema),
        "memory://",
        Some(WriteParams {
            max_rows_per_file: 256,
            ..Default::default()
        }),
    )
    .await
    .unwrap();
    create_fragmented_fts_index_with_order(&mut dataset, "text", false, true).await;

    let query: FtsQuery = MultiMatchQuery::try_new(
        "common".to_owned(),
        vec!["text".to_owned(), "text".to_owned()],
    )
    .unwrap()
    .into();
    let collected_stats = Arc::new(Mutex::new(None::<ExecutionSummaryCounts>));
    let stats_setter = collected_stats.clone();
    let mut scanner = dataset.scan();
    scanner
        .scan_stats_callback(Arc::new(move |stats| {
            *stats_setter.lock().unwrap() = Some(stats.clone());
        }))
        .with_row_id()
        .full_text_search(FullTextSearchQuery::new_query(query.clone()))
        .unwrap();
    scanner.limit(Some(1), None).unwrap();
    let limited = scanner.try_into_batch().await.unwrap();
    let limited_row_id = limited[ROW_ID].as_primitive::<UInt64Type>().value(0);

    let exhaustive = compound_fts_results(&dataset, query.clone(), None).await;
    assert_eq!(limited_row_id, exhaustive[0].0);
    assert_eq!(exhaustive.len(), 384);

    let stats = collected_stats.lock().unwrap().take().unwrap();
    assert_eq!(
        stats.all_counts.get(COMPOUND_SCORE_FLOOR_OVERFLOWS_METRIC),
        Some(&1)
    );
    assert_eq!(
        stats.all_counts.get(COMPOUND_ADDRESSES_RESOLVED_METRIC),
        Some(&384)
    );
    assert_eq!(
        stats
            .all_counts
            .get(COMPOUND_ADDRESS_RESOLUTION_BATCHES_METRIC),
        Some(&1)
    );

    let mut analyze_scanner = dataset.scan();
    analyze_scanner
        .with_row_id()
        .full_text_search(FullTextSearchQuery::new_query(query))
        .unwrap();
    analyze_scanner.limit(Some(1), None).unwrap();
    let analysis = analyze_scanner.analyze_plan().await.unwrap();
    let compound_line = analysis
        .lines()
        .find(|line| line.contains("CompoundFtsScorer"))
        .unwrap();
    assert!(
        compound_line.contains(&format!("{COMPOUND_PEAK_BUFFERED_CANDIDATES_METRIC}=128")),
        "compound FTS metrics missing the bounded candidate peak: {compound_line}"
    );
    assert!(
        compound_line.contains(&format!(
            "{COMPOUND_PEAK_ADDRESS_RESOLUTION_BATCH_SIZE_METRIC}=128"
        )),
        "compound FTS metrics missing the bounded resolution batch: {compound_line}"
    );
}

fn nested_fts_batch(
    ids: Vec<u64>,
    a_values: Vec<Option<&str>>,
    b_values: Vec<Option<&str>>,
) -> RecordBatch {
    let a_values = Arc::new(StringArray::from(a_values)) as ArrayRef;
    let b_values = Arc::new(StringArray::from(b_values)) as ArrayRef;
    let struct_array = StructArray::from(vec![
        (
            Arc::new(Field::new("a", DataType::Utf8, true)),
            a_values.clone(),
        ),
        (
            Arc::new(Field::new("b", DataType::Utf8, true)),
            b_values.clone(),
        ),
    ]);
    let struct_type = struct_array.data_type().clone();
    RecordBatch::try_new(
        Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::UInt64, false),
            Field::new("s", struct_type, true),
        ])),
        vec![
            Arc::new(UInt64Array::from(ids)) as ArrayRef,
            Arc::new(struct_array) as ArrayRef,
        ],
    )
    .unwrap()
}

async fn nested_fts_result_ids(dataset: &Dataset, query: FullTextSearchQuery) -> Vec<u64> {
    let batch = dataset
        .scan()
        .full_text_search(query)
        .unwrap()
        .try_into_batch()
        .await
        .unwrap();
    let mut ids = batch["id"].as_primitive::<UInt64Type>().values().to_vec();
    ids.sort_unstable();
    ids
}

#[tokio::test]
async fn test_fts_on_nested_fields() {
    let batch = nested_fts_batch(
        vec![0, 1, 2, 3],
        vec![
            Some("lance nested alpha"),
            Some("plain text"),
            None,
            Some("phrase target here"),
        ],
        vec![
            Some("metadata only"),
            Some("database nested beta"),
            Some("lance beta"),
            Some("other"),
        ],
    );
    let schema = batch.schema();
    let batches = RecordBatchIterator::new(vec![batch].into_iter().map(Ok), schema);
    let test_uri = TempStrDir::default();
    let mut dataset = Dataset::write(batches, &test_uri, None).await.unwrap();

    dataset
        .create_index(
            &["s.a"],
            IndexType::Inverted,
            None,
            &InvertedIndexParams::default().with_position(true),
            true,
        )
        .await
        .unwrap();
    dataset
        .create_index(
            &["s.b"],
            IndexType::Inverted,
            None,
            &InvertedIndexParams::default(),
            true,
        )
        .await
        .unwrap();

    let indices = dataset.load_indices().await.unwrap();
    let indexed_fields = indices
        .iter()
        .map(|index| dataset.schema().field_path(index.fields[0]).unwrap())
        .collect::<HashSet<_>>();
    assert_eq!(
        indexed_fields,
        HashSet::from(["s.a".to_string(), "s.b".to_string()])
    );

    let query = FullTextSearchQuery::new_query(FtsQuery::Match(
        MatchQuery::new("alpha".to_owned()).with_column(Some("s.a".to_owned())),
    ));
    assert_eq!(nested_fts_result_ids(&dataset, query).await, vec![0]);

    let query = FullTextSearchQuery::new_query(FtsQuery::Match(
        MatchQuery::new("beta".to_owned()).with_column(Some("s.b".to_owned())),
    ));
    assert_eq!(nested_fts_result_ids(&dataset, query).await, vec![1, 2]);

    assert_eq!(
        nested_fts_result_ids(&dataset, FullTextSearchQuery::new("lance".to_owned())).await,
        vec![0, 2]
    );

    let query = FullTextSearchQuery::new_query(FtsQuery::MultiMatch(MultiMatchQuery {
        match_queries: vec![
            MatchQuery::new("nested".to_owned()).with_column(Some("s.a".to_owned())),
            MatchQuery::new("nested".to_owned()).with_column(Some("s.b".to_owned())),
        ],
    }));
    assert_eq!(nested_fts_result_ids(&dataset, query).await, vec![0, 1]);

    let query = FullTextSearchQuery::new_query(
        PhraseQuery::new("phrase target".to_owned())
            .with_column(Some("s.a".to_owned()))
            .into(),
    );
    assert_eq!(nested_fts_result_ids(&dataset, query).await, vec![3]);

    let append_batch = nested_fts_batch(
        vec![4, 5],
        vec![Some("fresh lance append"), Some("plain append")],
        vec![Some("other"), Some("fresh beta append")],
    );
    let schema = append_batch.schema();
    let batches = RecordBatchIterator::new(vec![append_batch].into_iter().map(Ok), schema);
    dataset.append(batches, None).await.unwrap();

    assert_eq!(
        nested_fts_result_ids(&dataset, FullTextSearchQuery::new("fresh".to_owned())).await,
        vec![4, 5]
    );
}

#[tokio::test]
async fn test_fts_unindexed_data() {
    let params = InvertedIndexParams::default();
    let title_col = StringArray::from(vec!["title hello", "title lance", "title common"]);
    let content_col =
        StringArray::from(vec!["content world", "content database", "content common"]);
    let batch = RecordBatch::try_new(
        arrow_schema::Schema::new(vec![
            Field::new("title", title_col.data_type().to_owned(), false),
            Field::new("content", title_col.data_type().to_owned(), false),
        ])
        .into(),
        vec![
            Arc::new(title_col) as ArrayRef,
            Arc::new(content_col) as ArrayRef,
        ],
    )
    .unwrap();
    let schema = batch.schema();
    let batches = RecordBatchIterator::new(vec![batch].into_iter().map(Ok), schema);
    let mut dataset = Dataset::write(batches, "memory://test.lance", None)
        .await
        .unwrap();
    dataset
        .create_index(&["title"], IndexType::Inverted, None, &params, true)
        .await
        .unwrap();

    let results = dataset
        .scan()
        .full_text_search(FullTextSearchQuery::new("title".to_owned()))
        .unwrap()
        .try_into_batch()
        .await
        .unwrap();
    assert_eq!(results.num_rows(), 3);

    // write new data
    let title_col = StringArray::from(vec!["new title"]);
    let content_col = StringArray::from(vec!["new content"]);
    let batch = RecordBatch::try_new(
        arrow_schema::Schema::new(vec![
            Field::new("title", title_col.data_type().to_owned(), false),
            Field::new("content", title_col.data_type().to_owned(), false),
        ])
        .into(),
        vec![
            Arc::new(title_col) as ArrayRef,
            Arc::new(content_col) as ArrayRef,
        ],
    )
    .unwrap();
    let schema = batch.schema();
    let batches = RecordBatchIterator::new(vec![batch].into_iter().map(Ok), schema);
    dataset.append(batches, None).await.unwrap();

    let results = dataset
        .scan()
        .full_text_search(FullTextSearchQuery::new("title".to_owned()))
        .unwrap()
        .try_into_batch()
        .await
        .unwrap();
    assert_eq!(results.num_rows(), 4);

    let results = dataset
        .scan()
        .full_text_search(FullTextSearchQuery::new("new".to_owned()))
        .unwrap()
        .try_into_batch()
        .await
        .unwrap();
    assert_eq!(results.num_rows(), 1);
}

#[tokio::test]
async fn test_fts_v1_remains_queryable_after_append_optimize() {
    let params = InvertedIndexParams::default().format_version(InvertedListFormatVersion::V1);
    let text_col = StringArray::from(vec!["alpha original", "beta original"]);
    let batch = RecordBatch::try_new(
        arrow_schema::Schema::new(vec![Field::new(
            "text",
            text_col.data_type().to_owned(),
            false,
        )])
        .into(),
        vec![Arc::new(text_col) as ArrayRef],
    )
    .unwrap();
    let schema = batch.schema();
    let batches = RecordBatchIterator::new(vec![batch].into_iter().map(Ok), schema);
    let mut dataset = Dataset::write(batches, "memory://test.lance", None)
        .await
        .unwrap();
    dataset
        .create_index(&["text"], IndexType::Inverted, None, &params, true)
        .await
        .unwrap();
    assert_eq!(dataset.load_indices().await.unwrap()[0].index_version, 1);

    let appended = StringArray::from(vec!["alpha appended"]);
    let batch = RecordBatch::try_new(
        arrow_schema::Schema::new(vec![Field::new(
            "text",
            appended.data_type().to_owned(),
            false,
        )])
        .into(),
        vec![Arc::new(appended) as ArrayRef],
    )
    .unwrap();
    let schema = batch.schema();
    let batches = RecordBatchIterator::new(vec![batch].into_iter().map(Ok), schema);
    dataset.append(batches, None).await.unwrap();
    dataset
        .optimize_indices(&OptimizeOptions::append())
        .await
        .unwrap();

    let results = dataset
        .scan()
        .full_text_search(FullTextSearchQuery::new("alpha".to_owned()))
        .unwrap()
        .try_into_batch()
        .await
        .unwrap();
    assert_eq!(results.num_rows(), 2);
    assert!(
        dataset
            .load_indices()
            .await
            .unwrap()
            .iter()
            .all(|index| index.index_version == 1)
    );
}

#[tokio::test]
async fn test_fts_unindexed_data_with_stop_words() {
    // When indexed data has avg_doc_length < 1.0 (e.g. single-word stop words
    // that get filtered), the BM25 scorer must still produce non-zero scores
    // for unindexed rows. Regression test for #5871.
    let params = InvertedIndexParams::default();
    let text_col = StringArray::from(vec!["a", "is", "the", "bug"]);
    let batch = RecordBatch::try_new(
        arrow_schema::Schema::new(vec![Field::new("text", DataType::Utf8, false)]).into(),
        vec![Arc::new(text_col) as ArrayRef],
    )
    .unwrap();
    let schema = batch.schema();
    let batches = RecordBatchIterator::new(vec![batch].into_iter().map(Ok), schema);
    let mut dataset = Dataset::write(batches, "memory://stop_words.lance", None)
        .await
        .unwrap();
    dataset
        .create_index(&["text"], IndexType::Inverted, None, &params, true)
        .await
        .unwrap();

    // Append unindexed rows with a term not in the index
    let unindexed: Vec<String> = (0..10).map(|i| format!("hello_{i}")).collect();
    let text_col = StringArray::from(unindexed);
    let batch = RecordBatch::try_new(
        arrow_schema::Schema::new(vec![Field::new("text", DataType::Utf8, false)]).into(),
        vec![Arc::new(text_col) as ArrayRef],
    )
    .unwrap();
    let schema = batch.schema();
    let batches = RecordBatchIterator::new(vec![batch].into_iter().map(Ok), schema);
    dataset.append(batches, None).await.unwrap();

    let results = dataset
        .scan()
        .full_text_search(FullTextSearchQuery::new("hello".to_owned()))
        .unwrap()
        .try_into_batch()
        .await
        .unwrap();
    assert_eq!(results.num_rows(), 10);
}

#[tokio::test]
async fn test_fts_unindexed_data_on_empty_index() {
    // Empty dataset with fts index
    let params = InvertedIndexParams::default();
    let title_col = StringArray::from(Vec::<&str>::new());
    let content_col = StringArray::from(Vec::<&str>::new());
    let batch = RecordBatch::try_new(
        arrow_schema::Schema::new(vec![
            Field::new("title", title_col.data_type().to_owned(), false),
            Field::new("content", title_col.data_type().to_owned(), false),
        ])
        .into(),
        vec![
            Arc::new(title_col) as ArrayRef,
            Arc::new(content_col) as ArrayRef,
        ],
    )
    .unwrap();
    let schema = batch.schema();
    let batches = RecordBatchIterator::new(vec![batch].into_iter().map(Ok), schema);
    let mut dataset = Dataset::write(batches, "memory://test.lance", None)
        .await
        .unwrap();
    dataset
        .create_index(&["title"], IndexType::Inverted, None, &params, true)
        .await
        .unwrap();

    // Test fts search
    let results = dataset
        .scan()
        .full_text_search(FullTextSearchQuery::new_query(FtsQuery::Match(
            MatchQuery::new("title".to_owned()).with_column(Some("title".to_owned())),
        )))
        .unwrap()
        .try_into_batch()
        .await
        .unwrap();
    assert_eq!(results.num_rows(), 0);

    // write new data
    let title_col = StringArray::from(vec!["title hello", "title lance", "title common"]);
    let content_col =
        StringArray::from(vec!["content world", "content database", "content common"]);
    let batch = RecordBatch::try_new(
        arrow_schema::Schema::new(vec![
            Field::new("title", title_col.data_type().to_owned(), false),
            Field::new("content", title_col.data_type().to_owned(), false),
        ])
        .into(),
        vec![
            Arc::new(title_col) as ArrayRef,
            Arc::new(content_col) as ArrayRef,
        ],
    )
    .unwrap();
    let schema = batch.schema();
    let batches = RecordBatchIterator::new(vec![batch].into_iter().map(Ok), schema);
    dataset.append(batches, None).await.unwrap();

    let results = dataset
        .scan()
        .full_text_search(FullTextSearchQuery::new_query(FtsQuery::Match(
            MatchQuery::new("title".to_owned()).with_column(Some("title".to_owned())),
        )))
        .unwrap()
        .try_into_batch()
        .await
        .unwrap();
    assert_eq!(results.num_rows(), 3);
}

#[tokio::test]
async fn test_fts_without_index() {
    // create table without index
    let title_col = StringArray::from(vec!["title hello", "title lance", "title common"]);
    let content_col =
        StringArray::from(vec!["content world", "content database", "content common"]);
    let batch = RecordBatch::try_new(
        arrow_schema::Schema::new(vec![
            Field::new("title", title_col.data_type().to_owned(), false),
            Field::new("content", title_col.data_type().to_owned(), false),
        ])
        .into(),
        vec![
            Arc::new(title_col) as ArrayRef,
            Arc::new(content_col) as ArrayRef,
        ],
    )
    .unwrap();
    let schema = batch.schema();
    let batches = RecordBatchIterator::new(vec![batch].into_iter().map(Ok), schema);
    let mut dataset = Dataset::write(batches, "memory://test.lance", None)
        .await
        .unwrap();

    // match query on title and content
    let results = dataset
        .scan()
        .full_text_search(
            FullTextSearchQuery::new("title".to_owned())
                .with_columns(&["title".to_string(), "content".to_string()])
                .unwrap(),
        )
        .unwrap()
        .try_into_batch()
        .await
        .unwrap();
    assert_eq!(results.num_rows(), 3);

    // write new data
    let title_col = StringArray::from(vec!["new title"]);
    let content_col = StringArray::from(vec!["new content"]);
    let batch = RecordBatch::try_new(
        arrow_schema::Schema::new(vec![
            Field::new("title", title_col.data_type().to_owned(), false),
            Field::new("content", title_col.data_type().to_owned(), false),
        ])
        .into(),
        vec![
            Arc::new(title_col) as ArrayRef,
            Arc::new(content_col) as ArrayRef,
        ],
    )
    .unwrap();
    let schema = batch.schema();
    let batches = RecordBatchIterator::new(vec![batch].into_iter().map(Ok), schema);
    dataset.append(batches, None).await.unwrap();

    // match query on title and content
    let results = dataset
        .scan()
        .full_text_search(
            FullTextSearchQuery::new("title".to_owned())
                .with_columns(&["title".to_string(), "content".to_string()])
                .unwrap(),
        )
        .unwrap()
        .try_into_batch()
        .await
        .unwrap();
    assert_eq!(results.num_rows(), 4);

    let results = dataset
        .scan()
        .full_text_search(
            FullTextSearchQuery::new("new".to_owned())
                .with_columns(&["title".to_string(), "content".to_string()])
                .unwrap(),
        )
        .unwrap()
        .try_into_batch()
        .await
        .unwrap();
    assert_eq!(results.num_rows(), 1);
}

#[tokio::test]
async fn test_fts_without_index_uses_scalar_index_for_prefilter() {
    // Verify that flat FTS (no inverted index on text) routes its prefilter
    // through `FilteredReadExec` so a scalar index on the filter column is
    // actually used. Six rows with two distinct ids: a prefilter of `id = 1`
    // must match exactly the three text rows tagged with id=1.
    let text = StringArray::from(vec![
        "alpha bravo",
        "charlie delta",
        "alpha echo",
        "foxtrot",
        "alpha golf",
        "hotel india",
    ]);
    let ids = Int32Array::from(vec![1, 1, 1, 2, 2, 2]);
    let batch = RecordBatch::try_new(
        arrow_schema::Schema::new(vec![
            Field::new("text", text.data_type().to_owned(), false),
            Field::new("id", ids.data_type().to_owned(), false),
        ])
        .into(),
        vec![Arc::new(text) as ArrayRef, Arc::new(ids) as ArrayRef],
    )
    .unwrap();
    let schema = batch.schema();
    let batches = RecordBatchIterator::new(vec![batch].into_iter().map(Ok), schema);
    let test_uri = TempStrDir::default();
    let mut dataset = Dataset::write(batches, &test_uri, None).await.unwrap();

    // Scalar index on `id` only — no FTS index on `text`.
    dataset
        .create_index(
            &["id"],
            IndexType::BTree,
            None,
            &ScalarIndexParams::default(),
            true,
        )
        .await
        .unwrap();

    let mut scan = dataset.scan();
    scan.prefilter(true)
        .full_text_search(
            FullTextSearchQuery::new("alpha".to_owned())
                .with_columns(&["text".to_string()])
                .unwrap(),
        )
        .unwrap()
        .filter("id = 1")
        .unwrap();

    let plan = scan.analyze_plan().await.unwrap();
    // The flat-FTS path now reads via `FilteredReadExec` (prints as `LanceRead`)
    // with the prefilter plumbed into it, so the scalar index on `id` is used.
    assert_contains!(&plan, "FlatMatchQuery");
    assert_contains!(&plan, "LanceRead");
    assert_contains!(&plan, "full_filter=id = Int32(1)");
    // The legacy plan ran a `LanceScan` wrapped in a manual `LanceFilterExec`;
    // make sure we did not regress to that shape.
    assert_not_contains!(&plan, "LanceScan:");

    let results = scan.try_into_batch().await.unwrap();
    // Only rows with id=1 AND text matching "alpha": rows 0 ("alpha bravo")
    // and 2 ("alpha echo"). Row 4 ("alpha golf") has id=2 and must be excluded.
    assert_eq!(
        results.num_rows(),
        2,
        "expected the two id=1 rows that match `alpha`, got plan:\n{plan}"
    );
}

#[tokio::test]
async fn test_fts_rank() {
    let params = InvertedIndexParams::default();
    let text_col =
        GenericStringArray::<i32>::from(vec!["score", "find score", "try to find score"]);
    let batch = RecordBatch::try_new(
        arrow_schema::Schema::new(vec![arrow_schema::Field::new(
            "text",
            text_col.data_type().to_owned(),
            false,
        )])
        .into(),
        vec![Arc::new(text_col) as ArrayRef],
    )
    .unwrap();
    let schema = batch.schema();
    let batches = RecordBatchIterator::new(vec![batch].into_iter().map(Ok), schema);
    let test_uri = TempStrDir::default();
    let mut dataset = Dataset::write(batches, &test_uri, None).await.unwrap();
    dataset
        .create_index(&["text"], IndexType::Inverted, None, &params, true)
        .await
        .unwrap();

    let results = dataset
        .scan()
        .with_row_id()
        .full_text_search(FullTextSearchQuery::new("score".to_owned()))
        .unwrap()
        .limit(Some(3), None)
        .unwrap()
        .try_into_batch()
        .await
        .unwrap();
    assert_eq!(results.num_rows(), 3);
    let row_ids = results[ROW_ID].as_primitive::<UInt64Type>().values();
    assert_eq!(row_ids, &[0, 1, 2]);

    let results = dataset
        .scan()
        .with_row_id()
        .full_text_search(FullTextSearchQuery::new("score".to_owned()))
        .unwrap()
        .limit(Some(2), None)
        .unwrap()
        .try_into_batch()
        .await
        .unwrap();
    assert_eq!(results.num_rows(), 2);
    let row_ids = results[ROW_ID].as_primitive::<UInt64Type>().values();
    assert_eq!(row_ids, &[0, 1]);

    let results = dataset
        .scan()
        .with_row_id()
        .full_text_search(FullTextSearchQuery::new("score".to_owned()))
        .unwrap()
        .limit(Some(1), None)
        .unwrap()
        .try_into_batch()
        .await
        .unwrap();
    assert_eq!(results.num_rows(), 1);
    let row_ids = results[ROW_ID].as_primitive::<UInt64Type>().values();
    assert_eq!(row_ids, &[0]);
}

#[tokio::test]
async fn test_fts_unfiltered_after_filtered_returns_real_row_ids() {
    // After a filtered FTS scan populates the per-partition cache,
    // the next unfiltered scan must still return real row_ids, not
    // partition-local doc_ids. Needs >1 fragment so the two differ
    // (fragment N's row_ids start at N << 32).
    let text_col = GenericStringArray::<i32>::from(vec![
        "alpha first",
        "alpha second",
        "alpha third",
        "alpha fourth",
    ]);
    let batch = RecordBatch::try_new(
        arrow_schema::Schema::new(vec![arrow_schema::Field::new(
            "text",
            text_col.data_type().to_owned(),
            false,
        )])
        .into(),
        vec![Arc::new(text_col) as ArrayRef],
    )
    .unwrap();
    let schema = batch.schema();
    let test_uri = TempStrDir::default();
    let mut dataset = Dataset::write(
        RecordBatchIterator::new(vec![batch].into_iter().map(Ok), schema),
        &test_uri,
        Some(WriteParams {
            max_rows_per_file: 1,
            ..Default::default()
        }),
    )
    .await
    .unwrap();
    dataset
        .create_index(
            &["text"],
            IndexType::Inverted,
            None,
            &InvertedIndexParams::default(),
            true,
        )
        .await
        .unwrap();

    let fts = |ds: &Dataset, filter: Option<&str>| {
        let mut s = ds.scan();
        s.with_row_id()
            .full_text_search(FullTextSearchQuery::new("alpha".to_owned()))
            .unwrap();
        if let Some(f) = filter {
            s.prefilter(true).filter(f).unwrap();
        }
        s
    };
    let sorted_row_ids = |b: &RecordBatch| {
        let mut v: Vec<u64> = b[ROW_ID].as_primitive::<UInt64Type>().values().to_vec();
        v.sort();
        v
    };

    let fresh = sorted_row_ids(&fts(&dataset, None).try_into_batch().await.unwrap());
    assert_eq!(fresh.len(), 4);

    // Reopen so the baseline scan's cached LazyDocSet doesn't mask
    // the regression -- the filtered scan needs to be the first
    // thing that touches the DocSet.
    let dataset = Dataset::open(test_uri.as_str()).await.unwrap();
    fts(&dataset, Some("text LIKE 'alpha first%'"))
        .try_into_batch()
        .await
        .unwrap();

    let after = sorted_row_ids(&fts(&dataset, None).try_into_batch().await.unwrap());
    assert_eq!(after, fresh);
}

async fn create_fts_dataset<
    Offset: arrow::array::OffsetSizeTrait,
    ListOffset: arrow::array::OffsetSizeTrait,
>(
    is_list: bool,
    with_position: bool,
    params: InvertedIndexParams,
) -> Dataset {
    let tempdir = TempStrDir::default();
    let uri = tempdir.to_owned();
    drop(tempdir);

    let params = params.with_position(with_position);
    let doc_col: Arc<dyn Array> = if is_list {
        let string_builder = GenericStringBuilder::<Offset>::new();
        let mut list_col = GenericListBuilder::<ListOffset, _>::new(string_builder);
        // Create a list of strings
        list_col.values().append_value("lance database the search"); // for testing phrase query
        list_col.append(true);
        list_col.values().append_value("lance database"); // for testing phrase query
        list_col.append(true);
        list_col.values().append_value("lance search");
        list_col.append(true);
        list_col.values().append_value("database");
        list_col.values().append_value("search");
        list_col.append(true);
        list_col.values().append_value("unrelated doc");
        list_col.append(true);
        list_col.values().append_value("unrelated");
        list_col.append(true);
        list_col.values().append_value("mots");
        list_col.values().append_value("accentués");
        list_col.append(true);
        list_col
            .values()
            .append_value("lance database full text search");
        list_col.append(true);

        // for testing null
        list_col.append(false);

        Arc::new(list_col.finish())
    } else {
        Arc::new(GenericStringArray::<Offset>::from(vec![
            "lance database the search",
            "lance database",
            "lance search",
            "database search",
            "unrelated doc",
            "unrelated",
            "mots accentués",
            "lance database full text search",
        ]))
    };
    let ids = UInt64Array::from_iter_values(0..doc_col.len() as u64);
    let batch = RecordBatch::try_new(
        arrow_schema::Schema::new(vec![
            arrow_schema::Field::new("doc", doc_col.data_type().to_owned(), true),
            arrow_schema::Field::new("id", DataType::UInt64, false),
        ])
        .into(),
        vec![Arc::new(doc_col) as ArrayRef, Arc::new(ids) as ArrayRef],
    )
    .unwrap();
    let schema = batch.schema();
    let batches = RecordBatchIterator::new(vec![batch].into_iter().map(Ok), schema);
    let mut dataset = Dataset::write(batches, &uri, None).await.unwrap();

    dataset
        .create_index(&["doc"], IndexType::Inverted, None, &params, true)
        .await
        .unwrap();

    dataset
}

async fn test_fts_index<
    Offset: arrow::array::OffsetSizeTrait,
    ListOffset: arrow::array::OffsetSizeTrait,
>(
    is_list: bool,
) {
    let ds =
        create_fts_dataset::<Offset, ListOffset>(is_list, false, InvertedIndexParams::default())
            .await;
    let result = ds
        .scan()
        .project(&["id"])
        .unwrap()
        .full_text_search(FullTextSearchQuery::new("lance".to_owned()).limit(Some(3)))
        .unwrap()
        .try_into_batch()
        .await
        .unwrap();
    assert_eq!(result.num_rows(), 3, "{:?}", result);
    let ids = result["id"].as_primitive::<UInt64Type>().values();
    assert!(ids.contains(&0), "{:?}", result);
    assert!(ids.contains(&1), "{:?}", result);
    assert!(ids.contains(&2), "{:?}", result);

    let result = ds
        .scan()
        .project(&["id"])
        .unwrap()
        .full_text_search(FullTextSearchQuery::new("database".to_owned()).limit(Some(3)))
        .unwrap()
        .try_into_batch()
        .await
        .unwrap();
    assert_eq!(result.num_rows(), 3);
    let ids = result["id"].as_primitive::<UInt64Type>().values();
    assert!(ids.contains(&0), "{:?}", result);
    assert!(ids.contains(&1), "{:?}", result);
    assert!(ids.contains(&3), "{:?}", result);

    let result = ds
        .scan()
        .project(&["id"])
        .unwrap()
        .full_text_search(
            FullTextSearchQuery::new_query(
                MatchQuery::new("lance database".to_owned())
                    .with_operator(Operator::And)
                    .into(),
            )
            .limit(Some(5)),
        )
        .unwrap()
        .try_into_batch()
        .await
        .unwrap();
    assert_eq!(result.num_rows(), 3, "{:?}", result);
    let ids = result["id"].as_primitive::<UInt64Type>().values();
    assert!(ids.contains(&0), "{:?}", result);
    assert!(ids.contains(&1), "{:?}", result);
    assert!(ids.contains(&7), "{:?}", result);

    let result = ds
        .scan()
        .project(&["id"])
        .unwrap()
        .full_text_search(FullTextSearchQuery::new("unknown null".to_owned()).limit(Some(3)))
        .unwrap()
        .try_into_batch()
        .await
        .unwrap();
    assert_eq!(result.num_rows(), 0);

    // test phrase query
    // for non-phrasal query, the order of the tokens doesn't matter
    // so there should be 4 documents that contain "database" or "lance"

    // we built the index without position, so the phrase query will not work
    let result = ds
        .scan()
        .project(&["id"])
        .unwrap()
        .full_text_search(
            FullTextSearchQuery::new_query(PhraseQuery::new("lance database".to_owned()).into())
                .limit(Some(10)),
        )
        .unwrap()
        .try_into_batch()
        .await;
    let err = result.unwrap_err().to_string();
    assert!(err.contains("position is not found but required for phrase queries, try recreating the index with position"),"{}",err);

    // recreate the index with position
    let ds =
        create_fts_dataset::<Offset, ListOffset>(is_list, true, InvertedIndexParams::default())
            .await;
    let result = ds
        .scan()
        .project(&["id"])
        .unwrap()
        .full_text_search(FullTextSearchQuery::new("lance database".to_owned()).limit(Some(10)))
        .unwrap()
        .try_into_batch()
        .await
        .unwrap();
    assert_eq!(result.num_rows(), 5, "{:?}", result);
    let ids = result["id"].as_primitive::<UInt64Type>().values();
    assert!(ids.contains(&0));
    assert!(ids.contains(&1));
    assert!(ids.contains(&2));
    assert!(ids.contains(&3));
    assert!(ids.contains(&7));

    let result = ds
        .scan()
        .project(&["id"])
        .unwrap()
        .full_text_search(
            FullTextSearchQuery::new_query(PhraseQuery::new("lance database".to_owned()).into())
                .limit(Some(10)),
        )
        .unwrap()
        .try_into_batch()
        .await
        .unwrap();
    let ids = result["id"].as_primitive::<UInt64Type>().values();
    assert_eq!(result.num_rows(), 3, "{:?}", ids);
    assert!(ids.contains(&0));
    assert!(ids.contains(&1));
    assert!(ids.contains(&7));

    let result = ds
        .scan()
        .project(&["id"])
        .unwrap()
        .full_text_search(
            FullTextSearchQuery::new_query(PhraseQuery::new("database lance".to_owned()).into())
                .limit(Some(10)),
        )
        .unwrap()
        .try_into_batch()
        .await
        .unwrap();
    assert_eq!(result.num_rows(), 0);

    let result = ds
        .scan()
        .project(&["id"])
        .unwrap()
        .full_text_search(
            FullTextSearchQuery::new_query(PhraseQuery::new("lance unknown".to_owned()).into())
                .limit(Some(10)),
        )
        .unwrap()
        .try_into_batch()
        .await
        .unwrap();
    assert_eq!(result.num_rows(), 0);

    let result = ds
        .scan()
        .project(&["id"])
        .unwrap()
        .full_text_search(
            FullTextSearchQuery::new_query(PhraseQuery::new("unknown null".to_owned()).into())
                .limit(Some(3)),
        )
        .unwrap()
        .try_into_batch()
        .await
        .unwrap();
    assert_eq!(result.num_rows(), 0);

    let result = ds
        .scan()
        .project(&["id"])
        .unwrap()
        .full_text_search(
            FullTextSearchQuery::new_query(PhraseQuery::new("lance search".to_owned()).into())
                .limit(Some(3)),
        )
        .unwrap()
        .try_into_batch()
        .await
        .unwrap();
    assert_eq!(result.num_rows(), 1);

    let result = ds
        .scan()
        .project(&["id"])
        .unwrap()
        .full_text_search(
            FullTextSearchQuery::new_query(
                PhraseQuery::new("lance search".to_owned())
                    .with_slop(2)
                    .into(),
            )
            .limit(Some(3)),
        )
        .unwrap()
        .try_into_batch()
        .await
        .unwrap();
    assert_eq!(result.num_rows(), 2);

    let result = ds
        .scan()
        .project(&["id"])
        .unwrap()
        .full_text_search(
            FullTextSearchQuery::new_query(
                PhraseQuery::new("search lance".to_owned())
                    .with_slop(2)
                    .into(),
            )
            .limit(Some(3)),
        )
        .unwrap()
        .try_into_batch()
        .await
        .unwrap();
    assert_eq!(result.num_rows(), 0);

    let result = ds
        .scan()
        .project(&["id"])
        .unwrap()
        .full_text_search(
            // must contain "lance" and "database", and may contain "search"
            FullTextSearchQuery::new_query(
                BooleanQuery::new([
                    (
                        Occur::Should,
                        MatchQuery::new("search".to_owned())
                            .with_operator(Operator::And)
                            .into(),
                    ),
                    (
                        Occur::Must,
                        MatchQuery::new("lance database".to_owned())
                            .with_operator(Operator::And)
                            .into(),
                    ),
                ])
                .into(),
            )
            .limit(Some(3)),
        )
        .unwrap()
        .try_into_batch()
        .await
        .unwrap();
    assert_eq!(result.num_rows(), 3, "{:?}", result);
    let ids = result["id"].as_primitive::<UInt64Type>().values();
    assert!(ids.contains(&0), "{:?}", result);
    assert!(ids.contains(&1), "{:?}", result);
    assert!(ids.contains(&7), "{:?}", result);

    let result = ds
        .scan()
        .project(&["id"])
        .unwrap()
        .full_text_search(
            // must contain "lance" and "database", and may contain "search"
            FullTextSearchQuery::new_query(
                BooleanQuery::new([
                    (
                        Occur::Should,
                        MatchQuery::new("search".to_owned())
                            .with_operator(Operator::And)
                            .into(),
                    ),
                    (
                        Occur::Must,
                        MatchQuery::new("lance database".to_owned())
                            .with_operator(Operator::And)
                            .into(),
                    ),
                    (
                        Occur::MustNot,
                        MatchQuery::new("full text".to_owned()).into(),
                    ),
                ])
                .into(),
            )
            .limit(Some(3)),
        )
        .unwrap()
        .try_into_batch()
        .await
        .unwrap();
    assert_eq!(result.num_rows(), 2, "{:?}", result);
    let ids = result["id"].as_primitive::<UInt64Type>().values();
    assert!(ids.contains(&0), "{:?}", result);
    assert!(ids.contains(&1), "{:?}", result);
}

#[tokio::test]
async fn test_fts_index_with_string() {
    test_fts_index::<i32, i32>(false).await;
    test_fts_index::<i32, i32>(true).await;
    test_fts_index::<i32, i64>(true).await;
}

#[tokio::test]
async fn test_fts_index_with_large_string() {
    test_fts_index::<i64, i32>(false).await;
    test_fts_index::<i64, i32>(true).await;
    test_fts_index::<i64, i64>(true).await;
}

#[tokio::test]
async fn test_fts_list_index_uses_row_level_documents() {
    let tempdir = TempStrDir::default();
    let uri = tempdir.to_owned();
    drop(tempdir);

    let mut list_col = GenericListBuilder::<i32, _>::new(GenericStringBuilder::<i32>::new());
    list_col.values().append_value("lance");
    list_col.values().append_value("lance database");
    list_col.append(true);
    list_col.values().append_value("database");
    list_col.append(true);
    list_col.append(true);
    list_col.values().append_null();
    list_col.append(true);
    list_col.append(false);

    let docs = Arc::new(list_col.finish()) as ArrayRef;
    let ids = Arc::new(UInt64Array::from_iter_values(0..docs.len() as u64)) as ArrayRef;
    let batch = RecordBatch::try_new(
        Arc::new(ArrowSchema::new(vec![
            ArrowField::new("doc", docs.data_type().clone(), true),
            ArrowField::new("id", DataType::UInt64, false),
        ])),
        vec![docs, ids],
    )
    .unwrap();
    let batches = RecordBatchIterator::new(vec![Ok(batch.clone())], batch.schema());
    let mut dataset = Dataset::write(batches, &uri, None).await.unwrap();

    dataset
        .create_index(
            &["doc"],
            IndexType::Inverted,
            None,
            &InvertedIndexParams::default(),
            true,
        )
        .await
        .unwrap();

    let result = dataset
        .scan()
        .project(&["id"])
        .unwrap()
        .full_text_search(FullTextSearchQuery::new("lance".to_owned()).limit(Some(10)))
        .unwrap()
        .try_into_batch()
        .await
        .unwrap();
    assert_eq!(result["id"].as_primitive::<UInt64Type>().values(), &[0]);

    let result = dataset
        .scan()
        .project(&["id"])
        .unwrap()
        .full_text_search(FullTextSearchQuery::new("database".to_owned()).limit(Some(10)))
        .unwrap()
        .try_into_batch()
        .await
        .unwrap();
    let mut ids = result["id"]
        .as_primitive::<UInt64Type>()
        .values()
        .iter()
        .copied()
        .collect::<Vec<_>>();
    ids.sort_unstable();
    assert_eq!(ids, vec![0, 1], "{:?}", result);
}

#[tokio::test]
async fn test_fts_list_phrase_query_can_cross_elements() {
    assert_fts_list_phrase_query_can_cross_elements::<i32>().await;
}

#[tokio::test]
async fn test_fts_large_list_phrase_query_can_cross_elements() {
    assert_fts_list_phrase_query_can_cross_elements::<i64>().await;
}

async fn assert_fts_list_phrase_query_can_cross_elements<Offset: arrow::array::OffsetSizeTrait>() {
    let tempdir = TempStrDir::default();
    let uri = tempdir.to_owned();
    drop(tempdir);

    let mut list_col = GenericListBuilder::<Offset, _>::new(GenericStringBuilder::<Offset>::new());
    let rows: &[&[&str]] = &[
        &["alpha", "beta"],
        &["want the", "apple"],
        &["want", "apple"],
    ];
    for values in rows.iter().copied() {
        for value in values {
            list_col.values().append_value(value);
        }
        list_col.append(true);
    }

    let docs = Arc::new(list_col.finish()) as ArrayRef;
    let ids = Arc::new(UInt64Array::from(vec![0u64, 1, 2])) as ArrayRef;
    let batch = RecordBatch::try_new(
        Arc::new(ArrowSchema::new(vec![
            ArrowField::new("doc", docs.data_type().clone(), true),
            ArrowField::new("id", DataType::UInt64, false),
        ])),
        vec![docs, ids],
    )
    .unwrap();
    let batches = RecordBatchIterator::new(vec![Ok(batch.clone())], batch.schema());
    let mut dataset = Dataset::write(batches, &uri, None).await.unwrap();

    let cases: [(&str, &[u64]); 3] = [
        ("alpha beta", &[0]),
        ("want the apple", &[1]),
        ("want apple", &[2]),
    ];
    let mut flat_results = Vec::with_capacity(cases.len());
    for (terms, expected) in cases {
        let result = dataset
            .scan()
            .project(&["id"])
            .unwrap()
            .full_text_search(
                FullTextSearchQuery::new_query(
                    PhraseQuery::new(terms.to_owned())
                        .with_column(Some("doc".to_owned()))
                        .into(),
                )
                .limit(Some(10)),
            )
            .unwrap()
            .try_into_batch()
            .await
            .unwrap();
        assert_eq!(result["id"].as_primitive::<UInt64Type>().values(), expected);
        flat_results.push(result);
    }

    let params = InvertedIndexParams::default()
        .with_position(true)
        .remove_stop_words(true);
    dataset
        .create_index(&["doc"], IndexType::Inverted, None, &params, true)
        .await
        .unwrap();

    for ((terms, expected), flat_result) in cases.into_iter().zip(flat_results) {
        let indexed_result = dataset
            .scan()
            .project(&["id"])
            .unwrap()
            .full_text_search(
                FullTextSearchQuery::new_query(
                    PhraseQuery::new(terms.to_owned())
                        .with_column(Some("doc".to_owned()))
                        .into(),
                )
                .limit(Some(10)),
            )
            .unwrap()
            .try_into_batch()
            .await
            .unwrap();
        assert_eq!(
            indexed_result["id"].as_primitive::<UInt64Type>().values(),
            expected
        );
        assert_eq!(
            indexed_result["id"].as_primitive::<UInt64Type>().values(),
            flat_result["id"].as_primitive::<UInt64Type>().values(),
            "query={terms}"
        );
    }
}

#[tokio::test]
async fn test_fts_accented_chars() {
    let ds = create_fts_dataset::<i32, i32>(false, false, InvertedIndexParams::default()).await;
    let result = ds
        .scan()
        .project(&["id"])
        .unwrap()
        .full_text_search(FullTextSearchQuery::new("accentués".to_owned()).limit(Some(3)))
        .unwrap()
        .try_into_batch()
        .await
        .unwrap();
    assert_eq!(result.num_rows(), 1);

    let result = ds
        .scan()
        .project(&["id"])
        .unwrap()
        .full_text_search(FullTextSearchQuery::new("accentues".to_owned()).limit(Some(3)))
        .unwrap()
        .try_into_batch()
        .await
        .unwrap();
    assert_eq!(result.num_rows(), 0);

    // with ascii folding enabled, the search should be accent-insensitive
    let ds = create_fts_dataset::<i32, i32>(
        false,
        false,
        InvertedIndexParams::default()
            .stem(false)
            .ascii_folding(true),
    )
    .await;
    let result = ds
        .scan()
        .project(&["id"])
        .unwrap()
        .full_text_search(FullTextSearchQuery::new("accentués".to_owned()).limit(Some(3)))
        .unwrap()
        .try_into_batch()
        .await
        .unwrap();
    assert_eq!(result.num_rows(), 1);

    let result = ds
        .scan()
        .project(&["id"])
        .unwrap()
        .full_text_search(FullTextSearchQuery::new("accentues".to_owned()).limit(Some(3)))
        .unwrap()
        .try_into_batch()
        .await
        .unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[tokio::test]
async fn test_fts_phrase_query() {
    let tmpdir = TempStrDir::default();
    let uri = tmpdir.to_owned();
    drop(tmpdir);

    let words = ["lance", "full", "text", "search"];
    let mut lance_search_count = 0;
    let mut full_text_count = 0;
    let mut doc_array = (0..4096)
        .map(|_| {
            let mut rng = rand::rng();
            let mut text = String::with_capacity(512);
            let len = rng.random_range(127..512);
            for i in 0..len {
                if i > 0 {
                    text.push(' ');
                }
                text.push_str(words[rng.random_range(0..words.len())]);
            }
            if text.contains("lance search") {
                lance_search_count += 1;
            }
            if text.contains("full text") {
                full_text_count += 1;
            }
            text
        })
        .collect_vec();
    // Ensure at least one doc matches each phrase deterministically
    doc_array.push("lance search".to_owned());
    lance_search_count += 1;
    doc_array.push("full text".to_owned());
    full_text_count += 1;
    doc_array.push("position for phrase query".to_owned());

    // 1) Build index without positions and assert phrase query errors
    let params_no_pos = InvertedIndexParams::default().with_position(false);
    let doc_col: Arc<dyn Array> = Arc::new(GenericStringArray::<i32>::from(doc_array.clone()));
    let ids = UInt64Array::from_iter_values(0..doc_col.len() as u64);
    let batch = RecordBatch::try_new(
        arrow_schema::Schema::new(vec![
            arrow_schema::Field::new("doc", doc_col.data_type().to_owned(), true),
            arrow_schema::Field::new("id", DataType::UInt64, false),
        ])
        .into(),
        vec![Arc::new(doc_col) as ArrayRef, Arc::new(ids) as ArrayRef],
    )
    .unwrap();
    let schema = batch.schema();
    let batches = RecordBatchIterator::new(vec![batch].into_iter().map(Ok), schema);
    let mut dataset = Dataset::write(batches, &uri, None).await.unwrap();
    dataset
        .create_index(&["doc"], IndexType::Inverted, None, &params_no_pos, true)
        .await
        .unwrap();

    let err = dataset
        .scan()
        .project(&["id"])
        .unwrap()
        .full_text_search(FullTextSearchQuery::new_query(
            PhraseQuery::new("lance search".to_owned()).into(),
        ))
        .unwrap()
        .try_into_batch()
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("position is not found but required for phrase queries, try recreating the index with position"), "{}", err);
    assert!(err.starts_with("Invalid user input: "), "{}", err);

    // 2) Recreate index with positions and assert phrase query works
    let params_with_pos = InvertedIndexParams::default().with_position(true);
    dataset
        .create_index(&["doc"], IndexType::Inverted, None, &params_with_pos, true)
        .await
        .unwrap();

    let result = dataset
        .scan()
        .project(&["id"])
        .unwrap()
        .full_text_search(FullTextSearchQuery::new_query(
            PhraseQuery::new("lance search".to_owned()).into(),
        ))
        .unwrap()
        .try_into_batch()
        .await
        .unwrap();
    assert_eq!(result.num_rows(), lance_search_count);

    let result = dataset
        .scan()
        .project(&["id"])
        .unwrap()
        .full_text_search(FullTextSearchQuery::new_query(
            PhraseQuery::new("full text".to_owned()).into(),
        ))
        .unwrap()
        .try_into_batch()
        .await
        .unwrap();
    assert_eq!(result.num_rows(), full_text_count);

    let result = dataset
        .scan()
        .project(&["id"])
        .unwrap()
        .full_text_search(FullTextSearchQuery::new_query(
            PhraseQuery::new("phrase query".to_owned()).into(),
        ))
        .unwrap()
        .try_into_batch()
        .await
        .unwrap();
    assert_eq!(result.num_rows(), 1);

    let result = dataset
        .scan()
        .project(&["id"])
        .unwrap()
        .full_text_search(FullTextSearchQuery::new_query(
            PhraseQuery::new("".to_owned()).into(),
        ))
        .unwrap()
        .try_into_batch()
        .await
        .unwrap();
    assert_eq!(result.num_rows(), 0);
}

async fn open_dataset_with_fresh_session(uri: &str) -> Dataset {
    DatasetBuilder::from_uri(uri)
        .with_session(Arc::new(Session::new(1 << 20, 1 << 20, Default::default())))
        .load()
        .await
        .unwrap()
}

#[tokio::test]
async fn test_fts_prewarm_with_position_controls_phrase_query_cache() {
    let tmpdir = TempStrDir::default();
    let uri = tmpdir.to_owned();
    drop(tmpdir);

    let doc_col: Arc<dyn Array> = Arc::new(GenericStringArray::<i32>::from(vec![
        "lance search",
        "lance search with tail",
        "phrase query",
    ]));
    let ids = UInt64Array::from_iter_values(0..doc_col.len() as u64);
    let batch = RecordBatch::try_new(
        arrow_schema::Schema::new(vec![
            arrow_schema::Field::new("doc", doc_col.data_type().to_owned(), true),
            arrow_schema::Field::new("id", DataType::UInt64, false),
        ])
        .into(),
        vec![Arc::new(doc_col) as ArrayRef, Arc::new(ids) as ArrayRef],
    )
    .unwrap();
    let schema = batch.schema();
    let batches = RecordBatchIterator::new(vec![batch].into_iter().map(Ok), schema);
    let mut dataset = Dataset::write(batches, &uri, None).await.unwrap();
    dataset
        .create_index(
            &["doc"],
            IndexType::Inverted,
            Some("fts_idx".to_owned()),
            &InvertedIndexParams::default().with_position(true),
            true,
        )
        .await
        .unwrap();

    let dataset = open_dataset_with_fresh_session(&uri).await;
    dataset.prewarm_index("fts_idx").await.unwrap();
    let cache_entries_after_prewarm = dataset.index_cache_entry_count().await;
    let result = dataset
        .scan()
        .project(&["id"])
        .unwrap()
        .full_text_search(FullTextSearchQuery::new_query(
            PhraseQuery::new("lance search".to_owned()).into(),
        ))
        .unwrap()
        .try_into_batch()
        .await
        .unwrap();
    assert_eq!(result.num_rows(), 2);
    let cache_entries_after_query = dataset.index_cache_entry_count().await;
    assert!(
        cache_entries_after_query > cache_entries_after_prewarm,
        "phrase query should populate positions cache when prewarm skipped positions"
    );

    let dataset = open_dataset_with_fresh_session(&uri).await;
    dataset
        .prewarm_index_with_options(
            "fts_idx",
            &PrewarmOptions::Fts(FtsPrewarmOptions::new().with_position(true)),
        )
        .await
        .unwrap();
    let cache_entries_after_prewarm = dataset.index_cache_entry_count().await;
    let result = dataset
        .scan()
        .project(&["id"])
        .unwrap()
        .full_text_search(FullTextSearchQuery::new_query(
            PhraseQuery::new("lance search".to_owned()).into(),
        ))
        .unwrap()
        .try_into_batch()
        .await
        .unwrap();
    assert_eq!(result.num_rows(), 2);
    let cache_entries_after_query = dataset.index_cache_entry_count().await;
    assert_eq!(
        cache_entries_after_query, cache_entries_after_prewarm,
        "phrase query should not add cache entries after prewarming positions"
    );
}

#[tokio::test]
async fn test_prewarm_index_with_position_validation() {
    let tmpdir = TempStrDir::default();
    let uri = tmpdir.to_owned();
    drop(tmpdir);

    let doc_col: Arc<dyn Array> = Arc::new(GenericStringArray::<i32>::from(vec![
        "lance search",
        "phrase query",
    ]));
    let ids = UInt64Array::from_iter_values(0..doc_col.len() as u64);
    let batch = RecordBatch::try_new(
        arrow_schema::Schema::new(vec![
            arrow_schema::Field::new("doc", doc_col.data_type().to_owned(), true),
            arrow_schema::Field::new("id", DataType::UInt64, false),
        ])
        .into(),
        vec![Arc::new(doc_col) as ArrayRef, Arc::new(ids) as ArrayRef],
    )
    .unwrap();
    let schema = batch.schema();
    let batches = RecordBatchIterator::new(vec![batch].into_iter().map(Ok), schema);
    let mut dataset = Dataset::write(batches, &uri, None).await.unwrap();
    dataset
        .create_index(
            &["doc"],
            IndexType::Inverted,
            Some("fts_idx".to_owned()),
            &InvertedIndexParams::default().with_position(false),
            true,
        )
        .await
        .unwrap();

    let dataset = open_dataset_with_fresh_session(&uri).await;
    let err = dataset
        .prewarm_index_with_options(
            "fts_idx",
            &PrewarmOptions::Fts(FtsPrewarmOptions::new().with_position(true)),
        )
        .await
        .unwrap_err()
        .to_string();
    assert_contains!(
        err,
        "cannot prewarm positions for an inverted index that was built without positions"
    );

    let tmpdir = TempStrDir::default();
    let uri = tmpdir.to_owned();
    drop(tmpdir);

    let batch = RecordBatch::try_from_iter(vec![(
        "id",
        Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef,
    )])
    .unwrap();
    let schema = batch.schema();
    let batches = RecordBatchIterator::new(vec![batch].into_iter().map(Ok), schema);
    let mut dataset = Dataset::write(batches, &uri, None).await.unwrap();
    dataset
        .create_index(
            &["id"],
            IndexType::BTree,
            Some("id_idx".to_owned()),
            &ScalarIndexParams::default(),
            true,
        )
        .await
        .unwrap();

    let dataset = open_dataset_with_fresh_session(&uri).await;
    let err = dataset
        .prewarm_index_with_options("id_idx", &PrewarmOptions::Fts(FtsPrewarmOptions::default()))
        .await
        .unwrap_err()
        .to_string();
    assert_contains!(
        err,
        "FTS prewarm options are only supported for inverted indices"
    );
}

/// Validates the OSS-741 contract: after FTS prewarm through a serializing
/// cache backend, FTS queries serve results without any further IO. The
/// serializing backend forces every cache hit through the new
/// `CacheCodec` impls, so this also smoke-tests the round-trip path under
/// realistic data shapes (compressed posting blocks + shared position
/// stream when positions are enabled).
#[tokio::test]
async fn test_fts_prewarm_with_serializing_backend_serves_query_with_no_io() {
    use lance_io::assert_io_eq;

    use crate::utils::test::serializing_cache::SerializingCacheBackend;

    let tmpdir = TempStrDir::default();
    let uri = tmpdir.to_owned();
    drop(tmpdir);

    let doc_col: Arc<dyn Array> = Arc::new(GenericStringArray::<i32>::from(vec![
        "lance search engine",
        "lance search with tail",
        "phrase query example",
        "search query terms",
    ]));
    let ids = UInt64Array::from_iter_values(0..doc_col.len() as u64);
    let batch = RecordBatch::try_new(
        arrow_schema::Schema::new(vec![
            arrow_schema::Field::new("doc", doc_col.data_type().to_owned(), true),
            arrow_schema::Field::new("id", DataType::UInt64, false),
        ])
        .into(),
        vec![Arc::new(doc_col) as ArrayRef, Arc::new(ids) as ArrayRef],
    )
    .unwrap();
    let schema = batch.schema();
    let batches = RecordBatchIterator::new(vec![batch].into_iter().map(Ok), schema);
    let mut dataset = Dataset::write(batches, &uri, None).await.unwrap();
    dataset
        .create_index(
            &["doc"],
            IndexType::Inverted,
            Some("fts_idx".to_owned()),
            &InvertedIndexParams::default().with_position(true),
            true,
        )
        .await
        .unwrap();

    // Re-open the dataset on a session whose cache backend serializes every
    // entry through its codec. Set a generous capacity so nothing is evicted
    // before we query.
    let backend = Arc::new(SerializingCacheBackend::new());
    let session = Arc::new(Session::with_index_cache_backend(
        backend.clone(),
        128 * 1024 * 1024,
        Arc::new(lance_io::object_store::ObjectStoreRegistry::default()),
    ));
    let dataset = DatasetBuilder::from_uri(&uri)
        .with_session(session)
        .load()
        .await
        .unwrap();

    // Reset IO counters to isolate prewarm + query traffic from open/load.
    dataset.object_store.as_ref().io_stats_incremental();

    dataset
        .prewarm_index_with_options(
            "fts_idx",
            &PrewarmOptions::Fts(FtsPrewarmOptions::new().with_position(true)),
        )
        .await
        .unwrap();

    // The FTS codec must have been exercised. Posting lists and positions
    // enter the serialized store; non-FTS entries (e.g. the unsized
    // `ScalarIndexCacheKey` for the index itself) legitimately fall through
    // to the in-memory passthrough — those cannot have a codec by design.
    let serialized_after_prewarm = backend.serialized_entry_count().await;
    assert!(
        serialized_after_prewarm > 0,
        "prewarm should have routed FTS entries (PostingList / Positions) through CacheCodec, \
         but the serializing store was empty"
    );

    // After prewarm, a phrase query (which exercises both posting lists and
    // positions, deserializing them from bytes via the codec) must not hit
    // disk.
    dataset.object_store.as_ref().io_stats_incremental();

    // Project `_rowid` so the scan does not need to read a data column from
    // the dataset's parquet/lance files; the index path alone determines
    // whether the FTS cache is doing its job.
    let result = dataset
        .scan()
        .project(&[ROW_ID])
        .unwrap()
        .full_text_search(FullTextSearchQuery::new_query(
            PhraseQuery::new("lance search".to_owned()).into(),
        ))
        .unwrap()
        .try_into_batch()
        .await
        .unwrap();
    assert_eq!(
        result.num_rows(),
        2,
        "phrase query should still return correct results after deserialization"
    );

    let stats = dataset.object_store.as_ref().io_stats_incremental();
    assert_io_eq!(
        stats,
        read_iops,
        0,
        "FTS query should not perform IO after prewarm; the serializing cache \
         backend must serve every posting list and positions entry from memory"
    );
}

/// BTree analogue of `test_fts_prewarm_with_serializing_backend_serves_query_with_no_io`:
/// after prewarming a BTree scalar index through a serializing cache backend,
/// an indexed-filter query serves results without any further IO. The
/// serializing backend forces every cache hit through the `BTreeIndexState`
/// and `FlatIndex` `CacheCodec` impls, so this also smoke-tests those
/// round-trip paths on a multi-page index.
#[tokio::test]
async fn test_btree_prewarm_with_serializing_backend_serves_query_with_no_io() {
    use lance_io::assert_io_eq;

    use crate::utils::test::serializing_cache::SerializingCacheBackend;

    let tmpdir = TempStrDir::default();
    let uri = tmpdir.to_owned();
    drop(tmpdir);

    // Enough rows to span several BTree pages (default page size is 4096) so
    // the query has to consult more than one cached `FlatIndex`.
    let num_rows = 16_384;
    let values = Int32Array::from_iter_values(0..num_rows);
    let ids = UInt64Array::from_iter_values(0..num_rows as u64);
    let batch = RecordBatch::try_new(
        arrow_schema::Schema::new(vec![
            arrow_schema::Field::new("value", DataType::Int32, false),
            arrow_schema::Field::new("id", DataType::UInt64, false),
        ])
        .into(),
        vec![Arc::new(values) as ArrayRef, Arc::new(ids) as ArrayRef],
    )
    .unwrap();
    let schema = batch.schema();
    let batches = RecordBatchIterator::new(vec![batch].into_iter().map(Ok), schema);
    let mut dataset = Dataset::write(batches, &uri, None).await.unwrap();
    dataset
        .create_index(
            &["value"],
            IndexType::BTree,
            Some("value_idx".to_owned()),
            &ScalarIndexParams::default(),
            true,
        )
        .await
        .unwrap();

    // Re-open on a session whose cache backend serializes every entry through
    // its codec, with a generous capacity so nothing is evicted before we query.
    let backend = Arc::new(SerializingCacheBackend::new());
    let session = Arc::new(Session::with_index_cache_backend(
        backend.clone(),
        128 * 1024 * 1024,
        Arc::new(lance_io::object_store::ObjectStoreRegistry::default()),
    ));
    let dataset = DatasetBuilder::from_uri(&uri)
        .with_session(session)
        .load()
        .await
        .unwrap();

    // Reset IO counters to isolate prewarm + query traffic from open/load.
    dataset.object_store.as_ref().io_stats_incremental();

    dataset.prewarm_index("value_idx").await.unwrap();

    // Prewarm opens the index (serializing `BTreeIndexState`) and loads every
    // page (serializing each `FlatIndex`), so the serialized store must be
    // non-empty. The unsized fallback keys cannot have a codec by design.
    let serialized_after_prewarm = backend.serialized_entry_count().await;
    assert!(
        serialized_after_prewarm > 0,
        "prewarm should have routed the BTree state and pages through CacheCodec, \
         but the serializing store was empty"
    );

    drop(dataset);
    let backend = Arc::new(backend.restart());
    assert_eq!(
        backend.l1_entry_count().await,
        0,
        "restarting must discard the in-memory L1"
    );
    assert_eq!(
        backend.serialized_entry_count().await,
        serialized_after_prewarm,
        "restarting must retain only the serialized entries"
    );
    let session = Arc::new(Session::with_index_cache_backend(
        backend,
        128 * 1024 * 1024,
        Arc::new(lance_io::object_store::ObjectStoreRegistry::default()),
    ));
    let dataset = DatasetBuilder::from_uri(&uri)
        .with_session(session)
        .load()
        .await
        .unwrap();

    // After recreating the backend, an indexed-filter query must reconstruct
    // the index and every page it touches from serialized bytes, with no disk
    // IO. Project only `_rowid` so the scan does not read a data column.
    dataset.object_store.as_ref().io_stats_incremental();

    let result = dataset
        .scan()
        .project(&[ROW_ID])
        .unwrap()
        .filter("value >= 100 AND value < 200")
        .unwrap()
        .try_into_batch()
        .await
        .unwrap();
    assert_eq!(
        result.num_rows(),
        100,
        "indexed filter should still return correct results after deserialization"
    );

    let stats = dataset.object_store.as_ref().io_stats_incremental();
    assert_io_eq!(
        stats,
        read_iops,
        0,
        "BTree filter query should not perform IO after prewarm; the serializing \
         cache backend must serve the index state and every page from memory"
    );
}

/// Bitmap analogue of `test_btree_prewarm_with_serializing_backend_serves_query_with_no_io`:
/// after prewarming a Bitmap scalar index through a serializing cache backend,
/// an indexed-filter query serves results without any further IO. The
/// serializing backend forces every cache hit through the `BitmapIndexState`
/// (top-level state) and `RowAddrTreeMap` (per-value bitmap) `CacheCodec`
/// impls, so this exercises both round-trip paths.
#[tokio::test]
async fn test_bitmap_prewarm_with_serializing_backend_serves_query_with_no_io() {
    use lance_io::assert_io_eq;

    use crate::utils::test::serializing_cache::SerializingCacheBackend;

    let tmpdir = TempStrDir::default();
    let uri = tmpdir.to_owned();
    drop(tmpdir);

    // Low-cardinality column so the index has several per-value bitmaps to
    // round-trip through the per-key codec.
    let num_rows: i32 = 8_000;
    let values = Int32Array::from_iter_values((0..num_rows).map(|i| i % 16));
    let ids = UInt64Array::from_iter_values(0..num_rows as u64);
    let batch = RecordBatch::try_new(
        arrow_schema::Schema::new(vec![
            arrow_schema::Field::new("value", DataType::Int32, false),
            arrow_schema::Field::new("id", DataType::UInt64, false),
        ])
        .into(),
        vec![Arc::new(values) as ArrayRef, Arc::new(ids) as ArrayRef],
    )
    .unwrap();
    let schema = batch.schema();
    let batches = RecordBatchIterator::new(vec![batch].into_iter().map(Ok), schema);
    let mut dataset = Dataset::write(batches, &uri, None).await.unwrap();
    dataset
        .create_index(
            &["value"],
            IndexType::Bitmap,
            Some("value_idx".to_owned()),
            &ScalarIndexParams::default(),
            true,
        )
        .await
        .unwrap();

    let backend = Arc::new(SerializingCacheBackend::new());
    let session = Arc::new(Session::with_index_cache_backend(
        backend.clone(),
        128 * 1024 * 1024,
        Arc::new(lance_io::object_store::ObjectStoreRegistry::default()),
    ));
    let dataset = DatasetBuilder::from_uri(&uri)
        .with_session(session)
        .load()
        .await
        .unwrap();

    dataset.object_store.as_ref().io_stats_incremental();
    dataset.prewarm_index("value_idx").await.unwrap();

    let serialized_after_prewarm = backend.serialized_entry_count().await;
    assert!(
        serialized_after_prewarm > 0,
        "prewarm should have routed the bitmap state and per-value bitmaps through \
         CacheCodec, but the serializing store was empty"
    );

    dataset.object_store.as_ref().io_stats_incremental();
    let result = dataset
        .scan()
        .project(&[ROW_ID])
        .unwrap()
        .filter("value = 7")
        .unwrap()
        .try_into_batch()
        .await
        .unwrap();
    let expected = (num_rows as usize) / 16;
    assert_eq!(
        result.num_rows(),
        expected,
        "indexed bitmap filter should return correct results after deserialization"
    );

    let stats = dataset.object_store.as_ref().io_stats_incremental();
    assert_io_eq!(
        stats,
        read_iops,
        0,
        "Bitmap filter query should not perform IO after prewarm; the serializing \
         cache backend must serve the index state and every per-value bitmap from memory"
    );
}

#[rstest]
#[case::list(false)]
#[case::large_list(true)]
#[tokio::test]
async fn test_label_list_index_types(#[case] large_list: bool) {
    let test_uri = TempStrDir::default();
    let label_values = vec![
        Some(vec![Some(1), Some(2)]),
        Some(vec![Some(2)]),
        Some(vec![Some(1)]),
        Some(vec![Some(3)]),
    ];
    let labels: ArrayRef = if large_list {
        Arc::new(LargeListArray::from_iter_primitive::<Int64Type, _, _>(
            label_values,
        ))
    } else {
        Arc::new(ListArray::from_iter_primitive::<Int64Type, _, _>(
            label_values,
        ))
    };
    let schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("id", DataType::Int32, false),
        ArrowField::new("labels", labels.data_type().clone(), true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int32Array::from(vec![0, 1, 2, 3])), labels],
    )
    .unwrap();
    let reader = RecordBatchIterator::new(vec![Ok(batch)], schema);
    let mut dataset = Dataset::write(reader, &test_uri, None).await.unwrap();

    let expected = dataset
        .scan()
        .project(&["id"])
        .unwrap()
        .filter("array_has_any(labels, [1])")
        .unwrap()
        .try_into_batch()
        .await
        .unwrap();
    let expected_ids = expected
        .column(0)
        .as_primitive::<Int32Type>()
        .values()
        .to_vec();
    assert_eq!(expected_ids, vec![0, 2]);

    dataset
        .create_index(
            &["labels"],
            IndexType::LabelList,
            Some("labels_idx".to_owned()),
            &ScalarIndexParams::default(),
            true,
        )
        .await
        .unwrap();

    let result = dataset
        .scan()
        .project(&["id"])
        .unwrap()
        .filter("array_has_any(labels, [1])")
        .unwrap()
        .try_into_batch()
        .await
        .unwrap();
    let result_ids = result
        .column(0)
        .as_primitive::<Int32Type>()
        .values()
        .to_vec();
    assert_eq!(result_ids, expected_ids);

    let plan = dataset
        .scan()
        .filter("array_has_any(labels, [1])")
        .unwrap()
        .explain_plan(false)
        .await
        .unwrap();
    assert!(
        plan.contains("ScalarIndexQuery") && plan.contains("LabelList"),
        "Expected LabelList scalar index query in plan: {plan}"
    );
}

/// LabelList analogue: after prewarming, an `array_has_any` query against a
/// `LabelList` index serves results without any further IO. Exercises the
/// `LabelListIndexState` codec (which embeds the inner bitmap state and the
/// list-nulls bitmap) plus the same per-value bitmap codec.
#[tokio::test]
async fn test_label_list_prewarm_with_serializing_backend_serves_query_with_no_io() {
    use lance_io::assert_io_eq;

    use crate::utils::test::serializing_cache::SerializingCacheBackend;

    let tmpdir = TempStrDir::default();
    let uri = tmpdir.to_owned();
    drop(tmpdir);

    use crate::utils::test::{DatagenExt, FragmentCount, FragmentRowCount};

    let mut dataset = gen_batch()
        .col(
            "labels",
            lance_datagen::array::rand_list_any(
                lance_datagen::array::cycle::<arrow::datatypes::Int64Type>(vec![1, 2, 3, 4, 5]),
                false,
            ),
        )
        .into_dataset(&uri, FragmentCount::from(2), FragmentRowCount::from(2000))
        .await
        .unwrap();
    dataset
        .create_index(
            &["labels"],
            IndexType::LabelList,
            Some("labels_idx".to_owned()),
            &ScalarIndexParams::default(),
            true,
        )
        .await
        .unwrap();
    let expected = dataset
        .scan()
        .project(&[ROW_ID])
        .unwrap()
        .filter("array_has_any(labels, [3])")
        .unwrap()
        .try_into_batch()
        .await
        .unwrap()
        .num_rows();
    assert!(
        expected > 0,
        "test dataset must contain at least one row whose labels include 3"
    );

    let backend = Arc::new(SerializingCacheBackend::new());
    let session = Arc::new(Session::with_index_cache_backend(
        backend.clone(),
        128 * 1024 * 1024,
        Arc::new(lance_io::object_store::ObjectStoreRegistry::default()),
    ));
    let dataset = DatasetBuilder::from_uri(&uri)
        .with_session(session)
        .load()
        .await
        .unwrap();

    dataset.object_store.as_ref().io_stats_incremental();
    dataset.prewarm_index("labels_idx").await.unwrap();

    let serialized_after_prewarm = backend.serialized_entry_count().await;
    assert!(
        serialized_after_prewarm > 0,
        "prewarm should have routed the label-list state and per-value bitmaps through \
         CacheCodec, but the serializing store was empty"
    );

    dataset.object_store.as_ref().io_stats_incremental();
    let result = dataset
        .scan()
        .project(&[ROW_ID])
        .unwrap()
        .filter("array_has_any(labels, [3])")
        .unwrap()
        .try_into_batch()
        .await
        .unwrap();
    assert_eq!(
        result.num_rows(),
        expected,
        "indexed label-list filter should return correct results after deserialization"
    );

    let stats = dataset.object_store.as_ref().io_stats_incremental();
    assert_io_eq!(
        stats,
        read_iops,
        0,
        "LabelList filter query should not perform IO after prewarm; the serializing \
         cache backend must serve the index state and every per-value bitmap from memory"
    );
}

#[tokio::test]
async fn test_fts_phrase_query_with_removed_stop_words() {
    let tmpdir = TempStrDir::default();
    let uri = tmpdir.to_owned();
    drop(tmpdir);

    let doc_col: Arc<dyn Array> = Arc::new(GenericStringArray::<i32>::from(vec![
        "want the apple",
        "want an apple",
        "want green apple",
        "apple want the",
    ]));
    let ids = UInt64Array::from_iter_values(0..doc_col.len() as u64);
    let batch = RecordBatch::try_new(
        arrow_schema::Schema::new(vec![
            arrow_schema::Field::new("doc", doc_col.data_type().to_owned(), true),
            arrow_schema::Field::new("id", DataType::UInt64, false),
        ])
        .into(),
        vec![Arc::new(doc_col) as ArrayRef, Arc::new(ids) as ArrayRef],
    )
    .unwrap();
    let schema = batch.schema();
    let batches = RecordBatchIterator::new(vec![batch].into_iter().map(Ok), schema);
    let mut dataset = Dataset::write(batches, &uri, None).await.unwrap();

    dataset
        .create_index(
            &["doc"],
            IndexType::Inverted,
            None,
            &InvertedIndexParams::default()
                .with_position(true)
                .remove_stop_words(true),
            true,
        )
        .await
        .unwrap();

    for query in ["want the apple", "want an apple"] {
        let result = dataset
            .scan()
            .project(&["id"])
            .unwrap()
            .full_text_search(FullTextSearchQuery::new_query(
                PhraseQuery::new(query.to_owned()).into(),
            ))
            .unwrap()
            .try_into_batch()
            .await
            .unwrap();

        let ids = result["id"].as_primitive::<UInt64Type>().values();
        assert_eq!(result.num_rows(), 3, "query={query}, ids={ids:?}");
        assert!(ids.contains(&0), "query={query}, ids={ids:?}");
        assert!(ids.contains(&1), "query={query}, ids={ids:?}");
        assert!(ids.contains(&2), "query={query}, ids={ids:?}");
    }
}

#[tokio::test]
async fn test_fts_without_index_on_zero_fragment_dataset_is_empty() {
    let schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("id", DataType::UInt64, false),
        ArrowField::new("doc", DataType::Utf8, true),
    ]));
    let empty_reader = RecordBatchIterator::new(vec![], schema);
    let dataset = Dataset::write(empty_reader, "memory://", None)
        .await
        .unwrap();
    assert!(dataset.fragments().is_empty());

    let mut scan = dataset.scan();
    scan.project(&["id"]).unwrap();
    scan.full_text_search(FullTextSearchQuery::new_query(
        MatchQuery::new("alpha".to_owned())
            .with_column(Some("doc".to_owned()))
            .into(),
    ))
    .unwrap();
    let plan = scan.explain_plan(false).await.unwrap();
    assert!(plan.contains("EmptyExec"), "unexpected plan: {plan}");
    assert_eq!(scan.try_into_batch().await.unwrap().num_rows(), 0);

    let mut phrase_scan = dataset.scan();
    phrase_scan.project(&["id"]).unwrap();
    phrase_scan
        .full_text_search(FullTextSearchQuery::new_query(
            PhraseQuery::new("alpha beta".to_owned())
                .with_column(Some("doc".to_owned()))
                .into(),
        ))
        .unwrap();
    let phrase_plan = phrase_scan.explain_plan(false).await.unwrap();
    assert!(
        phrase_plan.contains("EmptyExec"),
        "unexpected phrase plan: {phrase_plan}"
    );
    assert_eq!(phrase_scan.try_into_batch().await.unwrap().num_rows(), 0);
}

#[tokio::test]
async fn test_fts_phrase_query_normalizes_leading_stop_word_position() {
    let tmpdir = TempStrDir::default();
    let uri = tmpdir.to_owned();
    drop(tmpdir);

    let initial = RecordBatch::try_from_iter(vec![
        ("id", Arc::new(UInt64Array::from(vec![99])) as ArrayRef),
        (
            "doc",
            Arc::new(StringArray::from(vec!["placeholder"])) as ArrayRef,
        ),
    ])
    .unwrap();
    let initial_reader = RecordBatchIterator::new(vec![Ok(initial.clone())], initial.schema());
    let mut dataset = Dataset::write(initial_reader, &uri, None).await.unwrap();
    let index_params = InvertedIndexParams::default()
        .with_position(true)
        .remove_stop_words(true);
    dataset
        .create_index(&["doc"], IndexType::Inverted, None, &index_params, true)
        .await
        .unwrap();

    let appended = RecordBatch::try_from_iter(vec![
        ("id", Arc::new(UInt64Array::from(vec![0, 1, 2])) as ArrayRef),
        (
            "doc",
            Arc::new(StringArray::from(vec![
                "alpha beta",
                "the alpha beta",
                "alpha gap beta",
            ])) as ArrayRef,
        ),
    ])
    .unwrap();
    let appended_reader = RecordBatchIterator::new(vec![Ok(appended.clone())], appended.schema());
    dataset = Dataset::write(
        appended_reader,
        Arc::new(dataset),
        Some(WriteParams {
            mode: WriteMode::Append,
            ..Default::default()
        }),
    )
    .await
    .unwrap();
    let appended_fragment = dataset.fragments().last().unwrap().clone();
    let query = FullTextSearchQuery::new_query(
        PhraseQuery::new("the alpha beta".to_owned())
            .with_column(Some("doc".to_owned()))
            .into(),
    );

    let mut flat_scan = dataset.scan();
    flat_scan.with_fragments(vec![appended_fragment.clone()]);
    flat_scan.project(&["id"]).unwrap();
    flat_scan.full_text_search(query.clone()).unwrap();
    let flat_result = flat_scan.try_into_batch().await.unwrap();
    let mut flat_ids = flat_result["id"]
        .as_primitive::<UInt64Type>()
        .values()
        .to_vec();
    flat_ids.sort_unstable();
    assert_eq!(flat_ids, vec![0, 1]);

    dataset
        .create_index(&["doc"], IndexType::Inverted, None, &index_params, true)
        .await
        .unwrap();
    let mut indexed_scan = dataset.scan();
    indexed_scan.with_fragments(vec![appended_fragment]);
    indexed_scan.project(&["id"]).unwrap();
    indexed_scan.full_text_search(query).unwrap();
    let indexed_result = indexed_scan.try_into_batch().await.unwrap();
    let mut indexed_ids = indexed_result["id"]
        .as_primitive::<UInt64Type>()
        .values()
        .to_vec();
    indexed_ids.sort_unstable();
    assert_eq!(indexed_ids, flat_ids);
}

#[tokio::test]
async fn test_fts_phrase_query_preserves_stop_word_gaps() {
    let tmpdir = TempStrDir::default();
    let uri = tmpdir.to_owned();
    drop(tmpdir);

    let doc_col: Arc<dyn Array> = Arc::new(GenericStringArray::<i32>::from(vec![
        "the united states of america",
        "the united states and america",
        "united states america",
        "the united states of north america",
    ]));
    let ids = UInt64Array::from_iter_values(0..doc_col.len() as u64);
    let batch = RecordBatch::try_new(
        arrow_schema::Schema::new(vec![
            arrow_schema::Field::new("doc", doc_col.data_type().to_owned(), true),
            arrow_schema::Field::new("id", DataType::UInt64, false),
        ])
        .into(),
        vec![Arc::new(doc_col) as ArrayRef, Arc::new(ids) as ArrayRef],
    )
    .unwrap();
    let schema = batch.schema();
    let batches = RecordBatchIterator::new(vec![batch].into_iter().map(Ok), schema);
    let mut dataset = Dataset::write(batches, &uri, None).await.unwrap();

    dataset
        .create_index(
            &["doc"],
            IndexType::Inverted,
            None,
            &InvertedIndexParams::default()
                .with_position(true)
                .remove_stop_words(true),
            true,
        )
        .await
        .unwrap();

    let result = dataset
        .scan()
        .project(&["id"])
        .unwrap()
        .full_text_search(FullTextSearchQuery::new_query(
            PhraseQuery::new("the united states of america".to_owned()).into(),
        ))
        .unwrap()
        .try_into_batch()
        .await
        .unwrap();

    let ids = result["id"].as_primitive::<UInt64Type>().values();
    assert_eq!(result.num_rows(), 2, "ids={ids:?}");
    assert!(ids.contains(&0), "ids={ids:?}");
    assert!(ids.contains(&1), "ids={ids:?}");
    assert!(!ids.contains(&2), "ids={ids:?}");
    assert!(!ids.contains(&3), "ids={ids:?}");
}

async fn prepare_json_dataset() -> (Dataset, String) {
    let text_col = Arc::new(StringArray::from(vec![
        r#"{
          "Title": "HarryPotter Chapter One",
          "Content": "Mr. and Mrs. Dursley, of number four, Privet Drive, were proud to say...",
          "Author": "J.K. Rowling",
          "Price": 128,
          "Language": ["english", "chinese"]
      }"#,
        r#"{
         "Title": "Fairy Talest",
         "Content": "Once upon a time, on a bitterly cold New Year's Eve, a little girl...",
         "Author": "ANDERSEN",
         "Price": 50,
         "Language": ["english", "chinese"]
      }"#,
    ]));
    let json_col = "json_field".to_string();

    // Prepare dataset
    let mut metadata = HashMap::new();
    metadata.insert(
        ARROW_EXT_NAME_KEY.to_string(),
        ARROW_JSON_EXT_NAME.to_string(),
    );
    let batch = RecordBatch::try_new(
        arrow_schema::Schema::new(vec![
            Field::new(&json_col, DataType::Utf8, false).with_metadata(metadata),
        ])
        .into(),
        vec![text_col.clone()],
    )
    .unwrap();
    let schema = batch.schema();
    let stream = RecordBatchIterator::new(vec![batch].into_iter().map(Ok), schema);
    let dataset = Dataset::write(stream, "memory://test/table", None)
        .await
        .unwrap();

    (dataset, json_col)
}

#[tokio::test]
async fn test_json_inverted_fuzziness_query() {
    let (mut dataset, json_col) = prepare_json_dataset().await;

    // Create inverted index for json col
    dataset
        .create_index(
            &[&json_col],
            IndexType::Inverted,
            None,
            &InvertedIndexParams::default().lance_tokenizer("json".to_string()),
            true,
        )
        .await
        .unwrap();

    // Match query with fuzziness
    let query = FullTextSearchQuery {
        query: FtsQuery::Match(
            MatchQuery::new("Content,str,Dursley".to_string()).with_column(Some(json_col.clone())),
        ),
        limit: None,
        wand_factor: None,
    };
    let batch = dataset
        .scan()
        .full_text_search(query)
        .unwrap()
        .try_into_batch()
        .await
        .unwrap();
    assert_eq!(1, batch.num_rows());

    let query = FullTextSearchQuery {
        query: FtsQuery::Match(
            MatchQuery::new("Content,str,Bursley".to_string()).with_column(Some(json_col.clone())),
        ),
        limit: None,
        wand_factor: None,
    };
    let batch = dataset
        .scan()
        .full_text_search(query)
        .unwrap()
        .try_into_batch()
        .await
        .unwrap();
    assert_eq!(0, batch.num_rows());

    let query = FullTextSearchQuery {
        query: FtsQuery::Match(
            MatchQuery::new("Content,str,Bursley".to_string())
                .with_column(Some(json_col.clone()))
                .with_fuzziness(Some(1)),
        ),
        limit: None,
        wand_factor: None,
    };
    let batch = dataset
        .scan()
        .full_text_search(query)
        .unwrap()
        .try_into_batch()
        .await
        .unwrap();
    assert_eq!(1, batch.num_rows());

    let query = FullTextSearchQuery {
        query: FtsQuery::Match(
            MatchQuery::new("Content,str,ABursley".to_string())
                .with_column(Some(json_col.clone()))
                .with_fuzziness(Some(1)),
        ),
        limit: None,
        wand_factor: None,
    };
    let batch = dataset
        .scan()
        .full_text_search(query)
        .unwrap()
        .try_into_batch()
        .await
        .unwrap();
    assert_eq!(0, batch.num_rows());

    let query = FullTextSearchQuery {
        query: FtsQuery::Match(
            MatchQuery::new("Content,str,ABursley".to_string())
                .with_column(Some(json_col.clone()))
                .with_fuzziness(Some(2)),
        ),
        limit: None,
        wand_factor: None,
    };
    let batch = dataset
        .scan()
        .full_text_search(query)
        .unwrap()
        .try_into_batch()
        .await
        .unwrap();
    assert_eq!(1, batch.num_rows());

    let query = FullTextSearchQuery {
        query: FtsQuery::Match(
            MatchQuery::new("Dontent,str,Bursley".to_string())
                .with_column(Some(json_col.clone()))
                .with_fuzziness(Some(2)),
        ),
        limit: None,
        wand_factor: None,
    };
    let batch = dataset
        .scan()
        .full_text_search(query)
        .unwrap()
        .try_into_batch()
        .await
        .unwrap();
    assert_eq!(0, batch.num_rows());
}

#[tokio::test]
async fn test_json_inverted_match_query() {
    let (mut dataset, json_col) = prepare_json_dataset().await;

    // Create inverted index for json col, with max token len 10 and enable stemming,
    // lower case, and remove stop words
    dataset
        .create_index(
            &[&json_col],
            IndexType::Inverted,
            None,
            &InvertedIndexParams::default()
                .lance_tokenizer("json".to_string())
                .max_token_length(Some(10))
                .stem(true)
                .lower_case(true)
                .remove_stop_words(true),
            true,
        )
        .await
        .unwrap();

    // Match query with token length exceed max token length
    let query = FullTextSearchQuery {
        query: FtsQuery::Match(
            MatchQuery::new("Title,str,harrypotter".to_string())
                .with_column(Some(json_col.clone())),
        ),
        limit: None,
        wand_factor: None,
    };
    let batch = dataset
        .scan()
        .full_text_search(query)
        .unwrap()
        .try_into_batch()
        .await
        .unwrap();
    assert_eq!(0, batch.num_rows());

    // Match query with stemming
    let query = FullTextSearchQuery {
        query: FtsQuery::Match(
            MatchQuery::new("Content,str,onc".to_string()).with_column(Some(json_col.clone())),
        ),
        limit: None,
        wand_factor: None,
    };
    let batch = dataset
        .scan()
        .full_text_search(query)
        .unwrap()
        .try_into_batch()
        .await
        .unwrap();
    assert_eq!(1, batch.num_rows());

    // Match query with lower case
    let query = FullTextSearchQuery {
        query: FtsQuery::Match(
            MatchQuery::new("Content,str,DURSLEY".to_string()).with_column(Some(json_col.clone())),
        ),
        limit: None,
        wand_factor: None,
    };
    let batch = dataset
        .scan()
        .full_text_search(query)
        .unwrap()
        .try_into_batch()
        .await
        .unwrap();
    assert_eq!(1, batch.num_rows());

    // Match query with stop word
    let query = FullTextSearchQuery {
        query: FtsQuery::Match(
            MatchQuery::new("Content,str,and".to_string()).with_column(Some(json_col.clone())),
        ),
        limit: None,
        wand_factor: None,
    };
    let batch = dataset
        .scan()
        .full_text_search(query)
        .unwrap()
        .try_into_batch()
        .await
        .unwrap();
    assert_eq!(0, batch.num_rows());
}

#[tokio::test]
async fn test_json_inverted_flat_match_query() {
    let (mut dataset, json_col) = prepare_json_dataset().await;

    // Create inverted index for json col
    dataset
        .create_index(
            &[&json_col],
            IndexType::Inverted,
            None,
            &InvertedIndexParams::default()
                .lance_tokenizer("json".to_string())
                .stem(false),
            true,
        )
        .await
        .unwrap();

    // Append data
    let text_col = Arc::new(StringArray::from(vec![
        r#"{
          "Title": "HarryPotter Chapter Two",
          "Content": "Nearly ten years had passed since the Dursleys had woken up...",
          "Author": "J.K. Rowling",
          "Price": 128,
          "Language": ["english", "chinese"]
        }"#,
    ]));

    let mut metadata = HashMap::new();
    metadata.insert(
        ARROW_EXT_NAME_KEY.to_string(),
        ARROW_JSON_EXT_NAME.to_string(),
    );
    let batch = RecordBatch::try_new(
        arrow_schema::Schema::new(vec![
            Field::new(&json_col, DataType::Utf8, false).with_metadata(metadata),
        ])
        .into(),
        vec![text_col.clone()],
    )
    .unwrap();
    let schema = batch.schema();
    let stream = RecordBatchIterator::new(vec![batch].into_iter().map(Ok), schema);
    dataset.append(stream, None).await.unwrap();

    // Test match query
    let query = FullTextSearchQuery {
        query: FtsQuery::Match(
            MatchQuery::new("Title,str,harrypotter".to_string())
                .with_column(Some(json_col.clone())),
        ),
        limit: None,
        wand_factor: None,
    };
    let batch = dataset
        .scan()
        .full_text_search(query)
        .unwrap()
        .try_into_batch()
        .await
        .unwrap();
    assert_eq!(2, batch.num_rows());
}

#[tokio::test]
async fn test_json_inverted_phrase_query() {
    // Prepare json dataset
    let (mut dataset, json_col) = prepare_json_dataset().await;

    // Create inverted index for json col
    dataset
        .create_index(
            &[&json_col],
            IndexType::Inverted,
            None,
            &InvertedIndexParams::default()
                .lance_tokenizer("json".to_string())
                .stem(false)
                .with_position(true),
            true,
        )
        .await
        .unwrap();

    // Test phrase query
    let query = FullTextSearchQuery {
        query: FtsQuery::Phrase(
            PhraseQuery::new("Title,str,harrypotter one chapter".to_string())
                .with_column(Some(json_col.clone())),
        ),
        limit: None,
        wand_factor: None,
    };
    let batch = dataset
        .scan()
        .full_text_search(query)
        .unwrap()
        .try_into_batch()
        .await
        .unwrap();
    assert_eq!(0, batch.num_rows());

    let query = FullTextSearchQuery {
        query: FtsQuery::Phrase(
            PhraseQuery::new("Title,str,harrypotter chapter one".to_string())
                .with_column(Some(json_col.clone())),
        ),
        limit: None,
        wand_factor: None,
    };
    let batch = dataset
        .scan()
        .full_text_search(query)
        .unwrap()
        .try_into_batch()
        .await
        .unwrap();
    assert_eq!(1, batch.num_rows());
}

#[tokio::test]
async fn test_json_inverted_multimatch_query() {
    // Prepare json dataset
    let (mut dataset, json_col) = prepare_json_dataset().await;

    // Create inverted index for json col
    dataset
        .create_index(
            &[&json_col],
            IndexType::Inverted,
            None,
            &InvertedIndexParams::default()
                .lance_tokenizer("json".to_string())
                .stem(false),
            true,
        )
        .await
        .unwrap();

    // Test multi match query
    let query = FullTextSearchQuery {
        query: FtsQuery::MultiMatch(MultiMatchQuery {
            match_queries: vec![
                MatchQuery::new("Title,str,harrypotter".to_string())
                    .with_column(Some(json_col.clone())),
                MatchQuery::new("Language,str,english".to_string())
                    .with_column(Some(json_col.clone())),
            ],
        }),
        limit: None,
        wand_factor: None,
    };
    let batch = dataset
        .scan()
        .full_text_search(query)
        .unwrap()
        .try_into_batch()
        .await
        .unwrap();
    assert_eq!(2, batch.num_rows());
}

#[tokio::test]
async fn test_json_inverted_boolean_query() {
    // Prepare json dataset
    let (mut dataset, json_col) = prepare_json_dataset().await;

    // Create inverted index for json col
    dataset
        .create_index(
            &[&json_col],
            IndexType::Inverted,
            None,
            &InvertedIndexParams::default()
                .lance_tokenizer("json".to_string())
                .stem(false),
            true,
        )
        .await
        .unwrap();

    // Test boolean query
    let query = FullTextSearchQuery {
        query: FtsQuery::Boolean(BooleanQuery {
            should: vec![],
            must: vec![
                FtsQuery::Match(
                    MatchQuery::new("Language,str,english".to_string())
                        .with_column(Some(json_col.clone())),
                ),
                FtsQuery::Match(
                    MatchQuery::new("Title,str,harrypotter".to_string())
                        .with_column(Some(json_col.clone())),
                ),
            ],
            must_not: vec![],
        }),
        limit: None,
        wand_factor: None,
    };
    let batch = dataset
        .scan()
        .full_text_search(query)
        .unwrap()
        .try_into_batch()
        .await
        .unwrap();
    assert_eq!(1, batch.num_rows());
}

#[tokio::test]
async fn test_sql_contains_tokens() {
    let text_col = Arc::new(StringArray::from(vec![
        "a cat catch a fish",
        "a fish catch a cat",
        "a white cat catch a big fish",
        "cat catchup fish",
        "cat fish catch",
    ]));

    // Prepare dataset
    let batch = RecordBatch::try_new(
        arrow_schema::Schema::new(vec![Field::new("text", DataType::Utf8, false)]).into(),
        vec![text_col.clone()],
    )
    .unwrap();
    let schema = batch.schema();
    let stream = RecordBatchIterator::new(vec![batch].into_iter().map(Ok), schema);
    let mut dataset = Dataset::write(stream, "memory://test/table", None)
        .await
        .unwrap();

    // Test without fts index
    let results = execute_sql(
        "select * from foo where contains_tokens(text, 'cat catch fish')",
        "foo".to_string(),
        Arc::new(dataset.clone()),
    )
    .await
    .unwrap();

    assert_results(
        results,
        &StringArray::from(vec![
            "a cat catch a fish",
            "a fish catch a cat",
            "a white cat catch a big fish",
            "cat fish catch",
        ]),
    );

    // Verify plan, should not contain ScalarIndexQuery.
    let results = execute_sql(
        "explain select * from foo where contains_tokens(text, 'cat catch fish')",
        "foo".to_string(),
        Arc::new(dataset.clone()),
    )
    .await
    .unwrap();
    let plan = format!("{:?}", results);
    assert_not_contains!(&plan, "ScalarIndexQuery");

    // Test with unsuitable fts index
    dataset
        .create_index(
            &["text"],
            IndexType::Inverted,
            None,
            &InvertedIndexParams::default().base_tokenizer("raw".to_string()),
            true,
        )
        .await
        .unwrap();

    let results = execute_sql(
        "select * from foo where contains_tokens(text, 'cat catch fish')",
        "foo".to_string(),
        Arc::new(dataset.clone()),
    )
    .await
    .unwrap();

    assert_results(
        results,
        &StringArray::from(vec![
            "a cat catch a fish",
            "a fish catch a cat",
            "a white cat catch a big fish",
            "cat fish catch",
        ]),
    );

    // Verify plan, should not contain ScalarIndexQuery because fts index is not unsuitable.
    let results = execute_sql(
        "explain select * from foo where contains_tokens(text, 'cat catch fish')",
        "foo".to_string(),
        Arc::new(dataset.clone()),
    )
    .await
    .unwrap();
    let plan = format!("{:?}", results);
    assert_not_contains!(&plan, "ScalarIndexQuery");

    // Test with suitable fts index
    dataset
        .create_index(
            &["text"],
            IndexType::Inverted,
            None,
            &InvertedIndexParams::default()
                .max_token_length(None)
                .stem(false),
            true,
        )
        .await
        .unwrap();

    let results = execute_sql(
        "select * from foo where contains_tokens(text, 'cat catch fish')",
        "foo".to_string(),
        Arc::new(dataset.clone()),
    )
    .await
    .unwrap();

    assert_results(
        results,
        &StringArray::from(vec![
            "a cat catch a fish",
            "a fish catch a cat",
            "a white cat catch a big fish",
            "cat fish catch",
        ]),
    );

    // Verify plan, should contain ScalarIndexQuery.
    let results = execute_sql(
        "explain select * from foo where contains_tokens(text, 'cat catch fish')",
        "foo".to_string(),
        Arc::new(dataset.clone()),
    )
    .await
    .unwrap();
    let plan = format!("{:?}", results);
    assert_contains!(&plan, "ScalarIndexQuery");
}

#[tokio::test]
async fn test_index_take_batch_size() -> Result<()> {
    use tempfile::tempdir;
    let temp_dir = tempdir()?;

    let dataset_path = temp_dir.path().join("ints_dataset");
    let values: Vec<i32> = (0..1024).collect();
    let array = Int32Array::from(values);
    let schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
        "ints",
        DataType::Int32,
        false,
    )]));
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(array)])?;
    let write_params = WriteParams {
        mode: WriteMode::Create,
        max_rows_per_file: 100,
        ..Default::default()
    };
    let batch_reader = RecordBatchIterator::new(vec![Ok(batch)], schema.clone());
    Dataset::write(
        batch_reader,
        dataset_path.to_str().unwrap(),
        Some(write_params),
    )
    .await?;
    let mut dataset = Dataset::open(dataset_path.to_str().unwrap()).await?;
    dataset
        .create_index(
            &["ints"],
            IndexType::Scalar,
            None,
            &ScalarIndexParams::default(),
            false,
        )
        .await?;

    let mut scanner = dataset.scan();
    scanner.batch_size(50).filter("ints > 0")?.with_row_id();
    let batches: Vec<RecordBatch> = scanner.try_into_stream().await?.try_collect().await?;
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(1023, total_rows);
    assert_eq!(21, batches.len());

    let mut scanner = dataset.scan();
    scanner
        .batch_size(50)
        .filter("ints > 0")?
        .limit(Some(1024), None)?
        .with_row_id();
    let batches: Vec<RecordBatch> = scanner.try_into_stream().await?.try_collect().await?;
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(1023, total_rows);
    assert_eq!(21, batches.len());

    let dataset_path2 = temp_dir.path().join("strings_dataset");
    let strings: Vec<String> = (0..1024).map(|i| format!("string-{}", i)).collect();
    let string_array = StringArray::from(strings);
    let schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
        "strings",
        DataType::Utf8,
        false,
    )]));
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(string_array)])?;
    let write_params = WriteParams {
        mode: WriteMode::Create,
        max_rows_per_file: 100,
        ..Default::default()
    };
    let batch_reader = RecordBatchIterator::new(vec![Ok(batch)], schema.clone());
    Dataset::write(
        batch_reader,
        dataset_path2.to_str().unwrap(),
        Some(write_params),
    )
    .await?;
    let mut dataset2 = Dataset::open(dataset_path2.to_str().unwrap()).await?;
    dataset2
        .create_index(
            &["strings"],
            IndexType::Scalar,
            None,
            &ScalarIndexParams::default(),
            false,
        )
        .await?;

    let mut scanner = dataset2.scan();
    scanner
        .batch_size(50)
        .filter("contains(strings, 'ing')")?
        .limit(Some(1024), None)?
        .with_row_id();
    let batches: Vec<RecordBatch> = scanner.try_into_stream().await?.try_collect().await?;
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(1024, total_rows);
    assert_eq!(21, batches.len());

    Ok(())
}

#[tokio::test]
async fn test_auto_infer_lance_tokenizer() {
    let (mut dataset, json_col) = prepare_json_dataset().await;

    // Create inverted index for json col. Expect auto-infer 'json' for lance tokenizer.
    dataset
        .create_index(
            &[&json_col],
            IndexType::Inverted,
            None,
            &InvertedIndexParams::default(),
            true,
        )
        .await
        .unwrap();

    // Match query succeed only when lance tokenizer is 'json'
    let query = FullTextSearchQuery {
        query: FtsQuery::Match(
            MatchQuery::new("Content,str,once".to_string()).with_column(Some(json_col.clone())),
        ),
        limit: None,
        wand_factor: None,
    };
    let batch = dataset
        .scan()
        .full_text_search(query)
        .unwrap()
        .try_into_batch()
        .await
        .unwrap();
    assert_eq!(1, batch.num_rows());
}

#[tokio::test]
async fn test_index_inherits_dataset_file_version() {
    // Test that index files use the same format version as the dataset
    let test_uri = TempStrDir::default();

    let dimension = 16;
    let schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
        "embeddings",
        DataType::FixedSizeList(
            Arc::new(ArrowField::new("item", DataType::Float32, true)),
            dimension,
        ),
        false,
    )]));

    let float_arr = generate_random_array(512 * dimension as usize);
    let vectors = Arc::new(
        <arrow_array::FixedSizeListArray as FixedSizeListArrayExt>::try_new_from_values(
            float_arr, dimension,
        )
        .unwrap(),
    );
    let batches = vec![RecordBatch::try_new(schema.clone(), vec![vectors.clone()]).unwrap()];

    let reader = RecordBatchIterator::new(batches.into_iter().map(Ok), schema.clone());

    // Create dataset with V2_1 file version
    let dataset_version = LanceFileVersion::V2_1;
    let mut dataset = Dataset::write(
        reader,
        &test_uri,
        Some(WriteParams {
            data_storage_version: Some(dataset_version),
            ..Default::default()
        }),
    )
    .await
    .unwrap();

    // Create a vector index
    let params = VectorIndexParams::ivf_pq(10, 8, 2, MetricType::L2, 50);
    let index_meta = dataset
        .create_index(&["embeddings"], IndexType::Vector, None, &params, true)
        .await
        .unwrap();

    // Get the index directory
    let index_dir = dataset.indices_dir().join(index_meta.uuid.to_string());

    // Open the index file and check its version
    let index_path = index_dir.clone().join("index.idx");
    let scheduler = ScanScheduler::new(
        dataset.object_store.clone(),
        SchedulerConfig::max_bandwidth(&dataset.object_store),
    );

    let file_handle = scheduler
        .open_file(&index_path, &CachedFileSize::unknown())
        .await
        .unwrap();

    let index_reader = FileReader::try_open(
        file_handle,
        None,
        Arc::default(),
        &LanceCache::no_cache(),
        FileReaderOptions::default(),
    )
    .await
    .unwrap();

    // Verify that the index file uses the same version as the dataset
    assert_eq!(
        index_reader.metadata().version(),
        dataset_version.into(),
        "Index file should use the same format version as the dataset"
    );

    // Also check the auxiliary file if it exists
    let aux_path = index_dir.clone().join("auxiliary.idx");
    if dataset
        .object_store
        .exists(&aux_path)
        .await
        .unwrap_or(false)
    {
        let aux_handle = scheduler
            .open_file(&aux_path, &CachedFileSize::unknown())
            .await
            .unwrap();

        let aux_reader = FileReader::try_open(
            aux_handle,
            None,
            Arc::default(),
            &LanceCache::no_cache(),
            FileReaderOptions::default(),
        )
        .await
        .unwrap();

        assert_eq!(
            aux_reader.metadata().version(),
            dataset_version.into(),
            "Auxiliary index file should use the same format version as the dataset"
        );
    }
}

#[tokio::test]
async fn test_legacy_dataset_uses_v2_0_for_indexes() {
    // Test that datasets with legacy format still use V2_0 for indexes (not legacy)
    let test_uri = TempStrDir::default();

    let dimension = 16;
    let schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
        "embeddings",
        DataType::FixedSizeList(
            Arc::new(ArrowField::new("item", DataType::Float32, true)),
            dimension,
        ),
        false,
    )]));

    let float_arr = generate_random_array(512 * dimension as usize);
    let vectors = Arc::new(
        <arrow_array::FixedSizeListArray as FixedSizeListArrayExt>::try_new_from_values(
            float_arr, dimension,
        )
        .unwrap(),
    );
    let batches = vec![RecordBatch::try_new(schema.clone(), vec![vectors.clone()]).unwrap()];

    let reader = RecordBatchIterator::new(batches.into_iter().map(Ok), schema.clone());

    // Create dataset with legacy file version
    let mut dataset = Dataset::write(
        reader,
        &test_uri,
        Some(WriteParams {
            data_storage_version: Some(LanceFileVersion::Legacy),
            ..Default::default()
        }),
    )
    .await
    .unwrap();

    // Create a vector index
    let params = VectorIndexParams::ivf_pq(10, 8, 2, MetricType::L2, 50);
    let index_meta = dataset
        .create_index(&["embeddings"], IndexType::Vector, None, &params, true)
        .await
        .unwrap();

    // Get the index directory
    let index_dir = dataset.indices_dir().join(index_meta.uuid.to_string());

    // Open the index file and check its version
    let index_path = index_dir.clone().join("index.idx");
    let scheduler = ScanScheduler::new(
        dataset.object_store.clone(),
        SchedulerConfig::max_bandwidth(&dataset.object_store),
    );

    let file_handle = scheduler
        .open_file(&index_path, &CachedFileSize::unknown())
        .await
        .unwrap();

    let index_reader = FileReader::try_open(
        file_handle,
        None,
        Arc::default(),
        &LanceCache::no_cache(),
        FileReaderOptions::default(),
    )
    .await
    .unwrap();

    // Verify that the index file uses V2_0 (not legacy)
    assert_eq!(
        index_reader.metadata().version(),
        LanceFileVersion::V2_0.into(),
        "Index files should never use legacy format, even for legacy datasets"
    );
}

#[tokio::test]
async fn test_manifest_read_recovers_from_stale_size() {
    // A cached `ManifestLocation.size` can lag the real object: a reader may pick
    // up a size from a stale listing/hint while another writer is committing
    // concurrently. Reading the manifest (or its index section) with that stale
    // size must not fail with a spurious "file size is too small" error. The
    // reader should drop the cached size, fetch the true size, and succeed.
    use crate::session::Session;
    use lance_table::io::commit::ManifestLocation;
    use lance_table::io::manifest::read_manifest_indexes;

    let test_uri = TempStrDir::default();
    let schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
        "id",
        DataType::Int32,
        false,
    )]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int32Array::from((0..100).collect::<Vec<i32>>()))],
    )
    .unwrap();
    let reader = RecordBatchIterator::new(vec![Ok(batch)], schema.clone());

    let mut dataset = Dataset::write(reader, &test_uri, None).await.unwrap();
    dataset
        .create_index(
            &["id"],
            IndexType::BTree,
            Some("id_idx".to_string()),
            &ScalarIndexParams::default(),
            true,
        )
        .await
        .unwrap();

    let real_location = dataset.manifest_location().clone();
    assert!(real_location.size.is_some());

    // A deliberately-too-small size stands in for a stale cached size. Without the
    // retry, both reads below decode a bogus footer offset and fail with
    // "file size is too small".
    let stale_location = ManifestLocation {
        size: Some(1),
        ..real_location.clone()
    };

    let session = Session::default();
    let manifest = Dataset::load_manifest(
        dataset.object_store.as_ref(),
        &stale_location,
        test_uri.as_ref(),
        &session,
    )
    .await
    .expect("load_manifest should recover from a stale manifest size");
    assert_eq!(manifest.version, real_location.version);

    let indices = read_manifest_indexes(dataset.object_store.as_ref(), &stale_location, &manifest)
        .await
        .expect("read_manifest_indexes should recover from a stale manifest size");
    assert_eq!(indices.len(), 1);
    assert_eq!(indices[0].name, "id_idx");
}

/// `load_segment_params` must match the fully opened segment's params,
/// including `custom_stop_words` — the field `InvertedIndexDetails` loses.
#[tokio::test]
async fn test_load_segment_params_full_fidelity() {
    use crate::index::DatasetIndexInternalExt;
    use lance_index::metrics::NoOpMetricsCollector;
    use lance_index::scalar::inverted::{DocumentGranularity, InvertedIndex};

    let batch = RecordBatch::try_new(
        arrow_schema::Schema::new(vec![Field::new("text", DataType::Utf8, false)]).into(),
        vec![Arc::new(StringArray::from(vec![
            "the quick brown fox",
            "lazy dogs sleep",
        ]))],
    )
    .unwrap();
    let schema = batch.schema();
    let stream = RecordBatchIterator::new(vec![batch].into_iter().map(Ok), schema);
    let mut dataset = Dataset::write(stream, "memory://test/segment_params", None)
        .await
        .unwrap();

    let params = InvertedIndexParams::default().custom_stop_words(Some(vec!["quick".to_string()]));
    dataset
        .create_index(&["text"], IndexType::Inverted, None, &params, true)
        .await
        .unwrap();

    let segments = crate::index::scalar::load_segments(&dataset, "text", DocumentGranularity::Row)
        .await
        .unwrap()
        .expect("FTS index segments");
    let read = crate::index::scalar::load_segment_params(&dataset, &segments[0])
        .await
        .unwrap();

    let generic = dataset
        .open_generic_index("text", &segments[0].uuid, &NoOpMetricsCollector)
        .await
        .unwrap();
    let opened = generic
        .as_any()
        .downcast_ref::<InvertedIndex>()
        .expect("inverted index");
    assert_eq!(&read, opened.params());
}
