// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::builder::StringDictionaryBuilder;
use arrow_array::cast::AsArray;
use arrow_array::types::{Int8Type, Int32Type};
use arrow_array::{
    Array, ArrayRef, Int32Array, LargeBinaryArray, ListArray, RecordBatch, StringArray,
};
use arrow_schema::{DataType, Field, Schema as ArrowSchema};
use bytes::Bytes;
use futures::TryStreamExt;
use lance_core::cache::LanceCache;
use lance_core::datatypes::Schema as LanceSchema;
use lance_encoding::decoder::{DecoderPlugins, EncodedBatchLayout, FilterExpression, decode_batch};
use lance_encoding::encoder::{EncodedBatch, EncodingOptions, encode_batch};
use lance_io::ReadBatchParams;
use lance_io::traits::Writer;
use lance_io::utils::CachedFileSize;
use rstest::rstest;
use tokio::io::AsyncWriteExt;

use crate::reader::{EncodedBatchReaderExt, FileReader, FileReaderOptions};
use crate::testing::FsFixture;
use crate::version::ConcreteFileVersion;
use crate::versions;
use crate::versions::v1::reader::FileReader as V1Reader;
use crate::versions::v1::writer::{
    FileWriter as V1Writer, FileWriterOptions as V1WriterOptions, NotSelfDescribing,
};
use crate::writer::FileWriterOptions;

fn compatibility_fixture_batch() -> RecordBatch {
    let row_count = 4097;
    let ids = Arc::new(Int32Array::from_iter_values(0..row_count)) as ArrayRef;
    let names = Arc::new(StringArray::from_iter((0..row_count).map(|index| {
        (index % 7 != 0).then(|| format!("value-{index:04}-deterministic-fixture"))
    }))) as ArrayRef;
    let items = Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(
        (0..row_count).map(|index| {
            (index % 11 != 0).then(|| {
                vec![
                    Some(index),
                    (index % 5 != 0).then_some(index * 2),
                    Some(index * 3),
                ]
            })
        }),
    )) as ArrayRef;
    let mut categories = StringDictionaryBuilder::<Int8Type>::new();
    for index in 0..row_count {
        if index % 13 == 0 {
            categories.append_null();
        } else {
            categories
                .append(match index % 3 {
                    0 => "red",
                    1 => "green",
                    _ => "blue",
                })
                .unwrap();
        }
    }
    let categories = Arc::new(categories.finish()) as ArrayRef;
    let blobs = Arc::new(LargeBinaryArray::from_iter_values(
        (0..row_count).map(|index| format!("blob-{index:04}-deterministic-payload").into_bytes()),
    )) as ArrayRef;

    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true).with_metadata(HashMap::from([(
            "lance-encoding:compression".to_string(),
            "none".to_string(),
        )])),
        Field::new(
            "items",
            DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
            true,
        ),
        Field::new(
            "category",
            DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
            true,
        )
        .with_metadata(HashMap::from([(
            "lance-encoding:dict-values-compression".to_string(),
            "none".to_string(),
        )])),
        Field::new("blob", DataType::LargeBinary, true).with_metadata(HashMap::from([(
            "lance-encoding:blob".to_string(),
            "true".to_string(),
        )])),
    ]));
    RecordBatch::try_new(schema, vec![ids, names, items, categories, blobs]).unwrap()
}

fn v1_reader_expected_batch(batch: &RecordBatch) -> RecordBatch {
    // The V1 reader historically materializes null lists as empty, null child integers as zero,
    // and null dictionary keys as the first dictionary value.
    let items = Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(
        (0..batch.num_rows() as i32).map(|index| {
            Some(if index % 11 == 0 {
                Vec::new()
            } else {
                vec![
                    Some(index),
                    Some(if index % 5 == 0 { 0 } else { index * 2 }),
                    Some(index * 3),
                ]
            })
        }),
    )) as ArrayRef;
    let mut categories = StringDictionaryBuilder::<Int8Type>::new();
    for index in 0..batch.num_rows() {
        categories
            .append(if index % 13 == 0 {
                "green"
            } else {
                match index % 3 {
                    0 => "red",
                    1 => "green",
                    _ => "blue",
                }
            })
            .unwrap();
    }
    let categories = Arc::new(categories.finish()) as ArrayRef;
    let mut columns = batch.columns().to_vec();
    columns[2] = items;
    columns[3] = categories;
    RecordBatch::try_new(batch.schema(), columns).unwrap()
}

fn stable_fixture(version: ConcreteFileVersion) -> &'static [u8] {
    match version {
        ConcreteFileVersion::V1 => include_bytes!("../test_data/exact_versions/v1.lance"),
        ConcreteFileVersion::V2_0 => {
            include_bytes!("../test_data/exact_versions/v2_0.lance")
        }
        ConcreteFileVersion::V2_1 => {
            include_bytes!("../test_data/exact_versions/v2_1.lance")
        }
        ConcreteFileVersion::V2_2 => {
            include_bytes!("../test_data/exact_versions/v2_2.lance")
        }
        ConcreteFileVersion::V2_3 => {
            unreachable!("v2.3 is unstable and has no compatibility fixture")
        }
    }
}

fn assert_blob_column_eq(actual: &dyn Array, expected: &dyn Array) {
    let actual = actual.as_binary::<i64>();
    let expected = expected.as_binary::<i64>();
    assert_eq!(actual.len(), expected.len());
    for index in 0..actual.len() {
        assert_eq!(
            actual.is_null(index),
            expected.is_null(index),
            "blob validity differs at row {index}"
        );
        if actual.is_valid(index) {
            assert_eq!(
                actual.value(index),
                expected.value(index),
                "blob payload differs at row {index}"
            );
        }
    }
}

fn assert_record_batch_eq(actual: &RecordBatch, expected: &RecordBatch) {
    assert_eq!(actual.schema_ref(), expected.schema_ref());
    assert_eq!(actual.num_rows(), expected.num_rows());
    assert_eq!(actual.num_columns(), expected.num_columns());

    for column_index in 0..actual.num_columns() {
        if expected.schema().field(column_index).name() == "blob" {
            assert_blob_column_eq(
                actual.column(column_index).as_ref(),
                expected.column(column_index).as_ref(),
            );
        } else if actual.column(column_index).to_data() != expected.column(column_index).to_data() {
            let row_index = (0..actual.num_rows())
                .find(|row_index| {
                    actual.column(column_index).slice(*row_index, 1).to_data()
                        != expected.column(column_index).slice(*row_index, 1).to_data()
                })
                .unwrap();
            panic!(
                "column {} ({}) differs at row {}: actual={:?}, expected={:?}",
                column_index,
                expected.schema().field(column_index).name(),
                row_index,
                actual.column(column_index).slice(row_index, 1),
                expected.column(column_index).slice(row_index, 1)
            );
        }
    }
}

fn footer_version(bytes: &[u8]) -> (u16, u16) {
    let version_start = bytes.len() - 8;
    (
        u16::from_le_bytes([bytes[version_start], bytes[version_start + 1]]),
        u16::from_le_bytes([bytes[version_start + 2], bytes[version_start + 3]]),
    )
}

fn assert_wire_bytes_equal(actual: &[u8], expected: &[u8]) {
    if let Some(offset) = actual
        .iter()
        .zip(expected)
        .position(|(actual, expected)| actual != expected)
    {
        panic!(
            "wire fixture first differs at byte {offset}: actual={}, expected={}",
            actual[offset], expected[offset]
        );
    }
    assert_eq!(
        actual.len(),
        expected.len(),
        "wire fixture length changed after a common {}-byte prefix",
        actual.len().min(expected.len())
    );
}

async fn write_current_fixture(
    version: ConcreteFileVersion,
    batch: &RecordBatch,
    schema: &LanceSchema,
) -> Vec<u8> {
    let fs = FsFixture::default();
    let object_writer = fs.object_store.create(&fs.tmp_path).await.unwrap();
    let options = FileWriterOptions {
        data_cache_bytes: Some(1),
        max_page_bytes: Some(1024),
        ..Default::default()
    };
    let summary = match version {
        ConcreteFileVersion::V1 => {
            unreachable!("legacy fixtures use the legacy writer")
        }
        ConcreteFileVersion::V2_0 => {
            let mut writer =
                versions::v2_0::create_writer(object_writer, schema.clone(), options).unwrap();
            for offset in (0..batch.num_rows()).step_by(1024) {
                let slice = batch.slice(offset, (batch.num_rows() - offset).min(1024));
                writer.write_batch(&slice).await.unwrap();
            }
            writer.finish().await.unwrap()
        }
        ConcreteFileVersion::V2_1 => {
            let mut writer =
                versions::v2_1::create_writer(object_writer, schema.clone(), options).unwrap();
            for offset in (0..batch.num_rows()).step_by(1024) {
                let slice = batch.slice(offset, (batch.num_rows() - offset).min(1024));
                writer.write_batch(&slice).await.unwrap();
            }
            writer.finish().await.unwrap()
        }
        ConcreteFileVersion::V2_2 => {
            let mut writer =
                versions::v2_2::create_writer(object_writer, schema.clone(), options).unwrap();
            for offset in (0..batch.num_rows()).step_by(1024) {
                let slice = batch.slice(offset, (batch.num_rows() - offset).min(1024));
                writer.write_batch(&slice).await.unwrap();
            }
            writer.finish().await.unwrap()
        }
        ConcreteFileVersion::V2_3 => {
            let mut writer =
                versions::v2_3::create_writer(object_writer, schema.clone(), options).unwrap();
            for offset in (0..batch.num_rows()).step_by(1024) {
                let slice = batch.slice(offset, (batch.num_rows() - offset).min(1024));
                writer.write_batch(&slice).await.unwrap();
            }
            writer.finish().await.unwrap()
        }
    };
    fs.object_store
        .open(&fs.tmp_path)
        .await
        .unwrap()
        .get_range(0..summary.size_bytes as usize)
        .await
        .unwrap()
        .to_vec()
}

async fn write_v2_0_embedded_fixtures(batch: &RecordBatch, schema: &LanceSchema) -> (Bytes, Bytes) {
    let options = EncodingOptions {
        cache_bytes_per_column: 1,
        max_page_bytes: 1024,
        keep_original_array: true,
        buffer_alignment: 64,
    };
    let encoding_strategy = crate::versions::v2_0::encoding_strategy();
    let encoded_batch = encode_batch(
        batch,
        Arc::new(schema.clone()),
        encoding_strategy.as_ref(),
        &options,
    )
    .await
    .unwrap();

    (
        versions::v2_0::encode_self_described_batch(&encoded_batch).unwrap(),
        versions::v2_0::encode_mini_batch(&encoded_batch).unwrap(),
    )
}

async fn assert_current_reader_roundtrip(
    fixture: &[u8],
    version: ConcreteFileVersion,
    expected: &RecordBatch,
) {
    let fs = FsFixture::default();
    let mut fixture_writer = fs.object_store.create(&fs.tmp_path).await.unwrap();
    fixture_writer.write_all(fixture).await.unwrap();
    Writer::shutdown(fixture_writer.as_mut()).await.unwrap();
    let scheduler = fs
        .scheduler
        .open_file(&fs.tmp_path, &CachedFileSize::new(fixture.len() as u64))
        .await
        .unwrap();
    let reader = FileReader::try_open(
        scheduler,
        None,
        Arc::<DecoderPlugins>::default(),
        &LanceCache::no_cache(),
        FileReaderOptions::default(),
    )
    .await
    .unwrap();
    assert_eq!(reader.metadata().version(), version);
    assert!(
        reader
            .metadata()
            .column_metadatas
            .iter()
            .any(|metadata| metadata.pages.len() > 1)
    );
    let batches = reader
        .read_stream(
            ReadBatchParams::RangeFull,
            1024,
            16,
            FilterExpression::no_filter(),
        )
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(
        batches.iter().map(RecordBatch::num_rows).sum::<usize>(),
        expected.num_rows()
    );
    assert!(
        batches
            .iter()
            .all(|actual| actual.schema_ref() == expected.schema_ref())
    );
    let mut row_offset = 0;
    for actual in &batches {
        let expected = expected.slice(row_offset, actual.num_rows());
        assert_record_batch_eq(actual, &expected);
        row_offset += actual.num_rows();
    }
    assert_eq!(row_offset, expected.num_rows());
}

#[rstest]
#[case::v2_0(ConcreteFileVersion::V2_0)]
#[case::v2_1(ConcreteFileVersion::V2_1)]
#[case::v2_2(ConcreteFileVersion::V2_2)]
#[tokio::test]
async fn stable_current_writer_and_reader_are_wire_compatible(
    #[case] version: ConcreteFileVersion,
) {
    let batch = compatibility_fixture_batch();
    let mut schema = LanceSchema::try_from(batch.schema().as_ref()).unwrap();
    schema.set_dictionary(&batch).unwrap();

    let actual = write_current_fixture(version, &batch, &schema).await;
    let expected = stable_fixture(version);
    assert_wire_bytes_equal(&actual, expected);
    assert_eq!(
        footer_version(expected),
        version.to_standard_footer_numbers()
    );
    assert_current_reader_roundtrip(expected, version, &batch).await;
}

#[tokio::test]
async fn v2_0_embedded_writer_and_reader_are_wire_compatible() {
    let batch = compatibility_fixture_batch()
        .project(&[0, 1])
        .unwrap()
        .slice(0, 257);
    let mut schema = LanceSchema::try_from(batch.schema().as_ref()).unwrap();
    schema.set_dictionary(&batch).unwrap();

    let (actual_self_described, actual_mini) = write_v2_0_embedded_fixtures(&batch, &schema).await;
    let expected_self_described =
        include_bytes!("../test_data/exact_versions/v2_0_self_described.lance");
    let expected_mini = include_bytes!("../test_data/exact_versions/v2_0_mini.lance");
    assert_wire_bytes_equal(&actual_self_described, expected_self_described);
    assert_wire_bytes_equal(&actual_mini, expected_mini);

    let expected_footer = ConcreteFileVersion::V2_0.to_embedded_footer_numbers();
    assert_eq!(footer_version(expected_self_described), expected_footer);
    assert_eq!(footer_version(expected_mini), expected_footer);

    let self_described =
        EncodedBatch::try_from_self_described_lance(Bytes::from_static(expected_self_described))
            .unwrap();
    let decoded = decode_batch(
        &self_described,
        &FilterExpression::no_filter(),
        Arc::<DecoderPlugins>::default(),
        false,
        EncodedBatchLayout::Array,
        None,
    )
    .await
    .unwrap();
    assert_record_batch_eq(&decoded, &batch);

    let mini =
        EncodedBatch::try_from_mini_lance(Bytes::from_static(expected_mini), &schema).unwrap();
    let decoded = decode_batch(
        &mini,
        &FilterExpression::no_filter(),
        Arc::<DecoderPlugins>::default(),
        false,
        EncodedBatchLayout::Array,
        None,
    )
    .await
    .unwrap();
    assert_record_batch_eq(&decoded, &batch);
}

#[tokio::test]
async fn v2_3_output_is_deterministic_within_the_current_revision() {
    let batch = compatibility_fixture_batch();
    let mut schema = LanceSchema::try_from(batch.schema().as_ref()).unwrap();
    schema.set_dictionary(&batch).unwrap();

    let first = write_current_fixture(ConcreteFileVersion::V2_3, &batch, &schema).await;
    let second = write_current_fixture(ConcreteFileVersion::V2_3, &batch, &schema).await;
    assert_eq!(first, second);
    assert_eq!(
        footer_version(&first),
        ConcreteFileVersion::V2_3.to_standard_footer_numbers()
    );
    assert_current_reader_roundtrip(&first, ConcreteFileVersion::V2_3, &batch).await;
}

#[tokio::test]
async fn v1_writer_and_reader_are_wire_compatible() {
    let expected = stable_fixture(ConcreteFileVersion::V1);
    let batch = compatibility_fixture_batch();
    let mut schema = LanceSchema::try_from(batch.schema().as_ref()).unwrap();
    schema.set_dictionary(&batch).unwrap();
    let fs = FsFixture::default();
    let mut writer = V1Writer::<NotSelfDescribing>::try_new(
        fs.object_store.as_ref(),
        &fs.tmp_path,
        schema.clone(),
        &V1WriterOptions {
            collect_stats_for_fields: Some(Vec::new()),
        },
    )
    .await
    .unwrap();
    for offset in (0..batch.num_rows()).step_by(1024) {
        let slice = batch.slice(offset, (batch.num_rows() - offset).min(1024));
        writer.write(std::slice::from_ref(&slice)).await.unwrap();
    }
    let summary = writer.finish().await.unwrap();
    let actual = fs
        .object_store
        .open(&fs.tmp_path)
        .await
        .unwrap()
        .get_range(0..summary.size_bytes as usize)
        .await
        .unwrap();
    assert_wire_bytes_equal(actual.as_ref(), expected);
    assert_eq!(
        footer_version(expected),
        ConcreteFileVersion::V1.to_standard_footer_numbers()
    );

    let fixture_fs = FsFixture::default();
    let mut fixture_writer = fixture_fs
        .object_store
        .create(&fixture_fs.tmp_path)
        .await
        .unwrap();
    fixture_writer.write_all(expected).await.unwrap();
    Writer::shutdown(fixture_writer.as_mut()).await.unwrap();
    let reader = V1Reader::try_new(
        fixture_fs.object_store.as_ref(),
        &fixture_fs.tmp_path,
        schema.clone(),
    )
    .await
    .unwrap();
    let actual_batch = reader
        .read_range(0..batch.num_rows(), &schema)
        .await
        .unwrap();
    assert_eq!(reader.num_batches(), 5);
    assert_record_batch_eq(&actual_batch, &v1_reader_expected_batch(&batch));
}
