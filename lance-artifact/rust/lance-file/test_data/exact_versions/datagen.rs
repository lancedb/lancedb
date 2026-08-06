// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use arrow_array::builder::StringDictionaryBuilder;
use arrow_array::types::{Int8Type, Int32Type};
use arrow_array::{ArrayRef, Int32Array, LargeBinaryArray, ListArray, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema as ArrowSchema};
use async_trait::async_trait;
use lance_core::datatypes::Schema as LanceSchema;
use lance_encoding::encoder::{EncodingOptions, default_encoding_strategy, encode_batch};
use lance_file::previous::writer::{
    FileWriter as V1Writer, FileWriterOptions as V1WriterOptions, ManifestProvider,
};
use lance_file::testing::FsFixture;
use lance_file::version::LanceFileVersion;
use lance_file::writer::{EncodedBatchWriteExt, FileWriter, FileWriterOptions};
use lance_io::traits::Writer;

type DynError = Box<dyn std::error::Error + Send + Sync>;

struct NoManifest;

#[async_trait]
impl ManifestProvider for NoManifest {
    async fn store_schema(
        _: &mut dyn Writer,
        _: &LanceSchema,
    ) -> lance_core::Result<Option<usize>> {
        Ok(None)
    }
}

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
                .expect("fixture dictionary values must fit");
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
    RecordBatch::try_new(schema, vec![ids, names, items, categories, blobs])
        .expect("fixture schema and arrays must agree")
}

async fn write_current_fixture(
    version: LanceFileVersion,
    batch: &RecordBatch,
    schema: &LanceSchema,
) -> Result<Vec<u8>, DynError> {
    let fs = FsFixture::default();
    let object_writer = fs.object_store.create(&fs.tmp_path).await?;
    let mut writer = FileWriter::try_new(
        object_writer,
        schema.clone(),
        FileWriterOptions {
            data_cache_bytes: Some(1),
            max_page_bytes: Some(1024),
            format_version: Some(version),
            ..Default::default()
        },
    )?;
    for offset in (0..batch.num_rows()).step_by(1024) {
        let slice = batch.slice(offset, (batch.num_rows() - offset).min(1024));
        writer.write_batch(&slice).await?;
    }
    let summary = writer.finish().await?;
    Ok(fs
        .object_store
        .open(&fs.tmp_path)
        .await?
        .get_range(0..summary.size_bytes as usize)
        .await?
        .to_vec())
}

async fn write_v1_fixture(batch: &RecordBatch, schema: &LanceSchema) -> Result<Vec<u8>, DynError> {
    let fs = FsFixture::default();
    let mut writer = V1Writer::<NoManifest>::try_new(
        fs.object_store.as_ref(),
        &fs.tmp_path,
        schema.clone(),
        &V1WriterOptions {
            collect_stats_for_fields: Some(Vec::new()),
        },
    )
    .await?;
    for offset in (0..batch.num_rows()).step_by(1024) {
        let slice = batch.slice(offset, (batch.num_rows() - offset).min(1024));
        writer.write(std::slice::from_ref(&slice)).await?;
    }
    let summary = writer.finish().await?;
    Ok(fs
        .object_store
        .open(&fs.tmp_path)
        .await?
        .get_range(0..summary.size_bytes as usize)
        .await?
        .to_vec())
}

async fn write_v2_0_embedded_fixtures(
    batch: &RecordBatch,
    schema: &LanceSchema,
) -> Result<(Vec<u8>, Vec<u8>), DynError> {
    let version = LanceFileVersion::V2_0;
    let options = EncodingOptions {
        cache_bytes_per_column: 1,
        max_page_bytes: 1024,
        keep_original_array: true,
        buffer_alignment: 64,
        version,
    };
    let encoding_strategy = default_encoding_strategy(version);
    let encoded_batch = encode_batch(
        batch,
        Arc::new(schema.clone()),
        encoding_strategy.as_ref(),
        &options,
    )
    .await?;

    Ok((
        encoded_batch.try_to_self_described_lance(version)?.to_vec(),
        encoded_batch.try_to_mini_lance(version)?.to_vec(),
    ))
}

fn write_fixture(output_dir: &Path, name: &str, bytes: &[u8]) -> Result<(), DynError> {
    std::fs::write(output_dir.join(name), bytes)?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), DynError> {
    let output_dir = std::env::args_os()
        .nth(1)
        .map(std::path::PathBuf::from)
        .ok_or("usage: exact_version_fixture_generator <output-directory>")?;
    std::fs::create_dir_all(&output_dir)?;

    let batch = compatibility_fixture_batch();
    let mut schema = LanceSchema::try_from(batch.schema().as_ref())?;
    schema.set_dictionary(&batch)?;

    write_fixture(
        &output_dir,
        "v1.lance",
        &write_v1_fixture(&batch, &schema).await?,
    )?;
    for (version, name) in [
        (LanceFileVersion::V2_0, "v2_0.lance"),
        (LanceFileVersion::V2_1, "v2_1.lance"),
        (LanceFileVersion::V2_2, "v2_2.lance"),
    ] {
        write_fixture(
            &output_dir,
            name,
            &write_current_fixture(version, &batch, &schema).await?,
        )?;
    }

    let embedded_batch = batch.project(&[0, 1])?.slice(0, 257);
    let mut embedded_schema = LanceSchema::try_from(embedded_batch.schema().as_ref())?;
    embedded_schema.set_dictionary(&embedded_batch)?;
    let (self_described, mini) =
        write_v2_0_embedded_fixtures(&embedded_batch, &embedded_schema).await?;
    write_fixture(&output_dir, "v2_0_self_described.lance", &self_described)?;
    write_fixture(&output_dir, "v2_0_mini.lance", &mini)?;

    Ok(())
}
