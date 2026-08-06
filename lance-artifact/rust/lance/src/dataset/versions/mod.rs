// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Dataset write policies that differ across exact Lance file versions.
//!
//! File grammar belongs to `lance_file::versions`. This module contains only
//! operation-level dataset choices whose behavior actually differs by version.

use std::sync::Arc;

use datafusion::execution::SendableRecordBatchStream;
use futures::{StreamExt, TryStreamExt};
use lance_core::{
    Result,
    datatypes::{Schema, SchemaCompareOptions},
};
use lance_datafusion::chunker::{break_stream, chunk_stream};
use lance_file::{
    version::ConcreteFileVersion,
    versions as file_versions,
    writer::{FileWriter, FileWriterOptions},
};
use lance_index::scalar::seed::IndexSeedWriter;
use lance_io::object_store::ObjectStore;
use lance_io::traits::Writer as ObjectWriter;
use lance_table::format::{DataFile, Fragment};
use object_store::path::Path;

use super::Dataset;
use super::fragment::write::FragmentCreateBuilder;
use super::utils::SchemaAdapter;
use super::write::{self, TargetBaseInfo, WriteParams, WriterOptions};

pub fn schema_compare_options(version: ConcreteFileVersion) -> SchemaCompareOptions {
    match version {
        ConcreteFileVersion::V1 => SchemaCompareOptions {
            compare_dictionary: true,
            ..Default::default()
        },
        ConcreteFileVersion::V2_0
        | ConcreteFileVersion::V2_1
        | ConcreteFileVersion::V2_2
        | ConcreteFileVersion::V2_3 => SchemaCompareOptions::default(),
    }
}

async fn create_seed_writers(
    version: ConcreteFileVersion,
    dataset: Option<&Dataset>,
    params: &WriteParams,
) -> Result<Vec<Box<dyn IndexSeedWriter>>> {
    match version {
        ConcreteFileVersion::V1 => Ok(Vec::new()),
        ConcreteFileVersion::V2_0
        | ConcreteFileVersion::V2_1
        | ConcreteFileVersion::V2_2
        | ConcreteFileVersion::V2_3 => write::create_seed_writers_current(dataset, params).await,
    }
}

fn create_current_file_writer(
    version: ConcreteFileVersion,
    object_writer: Box<dyn ObjectWriter>,
    schema: Schema,
    filename: String,
    base_id: Option<u32>,
) -> Result<(FileWriter, DataFile)> {
    let writer =
        file_versions::create_writer(version, object_writer, schema, FileWriterOptions::default())?;
    let mut data_file = DataFile::new_unstarted(filename, version);
    data_file.base_id = base_id;
    Ok((writer, data_file))
}

#[allow(clippy::too_many_arguments)]
pub async fn write_fragments(
    version: ConcreteFileVersion,
    dataset: Option<&Dataset>,
    object_store: Arc<ObjectStore>,
    base_dir: &Path,
    normalized_schema: Schema,
    data: SendableRecordBatchStream,
    params: WriteParams,
    target_bases_info: Option<Vec<TargetBaseInfo>>,
) -> Result<(Vec<Fragment>, Schema)> {
    let version_name = format!("{version:?}");
    let schema = write::prepare_write_schema(
        dataset,
        normalized_schema,
        &params,
        schema_compare_options(version),
    )?;
    match version {
        ConcreteFileVersion::V1 | ConcreteFileVersion::V2_0 | ConcreteFileVersion::V2_1 => {
            write::validate_legacy_blob_write_schema(&schema, &version_name)?;
        }
        ConcreteFileVersion::V2_2 | ConcreteFileVersion::V2_3 => {
            write::validate_blob_v2_write_schema(&schema)?;
        }
    }
    let seed_writers = create_seed_writers(version, dataset, &params).await?;
    let fragments = write_fragments_direct(
        version,
        dataset,
        object_store,
        base_dir,
        &schema,
        data,
        params,
        target_bases_info,
        seed_writers,
    )
    .await?;
    Ok((fragments, schema))
}

#[allow(clippy::too_many_arguments)]
pub async fn write_fragments_direct(
    version: ConcreteFileVersion,
    dataset: Option<&Dataset>,
    object_store: Arc<ObjectStore>,
    base_dir: &Path,
    schema: &Schema,
    data: SendableRecordBatchStream,
    params: WriteParams,
    target_bases_info: Option<Vec<TargetBaseInfo>>,
    seed_writers: Vec<Box<dyn IndexSeedWriter>>,
) -> Result<Vec<Fragment>> {
    let adapter = SchemaAdapter::new(data.schema());
    let data = adapter.to_physical_stream(data);
    let buffered_reader = match version {
        ConcreteFileVersion::V1 => chunk_stream(data, params.max_rows_per_group),
        ConcreteFileVersion::V2_0
        | ConcreteFileVersion::V2_1
        | ConcreteFileVersion::V2_2
        | ConcreteFileVersion::V2_3 => break_stream(data, params.max_rows_per_file)
            .map_ok(|batch| vec![batch])
            .boxed(),
    };
    let external_base_resolver = match version {
        ConcreteFileVersion::V2_2 | ConcreteFileVersion::V2_3 => {
            write::blob_v2_external_base_resolver(dataset, &params, schema).await?
        }
        ConcreteFileVersion::V1 | ConcreteFileVersion::V2_0 | ConcreteFileVersion::V2_1 => None,
    };
    write::do_write_fragments_impl(
        dataset,
        object_store,
        base_dir,
        schema,
        buffered_reader,
        params,
        move |object_store, schema, base_dir, options| async move {
            open_writer(version, &object_store, &schema, &base_dir, options).await
        },
        external_base_resolver,
        target_bases_info,
        seed_writers,
    )
    .await
}

pub async fn write_fragment(
    version: ConcreteFileVersion,
    builder: &FragmentCreateBuilder<'_>,
    stream: SendableRecordBatchStream,
    schema: Schema,
    id: u64,
) -> Result<Fragment> {
    match version {
        ConcreteFileVersion::V1 => builder.write_v1_impl(stream, schema, id).await,
        ConcreteFileVersion::V2_0
        | ConcreteFileVersion::V2_1
        | ConcreteFileVersion::V2_2
        | ConcreteFileVersion::V2_3 => {
            builder
                .write_current_impl(
                    move |object_writer, schema, filename| {
                        create_current_file_writer(version, object_writer, schema, filename, None)
                    },
                    stream,
                    schema,
                    id,
                )
                .await
        }
    }
}

pub async fn open_writer(
    version: ConcreteFileVersion,
    object_store: &ObjectStore,
    schema: &Schema,
    base_dir: &Path,
    options: WriterOptions,
) -> Result<Box<dyn write::GenericWriter>> {
    match version {
        ConcreteFileVersion::V1 => {
            write::open_v1_writer(object_store, schema, base_dir, options).await
        }
        ConcreteFileVersion::V2_0 | ConcreteFileVersion::V2_1 => {
            write::open_current_writer(
                move |object_writer, schema, filename, base_id| {
                    create_current_file_writer(version, object_writer, schema, filename, base_id)
                },
                object_store,
                schema,
                base_dir,
                options,
            )
            .await
        }
        ConcreteFileVersion::V2_2 | ConcreteFileVersion::V2_3 => {
            write::open_current_blob_v2_writer(
                move |object_writer, schema, filename, base_id| {
                    create_current_file_writer(version, object_writer, schema, filename, base_id)
                },
                object_store,
                schema,
                base_dir,
                options,
            )
            .await
        }
    }
}

pub async fn open_update_writer(
    version: ConcreteFileVersion,
    dataset: &Dataset,
    schema: &Schema,
) -> Result<Box<dyn write::GenericWriter>> {
    let external_base_resolver = match version {
        ConcreteFileVersion::V2_2 | ConcreteFileVersion::V2_3 => {
            write::blob_v2_external_base_resolver(Some(dataset), &WriteParams::default(), schema)
                .await?
        }
        ConcreteFileVersion::V1 | ConcreteFileVersion::V2_0 | ConcreteFileVersion::V2_1 => None,
    };
    open_writer(
        version,
        &dataset.object_store,
        schema,
        &dataset.base,
        WriterOptions::update(dataset.session.store_registry(), external_base_resolver),
    )
    .await
}
