// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::collections::HashMap;
use std::ops::Range;
use std::sync::Arc;

use arrow::array::AsArray;
use arrow::datatypes::UInt64Type;
use arrow_array::{FixedSizeListArray, RecordBatch, UInt32Array, UInt64Array};
use arrow_schema::{DataType, Field as ArrowField};
use futures::{StreamExt, TryStreamExt};
use lance_arrow::{RecordBatchExt, SchemaExt};
use lance_core::utils::address::RowAddress;
use lance_core::utils::tokio::{get_num_compute_intensive_cpus, spawn_cpu};
use lance_file::versions::v1::writer::FileWriter as V1FileWriter;
use lance_file::writer::FileWriterOptions;
use lance_index::vector::PART_ID_COLUMN;
use lance_index::vector::pq::ProductQuantizer;
use lance_index::vector::quantizer::Quantizer;
use lance_index::vector::{ivf::storage::IvfModel, transform::Transformer};
use lance_io::stream::RecordBatchStreamAdapter;
use lance_table::io::manifest::ManifestDescribing;
use log::info;
use object_store::path::Path;
use tracing::instrument;

use lance_core::{Error, ROW_ID, Result, traits::DatasetTakeRows};
use lance_index::vector::{
    hnsw::{HnswMetadata, builder::HnswBuildParams},
    ivf::shuffler::shuffle_dataset,
};
use lance_io::{stream::RecordBatchStream, traits::Writer};
use lance_linalg::distance::{DistanceType, MetricType};

use crate::Dataset;
use crate::dataset::builder::DatasetBuilder;
use crate::index::vector::ivf::io::write_pq_partitions;

use super::io::write_hnsw_quantization_index_partitions;

/// Build specific partitions of IVF index.
///
///
#[allow(clippy::too_many_arguments)]
#[instrument(level = "debug", skip_all)]
pub(super) async fn build_partitions(
    writer: &mut dyn Writer,
    data: impl RecordBatchStream + Unpin + 'static,
    column: &str,
    ivf: &mut IvfModel,
    pq: ProductQuantizer,
    metric_type: MetricType,
    part_range: Range<u32>,
    precomputed_partitions: Option<HashMap<u64, u32>>,
    shuffle_partition_batches: usize,
    shuffle_partition_concurrency: usize,
    precomputed_shuffle_buffers: Option<(Path, Vec<String>)>,
) -> Result<()> {
    let schema = data.schema();
    if schema.column_with_name(column).is_none() {
        return Err(Error::schema(format!(
            "column {} does not exist in data stream",
            column
        )));
    }
    if schema.column_with_name(ROW_ID).is_none() {
        return Err(Error::schema(
            "ROW ID is not set when building index partitions".to_string(),
        ));
    }

    let ivf_transformer = lance_index::vector::ivf::IvfTransformer::with_pq(
        ivf.centroids.clone().unwrap(),
        metric_type,
        column,
        pq.clone(),
        Some(part_range),
    );

    let stream = shuffle_dataset(
        data,
        ivf_transformer.into(),
        precomputed_partitions,
        ivf.num_partitions() as u32,
        shuffle_partition_batches,
        shuffle_partition_concurrency,
        precomputed_shuffle_buffers,
    )
    .await?;

    write_pq_partitions(writer, ivf, Some(stream), None).await?;

    Ok(())
}

async fn load_precomputed_partitions(
    src_dataset: &Dataset,
    partitions_ds_uri: &str,
) -> Result<Vec<Vec<i32>>> {
    let builder = DatasetBuilder::from_uri(partitions_ds_uri);
    let ds = builder.load().await?;
    let stream = ds.scan().try_into_stream().await?;
    let lookup = src_dataset
        .fragments()
        .iter()
        .map(|frag| {
            vec![
                -1;
                frag.physical_rows
                    .expect("new index building API does not work with datasets this old")
            ]
        })
        .collect::<Vec<_>>();
    let partition_lookup = stream
        .try_fold(lookup, |mut lookup, batch| {
            let row_addrs: &UInt64Array = batch
                .column_by_name("row_id")
                .expect("malformed partition file: missing row_id column")
                .as_primitive();
            let partitions: &UInt32Array = batch
                .column_by_name("partition")
                .expect("malformed partition file: missing partition column")
                .as_primitive();
            row_addrs
                .values()
                .iter()
                .zip(partitions.values().iter())
                .for_each(|(row_id, partition)| {
                    let addr = RowAddress::from(*row_id);
                    lookup[addr.fragment_id() as usize][addr.row_offset() as usize] =
                        *partition as i32;
                });
            async move { Ok(lookup) }
        })
        .await?;
    Ok(partition_lookup)
}

#[instrument(level = "debug", skip_all)]
fn add_precomputed_partitions(
    batch: RecordBatch,
    partition_map: &[Vec<i32>],
    part_id_field: &ArrowField,
) -> Result<RecordBatch> {
    let row_ids = batch
        .column_by_name(ROW_ID)
        .ok_or(Error::index("column does not exist".to_string()))?;
    let part_ids = UInt32Array::from_iter_values(
        row_ids
            .as_primitive::<UInt64Type>()
            .values()
            .iter()
            .filter_map(|row_id| {
                let addr = RowAddress::from(*row_id);
                let part_id =
                    partition_map[addr.fragment_id() as usize][addr.row_offset() as usize];
                if part_id < 0 {
                    None
                } else {
                    Some(part_id as u32)
                }
            }),
    );
    let batch = batch
        .try_with_column(part_id_field.clone(), Arc::new(part_ids))
        .expect("failed to add part id column");
    Ok(batch)
}

async fn apply_precomputed_partitions(
    dataset: &Dataset,
    data: impl RecordBatchStream + Unpin + 'static,
    partitions_ds_uri: &str,
) -> Result<impl RecordBatchStream + Unpin + 'static> {
    let partition_map = load_precomputed_partitions(dataset, partitions_ds_uri).await?;
    let part_id_field = ArrowField::new(PART_ID_COLUMN, DataType::UInt32, true);
    let schema_with_part_id = Arc::new(
        data.schema()
            .as_ref()
            .clone()
            .try_with_column(part_id_field.clone())?,
    );
    let mapped = data.map(move |batch| {
        let batch = batch?;
        add_precomputed_partitions(batch, &partition_map, &part_id_field)
    });
    Ok(RecordBatchStreamAdapter::new(schema_with_part_id, mapped))
}

#[allow(clippy::too_many_arguments)]
pub async fn write_vector_storage(
    dataset: &Dataset,
    data: impl RecordBatchStream + Unpin + 'static,
    num_rows: u64,
    centroids: FixedSizeListArray,
    pq: ProductQuantizer,
    distance_type: DistanceType,
    column: &str,
    writer: Box<dyn Writer>,
    precomputed_partitions_ds_uri: Option<&str>,
) -> Result<()> {
    info!("Transforming {} vectors for storage", num_rows);
    let ivf_transformer = Arc::new(lance_index::vector::ivf::IvfTransformer::with_pq(
        centroids,
        distance_type,
        column,
        pq,
        None,
    ));

    let data = if let Some(partitions_ds_uri) = precomputed_partitions_ds_uri {
        apply_precomputed_partitions(dataset, data, partitions_ds_uri)
            .await?
            .boxed()
    } else {
        data.boxed()
    };

    let mut writer =
        lance_file::versions::v2_1::create_lazy_writer(writer, FileWriterOptions::default());
    let mut transformed_stream = data
        .map_ok(move |batch| {
            let ivf_transformer = ivf_transformer.clone();
            spawn_cpu(move || ivf_transformer.transform(&batch))
        })
        .try_buffer_unordered(get_num_compute_intensive_cpus());
    let mut total_rows_written = 0;
    while let Some(batch) = transformed_stream.next().await {
        let batch = batch?;
        total_rows_written += batch.num_rows();
        writer.write_batch(&batch).await?;
        info!("Transform progress: {}/{}", total_rows_written, num_rows);
    }
    writer.finish().await?;
    Ok(())
}

/// Build specific partitions of IVF index.
///
///
#[allow(clippy::too_many_arguments)]
#[instrument(level = "debug", skip(writer, auxiliary_writer, data, ivf, quantizer))]
pub(super) async fn build_hnsw_partitions(
    dataset: Arc<dyn DatasetTakeRows>,
    writer: &mut V1FileWriter<ManifestDescribing>,
    auxiliary_writer: Option<&mut V1FileWriter<ManifestDescribing>>,
    data: impl RecordBatchStream + Unpin + 'static,
    column: &str,
    ivf: &mut IvfModel,
    quantizer: Quantizer,
    metric_type: MetricType,
    hnsw_params: &HnswBuildParams,
    part_range: Range<u32>,
    precomputed_partitions: Option<HashMap<u64, u32>>,
    shuffle_partition_batches: usize,
    shuffle_partition_concurrency: usize,
    precomputed_shuffle_buffers: Option<(Path, Vec<String>)>,
) -> Result<(Vec<HnswMetadata>, IvfModel)> {
    let schema = data.schema();
    if schema.column_with_name(column).is_none() {
        return Err(Error::schema(format!(
            "column {} does not exist in data stream",
            column
        )));
    }
    if schema.column_with_name(ROW_ID).is_none() {
        return Err(Error::schema(
            "ROW ID is not set when building index partitions".to_string(),
        ));
    }

    let ivf_model = lance_index::vector::ivf::new_ivf_transformer_with_quantizer(
        ivf.centroids.clone().unwrap(),
        metric_type,
        column,
        quantizer.clone(),
        Some(part_range),
    )?;

    let stream = shuffle_dataset(
        data,
        ivf_model.into(),
        precomputed_partitions,
        ivf.num_partitions() as u32,
        shuffle_partition_batches,
        shuffle_partition_concurrency,
        precomputed_shuffle_buffers,
    )
    .await?;

    write_hnsw_quantization_index_partitions(
        dataset,
        column,
        metric_type,
        hnsw_params,
        writer,
        auxiliary_writer,
        ivf,
        quantizer,
        Some(stream),
        None,
    )
    .await
}
