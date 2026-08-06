// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::collections::BinaryHeap;
use std::sync::{Arc, LazyLock};
use std::time::Instant;
use std::{cmp::Reverse, pin::Pin};

use super::IVFIndex;
use crate::dataset::ROW_ID;
use crate::index::vector::pq::{PQIndex, build_pq_storage};
use arrow::compute::concat;
use arrow_array::{
    Array, FixedSizeListArray, PrimitiveArray, RecordBatch, UInt32Array, UInt64Array,
    cast::AsArray,
    types::{ArrowPrimitiveType, UInt8Type, UInt64Type},
};
use futures::stream::Peekable;
use futures::{Stream, StreamExt, TryStreamExt};
use lance_arrow::*;
use lance_core::Error;
use lance_core::datatypes::Schema;
use lance_core::traits::DatasetTakeRows;
use lance_core::utils::tempfile::TempStdDir;
use lance_core::utils::tokio::{get_num_compute_intensive_cpus, spawn_cpu};
use lance_file::versions::v1::reader::FileReader as V1FileReader;
use lance_file::versions::v1::writer::FileWriter as V1FileWriter;
use lance_index::metrics::NoOpMetricsCollector;
use lance_index::vector::hnsw::HNSW;
use lance_index::vector::hnsw::{HnswMetadata, builder::HnswBuildParams};
use lance_index::vector::ivf::storage::IvfModel;
use lance_index::vector::pq::ProductQuantizer;
use lance_index::vector::pq::storage::transpose;
use lance_index::vector::quantizer::{Quantization, Quantizer};
use lance_index::vector::v3::subindex::IvfSubIndex;
use lance_index::vector::{PART_ID_COLUMN, PQ_CODE_COLUMN};
use lance_io::ReadBatchParams;
use lance_io::object_store::ObjectStore;
use lance_io::traits::Writer;
use lance_linalg::distance::{DistanceType, MetricType};
use lance_linalg::kernels::normalize_fsl;
use lance_table::format::SelfDescribingFileReader;
use lance_table::io::manifest::ManifestDescribing;
use object_store::path::Path;
use tokio::io::AsyncWriteExt;
use tokio::sync::Semaphore;
use tokio::task::JoinHandle;

use crate::Result;

// TODO: make it configurable, limit by the number of CPU cores & memory
static HNSW_PARTITIONS_BUILD_PARALLEL: LazyLock<usize> =
    LazyLock::new(get_num_compute_intensive_cpus);

async fn write_primitive_values<T: ArrowPrimitiveType>(
    writer: &mut dyn Writer,
    array: &PrimitiveArray<T>,
) -> Result<()> {
    let data = array.to_data();
    let byte_width = std::mem::size_of::<T::Native>();
    let start = array.offset() * byte_width;
    let end = start + array.len() * byte_width;
    writer
        .write_all(&data.buffers()[0].as_slice()[start..end])
        .await?;
    Ok(())
}

async fn write_pq_codes(writer: &mut dyn Writer, arrays: &[&dyn Array]) -> Result<()> {
    for array in arrays {
        // Loaded legacy partitions expose transposed codes as flat UInt8 arrays,
        // while newly shuffled partitions supply FixedSizeList<UInt8>.
        if let Some(values) = array.as_any().downcast_ref::<PrimitiveArray<UInt8Type>>() {
            write_primitive_values(writer, values).await?;
        } else if let Some(codes) = array.as_any().downcast_ref::<FixedSizeListArray>() {
            let value_offset = codes.value_offset(0) as usize;
            let value_len = codes.len() * codes.value_length() as usize;
            let values = codes.values().slice(value_offset, value_len);
            let values = values
                .as_any()
                .downcast_ref::<PrimitiveArray<UInt8Type>>()
                .ok_or_else(|| {
                    Error::index(format!(
                        "legacy IVF PQ code values must be UInt8, found {}",
                        values.data_type()
                    ))
                })?;
            write_primitive_values(writer, values).await?;
        } else {
            return Err(Error::index(format!(
                "legacy IVF PQ codes must be UInt8 or FixedSizeList<UInt8>, found {}",
                array.data_type()
            )));
        }
    }
    Ok(())
}

async fn write_row_ids(writer: &mut dyn Writer, arrays: &[&dyn Array]) -> Result<()> {
    for array in arrays {
        let row_ids = array
            .as_any()
            .downcast_ref::<PrimitiveArray<UInt64Type>>()
            .ok_or_else(|| {
                Error::index(format!(
                    "legacy IVF row ids must be UInt64, found {}",
                    array.data_type()
                ))
            })?;
        write_primitive_values(writer, row_ids).await?;
    }
    Ok(())
}

pub(super) async fn write_pq_partition_payload(
    writer: &mut dyn Writer,
    pq_codes: &[&dyn Array],
    row_ids: &[&dyn Array],
) -> Result<()> {
    write_pq_codes(writer, pq_codes).await?;
    write_row_ids(writer, row_ids).await
}

/// Merge streams with the same partition id and collect PQ codes and row IDs.
async fn merge_streams(
    streams_heap: &mut BinaryHeap<(Reverse<u32>, usize)>,
    new_streams: &mut [Pin<Box<Peekable<impl Stream<Item = Result<RecordBatch>>>>>],
    part_id: u32,
    code_column: Option<&str>,
    code_array: &mut Vec<Arc<dyn Array>>,
    row_id_array: &mut Vec<Arc<dyn Array>>,
) -> Result<()> {
    while let Some((Reverse(stream_part_id), stream_idx)) = streams_heap.pop() {
        if stream_part_id != part_id {
            streams_heap.push((Reverse(stream_part_id), stream_idx));
            break;
        }

        let mut stream = new_streams[stream_idx].as_mut();
        let batch = match stream.next().await {
            Some(Ok(batch)) => batch,
            Some(Err(e)) => {
                return Err(Error::io(format!("failed to read batch: {}", e)));
            }
            None => {
                return Err(Error::io(
                    "failed to read batch: unexpected end of stream".to_string(),
                ));
            }
        };

        let row_ids: Arc<dyn Array> = Arc::new(
            batch
                .column_by_name(ROW_ID)
                .expect("row id column not found")
                .as_primitive::<UInt64Type>()
                .clone(),
        );
        row_id_array.push(row_ids);

        if let Some(column) = code_column {
            let codes = Arc::new(
                batch
                    .column_by_name(column)
                    .ok_or_else(|| Error::index(format!("code column {} not found", column)))?
                    .as_fixed_size_list()
                    .clone(),
            );
            code_array.push(codes);
        }

        match stream.peek().await {
            Some(Ok(batch)) => {
                let part_ids: &UInt32Array = batch
                    .column_by_name(PART_ID_COLUMN)
                    .expect("part id column not found")
                    .as_primitive();
                if !part_ids.is_empty() {
                    streams_heap.push((Reverse(part_ids.value(0)), stream_idx));
                }
            }
            Some(Err(e)) => {
                return Err(Error::io(format!(
                    "IVF Shuffler::failed to read batch: {}",
                    e
                )));
            }
            None => {}
        }
    }
    Ok(())
}

/// Write each partition of IVF_PQ index to the index file.
///
/// Parameters
/// ----------
/// `writer`: Index file writer.
/// `ivf`: IVF index to be written.
/// `streams`: RecordBatch stream of PQ codes and row ids, sorted by PQ code.
/// `existing_partitions`: Existing IVF indices to be merged. Can be zero or more.
///
/// These existing partitions must have the same centroids and PQ codebook.
///
/// TODO: migrate this function to `lance-index` crate.
pub(super) async fn write_pq_partitions(
    writer: &mut dyn Writer,
    ivf: &mut IvfModel,
    streams: Option<Vec<impl Stream<Item = Result<RecordBatch>>>>,
    existing_indices: Option<&[&IVFIndex]>,
) -> Result<()> {
    // build the initial heap
    // TODO: extract heap sort to a separate function.
    let mut streams_heap = BinaryHeap::new();
    let mut new_streams = vec![];

    if let Some(streams) = streams {
        for stream in streams {
            let mut stream = Box::pin(stream.peekable());

            match stream.as_mut().peek().await {
                Some(Ok(batch)) => {
                    let part_ids: &UInt32Array = batch
                        .column_by_name(PART_ID_COLUMN)
                        .expect("part id column not found")
                        .as_primitive();
                    let part_id = part_ids.values()[0];
                    streams_heap.push((Reverse(part_id), new_streams.len()));
                    new_streams.push(stream);
                }
                Some(Err(e)) => {
                    return Err(Error::io(format!("failed to read batch: {}", e)));
                }
                None => {
                    return Err(Error::io("failed to read batch: end of stream".to_string()));
                }
            }
        }
    }

    for part_id in 0..ivf.num_partitions() as u32 {
        let start = Instant::now();
        let mut pq_array: Vec<Arc<dyn Array>> = vec![];
        let mut row_id_array: Vec<Arc<dyn Array>> = vec![];

        if let Some(&previous_indices) = existing_indices.as_ref() {
            for &idx in previous_indices.iter() {
                let sub_index = idx
                    .load_partition(part_id as usize, true, &NoOpMetricsCollector)
                    .await?;
                let pq_index = sub_index
                    .as_any()
                    .downcast_ref::<PQIndex>()
                    .ok_or(Error::index("Invalid sub index".to_string()))?;
                if let Some(pq_code) = pq_index.code.as_ref() {
                    let row_ids = pq_index.row_ids.as_ref().unwrap();
                    let num_vectors = row_ids.len();
                    if num_vectors == 0 || pq_code.is_empty() {
                        continue;
                    }
                    if pq_code.len() % num_vectors != 0 {
                        continue;
                    }
                    let num_bytes_per_code = pq_code.len() / num_vectors;
                    let original_pq_codes = transpose(pq_code, num_bytes_per_code, num_vectors);
                    let fsl = Arc::new(
                        FixedSizeListArray::try_new_from_values(
                            original_pq_codes,
                            num_bytes_per_code as i32,
                        )
                        .unwrap(),
                    );

                    pq_array.push(fsl);
                    row_id_array.push(row_ids.clone());
                }
            }
        }

        // Merge all streams with the same partition id.
        merge_streams(
            &mut streams_heap,
            &mut new_streams,
            part_id,
            Some(PQ_CODE_COLUMN),
            &mut pq_array,
            &mut row_id_array,
        )
        .await?;

        let total_records = row_id_array.iter().map(|a| a.len()).sum::<usize>();
        ivf.add_partition_with_offset(writer.tell().await?, total_records as u32);
        if total_records > 0 {
            let pq_refs = pq_array.iter().map(|a| a.as_ref()).collect::<Vec<_>>();
            let row_ids_refs = row_id_array.iter().map(|a| a.as_ref()).collect::<Vec<_>>();
            write_pq_partition_payload(writer, &pq_refs, &row_ids_refs).await?;
        }
        log::info!(
            "Wrote partition {} in {} ms",
            part_id,
            start.elapsed().as_millis()
        );
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn write_hnsw_quantization_index_partitions(
    dataset: Arc<dyn DatasetTakeRows>,
    column: &str,
    distance_type: DistanceType,
    hnsw_params: &HnswBuildParams,
    writer: &mut V1FileWriter<ManifestDescribing>,
    mut auxiliary_writer: Option<&mut V1FileWriter<ManifestDescribing>>,
    ivf: &mut IvfModel,
    quantizer: Quantizer,
    streams: Option<Vec<impl Stream<Item = Result<RecordBatch>>>>,
    existing_indices: Option<&[&IVFIndex]>,
) -> Result<(Vec<HnswMetadata>, IvfModel)> {
    let hnsw_params = Arc::new(hnsw_params.clone());

    let mut streams_heap = BinaryHeap::new();
    let mut new_streams = vec![];
    if let Some(streams) = streams {
        for stream in streams {
            let mut stream = Box::pin(stream.peekable());

            match stream.as_mut().peek().await {
                Some(Ok(batch)) => {
                    let part_ids: &UInt32Array = batch
                        .column_by_name(PART_ID_COLUMN)
                        .expect("part id column not found")
                        .as_primitive();
                    let part_id = part_ids.values()[0];
                    streams_heap.push((Reverse(part_id), new_streams.len()));
                    new_streams.push(stream);
                }
                Some(Err(e)) => {
                    return Err(Error::io(format!("failed to read batch: {}", e)));
                }
                None => {
                    return Err(Error::io("failed to read batch: end of stream".to_string()));
                }
            }
        }
    }

    let object_store = ObjectStore::local();
    // Partitions are staged in this scratch dir, then merged into the final index.
    // Share the guard with every task via `Arc` so its `Drop` removes the dir only
    // after the last task finishes -- never while one is still writing.
    let tmp_part_dir_guard = Arc::new(TempStdDir::default());
    let tmp_part_dir = Path::from_filesystem_path(&**tmp_part_dir_guard)?;

    // `Option` per handle so the consume loop can `take()` each one, leaving the
    // not-yet-consumed handles for the error-path drain.
    let mut tasks: Vec<Option<JoinHandle<Result<usize>>>> =
        Vec::with_capacity(ivf.num_partitions());

    let build_result: Result<(Vec<HnswMetadata>, IvfModel)> = async {
        let mut part_files = Vec::with_capacity(ivf.num_partitions());
        let mut aux_part_files = Vec::with_capacity(ivf.num_partitions());
        let sem = Arc::new(Semaphore::new(*HNSW_PARTITIONS_BUILD_PARALLEL));
        for part_id in 0..ivf.num_partitions() {
            part_files.push(tmp_part_dir.clone().join(format!("hnsw_part_{}", part_id)));
            aux_part_files.push(
                tmp_part_dir
                    .clone()
                    .join(format!("hnsw_part_aux_{}", part_id)),
            );

            let mut code_array: Vec<Arc<dyn Array>> = vec![];
            let mut row_id_array: Vec<Arc<dyn Array>> = vec![];

            // We don't transform vectors to SQ codes while shuffling,
            // so we won't merge SQ codes from the stream.

            if let Some(&previous_indices) = existing_indices.as_ref() {
                for &idx in previous_indices.iter() {
                    let sub_index = idx
                        .load_partition(part_id, true, &NoOpMetricsCollector)
                        .await?;
                    let row_ids =
                        Arc::new(UInt64Array::from_iter_values(sub_index.row_ids().cloned()));
                    row_id_array.push(row_ids);
                }
            }

            let code_column = match &quantizer {
                Quantizer::Product(pq) => Some(pq.column()),
                _ => None,
            };
            merge_streams(
                &mut streams_heap,
                &mut new_streams,
                part_id as u32,
                code_column,
                &mut code_array,
                &mut row_id_array,
            )
            .await?;

            if row_id_array.is_empty() {
                tasks.push(Some(tokio::spawn(async { Ok(0) })));
                continue;
            }

            let (part_file, aux_part_file) = (&part_files[part_id], &aux_part_files[part_id]);
            let part_writer = V1FileWriter::<ManifestDescribing>::try_new(
                &object_store,
                part_file,
                Schema::try_from(writer.schema())?,
                &Default::default(),
            )
            .await?;

            let aux_part_writer = match auxiliary_writer.as_ref() {
                Some(writer) => Some(
                    V1FileWriter::<ManifestDescribing>::try_new(
                        &object_store,
                        aux_part_file,
                        Schema::try_from(writer.schema())?,
                        &Default::default(),
                    )
                    .await?,
                ),
                None => None,
            };

            let dataset = dataset.clone();
            let column = column.to_owned();
            let hnsw_params = hnsw_params.clone();
            let quantizer = quantizer.clone();
            let sem = sem.clone();
            let tmp_part_dir_guard = tmp_part_dir_guard.clone();
            tasks.push(Some(tokio::spawn(async move {
                // Hold a guard clone so the scratch dir stays alive while this task writes.
                let _tmp_part_dir_guard = tmp_part_dir_guard;
                let _permit = sem.acquire().await.map_err(|err| {
                    Error::io(format!(
                        "failed to acquire HNSW partition build permit: {err}"
                    ))
                })?;

                log::debug!("Building HNSW partition {}", part_id);
                let result = build_hnsw_quantization_partition(
                    dataset,
                    &column,
                    distance_type,
                    hnsw_params,
                    part_writer,
                    aux_part_writer,
                    quantizer,
                    row_id_array,
                    code_array,
                )
                .await;
                log::debug!("Finished building HNSW partition {}", part_id);
                result
            })));
        }

        let mut aux_ivf = IvfModel::empty();
        let mut hnsw_metadata = Vec::with_capacity(ivf.num_partitions());
        for (part_id, task) in tasks.iter_mut().enumerate() {
            let task = task
                .take()
                .expect("each partition task is consumed exactly once");
            let offset = writer.len();
            let num_rows = task.await??;

            if num_rows == 0 {
                ivf.add_partition(0);
                aux_ivf.add_partition(0);
                hnsw_metadata.push(HnswMetadata::default());
                continue;
            }

            let (part_file, aux_part_file) = (&part_files[part_id], &aux_part_files[part_id]);
            let part_reader =
                V1FileReader::try_new_self_described(&object_store, part_file, None).await?;

            let batches = futures::stream::iter(0..part_reader.num_batches())
                .map(|batch_id| {
                    part_reader.read_batch(
                        batch_id as i32,
                        ReadBatchParams::RangeFull,
                        part_reader.schema(),
                    )
                })
                .buffered(object_store.io_parallelism())
                .try_collect::<Vec<_>>()
                .await?;
            writer.write(&batches).await?;

            ivf.add_partition((writer.len() - offset) as u32);
            hnsw_metadata.push(serde_json::from_str(
                part_reader.schema().metadata[HNSW::metadata_key()].as_str(),
            )?);
            std::mem::drop(part_reader);
            object_store.delete(part_file).await?;

            if let Some(aux_writer) = auxiliary_writer.as_mut() {
                let aux_part_reader =
                    V1FileReader::try_new_self_described(&object_store, aux_part_file, None)
                        .await?;

                let batches = futures::stream::iter(0..aux_part_reader.num_batches())
                    .map(|batch_id| {
                        aux_part_reader.read_batch(
                            batch_id as i32,
                            ReadBatchParams::RangeFull,
                            aux_part_reader.schema(),
                        )
                    })
                    .buffered(object_store.io_parallelism())
                    .try_collect::<Vec<_>>()
                    .await?;
                std::mem::drop(aux_part_reader);
                object_store.delete(aux_part_file).await?;

                aux_writer.write(&batches).await?;
                aux_ivf.add_partition(num_rows as u32);
            }
        }

        Ok((hnsw_metadata, aux_ivf))
    }
    .await;

    // On error, abort and await the partition builds we never consumed so none
    // keep running in the background; see `drain_partition_tasks`.
    if build_result.is_err() {
        for err in drain_partition_tasks(&mut tasks).await {
            log::warn!(
                "HNSW partition build task failed while draining after an earlier error: {err}"
            );
        }
    }

    build_result
}

/// Abort and await every still-outstanding partition-build task, returning the
/// non-cancellation errors they surfaced.
///
/// A dropped [`JoinHandle`] detaches its task, so every handle is aborted up front
/// before any is awaited: otherwise a task slow to observe its own cancellation
/// would keep running -- and keep writing into the scratch dir -- while an earlier
/// handle is still being awaited. Awaiting then resolves only once each task has
/// actually stopped. Cancellation errors are the expected result of the abort and
/// dropped; failures and panics from tasks that had already finished before the
/// abort are returned so the caller can surface them (a task still in flight is
/// cancelled, so this best-effort drain only reports errors already produced).
async fn drain_partition_tasks(tasks: &mut [Option<JoinHandle<Result<usize>>>]) -> Vec<Error> {
    for task in tasks.iter() {
        if let Some(handle) = task.as_ref() {
            handle.abort();
        }
    }

    let mut errors = Vec::with_capacity(tasks.len());
    for task in tasks.iter_mut() {
        let Some(handle) = task.take() else {
            continue;
        };
        match handle.await {
            Ok(Ok(_)) => {}
            Ok(Err(e)) => errors.push(e),
            Err(join_err) if join_err.is_cancelled() => {}
            Err(join_err) => errors.push(Error::io(format!(
                "HNSW partition build task panicked: {join_err}"
            ))),
        }
    }
    errors
}

#[allow(clippy::too_many_arguments)]
async fn build_hnsw_quantization_partition(
    dataset: Arc<dyn DatasetTakeRows>,
    column: &str,
    metric_type: MetricType,
    hnsw_params: Arc<HnswBuildParams>,
    writer: V1FileWriter<ManifestDescribing>,
    aux_writer: Option<V1FileWriter<ManifestDescribing>>,
    quantizer: Quantizer,
    row_ids_array: Vec<Arc<dyn Array>>,
    code_array: Vec<Arc<dyn Array>>,
) -> Result<usize> {
    let row_ids_arrs = row_ids_array.iter().map(|a| a.as_ref()).collect::<Vec<_>>();
    let row_ids = concat(&row_ids_arrs)?;
    std::mem::drop(row_ids_array);
    let num_rows = row_ids.len();

    let projection = Arc::new(dataset.schema().project(&[column])?);
    let mut vectors = dataset
        .take_rows(row_ids.as_primitive::<UInt64Type>().values(), &projection)
        .await?
        .column_by_name(column.as_ref())
        .expect("row id column not found")
        .clone();

    let mut metric_type = metric_type;
    if metric_type == MetricType::Cosine {
        // Normalize vectors for cosine similarity
        vectors = Arc::new(spawn_cpu(move || normalize_fsl(vectors.as_fixed_size_list())).await?);
        metric_type = MetricType::L2;
    }

    let build_hnsw =
        build_and_write_hnsw(vectors.clone(), (*hnsw_params).clone(), metric_type, writer);

    // Build PQ storage as a child future, joined below: it writes `aux_writer`'s
    // file into the scratch dir, so it is cancelled together with this task when the
    // error-path drain aborts it, and the join surfaces its errors.
    let build_store = match quantizer {
        Quantizer::Flat(_) => {
            return Err(Error::index(
                "Flat quantizer is not supported for IVF_HNSW".to_string(),
            ));
        }
        Quantizer::Product(pq) => {
            let aux_writer = aux_writer.ok_or_else(|| {
                Error::index("IVF_HNSW_PQ requires an auxiliary writer for PQ storage".to_string())
            })?;
            build_and_write_pq_storage(metric_type, row_ids, code_array, pq, aux_writer)
        }
        _ => {
            return Err(Error::index(
                "IVF_HNSW_SQ is not supported in the legacy HNSW partition writer".to_string(),
            ));
        }
    };

    let (index_rows, ()) = futures::try_join!(build_hnsw, build_store)?;
    assert!(
        index_rows >= num_rows,
        "index rows {} must be greater than or equal to num rows {}",
        index_rows,
        num_rows
    );
    Ok(num_rows)
}

async fn build_and_write_hnsw(
    vectors: Arc<dyn Array>,
    params: HnswBuildParams,
    distance_type: DistanceType,
    mut writer: V1FileWriter<ManifestDescribing>,
) -> Result<usize> {
    let batch = params.build(vectors, distance_type).await?.to_batch()?;
    let metadata = batch.schema_ref().metadata().clone();
    writer.write(&[batch]).await?;
    Ok(writer.finish_with_metadata(&metadata).await?.num_rows as usize)
}

async fn build_and_write_pq_storage(
    metric_type: MetricType,
    row_ids: Arc<dyn Array>,
    code_array: Vec<Arc<dyn Array>>,
    pq: ProductQuantizer,
    mut writer: V1FileWriter<ManifestDescribing>,
) -> Result<()> {
    let storage = spawn_cpu(move || build_pq_storage(metric_type, row_ids, code_array, pq)).await?;

    writer.write(&[storage.batch().clone()]).await?;
    writer.finish().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::path::PathBuf;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    use crate::Dataset;
    use crate::index::vector::ivf::v2;
    use crate::index::{DatasetIndexExt, DatasetIndexInternalExt, vector::VectorIndexParams};
    use arrow_array::{RecordBatchIterator, UInt8Array};
    use arrow_schema::{Field, Schema};
    use lance_core::utils::tempfile::{TempObjFile, TempStrDir};
    use lance_index::IndexType;
    use lance_index::metrics::NoOpMetricsCollector;
    use lance_index::vector::ivf::IvfBuildParams;
    use lance_index::vector::pq::PQBuildParams;
    use lance_testing::datagen::generate_random_array;

    #[tokio::test]
    async fn pq_partition_payload_preserves_sliced_values() {
        let codes =
            FixedSizeListArray::try_new_from_values(UInt8Array::from_iter_values(0..6), 2).unwrap();
        let codes = codes.slice(1, 2);
        let flat_codes = UInt8Array::from_iter_values(6..9).slice(1, 2);
        let row_ids = UInt64Array::from_iter_values([10, 11, 12]).slice(1, 2);
        let flat_row_id = UInt64Array::from_iter_values([13]);

        let path = TempObjFile::default();
        let object_store = ObjectStore::local();
        let mut writer = object_store.create(&path).await.unwrap();
        write_pq_partition_payload(
            writer.as_mut(),
            &[&codes as &dyn Array, &flat_codes as &dyn Array],
            &[&row_ids as &dyn Array, &flat_row_id as &dyn Array],
        )
        .await
        .unwrap();
        Writer::shutdown(&mut writer).await.unwrap();

        let reader = object_store.open(&path).await.unwrap();
        let bytes = reader.get_range(0..30).await.unwrap();
        let mut expected = vec![2, 3, 4, 5, 7, 8];
        expected.extend_from_slice(&11_u64.to_le_bytes());
        expected.extend_from_slice(&12_u64.to_le_bytes());
        expected.extend_from_slice(&13_u64.to_le_bytes());
        assert_eq!(bytes.as_ref(), expected);
    }

    #[tokio::test]
    async fn test_merge_multiple_indices() {
        const DIM: usize = 32;
        const TOTAL: usize = 1024;
        let vector_values = generate_random_array(TOTAL * DIM);
        let fsl =
            Arc::new(FixedSizeListArray::try_new_from_values(vector_values, DIM as i32).unwrap());

        let schema = Arc::new(Schema::new(vec![Field::new(
            "vector",
            fsl.data_type().clone(),
            false,
        )]));
        let batch = RecordBatch::try_new(schema.clone(), vec![fsl.clone()]).unwrap();
        let batches =
            RecordBatchIterator::new(vec![batch.clone()].into_iter().map(Ok), schema.clone());

        let tmp_uri = TempStrDir::default();

        let mut ds = Dataset::write(batches, tmp_uri.as_str(), Default::default())
            .await
            .unwrap();

        let idx_params = VectorIndexParams::ivf_pq(2, 8, 2, MetricType::L2, 50);
        ds.create_index(&["vector"], IndexType::Vector, None, &idx_params, true)
            .await
            .unwrap();
        let indices = ds.load_indices().await.unwrap();
        assert_eq!(indices.len(), 1);
        assert_eq!(ds.get_fragments().len(), 1);

        let batches =
            RecordBatchIterator::new(vec![batch.clone()].into_iter().map(Ok), schema.clone());
        ds.append(batches, None).await.unwrap();
        let indices = ds.load_indices().await.unwrap();
        assert_eq!(indices.len(), 1);
        assert_eq!(ds.get_fragments().len(), 2);

        let idx = ds
            .open_vector_index("vector", &indices[0].uuid, &NoOpMetricsCollector)
            .await
            .unwrap();
        let _ivf_idx = idx
            .as_any()
            .downcast_ref::<v2::IvfPq>()
            .expect("Invalid index type");

        //let indices = /ds.
    }

    /// The scratch dir must outlive every partition task: dropping the caller-side
    /// guard while tasks are still in flight must not remove it, because each task
    /// still holds an `Arc` clone of the guard.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_scratch_dir_outlives_partition_tasks() {
        let tmp_part_dir_guard = Arc::new(TempStdDir::default());
        let scratch_path = tmp_part_dir_guard.to_path_buf();

        // Park each task until the caller-side guard is dropped, so the dir's
        // survival is attributable solely to the clones the tasks still hold.
        let running = Arc::new(AtomicUsize::new(0));
        let released = Arc::new(AtomicBool::new(false));

        const NUM_TASKS: usize = 3;
        let mut tasks = Vec::with_capacity(NUM_TASKS);
        for _ in 0..NUM_TASKS {
            let task_guard = tmp_part_dir_guard.clone();
            let running = running.clone();
            let released = released.clone();
            tasks.push(tokio::spawn(async move {
                running.fetch_add(1, Ordering::SeqCst);
                while !released.load(Ordering::SeqCst) {
                    tokio::task::yield_now().await;
                }
                // Still holding `task_guard`, so the directory must be live.
                task_guard.exists()
            }));
        }

        // Drop the caller-side guard only once every task is parked holding its clone.
        while running.load(Ordering::SeqCst) < NUM_TASKS {
            tokio::task::yield_now().await;
        }
        drop(tmp_part_dir_guard);
        assert!(
            scratch_path.exists(),
            "scratch dir removed while partition tasks still held the guard"
        );

        released.store(true, Ordering::SeqCst);
        for task in tasks {
            assert!(
                task.await.unwrap(),
                "a partition task observed its scratch dir already removed"
            );
        }
        assert!(
            !scratch_path.exists(),
            "scratch dir was not removed after the last task's guard clone dropped"
        );
    }

    /// The drain step must outlive every spawned task: it aborts and awaits each
    /// one so that, once it returns, no task is still running against the scratch
    /// directory. It must also surface late failures rather than swallow them.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_drain_partition_tasks_waits_and_reports_errors() {
        // Scratch dir guard analogous to the one held by
        // `write_hnsw_quantization_index_partitions`; tasks read from it, and it
        // is dropped only after the drain completes.
        let scratch_guard = TempStdDir::default();
        let scratch_path = scratch_guard.to_path_buf();

        // Count of live task futures. `LiveGuard` decrements on both completion and
        // cancellation, so a zero count after the drain proves every task terminated
        // rather than being detached.
        let live = Arc::new(AtomicUsize::new(0));
        let saw_missing_dir = Arc::new(AtomicBool::new(false));

        struct LiveGuard(Arc<AtomicUsize>);
        impl Drop for LiveGuard {
            fn drop(&mut self) {
                self.0.fetch_sub(1, Ordering::SeqCst);
            }
        }

        const NUM_SLOW: usize = 3;
        let mut tasks: Vec<Option<JoinHandle<Result<usize>>>> = Vec::with_capacity(NUM_SLOW + 1);

        // Tasks that never resolve on their own: the drain's abort is the only thing
        // that can stop them, which is exactly what this test exercises.
        for _ in 0..NUM_SLOW {
            let live = live.clone();
            let saw_missing_dir = saw_missing_dir.clone();
            let scratch_path = scratch_path.clone();
            tasks.push(Some(tokio::spawn(async move {
                live.fetch_add(1, Ordering::SeqCst);
                let _guard = LiveGuard(live.clone());
                if !scratch_path.exists() {
                    saw_missing_dir.store(true, Ordering::SeqCst);
                }
                futures::future::pending::<()>().await;
                Ok(0)
            })));
        }

        // A task that fails; its error must be returned, not silently dropped.
        // The drain aborts every handle up front, and an abort only preserves a
        // task's output if the task has already finished -- an in-flight task is
        // cancelled and its error lost. So wait until this task has actually
        // completed before handing it to the drain; otherwise whether its error
        // surfaces would depend on the scheduler and the test would be flaky.
        let failing =
            tokio::spawn(async move { Err(Error::io("late partition failure".to_string())) });
        while !failing.is_finished() {
            tokio::task::yield_now().await;
        }
        tasks.push(Some(failing));

        // Ensure all slow tasks are actually running before draining, so the
        // drain has to await their cancellation rather than aborting them before
        // they ever start.
        while live.load(Ordering::SeqCst) < NUM_SLOW {
            tokio::task::yield_now().await;
        }

        let errors = drain_partition_tasks(&mut tasks).await;

        assert!(tasks.iter().all(Option::is_none), "handles left undrained");
        assert_eq!(
            live.load(Ordering::SeqCst),
            0,
            "a task was still running after the drain returned"
        );
        assert!(
            !saw_missing_dir.load(Ordering::SeqCst),
            "scratch dir was removed while a task was still running"
        );
        assert_eq!(errors.len(), 1, "expected exactly the one late failure");
        assert!(
            errors[0].to_string().contains("late partition failure"),
            "late failure was not surfaced: {}",
            errors[0]
        );

        // The guard, not the drain, removes the scratch dir.
        assert!(scratch_path.exists());
        drop(scratch_guard);
        assert!(!scratch_path.exists());
    }

    /// `write_hnsw_quantization_index_partitions` stages each partition in a scratch
    /// directory owned by a [`TempStdDir`] guard, which must remove it once the build
    /// finishes so the OS temp dir does not grow without bound across legacy
    /// IVF_HNSW_* builds.
    ///
    /// The OS temp dir is process-global, so an in-process check can't attribute a
    /// leak to our own build. Instead we run the build in a child process with
    /// `TMPDIR` pointed at an isolated dir we own, then assert nothing survives.
    #[test]
    fn test_hnsw_pq_scratch_dir_is_not_leaked() {
        // Isolated temp root for the child. Owned here so it -- and anything the
        // child leaks into it -- is removed when this guard drops at test end.
        let isolated_root = TempStdDir::default();

        let child_test = "index::vector::ivf::io::tests::build_legacy_hnsw_pq_in_child_process";
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([child_test, "--exact", "--ignored", "--nocapture"])
            .env("TMPDIR", isolated_root.as_ref())
            .env("LANCE_HNSW_LEAK_TEST_ROOT", isolated_root.as_ref())
            .output()
            .expect("failed to spawn child test process");
        assert!(
            output.status.success(),
            "child build process failed:\n--- stdout ---\n{}\n--- stderr ---\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        );

        // Each build stages its partitions in one `.tmp*` dir directly under TMPDIR.
        // Every guard removes its dir when it drops, so none should survive.
        let leaked: Vec<PathBuf> = std::fs::read_dir(&isolated_root)
            .expect("read isolated temp root")
            .flatten()
            .map(|entry| entry.path())
            .filter(|path| {
                path.is_dir()
                    && path
                        .file_name()
                        .and_then(|name| name.to_str())
                        .is_some_and(|name| name.starts_with(".tmp"))
            })
            .collect();

        assert!(
            leaked.is_empty(),
            "legacy IVF_HNSW_PQ build leaked {} scratch director{} under the temp dir; \
             the TempStdDir guard should remove each one when it drops: {:?}",
            leaked.len(),
            if leaked.len() == 1 { "y" } else { "ies" },
            leaked,
        );
    }

    /// Child half of [`test_hnsw_pq_scratch_dir_is_not_leaked`]. Ignored so it only
    /// runs when the parent spawns it with `TMPDIR` and `LANCE_HNSW_LEAK_TEST_ROOT`
    /// pointed at an isolated dir. Builds a few legacy IVF_HNSW_PQ indices; the
    /// parent does the leak detection.
    #[tokio::test]
    #[ignore = "spawned as a child process by test_hnsw_pq_scratch_dir_is_not_leaked"]
    async fn build_legacy_hnsw_pq_in_child_process() {
        // Only do work when spawned by the parent; a bare `--ignored` run leaves
        // the variable unset, so no-op rather than fail.
        let Ok(root) = std::env::var("LANCE_HNSW_LEAK_TEST_ROOT") else {
            return;
        };

        const DIM: usize = 32;
        const ROWS: usize = 1024;
        const NLIST: usize = 4;
        const NUM_BUILDS: usize = 3;

        // Keep the dataset out of the temp dir's `.tmp*` namespace so the parent
        // never confuses it with a leaked scratch directory.
        let dataset_uri = format!("{root}/dataset");
        let values = generate_random_array(ROWS * DIM);
        let fsl = Arc::new(FixedSizeListArray::try_new_from_values(values, DIM as i32).unwrap());
        let schema = Arc::new(Schema::new(vec![Field::new(
            "vector",
            fsl.data_type().clone(),
            false,
        )]));
        let batch = RecordBatch::try_new(schema.clone(), vec![fsl]).unwrap();
        let batches = RecordBatchIterator::new(vec![Ok(batch)], schema.clone());
        let ds = Dataset::write(batches, &dataset_uri, Default::default())
            .await
            .unwrap();

        for _ in 0..NUM_BUILDS {
            crate::index::vector::ivf::build_ivf_hnsw_pq_index(
                &ds,
                "vector",
                "idx",
                uuid::Uuid::new_v4(),
                MetricType::L2,
                &IvfBuildParams::new(NLIST),
                &HnswBuildParams::default(),
                &PQBuildParams::new(4, 8),
            )
            .await
            .unwrap();
        }
    }
}
