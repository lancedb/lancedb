// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use arrow_array::{
    Array, ArrayRef, GenericListViewArray, OffsetSizeTrait, RecordBatch, UInt64Array,
    cast::AsArray, make_array,
};
use arrow_buffer::ScalarBuffer;
use arrow_data::ArrayData;
use arrow_ipc::CompressionType;
use arrow_schema::{ArrowError, DataType, Field};
use arrow_select::{dictionary::garbage_collect_any_dictionary, take::take};
use futures::{Stream, StreamExt};
use reqwest::Response;

use crate::{Result, arrow::SendableRecordBatchStream};

use super::db::ServerVersion;

fn has_dictionary(data: &ArrayData) -> bool {
    matches!(data.data_type(), DataType::Dictionary(_, _))
        || data.child_data().iter().any(has_dictionary)
}

fn has_list_view(data: &ArrayData) -> bool {
    matches!(
        data.data_type(),
        DataType::ListView(_) | DataType::LargeListView(_)
    ) || data.child_data().iter().any(has_list_view)
}

// Arrow take on ordinary lists copies nested ListView ranges through
// MutableArrayData, losing sharing. Select child row IDs instead, then apply
// that selection recursively to the real children.
fn take_dictionary_rows(
    array: &dyn Array,
    indices: &UInt64Array,
) -> std::result::Result<ArrayRef, ArrowError> {
    let data = array.to_data();
    if !has_list_view(&data) {
        return take(array, indices, None);
    }
    let item = || std::sync::Arc::new(Field::new_list_field(DataType::UInt64, false));
    let proxy_type = match data.data_type() {
        DataType::List(_) | DataType::Map(_, _) => DataType::List(item()),
        DataType::LargeList(_) => DataType::LargeList(item()),
        DataType::FixedSizeList(_, size) => DataType::FixedSizeList(item(), *size),
        DataType::Struct(fields) => DataType::Struct(
            fields
                .iter()
                .map(|f| f.as_ref().clone().with_data_type(DataType::UInt64))
                .collect(),
        ),
        // ListView take already preserves sharing; its actual child selection
        // is performed by compact_list_view before dictionary collection.
        _ => return take(array, indices, None),
    };
    let children = data
        .child_data()
        .iter()
        .map(|child| UInt64Array::from_iter_values(0..child.len() as u64).to_data())
        .collect();
    let proxy = make_array(
        data.clone()
            .into_builder()
            .data_type(proxy_type)
            .child_data(children)
            .build()?,
    );
    let selected = take(proxy.as_ref(), indices, None)?.to_data();
    let children = data
        .child_data()
        .iter()
        .zip(selected.child_data())
        .map(|(child, ids)| {
            let ids = UInt64Array::from(ids.clone());
            Ok(take_dictionary_rows(make_array(child.clone()).as_ref(), &ids)?.to_data())
        })
        .collect::<std::result::Result<Vec<_>, ArrowError>>()?;
    Ok(make_array(
        selected
            .into_builder()
            .data_type(data.data_type().clone())
            .child_data(children)
            .build()?,
    ))
}

fn compact_list_view<O: OffsetSizeTrait>(
    views: &GenericListViewArray<O>,
) -> std::result::Result<ArrayData, ArrowError> {
    let mut ranges: Vec<_> = (0..views.len())
        .filter_map(|row| {
            let start = views.value_offsets()[row].as_usize();
            let size = views.value_sizes()[row].as_usize();
            (views.is_valid(row) && size != 0).then(|| (start, start + size))
        })
        .collect();
    ranges.sort_unstable();
    let mut union: Vec<(usize, usize, usize)> = Vec::with_capacity(ranges.len());
    let mut len = 0;
    for (start, end) in ranges {
        if let Some(last) = union.last_mut().filter(|last| start <= last.1) {
            let end = end.max(last.1);
            len += end - last.1;
            last.1 = end;
        } else {
            union.push((start, end, len));
            len += end - start;
        }
    }
    let mut offsets = Vec::with_capacity(views.len());
    let mut sizes = Vec::with_capacity(views.len());
    for row in 0..views.len() {
        let size = views.value_sizes()[row];
        if views.is_null(row) || size.as_usize() == 0 {
            offsets.push(O::usize_as(0));
            sizes.push(O::usize_as(0));
        } else {
            let start = views.value_offsets()[row].as_usize();
            let range = union[union.partition_point(|r| r.0 <= start) - 1];
            // Removing gaps cannot increase a valid original offset.
            offsets.push(O::from_usize(range.2 + start - range.0).unwrap());
            sizes.push(size);
        }
    }
    // Each child position is selected once, even for unsorted, overlapping views.
    let indices = UInt64Array::from_iter_values(
        union
            .iter()
            .flat_map(|&(start, end, _)| (start..end).map(|i| i as u64)),
    );
    let child = take_dictionary_rows(views.values().as_ref(), &indices)?;
    views
        .to_data()
        .into_builder()
        .offset(0)
        .buffers(vec![
            ScalarBuffer::from(offsets).into_inner(),
            ScalarBuffer::from(sizes).into_inner(),
        ])
        .child_data(vec![child.to_data()])
        .build()
}

fn compact_dictionary_data(data: ArrayData) -> std::result::Result<ArrayData, ArrowError> {
    let data = match data.data_type() {
        DataType::Dictionary(_, _) => {
            let array = make_array(data);
            garbage_collect_any_dictionary(array.as_any_dictionary())?.to_data()
        }
        DataType::ListView(_) => compact_list_view(make_array(data).as_list_view::<i32>())?,
        DataType::LargeListView(_) => compact_list_view(make_array(data).as_list_view::<i64>())?,
        // Selecting dictionary values retains view backing buffers, which IPC
        // otherwise serializes in full even when most views are unreferenced.
        DataType::Utf8View => make_array(data).as_string_view().gc().to_data(),
        DataType::BinaryView => make_array(data).as_binary_view().gc().to_data(),
        _ => data,
    };
    let children = data
        .child_data()
        .iter()
        .cloned()
        .map(compact_dictionary_data)
        .collect::<std::result::Result<Vec<_>, _>>()?;
    data.into_builder().child_data(children).build()
}

/// Compact dictionaries retained by row slices without changing the schema.
/// Row slices (and `take` alone) retain the original dictionary lookup tables.
/// Struct/fixed-size-list validity does not mask child slots, so values hidden
/// only by those ancestor nulls may remain referenced.
pub(super) fn compact_batch_dictionaries(
    batch: RecordBatch,
) -> std::result::Result<RecordBatch, ArrowError> {
    let columns = batch
        .columns()
        .iter()
        .map(|array| {
            let data = array.to_data();
            if !has_dictionary(&data) {
                return Ok(array.clone());
            }
            // Normalize container offsets and select their child rows before
            // collecting dictionaries nested in lists/structs/maps.
            let data = if matches!(data.data_type(), DataType::Dictionary(_, _)) {
                data
            } else {
                let indices = UInt64Array::from_iter_values(0..array.len() as u64);
                take_dictionary_rows(array.as_ref(), &indices)?.to_data()
            };
            Ok(make_array(compact_dictionary_data(data)?))
        })
        .collect::<std::result::Result<Vec<ArrayRef>, ArrowError>>()?;
    RecordBatch::try_new_with_options(
        batch.schema(),
        columns,
        &arrow_array::RecordBatchOptions::new().with_row_count(Some(batch.num_rows())),
    )
}

/// Split batches before IPC encoding, preserving row order and allowing a single
/// oversized row. Compact each dictionary before measuring or splitting it so
/// descendants never repeatedly scan or encode the full source dictionary.
/// Yield slices on demand so compacted dictionary copies are not all retained.
pub(super) fn split_batch(
    batch: RecordBatch,
    max_bytes: usize,
) -> impl Iterator<Item = std::result::Result<RecordBatch, ArrowError>> {
    let mut pending = vec![batch];
    std::iter::from_fn(move || {
        while let Some(batch) = pending.pop() {
            let batch = match compact_batch_dictionaries(batch) {
                Ok(batch) => batch,
                Err(err) => {
                    pending.clear();
                    return Some(Err(err));
                }
            };
            let size: usize = batch
                .columns()
                .iter()
                .map(|array| {
                    array
                        .to_data()
                        .get_slice_memory_size()
                        .unwrap_or_else(|_| array.get_buffer_memory_size())
                })
                .sum();
            if size > max_bytes && batch.num_rows() > 1 {
                let middle = batch.num_rows() / 2;
                pending.push(batch.slice(middle, batch.num_rows() - middle));
                pending.push(batch.slice(0, middle));
            } else {
                return Some(Ok(batch));
            }
        }
        None
    })
}

pub(super) const IPC_BATCH_BYTES: usize = 8 * 1024 * 1024;
pub(super) const MAX_CREATE_BYTES: usize = 256 * 1024 * 1024;

/// Validate the entire create payload before issuing its single atomic request.
/// A truncated IPC stream can be accepted as EOF by Arrow readers, so failing a
/// live request body after exceeding the budget cannot guarantee atomic failure.
/// Spooling bounds memory while ensuring an oversized input never reaches create.
pub(super) async fn create_body(data: SendableRecordBatchStream) -> Result<reqwest::Body> {
    use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
    let io_error = |err: std::io::Error| crate::Error::Other {
        message: "Failed to prepare atomic remote create payload".into(),
        source: Some(Box::new(err)),
    };
    let file = tempfile::tempfile().map_err(io_error)?;
    let mut file = tokio::fs::File::from_std(file);
    let mut stream = Box::pin(stream_as_ipc(data)?);
    let mut bytes = 0usize;
    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        bytes = bytes.saturating_add(chunk.len());
        if bytes > MAX_CREATE_BYTES {
            return Err(crate::Error::InvalidInput {
                message: format!(
                    "Remote create_table exceeds the {MAX_CREATE_BYTES}-byte single-request budget. \
                     Create the table with db.create_table(name, schema=...), then upload with \
                     table.add(data, progress=True)."
                ),
            });
        }
        file.write_all(&chunk).await.map_err(io_error)?;
    }
    file.rewind().await.map_err(io_error)?;
    let stream = futures::stream::try_unfold(file, |mut file| async move {
        let mut chunk = vec![0; 64 * 1024];
        let len = file.read(&mut chunk).await?;
        chunk.truncate(len);
        Ok::<_, std::io::Error>((len != 0).then_some((chunk, file)))
    });
    Ok(reqwest::Body::wrap_stream(stream))
}

pub fn stream_as_ipc(
    data: SendableRecordBatchStream,
) -> Result<impl Stream<Item = Result<bytes::Bytes>>> {
    let options = arrow_ipc::writer::IpcWriteOptions::default()
        .try_with_compression(Some(CompressionType::LZ4_FRAME))?;
    const WRITE_BUF_SIZE: usize = 4096;
    let buf = Vec::with_capacity(WRITE_BUF_SIZE);
    let writer =
        arrow_ipc::writer::StreamWriter::try_new_with_options(buf, &data.schema(), options)?;
    let data = data.flat_map(|batch| match batch {
        Ok(batch) => futures::stream::iter(
            split_batch(batch, IPC_BATCH_BYTES).map(|batch| batch.map_err(Into::into)),
        )
        .left_stream(),
        Err(err) => futures::stream::iter([Err(err)]).right_stream(),
    });
    let stream = futures::stream::try_unfold(
        (data, writer, false),
        move |(mut data, mut writer, finished)| async move {
            if finished {
                return Ok(None);
            }
            match data.next().await {
                Some(Ok(batch)) => {
                    writer.write(&batch)?;
                    let buffer = std::mem::take(writer.get_mut());
                    Ok(Some((bytes::Bytes::from(buffer), (data, writer, false))))
                }
                Some(Err(e)) => Err(e),
                None => {
                    writer.finish()?;
                    let buffer = std::mem::take(writer.get_mut());
                    Ok(Some((bytes::Bytes::from(buffer), (data, writer, true))))
                }
            }
        },
    );
    Ok(stream)
}

pub fn parse_server_version(req_id: &str, rsp: &Response) -> Result<ServerVersion> {
    let version = rsp
        .headers()
        .get("phalanx-version")
        .map(|v| {
            let v = v.to_str().map_err(|e| crate::Error::Http {
                source: e.into(),
                request_id: req_id.to_string(),
                status_code: Some(rsp.status()),
            })?;
            ServerVersion::parse(v).map_err(|e| crate::Error::Http {
                source: e.into(),
                request_id: req_id.to_string(),
                status_code: Some(rsp.status()),
            })
        })
        .transpose()?
        .unwrap_or_default();
    Ok(version)
}
