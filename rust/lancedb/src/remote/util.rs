// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use arrow_array::RecordBatch;
use arrow_ipc::CompressionType;
use futures::{Stream, StreamExt};
use reqwest::Response;

use crate::{Result, arrow::SendableRecordBatchStream};

use super::db::ServerVersion;

/// Split batches before IPC encoding, preserving row order and allowing a single
/// oversized row. Slices share the source buffers; encoding only copies each slice.
pub(super) fn split_batch(batch: RecordBatch, max_bytes: usize) -> Vec<RecordBatch> {
    let mut pending = vec![batch];
    let mut batches = Vec::new();
    while let Some(batch) = pending.pop() {
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
            batches.push(batch);
        }
    }
    batches
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
    let data = data.flat_map(|batch| {
        futures::stream::iter(match batch {
            Ok(batch) => split_batch(batch, IPC_BATCH_BYTES)
                .into_iter()
                .map(Ok)
                .collect(),
            Err(err) => vec![Err(err)],
        })
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
