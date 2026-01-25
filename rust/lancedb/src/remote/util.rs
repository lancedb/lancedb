// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use std::io::Cursor;

use arrow_array::RecordBatchReader;
use arrow_ipc::CompressionType;
use arrow_schema::ArrowError;
use futures::StreamExt;
use reqwest::Response;

use crate::{arrow::SendableRecordBatchStream, Result};

use super::db::ServerVersion;

pub fn batches_to_ipc_bytes(batches: impl RecordBatchReader) -> Result<Vec<u8>> {
    const WRITE_BUF_SIZE: usize = 4096;
    let buf = Vec::with_capacity(WRITE_BUF_SIZE);
    let mut buf = Cursor::new(buf);
    {
        let mut writer = arrow_ipc::writer::StreamWriter::try_new(&mut buf, &batches.schema())?;

        for batch in batches {
            let batch = batch?;
            writer.write(&batch)?;
        }
        writer.finish()?;
    }
    Ok(buf.into_inner())
}

pub fn stream_as_body(data: SendableRecordBatchStream) -> Result<reqwest::Body> {
    let options = arrow_ipc::writer::IpcWriteOptions::default()
        .try_with_compression(Some(CompressionType::LZ4_FRAME))?;
    let writer =
        arrow_ipc::writer::StreamWriter::try_new_with_options(Vec::new(), &data.schema(), options)?;
    let stream = futures::stream::try_unfold((data, writer), move |(mut data, mut writer)| {
        async move {
            match data.next().await {
                Some(Ok(batch)) => {
                    writer.write(&batch)?;
                    let buffer = std::mem::take(writer.get_mut());
                    Ok(Some((buffer, (data, writer))))
                }
                Some(Err(e)) => Err(e),
                None => {
                    if let Err(ArrowError::IpcError(_msg)) = writer.finish() {
                        // Will error if already closed.
                        return Ok(None);
                    };
                    let buffer = std::mem::take(writer.get_mut());
                    Ok(Some((buffer, (data, writer))))
                }
            }
        }
    });
    Ok(reqwest::Body::wrap_stream(stream))
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
