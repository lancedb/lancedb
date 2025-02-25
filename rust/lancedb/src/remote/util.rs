// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use std::io::Cursor;

use arrow_array::RecordBatchReader;
use reqwest::Response;

use crate::Result;

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
