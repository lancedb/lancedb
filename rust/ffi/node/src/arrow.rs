// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use std::io::Cursor;
use std::ops::Deref;

use arrow_array::RecordBatch;
use arrow_ipc::reader::FileReader;
use arrow_ipc::writer::FileWriter;
use arrow_schema::SchemaRef;

use crate::error::Result;

pub fn arrow_buffer_to_record_batch(slice: &[u8]) -> Result<(Vec<RecordBatch>, SchemaRef)> {
    let mut batches: Vec<RecordBatch> = Vec::new();
    let file_reader = FileReader::try_new(Cursor::new(slice), None)?;
    let schema = file_reader.schema();
    for b in file_reader {
        let record_batch = b?;
        batches.push(record_batch);
    }
    Ok((batches, schema))
}

pub fn record_batch_to_buffer(batches: Vec<RecordBatch>) -> Result<Vec<u8>> {
    if batches.is_empty() {
        return Ok(Vec::new());
    }

    let schema = batches.first().unwrap().schema();
    let mut fr = FileWriter::try_new(Vec::new(), schema.deref())?;
    for batch in batches.iter() {
        fr.write(batch)?
    }
    fr.finish()?;
    Ok(fr.into_inner()?)
}
