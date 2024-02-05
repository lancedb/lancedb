// Copyright 2024 Lance Developers.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
