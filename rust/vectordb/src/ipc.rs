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

//! IPC support

use std::io::Cursor;

use arrow_array::{RecordBatch, RecordBatchReader};
use arrow_ipc::{reader::StreamReader, writer::FileWriter};

use crate::{Error, Result};

/// Convert a Arrow IPC file to a batch reader
pub fn ipc_file_to_batches(buf: Vec<u8>) -> Result<impl RecordBatchReader> {
    let buf_reader = Cursor::new(buf);
    let reader = StreamReader::try_new(buf_reader, None)?;
    Ok(reader)
}

/// Convert record batches to Arrow IPC file
pub fn batches_to_ipc_file(batches: &[RecordBatch]) -> Result<Vec<u8>> {
    if batches.is_empty() {
        return Err(Error::Store {
            message: "No batches to write".to_string(),
        });
    }
    let schema = batches[0].schema();
    let mut writer = FileWriter::try_new(vec![], &schema)?;
    for batch in batches {
        writer.write(batch)?;
    }
    writer.finish()?;
    Ok(writer.into_inner()?)
}

#[cfg(test)]
mod tests {

    use super::*;
    use arrow_array::{Float32Array, Int64Array, RecordBatch};
    use arrow_ipc::writer::StreamWriter;
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

    fn create_record_batch() -> Result<RecordBatch> {
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Float32, false),
        ]);

        let a = Int64Array::from(vec![1, 2, 3, 4, 5]);
        let b = Float32Array::from(vec![1.1, 2.2, 3.3, 4.4, 5.5]);

        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(a), Arc::new(b)])?;

        Ok(batch)
    }

    #[test]
    fn test_ipc_file_to_batches() -> Result<()> {
        let batch = create_record_batch()?;

        let mut writer = StreamWriter::try_new(vec![], &batch.schema())?;
        writer.write(&batch)?;
        writer.finish()?;

        let buf = writer.into_inner().unwrap();
        let mut reader = ipc_file_to_batches(buf).unwrap();
        let read_batch = reader.next().unwrap()?;

        assert_eq!(batch.num_columns(), read_batch.num_columns());
        assert_eq!(batch.num_rows(), read_batch.num_rows());

        for i in 0..batch.num_columns() {
            let batch_column = batch.column(i);
            let read_batch_column = read_batch.column(i);

            assert_eq!(batch_column.data_type(), read_batch_column.data_type());
            assert_eq!(batch_column.len(), read_batch_column.len());
        }

        Ok(())
    }
}
