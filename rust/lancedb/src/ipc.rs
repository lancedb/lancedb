// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! IPC support

use std::{io::Cursor, sync::Arc};

use arrow_array::{RecordBatch, RecordBatchReader};
use arrow_ipc::{reader::FileReader, writer::FileWriter};
use arrow_schema::Schema;

use crate::{Error, Result};

/// Convert a Arrow IPC file to a batch reader
pub fn ipc_file_to_batches(buf: Vec<u8>) -> Result<impl RecordBatchReader> {
    let buf_reader = Cursor::new(buf);
    let reader = FileReader::try_new(buf_reader, None)?;
    Ok(reader)
}

/// Convert record batches to Arrow IPC file
pub fn batches_to_ipc_file(batches: &[RecordBatch]) -> Result<Vec<u8>> {
    if batches.is_empty() {
        return Err(Error::Other {
            message: "No batches to write".to_string(),
            source: None,
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

/// Convert a schema to an Arrow IPC file with 0 batches
pub fn schema_to_ipc_file(schema: &Schema) -> Result<Vec<u8>> {
    let mut writer = FileWriter::try_new(vec![], schema)?;
    writer.finish()?;
    Ok(writer.into_inner()?)
}

/// Retrieve the schema from an Arrow IPC file
pub fn ipc_file_to_schema(buf: Vec<u8>) -> Result<Arc<Schema>> {
    let buf_reader = Cursor::new(buf);
    let reader = FileReader::try_new(buf_reader, None)?;
    Ok(reader.schema())
}

#[cfg(test)]
mod tests {

    use super::*;
    use arrow_array::{Float32Array, Int64Array, RecordBatch};
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

        let mut writer = FileWriter::try_new(vec![], &batch.schema())?;
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
