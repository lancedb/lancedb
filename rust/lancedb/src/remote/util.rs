use std::io::Cursor;

use arrow_array::RecordBatchReader;

use crate::Result;

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
