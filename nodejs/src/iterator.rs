use futures::StreamExt;
use lancedb::arrow::SendableRecordBatchStream;
use lancedb::ipc::batches_to_ipc_file;
use napi::bindgen_prelude::*;
use napi_derive::napi;

/** Typescript-style Async Iterator over RecordBatches */
#[napi]
pub struct RecordBatchIterator {
    inner: SendableRecordBatchStream,
}

#[napi]
impl RecordBatchIterator {
    pub(crate) fn new(inner: SendableRecordBatchStream) -> Self {
        Self { inner }
    }

    #[napi(catch_unwind)]
    pub async unsafe fn next(&mut self) -> napi::Result<Option<Buffer>> {
        if let Some(rst) = self.inner.next().await {
            let batch = rst.map_err(|e| {
                napi::Error::from_reason(format!("Failed to get next batch from stream: {}", e))
            })?;
            batches_to_ipc_file(&[batch])
                .map_err(|e| napi::Error::from_reason(format!("Failed to write IPC file: {}", e)))
                .map(|buf| Some(Buffer::from(buf)))
        } else {
            // We are done with the stream.
            Ok(None)
        }
    }
}
