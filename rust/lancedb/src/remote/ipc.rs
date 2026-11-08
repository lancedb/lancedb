// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Incremental decoding of HTTP responses with Arrow IPC stream framing.

use arrow_array::RecordBatch;
use arrow_buffer::Buffer;
use arrow_ipc::reader::StreamDecoder;
use arrow_schema::ArrowError;
use datafusion_common::DataFusionError;
use datafusion_physical_plan::SendableRecordBatchStream;
use datafusion_physical_plan::stream::RecordBatchStreamAdapter;
use futures::{Stream, StreamExt};
use std::pin::Pin;
use tokio::time::Instant;

use super::client::RequestResultExt;
use crate::{Error, Result};

type BodyStream = Pin<Box<dyn Stream<Item = reqwest::Result<bytes::Bytes>> + Send>>;

struct Decoder {
    body: BodyStream,
    decoder: StreamDecoder,
    buffer: Buffer,
    first_batch: Option<RecordBatch>,
    request_id: String,
    deadline: Option<Instant>,
}

impl Decoder {
    async fn next_chunk(&mut self) -> Result<bool> {
        let next = if let Some(deadline) = self.deadline {
            tokio::time::timeout_at(deadline, self.body.next())
                .await
                .map_err(|_| Error::Other {
                    message: "Query timeout while reading Arrow response".into(),
                    source: None,
                })?
        } else {
            self.body.next().await
        };
        match next {
            Some(chunk) => {
                self.buffer = Buffer::from(chunk.err_to_http(self.request_id.clone())?);
                Ok(true)
            }
            None => {
                self.decoder.finish()?;
                Ok(false)
            }
        }
    }

    async fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        if self
            .deadline
            .is_some_and(|deadline| Instant::now() >= deadline)
        {
            return Err(Error::Other {
                message: "Query timeout while reading Arrow response".into(),
                source: None,
            });
        }
        if let Some(batch) = self.first_batch.take() {
            return Ok(Some(batch));
        }
        loop {
            if !self.buffer.is_empty() {
                if let Some(batch) = self.decoder.decode(&mut self.buffer)? {
                    return Ok(Some(batch));
                }
            } else if !self.next_chunk().await? {
                return Ok(None);
            }
        }
    }
}

/// Read only far enough to obtain a schema, then decode batches as they are polled.
/// The optional deadline applies to the entire query, including body consumption.
pub(super) async fn read_arrow_stream(
    response: reqwest::Response,
    request_id: &str,
    deadline: Option<Instant>,
) -> Result<SendableRecordBatchStream> {
    let mut state = Decoder {
        body: Box::pin(response.bytes_stream()),
        decoder: StreamDecoder::new(),
        buffer: Buffer::from(Vec::<u8>::new()),
        first_batch: None,
        request_id: request_id.to_owned(),
        deadline,
    };
    // The schema can span HTTP chunks. decode() may return the first batch in
    // the same call that discovers the schema, so retain it for the caller.
    let schema = loop {
        if !state.buffer.is_empty() {
            state.first_batch = state.decoder.decode(&mut state.buffer)?;
            if let Some(schema) = state.decoder.schema() {
                break schema;
            }
        } else if !state.next_chunk().await? {
            return Err(ArrowError::IpcError("Missing schema in Arrow IPC stream".into()).into());
        }
    };
    let batches = futures::stream::try_unfold(state, |mut state| async move {
        state
            .next_batch()
            .await
            .map(|batch| batch.map(|batch| (batch, state)))
            .map_err(|err| DataFusionError::External(Box::new(err)))
    });
    Ok(Box::pin(RecordBatchStreamAdapter::new(schema, batches)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int32Array, RecordBatch};
    use arrow_ipc::writer::StreamWriter;
    use arrow_schema::{DataType, Field, Schema};
    use bytes::Bytes;
    use futures::{TryStreamExt, stream};
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::sync::oneshot;

    fn batch(value: i32) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)])),
            vec![Arc::new(Int32Array::from(vec![value]))],
        )
        .unwrap()
    }

    fn payload() -> (Vec<u8>, usize) {
        let first = batch(1);
        let mut writer = StreamWriter::try_new(Vec::new(), &first.schema()).unwrap();
        writer.write(&first).unwrap();
        let split = writer.get_ref().len();
        writer.write(&batch(2)).unwrap();
        writer.finish().unwrap();
        (writer.into_inner().unwrap(), split)
    }

    fn response(body: reqwest::Body) -> reqwest::Response {
        http::Response::builder()
            .status(200)
            .body(body)
            .unwrap()
            .into()
    }

    #[tokio::test]
    async fn yields_first_batch_before_the_body_is_complete() {
        let (data, split) = payload();
        let head = Bytes::copy_from_slice(&data[..split]);
        let tail = Bytes::copy_from_slice(&data[split..]);
        let (release, wait) = oneshot::channel::<()>();
        let body = stream::once(async move { Ok::<_, std::io::Error>(head) }).chain(stream::once(
            async move {
                wait.await.unwrap();
                Ok::<_, std::io::Error>(tail)
            },
        ));
        let mut batches = tokio::time::timeout(
            Duration::from_secs(1),
            read_arrow_stream(
                response(reqwest::Body::wrap_stream(body)),
                "request-1",
                None,
            ),
        )
        .await
        .expect("schema must not wait for the whole HTTP body")
        .unwrap();
        let first = tokio::time::timeout(Duration::from_secs(1), batches.try_next())
            .await
            .expect("first batch must not wait for the whole HTTP body")
            .unwrap()
            .unwrap();
        assert_eq!(first, batch(1));
        release.send(()).unwrap();
        assert_eq!(batches.try_next().await.unwrap().unwrap(), batch(2));
        assert!(batches.try_next().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn handles_byte_sized_chunks() {
        let (data, _) = payload();
        let body = stream::iter(
            data.into_iter()
                .map(|byte| Ok::<_, std::io::Error>(Bytes::from(vec![byte]))),
        );
        let batches = read_arrow_stream(
            response(reqwest::Body::wrap_stream(body)),
            "request-2",
            None,
        )
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
        assert_eq!(batches, vec![batch(1), batch(2)]);
    }

    #[tokio::test]
    async fn preserves_dictionary_batches_across_chunk_boundaries() {
        use arrow_array::{Array, builder::StringDictionaryBuilder, types::Int32Type};
        let dictionary_batch = |value: &str| {
            let mut builder = StringDictionaryBuilder::<Int32Type>::new();
            builder.append(value).unwrap();
            let values = builder.finish();
            RecordBatch::try_new(
                Arc::new(Schema::new(vec![Field::new(
                    "word",
                    values.data_type().clone(),
                    false,
                )])),
                vec![Arc::new(values)],
            )
            .unwrap()
        };
        let first = dictionary_batch("alpha");
        let second = dictionary_batch("beta");
        let mut writer = StreamWriter::try_new(Vec::new(), &first.schema()).unwrap();
        writer.write(&first).unwrap();
        writer.write(&second).unwrap();
        writer.finish().unwrap();
        let body = stream::iter(
            writer
                .into_inner()
                .unwrap()
                .into_iter()
                .map(|byte| Ok::<_, std::io::Error>(Bytes::from(vec![byte]))),
        );
        let batches = read_arrow_stream(
            response(reqwest::Body::wrap_stream(body)),
            "dictionary-request",
            None,
        )
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
        assert_eq!(batches, vec![first, second]);
    }

    #[tokio::test]
    async fn decodes_compressed_stream_batches() {
        use arrow_ipc::{CompressionType, writer::IpcWriteOptions};
        let first = batch(1);
        let options = IpcWriteOptions::default()
            .try_with_compression(Some(CompressionType::LZ4_FRAME))
            .unwrap();
        let mut writer =
            StreamWriter::try_new_with_options(Vec::new(), &first.schema(), options).unwrap();
        writer.write(&first).unwrap();
        writer.finish().unwrap();
        let batches = read_arrow_stream(
            response(writer.into_inner().unwrap().into()),
            "compressed-request",
            None,
        )
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
        assert_eq!(batches, vec![first]);
    }

    #[tokio::test]
    async fn retains_multiple_batches_in_one_chunk() {
        let (data, _) = payload();
        let batches = read_arrow_stream(response(data.into()), "request-3", None)
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(batches, vec![batch(1), batch(2)]);
    }

    #[tokio::test]
    async fn handles_schema_only_streams() {
        let mut writer = StreamWriter::try_new(Vec::new(), &batch(1).schema()).unwrap();
        writer.finish().unwrap();
        let mut batches = read_arrow_stream(
            response(writer.into_inner().unwrap().into()),
            "request-4",
            None,
        )
        .await
        .unwrap();
        assert_eq!(batches.schema(), batch(1).schema());
        assert!(batches.try_next().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn rejects_incomplete_schema() {
        let (data, _) = payload();
        assert!(
            read_arrow_stream(response(data[..12].to_vec().into()), "request-5", None)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn rejects_truncated_batch_after_yielding_an_earlier_batch() {
        let (mut data, split) = payload();
        data.truncate(split + 12);
        let mut batches = read_arrow_stream(response(data.into()), "request-6", None)
            .await
            .unwrap();
        assert_eq!(batches.try_next().await.unwrap().unwrap(), batch(1));
        let err = batches.try_next().await.unwrap_err();
        assert!(
            err.to_string().contains("Unexpected End of Stream"),
            "{err}"
        );
    }

    #[tokio::test]
    async fn preserves_request_id_on_body_errors() {
        let (data, split) = payload();
        let body = stream::iter(vec![
            Ok(Bytes::copy_from_slice(&data[..split])),
            Err(std::io::Error::other("broken transport")),
        ]);
        let mut batches = read_arrow_stream(
            response(reqwest::Body::wrap_stream(body)),
            "body-error-request",
            None,
        )
        .await
        .unwrap();
        assert_eq!(batches.try_next().await.unwrap().unwrap(), batch(1));
        let err = batches.try_next().await.unwrap_err();
        assert!(err.to_string().contains("body-error-request"), "{err}");
    }

    #[tokio::test]
    async fn deadline_applies_to_already_buffered_batches() {
        let (data, _) = payload();
        let mut batches = read_arrow_stream(
            response(data.into()),
            "buffered-timeout",
            Some(Instant::now() + Duration::from_millis(20)),
        )
        .await
        .unwrap();
        tokio::time::sleep(Duration::from_millis(30)).await;
        let err = batches.try_next().await.unwrap_err();
        assert!(err.to_string().contains("Query timeout"), "{err}");
    }

    #[tokio::test]
    async fn deadline_still_applies_after_the_first_batch() {
        let (data, split) = payload();
        let body =
            stream::once(
                async move { Ok::<_, std::io::Error>(Bytes::copy_from_slice(&data[..split])) },
            )
            .chain(stream::pending());
        let mut batches = read_arrow_stream(
            response(reqwest::Body::wrap_stream(body)),
            "request-timeout",
            Some(Instant::now() + Duration::from_millis(50)),
        )
        .await
        .unwrap();
        assert_eq!(batches.try_next().await.unwrap().unwrap(), batch(1));
        let err = tokio::time::timeout(Duration::from_secs(1), batches.try_next())
            .await
            .expect("body must not hang past the query deadline")
            .unwrap_err();
        assert!(err.to_string().contains("Query timeout"), "{err}");
    }
}
