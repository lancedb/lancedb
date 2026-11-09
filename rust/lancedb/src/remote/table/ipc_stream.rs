// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Incremental decoding of response bodies that use Arrow IPC stream framing.

use arrow_array::RecordBatch;
use arrow_buffer::Buffer;
use arrow_ipc::reader::StreamDecoder;
use bytes::Bytes;
use datafusion_common::DataFusionError;
use datafusion_physical_plan::SendableRecordBatchStream;
use datafusion_physical_plan::stream::RecordBatchStreamAdapter;
use futures::stream::BoxStream;
use futures::{Stream, StreamExt, TryStreamExt};

use crate::error::{BoxError, Error, Result};

/// Decode an Arrow IPC stream from a response body as its chunks arrive.
///
/// Only the current chunk and the decoder's state are held, so memory is bounded
/// by the size of a batch rather than the size of the result. This waits for the
/// first batch, or for the end of an empty result, so a body that fails before
/// any data arrives fails the request itself.
///
/// A complete body ends with the end-of-stream marker; the server aborts the body
/// without it when the query fails after sending headers. A body that ends early,
/// even between messages, is reported as [`Error::ResponseInterrupted`] and never
/// as a short result.
pub(super) async fn read_ipc_stream<E>(
    chunks: impl Stream<Item = std::result::Result<Bytes, E>> + Send + 'static,
    request_id: String,
    table_version: Option<u64>,
) -> Result<SendableRecordBatchStream>
where
    E: Into<BoxError> + 'static,
{
    let mut body = IpcStreamBody {
        chunks: chunks.map_err(Into::into).boxed(),
        chunk: Buffer::default(),
        decoder: StreamDecoder::new(),
        finished: false,
        request_id,
        table_version,
        batches: 0,
        rows: 0,
    };
    let first = body.next_batch().await?;
    // The schema is the first message, so only a body holding nothing but the
    // end-of-stream marker can complete without one.
    let schema = body.decoder.schema().ok_or_else(|| Error::Http {
        source: "Arrow IPC stream ended without a schema".into(),
        request_id: body.request_id.clone(),
        status_code: None,
    })?;
    let rest = futures::stream::try_unfold(body, |mut body| async move {
        Ok(body.next_batch().await?.map(|batch| (batch, body)))
    });
    let batches = futures::stream::iter(first.map(Ok))
        .chain(rest)
        .map_err(|error: Error| DataFusionError::External(Box::new(error)));
    Ok(Box::pin(RecordBatchStreamAdapter::new(schema, batches)))
}

struct IpcStreamBody {
    chunks: BoxStream<'static, std::result::Result<Bytes, BoxError>>,
    /// The part of the current chunk that has not been decoded yet.
    chunk: Buffer,
    decoder: StreamDecoder,
    /// Set once the body has ended with the end-of-stream marker.
    finished: bool,
    request_id: String,
    table_version: Option<u64>,
    batches: u64,
    rows: u64,
}

impl IpcStreamBody {
    /// Decode until the next batch is complete. Returns `None` once the body
    /// has ended with the end-of-stream marker.
    async fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        while !self.finished {
            if self.chunk.is_empty() {
                match self.chunks.next().await {
                    Some(Ok(chunk)) => self.chunk = chunk.into(),
                    Some(Err(error)) => return Err(self.interrupted(error)),
                    None if read_end_of_stream(&mut self.decoder) => self.finished = true,
                    None => {
                        return Err(self.interrupted(
                            "the response ended before the Arrow end-of-stream marker".into(),
                        ));
                    }
                }
                continue;
            }
            // Decoding stops after each batch, keeping the rest of the chunk.
            if let Some(batch) = self.decoder.decode(&mut self.chunk)? {
                self.batches += 1;
                self.rows += batch.num_rows() as u64;
                return Ok(Some(batch));
            }
        }
        Ok(None)
    }

    fn interrupted(&self, source: BoxError) -> Error {
        Error::ResponseInterrupted {
            request_id: self.request_id.clone(),
            batches: self.batches,
            rows: self.rows,
            table_version: self.table_version,
            source,
        }
    }
}

/// Whether the decoder has read the end-of-stream marker.
///
/// [`StreamDecoder::finish`] also accepts input that stops between messages,
/// because the Arrow format lets a writer end a stream by closing it. A decoder
/// that has read the marker rejects any further input, which is the only way
/// its API tells the two apart.
fn read_end_of_stream(decoder: &mut StreamDecoder) -> bool {
    decoder.finish().is_ok() && decoder.decode(&mut Buffer::from_slice_ref([0u8])).is_err()
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use arrow_array::builder::StringDictionaryBuilder;
    use arrow_array::types::Int8Type;
    use arrow_array::{ArrayRef, Int32Array, StringArray};
    use arrow_ipc::CompressionType;
    use arrow_ipc::writer::{DictionaryHandling, FileWriter, IpcWriteOptions, StreamWriter};
    use arrow_schema::{DataType, Field, Schema, SchemaRef};
    use lance::arrow::json::JsonSchema;
    use rstest::rstest;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};
    use tokio::sync::mpsc;

    use super::*;
    use crate::query::{ExecutableQuery, QueryExecutionOptions};
    use crate::remote::client::{ClientConfig, RetryConfig};
    use crate::remote::{ARROW_FILE_CONTENT_TYPE, ARROW_STREAM_CONTENT_TYPE};
    use crate::{Table, connect};

    /// Three batches of two rows under a schema with metadata. The second batch's
    /// dictionary extends the first one's, which writers can send as a delta, and
    /// the third replaces it.
    fn batches() -> Vec<RecordBatch> {
        let schema = Arc::new(Schema::new_with_metadata(
            vec![
                Field::new("id", DataType::Int32, false),
                Field::new_dictionary("label", DataType::Int8, DataType::Utf8, false),
            ],
            HashMap::from([("origin".to_string(), "test".to_string())]),
        ));
        let labels: [(&[&str], [&str; 2]); 3] = [
            (&["a", "b"], ["a", "b"]),
            (&["a", "b", "c"], ["c", "a"]),
            (&["d"], ["d", "d"]),
        ];
        labels
            .into_iter()
            .enumerate()
            .map(|(i, (dictionary, values))| {
                let dictionary = StringArray::from(dictionary.to_vec());
                let mut label =
                    StringDictionaryBuilder::<Int8Type>::new_with_dictionary(2, &dictionary)
                        .unwrap();
                values
                    .into_iter()
                    .for_each(|value| label.append_value(value));
                let id = Int32Array::from(vec![2 * i as i32, 2 * i as i32 + 1]);
                RecordBatch::try_new(
                    schema.clone(),
                    vec![Arc::new(id) as ArrayRef, Arc::new(label.finish())],
                )
                .unwrap()
            })
            .collect()
    }

    /// An IPC stream cut where the server cuts it: after the schema, after each
    /// batch with its dictionaries, and after the end-of-stream marker.
    fn messages(
        schema: &Schema,
        batches: &[RecordBatch],
        options: IpcWriteOptions,
    ) -> Vec<Vec<u8>> {
        let mut writer = StreamWriter::try_new_with_options(Vec::new(), schema, options).unwrap();
        let mut messages = vec![std::mem::take(writer.get_mut())];
        for batch in batches {
            writer.write(batch).unwrap();
            messages.push(std::mem::take(writer.get_mut()));
        }
        writer.finish().unwrap();
        messages.push(std::mem::take(writer.get_mut()));
        messages
    }

    fn chunks(body: &[u8], size: usize) -> Vec<std::io::Result<Bytes>> {
        body.chunks(size)
            .map(|chunk| Ok(Bytes::copy_from_slice(chunk)))
            .collect()
    }

    /// The schema, the batches yielded, and the error that ended the stream.
    async fn read(
        chunks: Vec<std::io::Result<Bytes>>,
    ) -> (Option<SchemaRef>, Vec<RecordBatch>, Option<Error>) {
        let stream =
            match read_ipc_stream(futures::stream::iter(chunks), "request".into(), Some(7)).await {
                Ok(stream) => stream,
                Err(error) => return (None, vec![], Some(error)),
            };
        let schema = stream.schema();
        let results = stream.collect::<Vec<_>>().await;
        let (batches, errors): (Vec<_>, Vec<_>) = results.into_iter().partition(|r| r.is_ok());
        assert!(errors.len() <= 1, "the stream ends at its first error");
        let error = errors
            .into_iter()
            .next()
            .map(|e| Error::from(e.unwrap_err()));
        let batches = batches.into_iter().map(|r| r.unwrap()).collect();
        (Some(schema), batches, error)
    }

    fn assert_interrupted(error: Option<Error>, expected_batches: u64, expected_rows: u64) {
        match error {
            Some(Error::ResponseInterrupted {
                request_id,
                batches,
                rows,
                table_version,
                ..
            }) => {
                assert_eq!(request_id, "request");
                assert_eq!((batches, rows), (expected_batches, expected_rows));
                assert_eq!(table_version, Some(7));
            }
            other => panic!("expected an interrupted response, got {other:?}"),
        }
    }

    // One-byte chunks split every message; a single chunk holds them all.
    #[rstest]
    #[case(1)]
    #[case(7)]
    #[case(usize::MAX)]
    #[tokio::test]
    async fn decodes_any_chunking(#[case] chunk_size: usize) {
        let input = batches();
        let body = messages(&input[0].schema(), &input, IpcWriteOptions::default()).concat();

        let (schema, output, error) = read(chunks(&body, chunk_size)).await;

        assert!(error.is_none(), "{error:?}");
        assert_eq!(schema.unwrap(), input[0].schema());
        assert_eq!(output, input);
    }

    #[rstest]
    #[case(IpcWriteOptions::default().with_dictionary_handling(DictionaryHandling::Delta))]
    #[case(IpcWriteOptions::default().try_with_compression(Some(CompressionType::LZ4_FRAME)).unwrap())]
    #[case(IpcWriteOptions::default().try_with_compression(Some(CompressionType::ZSTD)).unwrap())]
    #[tokio::test]
    async fn decodes_dictionary_deltas_and_compressed_batches(#[case] options: IpcWriteOptions) {
        let input = batches();
        let body = messages(&input[0].schema(), &input, options).concat();

        let (_, output, error) = read(chunks(&body, 5)).await;

        assert!(error.is_none(), "{error:?}");
        assert_eq!(output, input);
    }

    /// The server answers an empty result with the schema and the end-of-stream marker.
    #[tokio::test]
    async fn empty_result_keeps_its_schema() {
        let schema = batches()[0].schema();
        let body = messages(&schema, &[], IpcWriteOptions::default()).concat();

        let (decoded, output, error) = read(chunks(&body, 3)).await;

        assert!(error.is_none(), "{error:?}");
        assert_eq!(decoded.unwrap(), schema);
        assert!(output.is_empty());
    }

    #[tokio::test]
    async fn body_ending_without_end_of_stream_is_interrupted() {
        let input = batches();
        let messages = messages(&input[0].schema(), &input, IpcWriteOptions::default());
        let complete = messages[..messages.len() - 1].concat();
        let after_first_batch = messages[..2].concat().len();
        // Where the body stops, and how many batches it holds by then.
        for (end, delivered) in [
            (complete.len(), 3),        // between messages, after the last batch
            (complete.len() - 3, 2),    // inside the last batch
            (after_first_batch, 1),     // between messages
            (messages[0].len(), 0),     // after the schema
            (messages[0].len() / 2, 0), // inside the schema
            (0, 0),                     // an empty body
        ] {
            let (_, output, error) = read(chunks(&complete[..end], 4)).await;
            assert_eq!(output, input[..delivered], "body cut at {end}");
            assert_interrupted(error, delivered as u64, 2 * delivered as u64);
        }
    }

    #[tokio::test]
    async fn body_error_is_interrupted() {
        let input = batches();
        let messages = messages(&input[0].schema(), &input, IpcWriteOptions::default());
        let mut body = chunks(&messages[..2].concat(), usize::MAX);
        body.push(Err(std::io::Error::other("connection reset")));

        let (_, output, error) = read(body).await;

        assert_eq!(output, input[..1]);
        let message = error.as_ref().map(ToString::to_string).unwrap_or_default();
        assert!(message.contains("connection reset"), "{message}");
        assert_interrupted(error, 1, 2);
    }

    #[tokio::test]
    async fn malformed_body_is_an_arrow_error() {
        let input = batches();
        let messages = messages(&input[0].schema(), &input, IpcWriteOptions::default());
        // A message whose metadata is not a flatbuffer.
        let garbage = [&[0xff; 4][..], &8u32.to_le_bytes(), &[0xab; 8]].concat();
        // Anything after the end-of-stream marker.
        let trailing = [messages.concat(), vec![0]].concat();
        for body in [[messages[0].clone(), garbage].concat(), trailing] {
            let (_, _, error) = read(chunks(&body, usize::MAX)).await;
            assert!(matches!(error, Some(Error::Arrow { .. })), "{error:?}");
        }
    }

    /// What the test server does next for a query request.
    enum Reply {
        /// A complete response with this status and an empty body.
        Status(u16),
        /// Headers of a chunked body with this content type.
        Head(Option<&'static str>),
        Chunk(Vec<u8>),
        /// The last chunk, ending the body cleanly.
        End,
        /// Close the connection in the middle of the body.
        Abort,
    }

    struct Server {
        table: Table,
        replies: mpsc::UnboundedSender<Reply>,
        /// Headers of each query request received.
        queries: Arc<Mutex<Vec<HashMap<String, String>>>>,
    }

    impl Server {
        fn reply(&self, replies: impl IntoIterator<Item = Reply>) {
            replies
                .into_iter()
                .for_each(|reply| self.replies.send(reply).unwrap());
        }
    }

    /// Serve table `t` over HTTP. Query requests are answered, in order, by the
    /// replies the test sends.
    async fn serve() -> Server {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (replies, mut pending) = mpsc::unbounded_channel();
        let queries = Arc::new(Mutex::new(Vec::new()));
        let received = queries.clone();
        tokio::spawn(async move {
            loop {
                let (mut socket, _) = listener.accept().await.unwrap();
                let (path, headers) = read_request(&mut socket).await;
                if path.ends_with("/describe/") {
                    let schema = Schema::new(vec![
                        Field::new("id", DataType::Int32, false),
                        Field::new(
                            "vector",
                            DataType::new_fixed_size_list(DataType::Float32, 2, true),
                            false,
                        ),
                    ]);
                    let body = serde_json::json!({
                        "version": 7,
                        "schema": JsonSchema::try_from(&schema).unwrap(),
                    })
                    .to_string();
                    let response = format!(
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len()
                    );
                    socket.write_all(response.as_bytes()).await.unwrap();
                    continue;
                }
                received.lock().unwrap().push(headers);
                while let Some(reply) = pending.recv().await {
                    let last = !matches!(reply, Reply::Head(_) | Reply::Chunk(_));
                    let bytes = match reply {
                        Reply::Status(status) => format!(
                            "HTTP/1.1 {status} Error\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                        )
                        .into_bytes(),
                        Reply::Head(content_type) => format!(
                            "HTTP/1.1 200 OK\r\n{}Transfer-Encoding: chunked\r\n\
                             x-lancedb-version: 7\r\nConnection: close\r\n\r\n",
                            content_type
                                .map(|value| format!("Content-Type: {value}\r\n"))
                                .unwrap_or_default()
                        )
                        .into_bytes(),
                        Reply::Chunk(chunk) => {
                            [format!("{:x}\r\n", chunk.len()).into_bytes(), chunk, b"\r\n".into()]
                                .concat()
                        }
                        Reply::End => b"0\r\n\r\n".to_vec(),
                        Reply::Abort => break,
                    };
                    // The client may have stopped reading.
                    if socket.write_all(&bytes).await.is_err() || last {
                        break;
                    }
                }
            }
        });
        let db = connect("db://test")
            .api_key("key")
            .region("us-east-1")
            .host_override(&format!("http://{address}"))
            .client_config(ClientConfig {
                retry_config: RetryConfig {
                    backoff_factor: Some(0.0),
                    backoff_jitter: Some(0.0),
                    ..Default::default()
                },
                ..Default::default()
            })
            .execute()
            .await
            .unwrap();
        let table = db.open_table("t").execute().await.unwrap();
        Server {
            table,
            replies,
            queries,
        }
    }

    async fn read_request(socket: &mut TcpStream) -> (String, HashMap<String, String>) {
        let mut request = Vec::new();
        let mut buf = [0; 4096];
        let head_end = loop {
            let n = socket.read(&mut buf).await.unwrap();
            assert!(n > 0, "connection closed before the request head");
            request.extend_from_slice(&buf[..n]);
            if let Some(end) = request.windows(4).position(|w| w == b"\r\n\r\n") {
                break end + 4;
            }
        };
        let head = String::from_utf8(request[..head_end].to_vec()).unwrap();
        let mut lines = head.lines();
        let path = lines.next().unwrap().split(' ').nth(1).unwrap().to_string();
        let headers: HashMap<_, _> = lines
            .filter_map(|line| line.split_once(':'))
            .map(|(name, value)| (name.to_ascii_lowercase(), value.trim().to_string()))
            .collect();
        let length: usize = headers
            .get("content-length")
            .map_or(0, |length| length.parse().unwrap());
        while request.len() < head_end + length {
            let n = socket.read(&mut buf).await.unwrap();
            assert!(n > 0, "connection closed before the request body");
            request.extend_from_slice(&buf[..n]);
        }
        (path, headers)
    }

    /// The query batches as the server sends them: `id` only.
    fn id_messages() -> Vec<Vec<u8>> {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let batches: Vec<_> = (0..3)
            .map(|i| {
                RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(vec![i]))])
                    .unwrap()
            })
            .collect();
        messages(&schema, &batches, IpcWriteOptions::default())
    }

    fn ids(batch: &RecordBatch) -> Vec<i32> {
        batch["id"]
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .values()
            .to_vec()
    }

    #[tokio::test]
    async fn first_batch_arrives_before_the_rest_of_the_body() {
        let server = serve().await;
        let messages = id_messages();
        server.reply([
            Reply::Head(Some(ARROW_STREAM_CONTENT_TYPE)),
            Reply::Chunk(messages[..2].concat()),
        ]);

        // The server sends the rest only after the first batch has been read.
        let (results, first) = tokio::time::timeout(Duration::from_secs(10), async {
            let mut results = server.table.query().execute().await.unwrap();
            let first = results.next().await.unwrap().unwrap();
            (results, first)
        })
        .await
        .expect("the first batch must not wait for the whole body");
        assert_eq!(ids(&first), [0]);
        server.reply([Reply::Chunk(messages[2..].concat()), Reply::End]);
        let rest = results.try_collect::<Vec<_>>().await.unwrap();
        assert_eq!(rest.iter().flat_map(ids).collect::<Vec<_>>(), [1, 2]);

        let queries = server.queries.lock().unwrap();
        assert_eq!(queries.len(), 1);
        assert_eq!(queries[0]["accept"], super::super::QUERY_ACCEPT);
    }

    /// A server abort, and a proxy that turns it into a clean end of the body,
    /// both fail the query after the batches already read. Neither is retried.
    #[rstest]
    #[case::abort(Reply::Abort, true)]
    #[case::clean_end(Reply::End, false)]
    #[tokio::test]
    async fn interrupted_body_is_typed_and_not_retried(
        #[case] end: Reply,
        #[case] transport_error: bool,
    ) {
        let server = serve().await;
        let messages = id_messages();
        server.reply([
            Reply::Head(Some(ARROW_STREAM_CONTENT_TYPE)),
            Reply::Chunk(messages[..2].concat()),
            end,
        ]);

        let mut results = server.table.query().execute().await.unwrap();
        assert_eq!(ids(&results.next().await.unwrap().unwrap()), [0]);
        let error = results.next().await.unwrap().unwrap_err();
        assert!(results.next().await.is_none());

        let Error::ResponseInterrupted {
            request_id,
            batches,
            rows,
            table_version,
            source,
        } = error
        else {
            panic!("expected an interrupted response, got {error:?}");
        };
        assert!(!request_id.is_empty());
        assert_eq!((batches, rows, table_version), (1, 1, Some(7)));
        assert_eq!(source.is::<reqwest::Error>(), transport_error, "{source}");
        assert_eq!(server.queries.lock().unwrap().len(), 1);
    }

    /// Vector queries run through a DataFusion plan, which must keep the error's type.
    #[tokio::test]
    async fn vector_query_keeps_the_interruption_type() {
        let server = serve().await;
        let messages = id_messages();
        server.reply([
            Reply::Head(Some(ARROW_STREAM_CONTENT_TYPE)),
            Reply::Chunk(messages[..2].concat()),
            Reply::Abort,
        ]);

        let results = server
            .table
            .query()
            .nearest_to(&[0.0f32, 0.0])
            .unwrap()
            .execute()
            .await
            .unwrap();
        let error = results.try_collect::<Vec<_>>().await.unwrap_err();

        assert!(
            matches!(error, Error::ResponseInterrupted { batches: 1, .. }),
            "{error:?}"
        );
    }

    #[tokio::test]
    async fn timeout_covers_reading_the_body() {
        let server = serve().await;
        let messages = id_messages();
        // The body stalls after the first batch.
        server.reply([
            Reply::Head(Some(ARROW_STREAM_CONTENT_TYPE)),
            Reply::Chunk(messages[..2].concat()),
        ]);

        let options = QueryExecutionOptions {
            timeout: Some(Duration::from_millis(500)),
            ..Default::default()
        };
        let mut results = server
            .table
            .query()
            .execute_with_options(options)
            .await
            .unwrap();
        assert_eq!(ids(&results.next().await.unwrap().unwrap()), [0]);
        let error = tokio::time::timeout(Duration::from_secs(10), results.next())
            .await
            .expect("the timeout must end a stalled body")
            .unwrap()
            .unwrap_err();

        assert!(
            !matches!(error, Error::ResponseInterrupted { .. }),
            "{error:?}"
        );
        assert!(
            error.to_string().contains("Query timeout after 500 ms"),
            "{error}"
        );
        assert_eq!(
            server.queries.lock().unwrap()[0]["x-request-timeout-ms"],
            "500"
        );
    }

    /// Servers that do not stream query results answer with file framing, or
    /// with no content type at all.
    #[rstest]
    #[case(Some(ARROW_FILE_CONTENT_TYPE))]
    #[case(None)]
    #[tokio::test]
    async fn file_framing_is_still_read(#[case] content_type: Option<&'static str>) {
        let server = serve().await;
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(vec![4, 5]))])
                .unwrap();
        let mut writer = FileWriter::try_new(Vec::new(), &schema).unwrap();
        writer.write(&batch).unwrap();
        writer.finish().unwrap();
        server.reply([
            Reply::Head(content_type),
            Reply::Chunk(writer.into_inner().unwrap()),
            Reply::End,
        ]);

        let results = server.table.query().execute().await.unwrap();
        let output = results.try_collect::<Vec<_>>().await.unwrap();

        assert_eq!(output, [batch]);
    }

    #[tokio::test]
    async fn failure_before_headers_is_retried() {
        let server = serve().await;
        server.reply([
            Reply::Status(503),
            Reply::Head(Some(ARROW_STREAM_CONTENT_TYPE)),
            Reply::Chunk(id_messages().concat()),
            Reply::End,
        ]);

        let results = server.table.query().execute().await.unwrap();
        let output = results.try_collect::<Vec<_>>().await.unwrap();

        assert_eq!(output.iter().flat_map(ids).collect::<Vec<_>>(), [0, 1, 2]);
        assert_eq!(server.queries.lock().unwrap().len(), 2);
    }
}
