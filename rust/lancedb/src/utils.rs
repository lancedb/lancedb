// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::{DataType, Schema, SchemaRef};
use datafusion_common::{DataFusionError, Result as DataFusionResult};
use datafusion_execution::RecordBatchStream;
use futures::{FutureExt, Stream};
use lance::arrow::json::JsonDataType;
use lance::dataset::{ReadParams, WriteParams};
use lance::index::vector::utils::infer_vector_dim;
use lance::io::{ObjectStoreParams, WrappingObjectStore};
use lazy_static::lazy_static;
use std::pin::Pin;

use crate::error::{Error, Result};
use datafusion_physical_plan::SendableRecordBatchStream;

lazy_static! {
    static ref TABLE_NAME_REGEX: regex::Regex = regex::Regex::new(r"^[a-zA-Z0-9_\-\.]+$").unwrap();
}

pub trait PatchStoreParam {
    fn patch_with_store_wrapper(
        self,
        wrapper: Arc<dyn WrappingObjectStore>,
    ) -> Result<Option<ObjectStoreParams>>;
}

impl PatchStoreParam for Option<ObjectStoreParams> {
    fn patch_with_store_wrapper(
        self,
        wrapper: Arc<dyn WrappingObjectStore>,
    ) -> Result<Option<ObjectStoreParams>> {
        let mut params = self.unwrap_or_default();
        if params.object_store_wrapper.is_some() {
            return Err(Error::Other {
                message: "can not patch param because object store is already set".into(),
                source: None,
            });
        }
        params.object_store_wrapper = Some(wrapper);

        Ok(Some(params))
    }
}

pub trait PatchWriteParam {
    fn patch_with_store_wrapper(self, wrapper: Arc<dyn WrappingObjectStore>)
        -> Result<WriteParams>;
}

impl PatchWriteParam for WriteParams {
    fn patch_with_store_wrapper(
        mut self,
        wrapper: Arc<dyn WrappingObjectStore>,
    ) -> Result<WriteParams> {
        self.store_params = self.store_params.patch_with_store_wrapper(wrapper)?;
        Ok(self)
    }
}

// NOTE: we have some API inconsistency here.
// WriteParam is found in the form of Option<WriteParam> and ReadParam is found in the form of ReadParam

pub trait PatchReadParam {
    fn patch_with_store_wrapper(self, wrapper: Arc<dyn WrappingObjectStore>) -> Result<ReadParams>;
}

impl PatchReadParam for ReadParams {
    fn patch_with_store_wrapper(
        mut self,
        wrapper: Arc<dyn WrappingObjectStore>,
    ) -> Result<ReadParams> {
        self.store_options = self.store_options.patch_with_store_wrapper(wrapper)?;
        Ok(self)
    }
}

/// Validate table name.
pub fn validate_table_name(name: &str) -> Result<()> {
    if name.is_empty() {
        return Err(Error::InvalidTableName {
            name: name.to_string(),
            reason: "Table names cannot be empty strings".to_string(),
        });
    }
    if !TABLE_NAME_REGEX.is_match(name) {
        return Err(Error::InvalidTableName {
            name: name.to_string(),
            reason:
                "Table names can only contain alphanumeric characters, underscores, hyphens, and periods"
                    .to_string(),
        });
    }
    Ok(())
}

/// Find one default column to create index or perform vector query.
pub(crate) fn default_vector_column(schema: &Schema, dim: Option<i32>) -> Result<String> {
    // Try to find a vector column.
    let candidates = schema
        .fields()
        .iter()
        .filter_map(|field| match infer_vector_dim(field.data_type()) {
            Ok(d) if dim.is_none() || dim == Some(d as i32) => Some(field.name()),
            _ => None,
        })
        .collect::<Vec<_>>();
    if candidates.is_empty() {
        Err(Error::InvalidInput {
            message: format!(
                "No vector column found to match with the query vector dimension: {}",
                dim.unwrap_or_default()
            ),
        })
    } else if candidates.len() != 1 {
        Err(Error::Schema {
            message: format!(
                "More than one vector columns found, \
                    please specify which column to create index or query: {:?}",
                candidates
            ),
        })
    } else {
        Ok(candidates[0].to_string())
    }
}

pub fn supported_btree_data_type(dtype: &DataType) -> bool {
    dtype.is_integer()
        || dtype.is_floating()
        || matches!(
            dtype,
            DataType::Boolean
                | DataType::Utf8
                | DataType::Time32(_)
                | DataType::Time64(_)
                | DataType::Date32
                | DataType::Date64
                | DataType::Timestamp(_, _)
                | DataType::FixedSizeBinary(_)
        )
}

pub fn supported_bitmap_data_type(dtype: &DataType) -> bool {
    dtype.is_integer() || matches!(dtype, DataType::Utf8)
}

pub fn supported_label_list_data_type(dtype: &DataType) -> bool {
    match dtype {
        DataType::List(field) => supported_bitmap_data_type(field.data_type()),
        DataType::FixedSizeList(field, _) => supported_bitmap_data_type(field.data_type()),
        _ => false,
    }
}

pub fn supported_fts_data_type(dtype: &DataType) -> bool {
    supported_fts_data_type_impl(dtype, false)
}

fn supported_fts_data_type_impl(dtype: &DataType, in_list: bool) -> bool {
    match (dtype, in_list) {
        (DataType::Utf8 | DataType::LargeUtf8, _) => true,
        (DataType::List(field) | DataType::LargeList(field), false) => {
            supported_fts_data_type_impl(field.data_type(), true)
        }
        _ => false,
    }
}

pub fn supported_vector_data_type(dtype: &DataType) -> bool {
    match dtype {
        DataType::FixedSizeList(field, _) => {
            field.data_type().is_floating() || field.data_type() == &DataType::UInt8
        }
        DataType::List(field) => supported_vector_data_type(field.data_type()),
        _ => false,
    }
}

/// Note: this is temporary until we get a proper datatype conversion in Lance.
pub fn string_to_datatype(s: &str) -> Option<DataType> {
    let data_type: serde_json::Value = {
        if let Ok(data_type) = serde_json::from_str(s) {
            data_type
        } else {
            serde_json::json!({ "type": s })
        }
    };
    let json_type: JsonDataType = serde_json::from_value(data_type).ok()?;
    (&json_type).try_into().ok()
}

enum TimeoutState {
    NotStarted {
        timeout: std::time::Duration,
    },
    Started {
        deadline: Pin<Box<tokio::time::Sleep>>,
        timeout: std::time::Duration,
    },
    Completed,
}

/// A `Stream` wrapper that implements a timeout.
///
/// The timeout starts when the first `poll_next` is called. As soon as the timeout
/// duration has passed, the stream will return an `Err` indicating a timeout error
/// for the next poll.
pub struct TimeoutStream {
    inner: SendableRecordBatchStream,
    state: TimeoutState,
}

impl TimeoutStream {
    pub fn new(inner: SendableRecordBatchStream, timeout: std::time::Duration) -> Self {
        Self {
            inner,
            state: TimeoutState::NotStarted { timeout },
        }
    }

    pub fn new_boxed(
        inner: SendableRecordBatchStream,
        timeout: std::time::Duration,
    ) -> SendableRecordBatchStream {
        Box::pin(Self::new(inner, timeout))
    }

    fn timeout_error(timeout: &std::time::Duration) -> DataFusionError {
        DataFusionError::Execution(format!("Query timeout after {} ms", timeout.as_millis()))
    }
}

impl RecordBatchStream for TimeoutStream {
    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }
}

impl Stream for TimeoutStream {
    type Item = DataFusionResult<RecordBatch>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        match &mut self.state {
            TimeoutState::NotStarted { timeout } => {
                if timeout.is_zero() {
                    return std::task::Poll::Ready(Some(Err(Self::timeout_error(timeout))));
                }
                let deadline = Box::pin(tokio::time::sleep(*timeout));
                self.state = TimeoutState::Started {
                    deadline,
                    timeout: *timeout,
                };
                self.poll_next(cx)
            }
            TimeoutState::Started { deadline, timeout } => match deadline.poll_unpin(cx) {
                std::task::Poll::Ready(_) => {
                    let err = Self::timeout_error(timeout);
                    self.state = TimeoutState::Completed;
                    std::task::Poll::Ready(Some(Err(err)))
                }
                std::task::Poll::Pending => {
                    let inner = Pin::new(&mut self.inner);
                    inner.poll_next(cx)
                }
            },
            TimeoutState::Completed => std::task::Poll::Ready(None),
        }
    }
}

#[cfg(test)]
mod tests {
    use arrow_array::Int32Array;
    use arrow_schema::Field;
    use datafusion_physical_plan::stream::RecordBatchStreamAdapter;
    use futures::{stream, StreamExt};
    use tokio::time::sleep;

    use super::*;

    #[test]
    fn test_guess_default_column() {
        let schema_no_vector = Schema::new(vec![
            Field::new("id", DataType::Int16, true),
            Field::new("tag", DataType::Utf8, false),
        ]);
        assert!(default_vector_column(&schema_no_vector, None)
            .unwrap_err()
            .to_string()
            .contains("No vector column"));

        let schema_with_vec_col = Schema::new(vec![
            Field::new("id", DataType::Int16, true),
            Field::new(
                "vec",
                DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float64, false)), 10),
                false,
            ),
        ]);
        assert_eq!(
            default_vector_column(&schema_with_vec_col, None).unwrap(),
            "vec"
        );

        let multi_vec_col = Schema::new(vec![
            Field::new("id", DataType::Int16, true),
            Field::new(
                "vec",
                DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float64, false)), 10),
                false,
            ),
            Field::new(
                "vec2",
                DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float64, false)), 50),
                false,
            ),
        ]);
        assert!(default_vector_column(&multi_vec_col, None)
            .unwrap_err()
            .to_string()
            .contains("More than one"));
    }

    #[test]
    fn test_validate_table_name() {
        assert!(validate_table_name("my_table").is_ok());
        assert!(validate_table_name("my_table_1").is_ok());
        assert!(validate_table_name("123mytable").is_ok());
        assert!(validate_table_name("_12345table").is_ok());
        assert!(validate_table_name("table.12345").is_ok());
        assert!(validate_table_name("table.._dot_..12345").is_ok());

        assert!(validate_table_name("").is_err());
        assert!(validate_table_name("my_table!").is_err());
        assert!(validate_table_name("my/table").is_err());
        assert!(validate_table_name("my@table").is_err());
        assert!(validate_table_name("name with space").is_err());
    }

    #[test]
    fn test_string_to_datatype() {
        let string = "int32";
        let expected = DataType::Int32;
        assert_eq!(string_to_datatype(string), Some(expected));
    }

    fn sample_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "col1",
            DataType::Int32,
            false,
        )]));
        RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn test_timeout_stream() {
        let batch = sample_batch();
        let schema = batch.schema();
        let mock_stream = stream::iter(vec![Ok(batch.clone()), Ok(batch.clone())]);

        let sendable_stream: SendableRecordBatchStream =
            Box::pin(RecordBatchStreamAdapter::new(schema.clone(), mock_stream));
        let timeout_duration = std::time::Duration::from_millis(10);
        let mut timeout_stream = TimeoutStream::new(sendable_stream, timeout_duration);

        // Poll the stream to get the first batch
        let first_result = timeout_stream.next().await;
        assert!(first_result.is_some());
        assert!(first_result.unwrap().is_ok());

        // Sleep for the timeout duration
        sleep(timeout_duration).await;

        // Poll the stream again and ensure it returns a timeout error
        let second_result = timeout_stream.next().await.unwrap();
        assert!(second_result.is_err());
        assert!(second_result
            .unwrap_err()
            .to_string()
            .contains("Query timeout"));
    }

    #[tokio::test]
    async fn test_timeout_stream_zero_duration() {
        let batch = sample_batch();
        let schema = batch.schema();
        let mock_stream = stream::iter(vec![Ok(batch.clone()), Ok(batch.clone())]);

        let sendable_stream: SendableRecordBatchStream =
            Box::pin(RecordBatchStreamAdapter::new(schema.clone(), mock_stream));

        // Setup similar to test_timeout_stream
        let timeout_duration = std::time::Duration::from_secs(0);
        let mut timeout_stream = TimeoutStream::new(sendable_stream, timeout_duration);

        // First poll should immediately return a timeout error
        let result = timeout_stream.next().await.unwrap();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Query timeout"));
    }

    #[tokio::test]
    async fn test_timeout_stream_completes_normally() {
        let batch = sample_batch();
        let schema = batch.schema();
        let mock_stream = stream::iter(vec![Ok(batch.clone()), Ok(batch.clone())]);

        let sendable_stream: SendableRecordBatchStream =
            Box::pin(RecordBatchStreamAdapter::new(schema.clone(), mock_stream));

        // Setup a stream with 2 batches
        // Use a longer timeout that won't trigger
        let timeout_duration = std::time::Duration::from_secs(1);
        let mut timeout_stream = TimeoutStream::new(sendable_stream, timeout_duration);

        // Both polls should return data normally
        assert!(timeout_stream.next().await.unwrap().is_ok());
        assert!(timeout_stream.next().await.unwrap().is_ok());
        // Stream should be empty now
        assert!(timeout_stream.next().await.is_none());
    }
}
