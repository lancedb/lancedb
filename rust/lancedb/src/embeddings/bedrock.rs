// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use aws_sdk_bedrockruntime::Client as BedrockClient;
use std::{borrow::Cow, fmt::Formatter, str::FromStr, sync::Arc};

use arrow::array::{AsArray, Float32Builder};
use arrow_array::{Array, ArrayRef, FixedSizeListArray, Float32Array};
use arrow_data::ArrayData;
use arrow_schema::DataType;
use serde_json::{Value, json};

use super::EmbeddingFunction;
use crate::{Error, Result};

use tokio::runtime::{Handle, RuntimeFlavor};
use tokio::task::block_in_place;

#[derive(Debug)]
pub enum BedrockEmbeddingModel {
    TitanEmbedding,
    CohereLarge,
}

impl BedrockEmbeddingModel {
    fn ndims(&self) -> usize {
        match self {
            Self::TitanEmbedding => 1536,
            Self::CohereLarge => 1024,
        }
    }

    fn model_id(&self) -> &str {
        match self {
            Self::TitanEmbedding => "amazon.titan-embed-text-v1",
            Self::CohereLarge => "cohere.embed-english-v3",
        }
    }
}

impl FromStr for BedrockEmbeddingModel {
    type Err = Error;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s {
            "titan-embed-text-v1" => Ok(Self::TitanEmbedding),
            "cohere-embed-english-v3" => Ok(Self::CohereLarge),
            _ => Err(Error::InvalidInput {
                message: "Invalid model. Available models are: 'titan-embed-text-v1', 'cohere-embed-english-v3'".to_string()
            }),
        }
    }
}

pub struct BedrockEmbeddingFunction {
    model: BedrockEmbeddingModel,
    client: BedrockClient,
}

impl BedrockEmbeddingFunction {
    pub fn new(client: BedrockClient) -> Self {
        Self {
            model: BedrockEmbeddingModel::TitanEmbedding,
            client,
        }
    }

    pub fn with_model(client: BedrockClient, model: BedrockEmbeddingModel) -> Self {
        Self { model, client }
    }
}

impl EmbeddingFunction for BedrockEmbeddingFunction {
    fn name(&self) -> &str {
        "bedrock"
    }

    fn source_type(&self) -> Result<Cow<'_, DataType>> {
        Ok(Cow::Owned(DataType::Utf8))
    }

    fn dest_type(&self) -> Result<Cow<'_, DataType>> {
        let n_dims = self.model.ndims();
        Ok(Cow::Owned(DataType::new_fixed_size_list(
            DataType::Float32,
            n_dims as i32,
            false,
        )))
    }

    fn compute_source_embeddings(&self, source: ArrayRef) -> Result<ArrayRef> {
        let len = source.len();
        let n_dims = self.model.ndims();
        let inner = self.compute_inner(source)?;

        let fsl = DataType::new_fixed_size_list(DataType::Float32, n_dims as i32, false);

        let array_data = ArrayData::builder(fsl)
            .len(len)
            .add_child_data(inner.into_data())
            .build()?;

        Ok(Arc::new(FixedSizeListArray::from(array_data)))
    }

    fn compute_query_embeddings(&self, input: Arc<dyn Array>) -> Result<Arc<dyn Array>> {
        let arr = self.compute_inner(input)?;
        Ok(Arc::new(arr))
    }
}

impl std::fmt::Debug for BedrockEmbeddingFunction {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BedrockEmbeddingFunction")
            .field("model", &self.model)
            // Skip client field as it doesn't implement Debug
            .finish()
    }
}

impl BedrockEmbeddingFunction {
    fn compute_inner(&self, source: Arc<dyn Array>) -> Result<Float32Array> {
        if source.is_nullable() {
            return Err(Error::InvalidInput {
                message: "Expected non-nullable data type".to_string(),
            });
        }

        if !matches!(source.data_type(), DataType::Utf8 | DataType::LargeUtf8) {
            return Err(Error::InvalidInput {
                message: "Expected Utf8 data type".to_string(),
            });
        }

        let mut builder = Float32Builder::new();

        let texts = match source.data_type() {
            DataType::Utf8 => source
                .as_string::<i32>()
                .into_iter()
                .map(|s| s.expect("array is non-nullable").to_string())
                .collect::<Vec<String>>(),
            DataType::LargeUtf8 => source
                .as_string::<i64>()
                .into_iter()
                .map(|s| s.expect("array is non-nullable").to_string())
                .collect::<Vec<String>>(),
            _ => unreachable!(),
        };

        // Bedrock's SDK is async but this trait method is synchronous, so we
        // bridge with `block_in_place` + `block_on`. That requires a
        // multi-threaded Tokio runtime; return a typed error instead of
        // panicking when no compatible runtime is available.
        let handle = current_multi_thread_handle()?;

        for text in texts {
            let request_body = match self.model {
                BedrockEmbeddingModel::TitanEmbedding => {
                    json!({
                        "inputText": text
                    })
                }
                BedrockEmbeddingModel::CohereLarge => {
                    json!({
                        "texts": [text],
                        "input_type": "search_document"
                    })
                }
            };

            // Serialize before entering the blocking section so a serialization
            // failure surfaces as a typed error rather than an `unwrap` panic.
            let body = serde_json::to_vec(&request_body).map_err(|e| Error::Runtime {
                message: format!("Failed to serialize Bedrock request: {e}"),
            })?;

            let client = self.client.clone();
            let model_id = self.model.model_id().to_string();

            let response = block_in_place(|| {
                handle.block_on(async move {
                    client
                        .invoke_model()
                        .model_id(model_id)
                        .body(aws_sdk_bedrockruntime::primitives::Blob::new(body))
                        .send()
                        .await
                        .map_err(|e| Error::Runtime {
                            message: format!("Bedrock invoke_model request failed: {e}"),
                        })
                })
            })?;

            let response_json: Value =
                serde_json::from_slice(response.body.as_ref()).map_err(|e| Error::Runtime {
                    message: format!("Failed to parse response: {}", e),
                })?;

            let embedding = match self.model {
                BedrockEmbeddingModel::TitanEmbedding => {
                    json_array_to_f32(&response_json["embedding"], "embedding")?
                }
                BedrockEmbeddingModel::CohereLarge => {
                    json_array_to_f32(&response_json["embeddings"][0], "embeddings")?
                }
            };

            builder.append_slice(&embedding);
        }

        Ok(builder.finish())
    }
}

/// Returns a handle to the current multi-threaded Tokio runtime, or a typed
/// [`Error::Runtime`] when called outside a runtime or on the current-thread
/// runtime. This keeps the synchronous-over-async bridge in
/// [`BedrockEmbeddingFunction::compute_inner`] from panicking on runtime
/// configurations that cannot support `block_in_place`.
fn current_multi_thread_handle() -> Result<Handle> {
    let handle = Handle::try_current().map_err(|e| Error::Runtime {
        message: format!("Bedrock embedding must be called from within a Tokio runtime: {e}"),
    })?;
    if handle.runtime_flavor() == RuntimeFlavor::CurrentThread {
        return Err(Error::Runtime {
            message: "Bedrock embedding requires a multi-threaded Tokio runtime; the \
                      current-thread runtime cannot use `block_in_place`"
                .to_string(),
        });
    }
    Ok(handle)
}

/// Converts a JSON value expected to be an array of numbers into `Vec<f32>`.
///
/// Returns a typed [`Error::Runtime`] (rather than panicking) when the value is
/// not an array or contains a non-numeric element, so malformed provider
/// responses degrade gracefully.
fn json_array_to_f32(value: &Value, field: &str) -> Result<Vec<f32>> {
    let arr = value.as_array().ok_or_else(|| Error::Runtime {
        message: format!("Missing or non-array '{field}' field in Bedrock response"),
    })?;
    arr.iter()
        .map(|v| {
            v.as_f64().map(|f| f as f32).ok_or_else(|| Error::Runtime {
                message: format!("Non-numeric value in Bedrock '{field}' embedding: {v}"),
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn json_array_to_f32_parses_numbers() {
        let v = json!([1.0, 2, -3.5]);
        let out = json_array_to_f32(&v, "embedding").unwrap();
        assert_eq!(out, vec![1.0_f32, 2.0, -3.5]);
    }

    #[test]
    fn json_array_to_f32_rejects_non_array() {
        // Missing field indexes to `Value::Null`; a malformed payload should be
        // a typed error, not a panic.
        let v = json!({"unexpected": "shape"});
        let err = json_array_to_f32(&v["embedding"], "embedding").unwrap_err();
        assert!(matches!(err, Error::Runtime { .. }), "got {err:?}");
    }

    #[test]
    fn json_array_to_f32_rejects_non_numeric_element() {
        let v = json!([1.0, "not-a-number", 3.0]);
        let err = json_array_to_f32(&v, "embedding").unwrap_err();
        assert!(matches!(err, Error::Runtime { .. }), "got {err:?}");
    }

    #[test]
    fn handle_errors_without_runtime() {
        // No Tokio runtime in scope -> typed error instead of a panic.
        let err = current_multi_thread_handle().unwrap_err();
        assert!(matches!(err, Error::Runtime { .. }), "got {err:?}");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn handle_errors_on_current_thread_runtime() {
        let err = current_multi_thread_handle().unwrap_err();
        assert!(matches!(err, Error::Runtime { .. }), "got {err:?}");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn handle_ok_on_multi_thread_runtime() {
        current_multi_thread_handle().expect("multi-threaded runtime should be accepted");
    }
}
