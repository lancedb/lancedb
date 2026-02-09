// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use aws_sdk_bedrockruntime::Client as BedrockClient;
use std::{borrow::Cow, fmt::Formatter, str::FromStr, sync::Arc};

use arrow::array::{AsArray, Float32Builder};
use arrow_array::{Array, ArrayRef, FixedSizeListArray, Float32Array};
use arrow_data::ArrayData;
use arrow_schema::DataType;
use serde_json::{json, Value};

use super::EmbeddingFunction;
use crate::{Error, Result};

use tokio::runtime::Handle;
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

            let client = self.client.clone();
            let model_id = self.model.model_id().to_string();
            let request_body = request_body.clone();

            let response = block_in_place(move || {
                Handle::current().block_on(async move {
                    client
                        .invoke_model()
                        .model_id(model_id)
                        .body(aws_sdk_bedrockruntime::primitives::Blob::new(
                            serde_json::to_vec(&request_body).unwrap(),
                        ))
                        .send()
                        .await
                })
            })
            .unwrap();

            let response_json: Value =
                serde_json::from_slice(response.body.as_ref()).map_err(|e| Error::Runtime {
                    message: format!("Failed to parse response: {}", e),
                })?;

            let embedding = match self.model {
                BedrockEmbeddingModel::TitanEmbedding => response_json["embedding"]
                    .as_array()
                    .ok_or_else(|| Error::Runtime {
                        message: "Missing embedding in response".to_string(),
                    })?
                    .iter()
                    .map(|v| v.as_f64().unwrap() as f32)
                    .collect::<Vec<f32>>(),
                BedrockEmbeddingModel::CohereLarge => response_json["embeddings"][0]
                    .as_array()
                    .ok_or_else(|| Error::Runtime {
                        message: "Missing embeddings in response".to_string(),
                    })?
                    .iter()
                    .map(|v| v.as_f64().unwrap() as f32)
                    .collect::<Vec<f32>>(),
            };

            builder.append_slice(&embedding);
        }

        Ok(builder.finish())
    }
}
