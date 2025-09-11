// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use std::{borrow::Cow, fmt::Formatter, str::FromStr, sync::Arc};

use arrow::array::{AsArray, Float32Builder};
use arrow_array::{Array, ArrayRef, FixedSizeListArray, Float32Array};
use arrow_data::ArrayData;
use arrow_schema::DataType;
use async_openai::{
    config::OpenAIConfig,
    types::{CreateEmbeddingRequest, Embedding, EmbeddingInput, EncodingFormat},
    Client,
};
use tokio::{runtime::Handle, task};

use crate::{Error, Result};

use super::EmbeddingFunction;

#[derive(Debug)]
pub enum EmbeddingModel {
    TextEmbeddingAda002,
    TextEmbedding3Small,
    TextEmbedding3Large,
}

impl EmbeddingModel {
    fn ndims(&self) -> usize {
        match self {
            Self::TextEmbeddingAda002 => 1536,
            Self::TextEmbedding3Small => 1536,
            Self::TextEmbedding3Large => 3072,
        }
    }
}

impl FromStr for EmbeddingModel {
    type Err = Error;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s {
            "text-embedding-ada-002" => Ok(Self::TextEmbeddingAda002),
            "text-embedding-3-small" => Ok(Self::TextEmbedding3Small),
            "text-embedding-3-large" => Ok(Self::TextEmbedding3Large),
            _ => Err(Error::InvalidInput {
                message: "Invalid input. Available models are: 'text-embedding-3-small', 'text-embedding-ada-002', 'text-embedding-3-large' ".to_string()
            }),
        }
    }
}

impl std::fmt::Display for EmbeddingModel {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            Self::TextEmbeddingAda002 => write!(f, "text-embedding-ada-002"),
            Self::TextEmbedding3Small => write!(f, "text-embedding-3-small"),
            Self::TextEmbedding3Large => write!(f, "text-embedding-3-large"),
        }
    }
}

impl TryFrom<&str> for EmbeddingModel {
    type Error = Error;

    fn try_from(value: &str) -> std::result::Result<Self, Self::Error> {
        value.parse()
    }
}

pub struct OpenAIEmbeddingFunction {
    model: EmbeddingModel,
    api_key: String,
    api_base: Option<String>,
    org_id: Option<String>,
}

impl std::fmt::Debug for OpenAIEmbeddingFunction {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        // let's be safe and not print the full API key
        let creds_display = if self.api_key.len() > 6 {
            format!(
                "{}***{}",
                &self.api_key[0..2],
                &self.api_key[self.api_key.len() - 4..]
            )
        } else {
            "[INVALID]".to_string()
        };

        f.debug_struct("OpenAI")
            .field("model", &self.model)
            .field("api_key", &creds_display)
            .field("api_base", &self.api_base)
            .field("org_id", &self.org_id)
            .finish()
    }
}

impl OpenAIEmbeddingFunction {
    /// Create a new OpenAIEmbeddingFunction
    pub fn new<A: Into<String>>(api_key: A) -> Self {
        Self::new_impl(api_key.into(), EmbeddingModel::TextEmbeddingAda002)
    }

    pub fn new_with_model<A: Into<String>, M: TryInto<EmbeddingModel>>(
        api_key: A,
        model: M,
    ) -> crate::Result<Self>
    where
        M::Error: Into<crate::Error>,
    {
        Ok(Self::new_impl(
            api_key.into(),
            model.try_into().map_err(|e| e.into())?,
        ))
    }

    /// concrete implementation to reduce monomorphization
    fn new_impl(api_key: String, model: EmbeddingModel) -> Self {
        Self {
            model,
            api_key,
            api_base: None,
            org_id: None,
        }
    }

    /// To use a API base url different from default "https://api.openai.com/v1"
    pub fn api_base<S: Into<String>>(mut self, api_base: S) -> Self {
        self.api_base = Some(api_base.into());
        self
    }

    /// To use a different OpenAI organization id other than default
    pub fn org_id<S: Into<String>>(mut self, org_id: S) -> Self {
        self.org_id = Some(org_id.into());
        self
    }
}

impl EmbeddingFunction for OpenAIEmbeddingFunction {
    fn name(&self) -> &str {
        "openai"
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

    fn compute_source_embeddings(&self, source: ArrayRef) -> crate::Result<ArrayRef> {
        let len = source.len();
        let n_dims = self.model.ndims();
        let inner = self.compute_inner(source)?;

        let fsl = DataType::new_fixed_size_list(DataType::Float32, n_dims as i32, false);

        // We can't use the FixedSizeListBuilder here because it always adds a null bitmap
        // and we want to explicitly work with non-nullable arrays.
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

impl OpenAIEmbeddingFunction {
    fn compute_inner(&self, source: Arc<dyn Array>) -> Result<Float32Array> {
        // OpenAI only supports non-nullable string arrays
        if source.is_nullable() {
            return Err(crate::Error::InvalidInput {
                message: "Expected non-nullable data type".to_string(),
            });
        }

        // OpenAI only supports string arrays
        if !matches!(source.data_type(), DataType::Utf8 | DataType::LargeUtf8) {
            return Err(crate::Error::InvalidInput {
                message: "Expected Utf8 data type".to_string(),
            });
        };

        let mut creds = OpenAIConfig::new().with_api_key(self.api_key.clone());

        if let Some(api_base) = &self.api_base {
            creds = creds.with_api_base(api_base.clone());
        }
        if let Some(org_id) = &self.org_id {
            creds = creds.with_org_id(org_id.clone());
        }

        let input = match source.data_type() {
            DataType::Utf8 => {
                let array = source
                    .as_string::<i32>()
                    .into_iter()
                    .map(|s| {
                        s.expect("we already asserted that the array is non-nullable")
                            .to_string()
                    })
                    .collect::<Vec<String>>();
                EmbeddingInput::StringArray(array)
            }
            DataType::LargeUtf8 => {
                let array = source
                    .as_string::<i64>()
                    .into_iter()
                    .map(|s| {
                        s.expect("we already asserted that the array is non-nullable")
                            .to_string()
                    })
                    .collect::<Vec<String>>();
                EmbeddingInput::StringArray(array)
            }
            _ => unreachable!("This should not happen. We already checked the data type."),
        };

        let client = Client::with_config(creds);
        let embed = client.embeddings();
        let req = CreateEmbeddingRequest {
            model: self.model.to_string(),
            input,
            encoding_format: Some(EncodingFormat::Float),
            user: None,
            dimensions: None,
        };

        // TODO: request batching and retry logic
        task::block_in_place(move || {
            Handle::current().block_on(async {
                let mut builder = Float32Builder::new();

                let res = embed.create(req).await.map_err(|e| crate::Error::Runtime {
                    message: format!("OpenAI embed request failed: {e}"),
                })?;

                for Embedding { embedding, .. } in res.data.iter() {
                    builder.append_slice(embedding);
                }

                Ok(builder.finish())
            })
        })
    }
}
