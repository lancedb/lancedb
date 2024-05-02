use std::{borrow::Cow, fmt::Formatter, str::FromStr, sync::Arc};

use arrow::array::{AsArray, Float32Builder};
use arrow_array::{Array, ArrayRef, FixedSizeListArray};
use arrow_data::ArrayData;
use arrow_schema::DataType;
use async_openai::{
    config::OpenAIConfig,
    types::{CreateEmbeddingRequest, Embedding, EmbeddingInput, EncodingFormat},
    Client,
};
use tokio::{runtime::Handle, task};

use crate::Error;

use super::EmbeddingFunction;

#[derive(Debug)]
pub enum EmbeddingModel {
    TextEmbedding3Small,
    TextEmbeddingAda002,
    TextEmbedding3Large,
}

impl EmbeddingModel {
    fn dims(&self) -> usize {
        match self {
            Self::TextEmbedding3Small => 1536,
            Self::TextEmbeddingAda002 => 1536,
            Self::TextEmbedding3Large => 3072,
        }
    }
}

impl FromStr for EmbeddingModel {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "text-embedding-3-small" => Ok(Self::TextEmbedding3Small),
            "text-embedding-ada-002" => Ok(Self::TextEmbeddingAda002),
            "text-embedding-3-large" => Ok(Self::TextEmbedding3Large),
            _ => Err(Error::InvalidInput {
                message: "Invalid input. Available models are: 'text-embedding-3-small', 'text-embedding-ada-002', 'text-embedding-3-large' ".to_string()
            }),
        }
    }
}

impl ToString for EmbeddingModel {
    fn to_string(&self) -> String {
        match self {
            Self::TextEmbedding3Small => "text-embedding-3-small".to_string(),
            Self::TextEmbeddingAda002 => "text-embedding-ada-002".to_string(),
            Self::TextEmbedding3Large => "text-embedding-3-large".to_string(),
        }
    }
}

impl TryFrom<&str> for EmbeddingModel {
    type Error = Error;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        value.parse()
    }
}

pub struct OpenAIEmbeddingFunction {
    source_column: String,
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
            .field("source_column", &self.source_column)
            .field("model", &self.model)
            .field("credentials", &creds_display)
            .field("api_base", &self.api_base)
            .field("org_id", &self.org_id)
            .finish()
    }
}

impl OpenAIEmbeddingFunction {
    pub fn new<S: Into<String>, A: Into<String>>(source_column: S, api_key: A) -> Self {
        Self::new_impl(
            source_column.into(),
            api_key.into(),
            EmbeddingModel::TextEmbeddingAda002,
        )
    }

    pub fn new_with_model<S: Into<String>, A: Into<String>, M: TryInto<EmbeddingModel>>(
        source_column: S,
        api_key: A,
        model: M,
    ) -> crate::Result<Self>
    where
        M::Error: Into<crate::Error>,
    {
        Ok(Self::new_impl(
            source_column.into(),
            api_key.into(),
            model.try_into().map_err(|e| e.into())?,
        ))
    }

    /// concrete implementation to reduce monomorphization
    fn new_impl(source_column: String, api_key: String, model: EmbeddingModel) -> Self {
        Self {
            source_column,
            model,
            api_key,
            api_base: None,
            org_id: None,
        }
    }
    pub fn api_base<S: Into<String>>(mut self, api_base: S) -> Self {
        self.api_base = Some(api_base.into());
        self
    }

    pub fn org_id<S: Into<String>>(mut self, org_id: S) -> Self {
        self.org_id = Some(org_id.into());
        self
    }
}

impl EmbeddingFunction for OpenAIEmbeddingFunction {
    fn name(&self) -> &str {
        "openai"
    }

    fn source_type(&self) -> Cow<DataType> {
        Cow::Owned(DataType::Utf8)
    }

    fn dest_type(&self) -> Cow<DataType> {
        let n_dims = self.model.dims();
        Cow::Owned(DataType::new_fixed_size_list(
            DataType::Float32,
            n_dims as i32,
            false,
        ))
    }

    fn embed(&self, source: ArrayRef) -> crate::Result<ArrayRef> {
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
        let dims = self.model.dims();
        let req = CreateEmbeddingRequest {
            model: self.model.to_string(),
            input,
            encoding_format: Some(EncodingFormat::Float),
            user: None,
            dimensions: match self.model {
                EmbeddingModel::TextEmbedding3Small | EmbeddingModel::TextEmbedding3Large => {
                    Some(dims as u32)
                }
                EmbeddingModel::TextEmbeddingAda002 => None,
            },
        };

        // TODO: parallelize and batch requests
        let res: crate::Result<FixedSizeListArray> = task::block_in_place(move || {
            Handle::current().block_on(async {
                let mut builder = Float32Builder::new();

                let res = embed.create(req).await.map_err(|e| crate::Error::Runtime {
                    message: format!("OpenAI embed request failed: {e}"),
                })?;

                let len = res.data.len();

                for Embedding { embedding, .. } in res.data.iter() {
                    builder.append_slice(embedding);
                }

                let fsl = DataType::new_fixed_size_list(DataType::Float32, dims as i32, false);

                // We can't use the FixedSizeListBuilder here because it always adds a null bitmap
                // and we want to explicitly work with non-nullable arrays.
                let array_data = ArrayData::builder(fsl)
                    .len(len)
                    .add_child_data(builder.finish().into_data())
                    .build()?;

                Ok(FixedSizeListArray::from(array_data))
            })
        });
        let array: ArrayRef = Arc::new(res?);
        Ok(array)
    }
}
