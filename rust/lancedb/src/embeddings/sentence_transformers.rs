use std::{borrow::Cow, sync::Arc};

use super::EmbeddingFunction;
use arrow::array::{
    AsArray, Float16Builder, Float32Builder, Float64Builder, Int64Builder, UInt32Builder,
    UInt8Builder,
};
use arrow_array::{Array, FixedSizeListArray};
use arrow_data::ArrayData;
use arrow_schema::DataType;
use candle_core::{CpuStorage, Device, Storage, Tensor};
use candle_nn::VarBuilder;
use candle_transformers::models::bert::{BertModel, Config, DTYPE};
use hf_hub::{api::sync::Api, Repo, RepoType};
use tokenizers::{tokenizer::Tokenizer, PaddingParams};

/// Compute embeddings using huggingface sentence-transformers.
pub struct SentenceTransformersEmbeddingsBuilder {
    /// The sentence-transformers model to use.
    /// Defaults to 'all-MiniLM-L6-v2'
    model: Option<String>,
    /// The device to use for computation.
    /// Defaults to 'cpu'
    device: Option<Device>,
    /// Defaults to true
    normalize: bool,
    n_dims: Option<usize>,
    revision: Option<String>,
    /// path to configuration file.
    /// Defaults to `config.json`
    config_path: Option<String>,
    /// path to tokenizer file.
    /// Defaults to `tokenizer.json`
    tokenizer_path: Option<String>,
    /// path to model file.
    /// Defaults to `model.safetensors`
    model_path: Option<String>,
    /// Padding parameters for the tokenizer.
    padding: Option<PaddingParams>,
}

pub struct SentenceTransformersEmbeddings {
    model: BertModel,
    tokenizer: Tokenizer,
    device: Device,
    n_dims: Option<usize>,
}

impl std::fmt::Debug for SentenceTransformersEmbeddings {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SentenceTransformersEmbeddings")
            .field("tokenizer", &self.tokenizer)
            .field("device", &self.device)
            .field("n_dims", &self.n_dims)
            .finish()
    }
}

impl Default for SentenceTransformersEmbeddingsBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl SentenceTransformersEmbeddingsBuilder {
    pub fn new() -> Self {
        Self {
            model: None,
            device: None,
            normalize: true,
            n_dims: None,
            revision: None,
            config_path: None,
            tokenizer_path: None,
            model_path: None,
            padding: None,
        }
    }

    pub fn model<S: Into<String>>(mut self, name: S) -> Self {
        self.model = Some(name.into());
        self
    }

    pub fn device<D: Into<Device>>(mut self, device: D) -> Self {
        self.device = Some(device.into());
        self
    }

    pub fn normalize(mut self, normalize: bool) -> Self {
        self.normalize = normalize;
        self
    }

    /// If you know the number of dimensions of the embeddings, you can set it here.
    /// This will avoid a call to the model to determine the number of dimensions.
    pub fn ndims(mut self, n_dims: usize) -> Self {
        self.n_dims = Some(n_dims);
        self
    }

    /// If you want to use a specific revision of the model, you can set it here.
    pub fn revision<S: Into<String>>(mut self, revision: S) -> Self {
        self.revision = Some(revision.into());
        self
    }

    /// Set the path to the configuration file.
    /// Defaults to `config.json`
    ///
    /// Note: this is the path inside the huggingface repo, **NOT the path on disk**.
    pub fn config_path<S: Into<String>>(mut self, config: S) -> Self {
        self.config_path = Some(config.into());
        self
    }

    /// Set the path to the tokenizer file.
    /// Defaults to `tokenizer.json`
    ///
    /// Note: this is the path inside the huggingface repo, **NOT the path on disk**.
    pub fn tokenizer_path<S: Into<String>>(mut self, tokenizer: S) -> Self {
        self.tokenizer_path = Some(tokenizer.into());
        self
    }

    /// Set the path inside the huggingface repo to the model file.
    /// Defaults to `model.safetensors`
    ///
    /// Note: this is the path inside the huggingface repo, **NOT the path on disk**.
    ///
    /// Note: we currently only support a single model file.
    pub fn model_path<S: Into<String>>(mut self, model: S) -> Self {
        self.model_path = Some(model.into());
        self
    }

    pub fn build(mut self) -> SentenceTransformersEmbeddings {
        let model_id = self.model.as_deref().unwrap_or("all-MiniLM-L6-v2");
        let model_id = format!("sentence-transformers/{}", model_id);
        let config = self.config_path.as_deref().unwrap_or("config.json");
        let tokenizer = self.tokenizer_path.as_deref().unwrap_or("tokenizer.json");
        let model_path = self.model_path.as_deref().unwrap_or("model.safetensors");
        let device = self.device.unwrap_or(Device::Cpu);

        let repo = if let Some(revision) = self.revision {
            Repo::with_revision(model_id, RepoType::Model, revision.to_string())
        } else {
            Repo::new(model_id, RepoType::Model)
        };
        let (config_filename, tokenizer_filename, weights_filename) = {
            let api = Api::new().expect("failed to create api");
            let api = api.repo(repo);
            let config = api.get(config).expect("failed to get config");
            let tokenizer = api.get(tokenizer).expect("failed to get tokenizer");
            let weights = api.get(model_path).expect("failed to get weights");

            (config, tokenizer, weights)
        };

        let config = std::fs::read_to_string(config_filename).expect("failed to read config");
        let config: Config = serde_json::from_str(&config).expect("failed to parse config");

        let mut tokenizer =
            Tokenizer::from_file(tokenizer_filename).expect("failed to load tokenizer");
        if self.padding.is_some() {
            tokenizer.with_padding(self.padding.take());
        }

        let vb = unsafe {
            VarBuilder::from_mmaped_safetensors(&[weights_filename], DTYPE, &device)
                .expect("failed to load weights")
        };
        let model = BertModel::load(vb, &config).expect("failed to load model");
        SentenceTransformersEmbeddings {
            model,
            tokenizer,
            device,
            n_dims: self.n_dims,
        }
    }
}

macro_rules! from_cpu_storage {
    ($cpu_storage:ident, $data:ident, $builder:ident, $layout:ident, $embeddings:ident, $d1:ident, $d2:ident) => {{
        let mut builder = $builder::new();
        match $layout.contiguous_offsets() {
            Some((o1, o2)) => {
                let $data = &$data[o1..o2];
                builder.append_slice($data);
                builder.finish()
            }
            None => {
                let mut src_index = $embeddings.strided_index();

                for _idx_row in 0..$d1 {
                    let row = (0..$d2)
                        .map(|_| $data[src_index.next().unwrap()])
                        .collect::<Vec<_>>();
                    builder.append_slice(&row);
                }
                builder.finish()
            }
        }
    }};
}
impl SentenceTransformersEmbeddings {
    pub fn builder() -> SentenceTransformersEmbeddingsBuilder {
        SentenceTransformersEmbeddingsBuilder::new()
    }

    fn ndims(&self) -> crate::Result<usize> {
        if let Some(n_dims) = self.n_dims {
            Ok(n_dims)
        } else {
            Ok(self.compute_ndims_and_dtype()?.0)
        }
    }

    fn compute_ndims_and_dtype(&self) -> crate::Result<(usize, DataType)> {
        let token = self.tokenizer.encode("hello", true).unwrap();
        let token = token.get_ids().to_vec();
        let input_ids =
            Tensor::new(vec![token], &self.device).map_err(|e| crate::Error::Runtime {
                message: format!("failed to create token ids: {}", e),
            })?;

        let token_type_ids = input_ids.zeros_like().map_err(|e| crate::Error::Runtime {
            message: format!("failed to create token ids: {}", e),
        })?;

        let embeddings = self
            .model
            .forward(&input_ids, &token_type_ids)
            .map_err(|e| crate::Error::Runtime {
                message: format!("failed to compute embeddings: {}", e),
            })?;

        let (_, _, n_dims) = embeddings.dims3().unwrap();
        let (storage, _) = embeddings.storage_and_layout();
        let dtype = match &*storage {
            Storage::Cpu(CpuStorage::U8(_)) => DataType::UInt8,
            Storage::Cpu(CpuStorage::U32(_)) => DataType::UInt32,
            Storage::Cpu(CpuStorage::I64(_)) => DataType::Int64,
            Storage::Cpu(CpuStorage::F16(_)) => DataType::Float16,
            Storage::Cpu(CpuStorage::F32(_)) => DataType::Float32,
            Storage::Cpu(CpuStorage::F64(_)) => DataType::Float64,
            Storage::Cpu(CpuStorage::BF16(_)) => {
                return Err(crate::Error::Runtime {
                    message: "unsupported data type".to_string(),
                })
            }
            _ => panic!("unsupported device"),
        };
        Ok((n_dims, dtype))
    }

    fn compute_inner(&self, source: Arc<dyn Array>) -> crate::Result<(Arc<dyn Array>, DataType)> {
        if source.is_nullable() {
            return Err(crate::Error::InvalidInput {
                message: "Expected non-nullable data type".to_string(),
            });
        }
        if !matches!(source.data_type(), DataType::Utf8 | DataType::LargeUtf8) {
            return Err(crate::Error::InvalidInput {
                message: "Expected Utf8 data type".to_string(),
            });
        }

        let tokens = match source.data_type() {
            DataType::Utf8 => {
                if source.null_count() > 0 {
                    return Err(crate::Error::Runtime {
                        message: "null values not supported".to_string(),
                    });
                }
                source
                    .as_string::<i32>()
                    // TODO: should we do this in parallel? (e.g. using rayon)
                    .into_iter()
                    .map(|v| {
                        let value = v.unwrap();
                        let token = self.tokenizer.encode(value, true).unwrap();
                        let token = token.get_ids().to_vec();
                        Tensor::new(token.as_slice(), &self.device).map_err(|e| {
                            crate::Error::Runtime {
                                message: format!("failed to create token ids: {}", e),
                            }
                        })
                    })
                    .collect::<crate::Result<Vec<_>>>()?
            }
            _ => {
                return Err(crate::Error::Runtime {
                    message: "invalid type".to_string(),
                })
            }
        };

        let embeddings = Tensor::stack(&tokens, 0)
            .and_then(|tokens| {
                let token_type_ids = tokens.zeros_like()?;
                self.model.forward(&tokens, &token_type_ids)
            })
            .map_err(|e| crate::Error::Runtime {
                message: format!("failed to compute embeddings: {}", e),
            })?;
        let (_, n_tokens, _) = embeddings.dims3().map_err(|e| crate::Error::Runtime {
            message: format!("failed to get embeddings dimensions: {}", e),
        })?;

        let embeddings = (embeddings.sum(1).unwrap() / (n_tokens as f64)).map_err(|e| {
            crate::Error::Runtime {
                message: format!("failed to compute mean embeddings: {}", e),
            }
        })?;
        let dims = embeddings.shape().dims().len();
        let (arr, dtype): (Arc<dyn Array>, DataType) = match dims {
            2 => {
                let (d1, d2) = embeddings.dims2().map_err(|e| crate::Error::Runtime {
                    message: format!("failed to get embeddings dimensions: {}", e),
                })?;
                let (storage, layout) = embeddings.storage_and_layout();
                match &*storage {
                    Storage::Cpu(CpuStorage::U8(data)) => (
                        Arc::new(from_cpu_storage!(
                            u8,
                            data,
                            UInt8Builder,
                            layout,
                            embeddings,
                            d1,
                            d2
                        )),
                        DataType::UInt8,
                    ),
                    Storage::Cpu(CpuStorage::U32(data)) => (
                        Arc::new(from_cpu_storage!(
                            u32,
                            data,
                            UInt32Builder,
                            layout,
                            embeddings,
                            d1,
                            d2
                        )),
                        DataType::UInt32,
                    ),
                    Storage::Cpu(CpuStorage::I64(data)) => (
                        Arc::new(from_cpu_storage!(
                            i64,
                            data,
                            Int64Builder,
                            layout,
                            embeddings,
                            d1,
                            d2
                        )),
                        DataType::Int64,
                    ),
                    Storage::Cpu(CpuStorage::F16(data)) => (
                        Arc::new(from_cpu_storage!(
                            f16,
                            data,
                            Float16Builder,
                            layout,
                            embeddings,
                            d1,
                            d2
                        )),
                        DataType::Float16,
                    ),
                    Storage::Cpu(CpuStorage::F32(data)) => (
                        Arc::new(from_cpu_storage!(
                            f32,
                            data,
                            Float32Builder,
                            layout,
                            embeddings,
                            d1,
                            d2
                        )),
                        DataType::Float32,
                    ),
                    Storage::Cpu(CpuStorage::F64(data)) => (
                        Arc::new(from_cpu_storage!(
                            f64,
                            data,
                            Float64Builder,
                            layout,
                            embeddings,
                            d1,
                            d2
                        )),
                        DataType::Float64,
                    ),
                    Storage::Cpu(CpuStorage::BF16(_)) => {
                        panic!("Unsupported storage type: BF16")
                    }
                    _ => unreachable!("Only CPU storage currently supported"),
                }
            }
            n_dims => todo!("Only 2 dimensions supported, got {}", n_dims),
        };
        Ok((arr, dtype))
    }
}

impl EmbeddingFunction for SentenceTransformersEmbeddings {
    fn name(&self) -> &str {
        "sentence-transformers"
    }

    fn source_type(&self) -> crate::Result<std::borrow::Cow<arrow_schema::DataType>> {
        Ok(Cow::Owned(DataType::Utf8))
    }

    fn dest_type(&self) -> crate::Result<std::borrow::Cow<arrow_schema::DataType>> {
        let (n_dims, dtype) = self.compute_ndims_and_dtype()?;
        Ok(Cow::Owned(DataType::new_fixed_size_list(
            dtype,
            n_dims as i32,
            false,
        )))
    }

    fn compute_source_embeddings(&self, source: Arc<dyn Array>) -> crate::Result<Arc<dyn Array>> {
        let len = source.len();
        let n_dims = self.ndims()?;
        let (inner, dtype) = self.compute_inner(source)?;

        let fsl = DataType::new_fixed_size_list(dtype, n_dims as i32, false);

        // We can't use the FixedSizeListBuilder here because it always adds a null bitmap
        // and we want to explicitly work with non-nullable arrays.
        let array_data = ArrayData::builder(fsl)
            .len(len)
            .add_child_data(inner.into_data())
            .build()?;

        Ok(Arc::new(FixedSizeListArray::from(array_data)))
    }

    fn compute_query_embeddings(&self, input: Arc<dyn Array>) -> crate::Result<Arc<dyn Array>> {
        let (arr, _) = self.compute_inner(input)?;
        Ok(arr)
    }
}
