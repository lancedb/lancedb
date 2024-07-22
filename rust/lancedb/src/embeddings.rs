// Copyright 2024 LanceDB Developers.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
#[cfg(feature = "openai")]
pub mod openai;

#[cfg(feature = "sentence-transformers")]
pub mod sentence_transformers;

use lance::arrow::RecordBatchExt;
use std::{
    borrow::Cow,
    collections::{HashMap, HashSet},
    sync::{Arc, RwLock},
};

use arrow_array::{Array, RecordBatch, RecordBatchReader};
use arrow_schema::{DataType, Field, SchemaBuilder};
// use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::{
    error::Result,
    table::{ColumnDefinition, ColumnKind, TableDefinition},
    Error,
};

/// Trait for embedding functions
///
/// An embedding function is a function that is applied to a column of input data
/// to produce an "embedding" of that input.  This embedding is then stored in the
/// database alongside (or instead of) the original input.
///
/// An "embedding" is often a lower-dimensional representation of the input data.
/// For example, sentence-transformers can be used to embed sentences into a 768-dimensional
/// vector space.  This is useful for tasks like similarity search, where we want to find
/// similar sentences to a query sentence.
///
/// To use an embedding function you must first register it with the `EmbeddingsRegistry`.
/// Then you can define it on a column in the table schema. That embedding will then be used
/// to embed the data in that column.
pub trait EmbeddingFunction: std::fmt::Debug + Send + Sync {
    fn name(&self) -> &str;
    /// The type of the input data
    fn source_type(&self) -> Result<Cow<DataType>>;
    /// The type of the output data
    /// This should **always** match the output of the `embed` function
    fn dest_type(&self) -> Result<Cow<DataType>>;
    /// Compute the embeddings for the source column in the database
    fn compute_source_embeddings(&self, source: Arc<dyn Array>) -> Result<Arc<dyn Array>>;
    /// Compute the embeddings for a given user query
    fn compute_query_embeddings(&self, input: Arc<dyn Array>) -> Result<Arc<dyn Array>>;
}

/// Defines an embedding from input data into a lower-dimensional space
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct EmbeddingDefinition {
    /// The name of the column in the input data
    pub source_column: String,
    /// The name of the embedding column, if not specified
    /// it will be the source column with `_embedding` appended
    pub dest_column: Option<String>,
    /// The name of the embedding function to apply
    pub embedding_name: String,
}

impl EmbeddingDefinition {
    pub fn new<S: Into<String>>(source_column: S, embedding_name: S, dest: Option<S>) -> Self {
        Self {
            source_column: source_column.into(),
            dest_column: dest.map(|d| d.into()),
            embedding_name: embedding_name.into(),
        }
    }
}

/// A registry of embedding
pub trait EmbeddingRegistry: Send + Sync + std::fmt::Debug {
    /// Return the names of all registered embedding functions
    fn functions(&self) -> HashSet<String>;
    /// Register a new [`EmbeddingFunction
    /// Returns an error if the function can not be registered
    fn register(&self, name: &str, function: Arc<dyn EmbeddingFunction>) -> Result<()>;
    /// Get an embedding function by name
    fn get(&self, name: &str) -> Option<Arc<dyn EmbeddingFunction>>;
}

/// A [`EmbeddingRegistry`] that uses in-memory [`HashMap`]s
#[derive(Debug, Default, Clone)]
pub struct MemoryRegistry {
    functions: Arc<RwLock<HashMap<String, Arc<dyn EmbeddingFunction>>>>,
}

impl EmbeddingRegistry for MemoryRegistry {
    fn functions(&self) -> HashSet<String> {
        self.functions.read().unwrap().keys().cloned().collect()
    }
    fn register(&self, name: &str, function: Arc<dyn EmbeddingFunction>) -> Result<()> {
        self.functions
            .write()
            .unwrap()
            .insert(name.to_string(), function);

        Ok(())
    }

    fn get(&self, name: &str) -> Option<Arc<dyn EmbeddingFunction>> {
        self.functions.read().unwrap().get(name).cloned()
    }
}

impl MemoryRegistry {
    /// Create a new `MemoryRegistry`
    pub fn new() -> Self {
        Self::default()
    }
}

/// A record batch reader that has embeddings applied to it
/// This is a wrapper around another record batch reader that applies an embedding function
/// when reading from the record batch
pub struct WithEmbeddings<R: RecordBatchReader> {
    inner: R,
    embeddings: Vec<(EmbeddingDefinition, Arc<dyn EmbeddingFunction>)>,
}

/// A record batch that might have embeddings applied to it.
pub enum MaybeEmbedded<R: RecordBatchReader> {
    /// The record batch reader has embeddings applied to it
    Yes(WithEmbeddings<R>),
    /// The record batch reader does not have embeddings applied to it
    /// The inner record batch reader is returned as-is
    No(R),
}

impl<R: RecordBatchReader> MaybeEmbedded<R> {
    /// Create a new RecordBatchReader with embeddings applied to it if the table definition
    /// specifies an embedding column and the registry contains an embedding function with that name
    /// Otherwise, this is a no-op and the inner RecordBatchReader is returned.
    pub fn try_new(
        inner: R,
        table_definition: TableDefinition,
        registry: Option<Arc<dyn EmbeddingRegistry>>,
    ) -> Result<Self> {
        if let Some(registry) = registry {
            let mut embeddings = Vec::with_capacity(table_definition.column_definitions.len());
            for cd in table_definition.column_definitions.iter() {
                if let ColumnKind::Embedding(embedding_def) = &cd.kind {
                    match registry.get(&embedding_def.embedding_name) {
                        Some(func) => {
                            embeddings.push((embedding_def.clone(), func));
                        }
                        None => {
                            return Err(Error::EmbeddingFunctionNotFound {
                                name: embedding_def.embedding_name.clone(),
                                reason: format!(
                                    "Table was defined with an embedding column `{}` but no embedding function was found with that name within the registry.",
                                    embedding_def.embedding_name
                                ),
                            });
                        }
                    }
                }
            }

            if !embeddings.is_empty() {
                return Ok(Self::Yes(WithEmbeddings { inner, embeddings }));
            }
        };

        // No embeddings to apply
        Ok(Self::No(inner))
    }
}

impl<R: RecordBatchReader> WithEmbeddings<R> {
    pub fn new(
        inner: R,
        embeddings: Vec<(EmbeddingDefinition, Arc<dyn EmbeddingFunction>)>,
    ) -> Self {
        Self { inner, embeddings }
    }
}

impl<R: RecordBatchReader> WithEmbeddings<R> {
    fn dest_fields(&self) -> Result<Vec<Field>> {
        let schema = self.inner.schema();
        self.embeddings
            .iter()
            .map(|(ed, func)| {
                let src_field = schema.field_with_name(&ed.source_column).unwrap();

                let field_name = ed
                    .dest_column
                    .clone()
                    .unwrap_or_else(|| format!("{}_embedding", &ed.source_column));
                Ok(Field::new(
                    field_name,
                    func.dest_type()?.into_owned(),
                    src_field.is_nullable(),
                ))
            })
            .collect()
    }

    fn column_defs(&self) -> Vec<ColumnDefinition> {
        let base_schema = self.inner.schema();
        base_schema
            .fields()
            .iter()
            .map(|_| ColumnDefinition {
                kind: ColumnKind::Physical,
            })
            .chain(self.embeddings.iter().map(|(ed, _)| ColumnDefinition {
                kind: ColumnKind::Embedding(ed.clone()),
            }))
            .collect::<Vec<_>>()
    }

    pub fn table_definition(&self) -> Result<TableDefinition> {
        let base_schema = self.inner.schema();

        let output_fields = self.dest_fields()?;
        let column_definitions = self.column_defs();

        let mut sb: SchemaBuilder = base_schema.as_ref().into();
        sb.extend(output_fields);

        let schema = Arc::new(sb.finish());
        Ok(TableDefinition {
            schema,
            column_definitions,
        })
    }
}

impl<R: RecordBatchReader> Iterator for MaybeEmbedded<R> {
    type Item = std::result::Result<RecordBatch, arrow_schema::ArrowError>;
    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Yes(inner) => inner.next(),
            Self::No(inner) => inner.next(),
        }
    }
}

impl<R: RecordBatchReader> RecordBatchReader for MaybeEmbedded<R> {
    fn schema(&self) -> Arc<arrow_schema::Schema> {
        match self {
            Self::Yes(inner) => inner.schema(),
            Self::No(inner) => inner.schema(),
        }
    }
}

impl<R: RecordBatchReader> Iterator for WithEmbeddings<R> {
    type Item = std::result::Result<RecordBatch, arrow_schema::ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        let batch = self.inner.next()?;
        match batch {
            Ok(mut batch) => {
                // todo: parallelize this
                for (fld, func) in self.embeddings.iter() {
                    let src_column = batch.column_by_name(&fld.source_column).unwrap();
                    let embedding = match func.compute_source_embeddings(src_column.clone()) {
                        Ok(embedding) => embedding,
                        Err(e) => {
                            return Some(Err(arrow_schema::ArrowError::ComputeError(format!(
                                "Error computing embedding: {}",
                                e
                            ))))
                        }
                    };
                    let dst_field_name = fld
                        .dest_column
                        .clone()
                        .unwrap_or_else(|| format!("{}_embedding", &fld.source_column));

                    let dst_field = Field::new(
                        dst_field_name,
                        embedding.data_type().clone(),
                        embedding.nulls().is_some(),
                    );

                    match batch.try_with_column(dst_field.clone(), embedding) {
                        Ok(b) => batch = b,
                        Err(e) => return Some(Err(e)),
                    };
                }
                Some(Ok(batch))
            }
            Err(e) => Some(Err(e)),
        }
    }
}

impl<R: RecordBatchReader> RecordBatchReader for WithEmbeddings<R> {
    fn schema(&self) -> Arc<arrow_schema::Schema> {
        self.table_definition()
            .expect("table definition should be infallible at this point")
            .into_rich_schema()
    }
}
