// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

#[cfg(feature = "openai")]
pub mod openai;

#[cfg(feature = "sentence-transformers")]
pub mod sentence_transformers;

#[cfg(feature = "bedrock")]
pub mod bedrock;

use lance_arrow::RecordBatchExt;
use std::{
    borrow::Cow,
    collections::{HashMap, HashSet},
    sync::{Arc, RwLock},
};

use arrow_array::{Array, RecordBatch, RecordBatchReader};
use arrow_schema::{DataType, Field, SchemaBuilder, SchemaRef};
// use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::{
    Error,
    error::Result,
    table::{ColumnDefinition, ColumnKind, TableDefinition},
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
    fn source_type(&self) -> Result<Cow<'_, DataType>>;
    /// The type of the output data
    /// This should **always** match the output of the `embed` function
    fn dest_type(&self) -> Result<Cow<'_, DataType>>;
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
///
/// This is a wrapper around another record batch reader that applies embedding functions
/// when reading from the record batch.
///
/// When multiple embedding functions are defined, they are computed in parallel using
/// scoped threads to improve performance. For a single embedding function, computation
/// is done inline without threading overhead.
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

/// Compute embedding arrays for a batch.
///
/// When multiple embedding functions are defined, they are computed in parallel using
/// scoped threads. For a single embedding function, computation is done inline.
fn compute_embedding_arrays(
    batch: &RecordBatch,
    embeddings: &[(EmbeddingDefinition, Arc<dyn EmbeddingFunction>)],
) -> Result<Vec<Arc<dyn Array>>> {
    let input_columns = embeddings
        .iter()
        .map(|(fld, func)| {
            let src_column =
                batch
                    .column_by_name(&fld.source_column)
                    .ok_or_else(|| Error::InvalidInput {
                        message: format!("Source column '{}' not found", fld.source_column),
                    })?;
            Ok((src_column.clone(), func))
        })
        .collect::<Result<Vec<_>>>()?;

    if batch.num_rows() == 0 {
        return input_columns
            .iter()
            .map(|(_, func)| Ok(arrow_array::new_empty_array(func.dest_type()?.as_ref())))
            .collect();
    }

    if input_columns.len() == 1 {
        let (src_column, func) = &input_columns[0];
        return Ok(vec![func.compute_source_embeddings(src_column.clone())?]);
    }

    // Parallel path: multiple embeddings
    std::thread::scope(|s| {
        let handles: Vec<_> = input_columns
            .iter()
            .map(|(src_column, func)| {
                let handle = s.spawn(move || func.compute_source_embeddings(src_column.clone()));

                Ok(handle)
            })
            .collect::<Result<_>>()?;

        handles
            .into_iter()
            .map(|h| {
                h.join().map_err(|e| Error::Runtime {
                    message: format!("Thread panicked during embedding computation: {:?}", e),
                })?
            })
            .collect()
    })
}

/// Compute the output schema when embeddings are applied to a base schema.
///
/// This returns the schema with embedding columns appended.
pub fn compute_output_schema(
    base_schema: &SchemaRef,
    embeddings: &[(EmbeddingDefinition, Arc<dyn EmbeddingFunction>)],
) -> Result<SchemaRef> {
    let mut sb: SchemaBuilder = base_schema.as_ref().into();

    for (ed, func) in embeddings {
        let src_field = base_schema
            .field_with_name(&ed.source_column)
            .map_err(|_| Error::InvalidInput {
                message: format!("Source column '{}' not found in schema", ed.source_column),
            })?;

        let field_name = ed
            .dest_column
            .clone()
            .unwrap_or_else(|| format!("{}_embedding", &ed.source_column));

        sb.push(Field::new(
            field_name,
            func.dest_type()?.into_owned(),
            src_field.is_nullable(),
        ));
    }

    Ok(Arc::new(sb.finish()))
}

/// Compute embeddings for a batch and append as new columns.
///
/// This function computes embeddings using the provided embedding functions and
/// appends them as new columns to the batch.
pub fn compute_embeddings_for_batch(
    batch: RecordBatch,
    embeddings: &[(EmbeddingDefinition, Arc<dyn EmbeddingFunction>)],
) -> Result<RecordBatch> {
    let embedding_arrays = compute_embedding_arrays(&batch, embeddings)?;

    let mut result = batch;
    for ((fld, _), embedding) in embeddings.iter().zip(embedding_arrays.iter()) {
        let dst_field_name = fld
            .dest_column
            .clone()
            .unwrap_or_else(|| format!("{}_embedding", &fld.source_column));

        let dst_field = Field::new(
            dst_field_name,
            embedding.data_type().clone(),
            embedding.nulls().is_some(),
        );

        result = result.try_with_column(dst_field, embedding.clone())?;
    }
    Ok(result)
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
            Ok(batch) => match compute_embeddings_for_batch(batch, &self.embeddings) {
                Ok(batch_with_embeddings) => Some(Ok(batch_with_embeddings)),
                Err(e) => Some(Err(arrow_schema::ArrowError::ComputeError(format!(
                    "Error computing embedding: {}",
                    e
                )))),
            },
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

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    use arrow_array::{Array, ArrayRef, FixedSizeListArray, RecordBatch, StringArray};
    use arrow_schema::DataType;

    use super::*;

    #[derive(Debug)]
    struct FailingEmbedding {
        calls: AtomicUsize,
    }

    impl EmbeddingFunction for FailingEmbedding {
        fn name(&self) -> &str {
            "failing"
        }

        fn source_type(&self) -> Result<Cow<'_, DataType>> {
            Ok(Cow::Owned(DataType::Utf8))
        }

        fn dest_type(&self) -> Result<Cow<'_, DataType>> {
            Ok(Cow::Owned(DataType::new_fixed_size_list(
                DataType::Float32,
                3,
                false,
            )))
        }

        fn compute_source_embeddings(&self, _source: Arc<dyn Array>) -> Result<Arc<dyn Array>> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Err(Error::Runtime {
                message: "embedding function must not receive an empty batch".to_string(),
            })
        }

        fn compute_query_embeddings(&self, _input: Arc<dyn Array>) -> Result<Arc<dyn Array>> {
            unreachable!("query embeddings are not exercised by this test")
        }
    }

    #[test]
    fn empty_batch_skips_embedding_functions() {
        let embedding_function = Arc::new(FailingEmbedding {
            calls: AtomicUsize::new(0),
        });
        let source: ArrayRef = Arc::new(StringArray::from(Vec::<&str>::new()));
        let batch = RecordBatch::try_from_iter([("text", source)]).unwrap();
        let embeddings = vec![(
            EmbeddingDefinition::new("text", "failing", Some("text_embedding")),
            embedding_function.clone() as Arc<dyn EmbeddingFunction>,
        )];

        let result = compute_embeddings_for_batch(batch, &embeddings).unwrap();

        assert_eq!(embedding_function.calls.load(Ordering::SeqCst), 0);
        assert_eq!(result.num_rows(), 0);

        let embedding = result.column_by_name("text_embedding").unwrap();
        assert_eq!(
            embedding.data_type(),
            &DataType::new_fixed_size_list(DataType::Float32, 3, false)
        );
        assert_eq!(embedding.null_count(), 0);

        let embedding = embedding
            .as_any()
            .downcast_ref::<FixedSizeListArray>()
            .unwrap();
        assert_eq!(embedding.len(), 0);
        assert_eq!(embedding.value_length(), 3);
        assert_eq!(embedding.values().len(), 0);
    }

    #[test]
    fn empty_batch_still_validates_source_column() {
        let embedding_function = Arc::new(FailingEmbedding {
            calls: AtomicUsize::new(0),
        });
        let source: ArrayRef = Arc::new(StringArray::from(Vec::<&str>::new()));
        let batch = RecordBatch::try_from_iter([("text", source)]).unwrap();
        let embeddings = vec![(
            EmbeddingDefinition::new("missing_column", "failing", Some("text_embedding")),
            embedding_function.clone() as Arc<dyn EmbeddingFunction>,
        )];

        let result = compute_embeddings_for_batch(batch, &embeddings);
        assert!(result.is_err());
        assert!(
            matches!(result.unwrap_err(), Error::InvalidInput { .. }),
            "expected InvalidInput error when source column is missing"
        );
        assert_eq!(embedding_function.calls.load(Ordering::SeqCst), 0);
    }
}
