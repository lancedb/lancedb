use std::sync::Arc;

use arrow_schema::SchemaRef;
use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};

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

/// Defines the type of column
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ColumnKind {
    /// Columns populated by data from the user (this is the most common case)
    Physical,
    /// Columns populated by applying an embedding function to the input
    Embedding(EmbeddingDefinition),
}

/// Defines a column in a table
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnDefinition {
    /// The source of the column data
    pub kind: ColumnKind,
}

#[derive(Debug, Clone)]
pub struct TableDefinition {
    pub column_definitions: Vec<ColumnDefinition>,
    pub schema: SchemaRef,
}

impl TableDefinition {
    pub fn new(schema: SchemaRef, column_definitions: Vec<ColumnDefinition>) -> Self {
        Self {
            column_definitions,
            schema,
        }
    }

    pub fn new_from_schema(schema: SchemaRef) -> Self {
        let column_definitions = schema
            .fields()
            .iter()
            .map(|_| ColumnDefinition {
                kind: ColumnKind::Physical,
            })
            .collect();
        Self::new(schema, column_definitions)
    }

    pub fn try_from_rich_schema(schema: SchemaRef) -> Result<Self> {
        let column_definitions = schema.metadata.get("lancedb::column_definitions");
        if let Some(column_definitions) = column_definitions {
            let column_definitions: Vec<ColumnDefinition> =
                serde_json::from_str(column_definitions).map_err(|e| Error::Runtime {
                    message: format!("Failed to deserialize column definitions: {}", e),
                })?;
            Ok(Self::new(schema, column_definitions))
        } else {
            let column_definitions = schema
                .fields()
                .iter()
                .map(|_| ColumnDefinition {
                    kind: ColumnKind::Physical,
                })
                .collect();
            Ok(Self::new(schema, column_definitions))
        }
    }

    pub fn into_rich_schema(self) -> SchemaRef {
        // We have full control over the structure of column definitions.  This should
        // not fail, except for a bug
        let lancedb_metadata = serde_json::to_string(&self.column_definitions).unwrap();
        let mut schema_with_metadata = (*self.schema).clone();
        schema_with_metadata
            .metadata
            .insert("lancedb::column_definitions".to_string(), lancedb_metadata);
        Arc::new(schema_with_metadata)
    }
}
