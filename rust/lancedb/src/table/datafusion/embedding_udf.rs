// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! DataFusion physical expression for computing embeddings.
//!
//! This module provides a [`PhysicalExpr`] implementation that wraps an
//! [`EmbeddingFunction`] to compute embeddings as part of a DataFusion
//! execution plan.

use std::any::Any;
use std::fmt::{self, Formatter};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::{DataType, Schema};
use datafusion_common::{DataFusionError, Result as DataFusionResult};
use datafusion_expr::ColumnarValue;
use datafusion_physical_plan::expressions::Column;
use datafusion_physical_plan::projection::ProjectionExec;
use datafusion_physical_plan::{ExecutionPlan, PhysicalExpr};

use crate::embeddings::{EmbeddingDefinition, EmbeddingFunction};
use crate::error::Result;

/// A DataFusion PhysicalExpr that computes embeddings using an EmbeddingFunction.
///
/// This expression takes a source column as input and produces an embedding
/// column as output by applying the wrapped EmbeddingFunction.
#[derive(Debug)]
pub struct EmbeddingPhysicalExpr {
    /// The child expression that provides the source data
    source: Arc<dyn PhysicalExpr>,
    /// The embedding function to apply
    func: Arc<dyn EmbeddingFunction>,
    /// The output data type (cached from func.dest_type())
    return_type: DataType,
}

impl EmbeddingPhysicalExpr {
    /// Create a new EmbeddingPhysicalExpr.
    ///
    /// # Arguments
    /// * `source` - The physical expression that provides the source column data
    /// * `func` - The embedding function to apply
    pub fn new(source: Arc<dyn PhysicalExpr>, func: Arc<dyn EmbeddingFunction>) -> Result<Self> {
        let return_type = func.dest_type()?.into_owned();
        Ok(Self {
            source,
            func,
            return_type,
        })
    }
}

impl std::fmt::Display for EmbeddingPhysicalExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "embedding_{}({})", self.func.name(), self.source)
    }
}

impl PartialEq for EmbeddingPhysicalExpr {
    fn eq(&self, other: &Self) -> bool {
        // Compare by function name and source expression
        self.func.name() == other.func.name() && self.source.eq(&other.source)
    }
}

impl Eq for EmbeddingPhysicalExpr {}

impl Hash for EmbeddingPhysicalExpr {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.func.name().hash(state);
        self.source.hash(state);
        self.return_type.hash(state);
    }
}

impl PhysicalExpr for EmbeddingPhysicalExpr {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self, _input_schema: &Schema) -> DataFusionResult<DataType> {
        Ok(self.return_type.clone())
    }

    fn nullable(&self, input_schema: &Schema) -> DataFusionResult<bool> {
        // Embeddings are nullable if the source is nullable
        self.source.nullable(input_schema)
    }

    fn evaluate(&self, batch: &RecordBatch) -> DataFusionResult<ColumnarValue> {
        // Evaluate the source expression
        let source_value = self.source.evaluate(batch)?;

        // Extract the array from the source value
        let source_array = match source_value {
            ColumnarValue::Array(arr) => arr,
            ColumnarValue::Scalar(s) => s.to_array_of_size(batch.num_rows())?,
        };

        // Compute embeddings
        let embeddings = self
            .func
            .compute_source_embeddings(source_array)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        Ok(ColumnarValue::Array(embeddings))
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.source]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> DataFusionResult<Arc<dyn PhysicalExpr>> {
        if children.len() != 1 {
            return Err(DataFusionError::Internal(
                "EmbeddingPhysicalExpr requires exactly one child".to_string(),
            ));
        }
        Ok(Arc::new(Self {
            source: children[0].clone(),
            func: self.func.clone(),
            return_type: self.return_type.clone(),
        }))
    }

    fn fmt_sql(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "embedding_{}(", self.func.name())?;
        self.source.fmt_sql(f)?;
        write!(f, ")")
    }
}

/// Build a ProjectionExec that computes embedding columns.
///
/// Given an input execution plan and a list of embedding definitions with their
/// corresponding embedding functions, this function creates a ProjectionExec that:
/// 1. Passes through all existing columns from the input
/// 2. Adds new columns for each embedding, computed by applying the embedding
///    function to the source column
///
/// # Arguments
/// * `input` - The input execution plan providing the source data
/// * `embeddings` - A list of (EmbeddingDefinition, EmbeddingFunction) pairs
///
/// # Returns
/// A new ExecutionPlan that includes the computed embedding columns
pub fn build_embedding_projection(
    input: Arc<dyn ExecutionPlan>,
    embeddings: Vec<(EmbeddingDefinition, Arc<dyn EmbeddingFunction>)>,
) -> Result<Arc<dyn ExecutionPlan>> {
    let input_schema = input.schema();

    // Start with all existing columns as pass-through
    let mut projections: Vec<(Arc<dyn PhysicalExpr>, String)> = input_schema
        .fields()
        .iter()
        .enumerate()
        .map(|(i, field)| {
            let expr = Arc::new(Column::new(field.name(), i)) as Arc<dyn PhysicalExpr>;
            (expr, field.name().clone())
        })
        .collect();

    // Add embedding columns
    for (def, func) in embeddings {
        let source_col_idx =
            input_schema
                .index_of(&def.source_column)
                .map_err(|e| crate::Error::InvalidInput {
                    message: format!(
                        "Source column '{}' not found in input schema: {}",
                        def.source_column, e
                    ),
                })?;

        let dest_name = def
            .dest_column
            .clone()
            .unwrap_or_else(|| format!("{}_embedding", &def.source_column));

        // Create the source column expression
        let source_expr: Arc<dyn PhysicalExpr> =
            Arc::new(Column::new(&def.source_column, source_col_idx));

        // Create the embedding expression
        let embedding_expr: Arc<dyn PhysicalExpr> =
            Arc::new(EmbeddingPhysicalExpr::new(source_expr, func)?);

        projections.push((embedding_expr, dest_name));
    }

    let projection =
        ProjectionExec::try_new(projections, input).map_err(|e| crate::Error::Runtime {
            message: format!("Failed to create projection for embeddings: {}", e),
        })?;

    Ok(Arc::new(projection))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Array, StringArray};
    use arrow_schema::Field;
    use datafusion_physical_plan::memory::LazyMemoryExec;
    use parking_lot::RwLock;
    use std::borrow::Cow;

    /// A mock embedding function for testing
    #[derive(Debug)]
    struct MockEmbeddingFunction {
        dim: usize,
    }

    impl EmbeddingFunction for MockEmbeddingFunction {
        fn name(&self) -> &str {
            "mock"
        }

        fn source_type(&self) -> Result<Cow<'_, DataType>> {
            Ok(Cow::Owned(DataType::Utf8))
        }

        fn dest_type(&self) -> Result<Cow<'_, DataType>> {
            Ok(Cow::Owned(DataType::FixedSizeList(
                Arc::new(Field::new("item", DataType::Float32, true)),
                self.dim as i32,
            )))
        }

        fn compute_source_embeddings(&self, source: Arc<dyn Array>) -> Result<Arc<dyn Array>> {
            use arrow_array::builder::{FixedSizeListBuilder, Float32Builder};

            let string_array = source
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| crate::Error::InvalidInput {
                    message: "Expected string array".to_string(),
                })?;

            let mut builder = FixedSizeListBuilder::new(Float32Builder::new(), self.dim as i32);

            for _ in 0..string_array.len() {
                // Generate mock embeddings (all zeros for simplicity)
                for _ in 0..self.dim {
                    builder.values().append_value(0.0);
                }
                builder.append(true);
            }

            Ok(Arc::new(builder.finish()))
        }

        fn compute_query_embeddings(&self, input: Arc<dyn Array>) -> Result<Arc<dyn Array>> {
            self.compute_source_embeddings(input)
        }
    }

    use datafusion_physical_plan::memory::LazyBatchGenerator;

    #[derive(Debug)]
    struct TestBatchGenerator {
        batches: Vec<RecordBatch>,
        index: usize,
    }

    impl std::fmt::Display for TestBatchGenerator {
        fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
            write!(f, "TestBatchGenerator")
        }
    }

    impl LazyBatchGenerator for TestBatchGenerator {
        fn as_any(&self) -> &dyn Any {
            self
        }

        fn generate_next_batch(&mut self) -> DataFusionResult<Option<RecordBatch>> {
            if self.index < self.batches.len() {
                let batch = self.batches[self.index].clone();
                self.index += 1;
                Ok(Some(batch))
            } else {
                Ok(None)
            }
        }
    }

    #[tokio::test]
    async fn test_embedding_expr_creation() {
        let func: Arc<dyn EmbeddingFunction> = Arc::new(MockEmbeddingFunction { dim: 4 });
        let source = Arc::new(Column::new("text", 0)) as Arc<dyn PhysicalExpr>;
        let expr = EmbeddingPhysicalExpr::new(source, func).unwrap();

        assert_eq!(format!("{}", expr), "embedding_mock(text@0)");
    }

    #[tokio::test]
    async fn test_build_embedding_projection() {
        let func: Arc<dyn EmbeddingFunction> = Arc::new(MockEmbeddingFunction { dim: 4 });

        // Create a simple input schema and data
        let schema = Arc::new(arrow_schema::Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("text", DataType::Utf8, false),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(arrow_array::Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["hello", "world", "test"])),
            ],
        )
        .unwrap();

        let generator = TestBatchGenerator {
            batches: vec![batch],
            index: 0,
        };
        let generators: Vec<Arc<RwLock<dyn LazyBatchGenerator>>> =
            vec![Arc::new(RwLock::new(generator))];

        let input: Arc<dyn ExecutionPlan> =
            Arc::new(LazyMemoryExec::try_new(schema, generators).unwrap());

        let def = EmbeddingDefinition::new("text", "mock", Some("text_vector"));
        let embeddings = vec![(def, func)];

        let projection = build_embedding_projection(input, embeddings).unwrap();

        // Verify output schema has the embedding column
        let output_schema = projection.schema();
        assert_eq!(output_schema.fields().len(), 3);
        assert_eq!(output_schema.field(0).name(), "id");
        assert_eq!(output_schema.field(1).name(), "text");
        assert_eq!(output_schema.field(2).name(), "text_vector");

        // Verify the embedding column type
        match output_schema.field(2).data_type() {
            DataType::FixedSizeList(inner, 4) => {
                assert_eq!(inner.data_type(), &DataType::Float32);
            }
            other => panic!("Expected FixedSizeList, got {:?}", other),
        }
    }
}
