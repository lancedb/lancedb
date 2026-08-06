// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema};
use datafusion::prelude::Expr;
use datafusion::scalar::ScalarValue;
use lance_datafusion::planner::Planner;

use crate::Dataset;
use crate::Result;

use super::NewColumnTransform;

/// Optimizes a `NewColumnTransform` into
pub(super) trait NewColumnTransformOptimizer: Send + Sync {
    /// Optimize the passed `NewColumnTransform` to a more efficient form.
    fn optimize(
        &self,
        dataset: &Dataset,
        transform: NewColumnTransform,
    ) -> Result<NewColumnTransform>;
}

/// A `NewColumnTransformOptimizer` that chains multiple `NewColumnTransformOptimizer`s together.
pub(super) struct ChainedNewColumnTransformOptimizer {
    optimizers: Vec<Box<dyn NewColumnTransformOptimizer>>,
}

impl ChainedNewColumnTransformOptimizer {
    pub(super) fn new(optimizers: Vec<Box<dyn NewColumnTransformOptimizer>>) -> Self {
        Self { optimizers }
    }

    pub(super) fn add_optimizer(&mut self, optimizer: Box<dyn NewColumnTransformOptimizer>) {
        self.optimizers.push(optimizer);
    }
}

/// A `NewColumnTransformOptimizer` that chains multiple `NewColumnTransformOptimizer`s together.
impl NewColumnTransformOptimizer for ChainedNewColumnTransformOptimizer {
    fn optimize(
        &self,
        dataset: &Dataset,
        transform: NewColumnTransform,
    ) -> Result<NewColumnTransform> {
        let mut transform = transform;
        for optimizer in &self.optimizers {
            transform = optimizer.optimize(dataset, transform)?;
        }
        Ok(transform)
    }
}

/// Optimizes a `NewColumnTransform` that is a SQL expression to a `NewColumnTransform::AllNulls` if
/// the SQL expression is "NULL". For example
/// `NewColumnTransform::SqlExpression(vec![("new_col", "CAST(NULL AS int)"])`
/// would be optimized to
/// `NewColumnTransform::AllNulls(Schema::new(vec![Field::new("new_col", DataType::Int)]))`.
///
pub(super) struct SqlToAllNullsOptimizer;

impl SqlToAllNullsOptimizer {
    pub(super) fn new() -> Self {
        Self
    }

    fn is_all_null(&self, expr: &Expr) -> AllNullsResult {
        match expr {
            Expr::Cast(cast) => {
                if matches!(cast.expr.as_ref(), Expr::Literal(ScalarValue::Null, _)) {
                    let data_type = cast.field.data_type().clone();
                    AllNullsResult::AllNulls(data_type)
                } else {
                    AllNullsResult::NotAllNulls
                }
            }
            _ => AllNullsResult::NotAllNulls,
        }
    }
}

enum AllNullsResult {
    AllNulls(DataType),
    NotAllNulls,
}

impl NewColumnTransformOptimizer for SqlToAllNullsOptimizer {
    fn optimize(
        &self,
        dataset: &Dataset,
        transform: NewColumnTransform,
    ) -> Result<NewColumnTransform> {
        match &transform {
            NewColumnTransform::SqlExpressions(expressions) => {
                let arrow_schema = Arc::new(Schema::from(dataset.schema()));
                let planner = Planner::new(arrow_schema);
                let mut all_null_schema_fields = vec![];
                for (name, expr) in expressions {
                    let expr = planner.parse_expr(expr)?;
                    if let AllNullsResult::AllNulls(data_type) = self.is_all_null(&expr) {
                        let field = Field::new(name, data_type, true);
                        all_null_schema_fields.push(field);
                    } else {
                        return Ok(transform);
                    }
                }

                let all_null_schema = Schema::new(all_null_schema_fields);
                Ok(NewColumnTransform::AllNulls(Arc::new(all_null_schema)))
            }
            _ => Ok(transform),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    use arrow_array::RecordBatchIterator;

    #[tokio::test]
    async fn test_sql_to_all_null_transform() {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, true)]));
        let empty_reader = RecordBatchIterator::new(vec![], schema.clone());
        let dataset = Arc::new(
            Dataset::write(empty_reader, "memory://", None)
                .await
                .unwrap(),
        );

        let original = NewColumnTransform::SqlExpressions(vec![
            ("new_col1".to_string(), "CAST(NULL AS int)".to_string()),
            ("new_col2".to_string(), "CAST(NULL AS bigint)".to_string()),
        ]);

        let optimizer = SqlToAllNullsOptimizer::new();
        let result = optimizer.optimize(&dataset, original).unwrap();

        assert!(matches!(result, NewColumnTransform::AllNulls(_)));
        if let NewColumnTransform::AllNulls(schema) = result {
            assert_eq!(schema.fields().len(), 2);
            assert_eq!(schema.field(0).name(), "new_col1");
            assert_eq!(schema.field(0).data_type(), &DataType::Int32);
            assert!(schema.field(0).is_nullable());
            assert_eq!(schema.field(1).name(), "new_col2");
            assert_eq!(schema.field(1).data_type(), &DataType::Int64);
            assert!(schema.field(1).is_nullable());
        }
    }
}
