// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use std::sync::Arc;

use arrow_schema::Schema;
use datafusion_physical_expr::expressions::cast;
use datafusion_physical_plan::expressions::Column;
use datafusion_physical_plan::projection::ProjectionExec;
use datafusion_physical_plan::{ExecutionPlan, PhysicalExpr};

use crate::{Error, Result};

pub fn cast_to_table_schema(
    input: Arc<dyn ExecutionPlan>,
    table_schema: &Schema,
) -> Result<Arc<dyn ExecutionPlan>> {
    let input_schema = input.schema();

    if input_schema.fields() == table_schema.fields() {
        return Ok(input);
    }

    let exprs: Vec<(Arc<dyn PhysicalExpr>, String)> = table_schema
        .fields()
        .iter()
        .map(|target_field| {
            let (input_idx, _) = input_schema
                .column_with_name(target_field.name())
                .ok_or_else(|| Error::InvalidInput {
                    message: format!("field '{}' not found in input schema", target_field.name()),
                })?;

            let col_expr =
                Arc::new(Column::new(target_field.name(), input_idx)) as Arc<dyn PhysicalExpr>;

            let input_field = input_schema.field(input_idx);
            let expr = if input_field.data_type() == target_field.data_type() {
                col_expr
            } else {
                cast(col_expr, &input_schema, target_field.data_type().clone()).map_err(|e| {
                    Error::InvalidInput {
                        message: format!(
                            "cannot cast field '{}' from {} to {}: {}",
                            target_field.name(),
                            input_field.data_type(),
                            target_field.data_type(),
                            e
                        ),
                    }
                })?
            };

            Ok((expr, target_field.name().clone()))
        })
        .collect::<Result<_>>()?;

    let projection = ProjectionExec::try_new(exprs, input).map_err(crate::Error::from)?;

    Ok(Arc::new(projection))
}
