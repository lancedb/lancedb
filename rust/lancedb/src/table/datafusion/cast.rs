// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use std::sync::Arc;

use arrow_schema::{DataType, Field, FieldRef, Fields, Schema};
use datafusion::functions::core::{get_field, named_struct};
use datafusion_common::config::ConfigOptions;
use datafusion_common::ScalarValue;
use datafusion_physical_expr::expressions::{cast, Literal};
use datafusion_physical_expr::ScalarFunctionExpr;
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

    let exprs = build_field_exprs(
        input_schema.fields(),
        table_schema.fields(),
        &|idx| Arc::new(Column::new(input_schema.field(idx).name(), idx)) as Arc<dyn PhysicalExpr>,
        &input_schema,
    )?;

    let exprs: Vec<(Arc<dyn PhysicalExpr>, String)> = exprs
        .into_iter()
        .map(|(expr, field)| (expr, field.name().clone()))
        .collect();

    let projection = ProjectionExec::try_new(exprs, input).map_err(crate::Error::from)?;

    Ok(Arc::new(projection))
}

/// Build expressions to project input fields to match the table schema.
///
/// For each table field that exists in the input, produce an expression that
/// reads from the input and casts if needed. Fields in the table but not in the
/// input are omitted (the storage layer handles missing columns).
fn build_field_exprs(
    input_fields: &Fields,
    table_fields: &Fields,
    get_input_expr: &dyn Fn(usize) -> Arc<dyn PhysicalExpr>,
    input_schema: &Schema,
) -> Result<Vec<(Arc<dyn PhysicalExpr>, FieldRef)>> {
    let config = Arc::new(ConfigOptions::default());
    let mut result = Vec::new();

    for table_field in table_fields {
        let Some(input_idx) = input_fields
            .iter()
            .position(|f| f.name() == table_field.name())
        else {
            continue;
        };

        let input_field = &input_fields[input_idx];
        let input_expr = get_input_expr(input_idx);

        let expr = match (input_field.data_type(), table_field.data_type()) {
            // Both are structs: recurse into sub-fields to handle subschemas and casts.
            (DataType::Struct(in_children), DataType::Struct(tbl_children))
                if in_children != tbl_children =>
            {
                let sub_exprs = build_field_exprs(
                    in_children,
                    tbl_children,
                    &|child_idx| {
                        let child_name = in_children[child_idx].name();
                        Arc::new(ScalarFunctionExpr::new(
                            &format!("get_field({child_name})"),
                            get_field(),
                            vec![
                                input_expr.clone(),
                                Arc::new(Literal::new(ScalarValue::from(child_name.as_str()))),
                            ],
                            Arc::new(in_children[child_idx].as_ref().clone()),
                            config.clone(),
                        )) as Arc<dyn PhysicalExpr>
                    },
                    input_schema,
                )?;

                let output_struct_fields: Fields = sub_exprs
                    .iter()
                    .map(|(_, f)| f.clone())
                    .collect::<Vec<_>>()
                    .into();
                let output_field: FieldRef = Arc::new(Field::new(
                    table_field.name(),
                    DataType::Struct(output_struct_fields),
                    table_field.is_nullable(),
                ));

                // Build named_struct(lit("a"), expr_a, lit("b"), expr_b, ...)
                let mut ns_args: Vec<Arc<dyn PhysicalExpr>> = Vec::new();
                for (sub_expr, sub_field) in &sub_exprs {
                    ns_args.push(Arc::new(Literal::new(ScalarValue::from(
                        sub_field.name().as_str(),
                    ))));
                    ns_args.push(sub_expr.clone());
                }

                let ns_expr: Arc<dyn PhysicalExpr> = Arc::new(ScalarFunctionExpr::new(
                    &format!("named_struct({})", table_field.name()),
                    named_struct(),
                    ns_args,
                    output_field.clone(),
                    config.clone(),
                ));

                result.push((ns_expr, output_field));
                continue;
            }
            // Types match: pass through.
            (inp, tbl) if inp == tbl => input_expr,
            // Types differ: cast.
            _ => cast(input_expr, input_schema, table_field.data_type().clone()).map_err(|e| {
                Error::InvalidInput {
                    message: format!(
                        "cannot cast field '{}' from {} to {}: {}",
                        table_field.name(),
                        input_field.data_type(),
                        table_field.data_type(),
                        e
                    ),
                }
            })?,
        };

        let output_field = Arc::new(Field::new(
            table_field.name(),
            table_field.data_type().clone(),
            table_field.is_nullable(),
        ));
        result.push((expr, output_field));
    }

    Ok(result)
}
