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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{
        Float32Array, Float64Array, Int32Array, Int64Array, RecordBatch, StringArray, StructArray,
    };
    use arrow_schema::{DataType, Field, Schema};
    use datafusion::prelude::SessionContext;
    use datafusion_catalog::MemTable;
    use futures::TryStreamExt;

    use super::cast_to_table_schema;

    async fn plan_from_batch(
        batch: RecordBatch,
    ) -> Arc<dyn datafusion_physical_plan::ExecutionPlan> {
        let schema = batch.schema();
        let table = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        let ctx = SessionContext::new();
        ctx.register_table("t", Arc::new(table)).unwrap();
        let df = ctx.table("t").await.unwrap();
        df.create_physical_plan().await.unwrap()
    }

    async fn collect(plan: Arc<dyn datafusion_physical_plan::ExecutionPlan>) -> RecordBatch {
        let ctx = SessionContext::new();
        let stream = plan.execute(0, ctx.task_ctx()).unwrap();
        let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();
        arrow_select::concat::concat_batches(&plan.schema(), &batches).unwrap()
    }

    #[tokio::test]
    async fn test_noop_when_schemas_match() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec!["x", "y"])),
            ],
        )
        .unwrap();

        let input = plan_from_batch(batch).await;
        let input_ptr = Arc::as_ptr(&input);
        let result = cast_to_table_schema(input, &schema).unwrap();
        assert_eq!(Arc::as_ptr(&result), input_ptr);
    }

    #[tokio::test]
    async fn test_simple_type_cast() {
        let input_batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int32, false),
                Field::new("val", DataType::Float32, false),
            ])),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(Float32Array::from(vec![1.5, 2.5, 3.5])),
            ],
        )
        .unwrap();

        let table_schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("val", DataType::Float64, false),
        ]);

        let plan = plan_from_batch(input_batch).await;
        let casted = cast_to_table_schema(plan, &table_schema).unwrap();
        let result = collect(casted).await;

        assert_eq!(result.schema().field(0).data_type(), &DataType::Int64);
        assert_eq!(result.schema().field(1).data_type(), &DataType::Float64);

        let ids: &Int64Array = result.column(0).as_any().downcast_ref().unwrap();
        assert_eq!(ids.values(), &[1, 2, 3]);

        let vals: &Float64Array = result.column(1).as_any().downcast_ref().unwrap();
        assert!((vals.value(0) - 1.5).abs() < 1e-6);
        assert!((vals.value(1) - 2.5).abs() < 1e-6);
        assert!((vals.value(2) - 3.5).abs() < 1e-6);
    }

    #[tokio::test]
    async fn test_missing_table_field_skipped() {
        // Input has "a", table expects "a" and "b". "b" is omitted from the
        // projection since the storage layer fills in missing columns.
        let input_batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)])),
            vec![Arc::new(Int32Array::from(vec![10, 20]))],
        )
        .unwrap();

        let table_schema = Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Utf8, true),
        ]);

        let plan = plan_from_batch(input_batch).await;
        let casted = cast_to_table_schema(plan, &table_schema).unwrap();
        let result = collect(casted).await;

        assert_eq!(result.num_columns(), 1);
        assert_eq!(result.schema().field(0).name(), "a");
    }

    #[tokio::test]
    async fn test_extra_input_fields_dropped() {
        // Input has "a" and "extra"; table only expects "a".
        let input_batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("a", DataType::Int32, false),
                Field::new("extra", DataType::Utf8, false),
            ])),
            vec![
                Arc::new(Int32Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec!["x", "y"])),
            ],
        )
        .unwrap();

        let table_schema = Schema::new(vec![Field::new("a", DataType::Int64, false)]);

        let plan = plan_from_batch(input_batch).await;
        let casted = cast_to_table_schema(plan, &table_schema).unwrap();
        let result = collect(casted).await;

        assert_eq!(result.num_columns(), 1);
        assert_eq!(result.schema().field(0).name(), "a");
        assert_eq!(result.schema().field(0).data_type(), &DataType::Int64);
    }

    #[tokio::test]
    async fn test_reorders_to_table_schema() {
        let input_batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("b", DataType::Utf8, false),
                Field::new("a", DataType::Int32, false),
            ])),
            vec![
                Arc::new(StringArray::from(vec!["x", "y"])),
                Arc::new(Int32Array::from(vec![1, 2])),
            ],
        )
        .unwrap();

        let table_schema = Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Utf8, false),
        ]);

        let plan = plan_from_batch(input_batch).await;
        let casted = cast_to_table_schema(plan, &table_schema).unwrap();
        let result = collect(casted).await;

        assert_eq!(result.schema().field(0).name(), "a");
        assert_eq!(result.schema().field(1).name(), "b");

        let a: &Int32Array = result.column(0).as_any().downcast_ref().unwrap();
        assert_eq!(a.values(), &[1, 2]);
        let b: &StringArray = result.column(1).as_any().downcast_ref().unwrap();
        assert_eq!(b.value(0), "x");
    }

    #[tokio::test]
    async fn test_struct_subfield_cast() {
        // Input struct has {x: Int32, y: Int32}, table expects {x: Int64, y: Int64}.
        let inner_fields = vec![
            Field::new("x", DataType::Int32, false),
            Field::new("y", DataType::Int32, false),
        ];
        let struct_array = StructArray::from(vec![
            (
                Arc::new(inner_fields[0].clone()),
                Arc::new(Int32Array::from(vec![1, 2])) as _,
            ),
            (
                Arc::new(inner_fields[1].clone()),
                Arc::new(Int32Array::from(vec![3, 4])) as _,
            ),
        ]);
        let input_batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "s",
                DataType::Struct(inner_fields.into()),
                false,
            )])),
            vec![Arc::new(struct_array)],
        )
        .unwrap();

        let table_inner = vec![
            Field::new("x", DataType::Int64, false),
            Field::new("y", DataType::Int64, false),
        ];
        let table_schema = Schema::new(vec![Field::new(
            "s",
            DataType::Struct(table_inner.into()),
            false,
        )]);

        let plan = plan_from_batch(input_batch).await;
        let casted = cast_to_table_schema(plan, &table_schema).unwrap();
        let result = collect(casted).await;

        let struct_col = result
            .column(0)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        assert_eq!(struct_col.column(0).data_type(), &DataType::Int64);
        assert_eq!(struct_col.column(1).data_type(), &DataType::Int64);

        let x: &Int64Array = struct_col.column(0).as_any().downcast_ref().unwrap();
        assert_eq!(x.values(), &[1, 2]);
        let y: &Int64Array = struct_col.column(1).as_any().downcast_ref().unwrap();
        assert_eq!(y.values(), &[3, 4]);
    }

    #[tokio::test]
    async fn test_struct_subschema() {
        // Input struct has {x, y, z}, table only expects {x, z}.
        let inner_fields = vec![
            Field::new("x", DataType::Int32, false),
            Field::new("y", DataType::Int32, false),
            Field::new("z", DataType::Int32, false),
        ];
        let struct_array = StructArray::from(vec![
            (
                Arc::new(inner_fields[0].clone()),
                Arc::new(Int32Array::from(vec![1, 2])) as _,
            ),
            (
                Arc::new(inner_fields[1].clone()),
                Arc::new(Int32Array::from(vec![10, 20])) as _,
            ),
            (
                Arc::new(inner_fields[2].clone()),
                Arc::new(Int32Array::from(vec![100, 200])) as _,
            ),
        ]);
        let input_batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "s",
                DataType::Struct(inner_fields.into()),
                false,
            )])),
            vec![Arc::new(struct_array)],
        )
        .unwrap();

        let table_inner = vec![
            Field::new("x", DataType::Int32, false),
            Field::new("z", DataType::Int32, false),
        ];
        let table_schema = Schema::new(vec![Field::new(
            "s",
            DataType::Struct(table_inner.into()),
            false,
        )]);

        let plan = plan_from_batch(input_batch).await;
        let casted = cast_to_table_schema(plan, &table_schema).unwrap();
        let result = collect(casted).await;

        let struct_col = result
            .column(0)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        assert_eq!(struct_col.num_columns(), 2);

        let x: &Int32Array = struct_col
            .column_by_name("x")
            .unwrap()
            .as_any()
            .downcast_ref()
            .unwrap();
        assert_eq!(x.values(), &[1, 2]);
        let z: &Int32Array = struct_col
            .column_by_name("z")
            .unwrap()
            .as_any()
            .downcast_ref()
            .unwrap();
        assert_eq!(z.values(), &[100, 200]);
    }

    #[tokio::test]
    async fn test_incompatible_cast_errors() {
        let input_batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("a", DataType::Binary, false)])),
            vec![Arc::new(arrow_array::BinaryArray::from_vec(vec![b"hi"]))],
        )
        .unwrap();

        let table_schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);

        let plan = plan_from_batch(input_batch).await;
        let result = cast_to_table_schema(plan, &table_schema);
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("cannot cast field 'a'"),
            "unexpected error: {err_msg}"
        );
    }

    #[tokio::test]
    async fn test_mixed_cast_and_passthrough() {
        // "a" needs cast (Int32→Int64), "b" passes through unchanged.
        let input_batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("a", DataType::Int32, false),
                Field::new("b", DataType::Utf8, false),
            ])),
            vec![
                Arc::new(Int32Array::from(vec![7, 8])),
                Arc::new(StringArray::from(vec!["hello", "world"])),
            ],
        )
        .unwrap();

        let table_schema = Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Utf8, false),
        ]);

        let plan = plan_from_batch(input_batch).await;
        let casted = cast_to_table_schema(plan, &table_schema).unwrap();
        let result = collect(casted).await;

        assert_eq!(result.schema().field(0).data_type(), &DataType::Int64);
        assert_eq!(result.schema().field(1).data_type(), &DataType::Utf8);

        let a: &Int64Array = result.column(0).as_any().downcast_ref().unwrap();
        assert_eq!(a.values(), &[7, 8]);
        let b: &StringArray = result.column(1).as_any().downcast_ref().unwrap();
        assert_eq!(b.value(0), "hello");
        assert_eq!(b.value(1), "world");
    }
}
