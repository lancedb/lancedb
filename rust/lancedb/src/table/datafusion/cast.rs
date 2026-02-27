// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use std::sync::Arc;

use arrow_schema::{DataType, Field, FieldRef, Fields, Schema};
use datafusion::functions::core::{get_field, named_struct};
use datafusion_common::config::ConfigOptions;
use datafusion_common::ScalarValue;
use datafusion_physical_expr::expressions::{CastExpr, Literal};
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

    let exprs = build_field_exprs(input_schema.fields(), table_schema.fields(), &|idx| {
        Arc::new(Column::new(input_schema.field(idx).name(), idx)) as Arc<dyn PhysicalExpr>
    })?;

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
            // Same type family (e.g. numeric↔numeric): attempt the cast.
            // safe: false (the default) means overflow/truncation errors surface at execution time.
            (_, _) if is_safe_cast(input_field.data_type(), table_field.data_type()) => {
                Arc::new(CastExpr::new(
                    input_expr,
                    table_field.data_type().clone(),
                    None,
                )) as Arc<dyn PhysicalExpr>
            }
            // Cross-family casts (e.g. numeric→string) are rejected at plan time.
            (inp, tbl) => {
                return Err(Error::InvalidInput {
                    message: format!(
                        "cannot cast field '{}' from {} to {}: automatic casting between these types is not supported",
                        table_field.name(),
                        inp,
                        tbl,
                    ),
                })
            }
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

/// Returns true if `from_type` and `to_type` are in the same type family and can be
/// implicitly cast. Cross-family casts (e.g. numeric → string) are rejected.
fn is_safe_cast(from_type: &DataType, to_type: &DataType) -> bool {
    fn is_integer(dt: &DataType) -> bool {
        matches!(
            dt,
            DataType::Int8
                | DataType::Int16
                | DataType::Int32
                | DataType::Int64
                | DataType::UInt8
                | DataType::UInt16
                | DataType::UInt32
                | DataType::UInt64
        )
    }
    fn is_float(dt: &DataType) -> bool {
        matches!(
            dt,
            DataType::Float16 | DataType::Float32 | DataType::Float64
        )
    }
    fn is_list_like(dt: &DataType) -> bool {
        matches!(
            dt,
            DataType::List(_) | DataType::LargeList(_) | DataType::FixedSizeList(_, _)
        )
    }
    // int↔int: may overflow at runtime, but semantically intended.
    // float↔float: same.
    // int→float: always produces a value (may lose precision for large i64→f32).
    // float→int: REJECTED. Arrow's safe:false does not error on truncation (1.5→1 silently),
    //   so this would silently corrupt data for non-integer float values.
    matches!(from_type, DataType::Null) ||
    (is_integer(from_type) && is_integer(to_type))
        || (is_float(from_type) && is_float(to_type))
        || (is_integer(from_type) && is_float(to_type))
        // Decimal: same precision and scale, different storage width (128 vs 256).
        || match (from_type, to_type) {
            (DataType::Decimal128(p1, s1), DataType::Decimal256(p2, s2))
            | (DataType::Decimal256(p1, s1), DataType::Decimal128(p2, s2)) => {
                p1 == p2 && s1 == s2
            }
            _ => false,
        }
        // String variants are interchangeable encodings.
        || matches!(
            (from_type, to_type),
            (
                DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View,
                DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View,
            )
        )
        // Binary variants are interchangeable encodings.
        || matches!(
            (from_type, to_type),
            (
                DataType::Binary | DataType::LargeBinary | DataType::BinaryView,
                DataType::Binary | DataType::LargeBinary | DataType::BinaryView,
            )
        )
        // List-like variants for embedding vectors.
        || (is_list_like(from_type) && is_list_like(to_type))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{
        BinaryArray, Decimal128Array, Float32Array, Float64Array, Int32Array, Int64Array,
        LargeBinaryArray, RecordBatch, StringArray, StructArray, UInt32Array, UInt64Array,
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

    #[tokio::test]
    async fn test_numeric_to_string_rejected() {
        let input_batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("a", DataType::UInt32, false)])),
            vec![Arc::new(UInt32Array::from(vec![1u32]))],
        )
        .unwrap();

        let table_schema = Schema::new(vec![Field::new("a", DataType::Utf8, false)]);

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
    async fn test_narrowing_numeric_cast_success() {
        let input_batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("a", DataType::UInt64, false)])),
            vec![Arc::new(UInt64Array::from(vec![1u64, 2, 3]))],
        )
        .unwrap();

        let table_schema = Schema::new(vec![Field::new("a", DataType::UInt32, false)]);

        let plan = plan_from_batch(input_batch).await;
        let casted = cast_to_table_schema(plan, &table_schema).unwrap();
        let result = collect(casted).await;

        assert_eq!(result.schema().field(0).data_type(), &DataType::UInt32);
        let a: &UInt32Array = result.column(0).as_any().downcast_ref().unwrap();
        assert_eq!(a.values(), &[1u32, 2, 3]);
    }

    #[tokio::test]
    async fn test_narrowing_numeric_cast_overflow_errors() {
        let overflow_val = u32::MAX as u64 + 1;
        let input_batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("a", DataType::UInt64, false)])),
            vec![Arc::new(UInt64Array::from(vec![overflow_val]))],
        )
        .unwrap();

        let table_schema = Schema::new(vec![Field::new("a", DataType::UInt32, false)]);

        let plan = plan_from_batch(input_batch).await;
        // Planning succeeds — the overflow is only detected at execution time.
        let casted = cast_to_table_schema(plan, &table_schema).unwrap();

        let ctx = SessionContext::new();
        let stream = casted.execute(0, ctx.task_ctx()).unwrap();
        let result: Result<Vec<RecordBatch>, _> = stream.try_collect().await;
        assert!(result.is_err(), "expected overflow error at execution time");
    }

    #[tokio::test]
    async fn test_float_to_int_rejected() {
        // Arrow's safe:false does not error on truncation (1.5 → 1 silently), so
        // float→int must be rejected at plan time rather than relying on runtime errors.
        let input_batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("a", DataType::Float64, false)])),
            vec![Arc::new(Float64Array::from(vec![1.5f64]))],
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
    async fn test_binary_variants_cast() {
        let input_batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("a", DataType::Binary, false)])),
            vec![Arc::new(BinaryArray::from_vec(vec![b"hello", b"world"]))],
        )
        .unwrap();

        let table_schema = Schema::new(vec![Field::new("a", DataType::LargeBinary, false)]);

        let plan = plan_from_batch(input_batch).await;
        let casted = cast_to_table_schema(plan, &table_schema).unwrap();
        let result = collect(casted).await;

        assert_eq!(result.schema().field(0).data_type(), &DataType::LargeBinary);
        let a: &LargeBinaryArray = result.column(0).as_any().downcast_ref().unwrap();
        assert_eq!(a.value(0), b"hello");
        assert_eq!(a.value(1), b"world");
    }

    #[tokio::test]
    async fn test_decimal_same_ps_different_width() {
        // Decimal128 → Decimal256 with identical precision and scale is a pure
        // storage-width change and must succeed.
        let input_batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "a",
                DataType::Decimal128(10, 2),
                false,
            )])),
            vec![Arc::new(
                Decimal128Array::from(vec![12345i128])
                    .with_precision_and_scale(10, 2)
                    .unwrap(),
            )],
        )
        .unwrap();

        let table_schema = Schema::new(vec![Field::new("a", DataType::Decimal256(10, 2), false)]);

        let plan = plan_from_batch(input_batch).await;
        let casted = cast_to_table_schema(plan, &table_schema).unwrap();
        let result = collect(casted).await;

        assert_eq!(
            result.schema().field(0).data_type(),
            &DataType::Decimal256(10, 2)
        );
    }

    #[tokio::test]
    async fn test_decimal_different_ps_rejected() {
        let input_batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "a",
                DataType::Decimal128(10, 2),
                false,
            )])),
            vec![Arc::new(
                Decimal128Array::from(vec![12345i128])
                    .with_precision_and_scale(10, 2)
                    .unwrap(),
            )],
        )
        .unwrap();

        // Different scale — rejected even though it's Decimal128→Decimal256.
        let table_schema = Schema::new(vec![Field::new("a", DataType::Decimal256(10, 4), false)]);

        let plan = plan_from_batch(input_batch).await;
        let result = cast_to_table_schema(plan, &table_schema);
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("cannot cast field 'a'"),
            "unexpected error: {err_msg}"
        );
    }
}
