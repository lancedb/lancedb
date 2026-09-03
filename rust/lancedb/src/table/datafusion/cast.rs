// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Coerces write-path input into the shape the table declares.
//!
//! [`cast_to_table_schema`] pairs each table column with the input column of the same name
//! and coerces it in two phases: [`write_schema::resolve_write_field`] decides which field
//! Lance should receive, then the [`RULES`] below build the expression that produces it.

use std::sync::Arc;

use arrow_cast::can_cast_types;
use arrow_schema::{DataType, Field, FieldRef, Fields, Schema};
use datafusion_common::config::ConfigOptions;
use datafusion_physical_plan::expressions::Column;
use datafusion_physical_plan::projection::ProjectionExec;
use datafusion_physical_plan::{ExecutionPlan, PhysicalExpr};

use super::blob_coerce::coerce_blob_expr;
use super::extension::ExtensionKind;
use super::struct_expr::{StructChild, build_struct, cast_to_field, get_field_expr};
use super::write_schema::resolve_write_field;
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

/// One column of the write path, as the rules see it.
struct WriteColumn<'a> {
    /// Reads the column out of the input.
    input_expr: Arc<dyn PhysicalExpr>,
    input_field: &'a FieldRef,
    /// The field Lance should receive; see [`resolve_write_field`].
    write_field: &'a FieldRef,
    config: &'a Arc<ConfigOptions>,
}

/// A coercion rule: `Ok(None)` means it does not apply and the next one should be tried.
type Rule = fn(&WriteColumn) -> Result<Option<(Arc<dyn PhysicalExpr>, FieldRef)>>;

/// The rules that synthesize a column, tried in order, with [`cast_column`] as the fallback
/// when none of them applies.
///
/// The order is behaviour, not taste:
///
/// * A blob column is built out of whatever the input happens to be - raw bytes, a URI, a
///   partial struct - which no later rule would attempt, so it goes first.
/// * A struct is rebuilt child by child, because that is the only way to line up input whose
///   children are reordered or partial. It has to precede [`cast_column`], which would ask
///   arrow for a struct cast and get an error for anything but an exact positional match.
///
/// Rules that were needed before phase one existed and are now subsumed by [`cast_column`]:
/// JSON no longer needs one because [`resolve_write_field`] asks for an `arrow.json` field
/// that a plain cast can satisfy, and null input needs none because arrow casts `Null` into
/// any type as an all-null array.
const RULES: &[Rule] = &[blob_column, rebuild_struct];

/// Build expressions to project input fields to match the table schema.
///
/// For each table field that exists in the input, produce an expression that reads from the
/// input and coerces it. Fields in the table but not in the input are omitted (the storage
/// layer handles missing columns).
fn build_field_exprs(
    input_fields: &Fields,
    table_fields: &Fields,
    get_input_expr: &dyn Fn(usize) -> Arc<dyn PhysicalExpr>,
) -> Result<Vec<(Arc<dyn PhysicalExpr>, FieldRef)>> {
    let config = Arc::new(ConfigOptions::default());
    let mut result = Vec::with_capacity(table_fields.len());

    for table_field in table_fields {
        let Some(input_idx) = input_fields
            .iter()
            .position(|f| f.name() == table_field.name())
        else {
            continue;
        };

        let input_field = &input_fields[input_idx];
        let write_field = resolve_write_field(input_field, table_field);
        let column = WriteColumn {
            input_expr: get_input_expr(input_idx),
            input_field,
            write_field: &write_field,
            config: &config,
        };

        let mut coerced = None;
        for rule in RULES {
            coerced = rule(&column)?;
            if coerced.is_some() {
                break;
            }
        }
        result.push(match coerced {
            Some(coerced) => coerced,
            None => cast_column(&column)?,
        });
    }

    Ok(result)
}

/// Blob columns accept raw bytes, a URI or a partial struct on write, and the struct Lance
/// stores is synthesized from whichever arrived.
///
/// All-null input is declined: there is no blob to describe, and the fallback cast turns
/// `Null` into null structs without any of this.
fn blob_column(column: &WriteColumn) -> Result<Option<(Arc<dyn PhysicalExpr>, FieldRef)>> {
    if ExtensionKind::of(column.write_field) != ExtensionKind::BlobV2
        || column.input_field.as_ref() == column.write_field.as_ref()
        || column.input_field.data_type() == &DataType::Null
    {
        return Ok(None);
    }
    coerce_blob_expr(
        column.input_expr.clone(),
        column.input_field,
        column.write_field,
        column.config,
    )
    .map(Some)
}

/// Rebuild a struct child by child, so that input whose children are reordered, partial or
/// themselves in need of coercion still lines up with what the table declares.
fn rebuild_struct(column: &WriteColumn) -> Result<Option<(Arc<dyn PhysicalExpr>, FieldRef)>> {
    let (DataType::Struct(input_children), DataType::Struct(write_children)) = (
        column.input_field.data_type(),
        column.write_field.data_type(),
    ) else {
        return Ok(None);
    };
    if input_children == write_children {
        return Ok(None);
    }

    let config = column.config.clone();
    let input_expr = column.input_expr.clone();
    let children = build_field_exprs(input_children, write_children, &|child_idx| {
        get_field_expr(input_expr.clone(), &input_children[child_idx], &config)
    })?;

    let output_field: FieldRef = Arc::new(Field::new(
        column.write_field.name(),
        DataType::Struct(children.iter().map(|(_, f)| f.clone()).collect()),
        column.write_field.is_nullable(),
    ));
    let children = children
        .into_iter()
        .map(|(value, field)| StructChild { field, value })
        .collect();

    let nulls_from = column
        .input_field
        .is_nullable()
        .then(|| column.input_expr.clone());
    let expr = build_struct(children, &output_field, nulls_from, column.config)?;
    Ok(Some((expr, output_field)))
}

/// Pass the column through if it already matches, otherwise cast it to the write field.
fn cast_column(column: &WriteColumn) -> Result<(Arc<dyn PhysicalExpr>, FieldRef)> {
    let write_field = column.write_field.clone();

    if column.input_field == &write_field {
        return Ok((column.input_expr.clone(), write_field));
    }

    // Types can match while the field does not, when the input is missing the metadata that
    // marks it as an extension column. Arrow casts a type to itself by cloning, so this pays
    // nothing beyond stamping the field on.
    if column.input_field.data_type() == write_field.data_type()
        || can_cast_types(column.input_field.data_type(), write_field.data_type())
    {
        let expr = cast_to_field(column.input_expr.clone(), &write_field);
        return Ok((expr, write_field));
    }

    Err(Error::InvalidInput {
        message: format!(
            "cannot cast field '{}' from {} to {}",
            write_field.name(),
            column.input_field.data_type(),
            write_field.data_type(),
        ),
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::buffer::OffsetBuffer;
    use arrow_array::{
        Array, Float32Array, Float64Array, Int32Array, Int64Array, ListArray, RecordBatch,
        StringArray, StructArray, UInt32Array, UInt64Array,
    };
    use arrow_schema::{DataType, Field, Fields, Schema};
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
    async fn test_list_struct_field_reorder() {
        // list<struct<a: Int32, b: Int32>> → list<struct<b: Int64, a: Int64>>
        // Tests both reordering (a,b → b,a) and element-type widening (Int32 → Int64).
        let inner_fields: Fields = vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Int32, true),
        ]
        .into();
        let struct_array = StructArray::from(vec![
            (
                Arc::new(inner_fields[0].as_ref().clone()),
                Arc::new(Int32Array::from(vec![1, 3])) as _,
            ),
            (
                Arc::new(inner_fields[1].as_ref().clone()),
                Arc::new(Int32Array::from(vec![2, 4])) as _,
            ),
        ]);
        // Offsets: one list element containing two struct rows (0..2).
        let offsets = OffsetBuffer::from_lengths(vec![2]);
        let list_array = ListArray::try_new(
            Arc::new(Field::new("item", DataType::Struct(inner_fields), true)),
            offsets,
            Arc::new(struct_array),
            None,
        )
        .unwrap();
        let input_batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "s_list",
                list_array.data_type().clone(),
                false,
            )])),
            vec![Arc::new(list_array)],
        )
        .unwrap();

        let table_inner: Fields = vec![
            Field::new("b", DataType::Int64, true),
            Field::new("a", DataType::Int64, true),
        ]
        .into();
        let table_schema = Schema::new(vec![Field::new(
            "s_list",
            DataType::List(Arc::new(Field::new(
                "item",
                DataType::Struct(table_inner),
                true,
            ))),
            false,
        )]);

        let plan = plan_from_batch(input_batch).await;
        let casted = cast_to_table_schema(plan, &table_schema).unwrap();
        let result = collect(casted).await;

        let list_col = result
            .column(0)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        let struct_col = list_col
            .values()
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        assert_eq!(struct_col.num_columns(), 2);

        let b: &Int64Array = struct_col
            .column_by_name("b")
            .unwrap()
            .as_any()
            .downcast_ref()
            .unwrap();
        assert_eq!(b.values(), &[2, 4]);
        let a: &Int64Array = struct_col
            .column_by_name("a")
            .unwrap()
            .as_any()
            .downcast_ref()
            .unwrap();
        assert_eq!(a.values(), &[1, 3]);
    }

    /// `arrow.json` input (PyArrow `pa.json_()`, Utf8/LargeUtf8 + extension metadata) against a
    /// `lance.json` table field (LargeBinary + extension metadata) must be passed through
    /// without a cast so that lance-core can perform its own arrow.json → JSONB conversion.
    ///
    /// Before the fix, `cast_to_table_schema` attempted a `Utf8 → LargeBinary` DataFusion
    /// cast that preserved the wrong extension metadata, causing lance-core to reject the
    /// batch with a "json vs large_binary" schema-mismatch error.
    #[rstest::rstest]
    #[case::utf8(DataType::Utf8)]
    #[case::large_utf8(DataType::LargeUtf8)]
    #[tokio::test]
    async fn test_arrow_json_passthrough_to_lance_json(#[case] input_type: DataType) {
        use lance_arrow::ARROW_EXT_NAME_KEY;
        use lance_arrow::json::{ARROW_JSON_EXT_NAME, json_field};

        // Build a table schema with a lance.json field (LargeBinary + lance.json metadata).
        let lance_field = json_field("data", true);
        let table_schema = Schema::new(vec![lance_field]);

        // Build an input batch with an arrow.json field (Utf8/LargeUtf8 + arrow.json metadata).
        let arrow_meta = std::collections::HashMap::from([(
            ARROW_EXT_NAME_KEY.to_string(),
            ARROW_JSON_EXT_NAME.to_string(),
        )]);
        let arrow_field = Field::new("data", input_type.clone(), true).with_metadata(arrow_meta);
        let input_schema = Arc::new(Schema::new(vec![arrow_field]));

        let values = vec![Some(r#"{"x": 1}"#), None, Some(r#"{"y": 2}"#)];
        let input_array: Arc<dyn arrow_array::Array> = match input_type {
            DataType::Utf8 => Arc::new(StringArray::from(values)),
            DataType::LargeUtf8 => Arc::new(arrow_array::LargeStringArray::from(values)),
            other => panic!("unsupported arrow.json backing type for this test: {other:?}"),
        };
        let input_batch = RecordBatch::try_new(input_schema, vec![input_array]).unwrap();

        let plan = plan_from_batch(input_batch).await;
        let projected = cast_to_table_schema(plan, &table_schema).unwrap();

        // The projected schema's "data" field must carry arrow.json metadata
        // (the input field), not be silently dropped or miscast.
        let out_field = projected.schema().field_with_name("data").unwrap().clone();
        assert_eq!(out_field.data_type(), &input_type);
        assert_eq!(
            out_field
                .metadata()
                .get(ARROW_EXT_NAME_KEY)
                .map(|s| s.as_str()),
            Some(ARROW_JSON_EXT_NAME),
            "output field must still carry arrow.json metadata so lance-core can handle it"
        );

        // The data must flow through correctly (3 rows, no panic).
        let result = collect(projected).await;
        assert_eq!(result.num_rows(), 3);
        let (v0, v2) = match input_type {
            DataType::Utf8 => {
                let col: &StringArray = result.column(0).as_any().downcast_ref().unwrap();
                (col.value(0).to_string(), col.value(2).to_string())
            }
            DataType::LargeUtf8 => {
                let col: &arrow_array::LargeStringArray =
                    result.column(0).as_any().downcast_ref().unwrap();
                (col.value(0).to_string(), col.value(2).to_string())
            }
            _ => unreachable!(),
        };
        assert_eq!(v0, r#"{"x": 1}"#);
        assert!(result.column(0).is_null(1));
        assert_eq!(v2, r#"{"y": 2}"#);
    }

    /// Plain JSON text (what pyarrow infers for a column of `str`, and what a caller writing
    /// JSON by hand supplies) has to be labelled arrow.json so lance-core encodes it as JSONB.
    /// Casting it to the table field's LargeBinary storage type would store the raw text.
    #[rstest::rstest]
    #[case::utf8(DataType::Utf8, DataType::Utf8)]
    #[case::large_utf8(DataType::LargeUtf8, DataType::LargeUtf8)]
    #[case::utf8_view(DataType::Utf8View, DataType::Utf8)]
    #[tokio::test]
    async fn test_unlabelled_string_into_lance_json_gets_arrow_json_label(
        #[case] input_type: DataType,
        #[case] expected_type: DataType,
    ) {
        use lance_arrow::json::{is_arrow_json_field, json_field};

        let table_schema = Schema::new(vec![json_field("data", true)]);

        let input_schema = Arc::new(Schema::new(vec![Field::new("data", input_type, true)]));
        let values = vec![Some(r#"{"x": 1}"#), None];
        let input_array = arrow_cast::cast(
            &StringArray::from(values) as &dyn arrow_array::Array,
            input_schema.field(0).data_type(),
        )
        .unwrap();
        let input_batch = RecordBatch::try_new(input_schema, vec![input_array]).unwrap();

        let plan = plan_from_batch(input_batch).await;
        let projected = cast_to_table_schema(plan, &table_schema).unwrap();

        let out_field = projected.schema().field_with_name("data").unwrap().clone();
        assert_eq!(out_field.data_type(), &expected_type);
        assert!(
            is_arrow_json_field(&out_field),
            "output field must be labelled arrow.json, got {:?}",
            out_field.metadata()
        );

        let result = collect(projected).await;
        assert_eq!(result.num_rows(), 2);
        assert_eq!(result.column(0).null_count(), 1);
    }

    /// A json leaf inside a list is relabelled too. The outer field is a list, so without
    /// recursing into it the generic container cast would turn the text into LargeBinary
    /// labelled lance.json - an append that succeeds but stores unreadable JSON.
    #[rstest::rstest]
    #[case::unlabelled(DataType::Utf8, false)]
    #[case::already_labelled(DataType::Utf8, true)]
    #[tokio::test]
    async fn test_unlabelled_list_item_into_lance_json_gets_arrow_json_label(
        #[case] item_type: DataType,
        #[case] input_labelled: bool,
    ) {
        use lance_arrow::json::{is_arrow_json_field, json_field};

        use crate::table::datafusion::extension::arrow_json_field;

        let table_schema = Schema::new(vec![Field::new(
            "docs",
            DataType::List(Arc::new(json_field("item", true))),
            true,
        )]);

        let input_item = if input_labelled {
            Arc::new(arrow_json_field("item", item_type, true))
        } else {
            Arc::new(Field::new("item", item_type, true))
        };
        let input_schema = Arc::new(Schema::new(vec![Field::new(
            "docs",
            DataType::List(input_item.clone()),
            true,
        )]));
        let values = StringArray::from(vec![Some(r#"{"k": 1}"#), Some(r#"{"k": 2}"#)]);
        let input_batch = RecordBatch::try_new(
            input_schema,
            vec![Arc::new(ListArray::new(
                input_item,
                OffsetBuffer::new(vec![0, 1, 2].into()),
                Arc::new(values),
                None,
            ))],
        )
        .unwrap();

        let plan = plan_from_batch(input_batch).await;
        let projected = cast_to_table_schema(plan, &table_schema).unwrap();

        let out_field = projected.schema().field_with_name("docs").unwrap().clone();
        let DataType::List(out_item) = out_field.data_type() else {
            panic!("expected a list, got {}", out_field.data_type());
        };
        assert!(
            is_arrow_json_field(out_item),
            "the list item must be labelled arrow.json, got {out_item:?}"
        );

        let result = collect(projected).await;
        assert_eq!(result.num_rows(), 2);
    }

    /// The same, for a json column nested inside a struct: the struct is rebuilt from its
    /// children, so the label has to travel on the child field.
    #[tokio::test]
    async fn test_unlabelled_struct_child_into_lance_json_gets_arrow_json_label() {
        use lance_arrow::json::{is_arrow_json_field, json_field};

        let table_schema = Schema::new(vec![Field::new(
            "info",
            DataType::Struct(
                vec![
                    Field::new("id", DataType::Int64, true),
                    json_field("value", true),
                ]
                .into(),
            ),
            true,
        )]);

        let input_children: Fields = vec![
            Field::new("id", DataType::Int64, true),
            Field::new("value", DataType::Utf8, true),
        ]
        .into();
        let input_schema = Arc::new(Schema::new(vec![Field::new(
            "info",
            DataType::Struct(input_children.clone()),
            true,
        )]));
        let input_batch = RecordBatch::try_new(
            input_schema,
            vec![Arc::new(StructArray::new(
                input_children,
                vec![
                    Arc::new(Int64Array::from(vec![1, 2])),
                    Arc::new(StringArray::from(vec![Some(r#"{"a": 1}"#), None])),
                ],
                None,
            ))],
        )
        .unwrap();

        let plan = plan_from_batch(input_batch).await;
        let projected = cast_to_table_schema(plan, &table_schema).unwrap();

        let out_field = projected.schema().field_with_name("info").unwrap().clone();
        let DataType::Struct(out_children) = out_field.data_type() else {
            panic!("expected a struct, got {}", out_field.data_type());
        };
        let value = out_children.iter().find(|f| f.name() == "value").unwrap();
        assert!(
            is_arrow_json_field(value),
            "nested field must be labelled arrow.json, got {:?}",
            value.metadata()
        );

        let result = collect(projected).await;
        let info: &StructArray = result.column(0).as_any().downcast_ref().unwrap();
        assert_eq!(info.column_by_name("value").unwrap().null_count(), 1);
    }

    /// An all-null column comes through as `DataType::Null` (pyarrow infers that for a batch
    /// of dicts whose values are all `None`). The lance.json extension metadata lives on the
    /// field alone, so it has to be carried into the output schema or lance-core rejects the
    /// batch with a "json vs large_binary" schema mismatch.
    #[tokio::test]
    async fn test_null_column_into_lance_json_keeps_extension_metadata() {
        use lance_arrow::ARROW_EXT_NAME_KEY;
        use lance_arrow::json::{JSON_EXT_NAME, json_field};

        let table_schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            json_field("data", true),
        ]);

        let input_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("data", DataType::Null, true),
        ]));
        let input_batch = RecordBatch::try_new(
            input_schema,
            vec![
                Arc::new(Int64Array::from(vec![0, 1, 2])),
                arrow_array::new_null_array(&DataType::Null, 3),
            ],
        )
        .unwrap();

        let plan = plan_from_batch(input_batch).await;
        let projected = cast_to_table_schema(plan, &table_schema).unwrap();

        let out_field = projected.schema().field_with_name("data").unwrap().clone();
        assert_eq!(out_field.data_type(), &DataType::LargeBinary);
        assert_eq!(
            out_field
                .metadata()
                .get(ARROW_EXT_NAME_KEY)
                .map(|s| s.as_str()),
            Some(JSON_EXT_NAME),
            "output field must still identify itself as lance.json"
        );

        let result = collect(projected).await;
        assert_eq!(result.num_rows(), 3);
        assert_eq!(result.column_by_name("data").unwrap().null_count(), 3);
    }

    /// The same, for a lance.json column nested inside a struct: the struct is rebuilt from
    /// its children, so each child field must keep its own metadata, and a null struct must
    /// stay null even though it is rebuilt child by child.
    #[tokio::test]
    async fn test_null_struct_child_into_lance_json_keeps_extension_metadata() {
        use lance_arrow::ARROW_EXT_NAME_KEY;
        use lance_arrow::json::{JSON_EXT_NAME, json_field};

        let table_schema = Schema::new(vec![Field::new(
            "meta",
            DataType::Struct(
                vec![
                    Field::new("id", DataType::Int64, true),
                    json_field("doc", true),
                ]
                .into(),
            ),
            true,
        )]);

        let input_children: Fields = vec![
            Field::new("id", DataType::Int64, true),
            Field::new("doc", DataType::Null, true),
        ]
        .into();
        let input_schema = Arc::new(Schema::new(vec![Field::new(
            "meta",
            DataType::Struct(input_children.clone()),
            true,
        )]));
        let input_batch = RecordBatch::try_new(
            input_schema,
            vec![Arc::new(StructArray::new(
                input_children,
                vec![
                    Arc::new(Int64Array::from(vec![7, 8])),
                    arrow_array::new_null_array(&DataType::Null, 2),
                ],
                Some(arrow::buffer::NullBuffer::from(vec![false, true])),
            ))],
        )
        .unwrap();

        let plan = plan_from_batch(input_batch).await;
        let projected = cast_to_table_schema(plan, &table_schema).unwrap();

        let out_field = projected.schema().field_with_name("meta").unwrap().clone();
        let DataType::Struct(out_children) = out_field.data_type() else {
            panic!("expected a struct, got {}", out_field.data_type());
        };
        let doc = out_children.iter().find(|f| f.name() == "doc").unwrap();
        assert_eq!(doc.data_type(), &DataType::LargeBinary);
        assert_eq!(
            doc.metadata().get(ARROW_EXT_NAME_KEY).map(|s| s.as_str()),
            Some(JSON_EXT_NAME)
        );

        let result = collect(projected).await;
        let meta: &StructArray = result.column(0).as_any().downcast_ref().unwrap();
        assert!(meta.is_null(0), "a null struct must stay null once rebuilt");
        assert!(meta.is_valid(1));
        assert_eq!(meta.column_by_name("doc").unwrap().null_count(), 2);
    }

    /// Any struct whose children need adjusting is rebuilt child by child, so the parent's
    /// nulls have to be restored afterwards - not only for the extension-column cases.
    #[tokio::test]
    async fn test_null_struct_stays_null_when_child_is_cast() {
        let input_children: Fields = vec![Field::new("x", DataType::Int32, true)].into();
        let table_schema = Schema::new(vec![Field::new(
            "s",
            DataType::Struct(vec![Field::new("x", DataType::Int64, true)].into()),
            true,
        )]);

        let input_schema = Arc::new(Schema::new(vec![Field::new(
            "s",
            DataType::Struct(input_children.clone()),
            true,
        )]));
        let input_batch = RecordBatch::try_new(
            input_schema,
            vec![Arc::new(StructArray::new(
                input_children,
                vec![Arc::new(Int32Array::from(vec![5, 6]))],
                Some(arrow::buffer::NullBuffer::from(vec![false, true])),
            ))],
        )
        .unwrap();

        let plan = plan_from_batch(input_batch).await;
        let projected = cast_to_table_schema(plan, &table_schema).unwrap();

        let result = collect(projected).await;
        let s: &StructArray = result.column(0).as_any().downcast_ref().unwrap();
        assert!(s.is_null(0));
        assert!(s.is_valid(1));
        let x: &Int64Array = s.column(0).as_any().downcast_ref().unwrap();
        assert_eq!(x.value(1), 6);
    }

    /// Lance rejects a non-nullable child that carries nulls even where the parent masks
    /// them, so the null rows have to keep the placeholder children the input gave them.
    #[tokio::test]
    async fn test_null_struct_keeps_children_of_a_non_nullable_child() {
        let input_children: Fields = vec![Field::new("x", DataType::Int32, false)].into();
        let table_schema = Schema::new(vec![Field::new(
            "s",
            DataType::Struct(vec![Field::new("x", DataType::Int64, false)].into()),
            true,
        )]);

        let input_schema = Arc::new(Schema::new(vec![Field::new(
            "s",
            DataType::Struct(input_children.clone()),
            true,
        )]));
        let input_batch = RecordBatch::try_new(
            input_schema,
            vec![Arc::new(StructArray::new(
                input_children,
                vec![Arc::new(Int32Array::from(vec![0, 6]))],
                Some(arrow::buffer::NullBuffer::from(vec![false, true])),
            ))],
        )
        .unwrap();

        let plan = plan_from_batch(input_batch).await;
        let projected = cast_to_table_schema(plan, &table_schema).unwrap();

        let result = collect(projected).await;
        let s: &StructArray = result.column(0).as_any().downcast_ref().unwrap();
        assert!(s.is_null(0));
        assert!(s.is_valid(1));
        let x: &Int64Array = s.column(0).as_any().downcast_ref().unwrap();
        assert_eq!(x.null_count(), 0);
        assert_eq!(x.value(1), 6);
    }

    /// A `Null` input column against a plain table column writes nulls too, including for
    /// target types that a DataFusion cast would not reach.
    #[tokio::test]
    async fn test_null_column_into_struct_column() {
        let children: Fields = vec![Field::new("x", DataType::Int32, true)].into();
        let table_schema = Schema::new(vec![Field::new(
            "s",
            DataType::Struct(children.clone()),
            true,
        )]);

        let input_schema = Arc::new(Schema::new(vec![Field::new("s", DataType::Null, true)]));
        let input_batch = RecordBatch::try_new(
            input_schema,
            vec![arrow_array::new_null_array(&DataType::Null, 2)],
        )
        .unwrap();

        let plan = plan_from_batch(input_batch).await;
        let projected = cast_to_table_schema(plan, &table_schema).unwrap();
        assert_eq!(
            projected.schema().field_with_name("s").unwrap().data_type(),
            &DataType::Struct(children)
        );

        let result = collect(projected).await;
        assert_eq!(result.num_rows(), 2);
        assert_eq!(result.column(0).null_count(), 2);
    }
}
