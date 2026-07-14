// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Coerces write-path input into blob v2 struct columns.
//!
//! [`super::cast::cast_to_table_schema`] calls [`coerce_blob_expr`].

use std::sync::Arc;

use arrow_schema::{DataType, Field, FieldRef};
use datafusion::functions::core::{get_field, named_struct};
use datafusion_common::ScalarValue;
use datafusion_common::config::ConfigOptions;
use datafusion_physical_expr::ScalarFunctionExpr;
use datafusion_physical_expr::expressions::{CastExpr, Literal};
use datafusion_physical_plan::PhysicalExpr;

use crate::error::{Error, Result};

/// Build a projection expression coercing `input_expr` into the blob struct
/// declared by `table_field`, composing `named_struct` / `get_field` / `cast`.
pub(super) fn coerce_blob_expr(
    input_expr: Arc<dyn PhysicalExpr>,
    input_field: &Field,
    table_field: &FieldRef,
    config: &Arc<ConfigOptions>,
) -> Result<(Arc<dyn PhysicalExpr>, FieldRef)> {
    let DataType::Struct(declared_fields) = table_field.data_type() else {
        return Err(Error::InvalidInput {
            message: format!(
                "blob v2 column '{}' must be a struct, table declares {}",
                table_field.name(),
                table_field.data_type()
            ),
        });
    };

    let input_struct_children = match input_field.data_type() {
        DataType::Binary | DataType::LargeBinary | DataType::BinaryView => None,
        DataType::Struct(children) => {
            if !children
                .iter()
                .any(|c| c.name() == "data" || c.name() == "uri")
            {
                return Err(Error::InvalidInput {
                    message: format!(
                        "blob struct input for column '{}' must contain a 'data' or 'uri' child",
                        table_field.name()
                    ),
                });
            }
            Some(children)
        }
        other => {
            return Err(Error::InvalidInput {
                message: format!(
                    "cannot coerce column '{}' with type {} into a blob v2 struct. \
                     expected Binary, LargeBinary, BinaryView, or a Struct with a 'data' or 'uri' child",
                    table_field.name(),
                    other,
                ),
            });
        }
    };

    let mut ns_args: Vec<Arc<dyn PhysicalExpr>> = Vec::with_capacity(declared_fields.len() * 2);
    for declared in declared_fields.iter() {
        ns_args.push(Arc::new(Literal::new(ScalarValue::from(
            declared.name().as_str(),
        ))));

        let value: Arc<dyn PhysicalExpr> = match input_struct_children {
            // Raw binary lands in `data` and everything else is a typed null.
            None => {
                if declared.name() == "data" {
                    Arc::new(CastExpr::new(
                        input_expr.clone(),
                        declared.data_type().clone(),
                        None,
                    ))
                } else {
                    typed_null(declared.data_type())?
                }
            }
            Some(children) => match children.iter().find(|c| c.name() == declared.name()) {
                Some(child) => {
                    let field_expr: Arc<dyn PhysicalExpr> = Arc::new(ScalarFunctionExpr::new(
                        &format!("get_field({})", declared.name()),
                        get_field(),
                        vec![
                            input_expr.clone(),
                            Arc::new(Literal::new(ScalarValue::from(declared.name().as_str()))),
                        ],
                        Arc::new(child.as_ref().clone()),
                        config.clone(),
                    ));
                    if child.data_type() == declared.data_type() {
                        field_expr
                    } else {
                        Arc::new(CastExpr::new(
                            field_expr,
                            declared.data_type().clone(),
                            None,
                        ))
                    }
                }
                None => typed_null(declared.data_type())?,
            },
        };
        ns_args.push(value);
    }

    let expr: Arc<dyn PhysicalExpr> = Arc::new(ScalarFunctionExpr::new(
        &format!("named_struct({})", table_field.name()),
        named_struct(),
        ns_args,
        table_field.clone(),
        config.clone(),
    ));
    Ok((expr, table_field.clone()))
}

fn typed_null(data_type: &DataType) -> Result<Arc<dyn PhysicalExpr>> {
    let scalar = ScalarValue::try_from(data_type).map_err(|e| Error::InvalidInput {
        message: format!("cannot build null literal for blob child type {data_type}: {e}"),
    })?;
    Ok(Arc::new(Literal::new(scalar)))
}

#[cfg(test)]
mod tests {
    use super::super::cast::cast_to_table_schema;
    use super::*;
    use crate::blob::blob;
    use arrow_array::{
        Array, ArrayRef, BinaryArray, BinaryViewArray, Int32Array, Int64Array, LargeBinaryArray,
        RecordBatch, StringArray, StructArray, UInt8Array, UInt64Array,
    };
    use arrow_schema::Schema;
    use datafusion::prelude::SessionContext;
    use datafusion_catalog::MemTable;
    use datafusion_physical_plan::ExecutionPlan;
    use futures::TryStreamExt;
    use lance_arrow::FieldExt;
    use std::collections::HashMap;

    fn wide_blob_field(name: &str) -> Field {
        Field::new(
            name,
            DataType::Struct(
                vec![
                    Field::new("data", DataType::LargeBinary, true),
                    Field::new("uri", DataType::Utf8, true),
                    Field::new("position", DataType::UInt64, true),
                    Field::new("size", DataType::UInt64, true),
                ]
                .into(),
            ),
            true,
        )
        .with_metadata(HashMap::from([(
            "ARROW:extension:name".to_string(),
            "lance.blob.v2".to_string(),
        )]))
    }

    fn blob_table_schema() -> Schema {
        Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            blob("image", true),
        ])
    }

    fn batch_with_image(image_field: Field, image: ArrayRef) -> RecordBatch {
        let len = image.len();
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                image_field,
            ])),
            vec![Arc::new(Int64Array::from_iter_values(0..len as i64)), image],
        )
        .unwrap()
    }

    fn image_struct(batch: &RecordBatch) -> &StructArray {
        batch
            .column_by_name("image")
            .unwrap()
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap()
    }

    async fn plan_from_batch(batch: RecordBatch) -> Arc<dyn ExecutionPlan> {
        let schema = batch.schema();
        let table = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        let ctx = SessionContext::new();
        ctx.register_table("t", Arc::new(table)).unwrap();
        let df = ctx.table("t").await.unwrap();
        df.create_physical_plan().await.unwrap()
    }

    async fn coerce(batch: RecordBatch, table_schema: &Schema) -> RecordBatch {
        let plan = plan_from_batch(batch).await;
        let plan = cast_to_table_schema(plan, table_schema).unwrap();
        let ctx = SessionContext::new();
        let stream = plan.execute(0, ctx.task_ctx()).unwrap();
        let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();
        arrow_select::concat::concat_batches(&plan.schema(), &batches).unwrap()
    }

    async fn coerce_err(batch: RecordBatch, table_schema: &Schema) -> Error {
        let plan = plan_from_batch(batch).await;
        cast_to_table_schema(plan, table_schema).unwrap_err()
    }

    #[tokio::test]
    async fn large_binary_coerces_to_declared_blob_struct() {
        let batch = batch_with_image(
            Field::new("image", DataType::LargeBinary, true),
            Arc::new(LargeBinaryArray::from_iter_values([b"hello".as_slice()])),
        );
        let coerced = coerce(batch, &blob_table_schema()).await;
        let image_field = coerced.schema().field_with_name("image").unwrap().clone();
        assert!(image_field.is_blob_v2());
        assert!(matches!(image_field.data_type(), DataType::Struct(_)));
        let data = image_struct(&coerced).column_by_name("data").unwrap();
        let data: &LargeBinaryArray = data.as_any().downcast_ref().unwrap();
        assert_eq!(data.value(0), b"hello");
    }

    #[tokio::test]
    async fn binary_coerces_to_declared_blob_struct() {
        let batch = batch_with_image(
            Field::new("image", DataType::Binary, true),
            Arc::new(BinaryArray::from_iter_values([b"hi".as_slice()])),
        );
        let coerced = coerce(batch, &blob_table_schema()).await;
        assert!(
            coerced
                .schema()
                .field_with_name("image")
                .unwrap()
                .is_blob_v2()
        );
    }

    #[tokio::test]
    async fn binary_view_coerces_to_declared_blob_struct() {
        let batch = batch_with_image(
            Field::new("image", DataType::BinaryView, true),
            Arc::new(BinaryViewArray::from_iter_values([b"view".as_slice()])),
        );
        let coerced = coerce(batch, &blob_table_schema()).await;
        let data = image_struct(&coerced).column_by_name("data").unwrap();
        let data: &LargeBinaryArray = data.as_any().downcast_ref().unwrap();
        assert_eq!(data.value(0), b"view");
    }

    #[tokio::test]
    async fn binary_nulls_stay_null_after_coercion() {
        let batch = batch_with_image(
            Field::new("image", DataType::Binary, true),
            Arc::new(BinaryArray::from_iter(vec![
                Some(b"present".as_slice()),
                None,
            ])),
        );
        let coerced = coerce(batch, &blob_table_schema()).await;
        let image = image_struct(&coerced);
        let data = image.column_by_name("data").unwrap();
        assert!(!data.is_null(0));
        assert!(data.is_null(1));
    }

    #[tokio::test]
    async fn binary_coerces_into_four_child_blob_layout() {
        let table_schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            wide_blob_field("image"),
        ]);
        let batch = batch_with_image(
            Field::new("image", DataType::LargeBinary, true),
            Arc::new(LargeBinaryArray::from_iter(vec![
                Some(b"alpha".as_slice()),
                None,
            ])),
        );
        let coerced = coerce(batch, &table_schema).await;
        let image = image_struct(&coerced);
        assert_eq!(
            image.num_columns(),
            4,
            "coerced struct keeps the declared layout"
        );
        assert!(image.column_by_name("position").unwrap().is_null(0));
        assert!(image.column_by_name("size").unwrap().is_null(0));
        assert!(!image.column_by_name("data").unwrap().is_null(0));
        assert!(image.column_by_name("data").unwrap().is_null(1));
    }

    #[tokio::test]
    async fn prebuilt_struct_gains_blob_field_metadata() {
        let DataType::Struct(children) = blob("image", true).data_type().clone() else {
            unreachable!("blob field is a struct")
        };
        let prebuilt = StructArray::new(
            children,
            vec![
                Arc::new(LargeBinaryArray::from_iter_values([b"prebuilt".as_slice()])),
                Arc::new(StringArray::from(vec![None::<&str>])),
            ],
            None,
        );
        let batch = batch_with_image(
            Field::new("image", prebuilt.data_type().clone(), true),
            Arc::new(prebuilt),
        );
        let coerced = coerce(batch, &blob_table_schema()).await;
        assert!(
            coerced
                .schema()
                .field_with_name("image")
                .unwrap()
                .is_blob_v2()
        );
    }

    #[tokio::test]
    async fn prebuilt_narrow_struct_widens_to_declared_layout() {
        let DataType::Struct(narrow_children) = blob("image", true).data_type().clone() else {
            unreachable!("blob field is a struct")
        };
        let prebuilt = StructArray::new(
            narrow_children,
            vec![
                Arc::new(LargeBinaryArray::from_iter_values([b"prebuilt".as_slice()])),
                Arc::new(StringArray::from(vec![None::<&str>])),
            ],
            None,
        );
        let table_schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            wide_blob_field("image"),
        ]);
        let batch = batch_with_image(
            Field::new("image", prebuilt.data_type().clone(), true),
            Arc::new(prebuilt),
        );
        let coerced = coerce(batch, &table_schema).await;
        let image = image_struct(&coerced);
        assert_eq!(image.num_columns(), 4);
        assert!(image.column_by_name("position").unwrap().is_null(0));
        assert!(image.column_by_name("size").unwrap().is_null(0));
    }

    #[tokio::test]
    async fn external_reference_struct_preserves_uri_position_and_size() {
        let prebuilt = StructArray::new(
            vec![
                Field::new("data", DataType::LargeBinary, true),
                Field::new("uri", DataType::Utf8, true),
                Field::new("position", DataType::UInt64, true),
                Field::new("size", DataType::UInt64, true),
            ]
            .into(),
            vec![
                Arc::new(LargeBinaryArray::from(vec![None::<&[u8]>])) as ArrayRef,
                Arc::new(StringArray::from(vec![Some("s3://bucket/blob.bin")])) as ArrayRef,
                Arc::new(UInt64Array::from(vec![Some(7)])) as ArrayRef,
                Arc::new(UInt64Array::from(vec![Some(6)])) as ArrayRef,
            ],
            None,
        );
        let table_schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            wide_blob_field("image"),
        ]);
        let batch = batch_with_image(
            Field::new("image", prebuilt.data_type().clone(), true),
            Arc::new(prebuilt),
        );
        let coerced = coerce(batch, &table_schema).await;
        let image = image_struct(&coerced);

        let uri: &StringArray = image
            .column_by_name("uri")
            .unwrap()
            .as_any()
            .downcast_ref()
            .unwrap();
        assert_eq!(uri.value(0), "s3://bucket/blob.bin");
        let position: &UInt64Array = image
            .column_by_name("position")
            .unwrap()
            .as_any()
            .downcast_ref()
            .unwrap();
        assert_eq!(position.value(0), 7);
        let size: &UInt64Array = image
            .column_by_name("size")
            .unwrap()
            .as_any()
            .downcast_ref()
            .unwrap();
        assert_eq!(size.value(0), 6);
        assert!(image.column_by_name("data").unwrap().is_null(0));
    }

    #[tokio::test]
    async fn descriptor_struct_without_value_child_is_rejected() {
        let descriptor = StructArray::new(
            vec![
                Field::new("kind", DataType::UInt8, false),
                Field::new("position", DataType::UInt64, false),
                Field::new("size", DataType::UInt64, false),
            ]
            .into(),
            vec![
                Arc::new(UInt8Array::from(vec![0])),
                Arc::new(UInt64Array::from(vec![0])),
                Arc::new(UInt64Array::from(vec![0])),
            ],
            None,
        );
        let batch = batch_with_image(
            Field::new("image", descriptor.data_type().clone(), true),
            Arc::new(descriptor),
        );
        let err = coerce_err(batch, &blob_table_schema()).await;
        assert!(err.to_string().contains("'data' or 'uri'"));
        assert!(err.to_string().contains("image"));
    }

    #[tokio::test]
    async fn unsupported_input_type_is_rejected_with_column_name() {
        let batch = batch_with_image(
            Field::new("image", DataType::Utf8, true),
            Arc::new(StringArray::from(vec!["not bytes"])),
        );
        let err = coerce_err(batch, &blob_table_schema()).await;
        assert!(matches!(err, Error::InvalidInput { .. }), "got {err:?}");
        assert!(err.to_string().contains("image"));
    }

    #[tokio::test]
    async fn blob_metadata_survives_cast_of_sibling_column() {
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int32, false),
                Field::new("image", DataType::LargeBinary, true),
            ])),
            vec![
                Arc::new(Int32Array::from(vec![1])),
                Arc::new(LargeBinaryArray::from_iter_values([b"x".as_slice()])),
            ],
        )
        .unwrap();
        let coerced = coerce(batch, &blob_table_schema()).await;

        let image_field = coerced.schema().field_with_name("image").unwrap().clone();
        assert!(
            image_field.is_blob_v2(),
            "expected blob marker on image field, got {:?}",
            image_field.metadata()
        );
        assert_eq!(
            coerced.schema().field_with_name("id").unwrap().data_type(),
            &DataType::Int64
        );
    }

    #[tokio::test]
    async fn exact_blob_input_passes_through_unchanged() {
        let DataType::Struct(children) = blob("image", true).data_type().clone() else {
            unreachable!("blob field is a struct")
        };
        let image = StructArray::new(
            children,
            vec![
                Arc::new(LargeBinaryArray::from_iter_values([b"exact".as_slice()])),
                Arc::new(StringArray::from(vec![None::<&str>])),
            ],
            None,
        );
        let batch = batch_with_image(blob("image", true), Arc::new(image));
        let table_schema = blob_table_schema();

        let input = plan_from_batch(batch).await;
        let input_ptr = Arc::as_ptr(&input);
        let plan = cast_to_table_schema(input, &table_schema).unwrap();
        assert_eq!(Arc::as_ptr(&plan), input_ptr, "no projection inserted");
    }
}
