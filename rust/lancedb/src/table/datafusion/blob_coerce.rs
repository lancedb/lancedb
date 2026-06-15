// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Coerces write-path input into blob v2 struct columns.
//!
//! [`coerce_blob_expr`] is invoked from [`super::cast::cast_to_table_schema`].

use std::any::Any;
use std::sync::{Arc, LazyLock};

use arrow_array::{Array, ArrayRef, StructArray, new_null_array};
use arrow_schema::{DataType, Field, FieldRef, Fields};
use datafusion_common::config::ConfigOptions;
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use datafusion_physical_expr::ScalarFunctionExpr;
use datafusion_physical_plan::PhysicalExpr;

use crate::error::{Error, Result};

static COERCE_BLOB_UDF: LazyLock<Arc<ScalarUDF>> =
    LazyLock::new(|| Arc::new(ScalarUDF::from(CoerceBlobUdf::new())));

/// Build a projection expression coercing `input_expr` into the blob struct
/// declared by `table_field`.
///
/// The table field (metadata included) is the expression return field. Raw
/// binary lands in `data`; struct input is normalized to the declared layout.
pub(super) fn coerce_blob_expr(
    input_expr: Arc<dyn PhysicalExpr>,
    input_field: &Field,
    table_field: &FieldRef,
    config: &Arc<ConfigOptions>,
) -> Result<(Arc<dyn PhysicalExpr>, FieldRef)> {
    match input_field.data_type() {
        DataType::Binary | DataType::LargeBinary | DataType::BinaryView => {}
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
    }

    let expr: Arc<dyn PhysicalExpr> = Arc::new(ScalarFunctionExpr::new(
        &format!("coerce_blob({})", table_field.name()),
        COERCE_BLOB_UDF.clone(),
        vec![input_expr],
        table_field.clone(),
        config.clone(),
    ));
    Ok((expr, table_field.clone()))
}

/// Scalar UDF rewriting binary or struct input into the declared blob layout.
#[derive(Debug, Hash, PartialEq, Eq)]
struct CoerceBlobUdf {
    signature: Signature,
}

impl CoerceBlobUdf {
    fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for CoerceBlobUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "coerce_blob"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion_common::Result<DataType> {
        Err(datafusion_common::DataFusionError::Internal(
            "coerce_blob is only usable through coerce_blob_expr".into(),
        ))
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion_common::Result<ColumnarValue> {
        let array = match &args.args[0] {
            ColumnarValue::Array(array) => array.clone(),
            ColumnarValue::Scalar(scalar) => scalar.to_array_of_size(args.number_rows)?,
        };
        let coerced = coerce_array(&array, args.return_field.as_ref())
            .map_err(|e| datafusion_common::DataFusionError::External(e.into()))?;
        Ok(ColumnarValue::Array(coerced))
    }
}

fn coerce_array(column: &ArrayRef, field: &Field) -> Result<ArrayRef> {
    let DataType::Struct(declared_fields) = field.data_type() else {
        return Err(Error::InvalidInput {
            message: format!(
                "blob v2 column '{}' must be a struct, table declares {}",
                field.name(),
                field.data_type()
            ),
        });
    };

    match column.data_type() {
        DataType::Binary | DataType::LargeBinary | DataType::BinaryView => {
            let bytes = if column.data_type() == &DataType::LargeBinary {
                column.clone()
            } else {
                arrow_cast::cast(column, &DataType::LargeBinary).map_err(|e| {
                    Error::InvalidInput {
                        message: format!(
                            "failed to cast blob column '{}' to large binary: {}",
                            field.name(),
                            e
                        ),
                    }
                })?
            };
            binary_to_blob_struct(&bytes, declared_fields)
        }
        DataType::Struct(_) => struct_to_blob_struct(column, declared_fields),
        other => Err(Error::InvalidInput {
            message: format!(
                "cannot coerce column '{}' with type {} into a blob v2 struct. \
                 expected Binary, LargeBinary, BinaryView, or a Struct with a 'data' or 'uri' child",
                field.name(),
                other,
            ),
        }),
    }
}

fn binary_to_blob_struct(bytes: &ArrayRef, declared_fields: &Fields) -> Result<ArrayRef> {
    let children: Vec<ArrayRef> = declared_fields
        .iter()
        .map(|field| {
            if field.name() == "data" {
                bytes.clone()
            } else {
                new_null_array(field.data_type(), bytes.len())
            }
        })
        .collect();

    let blob_struct =
        StructArray::try_new(declared_fields.clone(), children, bytes.nulls().cloned()).map_err(
            |e| Error::Runtime {
                message: format!("failed to build blob struct: {e}"),
            },
        )?;
    Ok(Arc::new(blob_struct))
}

fn struct_to_blob_struct(column: &ArrayRef, declared_fields: &Fields) -> Result<ArrayRef> {
    let input = column
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| Error::InvalidInput {
            message: "expected struct array for blob coercion".into(),
        })?;

    let children: Result<Vec<ArrayRef>> = declared_fields
        .iter()
        .map(|field| {
            let Some(child) = input.column_by_name(field.name()) else {
                return Ok(new_null_array(field.data_type(), input.len()));
            };
            if child.data_type() == field.data_type() {
                return Ok(child.clone());
            }
            arrow_cast::cast(child, field.data_type()).map_err(|e| Error::InvalidInput {
                message: format!(
                    "failed to cast blob child '{}' to {}: {}",
                    field.name(),
                    field.data_type(),
                    e
                ),
            })
        })
        .collect();

    let blob_struct =
        StructArray::try_new(declared_fields.clone(), children?, input.nulls().cloned()).map_err(
            |e| Error::Runtime {
                message: format!("failed to build blob struct: {e}"),
            },
        )?;
    Ok(Arc::new(blob_struct))
}

#[cfg(test)]
mod tests {
    use super::super::cast::cast_to_table_schema;
    use super::*;
    use crate::blob::blob;
    use arrow_array::{
        BinaryArray, BinaryViewArray, Int32Array, Int64Array, LargeBinaryArray, RecordBatch,
        StringArray, UInt8Array, UInt64Array,
    };
    use arrow_schema::Schema;
    use datafusion::prelude::SessionContext;
    use datafusion_catalog::MemTable;
    use datafusion_physical_plan::ExecutionPlan;
    use futures::TryStreamExt;
    use lance_arrow::FieldExt;
    use std::collections::HashMap;

    /// Four-child layout from pyarrow `lance.blob.v2`.
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
        assert!(!image.is_null(0));
        assert!(image.is_null(1), "null input rows become null blob rows");
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
