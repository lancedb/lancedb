// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Coerces write-path input into blob v2 struct columns.
//!
//! Reached through the `blob_column` rule in [`super::cast`].

use std::sync::Arc;

use arrow_schema::{DataType, Field, FieldRef, Fields};
use datafusion_common::config::ConfigOptions;
use datafusion_physical_plan::PhysicalExpr;

use super::struct_expr::{StructChild, build_struct, cast_to_field, get_field_expr, typed_null};
use crate::error::{Error, Result};

/// Build a projection expression coercing `input_expr` into the blob struct declared by
/// `table_field`.
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

    let input_shape = BlobInputShape::of(input_field, table_field)?;

    // A missing blob is a null struct, not a struct of null children: the struct builder
    // produces no validity of its own, so `build_struct` restores the input's.
    let nulls_from = input_field.is_nullable().then(|| input_expr.clone());

    let mut children = Vec::with_capacity(declared_fields.len());
    for declared in declared_fields.iter() {
        children.push(StructChild {
            field: declared.clone(),
            value: input_shape.child_value(declared, &input_expr, config)?,
        });
    }

    let expr = build_struct(children, table_field, nulls_from, config)?;
    Ok((expr, table_field.clone()))
}

/// What the input looks like, and therefore which blob child it feeds.
///
/// All-null input is absent from this list: it needs nothing synthesized, so the
/// `blob_column` rule declines it and the generic cast produces the null structs.
enum BlobInputShape<'a> {
    /// Raw bytes, which become the blob's inline `data`.
    Bytes,
    /// A string, which becomes the blob's `uri`.
    String,
    /// A struct that already names some of the blob's children.
    Struct(&'a Fields),
}

impl<'a> BlobInputShape<'a> {
    fn of(input_field: &'a Field, table_field: &FieldRef) -> Result<Self> {
        match input_field.data_type() {
            DataType::Binary | DataType::LargeBinary | DataType::BinaryView => Ok(Self::Bytes),
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => Ok(Self::String),
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
                Ok(Self::Struct(children))
            }
            other => Err(Error::InvalidInput {
                message: format!(
                    "cannot coerce column '{}' with type {} into a blob v2 struct. \
                     expected binary bytes (Binary, LargeBinary, BinaryView), \
                     strings (Utf8, LargeUtf8, Utf8View), \
                     or a Struct with a 'data' or 'uri' child",
                    table_field.name(),
                    other,
                ),
            }),
        }
    }

    /// The expression feeding the blob's `declared` child.
    fn child_value(
        &self,
        declared: &FieldRef,
        input_expr: &Arc<dyn PhysicalExpr>,
        config: &Arc<ConfigOptions>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        let fed_by = match self {
            Self::Bytes => (declared.name() == "data").then_some(input_expr.clone()),
            Self::String => (declared.name() == "uri").then_some(input_expr.clone()),
            Self::Struct(children) => children
                .iter()
                .find(|c| c.name() == declared.name())
                .map(|child| get_field_expr(input_expr.clone(), child, config)),
        };

        match fed_by {
            Some(expr) => Ok(cast_to_field(expr, declared)),
            None => typed_null(declared),
        }
    }
}
#[cfg(test)]
mod tests {
    use super::super::cast::cast_to_table_schema;
    use super::*;
    use crate::blob::blob;
    use arrow_array::cast::AsArray;
    use arrow_array::{
        Array, ArrayRef, BinaryArray, BinaryViewArray, Int32Array, Int64Array, LargeBinaryArray,
        NullArray, RecordBatch, StringArray, StringViewArray, StructArray, UInt8Array, UInt64Array,
    };
    use arrow_buffer::NullBuffer;
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
    async fn null_column_coerces_to_all_null_blob_struct() {
        let batch = batch_with_image(
            Field::new("image", DataType::Null, true),
            Arc::new(NullArray::new(2)),
        );
        let coerced = coerce(batch, &blob_table_schema()).await;
        let image = image_struct(&coerced);
        assert!(image.is_null(0));
        assert!(image.is_null(1));
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
        assert!(
            image.is_null(1),
            "a missing blob is a null struct, not a struct of null children"
        );
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

    /// Arrow cannot cast bytes into a struct, so a list of blobs is only reachable through
    /// the list rule coercing the items one level down.
    #[tokio::test]
    async fn raw_bytes_coerce_into_a_list_of_blobs() {
        use arrow_array::ListArray;
        use arrow_buffer::OffsetBuffer;

        let images = ListArray::new(
            Arc::new(Field::new("item", DataType::LargeBinary, true)),
            OffsetBuffer::new(vec![0, 2, 2].into()),
            Arc::new(LargeBinaryArray::from_iter_values([
                b"one".as_slice(),
                b"two".as_slice(),
            ])),
            Some(NullBuffer::from(vec![true, false])),
        );
        let table_schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("images", DataType::List(Arc::new(blob("item", true))), true),
        ]);
        let batch = batch_with_image(
            Field::new("images", images.data_type().clone(), true),
            Arc::new(images),
        );

        let coerced = coerce(batch, &table_schema).await;
        let images = coerced.column_by_name("images").unwrap().as_list::<i32>();
        assert!(!images.is_null(0));
        assert!(images.is_null(1), "the list's own nulls must survive");

        let items = images.value(0);
        let items = items.as_any().downcast_ref::<StructArray>().unwrap();
        assert_eq!(items.len(), 2);
        let data = items
            .column_by_name("data")
            .unwrap()
            .as_any()
            .downcast_ref::<LargeBinaryArray>()
            .unwrap();
        assert_eq!(data.value(0), b"one");
        assert_eq!(data.value(1), b"two");

        let schema = coerced.schema();
        let DataType::List(item) = schema.field_with_name("images").unwrap().data_type() else {
            panic!("expected a list")
        };
        assert!(item.is_blob_v2(), "the item must keep its blob metadata");
    }

    /// The rebuilt list is the kind the input was, so a table field asking for another kind
    /// relies on the cast that follows.
    #[tokio::test]
    async fn a_list_of_raw_bytes_coerces_into_a_fixed_size_list_of_blobs() {
        use arrow_array::ListArray;
        use arrow_buffer::OffsetBuffer;

        let images = ListArray::new(
            Arc::new(Field::new("item", DataType::LargeBinary, true)),
            OffsetBuffer::new(vec![0, 2].into()),
            Arc::new(LargeBinaryArray::from_iter_values([
                b"one".as_slice(),
                b"two".as_slice(),
            ])),
            None,
        );
        let table_schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new(
                "images",
                DataType::FixedSizeList(Arc::new(blob("item", true)), 2),
                true,
            ),
        ]);
        let batch = batch_with_image(
            Field::new("images", images.data_type().clone(), true),
            Arc::new(images),
        );

        let coerced = coerce(batch, &table_schema).await;
        let images = coerced
            .column_by_name("images")
            .unwrap()
            .as_fixed_size_list();
        assert_eq!(images.value_length(), 2);

        let items = images.value(0);
        let items = items.as_any().downcast_ref::<StructArray>().unwrap();
        let data = items
            .column_by_name("data")
            .unwrap()
            .as_any()
            .downcast_ref::<LargeBinaryArray>()
            .unwrap();
        assert_eq!(data.value(0), b"one");
        assert_eq!(data.value(1), b"two");

        let schema = coerced.schema();
        let DataType::FixedSizeList(item, _) =
            schema.field_with_name("images").unwrap().data_type()
        else {
            panic!("expected a fixed size list")
        };
        assert!(item.is_blob_v2(), "the item must keep its blob metadata");
    }

    /// pyarrow names an inferred list's item `item`, whatever the table calls its own. A
    /// list has one child, so the two line up positionally regardless.
    #[tokio::test]
    async fn a_list_whose_item_is_named_differently_still_coerces() {
        use arrow_array::{BinaryArray, ListArray};
        use arrow_buffer::OffsetBuffer;

        let images = ListArray::new(
            Arc::new(Field::new("item", DataType::Binary, true)),
            OffsetBuffer::new(vec![0, 1].into()),
            Arc::new(BinaryArray::from_iter_values([b"one".as_slice()])),
            None,
        );
        let table_schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new(
                "images",
                DataType::LargeList(Arc::new(blob("element", true))),
                true,
            ),
        ]);
        let batch = batch_with_image(
            Field::new("images", images.data_type().clone(), true),
            Arc::new(images),
        );

        let coerced = coerce(batch, &table_schema).await;
        let schema = coerced.schema();
        let DataType::LargeList(item) = schema.field_with_name("images").unwrap().data_type()
        else {
            panic!("expected a large list")
        };
        assert_eq!(item.name(), "element");
        assert!(item.is_blob_v2(), "the item must keep its blob metadata");

        let images = coerced.column_by_name("images").unwrap();
        let items = images.as_list::<i64>().value(0);
        let items = items.as_any().downcast_ref::<StructArray>().unwrap();
        let data = items
            .column_by_name("data")
            .unwrap()
            .as_any()
            .downcast_ref::<LargeBinaryArray>()
            .unwrap();
        assert_eq!(data.value(0), b"one");
    }

    /// The struct rebuild drops the input's null bitmap unless validity is restored, which
    /// would resurrect a null blob as a struct holding the bytes the mask hid.
    #[tokio::test]
    async fn prebuilt_struct_nulls_stay_null_after_widening() {
        let DataType::Struct(narrow_children) = blob("image", true).data_type().clone() else {
            unreachable!("blob field is a struct")
        };
        let prebuilt = StructArray::new(
            narrow_children,
            vec![
                Arc::new(LargeBinaryArray::from_iter_values([
                    b"present".as_slice(),
                    b"masked".as_slice(),
                ])),
                Arc::new(StringArray::from(vec![None::<&str>, None])),
            ],
            Some(NullBuffer::from(vec![true, false])),
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
        assert!(!image.is_null(0));
        assert!(image.is_null(1));
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
            Field::new("image", DataType::Int64, true),
            Arc::new(Int64Array::from(vec![42])),
        );
        let err = coerce_err(batch, &blob_table_schema()).await;
        assert!(matches!(err, Error::InvalidInput { .. }), "got {err:?}");
        assert!(err.to_string().contains("image"));
    }

    #[tokio::test]
    async fn utf8_string_coerces_to_uri_child() {
        let batch = batch_with_image(
            Field::new("image", DataType::Utf8, true),
            Arc::new(StringArray::from(vec![Some("s3://bucket/key"), None])),
        );
        let coerced = coerce(batch, &blob_table_schema()).await;
        let image = image_struct(&coerced);
        let uri: &StringArray = image
            .column_by_name("uri")
            .unwrap()
            .as_any()
            .downcast_ref()
            .unwrap();
        assert_eq!(uri.value(0), "s3://bucket/key");
        assert!(image.column_by_name("data").unwrap().is_null(0));
        assert!(uri.is_null(1));
    }

    #[tokio::test]
    async fn large_utf8_string_coerces_into_four_child_blob_layout() {
        use arrow_array::LargeStringArray;

        let table_schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            wide_blob_field("image"),
        ]);
        let batch = batch_with_image(
            Field::new("image", DataType::LargeUtf8, true),
            Arc::new(LargeStringArray::from(vec!["file:///tmp/blob.bin"])),
        );
        let coerced = coerce(batch, &table_schema).await;
        let image = image_struct(&coerced);
        assert_eq!(image.num_columns(), 4);
        let uri: &StringArray = image
            .column_by_name("uri")
            .unwrap()
            .as_any()
            .downcast_ref()
            .unwrap();
        assert_eq!(uri.value(0), "file:///tmp/blob.bin");
        assert!(image.column_by_name("data").unwrap().is_null(0));
        assert!(image.column_by_name("position").unwrap().is_null(0));
        assert!(image.column_by_name("size").unwrap().is_null(0));
    }

    #[tokio::test]
    async fn utf8_view_string_coerces_to_uri_child() {
        let batch = batch_with_image(
            Field::new("image", DataType::Utf8View, true),
            Arc::new(StringViewArray::from(vec![Some("s3://bucket/view-key")])),
        );
        let coerced = coerce(batch, &blob_table_schema()).await;
        let image = image_struct(&coerced);
        let uri: &StringArray = image
            .column_by_name("uri")
            .unwrap()
            .as_any()
            .downcast_ref()
            .unwrap();
        assert_eq!(uri.value(0), "s3://bucket/view-key");
        assert!(image.column_by_name("data").unwrap().is_null(0));
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
