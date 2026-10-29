// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Coerces write-path input into blob v2 struct columns.
//!
//! [`super::cast::cast_to_table_schema`] calls [`coerce_blob_expr`].

use std::collections::HashSet;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use arrow_array::{
    Array, ArrayRef, BooleanArray, FixedSizeListArray, LargeListArray, ListArray, RecordBatch,
};
use arrow_buffer::{BooleanBuffer, NullBuffer, NullBufferBuilder, OffsetBuffer};
use arrow_cast::cast;
use arrow_schema::{DataType, Field, FieldRef, Fields, Schema, SchemaRef};
use arrow_select::nullif::nullif;
use datafusion::functions::core::{get_field, named_struct};
use datafusion_common::config::ConfigOptions;
use datafusion_common::{DataFusionError, ScalarValue};
use datafusion_expr::ColumnarValue;
use datafusion_physical_expr::ScalarFunctionExpr;
use datafusion_physical_expr::expressions::{CastExpr, Literal};
use datafusion_physical_plan::PhysicalExpr;
use datafusion_physical_plan::expressions::Column;

use crate::error::{Error, Result};

/// Build a projection expression coercing `input_expr` into the blob struct
/// declared by `table_field`, composing `named_struct` / `get_field` / `cast`.
pub(super) fn coerce_blob_expr(
    input_expr: Arc<dyn PhysicalExpr>,
    input_field: &Field,
    table_field: &FieldRef,
    config: &Arc<ConfigOptions>,
    ancestors: &[Arc<dyn PhysicalExpr>],
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

    let input_shape = match input_field.data_type() {
        DataType::Null => {
            let expr: Arc<dyn PhysicalExpr> = Arc::new(CastExpr::new(
                input_expr,
                table_field.data_type().clone(),
                None,
            ));
            return Ok((expr, table_field.clone()));
        }
        DataType::Binary | DataType::LargeBinary | DataType::BinaryView => BlobInputShape::Bytes,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => BlobInputShape::String,
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
            let mut names = HashSet::with_capacity(children.len());
            for child in children {
                // Name-based extraction must not discard an ambiguous payload
                // or silently ignore a duplicate range child during narrowing.
                if !names.insert(child.name()) {
                    return Err(Error::InvalidInput {
                        message: format!(
                            "blob child '{}' in column '{}' is specified more than once",
                            child.name(),
                            table_field.name()
                        ),
                    });
                }
                if !declared_fields
                    .iter()
                    .any(|field| field.name() == child.name())
                    && !matches!(child.name().as_str(), "position" | "size")
                {
                    return Err(Error::InvalidInput {
                        message: format!(
                            "unknown blob child '{}' in column '{}'",
                            child.name(),
                            table_field.name()
                        ),
                    });
                }
            }
            BlobInputShape::Struct(children)
        }
        other => {
            return Err(Error::InvalidInput {
                message: format!(
                    "cannot coerce column '{}' with type {} into a blob v2 struct. \
                     expected binary bytes (Binary, LargeBinary, BinaryView), \
                     strings (Utf8, LargeUtf8, Utf8View), \
                     or a Struct with a 'data' or 'uri' child",
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

        let value: Arc<dyn PhysicalExpr> = match &input_shape {
            BlobInputShape::Bytes => {
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
            BlobInputShape::String => {
                if declared.name() == "uri" {
                    Arc::new(CastExpr::new(
                        input_expr.clone(),
                        declared.data_type().clone(),
                        None,
                    ))
                } else {
                    typed_null(declared.data_type())?
                }
            }
            BlobInputShape::Struct(children) => {
                match children.iter().find(|c| c.name() == declared.name()) {
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
                }
            }
        };
        ns_args.push(value);
    }

    let built: Arc<dyn PhysicalExpr> = Arc::new(ScalarFunctionExpr::new(
        &format!("named_struct({})", table_field.name()),
        named_struct(),
        ns_args,
        table_field.clone(),
        config.clone(),
    ));

    // `named_struct` always yields a valid struct, so a null input would land
    // as a row that set neither `data` nor `uri` -- not an absent blob but a
    // malformed one, which Lance rejects on write.
    let dropped_ranges = match &input_shape {
        BlobInputShape::Struct(children) => children
            .iter()
            .enumerate()
            .filter(|(_, child)| {
                matches!(child.name().as_str(), "position" | "size")
                    && !declared_fields
                        .iter()
                        .any(|field| field.name() == child.name())
            })
            .map(|(index, _)| index)
            .collect(),
        _ => Vec::new(),
    };
    let expr: Arc<dyn PhysicalExpr> = Arc::new(AbsentBlobIsNull {
        source: input_expr,
        built,
        field: table_field.clone(),
        dropped_ranges,
        ancestors: ancestors.to_vec(),
    });
    Ok((expr, table_field.clone()))
}

/// Recurse through list items while retaining the list's offsets and validity.
/// A child batch reuses the field projection, including nested structs and lists,
/// so every blob leaf receives the same coercion as a top-level blob column.
pub(super) fn coerce_blob_list_expr(
    input_expr: Arc<dyn PhysicalExpr>,
    input_field: &Field,
    table_field: &FieldRef,
    ancestors: &[Arc<dyn PhysicalExpr>],
) -> Result<Option<(Arc<dyn PhysicalExpr>, FieldRef)>> {
    let (DataType::List(input_item)
    | DataType::LargeList(input_item)
    | DataType::FixedSizeList(input_item, _)) = input_field.data_type()
    else {
        return Ok(None);
    };
    let (DataType::List(table_item)
    | DataType::LargeList(table_item)
    | DataType::FixedSizeList(table_item, _)) = table_field.data_type()
    else {
        return Ok(None);
    };
    if !crate::blob::field_tree_has_blob_v2(table_item) || input_item == table_item {
        return Ok(None);
    }

    // List item names need not match across SDKs. Their position identifies the
    // item, while named children inside a struct still match by name.
    let input_fields = vec![Arc::new(
        input_item.as_ref().clone().with_name(table_item.name()),
    )]
    .into();
    let validity_field = Field::new("__blob_parent_validity", DataType::Boolean, true);
    let (item_expr, output_item) = super::cast::build_field_exprs(
        &input_fields,
        &vec![table_item.clone()].into(),
        &|_| Arc::new(Column::new(input_item.name(), 0)),
        &[Arc::new(Column::new(validity_field.name(), 1))],
    )?
    .pop()
    .expect("one list item projection");
    let item_schema = Arc::new(Schema::new(vec![
        input_item.clone(),
        Arc::new(validity_field),
    ]));
    let output_type = match table_field.data_type() {
        DataType::List(_) => DataType::List(output_item),
        DataType::LargeList(_) => DataType::LargeList(output_item),
        DataType::FixedSizeList(_, size) => DataType::FixedSizeList(output_item, *size),
        _ => unreachable!(),
    };
    let output_field = Arc::new(table_field.as_ref().clone().with_data_type(output_type));
    let expr: Arc<dyn PhysicalExpr> = Arc::new(CoerceBlobList {
        source: input_expr,
        input_item: input_item.clone(),
        item_expr,
        field: output_field.clone(),
        ancestors: ancestors.to_vec(),
        item_schema,
    });
    Ok(Some((expr, output_field)))
}

#[derive(Debug, Clone)]
struct CoerceBlobList {
    source: Arc<dyn PhysicalExpr>,
    input_item: FieldRef,
    item_expr: Arc<dyn PhysicalExpr>,
    field: FieldRef,
    ancestors: Vec<Arc<dyn PhysicalExpr>>,
    item_schema: SchemaRef,
}

impl fmt::Display for CoerceBlobList {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "coerce_blob_list({})", self.source)
    }
}

impl PartialEq for CoerceBlobList {
    fn eq(&self, other: &Self) -> bool {
        self.source.eq(&other.source)
            && self.input_item == other.input_item
            && self.item_expr.eq(&other.item_expr)
            && self.field == other.field
            && self.ancestors == other.ancestors
    }
}

impl Eq for CoerceBlobList {}

impl Hash for CoerceBlobList {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.source.hash(state);
        self.input_item.hash(state);
        self.item_expr.hash(state);
        self.field.hash(state);
        self.ancestors.hash(state);
    }
}

impl PhysicalExpr for CoerceBlobList {
    fn return_field(&self, _input_schema: &Schema) -> datafusion_common::Result<FieldRef> {
        Ok(self.field.clone())
    }

    fn nullable(&self, _input_schema: &Schema) -> datafusion_common::Result<bool> {
        Ok(self.field.is_nullable())
    }

    fn evaluate(&self, batch: &RecordBatch) -> datafusion_common::Result<ColumnarValue> {
        let source = self.source.evaluate(batch)?.into_array(batch.num_rows())?;
        // First convert the outer list kind, if needed, while keeping its binary
        // item type. PyArrow commonly infers List even for a LargeList column.
        let intermediate_type = match self.field.data_type() {
            DataType::List(_) => DataType::List(self.input_item.clone()),
            DataType::LargeList(_) => DataType::LargeList(self.input_item.clone()),
            DataType::FixedSizeList(_, size) => {
                DataType::FixedSizeList(self.input_item.clone(), *size)
            }
            _ => unreachable!("validated list type when building the expression"),
        };
        let source = if source.data_type() == &intermediate_type {
            source
        } else {
            cast(source.as_ref(), &intermediate_type)?
        };
        let row_nulls = NullBuffer::union(
            source.logical_nulls().as_ref(),
            ancestor_nulls(&self.ancestors, batch)?.as_ref(),
        );
        let (values, item_nulls) = match source.data_type() {
            DataType::List(_) => {
                let list = source.as_any().downcast_ref::<ListArray>().unwrap();
                let first = list.offsets()[0] as usize;
                let last = *list.offsets().last().unwrap() as usize;
                (
                    list.values().slice(first, last - first),
                    expand_list_nulls(row_nulls.as_ref(), list.offsets().lengths(), last - first),
                )
            }
            DataType::LargeList(_) => {
                let list = source.as_any().downcast_ref::<LargeListArray>().unwrap();
                let first = list.offsets()[0] as usize;
                let last = *list.offsets().last().unwrap() as usize;
                (
                    list.values().slice(first, last - first),
                    expand_list_nulls(row_nulls.as_ref(), list.offsets().lengths(), last - first),
                )
            }
            DataType::FixedSizeList(_, size) => (
                source
                    .as_any()
                    .downcast_ref::<FixedSizeListArray>()
                    .unwrap()
                    .values()
                    .clone(),
                row_nulls.as_ref().map(|nulls| nulls.expand(*size as usize)),
            ),
            other => {
                return Err(DataFusionError::Internal(format!(
                    "coerce_blob_list expected a list, got {other}"
                )));
            }
        };
        // Carry list/ancestor validity in a separate column. Applying it to the
        // items themselves would introduce nulls into non-nullable child fields.
        let validity = BooleanArray::new(
            item_nulls
                .as_ref()
                .map(|nulls| nulls.inner().clone())
                .unwrap_or_else(|| BooleanBuffer::new_set(values.len())),
            item_nulls,
        );
        let item_batch = RecordBatch::try_new(
            self.item_schema.clone(),
            vec![values.clone(), Arc::new(validity)],
        )?;
        let coerced = self
            .item_expr
            .evaluate(&item_batch)?
            .into_array(values.len())?;

        let rebuilt: ArrayRef = match (source.data_type(), self.field.data_type()) {
            (DataType::List(_), DataType::List(item)) => {
                let list = source.as_any().downcast_ref::<ListArray>().unwrap();
                Arc::new(ListArray::try_new(
                    item.clone(),
                    OffsetBuffer::from_lengths(list.offsets().lengths()),
                    coerced,
                    list.nulls().cloned(),
                )?)
            }
            (DataType::LargeList(_), DataType::LargeList(item)) => {
                let list = source.as_any().downcast_ref::<LargeListArray>().unwrap();
                Arc::new(LargeListArray::try_new(
                    item.clone(),
                    OffsetBuffer::from_lengths(list.offsets().lengths()),
                    coerced,
                    list.nulls().cloned(),
                )?)
            }
            (DataType::FixedSizeList(_, _), DataType::FixedSizeList(item, size)) => {
                let list = source
                    .as_any()
                    .downcast_ref::<FixedSizeListArray>()
                    .unwrap();
                Arc::new(FixedSizeListArray::try_new_with_length(
                    item.clone(),
                    *size,
                    coerced,
                    list.nulls().cloned(),
                    list.len(),
                )?)
            }
            _ => unreachable!("validated list types when building the expression"),
        };
        Ok(ColumnarValue::Array(rebuilt))
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        std::iter::once(&self.source)
            .chain(self.ancestors.iter())
            .collect()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> datafusion_common::Result<Arc<dyn PhysicalExpr>> {
        Ok(Arc::new(Self {
            source: children[0].clone(),
            input_item: self.input_item.clone(),
            item_expr: self.item_expr.clone(),
            field: self.field.clone(),
            ancestors: children[1..].to_vec(),
            item_schema: self.item_schema.clone(),
        }))
    }

    fn fmt_sql(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self}")
    }
}

fn ancestor_nulls(
    ancestors: &[Arc<dyn PhysicalExpr>],
    batch: &RecordBatch,
) -> datafusion_common::Result<Option<NullBuffer>> {
    let mut nulls = None;
    for ancestor in ancestors {
        let array = ancestor.evaluate(batch)?.into_array(batch.num_rows())?;
        nulls = NullBuffer::union(nulls.as_ref(), array.logical_nulls().as_ref());
    }
    Ok(nulls)
}

fn expand_list_nulls(
    nulls: Option<&NullBuffer>,
    lengths: impl Iterator<Item = usize>,
    item_count: usize,
) -> Option<NullBuffer> {
    let nulls = nulls?;
    let mut builder = NullBufferBuilder::new(item_count);
    for (length, valid) in lengths.zip(nulls.iter()) {
        if valid {
            builder.append_n_non_nulls(length);
        } else {
            builder.append_n_nulls(length);
        }
    }
    builder.finish()
}

/// Carries the source column's nullity onto the struct built for it.
///
/// This is its own expression rather than a `CASE` because the projection
/// takes its output field from `return_field`, and the generic implementation
/// rebuilds a bare field -- which would drop the `lance.blob.v2` extension
/// metadata and stop the column being recognised as a blob at all.
#[derive(Debug, Clone)]
struct AbsentBlobIsNull {
    source: Arc<dyn PhysicalExpr>,
    built: Arc<dyn PhysicalExpr>,
    field: FieldRef,
    dropped_ranges: Vec<usize>,
    ancestors: Vec<Arc<dyn PhysicalExpr>>,
}

impl fmt::Display for AbsentBlobIsNull {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "absent_blob_is_null({}, {})", self.source, self.built)
    }
}

impl PartialEq for AbsentBlobIsNull {
    fn eq(&self, other: &Self) -> bool {
        self.source.eq(&other.source)
            && self.built.eq(&other.built)
            && self.field == other.field
            && self.dropped_ranges == other.dropped_ranges
            && self.ancestors == other.ancestors
    }
}

impl Eq for AbsentBlobIsNull {}

impl Hash for AbsentBlobIsNull {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.source.hash(state);
        self.built.hash(state);
        self.field.hash(state);
        self.dropped_ranges.hash(state);
        self.ancestors.hash(state);
    }
}

impl PhysicalExpr for AbsentBlobIsNull {
    fn return_field(&self, _input_schema: &Schema) -> datafusion_common::Result<FieldRef> {
        Ok(self.field.clone())
    }

    fn nullable(&self, _input_schema: &Schema) -> datafusion_common::Result<bool> {
        Ok(true)
    }

    fn evaluate(&self, batch: &RecordBatch) -> datafusion_common::Result<ColumnarValue> {
        let rows = batch.num_rows();
        let source = self.source.evaluate(batch)?.into_array(rows)?;
        if !self.dropped_ranges.is_empty() {
            let ancestor_nulls = ancestor_nulls(&self.ancestors, batch)?;
            let values = source
                .as_any()
                .downcast_ref::<arrow_array::StructArray>()
                .unwrap();
            let effective_nulls = NullBuffer::union(values.nulls(), ancestor_nulls.as_ref());
            for index in &self.dropped_ranges {
                let nulls = NullBuffer::union(
                    effective_nulls.as_ref(),
                    values.column(*index).logical_nulls().as_ref(),
                );
                if nulls.as_ref().map_or(values.len() != 0, |nulls| {
                    nulls.null_count() != values.len()
                }) {
                    return Err(DataFusionError::Execution(format!(
                        "blob storage schema cannot store byte ranges in column '{}': position and size must both be null",
                        self.field.name()
                    )));
                }
            }
        }
        let built = self.built.evaluate(batch)?.into_array(rows)?;
        let Some(nulls) = source.logical_nulls() else {
            return Ok(ColumnarValue::Array(built));
        };
        // `nullif` nulls the rows the mask marks true, which is where the
        // source had no value.
        let absent = BooleanArray::new(!nulls.inner(), None);
        Ok(ColumnarValue::Array(nullif(built.as_ref(), &absent)?))
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        [&self.source, &self.built]
            .into_iter()
            .chain(self.ancestors.iter())
            .collect()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> datafusion_common::Result<Arc<dyn PhysicalExpr>> {
        Ok(Arc::new(Self {
            source: children[0].clone(),
            built: children[1].clone(),
            field: self.field.clone(),
            dropped_ranges: self.dropped_ranges.clone(),
            ancestors: children[2..].to_vec(),
        }))
    }

    fn fmt_sql(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self}")
    }
}

enum BlobInputShape<'a> {
    Bytes,
    String,
    Struct(&'a Fields),
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
    use arrow_array::{
        Array, ArrayRef, BinaryArray, BinaryViewArray, FixedSizeListArray, Int32Array, Int64Array,
        LargeBinaryArray, LargeListArray, ListArray, NullArray, RecordBatch, StringArray,
        StringViewArray, StructArray, UInt8Array, UInt64Array,
    };
    use arrow_buffer::{NullBuffer, OffsetBuffer};
    use arrow_schema::Schema;
    use datafusion::prelude::SessionContext;
    use datafusion_catalog::MemTable;
    use datafusion_physical_plan::ExecutionPlan;
    use futures::TryStreamExt;
    use lance::blob::blob_field as blob;
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
    async fn binary_list_items_coerce_to_blobs_without_losing_list_shape() {
        for kind in [
            "list",
            "large_list",
            "large_list_from_list",
            "fixed_size_list",
        ] {
            let input_item = Arc::new(Field::new("item", DataType::Binary, true));
            let table_item = Arc::new(wide_blob_field("item"));
            let values: ArrayRef = Arc::new(BinaryArray::from_iter(vec![
                Some(b"a".as_slice()),
                None,
                Some(b"".as_slice()),
                Some(b"ignored".as_slice()),
                None,
                None,
            ]));
            let (input_type, table_type, list): (DataType, DataType, ArrayRef) = match kind {
                "list" => (
                    DataType::List(input_item.clone()),
                    DataType::List(table_item.clone()),
                    Arc::new(ListArray::new(
                        input_item,
                        OffsetBuffer::new(vec![0, 3, 3].into()),
                        values,
                        Some(NullBuffer::from(vec![true, false])),
                    )),
                ),
                "large_list" => (
                    DataType::LargeList(input_item.clone()),
                    DataType::LargeList(table_item.clone()),
                    Arc::new(LargeListArray::new(
                        input_item,
                        OffsetBuffer::new(vec![0_i64, 3, 3].into()),
                        values,
                        Some(NullBuffer::from(vec![true, false])),
                    )),
                ),
                "large_list_from_list" => (
                    DataType::List(input_item.clone()),
                    DataType::LargeList(table_item.clone()),
                    Arc::new(ListArray::new(
                        input_item,
                        OffsetBuffer::new(vec![0, 3, 3].into()),
                        values,
                        Some(NullBuffer::from(vec![true, false])),
                    )),
                ),
                "fixed_size_list" => (
                    DataType::FixedSizeList(input_item.clone(), 3),
                    DataType::FixedSizeList(table_item.clone(), 3),
                    Arc::new(FixedSizeListArray::new(
                        input_item,
                        3,
                        values,
                        Some(NullBuffer::from(vec![true, false])),
                    )),
                ),
                _ => unreachable!(),
            };
            let batch = batch_with_image(Field::new("image", input_type, true), list);
            let schema = Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("image", table_type, true),
            ]);
            let coerced = coerce(batch, &schema).await;
            let image = coerced.column_by_name("image").unwrap();
            let output_schema = coerced.schema();
            let (DataType::List(item_field)
            | DataType::LargeList(item_field)
            | DataType::FixedSizeList(item_field, _)) =
                output_schema.field_with_name("image").unwrap().data_type()
            else {
                unreachable!()
            };
            assert!(item_field.is_blob_v2(), "{kind} lost blob metadata");
            let (offsets, items): (Vec<i64>, &StructArray) = match kind {
                "list" => {
                    let array = image.as_any().downcast_ref::<ListArray>().unwrap();
                    assert!(array.is_null(1));
                    (
                        array.offsets().iter().map(|v| i64::from(*v)).collect(),
                        array.values().as_any().downcast_ref().unwrap(),
                    )
                }
                "large_list" | "large_list_from_list" => {
                    let array = image.as_any().downcast_ref::<LargeListArray>().unwrap();
                    assert!(array.is_null(1));
                    (
                        array.offsets().iter().copied().collect(),
                        array.values().as_any().downcast_ref().unwrap(),
                    )
                }
                "fixed_size_list" => {
                    let array = image.as_any().downcast_ref::<FixedSizeListArray>().unwrap();
                    assert!(array.is_null(1));
                    (
                        vec![0, 3, 6],
                        array.values().as_any().downcast_ref().unwrap(),
                    )
                }
                _ => unreachable!(),
            };
            assert_eq!(
                offsets,
                if kind == "fixed_size_list" {
                    vec![0, 3, 6]
                } else {
                    vec![0, 3, 3]
                }
            );
            assert_eq!(items.len(), if kind == "fixed_size_list" { 6 } else { 3 });
            let data: &LargeBinaryArray = items
                .column_by_name("data")
                .unwrap()
                .as_any()
                .downcast_ref()
                .unwrap();
            assert_eq!(data.value(0), b"a");
            assert!(items.is_null(1));
            assert_eq!(data.value(2), b"");
            assert!(!items.is_null(2));
        }
    }

    #[tokio::test]
    async fn sliced_binary_lists_only_coerce_visible_items() {
        for large in [false, true] {
            let input_item = Arc::new(Field::new("item", DataType::Binary, true));
            let table_item = Arc::new(wide_blob_field("item"));
            let values: ArrayRef = Arc::new(BinaryArray::from_iter(vec![
                Some(b"before".as_slice()),
                Some(b"a".as_slice()),
                None,
                Some(b"after".as_slice()),
                Some(b"tail".as_slice()),
            ]));
            let (input_type, table_type, list): (DataType, DataType, ArrayRef) = if large {
                (
                    DataType::LargeList(input_item.clone()),
                    DataType::LargeList(table_item),
                    Arc::new(
                        LargeListArray::new(
                            input_item,
                            OffsetBuffer::new(vec![0_i64, 1, 3, 4, 5].into()),
                            values,
                            None,
                        )
                        .slice(1, 2),
                    ),
                )
            } else {
                (
                    DataType::List(input_item.clone()),
                    DataType::List(table_item),
                    Arc::new(
                        ListArray::new(
                            input_item,
                            OffsetBuffer::new(vec![0, 1, 3, 4, 5].into()),
                            values,
                            None,
                        )
                        .slice(1, 2),
                    ),
                )
            };
            let batch = batch_with_image(Field::new("image", input_type, true), list);
            let schema = Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("image", table_type, true),
            ]);
            let coerced = coerce(batch, &schema).await;
            let image = coerced.column_by_name("image").unwrap();
            let (offsets, items): (Vec<i64>, &StructArray) = if large {
                let list = image.as_any().downcast_ref::<LargeListArray>().unwrap();
                (
                    list.offsets().iter().copied().collect(),
                    list.values().as_any().downcast_ref().unwrap(),
                )
            } else {
                let list = image.as_any().downcast_ref::<ListArray>().unwrap();
                (
                    list.offsets().iter().map(|v| i64::from(*v)).collect(),
                    list.values().as_any().downcast_ref().unwrap(),
                )
            };
            assert_eq!(offsets, [0, 2, 3]);
            assert_eq!(items.len(), 3);
            let data: &LargeBinaryArray = items
                .column_by_name("data")
                .unwrap()
                .as_any()
                .downcast_ref()
                .unwrap();
            assert_eq!(data.value(0), b"a");
            assert!(items.is_null(1));
            assert_eq!(data.value(2), b"after");
        }
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
        // The row itself has to be null, not merely a struct whose children
        // are. A present-but-empty struct set neither `data` nor `uri`, which
        // Lance rejects as malformed rather than reading as an absent blob.
        assert!(!image.is_null(0));
        assert!(image.is_null(1));
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
    async fn dropping_ranges_respects_parent_validity_and_slices() {
        for parent_valid in [false, true] {
            let DataType::Struct(children) = wide_blob_field("image").data_type().clone() else {
                unreachable!()
            };
            let input = StructArray::new(
                children,
                vec![
                    Arc::new(LargeBinaryArray::from(vec![
                        Some(b"ignored".as_slice()),
                        Some(b"payload".as_slice()),
                    ])),
                    Arc::new(StringArray::from(vec![None::<&str>, None])),
                    Arc::new(UInt64Array::from(vec![None, Some(4)])),
                    Arc::new(UInt64Array::new_null(2)),
                ],
                Some(NullBuffer::from(vec![true, parent_valid])),
            )
            .slice(1, 1);
            let batch = batch_with_image(
                Field::new("image", input.data_type().clone(), true),
                Arc::new(input),
            );
            if parent_valid {
                let plan = cast_to_table_schema(plan_from_batch(batch).await, &blob_table_schema())
                    .unwrap();
                let error = plan
                    .execute(0, SessionContext::new().task_ctx())
                    .unwrap()
                    .try_collect::<Vec<_>>()
                    .await
                    .unwrap_err();
                assert!(error.to_string().contains("cannot store byte ranges"));
            } else {
                let output = coerce(batch, &blob_table_schema()).await;
                assert!(image_struct(&output).is_null(0));
            }
        }
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

    fn nested_layout_batch(
        wide: bool,
        id: i64,
        range: Option<u64>,
        nesting: &[&str],
    ) -> RecordBatch {
        nested_layout_batch_with_null(wide, id, range, nesting, None)
    }

    fn nested_layout_batch_with_null(
        wide: bool,
        id: i64,
        range: Option<u64>,
        nesting: &[&str],
        null_depth: Option<usize>,
    ) -> RecordBatch {
        nested_layout_batch_with_options(wide, id, range, nesting, null_depth, &[])
    }

    fn nested_layout_batch_with_options(
        wide: bool,
        id: i64,
        range: Option<u64>,
        nesting: &[&str],
        null_depth: Option<usize>,
        captions: &[&str],
    ) -> RecordBatch {
        // Ancestor validity must not introduce nulls into a required blob child.
        let field = if wide {
            crate::blob("image", false)
        } else {
            blob("image", false)
        };
        let DataType::Struct(children) = field.data_type().clone() else {
            unreachable!()
        };
        let mut columns: Vec<ArrayRef> = vec![
            Arc::new(LargeBinaryArray::from(vec![Some(b"payload".as_slice())])),
            Arc::new(StringArray::from(vec![None::<&str>])),
        ];
        if wide {
            columns.push(Arc::new(UInt64Array::from(vec![range])));
            columns.push(Arc::new(UInt64Array::from(vec![range])));
        }
        let array: ArrayRef = Arc::new(StructArray::new(children, columns, None));
        nest_blob_batch(field, array, id, nesting, null_depth, captions)
    }

    fn nest_blob_batch(
        mut field: Field,
        mut array: ArrayRef,
        id: i64,
        nesting: &[&str],
        null_depth: Option<usize>,
        captions: &[&str],
    ) -> RecordBatch {
        for (depth, kind) in nesting.iter().enumerate().rev() {
            let nulls = (null_depth == Some(depth)).then(|| NullBuffer::from(vec![false]));
            let data_type = match *kind {
                "struct" => {
                    let mut fields = vec![Arc::new(field)];
                    let mut columns = vec![array];
                    if depth + 1 == nesting.len() {
                        for caption in captions {
                            fields.push(Arc::new(Field::new("caption", DataType::Utf8, true)));
                            columns.push(Arc::new(StringArray::from(vec![*caption])));
                        }
                    }
                    let fields: Fields = fields.into();
                    array = Arc::new(StructArray::new(fields.clone(), columns, nulls));
                    DataType::Struct(fields)
                }
                "list" => {
                    let item = Arc::new(field.with_name("item"));
                    array = Arc::new(ListArray::new(
                        item.clone(),
                        OffsetBuffer::new(vec![0, 1].into()),
                        array,
                        nulls,
                    ));
                    DataType::List(item)
                }
                "large_list" => {
                    let item = Arc::new(field.with_name("item"));
                    array = Arc::new(LargeListArray::new(
                        item.clone(),
                        OffsetBuffer::new(vec![0_i64, 1].into()),
                        array,
                        nulls,
                    ));
                    DataType::LargeList(item)
                }
                "fixed_size_list" => {
                    let item = Arc::new(field.with_name("item"));
                    array = Arc::new(FixedSizeListArray::new(item.clone(), 1, array, nulls));
                    DataType::FixedSizeList(item, 1)
                }
                _ => unreachable!(),
            };
            field = Field::new("value", data_type, true);
        }
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                field,
            ])),
            vec![Arc::new(Int64Array::from(vec![id])), array],
        )
        .unwrap()
    }

    fn duplicate_blob_child_batch(wide: bool, nesting: &[&str], child_index: usize) -> RecordBatch {
        let field = if wide {
            crate::blob("image", false)
        } else {
            blob("image", false)
        };
        let DataType::Struct(children) = field.data_type() else {
            unreachable!()
        };
        let duplicate = children[child_index].clone();
        let mut fields = children.to_vec();
        fields.push(duplicate.clone());
        let mut columns = children
            .iter()
            .map(|child| arrow_array::new_null_array(child.data_type(), 1))
            .collect::<Vec<_>>();
        columns[0] = Arc::new(LargeBinaryArray::from(vec![b"first".as_slice()]));
        // Distinct duplicate values expose silent name-based selection. Range
        // duplicates also must not bypass null-only narrowing to old layouts.
        columns.push(match duplicate.name().as_str() {
            "data" => Arc::new(LargeBinaryArray::from(vec![b"second".as_slice()])) as ArrayRef,
            "uri" => Arc::new(StringArray::from(vec!["file:///discarded"])),
            "position" | "size" => Arc::new(UInt64Array::from(vec![4])),
            _ => unreachable!(),
        });
        let fields: Fields = fields.into();
        let array = Arc::new(StructArray::new(fields.clone(), columns, None));
        let field = field.with_data_type(DataType::Struct(fields));
        nest_blob_batch(field, array, 1, nesting, None, &[])
    }

    #[rstest::rstest]
    #[case(&[])]
    #[case(&["struct"])]
    #[case(&["list"])]
    #[case(&["list", "struct"])]
    #[case(&["list", "list", "struct"])]
    #[case(&["struct", "list", "struct"])]
    #[case(&["large_list", "struct"])]
    #[case(&["fixed_size_list", "struct", "large_list"])]
    #[tokio::test]
    async fn projection_rejects_duplicate_blob_children(
        #[case] nesting: &[&str],
        #[values(false, true)] wide_source: bool,
        #[values(false, true)] wide_target: bool,
    ) {
        let target = nested_layout_batch(wide_target, 0, None, nesting);
        for child_index in 0..if wide_source { 4 } else { 2 } {
            let input = duplicate_blob_child_batch(wide_source, nesting, child_index);
            let error = coerce_err(input, target.schema().as_ref())
                .await
                .to_string();
            assert!(error.contains("specified more than once"), "{error}");
        }
    }

    #[cfg(feature = "remote")]
    #[rstest::rstest]
    #[case(&[])]
    #[case(&["struct"])]
    #[case(&["list"])]
    #[case(&["list", "struct"])]
    #[case(&["list", "list", "struct"])]
    #[case(&["struct", "list", "struct"])]
    #[case(&["large_list", "struct"])]
    #[tokio::test]
    async fn writes_reject_duplicate_blob_children(
        #[case] nesting: &[&str],
        #[values(false, true)] wide_source: bool,
        #[values(false, true)] wide_target: bool,
        #[values(false, true)] remote: bool,
        #[values(false, true)] merge: bool,
    ) {
        use crate::query::ExecutableQuery;

        let initial = nested_layout_batch(wide_target, 0, None, nesting);
        let schema = initial.schema();
        let db = crate::connect("memory://").execute().await.unwrap();
        let (table, receiver) = if remote {
            let (table, receiver) = remote_layout_table(schema.as_ref());
            (table, Some(receiver))
        } else {
            (
                db.create_table("descriptors", initial)
                    .execute()
                    .await
                    .unwrap(),
                None,
            )
        };
        let snapshot = if remote {
            None
        } else {
            Some((
                table.version().await.unwrap(),
                table
                    .query()
                    .execute()
                    .await
                    .unwrap()
                    .try_collect::<Vec<_>>()
                    .await
                    .unwrap(),
            ))
        };
        for child_index in 0..if wide_source { 4 } else { 2 } {
            let input = duplicate_blob_child_batch(wide_source, nesting, child_index);
            let result = if merge {
                let mut builder = table.merge_insert(&["id"]);
                builder
                    .when_matched_update_all(None)
                    .when_not_matched_insert_all();
                let schema = input.schema();
                builder
                    .execute(Box::new(arrow_array::RecordBatchIterator::new(
                        vec![Ok(input)],
                        schema,
                    )))
                    .await
                    .map(|_| ())
            } else {
                table.add(input).execute().await.map(|_| ())
            };
            let error = result.unwrap_err().to_string();
            assert!(error.contains("specified more than once"), "{error}");
            if let Some(receiver) = &receiver {
                assert!(
                    receiver.try_recv().is_err(),
                    "ambiguous write reached server"
                );
            } else {
                let (version, before) = snapshot.as_ref().unwrap();
                assert_eq!(table.version().await.unwrap(), *version);
                assert_eq!(table.count_rows(None).await.unwrap(), 1);
                assert_eq!(table.schema().await.unwrap(), schema);
                let batches = table
                    .query()
                    .execute()
                    .await
                    .unwrap()
                    .try_collect::<Vec<_>>()
                    .await
                    .unwrap();
                assert_eq!(&batches, before);
            }
        }
    }

    #[rstest::rstest]
    #[case(&["struct"])]
    #[case(&["struct", "struct"])]
    #[case(&["list", "struct"])]
    #[case(&["struct", "list", "struct"])]
    #[case(&["large_list", "struct", "list"])]
    #[case(&["fixed_size_list", "struct", "large_list"])]
    #[tokio::test]
    async fn nullable_nested_blob_layout_projection(
        #[case] nesting: &[&str],
        #[values(false, true)] wide_target: bool,
        #[values(false, true)] sliced: bool,
    ) {
        for depth in 0..nesting.len() {
            // Physical range slots beneath a null ancestor are not logical values.
            let input =
                nested_layout_batch_with_null(!wide_target, 1, Some(0), nesting, Some(depth));
            let input = if sliced {
                let outside = nested_layout_batch(!wide_target, 0, Some(4), nesting);
                arrow_select::concat::concat_batches(
                    &input.schema(),
                    &[outside.clone(), input, outside],
                )
                .unwrap()
                .slice(1, 1)
            } else {
                input
            };
            let expected =
                nested_layout_batch_with_null(wide_target, 1, None, nesting, Some(depth));
            let output = coerce(input, expected.schema().as_ref()).await;
            assert_eq!(output, expected, "null ancestor at {depth} in {nesting:?}");
        }
    }

    #[rstest::rstest]
    #[case(&["list"])]
    #[case(&["list", "struct"])]
    #[case(&["list", "list"])]
    #[case(&["large_list", "struct", "list"])]
    #[case(&["fixed_size_list", "struct", "large_list"])]
    #[case(&["struct", "list", "struct"])]
    #[tokio::test]
    async fn nested_blob_layout_projection(
        #[case] nesting: &[&str],
        #[values(false, true)] wide_target: bool,
        #[values(false, true)] sliced: bool,
    ) {
        let input = nested_layout_batch(!wide_target, 1, None, nesting);
        let input = if sliced {
            // Populated ranges outside a sliced collection must not be examined.
            let before = nested_layout_batch(!wide_target, 0, Some(4), nesting);
            let after = nested_layout_batch(!wide_target, 2, Some(4), nesting);
            arrow_select::concat::concat_batches(&input.schema(), &[before, input, after])
                .unwrap()
                .slice(1, 1)
        } else {
            input
        };
        let expected = nested_layout_batch(wide_target, 1, None, nesting);
        let output = coerce(input, expected.schema().as_ref()).await;
        assert_eq!(output, expected);
    }

    #[cfg(feature = "remote")]
    fn remote_layout_table(
        schema: &Schema,
    ) -> (crate::Table, std::sync::mpsc::Receiver<reqwest::Body>) {
        let describe = serde_json::json!({
            "version": 1,
            "schema": lance::arrow::json::JsonSchema::try_from(schema).unwrap(),
        })
        .to_string();
        let (sender, receiver) = std::sync::mpsc::channel();
        let table = crate::Table::new_with_handler("layouts", move |mut request| {
            if request.url().path().ends_with("/describe/") {
                return http::Response::builder()
                    .status(200)
                    .body(describe.clone())
                    .unwrap();
            }
            assert!(
                request.url().path().ends_with("/insert/")
                    || request.url().path().ends_with("/merge_insert/")
            );
            sender.send(request.body_mut().take().unwrap()).unwrap();
            http::Response::builder()
                .status(200)
                .body(r#"{"version":2,"num_inserted_rows":1}"#.to_string())
                .unwrap()
        });
        (table, receiver)
    }

    #[cfg(feature = "remote")]
    #[rstest::rstest]
    #[case(&["list", "struct"])]
    #[case(&["list", "list", "struct"])]
    #[case(&["struct", "list", "struct"])]
    #[case(&["large_list", "struct"])]
    #[case(&["large_list", "struct", "list", "struct"])]
    #[tokio::test]
    async fn nested_blob_append_validates_siblings(
        #[case] nesting: &[&str],
        #[values(false, true)] wide_source: bool,
        #[values(false, true)] wide_target: bool,
        #[values(false, true)] remote: bool,
    ) {
        use crate::query::ExecutableQuery;
        use futures::StreamExt;
        use http_body::Body;

        let db = crate::connect("memory://").execute().await.unwrap();
        let initial = nested_layout_batch(wide_target, 0, None, nesting);
        let schema = initial.schema();
        let (table, receiver) = if remote {
            let (table, receiver) = remote_layout_table(schema.as_ref());
            (table, Some(receiver))
        } else {
            (
                db.create_table("no_caption", initial)
                    .execute()
                    .await
                    .unwrap(),
                None,
            )
        };
        let input =
            nested_layout_batch_with_options(wide_source, 1, None, nesting, None, &["keep me"]);
        let version = if remote {
            None
        } else {
            Some(table.version().await.unwrap())
        };
        let error = table
            .add(input.clone())
            .execute()
            .await
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("caption") && error.contains("does not exist"),
            "{error}"
        );
        if let Some(receiver) = receiver {
            assert!(
                receiver.try_recv().is_err(),
                "invalid append reached server"
            );
        } else {
            assert_eq!(table.version().await.unwrap(), version.unwrap());
            assert_eq!(table.count_rows(None).await.unwrap(), 1);
            assert_eq!(table.schema().await.unwrap(), schema);
        }

        // A declared sibling must survive normalization in every layout direction.
        let initial =
            nested_layout_batch_with_options(wide_target, 0, None, nesting, None, &["keep me"]);
        let schema = initial.schema();
        let (table, receiver) = if remote {
            let (table, receiver) = remote_layout_table(schema.as_ref());
            (table, Some(receiver))
        } else {
            (
                db.create_table("with_caption", initial)
                    .execute()
                    .await
                    .unwrap(),
                None,
            )
        };
        table.add(input).execute().await.unwrap();
        if let Some(receiver) = receiver {
            let mut body = receiver.recv().unwrap();
            let mut bytes = Vec::new();
            while let Some(frame) =
                futures::stream::poll_fn(|cx| std::pin::Pin::new(&mut body).poll_frame(cx))
                    .next()
                    .await
            {
                bytes.extend_from_slice(frame.unwrap().data_ref().unwrap());
            }
            let batches =
                arrow_ipc::reader::StreamReader::try_new(std::io::Cursor::new(bytes), None)
                    .unwrap()
                    .collect::<std::result::Result<Vec<_>, _>>()
                    .unwrap();
            assert_eq!(
                batches,
                vec![nested_layout_batch_with_options(
                    wide_target,
                    1,
                    None,
                    nesting,
                    None,
                    &["keep me"]
                )]
            );
        } else {
            assert_eq!(table.count_rows(None).await.unwrap(), 2);
            assert_eq!(table.schema().await.unwrap(), schema);
            let batches = table
                .query()
                .execute()
                .await
                .unwrap()
                .try_collect::<Vec<_>>()
                .await
                .unwrap();
            let mut found = false;
            for batch in batches {
                let ids = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                let Some(row) = (0..ids.len()).find(|row| ids.value(*row) == 1) else {
                    continue;
                };
                let mut array = batch.column(1).slice(row, 1);
                for kind in nesting {
                    array = match *kind {
                        "list" => array.as_any().downcast_ref::<ListArray>().unwrap().value(0),
                        "large_list" => array
                            .as_any()
                            .downcast_ref::<LargeListArray>()
                            .unwrap()
                            .value(0),
                        "struct" => {
                            let values = array.as_any().downcast_ref::<StructArray>().unwrap();
                            if let Some(caption) = values.column_by_name("caption") {
                                assert_eq!(
                                    caption
                                        .as_any()
                                        .downcast_ref::<StringArray>()
                                        .unwrap()
                                        .value(0),
                                    "keep me"
                                );
                                found = true;
                                break;
                            }
                            values.column(0).clone()
                        }
                        _ => unreachable!(),
                    };
                }
            }
            assert!(found, "declared caption was lost");
        }
    }

    #[cfg(feature = "remote")]
    #[rstest::rstest]
    #[case(&["list", "struct"])]
    #[case(&["list", "list", "struct"])]
    #[case(&["struct", "list", "struct"])]
    #[case(&["large_list", "struct"])]
    #[case(&["large_list", "struct", "list", "struct"])]
    #[tokio::test]
    async fn nested_blob_append_rejects_duplicate_siblings(
        #[case] nesting: &[&str],
        #[values(false, true)] wide_source: bool,
        #[values(false, true)] wide_target: bool,
        #[values(false, true)] remote: bool,
    ) {
        use crate::query::ExecutableQuery;

        let initial =
            nested_layout_batch_with_options(wide_target, 0, None, nesting, None, &["keep me"]);
        let schema = initial.schema();
        let db = crate::connect("memory://").execute().await.unwrap();
        let (table, receiver) = if remote {
            let (table, receiver) = remote_layout_table(schema.as_ref());
            (table, Some(receiver))
        } else {
            (
                db.create_table("captions", initial)
                    .execute()
                    .await
                    .unwrap(),
                None,
            )
        };
        let snapshot = if remote {
            None
        } else {
            Some((
                table.version().await.unwrap(),
                table
                    .query()
                    .execute()
                    .await
                    .unwrap()
                    .try_collect::<Vec<_>>()
                    .await
                    .unwrap(),
            ))
        };
        // Construct distinct arrays for the duplicate names: selecting only the
        // first matching field must not silently discard the second value.
        let input = nested_layout_batch_with_options(
            wide_source,
            1,
            None,
            nesting,
            None,
            &["first", "second"],
        );
        let error = table.add(input).execute().await.unwrap_err().to_string();
        assert!(
            error.contains("caption") && error.contains("specified more than once"),
            "{error}"
        );
        if let Some(receiver) = receiver {
            assert!(
                receiver.try_recv().is_err(),
                "ambiguous append reached server"
            );
        } else {
            let (version, before) = snapshot.unwrap();
            assert_eq!(table.version().await.unwrap(), version);
            assert_eq!(table.count_rows(None).await.unwrap(), 1);
            assert_eq!(table.schema().await.unwrap(), schema);
            let batches = table
                .query()
                .execute()
                .await
                .unwrap()
                .try_collect::<Vec<_>>()
                .await
                .unwrap();
            assert_eq!(batches, before);
        }
    }

    #[cfg(feature = "remote")]
    #[rstest::rstest]
    #[case(&["struct"])]
    #[case(&["list", "struct"])]
    #[case(&["struct", "list", "struct"])]
    #[case(&["large_list", "list"])]
    #[tokio::test]
    async fn nullable_nested_blob_layout_writes(
        #[case] nesting: &[&str],
        #[values(false, true)] merge: bool,
        #[values(false, true)] remote: bool,
    ) {
        use crate::query::ExecutableQuery;
        use futures::StreamExt;
        use http_body::Body;

        for depth in 0..nesting.len() {
            let initial = nested_layout_batch(false, 0, None, nesting);
            let schema = initial.schema();
            let db = crate::connect("memory://").execute().await.unwrap();
            let (table, receiver) = if remote {
                let (table, receiver) = remote_layout_table(schema.as_ref());
                (table, Some(receiver))
            } else {
                (
                    db.create_table("layouts", initial).execute().await.unwrap(),
                    None,
                )
            };
            let visible_id = if merge { 0 } else { 1 };
            let visible = nested_layout_batch(true, visible_id, None, nesting);
            let hidden = nested_layout_batch_with_null(true, 2, Some(0), nesting, Some(depth));
            let outside = nested_layout_batch(true, 99, Some(4), nesting);
            let input = arrow_select::concat::concat_batches(
                &visible.schema(),
                &[outside.clone(), visible, hidden, outside],
            )
            .unwrap()
            .slice(1, 2);
            let expected = arrow_select::concat::concat_batches(
                &schema,
                &[
                    nested_layout_batch(false, visible_id, None, nesting),
                    nested_layout_batch_with_null(false, 2, None, nesting, Some(depth)),
                ],
            )
            .unwrap();
            if merge {
                let mut builder = table.merge_insert(&["id"]);
                builder
                    .when_matched_update_all(None)
                    .when_not_matched_insert_all();
                let input_schema = input.schema();
                builder
                    .execute(Box::new(arrow_array::RecordBatchIterator::new(
                        vec![Ok(input)],
                        input_schema,
                    )))
                    .await
                    .unwrap();
            } else {
                table.add(input).execute().await.unwrap();
            }
            let count = if merge { 2 } else { 3 };
            if let Some(receiver) = &receiver {
                let mut body = receiver.recv().unwrap();
                let mut bytes = Vec::new();
                while let Some(frame) =
                    futures::stream::poll_fn(|cx| std::pin::Pin::new(&mut body).poll_frame(cx))
                        .next()
                        .await
                {
                    bytes.extend_from_slice(frame.unwrap().data_ref().unwrap());
                }
                let batches =
                    arrow_ipc::reader::StreamReader::try_new(std::io::Cursor::new(bytes), None)
                        .unwrap()
                        .collect::<std::result::Result<Vec<_>, _>>()
                        .unwrap();
                assert_eq!(batches, vec![expected]);
            } else {
                assert_eq!(table.schema().await.unwrap(), schema);
                assert_eq!(table.count_rows(None).await.unwrap(), count);
                let batches = table
                    .query()
                    .execute()
                    .await
                    .unwrap()
                    .try_collect::<Vec<_>>()
                    .await
                    .unwrap();
                let mut found = false;
                for batch in batches {
                    let ids = batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap();
                    if let Some(row) = (0..ids.len()).find(|row| ids.value(*row) == 2) {
                        let mut array = batch.column(1).slice(row, 1);
                        for kind in &nesting[..depth] {
                            array = match *kind {
                                "struct" => array
                                    .as_any()
                                    .downcast_ref::<StructArray>()
                                    .unwrap()
                                    .column(0)
                                    .clone(),
                                "list" => {
                                    array.as_any().downcast_ref::<ListArray>().unwrap().value(0)
                                }
                                "large_list" => array
                                    .as_any()
                                    .downcast_ref::<LargeListArray>()
                                    .unwrap()
                                    .value(0),
                                _ => unreachable!(),
                            };
                        }
                        assert!(array.is_null(0), "lost null ancestor at {depth}");
                        found = true;
                    }
                }
                assert!(found);
            }

            // A visible populated range in a later batch must still abort everything.
            let version = if remote {
                None
            } else {
                Some(table.version().await.unwrap())
            };
            let hidden = nested_layout_batch_with_null(true, 3, Some(0), nesting, Some(depth));
            let bad = nested_layout_batch(true, 4, Some(4), nesting);
            let reader: Box<dyn arrow_array::RecordBatchReader + Send> =
                Box::new(arrow_array::RecordBatchIterator::new(
                    vec![Ok(hidden), Ok(bad.clone())],
                    bad.schema(),
                ));
            let result = if merge {
                let mut builder = table.merge_insert(&["id"]);
                builder
                    .when_matched_update_all(None)
                    .when_not_matched_insert_all();
                builder.execute(reader).await.map(|_| ())
            } else {
                table.add(reader).execute().await.map(|_| ())
            };
            let error = result.unwrap_err().to_string();
            assert!(error.contains("cannot store byte ranges"), "{error}");
            if let Some(receiver) = receiver {
                assert!(
                    receiver.try_recv().is_err(),
                    "rejected write reached server"
                );
            } else {
                assert_eq!(table.version().await.unwrap(), version.unwrap());
                assert_eq!(table.count_rows(None).await.unwrap(), count);
            }
        }
    }

    #[cfg(feature = "remote")]
    #[rstest::rstest]
    #[case(&["list"])]
    #[case(&["list", "struct"])]
    #[case(&["list", "list"])]
    #[case(&["large_list", "struct", "list"])]
    #[case(&["struct", "list", "struct"])]
    #[tokio::test]
    async fn nested_blob_layout_writes(
        #[case] nesting: &[&str],
        #[values(false, true)] wide_target: bool,
        #[values(false, true)] merge: bool,
        #[values(false, true)] remote: bool,
    ) {
        use crate::query::ExecutableQuery;
        use futures::StreamExt;
        use http_body::Body;

        let initial = nested_layout_batch(wide_target, 0, None, nesting);
        let schema = initial.schema();
        let db = crate::connect("memory://").execute().await.unwrap();
        let (table, receiver) = if remote {
            let (table, receiver) = remote_layout_table(schema.as_ref());
            (table, Some(receiver))
        } else {
            (
                db.create_table("layouts", initial).execute().await.unwrap(),
                None,
            )
        };
        let input = nested_layout_batch(!wide_target, 1, None, nesting);
        let expected = nested_layout_batch(wide_target, 1, None, nesting);
        let (input, expected) = if merge {
            (
                arrow_select::concat::concat_batches(
                    &input.schema(),
                    &[nested_layout_batch(!wide_target, 0, None, nesting), input],
                )
                .unwrap(),
                arrow_select::concat::concat_batches(
                    &schema,
                    &[nested_layout_batch(wide_target, 0, None, nesting), expected],
                )
                .unwrap(),
            )
        } else {
            (input, expected)
        };
        if merge {
            let mut builder = table.merge_insert(&["id"]);
            builder
                .when_matched_update_all(None)
                .when_not_matched_insert_all();
            let schema = input.schema();
            builder
                .execute(Box::new(arrow_array::RecordBatchIterator::new(
                    vec![Ok(input)],
                    schema,
                )))
                .await
                .unwrap();
        } else {
            table.add(input).execute().await.unwrap();
        }
        if let Some(receiver) = &receiver {
            let mut body = receiver.recv().unwrap();
            let mut bytes = Vec::new();
            while let Some(frame) =
                futures::stream::poll_fn(|cx| std::pin::Pin::new(&mut body).poll_frame(cx))
                    .next()
                    .await
            {
                bytes.extend_from_slice(frame.unwrap().data_ref().unwrap());
            }
            let batches =
                arrow_ipc::reader::StreamReader::try_new(std::io::Cursor::new(bytes), None)
                    .unwrap()
                    .collect::<std::result::Result<Vec<_>, _>>()
                    .unwrap();
            assert_eq!(batches, vec![expected]);
        } else {
            assert_eq!(table.schema().await.unwrap(), schema);
            assert_eq!(table.count_rows(None).await.unwrap(), 2);
            // Read the stored nested descriptors as well as the row count.
            table
                .query()
                .execute()
                .await
                .unwrap()
                .try_collect::<Vec<_>>()
                .await
                .unwrap();
        }
        if !wide_target {
            let version = if remote {
                None
            } else {
                Some(table.version().await.unwrap())
            };
            let first = nested_layout_batch(true, 2, None, nesting);
            let bad = nested_layout_batch(true, 3, Some(4), nesting);
            let reader: Box<dyn arrow_array::RecordBatchReader + Send> =
                Box::new(arrow_array::RecordBatchIterator::new(
                    vec![Ok(first), Ok(bad.clone())],
                    bad.schema(),
                ));
            let result = if merge {
                let mut builder = table.merge_insert(&["id"]);
                builder
                    .when_matched_update_all(None)
                    .when_not_matched_insert_all();
                builder.execute(reader).await.map(|_| ())
            } else {
                table.add(reader).execute().await.map(|_| ())
            };
            let error = result.unwrap_err().to_string();
            assert!(error.contains("cannot store byte ranges"), "{error}");
            if let Some(receiver) = receiver {
                assert!(
                    receiver.try_recv().is_err(),
                    "a rejected range must not reach the server"
                );
            } else {
                assert_eq!(table.version().await.unwrap(), version.unwrap());
                assert_eq!(table.count_rows(None).await.unwrap(), 2);
            }
        }
    }
}
