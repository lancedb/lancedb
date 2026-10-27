// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Apply Blob v2 struct validity before DataFusion extracts descriptor fields.

use std::fmt;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use arrow_array::{
    Array, ArrayRef, FixedSizeListArray, LargeListArray, ListArray, RecordBatch, StructArray,
    cast::AsArray, make_array,
};
use arrow_buffer::NullBuffer;
use arrow_schema::{DataType, Field, FieldRef, Schema};
use datafusion_common::{
    Result,
    tree_node::{TreeNode, TreeNodeRecursion},
};
use datafusion_expr::{ColumnarValue, Expr};
use datafusion_functions::core::getfield::GetFieldFunc;
use datafusion_physical_expr::{PhysicalExpr, expressions::Column};
use datafusion_physical_plan::{ExecutionPlan, projection::ProjectionExec};
use lance_arrow::FieldExt;
use lance_core::datatypes::BlobV2Layout;

pub(super) fn is_descriptor(field: &Field) -> bool {
    field.is_blob()
        && matches!(field.data_type(), DataType::Struct(fields)
            if BlobV2Layout::classify(fields) == Some(BlobV2Layout::Descriptor))
}

pub(super) fn has_descriptor(field: &Field) -> bool {
    if is_descriptor(field) {
        return true;
    }
    match field.data_type() {
        DataType::Struct(fields) => fields.iter().any(|field| has_descriptor(field)),
        DataType::List(item) | DataType::LargeList(item) | DataType::FixedSizeList(item, _) => {
            has_descriptor(item)
        }
        _ => false,
    }
}

/// Resolve static struct accesses without confusing a scalar sibling with the
/// entire root column. Track descriptor ancestors too: `blob.size` is scalar,
/// but still requires the descriptor validity mask before evaluation.
fn referenced_field<'a>(expr: &Expr, schema: &'a Schema) -> Option<(&'a Field, bool)> {
    match expr {
        Expr::Column(column) => Some((schema.field_with_name(&column.name).ok()?, false)),
        Expr::ScalarFunction(call)
            if call.func.inner().downcast_ref::<GetFieldFunc>().is_some() =>
        {
            let (base, path) = call.args.split_first()?;
            let (mut field, mut within_descriptor) = referenced_field(base, schema)?;
            for name in path {
                let Expr::Literal(name, _) = name else {
                    return None;
                };
                within_descriptor |= is_descriptor(field);
                let DataType::Struct(fields) = field.data_type() else {
                    return None;
                };
                field = fields.find(name.try_as_str().flatten()?)?.1;
            }
            Some((field, within_descriptor))
        }
        _ => None,
    }
}

pub(super) fn references_descriptor(expr: &Expr, schema: &Schema) -> Result<bool> {
    let mut found = false;
    expr.apply(|expr| {
        if let Some((field, within_descriptor)) = referenced_field(expr, schema) {
            if within_descriptor || has_descriptor(field) {
                found = true;
                return Ok(TreeNodeRecursion::Stop);
            }
            // The complete path is safe. Do not revisit its root as a whole
            // struct, which could contain an unrelated blob sibling.
            return Ok(TreeNodeRecursion::Jump);
        }
        // Unknown/computed paths conservatively visit their arguments.
        Ok(TreeNodeRecursion::Continue)
    })?;
    Ok(found)
}

fn nullable_descriptor_fields(field: &FieldRef) -> FieldRef {
    let data_type = match field.data_type() {
        DataType::Struct(fields) if is_descriptor(field) => DataType::Struct(
            fields
                .iter()
                .map(|field| Arc::new(field.as_ref().clone().with_nullable(true)))
                .collect(),
        ),
        DataType::Struct(fields) => {
            DataType::Struct(fields.iter().map(nullable_descriptor_fields).collect())
        }
        DataType::List(item) => DataType::List(nullable_descriptor_fields(item)),
        DataType::LargeList(item) => DataType::LargeList(nullable_descriptor_fields(item)),
        DataType::FixedSizeList(item, size) => {
            DataType::FixedSizeList(nullable_descriptor_fields(item), *size)
        }
        _ => return field.clone(),
    };
    Arc::new(field.as_ref().clone().with_data_type(data_type))
}

pub(super) fn read_schema(schema: &Schema) -> Schema {
    Schema::new_with_metadata(
        schema
            .fields()
            .iter()
            .map(nullable_descriptor_fields)
            .collect::<Vec<_>>(),
        schema.metadata().clone(),
    )
}

/// Preserve value buffers, offsets, metadata, and existing child nulls. Only
/// validity buffers are combined; this never materializes blob payloads.
fn mask_array(array: &ArrayRef, field: &Field, ancestors: Option<&NullBuffer>) -> Result<ArrayRef> {
    if !has_descriptor(field) {
        return Ok(array.clone());
    }
    match field.data_type() {
        DataType::Struct(fields) => {
            let values = array.as_struct();
            let nulls = NullBuffer::union(values.nulls(), ancestors);
            let columns = if is_descriptor(field) {
                values
                    .columns()
                    .iter()
                    .map(|column| {
                        let nulls = NullBuffer::union(column.nulls(), nulls.as_ref());
                        Ok(make_array(
                            column.to_data().into_builder().nulls(nulls).build()?,
                        ))
                    })
                    .collect::<Result<Vec<_>>>()?
            } else {
                values
                    .columns()
                    .iter()
                    .zip(fields)
                    .map(|(column, field)| mask_array(column, field, nulls.as_ref()))
                    .collect::<Result<Vec<_>>>()?
            };
            Ok(Arc::new(StructArray::try_new_with_length(
                fields.clone(),
                columns,
                nulls,
                values.len(),
            )?))
        }
        DataType::List(item) => {
            let list = array.as_list::<i32>();
            Ok(Arc::new(ListArray::try_new(
                item.clone(),
                list.offsets().clone(),
                mask_array(list.values(), item, None)?,
                list.nulls().cloned(),
            )?))
        }
        DataType::LargeList(item) => {
            let list = array.as_list::<i64>();
            Ok(Arc::new(LargeListArray::try_new(
                item.clone(),
                list.offsets().clone(),
                mask_array(list.values(), item, None)?,
                list.nulls().cloned(),
            )?))
        }
        DataType::FixedSizeList(item, size) => {
            let list = array.as_fixed_size_list();
            Ok(Arc::new(FixedSizeListArray::try_new(
                item.clone(),
                *size,
                mask_array(list.values(), item, None)?,
                list.nulls().cloned(),
            )?))
        }
        _ => Ok(array.clone()),
    }
}

#[derive(Debug)]
struct MaskBlobNulls {
    source: Arc<dyn PhysicalExpr>,
    field: FieldRef,
}

impl fmt::Display for MaskBlobNulls {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "mask_blob_nulls({})", self.source)
    }
}

impl PartialEq for MaskBlobNulls {
    fn eq(&self, other: &Self) -> bool {
        self.source.eq(&other.source) && self.field == other.field
    }
}

impl Eq for MaskBlobNulls {}

impl Hash for MaskBlobNulls {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.source.hash(state);
        self.field.hash(state);
    }
}

impl PhysicalExpr for MaskBlobNulls {
    fn return_field(&self, _input_schema: &Schema) -> Result<FieldRef> {
        Ok(self.field.clone())
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        let source = self.source.evaluate(batch)?.into_array(batch.num_rows())?;
        Ok(ColumnarValue::Array(mask_array(
            &source,
            &self.field,
            None,
        )?))
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.source]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        assert_eq!(children.len(), 1);
        Ok(Arc::new(Self {
            source: children[0].clone(),
            field: self.field.clone(),
        }))
    }

    fn fmt_sql(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self}")
    }
}

pub(super) fn mask_plan(input: Arc<dyn ExecutionPlan>) -> Result<Arc<dyn ExecutionPlan>> {
    let schema = input.schema();
    if !schema.fields().iter().any(|field| has_descriptor(field)) {
        return Ok(input);
    }
    let expressions = schema
        .fields()
        .iter()
        .enumerate()
        .map(|(index, field)| {
            let source: Arc<dyn PhysicalExpr> = Arc::new(Column::new(field.name(), index));
            let expression: Arc<dyn PhysicalExpr> = if has_descriptor(field) {
                Arc::new(MaskBlobNulls {
                    source,
                    field: nullable_descriptor_fields(field),
                })
            } else {
                source
            };
            (expression, field.name().clone())
        })
        .collect::<Vec<_>>();
    Ok(Arc::new(ProjectionExec::try_new(expressions, input)?))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{StringArray, UInt8Array, UInt32Array, UInt64Array};
    use arrow_buffer::OffsetBuffer;
    use lance_core::datatypes::BLOB_V2_DESC_FIELD;

    fn descriptor() -> (FieldRef, ArrayRef) {
        let field = Arc::new(BLOB_V2_DESC_FIELD.clone().with_nullable(true));
        let field = nullable_descriptor_fields(&field);
        let DataType::Struct(fields) = field.data_type() else {
            unreachable!()
        };
        let array = StructArray::new(
            fields.clone(),
            vec![
                Arc::new(UInt8Array::from(vec![0; 5])),
                Arc::new(UInt64Array::from(vec![10; 5])),
                Arc::new(UInt64Array::from(vec![
                    Some(9),
                    Some(5),
                    Some(0),
                    Some(0),
                    None,
                ])),
                Arc::new(UInt32Array::from(vec![0; 5])),
                Arc::new(StringArray::from(vec![""; 5])),
            ],
            Some(NullBuffer::from(vec![true, true, true, false, true])),
        );
        (field, Arc::new(array))
    }

    #[test]
    fn test_sliced_descriptor_preserves_child_nulls_and_buffers() {
        let (field, array) = descriptor();
        let array = array.slice(1, 4);
        let masked = mask_array(&array, &field, None).unwrap();
        let masked = masked.as_struct();
        let size = masked
            .column_by_name("size")
            .unwrap()
            .as_primitive::<arrow_array::types::UInt64Type>();
        assert_eq!(size, &UInt64Array::from(vec![Some(5), Some(0), None, None]));
        assert_eq!(masked.nulls(), array.nulls());
        for (input, output) in array.as_struct().columns().iter().zip(masked.columns()) {
            assert_eq!(input.to_data().buffers(), output.to_data().buffers());
            assert!(output.is_null(2));
        }

        let empty = mask_array(&array.slice(0, 0), &field, None).unwrap();
        assert_eq!(empty.len(), 0);
    }

    #[test]
    fn test_nested_descriptor_inherits_struct_nulls() {
        let (blob_field, array) = descriptor();
        let parent = Arc::new(Field::new(
            "parent",
            DataType::Struct(vec![blob_field.clone()].into()),
            true,
        ));
        let array: ArrayRef = Arc::new(StructArray::new(
            vec![blob_field].into(),
            vec![array],
            Some(NullBuffer::from(vec![true, false, true, true, true])),
        ));
        let array = array.slice(1, 3);
        let masked = mask_array(&array, &parent, None).unwrap();
        let blob = masked.as_struct().column(0).as_struct();
        assert_eq!(
            blob.nulls().unwrap(),
            &NullBuffer::from(vec![false, true, false])
        );
        for column in blob.columns() {
            assert!(column.is_null(0));
            assert!(!column.is_null(1));
            assert!(column.is_null(2));
        }
    }

    #[test]
    fn test_list_descriptor_masks_preserve_offsets_and_validity() {
        let (item, values) = descriptor();
        let nulls = Some(NullBuffer::from(vec![true, false, true]));
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(ListArray::new(
                item.clone(),
                OffsetBuffer::new(vec![0, 1, 3, 5].into()),
                values.clone(),
                nulls.clone(),
            )),
            Arc::new(LargeListArray::new(
                item.clone(),
                OffsetBuffer::new(vec![0_i64, 1, 3, 5].into()),
                values.clone(),
                nulls,
            )),
            Arc::new(FixedSizeListArray::new(item.clone(), 1, values, None)),
        ];
        for array in arrays {
            let field = Field::new("blobs", array.data_type().clone(), true);
            let masked = mask_array(&array, &field, None).unwrap();
            assert_eq!(masked.nulls(), array.nulls());
            assert_eq!(masked.to_data().buffers(), array.to_data().buffers());
            let values = match masked.data_type() {
                DataType::List(_) => masked.as_list::<i32>().values(),
                DataType::LargeList(_) => masked.as_list::<i64>().values(),
                DataType::FixedSizeList(_, _) => masked.as_fixed_size_list().values(),
                _ => unreachable!(),
            };
            for column in values.as_struct().columns() {
                assert!(column.is_null(3));
            }
            assert_eq!(
                mask_array(&array.slice(1, 1), &field, None).unwrap().len(),
                1
            );
        }
    }
}
