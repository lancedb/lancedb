// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Schema inference and transformation utilities.

use anyhow::{Context, Result};
use arrow_array::{Array, ArrayRef, FixedSizeListArray, ListArray, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::info;

/// Information about detected vector columns that need transformation.
#[derive(Debug, Clone)]
pub struct VectorColumnInfo {
    pub name: String,
    pub dims: i32,
    pub inner_type: DataType,
}

/// Detect all List columns with float elements that look like vectors.
/// Returns a map of column name -> dimensions.
pub fn detect_vector_columns(batch: &RecordBatch) -> HashMap<String, VectorColumnInfo> {
    let mut vector_columns = HashMap::new();

    for (idx, field) in batch.schema().fields().iter().enumerate() {
        if let Some(info) = detect_vector_column_info(field, batch.column(idx)) {
            info!(
                "Auto-detected vector column '{}': {} dimensions, {:?}",
                info.name, info.dims, info.inner_type
            );
            vector_columns.insert(info.name.clone(), info);
        }
    }

    vector_columns
}

/// Detect vector columns using only the schema. Picks up `FixedSizeList<Float>`
/// columns (dims live in the data type) but skips variable-length `List<Float>`
/// columns since their dims aren't in the schema.
pub fn detect_vector_columns_from_schema(schema: &Schema) -> HashMap<String, VectorColumnInfo> {
    let mut vector_columns = HashMap::new();

    for field in schema.fields().iter() {
        if let DataType::FixedSizeList(inner, dims) = field.data_type() {
            if matches!(
                inner.data_type(),
                DataType::Float16 | DataType::Float32 | DataType::Float64
            ) {
                let info = VectorColumnInfo {
                    name: field.name().clone(),
                    dims: *dims,
                    inner_type: inner.data_type().clone(),
                };
                info!(
                    "Auto-detected vector column '{}' from schema: {} dimensions, {:?}",
                    info.name, info.dims, info.inner_type
                );
                vector_columns.insert(info.name.clone(), info);
            }
        }
    }

    vector_columns
}

/// Check if a field/column looks like a vector column.
fn detect_vector_column_info(field: &Field, column: &ArrayRef) -> Option<VectorColumnInfo> {
    match field.data_type() {
        DataType::List(inner) | DataType::LargeList(inner) => {
            // Check if inner type is float
            match inner.data_type() {
                DataType::Float16 | DataType::Float32 | DataType::Float64 => {
                    // Get dimensions from first non-null element
                    if let Some(list_array) = column.as_any().downcast_ref::<ListArray>() {
                        for i in 0..list_array.len() {
                            if !list_array.is_null(i) {
                                let dims = list_array.value(i).len() as i32;
                                if dims > 0 {
                                    return Some(VectorColumnInfo {
                                        name: field.name().clone(),
                                        dims,
                                        inner_type: inner.data_type().clone(),
                                    });
                                }
                            }
                        }
                    }
                }
                _ => {}
            }
        }
        DataType::FixedSizeList(inner, dims) => {
            // Already a FixedSizeList with float - still a vector column
            match inner.data_type() {
                DataType::Float16 | DataType::Float32 | DataType::Float64 => {
                    return Some(VectorColumnInfo {
                        name: field.name().clone(),
                        dims: *dims,
                        inner_type: inner.data_type().clone(),
                    });
                }
                _ => {}
            }
        }
        _ => {}
    }
    None
}

/// Transform a schema to convert List columns to FixedSizeList for all detected vector columns.
pub fn transform_schema(
    schema: &Schema,
    vector_columns: &HashMap<String, VectorColumnInfo>,
) -> Result<SchemaRef> {
    let fields: Vec<Field> = schema
        .fields()
        .iter()
        .map(|field| {
            if let Some(info) = vector_columns.get(field.name()) {
                // Convert to FixedSizeList
                let inner_field = Arc::new(Field::new("item", info.inner_type.clone(), true));
                Field::new(
                    field.name(),
                    DataType::FixedSizeList(inner_field, info.dims),
                    field.is_nullable(),
                )
            } else {
                field.as_ref().clone()
            }
        })
        .collect();

    Ok(Arc::new(Schema::new(fields)))
}

/// Transform a record batch to convert List columns to FixedSizeList for all vector columns.
pub fn transform_batch(
    batch: &RecordBatch,
    target_schema: &SchemaRef,
    vector_columns: &HashMap<String, VectorColumnInfo>,
) -> Result<RecordBatch> {
    let columns: Vec<ArrayRef> = batch
        .schema()
        .fields()
        .iter()
        .zip(batch.columns())
        .map(|(field, col)| {
            if vector_columns.contains_key(field.name()) {
                convert_to_fixed_size_list(col, target_schema, field.name())
                    .unwrap_or_else(|_| col.clone())
            } else {
                col.clone()
            }
        })
        .collect();

    RecordBatch::try_new(target_schema.clone(), columns)
        .context("Failed to create transformed batch")
}

/// Convert a List array to a FixedSizeList array.
fn convert_to_fixed_size_list(
    array: &ArrayRef,
    target_schema: &SchemaRef,
    column_name: &str,
) -> Result<ArrayRef> {
    let target_field = target_schema
        .field_with_name(column_name)
        .context("Column not found in target schema")?;

    let dims = match target_field.data_type() {
        DataType::FixedSizeList(_, d) => *d,
        _ => return Ok(array.clone()), // Not a FixedSizeList target, return as-is
    };

    match array.data_type() {
        DataType::List(_) => {
            let list_array = array
                .as_any()
                .downcast_ref::<ListArray>()
                .ok_or_else(|| anyhow::anyhow!("Failed to downcast to ListArray"))?;

            // Flatten all values and create FixedSizeList
            let values = list_array.values();

            // Create inner field matching the actual data type
            let (inner_field, float_values): (Arc<Field>, ArrayRef) = match values.data_type() {
                DataType::Float16 => (
                    Arc::new(Field::new("item", DataType::Float16, true)),
                    values.clone(),
                ),
                DataType::Float32 => (
                    Arc::new(Field::new("item", DataType::Float32, true)),
                    values.clone(),
                ),
                DataType::Float64 => (
                    Arc::new(Field::new("item", DataType::Float64, true)),
                    values.clone(),
                ),
                other => {
                    anyhow::bail!("Unsupported vector element type: {:?}", other);
                }
            };

            let fsl = FixedSizeListArray::new(inner_field, dims, float_values, None);
            Ok(Arc::new(fsl))
        }
        DataType::FixedSizeList(_, _) => {
            // Already a FixedSizeList, return as-is
            Ok(array.clone())
        }
        other => {
            anyhow::bail!(
                "Cannot convert {:?} to FixedSizeList for column '{}'",
                other,
                column_name
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Float16Array, Float32Array, Float64Array, Int64Array, StringArray};
    use half::f16;

    struct VectorColumnTestCase {
        name: &'static str,
        inner_type: DataType,
        values: ArrayRef,
        expected_detected: bool,
    }

    fn make_list_array(inner_type: DataType, values: ArrayRef) -> ListArray {
        let offsets = arrow_buffer::OffsetBuffer::from_lengths([3, 3]);
        let field = Arc::new(Field::new("item", inner_type, true));
        ListArray::new(field, offsets, values, None)
    }

    #[test]
    fn test_detect_vector_columns() {
        let test_cases = vec![
            VectorColumnTestCase {
                name: "Float16 vector",
                inner_type: DataType::Float16,
                values: Arc::new(Float16Array::from(vec![
                    f16::from_f32(1.0),
                    f16::from_f32(2.0),
                    f16::from_f32(3.0),
                    f16::from_f32(4.0),
                    f16::from_f32(5.0),
                    f16::from_f32(6.0),
                ])),
                expected_detected: true,
            },
            VectorColumnTestCase {
                name: "Float32 vector",
                inner_type: DataType::Float32,
                values: Arc::new(Float32Array::from(vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0])),
                expected_detected: true,
            },
            VectorColumnTestCase {
                name: "Float64 vector",
                inner_type: DataType::Float64,
                values: Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0])),
                expected_detected: true,
            },
            VectorColumnTestCase {
                name: "String list (not a vector)",
                inner_type: DataType::Utf8,
                values: Arc::new(StringArray::from(vec!["a", "b", "c", "d", "e", "f"])),
                expected_detected: false,
            },
            VectorColumnTestCase {
                name: "Int64 list (not a vector)",
                inner_type: DataType::Int64,
                values: Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5, 6])),
                expected_detected: false,
            },
        ];

        for case in test_cases {
            let list_array = make_list_array(case.inner_type.clone(), case.values);

            let schema = Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new(
                    "vector",
                    DataType::List(Arc::new(Field::new("item", case.inner_type.clone(), true))),
                    false,
                ),
            ]));

            let batch = RecordBatch::try_new(
                schema,
                vec![Arc::new(Int64Array::from(vec![1, 2])), Arc::new(list_array)],
            )
            .unwrap();

            let vector_columns = detect_vector_columns(&batch);

            if case.expected_detected {
                assert_eq!(
                    vector_columns.len(),
                    1,
                    "{}: expected 1 vector column",
                    case.name
                );
                assert!(
                    vector_columns.contains_key("vector"),
                    "{}: expected 'vector' column",
                    case.name
                );
                assert_eq!(
                    vector_columns["vector"].dims, 3,
                    "{}: expected 3 dimensions",
                    case.name
                );
                assert_eq!(
                    vector_columns["vector"].inner_type, case.inner_type,
                    "{}: inner type mismatch",
                    case.name
                );
            } else {
                assert!(
                    vector_columns.is_empty(),
                    "{}: expected no vector columns detected",
                    case.name
                );
            }
        }
    }
}
