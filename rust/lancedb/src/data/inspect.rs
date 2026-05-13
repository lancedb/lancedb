// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use std::{collections::HashMap, sync::Arc};

use arrow::compute::kernels::{aggregate::bool_and, length::length};
use arrow_array::{
    Array, ArrayRef, GenericListArray, OffsetSizeTrait, PrimitiveArray, RecordBatch,
    RecordBatchReader,
    cast::AsArray,
    types::{ArrowPrimitiveType, Int32Type, Int64Type},
};
use arrow_cast::cast;
use arrow_ord::cmp::eq;
use arrow_schema::{DataType, Field, Schema};
use num_traits::{ToPrimitive, Zero};

use crate::error::{Error, Result};

pub(crate) fn infer_dimension<T: ArrowPrimitiveType>(
    list_arr: &GenericListArray<T::Native>,
) -> Result<Option<T::Native>>
where
    T::Native: OffsetSizeTrait + ToPrimitive,
{
    let len_arr = length(list_arr)?;
    if len_arr.is_empty() {
        return Ok(Some(Zero::zero()));
    }

    let dim = len_arr.as_primitive::<T>().value(0);
    let datum = PrimitiveArray::<T>::new_scalar(dim);
    if bool_and(&eq(len_arr.as_primitive::<T>(), &datum)?) != Some(true) {
        Ok(None)
    } else {
        Ok(Some(dim))
    }
}

/// Given a batch, return a schema where variable-length list columns whose names
/// contain "vector" or "embedding" (case-insensitive substring) are converted to
/// FixedSizeList, if a uniform dimension can be determined from the batch.
///
/// - Float list columns → `FixedSizeList(Float32, dim)`. Float32 is the canonical
///   element type for vector indices in Lance.
/// - Integer list columns → `FixedSizeList(UInt8, dim)` if all values fit in [0, 255]
///   (i.e. plausible binary vectors), otherwise `FixedSizeList(Float32, dim)`.
///   The Float32 fallback exists because users commonly write `[1, 2, 3]` in Python
///   meaning floats; preserving Int32/Int64 here would surprise them later when a
///   vector index requires a float element type.
///
/// Note: this is the substring match used by Python's `_name_suggests_vector_column`
/// on the no-target-schema (inference) path. The on-bad-vectors sanitization path
/// in Python uses a stricter exact-match rule; that path is not represented here.
///
/// Columns that don't match the name heuristic, have non-uniform lengths, or have
/// non-numeric value types are left unchanged.
pub fn infer_fsl_schema(batch: &RecordBatch) -> Arc<Schema> {
    let schema = batch.schema();
    let mut new_fields: Vec<Arc<Field>> = schema.fields().iter().cloned().collect();
    let mut changed = false;

    for (i, field) in schema.fields().iter().enumerate() {
        let name_lower = field.name().to_lowercase();
        if !name_lower.contains("vector") && !name_lower.contains("embedding") {
            continue;
        }

        let col = batch.column(i);

        let (value_dtype, dim) = match field.data_type() {
            DataType::List(value_field)
                if value_field.data_type().is_floating()
                    || value_field.data_type().is_integer() =>
            {
                let dim = infer_dimension::<Int32Type>(col.as_list::<i32>())
                    .ok()
                    .flatten()
                    .map(|d| d as i64);
                (value_field.data_type().clone(), dim)
            }
            DataType::LargeList(value_field)
                if value_field.data_type().is_floating()
                    || value_field.data_type().is_integer() =>
            {
                let dim = infer_dimension::<Int64Type>(col.as_list::<i64>())
                    .ok()
                    .flatten();
                (value_field.data_type().clone(), dim)
            }
            _ => continue,
        };

        let Some(dim) = dim else { continue };
        if dim == 0 {
            continue;
        }

        let target_element_type = if value_dtype.is_floating() {
            DataType::Float32
        } else {
            let values_arr = match field.data_type() {
                DataType::List(_) => col.as_list::<i32>().values().clone(),
                DataType::LargeList(_) => col.as_list::<i64>().values().clone(),
                _ => unreachable!(),
            };
            infer_integer_element_type(&values_arr)
        };

        let new_value_field = Arc::new(Field::new("item", target_element_type, true));
        let new_type = DataType::FixedSizeList(new_value_field, dim as i32);
        new_fields[i] = Arc::new(field.as_ref().clone().with_data_type(new_type));
        changed = true;
    }

    if changed {
        Arc::new(Schema::new_with_metadata(
            new_fields,
            schema.metadata().clone(),
        ))
    } else {
        schema
    }
}

/// Determine whether integer list values should be stored as UInt8 or Float32.
///
/// Returns UInt8 if all non-null values are in [0, 255]; otherwise Float32.
fn infer_integer_element_type(values: &ArrayRef) -> DataType {
    let Ok(int64_arr) = cast(values.as_ref(), &DataType::Int64) else {
        return DataType::Float32;
    };
    let int64_arr = int64_arr.as_primitive::<Int64Type>();

    let mut any_valid = false;
    let mut min_val = i64::MAX;
    let mut max_val = i64::MIN;

    for v in int64_arr.iter().flatten() {
        any_valid = true;
        if v < min_val {
            min_val = v;
        }
        if v > max_val {
            max_val = v;
        }
    }

    if !any_valid || (min_val >= 0 && max_val <= 255) {
        DataType::UInt8
    } else {
        DataType::Float32
    }
}

/// Infer the vector columns from a dataset.
///
/// Parameters
/// ----------
/// - reader: RecordBatchReader
/// - strict: if set true, only `fixed_size_list<float>` is considered as vector column. If set to false,
///   a `list<float>` column with same length is also considered as vector column.
pub fn infer_vector_columns(
    reader: impl RecordBatchReader + Send,
    strict: bool,
) -> Result<Vec<String>> {
    let mut columns = vec![];

    let mut columns_to_infer: HashMap<String, Option<i64>> = HashMap::new();
    for field in reader.schema().fields() {
        match field.data_type() {
            DataType::FixedSizeList(sub_field, _) if sub_field.data_type().is_floating() => {
                columns.push(field.name().clone());
            }
            DataType::List(sub_field) if sub_field.data_type().is_floating() && !strict => {
                columns_to_infer.insert(field.name().clone(), None);
            }
            DataType::LargeList(sub_field) if sub_field.data_type().is_floating() && !strict => {
                columns_to_infer.insert(field.name().clone(), None);
            }
            _ => {}
        }
    }
    for batch in reader {
        let batch = batch?;
        let col_names = columns_to_infer.keys().cloned().collect::<Vec<_>>();
        for col_name in col_names {
            let col = batch.column_by_name(&col_name).ok_or(Error::Schema {
                message: format!("Column {} not found", col_name),
            })?;
            if let Some(dim) = match *col.data_type() {
                DataType::List(_) => {
                    infer_dimension::<Int32Type>(col.as_list::<i32>())?.map(|d| d as i64)
                }
                DataType::LargeList(_) => infer_dimension::<Int64Type>(col.as_list::<i64>())?,
                _ => {
                    return Err(Error::Schema {
                        message: format!("Column {} is not a list", col_name),
                    });
                }
            } {
                if let Some(Some(prev_dim)) = columns_to_infer.get(&col_name) {
                    if prev_dim != &dim {
                        columns_to_infer.remove(&col_name);
                    }
                } else {
                    columns_to_infer.insert(col_name, Some(dim));
                }
            } else {
                columns_to_infer.remove(&col_name);
            }
        }
    }
    columns.extend(columns_to_infer.keys().cloned());
    Ok(columns)
}

#[cfg(test)]
mod tests {
    use super::*;

    use arrow_array::{
        FixedSizeListArray, Float32Array, Int32Array, ListArray, RecordBatch, RecordBatchIterator,
        StringArray,
        types::{Float32Type, Float64Type, Int32Type as Int32ArrayType},
    };
    use arrow_schema::{DataType, Field, Schema};
    use std::{sync::Arc, vec};

    #[test]
    fn test_infer_fsl_schema_float_list() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new(
                "embedding",
                DataType::List(Arc::new(Field::new("item", DataType::Float32, true))),
                true,
            ),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(ListArray::from_iter_primitive::<Float32Type, _, _>(
                    (0..3).map(|_| Some(vec![Some(1.0f32), Some(2.0), Some(3.0), Some(4.0)])),
                )),
            ],
        )
        .unwrap();

        let inferred = infer_fsl_schema(&batch);

        let expected = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new(
                "embedding",
                DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), 4),
                true,
            ),
        ]));
        assert_eq!(inferred, expected);
    }

    #[test]
    fn test_infer_fsl_schema_int_list_uint8() {
        // List<Int32> named "vector" with values in [0, 255] → FSL<UInt8, dim>
        let schema = Arc::new(Schema::new(vec![Field::new(
            "vector",
            DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
            true,
        )]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(ListArray::from_iter_primitive::<
                Int32ArrayType,
                _,
                _,
            >(
                (0..3).map(|_| Some(vec![Some(0i32), Some(100), Some(255)])),
            ))],
        )
        .unwrap();

        let inferred = infer_fsl_schema(&batch);

        let expected = Arc::new(Schema::new(vec![Field::new(
            "vector",
            DataType::FixedSizeList(Arc::new(Field::new("item", DataType::UInt8, true)), 3),
            true,
        )]));
        assert_eq!(inferred, expected);
    }

    #[test]
    fn test_infer_fsl_schema_int_list_float32() {
        // List<Int32> named "vector" with values outside [0, 255] → FSL<Float32, dim>
        let schema = Arc::new(Schema::new(vec![Field::new(
            "vector",
            DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
            true,
        )]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(ListArray::from_iter_primitive::<
                Int32ArrayType,
                _,
                _,
            >(
                (0..3).map(|_| Some(vec![Some(-1i32), Some(100), Some(200)])),
            ))],
        )
        .unwrap();

        let inferred = infer_fsl_schema(&batch);

        let expected = Arc::new(Schema::new(vec![Field::new(
            "vector",
            DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), 3),
            true,
        )]));
        assert_eq!(inferred, expected);
    }

    #[test]
    fn test_infer_fsl_schema_int_list_value_over_255_is_float32() {
        // Mirrors Python's `test_create_table_infers_large_int_vectors`: an
        // integer value exceeding 255 forces the inferred element type to Float32.
        let schema = Arc::new(Schema::new(vec![Field::new(
            "vector",
            DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
            true,
        )]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(ListArray::from_iter_primitive::<
                Int32ArrayType,
                _,
                _,
            >(vec![Some(vec![
                Some(0i32),
                Some(300),
            ])]))],
        )
        .unwrap();

        let inferred = infer_fsl_schema(&batch);
        let f = inferred.field_with_name("vector").unwrap();
        assert!(
            matches!(
                f.data_type(),
                DataType::FixedSizeList(inner, 2) if inner.data_type() == &DataType::Float32
            ),
            "expected FixedSizeList(Float32, 2), got {:?}",
            f.data_type()
        );
    }

    #[test]
    fn test_infer_fsl_schema_non_uniform() {
        // List with varying lengths named "embedding" → stays as List
        let schema = Arc::new(Schema::new(vec![Field::new(
            "embedding",
            DataType::List(Arc::new(Field::new("item", DataType::Float32, true))),
            true,
        )]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(
                ListArray::from_iter_primitive::<Float32Type, _, _>(vec![
                    Some(vec![Some(1.0f32), Some(2.0)]),
                    Some(vec![Some(3.0f32), Some(4.0), Some(5.0)]),
                ]),
            )],
        )
        .unwrap();

        let inferred = infer_fsl_schema(&batch);
        assert_eq!(inferred, schema);
    }

    #[test]
    fn test_infer_fsl_schema_non_vector_name() {
        // List<Float32> named "data" (no vector/embedding) → stays as List
        let schema = Arc::new(Schema::new(vec![Field::new(
            "data",
            DataType::List(Arc::new(Field::new("item", DataType::Float32, true))),
            true,
        )]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(
                ListArray::from_iter_primitive::<Float32Type, _, _>(
                    (0..3).map(|_| Some(vec![Some(1.0f32), Some(2.0), Some(3.0)])),
                ),
            )],
        )
        .unwrap();

        let inferred = infer_fsl_schema(&batch);
        assert_eq!(inferred, schema);
    }

    #[test]
    fn test_infer_fsl_schema_large_list() {
        // LargeList<Float32> named "embedding" should also infer FSL.
        use arrow_array::LargeListArray;
        let schema = Arc::new(Schema::new(vec![Field::new(
            "embedding",
            DataType::LargeList(Arc::new(Field::new("item", DataType::Float32, true))),
            true,
        )]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(LargeListArray::from_iter_primitive::<
                Float32Type,
                _,
                _,
            >(
                (0..3).map(|_| Some(vec![Some(1.0f32), Some(2.0), Some(3.0)])),
            ))],
        )
        .unwrap();

        let inferred = infer_fsl_schema(&batch);
        let embedding_field = inferred.field_with_name("embedding").unwrap();
        assert!(
            matches!(embedding_field.data_type(), DataType::FixedSizeList(_, 3)),
            "expected FixedSizeList(_, 3), got {:?}",
            embedding_field.data_type()
        );
    }

    #[test]
    fn test_infer_fsl_schema_dim_zero_stays_list() {
        // List<Float32> named "embedding" where all rows are empty lists → stays List.
        let schema = Arc::new(Schema::new(vec![Field::new(
            "embedding",
            DataType::List(Arc::new(Field::new("item", DataType::Float32, true))),
            true,
        )]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(
                ListArray::from_iter_primitive::<Float32Type, _, _>(
                    (0..3).map(|_| Some(Vec::<Option<f32>>::new())),
                ),
            )],
        )
        .unwrap();

        let inferred = infer_fsl_schema(&batch);
        assert_eq!(inferred, schema);
    }

    #[test]
    fn test_infer_fsl_schema_with_null_rows() {
        // Some null rows mixed with uniform-length valid rows: the inferred
        // dimension comes from the non-null rows (Kleene `bool_and` over the
        // length-equality mask treats null lengths as "unknown"), so the column
        // is converted to FixedSizeList(Float32, 3).
        let schema = Arc::new(Schema::new(vec![Field::new(
            "embedding",
            DataType::List(Arc::new(Field::new("item", DataType::Float32, true))),
            true,
        )]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(
                ListArray::from_iter_primitive::<Float32Type, _, _>(vec![
                    Some(vec![Some(1.0f32), Some(2.0), Some(3.0)]),
                    None,
                    Some(vec![Some(4.0f32), Some(5.0), Some(6.0)]),
                ]),
            )],
        )
        .unwrap();

        let inferred = infer_fsl_schema(&batch);
        let f = inferred.field_with_name("embedding").unwrap();
        let DataType::FixedSizeList(inner, dim) = f.data_type() else {
            panic!("expected FixedSizeList, got {:?}", f.data_type());
        };
        assert_eq!(*dim, 3);
        assert_eq!(inner.data_type(), &DataType::Float32);

        // The null row must remain null after coercion to the inferred schema —
        // pin this so a future change that silently drops the null row fails here.
        let coerced = crate::data::sanitize::coerce_schema_batch(batch.clone(), inferred).unwrap();
        let fsl = coerced.column(0).as_fixed_size_list();
        assert!(fsl.is_valid(0));
        assert!(fsl.is_null(1));
        assert!(fsl.is_valid(2));
    }

    #[test]
    fn test_infer_fsl_schema_all_null_list() {
        // All-null list column → stays List.
        let schema = Arc::new(Schema::new(vec![Field::new(
            "embedding",
            DataType::List(Arc::new(Field::new("item", DataType::Float32, true))),
            true,
        )]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(
                ListArray::from_iter_primitive::<Float32Type, _, _>(vec![
                    None::<Vec<Option<f32>>>,
                    None,
                    None,
                ]),
            )],
        )
        .unwrap();

        let inferred = infer_fsl_schema(&batch);
        let f = inferred.field_with_name("embedding").unwrap();
        // Null rows have length 0 so this should NOT be promoted to FSL.
        assert!(
            matches!(f.data_type(), DataType::List(_)),
            "expected List, got {:?}",
            f.data_type()
        );
    }

    #[test]
    fn test_infer_fsl_schema_int_list_all_null_values_uint8() {
        // Integer list column where every value is null exercises the
        // !any_valid path of infer_integer_element_type, which returns UInt8.
        let schema = Arc::new(Schema::new(vec![Field::new(
            "vector",
            DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
            true,
        )]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(ListArray::from_iter_primitive::<
                Int32ArrayType,
                _,
                _,
            >(
                (0..3).map(|_| Some(vec![None::<i32>, None, None])),
            ))],
        )
        .unwrap();

        let inferred = infer_fsl_schema(&batch);
        let f = inferred.field_with_name("vector").unwrap();
        match f.data_type() {
            DataType::FixedSizeList(inner, 3) => {
                assert_eq!(inner.data_type(), &DataType::UInt8);
            }
            other => panic!("expected FSL<UInt8, 3>, got {:?}", other),
        }
    }

    #[test]
    fn test_infer_fsl_schema_case_insensitive_substring_name() {
        // Case-insensitive substring matches: "Embedding", "VECTOR", and names
        // that contain "vector"/"embedding" as a substring (e.g. "VectorId",
        // "my_embedding_v2") all trigger inference. Matches Python's
        // `_name_suggests_vector_column`.
        for name in [
            "Embedding",
            "VECTOR",
            "VectorId",
            "my_embedding_v2",
            "user_vectors",
        ] {
            let schema = Arc::new(Schema::new(vec![Field::new(
                name,
                DataType::List(Arc::new(Field::new("item", DataType::Float32, true))),
                true,
            )]));
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(
                    ListArray::from_iter_primitive::<Float32Type, _, _>(
                        (0..2).map(|_| Some(vec![Some(1.0f32), Some(2.0)])),
                    ),
                )],
            )
            .unwrap();
            let inferred = infer_fsl_schema(&batch);
            let f = inferred.field_with_name(name).unwrap();
            assert!(
                matches!(f.data_type(), DataType::FixedSizeList(_, 2)),
                "column {name}: expected FSL, got {:?}",
                f.data_type()
            );
        }
    }

    #[test]
    fn test_infer_fsl_schema_non_numeric_value_type() {
        // List<Utf8> named "embedding" → stays List (non-numeric values skipped).
        use arrow_array::builder::{ListBuilder, StringBuilder};
        let mut builder = ListBuilder::new(StringBuilder::new());
        for _ in 0..2 {
            builder.values().append_value("a");
            builder.values().append_value("b");
            builder.append(true);
        }
        let list = builder.finish();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "embedding",
            list.data_type().clone(),
            true,
        )]));
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(list)]).unwrap();
        let inferred = infer_fsl_schema(&batch);
        assert_eq!(inferred, schema);
    }

    #[test]
    fn test_infer_fsl_schema_preserves_metadata() {
        // Schema-level and field-level metadata should survive inference.
        let mut schema_md = std::collections::HashMap::new();
        schema_md.insert("schema_key".to_string(), "schema_val".to_string());

        let mut field_md = std::collections::HashMap::new();
        field_md.insert("field_key".to_string(), "field_val".to_string());

        let embedding_field = Field::new(
            "embedding",
            DataType::List(Arc::new(Field::new("item", DataType::Float32, true))),
            true,
        )
        .with_metadata(field_md.clone());

        let schema = Arc::new(Schema::new_with_metadata(
            vec![embedding_field],
            schema_md.clone(),
        ));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(
                ListArray::from_iter_primitive::<Float32Type, _, _>(
                    (0..2).map(|_| Some(vec![Some(1.0f32), Some(2.0)])),
                ),
            )],
        )
        .unwrap();

        let inferred = infer_fsl_schema(&batch);
        assert_eq!(inferred.metadata(), &schema_md);
        let f = inferred.field_with_name("embedding").unwrap();
        assert_eq!(f.metadata(), &field_md);
        assert!(matches!(f.data_type(), DataType::FixedSizeList(_, 2)));
    }

    #[test]
    fn test_infer_vector_columns() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("f", DataType::Float32, false),
            Field::new("s", DataType::Utf8, false),
            Field::new(
                "l1",
                DataType::List(Arc::new(Field::new("item", DataType::Float32, true))),
                false,
            ),
            Field::new(
                "l2",
                DataType::List(Arc::new(Field::new("item", DataType::Float64, true))),
                false,
            ),
            Field::new(
                "fl",
                DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), 32),
                true,
            ),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Float32Array::from(vec![1.0, 2.0, 3.0])),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
                Arc::new(ListArray::from_iter_primitive::<Float32Type, _, _>(
                    (0..3).map(|_| Some(vec![Some(1.0), Some(2.0), Some(3.0), Some(4.0)])),
                )),
                // Var-length list
                Arc::new(ListArray::from_iter_primitive::<Float64Type, _, _>(vec![
                    Some(vec![Some(1.0_f64)]),
                    Some(vec![Some(2.0_f64), Some(3.0_f64)]),
                    Some(vec![Some(4.0_f64), Some(5.0_f64), Some(6.0_f64)]),
                ])),
                Arc::new(
                    FixedSizeListArray::from_iter_primitive::<Float32Type, _, _>(
                        vec![
                            Some(vec![Some(1.0); 32]),
                            Some(vec![Some(2.0); 32]),
                            Some(vec![Some(3.0); 32]),
                        ],
                        32,
                    ),
                ),
            ],
        )
        .unwrap();
        let reader =
            RecordBatchIterator::new(vec![batch.clone()].into_iter().map(Ok), schema.clone());

        let cols = infer_vector_columns(reader, false).unwrap();
        assert_eq!(cols, vec!["fl", "l1"]);

        let reader = RecordBatchIterator::new(vec![batch].into_iter().map(Ok), schema);
        let cols = infer_vector_columns(reader, true).unwrap();
        assert_eq!(cols, vec!["fl"]);
    }
}
