// Copyright 2023 Lance Developers.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashMap;

use arrow::compute::kernels::{aggregate::bool_and, length::length};
use arrow_array::{cast::AsArray, types::Int32Type, Array, RecordBatchReader};
use arrow_ord::comparison::eq_dyn_scalar;
use arrow_schema::DataType;

use crate::error::{Error, Result};

pub(crate) fn infer_dimension(arr: &dyn Array) -> Result<Option<i32>> {
    if !matches!(arr.data_type(), DataType::List(_)) {
        return Ok(None);
    };
    let list_arr = arr.as_list_opt::<i32>().expect("must be a list now");
    let len_arr = length(list_arr)?;
    if len_arr.is_empty() {
        return Ok(Some(0));
    }

    let dim = len_arr.as_primitive::<Int32Type>().value(0);
    if bool_and(&eq_dyn_scalar(len_arr.as_primitive::<Int32Type>(), dim)?) != Some(true) {
        return Ok(None);
    }
    return Ok(Some(dim));
}

/// Infer the vector columns from a dataset.
///
/// Parameters
/// ----------
/// - reader: RecordBatchReader
/// - strict: if set true, only fixed_size_list<float> is considered as vector column. If set to false,
///           a list<float> column with same length is also considered as vector column.
pub fn infer_vector_columns(
    reader: impl RecordBatchReader + Send,
    strict: bool,
) -> Result<Vec<String>> {
    let mut columns = vec![];

    let mut columns_map: HashMap<String, Option<i32>> = HashMap::new();
    for field in reader.schema().fields() {
        match field.data_type() {
            DataType::FixedSizeList(sub_field, _) if sub_field.data_type().is_floating() => {
                columns.push(field.name().to_string());
            }
            DataType::List(sub_field) if sub_field.data_type().is_floating() && !strict => {
                columns_map.insert(field.name().to_string(), None);
            }
            _ => {}
        }
    }
    for batch in reader {
        let batch = batch?;
        let col_names = columns_map.keys().cloned().collect::<Vec<_>>();
        for col_name in col_names {
            let col = batch.column_by_name(&col_name).ok_or(Error::Schema {
                message: format!("Column {} not found", col_name),
            })?;
            if let Some(dim) = infer_dimension(col.as_ref())? {
                if let Some(Some(prev_dim)) = columns_map.get(&col_name) {
                    if prev_dim != &dim {
                        columns_map.remove(&col_name);
                    }
                } else {
                    columns_map.insert(col_name, Some(dim));
                }
            } else {
                columns_map.remove(&col_name);
            }
        }
    }
    columns.extend(columns_map.keys().cloned());
    Ok(columns)
}

#[cfg(test)]
mod tests {
    use super::*;

    use arrow_array::{
        types::{Float32Type, Float64Type},
        FixedSizeListArray, Float32Array, ListArray, RecordBatch, RecordBatchIterator, StringArray,
    };
    use arrow_schema::{DataType, Field, Schema};
    use std::{sync::Arc, vec};

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
