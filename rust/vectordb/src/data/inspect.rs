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
use arrow_array::{cast::AsArray, types::UInt32Type, RecordBatchReader};
use arrow_ord::comparison::eq_dyn_scalar;
use arrow_schema::DataType;

use crate::error::{Error, Result};

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

    let mut columns_map: HashMap<String, Option<u32>> = HashMap::new();
    for field in reader.schema().fields() {
        match field.data_type() {
            DataType::FixedSizeList(field, _) if field.data_type().is_floating() => {
                columns.push(field.name().to_string());
            }
            DataType::List(field) if field.data_type().is_floating() && !strict => {
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
            let list_arr = col.as_list_opt::<i32>().expect("Must be a list array now");
            let len_arr = length(list_arr)?;
            if len_arr.is_empty() {
                columns_map.remove(&col_name);
            } else {
                let len: u32 = len_arr.as_primitive::<UInt32Type>().value(0);
                if bool_and(&eq_dyn_scalar(len_arr.as_primitive::<UInt32Type>(), len)?)
                    == Some(true)
                {
                    columns_map.remove(&col_name);
                }
            }
        }
    }
    columns.extend(columns_map.keys().cloned());
    Ok(columns)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_infer_vector_columns() {}
}
