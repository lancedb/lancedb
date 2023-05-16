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

use std::ops::Deref;
use std::sync::Arc;

use arrow_array::cast::as_list_array;
use arrow_array::{Array, FixedSizeListArray, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use lance::arrow::{FixedSizeListArrayExt, RecordBatchExt};

pub(crate) fn convert_record_batch(record_batch: RecordBatch) -> RecordBatch {
    let column = record_batch
        .column_by_name("vector")
        .expect("vector column is missing");
    let arr = as_list_array(column.deref());
    let list_size = arr.values().len() / record_batch.num_rows();
    let r = FixedSizeListArray::try_new(arr.values(), list_size as i32).unwrap();

    let schema = Arc::new(Schema::new(vec![Field::new(
        "vector",
        DataType::FixedSizeList(
            Arc::new(Field::new("item", DataType::Float32, true)),
            list_size as i32,
        ),
        true,
    )]));

    let mut new_batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(r)]).unwrap();

    if record_batch.num_columns() > 1 {
        let rb = record_batch.drop_column("vector").unwrap();
        new_batch = new_batch.merge(&rb).unwrap();
    }
    new_batch
}
