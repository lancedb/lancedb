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

use std::io::Cursor;
use std::sync::Arc;

use arrow_array::cast::as_list_array;
use arrow_array::{Array, FixedSizeListArray, RecordBatch};
use arrow_ipc::reader::FileReader;
use arrow_schema::{DataType, Field, Schema};
use lance::arrow::{FixedSizeListArrayExt, RecordBatchExt};

pub(crate) fn convert_record_batch(record_batch: RecordBatch) -> RecordBatch {
    let column = record_batch
        .column_by_name("vector")
        .cloned()
        .expect("vector column is missing");
    // TODO: we should just consume the underlaying js buffer in the future instead of this arrow around a bunch of times
    let arr = as_list_array(column.as_ref());
    let list_size = arr.values().len() / record_batch.num_rows();
    let r =
        FixedSizeListArray::try_new_from_values(arr.values().to_owned(), list_size as i32).unwrap();

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

pub(crate) fn arrow_buffer_to_record_batch(slice: &[u8]) -> Vec<RecordBatch> {
    let mut batches: Vec<RecordBatch> = Vec::new();
    let fr = FileReader::try_new(Cursor::new(slice), None);
    let file_reader = fr.unwrap();
    for b in file_reader {
        let record_batch = convert_record_batch(b.unwrap());
        batches.push(record_batch);
    }
    batches
}
