// Copyright 2024 Lance Developers.
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

use std::sync::Arc;

use arrow_array::types::Float32Type;
use arrow_array::{FixedSizeListArray, Int32Array, RecordBatch, RecordBatchIterator};
use arrow_schema::{DataType, Field, Schema};
use futures::TryStreamExt;

use vectordb::Connection;
use vectordb::{connect, Result, Table, TableRef};

#[tokio::main]
async fn main() -> Result<()> {
    // --8<-- [start:connect]
    let uri = "data/sample-lancedb";
    let db = connect(uri).await?;
    // --8<-- [end:connect]
    let tbl = create_table(db).await?;
    create_index(tbl.as_ref()).await?;
    let batches = search(tbl.as_ref()).await?;
    println!("{:?}", batches);
    Ok(())
}

async fn create_table(db: Arc<dyn Connection>) -> Result<TableRef> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new(
            "vector",
            DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), 128),
            true,
        ),
    ]));
    const TOTAL: usize = 1000;
    const DIM: usize = 128;
    // Create a RecordBatch stream.
    let batches = RecordBatchIterator::new(
        vec![RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from_iter_values(0..TOTAL as i32)),
                Arc::new(
                    FixedSizeListArray::from_iter_primitive::<Float32Type, _, _>(
                        (0..TOTAL).map(|_| Some(vec![Some(1.0); DIM])),
                        DIM as i32,
                    ),
                ),
            ],
        )
        .unwrap()]
        .into_iter()
        .map(Ok),
        schema.clone(),
    );
    db.create_table("my_table", Box::new(batches), None).await
}

async fn create_index(table: &dyn Table) -> Result<()> {
    table
        .create_index(&["vector"])
        .ivf_pq()
        .num_partitions(2)
        .build()
        .await
}

async fn search(table: &dyn Table) -> Result<Vec<RecordBatch>> {
    Ok(table
        .search(&[1.0; 128])
        .limit(2)
        .execute_stream()
        .await?
        .try_collect::<Vec<_>>()
        .await?)
}
