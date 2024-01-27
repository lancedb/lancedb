use std::sync::Arc;

use arrow_array::types::Float32Type;
use arrow_array::{FixedSizeListArray, Int32Array, RecordBatch, RecordBatchIterator};
use arrow_schema::{DataType, Field, Schema};

use vectordb::Result;
use vectordb::TableRef;
use vectordb::{connect, Connection};

#[tokio::main]
async fn main() -> Result<()> {
    let uri = "data/sample-lancedb";
    let db = connect(uri).await?;
    create_table(db).await?;
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
    // Create a RecordBatch stream.
    let batches = RecordBatchIterator::new(
        vec![RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from_iter_values(0..10)),
                Arc::new(
                    FixedSizeListArray::from_iter_primitive::<Float32Type, _, _>(
                        (0..10).map(|_| Some(vec![Some(1.0); 128])),
                        128,
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
