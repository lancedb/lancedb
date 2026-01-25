// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use arrow_array::RecordBatch;
use futures::TryStreamExt;
use lance_datagen::{BatchCount, BatchGeneratorBuilder, RowCount};

use crate::{
    arrow::{SendableRecordBatchStream, SimpleRecordBatchStream},
    connect, Error, Table,
};

#[async_trait::async_trait]
pub trait LanceDbDatagenExt {
    async fn into_mem_table(
        self,
        table_name: &str,
        rows_per_batch: RowCount,
        num_batches: BatchCount,
    ) -> Table;
}

#[async_trait::async_trait]
impl LanceDbDatagenExt for BatchGeneratorBuilder {
    async fn into_mem_table(
        self,
        table_name: &str,
        rows_per_batch: RowCount,
        num_batches: BatchCount,
    ) -> Table {
        let (stream, schema) = self.into_reader_stream(rows_per_batch, num_batches);
        let stream: SendableRecordBatchStream = Box::pin(SimpleRecordBatchStream::new(
            stream.map_err(Error::from),
            schema,
        ));
        let db = connect("memory:///").execute().await.unwrap();
        db.create_table(table_name, stream).execute().await.unwrap()
    }
}

pub async fn virtual_table(name: &str, values: &RecordBatch) -> Table {
    let schema = values.schema();
    let stream: SendableRecordBatchStream = Box::pin(SimpleRecordBatchStream::new(
        futures::stream::once(std::future::ready(Ok(values.clone()))),
        schema,
    ));
    let db = connect("memory:///").execute().await.unwrap();
    db.create_table(name, stream).execute().await.unwrap()
}
