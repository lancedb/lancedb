// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use std::sync::Arc;

use lance::dataset::UpdateBuilder as LanceUpdateBuilder;
use serde::{Deserialize, Serialize};

use super::{BaseTable, NativeTable};
use crate::Error;
use crate::Result;

/// The result of an update operation
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct UpdateResult {
    #[serde(default)]
    pub rows_updated: u64,
    /// The commit version associated with the operation.
    #[serde(default)]
    pub version: u64,
}

/// A builder for configuring a [`crate::table::Table::update`] operation
#[derive(Debug, Clone)]
pub struct UpdateBuilder {
    parent: Arc<dyn BaseTable>,
    pub(crate) filter: Option<String>,
    pub(crate) columns: Vec<(String, String)>,
}

impl UpdateBuilder {
    pub(crate) fn new(parent: Arc<dyn BaseTable>) -> Self {
        Self {
            parent,
            filter: None,
            columns: Vec::new(),
        }
    }

    /// Limits the update operation to rows matching the given filter
    ///
    /// If a row does not match the filter then it will be left unchanged.
    pub fn only_if(mut self, filter: impl Into<String>) -> Self {
        self.filter = Some(filter.into());
        self
    }

    /// Specifies a column to update
    ///
    /// This method may be called multiple times to update multiple columns
    ///
    /// The `update_expr` should be an SQL expression explaining how to calculate
    /// the new value for the column.  The expression will be evaluated against the
    /// previous row's value.
    pub fn column(
        mut self,
        column_name: impl Into<String>,
        update_expr: impl Into<String>,
    ) -> Self {
        self.columns.push((column_name.into(), update_expr.into()));
        self
    }

    /// Executes the update operation.
    pub async fn execute(self) -> Result<UpdateResult> {
        if self.columns.is_empty() {
            Err(Error::InvalidInput {
                message: "at least one column must be specified in an update operation".to_string(),
            })
        } else {
            self.parent.clone().update(self).await
        }
    }
}

/// Internal implementation of the update logic
pub(crate) async fn execute_update(
    table: &NativeTable,
    update: UpdateBuilder,
) -> Result<UpdateResult> {
    // 1. Snapshot the current dataset
    let dataset = table.dataset.get().await?.clone();

    // 2. Initialize the Lance Core builder
    let mut builder = LanceUpdateBuilder::new(Arc::new(dataset));

    // 3. Apply the filter (WHERE clause)
    if let Some(predicate) = update.filter {
        builder = builder.update_where(&predicate)?;
    }

    // 4. Apply the columns (SET clause)
    for (column, value) in update.columns {
        builder = builder.set(column, &value)?;
    }

    // 5. Execute the operation (Write new files)
    let operation = builder.build()?;
    let res = operation.execute().await?;

    // 6. Update the table's view of the latest version
    table
        .dataset
        .set_latest(res.new_dataset.as_ref().clone())
        .await;

    Ok(UpdateResult {
        rows_updated: res.rows_updated,
        version: res.new_dataset.version().version,
    })
}

#[cfg(test)]
mod tests {
    use crate::connect;
    use crate::query::QueryBase;
    use crate::query::{ExecutableQuery, Select};
    use arrow_array::{Int32Array, RecordBatch, RecordBatchIterator, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use futures::TryStreamExt;
    use std::sync::Arc; // Import traits for query execution

    #[tokio::test]
    async fn test_update_simple() {
        let tmp_dir = tempfile::tempdir().unwrap();
        let uri = tmp_dir.path().to_str().unwrap();
        let conn = connect(uri).execute().await.unwrap();

        // 1. Create table with id and value
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("val", DataType::Int32, false),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from_iter_values(0..10)), // ids 0-9
                Arc::new(Int32Array::from_iter_values(vec![1; 10])), // all values are 1
            ],
        )
        .unwrap();

        let table = conn
            .create_table(
                "test_update",
                RecordBatchIterator::new(vec![Ok(batch)], schema),
            )
            .execute()
            .await
            .unwrap();

        // 2. Execute Update: Set val = 5 where id > 5
        table
            .update()
            .only_if("id > 5")
            .column("val", "5")
            .execute()
            .await
            .unwrap();

        // 3. Verify Results
        let batches = table
            .query()
            .select(Select::columns(&["id", "val"]))
            .execute()
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        let batch = &batches[0];
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let vals = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();

        for i in 0..ids.len() {
            let id = ids.value(i);
            let val = vals.value(i);
            if id > 5 {
                assert_eq!(val, 5, "Row with id {} should have been updated to 5", id);
            } else {
                assert_eq!(val, 1, "Row with id {} should stay 1", id);
            }
        }
    }

    #[tokio::test]
    async fn test_update_string() {
        let tmp_dir = tempfile::tempdir().unwrap();
        let uri = tmp_dir.path().to_str().unwrap();
        let conn = connect(uri).execute().await.unwrap();

        // 1. Create table with id and Name (String)
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from_iter_values(0..5)),
                Arc::new(StringArray::from(vec![
                    "alice", "bob", "charlie", "dave", "eve",
                ])),
            ],
        )
        .unwrap();

        let table = conn
            .create_table(
                "test_update_string",
                RecordBatchIterator::new(vec![Ok(batch)], schema),
            )
            .execute()
            .await
            .unwrap();

        // 2. Execute Update: Change "charlie" (id=2) to "chuck"
        // IMPORTANT: Note the single quotes inside the string: "'chuck'"
        // If you forget them, SQL thinks "chuck" is a column name!
        table
            .update()
            .only_if("id = 2")
            .column("name", "'chuck'")
            .execute()
            .await
            .unwrap();

        // 3. Verify Results
        let batches = table
            .query()
            .select(Select::columns(&["id", "name"]))
            .execute()
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        let batch = &batches[0];
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let names = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        for i in 0..ids.len() {
            let id = ids.value(i);
            let name = names.value(i);

            if id == 2 {
                assert_eq!(name, "chuck", "Charlie should become Chuck");
            } else {
                // Ensure others didn't change (e.g. id 0 is still alice)
                assert_ne!(name, "chuck");
            }
        }
    }
}
