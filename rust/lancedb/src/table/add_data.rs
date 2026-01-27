// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::data::scannable::Scannable;
use crate::embeddings::EmbeddingRegistry;
use crate::Result;

use super::{BaseTable, WriteOptions};

#[derive(Debug, Clone, Default)]
pub enum AddDataMode {
    /// Rows will be appended to the table (the default)
    #[default]
    Append,
    /// The existing table will be overwritten with the new data
    Overwrite,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct AddResult {
    // The commit version associated with the operation.
    // A version of `0` indicates compatibility with legacy servers that do not return
    /// a commit version.
    #[serde(default)]
    pub version: u64,
}

/// A builder for configuring a [`crate::table::Table::add`] operation
pub struct AddDataBuilder {
    pub(crate) parent: Arc<dyn BaseTable>,
    pub(crate) data: Box<dyn Scannable>,
    pub(crate) mode: AddDataMode,
    pub(crate) write_options: WriteOptions,
    #[allow(dead_code)] // Used for future embedding support
    pub(crate) embedding_registry: Option<Arc<dyn EmbeddingRegistry>>,
}

impl std::fmt::Debug for AddDataBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AddDataBuilder")
            .field("parent", &self.parent)
            .field("mode", &self.mode)
            .field("write_options", &self.write_options)
            .finish()
    }
}

impl AddDataBuilder {
    pub(crate) fn new(
        parent: Arc<dyn BaseTable>,
        data: Box<dyn Scannable>,
        embedding_registry: Option<Arc<dyn EmbeddingRegistry>>,
    ) -> Self {
        Self {
            parent,
            data,
            mode: AddDataMode::Append,
            write_options: WriteOptions::default(),
            embedding_registry,
        }
    }

    pub fn mode(mut self, mode: AddDataMode) -> Self {
        self.mode = mode;
        self
    }

    pub fn write_options(mut self, options: WriteOptions) -> Self {
        self.write_options = options;
        self
    }

    pub async fn execute(self) -> Result<AddResult> {
        self.parent.clone().add(self).await
    }
}

#[cfg(test)]
mod tests {
    use arrow_array::record_batch;
    use lance::dataset::{WriteMode, WriteParams};

    use crate::connect;
    use crate::table::WriteOptions;

    use super::AddDataMode;

    #[tokio::test]
    async fn test_add() {
        let conn = connect("memory://").execute().await.unwrap();

        let batch = record_batch!(("i", Int32, [0, 1, 2])).unwrap();
        let table = conn
            .create_table("test", batch.clone())
            .execute()
            .await
            .unwrap();
        assert_eq!(table.count_rows(None).await.unwrap(), 3);

        let new_batch = record_batch!(("i", Int32, [3])).unwrap();
        table.add(new_batch).execute().await.unwrap();

        assert_eq!(table.count_rows(None).await.unwrap(), 4);
        assert_eq!(table.schema().await.unwrap(), batch.schema());
    }

    #[tokio::test]
    async fn test_add_overwrite() {
        let conn = connect("memory://").execute().await.unwrap();

        let batch = record_batch!(("i", Int32, [0, 1, 2])).unwrap();
        let table = conn
            .create_table("test", batch.clone())
            .execute()
            .await
            .unwrap();
        assert_eq!(table.count_rows(None).await.unwrap(), batch.num_rows());

        let new_batch = record_batch!(("x", Float32, [0.0, 1.0])).unwrap();
        let res = table
            .add(new_batch.clone())
            .mode(AddDataMode::Overwrite)
            .execute()
            .await
            .unwrap();
        assert_eq!(res.version, table.version().await.unwrap());
        assert_eq!(table.count_rows(None).await.unwrap(), new_batch.num_rows());
        assert_eq!(table.schema().await.unwrap(), new_batch.schema());

        // Can overwrite using underlying WriteParams (which
        // take precedence over AddDataMode)
        let param: WriteParams = WriteParams {
            mode: WriteMode::Overwrite,
            ..Default::default()
        };

        table
            .add(new_batch.clone())
            .write_options(WriteOptions {
                lance_write_params: Some(param),
            })
            .mode(AddDataMode::Append)
            .execute()
            .await
            .unwrap();
        assert_eq!(table.count_rows(None).await.unwrap(), new_batch.num_rows());
    }
}
