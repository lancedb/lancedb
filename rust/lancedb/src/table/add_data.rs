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
