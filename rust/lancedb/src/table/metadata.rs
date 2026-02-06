// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use std::collections::HashMap;

use futures::future::BoxFuture;
use lance::dataset::transaction::UpdateMapEntry;

use crate::error::Result;
use crate::table::Table;

/// Builder for metadata update operations with optional replace semantics.
pub struct UpdateMetadataBuilder<'a> {
    table: &'a Table,
    values: Vec<UpdateMapEntry>,
    replace: bool,
    metadata_type: MetadataType,
}

/// Type of metadata being updated
pub(crate) enum MetadataType {
    Config,
    TableMetadata,
    SchemaMetadata,
}

impl<'a> UpdateMetadataBuilder<'a> {
    pub(crate) fn new(
        table: &'a Table,
        values: impl IntoIterator<Item = impl Into<UpdateMapEntry>>,
        metadata_type: MetadataType,
    ) -> Self {
        Self {
            table,
            values: values.into_iter().map(Into::into).collect(),
            replace: false,
            metadata_type,
        }
    }

    /// Set the replace flag to true, causing the entire metadata map to be replaced
    /// instead of merged.
    pub fn replace(mut self) -> Self {
        self.replace = true;
        self
    }
}

impl<'a> std::future::IntoFuture for UpdateMetadataBuilder<'a> {
    type Output = Result<HashMap<String, String>>;
    type IntoFuture = BoxFuture<'a, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            match self.metadata_type {
                MetadataType::Config => {
                    self.table
                        .inner
                        .update_config(self.values, self.replace)
                        .await
                }
                MetadataType::TableMetadata => {
                    self.table
                        .inner
                        .update_metadata(self.values, self.replace)
                        .await
                }
                MetadataType::SchemaMetadata => {
                    self.table
                        .inner
                        .update_schema_metadata(self.values, self.replace)
                        .await
                }
            }
        })
    }
}
