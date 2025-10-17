// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use std::collections::HashMap;

use futures::future::BoxFuture;
use lance::dataset::transaction::UpdateMapEntry;

use crate::error::Result;
use crate::table::Table;

/// Builder for metadata update operations that supports optional replace semantics.
/// This provides backward compatibility while adding new functionality.
pub struct UpdateMetadataBuilder<'a> {
    table: &'a Table,
    values: Vec<UpdateMapEntry>,
    replace: bool,
    metadata_type: MetadataType,
}

/// Type of metadata being updated
pub enum MetadataType {
    Config,
    TableMetadata,
    SchemaMetadata,
}

impl<'a> UpdateMetadataBuilder<'a> {
    pub fn new(
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
                    // For NativeTable, we need to access the dataset and use its builder
                    if let Some(native_table) = self.table.as_native() {
                        let mut dataset = native_table.dataset.get_mut().await?;
                        let result = if self.replace {
                            dataset.update_config(self.values).replace().await?
                        } else {
                            dataset.update_config(self.values).await?
                        };
                        Ok(result)
                    } else {
                        // For RemoteTable, use the BaseTable trait method
                        self.table
                            .inner
                            .update_config(self.values, self.replace)
                            .await
                    }
                }
                MetadataType::TableMetadata => {
                    // Call the BaseTable trait method which handles both Native and Remote
                    self.table
                        .inner
                        .update_metadata(self.values, self.replace)
                        .await
                }
                MetadataType::SchemaMetadata => {
                    // Call the BaseTable trait method which handles both Native and Remote
                    self.table
                        .inner
                        .update_schema_metadata(self.values, self.replace)
                        .await
                }
            }
        })
    }
}
