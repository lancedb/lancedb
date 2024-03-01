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

use arrow_ipc::writer::FileWriter;
use lance::dataset::ColumnAlteration as LanceColumnAlteration;
use lancedb::{
    ipc::ipc_file_to_batches,
    table::{AddDataOptions, TableRef},
};
use napi::bindgen_prelude::*;
use napi_derive::napi;

use crate::index::IndexBuilder;
use crate::query::Query;

#[napi]
pub struct Table {
    pub(crate) table: TableRef,
}

#[napi]
impl Table {
    pub(crate) fn new(table: TableRef) -> Self {
        Self { table }
    }

    /// Return Schema as empty Arrow IPC file.
    #[napi]
    pub async fn schema(&self) -> napi::Result<Buffer> {
        let schema =
            self.table.schema().await.map_err(|e| {
                napi::Error::from_reason(format!("Failed to create IPC file: {}", e))
            })?;
        let mut writer = FileWriter::try_new(vec![], &schema)
            .map_err(|e| napi::Error::from_reason(format!("Failed to create IPC file: {}", e)))?;
        writer
            .finish()
            .map_err(|e| napi::Error::from_reason(format!("Failed to finish IPC file: {}", e)))?;
        Ok(Buffer::from(writer.into_inner().map_err(|e| {
            napi::Error::from_reason(format!("Failed to get IPC file: {}", e))
        })?))
    }

    #[napi]
    pub async fn add(&self, buf: Buffer) -> napi::Result<()> {
        let batches = ipc_file_to_batches(buf.to_vec())
            .map_err(|e| napi::Error::from_reason(format!("Failed to read IPC file: {}", e)))?;
        self.table
            .add(Box::new(batches), AddDataOptions::default())
            .await
            .map_err(|e| {
                napi::Error::from_reason(format!(
                    "Failed to add batches to table {}: {}",
                    self.table, e
                ))
            })
    }

    #[napi]
    pub async fn count_rows(&self, filter: Option<String>) -> napi::Result<i64> {
        self.table
            .count_rows(filter)
            .await
            .map(|val| val as i64)
            .map_err(|e| {
                napi::Error::from_reason(format!(
                    "Failed to count rows in table {}: {}",
                    self.table, e
                ))
            })
    }

    #[napi]
    pub async fn delete(&self, predicate: String) -> napi::Result<()> {
        self.table.delete(&predicate).await.map_err(|e| {
            napi::Error::from_reason(format!(
                "Failed to delete rows in table {}: predicate={}",
                self.table, e
            ))
        })
    }

    #[napi]
    pub fn create_index(&self) -> IndexBuilder {
        IndexBuilder::new(self.table.as_ref())
    }

    #[napi]
    pub fn query(&self) -> Query {
        Query::new(self)
    }

    #[napi]
    pub async fn add_columns(&self, transforms: Vec<AddColumnsSql>) -> napi::Result<()> {
        let transforms = transforms
            .into_iter()
            .map(|sql| (sql.name, sql.value_sql))
            .collect::<Vec<_>>();
        let transforms = lance::dataset::NewColumnTransform::SqlExpressions(transforms);
        self.table
            .add_columns(transforms, None)
            .await
            .map_err(|err| {
                napi::Error::from_reason(format!(
                    "Failed to add columns to table {}: {}",
                    self.table, err
                ))
            })?;
        Ok(())
    }

    #[napi]
    pub async fn alter_columns(&self, alterations: Vec<ColumnAlteration>) -> napi::Result<()> {
        for alteration in &alterations {
            if alteration.rename.is_none() && alteration.nullable.is_none() {
                return Err(napi::Error::from_reason(
                    "Alteration must have a 'rename' or 'nullable' field.",
                ));
            }
        }
        let alterations = alterations
            .into_iter()
            .map(LanceColumnAlteration::from)
            .collect::<Vec<_>>();

        self.table
            .alter_columns(&alterations)
            .await
            .map_err(|err| {
                napi::Error::from_reason(format!(
                    "Failed to alter columns in table {}: {}",
                    self.table, err
                ))
            })?;
        Ok(())
    }

    #[napi]
    pub async fn drop_columns(&self, columns: Vec<String>) -> napi::Result<()> {
        let col_refs = columns.iter().map(String::as_str).collect::<Vec<_>>();
        self.table.drop_columns(&col_refs).await.map_err(|err| {
            napi::Error::from_reason(format!(
                "Failed to drop columns from table {}: {}",
                self.table, err
            ))
        })?;
        Ok(())
    }
}

///  A definition of a column alteration. The alteration changes the column at
/// `path` to have the new name `name`, to be nullable if `nullable` is true,
/// and to have the data type `data_type`. At least one of `rename` or `nullable`
/// must be provided.
#[napi(object)]
pub struct ColumnAlteration {
    /// The path to the column to alter. This is a dot-separated path to the column.
    /// If it is a top-level column then it is just the name of the column. If it is
    /// a nested column then it is the path to the column, e.g. "a.b.c" for a column
    /// `c` nested inside a column `b` nested inside a column `a`.
    pub path: String,
    /// The new name of the column. If not provided then the name will not be changed.
    /// This must be distinct from the names of all other columns in the table.
    pub rename: Option<String>,
    /// Set the new nullability. Note that a nullable column cannot be made non-nullable.
    pub nullable: Option<bool>,
}

impl From<ColumnAlteration> for LanceColumnAlteration {
    fn from(js: ColumnAlteration) -> Self {
        let ColumnAlteration {
            path,
            rename,
            nullable,
        } = js;
        Self {
            path,
            rename,
            nullable,
            // TODO: wire up this field
            data_type: None,
        }
    }
}

/// A definition of a new column to add to a table.
#[napi(object)]
pub struct AddColumnsSql {
    /// The name of the new column.
    pub name: String,
    /// The values to populate the new column with, as a SQL expression.
    /// The expression can reference other columns in the table.
    pub value_sql: String,
}
