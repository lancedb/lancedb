use arrow_array::RecordBatchReader;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use lance::dataset::{ColumnAlteration, NewColumnTransform};

use crate::{
    error::Result,
    index::IndexBuilder,
    query::Query,
    table::{
        merge::MergeInsertBuilder, AddDataOptions, NativeTable, OptimizeAction, OptimizeStats,
    },
    Table,
};

use super::client::RestfulLanceDbClient;

#[derive(Debug)]
pub struct RemoteTable {
    #[allow(dead_code)]
    client: RestfulLanceDbClient,
    name: String,
}

impl RemoteTable {
    pub fn new(client: RestfulLanceDbClient, name: String) -> Self {
        Self { client, name }
    }
}

impl std::fmt::Display for RemoteTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "RemoteTable({})", self.name)
    }
}

#[async_trait]
impl Table for RemoteTable {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn as_native(&self) -> Option<&NativeTable> {
        None
    }
    fn name(&self) -> &str {
        &self.name
    }
    async fn schema(&self) -> Result<SchemaRef> {
        todo!()
    }
    async fn count_rows(&self, _filter: Option<String>) -> Result<usize> {
        todo!()
    }
    async fn add(
        &self,
        _batches: Box<dyn RecordBatchReader + Send>,
        _options: AddDataOptions,
    ) -> Result<()> {
        todo!()
    }
    async fn delete(&self, _predicate: &str) -> Result<()> {
        todo!()
    }
    fn create_index(&self, _column: &[&str]) -> IndexBuilder {
        todo!()
    }
    fn merge_insert(&self, _on: &[&str]) -> MergeInsertBuilder {
        todo!()
    }
    fn query(&self) -> Query {
        todo!()
    }
    async fn optimize(&self, _action: OptimizeAction) -> Result<OptimizeStats> {
        todo!()
    }
    async fn add_columns(
        &self,
        _transforms: NewColumnTransform,
        _read_columns: Option<Vec<String>>,
    ) -> Result<()> {
        todo!()
    }
    async fn alter_columns(&self, _alterations: &[ColumnAlteration]) -> Result<()> {
        todo!()
    }
    async fn drop_columns(&self, _columns: &[&str]) -> Result<()> {
        todo!()
    }
}
