use arrow_array::RecordBatchReader;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use lance::dataset::{scanner::DatasetRecordBatchStream, ColumnAlteration, NewColumnTransform};

use crate::{
    error::Result,
    index::{IndexBuilder, IndexConfig},
    query::Query,
    table::{
        merge::MergeInsertBuilder, AddDataBuilder, NativeTable, OptimizeAction, OptimizeStats,
        TableInternal, UpdateBuilder,
    },
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
impl TableInternal for RemoteTable {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn as_native(&self) -> Option<&NativeTable> {
        None
    }
    fn name(&self) -> &str {
        &self.name
    }
    async fn version(&self) -> Result<u64> {
        todo!()
    }
    async fn checkout(&self, _version: u64) -> Result<()> {
        todo!()
    }
    async fn checkout_latest(&self) -> Result<()> {
        todo!()
    }
    async fn restore(&self) -> Result<()> {
        todo!()
    }
    async fn schema(&self) -> Result<SchemaRef> {
        todo!()
    }
    async fn count_rows(&self, _filter: Option<String>) -> Result<usize> {
        todo!()
    }
    async fn add(&self, _add: AddDataBuilder) -> Result<()> {
        todo!()
    }
    async fn query(&self, _query: &Query) -> Result<DatasetRecordBatchStream> {
        todo!()
    }
    async fn update(&self, _update: UpdateBuilder) -> Result<()> {
        todo!()
    }
    async fn delete(&self, _predicate: &str) -> Result<()> {
        todo!()
    }
    async fn create_index(&self, _index: IndexBuilder) -> Result<()> {
        todo!()
    }
    async fn merge_insert(
        &self,
        _params: MergeInsertBuilder,
        _new_data: Box<dyn RecordBatchReader + Send>,
    ) -> Result<()> {
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
    async fn list_indices(&self) -> Result<Vec<IndexConfig>> {
        todo!()
    }
}
