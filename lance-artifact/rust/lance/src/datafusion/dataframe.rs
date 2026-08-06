// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::sync::{Arc, Mutex};

use arrow_schema::{Schema, SchemaRef};
use async_trait::async_trait;
use datafusion::{
    catalog::{Session, streaming::StreamingTable},
    dataframe::DataFrame,
    datasource::TableProvider,
    error::DataFusionError,
    execution::{TaskContext, context::SessionContext},
    logical_expr::{Expr, TableProviderFilterPushDown, TableType},
    physical_plan::{ExecutionPlan, SendableRecordBatchStream, streaming::PartitionStream},
};
use lance_arrow::SchemaExt;
use lance_core::{ROW_ADDR_FIELD, ROW_ID_FIELD};

use crate::Dataset;

/// A [TableProvider] for Lance datasets.
///
/// Note: Datafusion has no concept of "system columns".  As a result, you must specify
/// which schema columns should be included in the table's schema when you create the
/// provider.
///
/// This table provider should support:
///  - Filter pushdown
///  - Limit pushdown
///  - Projection pushdown
///
/// Note that LanceDB also has a TableProvider implementation that should be preferred
/// if you are working in LanceDB.
#[derive(Debug)]
pub struct LanceTableProvider {
    dataset: Arc<Dataset>,
    full_schema: Arc<Schema>,
    row_id_idx: Option<usize>,
    row_addr_idx: Option<usize>,
    ordered: bool,
    blob_handling: Option<lance_core::datatypes::BlobHandling>,
}

impl LanceTableProvider {
    pub fn new(dataset: Arc<Dataset>, with_row_id: bool, with_row_addr: bool) -> Self {
        Self::new_with_ordering(dataset, with_row_id, with_row_addr, true)
    }

    pub fn new_with_ordering(
        dataset: Arc<Dataset>,
        with_row_id: bool,
        with_row_addr: bool,
        ordered: bool,
    ) -> Self {
        let mut full_schema = Schema::from(dataset.schema());
        let mut row_id_idx = None;
        let mut row_addr_idx = None;
        if with_row_id {
            full_schema = full_schema.try_with_column(ROW_ID_FIELD.clone()).unwrap();
            row_id_idx = Some(full_schema.fields.len() - 1);
        }
        if with_row_addr {
            full_schema = full_schema.try_with_column(ROW_ADDR_FIELD.clone()).unwrap();
            row_addr_idx = Some(full_schema.fields.len() - 1);
        }
        Self {
            dataset,
            full_schema: Arc::new(full_schema),
            row_id_idx,
            row_addr_idx,
            ordered,
            blob_handling: None,
        }
    }

    /// Overrides how blob columns are read during [`TableProvider::scan`].
    /// When unset, the underlying dataset scan uses its default
    /// [`BlobHandling`](lance_core::datatypes::BlobHandling) policy.
    pub fn with_blob_handling(mut self, handling: lance_core::datatypes::BlobHandling) -> Self {
        let converted = self
            .dataset
            .full_projection()
            .with_blob_handling(handling.clone())
            .to_bare_schema();
        let mut full_schema = Schema::from(&converted);
        if self.row_id_idx.is_some() {
            full_schema = full_schema.try_with_column(ROW_ID_FIELD.clone()).unwrap();
        }
        if self.row_addr_idx.is_some() {
            full_schema = full_schema.try_with_column(ROW_ADDR_FIELD.clone()).unwrap();
        }
        self.full_schema = Arc::new(full_schema);
        self.blob_handling = Some(handling);
        self
    }

    pub fn dataset(&self) -> Arc<Dataset> {
        self.dataset.clone()
    }
}

#[async_trait]
impl TableProvider for LanceTableProvider {
    fn schema(&self) -> SchemaRef {
        self.full_schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        let mut scan = self.dataset.scan();
        if let Some(handling) = self.blob_handling.clone() {
            scan.blob_handling(handling);
        }

        match projection {
            Some(projection) if projection.is_empty() => {
                scan.empty_project()?;
            }
            Some(projection) => {
                let mut columns = Vec::with_capacity(projection.len());
                for field_idx in projection {
                    if Some(*field_idx) == self.row_id_idx {
                        scan.with_row_id();
                    } else if Some(*field_idx) == self.row_addr_idx {
                        scan.with_row_address();
                    } else {
                        columns.push(self.full_schema.field(*field_idx).name());
                    }
                }
                if !columns.is_empty() {
                    scan.project(&columns)?;
                }
            }
            _ => {}
        }

        let combined_filter = match filters.len() {
            0 => None,
            1 => Some(filters[0].clone()),
            _ => {
                let mut expr = filters[0].clone();
                for filter in &filters[1..] {
                    expr = Expr::and(expr, filter.clone());
                }
                Some(expr)
            }
        };
        if let Some(combined_filter) = combined_filter {
            scan.filter_expr(combined_filter);
        }
        scan.limit(limit.map(|l| l as i64), None)?;
        scan.scan_in_order(self.ordered);

        scan.create_plan().await.map_err(DataFusionError::from)
    }

    // Since we are using datafusion itself to apply the filters it should
    // be safe to assume that we can exactly apply any of the given pushdown
    // filters.
    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> datafusion::common::Result<Vec<TableProviderFilterPushDown>> {
        Ok(filters
            .iter()
            .map(|_| TableProviderFilterPushDown::Exact)
            .collect())
    }
}

pub trait SessionContextExt {
    /// Creates a DataFrame for reading a Lance dataset
    fn read_lance(
        &self,
        dataset: Arc<Dataset>,
        with_row_id: bool,
        with_row_addr: bool,
    ) -> datafusion::common::Result<DataFrame>;

    /// Creates a DataFrame for reading a stream of data
    ///
    /// This dataframe may only be queried once, future queries will fail
    fn read_one_shot(
        &self,
        data: SendableRecordBatchStream,
    ) -> datafusion::common::Result<DataFrame>;
}

pub struct OneShotPartitionStream {
    data: Arc<Mutex<Option<SendableRecordBatchStream>>>,
    schema: Arc<Schema>,
}

impl OneShotPartitionStream {
    pub fn new(data: SendableRecordBatchStream) -> Self {
        let schema = data.schema();
        Self {
            data: Arc::new(Mutex::new(Some(data))),
            schema,
        }
    }
}

impl std::fmt::Debug for OneShotPartitionStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OneShotPartitionStream")
            .field("schema", &self.schema)
            .finish()
    }
}

impl PartitionStream for OneShotPartitionStream {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, _ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        let mut stream = self.data.lock().unwrap();
        stream
            .take()
            .expect("Attempt to consume a one shot dataframe multiple times")
    }
}

impl SessionContextExt for SessionContext {
    fn read_lance(
        &self,
        dataset: Arc<Dataset>,
        with_row_id: bool,
        with_row_addr: bool,
    ) -> datafusion::common::Result<DataFrame> {
        self.read_table(Arc::new(LanceTableProvider::new(
            dataset,
            with_row_id,
            with_row_addr,
        )))
    }

    fn read_one_shot(
        &self,
        data: SendableRecordBatchStream,
    ) -> datafusion::common::Result<DataFrame> {
        let schema = data.schema();
        let part_stream = Arc::new(OneShotPartitionStream::new(data));
        let provider = StreamingTable::try_new(schema, vec![part_stream])?;
        self.read_table(Arc::new(provider))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::{
        array::AsArray,
        datatypes::{Int32Type, Int64Type},
    };
    use datafusion::prelude::SessionContext;
    use lance_core::utils::tempfile::TempStrDir;
    use lance_datagen::array;

    use crate::{
        datafusion::LanceTableProvider,
        utils::test::{DatagenExt, FragmentCount, FragmentRowCount},
    };

    #[tokio::test]
    pub async fn test_table_provider() {
        let test_uri = TempStrDir::default();
        let data = lance_datagen::gen_batch()
            .col("x", array::step::<Int32Type>())
            .col("y", array::step_custom::<Int32Type>(0, 2))
            .into_dataset(
                &test_uri,
                FragmentCount::from(10),
                FragmentRowCount::from(10),
            )
            .await
            .unwrap();

        let ctx = SessionContext::new();

        ctx.register_table(
            "foo",
            Arc::new(LanceTableProvider::new(Arc::new(data), true, true)),
        )
        .unwrap();

        let df = ctx
            .sql("SELECT SUM(x) FROM foo WHERE y > 100")
            .await
            .unwrap();

        let results = df.collect().await.unwrap();
        assert_eq!(results.len(), 1);
        let results = results.into_iter().next().unwrap();
        assert_eq!(results.num_columns(), 1);
        assert_eq!(results.num_rows(), 1);
        // SUM(0..100) - SUM(0..50) = 3675
        assert_eq!(results.column(0).as_primitive::<Int64Type>().value(0), 3675);
    }
}
