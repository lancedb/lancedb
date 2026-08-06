// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use crate::Dataset;
use arrow_schema::{Schema, SchemaRef};
use async_trait::async_trait;
use datafusion::catalog::{Session, TableFunctionImpl, TableProvider};
use datafusion::common::{DataFusionError, ScalarValue};
use datafusion_expr::{Expr, TableType};
use datafusion_physical_plan::ExecutionPlan;
use lance_arrow::SchemaExt;
use lance_core::{Error, ROW_ADDR_FIELD, ROW_ID_FIELD};
use lance_index::scalar::FullTextSearchQuery;
use lance_index::scalar::inverted::parser::from_json;
use serde_json::Value;
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;

/// Provide a table based on full text search query.
#[derive(Debug)]
struct FtsTableProvider {
    dataset: Arc<Dataset>,
    fts_query: FullTextSearchQuery,
    full_schema: Arc<Schema>,
    row_id_idx: Option<usize>,
    row_addr_idx: Option<usize>,
    ordered: bool,
}

impl FtsTableProvider {
    pub fn new(
        dataset: Arc<Dataset>,
        fts_query: FullTextSearchQuery,
        with_row_id: bool,
        with_row_addr: bool,
        ordered: bool,
    ) -> Self {
        let mut full_schema = Schema::from(dataset.schema());
        let mut row_id_idx = None;
        let mut row_addr_idx = None;
        if with_row_id {
            full_schema = full_schema.try_with_column(ROW_ID_FIELD.clone()).unwrap();
            row_id_idx = Some(full_schema.fields().len() - 1);
        }
        if with_row_addr {
            full_schema = full_schema.try_with_column(ROW_ADDR_FIELD.clone()).unwrap();
            row_addr_idx = Some(full_schema.fields().len() - 1);
        }
        Self {
            dataset,
            fts_query,
            full_schema: Arc::new(full_schema),
            row_id_idx,
            row_addr_idx,
            ordered,
        }
    }
}

#[async_trait]
impl TableProvider for FtsTableProvider {
    fn schema(&self) -> SchemaRef {
        self.full_schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Temporary
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        let mut scan = self.dataset.scan();
        scan.full_text_search(self.fts_query.clone())?;

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
}

/// This function creates a virtual table from the input fts query.
///
/// It takes 3 parameters:
/// 1. table_name: the name of the table to be queried.
/// 2. fts_query: the fts query in json format.
/// 3. options: the query options in json format.
///
/// ```ignore
/// use crate::datafusion::LanceTableProvider;
/// use crate::dataset::udtf::FtsQueryUDTF;
/// use crate::dataset::udtf::FtsQueryUDTFBuilder;
/// use datafusion::prelude::SessionContext;
///
/// let ctx = SessionContext::new();
/// let fts_query_udtf = FtsQueryUDTFBuilder::builder()
///   .register_table("HarryPotter_Chapter1", data1.clone())
///   .register_table("HarryPotter_Chapter2", data2.clone())
///   .build();
/// ctx.register_udtf("fts", Arc::new(fts_query_udtf));
///
/// let fts_query = r#"
///             {
///                 "match": {
///                     "column": "text",
///                     "terms": "catch fish",
///                     "operator": "And"
///                 }
///             }
///      "#;
/// let options = r#"
///             {
///                 "with_row_id": true
///             }
///      "#;
/// let df = ctx
///  .sql(&format!("SELECT * FROM fts('HarryPotter_Chapter1', '{}', '{}') WHERE number > 1",
///        fts_query, options))
///  .await
///  .unwrap();
/// ```
#[derive(Debug)]
pub struct FtsQueryUDTF {
    datasets: HashMap<String, Arc<Dataset>>,
}

impl TableFunctionImpl for FtsQueryUDTF {
    fn call(&self, expr: &[Expr]) -> datafusion::common::Result<Arc<dyn TableProvider>> {
        // Parse params: table_name, fts_query, options
        if expr.len() < 2 || expr.len() > 3 {
            return Err(DataFusionError::Execution(
                "FtsQueryUDTF function takes table_name, fts_query and optional options as parameters".to_string(),
            ));
        }

        let Some(Expr::Literal(ScalarValue::Utf8(Some(table_name)), _)) = expr.first() else {
            return Err(DataFusionError::Execution(
                "FtsQueryUDTF first argument should be table name in string".to_string(),
            ));
        };

        let Some(Expr::Literal(ScalarValue::Utf8(Some(fts_query)), _)) = expr.get(1) else {
            return Err(DataFusionError::Execution(
                "FtsQueryUDTF second argument should be fts query in json format".to_string(),
            ));
        };

        let (with_row_id, with_row_addr, ordered) =
            if let Some(Expr::Literal(ScalarValue::Utf8(Some(options)), _)) = expr.get(2) {
                parse_query_options(options)?
            } else {
                (false, false, false)
            };

        // Fts query
        let dataset = self
            .datasets
            .get(table_name)
            .ok_or_else(|| DataFusionError::Execution(format!("Table {} not found", table_name)))?;

        let provider = FtsTableProvider::new(
            dataset.clone(),
            FullTextSearchQuery::new_query(from_json(fts_query)?),
            with_row_id,
            with_row_addr,
            ordered,
        );
        Ok(Arc::new(provider))
    }
}

fn parse_query_options(options: &str) -> datafusion::common::Result<(bool, bool, bool)> {
    let value: Value = serde_json::from_str(options)
        .map_err(|e| Error::invalid_input(format!("invalid json options: {}", e)))?;
    let with_row_id = value
        .get("with_row_id")
        .is_some_and(|v| v.as_bool().unwrap_or(false));
    let with_row_addr = value
        .get("with_row_addr")
        .is_some_and(|v| v.as_bool().unwrap_or(false));
    let ordered = value
        .get("ordered")
        .is_some_and(|v| v.as_bool().unwrap_or(false));
    Ok((with_row_id, with_row_addr, ordered))
}

/// Builder of `FtsQueryUDTF`
pub struct FtsQueryUDTFBuilder {
    datasets: HashMap<String, Arc<Dataset>>,
}

impl FtsQueryUDTFBuilder {
    pub fn builder() -> Self {
        Self {
            datasets: HashMap::new(),
        }
    }

    pub fn register_table(mut self, table_name: &str, dataset: Arc<Dataset>) -> Self {
        self.datasets.insert(table_name.to_string(), dataset);
        self
    }

    pub fn build(self) -> FtsQueryUDTF {
        FtsQueryUDTF {
            datasets: self.datasets,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::Dataset;
    use crate::dataset::udtf::FtsQueryUDTFBuilder;
    use crate::index::DatasetIndexExt;
    use arrow_array::{
        Array, Int32Array, RecordBatch, RecordBatchIterator, StringArray, UInt64Array,
    };
    use arrow_schema::{DataType, Field};
    use datafusion::prelude::SessionContext;
    use lance_index::IndexType;
    use lance_index::scalar::InvertedIndexParams;
    use std::sync::Arc;

    #[tokio::test]
    pub async fn test_fts_query_udtf() {
        let text_col = Arc::new(StringArray::from(vec![
            "a cat catch a fish",
            "a fish catch a cat",
            "a white cat catch a big fish",
            "cat catchup fish",
            "cat fish catch",
        ]));
        let number_col = Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5]));

        // Prepare dataset
        let batch = RecordBatch::try_new(
            arrow_schema::Schema::new(vec![
                Field::new("text", DataType::Utf8, false),
                Field::new("number", DataType::Int32, false),
            ])
            .into(),
            vec![text_col.clone(), number_col.clone()],
        )
        .unwrap();
        let schema = batch.schema();
        let stream = RecordBatchIterator::new(vec![batch].into_iter().map(Ok), schema);
        let mut data = Dataset::write(stream, "memory://test/table", None)
            .await
            .unwrap();
        data.create_index(
            &["text"],
            IndexType::Inverted,
            None,
            &InvertedIndexParams::default(),
            true,
        )
        .await
        .unwrap();
        let data = Arc::new(data);

        // Prepare datafusion
        let ctx = Arc::new(SessionContext::new());
        let fts_query_udtf = FtsQueryUDTFBuilder::builder()
            .register_table("foo", data.clone())
            .build();
        ctx.register_udtf("fts", Arc::new(fts_query_udtf));

        // Full text search
        let fts_query = r#"
            {
                "match": {
                    "column": "text",
                    "terms": "catch fish",
                    "operator": "And"
                }
            }
            "#;
        let options = r#"
            {
                "with_row_id": true
            }
            "#;

        let df = ctx
            .sql(&format!(
                "SELECT * FROM fts('foo', '{}', '{}') WHERE number > 1",
                fts_query, options
            ))
            .await
            .unwrap();

        let results = df.collect().await.unwrap();
        assert_eq!(results.len(), 1);
        let results = results.into_iter().next().unwrap();
        assert_eq!(results.num_columns(), 4); // text, number, _score, _rowid
        assert_eq!(results.num_rows(), 3);
        let row_id_col = results.column_by_name("_rowid").unwrap();
        let row_id_col = row_id_col.as_any().downcast_ref::<UInt64Array>().unwrap();
        row_id_col
            .iter()
            .for_each(|v| assert!([1u64, 2u64, 4u64].contains(&v.unwrap())));
    }
}
