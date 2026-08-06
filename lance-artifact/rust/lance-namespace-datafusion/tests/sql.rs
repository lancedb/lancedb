// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

#![recursion_limit = "256"]

use std::sync::Arc;

use arrow_array::{Int32Array, Int64Array, RecordBatch, RecordBatchIterator, StringArray};
use arrow_schema::Schema;
use datafusion::common::record_batch;
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::prelude::SessionContext;
use lance::Dataset;
use lance::dataset::{WriteMode, WriteParams};
use lance_namespace::LanceNamespace;
use lance_namespace::models::CreateNamespaceRequest;
use lance_namespace_datafusion::{NamespaceLevel, SessionBuilder};
use lance_namespace_impls::DirectoryNamespaceBuilder;
use tempfile::TempDir;

struct Context {
    #[allow(dead_code)]
    root_dir: TempDir,
    #[allow(dead_code)]
    extra_dir: TempDir,
    ctx: SessionContext,
}

fn col<T: 'static>(batch: &RecordBatch, idx: usize) -> &T {
    batch.column(idx).as_any().downcast_ref::<T>().unwrap()
}

fn customers_data() -> (Arc<Schema>, RecordBatch) {
    let batch = record_batch!(
        ("customer_id", Int32, vec![1, 2, 3]),
        ("name", Utf8, vec!["Alice", "Bob", "Carol"]),
        ("city", Utf8, vec!["NY", "SF", "LA"])
    )
    .unwrap();
    let schema = batch.schema();

    (schema, batch)
}

fn orders_data() -> (Arc<Schema>, RecordBatch) {
    let batch = record_batch!(
        ("order_id", Int32, vec![101, 102, 103]),
        ("customer_id", Int32, vec![1, 2, 3]),
        ("amount", Int32, vec![100, 200, 300])
    )
    .unwrap();
    let schema = batch.schema();

    (schema, batch)
}

fn orders2_data() -> (Arc<Schema>, RecordBatch) {
    let batch = record_batch!(
        ("order_id", Int32, vec![201, 202]),
        ("customer_id", Int32, vec![1, 2]),
        ("amount", Int32, vec![150, 250])
    )
    .unwrap();
    let schema = batch.schema();

    (schema, batch)
}

fn customers_dim_data() -> (Arc<Schema>, RecordBatch) {
    let batch = record_batch!(
        ("customer_id", Int32, vec![1, 2, 3]),
        ("segment", Utf8, vec!["Silver", "Gold", "Platinum"])
    )
    .unwrap();
    let schema = batch.schema();

    (schema, batch)
}

async fn write_table(
    dir: &TempDir,
    file_name: &str,
    schema: Arc<Schema>,
    batch: RecordBatch,
) -> DFResult<()> {
    let full_path = dir.path().join(file_name);
    if let Some(parent) = full_path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    let uri = full_path.to_str().unwrap().to_string();
    let reader = RecordBatchIterator::new(vec![Ok(batch)], schema);
    let write_params = WriteParams {
        mode: WriteMode::Create,
        ..Default::default()
    };

    Dataset::write(reader, &uri, Some(write_params))
        .await
        .map_err(|e| DataFusionError::Execution(e.to_string()))?;

    Ok(())
}

async fn setup_test_context() -> DFResult<Context> {
    let root_dir = TempDir::new()?;
    let extra_dir = TempDir::new()?;

    let (customers_schema, customers_batch) = customers_data();
    write_table(
        &root_dir,
        "retail$sales$customers.lance",
        customers_schema,
        customers_batch,
    )
    .await?;

    let (orders_schema, orders_batch) = orders_data();
    write_table(
        &root_dir,
        "retail$sales$orders.lance",
        orders_schema,
        orders_batch,
    )
    .await?;

    let (orders2_schema, orders2_batch) = orders2_data();
    write_table(
        &root_dir,
        "wholesale$sales2$orders2.lance",
        orders2_schema,
        orders2_batch,
    )
    .await?;

    let (dim_schema, dim_batch) = customers_dim_data();
    write_table(
        &extra_dir,
        "crm$dim$customers_dim.lance",
        dim_schema,
        dim_batch,
    )
    .await?;

    let root_path = root_dir.path().to_string_lossy().to_string();
    let root_dir_ns = DirectoryNamespaceBuilder::new(root_path)
        .manifest_enabled(true)
        .dir_listing_enabled(true)
        .build()
        .await
        .map_err(|e| DataFusionError::Execution(e.to_string()))?;

    let extra_path = extra_dir.path().to_string_lossy().to_string();
    let extra_dir_ns = DirectoryNamespaceBuilder::new(extra_path)
        .manifest_enabled(true)
        .dir_listing_enabled(true)
        .build()
        .await
        .map_err(|e| DataFusionError::Execution(e.to_string()))?;

    // Create nested namespaces for retail / wholesale / crm.
    let mut create_retail = CreateNamespaceRequest::new();
    create_retail.id = Some(vec!["retail".to_string()]);
    root_dir_ns
        .create_namespace(create_retail)
        .await
        .map_err(|e| DataFusionError::Execution(e.to_string()))?;

    let mut create_sales = CreateNamespaceRequest::new();
    create_sales.id = Some(vec!["retail".to_string(), "sales".to_string()]);
    root_dir_ns
        .create_namespace(create_sales)
        .await
        .map_err(|e| DataFusionError::Execution(e.to_string()))?;

    let mut create_wholesale = CreateNamespaceRequest::new();
    create_wholesale.id = Some(vec!["wholesale".to_string()]);
    root_dir_ns
        .create_namespace(create_wholesale)
        .await
        .map_err(|e| DataFusionError::Execution(e.to_string()))?;

    let mut create_sales2 = CreateNamespaceRequest::new();
    create_sales2.id = Some(vec!["wholesale".to_string(), "sales2".to_string()]);
    root_dir_ns
        .create_namespace(create_sales2)
        .await
        .map_err(|e| DataFusionError::Execution(e.to_string()))?;

    let mut create_crm = CreateNamespaceRequest::new();
    create_crm.id = Some(vec!["crm".to_string()]);
    extra_dir_ns
        .create_namespace(create_crm)
        .await
        .map_err(|e| DataFusionError::Execution(e.to_string()))?;

    let mut create_dim = CreateNamespaceRequest::new();
    create_dim.id = Some(vec!["crm".to_string(), "dim".to_string()]);
    extra_dir_ns
        .create_namespace(create_dim)
        .await
        .map_err(|e| DataFusionError::Execution(e.to_string()))?;

    root_dir_ns
        .migrate()
        .await
        .map_err(|e| DataFusionError::Execution(e.to_string()))?;

    extra_dir_ns
        .migrate()
        .await
        .map_err(|e| DataFusionError::Execution(e.to_string()))?;

    let root_ns: Arc<dyn LanceNamespace> = Arc::new(root_dir_ns);
    let extra_ns: Arc<dyn LanceNamespace> = Arc::new(extra_dir_ns);

    let ctx = SessionBuilder::new()
        .with_root(NamespaceLevel::from_root(Arc::clone(&root_ns)))
        .add_catalog(
            "crm",
            NamespaceLevel::from_namespace(Arc::clone(&extra_ns), vec!["crm".to_string()]),
        )
        .build()
        .await?;

    Ok(Context {
        root_dir,
        extra_dir,
        ctx,
    })
}

#[tokio::test]
async fn join_within_retail() -> DFResult<()> {
    let ns = setup_test_context().await?;

    let df = ns
        .ctx
        .sql(
            "SELECT customers.name, orders.amount \
             FROM retail.sales.customers customers \
             JOIN retail.sales.orders orders \
               ON customers.customer_id = orders.customer_id \
             WHERE customers.customer_id = 2",
        )
        .await?;
    let batches = df.collect().await?;
    assert_eq!(batches.len(), 1);
    let batch = &batches[0];
    assert_eq!(batch.num_rows(), 1);

    let name_col = col::<StringArray>(batch, 0);
    let amount_col = col::<Int32Array>(batch, 1);

    assert_eq!(name_col.value(0), "Bob");
    assert_eq!(amount_col.value(0), 200);

    Ok(())
}

#[tokio::test]
async fn join_across_root_catalogs() -> DFResult<()> {
    let ns = setup_test_context().await?;

    let df = ns
        .ctx
        .sql(
            "SELECT c.name, o2.amount \
             FROM retail.sales.customers c \
             JOIN wholesale.sales2.orders2 o2 \
               ON c.customer_id = o2.customer_id \
             WHERE o2.order_id = 202",
        )
        .await?;
    let batches = df.collect().await?;
    assert_eq!(batches.len(), 1);
    let batch = &batches[0];
    assert_eq!(batch.num_rows(), 1);

    let name_col = col::<StringArray>(batch, 0);
    let amount_col = col::<Int32Array>(batch, 1);

    assert_eq!(name_col.value(0), "Bob");
    assert_eq!(amount_col.value(0), 250);

    Ok(())
}

#[tokio::test]
async fn join_across_catalogs() -> DFResult<()> {
    let ns = setup_test_context().await?;

    let df = ns
        .ctx
        .sql(
            "SELECT customers.name, dim.segment \
             FROM retail.sales.customers customers \
             JOIN crm.dim.customers_dim dim \
               ON customers.customer_id = dim.customer_id \
             WHERE customers.customer_id = 3",
        )
        .await?;
    let batches = df.collect().await?;
    assert_eq!(batches.len(), 1);
    let batch = &batches[0];
    assert_eq!(batch.num_rows(), 1);

    let name_col = col::<StringArray>(batch, 0);
    let segment_col = col::<StringArray>(batch, 1);

    assert_eq!(name_col.value(0), "Carol");
    assert_eq!(segment_col.value(0), "Platinum");

    Ok(())
}

#[tokio::test]
async fn aggregation_city_totals() -> DFResult<()> {
    let ns = setup_test_context().await?;

    let df = ns
        .ctx
        .sql(
            "SELECT city, SUM(amount) AS total \
             FROM retail.sales.orders o \
             JOIN retail.sales.customers c \
               ON c.customer_id = o.customer_id \
             GROUP BY city \
             ORDER BY city",
        )
        .await?;
    let batches = df.collect().await?;
    assert_eq!(batches.len(), 1);
    let batch = &batches[0];
    assert_eq!(batch.num_rows(), 3);

    let city_col = col::<StringArray>(batch, 0);
    let total_col = col::<Int64Array>(batch, 1);

    assert_eq!(city_col.value(0), "LA");
    assert_eq!(total_col.value(0), 300);

    assert_eq!(city_col.value(1), "NY");
    assert_eq!(total_col.value(1), 100);

    assert_eq!(city_col.value(2), "SF");
    assert_eq!(total_col.value(2), 200);

    Ok(())
}

#[tokio::test]
async fn cte_view_customer_orders() -> DFResult<()> {
    let ns = setup_test_context().await?;

    let df = ns
        .ctx
        .sql(
            "WITH customer_orders AS ( \
                 SELECT c.customer_id, c.name, o.order_id, o.amount \
                 FROM retail.sales.customers c \
                 JOIN retail.sales.orders o \
                   ON c.customer_id = o.customer_id \
             ) \
             SELECT order_id, name, amount FROM customer_orders WHERE customer_id = 1",
        )
        .await?;
    let batches = df.collect().await?;
    assert_eq!(batches.len(), 1);
    let batch = &batches[0];
    assert_eq!(batch.num_rows(), 1);

    let order_id_col = col::<Int32Array>(batch, 0);
    let name_col = col::<StringArray>(batch, 1);
    let amount_col = col::<Int32Array>(batch, 2);

    assert_eq!(order_id_col.value(0), 101);
    assert_eq!(name_col.value(0), "Alice");
    assert_eq!(amount_col.value(0), 100);

    Ok(())
}
