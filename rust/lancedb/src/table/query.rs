// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use std::sync::Arc;

use super::NativeTable;
use crate::error::{Error, Result};
use crate::expr::expr_to_sql_string;
use crate::query::{
    QueryExecutionOptions, QueryFilter, QueryRequest, Select, VectorQueryRequest, DEFAULT_TOP_K,
};
use crate::utils::{default_vector_column, TimeoutStream};
use arrow::array::{AsArray, FixedSizeListBuilder, Float32Builder};
use arrow::datatypes::{Float32Type, UInt8Type};
use arrow_array::Array;
use arrow_schema::{DataType, Schema};
use datafusion_physical_plan::projection::ProjectionExec;
use datafusion_physical_plan::repartition::RepartitionExec;
use datafusion_physical_plan::union::UnionExec;
use datafusion_physical_plan::ExecutionPlan;
use futures::future::try_join_all;
use lance::dataset::scanner::DatasetRecordBatchStream;
use lance::dataset::scanner::Scanner;
use lance_datafusion::exec::{analyze_plan as lance_analyze_plan, execute_plan};
use lance_namespace::models::{
    QueryTableRequest as NsQueryTableRequest, QueryTableRequestColumns,
    QueryTableRequestFullTextQuery, QueryTableRequestVector, StringFtsQuery,
};
use lance_namespace::LanceNamespace;

#[derive(Debug, Clone)]
pub enum AnyQuery {
    Query(QueryRequest),
    VectorQuery(VectorQueryRequest),
}

//Decide between namespace or local
pub async fn execute_query(
    table: &NativeTable,
    query: &AnyQuery,
    options: QueryExecutionOptions,
) -> Result<DatasetRecordBatchStream> {
    // If namespace client is configured, use server-side query execution
    if let Some(ref namespace_client) = table.namespace_client {
        return execute_namespace_query(table, namespace_client.clone(), query, options).await;
    }
    execute_generic_query(table, query, options).await
}

pub async fn analyze_query_plan(
    table: &NativeTable,
    query: &AnyQuery,
    options: QueryExecutionOptions,
) -> Result<String> {
    let plan = create_plan(table, query, options).await?;
    Ok(lance_analyze_plan(plan, Default::default()).await?)
}

/// Local Execution Path (DataFusion)
async fn execute_generic_query(
    table: &NativeTable,
    query: &AnyQuery,
    options: QueryExecutionOptions,
) -> Result<DatasetRecordBatchStream> {
    let plan = create_plan(table, query, options.clone()).await?;
    let inner = execute_plan(plan, Default::default())?;
    let inner = if let Some(timeout) = options.timeout {
        TimeoutStream::new_boxed(inner, timeout)
    } else {
        inner
    };
    Ok(DatasetRecordBatchStream::new(inner))
}

pub async fn create_plan(
    table: &NativeTable,
    query: &AnyQuery,
    options: QueryExecutionOptions,
) -> Result<Arc<dyn ExecutionPlan>> {
    let query = match query {
        AnyQuery::VectorQuery(query) => query.clone(),
        AnyQuery::Query(query) => VectorQueryRequest::from_plain_query(query.clone()),
    };

    let ds_ref = table.dataset.get().await?;
    let schema = ds_ref.schema();
    let mut column = query.column.clone();

    let mut query_vector = query.query_vector.first().cloned();
    if query.query_vector.len() > 1 {
        if column.is_none() {
            // Infer a vector column with the same dimension of the query vector.
            let arrow_schema = Schema::from(ds_ref.schema());
            column = Some(default_vector_column(
                &arrow_schema,
                Some(query.query_vector[0].len() as i32),
            )?);
        }
        let vector_field = schema.field(column.as_ref().unwrap()).unwrap();
        if let DataType::List(_) = vector_field.data_type() {
            // Multivector handling: concatenate into FixedSizeList<FixedSizeList<_>>
            let vectors = query
                .query_vector
                .iter()
                .map(|arr| arr.as_ref())
                .collect::<Vec<_>>();
            let dim = vectors[0].len();
            let mut fsl_builder = FixedSizeListBuilder::with_capacity(
                Float32Builder::with_capacity(dim),
                dim as i32,
                vectors.len(),
            );
            for vec in vectors {
                fsl_builder
                    .values()
                    .append_slice(vec.as_primitive::<Float32Type>().values());
                fsl_builder.append(true);
            }
            query_vector = Some(Arc::new(fsl_builder.finish()));
        } else {
            // Multiple query vectors: create a plan for each and union them
            let query_vecs = query.query_vector.clone();
            let plan_futures = query_vecs
                .into_iter()
                .map(|query_vector| {
                    let mut sub_query = query.clone();
                    sub_query.query_vector = vec![query_vector];
                    let options_ref = options.clone();
                    async move {
                        create_plan(table, &AnyQuery::VectorQuery(sub_query), options_ref).await
                    }
                })
                .collect::<Vec<_>>();
            let plans = try_join_all(plan_futures).await?;
            return create_multi_vector_plan(plans);
        }
    }

    let mut scanner: Scanner = ds_ref.scan();

    if let Some(query_vector) = query_vector {
        let column = if let Some(col) = column {
            col
        } else {
            let arrow_schema = Schema::from(ds_ref.schema());
            default_vector_column(&arrow_schema, Some(query_vector.len() as i32))?
        };

        let (_, element_type) = lance::index::vector::utils::get_vector_type(schema, &column)?;
        let is_binary = matches!(element_type, DataType::UInt8);
        let top_k = query.base.limit.unwrap_or(DEFAULT_TOP_K) + query.base.offset.unwrap_or(0);

        if is_binary {
            let query_vector = arrow::compute::cast(&query_vector, &DataType::UInt8)?;
            let query_vector = query_vector.as_primitive::<UInt8Type>();
            scanner.nearest(&column, query_vector, top_k)?;
        } else {
            scanner.nearest(&column, query_vector.as_ref(), top_k)?;
        }

        scanner.minimum_nprobes(query.minimum_nprobes);
        if let Some(maximum_nprobes) = query.maximum_nprobes {
            scanner.maximum_nprobes(maximum_nprobes);
        }
    }

    scanner.limit(
        query.base.limit.map(|limit| limit as i64),
        query.base.offset.map(|offset| offset as i64),
    )?;

    if let Some(ef) = query.ef {
        scanner.ef(ef);
    }

    scanner.distance_range(query.lower_bound, query.upper_bound);
    scanner.use_index(query.use_index);
    scanner.prefilter(query.base.prefilter);

    match query.base.select {
        Select::Columns(ref columns) => {
            scanner.project(columns.as_slice())?;
        }
        Select::Dynamic(ref select_with_transform) => {
            scanner.project_with_transform(select_with_transform.as_slice())?;
        }
        Select::All => {}
    }

    if query.base.with_row_id {
        scanner.with_row_id();
    }

    scanner.batch_size(options.max_batch_length as usize);

    if query.base.fast_search {
        scanner.fast_search();
    }

    if let Some(filter) = &query.base.filter {
        match filter {
            QueryFilter::Sql(sql) => {
                scanner.filter(sql)?;
            }
            QueryFilter::Substrait(substrait) => {
                scanner.filter_substrait(substrait)?;
            }
            QueryFilter::Datafusion(expr) => {
                scanner.filter_expr(expr.clone());
            }
        }
    }

    if let Some(fts) = &query.base.full_text_search {
        scanner.full_text_search(fts.clone())?;
    }

    if let Some(refine_factor) = query.refine_factor {
        scanner.refine(refine_factor);
    }

    if let Some(distance_type) = query.distance_type {
        scanner.distance_metric(distance_type.into());
    }

    if query.base.disable_scoring_autoprojection {
        scanner.disable_scoring_autoprojection();
    }

    Ok(scanner.create_plan().await?)
}

//Helper functions below

// Take many execution plans and map them into a single plan that adds
// a query_index column and unions them.
pub(crate) fn create_multi_vector_plan(
    plans: Vec<Arc<dyn ExecutionPlan>>,
) -> Result<Arc<dyn ExecutionPlan>> {
    if plans.is_empty() {
        return Err(Error::InvalidInput {
            message: "No plans provided".to_string(),
        });
    }
    // Projection to keeping all existing columns
    let first_plan = plans[0].clone();
    let project_all_columns = first_plan
        .schema()
        .fields()
        .iter()
        .enumerate()
        .map(|(i, field)| {
            let expr = datafusion_physical_plan::expressions::Column::new(field.name().as_str(), i);
            let expr = Arc::new(expr) as Arc<dyn datafusion_physical_plan::PhysicalExpr>;
            (expr, field.name().clone())
        })
        .collect::<Vec<_>>();

    let projected_plans = plans
        .into_iter()
        .enumerate()
        .map(|(plan_i, plan)| {
            let query_index = datafusion_common::ScalarValue::Int32(Some(plan_i as i32));
            let query_index_expr = datafusion_physical_plan::expressions::Literal::new(query_index);
            let query_index_expr =
                Arc::new(query_index_expr) as Arc<dyn datafusion_physical_plan::PhysicalExpr>;
            let mut projections = vec![(query_index_expr, "query_index".to_string())];
            projections.extend_from_slice(&project_all_columns);
            let projection = ProjectionExec::try_new(projections, plan).unwrap();
            Arc::new(projection) as Arc<dyn datafusion_physical_plan::ExecutionPlan>
        })
        .collect::<Vec<_>>();

    let unioned = UnionExec::try_new(projected_plans).map_err(|err| Error::Runtime {
        message: err.to_string(),
    })?;
    // We require 1 partition in the final output
    let repartitioned = RepartitionExec::try_new(
        unioned,
        datafusion_physical_plan::Partitioning::RoundRobinBatch(1),
    )
    .unwrap();
    Ok(Arc::new(repartitioned))
}

/// Execute a query on the namespace server instead of locally.
async fn execute_namespace_query(
    table: &NativeTable,
    namespace_client: Arc<dyn LanceNamespace>,
    query: &AnyQuery,
    _options: QueryExecutionOptions,
) -> Result<DatasetRecordBatchStream> {
    // Build table_id from namespace + table name
    let mut table_id = table.namespace.clone();
    table_id.push(table.name.clone());

    // Convert AnyQuery to namespace QueryTableRequest
    let mut ns_request = convert_to_namespace_query(query)?;
    // Set the table ID on the request
    ns_request.id = Some(table_id);

    // Call the namespace query_table API
    let response_bytes = namespace_client
        .query_table(ns_request)
        .await
        .map_err(|e| Error::Runtime {
            message: format!("Failed to execute server-side query: {}", e),
        })?;

    // Parse the Arrow IPC response into a RecordBatchStream
    parse_arrow_ipc_response(response_bytes).await
}

/// Convert an AnyQuery to the namespace QueryTableRequest format.
fn convert_to_namespace_query(query: &AnyQuery) -> Result<NsQueryTableRequest> {
    match query {
        AnyQuery::VectorQuery(vq) => {
            // Extract the query vector(s)
            let vector = extract_query_vector(&vq.query_vector)?;

            // Convert filter to SQL string
            let filter = match &vq.base.filter {
                Some(f) => Some(filter_to_sql(f)?),
                None => None,
            };

            // Convert select to columns list
            let columns = match &vq.base.select {
                Select::All => None,
                Select::Columns(cols) => Some(Box::new(QueryTableRequestColumns {
                    column_names: Some(cols.clone()),
                    column_aliases: None,
                })),
                Select::Dynamic(_) => {
                    return Err(Error::NotSupported {
                        message:
                            "Dynamic column selection is not supported for server-side queries"
                                .to_string(),
                    });
                }
            };

            // Check for unsupported features
            if vq.base.reranker.is_some() {
                return Err(Error::NotSupported {
                    message: "Reranker is not supported for server-side queries".to_string(),
                });
            }

            // Convert FTS query if present
            let full_text_query = vq.base.full_text_search.as_ref().map(|fts| {
                let columns = fts.columns();
                let columns_vec = if columns.is_empty() {
                    None
                } else {
                    Some(columns.into_iter().collect())
                };
                Box::new(QueryTableRequestFullTextQuery {
                    string_query: Some(Box::new(StringFtsQuery {
                        query: fts.query.to_string(),
                        columns: columns_vec,
                    })),
                    structured_query: None,
                })
            });

            Ok(NsQueryTableRequest {
                id: None, // Will be set in namespace_query
                k: vq.base.limit.unwrap_or(10) as i32,
                vector: Box::new(vector),
                vector_column: vq.column.clone(),
                filter,
                columns,
                offset: vq.base.offset.map(|o| o as i32),
                distance_type: vq.distance_type.map(|dt| dt.to_string()),
                nprobes: Some(vq.minimum_nprobes as i32),
                ef: vq.ef.map(|e| e as i32),
                refine_factor: vq.refine_factor.map(|r| r as i32),
                lower_bound: vq.lower_bound,
                upper_bound: vq.upper_bound,
                prefilter: Some(vq.base.prefilter),
                fast_search: Some(vq.base.fast_search),
                with_row_id: Some(vq.base.with_row_id),
                bypass_vector_index: Some(!vq.use_index),
                full_text_query,
                ..Default::default()
            })
        }
        AnyQuery::Query(q) => {
            // For non-vector queries, pass an empty vector (similar to remote table implementation)
            if q.reranker.is_some() {
                return Err(Error::NotSupported {
                    message: "Reranker is not supported for server-side query execution"
                        .to_string(),
                });
            }

            let filter = q.filter.as_ref().map(filter_to_sql).transpose()?;

            let columns = match &q.select {
                Select::All => None,
                Select::Columns(cols) => Some(Box::new(QueryTableRequestColumns {
                    column_names: Some(cols.clone()),
                    column_aliases: None,
                })),
                Select::Dynamic(_) => {
                    return Err(Error::NotSupported {
                        message: "Dynamic columns are not supported for server-side query"
                            .to_string(),
                    });
                }
            };

            // Handle full text search if present
            let full_text_query = q.full_text_search.as_ref().map(|fts| {
                let columns_vec = if fts.columns().is_empty() {
                    None
                } else {
                    Some(fts.columns().iter().cloned().collect())
                };
                Box::new(QueryTableRequestFullTextQuery {
                    string_query: Some(Box::new(StringFtsQuery {
                        query: fts.query.to_string(),
                        columns: columns_vec,
                    })),
                    structured_query: None,
                })
            });

            // Empty vector for non-vector queries
            let vector = Box::new(QueryTableRequestVector {
                single_vector: Some(vec![]),
                multi_vector: None,
            });

            Ok(NsQueryTableRequest {
                id: None, // Will be set by caller
                vector,
                k: q.limit.unwrap_or(10) as i32,
                filter,
                columns,
                prefilter: Some(q.prefilter),
                offset: q.offset.map(|o| o as i32),
                vector_column: None, // No vector column for plain queries
                with_row_id: Some(q.with_row_id),
                bypass_vector_index: Some(true), // No vector index for plain queries
                full_text_query,
                ..Default::default()
            })
        }
    }
}

fn filter_to_sql(filter: &QueryFilter) -> Result<String> {
    match filter {
        QueryFilter::Sql(sql) => Ok(sql.clone()),
        QueryFilter::Substrait(_) => Err(Error::NotSupported {
            message: "Substrait filters are not supported for server-side queries".to_string(),
        }),
        QueryFilter::Datafusion(expr) => expr_to_sql_string(expr),
    }
}

/// Extract query vector(s) from Arrow arrays into the namespace format.
fn extract_query_vector(
    query_vectors: &[Arc<dyn arrow_array::Array>],
) -> Result<QueryTableRequestVector> {
    if query_vectors.is_empty() {
        return Err(Error::InvalidInput {
            message: "Query vector is required for vector search".to_string(),
        });
    }

    // Handle single vector case
    if query_vectors.len() == 1 {
        let arr = &query_vectors[0];
        let single_vector = array_to_f32_vec(arr)?;
        Ok(QueryTableRequestVector {
            single_vector: Some(single_vector),
            multi_vector: None,
        })
    } else {
        // Handle multi-vector case
        let multi_vector: Result<Vec<Vec<f32>>> =
            query_vectors.iter().map(array_to_f32_vec).collect();
        Ok(QueryTableRequestVector {
            single_vector: None,
            multi_vector: Some(multi_vector?),
        })
    }
}

/// Convert an Arrow array to a Vec<f32>.
fn array_to_f32_vec(arr: &Arc<dyn arrow_array::Array>) -> Result<Vec<f32>> {
    // Handle FixedSizeList (common for vectors)
    if let Some(fsl) = arr
        .as_any()
        .downcast_ref::<arrow_array::FixedSizeListArray>()
    {
        let values = fsl.values();
        if let Some(f32_arr) = values.as_any().downcast_ref::<arrow_array::Float32Array>() {
            return Ok(f32_arr.values().to_vec());
        }
    }

    // Handle direct Float32Array
    if let Some(f32_arr) = arr.as_any().downcast_ref::<arrow_array::Float32Array>() {
        return Ok(f32_arr.values().to_vec());
    }

    Err(Error::InvalidInput {
        message: "Query vector must be Float32 type".to_string(),
    })
}

/// Parse Arrow IPC response from the namespace server.
async fn parse_arrow_ipc_response(bytes: bytes::Bytes) -> Result<DatasetRecordBatchStream> {
    use arrow_ipc::reader::StreamReader;
    use std::io::Cursor;

    let cursor = Cursor::new(bytes);
    let reader = StreamReader::try_new(cursor, None).map_err(|e| Error::Runtime {
        message: format!("Failed to parse Arrow IPC response: {}", e),
    })?;

    // Collect all record batches
    let schema = reader.schema();
    let batches: Vec<_> = reader
        .into_iter()
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|e| Error::Runtime {
            message: format!("Failed to read Arrow IPC batches: {}", e),
        })?;

    // Create a stream from the batches
    let stream = futures::stream::iter(batches.into_iter().map(Ok));
    let record_batch_stream =
        Box::pin(datafusion_physical_plan::stream::RecordBatchStreamAdapter::new(schema, stream));

    Ok(DatasetRecordBatchStream::new(record_batch_stream))
}

#[cfg(test)]
#[allow(deprecated)]
mod tests {
    use arrow_array::Float32Array;
    use futures::TryStreamExt;
    use std::sync::Arc;

    use super::*;
    use crate::query::QueryExecutionOptions;

    #[test]
    fn test_convert_to_namespace_query_vector() {
        let query_vector = Arc::new(Float32Array::from(vec![1.0, 2.0, 3.0, 4.0]));

        let vq = VectorQueryRequest {
            base: QueryRequest {
                limit: Some(10),
                offset: Some(5),
                filter: Some(QueryFilter::Sql("id > 0".to_string())),
                select: Select::Columns(vec!["id".to_string()]),
                ..Default::default()
            },
            column: Some("vector".to_string()),
            // We cast here to satisfy the struct definition
            query_vector: vec![query_vector as Arc<dyn Array>],
            minimum_nprobes: 20,
            distance_type: Some(crate::DistanceType::L2),
            ..Default::default()
        };

        let any_query = AnyQuery::VectorQuery(vq);

        let ns_request = convert_to_namespace_query(&any_query).unwrap();

        assert_eq!(ns_request.k, 10);
        assert_eq!(ns_request.offset, Some(5));
        assert_eq!(ns_request.filter, Some("id > 0".to_string()));
        assert_eq!(
            ns_request
                .columns
                .as_ref()
                .and_then(|c| c.column_names.as_ref()),
            Some(&vec!["id".to_string()])
        );
        assert_eq!(ns_request.vector_column, Some("vector".to_string()));
        assert_eq!(ns_request.distance_type, Some("l2".to_string()));

        // Verify the vector data was extracted correctly
        assert!(ns_request.vector.single_vector.is_some());
        assert_eq!(
            ns_request.vector.single_vector.as_ref().unwrap(),
            &vec![1.0, 2.0, 3.0, 4.0]
        );
    }

    #[test]
    fn test_convert_to_namespace_query_plain_query() {
        let q = QueryRequest {
            limit: Some(20),
            offset: Some(5),
            filter: Some(QueryFilter::Sql("id > 5".to_string())),
            select: Select::Columns(vec!["id".to_string()]),
            with_row_id: true,
            ..Default::default()
        };

        let any_query = AnyQuery::Query(q);

        let ns_request = convert_to_namespace_query(&any_query).unwrap();

        assert_eq!(ns_request.k, 20);
        assert_eq!(ns_request.offset, Some(5));
        assert_eq!(ns_request.filter, Some("id > 5".to_string()));
        assert_eq!(
            ns_request
                .columns
                .as_ref()
                .and_then(|c| c.column_names.as_ref()),
            Some(&vec!["id".to_string()])
        );
        assert_eq!(ns_request.with_row_id, Some(true));
        assert_eq!(ns_request.bypass_vector_index, Some(true));
        assert!(ns_request.vector_column.is_none());

        assert!(ns_request.vector.single_vector.as_ref().unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_execute_query_local_routing() {
        use crate::connect;
        use crate::table::query::execute_query;
        use arrow_array::{Int32Array, RecordBatch};
        use arrow_schema::{DataType, Field, Schema};

        let conn = connect("memory://").execute().await.unwrap();

        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5]))],
        )
        .unwrap();

        let table = conn
            .create_table("test_routing", vec![batch])
            .execute()
            .await
            .unwrap();

        let native_table = table.as_native().unwrap();

        // Setup a request
        let req = QueryRequest {
            filter: Some(QueryFilter::Sql("id > 3".to_string())),
            ..Default::default()
        };
        let query = AnyQuery::Query(req);

        // Action: Call execute_query directly
        // This validates that execute_query correctly routes to the local DataFusion engine
        // when table.namespace_client is None.
        let stream = execute_query(native_table, &query, QueryExecutionOptions::default())
            .await
            .unwrap();

        // Verify results
        let batches = stream.try_collect::<Vec<_>>().await.unwrap();
        let count: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(count, 2); // 4 and 5
    }

    #[tokio::test]
    async fn test_create_plan_multivector_structure() {
        use arrow_array::{Float32Array, RecordBatch};
        use arrow_schema::{DataType, Field, Schema};
        use datafusion_physical_plan::display::DisplayableExecutionPlan;

        use crate::table::query::create_plan;

        use crate::connect;

        let conn = connect("memory://").execute().await.unwrap();
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new(
                "vector",
                DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), 2),
                false,
            ),
        ]));

        let batch = RecordBatch::new_empty(schema.clone());
        let table = conn
            .create_table("test_plan", vec![batch])
            .execute()
            .await
            .unwrap();
        let native_table = table.as_native().unwrap();

        // This triggers the "create_multi_vector_plan" logic branch
        let q1 = Arc::new(Float32Array::from(vec![1.0, 2.0]));
        let q2 = Arc::new(Float32Array::from(vec![3.0, 4.0]));

        let req = VectorQueryRequest {
            column: Some("vector".to_string()),
            query_vector: vec![q1, q2],
            ..Default::default()
        };
        let query = AnyQuery::VectorQuery(req);

        // Create the Plan
        let plan = create_plan(native_table, &query, QueryExecutionOptions::default())
            .await
            .unwrap();

        // formatting it allows us to see the hierarchy
        let display = DisplayableExecutionPlan::new(plan.as_ref())
            .indent(true)
            .to_string();

        // We expect a RepartitionExec wrapping a UnionExec
        assert!(
            display.contains("RepartitionExec"),
            "Plan should include Repartitioning"
        );
        assert!(
            display.contains("UnionExec"),
            "Plan should include a Union of multiple searches"
        );
        // We expect the projection to add the 'query_index' column (logic inside multi_vector_plan)
        assert!(
            display.contains("query_index"),
            "Plan should add query_index column"
        );
    }
}
