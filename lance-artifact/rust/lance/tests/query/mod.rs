// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::sync::Arc;

use arrow_array::{RecordBatch, UInt32Array, cast::AsArray};
use arrow_select::concat::concat_batches;
use datafusion::datasource::MemTable;
use datafusion::prelude::SessionContext;
use lance::Dataset;
use lance::dataset::scanner::ColumnOrdering;
use lance_datafusion::udf::register_functions;
use lance_index::scalar::FullTextSearchQuery;
use lance_index::scalar::inverted::query::{FtsQuery, PhraseQuery};

/// Creates a fresh SessionContext with Lance UDFs registered
fn create_datafusion_context() -> SessionContext {
    let ctx = SessionContext::new();
    register_functions(&ctx);
    ctx
}

mod inverted;
mod primitives;
mod vectors;

/// Scanning and ordering by id should give same result as original.
async fn test_scan(original: &RecordBatch, ds: &Dataset) {
    let mut scanner = ds.scan();
    scanner
        .order_by(Some(vec![ColumnOrdering::asc_nulls_first(
            "id".to_string(),
        )]))
        .unwrap();
    let scanned = scanner.try_into_batch().await.unwrap();

    assert_eq!(original, &scanned);
}

/// Taking specific rows should give the same result as taking from the original.
async fn test_take(original: &RecordBatch, ds: &Dataset) {
    let num_rows = original.num_rows();
    let cases: Vec<Vec<usize>> = vec![
        vec![0, 1, 2],                    // First few rows
        vec![5, 3, 1],                    // Out of order
        vec![0],                          // Single row
        vec![],                           // Empty
        (0..num_rows.min(10)).collect(),  // Sequential
        vec![num_rows - 1, 0],            // Last and first
        vec![1, 1, 2],                    // Duplicate indices
        vec![0, 0, 0],                    // All same index
        vec![num_rows - 1, num_rows - 1], // Duplicate of last row
    ];

    for indices in cases {
        // Convert to u64 for Lance take
        let indices_u64: Vec<u64> = indices.iter().map(|&i| i as u64).collect();

        let taken_ds = ds.take(&indices_u64, ds.schema().clone()).await.unwrap();

        // Take from RecordBatch using arrow::compute
        let indices_u32: Vec<u32> = indices.iter().map(|&i| i as u32).collect();
        let indices_array = UInt32Array::from(indices_u32);
        let taken_rb = arrow::compute::take_record_batch(original, &indices_array).unwrap();

        assert_eq!(
            taken_rb, taken_ds,
            "Take results don't match for indices: {:?}",
            indices
        );
    }
}

/// Querying with filter should give same result as filtering original
/// record batch in DataFusion.
async fn test_filter(original: &RecordBatch, ds: &Dataset, predicate: &str) {
    // Scan with filter and order
    let mut scanner = ds.scan();
    scanner
        .filter(predicate)
        .unwrap()
        .order_by(Some(vec![ColumnOrdering::asc_nulls_first(
            "id".to_string(),
        )]))
        .unwrap();
    let scanned = scanner.try_into_batch().await.unwrap();

    let ctx = create_datafusion_context();
    let table = MemTable::try_new(original.schema(), vec![vec![original.clone()]]).unwrap();
    ctx.register_table("t", Arc::new(table)).unwrap();

    let sql = format!("SELECT * FROM t WHERE {} ORDER BY id", predicate);
    let df = ctx.sql(&sql).await.unwrap();
    let expected_batches = df.collect().await.unwrap();
    let expected = concat_batches(&original.schema(), &expected_batches).unwrap();

    assert_eq!(&expected, &scanned);
}

// Rebuild a batch using only columns present in the schema (drops _score from FTS results).
fn strip_score_column(batch: &RecordBatch, schema: &arrow_schema::Schema) -> RecordBatch {
    let columns = schema
        .fields()
        .iter()
        .map(|field| batch.column_by_name(field.name()).unwrap().clone())
        .collect::<Vec<_>>();
    RecordBatch::try_new(Arc::new(schema.clone()), columns).unwrap()
}

/// Full text search should match results computed in DataFusion using the constructed SQL
async fn test_fts(
    original: &RecordBatch,
    ds: &Dataset,
    column: &str,
    query: &str,
    filter: Option<&str>,
    lower_case: bool,
    phrase_query: bool,
) {
    // Scan with FTS and order
    let mut scanner = ds.scan();
    let fts_query = if phrase_query {
        let phrase = PhraseQuery::new(query.to_string()).with_column(Some(column.to_string()));
        FullTextSearchQuery::new_query(FtsQuery::Phrase(phrase))
    } else {
        FullTextSearchQuery::new(query.to_string())
            .with_column(column.to_string())
            .unwrap()
    };
    scanner.full_text_search(fts_query).unwrap();
    if let Some(predicate) = filter {
        scanner.filter(predicate).unwrap();
    }
    scanner
        .order_by(Some(vec![ColumnOrdering::asc_nulls_first(
            "id".to_string(),
        )]))
        .unwrap();
    let scanned = scanner.try_into_batch().await.unwrap();
    let scanned = strip_score_column(&scanned, original.schema().as_ref());

    let ctx = create_datafusion_context();
    let table = MemTable::try_new(original.schema(), vec![vec![original.clone()]]).unwrap();
    ctx.register_table("t", Arc::new(table)).unwrap();

    let col_expr = if lower_case {
        format!("lower(t.{})", column)
    } else {
        format!("t.{}", column)
    };
    let normalized_query = if lower_case {
        query.to_lowercase()
    } else {
        query.to_string()
    };
    let expected_from_where = |where_clause: String| async move {
        let sql = format!("SELECT * FROM t WHERE {} ORDER BY id", where_clause);
        let df = ctx.sql(&sql).await.unwrap();
        let expected_batches = df.collect().await.unwrap();
        concat_batches(&original.schema(), &expected_batches).unwrap()
    };
    let expected = if normalized_query.is_empty() {
        expected_from_where(filter.unwrap_or("true").to_string()).await
    } else if phrase_query {
        let predicate = format!("{} LIKE '%{}%'", col_expr, normalized_query);
        let where_clause = if let Some(extra) = filter {
            format!("{} AND {}", predicate, extra)
        } else {
            predicate
        };
        expected_from_where(where_clause).await
    } else {
        let tokens = collect_tokens(&normalized_query);
        if tokens.is_empty() {
            expected_from_where(filter.unwrap_or("true").to_string()).await
        } else {
            let predicate = tokens
                .into_iter()
                .map(|token| format!("{} LIKE '%{}%'", col_expr, token))
                .collect::<Vec<_>>()
                .join(" AND ");
            let where_clause = if let Some(extra) = filter {
                format!("{} AND {}", predicate, extra)
            } else {
                predicate
            };
            expected_from_where(where_clause).await
        }
    };

    assert_eq!(&expected, &scanned);
}

fn collect_tokens(text: &str) -> Vec<&str> {
    text.split(|c: char| !c.is_alphanumeric())
        .filter(|word| !word.is_empty())
        .collect()
}

/// Test that an exhaustive ANN query gives the same results as brute force
/// KNN against the original batch.
///
/// By exhaustive ANN, I mean we search all the partitions so we get perfect recall.
async fn test_ann(original: &RecordBatch, ds: &Dataset, column: &str, predicate: Option<&str>) {
    // Extract first vector from the column as query vector
    let vector_column = original.column_by_name(column).unwrap();
    let fixed_size_list = vector_column.as_fixed_size_list();

    // Extract the first vector's values as a new array
    let vector_values = fixed_size_list
        .values()
        .slice(0, fixed_size_list.value_length() as usize);
    let query_vector = vector_values;

    let mut scanner = ds.scan();
    scanner
        .nearest(column, query_vector.as_ref(), 10)
        .unwrap()
        .prefilter(true)
        .refine(2);
    if let Some(pred) = predicate {
        scanner.filter(pred).unwrap();
    }
    let result = scanner.try_into_batch().await.unwrap();

    // Use DataFusion to apply same vector search using SQL
    let ctx = create_datafusion_context();
    let table = MemTable::try_new(original.schema(), vec![vec![original.clone()]]).unwrap();
    ctx.register_table("t", Arc::new(table)).unwrap();

    // Convert query vector to SQL array literal
    let float_array = query_vector.as_primitive::<arrow::datatypes::Float32Type>();
    let vector_values_str = float_array
        .values()
        .iter()
        .map(|v| v.to_string())
        .collect::<Vec<_>>()
        .join(", ");

    // DataFusion's built-in `array_distance` function uses L2 distance.
    let sql = format!(
        "SELECT * FROM t {} ORDER BY array_distance(t.{}, [{}]) LIMIT 10",
        if let Some(pred) = predicate {
            format!("WHERE {}", pred)
        } else {
            String::new()
        },
        column,
        vector_values_str
    );

    let df = ctx.sql(&sql).await.unwrap();
    let expected_batches = df.collect().await.unwrap();
    let expected = concat_batches(&original.schema(), &expected_batches).unwrap();

    // Compare only the main data (excluding _distance column which Lance adds).
    // We validate that both return the same number of rows and same row ordering.
    // Note: We don't validate the _distance column values because:
    // 1. ANN indices provide approximate distances, not exact values
    // 2. Some distance functions return ordering values (e.g., squared euclidean
    //    without the final sqrt step) rather than true distances
    assert_eq!(
        expected.num_rows(),
        result.num_rows(),
        "Different number of results"
    );

    // Compare the first few columns (excluding _distance)
    for (col_idx, field) in original.schema().fields().iter().enumerate() {
        let expected_col = expected.column(col_idx);
        let result_col = result.column(col_idx);
        assert_eq!(
            expected_col,
            result_col,
            "Column '{}' differs between DataFusion and Lance results",
            field.name()
        );
    }
}
