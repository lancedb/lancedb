// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Tests for Substrait aggregate

use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::{Float64Type, Int64Type};
use arrow_array::{
    FixedSizeListArray, Float32Array, Int64Array, RecordBatch, RecordBatchIterator, StringArray,
};
use arrow_schema::{DataType, Field, Schema as ArrowSchema};
use datafusion_substrait::substrait::proto::{
    AggregateFunction, AggregateRel, Expression, FunctionArgument, Plan, PlanRel, Rel, RelRoot,
    SortField, Version,
    aggregate_function::AggregationInvocation,
    aggregate_rel::{Grouping, Measure},
    expression::{
        FieldReference, ReferenceSegment, RexType,
        field_reference::{ReferenceType, RootReference, RootType},
        reference_segment::{self, StructField},
    },
    extensions::{
        SimpleExtensionDeclaration, SimpleExtensionUrn,
        simple_extension_declaration::{ExtensionFunction, MappingType},
    },
    function_argument::ArgType,
    rel::RelType,
    sort_field::SortKind,
};
use futures::TryStreamExt;
use lance_datafusion::exec::{LanceExecutionOptions, execute_plan};
use lance_datagen::{array, gen_batch};
use lance_table::format::Fragment;
use prost::Message;
use tempfile::tempdir;

use crate::Dataset;
use crate::dataset::scanner::AggregateExpr;
use crate::index::DatasetIndexExt;
use crate::index::vector::VectorIndexParams;
use crate::utils::test::{DatagenExt, FragmentCount, FragmentRowCount, assert_plan_node_equals};
use lance_arrow::FixedSizeListArrayExt;
use lance_index::IndexType;
use lance_index::scalar::FullTextSearchQuery;
use lance_index::scalar::ScalarIndexParams;
use lance_index::scalar::inverted::InvertedIndexParams;
use lance_linalg::distance::MetricType;

/// Helper to create a field reference expression for a column index
fn field_ref(field_index: i32) -> Expression {
    Expression {
        rex_type: Some(RexType::Selection(Box::new(FieldReference {
            reference_type: Some(ReferenceType::DirectReference(ReferenceSegment {
                reference_type: Some(reference_segment::ReferenceType::StructField(Box::new(
                    StructField {
                        field: field_index,
                        child: None,
                    },
                ))),
            })),
            root_type: Some(RootType::RootReference(RootReference {})),
        }))),
    }
}

/// Helper to create a Substrait AggregateRel with given measures and groupings
fn create_aggregate_rel(
    measures: Vec<Measure>,
    grouping_expressions: Vec<Expression>,
    groupings: Vec<Grouping>,
    extensions: Vec<SimpleExtensionDeclaration>,
    output_names: Vec<String>,
) -> Vec<u8> {
    let aggregate_rel = AggregateRel {
        common: None,
        input: None, // Input is ignored for pushdown
        groupings,
        measures,
        grouping_expressions,
        advanced_extension: None,
    };

    let rel = Rel {
        rel_type: Some(RelType::Aggregate(Box::new(aggregate_rel))),
    };

    // Wrap in a Plan to include extensions
    let plan = Plan {
        version: Some(Version {
            major_number: 0,
            minor_number: 63,
            patch_number: 0,
            git_hash: String::new(),
            producer: "lance-test".to_string(),
        }),
        extensions,
        relations: vec![PlanRel {
            rel_type: Some(datafusion_substrait::substrait::proto::plan_rel::RelType::Root(
                RelRoot {
                    input: Some(rel),
                    names: output_names,
                },
            )),
        }],
        advanced_extensions: None,
        expected_type_urls: vec![],
        extension_urns: vec![
            SimpleExtensionUrn {
                extension_urn_anchor: 1,
                urn: "https://github.com/substrait-io/substrait/blob/main/extensions/functions_aggregate_generic.yaml".to_string(),
            },
            SimpleExtensionUrn {
                extension_urn_anchor: 2,
                urn: "https://github.com/substrait-io/substrait/blob/main/extensions/functions_arithmetic.yaml".to_string(),
            },
        ],
        parameter_bindings: vec![],
        type_aliases: vec![],
    };

    plan.encode_to_vec()
}

/// Create extension declaration for an aggregate function
fn agg_extension(anchor: u32, name: &str) -> SimpleExtensionDeclaration {
    SimpleExtensionDeclaration {
        mapping_type: Some(MappingType::ExtensionFunction(ExtensionFunction {
            extension_urn_reference: 1,
            function_anchor: anchor,
            name: name.to_string(),
        })),
    }
}

/// Create a COUNT(*) measure
fn count_star_measure(function_ref: u32) -> Measure {
    Measure {
        measure: Some(AggregateFunction {
            function_reference: function_ref,
            arguments: vec![], // COUNT(*) has no arguments
            options: vec![],
            output_type: None,
            phase: 0,
            sorts: vec![],
            invocation: AggregationInvocation::All as i32,
            #[allow(deprecated)]
            args: vec![],
        }),
        filter: None,
    }
}

/// Create a SUM/AVG/MIN/MAX measure on a column
fn simple_agg_measure(function_ref: u32, column_index: i32) -> Measure {
    Measure {
        measure: Some(AggregateFunction {
            function_reference: function_ref,
            arguments: vec![FunctionArgument {
                arg_type: Some(ArgType::Value(field_ref(column_index))),
            }],
            options: vec![],
            output_type: None,
            phase: 0,
            sorts: vec![],
            invocation: AggregationInvocation::All as i32,
            #[allow(deprecated)]
            args: vec![],
        }),
        filter: None,
    }
}

/// Create an ordered aggregate measure (e.g., FIRST_VALUE with ORDER BY)
fn ordered_agg_measure(
    function_ref: u32,
    column_index: i32,
    sort_column_index: i32,
    ascending: bool,
) -> Measure {
    use datafusion_substrait::substrait::proto::sort_field::SortDirection;

    let sort_direction = if ascending {
        SortDirection::AscNullsLast
    } else {
        SortDirection::DescNullsLast
    };

    Measure {
        measure: Some(AggregateFunction {
            function_reference: function_ref,
            arguments: vec![FunctionArgument {
                arg_type: Some(ArgType::Value(field_ref(column_index))),
            }],
            options: vec![],
            output_type: None,
            phase: 0,
            sorts: vec![SortField {
                expr: Some(field_ref(sort_column_index)),
                sort_kind: Some(SortKind::Direction(sort_direction as i32)),
            }],
            invocation: AggregationInvocation::All as i32,
            #[allow(deprecated)]
            args: vec![],
        }),
        filter: None,
    }
}

/// Execute aggregate plan and collect results
async fn execute_aggregate(
    dataset: &Dataset,
    aggregate_bytes: &[u8],
) -> crate::Result<Vec<RecordBatch>> {
    let mut scanner = dataset.scan();
    scanner.aggregate(AggregateExpr::substrait(aggregate_bytes))?;

    let plan = scanner.create_plan().await?;
    let stream = execute_plan(plan, LanceExecutionOptions::default())?;
    stream.try_collect().await.map_err(|e| e.into())
}

/// Execute aggregate plan on specific fragments
async fn execute_aggregate_on_fragments(
    dataset: &Dataset,
    aggregate_bytes: &[u8],
    fragments: Vec<Fragment>,
) -> crate::Result<Vec<RecordBatch>> {
    let mut scanner = dataset.scan();
    scanner.with_fragments(fragments);
    scanner.aggregate(AggregateExpr::substrait(aggregate_bytes))?;

    let plan = scanner.create_plan().await?;
    let stream = execute_plan(plan, LanceExecutionOptions::default())?;
    stream.try_collect().await.map_err(|e| e.into())
}

/// Create a test dataset with numeric columns
async fn create_numeric_dataset(uri: &str, num_fragments: u32, rows_per_fragment: u32) -> Dataset {
    gen_batch()
        .col("x", array::step::<Int64Type>())
        .col("y", array::step_custom::<Int64Type>(0, 2))
        .col("category", array::cycle::<Int64Type>(vec![1, 2, 3]))
        .into_dataset(
            uri,
            FragmentCount::from(num_fragments),
            FragmentRowCount::from(rows_per_fragment),
        )
        .await
        .unwrap()
}

#[tokio::test]
async fn test_count_star_single_fragment() {
    let tmp_dir = tempdir().unwrap();
    let uri = tmp_dir.path().to_str().unwrap();
    let ds = create_numeric_dataset(uri, 1, 100).await;

    let agg_bytes = create_aggregate_rel(
        vec![count_star_measure(1)],
        vec![],
        vec![],
        vec![agg_extension(1, "count")],
        vec![],
    );

    // COUNT(*) is rewritten by CountPushdown into a Final aggregate
    // over CountFromMaskExec, which answers from manifest metadata + the
    // deletion mask instead of scanning column data.
    let mut scanner = ds.scan();
    scanner
        .aggregate(AggregateExpr::substrait(agg_bytes.clone()))
        .unwrap();
    let plan = scanner.create_plan().await.unwrap();
    assert_plan_node_equals(
        plan,
        "AggregateExec: mode=Final, gby=[], aggr=[count(...)]
  CountFromMask",
    )
    .await
    .unwrap();

    let results = execute_aggregate(&ds, &agg_bytes).await.unwrap();
    assert_eq!(results.len(), 1);
    let batch = &results[0];
    assert_eq!(batch.num_rows(), 1);
    assert_eq!(batch.column(0).as_primitive::<Int64Type>().value(0), 100);
}

#[tokio::test]
async fn test_count_star_multiple_fragments() {
    let tmp_dir = tempdir().unwrap();
    let uri = tmp_dir.path().to_str().unwrap();
    let ds = create_numeric_dataset(uri, 5, 100).await;

    let agg_bytes = create_aggregate_rel(
        vec![count_star_measure(1)],
        vec![],
        vec![],
        vec![agg_extension(1, "count")],
        vec![],
    );

    let results = execute_aggregate(&ds, &agg_bytes).await.unwrap();
    assert_eq!(results.len(), 1);
    let batch = &results[0];
    assert_eq!(batch.num_rows(), 1);
    // 5 fragments * 100 rows = 500 total
    assert_eq!(batch.column(0).as_primitive::<Int64Type>().value(0), 500);
}

#[tokio::test]
async fn test_count_star_subset_fragments() {
    let tmp_dir = tempdir().unwrap();
    let uri = tmp_dir.path().to_str().unwrap();
    let ds = create_numeric_dataset(uri, 5, 100).await;

    // Get only first 2 fragments
    let all_fragments = ds.get_fragments();
    let subset: Vec<Fragment> = all_fragments
        .into_iter()
        .take(2)
        .map(|f| f.metadata)
        .collect();

    let agg_bytes = create_aggregate_rel(
        vec![count_star_measure(1)],
        vec![],
        vec![],
        vec![agg_extension(1, "count")],
        vec![],
    );

    let results = execute_aggregate_on_fragments(&ds, &agg_bytes, subset)
        .await
        .unwrap();
    assert_eq!(results.len(), 1);
    let batch = &results[0];
    assert_eq!(batch.num_rows(), 1);
    // 2 fragments * 100 rows = 200 total
    assert_eq!(batch.column(0).as_primitive::<Int64Type>().value(0), 200);
}

#[tokio::test]
async fn test_sum_single_fragment() {
    let tmp_dir = tempdir().unwrap();
    let uri = tmp_dir.path().to_str().unwrap();
    let ds = create_numeric_dataset(uri, 1, 100).await;

    // SUM(x) where x = 0..99
    let agg_bytes = create_aggregate_rel(
        vec![simple_agg_measure(1, 0)], // column 0 = x
        vec![],
        vec![],
        vec![agg_extension(1, "sum")],
        vec![],
    );

    // Verify SUM(x) only reads column x
    let mut scanner = ds.scan();
    scanner
        .aggregate(AggregateExpr::substrait(agg_bytes.clone()))
        .unwrap();
    let plan = scanner.create_plan().await.unwrap();
    assert_plan_node_equals(
        plan,
        "AggregateExec: mode=Single, gby=[], aggr=[sum(...)]
  LanceRead: uri=..., projection=[x], num_fragments=1, range_before=None, range_after=None, row_id=false, row_addr=false, full_filter=--, refine_filter=--",
    )
    .await
    .unwrap();

    let results = execute_aggregate(&ds, &agg_bytes).await.unwrap();
    assert_eq!(results.len(), 1);
    let batch = &results[0];
    assert_eq!(batch.num_rows(), 1);
    // SUM(0..99) = 99*100/2 = 4950
    assert_eq!(batch.column(0).as_primitive::<Int64Type>().value(0), 4950);
}

#[tokio::test]
async fn test_sum_multiple_fragments() {
    let tmp_dir = tempdir().unwrap();
    let uri = tmp_dir.path().to_str().unwrap();
    let ds = create_numeric_dataset(uri, 4, 25).await;

    // SUM(x) where x = 0..99 across 4 fragments
    let agg_bytes = create_aggregate_rel(
        vec![simple_agg_measure(1, 0)],
        vec![],
        vec![],
        vec![agg_extension(1, "sum")],
        vec![],
    );

    let results = execute_aggregate(&ds, &agg_bytes).await.unwrap();
    assert_eq!(results.len(), 1);
    let batch = &results[0];
    // SUM(0..99) = 4950
    assert_eq!(batch.column(0).as_primitive::<Int64Type>().value(0), 4950);
}

#[tokio::test]
async fn test_min_max() {
    let tmp_dir = tempdir().unwrap();
    let uri = tmp_dir.path().to_str().unwrap();
    let ds = create_numeric_dataset(uri, 4, 25).await;

    // MIN(x) and MAX(x)
    let agg_bytes = create_aggregate_rel(
        vec![
            simple_agg_measure(1, 0), // MIN(x)
            simple_agg_measure(2, 0), // MAX(x)
        ],
        vec![],
        vec![],
        vec![agg_extension(1, "min"), agg_extension(2, "max")],
        vec![],
    );

    let results = execute_aggregate(&ds, &agg_bytes).await.unwrap();
    assert_eq!(results.len(), 1);
    let batch = &results[0];
    assert_eq!(batch.num_rows(), 1);
    assert_eq!(batch.num_columns(), 2);
    // MIN should be 0, MAX should be 99
    assert_eq!(batch.column(0).as_primitive::<Int64Type>().value(0), 0);
    assert_eq!(batch.column(1).as_primitive::<Int64Type>().value(0), 99);
}

#[tokio::test]
async fn test_avg() {
    let tmp_dir = tempdir().unwrap();
    let uri = tmp_dir.path().to_str().unwrap();
    let ds = create_numeric_dataset(uri, 4, 25).await;

    // AVG(x) where x = 0..99
    let agg_bytes = create_aggregate_rel(
        vec![simple_agg_measure(1, 0)],
        vec![],
        vec![],
        vec![agg_extension(1, "avg")],
        vec![],
    );

    let results = execute_aggregate(&ds, &agg_bytes).await.unwrap();
    assert_eq!(results.len(), 1);
    let batch = &results[0];
    // AVG(0..99) = 49.5
    let avg = batch.column(0).as_primitive::<Float64Type>().value(0);
    assert!((avg - 49.5).abs() < 0.001);
}

#[tokio::test]
async fn test_multiple_aggregates() {
    let tmp_dir = tempdir().unwrap();
    let uri = tmp_dir.path().to_str().unwrap();
    let ds = create_numeric_dataset(uri, 4, 25).await;

    // COUNT(*), SUM(x), MIN(x), MAX(x), AVG(x)
    let agg_bytes = create_aggregate_rel(
        vec![
            count_star_measure(1),
            simple_agg_measure(2, 0), // SUM(x)
            simple_agg_measure(3, 0), // MIN(x)
            simple_agg_measure(4, 0), // MAX(x)
            simple_agg_measure(5, 0), // AVG(x)
        ],
        vec![],
        vec![],
        vec![
            agg_extension(1, "count"),
            agg_extension(2, "sum"),
            agg_extension(3, "min"),
            agg_extension(4, "max"),
            agg_extension(5, "avg"),
        ],
        vec![],
    );

    let results = execute_aggregate(&ds, &agg_bytes).await.unwrap();
    assert_eq!(results.len(), 1);
    let batch = &results[0];
    assert_eq!(batch.num_rows(), 1);
    assert_eq!(batch.num_columns(), 5);

    // Verify all aggregates
    assert_eq!(batch.column(0).as_primitive::<Int64Type>().value(0), 100); // COUNT
    assert_eq!(batch.column(1).as_primitive::<Int64Type>().value(0), 4950); // SUM
    assert_eq!(batch.column(2).as_primitive::<Int64Type>().value(0), 0); // MIN
    assert_eq!(batch.column(3).as_primitive::<Int64Type>().value(0), 99); // MAX
    let avg = batch.column(4).as_primitive::<Float64Type>().value(0);
    assert!((avg - 49.5).abs() < 0.001); // AVG
}

#[tokio::test]
async fn test_group_by_with_count() {
    let tmp_dir = tempdir().unwrap();
    let uri = tmp_dir.path().to_str().unwrap();
    let ds = create_numeric_dataset(uri, 4, 30).await;

    // COUNT(*) GROUP BY category
    // category cycles through 1, 2, 3
    let agg_bytes = create_aggregate_rel(
        vec![count_star_measure(1)],
        vec![field_ref(2)], // category is column index 2
        vec![Grouping {
            #[allow(deprecated)]
            grouping_expressions: vec![],
            expression_references: vec![0], // Reference to first grouping expression
        }],
        vec![agg_extension(1, "count")],
        vec![],
    );

    // Verify GROUP BY category only reads category column
    let mut scanner = ds.scan();
    scanner
        .aggregate(AggregateExpr::substrait(agg_bytes.clone()))
        .unwrap();
    let plan = scanner.create_plan().await.unwrap();
    assert_plan_node_equals(
        plan,
        "AggregateExec: mode=Single, gby=[category@0 as category], aggr=[count(...)]
  LanceRead: uri=..., projection=[category], num_fragments=4, range_before=None, range_after=None, row_id=false, row_addr=false, full_filter=--, refine_filter=--",
    )
    .await
    .unwrap();

    let results = execute_aggregate(&ds, &agg_bytes).await.unwrap();
    assert!(!results.is_empty());

    let batch = arrow::compute::concat_batches(&results[0].schema(), &results).unwrap();
    assert_eq!(batch.num_rows(), 3); // 3 categories

    // Each category should have 40 rows (120 total / 3 categories)
    let counts: Vec<i64> = batch
        .column(1) // count column
        .as_primitive::<Int64Type>()
        .values()
        .to_vec();

    for count in counts {
        assert_eq!(count, 40);
    }
}

#[tokio::test]
async fn test_group_by_with_sum() {
    let tmp_dir = tempdir().unwrap();
    let uri = tmp_dir.path().to_str().unwrap();
    let ds = create_numeric_dataset(uri, 1, 9).await;

    // SUM(x) GROUP BY category
    // x = 0..8, category cycles 1,2,3,1,2,3,1,2,3
    // category 1: sum(0,3,6) = 9
    // category 2: sum(1,4,7) = 12
    // category 3: sum(2,5,8) = 15
    let agg_bytes = create_aggregate_rel(
        vec![simple_agg_measure(1, 0)], // SUM(x)
        vec![field_ref(2)],             // GROUP BY category
        vec![Grouping {
            #[allow(deprecated)]
            grouping_expressions: vec![],
            expression_references: vec![0],
        }],
        vec![agg_extension(1, "sum")],
        vec![],
    );

    let results = execute_aggregate(&ds, &agg_bytes).await.unwrap();
    assert!(!results.is_empty());

    let batch = arrow::compute::concat_batches(&results[0].schema(), &results).unwrap();
    assert_eq!(batch.num_rows(), 3); // 3 categories

    // Collect results into a map for verification
    let categories: Vec<i64> = batch
        .column(0) // category column
        .as_primitive::<Int64Type>()
        .values()
        .to_vec();
    let sums: Vec<i64> = batch
        .column(1) // sum column
        .as_primitive::<Int64Type>()
        .values()
        .to_vec();

    let mut results_map = std::collections::HashMap::new();
    for (cat, sum) in categories.iter().zip(sums.iter()) {
        results_map.insert(*cat, *sum);
    }

    assert_eq!(results_map.get(&1), Some(&9));
    assert_eq!(results_map.get(&2), Some(&12));
    assert_eq!(results_map.get(&3), Some(&15));
}

#[tokio::test]
async fn test_aggregate_specific_fragments() {
    let tmp_dir = tempdir().unwrap();
    let uri = tmp_dir.path().to_str().unwrap();
    let ds = create_numeric_dataset(uri, 10, 10).await;

    // Get fragments 3, 5, 7 (0-indexed)
    let all_fragments = ds.get_fragments();
    let subset: Vec<Fragment> = all_fragments
        .into_iter()
        .enumerate()
        .filter(|(i, _)| *i == 3 || *i == 5 || *i == 7)
        .map(|(_, f)| f.metadata)
        .collect();

    let agg_bytes = create_aggregate_rel(
        vec![count_star_measure(1)],
        vec![],
        vec![],
        vec![agg_extension(1, "count")],
        vec![],
    );

    let results = execute_aggregate_on_fragments(&ds, &agg_bytes, subset)
        .await
        .unwrap();

    assert_eq!(results.len(), 1);
    let batch = &results[0];
    // 3 fragments * 10 rows = 30 total
    assert_eq!(batch.column(0).as_primitive::<Int64Type>().value(0), 30);
}

#[tokio::test]
async fn test_sum_specific_fragments() {
    let tmp_dir = tempdir().unwrap();
    let uri = tmp_dir.path().to_str().unwrap();

    // Create dataset where each fragment has distinct values
    // Fragment 0: x = 0..9 (sum = 45)
    // Fragment 1: x = 10..19 (sum = 145)
    // Fragment 2: x = 20..29 (sum = 245)
    // Fragment 3: x = 30..39 (sum = 345)
    let ds = create_numeric_dataset(uri, 4, 10).await;

    // Only scan fragments 1 and 2
    let all_fragments = ds.get_fragments();
    let subset: Vec<Fragment> = all_fragments
        .into_iter()
        .enumerate()
        .filter(|(i, _)| *i == 1 || *i == 2)
        .map(|(_, f)| f.metadata)
        .collect();

    let agg_bytes = create_aggregate_rel(
        vec![simple_agg_measure(1, 0)], // SUM(x)
        vec![],
        vec![],
        vec![agg_extension(1, "sum")],
        vec![],
    );

    let results = execute_aggregate_on_fragments(&ds, &agg_bytes, subset)
        .await
        .unwrap();

    assert_eq!(results.len(), 1);
    let batch = &results[0];
    // Fragment 1: sum(10..19) = 145
    // Fragment 2: sum(20..29) = 245
    // Total = 390
    assert_eq!(batch.column(0).as_primitive::<Int64Type>().value(0), 390);
}

#[tokio::test]
async fn test_aggregate_with_filter() {
    let tmp_dir = tempdir().unwrap();
    let uri = tmp_dir.path().to_str().unwrap();
    let ds = create_numeric_dataset(uri, 1, 100).await;

    let mut scanner = ds.scan();
    scanner.filter("x >= 50").unwrap();

    let agg_bytes = create_aggregate_rel(
        vec![
            count_star_measure(1),
            simple_agg_measure(2, 0), // SUM(x)
            simple_agg_measure(3, 0), // MIN(x)
            simple_agg_measure(4, 0), // MAX(x)
        ],
        vec![],
        vec![],
        vec![
            agg_extension(1, "count"),
            agg_extension(2, "sum"),
            agg_extension(3, "min"),
            agg_extension(4, "max"),
        ],
        vec![],
    );
    scanner
        .aggregate(AggregateExpr::substrait(agg_bytes))
        .unwrap();

    let plan = scanner.create_plan().await.unwrap();
    let stream = execute_plan(plan, LanceExecutionOptions::default()).unwrap();
    let results: Vec<RecordBatch> = stream.try_collect().await.unwrap();

    assert_eq!(results.len(), 1);
    let batch = &results[0];
    assert_eq!(batch.num_rows(), 1);

    // Filter x >= 50 matches rows 50..99 (50 rows)
    assert_eq!(batch.column(0).as_primitive::<Int64Type>().value(0), 50); // COUNT
    // SUM(50..99) = (50+99)*50/2 = 3725
    assert_eq!(batch.column(1).as_primitive::<Int64Type>().value(0), 3725); // SUM
    assert_eq!(batch.column(2).as_primitive::<Int64Type>().value(0), 50); // MIN
    assert_eq!(batch.column(3).as_primitive::<Int64Type>().value(0), 99); // MAX
}

#[tokio::test]
async fn test_aggregate_empty_result() {
    let tmp_dir = tempdir().unwrap();
    let uri = tmp_dir.path().to_str().unwrap();
    let ds = create_numeric_dataset(uri, 1, 100).await;

    // Apply filter that matches no rows, then aggregate
    let mut scanner = ds.scan();
    scanner.project::<&str>(&[]).unwrap();
    scanner.with_row_id();
    scanner.filter("x > 1000").unwrap(); // No rows match

    let agg_bytes = create_aggregate_rel(
        vec![count_star_measure(1)],
        vec![],
        vec![],
        vec![agg_extension(1, "count")],
        vec![],
    );
    scanner
        .aggregate(AggregateExpr::substrait(agg_bytes))
        .unwrap();

    let plan = scanner.create_plan().await.unwrap();
    let stream = execute_plan(plan, LanceExecutionOptions::default()).unwrap();
    let results: Vec<RecordBatch> = stream.try_collect().await.unwrap();

    assert_eq!(results.len(), 1);
    let batch = &results[0];
    assert_eq!(batch.num_rows(), 1);
    // COUNT(*) of empty result should be 0
    assert_eq!(batch.column(0).as_primitive::<Int64Type>().value(0), 0);
}

#[tokio::test]
async fn test_aggregate_single_row() {
    let tmp_dir = tempdir().unwrap();
    let uri = tmp_dir.path().to_str().unwrap();

    // Create dataset with single row using Int64 to avoid type coercion issues
    let schema = Arc::new(ArrowSchema::new(vec![Field::new(
        "x",
        DataType::Int64,
        false,
    )]));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(arrow_array::Int64Array::from(vec![42]))],
    )
    .unwrap();

    let reader = RecordBatchIterator::new(vec![Ok(batch)], schema);
    let ds = Dataset::write(reader, uri, None).await.unwrap();

    let agg_bytes = create_aggregate_rel(
        vec![
            count_star_measure(1),
            simple_agg_measure(2, 0), // SUM(x)
            simple_agg_measure(3, 0), // MIN(x)
            simple_agg_measure(4, 0), // MAX(x)
        ],
        vec![],
        vec![],
        vec![
            agg_extension(1, "count"),
            agg_extension(2, "sum"),
            agg_extension(3, "min"),
            agg_extension(4, "max"),
        ],
        vec![],
    );

    let results = execute_aggregate(&ds, &agg_bytes).await.unwrap();
    assert_eq!(results.len(), 1);
    let batch = &results[0];

    assert_eq!(batch.column(0).as_primitive::<Int64Type>().value(0), 1); // COUNT
    assert_eq!(batch.column(1).as_primitive::<Int64Type>().value(0), 42); // SUM
    assert_eq!(batch.column(2).as_primitive::<Int64Type>().value(0), 42); // MIN
    assert_eq!(batch.column(3).as_primitive::<Int64Type>().value(0), 42); // MAX
}

#[tokio::test]
async fn test_aggregate_with_aliases() {
    let tmp_dir = tempdir().unwrap();
    let uri = tmp_dir.path().to_str().unwrap();
    let ds = create_numeric_dataset(uri, 1, 100).await;

    // COUNT(*), SUM(x), MIN(x) with custom aliases
    let agg_bytes = create_aggregate_rel(
        vec![
            count_star_measure(1),
            simple_agg_measure(2, 0),
            simple_agg_measure(3, 0),
        ],
        vec![],
        vec![],
        vec![
            agg_extension(1, "count"),
            agg_extension(2, "sum"),
            agg_extension(3, "min"),
        ],
        vec![
            "total_count".to_string(),
            "sum_of_x".to_string(),
            "min_x".to_string(),
        ],
    );

    let results = execute_aggregate(&ds, &agg_bytes).await.unwrap();
    assert_eq!(results.len(), 1);
    let batch = &results[0];

    // Verify output schema has the expected aliases
    let schema = batch.schema();
    assert_eq!(schema.fields().len(), 3);
    assert_eq!(schema.field(0).name(), "total_count");
    assert_eq!(schema.field(1).name(), "sum_of_x");
    assert_eq!(schema.field(2).name(), "min_x");

    // Verify values are correct
    assert_eq!(batch.column(0).as_primitive::<Int64Type>().value(0), 100);
    assert_eq!(batch.column(1).as_primitive::<Int64Type>().value(0), 4950);
    assert_eq!(batch.column(2).as_primitive::<Int64Type>().value(0), 0);
}

#[tokio::test]
async fn test_group_by_with_aliases() {
    let tmp_dir = tempdir().unwrap();
    let uri = tmp_dir.path().to_str().unwrap();
    let ds = create_numeric_dataset(uri, 1, 9).await;

    // SUM(x) GROUP BY category with aliases
    let agg_bytes = create_aggregate_rel(
        vec![simple_agg_measure(1, 0)],
        vec![field_ref(2)],
        vec![Grouping {
            #[allow(deprecated)]
            grouping_expressions: vec![],
            expression_references: vec![0],
        }],
        vec![agg_extension(1, "sum")],
        vec!["group_key".to_string(), "total_sum".to_string()],
    );

    let results = execute_aggregate(&ds, &agg_bytes).await.unwrap();
    assert!(!results.is_empty());

    let batch = arrow::compute::concat_batches(&results[0].schema(), &results).unwrap();

    // Verify output schema has the expected aliases
    let schema = batch.schema();
    assert_eq!(schema.fields().len(), 2);
    assert_eq!(schema.field(0).name(), "group_key");
    assert_eq!(schema.field(1).name(), "total_sum");
}

#[tokio::test]
async fn test_first_value_with_order_by() {
    let tmp_dir = tempdir().unwrap();
    let uri = tmp_dir.path().to_str().unwrap();
    let ds = create_numeric_dataset(uri, 1, 9).await;

    // FIRST_VALUE(x) ORDER BY x ASC GROUP BY category
    // x = 0..8, category cycles 1,2,3,1,2,3,1,2,3
    // category 1 has x values: 0, 3, 6 -> first_value(ORDER BY x ASC) = 0
    // category 2 has x values: 1, 4, 7 -> first_value(ORDER BY x ASC) = 1
    // category 3 has x values: 2, 5, 8 -> first_value(ORDER BY x ASC) = 2
    let agg_bytes = create_aggregate_rel(
        vec![ordered_agg_measure(1, 0, 0, true)], // FIRST_VALUE(x) ORDER BY x ASC
        vec![field_ref(2)],                       // GROUP BY category
        vec![Grouping {
            #[allow(deprecated)]
            grouping_expressions: vec![],
            expression_references: vec![0],
        }],
        vec![agg_extension(1, "first_value")],
        vec![],
    );

    let results = execute_aggregate(&ds, &agg_bytes).await.unwrap();
    assert!(!results.is_empty());

    let batch = arrow::compute::concat_batches(&results[0].schema(), &results).unwrap();
    assert_eq!(batch.num_rows(), 3);

    let categories: Vec<i64> = batch
        .column(0)
        .as_primitive::<Int64Type>()
        .values()
        .to_vec();
    let first_values: Vec<i64> = batch
        .column(1)
        .as_primitive::<Int64Type>()
        .values()
        .to_vec();

    let mut results_map = std::collections::HashMap::new();
    for (cat, val) in categories.iter().zip(first_values.iter()) {
        results_map.insert(*cat, *val);
    }

    assert_eq!(results_map.get(&1), Some(&0));
    assert_eq!(results_map.get(&2), Some(&1));
    assert_eq!(results_map.get(&3), Some(&2));
}

#[tokio::test]
async fn test_first_value_with_order_by_desc() {
    let tmp_dir = tempdir().unwrap();
    let uri = tmp_dir.path().to_str().unwrap();
    let ds = create_numeric_dataset(uri, 1, 9).await;

    // FIRST_VALUE(x) ORDER BY x DESC GROUP BY category
    // category 1 has x values: 0, 3, 6 -> first_value(ORDER BY x DESC) = 6
    // category 2 has x values: 1, 4, 7 -> first_value(ORDER BY x DESC) = 7
    // category 3 has x values: 2, 5, 8 -> first_value(ORDER BY x DESC) = 8
    let agg_bytes = create_aggregate_rel(
        vec![ordered_agg_measure(1, 0, 0, false)], // FIRST_VALUE(x) ORDER BY x DESC
        vec![field_ref(2)],                        // GROUP BY category
        vec![Grouping {
            #[allow(deprecated)]
            grouping_expressions: vec![],
            expression_references: vec![0],
        }],
        vec![agg_extension(1, "first_value")],
        vec![],
    );

    let results = execute_aggregate(&ds, &agg_bytes).await.unwrap();
    assert!(!results.is_empty());

    let batch = arrow::compute::concat_batches(&results[0].schema(), &results).unwrap();
    assert_eq!(batch.num_rows(), 3);

    let categories: Vec<i64> = batch
        .column(0)
        .as_primitive::<Int64Type>()
        .values()
        .to_vec();
    let first_values: Vec<i64> = batch
        .column(1)
        .as_primitive::<Int64Type>()
        .values()
        .to_vec();

    let mut results_map = std::collections::HashMap::new();
    for (cat, val) in categories.iter().zip(first_values.iter()) {
        results_map.insert(*cat, *val);
    }

    assert_eq!(results_map.get(&1), Some(&6));
    assert_eq!(results_map.get(&2), Some(&7));
    assert_eq!(results_map.get(&3), Some(&8));
}

/// Create a dataset with vectors, text, and category for vector search and FTS aggregate tests.
/// Schema: id (i64), vec (fixed_size_list<f32>[4]), text (utf8), category (utf8)
async fn create_vector_text_dataset(uri: &str, num_rows: i64) -> Dataset {
    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new(
            "vec",
            DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), 4),
            true,
        ),
        Field::new("text", DataType::Utf8, false),
        Field::new("category", DataType::Utf8, false),
    ]));

    let ids: Vec<i64> = (0..num_rows).collect();
    let vectors: Vec<f32> = (0..num_rows).flat_map(|i| vec![i as f32; 4]).collect();
    let texts: Vec<String> = (0..num_rows).map(|i| format!("document {}", i)).collect();
    let categories: Vec<String> = (0..num_rows)
        .map(|i| match i % 3 {
            0 => "category_a".to_string(),
            1 => "category_b".to_string(),
            _ => "category_c".to_string(),
        })
        .collect();

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(
                FixedSizeListArray::try_new_from_values(Float32Array::from(vectors), 4).unwrap(),
            ),
            Arc::new(StringArray::from(texts)),
            Arc::new(StringArray::from(categories)),
        ],
    )
    .unwrap();

    let reader = RecordBatchIterator::new(vec![Ok(batch)], schema);
    Dataset::write(reader, uri, None).await.unwrap()
}

#[tokio::test]
async fn test_vector_search_with_aggregate() {
    let tmp_dir = tempdir().unwrap();
    let uri = tmp_dir.path().to_str().unwrap();
    let mut dataset = create_vector_text_dataset(uri, 100).await;

    // Create vector index
    let params = VectorIndexParams::ivf_flat(2, MetricType::L2);
    dataset
        .create_index(&["vec"], IndexType::Vector, None, &params, true)
        .await
        .unwrap();

    // Vector search for top 30 results, then aggregate by category with COUNT(*)
    // Query vector close to id=50 (vec=[50,50,50,50])
    let query_vector = Float32Array::from(vec![50.0f32, 50.0, 50.0, 50.0]);

    // COUNT(*) GROUP BY category (column index 3)
    let agg_bytes = create_aggregate_rel(
        vec![count_star_measure(1)],
        vec![field_ref(3)], // GROUP BY category
        vec![Grouping {
            #[allow(deprecated)]
            grouping_expressions: vec![],
            expression_references: vec![0],
        }],
        vec![agg_extension(1, "count")],
        vec!["category".to_string(), "count".to_string()],
    );

    let mut scanner = dataset.scan();
    scanner
        .nearest("vec", &query_vector, 30)
        .unwrap()
        .project(&["id", "category"])
        .unwrap()
        .aggregate(AggregateExpr::substrait(agg_bytes))
        .unwrap();

    let results = scanner.try_into_batch().await.unwrap();

    // Should have 3 categories (or fewer if search results don't cover all)
    assert!(
        results.num_rows() >= 1 && results.num_rows() <= 3,
        "Expected 1-3 rows but got {}",
        results.num_rows()
    );

    // Total count should be 30 (top K results)
    let counts: Vec<i64> = results
        .column(1)
        .as_primitive::<Int64Type>()
        .values()
        .to_vec();
    let total: i64 = counts.iter().sum();
    assert_eq!(total, 30);
}

#[tokio::test]
async fn test_fts_with_aggregate() {
    let tmp_dir = tempdir().unwrap();
    let uri = tmp_dir.path().to_str().unwrap();
    let mut dataset = create_vector_text_dataset(uri, 100).await;

    // Create FTS index on text column
    dataset
        .create_index(
            &["text"],
            IndexType::Inverted,
            None,
            &InvertedIndexParams::default(),
            true,
        )
        .await
        .unwrap();

    // FTS search for "document", then aggregate by category with COUNT(*)
    // All documents match "document" so we should get all 100 rows
    // COUNT(*) GROUP BY category (column index 3)
    let agg_bytes = create_aggregate_rel(
        vec![count_star_measure(1)],
        vec![field_ref(3)], // GROUP BY category
        vec![Grouping {
            #[allow(deprecated)]
            grouping_expressions: vec![],
            expression_references: vec![0],
        }],
        vec![agg_extension(1, "count")],
        vec!["category".to_string(), "count".to_string()],
    );

    let mut scanner = dataset.scan();
    scanner
        .full_text_search(FullTextSearchQuery::new("document".to_string()))
        .unwrap()
        .project(&["id", "category"])
        .unwrap()
        .aggregate(AggregateExpr::substrait(agg_bytes))
        .unwrap();

    let results = scanner.try_into_batch().await.unwrap();

    // Should have 3 categories
    assert_eq!(
        results.num_rows(),
        3,
        "Expected 3 rows but got {}",
        results.num_rows()
    );

    // Total count should be 100 (all documents match "document")
    let counts: Vec<i64> = results
        .column(1)
        .as_primitive::<Int64Type>()
        .values()
        .to_vec();
    let total: i64 = counts.iter().sum();
    assert_eq!(total, 100);

    // Each category should have ~33 rows (100/3)
    for count in &counts {
        assert!(*count >= 33 && *count <= 34);
    }
}

#[tokio::test]
async fn test_vector_search_with_sum_aggregate() {
    let tmp_dir = tempdir().unwrap();
    let uri = tmp_dir.path().to_str().unwrap();
    let mut dataset = create_vector_text_dataset(uri, 100).await;

    // Create vector index
    let params = VectorIndexParams::ivf_flat(2, MetricType::L2);
    dataset
        .create_index(&["vec"], IndexType::Vector, None, &params, true)
        .await
        .unwrap();

    // Vector search for top 10 results, then SUM(id) GROUP BY category
    let query_vector = Float32Array::from(vec![50.0f32, 50.0, 50.0, 50.0]);

    // SUM(id) GROUP BY category
    let agg_bytes = create_aggregate_rel(
        vec![simple_agg_measure(1, 0)], // SUM(id) - column 0
        vec![field_ref(3)],             // GROUP BY category
        vec![Grouping {
            #[allow(deprecated)]
            grouping_expressions: vec![],
            expression_references: vec![0],
        }],
        vec![agg_extension(1, "sum")],
        vec!["category".to_string(), "sum_id".to_string()],
    );

    let mut scanner = dataset.scan();
    scanner
        .nearest("vec", &query_vector, 10)
        .unwrap()
        .project(&["id", "category"])
        .unwrap()
        .aggregate(AggregateExpr::substrait(agg_bytes))
        .unwrap();

    let results = scanner.try_into_batch().await.unwrap();

    // Should have results grouped by category (1-3 depending on which categories are in top K)
    assert!(
        results.num_rows() >= 1 && results.num_rows() <= 3,
        "Expected 1-3 rows but got {}",
        results.num_rows()
    );

    // Verify we have 2 columns: category and sum_id
    assert_eq!(results.num_columns(), 2);
}

#[tokio::test]
async fn test_scanner_count_rows() {
    let ds = create_numeric_dataset("memory://test_count_rows", 2, 50).await;

    // Check plan structure
    let mut scanner = ds.scan();
    scanner
        .aggregate(AggregateExpr::builder().count_star().build())
        .unwrap();
    let plan = scanner.create_plan().await.unwrap();

    // COUNT(*) is rewritten by CountPushdown into a Final aggregate
    // over CountFromMaskExec.
    assert_plan_node_equals(
        plan.clone(),
        "AggregateExec: mode=Final, gby=[], aggr=[count(Int32(1))]
  CountFromMask",
    )
    .await
    .unwrap();

    // Execute and verify result
    let stream = execute_plan(plan, LanceExecutionOptions::default()).unwrap();
    let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();
    assert_eq!(batches.len(), 1);
    assert_eq!(
        batches[0].column(0).as_primitive::<Int64Type>().value(0),
        100 // 2 fragments * 50 rows
    );
}

#[tokio::test]
async fn test_scanner_count_rows_with_filter() {
    let ds = create_numeric_dataset("memory://test_count_rows_filter", 1, 100).await;

    // Check plan structure
    let mut scanner = ds.scan();
    scanner.filter("x >= 50").unwrap();
    scanner
        .aggregate(AggregateExpr::builder().count_star().build())
        .unwrap();
    let plan = scanner.create_plan().await.unwrap();

    // COUNT(*) with filter: filter columns are needed, but no data columns for the aggregate
    assert_plan_node_equals(
        plan.clone(),
        "AggregateExec: mode=Single, gby=[], aggr=[count(Int32(1))]
  LanceRead: uri=..., projection=[x], num_fragments=1, range_before=None, range_after=None, row_id=true, row_addr=false, full_filter=x >= Int64(50), refine_filter=x >= Int64(50)",
    )
    .await
    .unwrap();

    // Execute and verify result
    let stream = execute_plan(plan, LanceExecutionOptions::default()).unwrap();
    let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();
    assert_eq!(batches.len(), 1);
    // x ranges from 0 to 99, so x >= 50 matches rows 50..99 (50 rows)
    assert_eq!(
        batches[0].column(0).as_primitive::<Int64Type>().value(0),
        50
    );
}

#[tokio::test]
async fn test_scanner_count_rows_with_indexed_filter() {
    // When the filter is fully evaluable by a scalar index that covers
    // every dataset fragment, the rule rewrites COUNT(*) into a Final
    // aggregate over CountFromMaskExec, with the ScalarIndexExec
    // wired in as the prefilter — no LanceRead, no column scan.
    let mut ds = create_numeric_dataset("memory://test_count_indexed", 2, 50).await;
    ds.create_index(
        &["x"],
        IndexType::BTree,
        None,
        &ScalarIndexParams::default(),
        true,
    )
    .await
    .unwrap();

    let mut scanner = ds.scan();
    scanner.filter("x < 50").unwrap();
    scanner
        .aggregate(AggregateExpr::builder().count_star().build())
        .unwrap();
    let plan = scanner.create_plan().await.unwrap();

    assert_plan_node_equals(
        plan.clone(),
        "AggregateExec: mode=Final, gby=[], aggr=[count(Int32(1))]
  CountFromMask
    ScalarIndexQuery: query=[x < 50]@x_idx(BTree)",
    )
    .await
    .unwrap();

    let stream = execute_plan(plan, LanceExecutionOptions::default()).unwrap();
    let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();
    assert_eq!(batches.len(), 1);
    assert_eq!(
        batches[0].column(0).as_primitive::<Int64Type>().value(0),
        50,
    );
}

#[tokio::test]
async fn test_scanner_count_rows_with_indexed_filter_stable_row_ids() {
    // Indexed-filter count under stable row ids, with deletions in both
    // fragments. The rule fires and the cross-fragment count stays correct.
    let tmp = tempdir().unwrap();
    let uri = tmp.path().to_str().unwrap();
    let mut ds = gen_batch()
        .col("x", array::step::<Int64Type>())
        .col("y", array::step_custom::<Int64Type>(0, 2))
        .col("category", array::cycle::<Int64Type>(vec![1, 2, 3]))
        .into_dataset_with_params(
            uri,
            FragmentCount::from(2),
            FragmentRowCount::from(50),
            Some(crate::dataset::WriteParams {
                max_rows_per_file: 50,
                enable_stable_row_ids: true,
                ..Default::default()
            }),
        )
        .await
        .unwrap();
    ds.create_index(
        &["x"],
        IndexType::BTree,
        None,
        &ScalarIndexParams::default(),
        true,
    )
    .await
    .unwrap();
    // Delete one row from each fragment (x=10 in frag 0, x=70 in frag 1).
    ds.delete("x = 10 OR x = 70").await.unwrap();

    let mut scanner = ds.scan();
    scanner.filter("x < 100").unwrap();
    scanner
        .aggregate(AggregateExpr::builder().count_star().build())
        .unwrap();
    let plan = scanner.create_plan().await.unwrap();

    assert_plan_node_equals(
        plan.clone(),
        "AggregateExec: mode=Final, gby=[], aggr=[count(Int32(1))]
  CountFromMask
    ScalarIndexQuery: query=[x < 100]@x_idx(BTree)",
    )
    .await
    .unwrap();

    let stream = execute_plan(plan, LanceExecutionOptions::default()).unwrap();
    let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();
    assert_eq!(batches.len(), 1);
    // 100 rows match `x < 100`, minus the two deletions.
    assert_eq!(
        batches[0].column(0).as_primitive::<Int64Type>().value(0),
        98,
    );
}

#[tokio::test]
async fn test_scanner_count_rows_indexed_filter_stable_row_ids_after_compaction() {
    // Update rewrites a scattered subset of rows under stable row ids; the
    // rewritten copies keep their stable ids, so compaction folds the surviving
    // and rewritten halves of a fragment into row-id segments whose ranges
    // overlap. The indexed-filter count must still see every live row.
    let tmp = tempdir().unwrap();
    let uri = tmp.path().to_str().unwrap();
    let ds = gen_batch()
        .col("x", array::step::<Int64Type>())
        .col("category", array::cycle::<Int64Type>(vec![1, 2, 3]))
        .into_dataset_with_params(
            uri,
            FragmentCount::from(2),
            FragmentRowCount::from(50),
            Some(crate::dataset::WriteParams {
                max_rows_per_file: 50,
                enable_stable_row_ids: true,
                ..Default::default()
            }),
        )
        .await
        .unwrap();
    // Update every third row (category == 1), scattered across stable ids.
    let res = crate::dataset::UpdateBuilder::new(Arc::new(ds))
        .update_where("category = 1")
        .unwrap()
        .set("category", "0")
        .unwrap()
        .build()
        .unwrap()
        .execute()
        .await
        .unwrap();
    let mut ds = res.new_dataset.as_ref().clone();
    // Compaction merges the surviving and rewritten fragments, producing a
    // fragment whose row-id sequence has overlapping segments.
    crate::dataset::optimize::compact_files(&mut ds, Default::default(), None)
        .await
        .unwrap();
    // Index after compaction: it covers every fragment and no deletions remain,
    // so the count is answered from the stable-id universe (the path the
    // overlapping segments corrupt).
    ds.create_index(
        &["x"],
        IndexType::BTree,
        None,
        &ScalarIndexParams::default(),
        true,
    )
    .await
    .unwrap();

    let mut scanner = ds.scan();
    scanner.filter("x < 100").unwrap();
    scanner
        .aggregate(AggregateExpr::builder().count_star().build())
        .unwrap();
    let plan = scanner.create_plan().await.unwrap();

    assert_plan_node_equals(
        plan.clone(),
        "AggregateExec: mode=Final, gby=[], aggr=[count(Int32(1))]
  CountFromMask
    ScalarIndexQuery: query=[x < 100]@x_idx(BTree)",
    )
    .await
    .unwrap();

    let stream = execute_plan(plan, LanceExecutionOptions::default()).unwrap();
    let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();
    assert_eq!(batches.len(), 1);
    // No deletions remain after compaction; all 100 rows match `x < 100`.
    assert_eq!(
        batches[0].column(0).as_primitive::<Int64Type>().value(0),
        100,
    );
}

#[tokio::test]
async fn test_scanner_count_rows_with_partial_index_coverage() {
    // Index covers the first two fragments, then a third fragment is
    // appended. The rule cannot answer the count from the index alone for
    // the appended fragment, so it emits a split plan: CountFromMaskExec
    // over the indexed fragments + AggregateExec(Partial)/FilteredReadExec
    // over the rest, both unioned and summed by AggregateExec(Final).
    let tmp = tempdir().unwrap();
    let uri = tmp.path().to_str().unwrap();
    let mut ds = create_numeric_dataset(uri, 2, 50).await;
    ds.create_index(
        &["x"],
        IndexType::BTree,
        None,
        &ScalarIndexParams::default(),
        true,
    )
    .await
    .unwrap();

    // Append a third fragment that the index does not cover.
    let reader = gen_batch()
        .col("x", array::step_custom::<Int64Type>(100, 1))
        .col("y", array::step_custom::<Int64Type>(0, 2))
        .col("category", array::cycle::<Int64Type>(vec![1, 2, 3]))
        .into_reader_rows(
            lance_datagen::RowCount::from(50),
            lance_datagen::BatchCount::from(1),
        );
    let ds = Dataset::write(
        reader,
        uri,
        Some(crate::dataset::WriteParams {
            mode: crate::dataset::WriteMode::Append,
            max_rows_per_file: 50,
            ..Default::default()
        }),
    )
    .await
    .unwrap();
    assert_eq!(ds.get_fragments().len(), 3);

    let mut scanner = ds.scan();
    // `x < 1000` matches every row (values are 0..100 + 100..150). The
    // pushdown branch contributes the first 100 from the indexed fragments;
    // the scan branch contributes the 50 rows from the appended fragment.
    scanner.filter("x < 1000").unwrap();
    scanner
        .aggregate(AggregateExpr::builder().count_star().build())
        .unwrap();
    // Pin target_parallelism=1 so EnforceDistribution produces a deterministic
    // plan snapshot regardless of the machine's CPU count.
    scanner.target_parallelism(1);
    let plan = scanner.create_plan().await.unwrap();

    assert_plan_node_equals(
        plan.clone(),
        "AggregateExec: mode=Final, gby=[], aggr=[count(Int32(1))]
  CoalescePartitionsExec
    UnionExec
      CountFromMask
        ScalarIndexQuery: query=[x < 1000]@x_idx(BTree)
      AggregateExec: mode=Partial, gby=[], aggr=[count(Int32(1))]
        LanceRead: uri=..., projection=[], num_fragments=1, range_before=None, range_after=None, row_id=false, row_addr=true, full_filter=x < Int64(1000), refine_filter=--",
    )
    .await
    .unwrap();

    let stream = execute_plan(plan, LanceExecutionOptions::default()).unwrap();
    let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();
    assert_eq!(batches.len(), 1);
    assert_eq!(
        batches[0].column(0).as_primitive::<Int64Type>().value(0),
        150,
    );
}

#[tokio::test]
async fn test_scanner_count_rows_empty_result() {
    let ds = create_numeric_dataset("memory://test_count_rows_empty", 1, 100).await;

    let mut scanner = ds.scan();
    scanner.filter("x > 1000").unwrap(); // No rows match
    let count = scanner.count_rows().await.unwrap();

    assert_eq!(count, 0);
}

#[tokio::test]
async fn test_scanner_count_rows_with_vector_search() {
    let tmp_dir = tempdir().unwrap();
    let uri = tmp_dir.path().to_str().unwrap();
    let mut dataset = create_vector_text_dataset(uri, 100).await;

    // Create vector index
    let params = VectorIndexParams::ivf_flat(2, MetricType::L2);
    dataset
        .create_index(&["vec"], IndexType::Vector, None, &params, true)
        .await
        .unwrap();

    let query_vector = Float32Array::from(vec![50.0f32, 50.0, 50.0, 50.0]);

    // Check plan structure
    let mut scanner = dataset.scan();
    scanner.nearest("vec", &query_vector, 30).unwrap();
    scanner
        .aggregate(AggregateExpr::builder().count_star().build())
        .unwrap();
    let plan = scanner.create_plan().await.unwrap();

    assert_plan_node_equals(
        plan.clone(),
        "AggregateExec: mode=Single, gby=[], aggr=[count(Int32(1))]
  SortExec: TopK(fetch=30), ...
    ANNSubIndex: ...
      ANNIvfPartition: ...deltas=1",
    )
    .await
    .unwrap();

    // Execute and verify result
    let stream = execute_plan(plan, LanceExecutionOptions::default()).unwrap();
    let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();
    assert_eq!(batches.len(), 1);
    assert_eq!(
        batches[0].column(0).as_primitive::<Int64Type>().value(0),
        30 // top K results
    );
}

#[tokio::test]
async fn test_scanner_count_rows_with_fts() {
    let tmp_dir = tempdir().unwrap();
    let uri = tmp_dir.path().to_str().unwrap();
    let mut dataset = create_vector_text_dataset(uri, 100).await;

    // Create FTS index on text column
    dataset
        .create_index(
            &["text"],
            IndexType::Inverted,
            None,
            &InvertedIndexParams::default(),
            true,
        )
        .await
        .unwrap();

    // Check plan structure
    let mut scanner = dataset.scan();
    scanner
        .full_text_search(FullTextSearchQuery::new("document".to_string()))
        .unwrap();
    scanner
        .aggregate(AggregateExpr::builder().count_star().build())
        .unwrap();
    let plan = scanner.create_plan().await.unwrap();

    assert_plan_node_equals(
        plan.clone(),
        "AggregateExec: mode=Single, gby=[], aggr=[count(Int32(1))]
  MatchQuery: column=text, query=[document]",
    )
    .await
    .unwrap();

    // Execute and verify result
    let stream = execute_plan(plan, LanceExecutionOptions::default()).unwrap();
    let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();
    assert_eq!(batches.len(), 1);
    // All 100 documents contain "document"
    assert_eq!(
        batches[0].column(0).as_primitive::<Int64Type>().value(0),
        100
    );
}

#[tokio::test]
async fn test_scanner_count_rows_with_vector_search_and_filter() {
    let tmp_dir = tempdir().unwrap();
    let uri = tmp_dir.path().to_str().unwrap();
    let mut dataset = create_vector_text_dataset(uri, 100).await;

    // Create vector index
    let params = VectorIndexParams::ivf_flat(2, MetricType::L2);
    dataset
        .create_index(&["vec"], IndexType::Vector, None, &params, true)
        .await
        .unwrap();

    // Vector search for top 50 results, then filter by category
    let query_vector = Float32Array::from(vec![50.0f32, 50.0, 50.0, 50.0]);

    let mut scanner = dataset.scan();
    scanner
        .nearest("vec", &query_vector, 50)
        .unwrap()
        .filter("category = 'category_a'")
        .unwrap();
    let count = scanner.count_rows().await.unwrap();

    // Only ~1/3 of the top 50 results should be in category_a
    assert!(count > 0 && count <= 50);
}
