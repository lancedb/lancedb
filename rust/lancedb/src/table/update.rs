// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use std::sync::Arc;

use arrow_schema::DataType;
use lance::dataset::UpdateBuilder as LanceUpdateBuilder;
use serde::{Deserialize, Serialize};

use super::{BaseTable, NativeTable};
use crate::Error;
use crate::Result;

/// The result of an update operation
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct UpdateResult {
    #[serde(default)]
    pub rows_updated: u64,
    /// The commit version associated with the operation.
    #[serde(default)]
    pub version: u64,
}

/// A builder for configuring a [`crate::table::Table::update`] operation
#[derive(Debug, Clone)]
pub struct UpdateBuilder {
    parent: Arc<dyn BaseTable>,
    pub(crate) filter: Option<String>,
    pub(crate) columns: Vec<(String, String)>,
}

impl UpdateBuilder {
    pub(crate) fn new(parent: Arc<dyn BaseTable>) -> Self {
        Self {
            parent,
            filter: None,
            columns: Vec::new(),
        }
    }

    /// Limits the update operation to rows matching the given filter
    ///
    /// If a row does not match the filter then it will be left unchanged.
    pub fn only_if(mut self, filter: impl Into<String>) -> Self {
        self.filter = Some(filter.into());
        self
    }

    /// Specifies a column to update
    ///
    /// This method may be called multiple times to update multiple columns
    ///
    /// The `update_expr` should be an SQL expression explaining how to calculate
    /// the new value for the column.  The expression will be evaluated against the
    /// previous row's value.
    pub fn column(
        mut self,
        column_name: impl Into<String>,
        update_expr: impl Into<String>,
    ) -> Self {
        self.columns.push((column_name.into(), update_expr.into()));
        self
    }

    /// Executes the update operation.
    pub async fn execute(self) -> Result<UpdateResult> {
        if self.columns.is_empty() {
            Err(Error::InvalidInput {
                message: "at least one column must be specified in an update operation".to_string(),
            })
        } else {
            self.parent.clone().update(self).await
        }
    }
}

/// Internal implementation of the update logic
pub(crate) async fn execute_update(
    table: &NativeTable,
    update: UpdateBuilder,
) -> Result<UpdateResult> {
    table.dataset.ensure_mutable()?;

    // 1. Snapshot the current dataset
    let dataset = table.dataset.get().await?;

    // 2. Initialize the Lance Core builder
    let mut builder = LanceUpdateBuilder::new(dataset.clone());

    // 3. Apply the filter (WHERE clause)
    if let Some(predicate) = update.filter {
        let predicate = safe_update_filter(&predicate, dataset.as_ref());
        builder = builder.update_where(&predicate)?;
    }

    // 4. Apply the columns (SET clause)
    for (column, value) in update.columns {
        builder = builder.set(column, &value)?;
    }

    // 5. Execute the operation (Write new files)
    let operation = builder.build()?;
    let res = operation.execute().await?;

    // 6. Update the table's view of the latest version
    table.dataset.update(res.new_dataset.as_ref().clone());

    Ok(UpdateResult {
        rows_updated: res.rows_updated,
        version: res.new_dataset.version().version,
    })
}

/// Keep vulnerable legacy updates on the early-materialization scan path.
///
/// Late materialization uses `TakeExec` to concatenate values read from multiple
/// fragments. That can overflow a single 32-bit-offset array. Lance's update
/// builder does not currently expose its scanner's materialization controls, so
/// cast the predicate to an integer before comparing it with `1`. Lance's
/// scalar-index extractor does not unwrap non-literal casts, keeping every
/// supported predicate out of the vulnerable late-materialization plan.
///
/// Keep the original SQL verbatim instead of parsing and serializing it. Newlines
/// isolate the generated syntax from a trailing line comment in the predicate.
///
/// This compatibility fallback is intentionally limited to legacy storage. V2
/// readers do not use the affected materialization path and keep their original
/// filter expression and indexed plan.
fn safe_update_filter(predicate: &str, dataset: &lance::Dataset) -> String {
    let has_offset_columns = dataset
        .schema()
        .fields
        .iter()
        .any(|field| has_32_bit_offsets(&field.data_type()));

    if !dataset.manifest().should_use_legacy_format() || !has_offset_columns {
        return predicate.to_owned();
    }

    format!("CAST((\n{predicate}\n) AS INT) = 1")
}

fn has_32_bit_offsets(data_type: &DataType) -> bool {
    match data_type {
        DataType::Binary
        | DataType::Utf8
        | DataType::List(_)
        | DataType::ListView(_)
        | DataType::Map(_, _)
        | DataType::Union(_, _) => true,
        DataType::FixedSizeList(field, _)
        | DataType::LargeList(field)
        | DataType::LargeListView(field) => has_32_bit_offsets(field.data_type()),
        DataType::Struct(fields) => fields
            .iter()
            .any(|field| has_32_bit_offsets(field.data_type())),
        DataType::Dictionary(_, values) => has_32_bit_offsets(values),
        DataType::RunEndEncoded(_, values) => has_32_bit_offsets(values.data_type()),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use crate::connect;
    use crate::connection::LanceFileVersion;
    use crate::database::listing::{ListingDatabaseOptions, NewTableConfig};
    use crate::index::{Index, scalar::BTreeIndexBuilder};
    use crate::query::QueryBase;
    use crate::query::{ExecutableQuery, Select};
    use arrow_array::{
        Array, BooleanArray, Date32Array, FixedSizeListArray, Float32Array, Float64Array,
        Int32Array, Int64Array, LargeStringArray, RecordBatch, StringArray,
        TimestampMillisecondArray, TimestampNanosecondArray, UInt32Array, record_batch,
    };
    use arrow_data::ArrayDataBuilder;
    use arrow_schema::{ArrowError, DataType, Field, Schema, TimeUnit};
    use futures::TryStreamExt;
    use lance::io::exec::Planner;
    use std::sync::Arc;
    use std::time::Duration;

    fn contains_take(plan: &dyn datafusion_physical_plan::ExecutionPlan) -> bool {
        plan.name() == "TakeExec"
            || plan
                .children()
                .iter()
                .any(|child| contains_take(child.as_ref()))
    }

    #[tokio::test]
    async fn test_update_all_types() {
        let conn = connect("memory://")
            .read_consistency_interval(Duration::from_secs(0))
            .execute()
            .await
            .unwrap();

        let schema = Arc::new(Schema::new(vec![
            Field::new("int32", DataType::Int32, false),
            Field::new("int64", DataType::Int64, false),
            Field::new("uint32", DataType::UInt32, false),
            Field::new("string", DataType::Utf8, false),
            Field::new("large_string", DataType::LargeUtf8, false),
            Field::new("float32", DataType::Float32, false),
            Field::new("float64", DataType::Float64, false),
            Field::new("bool", DataType::Boolean, false),
            Field::new("date32", DataType::Date32, false),
            Field::new(
                "timestamp_ns",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new(
                "timestamp_ms",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new(
                "vec_f32",
                DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), 2),
                false,
            ),
            Field::new(
                "vec_f64",
                DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float64, true)), 2),
                false,
            ),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from_iter_values(0..10)),
                Arc::new(Int64Array::from_iter_values(0..10)),
                Arc::new(UInt32Array::from_iter_values(0..10)),
                Arc::new(StringArray::from_iter_values(vec![
                    "a", "b", "c", "d", "e", "f", "g", "h", "i", "j",
                ])),
                Arc::new(LargeStringArray::from_iter_values(vec![
                    "a", "b", "c", "d", "e", "f", "g", "h", "i", "j",
                ])),
                Arc::new(Float32Array::from_iter_values((0..10).map(|i| i as f32))),
                Arc::new(Float64Array::from_iter_values((0..10).map(|i| i as f64))),
                Arc::new(Into::<BooleanArray>::into(vec![
                    true, false, true, false, true, false, true, false, true, false,
                ])),
                Arc::new(Date32Array::from_iter_values(0..10)),
                Arc::new(TimestampNanosecondArray::from_iter_values(0..10)),
                Arc::new(TimestampMillisecondArray::from_iter_values(0..10)),
                Arc::new(
                    create_fixed_size_list(
                        Float32Array::from_iter_values((0..20).map(|i| i as f32)),
                        2,
                    )
                    .unwrap(),
                ),
                Arc::new(
                    create_fixed_size_list(
                        Float64Array::from_iter_values((0..20).map(|i| i as f64)),
                        2,
                    )
                    .unwrap(),
                ),
            ],
        )
        .unwrap();

        let table = conn
            .create_table("my_table", batch)
            .execute()
            .await
            .unwrap();

        // check it can do update for each type
        let updates: Vec<(&str, &str)> = vec![
            ("string", "'foo'"),
            ("large_string", "'large_foo'"),
            ("int32", "1"),
            ("int64", "1"),
            ("uint32", "1"),
            ("float32", "1.0"),
            ("float64", "1.0"),
            ("bool", "true"),
            ("date32", "1"),
            ("timestamp_ns", "1"),
            ("timestamp_ms", "1"),
            ("vec_f32", "[1.0, 1.0]"),
            ("vec_f64", "[1.0, 1.0]"),
        ];

        let mut update_op = table.update();
        for (column, value) in updates {
            update_op = update_op.column(column, value);
        }
        update_op.execute().await.unwrap();

        let mut batches = table
            .query()
            .select(Select::columns(&[
                "string",
                "large_string",
                "int32",
                "int64",
                "uint32",
                "float32",
                "float64",
                "bool",
                "date32",
                "timestamp_ns",
                "timestamp_ms",
                "vec_f32",
                "vec_f64",
            ]))
            .execute()
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        let batch = batches.pop().unwrap();

        macro_rules! assert_column {
            ($column:expr, $array_type:ty, $expected:expr) => {
                let array = $column
                    .as_any()
                    .downcast_ref::<$array_type>()
                    .unwrap()
                    .iter()
                    .collect::<Vec<_>>();
                for v in array {
                    assert_eq!(v, Some($expected));
                }
            };
        }

        assert_column!(batch.column(0), StringArray, "foo");
        assert_column!(batch.column(1), LargeStringArray, "large_foo");
        assert_column!(batch.column(2), Int32Array, 1);
        assert_column!(batch.column(3), Int64Array, 1);
        assert_column!(batch.column(4), UInt32Array, 1);
        assert_column!(batch.column(5), Float32Array, 1.0);
        assert_column!(batch.column(6), Float64Array, 1.0);
        assert_column!(batch.column(7), BooleanArray, true);
        assert_column!(batch.column(8), Date32Array, 1);
        assert_column!(batch.column(9), TimestampNanosecondArray, 1);
        assert_column!(batch.column(10), TimestampMillisecondArray, 1);

        let array = batch
            .column(11)
            .as_any()
            .downcast_ref::<FixedSizeListArray>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>();
        for v in array {
            let v = v.unwrap();
            let f32array = v.as_any().downcast_ref::<Float32Array>().unwrap();
            for v in f32array {
                assert_eq!(v, Some(1.0));
            }
        }

        let array = batch
            .column(12)
            .as_any()
            .downcast_ref::<FixedSizeListArray>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>();
        for v in array {
            let v = v.unwrap();
            let f64array = v.as_any().downcast_ref::<Float64Array>().unwrap();
            for v in f64array {
                assert_eq!(v, Some(1.0));
            }
        }
    }
    ///Two helper functions
    fn create_fixed_size_list<T: Array>(
        values: T,
        list_size: i32,
    ) -> Result<FixedSizeListArray, ArrowError> {
        let list_type = DataType::FixedSizeList(
            Arc::new(Field::new("item", values.data_type().clone(), true)),
            list_size,
        );
        let data = ArrayDataBuilder::new(list_type)
            .len(values.len() / list_size as usize)
            .add_child_data(values.into_data())
            .build()
            .unwrap();

        Ok(FixedSizeListArray::from(data))
    }

    fn make_test_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("i", DataType::Int32, false)]));
        RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from_iter_values(0..10))],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn test_update_with_predicate() {
        let conn = connect("memory://")
            .read_consistency_interval(Duration::from_secs(0))
            .execute()
            .await
            .unwrap();

        let batch = record_batch!(
            ("id", Int32, [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]),
            (
                "name",
                Utf8,
                ["a", "b", "c", "d", "e", "f", "g", "h", "i", "j"]
            )
        )
        .unwrap();

        let table = conn
            .create_table("my_table", batch)
            .execute()
            .await
            .unwrap();

        table
            .update()
            .only_if("id > 5")
            .column("name", "'foo'")
            .execute()
            .await
            .unwrap();

        let mut batches = table
            .query()
            .select(Select::columns(&["id", "name"]))
            .execute()
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        while let Some(batch) = batches.pop() {
            let ids = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>();
            let names = batch
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>();
            for (i, name) in names.iter().enumerate() {
                let id = ids[i].unwrap();
                let name = name.unwrap();
                if id > 5 {
                    assert_eq!(name, "foo");
                } else {
                    assert_eq!(name, &format!("{}", (b'a' + id as u8) as char));
                }
            }
        }
    }

    #[tokio::test]
    async fn test_update_materializes_offset_columns_before_filter() {
        let batch = record_batch!(
            ("id", Int32, [0, 1, 2, 3]),
            (
                "split",
                Utf8,
                [Some("test"), None, Some("test"), Some("train")]
            ),
            ("payload", Utf8, ["a", "b", "c", "d"])
        )
        .unwrap();
        let conn = connect("memory://")
            .database_options(&ListingDatabaseOptions {
                new_table_config: NewTableConfig {
                    data_storage_version: Some(LanceFileVersion::Legacy),
                    ..Default::default()
                },
                ..Default::default()
            })
            .execute()
            .await
            .unwrap();
        let table = conn
            .create_table("offset_table", batch.clone())
            .execute()
            .await
            .unwrap();
        table.add(batch).execute().await.unwrap();
        table
            .create_index(&["split"], Index::BTree(BTreeIndexBuilder::default()))
            .execute()
            .await
            .unwrap();

        let dataset = table.dataset().unwrap().get().await.unwrap();
        let planner = Planner::new(Arc::new(dataset.schema().into()));

        let filter = planner.parse_filter("split = 'test'").unwrap();
        let filter = planner.optimize_expr(filter).unwrap();
        let mut scanner = dataset.scan();
        scanner.with_row_id().filter_expr(filter);
        let explanation = scanner.explain_plan(false).await.unwrap();
        let plan = scanner.create_plan().await.unwrap();
        assert!(
            contains_take(plan.as_ref()),
            "test setup must late-materialize payload:\n{explanation}"
        );

        let guarded_filter = super::safe_update_filter("split = 'test'", dataset.as_ref());
        let filter = planner.parse_filter(&guarded_filter).unwrap();
        let filter = planner.optimize_expr(filter).unwrap();
        let mut scanner = dataset.scan();
        scanner.with_row_id().filter_expr(filter);
        let explanation = scanner.explain_plan(false).await.unwrap();
        let plan = scanner.create_plan().await.unwrap();

        // Regression test for #1291: the payload must be read by the scan, not
        // concatenated across fragments by a late-materializing TakeExec.
        assert!(
            !contains_take(plan.as_ref()),
            "unexpected late materialization:\n{explanation}"
        );

        let result = table
            .update()
            .only_if("split = 'test'")
            .column("split", "'TEST'")
            .execute()
            .await
            .unwrap();

        assert_eq!(result.rows_updated, 4);
        assert_eq!(
            table
                .count_rows(Some("split = 'TEST'".to_string()))
                .await
                .unwrap(),
            4
        );
        assert_eq!(
            table
                .count_rows(Some("payload IN ('a', 'b', 'c', 'd')".to_string()))
                .await
                .unwrap(),
            8
        );
    }

    #[tokio::test]
    async fn test_update_v2_keeps_indexed_plan() {
        let batch = record_batch!(
            ("id", Int32, [0, 1, 2, 3]),
            ("split", Utf8, ["test", "train", "test", "train"]),
            ("payload", Utf8, ["a", "b", "c", "d"])
        )
        .unwrap();
        let conn = connect("memory://")
            .database_options(&ListingDatabaseOptions {
                new_table_config: NewTableConfig {
                    data_storage_version: Some(LanceFileVersion::V2_0),
                    ..Default::default()
                },
                ..Default::default()
            })
            .execute()
            .await
            .unwrap();
        let table = conn
            .create_table("v2_offset_table", batch.clone())
            .execute()
            .await
            .unwrap();
        table.add(batch).execute().await.unwrap();
        table
            .create_index(&["split"], Index::BTree(BTreeIndexBuilder::default()))
            .execute()
            .await
            .unwrap();

        let dataset = table.dataset().unwrap().get().await.unwrap();
        let predicate = "split = 'test'";
        let update_filter = super::safe_update_filter(predicate, dataset.as_ref());
        assert_eq!(update_filter, predicate);

        let planner = Planner::new(Arc::new(dataset.schema().into()));
        let filter = planner.parse_filter(&update_filter).unwrap();
        let filter = planner.optimize_expr(filter).unwrap();
        let mut scanner = dataset.scan();
        scanner.with_row_id().filter_expr(filter);
        let explanation = scanner.explain_plan(false).await.unwrap();
        let plan = scanner.create_plan().await.unwrap();
        assert!(
            explanation.contains("ScalarIndexQuery"),
            "v2 plan unexpectedly lost its scalar index:\n{explanation}"
        );
        assert!(
            !contains_take(plan.as_ref()),
            "v2 plan unexpectedly used the legacy TakeExec path:\n{explanation}"
        );

        let result = table
            .update()
            .only_if(predicate)
            .column("split", "'TEST'")
            .execute()
            .await
            .unwrap();
        assert_eq!(result.rows_updated, 4);
    }

    #[tokio::test]
    async fn test_update_accepts_trailing_comment_filter() {
        let conn = connect("memory://")
            .database_options(&ListingDatabaseOptions {
                new_table_config: NewTableConfig {
                    data_storage_version: Some(LanceFileVersion::Legacy),
                    ..Default::default()
                },
                ..Default::default()
            })
            .execute()
            .await
            .unwrap();
        let batch = record_batch!(("id", Int32, [1, 2]), ("payload", Utf8, ["a", "b"])).unwrap();
        let table = conn
            .create_table("trailing_comment", batch)
            .execute()
            .await
            .unwrap();

        let predicate = "id = 1 -- valid trailing comment";
        assert_eq!(table.count_rows(Some(predicate.into())).await.unwrap(), 1);
        let result = table
            .update()
            .only_if(predicate)
            .column("payload", "'updated'")
            .execute()
            .await
            .unwrap();

        assert_eq!(result.rows_updated, 1);
        assert_eq!(
            table
                .count_rows(Some("payload = 'updated'".into()))
                .await
                .unwrap(),
            1
        );
    }

    #[tokio::test]
    async fn test_update_boolean_index_uses_early_materialization() {
        let conn = connect("memory://")
            .database_options(&ListingDatabaseOptions {
                new_table_config: NewTableConfig {
                    data_storage_version: Some(LanceFileVersion::Legacy),
                    ..Default::default()
                },
                ..Default::default()
            })
            .execute()
            .await
            .unwrap();
        let batch = record_batch!(
            ("flag", Boolean, [true, false]),
            ("payload", Utf8, ["a", "b"])
        )
        .unwrap();
        let table = conn
            .create_table("boolean_index", batch.clone())
            .execute()
            .await
            .unwrap();
        table.add(batch).execute().await.unwrap();
        table
            .create_index(&["flag"], Index::BTree(BTreeIndexBuilder::default()))
            .execute()
            .await
            .unwrap();

        let dataset = table.dataset().unwrap().get().await.unwrap();
        let guarded_filter = super::safe_update_filter("flag", dataset.as_ref());
        let mut scanner = dataset.scan();
        scanner.with_row_id().filter(&guarded_filter).unwrap();
        let explanation = scanner.explain_plan(false).await.unwrap();
        let plan = scanner.create_plan().await.unwrap();
        assert!(
            !contains_take(plan.as_ref()),
            "Boolean predicate retained late materialization:\n{explanation}"
        );
        assert!(
            !explanation.contains("MaterializeIndex"),
            "Boolean predicate retained scalar-index extraction:\n{explanation}"
        );

        let result = table
            .update()
            .only_if("flag")
            .column("payload", "'updated'")
            .execute()
            .await
            .unwrap();
        assert_eq!(result.rows_updated, 2);
        assert_eq!(
            table
                .count_rows(Some("payload = 'updated'".into()))
                .await
                .unwrap(),
            2
        );
    }

    #[tokio::test]
    async fn test_update_accepts_quoted_reserved_identifier() {
        let conn = connect("memory://")
            .database_options(&ListingDatabaseOptions {
                new_table_config: NewTableConfig {
                    data_storage_version: Some(LanceFileVersion::Legacy),
                    ..Default::default()
                },
                ..Default::default()
            })
            .execute()
            .await
            .unwrap();
        let batch =
            record_batch!(("select", Int32, [1, 2]), ("payload", Utf8, ["a", "b"])).unwrap();
        let table = conn
            .create_table("reserved_identifier", batch.clone())
            .execute()
            .await
            .unwrap();
        table.add(batch).execute().await.unwrap();

        let predicate = "`select` = 1";
        assert_eq!(table.count_rows(Some(predicate.into())).await.unwrap(), 2);
        let result = table
            .update()
            .only_if(predicate)
            .column("payload", "'updated'")
            .execute()
            .await
            .unwrap();

        assert_eq!(result.rows_updated, 2);
        assert_eq!(
            table
                .count_rows(Some("payload = 'updated'".into()))
                .await
                .unwrap(),
            2
        );
    }

    #[tokio::test]
    async fn test_update_via_expr() {
        let conn = connect("memory://")
            .read_consistency_interval(Duration::from_secs(0))
            .execute()
            .await
            .unwrap();
        let tbl = conn
            .create_table("my_table", make_test_batch())
            .execute()
            .await
            .unwrap();
        assert_eq!(1, tbl.count_rows(Some("i == 0".to_string())).await.unwrap());
        tbl.update().column("i", "i+1").execute().await.unwrap();
        assert_eq!(0, tbl.count_rows(Some("i == 0".to_string())).await.unwrap());
    }
}
