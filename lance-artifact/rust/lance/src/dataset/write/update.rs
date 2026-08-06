// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::Duration;

use super::cleanup_data_fragments;
use super::retry::{RetryConfig, RetryExecutor, execute_with_retry};
use super::{CommitBuilder, WriteParams, write_fragments_internal};
use crate::dataset::rowids::get_row_id_index;
use crate::dataset::transaction::UpdateMode::RewriteRows;
use crate::dataset::transaction::{Operation, Transaction};
use crate::dataset::utils::make_rowid_capture_stream;
use crate::{Dataset, io::exec::Planner};
use crate::{Error, Result};
use arrow_array::RecordBatch;
use arrow_schema::{ArrowError, DataType, Schema as ArrowSchema};
use datafusion::common::DFSchema;
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::logical_expr::ExprSchemable;
use datafusion::physical_plan::PhysicalExpr;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::prelude::Expr;
use datafusion::scalar::ScalarValue;
use futures::StreamExt;
use lance_arrow::RecordBatchExt;
use lance_core::datatypes::BlobHandling;
use lance_core::error::{InvalidInputSnafu, box_error};
use lance_core::utils::tokio::get_num_compute_intensive_cpus;
use lance_core::{ROW_ADDR_FIELD, ROW_ID_FIELD, ROW_OFFSET_FIELD};
use lance_datafusion::expr::safe_coerce_scalar;
use lance_select::RowAddrTreeMap;
use lance_table::format::{Fragment, RowIdMeta};
use roaring::RoaringTreemap;
use snafu::ResultExt;

/// Collect a field id and all of its descendant field ids (pre-order). A struct
/// column update rewrites the whole subtree, so an index on any descendant must be
/// treated as modified.
fn collect_subtree_field_ids(field: &lance_core::datatypes::Field, out: &mut Vec<u32>) {
    out.push(field.id as u32);
    for child in &field.children {
        collect_subtree_field_ids(child, out);
    }
}

/// Build an update operation.
///
/// This operation is similar to SQL's UPDATE statement. It allows you to change
/// the values of all or a subset of columns with SQL expressions.
///
/// Use the [UpdateBuilder] to construct an update job. For example:
///
/// ```
/// # use lance::{Dataset, Result};
/// # use lance::dataset::UpdateBuilder;
/// # use std::sync::Arc;
/// # async fn example(dataset: Arc<Dataset>) -> Result<()> {
/// let result = UpdateBuilder::new(dataset)
///     .update_where("region_id = 10")?
///     .set("region_name", "New York")?
///     .build()?
///     .execute()
///     .await?;
/// # Ok(())
/// # }
/// ```
///
#[derive(Debug, Clone)]
pub struct UpdateBuilder {
    /// The dataset snapshot to update.
    dataset: Arc<Dataset>,
    /// The condition to apply to find matching rows to update. If None, all rows are updated.
    condition: Option<Expr>,
    /// The updates to apply to matching rows.
    updates: HashMap<String, Expr>,
    /// Number of times to retry on commit conflicts.
    conflict_retries: u32,
    /// Total timeout for retries.
    retry_timeout: Duration,
}

impl UpdateBuilder {
    pub fn new(dataset: Arc<Dataset>) -> Self {
        Self {
            dataset,
            condition: None,
            updates: HashMap::new(),
            conflict_retries: 10,
            retry_timeout: Duration::from_secs(30),
        }
    }

    fn filterable_schema(dataset_schema: &lance_core::datatypes::Schema) -> ArrowSchema {
        let extra_columns = ArrowSchema::new(vec![
            ROW_ID_FIELD.clone(),
            ROW_ADDR_FIELD.clone(),
            ROW_OFFSET_FIELD.clone(),
        ]);
        let merged = dataset_schema
            .merge(&extra_columns)
            .expect("Failed to merge system columns into filterable schema");
        (&merged).into()
    }

    pub fn update_where(mut self, filter: &str) -> Result<Self> {
        let filter_schema = Self::filterable_schema(self.dataset.schema());
        let planner = Planner::new(Arc::new(filter_schema));
        let expr = planner
            .parse_filter(filter)
            .map_err(box_error)
            .context(InvalidInputSnafu {})?;
        self.condition = Some(
            planner
                .optimize_expr(expr)
                .map_err(box_error)
                .context(InvalidInputSnafu {})?,
        );
        Ok(self)
    }

    pub fn set(mut self, column: impl AsRef<str>, value: &str) -> Result<Self> {
        let field = self
            .dataset
            .schema()
            .field(column.as_ref())
            .ok_or_else(|| {
                Error::invalid_input(format!(
                    "Column '{}' does not exist in dataset schema: {:?}",
                    column.as_ref(),
                    self.dataset.schema()
                ))
            })?;

        // TODO: support nested column references. This is mostly blocked on the
        // ability to insert them into the RecordBatch properly.
        if column.as_ref().contains('.') {
            return Err(Error::not_supported_source(
                format!(
                    "Nested column references are not yet supported. Referenced: {}",
                    column.as_ref(),
                )
                .into(),
            ));
        }

        let schema: Arc<ArrowSchema> = Arc::new(self.dataset.schema().into());
        let planner = Planner::new(schema.clone());
        let mut expr = planner
            .parse_expr(value)
            .map_err(box_error)
            .context(InvalidInputSnafu {})?;

        // Cast expression to the column's data type if necessary.
        let dest_type = field.data_type();
        let df_schema = DFSchema::try_from(schema.as_ref().clone())?;
        let src_type = expr
            .get_type(&df_schema)
            .map_err(box_error)
            .context(InvalidInputSnafu {})?;
        if dest_type != src_type {
            expr = match expr {
                // TODO: remove this branch once DataFusion supports casting List to FSL
                // This should happen in Arrow 51.0.0
                Expr::Literal(value @ ScalarValue::List(_), metadata)
                    if matches!(dest_type, DataType::FixedSizeList(_, _)) =>
                {
                    Expr::Literal(
                        safe_coerce_scalar(&value, &dest_type).ok_or_else(|| {
                            ArrowError::CastError(format!(
                                "Failed to cast {} to {} during planning",
                                value.data_type(),
                                dest_type
                            ))
                        })?,
                        metadata,
                    )
                }
                _ => expr
                    .cast_to(&dest_type, &df_schema)
                    .map_err(box_error)
                    .context(InvalidInputSnafu {})?,
            };
        }

        // Optimize the expression. For example, this might apply the cast on
        // literals. (Expr.cast_to() only wraps the expression in a Cast node,
        // it doesn't actually apply the cast to the literals.)
        let expr = planner
            .optimize_expr(expr)
            .map_err(box_error)
            .context(InvalidInputSnafu {})?;

        self.updates.insert(column.as_ref().to_string(), expr);
        Ok(self)
    }

    /// Set the number of times to retry on commit conflicts.
    ///
    /// Default is 10.
    pub fn conflict_retries(mut self, retries: u32) -> Self {
        self.conflict_retries = retries;
        self
    }

    /// Set the total timeout for all retries.
    ///
    /// Default is 30 seconds.
    pub fn retry_timeout(mut self, timeout: Duration) -> Self {
        self.retry_timeout = timeout;
        self
    }

    // TODO: set write params
    // pub fn with_write_params(mut self, params: WriteParams) -> Self { ... }

    pub fn build(self) -> Result<UpdateJob> {
        let mut updates = HashMap::new();

        let planner = Planner::new(Arc::new(self.dataset.schema().into()));

        for (column, expr) in self.updates {
            let physical_expr = planner.create_physical_expr(&expr)?;
            updates.insert(column, physical_expr);
        }

        if updates.is_empty() {
            return Err(Error::invalid_input("No updates provided"));
        }

        let updates = Arc::new(updates);

        Ok(UpdateJob {
            dataset: self.dataset,
            condition: self.condition,
            updates,
            conflict_retries: self.conflict_retries,
            retry_timeout: self.retry_timeout,
        })
    }
}

// TODO: support distributed operation.

#[derive(Debug, Clone)]
pub struct UpdateResult {
    pub new_dataset: Arc<Dataset>,
    pub rows_updated: u64,
}

#[derive(Debug)]
pub struct UpdateData {
    removed_fragment_ids: Vec<u64>,
    old_fragments: Vec<Fragment>,
    new_fragments: Vec<Fragment>,
    affected_rows: RowAddrTreeMap,
    num_updated_rows: u64,
}

#[derive(Debug, Clone)]
pub struct UpdateJob {
    dataset: Arc<Dataset>,
    condition: Option<Expr>,
    updates: Arc<HashMap<String, Arc<dyn PhysicalExpr>>>,
    conflict_retries: u32,
    retry_timeout: Duration,
}

impl UpdateJob {
    pub async fn execute(self) -> Result<UpdateResult> {
        let dataset = self.dataset.clone();
        let config = RetryConfig {
            max_retries: self.conflict_retries,
            retry_timeout: self.retry_timeout,
        };

        Box::pin(execute_with_retry(self, dataset, config)).await
    }

    async fn execute_impl(self) -> Result<UpdateData> {
        let mut scanner = self.dataset.scan();
        scanner.with_row_id();
        scanner.blob_handling(BlobHandling::AllBinary);

        if let Some(expr) = &self.condition {
            scanner.filter_expr(expr.clone());
        }

        let stream = scanner
            .try_into_dfstream(scanner.execution_options())
            .await?;

        // We keep track of seen row ids so we can delete them from the existing
        // fragments and then set the row id segments in the new fragments.
        let (stream, row_id_rx) =
            make_rowid_capture_stream(stream, self.dataset.manifest.uses_stable_row_ids())?;

        let schema = stream.schema();

        let expected_schema = self.dataset.schema().into();
        if schema.as_ref() != &expected_schema {
            return Err(Error::internal(format!(
                "Expected schema {:?} but got {:?}",
                expected_schema, schema
            )));
        }

        let updates_ref = self.updates.clone();
        let stream = stream
            .map(move |batch| {
                let updates = updates_ref.clone();
                tokio::task::spawn_blocking(move || Self::apply_updates(batch?, updates))
            })
            .buffered(get_num_compute_intensive_cpus())
            .map(|res| match res {
                Ok(Ok(batch)) => Ok(batch),
                Ok(Err(err)) => Err(err),
                Err(e) => Err(DataFusionError::ExecutionJoin(Box::new(e))),
            });
        let stream = RecordBatchStreamAdapter::new(schema, stream);

        let (mut new_fragments, _) = write_fragments_internal(
            self.dataset
                .manifest
                .data_storage_format
                .lance_file_format(),
            Some(&self.dataset),
            self.dataset.object_store.clone(),
            &self.dataset.base,
            self.dataset.schema().clone(),
            Box::pin(stream),
            WriteParams::default(),
            None, // TODO: support multiple bases for update
        )
        .await?;

        let removed_row_ids = row_id_rx
            .try_recv()
            .map_err(|err| Error::internal(format!("Failed to receive row ids: {}", err)))?;

        if let Some(row_id_sequence) = removed_row_ids.row_id_sequence() {
            let fragment_sizes = new_fragments
                .iter()
                .map(|f| f.physical_rows.unwrap() as u64);
            let sequences = lance_table::rowids::rechunk_sequences(
                [row_id_sequence.clone()],
                fragment_sizes,
                false,
            )
            .map_err(|e| {
                Error::internal(format!(
                    "Captured row ids not equal to number of rows written: {}",
                    e
                ))
            })?;
            for (fragment, sequence) in new_fragments.iter_mut().zip(sequences) {
                let serialized = lance_table::rowids::write_row_ids(&sequence);
                fragment.row_id_meta = Some(RowIdMeta::Inline(serialized));
            }
        }

        // Apply deletions
        let row_id_index = get_row_id_index(&self.dataset).await?;
        let row_addrs = removed_row_ids.row_addrs(row_id_index.as_deref());
        let deletions_result = self.apply_deletions(&row_addrs).await;
        let (old_fragments, removed_fragment_ids) = match deletions_result {
            Ok(v) => v,
            Err(e) => {
                cleanup_data_fragments(
                    &self.dataset.object_store,
                    &self.dataset.base,
                    None,
                    &new_fragments,
                )
                .await;
                return Err(e);
            }
        };
        let affected_rows = RowAddrTreeMap::from(row_addrs.as_ref().clone());

        let num_updated_rows = new_fragments
            .iter()
            .map(|f| f.physical_rows.unwrap() as u64)
            .sum::<u64>();

        Ok(UpdateData {
            removed_fragment_ids,
            old_fragments,
            new_fragments,
            affected_rows,
            num_updated_rows,
        })
    }

    async fn commit_impl(
        &self,
        dataset: Arc<Dataset>,
        update_data: UpdateData,
    ) -> Result<UpdateResult> {
        // Updated columns are top-level (nested references are rejected by `set`), but a
        // struct-column update rewrites all of its descendants. Collect the full field
        // subtree so an index on a nested child field is recognized as modified and not
        // wrongly extended over the rewritten fragment.
        let mut fields_for_preserving_frag_bitmap = Vec::new();
        for column_name in self.updates.keys() {
            if let Some(field) = dataset.schema().field(column_name) {
                collect_subtree_field_ids(field, &mut fields_for_preserving_frag_bitmap);
            }
        }

        // Commit updated and new fragments
        let operation = Operation::Update {
            removed_fragment_ids: update_data.removed_fragment_ids,
            updated_fragments: update_data.old_fragments,
            new_fragments: update_data.new_fragments,
            // In "rewrite rows" mode, the rows that are updated in the fragment
            // are moved(deleted and appended).
            // so we do not need to handle the frag bitmap of the index about it.
            fields_modified: vec![],
            compacted_sstables: Vec::new(),
            fields_for_preserving_frag_bitmap,
            update_mode: Some(RewriteRows),
            inserted_rows_filter: None,
            updated_fragment_offsets: None,
        };

        let transaction = Transaction::new(dataset.manifest.version, operation, None);

        let new_dataset = CommitBuilder::new(dataset)
            .with_affected_rows(update_data.affected_rows)
            .execute(transaction)
            .await?;

        Ok(UpdateResult {
            new_dataset: Arc::new(new_dataset),
            rows_updated: update_data.num_updated_rows,
        })
    }

    fn apply_updates(
        mut batch: RecordBatch,
        updates: Arc<HashMap<String, Arc<dyn PhysicalExpr>>>,
    ) -> DFResult<RecordBatch> {
        for (column, expr) in updates.iter() {
            let new_values = expr.evaluate(&batch)?.into_array(batch.num_rows())?;
            batch = batch.replace_column_by_name(column.as_str(), new_values)?;
        }
        Ok(batch)
    }

    /// Use previous found rows ids to delete rows from existing fragments.
    ///
    /// Returns the set of modified fragments and removed fragments, if any.
    async fn apply_deletions(
        &self,
        removed_row_addrs: &RoaringTreemap,
    ) -> Result<(Vec<Fragment>, Vec<u64>)> {
        let bitmaps = Arc::new(removed_row_addrs.bitmaps().collect::<BTreeMap<_, _>>());

        enum FragmentChange {
            Unchanged,
            Modified(Box<Fragment>),
            Removed(u64),
        }

        let mut updated_fragments = Vec::new();
        let mut removed_fragments = Vec::new();

        let mut stream = futures::stream::iter(self.dataset.get_fragments())
            .map(move |fragment| {
                let bitmaps_ref = bitmaps.clone();
                async move {
                    let fragment_id = fragment.id();
                    if let Some(bitmap) = bitmaps_ref.get(&(fragment_id as u32)) {
                        match fragment.extend_deletions(*bitmap).await {
                            Ok(Some(new_fragment)) => {
                                Ok(FragmentChange::Modified(Box::new(new_fragment.metadata)))
                            }
                            Ok(None) => Ok(FragmentChange::Removed(fragment_id as u64)),
                            Err(e) => Err(e),
                        }
                    } else {
                        Ok(FragmentChange::Unchanged)
                    }
                }
            })
            .buffer_unordered(self.dataset.object_store.io_parallelism());

        while let Some(res) = stream.next().await.transpose()? {
            match res {
                FragmentChange::Unchanged => {}
                FragmentChange::Modified(fragment) => updated_fragments.push(*fragment),
                FragmentChange::Removed(fragment_id) => removed_fragments.push(fragment_id),
            }
        }

        Ok((updated_fragments, removed_fragments))
    }
}

impl RetryExecutor for UpdateJob {
    type Data = UpdateData;
    type Result = UpdateResult;

    async fn execute_impl(&self) -> Result<Self::Data> {
        self.clone().execute_impl().await
    }

    async fn commit(&self, dataset: Arc<Dataset>, data: Self::Data) -> Result<Self::Result> {
        self.commit_impl(dataset, data).await
    }

    fn update_dataset(&mut self, dataset: Arc<Dataset>) {
        self.dataset = dataset;
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::time::Duration;

    use crate::{
        dataset::{InsertBuilder, ReadParams, WriteParams, builder::DatasetBuilder},
        session::Session,
        utils::test::ThrottledStoreWrapper,
    };

    use super::*;

    use crate::dataset::{WriteDestination, WriteMode};
    use crate::index::DatasetIndexExt;
    use crate::index::vector::VectorIndexParams;
    use crate::utils::test::{DatagenExt, FragmentCount, FragmentRowCount};
    use arrow::{
        array::AsArray,
        datatypes::{Int64Type, UInt32Type},
    };
    use arrow_array::types::{Float32Type, Int32Type};
    use arrow_array::{Int64Array, RecordBatchIterator, StringArray, UInt32Array, UInt64Array};
    use arrow_schema::{Field, Schema as ArrowSchema};
    use arrow_select::concat::concat_batches;
    use futures::{TryStreamExt, future::try_join_all};
    use lance_arrow::ARROW_EXT_NAME_KEY;
    use lance_arrow::json::{ARROW_JSON_EXT_NAME, is_arrow_json_field, is_json_field};
    use lance_core::ROW_ID;
    use lance_core::utils::tempfile::TempStrDir;
    use lance_datagen::{Dimension, RowCount};
    use lance_file::version::LanceFileVersion;
    use lance_index::IndexType;
    use lance_index::scalar::{BuiltinIndexType, ScalarIndexParams};
    use lance_io::object_store::ObjectStoreParams;
    use lance_linalg::distance::MetricType;
    use object_store::throttle::ThrottleConfig;
    use rstest::rstest;
    use tokio::sync::Barrier;

    /// Returns a dataset with 3 fragments, each with 10 rows.
    ///
    /// Also returns the TempDir, which should be kept alive as long as the
    /// dataset is being accessed. Once that is dropped, the temp directory is
    /// deleted.
    async fn make_test_dataset(
        version: LanceFileVersion,
        enable_stable_row_ids: bool,
    ) -> (Arc<Dataset>, TempStrDir) {
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from_iter_values(0..30)),
                Arc::new(StringArray::from_iter_values(std::iter::repeat_n(
                    "foo", 30,
                ))),
            ],
        )
        .unwrap();

        let write_params = WriteParams {
            max_rows_per_file: 10,
            data_storage_version: Some(version),
            enable_stable_row_ids,
            ..Default::default()
        };

        let test_dir = TempStrDir::default();
        let test_uri = &test_dir;

        let batches = RecordBatchIterator::new([Ok(batch)], schema.clone());
        let ds = Dataset::write(batches, test_uri, Some(write_params))
            .await
            .unwrap();

        (Arc::new(ds), test_dir)
    }

    #[tokio::test]
    async fn test_update_validation() {
        let (dataset, _test_dir) = make_test_dataset(LanceFileVersion::Legacy, false).await;

        let builder = UpdateBuilder::new(dataset);

        assert!(
            matches!(
                builder.clone().update_where("foo = 10"),
                Err(Error::InvalidInput { .. })
            ),
            "Should return error if condition references non-existent column"
        );

        assert!(
            matches!(
                builder.clone().set("foo", "1"),
                Err(Error::InvalidInput { .. })
            ),
            "Should return error if update key references non-existent column"
        );

        assert!(
            matches!(
                builder.clone().set("id", "id2 + 1"),
                Err(Error::InvalidInput { .. })
            ),
            "Should return error if update expression references non-existent column"
        );

        assert!(
            matches!(builder.build(), Err(Error::InvalidInput { .. })),
            "Should return error if no update expressions are provided"
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_update_all(
        #[values(LanceFileVersion::Legacy, LanceFileVersion::V2_0)] version: LanceFileVersion,
        #[values(false, true)] enable_stable_row_ids: bool,
    ) {
        let (dataset, _test_dir) = make_test_dataset(version, enable_stable_row_ids).await;

        let update_result = UpdateBuilder::new(dataset)
            .set("name", "'bar' || cast(id as string)")
            .unwrap()
            .build()
            .unwrap()
            .execute()
            .await
            .unwrap();

        let dataset = update_result.new_dataset;
        let actual_batches = dataset
            .scan()
            .try_into_stream()
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        let actual_batch = concat_batches(&actual_batches[0].schema(), &actual_batches).unwrap();

        let expected = RecordBatch::try_new(
            Arc::new(dataset.schema().into()),
            vec![
                Arc::new(Int64Array::from_iter_values(0..30)),
                Arc::new(StringArray::from_iter_values(
                    (0..30).map(|i| format!("bar{}", i)),
                )),
            ],
        )
        .unwrap();

        assert_eq!(actual_batch, expected);

        assert_eq!(dataset.get_fragments().len(), 1);
    }

    #[rstest]
    #[tokio::test]
    async fn test_update_conditional(
        #[values(LanceFileVersion::Legacy, LanceFileVersion::V2_0)] version: LanceFileVersion,
        #[values(false, true)] enable_stable_row_ids: bool,
    ) {
        let (dataset, _test_dir) = make_test_dataset(version, enable_stable_row_ids).await;

        let original_fragments = dataset.get_fragments();

        let update_result = UpdateBuilder::new(dataset)
            .update_where("id >= 15")
            .unwrap()
            .set("name", "'bar' || cast(id as string)")
            .unwrap()
            .build()
            .unwrap()
            .execute()
            .await
            .unwrap();

        let dataset = update_result.new_dataset;
        let actual_batches = dataset
            .scan()
            .try_into_stream()
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        let actual_batch = concat_batches(&actual_batches[0].schema(), &actual_batches).unwrap();

        let expected = RecordBatch::try_new(
            Arc::new(dataset.schema().into()),
            vec![
                Arc::new(Int64Array::from_iter_values(0..30)),
                Arc::new(StringArray::from_iter_values(
                    (0..15)
                        .map(|_| "foo".to_string())
                        .chain((15..30).map(|i| format!("bar{}", i))),
                )),
            ],
        )
        .unwrap();

        assert_eq!(actual_batch, expected);

        let fragments = dataset.get_fragments();
        assert_eq!(fragments.len(), 3);

        // One fragment not touched (id = 0..10)
        assert_eq!(fragments[0].metadata.id, original_fragments[0].metadata.id);
        assert_eq!(
            fragments[0].metadata.files,
            original_fragments[0].metadata.files
        );
        assert_eq!(
            fragments[0].metadata.physical_rows,
            original_fragments[0].metadata.physical_rows
        );
        assert_eq!(
            fragments[0].metadata.row_id_meta,
            original_fragments[0].metadata.row_id_meta
        );
        // One fragment partially modified (id = 10..15)
        assert_eq!(
            fragments[1].metadata.files,
            original_fragments[1].metadata.files,
        );
        assert_eq!(
            fragments[1]
                .metadata
                .deletion_file
                .as_ref()
                .and_then(|f| f.num_deleted_rows),
            Some(5)
        );
        // One fragment fully modified
        assert_eq!(fragments[2].metadata.physical_rows, Some(15));
    }

    #[tokio::test]
    async fn test_update_json_and_regular_columns() {
        let mut metadata = HashMap::new();
        metadata.insert(
            ARROW_EXT_NAME_KEY.to_string(),
            ARROW_JSON_EXT_NAME.to_string(),
        );
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("meta", DataType::Utf8, true).with_metadata(metadata),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from_iter_values([1, 2, 3])),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
                Arc::new(StringArray::from(vec![
                    r#"{"before":1}"#,
                    r#"{"before":2}"#,
                    r#"{"before":3}"#,
                ])),
            ],
        )
        .unwrap();

        let test_dir = TempStrDir::default();
        let batches = RecordBatchIterator::new([Ok(batch)], schema);
        let dataset = Arc::new(
            Dataset::write(batches, &test_dir, Some(WriteParams::default()))
                .await
                .unwrap(),
        );

        let physical_schema: ArrowSchema = dataset.schema().into();
        assert!(is_json_field(
            physical_schema.field_with_name("meta").unwrap()
        ));

        let update_result = UpdateBuilder::new(dataset)
            .update_where("id = 2")
            .unwrap()
            .set("name", "'updated'")
            .unwrap()
            .set("meta", r#"jsonb '{"after":true,"n":2}'"#)
            .unwrap()
            .build()
            .unwrap()
            .execute()
            .await
            .unwrap();

        let updated_dataset = update_result.new_dataset;
        let actual_batches = updated_dataset
            .scan()
            .try_into_stream()
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        let actual_batch = concat_batches(&actual_batches[0].schema(), &actual_batches).unwrap();
        assert!(is_arrow_json_field(
            actual_batch.schema().field_with_name("meta").unwrap()
        ));

        let ids = actual_batch["id"].as_primitive::<Int64Type>();
        let names = actual_batch["name"].as_string::<i32>();
        let metas = actual_batch["meta"].as_string::<i32>();
        let updated_row_idx = ids.iter().position(|id| id == Some(2)).unwrap();

        assert_eq!(names.value(updated_row_idx), "updated");
        assert_eq!(metas.value(updated_row_idx), r#"{"after":true,"n":2}"#);
    }

    #[rstest]
    #[tokio::test]
    async fn test_update_concurrency(#[values(false, true)] enable_stable_row_ids: bool) {
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::UInt32, false),
            Field::new("value", DataType::UInt32, false),
        ]));
        let concurrency = 3;
        let initial_data = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(UInt32Array::from_iter_values(0..concurrency)),
                Arc::new(UInt32Array::from_iter_values(std::iter::repeat_n(
                    0,
                    concurrency as usize,
                ))),
            ],
        )
        .unwrap();

        // Increase likelihood of contention by throttling the store
        let throttled = Arc::new(ThrottledStoreWrapper {
            config: ThrottleConfig {
                wait_list_per_call: Duration::from_millis(1),
                wait_get_per_call: Duration::from_millis(1),
                ..Default::default()
            },
        });
        let session = Arc::new(Session::default());

        let mut dataset = InsertBuilder::new("memory://")
            .with_params(&WriteParams {
                store_params: Some(ObjectStoreParams {
                    object_store_wrapper: Some(throttled.clone()),
                    ..Default::default()
                }),
                session: Some(session.clone()),
                enable_stable_row_ids,
                ..Default::default()
            })
            .execute(vec![initial_data])
            .await
            .unwrap();

        let barrier = Arc::new(Barrier::new(concurrency as usize));
        let mut handles = Vec::new();
        for i in 0..concurrency {
            let session_ref = session.clone();
            let barrier_ref = barrier.clone();
            let throttled_ref = throttled.clone();
            let handle = tokio::task::spawn(async move {
                let dataset = DatasetBuilder::from_uri("memory://")
                    .with_read_params(ReadParams {
                        store_options: Some(ObjectStoreParams {
                            object_store_wrapper: Some(throttled_ref.clone()),
                            ..Default::default()
                        }),
                        session: Some(session_ref.clone()),
                        ..Default::default()
                    })
                    .load()
                    .await
                    .unwrap();

                let job = UpdateBuilder::new(Arc::new(dataset))
                    .update_where(&format!("id = {}", i))
                    .unwrap()
                    .set("value", "1")
                    .unwrap()
                    .build()
                    .unwrap();
                barrier_ref.wait().await;

                job.execute().await.unwrap();
            });
            handles.push(handle);
        }

        try_join_all(handles).await.unwrap();

        dataset.checkout_latest().await.unwrap();

        let data = dataset.scan().try_into_batch().await.unwrap();

        let mut ids = data["id"]
            .as_primitive::<UInt32Type>()
            .values()
            .iter()
            .cloned()
            .collect::<Vec<_>>();
        ids.sort();
        assert_eq!(ids, vec![0, 1, 2],);
        let values = data["value"].as_primitive::<UInt32Type>().values();
        assert!(values.iter().all(|&value| value == 1));
    }

    #[rstest]
    #[tokio::test]
    async fn test_update_same_row_concurrency(#[values(false, true)] enable_stable_row_ids: bool) {
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::UInt32, false),
            Field::new("value", DataType::UInt32, false),
        ]));
        let concurrency = 3;
        // Create dataset with just one row that all workers will update
        let initial_data = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(UInt32Array::from(vec![0])),
                Arc::new(UInt32Array::from(vec![10])),
            ],
        )
        .unwrap();

        // Increase likelihood of contention by throttling the store
        let throttled = Arc::new(ThrottledStoreWrapper {
            config: ThrottleConfig {
                wait_list_per_call: Duration::from_millis(10),
                wait_get_per_call: Duration::from_millis(10),
                ..Default::default()
            },
        });
        let session = Arc::new(Session::default());

        let mut dataset = InsertBuilder::new("memory://")
            .with_params(&WriteParams {
                store_params: Some(ObjectStoreParams {
                    object_store_wrapper: Some(throttled.clone()),
                    ..Default::default()
                }),
                session: Some(session.clone()),
                enable_stable_row_ids,
                ..Default::default()
            })
            .execute(vec![initial_data])
            .await
            .unwrap();

        let barrier = Arc::new(Barrier::new(concurrency as usize));
        let mut handles = Vec::new();
        for _i in 0..concurrency {
            let session_ref = session.clone();
            let barrier_ref = barrier.clone();
            let throttled_ref = throttled.clone();
            let handle = tokio::task::spawn(async move {
                let dataset = DatasetBuilder::from_uri("memory://")
                    .with_read_params(ReadParams {
                        store_options: Some(ObjectStoreParams {
                            object_store_wrapper: Some(throttled_ref.clone()),
                            ..Default::default()
                        }),
                        session: Some(session_ref.clone()),
                        ..Default::default()
                    })
                    .load()
                    .await
                    .unwrap();

                let job = UpdateBuilder::new(Arc::new(dataset))
                    .update_where("id = 0")
                    .unwrap()
                    .set("value", "99")
                    .unwrap()
                    .build()
                    .unwrap();
                barrier_ref.wait().await;

                job.execute().await.unwrap();
            });
            handles.push(handle);
        }

        try_join_all(handles).await.unwrap();

        dataset.checkout_latest().await.unwrap();

        let data = dataset.scan().try_into_batch().await.unwrap();

        // With retry-based conflict resolution, all concurrent updates should succeed
        // Even though they all target the same row, they should not fail with commit conflicts
        // The final result should be exactly one row (not duplicated) because the retries
        // should work from the latest dataset state, preventing duplicate row creation
        let ids = data["id"].as_primitive::<UInt32Type>().values();
        assert_eq!(ids, &[0]);

        let values = data["value"].as_primitive::<UInt32Type>().values();
        assert_eq!(values, &[99]);
    }

    #[tokio::test]
    async fn test_row_ids_stable_after_update() {
        let (dataset, _test_dir) = make_test_dataset(LanceFileVersion::V2_0, true).await;

        let orig_batch = dataset.scan().with_row_id().try_into_batch().await.unwrap();
        let orig_row_ids = orig_batch
            .column_by_name(ROW_ID)
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        let orig_ids = orig_batch
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();

        let updated_batch = UpdateBuilder::new(dataset)
            .update_where("id >= 15")
            .unwrap()
            .set("name", "'updated'")
            .unwrap()
            .build()
            .unwrap()
            .execute()
            .await
            .unwrap()
            .new_dataset
            .scan()
            .with_row_id()
            .try_into_batch()
            .await
            .unwrap();

        let updated_row_ids = updated_batch
            .column_by_name(ROW_ID)
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        let updated_ids = updated_batch
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();

        assert_eq!(orig_row_ids, updated_row_ids);
        assert_eq!(orig_ids, updated_ids);
    }

    #[tokio::test]
    async fn test_row_ids_stable_after_update_odd_id() {
        use std::collections::HashSet;

        let (dataset, _test_dir) = make_test_dataset(LanceFileVersion::V2_0, true).await;

        let orig_batch = dataset.scan().with_row_id().try_into_batch().await.unwrap();
        let orig_row_ids = orig_batch
            .column_by_name(ROW_ID)
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        let orig_ids = orig_batch
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let orig_names = orig_batch
            .column_by_name("name")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        let updated_batch = UpdateBuilder::new(dataset)
            .update_where("id % 2 = 1")
            .unwrap()
            .set("name", "'updated'")
            .unwrap()
            .build()
            .unwrap()
            .execute()
            .await
            .unwrap()
            .new_dataset
            .scan()
            .with_row_id()
            .try_into_batch()
            .await
            .unwrap();

        let updated_row_ids = updated_batch
            .column_by_name(ROW_ID)
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        let updated_ids = updated_batch
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let updated_names = updated_batch
            .column_by_name("name")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        assert_eq!(
            orig_row_ids
                .values()
                .iter()
                .cloned()
                .collect::<HashSet<_>>(),
            updated_row_ids
                .values()
                .iter()
                .cloned()
                .collect::<HashSet<_>>()
        );
        assert_eq!(
            orig_ids.values().iter().cloned().collect::<HashSet<_>>(),
            updated_ids.values().iter().cloned().collect::<HashSet<_>>()
        );

        for i in 0..orig_row_ids.len() {
            let row_id = orig_row_ids.value(i);
            let updated_idx = updated_row_ids
                .iter()
                .position(|rid| rid == Some(row_id))
                .unwrap();
            let id = orig_ids.value(i);
            let updated_name = updated_names.value(updated_idx);
            if id % 2 == 1 {
                assert_eq!(updated_name, "updated");
            } else {
                assert_eq!(updated_name, orig_names.value(i));
            }
        }
    }

    #[tokio::test]
    async fn test_update_affects_index_fragment_bitmap() {
        let mut dataset = lance_datagen::gen_batch()
            .col(
                "str",
                lance_datagen::array::cycle_utf8_literals(&["a", "b", "c", "d", "e", "f"]),
            )
            .col(
                "vec",
                lance_datagen::array::rand_vec::<Float32Type>(Dimension::from(4)),
            )
            .into_ram_dataset_with_params(
                FragmentCount::from(2),
                FragmentRowCount::from(3),
                Some(WriteParams {
                    max_rows_per_file: 3,
                    enable_stable_row_ids: true,
                    ..Default::default()
                }),
            )
            .await
            .unwrap();

        let scalar_params = ScalarIndexParams::default();
        dataset
            .create_index(
                &["str"],
                IndexType::Scalar,
                Some("str_idx".to_string()),
                &scalar_params,
                true,
            )
            .await
            .unwrap();

        let vector_params = VectorIndexParams::ivf_flat(1, MetricType::L2);
        dataset
            .create_index(
                &["vec"],
                IndexType::Vector,
                Some("vec_idx".to_string()),
                &vector_params,
                true,
            )
            .await
            .unwrap();

        let indices = dataset.load_indices().await.unwrap();
        let str_index = indices.iter().find(|idx| idx.name == "str_idx").unwrap();
        let vec_index = indices.iter().find(|idx| idx.name == "vec_idx").unwrap();

        assert_eq!(
            str_index
                .fragment_bitmap
                .as_ref()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            vec![0, 1]
        );
        assert_eq!(
            vec_index
                .fragment_bitmap
                .as_ref()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            vec![0, 1]
        );

        let updated_dataset = UpdateBuilder::new(Arc::new(dataset))
            .update_where("str = 'e'")
            .unwrap()
            .set("vec", "array[25.0, 26.0, 27.0, 28.0]")
            .unwrap()
            .build()
            .unwrap()
            .execute()
            .await
            .unwrap()
            .new_dataset;

        let updated_indices = updated_dataset.load_indices().await.unwrap();
        let updated_str_index = updated_indices
            .iter()
            .find(|idx| idx.name == "str_idx")
            .unwrap();
        let updated_vec_index = updated_indices
            .iter()
            .find(|idx| idx.name == "vec_idx")
            .unwrap();

        let str_bitmap = updated_str_index.fragment_bitmap.as_ref().unwrap();
        assert_eq!(str_bitmap.len(), 3);
        assert_eq!(str_bitmap.iter().collect::<Vec<_>>(), vec![0, 1, 2]);

        let vec_bitmap = updated_vec_index.fragment_bitmap.as_ref().unwrap();
        assert_eq!(vec_bitmap.len(), 2);
        assert_eq!(vec_bitmap.iter().collect::<Vec<_>>(), vec![0, 1]);

        let fragments = updated_dataset.get_fragments();
        assert!(fragments.len() > 2);

        let second_fragment = &fragments[1];
        assert!(
            second_fragment
                .get_deletion_vector()
                .await
                .unwrap()
                .is_some()
        );
    }

    #[rstest]
    #[case::zone_map(BuiltinIndexType::ZoneMap, "i < 100", 100)]
    #[case::bloom_filter(BuiltinIndexType::BloomFilter, "i = 0", 1)]
    #[tokio::test]
    async fn test_addr_domain_index_does_not_cover_rewritten_update_fragment(
        #[case] index_type: BuiltinIndexType,
        #[case] query: &str,
        #[case] expected_rows: usize,
    ) {
        let mut dataset = lance_datagen::gen_batch()
            .col("i", lance_datagen::array::step::<Int32Type>())
            .col("category", lance_datagen::array::step::<Int32Type>())
            .into_ram_dataset_with_params(
                FragmentCount::from(1),
                FragmentRowCount::from(100),
                Some(WriteParams {
                    max_rows_per_file: 100,
                    enable_stable_row_ids: true,
                    ..Default::default()
                }),
            )
            .await
            .unwrap();

        dataset
            .create_index(
                &["i"],
                IndexType::Scalar,
                Some("i_idx".to_string()),
                &ScalarIndexParams::for_builtin(index_type),
                true,
            )
            .await
            .unwrap();

        let before = dataset
            .scan()
            .filter(query)
            .unwrap()
            .try_into_batch()
            .await
            .unwrap();
        assert_eq!(before.num_rows(), expected_rows);

        let dataset = UpdateBuilder::new(Arc::new(dataset))
            .update_where("i < 20")
            .unwrap()
            .set("category", "-1")
            .unwrap()
            .build()
            .unwrap()
            .execute()
            .await
            .unwrap()
            .new_dataset;

        let indices = dataset.load_indices().await.unwrap();
        let index = indices.iter().find(|index| index.name == "i_idx").unwrap();
        assert_eq!(
            index
                .fragment_bitmap
                .as_ref()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            vec![0],
            "the address-domain index must not cover the rewritten fragment"
        );

        // Regression for https://github.com/lance-format/lance/issues/8278: a later
        // update must find rows moved out of the address-domain index's coverage.
        let second_update = UpdateBuilder::new(dataset)
            .update_where(query)
            .unwrap()
            .set("category", "-2")
            .unwrap()
            .build()
            .unwrap()
            .execute()
            .await
            .unwrap();
        assert_eq!(second_update.rows_updated, expected_rows as u64);
        let dataset = second_update.new_dataset;

        let after = dataset
            .scan()
            .filter(query)
            .unwrap()
            .try_into_batch()
            .await
            .unwrap();
        assert_eq!(after.num_rows(), expected_rows);

        let updated = dataset
            .scan()
            .filter("category = -2")
            .unwrap()
            .try_into_batch()
            .await
            .unwrap();
        assert_eq!(updated.num_rows(), expected_rows);
    }

    /// Regression test for https://github.com/lance-format/lance/issues/8076
    ///
    /// A bloom filter index reports matches as physical row addresses. An update that
    /// replaces every row of a fragment removes that fragment, but the index keeps the
    /// addresses it holds for it, so translating its results to row ids has to tolerate
    /// a fragment that is gone rather than fail with an internal error.
    #[tokio::test]
    async fn test_addr_domain_index_after_update_drops_fragment() {
        let mut dataset = lance_datagen::gen_batch()
            .col("i", lance_datagen::array::step::<Int32Type>())
            .into_ram_dataset_with_params(
                FragmentCount::from(2),
                FragmentRowCount::from(3),
                Some(WriteParams {
                    max_rows_per_file: 3,
                    enable_stable_row_ids: true,
                    ..Default::default()
                }),
            )
            .await
            .unwrap();

        dataset
            .create_index(
                &["i"],
                IndexType::BloomFilter,
                Some("i_idx".to_string()),
                &ScalarIndexParams::for_builtin(BuiltinIndexType::BloomFilter),
                true,
            )
            .await
            .unwrap();

        // Rewrites all of fragment 1 (rows 3, 4, 5), which drops the fragment.
        let dataset = UpdateBuilder::new(Arc::new(dataset))
            .update_where("i >= 3")
            .unwrap()
            .set("i", "-1")
            .unwrap()
            .build()
            .unwrap()
            .execute()
            .await
            .unwrap()
            .new_dataset;
        assert!(dataset.get_fragments().iter().all(|frag| frag.id() != 1));

        // The index still holds a block for the dropped fragment, and a bloom filter
        // cannot rule out a value it once held, so this query is the one that reaches
        // the index with addresses in that fragment.
        let matched = dataset
            .scan()
            .filter("i = 4")
            .unwrap()
            .try_into_batch()
            .await
            .unwrap();
        assert_eq!(matched.num_rows(), 0);

        let updated = dataset
            .scan()
            .filter("i = -1")
            .unwrap()
            .try_into_batch()
            .await
            .unwrap();
        assert_eq!(updated.num_rows(), 3);
    }

    #[tokio::test]
    async fn test_update_mixed_indexed_unindexed_fragments() {
        let mut dataset = lance_datagen::gen_batch()
            .col(
                "str",
                lance_datagen::array::cycle_utf8_literals(&["a", "b", "c", "d", "e", "f"]),
            )
            .col(
                "vec",
                lance_datagen::array::rand_vec::<Float32Type>(Dimension::from(4)),
            )
            .into_ram_dataset_with_params(
                FragmentCount::from(2),
                FragmentRowCount::from(3),
                Some(WriteParams {
                    max_rows_per_file: 3,
                    enable_stable_row_ids: true,
                    ..Default::default()
                }),
            )
            .await
            .unwrap();

        dataset
            .create_index(
                &["str"],
                IndexType::Scalar,
                Some("str_idx".to_string()),
                &ScalarIndexParams::default(),
                true,
            )
            .await
            .unwrap();

        dataset
            .create_index(
                &["vec"],
                IndexType::Vector,
                Some("vec_idx".to_string()),
                &VectorIndexParams::ivf_flat(1, MetricType::L2),
                true,
            )
            .await
            .unwrap();

        let initial_indices = dataset.load_indices().await.unwrap();
        let str_index = initial_indices
            .iter()
            .find(|idx| idx.name == "str_idx")
            .unwrap();
        let vec_index = initial_indices
            .iter()
            .find(|idx| idx.name == "vec_idx")
            .unwrap();

        assert_eq!(
            str_index
                .fragment_bitmap
                .as_ref()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            vec![0, 1]
        );
        assert_eq!(
            vec_index
                .fragment_bitmap
                .as_ref()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            vec![0, 1]
        );

        // insert data to create the third frag
        let new_batch = lance_datagen::gen_batch()
            .col(
                "str",
                lance_datagen::array::cycle_utf8_literals(&["g", "h", "i"]),
            )
            .col(
                "vec",
                lance_datagen::array::rand_vec::<Float32Type>(Dimension::from(4)),
            )
            .into_batch_rows(RowCount::from(3))
            .unwrap();

        dataset = InsertBuilder::new(WriteDestination::Dataset(Arc::new(dataset)))
            .with_params(&WriteParams {
                mode: WriteMode::Append,
                enable_stable_row_ids: true,
                ..Default::default()
            })
            .execute(vec![new_batch])
            .await
            .unwrap();

        assert_eq!(dataset.get_fragments().len(), 3);

        let indices_after_insert = dataset.load_indices().await.unwrap();
        let str_index_after_insert = indices_after_insert
            .iter()
            .find(|idx| idx.name == "str_idx")
            .unwrap();
        let vec_index_after_insert = indices_after_insert
            .iter()
            .find(|idx| idx.name == "vec_idx")
            .unwrap();

        assert_eq!(
            str_index_after_insert
                .fragment_bitmap
                .as_ref()
                .unwrap()
                .len(),
            2
        );
        assert!(
            !str_index_after_insert
                .fragment_bitmap
                .as_ref()
                .unwrap()
                .contains(2)
        );
        assert_eq!(
            vec_index_after_insert
                .fragment_bitmap
                .as_ref()
                .unwrap()
                .len(),
            2
        );
        assert!(
            !vec_index_after_insert
                .fragment_bitmap
                .as_ref()
                .unwrap()
                .contains(2)
        );

        let updated_dataset = UpdateBuilder::new(Arc::new(dataset))
            // 'a' in fragment 0，'g' in fragment 2, and frag 2 not in frag bitmap
            .update_where("str = 'a' OR str = 'g'")
            .unwrap()
            .set("vec", "array[99.0, 99.0, 99.0, 99.0]")
            .unwrap()
            .build()
            .unwrap()
            .execute()
            .await
            .unwrap()
            .new_dataset;

        // reload indices
        let updated_indices = updated_dataset.load_indices().await.unwrap();
        let updated_str_index = updated_indices
            .iter()
            .find(|idx| idx.name == "str_idx")
            .unwrap();
        let updated_vec_index = updated_indices
            .iter()
            .find(|idx| idx.name == "vec_idx")
            .unwrap();

        let str_bitmap = updated_str_index.fragment_bitmap.as_ref().unwrap();
        let vec_bitmap = updated_vec_index.fragment_bitmap.as_ref().unwrap();

        assert!(updated_dataset.get_fragments().len() > 3);
        assert_eq!(str_bitmap.len(), 2);
        assert_eq!(vec_bitmap.len(), 2);

        // frag 3 not in the index's frag bitmap
        for &fragment_id in str_bitmap.iter().collect::<Vec<_>>().iter() {
            assert!(
                fragment_id < 2,
                "str index bitmap should not contain fragments with unindexed data, found fragment {}",
                fragment_id
            );
        }

        // frag 3 not in the index's frag bitmap
        for &fragment_id in vec_bitmap.iter().collect::<Vec<_>>().iter() {
            assert!(
                fragment_id < 2,
                "vec index bitmap should not contain fragments with unindexed data, found fragment {}",
                fragment_id
            );
        }
    }

    #[tokio::test]
    async fn test_update_by_rowid() {
        let (dataset, _test_dir) = make_test_dataset(LanceFileVersion::Stable, true).await;

        let orig_batch = dataset.scan().with_row_id().try_into_batch().await.unwrap();
        let orig_row_ids = orig_batch
            .column_by_name(ROW_ID)
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        let orig_ids = orig_batch
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();

        let target_idx = 5;
        let target_row_id = orig_row_ids.value(target_idx);
        let target_id = orig_ids.value(target_idx);

        let update_result = UpdateBuilder::new(dataset)
            .update_where(&format!("_rowid = {}", target_row_id))
            .unwrap()
            .set("name", "'updated'")
            .unwrap()
            .build()
            .unwrap()
            .execute()
            .await
            .unwrap();

        assert_eq!(update_result.rows_updated, 1);

        let updated_batch = update_result
            .new_dataset
            .scan()
            .with_row_id()
            .try_into_batch()
            .await
            .unwrap();
        let updated_ids = updated_batch
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let updated_names = updated_batch
            .column_by_name("name")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        for i in 0..updated_ids.len() {
            if updated_ids.value(i) == target_id {
                assert_eq!(updated_names.value(i), "updated");
            } else {
                assert_eq!(updated_names.value(i), "foo");
            }
        }
    }

    #[tokio::test]
    async fn test_update_by_rowid_in_list() {
        let (dataset, _test_dir) = make_test_dataset(LanceFileVersion::Stable, true).await;

        let orig_batch = dataset.scan().with_row_id().try_into_batch().await.unwrap();
        let orig_row_ids = orig_batch
            .column_by_name(ROW_ID)
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        let orig_ids = orig_batch
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();

        let target_indices = [3, 7, 15];
        let target_row_ids: Vec<u64> = target_indices
            .iter()
            .map(|&i| orig_row_ids.value(i))
            .collect();
        let target_ids: std::collections::HashSet<i64> =
            target_indices.iter().map(|&i| orig_ids.value(i)).collect();
        let in_list: String = target_row_ids
            .iter()
            .map(|id| id.to_string())
            .collect::<Vec<_>>()
            .join(", ");

        let update_result = UpdateBuilder::new(dataset)
            .update_where(&format!("_rowid IN ({})", in_list))
            .unwrap()
            .set("name", "'updated'")
            .unwrap()
            .build()
            .unwrap()
            .execute()
            .await
            .unwrap();

        assert_eq!(update_result.rows_updated, 3);

        let updated_batch = update_result
            .new_dataset
            .scan()
            .with_row_id()
            .try_into_batch()
            .await
            .unwrap();
        let updated_ids = updated_batch
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let updated_names = updated_batch
            .column_by_name("name")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        for i in 0..updated_ids.len() {
            if target_ids.contains(&updated_ids.value(i)) {
                assert_eq!(updated_names.value(i), "updated");
            } else {
                assert_eq!(updated_names.value(i), "foo");
            }
        }
    }

    fn count_data_files(base_dir: &str) -> usize {
        let data_dir = std::path::Path::new(base_dir).join("data");
        if !data_dir.exists() {
            return 0;
        }
        std::fs::read_dir(data_dir)
            .unwrap()
            .filter(|e| e.as_ref().unwrap().path().is_file())
            .count()
    }

    /// Site 4 in PR #6320: when `UpdateJob::apply_deletions` fails after the new
    /// rewrite fragments have been written, those new data files must be cleaned up.
    #[tokio::test]
    async fn test_update_cleans_up_data_on_apply_deletions_failure() {
        use crate::utils::test::FailingProxyStore;
        use lance_io::object_store::ObjectStoreParams;

        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from_iter_values(0..30)),
                Arc::new(StringArray::from_iter_values(std::iter::repeat_n(
                    "foo", 30,
                ))),
            ],
        )
        .unwrap();

        let test_dir = TempStrDir::default();
        let test_uri = test_dir.as_str();
        // Prefix `/` so Windows drive letters (e.g. `C:`) don't get parsed as
        // the URL authority.
        let path_prefix = if test_uri.starts_with('/') { "" } else { "/" };
        let routed_uri = format!("file-object-store://{path_prefix}{test_uri}");

        let write_params = WriteParams {
            max_rows_per_file: 10,
            data_storage_version: Some(LanceFileVersion::V2_1),
            ..Default::default()
        };
        let batches = RecordBatchIterator::new([Ok(batch)], schema.clone());
        Dataset::write(batches, &routed_uri, Some(write_params))
            .await
            .unwrap();

        let baseline_files = count_data_files(test_uri);
        assert!(baseline_files > 0);

        // Fail writes to `_deletions/`: this is where `apply_deletions` writes
        // the new deletion file. The rewrite fragments (in `data/`) are written
        // earlier and should be successfully created, then cleaned up on failure.
        let failing = Arc::new(FailingProxyStore::new());
        failing.fail_when("put", "_deletions", "injected deletions failure");
        failing.fail_when("put_multipart", "_deletions", "injected deletions failure");

        let dataset = DatasetBuilder::from_uri(&routed_uri)
            .with_read_params(ReadParams {
                store_options: Some(ObjectStoreParams {
                    object_store_wrapper: Some(failing.clone()),
                    ..Default::default()
                }),
                ..Default::default()
            })
            .load()
            .await
            .unwrap();

        let result = UpdateBuilder::new(Arc::new(dataset))
            .update_where("id < 5")
            .unwrap()
            .set("name", "'bar'")
            .unwrap()
            .build()
            .unwrap()
            .execute()
            .await;

        assert!(
            result.is_err(),
            "Update should fail when deletion-file write fails"
        );

        assert_eq!(
            count_data_files(test_uri),
            baseline_files,
            "Rewritten data files should be cleaned up on apply_deletions failure"
        );
    }

    #[tokio::test]
    async fn test_update_with_blob() {
        use arrow_array::LargeBinaryArray;
        use arrow_schema::Field;
        use lance_arrow::BLOB_META_KEY;

        let test_dir = TempStrDir::default();
        let blob_meta = HashMap::from([(BLOB_META_KEY.to_string(), "true".to_string())]);
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("blobs", DataType::LargeBinary, true).with_metadata(blob_meta),
            Field::new("id", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(LargeBinaryArray::from(vec![
                    Some(b"foo".as_slice()),
                    Some(b"bar".as_slice()),
                    Some(b"baz".as_slice()),
                ])),
                Arc::new(Int64Array::from(vec![0, 1, 2])),
            ],
        )
        .unwrap();

        let reader = RecordBatchIterator::new(vec![Ok(batch)], schema.clone());
        let dataset = Dataset::write(
            reader,
            &test_dir,
            Some(WriteParams {
                data_storage_version: Some(LanceFileVersion::V2_1),
                ..Default::default()
            }),
        )
        .await
        .unwrap();

        // Perform an update: update the "blobs" column where id = 1
        let dataset = Arc::new(dataset);
        let updated_dataset = UpdateBuilder::new(dataset)
            .update_where("id = 1")
            .unwrap()
            .set("blobs", "arrow_cast('updated_bar', 'LargeBinary')")
            .unwrap()
            .build()
            .unwrap()
            .execute()
            .await
            .unwrap()
            .new_dataset;

        // Verify the updated value
        let mut scanner = updated_dataset.scan();
        // Read as binary to assert actual value
        scanner.blob_handling(BlobHandling::AllBinary);
        let batches = scanner.try_into_batch().await.unwrap();
        let blobs = batches.column_by_name("blobs").unwrap().as_binary::<i64>();
        let ids = batches
            .column_by_name("id")
            .unwrap()
            .as_primitive::<Int64Type>();

        // Find the index of id = 1
        let idx = ids.values().iter().position(|&x| x == 1).unwrap();
        assert_eq!(blobs.value(idx), b"updated_bar");

        let idx_foo = ids.values().iter().position(|&x| x == 0).unwrap();
        assert_eq!(blobs.value(idx_foo), b"foo");
    }
}
