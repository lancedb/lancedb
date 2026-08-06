// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use super::fragment::FileFragment;
use super::{
    Dataset,
    transaction::{Operation, Transaction},
    write::cleanup_data_fragments,
};
use crate::index::DatasetIndexExt;
use crate::{Error, Result, io::exec::Planner};
use arrow::compute::CastOptions;
use arrow::compute::can_cast_types;
use arrow_array::{Array, RecordBatch, RecordBatchReader};
use arrow_cast::cast_with_options;
use arrow_schema::{DataType, Field as ArrowField, Schema as ArrowSchema};
use datafusion::execution::SendableRecordBatchStream;
use futures::stream::{StreamExt, TryStreamExt};
use lance_arrow::SchemaExt;
use lance_core::datatypes::{Field, Schema};
use lance_datafusion::utils::StreamingWriteSource;
use lance_encoding::constants::{PACKED_STRUCT_LEGACY_META_KEY, PACKED_STRUCT_META_KEY};
use lance_file::version::LanceFileVersion;
use lance_table::format::Fragment;

mod optimize;

use optimize::{
    ChainedNewColumnTransformOptimizer, NewColumnTransformOptimizer, SqlToAllNullsOptimizer,
};

async fn validate_no_nulls_before_making_non_nullable(dataset: &Dataset, path: &str) -> Result<()> {
    let field = dataset.schema().field(path).ok_or_else(|| {
        Error::invalid_input(format!("Column \"{}\" does not exist in the dataset", path))
    })?;

    if !field.nullable {
        return Ok(());
    }

    let mut scanner = dataset.scan();
    scanner.project(&[path])?;
    let mut stream = scanner.try_into_stream().await?;
    while let Some(batch) = stream.try_next().await? {
        // `path` can be a nested path (e.g. "b.c") which will not be found by
        // `RecordBatch::column_by_name`. We project exactly one column and validate it directly.
        if batch.num_columns() != 1 {
            return Err(Error::internal(format!(
                "Expected exactly one column in validation scan for {}, got {}",
                path,
                batch.num_columns()
            )));
        }
        let col = batch.column(0);
        if col.null_count() > 0 {
            return Err(Error::invalid_input(format!(
                "Column \"{}\" contains NULL values and cannot be made non-nullable",
                path
            )));
        }
    }

    Ok(())
}

#[derive(Debug, Clone, PartialEq)]
pub struct BatchInfo {
    pub fragment_id: u32,
    pub batch_index: usize,
}

/// A mechanism for saving UDF results.
///
/// This is used to determine if a UDF has already been run on a given input,
/// and to store the results of a UDF for future use.
pub trait UDFCheckpointStore: Send + Sync {
    fn get_batch(&self, info: &BatchInfo) -> Result<Option<RecordBatch>>;
    fn insert_batch(&self, info: BatchInfo, batch: RecordBatch) -> Result<()>;
    fn get_fragment(&self, fragment_id: u32) -> Result<Option<Fragment>>;
    fn insert_fragment(&self, fragment: Fragment) -> Result<()>;
}

pub struct BatchUDF {
    #[allow(clippy::type_complexity)]
    pub mapper: Box<dyn Fn(&RecordBatch) -> Result<RecordBatch> + Send + Sync>,
    /// The schema of the returned RecordBatch
    pub output_schema: Arc<ArrowSchema>,
    /// A checkpoint store for the UDF results
    pub result_checkpoint: Option<Arc<dyn UDFCheckpointStore>>,
}

/// A way to define one or more new columns in a dataset
pub enum NewColumnTransform {
    /// A UDF that takes a RecordBatch of existing data and returns a
    /// RecordBatch with the new columns for those corresponding rows. The returned
    /// batch must return the same number of rows as the input batch.
    BatchUDF(BatchUDF),
    /// A set of SQL expressions that define new columns.
    SqlExpressions(Vec<(String, String)>),
    /// A stream of RecordBatches that define new columns.
    Stream(SendableRecordBatchStream),
    /// An iterator of RecordBatches that define new columns.
    Reader(Box<dyn RecordBatchReader + Send>),
    /// Add new columns that are initially all null
    AllNulls(Arc<ArrowSchema>),
}

/// Definition of a change to a column in a dataset
pub struct ColumnAlteration {
    /// Path to the existing column to be altered.
    pub path: String,
    /// The new name of the column. If None, the column name will not be changed.
    pub rename: Option<String>,
    /// Whether the column is nullable. If None, the nullability will not be changed.
    pub nullable: Option<bool>,
    /// The new data type of the column. If None, the data type will not be changed.
    pub data_type: Option<DataType>,
}

impl ColumnAlteration {
    pub fn new(path: String) -> Self {
        Self {
            path,
            rename: None,
            nullable: None,
            data_type: None,
        }
    }

    pub fn rename(mut self, name: String) -> Self {
        self.rename = Some(name);
        self
    }

    pub fn set_nullable(mut self, nullable: bool) -> Self {
        self.nullable = Some(nullable);
        self
    }

    pub fn cast_to(mut self, data_type: DataType) -> Self {
        self.data_type = Some(data_type);
        self
    }
}

/// Limit casts to same type. This is mostly to filter out weird casts like
/// casting a string to a boolean or float to string.
fn is_upcast_downcast(from_type: &DataType, to_type: &DataType, version: LanceFileVersion) -> bool {
    use DataType::*;
    match (from_type, to_type) {
        // Legacy storage cannot materialize a fresh Dictionary column via
        // alter because the writer expects `field.dictionary` metadata to be
        // pre-populated, which the alter pipeline does not compute.
        (_, Dictionary(_, _)) if matches!(version, LanceFileVersion::Legacy) => false,
        // These need to be in front
        (Dictionary(_, from_value_type), _) => {
            is_upcast_downcast(from_value_type, to_type, version)
        }
        (_, Dictionary(_, to_value_type)) => is_upcast_downcast(from_type, to_value_type, version),
        (from, to) if from.is_integer() => to.is_integer(),
        (from, to) if from.is_floating() => to.is_floating(),
        (from, to) if from.is_temporal() => to.is_temporal(),
        (Boolean, to) => matches!(to, Boolean),
        (Utf8 | LargeUtf8, to) => matches!(to, Utf8 | LargeUtf8),
        (Binary | LargeBinary, to) => matches!(to, Binary | LargeBinary),
        (Decimal128(_, _) | Decimal256(_, _), to) => {
            matches!(to, Decimal128(_, _) | Decimal256(_, _))
        }
        (List(from_field) | LargeList(from_field) | FixedSizeList(from_field, _), to) => match to {
            List(to_field) | LargeList(to_field) | FixedSizeList(to_field, _) => {
                is_upcast_downcast(from_field.data_type(), to_field.data_type(), version)
            }
            _ => false,
        },

        _ => false,
    }
}

trait ArrowFieldExt {
    fn is_packed(&self) -> bool;
}

impl ArrowFieldExt for ArrowField {
    fn is_packed(&self) -> bool {
        let metadata = self.metadata();
        metadata
            .get(PACKED_STRUCT_LEGACY_META_KEY)
            .map(|v| v == "true")
            .unwrap_or(metadata.contains_key(PACKED_STRUCT_META_KEY))
    }
}

fn check_field_conflict(
    left: &ArrowField,
    right: &ArrowField,
    version: &LanceFileVersion,
) -> Result<()> {
    if left.name() != right.name() {
        return Ok(());
    }

    match (left.data_type(), right.data_type()) {
        (DataType::Struct(fl), DataType::Struct(fr)) => {
            if !version.support_add_sub_column() {
                return Err(Error::invalid_input(format!(
                    "Column {} is a struct col, add sub column is not supported in Lance file version {}",
                    left.name(),
                    version
                )));
            }

            if left.is_packed() || right.is_packed() {
                return Err(Error::invalid_input(format!(
                    "Column {} is packed struct and already exists in the dataset",
                    left.name()
                )));
            }

            for l_field in fl.iter() {
                if let Some((_, r_field)) = fr.find(l_field.name()) {
                    check_field_conflict(l_field, r_field, version)?;
                }
            }
            Ok(())
        }
        (DataType::List(fl), DataType::List(fr)) => check_field_conflict(fl, fr, version),
        (DataType::LargeList(fl), DataType::LargeList(fr)) => check_field_conflict(fl, fr, version),
        (DataType::FixedSizeList(fl, _), DataType::FixedSizeList(fr, _)) => {
            check_field_conflict(fl, fr, version)
        }
        (l_type, r_type) if l_type == r_type => Err(Error::invalid_input(format!(
            "Column {} already exists in the dataset",
            left.name()
        ))),
        (_, _) => Err(Error::invalid_input(format!(
            "Type conflicts between {}({}) and {}({})",
            left.name(),
            left.data_type(),
            right.name(),
            right.data_type()
        ))),
    }
}

pub(super) async fn add_columns_to_fragments(
    dataset: &Dataset,
    transforms: NewColumnTransform,
    read_columns: Option<Vec<String>>,
    fragments: &[FileFragment],
    batch_size: Option<u32>,
) -> Result<(Vec<Fragment>, Schema, Vec<Fragment>)> {
    // Check names early (before calling add_columns_impl) to avoid extra work if
    // the names are wrong.
    let version = dataset.manifest.data_storage_format.lance_file_version()?;
    let check_names = |output_schema: &ArrowSchema| {
        for field in &dataset.schema().fields {
            if let Ok(out_field) = output_schema.field_with_name(&field.name) {
                let ds_field = ArrowField::from(field);
                check_field_conflict(&ds_field, out_field, &version)?;
            }
        }
        Ok::<(), Error>(())
    };

    // Optimize the transforms
    let mut optimizer = ChainedNewColumnTransformOptimizer::new(vec![]);
    // ALlNull transform can not performed on legacy files
    if !dataset.is_legacy_storage() {
        optimizer.add_optimizer(Box::new(SqlToAllNullsOptimizer::new()));
    }
    let transforms = optimizer.optimize(dataset, transforms)?;

    let (output_schema, new_fragments, fragments_to_cleanup) = match transforms {
        NewColumnTransform::BatchUDF(udf) => {
            check_names(udf.output_schema.as_ref())?;
            let result = add_columns_impl(
                fragments,
                read_columns,
                udf.mapper,
                batch_size,
                udf.result_checkpoint,
                None,
            )
            .await?;
            Result::Ok((
                udf.output_schema,
                result.fragments,
                result.fragments_to_cleanup,
            ))
        }
        NewColumnTransform::SqlExpressions(expressions) => {
            // We just transform the SQL expression into a UDF backed by DataFusion
            // physical expressions.
            let arrow_schema = Arc::new(ArrowSchema::from(dataset.schema()));
            let planner = Planner::new(arrow_schema);
            let exprs = expressions
                .into_iter()
                .map(|(name, expr)| {
                    let expr = planner.parse_expr(&expr)?;
                    let expr = planner.optimize_expr(expr)?;
                    Ok((name, expr))
                })
                .collect::<Result<Vec<_>>>()?;

            let needed_columns = exprs
                .iter()
                .flat_map(|(_, expr)| Planner::column_names_in_expr(expr))
                .collect::<HashSet<_>>()
                .into_iter()
                .collect::<Vec<_>>();
            let read_schema = dataset.schema().project(&needed_columns)?;
            let read_schema = Arc::new(ArrowSchema::from(&read_schema));
            // Need to re-create the planner with the read schema because physical
            // expressions use positional column references.
            let planner = Planner::new(read_schema.clone());
            let exprs = exprs
                .into_iter()
                .map(|(name, expr)| {
                    let expr = planner.create_physical_expr(&expr)?;
                    Ok((name, expr))
                })
                .collect::<Result<Vec<_>>>()?;

            let output_schema = Arc::new(ArrowSchema::new(
                exprs
                    .iter()
                    .map(|(name, expr)| {
                        Ok(ArrowField::new(
                            name,
                            expr.data_type(read_schema.as_ref())?,
                            expr.nullable(read_schema.as_ref())?,
                        ))
                    })
                    .collect::<Result<Vec<_>>>()?,
            ));
            check_names(output_schema.as_ref())?;

            let schema_ref = output_schema.clone();
            let mapper = move |batch: &RecordBatch| {
                let num_rows = batch.num_rows();
                let columns = exprs
                    .iter()
                    .map(|(_, expr)| Ok(expr.evaluate(batch)?.into_array(num_rows)?))
                    .collect::<Result<Vec<_>>>()?;

                let batch = RecordBatch::try_new(schema_ref.clone(), columns)?;
                Ok(batch)
            };
            let mapper = Box::new(mapper);

            let read_columns = Some(read_schema.field_names().into_iter().cloned().collect());
            let result =
                add_columns_impl(fragments, read_columns, mapper, batch_size, None, None).await?;
            Ok((output_schema, result.fragments, result.fragments_to_cleanup))
        }
        NewColumnTransform::Stream(stream) => {
            let output_schema = stream.schema();
            check_names(output_schema.as_ref())?;
            let fragments = add_columns_from_stream(fragments, stream, None, batch_size).await?;
            Ok((output_schema, fragments.clone(), fragments))
        }
        NewColumnTransform::Reader(reader) => {
            let output_schema = reader.schema();
            check_names(output_schema.as_ref())?;
            let stream = reader.into_stream();
            let fragments = add_columns_from_stream(fragments, stream, None, batch_size).await?;
            Ok((output_schema, fragments.clone(), fragments))
        }
        NewColumnTransform::AllNulls(output_schema) => {
            check_names(output_schema.as_ref())?;

            // AllNulls is metadata-only; missing columns are synthesized as nulls at
            // read time, so only each new top-level column needs to be nullable.
            if let Some(field) = output_schema.fields().iter().find(|f| !f.is_nullable()) {
                return Err(Error::invalid_input_source(
                    format!(
                        "All-null columns must be nullable, but field '{}' is not.",
                        field.name()
                    )
                    .into(),
                ));
            }

            let fragments = fragments
                .iter()
                .map(|f| f.metadata.clone())
                .collect::<Vec<_>>();

            // Check if any of the fragment's files are using the legacy dataset version if so, we
            // can't add all-null columns as a metadata-only operation. The reason is because we
            // use the NullReader for fragments that have missing columns and we can't mix legacy
            // and non-legacy readers when reading the fragment.
            if dataset.is_legacy_storage() {
                return Err(Error::not_supported_source(
                    "Cannot add all-null columns to legacy dataset version.".into(),
                ));
            }

            Ok((output_schema, fragments, Vec::new()))
        }
    }?;

    let mut schema = match dataset.schema().merge(output_schema.as_ref()) {
        Ok(schema) => schema,
        Err(e) => {
            cleanup_new_column_data_files(fragments, &fragments_to_cleanup).await;
            return Err(e);
        }
    };
    schema.set_field_id(Some(dataset.manifest.max_field_id()));

    Ok((new_fragments, schema, fragments_to_cleanup))
}

pub(super) async fn add_columns(
    dataset: &mut Dataset,
    transforms: NewColumnTransform,
    read_columns: Option<Vec<String>>,
    batch_size: Option<u32>,
) -> Result<()> {
    let (fragments, schema, _fragments_to_cleanup) = add_columns_to_fragments(
        dataset,
        transforms,
        read_columns,
        &dataset.get_fragments(),
        batch_size,
    )
    .await?;

    let operation = Operation::Merge { fragments, schema };
    let transaction = Transaction::new(dataset.manifest.version, operation, None);
    // Once the manifest commit has been attempted, an error does not prove
    // that the new files are unreferenced: the commit may have landed and only
    // its response (or a post-commit callback) may have failed. Leave files
    // from failed attempts for dataset GC instead of risking live-data loss.
    dataset
        .apply_commit(transaction, &Default::default(), &Default::default())
        .await
}

async fn cleanup_new_column_data_files(fragments: &[FileFragment], new_fragments: &[Fragment]) {
    let Some(first_fragment) = fragments.first() else {
        return;
    };

    // add_columns rewrites fragment metadata in place, so cleanup must delete
    // only files created by the current attempt and must not touch pre-existing
    // files that still belong to the fragment.
    let original_files_by_fragment = fragments
        .iter()
        .map(|fragment| {
            let files = fragment
                .metadata
                .files
                .iter()
                .map(|file| (file.base_id, file.path.clone()))
                .collect::<HashSet<_>>();
            (fragment.id() as u64, files)
        })
        .collect::<HashMap<_, _>>();

    let fragments_to_cleanup = new_fragments
        .iter()
        .filter_map(|fragment| {
            let original_files = original_files_by_fragment.get(&fragment.id)?;
            let files = fragment
                .files
                .iter()
                .filter(|file| !original_files.contains(&(file.base_id, file.path.clone())))
                .cloned()
                .collect::<Vec<_>>();

            if files.is_empty() {
                None
            } else {
                let mut fragment = fragment.clone();
                fragment.files = files;
                Some(fragment)
            }
        })
        .collect::<Vec<_>>();

    cleanup_data_fragments(
        &first_fragment.dataset().object_store,
        &first_fragment.dataset().base,
        None,
        &fragments_to_cleanup,
    )
    .await;
}

struct AddColumnFragments {
    /// Fragments produced by the add-columns operation and returned to the
    /// caller for the final merge commit.
    fragments: Vec<Fragment>,
    /// Uncommitted fragments whose newly written data files must be removed if
    /// the operation fails before the merge commit completes.
    fragments_to_cleanup: Vec<Fragment>,
}

#[allow(clippy::type_complexity)]
async fn add_columns_impl(
    fragments: &[FileFragment],
    read_columns: Option<Vec<String>>,
    mapper: Box<dyn Fn(&RecordBatch) -> Result<RecordBatch> + Send + Sync>,
    batch_size: Option<u32>,
    result_cache: Option<Arc<dyn UDFCheckpointStore>>,
    schemas: Option<(Schema, Schema)>,
) -> Result<AddColumnFragments> {
    let read_columns_ref = read_columns.as_deref();
    let mapper_ref = mapper.as_ref();

    let mut new_fragments = Vec::with_capacity(fragments.len());
    let mut fragments_to_cleanup = Vec::with_capacity(fragments.len());

    for fragment in fragments {
        if let Some(cache) = &result_cache {
            let fragment_id = fragment.id() as u32;
            let fragment = match cache.get_fragment(fragment_id) {
                Ok(fragment) => fragment,
                Err(e) => {
                    cleanup_new_column_data_files(fragments, &fragments_to_cleanup).await;
                    return Err(e);
                }
            };
            if let Some(fragment) = fragment {
                new_fragments.push(fragment);
                continue;
            }
        }

        let mut updater = match fragment
            .updater(read_columns_ref, schemas.clone(), batch_size)
            .await
        {
            Ok(updater) => updater,
            Err(e) => {
                cleanup_new_column_data_files(fragments, &fragments_to_cleanup).await;
                return Err(e);
            }
        };
        let fragment_result = async {
            let mut batch_index = 0;
            // TODO: the structure of the updater prevents batch-level parallelism here,
            //       but there is no reason why we couldn't do this in parallel.
            while let Some(batch) = updater.next().await? {
                let batch_info = BatchInfo {
                    fragment_id: fragment.id() as u32,
                    batch_index,
                };

                let new_batch = if let Some(cache) = &result_cache {
                    if let Some(batch) = cache.get_batch(&batch_info)? {
                        batch
                    } else {
                        let new_batch = mapper_ref(batch)?;
                        cache.insert_batch(batch_info, new_batch.clone())?;
                        new_batch
                    }
                } else {
                    mapper_ref(batch)?
                };

                updater.update(new_batch).await?;
                batch_index += 1;
            }

            let new_fragment = updater.finish().await?;
            fragments_to_cleanup.push(new_fragment.clone());

            if let Some(cache) = &result_cache {
                // Once the checkpoint store owns this fragment, retries may load
                // it back instead of rewriting it. Removing it from the cleanup
                // set avoids deleting data that has already been checkpointed.
                cache.insert_fragment(new_fragment.clone())?;
                fragments_to_cleanup.pop();
            }

            Ok::<_, Error>(new_fragment)
        }
        .await;

        match fragment_result {
            Ok(new_fragment) => {
                new_fragments.push(new_fragment);
            }
            Err(e) => {
                updater.cleanup_unfinished_writer().await;
                cleanup_new_column_data_files(fragments, &fragments_to_cleanup).await;
                return Err(e);
            }
        }
    }

    Ok(AddColumnFragments {
        fragments: new_fragments,
        fragments_to_cleanup,
    })
}

async fn add_columns_from_stream(
    fragments: &[FileFragment],
    mut stream: SendableRecordBatchStream,
    schemas: Option<(Schema, Schema)>,
    batch_size: Option<u32>,
) -> Result<Vec<Fragment>> {
    let mut new_fragments = Vec::with_capacity(fragments.len());
    let mut last_seen_batch: Option<RecordBatch> = None;
    for fragment in fragments {
        let mut updater = match fragment
            .updater::<String>(Some(&[]), schemas.clone(), batch_size)
            .await
        {
            Ok(updater) => updater,
            Err(e) => {
                cleanup_new_column_data_files(fragments, &new_fragments).await;
                return Err(e);
            }
        };
        let result: Result<Fragment> = async {
            while let Some(batch) = updater.next().await? {
                debug_assert_eq!(batch.num_columns(), 1);
                let mut rows_remaining = batch.num_rows();

                // The updater yields an empty batch when every row in a read batch
                // has been deleted (e.g. a whole batch falls within the deletion
                // vector). There is nothing to pull from the stream in that case, so
                // feed an empty batch back to keep the updater in sync and continue.
                if rows_remaining == 0 {
                    updater
                        .update(RecordBatch::new_empty(stream.schema()))
                        .await?;
                    continue;
                }

                let mut batches = Vec::new();

                while rows_remaining > 0 {
                    let next_batch = if let Some(last_seen) = last_seen_batch.take() {
                        last_seen
                    } else {
                        stream.next().await.ok_or_else(|| {
                            Error::invalid_input(
                                "Stream ended before producing values for all rows in dataset",
                            )
                        })??
                    };
                    let num_rows = next_batch.num_rows();
                    if num_rows > rows_remaining {
                        let new_batch = next_batch.slice(0, rows_remaining);
                        batches.push(new_batch);
                        last_seen_batch =
                            Some(next_batch.slice(rows_remaining, num_rows - rows_remaining));
                        rows_remaining = 0;
                    } else {
                        batches.push(next_batch);
                        rows_remaining -= num_rows;
                        last_seen_batch = None;
                    }
                }

                let new_batch =
                    arrow_select::concat::concat_batches(&batches[0].schema(), batches.iter())?;

                updater.update(new_batch).await?;
            }
            updater.finish().await
        }
        .await;

        match result {
            Ok(new_fragment) => new_fragments.push(new_fragment),
            Err(e) => {
                updater.cleanup_unfinished_writer().await;
                cleanup_new_column_data_files(fragments, &new_fragments).await;
                return Err(e);
            }
        }
    }

    // Ensure the stream is fully consumed
    if last_seen_batch.is_some() || stream.next().await.is_some() {
        cleanup_new_column_data_files(fragments, &new_fragments).await;
        return Err(Error::invalid_input_source(
            "Stream produced more values than expected for dataset".into(),
        ));
    }

    Ok(new_fragments)
}

/// Modify columns in the dataset, changing their name, type, or nullability.
///
/// If a column has an index, its index will be preserved.
pub(super) async fn alter_columns(
    dataset: &mut Dataset,
    alterations: &[ColumnAlteration],
) -> Result<()> {
    // Validate referenced columns exist and enforce NOT NULL when tightening
    // a column from nullable to non-nullable.
    let mut new_schema = dataset.schema().clone();

    // Mapping of old to new fields that need to be casted.
    let mut cast_fields: Vec<(Field, Field)> = Vec::new();

    let mut next_field_id = dataset.manifest.max_field_id() + 1;
    let version = dataset.manifest.data_storage_format.lance_file_version()?;

    for alteration in alterations {
        let field_src = dataset.schema().field(&alteration.path).ok_or_else(|| {
            Error::invalid_input(format!(
                "Column \"{}\" does not exist in the dataset",
                alteration.path
            ))
        })?;

        if let Some(nullable) = alteration.nullable
            && field_src.nullable
            && !nullable
        {
            validate_no_nulls_before_making_non_nullable(dataset, &alteration.path).await?;
        }

        let field_dest = new_schema.mut_field_by_id(field_src.id).unwrap();
        if let Some(rename) = &alteration.rename {
            field_dest.name.clone_from(rename);
        }
        if let Some(nullable) = alteration.nullable {
            field_dest.nullable = nullable;
        }

        if let Some(data_type) = &alteration.data_type {
            if !(can_cast_types(&field_src.data_type(), data_type)
                && is_upcast_downcast(&field_src.data_type(), data_type, version))
            {
                return Err(Error::invalid_input(format!(
                    "Cannot cast column \"{}\" from {:?} to {:?}",
                    alteration.path,
                    field_src.data_type(),
                    data_type
                )));
            }

            let arrow_field = ArrowField::new(
                field_dest.name.clone(),
                data_type.clone(),
                field_dest.nullable,
            );
            *field_dest = Field::try_from(&arrow_field)?;
            field_dest.set_id(field_src.parent_id, &mut next_field_id);

            cast_fields.push((field_src.clone(), field_dest.clone()));
        }
    }

    new_schema.validate()?;

    // If any column being cast has an attached index, fail fast. Cast operations
    // rewrite the underlying column data and silently invalidate any index on the
    // affected column(s). The current behavior is to drop such indices without
    // warning, which has caused production incidents where vector search silently
    // regressed to brute-force scan. We require users to explicitly drop the
    // index before altering the column type, so the action is never silent.
    if !cast_fields.is_empty() {
        let indices = dataset.load_indices().await?;
        let affected: Vec<&lance_table::format::IndexMetadata> = indices
            .iter()
            .filter(|idx| {
                cast_fields
                    .iter()
                    .any(|(old, _)| idx.fields.contains(&old.id))
            })
            .collect();
        if !affected.is_empty() {
            let affected_cols: Vec<String> = cast_fields
                .iter()
                .filter(|(old, _)| affected.iter().any(|i| i.fields.contains(&old.id)))
                .map(|(old, _)| old.name.clone())
                .collect();
            let affected_idx_names: Vec<String> = affected.iter().map(|i| i.name.clone()).collect();
            return Err(Error::invalid_input(format!(
                "Cannot cast column(s) [{}] to a new type: they have {} index(es) \
                 attached: [{}]. Cast rewrites column data and invalidates any index \
                 on the affected column(s). Drop the index(es) with drop_index() \
                 before altering, then recreate them after the cast completes.",
                affected_cols.join(", "),
                affected.len(),
                affected_idx_names.join(", "),
            )));
        }
    }

    // If we aren't casting a column, we don't need to touch the fragments.
    let transaction = if cast_fields.is_empty() {
        Transaction::new(
            dataset.manifest.version,
            Operation::Project { schema: new_schema },
            // TODO: Make it possible to alter blob columns
            /*blob_op= */ None,
        )
    } else {
        // Otherwise, we need to re-write the relevant fields.
        let read_columns = cast_fields
            .iter()
            .map(|(old, _new)| {
                let parts = dataset.schema().field_ancestry_by_id(old.id).unwrap();
                let part_names = parts.iter().map(|p| p.name.clone()).collect::<Vec<_>>();
                part_names.join(".")
            })
            .collect::<Vec<_>>();

        let new_ids = cast_fields
            .iter()
            .map(|(_old, new)| new.id)
            .collect::<Vec<_>>();
        // This schema contains the exact field ids we want to write the new fields with.
        let new_col_schema = new_schema.project_by_ids(&new_ids, true);

        let mapper = move |batch: &RecordBatch| {
            let mut fields = Vec::with_capacity(cast_fields.len());
            let mut columns = Vec::with_capacity(batch.num_columns());
            for (old, new) in &cast_fields {
                let old_column = batch[&old.name].clone();
                let new_column = cast_with_options(
                    &old_column,
                    &new.data_type(),
                    // Safe: false means it will error if the cast is lossy.
                    &CastOptions {
                        safe: false,
                        ..Default::default()
                    },
                )?;
                columns.push(new_column);
                fields.push(Arc::new(ArrowField::from(new)));
            }
            let schema = Arc::new(ArrowSchema::new(fields));
            Ok(RecordBatch::try_new(schema, columns)?)
        };
        let mapper = Box::new(mapper);

        let result = add_columns_impl(
            &dataset.get_fragments(),
            Some(read_columns),
            mapper,
            None,
            None,
            Some((new_col_schema, new_schema.clone())),
        )
        .await?;

        // Some data files may no longer contain any columns in the dataset (e.g. if every
        // remaining column has been altered into a different data file) and so we remove them
        let schema_field_ids = new_schema.field_ids().into_iter().collect::<Vec<_>>();
        let fragments = result
            .fragments
            .into_iter()
            .map(|mut frag| {
                frag.files.retain(|f| {
                    f.fields
                        .iter()
                        .any(|field| schema_field_ids.contains(field))
                });
                frag
            })
            .collect::<Vec<_>>();

        Transaction::new(
            dataset.manifest.version,
            Operation::Merge {
                schema: new_schema,
                fragments,
            },
            /*blob_op= */ None,
        )
    };

    // TODO: adjust the indices here for the new schema

    dataset
        .apply_commit(transaction, &Default::default(), &Default::default())
        .await?;

    Ok(())
}

/// Remove columns from the dataset.
///
/// This is a metadata-only operation and does not remove the data from the
/// underlying storage. In order to remove the data, you must subsequently
/// call `compact_files` to rewrite the data without the removed columns and
/// then call `cleanup_old_versions` to remove the old files.
pub(super) async fn drop_columns(dataset: &mut Dataset, columns: &[&str]) -> Result<()> {
    // Check if columns are present in the dataset and construct the new schema.
    for col in columns {
        if dataset.schema().field(col).is_none() {
            return Err(Error::invalid_input(format!(
                "Column {} does not exist in the dataset",
                col
            )));
        }
    }

    let version = dataset.manifest.data_storage_format.lance_file_version()?;
    let columns_to_remove = dataset.manifest.schema.project(columns)?;
    let new_schema = exclude(&dataset.manifest.schema, &columns_to_remove, &version)?;

    if new_schema.fields.is_empty() {
        return Err(Error::invalid_input(
            "Cannot drop all columns from a dataset",
        ));
    }

    let transaction = Transaction::new(
        dataset.manifest.version,
        Operation::Project { schema: new_schema },
        /*blob_op= */ None,
    );

    dataset
        .apply_commit(transaction, &Default::default(), &Default::default())
        .await?;

    Ok(())
}

/// Exclude the fields from `other` Schema, and returns a new Schema.
pub fn exclude(source: &Schema, other: &Schema, version: &LanceFileVersion) -> Result<Schema> {
    let other: Schema = other.try_into().map_err(|_| {
        Error::schema("The other schema is not compatible with this schema".to_string())
    })?;
    let mut fields = vec![];
    for field in source.fields.iter() {
        if let Some(other_field) = other.field(&field.name) {
            if version.support_remove_sub_column(field)
                && let Some(f) = field.exclude(other_field)
            {
                fields.push(f)
            }
        } else {
            fields.push(field.clone());
        }
    }
    Ok(Schema {
        fields,
        metadata: source.metadata.clone(),
    })
}

#[cfg(test)]
mod test {
    use std::{collections::HashMap, fs, num::NonZero, path::Path as StdPath, sync::Mutex};

    use crate::dataset::WriteParams;
    use arrow_array::{
        ArrayRef, Int32Array, ListArray, RecordBatchIterator, StringArray, StructArray,
    };

    use super::*;
    use arrow_schema::Fields as ArrowFields;
    use lance_core::utils::tempfile::TempStrDir;
    use lance_file::version::{ConcreteFileVersion, LanceFileVersion};
    use lance_table::format::{BasePath, DataFile};
    use rstest::rstest;

    // Used to validate that futures returned are Send.
    fn require_send<T: Send>(t: T) -> T {
        t
    }

    fn file_paths_in(dir: impl AsRef<StdPath>) -> Vec<String> {
        fn collect_files(
            base_dir: &StdPath,
            dir: &StdPath,
            files: &mut Vec<String>,
        ) -> std::io::Result<()> {
            if !dir.exists() {
                return Ok(());
            }
            for entry in std::fs::read_dir(dir)? {
                let path = entry?.path();
                if path.is_dir() {
                    collect_files(base_dir, &path, files)?;
                } else if path.is_file()
                    && path
                        .file_name()
                        .and_then(|name| name.to_str())
                        .is_some_and(|file_name| !file_name.starts_with('.'))
                {
                    files.push(
                        path.strip_prefix(base_dir)
                            .unwrap()
                            .to_string_lossy()
                            .to_string(),
                    );
                }
            }
            Ok(())
        }

        let base_dir = dir.as_ref();
        let mut files = Vec::new();
        collect_files(base_dir, base_dir, &mut files).unwrap();
        files.sort();
        files
    }

    fn data_file_paths_in(base_dir: &str) -> Vec<String> {
        file_paths_in(StdPath::new(base_dir).join("data"))
    }

    #[tokio::test]
    async fn test_append_columns_exprs() -> Result<()> {
        let num_rows = 5;
        let schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
            "id",
            DataType::Int32,
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from_iter_values(0..num_rows as i32))],
        )?;
        let reader = RecordBatchIterator::new(vec![Ok(batch)], schema.clone());

        let test_dir = TempStrDir::default();
        let test_uri = &test_dir;
        let mut dataset = Dataset::write(
            reader,
            test_uri,
            Some(WriteParams {
                data_storage_version: Some(LanceFileVersion::Legacy),
                ..Default::default()
            }),
        )
        .await?;
        dataset.validate().await?;

        // Adding a duplicate column name will break
        let fut = dataset.add_columns(
            NewColumnTransform::SqlExpressions(vec![("id".into(), "id + 1".into())]),
            None,
            None,
        );
        // (Quick validation that the future is Send)
        let res = require_send(fut).await;
        assert!(matches!(res, Err(Error::InvalidInput { .. })));

        // Can add a column that is independent of any existing ones
        dataset
            .add_columns(
                NewColumnTransform::SqlExpressions(vec![("value".into(), "2 * random()".into())]),
                None,
                None,
            )
            .await?;

        // Can add a column derived from an existing one.
        dataset
            .add_columns(
                NewColumnTransform::SqlExpressions(vec![("double_id".into(), "2 * id".into())]),
                None,
                None,
            )
            .await?;

        // Can derive a column from existing ones across multiple data files.
        dataset
            .add_columns(
                NewColumnTransform::SqlExpressions(vec![(
                    "triple_id".into(),
                    "id + double_id".into(),
                )]),
                None,
                None,
            )
            .await?;

        // These can be read back, the dataset is valid
        dataset.validate().await?;

        let data = dataset.scan().try_into_batch().await?;
        let expected_schema = ArrowSchema::new(vec![
            ArrowField::new("id", DataType::Int32, false),
            ArrowField::new("value", DataType::Float64, true),
            ArrowField::new("double_id", DataType::Int32, false),
            ArrowField::new("triple_id", DataType::Int32, false),
        ]);
        assert_eq!(data.schema().as_ref(), &expected_schema);
        assert_eq!(data.num_rows(), num_rows);

        Ok(())
    }

    #[tokio::test]
    async fn test_add_columns_preserves_files_when_commit_status_is_unknown() -> Result<()> {
        use crate::utils::test::{AmbiguousCommitHandler, AmbiguousFailure};

        let num_rows = 5;
        let schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
            "id",
            DataType::Int32,
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from_iter_values(0..num_rows))],
        )?;
        let reader = RecordBatchIterator::new(vec![Ok(batch)], schema);
        let test_dir = TempStrDir::default();
        let test_uri = test_dir.as_str();
        let handler = Arc::new(AmbiguousCommitHandler::default());
        let mut dataset = Dataset::write(
            reader,
            test_uri,
            Some(WriteParams {
                commit_handler: Some(handler.clone()),
                ..Default::default()
            }),
        )
        .await?;
        let files_before = data_file_paths_in(test_uri);

        handler.fail_next(AmbiguousFailure::LandAndError);
        handler
            .fail_resolve
            .store(true, std::sync::atomic::Ordering::SeqCst);
        let error = dataset
            .add_columns(
                NewColumnTransform::SqlExpressions(vec![("double_id".into(), "2 * id".into())]),
                None,
                None,
            )
            .await
            .expect_err("unverifiable commit outcome must be reported");
        assert!(
            error.is_commit_status_unknown(),
            "expected CommitStatusUnknown, got: {error:?}"
        );
        assert!(data_file_paths_in(test_uri).len() > files_before.len());

        handler
            .fail_resolve
            .store(false, std::sync::atomic::Ordering::SeqCst);
        let reopened = Dataset::open(test_uri).await?;
        let data = reopened.scan().try_into_batch().await?;
        let double_id = data
            .column_by_name("double_id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(double_id, &Int32Array::from(vec![0, 2, 4, 6, 8]));

        Ok(())
    }

    #[tokio::test]
    async fn test_add_columns_with_fully_deleted_batch() -> Result<()> {
        // Regression test: when an entire read batch has been deleted, the
        // updater yields a 0-row batch. The inner loop then never runs and
        // `batches` stays empty, so `concat_batches(&batches[0]..)` used to
        // panic with "index out of bounds: the len is 0 but the index is 0".
        //
        // A single fragment holds 105 rows; deleting the trailing 5 rows means
        // that, when read with batch_size=50, the third batch [100..105) is
        // fully filtered out and produces an empty batch.
        let schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
            "i",
            DataType::Int32,
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from_iter_values(0..105))],
        )?;
        let reader = RecordBatchIterator::new(vec![Ok(batch)], schema.clone());

        let test_dir = TempStrDir::default();
        let test_uri = &test_dir;
        let mut dataset = Dataset::write(
            reader,
            test_uri,
            Some(WriteParams {
                max_rows_per_file: 200, // keep all rows in a single fragment
                ..Default::default()
            }),
        )
        .await?;

        // Delete the entire trailing batch [100..105).
        dataset.delete("i >= 100").await?;
        assert_eq!(dataset.count_rows(None).await?, 100);

        let new_schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
            "j",
            DataType::Int32,
            false,
        )]));
        let new_batch = RecordBatch::try_new(
            new_schema.clone(),
            vec![Arc::new(Int32Array::from_iter_values(0..100))],
        )?;
        let reader = RecordBatchIterator::new(vec![Ok(new_batch)], new_schema.clone());

        // Read with batch_size=50 so the deleted trailing rows form a full empty batch.
        dataset
            .add_columns(NewColumnTransform::Reader(Box::new(reader)), None, Some(50))
            .await?;

        let data = dataset.scan().try_into_batch().await?;
        assert_eq!(data.num_rows(), 100);
        assert_eq!(
            data.column_by_name("j").unwrap().as_ref(),
            &Int32Array::from_iter_values(0..100)
        );

        Ok(())
    }

    #[rstest]
    #[tokio::test]
    async fn test_add_columns_cleans_up_blob_v2_data_on_stream_error(
        #[values(
            ("inline", b"inline".to_vec()),
            ("packed", vec![1u8; 128 * 1024]),
            ("dedicated", vec![2u8; 5 * 1024 * 1024]),
            ("external", b"external".to_vec())
        )]
        blob_case: (&str, Vec<u8>),
    ) -> Result<()> {
        let (blob_kind, payload) = blob_case;
        let schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
            "id",
            DataType::Int32,
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from_iter_values(0..1))],
        )?;
        let reader = RecordBatchIterator::new(vec![Ok(batch)], schema.clone());

        let test_dir = TempStrDir::default();
        let test_uri = test_dir.as_str();
        let external_dir = tempfile::tempdir()?;
        let external_path = external_dir.path().join("blob.bin");
        fs::write(&external_path, &payload)?;
        let external_baseline_files = file_paths_in(external_dir.path());
        let external_baseline_payload = fs::read(&external_path)?;

        let mut dataset = Dataset::write(
            reader,
            test_uri,
            Some(WriteParams {
                data_storage_version: Some(LanceFileVersion::V2_2),
                initial_bases: Some(vec![BasePath::new(
                    1,
                    external_dir.path().to_string_lossy().to_string(),
                    Some("external".to_string()),
                    false,
                )]),
                ..Default::default()
            }),
        )
        .await?;
        let baseline_files = data_file_paths_in(test_uri);

        let mut blob_builder = crate::BlobArrayBuilder::new(2);
        if blob_kind == "external" {
            blob_builder.push_uri(external_path.to_string_lossy())?;
        } else {
            blob_builder.push_bytes(payload)?;
        }
        blob_builder.push_bytes(b"extra")?;
        let blob_array = blob_builder.finish()?;
        let blob_schema = Arc::new(ArrowSchema::new(vec![crate::blob_field("blob", true)]));
        let blob_batch = RecordBatch::try_new(blob_schema.clone(), vec![blob_array])?;
        let reader = RecordBatchIterator::new(vec![Ok(blob_batch)], blob_schema);

        let err = dataset
            .add_columns(NewColumnTransform::Reader(Box::new(reader)), None, None)
            .await
            .unwrap_err();
        assert!(
            err.to_string()
                .contains("Stream produced more values than expected for dataset")
        );

        assert_eq!(
            data_file_paths_in(test_uri),
            baseline_files,
            "add_columns should clean up new data files and blob v2 sidecars on failure"
        );
        assert_eq!(
            file_paths_in(external_dir.path()),
            external_baseline_files,
            "cleanup must not delete external files"
        );
        assert_eq!(
            fs::read(&external_path)?,
            external_baseline_payload,
            "cleanup must not modify external files"
        );
        dataset.validate().await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_cleanup_preserves_checkpointed_fragment_files() -> Result<()> {
        let schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
            "id",
            DataType::Int32,
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from_iter_values(0..2))],
        )?;
        let reader = RecordBatchIterator::new(vec![Ok(batch)], schema);

        let test_dir = TempStrDir::default();
        let test_uri = test_dir.as_str();
        let mut dataset = Dataset::write(
            reader,
            test_uri,
            Some(WriteParams {
                max_rows_per_file: 1,
                data_storage_version: Some(LanceFileVersion::V2_2),
                ..Default::default()
            }),
        )
        .await?;
        let original_fragments = dataset.get_fragments();
        assert_eq!(original_fragments.len(), 2);

        let data_dir = StdPath::new(test_uri).join("data");
        let cached_file = data_dir.join("checkpointed.lance");
        let cached_blob_dir = data_dir.join("checkpointed");
        fs::write(&cached_file, b"checkpointed data")?;
        fs::create_dir_all(&cached_blob_dir)?;
        fs::write(
            cached_blob_dir.join("00000000000000000000000000000001.blob"),
            b"blob",
        )?;

        let mut checkpointed_fragment = original_fragments[0].metadata().clone();
        checkpointed_fragment.files.push(DataFile::new(
            "checkpointed.lance",
            vec![dataset.manifest.max_field_id() + 1],
            vec![0],
            ConcreteFileVersion::V2_2,
            NonZero::new(17),
            None,
        ));

        #[derive(Default)]
        struct CheckpointedFragmentStore {
            fragment: Mutex<Option<Fragment>>,
        }

        impl UDFCheckpointStore for CheckpointedFragmentStore {
            fn get_batch(&self, _info: &BatchInfo) -> Result<Option<RecordBatch>> {
                Ok(None)
            }

            fn insert_batch(&self, _info: BatchInfo, _batch: RecordBatch) -> Result<()> {
                Ok(())
            }

            fn get_fragment(&self, fragment_id: u32) -> Result<Option<Fragment>> {
                if fragment_id == 0 {
                    Ok(self.fragment.lock().unwrap().clone())
                } else {
                    Ok(None)
                }
            }

            fn insert_fragment(&self, _fragment: Fragment) -> Result<()> {
                Ok(())
            }
        }

        let transforms = NewColumnTransform::BatchUDF(BatchUDF {
            mapper: Box::new(|_| Err(Error::invalid_input("injected UDF failure"))),
            output_schema: Arc::new(ArrowSchema::new(vec![ArrowField::new(
                "checkpointed",
                DataType::Int32,
                true,
            )])),
            result_checkpoint: Some(Arc::new(CheckpointedFragmentStore {
                fragment: Mutex::new(Some(checkpointed_fragment)),
            })),
        });

        let err = dataset
            .add_columns(transforms, None, None)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("injected UDF failure"));

        assert!(
            cached_file.exists(),
            "cleanup must not delete fragment files restored from a checkpoint"
        );
        assert!(
            cached_blob_dir.exists(),
            "cleanup must not delete blob sidecars restored from a checkpoint"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_add_columns_cleans_current_blob_v2_writer_on_udf_error() -> Result<()> {
        let schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
            "id",
            DataType::Int32,
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from_iter_values(0..2))],
        )?;
        let reader = RecordBatchIterator::new(vec![Ok(batch)], schema);

        let test_dir = TempStrDir::default();
        let test_uri = test_dir.as_str();
        let mut dataset = Dataset::write(
            reader,
            test_uri,
            Some(WriteParams {
                data_storage_version: Some(LanceFileVersion::V2_2),
                ..Default::default()
            }),
        )
        .await?;
        let baseline_files = data_file_paths_in(test_uri);

        let call_count = Arc::new(Mutex::new(0usize));
        let mapper_call_count = call_count.clone();
        let output_schema = Arc::new(ArrowSchema::new(vec![crate::blob_field("blob", true)]));
        let mapper = move |batch: &RecordBatch| {
            let mut call_count = mapper_call_count.lock().unwrap();
            *call_count += 1;
            if *call_count == 2 {
                return Err(Error::invalid_input("injected UDF failure"));
            }

            let mut blob_builder = crate::BlobArrayBuilder::new(batch.num_rows());
            for _ in 0..batch.num_rows() {
                blob_builder.push_bytes(vec![7u8; 5 * 1024 * 1024])?;
            }
            Ok(RecordBatch::try_new(
                Arc::new(ArrowSchema::new(vec![crate::blob_field("blob", true)])),
                vec![blob_builder.finish()?],
            )?)
        };
        let transforms = NewColumnTransform::BatchUDF(BatchUDF {
            mapper: Box::new(mapper),
            output_schema,
            result_checkpoint: None,
        });

        let err = dataset
            .add_columns(transforms, None, Some(1))
            .await
            .unwrap_err();
        assert!(err.to_string().contains("injected UDF failure"));
        assert_eq!(
            data_file_paths_in(test_uri),
            baseline_files,
            "add_columns should clean files written by the current unfinished writer"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_add_columns_preserves_checkpointed_blob_v2_fragment_on_checkpoint_lookup_error()
    -> Result<()> {
        let schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
            "id",
            DataType::Int32,
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from_iter_values(0..2))],
        )?;
        let reader = RecordBatchIterator::new(vec![Ok(batch)], schema);

        let test_dir = TempStrDir::default();
        let test_uri = test_dir.as_str();
        let mut dataset = Dataset::write(
            reader,
            test_uri,
            Some(WriteParams {
                max_rows_per_file: 1,
                data_storage_version: Some(LanceFileVersion::V2_2),
                ..Default::default()
            }),
        )
        .await?;

        struct FailingLookupStore {
            inserted: Arc<Mutex<Option<Fragment>>>,
        }

        impl UDFCheckpointStore for FailingLookupStore {
            fn get_batch(&self, _info: &BatchInfo) -> Result<Option<RecordBatch>> {
                Ok(None)
            }

            fn insert_batch(&self, _info: BatchInfo, _batch: RecordBatch) -> Result<()> {
                Ok(())
            }

            fn get_fragment(&self, fragment_id: u32) -> Result<Option<Fragment>> {
                if fragment_id == 1 {
                    Err(Error::invalid_input("injected checkpoint lookup failure"))
                } else {
                    Ok(None)
                }
            }

            fn insert_fragment(&self, fragment: Fragment) -> Result<()> {
                *self.inserted.lock().unwrap() = Some(fragment);
                Ok(())
            }
        }

        let inserted = Arc::new(Mutex::new(None));
        let output_schema = Arc::new(ArrowSchema::new(vec![crate::blob_field("blob", true)]));
        let mapper = move |batch: &RecordBatch| {
            let mut blob_builder = crate::BlobArrayBuilder::new(batch.num_rows());
            for _ in 0..batch.num_rows() {
                blob_builder.push_bytes(vec![7u8; 5 * 1024 * 1024])?;
            }
            Ok(RecordBatch::try_new(
                Arc::new(ArrowSchema::new(vec![crate::blob_field("blob", true)])),
                vec![blob_builder.finish()?],
            )?)
        };
        let transforms = NewColumnTransform::BatchUDF(BatchUDF {
            mapper: Box::new(mapper),
            output_schema,
            result_checkpoint: Some(Arc::new(FailingLookupStore {
                inserted: inserted.clone(),
            })),
        });

        let err = dataset
            .add_columns(transforms, None, None)
            .await
            .unwrap_err();
        assert!(
            err.to_string()
                .contains("injected checkpoint lookup failure")
        );
        let inserted = inserted.lock().unwrap().clone().unwrap();
        let new_file = inserted
            .files
            .iter()
            .find(|file| {
                file.fields
                    .iter()
                    .any(|field| *field > dataset.manifest.max_field_id())
            })
            .expect("checkpoint should record the newly written data file");
        let new_file_path = StdPath::new(test_uri).join("data").join(&new_file.path);
        let new_blob_dir = StdPath::new(test_uri)
            .join("data")
            .join(StdPath::new(&new_file.path).file_stem().unwrap());
        assert!(
            new_file_path.exists(),
            "cleanup must not delete data files after checkpoint takes ownership"
        );
        assert!(
            new_blob_dir.exists(),
            "cleanup must not delete blob sidecars after checkpoint takes ownership"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_add_columns_cleans_finished_blob_v2_writer_on_checkpoint_insert_error()
    -> Result<()> {
        let schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
            "id",
            DataType::Int32,
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from_iter_values(0..1))],
        )?;
        let reader = RecordBatchIterator::new(vec![Ok(batch)], schema);

        let test_dir = TempStrDir::default();
        let test_uri = test_dir.as_str();
        let mut dataset = Dataset::write(
            reader,
            test_uri,
            Some(WriteParams {
                data_storage_version: Some(LanceFileVersion::V2_2),
                ..Default::default()
            }),
        )
        .await?;
        let baseline_files = data_file_paths_in(test_uri);

        struct FailingInsertStore;

        impl UDFCheckpointStore for FailingInsertStore {
            fn get_batch(&self, _info: &BatchInfo) -> Result<Option<RecordBatch>> {
                Ok(None)
            }

            fn insert_batch(&self, _info: BatchInfo, _batch: RecordBatch) -> Result<()> {
                Ok(())
            }

            fn get_fragment(&self, _fragment_id: u32) -> Result<Option<Fragment>> {
                Ok(None)
            }

            fn insert_fragment(&self, _fragment: Fragment) -> Result<()> {
                Err(Error::invalid_input("injected checkpoint insert failure"))
            }
        }

        let output_schema = Arc::new(ArrowSchema::new(vec![crate::blob_field("blob", true)]));
        let mapper = move |batch: &RecordBatch| {
            let mut blob_builder = crate::BlobArrayBuilder::new(batch.num_rows());
            for _ in 0..batch.num_rows() {
                blob_builder.push_bytes(vec![7u8; 5 * 1024 * 1024])?;
            }
            Ok(RecordBatch::try_new(
                Arc::new(ArrowSchema::new(vec![crate::blob_field("blob", true)])),
                vec![blob_builder.finish()?],
            )?)
        };
        let transforms = NewColumnTransform::BatchUDF(BatchUDF {
            mapper: Box::new(mapper),
            output_schema,
            result_checkpoint: Some(Arc::new(FailingInsertStore)),
        });

        let err = dataset
            .add_columns(transforms, None, None)
            .await
            .unwrap_err();
        assert!(
            err.to_string()
                .contains("injected checkpoint insert failure")
        );
        assert_eq!(
            data_file_paths_in(test_uri),
            baseline_files,
            "add_columns should clean finished writer files when checkpoint insert fails"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_add_columns_cleans_blob_v2_files_on_declared_schema_merge_error() -> Result<()> {
        let schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
            "id",
            DataType::Int32,
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from_iter_values(0..1))],
        )?;
        let reader = RecordBatchIterator::new(vec![Ok(batch)], schema);

        let test_dir = TempStrDir::default();
        let test_uri = test_dir.as_str();
        let mut dataset = Dataset::write(
            reader,
            test_uri,
            Some(WriteParams {
                data_storage_version: Some(LanceFileVersion::V2_2),
                ..Default::default()
            }),
        )
        .await?;
        let baseline_files = data_file_paths_in(test_uri);

        let mapper = move |batch: &RecordBatch| {
            let mut blob_builder = crate::BlobArrayBuilder::new(batch.num_rows());
            for _ in 0..batch.num_rows() {
                blob_builder.push_bytes(vec![7u8; 5 * 1024 * 1024])?;
            }
            Ok(RecordBatch::try_new(
                Arc::new(ArrowSchema::new(vec![crate::blob_field("blob", true)])),
                vec![blob_builder.finish()?],
            )?)
        };
        let transforms = NewColumnTransform::BatchUDF(BatchUDF {
            mapper: Box::new(mapper),
            output_schema: Arc::new(ArrowSchema::new(vec![
                ArrowField::new("declared", DataType::Int32, true),
                ArrowField::new("declared", DataType::Int32, true),
            ])),
            result_checkpoint: None,
        });

        let err = dataset
            .add_columns(transforms, None, None)
            .await
            .unwrap_err();
        assert!(matches!(err, Error::Schema { .. }));
        assert_eq!(
            data_file_paths_in(test_uri),
            baseline_files,
            "add_columns should clean files written before declared schema merge fails"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_add_columns_preserves_checkpointed_blob_v2_fragment_after_later_failure()
    -> Result<()> {
        let schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
            "id",
            DataType::Int32,
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from_iter_values(0..2))],
        )?;
        let reader = RecordBatchIterator::new(vec![Ok(batch)], schema);

        let test_dir = TempStrDir::default();
        let test_uri = test_dir.as_str();
        let mut dataset = Dataset::write(
            reader,
            test_uri,
            Some(WriteParams {
                max_rows_per_file: 1,
                data_storage_version: Some(LanceFileVersion::V2_2),
                ..Default::default()
            }),
        )
        .await?;

        struct InsertThenFailStore {
            inserted: Arc<Mutex<Option<Fragment>>>,
        }

        impl UDFCheckpointStore for InsertThenFailStore {
            fn get_batch(&self, info: &BatchInfo) -> Result<Option<RecordBatch>> {
                if info.fragment_id == 1 {
                    Err(Error::invalid_input("injected later checkpoint failure"))
                } else {
                    Ok(None)
                }
            }

            fn insert_batch(&self, _info: BatchInfo, _batch: RecordBatch) -> Result<()> {
                Ok(())
            }

            fn get_fragment(&self, _fragment_id: u32) -> Result<Option<Fragment>> {
                Ok(None)
            }

            fn insert_fragment(&self, fragment: Fragment) -> Result<()> {
                *self.inserted.lock().unwrap() = Some(fragment);
                Ok(())
            }
        }

        let inserted = Arc::new(Mutex::new(None));
        let output_schema = Arc::new(ArrowSchema::new(vec![crate::blob_field("blob", true)]));
        let mapper = move |batch: &RecordBatch| {
            let mut blob_builder = crate::BlobArrayBuilder::new(batch.num_rows());
            for _ in 0..batch.num_rows() {
                blob_builder.push_bytes(vec![7u8; 5 * 1024 * 1024])?;
            }
            Ok(RecordBatch::try_new(
                Arc::new(ArrowSchema::new(vec![crate::blob_field("blob", true)])),
                vec![blob_builder.finish()?],
            )?)
        };
        let transforms = NewColumnTransform::BatchUDF(BatchUDF {
            mapper: Box::new(mapper),
            output_schema,
            result_checkpoint: Some(Arc::new(InsertThenFailStore {
                inserted: inserted.clone(),
            })),
        });

        let err = dataset
            .add_columns(transforms, None, None)
            .await
            .unwrap_err();
        assert!(
            err.to_string()
                .contains("injected later checkpoint failure")
        );

        let inserted = inserted.lock().unwrap().clone().unwrap();
        let new_file = inserted
            .files
            .iter()
            .find(|file| {
                file.fields
                    .iter()
                    .any(|field| *field > dataset.manifest.max_field_id())
            })
            .expect("checkpoint should record the newly written data file");
        let new_file_path = StdPath::new(test_uri).join("data").join(&new_file.path);
        let new_blob_dir = StdPath::new(test_uri)
            .join("data")
            .join(StdPath::new(&new_file.path).file_stem().unwrap());
        assert!(
            new_file_path.exists(),
            "cleanup must not delete data files after checkpoint takes ownership"
        );
        assert!(
            new_blob_dir.exists(),
            "cleanup must not delete blob sidecars after checkpoint takes ownership"
        );

        Ok(())
    }

    #[rstest]
    #[tokio::test]
    async fn test_append_columns_udf(
        #[values(LanceFileVersion::Legacy, LanceFileVersion::Stable)]
        data_storage_version: LanceFileVersion,
    ) -> Result<()> {
        use arrow_array::Float64Array;

        let num_rows = 5;
        let schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
            "id",
            DataType::Int32,
            false,
        )]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from_iter_values(0..num_rows as i32))],
        )?;
        let reader = RecordBatchIterator::new(vec![Ok(batch)], schema.clone());

        let test_dir = TempStrDir::default();
        let test_uri = &test_dir;
        let mut dataset = Dataset::write(
            reader,
            test_uri,
            Some(WriteParams {
                data_storage_version: Some(data_storage_version),
                ..Default::default()
            }),
        )
        .await?;
        dataset.validate().await?;

        // Adding a duplicate column name will break
        let transforms = NewColumnTransform::BatchUDF(BatchUDF {
            mapper: Box::new(|_| unimplemented!()),
            output_schema: Arc::new(ArrowSchema::new(vec![ArrowField::new(
                "id",
                DataType::Int32,
                false,
            )])),
            result_checkpoint: None,
        });
        let res = dataset.add_columns(transforms, None, None).await;
        assert!(matches!(res, Err(Error::InvalidInput { .. })));

        // Can add a column that independent (empty read_schema)
        let output_schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
            "value",
            DataType::Float64,
            true,
        )]));
        let output_schema_ref = output_schema.clone();
        let mapper = move |batch: &RecordBatch| {
            Ok(RecordBatch::try_new(
                output_schema_ref.clone(),
                vec![Arc::new(Float64Array::from_iter_values(
                    (0..batch.num_rows()).map(|i| i as f64),
                ))],
            )?)
        };
        let transforms = NewColumnTransform::BatchUDF(BatchUDF {
            mapper: Box::new(mapper),
            output_schema,
            result_checkpoint: None,
        });
        dataset.add_columns(transforms, None, None).await?;

        // Can add a column that depends on another column (double id)
        let output_schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
            "double_id",
            DataType::Int32,
            false,
        )]));
        let output_schema_ref = output_schema.clone();
        let mapper = move |batch: &RecordBatch| {
            let id = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            Ok(RecordBatch::try_new(
                output_schema_ref.clone(),
                vec![Arc::new(Int32Array::from_iter_values(
                    id.values().iter().map(|i| i * 2),
                ))],
            )?)
        };
        let transforms = NewColumnTransform::BatchUDF(BatchUDF {
            mapper: Box::new(mapper),
            output_schema,
            result_checkpoint: None,
        });
        dataset.add_columns(transforms, None, None).await?;
        // These can be read back, the dataset is valid
        dataset.validate().await?;

        let data = dataset.scan().try_into_batch().await?;
        let expected_schema = ArrowSchema::new(vec![
            ArrowField::new("id", DataType::Int32, false),
            ArrowField::new("value", DataType::Float64, true),
            ArrowField::new("double_id", DataType::Int32, false),
        ]);
        assert_eq!(data.schema().as_ref(), &expected_schema);
        assert_eq!(data.num_rows(), num_rows);

        Ok(())
    }

    #[tokio::test]
    async fn test_append_columns_udf_cache() -> Result<()> {
        let num_rows = 100;
        let schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
            "id",
            DataType::Int32,
            false,
        )]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from_iter_values(0..num_rows))],
        )?;
        let reader = RecordBatchIterator::new(vec![Ok(batch)], schema.clone());

        let test_dir = TempStrDir::default();
        let test_uri = &test_dir;
        let mut dataset = Dataset::write(
            reader,
            test_uri,
            Some(WriteParams {
                max_rows_per_file: 50,
                max_rows_per_group: 25,
                data_storage_version: Some(LanceFileVersion::Legacy),
                ..Default::default()
            }),
        )
        .await?;
        dataset.validate().await?;

        #[derive(Default)]
        struct RequestCounter {
            pub get_batch_requests: Mutex<Vec<BatchInfo>>,
            pub insert_batch_requests: Mutex<Vec<BatchInfo>>,
            pub get_fragment_requests: Mutex<Vec<u32>>,
            pub insert_fragment_requests: Mutex<Vec<u32>>,
        }

        impl UDFCheckpointStore for RequestCounter {
            fn get_batch(&self, info: &BatchInfo) -> Result<Option<RecordBatch>> {
                self.get_batch_requests.lock().unwrap().push(info.clone());

                if info.fragment_id == 1 && info.batch_index == 0 {
                    Ok(Some(RecordBatch::try_new(
                        Arc::new(ArrowSchema::new(vec![ArrowField::new(
                            "double_id",
                            DataType::Int32,
                            false,
                        )])),
                        vec![Arc::new(Int32Array::from_iter_values(50..75))],
                    )?))
                } else {
                    Ok(None)
                }
            }

            fn insert_batch(&self, info: BatchInfo, _value: RecordBatch) -> Result<()> {
                self.insert_batch_requests.lock().unwrap().push(info);
                Ok(())
            }

            fn get_fragment(&self, fragment_id: u32) -> Result<Option<Fragment>> {
                self.get_fragment_requests.lock().unwrap().push(fragment_id);
                if fragment_id == 0 {
                    Ok(Some(Fragment {
                        files: vec![],
                        id: 0,
                        overlays: vec![],
                        deletion_file: None,
                        row_id_meta: None,
                        physical_rows: Some(50),
                        last_updated_at_version_meta: None,
                        created_at_version_meta: None,
                    }))
                } else {
                    Ok(None)
                }
            }

            fn insert_fragment(&self, fragment: Fragment) -> Result<()> {
                self.insert_fragment_requests
                    .lock()
                    .unwrap()
                    .push(fragment.id as u32);
                Ok(())
            }
        }

        let request_counter = Arc::new(RequestCounter::default());

        let output_schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
            "double_id",
            DataType::Int32,
            false,
        )]));
        let output_schema_ref = output_schema.clone();
        let mapper = move |batch: &RecordBatch| {
            let id = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            Ok(RecordBatch::try_new(
                output_schema_ref.clone(),
                vec![Arc::new(Int32Array::from_iter_values(
                    id.values().iter().map(|i| i * 2),
                ))],
            )?)
        };
        let transforms = NewColumnTransform::BatchUDF(BatchUDF {
            mapper: Box::new(mapper),
            output_schema,
            result_checkpoint: Some(request_counter.clone()),
        });
        dataset.add_columns(transforms, None, None).await?;

        // Should have requested both fragments
        assert_eq!(
            request_counter
                .get_fragment_requests
                .lock()
                .unwrap()
                .as_slice(),
            &[0, 1]
        );
        // Should have only inserted the second fragment, since the first one was already cached
        assert_eq!(
            request_counter
                .insert_fragment_requests
                .lock()
                .unwrap()
                .as_slice(),
            &[1]
        );

        // Should have only requested the second two batches, since the first fragment was already cached
        assert_eq!(
            request_counter
                .get_batch_requests
                .lock()
                .unwrap()
                .as_slice(),
            &[
                BatchInfo {
                    fragment_id: 1,
                    batch_index: 0,
                },
                BatchInfo {
                    fragment_id: 1,
                    batch_index: 1,
                },
            ]
        );
        // Should have only saved the last batch, since the first batch of second fragment was already cached
        assert_eq!(
            request_counter
                .insert_batch_requests
                .lock()
                .unwrap()
                .as_slice(),
            &[BatchInfo {
                fragment_id: 1,
                batch_index: 1,
            },]
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_add_column_all_nulls() -> Result<()> {
        let num_rows = 100;
        let schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
            "id",
            DataType::Int32,
            false,
        )]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from_iter_values(0..num_rows))],
        )?;
        let reader = RecordBatchIterator::new(vec![Ok(batch)], schema.clone());

        let test_dir = TempStrDir::default();
        let test_uri = &test_dir;
        let mut dataset = Dataset::write(
            reader,
            test_uri,
            Some(WriteParams {
                max_rows_per_file: 50,
                max_rows_per_group: 25,
                data_storage_version: Some(LanceFileVersion::Stable),
                ..Default::default()
            }),
        )
        .await?;
        dataset.validate().await?;

        dataset
            .add_columns(
                NewColumnTransform::AllNulls(Arc::new(ArrowSchema::new(vec![ArrowField::new(
                    "nulls",
                    DataType::Int32,
                    true,
                )]))),
                None,
                None,
            )
            .await?;

        let data = dataset.scan().try_into_batch().await?;
        let expected_schema = ArrowSchema::new(vec![
            ArrowField::new("id", DataType::Int32, false),
            ArrowField::new("nulls", DataType::Int32, true),
        ]);
        assert_eq!(data.schema().as_ref(), &expected_schema);
        assert_eq!(data.num_rows(), num_rows as usize);

        // check that can't add non-nullable columns
        let err =
            dataset
                .add_columns(
                    NewColumnTransform::AllNulls(Arc::new(ArrowSchema::new(vec![
                        ArrowField::new("non_nulls", DataType::Int32, false),
                    ]))),
                    None,
                    None,
                )
                .await
                .unwrap_err();
        assert!(
            matches!(err, Error::InvalidInput { .. }),
            "unexpected error: {err}"
        );

        let data = dataset.scan().try_into_batch().await?;
        let expected_schema = ArrowSchema::new(vec![
            ArrowField::new("id", DataType::Int32, false),
            ArrowField::new("nulls", DataType::Int32, true),
        ]);
        assert_eq!(data.schema().as_ref(), &expected_schema);
        assert_eq!(data.num_rows(), num_rows as usize);

        Ok(())
    }

    /// `AllNulls` accepts any nullable top-level column whatever its inner-field
    /// nullability (Map/List/Struct with non-null children); non-nullable ones are rejected.
    #[tokio::test]
    async fn test_add_column_all_nulls_nested() -> Result<()> {
        let num_rows = 100;
        let schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
            "id",
            DataType::Int32,
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from_iter_values(0..num_rows))],
        )?;
        let reader = RecordBatchIterator::new(vec![Ok(batch)], schema.clone());

        let test_dir = TempStrDir::default();
        let mut dataset = Dataset::write(
            reader,
            &test_dir,
            Some(WriteParams {
                max_rows_per_file: 50,
                max_rows_per_group: 25,
                data_storage_version: Some(LanceFileVersion::Stable),
                ..Default::default()
            }),
        )
        .await?;

        let map_with_non_null_entries = DataType::Map(
            Arc::new(ArrowField::new(
                "entries",
                DataType::Struct(ArrowFields::from(vec![
                    ArrowField::new("key", DataType::Utf8, false),
                    ArrowField::new("value", DataType::Float64, true),
                ])),
                false,
            )),
            false,
        );
        let list_with_non_null_items =
            DataType::List(Arc::new(ArrowField::new("item", DataType::Utf8, false)));
        let struct_with_non_null_child =
            DataType::Struct(ArrowFields::from(vec![ArrowField::new(
                "a",
                DataType::Int32,
                false,
            )]));

        dataset
            .add_columns(
                NewColumnTransform::AllNulls(Arc::new(ArrowSchema::new(vec![
                    ArrowField::new("cutoffs", map_with_non_null_entries.clone(), true),
                    ArrowField::new("tags", list_with_non_null_items.clone(), true),
                    ArrowField::new("info", struct_with_non_null_child.clone(), true),
                ]))),
                None,
                None,
            )
            .await?;

        let data = dataset.scan().try_into_batch().await?;
        assert_eq!(data.num_rows(), num_rows as usize);
        for (name, expected_type) in [
            ("cutoffs", &map_with_non_null_entries),
            ("tags", &list_with_non_null_items),
            ("info", &struct_with_non_null_child),
        ] {
            let column = data.column_by_name(name).unwrap();
            assert_eq!(
                column.data_type(),
                expected_type,
                "type mismatch for {name}"
            );
            assert_eq!(
                column.null_count(),
                num_rows as usize,
                "column {name} should be all-null"
            );
        }

        // A non-nullable top-level field is still rejected, and the error names it.
        let err =
            dataset
                .add_columns(
                    NewColumnTransform::AllNulls(Arc::new(ArrowSchema::new(vec![
                        ArrowField::new("non_null_cutoffs", map_with_non_null_entries, false),
                    ]))),
                    None,
                    None,
                )
                .await
                .unwrap_err();
        assert!(
            matches!(err, Error::InvalidInput { .. }),
            "unexpected error: {err}"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_add_column_all_nulls_legacy() -> Result<()> {
        let num_rows = 100;
        let schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
            "id",
            DataType::Int32,
            false,
        )]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from_iter_values(0..num_rows))],
        )?;
        let reader = RecordBatchIterator::new(vec![Ok(batch)], schema.clone());

        let test_dir = TempStrDir::default();
        let test_uri = &test_dir;
        let mut dataset = Dataset::write(
            reader,
            test_uri,
            Some(WriteParams {
                max_rows_per_file: 50,
                max_rows_per_group: 25,
                data_storage_version: Some(LanceFileVersion::Legacy),
                ..Default::default()
            }),
        )
        .await?;
        dataset.validate().await?;

        let err =
            dataset
                .add_columns(
                    NewColumnTransform::AllNulls(Arc::new(ArrowSchema::new(vec![
                        ArrowField::new("nulls", DataType::Int32, true),
                    ]))),
                    None,
                    None,
                )
                .await
                .unwrap_err();
        assert!(
            err.to_string()
                .contains("Cannot add all-null columns to legacy dataset version")
        );

        Ok(())
    }

    async fn prepare_dataset(version: LanceFileVersion) -> Result<Dataset> {
        // id: int32
        // people: list<struct<name: utf8, age: int32, city: utf8>>
        let person_struct_type = DataType::Struct(ArrowFields::from(vec![
            ArrowField::new("name", DataType::Utf8, false),
            ArrowField::new("age", DataType::Int32, false),
            ArrowField::new("city", DataType::Utf8, false),
        ]));

        let list_of_struct_type = DataType::List(Arc::new(ArrowField::new(
            "item",
            person_struct_type.clone(),
            false,
        )));

        let schema = Arc::new(ArrowSchema::new_with_metadata(
            vec![
                ArrowField::new("id", DataType::Int32, false),
                ArrowField::new("people", list_of_struct_type.clone(), false),
            ],
            HashMap::<String, String>::new(),
        ));

        // Data: 3 rows, people is a list of 2, 3, 1 structs
        let all_names = StringArray::from(vec!["Alice", "Bob", "Charlie", "David", "Eve", "Frank"]);
        let all_ages = Int32Array::from(vec![25, 30, 35, 28, 32, 40]);
        let all_cities = StringArray::from(vec![
            "Beijing",
            "Shanghai",
            "Guangzhou",
            "Shenzhen",
            "Hangzhou",
            "Chengdu",
        ]);
        let all_struct = StructArray::new(
            ArrowFields::from(vec![
                ArrowField::new("name", DataType::Utf8, false),
                ArrowField::new("age", DataType::Int32, false),
                ArrowField::new("city", DataType::Utf8, false),
            ]),
            vec![
                Arc::new(all_names) as ArrayRef,
                Arc::new(all_ages) as ArrayRef,
                Arc::new(all_cities) as ArrayRef,
            ],
            None,
        );

        let all_people = ListArray::new(
            Arc::new(ArrowField::new("item", person_struct_type, false)),
            arrow_buffer::OffsetBuffer::new(arrow_buffer::ScalarBuffer::from(vec![
                0i32, 2i32, 5i32, 6i32,
            ])),
            Arc::new(all_struct),
            None,
        );

        let ids = Int32Array::from(vec![1, 2, 3]);
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(ids) as ArrayRef, Arc::new(all_people) as ArrayRef],
        )?;

        let reader = RecordBatchIterator::new(vec![Ok(batch)], schema.clone());
        let dataset = Dataset::write(
            reader,
            "memory://test",
            Some(WriteParams {
                data_storage_version: Some(version),
                ..Default::default()
            }),
        )
        .await?;

        // Verify schema
        assert_eq!(dataset.schema().fields.len(), 2);
        assert_eq!(dataset.schema().fields[0].name, "id");
        assert_eq!(dataset.schema().fields[1].name, "people");

        Ok(dataset)
    }

    #[rstest]
    #[tokio::test]
    async fn test_drop_list_struct_sub_columns_legacy(
        #[values(
            LanceFileVersion::Legacy,
            LanceFileVersion::V2_0,
            LanceFileVersion::V2_1
        )]
        version: LanceFileVersion,
    ) -> Result<()> {
        let mut dataset = prepare_dataset(version).await?;

        // drop sub-column city from list(struct)
        dataset.drop_columns(&["people.item.city"]).await?;
        dataset.validate().await?;

        // people column has been fully removed
        assert_eq!(dataset.schema().fields.len(), 1);
        assert_eq!(dataset.schema().fields[0].name, "id");

        Ok(())
    }

    #[rstest]
    #[tokio::test]
    async fn test_drop_list_struct_sub_columns(
        #[values(LanceFileVersion::V2_2)] version: LanceFileVersion,
    ) -> Result<()> {
        let mut dataset = prepare_dataset(version).await?;

        // drop sub-column city from list(struct)
        dataset.drop_columns(&["people.item.city"]).await?;
        dataset.validate().await?;

        // people.item only contains name, age
        let expected_schema = ArrowSchema::new_with_metadata(
            vec![
                ArrowField::new("id", DataType::Int32, false),
                ArrowField::new(
                    "people",
                    DataType::List(Arc::new(ArrowField::new(
                        "item",
                        DataType::Struct(ArrowFields::from(vec![
                            ArrowField::new("name", DataType::Utf8, false),
                            ArrowField::new("age", DataType::Int32, false),
                        ])),
                        false,
                    ))),
                    false,
                ),
            ],
            HashMap::<String, String>::new(),
        );
        assert_eq!(ArrowSchema::from(dataset.schema()), expected_schema);

        // Verify data
        let batch = dataset.scan().try_into_batch().await?;
        assert_eq!(batch.num_rows(), 3);
        assert_eq!(batch.num_columns(), 2);

        let list_array = batch
            .column(1)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        let list_value = list_array.value(0);
        let struct_array = list_value.as_any().downcast_ref::<StructArray>().unwrap();
        assert!(struct_array.column_by_name("city").is_none());

        Ok(())
    }

    #[test]
    fn test_exclude_fields() {
        let arrow_schema = ArrowSchema::new(vec![
            ArrowField::new("a", DataType::Int32, false),
            ArrowField::new(
                "b",
                DataType::Struct(ArrowFields::from(vec![
                    ArrowField::new("f1", DataType::Utf8, true),
                    ArrowField::new("f2", DataType::Boolean, false),
                    ArrowField::new("f3", DataType::Float32, false),
                ])),
                true,
            ),
            ArrowField::new("c", DataType::Float64, false),
        ]);
        let schema = Schema::try_from(&arrow_schema).unwrap();

        let projection = schema.project(&["a", "b.f2", "b.f3"]).unwrap();
        let excluded = exclude(&schema, &projection, &LanceFileVersion::V2_2).unwrap();

        let expected_arrow_schema = ArrowSchema::new(vec![
            ArrowField::new(
                "b",
                DataType::Struct(ArrowFields::from(vec![ArrowField::new(
                    "f1",
                    DataType::Utf8,
                    true,
                )])),
                true,
            ),
            ArrowField::new("c", DataType::Float64, false),
        ]);
        assert_eq!(ArrowSchema::from(&excluded), expected_arrow_schema);
    }

    #[rstest]
    #[tokio::test]
    async fn test_rename_columns(
        #[values(LanceFileVersion::Legacy, LanceFileVersion::Stable)]
        data_storage_version: LanceFileVersion,
    ) -> Result<()> {
        use std::collections::HashMap;

        use arrow_array::{ArrayRef, StructArray};

        let metadata: HashMap<String, String> = [("k1".into(), "v1".into())].into();

        let schema = Arc::new(ArrowSchema::new_with_metadata(
            vec![
                ArrowField::new("a", DataType::Int32, false),
                ArrowField::new(
                    "b",
                    DataType::Struct(ArrowFields::from(vec![ArrowField::new(
                        "c",
                        DataType::Int32,
                        true,
                    )])),
                    true,
                ),
            ],
            metadata.clone(),
        ));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2])),
                Arc::new(StructArray::from(vec![(
                    Arc::new(ArrowField::new("c", DataType::Int32, true)),
                    Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef,
                )])),
            ],
        )?;

        let test_dir = TempStrDir::default();
        let test_uri = &test_dir;

        let batches = RecordBatchIterator::new(vec![Ok(batch)], schema.clone());
        let mut dataset = Dataset::write(
            batches,
            test_uri,
            Some(WriteParams {
                data_storage_version: Some(data_storage_version),
                ..Default::default()
            }),
        )
        .await?;

        let original_fragments = dataset.fragments().to_vec();

        // Rename a top-level column
        dataset
            .alter_columns(&[ColumnAlteration::new("a".into())
                .rename("x".into())
                .set_nullable(true)])
            .await?;
        dataset.validate().await?;
        assert_eq!(dataset.manifest.version, 2);
        assert_eq!(dataset.fragments().as_ref(), &original_fragments);

        let expected_schema = ArrowSchema::new_with_metadata(
            vec![
                ArrowField::new("x", DataType::Int32, true),
                ArrowField::new(
                    "b",
                    DataType::Struct(ArrowFields::from(vec![ArrowField::new(
                        "c",
                        DataType::Int32,
                        true,
                    )])),
                    true,
                ),
            ],
            metadata.clone(),
        );
        assert_eq!(&ArrowSchema::from(dataset.schema()), &expected_schema);

        // Rename to duplicate name fails
        let err = dataset
            .alter_columns(&[ColumnAlteration::new("b".into()).rename("x".into())])
            .await
            .unwrap_err();
        assert!(err.to_string().contains("Duplicate field name \"x\""));

        // Rename a nested column.
        dataset
            .alter_columns(&[ColumnAlteration::new("b.c".into()).rename("d".into())])
            .await?;
        dataset.validate().await?;
        assert_eq!(dataset.manifest.version, 3);
        assert_eq!(dataset.fragments().as_ref(), &original_fragments);

        let expected_schema = ArrowSchema::new_with_metadata(
            vec![
                ArrowField::new("x", DataType::Int32, true),
                ArrowField::new(
                    "b",
                    DataType::Struct(ArrowFields::from(vec![ArrowField::new(
                        "d",
                        DataType::Int32,
                        true,
                    )])),
                    true,
                ),
            ],
            metadata.clone(),
        );
        assert_eq!(&ArrowSchema::from(dataset.schema()), &expected_schema);

        Ok(())
    }

    #[rstest]
    #[tokio::test]
    async fn test_set_not_null_succeeds(
        #[values(LanceFileVersion::Legacy, LanceFileVersion::Stable)]
        data_storage_version: LanceFileVersion,
    ) -> Result<()> {
        let schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
            "a",
            DataType::Int32,
            true,
        )]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from_iter_values([1, 2, 3]))],
        )?;
        let test_dir = TempStrDir::default();
        let test_uri = &test_dir;
        let mut dataset = Dataset::write(
            RecordBatchIterator::new(vec![Ok(batch)], schema.clone()),
            test_uri,
            Some(WriteParams {
                data_storage_version: Some(data_storage_version),
                ..Default::default()
            }),
        )
        .await?;

        let original_fragments = dataset.fragments().to_vec();
        dataset
            .alter_columns(&[ColumnAlteration::new("a".into()).set_nullable(false)])
            .await?;
        dataset.validate().await?;

        assert_eq!(dataset.manifest.version, 2);
        assert_eq!(dataset.fragments().as_ref(), &original_fragments);
        assert_eq!(
            &ArrowSchema::from(dataset.schema()),
            &ArrowSchema::new(vec![ArrowField::new("a", DataType::Int32, false)])
        );

        Ok(())
    }

    #[rstest]
    #[tokio::test]
    async fn test_set_not_null_succeeds_nested(
        #[values(LanceFileVersion::Legacy, LanceFileVersion::Stable)]
        data_storage_version: LanceFileVersion,
    ) -> Result<()> {
        use arrow_array::{ArrayRef, StructArray};

        let schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
            "b",
            DataType::Struct(ArrowFields::from(vec![ArrowField::new(
                "c",
                DataType::Int32,
                true,
            )])),
            false,
        )]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(StructArray::from(vec![(
                Arc::new(ArrowField::new("c", DataType::Int32, true)),
                Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef,
            )]))],
        )?;
        let test_dir = TempStrDir::default();
        let test_uri = &test_dir;
        let mut dataset = Dataset::write(
            RecordBatchIterator::new(vec![Ok(batch)], schema.clone()),
            test_uri,
            Some(WriteParams {
                data_storage_version: Some(data_storage_version),
                ..Default::default()
            }),
        )
        .await?;

        let original_fragments = dataset.fragments().to_vec();
        dataset
            .alter_columns(&[ColumnAlteration::new("b.c".into()).set_nullable(false)])
            .await?;
        dataset.validate().await?;

        assert_eq!(dataset.fragments().as_ref(), &original_fragments);
        assert_eq!(
            &ArrowSchema::from(dataset.schema()),
            &ArrowSchema::new(vec![ArrowField::new(
                "b",
                DataType::Struct(ArrowFields::from(vec![ArrowField::new(
                    "c",
                    DataType::Int32,
                    false
                )])),
                false
            )])
        );

        Ok(())
    }

    #[rstest]
    #[tokio::test]
    async fn test_set_not_null_fails_with_nulls(
        #[values(LanceFileVersion::Stable)] data_storage_version: LanceFileVersion,
    ) -> Result<()> {
        let schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
            "a",
            DataType::Int32,
            true,
        )]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![Some(1), None, Some(3)]))],
        )?;
        let test_dir = TempStrDir::default();
        let test_uri = &test_dir;
        let mut dataset = Dataset::write(
            RecordBatchIterator::new(vec![Ok(batch)], schema.clone()),
            test_uri,
            Some(WriteParams {
                data_storage_version: Some(data_storage_version),
                ..Default::default()
            }),
        )
        .await?;

        let err = dataset
            .alter_columns(&[ColumnAlteration::new("a".into()).set_nullable(false)])
            .await
            .unwrap_err();
        assert!(err.to_string().contains("contains NULL values"));
        assert_eq!(
            &ArrowSchema::from(dataset.schema()),
            &ArrowSchema::new(vec![ArrowField::new("a", DataType::Int32, true)])
        );

        Ok(())
    }

    #[rstest]
    #[tokio::test]
    async fn test_set_not_null_fails_with_nulls_nested(
        #[values(LanceFileVersion::Stable)] data_storage_version: LanceFileVersion,
    ) -> Result<()> {
        use arrow_array::{ArrayRef, StructArray};

        let schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
            "b",
            DataType::Struct(ArrowFields::from(vec![ArrowField::new(
                "c",
                DataType::Int32,
                true,
            )])),
            false,
        )]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(StructArray::from(vec![(
                Arc::new(ArrowField::new("c", DataType::Int32, true)),
                Arc::new(Int32Array::from(vec![Some(1), None, Some(3)])) as ArrayRef,
            )]))],
        )?;
        let test_dir = TempStrDir::default();
        let test_uri = &test_dir;
        let mut dataset = Dataset::write(
            RecordBatchIterator::new(vec![Ok(batch)], schema.clone()),
            test_uri,
            Some(WriteParams {
                data_storage_version: Some(data_storage_version),
                ..Default::default()
            }),
        )
        .await?;

        let err = dataset
            .alter_columns(&[ColumnAlteration::new("b.c".into()).set_nullable(false)])
            .await
            .unwrap_err();
        assert!(err.to_string().contains("contains NULL values"));
        assert_eq!(
            &ArrowSchema::from(dataset.schema()),
            &ArrowSchema::new(vec![ArrowField::new(
                "b",
                DataType::Struct(ArrowFields::from(vec![ArrowField::new(
                    "c",
                    DataType::Int32,
                    true
                )])),
                false
            )])
        );

        Ok(())
    }

    #[rstest]
    #[tokio::test]
    async fn test_cast_column(
        #[values(LanceFileVersion::Legacy, LanceFileVersion::Stable)]
        data_storage_version: LanceFileVersion,
    ) -> Result<()> {
        // Create a table with 2 scalar columns, 1 vector column

        use arrow::datatypes::{Int32Type, Int64Type};
        use arrow_array::{Float16Array, Float32Array, Int64Array, ListArray};
        use half::f16;
        use lance_arrow::FixedSizeListArrayExt;
        use lance_index::{IndexType, scalar::ScalarIndexParams};
        use lance_linalg::distance::MetricType;
        use lance_testing::datagen::generate_random_array;

        use crate::index::vector::VectorIndexParams;
        let schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("i", DataType::Int32, false),
            ArrowField::new("f", DataType::Float32, false),
            ArrowField::new(
                "vec",
                DataType::FixedSizeList(
                    Arc::new(ArrowField::new("item", DataType::Float32, true)),
                    128,
                ),
                false,
            ),
            ArrowField::new("l", DataType::new_list(DataType::Int32, true), true),
        ]));

        let nrows = 512;
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from_iter_values(0..nrows)),
                Arc::new(Float32Array::from_iter_values((0..nrows).map(|i| i as f32))),
                Arc::new(
                    <arrow_array::FixedSizeListArray as FixedSizeListArrayExt>::try_new_from_values(
                        generate_random_array(128 * nrows as usize),
                        128,
                    )
                    .unwrap(),
                ),
                Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(
                    (0..nrows).map(|i| Some(vec![Some(i), Some(i + 1)])),
                )),
            ],
        )?;

        let test_dir = TempStrDir::default();
        let test_uri = &test_dir;

        let mut dataset = Dataset::write(
            RecordBatchIterator::new(vec![Ok(batch.clone())], schema.clone()),
            test_uri,
            Some(WriteParams {
                data_storage_version: Some(data_storage_version),
                ..Default::default()
            }),
        )
        .await?;

        let params = VectorIndexParams::ivf_pq(10, 8, 2, MetricType::L2, 50);
        dataset
            .create_index(&["vec"], IndexType::Vector, None, &params, false)
            .await?;
        dataset
            .create_index(
                &["i"],
                IndexType::Scalar,
                None,
                &ScalarIndexParams::default(),
                false,
            )
            .await?;
        dataset.validate().await?;

        let indices = dataset.load_indices().await?;
        assert_eq!(indices.len(), 2);

        // Cast a scalar column to another type, nullability
        dataset
            .alter_columns(&[ColumnAlteration::new("f".into())
                .cast_to(DataType::Float16)
                .set_nullable(true)])
            .await?;
        dataset.validate().await?;
        let expected_schema = ArrowSchema::new(vec![
            ArrowField::new("i", DataType::Int32, false),
            ArrowField::new("f", DataType::Float16, true),
            ArrowField::new(
                "vec",
                DataType::FixedSizeList(
                    Arc::new(ArrowField::new("item", DataType::Float32, true)),
                    128,
                ),
                false,
            ),
            ArrowField::new("l", DataType::new_list(DataType::Int32, true), true),
        ]);
        assert_eq!(&ArrowSchema::from(dataset.schema()), &expected_schema);

        // Each fragment gains a file with the new columns
        dataset.fragments().iter().for_each(|f| {
            assert_eq!(f.files.len(), 2);
        });

        // Cast scalar column with index. The index must be dropped first; cast
        // is now a fail-fast operation when an index is attached, see
        // test_alter_columns_cast_fails_with_attached_index for that path.
        dataset.drop_index("i_idx").await?;
        dataset
            .alter_columns(&[ColumnAlteration::new("i".into()).cast_to(DataType::Int64)])
            .await?;
        dataset.validate().await?;

        let expected_schema = ArrowSchema::new(vec![
            ArrowField::new("i", DataType::Int64, false),
            ArrowField::new("f", DataType::Float16, true),
            ArrowField::new(
                "vec",
                DataType::FixedSizeList(
                    Arc::new(ArrowField::new("item", DataType::Float32, true)),
                    128,
                ),
                false,
            ),
            ArrowField::new("l", DataType::new_list(DataType::Int32, true), true),
        ]);
        assert_eq!(&ArrowSchema::from(dataset.schema()), &expected_schema);

        // The scalar index on `i` is gone (we dropped it); the vector index on
        // `vec` is still present.
        let indices = dataset.load_indices().await?;
        assert_eq!(indices.len(), 1);

        // Each fragment gains a file with the new columns
        dataset.fragments().iter().for_each(|f| {
            assert_eq!(f.files.len(), 3);
        });

        // Cast vector column. Drop its index first (same reason as above).
        dataset.drop_index("vec_idx").await?;
        dataset
            .alter_columns(&[
                ColumnAlteration::new("vec".into()).cast_to(DataType::FixedSizeList(
                    Arc::new(ArrowField::new("item", DataType::Float16, true)),
                    128,
                )),
            ])
            .await?;
        dataset.validate().await?;

        // Finally, case list column to show we can handle children.
        dataset
            .alter_columns(&[ColumnAlteration::new("l".into())
                .cast_to(DataType::new_list(DataType::Int64, true))])
            .await?;
        dataset.validate().await?;

        let expected_schema = ArrowSchema::new(vec![
            ArrowField::new("i", DataType::Int64, false),
            ArrowField::new("f", DataType::Float16, true),
            ArrowField::new(
                "vec",
                DataType::FixedSizeList(
                    Arc::new(ArrowField::new("item", DataType::Float16, true)),
                    128,
                ),
                false,
            ),
            ArrowField::new("l", DataType::new_list(DataType::Int64, true), true),
        ]);
        assert_eq!(&ArrowSchema::from(dataset.schema()), &expected_schema);

        // We currently lose the index when casting a column
        let indices = dataset.load_indices().await?;
        assert_eq!(indices.len(), 0);

        // Each fragment gains a file with the new columns, but then the original file is dropped
        dataset.fragments().iter().for_each(|f| {
            assert_eq!(f.files.len(), 4);
        });

        let expected_data = RecordBatch::try_new(
            Arc::new(expected_schema),
            vec![
                Arc::new(Int64Array::from_iter_values(0..nrows as i64)),
                Arc::new(Float16Array::from_iter_values(
                    (0..nrows).map(|i| f16::from_f32(i as f32)),
                )),
                cast_with_options(
                    batch["vec"].as_ref(),
                    &DataType::FixedSizeList(
                        Arc::new(ArrowField::new("item", DataType::Float16, true)),
                        128,
                    ),
                    &Default::default(),
                )?,
                Arc::new(ListArray::from_iter_primitive::<Int64Type, _, _>(
                    (0..nrows as i64).map(|i| Some(vec![Some(i), Some(i + 1)])),
                )),
            ],
        )?;
        let actual_data = dataset.scan().try_into_batch().await?;
        assert_eq!(actual_data, expected_data);

        Ok(())
    }

    /// Cast on a column with an attached index must fail fast rather than
    /// silently dropping the index. This guards against the historical behavior
    /// where cast would rewrite column data and the index would vanish without
    /// any error or warning, causing vector search to silently regress to a
    /// brute-force scan.
    #[rstest]
    #[tokio::test]
    async fn test_alter_columns_cast_fails_with_attached_index(
        #[values(LanceFileVersion::Legacy, LanceFileVersion::Stable)]
        data_storage_version: LanceFileVersion,
    ) -> Result<()> {
        use lance_arrow::FixedSizeListArrayExt;
        use lance_index::IndexType;
        use lance_linalg::distance::MetricType;
        use lance_testing::datagen::generate_random_array;

        use crate::index::vector::VectorIndexParams;

        // Build a small dataset with one indexed vector column.
        let schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
            "vec",
            DataType::FixedSizeList(
                Arc::new(ArrowField::new("item", DataType::Float32, true)),
                64,
            ),
            false,
        )]));
        let nrows = 256;
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(
                <arrow_array::FixedSizeListArray as FixedSizeListArrayExt>::try_new_from_values(
                    generate_random_array(64 * nrows as usize),
                    64,
                )
                .unwrap(),
            )],
        )?;

        let test_dir = TempStrDir::default();
        let mut dataset = Dataset::write(
            RecordBatchIterator::new(vec![Ok(batch)], schema.clone()),
            &test_dir,
            Some(WriteParams {
                data_storage_version: Some(data_storage_version),
                ..Default::default()
            }),
        )
        .await?;

        // Build an IVF_PQ index on the vector column.
        let params = VectorIndexParams::ivf_pq(4, 8, 8, MetricType::L2, 50);
        dataset
            .create_index(&["vec"], IndexType::Vector, None, &params, false)
            .await?;

        let indices_before = dataset.load_indices().await?;
        assert_eq!(indices_before.len(), 1, "precondition: index exists");
        let index_name = indices_before[0].name.clone();

        // Attempting to cast the indexed column must fail with a clear message
        // that names the offending index(es).
        let result = dataset
            .alter_columns(&[
                ColumnAlteration::new("vec".into()).cast_to(DataType::FixedSizeList(
                    Arc::new(ArrowField::new("item", DataType::Float16, true)),
                    64,
                )),
            ])
            .await;
        let err = result.expect_err("cast on indexed column should fail");
        let msg = err.to_string();
        assert!(
            msg.contains("vec") && msg.contains(&index_name),
            "error should mention column and index name, got: {msg}"
        );
        assert!(
            msg.contains("drop_index"),
            "error should suggest the remediation, got: {msg}"
        );

        // The dataset must be unchanged: schema is still float32, index still present.
        assert_eq!(
            dataset.schema().field("vec").unwrap().data_type(),
            DataType::FixedSizeList(
                Arc::new(ArrowField::new("item", DataType::Float32, true)),
                64,
            ),
        );
        let indices_after = dataset.load_indices().await?;
        assert_eq!(indices_after.len(), 1, "index should still exist");
        assert_eq!(indices_after[0].name, index_name);

        // Sanity check: after dropping the index, the same cast should succeed.
        dataset.drop_index(&index_name).await?;
        dataset
            .alter_columns(&[
                ColumnAlteration::new("vec".into()).cast_to(DataType::FixedSizeList(
                    Arc::new(ArrowField::new("item", DataType::Float16, true)),
                    64,
                )),
            ])
            .await?;
        assert_eq!(
            dataset.schema().field("vec").unwrap().data_type(),
            DataType::FixedSizeList(
                Arc::new(ArrowField::new("item", DataType::Float16, true)),
                64,
            ),
        );

        Ok(())
    }

    #[test]
    fn test_is_upcast_downcast_dictionary() {
        use DataType::*;

        let dict_i32_utf8 = Dictionary(Box::new(Int32), Box::new(Utf8));
        let dict_i16_utf8 = Dictionary(Box::new(Int16), Box::new(Utf8));
        let dict_i32_large_utf8 = Dictionary(Box::new(Int32), Box::new(LargeUtf8));
        let dict_i32_int64 = Dictionary(Box::new(Int32), Box::new(Int64));
        let stable = LanceFileVersion::Stable;
        let legacy = LanceFileVersion::Legacy;

        // Dict(_, Utf8) -> Utf8 / LargeUtf8 (decode direction): both versions.
        assert!(is_upcast_downcast(&dict_i32_utf8, &Utf8, stable));
        assert!(is_upcast_downcast(&dict_i32_utf8, &LargeUtf8, stable));
        assert!(is_upcast_downcast(&dict_i32_utf8, &Utf8, legacy));
        assert!(is_upcast_downcast(&dict_i32_utf8, &LargeUtf8, legacy));

        // Utf8 / LargeUtf8 -> Dict(_, Utf8) (encode direction): stable only.
        assert!(is_upcast_downcast(&Utf8, &dict_i32_utf8, stable));
        assert!(is_upcast_downcast(&LargeUtf8, &dict_i32_utf8, stable));
        assert!(!is_upcast_downcast(&Utf8, &dict_i32_utf8, legacy));
        assert!(!is_upcast_downcast(&LargeUtf8, &dict_i32_utf8, legacy));

        // Dict -> Dict with compatible value types, including different index
        // types. Stable only; Legacy can't materialize a fresh dictionary.
        assert!(is_upcast_downcast(&dict_i32_utf8, &dict_i16_utf8, stable));
        assert!(is_upcast_downcast(
            &dict_i32_utf8,
            &dict_i32_large_utf8,
            stable
        ));
        assert!(!is_upcast_downcast(&dict_i32_utf8, &dict_i16_utf8, legacy));

        // Dict(_, Int64) <-> integer types (peel applies to non-string families).
        assert!(is_upcast_downcast(&dict_i32_int64, &Int32, stable));
        assert!(is_upcast_downcast(&Int32, &dict_i32_int64, stable));
        assert!(is_upcast_downcast(&dict_i32_int64, &Int32, legacy));
        assert!(!is_upcast_downcast(&Int32, &dict_i32_int64, legacy));

        // Cross-family casts must still be rejected after peeling.
        assert!(!is_upcast_downcast(&dict_i32_utf8, &Int32, stable));
        assert!(!is_upcast_downcast(&Int32, &dict_i32_utf8, stable));
        assert!(!is_upcast_downcast(&dict_i32_utf8, &Boolean, stable));
        assert!(!is_upcast_downcast(&Boolean, &dict_i32_utf8, stable));
    }

    #[rstest]
    #[tokio::test]
    async fn test_cast_dictionary_to_string(
        #[values(LanceFileVersion::Legacy, LanceFileVersion::Stable)]
        data_storage_version: LanceFileVersion,
    ) -> Result<()> {
        use arrow_array::DictionaryArray;
        use arrow_array::types::Int32Type;

        let dict_type = DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));
        let schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
            "d",
            dict_type.clone(),
            false,
        )]));

        let values = ["alpha", "beta", "gamma", "alpha", "beta"];
        let dict_array: DictionaryArray<Int32Type> = values.iter().copied().collect();
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(dict_array)])?;

        let test_dir = TempStrDir::default();
        let test_uri = &test_dir;

        let mut dataset = Dataset::write(
            RecordBatchIterator::new(vec![Ok(batch.clone())], schema.clone()),
            test_uri,
            Some(WriteParams {
                data_storage_version: Some(data_storage_version),
                ..Default::default()
            }),
        )
        .await?;

        dataset
            .alter_columns(&[ColumnAlteration::new("d".into()).cast_to(DataType::Utf8)])
            .await?;
        dataset.validate().await?;
        assert_eq!(
            dataset.schema().field("d").unwrap().data_type(),
            DataType::Utf8
        );
        let scanned = dataset.scan().try_into_batch().await?;
        let decoded = scanned.column_by_name("d").unwrap();
        let expected_decoded = StringArray::from(values.to_vec());
        assert_eq!(
            decoded.as_ref(),
            &expected_decoded as &dyn arrow_array::Array
        );

        // Cross-family casts must still be rejected even through a dictionary.
        let err = dataset
            .alter_columns(&[ColumnAlteration::new("d".into()).cast_to(DataType::Int32)])
            .await
            .unwrap_err();
        assert!(err.to_string().contains("Cannot cast column"));

        Ok(())
    }

    // Stable can materialize a fresh Dictionary column via alter; Legacy
    // cannot, because its writer requires `field.dictionary` metadata to be
    // pre-populated, so the cast is rejected upfront with a clean error.
    #[rstest]
    #[tokio::test]
    async fn test_cast_string_to_dictionary(
        #[values(LanceFileVersion::Legacy, LanceFileVersion::Stable)]
        data_storage_version: LanceFileVersion,
    ) -> Result<()> {
        use arrow_array::DictionaryArray;
        use arrow_array::types::Int32Type;

        let dict_type = DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));
        let schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
            "s",
            DataType::Utf8,
            false,
        )]));

        let values = ["alpha", "beta", "gamma", "alpha", "beta"];
        let string_array = StringArray::from(values.to_vec());
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(string_array)])?;

        let test_dir = TempStrDir::default();
        let test_uri = &test_dir;

        let mut dataset = Dataset::write(
            RecordBatchIterator::new(vec![Ok(batch.clone())], schema.clone()),
            test_uri,
            Some(WriteParams {
                data_storage_version: Some(data_storage_version),
                ..Default::default()
            }),
        )
        .await?;

        let result = dataset
            .alter_columns(&[ColumnAlteration::new("s".into()).cast_to(dict_type.clone())])
            .await;

        match data_storage_version {
            LanceFileVersion::Legacy => {
                let err = result.unwrap_err();
                assert!(
                    err.to_string().contains("Cannot cast column"),
                    "expected upfront rejection on Legacy, got: {err}"
                );
            }
            _ => {
                result?;
                dataset.validate().await?;
                assert_eq!(
                    dataset.schema().field("s").unwrap().data_type(),
                    dict_type.clone()
                );
                let scanned = dataset.scan().try_into_batch().await?;
                let encoded = scanned.column_by_name("s").unwrap();
                let expected_encoded: DictionaryArray<Int32Type> = values.iter().copied().collect();
                assert_eq!(
                    encoded.as_ref(),
                    &expected_encoded as &dyn arrow_array::Array
                );
            }
        }

        Ok(())
    }

    #[rstest]
    #[tokio::test]
    async fn test_drop_columns(
        #[values(LanceFileVersion::Legacy, LanceFileVersion::Stable)]
        data_storage_version: LanceFileVersion,
    ) -> Result<()> {
        use std::collections::HashMap;

        use arrow_array::{ArrayRef, Float32Array, StructArray};

        let metadata: HashMap<String, String> = [("k1".into(), "v1".into())].into();

        let schema = Arc::new(ArrowSchema::new_with_metadata(
            vec![
                ArrowField::new("i", DataType::Int32, false),
                ArrowField::new(
                    "s",
                    DataType::Struct(ArrowFields::from(vec![
                        ArrowField::new("d", DataType::Int32, true),
                        ArrowField::new("l", DataType::Int32, true),
                    ])),
                    true,
                ),
                ArrowField::new("x", DataType::Float32, false),
            ],
            metadata.clone(),
        ));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2])),
                Arc::new(StructArray::from(vec![
                    (
                        Arc::new(ArrowField::new("d", DataType::Int32, true)),
                        Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef,
                    ),
                    (
                        Arc::new(ArrowField::new("l", DataType::Int32, true)),
                        Arc::new(Int32Array::from(vec![1, 2])),
                    ),
                ])),
                Arc::new(Float32Array::from(vec![1.0, 2.0])),
            ],
        )?;

        let test_dir = TempStrDir::default();
        let test_uri = &test_dir;

        let batches = RecordBatchIterator::new(vec![Ok(batch)], schema.clone());
        let mut dataset = Dataset::write(
            batches,
            test_uri,
            Some(WriteParams {
                data_storage_version: Some(data_storage_version),
                ..Default::default()
            }),
        )
        .await?;

        let lance_schema = dataset.schema().clone();
        let original_fragments = dataset.fragments().to_vec();

        dataset.drop_columns(&["x"]).await?;
        dataset.validate().await?;

        let expected_schema = lance_schema.project(&["i", "s"])?;
        assert_eq!(dataset.schema(), &expected_schema);

        assert_eq!(dataset.version().version, 2);
        assert_eq!(dataset.fragments().as_ref(), &original_fragments);

        dataset.drop_columns(&["s.d"]).await?;
        dataset.validate().await?;

        let expected_schema = expected_schema.project(&["i", "s.l"])?;
        assert_eq!(dataset.schema(), &expected_schema);

        let expected_data = RecordBatch::try_new(
            Arc::new(ArrowSchema::from(&expected_schema)),
            vec![
                Arc::new(Int32Array::from(vec![1, 2])),
                Arc::new(StructArray::from(vec![(
                    Arc::new(ArrowField::new("l", DataType::Int32, true)),
                    Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef,
                )])),
            ],
        )?;
        let actual_data = dataset.scan().try_into_batch().await?;
        assert_eq!(actual_data, expected_data);

        assert_eq!(dataset.version().version, 3);
        assert_eq!(dataset.fragments().as_ref(), &original_fragments);

        Ok(())
    }

    #[rstest]
    #[tokio::test]
    async fn test_drop_add_columns(
        #[values(LanceFileVersion::Legacy, LanceFileVersion::Stable)]
        data_storage_version: LanceFileVersion,
    ) -> Result<()> {
        let schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
            "i",
            DataType::Int32,
            false,
        )]));
        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(vec![1, 2]))])?;

        let test_dir = TempStrDir::default();
        let test_uri = &test_dir;

        let batches = RecordBatchIterator::new(vec![Ok(batch)], schema.clone());
        let mut dataset = Dataset::write(
            batches,
            test_uri,
            Some(WriteParams {
                data_storage_version: Some(data_storage_version),
                ..Default::default()
            }),
        )
        .await?;
        assert_eq!(dataset.manifest.max_field_id(), 0);

        // Test we can add 1 column, drop it, then add another column. Validate
        // the field ids are as expected.
        dataset
            .add_columns(
                NewColumnTransform::SqlExpressions(vec![("x".into(), "i + 1".into())]),
                Some(vec!["i".into()]),
                None,
            )
            .await?;
        assert_eq!(dataset.manifest.max_field_id(), 1);

        dataset.drop_columns(&["x"]).await?;
        assert_eq!(dataset.manifest.max_field_id(), 0);

        dataset
            .add_columns(
                NewColumnTransform::SqlExpressions(vec![("y".into(), "2 * i".into())]),
                Some(vec!["i".into()]),
                None,
            )
            .await?;
        assert_eq!(dataset.manifest.max_field_id(), 1);

        let data = dataset.scan().try_into_batch().await?;
        let expected_data = RecordBatch::try_new(
            Arc::new(schema.try_with_column(ArrowField::new("y", DataType::Int32, false))?),
            vec![
                Arc::new(Int32Array::from(vec![1, 2])),
                Arc::new(Int32Array::from(vec![2, 4])),
            ],
        )?;
        assert_eq!(data, expected_data);
        dataset.drop_columns(&["y"]).await?;
        assert_eq!(dataset.manifest.max_field_id(), 0);

        // Test we can add 2 columns, drop 1, then add another column. Validate
        // the field ids are as expected.
        dataset
            .add_columns(
                NewColumnTransform::SqlExpressions(vec![
                    ("a".into(), "i + 3".into()),
                    ("b".into(), "i + 7".into()),
                ]),
                Some(vec!["i".into()]),
                None,
            )
            .await?;
        assert_eq!(dataset.manifest.max_field_id(), 2);

        dataset.drop_columns(&["b"]).await?;
        // Even though we dropped a column, we still have the fragment with a and
        // b. So it should still act as if that field id is still in play.
        assert_eq!(dataset.manifest.max_field_id(), 2);

        dataset
            .add_columns(
                NewColumnTransform::SqlExpressions(vec![("c".into(), "i + 11".into())]),
                Some(vec!["i".into()]),
                None,
            )
            .await?;
        assert_eq!(dataset.manifest.max_field_id(), 3);

        let data = dataset.scan().try_into_batch().await?;
        let expected_schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("i", DataType::Int32, false),
            ArrowField::new("a", DataType::Int32, false),
            ArrowField::new("c", DataType::Int32, false),
        ]));
        let expected_data = RecordBatch::try_new(
            expected_schema,
            vec![
                Arc::new(Int32Array::from(vec![1, 2])),
                Arc::new(Int32Array::from(vec![4, 5])),
                Arc::new(Int32Array::from(vec![12, 13])),
            ],
        )?;
        assert_eq!(data, expected_data);

        Ok(())
    }

    #[tokio::test]
    async fn test_new_column_sql_to_all_nulls_transform_optimizer() {
        let schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
            "a",
            DataType::Int32,
            false,
        )]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from_iter(0..100))],
        )
        .unwrap();
        let reader = RecordBatchIterator::new(vec![Ok(batch)], schema.clone());
        let test_dir = TempStrDir::default();
        let test_uri = &test_dir;
        let mut dataset = Dataset::write(
            reader,
            test_uri,
            Some(WriteParams {
                max_rows_per_file: 50,
                max_rows_per_group: 25,
                data_storage_version: Some(LanceFileVersion::Stable),
                ..Default::default()
            }),
        )
        .await
        .unwrap();
        dataset.validate().await.unwrap();

        let manifest_before = dataset.manifest.clone();

        // Add all null column
        dataset
            .add_columns(
                NewColumnTransform::SqlExpressions(vec![(
                    "b".to_string(),
                    "CAST(NULL AS int)".to_string(),
                )]),
                None,
                None,
            )
            .await
            .unwrap();
        let manifest_after = dataset.manifest.clone();

        // Check that this is a metadata-only operation (the fragments don't change)
        assert_eq!(&manifest_before.fragments, &manifest_after.fragments);

        // check that the new field was added to the schema
        let expected_schema = ArrowSchema::new(vec![
            ArrowField::new("a", DataType::Int32, false),
            ArrowField::new("b", DataType::Int32, true),
        ]);
        assert_eq!(ArrowSchema::from(dataset.schema()), expected_schema);
    }

    #[tokio::test]
    async fn test_new_column_sql_to_all_nulls_transform_optimizer_legacy() {
        let schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
            "a",
            DataType::Int32,
            false,
        )]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from_iter(0..100))],
        )
        .unwrap();
        let reader = RecordBatchIterator::new(vec![Ok(batch)], schema.clone());
        let test_dir = TempStrDir::default();
        let test_uri = &test_dir;
        let mut dataset = Dataset::write(
            reader,
            test_uri,
            Some(WriteParams {
                max_rows_per_file: 50,
                max_rows_per_group: 25,
                data_storage_version: Some(LanceFileVersion::Legacy),
                ..Default::default()
            }),
        )
        .await
        .unwrap();
        dataset.validate().await.unwrap();

        // Add all null column ...
        // This is basically a smoke test to ensure we don't try to use the all-nulls
        // transform optimizer where it's not supported, and then blow up when we try
        // to apply the transform
        dataset
            .add_columns(
                NewColumnTransform::SqlExpressions(vec![(
                    "b".to_string(),
                    "CAST(NULL AS int)".to_string(),
                )]),
                None,
                None,
            )
            .await
            .unwrap();

        // check that the new field was added to the schema
        let expected_schema = ArrowSchema::new(vec![
            ArrowField::new("a", DataType::Int32, false),
            ArrowField::new("b", DataType::Int32, true),
        ]);
        assert_eq!(ArrowSchema::from(dataset.schema()), expected_schema);
    }

    #[test]
    fn test_check_field_conflict() {
        // same struct
        let field1 = ArrowField::new(
            "test",
            DataType::Struct(vec![ArrowField::new("a", DataType::Int32, false)].into()),
            false,
        );
        let field2 = ArrowField::new(
            "test",
            DataType::Struct(vec![ArrowField::new("a", DataType::Int32, false)].into()),
            false,
        );
        assert!(check_field_conflict(&field1, &field2, &LanceFileVersion::V2_2).is_err());

        // different struct
        let field1 = ArrowField::new(
            "test",
            DataType::Struct(vec![ArrowField::new("a", DataType::Int32, false)].into()),
            false,
        );
        let field2 = ArrowField::new(
            "test",
            DataType::Struct(vec![ArrowField::new("b", DataType::Int32, false)].into()),
            false,
        );
        assert!(check_field_conflict(&field1, &field2, &LanceFileVersion::V2_2).is_ok());

        // same nested struct
        let inner_struct1 = ArrowField::new(
            "inner",
            DataType::Struct(vec![ArrowField::new("x", DataType::Int32, false)].into()),
            false,
        );
        let inner_struct2 = ArrowField::new(
            "inner",
            DataType::Struct(vec![ArrowField::new("x", DataType::Int32, false)].into()),
            false,
        );
        let field1 = ArrowField::new("test", DataType::Struct(vec![inner_struct1].into()), false);
        let field2 = ArrowField::new("test", DataType::Struct(vec![inner_struct2].into()), false);
        assert!(check_field_conflict(&field1, &field2, &LanceFileVersion::V2_2).is_err());

        // basic type with different name
        let field1 = ArrowField::new("test1", DataType::Int32, false);
        let field2 = ArrowField::new("test2", DataType::Int32, false);
        assert!(check_field_conflict(&field1, &field2, &LanceFileVersion::V2_2).is_ok());

        // basic type with same name
        let field1 = ArrowField::new("test", DataType::Int32, false);
        let field2 = ArrowField::new("test", DataType::Int32, false);
        assert!(check_field_conflict(&field1, &field2, &LanceFileVersion::V2_2).is_err());

        // different basic type
        let field1 = ArrowField::new("test", DataType::Int32, false);
        let field2 = ArrowField::new("test", DataType::Float64, false);
        assert!(check_field_conflict(&field1, &field2, &LanceFileVersion::V2_2).is_err());

        // partial conflict
        let field1 = ArrowField::new(
            "test",
            DataType::Struct(
                vec![
                    ArrowField::new("a", DataType::Int32, false),
                    ArrowField::new("b", DataType::Utf8, false),
                ]
                .into(),
            ),
            false,
        );
        let field2 = ArrowField::new(
            "test",
            DataType::Struct(
                vec![
                    ArrowField::new("a", DataType::Int32, false),
                    ArrowField::new("c", DataType::Utf8, false),
                ]
                .into(),
            ),
            false,
        );
        assert!(check_field_conflict(&field1, &field2, &LanceFileVersion::V2_2).is_err());

        // same list
        let field1 = ArrowField::new(
            "test",
            DataType::List(Arc::new(ArrowField::new("item", DataType::Int32, true))),
            false,
        );
        let field2 = ArrowField::new(
            "test",
            DataType::List(Arc::new(ArrowField::new("item", DataType::Int32, true))),
            false,
        );
        assert!(check_field_conflict(&field1, &field2, &LanceFileVersion::V2_2).is_err());

        // list with struct
        let field1 = ArrowField::new(
            "test",
            DataType::List(Arc::new(ArrowField::new(
                "item",
                DataType::Struct(vec![ArrowField::new("a", DataType::Int32, false)].into()),
                false,
            ))),
            false,
        );
        let field2 = ArrowField::new(
            "test",
            DataType::List(Arc::new(ArrowField::new(
                "item",
                DataType::Struct(vec![ArrowField::new("a", DataType::Int32, false)].into()),
                false,
            ))),
            false,
        );
        assert!(check_field_conflict(&field1, &field2, &LanceFileVersion::V2_2).is_err());

        // list with different struct
        let field1 = ArrowField::new(
            "test",
            DataType::List(Arc::new(ArrowField::new(
                "item",
                DataType::Struct(vec![ArrowField::new("a", DataType::Int32, false)].into()),
                false,
            ))),
            false,
        );
        let field2 = ArrowField::new(
            "test",
            DataType::List(Arc::new(ArrowField::new(
                "item",
                DataType::Struct(vec![ArrowField::new("b", DataType::Int32, false)].into()),
                false,
            ))),
            false,
        );
        assert!(check_field_conflict(&field1, &field2, &LanceFileVersion::V2_2).is_ok());

        // list of struct and basic
        let field1 = ArrowField::new(
            "test",
            DataType::List(Arc::new(ArrowField::new(
                "item",
                DataType::Struct(vec![ArrowField::new("a", DataType::Int32, false)].into()),
                false,
            ))),
            false,
        );
        let field2 = ArrowField::new(
            "test",
            DataType::List(Arc::new(ArrowField::new("item", DataType::Int32, true))),
            false,
        );
        assert!(check_field_conflict(&field1, &field2, &LanceFileVersion::V2_2).is_err());

        // FixedSizeList with struct
        let field1 = ArrowField::new(
            "test",
            DataType::FixedSizeList(
                Arc::new(ArrowField::new(
                    "item",
                    DataType::Struct(vec![ArrowField::new("a", DataType::Int32, false)].into()),
                    false,
                )),
                2,
            ),
            false,
        );
        let field2 = ArrowField::new(
            "test",
            DataType::FixedSizeList(
                Arc::new(ArrowField::new(
                    "item",
                    DataType::Struct(vec![ArrowField::new("a", DataType::Int32, false)].into()),
                    false,
                )),
                2,
            ),
            false,
        );
        assert!(check_field_conflict(&field1, &field2, &LanceFileVersion::V2_2).is_err());

        // FixedSizeList with different struct
        let field1 = ArrowField::new(
            "test",
            DataType::FixedSizeList(
                Arc::new(ArrowField::new(
                    "item",
                    DataType::Struct(vec![ArrowField::new("a", DataType::Int32, false)].into()),
                    false,
                )),
                2,
            ),
            false,
        );
        let field2 = ArrowField::new(
            "test",
            DataType::FixedSizeList(
                Arc::new(ArrowField::new(
                    "item",
                    DataType::Struct(vec![ArrowField::new("b", DataType::Int32, false)].into()),
                    false,
                )),
                2,
            ),
            false,
        );
        assert!(check_field_conflict(&field1, &field2, &LanceFileVersion::V2_2).is_ok());

        // LargeList with struct
        let field1 = ArrowField::new(
            "test",
            DataType::LargeList(Arc::new(ArrowField::new(
                "item",
                DataType::Struct(vec![ArrowField::new("a", DataType::Int32, false)].into()),
                false,
            ))),
            false,
        );
        let field2 = ArrowField::new(
            "test",
            DataType::LargeList(Arc::new(ArrowField::new(
                "item",
                DataType::Struct(vec![ArrowField::new("a", DataType::Int32, false)].into()),
                false,
            ))),
            false,
        );
        assert!(check_field_conflict(&field1, &field2, &LanceFileVersion::V2_2).is_err());

        // LargeList with different struct
        let field1 = ArrowField::new(
            "test",
            DataType::LargeList(Arc::new(ArrowField::new(
                "item",
                DataType::Struct(vec![ArrowField::new("a", DataType::Int32, false)].into()),
                false,
            ))),
            false,
        );
        let field2 = ArrowField::new(
            "test",
            DataType::LargeList(Arc::new(ArrowField::new(
                "item",
                DataType::Struct(vec![ArrowField::new("b", DataType::Int32, false)].into()),
                false,
            ))),
            false,
        );
        assert!(check_field_conflict(&field1, &field2, &LanceFileVersion::V2_2).is_ok());

        // packed struct
        let mut packed_meta = HashMap::new();
        packed_meta.insert(PACKED_STRUCT_META_KEY.to_string(), "true".to_string());

        let packed_field = ArrowField::new(
            "packed",
            DataType::Struct(vec![ArrowField::new("foo", DataType::Int32, false)].into()),
            false,
        )
        .with_metadata(packed_meta.clone());

        let field1 = ArrowField::new("test", DataType::Struct(vec![packed_field].into()), false);
        let field2 = ArrowField::new(
            "test",
            DataType::Struct(vec![ArrowField::new("b", DataType::Int32, false)].into()),
            false,
        );
        assert!(check_field_conflict(&field1, &field2, &LanceFileVersion::V2_2).is_ok());

        let new_packed_field = ArrowField::new(
            "new_packed",
            DataType::Struct(vec![ArrowField::new("foo", DataType::Int32, false)].into()),
            false,
        )
        .with_metadata(packed_meta.clone());
        let field3 = ArrowField::new(
            "test",
            DataType::Struct(vec![new_packed_field].into()),
            false,
        );
        assert!(check_field_conflict(&field1, &field3, &LanceFileVersion::V2_2).is_ok());

        let conflict_field = ArrowField::new(
            "packed",
            DataType::Struct(vec![ArrowField::new("new_col", DataType::Int32, false)].into()),
            false,
        )
        .with_metadata(packed_meta);
        let field4 = ArrowField::new("test", DataType::Struct(vec![conflict_field].into()), false);
        assert!(check_field_conflict(&field1, &field4, &LanceFileVersion::V2_2).is_err());
    }
}
