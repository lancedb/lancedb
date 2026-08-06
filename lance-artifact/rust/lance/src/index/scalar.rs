// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Utilities for integrating scalar indices with datasets
//!

pub(crate) mod bitmap;
pub(crate) mod bloomfilter;
pub(crate) mod btree;
pub(crate) mod fmindex;
pub(crate) mod inverted;
pub(crate) mod label_list;
pub(crate) mod ngram;
#[cfg(feature = "geo")]
pub(crate) mod rtree;
pub(crate) mod zonemap;

pub use inverted::{load_segment_details, load_segment_params, load_segments};

pub use crate::index::scalar_logical::{LogicalScalarIndex, load_named_scalar_segments};

use std::sync::{Arc, LazyLock};

use uuid::Uuid;

use crate::index::DatasetIndexExt;
use crate::index::DatasetIndexInternalExt;
use crate::session::index_caches::ProstAny;
use crate::{
    Dataset,
    dataset::{index::LanceIndexStoreExt, scanner::ColumnOrdering},
};
use arrow_schema::DataType;
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use futures::TryStreamExt;
use itertools::Itertools;
use lance_core::datatypes::Field;
use lance_core::utils::tracing::{IO_TYPE_OPEN_SCALAR, TRACE_IO_EVENTS};
use lance_core::{Error, ROW_ADDR, ROW_ID, Result};
use lance_datafusion::exec::LanceExecutionOptions;
use lance_index::frag_reuse::FragReuseIndexHandle;
use lance_index::metrics::{MetricsCollector, NoOpMetricsCollector};
use lance_index::pbold::{
    BTreeIndexDetails, BitmapIndexDetails, InvertedIndexDetails, LabelListIndexDetails,
};
use lance_index::progress::IndexBuildProgress;
use lance_index::registry::IndexPluginRegistry;
use lance_index::scalar::IndexStore;
use lance_index::scalar::inverted::METADATA_FILE;
use lance_index::scalar::label_list::{
    LABEL_LIST_NULLS_METADATA_KEY, LABEL_LIST_NULLS_MIN_VERSION,
};
use lance_index::scalar::registry::{
    ScalarIndexLoad, ScalarIndexPlugin, TrainingCriteria, TrainingOrdering, VALUE_COLUMN_NAME,
};
use lance_index::scalar::{BuiltinIndexType, CreatedIndex, InvertedIndexParams};
use lance_index::scalar::{
    RowIdRemapper, ScalarIndex, ScalarIndexParams, bitmap::BITMAP_LOOKUP_NAME,
    inverted::INVERT_LIST_FILE, lance_format::LanceIndexStore,
};
use lance_index::{IndexCriteria, IndexType};
use lance_table::format::{Fragment, IndexMetadata};
use log::info;
use prost::Message;
use tracing::instrument;

// Log an update every TRAINING_UPDATE_FREQ million rows processed
const TRAINING_UPDATE_FREQ: usize = 1000000;

pub(crate) struct TrainingRequest {
    pub fragment_ids: Option<Vec<u32>>,
}

impl TrainingRequest {
    pub fn with_fragment_ids(
        _dataset: Arc<Dataset>,
        _column: String,
        fragment_ids: Vec<u32>,
    ) -> Self {
        Self {
            fragment_ids: Some(fragment_ids),
        }
    }

    async fn create_empty_stream(
        dataset: &Dataset,
        column: &str,
        criteria: &TrainingCriteria,
    ) -> Result<SendableRecordBatchStream> {
        let column_field = dataset
            .schema()
            .field(column)
            .ok_or(Error::invalid_input_source(
                format!("No column with name {}", column).into(),
            ))?;

        let mut fields = Vec::with_capacity(3);
        fields.push(arrow_schema::Field::new(
            VALUE_COLUMN_NAME,
            column_field.data_type(),
            true,
        ));
        if criteria.needs_row_ids {
            fields.push(arrow_schema::Field::new(
                ROW_ID,
                arrow_schema::DataType::UInt64,
                false,
            ));
        }
        if criteria.needs_row_addrs {
            fields.push(arrow_schema::Field::new(
                ROW_ADDR,
                arrow_schema::DataType::UInt64,
                false,
            ));
        }

        // Create schema with the column and row_id field (matching scan_chunks behavior)
        let schema = Arc::new(arrow_schema::Schema::new(fields));

        // Create empty stream
        let empty_stream = futures::stream::empty();
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            schema,
            empty_stream,
        )))
    }
}

pub(crate) async fn scan_training_data(
    dataset: &Dataset,
    column: &str,
    criteria: &TrainingCriteria,
    fragments: Option<Vec<Fragment>>,
) -> Result<SendableRecordBatchStream> {
    let num_rows = dataset.count_all_rows().await?;

    let mut scan = dataset.scan();
    // Fragment filtering is now handled in load_training_data function
    // This function just processes the fragments passed to it

    // Note: we don't need to sort for TrainingOrdering::Addresses because
    // Lance will return data in the order of the row_address by default.
    if TrainingOrdering::Values == criteria.ordering {
        scan.order_by(Some(vec![ColumnOrdering::asc_nulls_first(
            column.to_string(),
        )]))?;
    }

    if criteria.needs_row_ids {
        scan.with_row_id();
    }
    if criteria.needs_row_addrs {
        scan.with_row_address();
    }

    scan.project_with_transform(&[(VALUE_COLUMN_NAME, column)])?;

    if let Some(fragments) = fragments {
        scan.with_fragments(fragments);
    }

    let batches = scan
        .try_into_dfstream(LanceExecutionOptions {
            use_spilling: true,
            ..Default::default()
        })
        .await?;

    let schema = batches.schema();
    let mut rows_processed = 0;
    let mut next_update = TRAINING_UPDATE_FREQ;
    let training_uuid = uuid::Uuid::new_v4().to_string();
    info!(
        "Starting index training job with id {} on column {}",
        training_uuid, column
    );
    info!("Training index (job_id={}): 0/{}", training_uuid, num_rows);
    let batches = batches.map_ok(move |batch| {
        rows_processed += batch.num_rows();
        if rows_processed >= next_update {
            next_update += TRAINING_UPDATE_FREQ;
            info!(
                "Training index (job_id={}): {}/{}",
                training_uuid, rows_processed, num_rows
            );
        }
        batch
    });

    Ok(Box::pin(RecordBatchStreamAdapter::new(schema, batches)))
}

pub(crate) async fn load_training_data(
    dataset: &Dataset,
    column: &str,
    criteria: &TrainingCriteria,
    fragments: Option<Vec<Fragment>>,
    train: bool,
    fragment_ids: Option<Vec<u32>>,
) -> Result<SendableRecordBatchStream> {
    // Create training request with fragment_ids if provided
    let training_request = Box::new(match fragment_ids.clone() {
        Some(fragment_ids) => TrainingRequest::with_fragment_ids(
            Arc::new(dataset.clone()),
            column.to_string(),
            fragment_ids,
        ),
        None => TrainingRequest { fragment_ids: None },
    });

    if train {
        // Use the training request to scan data with fragment filtering
        if let Some(ref fragment_ids) = training_request.fragment_ids {
            let fragment_ids = fragment_ids
                .clone()
                .into_iter()
                .sorted()
                .dedup()
                .collect_vec();
            let frags = dataset.get_frags_from_ordered_ids(&fragment_ids);
            let frags: Result<Vec<_>> = fragment_ids
                .iter()
                .zip(frags)
                .map(|(id, frag)| {
                    let Some(frag) = frag else {
                        return Err(Error::invalid_input_source(
                            format!("No fragment with id {}", id).into(),
                        ));
                    };
                    Ok(frag.metadata().clone())
                })
                .collect();
            scan_training_data(dataset, column, criteria, Some(frags?)).await
        } else {
            scan_training_data(dataset, column, criteria, fragments).await
        }
    } else {
        TrainingRequest::create_empty_stream(dataset, column, criteria).await
    }
}

pub(crate) async fn load_fts_training_data(
    dataset: &Dataset,
    resolved: &inverted::ResolvedFtsField,
    criteria: &TrainingCriteria,
    fragments: Option<Vec<Fragment>>,
    train: bool,
    fragment_ids: Option<Vec<u32>>,
) -> Result<SendableRecordBatchStream> {
    let scan_column = if resolved.has_lists() {
        resolved.root_column.as_str()
    } else {
        resolved.canonical_path.as_str()
    };
    let stream = load_training_data(
        dataset,
        scan_column,
        criteria,
        fragments,
        train,
        fragment_ids,
    )
    .await?;
    if resolved.has_lists() {
        inverted::transform_fts_document_stream(stream, resolved.clone())
    } else {
        Ok(stream)
    }
}

// TODO: Allow users to register their own plugins
static SCALAR_INDEX_PLUGIN_REGISTRY: LazyLock<Arc<IndexPluginRegistry>> =
    LazyLock::new(IndexPluginRegistry::with_default_plugins);

pub struct IndexDetails(pub Arc<prost_types::Any>);

impl IndexDetails {
    /// Returns true if the index is a vector index
    pub fn is_vector(&self) -> bool {
        self.0.type_url.ends_with("VectorIndexDetails")
    }

    /// Returns true if the index supports FTS
    pub fn supports_fts(&self) -> bool {
        // In the future this may need to change if we want FTS indices to be pluggable
        self.0.type_url.ends_with("InvertedIndexDetails")
    }

    /// Returns the plugin for the index
    pub fn get_plugin(&self) -> Result<&dyn ScalarIndexPlugin> {
        SCALAR_INDEX_PLUGIN_REGISTRY.get_plugin_by_details(self.0.as_ref())
    }

    /// Returns the index version
    pub fn index_version(&self) -> Result<u32> {
        if self.is_vector() {
            // VectorIndexDetails currently does not include the concrete vector
            // subtype (IVF_PQ / IVF_RQ / ...), so compatibility filtering cannot
            // do per-subtype version checks here. Use the highest supported
            // vector index version as a safe upper bound; older binaries still
            // ignore newer indices based on their own lower bound.
            Ok(IndexType::max_vector_version())
        } else {
            self.get_plugin().map(|p| p.version())
        }
    }
}

/// Build a Scalar Index (returns details to store in the manifest)
#[allow(clippy::too_many_arguments)]
#[instrument(level = "debug", skip_all)]
pub(super) async fn build_scalar_index(
    dataset: &Dataset,
    column: &str,
    uuid: Uuid,
    params: &ScalarIndexParams,
    train: bool,
    fragment_ids: Option<Vec<u32>>,
    preprocessed_data: Option<SendableRecordBatchStream>,
    progress: Arc<dyn IndexBuildProgress>,
) -> Result<CreatedIndex> {
    let inverted_params = (params.index_type.eq_ignore_ascii_case("inverted")
        || params.index_type.eq_ignore_ascii_case("fts"))
    .then(|| serde_json::from_str::<InvertedIndexParams>(params.params.as_deref().unwrap_or("{}")))
    .transpose()?;
    let resolved_fts_field = inverted_params
        .as_ref()
        .map(|params| {
            inverted::resolve_fts_field(dataset.schema(), column, params.get_document_granularity())
        })
        .transpose()?;
    let field = if let Some(resolved) = &resolved_fts_field {
        let source = dataset
            .schema()
            .field_by_id(resolved.final_field_id)
            .ok_or_else(|| {
                Error::invalid_input(format!(
                    "FTS field id {} is missing from the dataset schema",
                    resolved.final_field_id
                ))
            })?;
        if resolved.has_lists() {
            arrow_schema::Field::new(VALUE_COLUMN_NAME, DataType::Utf8, true)
        } else {
            source.into()
        }
    } else {
        let field = dataset
            .schema()
            .field(column)
            .ok_or(Error::invalid_input_source(
                format!("No column with name {}", column).into(),
            ))?;
        field.into()
    };

    let index_store = LanceIndexStore::from_dataset_for_new(dataset, &uuid)?;

    let plugin = SCALAR_INDEX_PLUGIN_REGISTRY.get_plugin_by_name(&params.index_type)?;
    let trainer = plugin.basic_trainer().ok_or_else(|| {
        Error::invalid_input_source(
            format!("The '{}' index type does not support basic training, please refer to the index's documentation for more details on how to create this index.", params.index_type).into(),
        )
    })?;
    let training_request =
        trainer.new_training_request(params.params.as_deref().unwrap_or("{}"), &field)?;

    progress.stage_start("load_data", None, "rows").await?;
    let training_data = match (preprocessed_data, resolved_fts_field.as_ref()) {
        (Some(preprocessed_data), _) => preprocessed_data,
        (None, Some(resolved)) => {
            load_fts_training_data(
                dataset,
                resolved,
                training_request.criteria(),
                None,
                train,
                fragment_ids.clone(),
            )
            .await?
        }
        (None, None) => {
            load_training_data(
                dataset,
                column,
                training_request.criteria(),
                None,
                train,
                fragment_ids.clone(),
            )
            .await?
        }
    };
    progress.stage_complete("load_data").await?;

    let created_index = trainer
        .train_index(
            training_data,
            &index_store,
            training_request,
            fragment_ids,
            progress,
        )
        .await?;

    Ok(created_index)
}

/// Build a canonical bitmap index segment over a caller-selected fragment set.
///
/// This is intentionally separate from `build_scalar_index(..., fragment_ids=Some(...))`.
/// The latter is the legacy distributed scalar-index shard path. Here fragment ids only
/// restrict the scanned rows; the bitmap plugin receives no shard id and writes the
/// canonical bitmap layout for the staged segment root.
#[instrument(level = "debug", skip_all)]
pub(super) async fn build_bitmap_index_segment(
    dataset: &Dataset,
    column: &str,
    uuid: Uuid,
    fragment_ids: Vec<u32>,
    progress: Arc<dyn IndexBuildProgress>,
) -> Result<CreatedIndex> {
    let field = dataset
        .schema()
        .field(column)
        .ok_or(Error::invalid_input_source(
            format!("No column with name {}", column).into(),
        ))?;
    let field: arrow_schema::Field = field.into();

    let params = ScalarIndexParams::for_builtin(BuiltinIndexType::Bitmap);
    let plugin = SCALAR_INDEX_PLUGIN_REGISTRY.get_plugin_by_name(&params.index_type)?;
    let trainer = plugin.basic_trainer().ok_or_else(|| {
        Error::invalid_input_source(
            format!("The '{}' index type does not support basic training, please refer to the index's documentation for more details on how to create this index.", params.index_type).into(),
        )
    })?;
    let training_request =
        trainer.new_training_request(params.params.as_deref().unwrap_or("{}"), &field)?;
    let criteria = training_request.criteria();

    progress.stage_start("load_data", None, "rows").await?;
    let training_data =
        load_training_data(dataset, column, criteria, None, true, Some(fragment_ids)).await?;
    progress.stage_complete("load_data").await?;

    let index_store = LanceIndexStore::from_dataset_for_new(dataset, &uuid)?;
    trainer
        .train_index(
            training_data,
            &index_store,
            training_request,
            None,
            progress,
        )
        .await
}

/// Fetches the scalar index plugin for a given index metadata
///
/// The fast path, on newer datasets, is just a plugin lookup by the type URL of the index details.
///
/// If the index details are missing (older dataset) then we need to look at the files present in the
/// index directory to guess the index type.
pub async fn fetch_index_details(
    dataset: &Dataset,
    column: &str,
    index: &IndexMetadata,
) -> Result<Arc<prost_types::Any>> {
    let index_details = match index.index_details.as_ref() {
        Some(details) => details.clone(),
        None => infer_scalar_index_details(dataset, column, index).await?,
    };

    if index_details.type_url.ends_with("InvertedIndexDetails") {
        let details =
            InvertedIndexDetails::decode(index_details.value.as_slice()).map_err(|err| {
                Error::io(format!(
                    "failed to decode InvertedIndexDetails payload: {err}"
                ))
            })?;
        let details = inverted::normalize_inverted_details(index, details)?;
        return Ok(Arc::new(prost_types::Any::from_msg(&details).map_err(
            |err| {
                Error::io(format!(
                    "failed to encode InvertedIndexDetails payload: {err}"
                ))
            },
        )?));
    }

    Ok(index_details)
}

async fn validate_label_list_index_compatibility(
    dataset: &Dataset,
    column: &str,
    index: &IndexMetadata,
    index_store: &Arc<LanceIndexStore>,
) -> Result<()> {
    let Some(field) = dataset.schema().field(column) else {
        return Ok(());
    };

    if !field.nullable {
        return Ok(());
    }

    if index.index_version < LABEL_LIST_NULLS_MIN_VERSION {
        log::warn!(
            "LabelList index {} is old; NOT filters may be incorrect on nullable lists. Consider rebuilding.",
            index.name
        );
        return Ok(());
    }

    let reader = index_store.open_index_file(BITMAP_LOOKUP_NAME).await?;
    if !reader
        .schema()
        .metadata
        .contains_key(LABEL_LIST_NULLS_METADATA_KEY)
    {
        return Err(Error::internal(format!(
            "LabelList index {} is missing required metadata key {}",
            index.name, LABEL_LIST_NULLS_METADATA_KEY
        )));
    }

    Ok(())
}

pub async fn open_scalar_index(
    dataset: &Dataset,
    column: &str,
    index: &IndexMetadata,
    metrics: &dyn MetricsCollector,
) -> Result<Arc<dyn ScalarIndex>> {
    let index_uuid = index.uuid;
    let index_store = Arc::new(LanceIndexStore::from_dataset_for_existing(dataset, index).await?);

    let index_details = fetch_index_details(dataset, column, index).await?;
    let plugin = SCALAR_INDEX_PLUGIN_REGISTRY.get_plugin_by_details(index_details.as_ref())?;

    let frag_reuse_index = dataset.open_frag_reuse_index(metrics).await?;

    let index_cache = dataset
        .index_cache
        .for_index(&index.uuid, frag_reuse_index.as_ref().map(|f| &f.uuid));

    let frag_reuse_index: Option<Arc<dyn RowIdRemapper>> =
        frag_reuse_index.map(|f| Arc::new(FragReuseIndexHandle(f)) as Arc<dyn RowIdRemapper>);

    // Runs only on a cold miss, and at most once even under concurrent opens
    // (the plugin coalesces). The compat check lives here because a warm hit was
    // already validated this session, saving the extra `open_index_file` IOP.
    let load: ScalarIndexLoad = Box::pin({
        let index_store = index_store.clone();
        let frag_reuse_index = frag_reuse_index.clone();
        let index_cache = index_cache.clone();
        async move {
            if index_details.type_url.ends_with("LabelListIndexDetails") {
                validate_label_list_index_compatibility(dataset, column, index, &index_store)
                    .await?;
            }

            let index = plugin
                .load_index(index_store, &index_details, frag_reuse_index, &index_cache)
                .await?;

            tracing::info!(target: TRACE_IO_EVENTS, index_uuid = %index_uuid, r#type = IO_TYPE_OPEN_SCALAR, index_type = index.index_type().to_string());
            metrics.record_index_load();
            Ok(index)
        }
    });

    plugin
        .get_or_insert_in_cache(index_store, frag_reuse_index, &index_cache, load)
        .await
}

pub(crate) async fn infer_scalar_index_details(
    dataset: &Dataset,
    column: &str,
    index: &IndexMetadata,
) -> Result<Arc<prost_types::Any>> {
    let type_key = crate::session::index_caches::ScalarIndexDetailsKey { uuid: &index.uuid };
    if let Some(index_details) = dataset.index_cache.get_with_key(&type_key).await {
        return Ok(index_details.0.clone());
    }

    let index_dir = dataset
        .indice_files_dir(index)?
        .join(index.uuid.to_string());
    let col = dataset
        .schema()
        .field(column)
        .ok_or(Error::internal(format!(
            "Index refers to column {} which does not exist in dataset schema",
            column
        )))?;

    let bitmap_page_lookup = index_dir.clone().join(BITMAP_LOOKUP_NAME);
    let inverted_list_lookup = index_dir.clone().join(METADATA_FILE);
    let legacy_inverted_list_lookup = index_dir.clone().join(INVERT_LIST_FILE);
    let object_store = dataset.object_store_for_index(index).await?;
    let index_details = if object_store.exists(&inverted_list_lookup).await? {
        // Try to infer inverted index details from metadata file to capture with_position and other params
        // Fall back to defaults if anything goes wrong
        let default_details = prost_types::Any::from_msg(&InvertedIndexDetails::default()).unwrap();
        let parse_params = async || {
            let index_store = LanceIndexStore::from_dataset_for_existing(dataset, index)
                .await
                .ok()?;
            let reader = index_store.open_index_file(METADATA_FILE).await.ok()?;
            let params_str = reader.schema().metadata.get("params")?;
            let params = ::serde_json::from_str::<InvertedIndexParams>(params_str).ok()?;
            let details = InvertedIndexDetails::try_from(&params).ok()?;
            Some(prost_types::Any::from_msg(&details).unwrap())
        };
        parse_params().await.unwrap_or(default_details)
    } else if object_store.exists(&legacy_inverted_list_lookup).await? {
        prost_types::Any::from_msg(&InvertedIndexDetails::default()).unwrap()
    } else if let DataType::List(_) | DataType::LargeList(_) = col.data_type() {
        prost_types::Any::from_msg(&LabelListIndexDetails::default()).unwrap()
    } else if object_store.exists(&bitmap_page_lookup).await? {
        prost_types::Any::from_msg(&BitmapIndexDetails::default()).unwrap()
    } else {
        prost_types::Any::from_msg(&BTreeIndexDetails::default()).unwrap()
    };

    let index_details = Arc::new(index_details);
    let prost_any = Arc::new(ProstAny(index_details.clone()));

    dataset
        .index_cache
        .insert_with_key(&type_key, prost_any)
        .await;
    Ok(index_details)
}

pub fn index_matches_criteria(
    index: &IndexMetadata,
    criteria: &IndexCriteria,
    fields: &[&Field],
    has_multiple_indices: bool,
    schema: &lance_core::datatypes::Schema,
) -> Result<bool> {
    if let Some(name) = &criteria.has_name
        && &index.name != name
    {
        return Ok(false);
    }

    if let Some(for_column) = criteria.for_column {
        if index.fields.len() != 1 {
            return Ok(false);
        }
        if fields.len() != 1 {
            // This should be unreachable since we just verified index.fields.len() == 1 but
            // return false just in case
            return Ok(false);
        }
        let is_fts_index = index
            .index_details
            .as_ref()
            .is_some_and(|details| details.type_url.ends_with("InvertedIndexDetails"));
        if criteria.must_support_fts && is_fts_index {
            let requested_granularity = criteria
                .fts_document_granularity
                .unwrap_or(lance_index::scalar::inverted::DocumentGranularity::Row);
            let requested = inverted::resolve_fts_field(schema, for_column, requested_granularity)?;
            if index.fields[0] != requested.final_field_id {
                return Ok(false);
            }
            let Some(details_any) = index.index_details.as_ref() else {
                return Ok(false);
            };
            let details =
                InvertedIndexDetails::decode(details_any.value.as_slice()).map_err(|err| {
                    Error::io(format!(
                        "failed to decode InvertedIndexDetails payload: {err}"
                    ))
                })?;
            let details = inverted::normalize_inverted_details(index, details)?;
            let stored_granularity = lance_index::scalar::inverted::DocumentGranularity::try_from(
                details.document_granularity,
            )?;
            if stored_granularity != requested_granularity {
                return Ok(false);
            }
        } else {
            let field = fields[0];
            // Build the full field path for nested fields
            let field_path = if let Some(ancestors) = schema.field_ancestry_by_id(field.id) {
                let field_refs: Vec<&str> = ancestors.iter().map(|f| f.name.as_str()).collect();
                lance_core::datatypes::format_field_path(&field_refs)
            } else {
                field.name.clone()
            };
            if for_column != field_path {
                return Ok(false);
            }
        }
    }

    let index_details = index.index_details.clone().map(IndexDetails);
    let Some(index_details) = index_details else {
        if has_multiple_indices {
            return Err(Error::invalid_input_source(format!(
                "An index {} on the field with id {} co-exists with other indices on the same column but was written with an older Lance version, and this is not supported.  Please retrain this index.",
                index.name,
                index.fields.first().unwrap_or(&0),
            ).into()));
        }

        // If we don't have details then allow it for backwards compatibility
        return Ok(true);
    };

    // Only apply scalar-specific checks to scalar indices
    if !index_details.is_vector() {
        if criteria.must_support_fts && !index_details.supports_fts() {
            return Ok(false);
        }

        // We should not use FTS / NGram indices for exact equality queries
        // (i.e. merge insert with a join on the indexed column)
        if criteria.must_support_exact_equality {
            let plugin = index_details.get_plugin()?;
            if !plugin.provides_exact_answer() {
                return Ok(false);
            }
        }
    }
    Ok(true)
}

/// Initialize a scalar index from a source dataset
pub async fn initialize_scalar_index(
    target_dataset: &mut Dataset,
    source_dataset: &Dataset,
    source_index: &IndexMetadata,
    field_names: &[&str],
) -> Result<()> {
    if field_names.is_empty() || field_names.len() > 1 {
        return Err(Error::index(format!(
            "Unsupported fields for scalar index: {:?}",
            field_names
        )));
    }

    // Scalar indices currently support only single fields, use the first one
    let column_name = field_names[0];

    let source_scalar_index = source_dataset
        .open_scalar_index(column_name, &source_index.uuid, &NoOpMetricsCollector)
        .await?;

    let params = source_scalar_index.derive_index_params()?;
    let index_type = source_scalar_index.index_type();

    // For Inverted index, we need to parse the params JSON and create InvertedIndexParams
    if index_type == IndexType::Inverted {
        // Extract the JSON string from ScalarIndexParams
        let params_json = params
            .params
            .as_ref()
            .ok_or_else(|| Error::index("Inverted index params missing".to_string()))?;

        // Parse the JSON into InvertedIndexParams
        let inverted_params: InvertedIndexParams = serde_json::from_str(params_json)?;
        let resolved = inverted::resolve_fts_field_by_id(
            target_dataset.schema(),
            *source_index.fields.first().ok_or_else(|| {
                Error::index("Inverted index metadata has no indexed field".to_string())
            })?,
            inverted_params.get_document_granularity(),
        )?;

        target_dataset
            .create_index(
                &[&resolved.canonical_path],
                index_type,
                Some(source_index.name.clone()),
                &inverted_params,
                false,
            )
            .await?;
    } else {
        target_dataset
            .create_index(
                &[column_name],
                index_type,
                Some(source_index.name.clone()),
                &params,
                false,
            )
            .await?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::utils::test::{DatagenExt, FragmentCount, FragmentRowCount};

    use super::*;
    use crate::dataset::Dataset;
    use arrow::{
        array::AsArray,
        datatypes::{Int32Type, UInt64Type},
    };
    use arrow_schema::DataType;
    use futures::TryStreamExt;
    use lance_core::utils::tempfile::TempStrDir;
    use lance_core::{datatypes::Field, utils::address::RowAddress};
    use lance_datagen::array;
    use lance_index::pb::VectorIndexDetails;
    use lance_index::{IndexType, optimize::OptimizeOptions};
    use lance_index::{
        pbold::NGramIndexDetails,
        scalar::{BuiltinIndexType, ScalarIndexParams},
    };

    fn make_index_metadata(
        name: &str,
        field_id: i32,
        index_type: Option<IndexType>,
    ) -> crate::index::IndexMetadata {
        let index_details = index_type
            .map(|index_type| match index_type {
                IndexType::BTree => {
                    prost_types::Any::from_msg(&BTreeIndexDetails::default()).unwrap()
                }
                IndexType::Inverted => {
                    prost_types::Any::from_msg(&InvertedIndexDetails::default()).unwrap()
                }
                IndexType::NGram => {
                    prost_types::Any::from_msg(&NGramIndexDetails::default()).unwrap()
                }
                IndexType::Vector => {
                    prost_types::Any::from_msg(&VectorIndexDetails::default()).unwrap()
                }
                _ => {
                    unimplemented!("unsupported index type: {}", index_type)
                }
            })
            .map(Arc::new);
        crate::index::IndexMetadata {
            uuid: uuid::Uuid::new_v4(),
            name: name.to_string(),
            fields: vec![field_id],
            dataset_version: 1,
            fragment_bitmap: None,
            index_details,
            index_version: 0,
            created_at: None,
            base_id: None,
            files: None,
        }
    }

    #[test]
    fn test_index_matches_criteria_vector_index() {
        let index1 = make_index_metadata("vector_index", 1, Some(IndexType::Vector));

        let criteria = IndexCriteria {
            must_support_fts: false,
            fts_document_granularity: None,
            must_support_exact_equality: false,
            for_column: None,
            has_name: None,
        };

        let field = Field::new_arrow("mycol", DataType::Int32, true).unwrap();
        let schema = lance_core::datatypes::Schema {
            fields: vec![field.clone()],
            metadata: Default::default(),
        };
        // Vector indices should now match basic criteria
        let result = index_matches_criteria(&index1, &criteria, &[&field], true, &schema).unwrap();
        assert!(result);

        let result = index_matches_criteria(&index1, &criteria, &[&field], false, &schema).unwrap();
        assert!(result);
    }

    #[test]
    fn test_index_matches_criteria_scalar_index() {
        let btree_index = make_index_metadata("btree_index", 1, Some(IndexType::BTree));
        let inverted_index = make_index_metadata("inverted_index", 1, Some(IndexType::Inverted));
        let ngram_index = make_index_metadata("ngram_index", 1, Some(IndexType::NGram));

        let criteria = IndexCriteria {
            must_support_fts: false,
            fts_document_granularity: None,
            must_support_exact_equality: false,
            for_column: None,
            has_name: None,
        };

        let field = Field::new_arrow("mycol", DataType::Int32, true).unwrap();
        let schema = lance_core::datatypes::Schema {
            fields: vec![field.clone()],
            metadata: Default::default(),
        };
        let result =
            index_matches_criteria(&btree_index, &criteria, &[&field], true, &schema).unwrap();
        assert!(result);

        let result =
            index_matches_criteria(&btree_index, &criteria, &[&field], false, &schema).unwrap();
        assert!(result);

        // test for_column
        let mut criteria = IndexCriteria {
            must_support_fts: false,
            fts_document_granularity: None,
            must_support_exact_equality: false,
            for_column: Some("mycol"),
            has_name: None,
        };
        let result =
            index_matches_criteria(&btree_index, &criteria, &[&field], false, &schema).unwrap();
        assert!(result);

        criteria.for_column = Some("mycol2");
        let result =
            index_matches_criteria(&btree_index, &criteria, &[&field], false, &schema).unwrap();
        assert!(!result);

        // test has_name
        let mut criteria = IndexCriteria {
            must_support_fts: false,
            fts_document_granularity: None,
            must_support_exact_equality: false,
            for_column: None,
            has_name: Some("btree_index"),
        };
        let result =
            index_matches_criteria(&btree_index, &criteria, &[&field], true, &schema).unwrap();
        assert!(result);
        let result =
            index_matches_criteria(&btree_index, &criteria, &[&field], false, &schema).unwrap();
        assert!(result);

        criteria.has_name = Some("btree_index2");
        let result =
            index_matches_criteria(&btree_index, &criteria, &[&field], true, &schema).unwrap();
        assert!(!result);
        let result =
            index_matches_criteria(&btree_index, &criteria, &[&field], false, &schema).unwrap();
        assert!(!result);

        // test supports_exact_equality
        let mut criteria = IndexCriteria {
            must_support_fts: false,
            fts_document_granularity: None,
            must_support_exact_equality: true,
            for_column: None,
            has_name: None,
        };
        let result =
            index_matches_criteria(&btree_index, &criteria, &[&field], false, &schema).unwrap();
        assert!(result);

        criteria.must_support_fts = true;
        let result =
            index_matches_criteria(&inverted_index, &criteria, &[&field], false, &schema).unwrap();
        assert!(!result);

        criteria.must_support_fts = false;
        let result =
            index_matches_criteria(&ngram_index, &criteria, &[&field], true, &schema).unwrap();
        assert!(!result);

        // test multiple indices
        let mut criteria = IndexCriteria {
            must_support_fts: false,
            fts_document_granularity: None,
            must_support_exact_equality: false,
            for_column: None,
            has_name: None,
        };
        let result =
            index_matches_criteria(&btree_index, &criteria, &[&field], true, &schema).unwrap();
        assert!(result);

        criteria.must_support_fts = true;
        let result =
            index_matches_criteria(&inverted_index, &criteria, &[&field], true, &schema).unwrap();
        assert!(result);

        criteria.must_support_fts = false;
        let result =
            index_matches_criteria(&ngram_index, &criteria, &[&field], true, &schema).unwrap();
        assert!(result);
    }

    /// Regression guard for over-projection of `Map` siblings in
    /// `Field::apply_projection`. Before the parent-selection guard,
    /// every `Map` column in a schema survived every projection because
    /// `apply_projection` cloned `Map` children unconditionally without
    /// checking whether the parent itself was selected. On scalar-index
    /// training scans this turned a single-column projection into a wide
    /// one, ballooning the per-row tuple width fed into `SortExec` and
    /// producing >100 GiB external-sort spills on tables with several
    /// `Map` columns.
    ///
    /// Coverage:
    ///
    /// 1. Plain `Binary` siblings stay narrow — sanity check that
    ///    non-Map schemas weren't affected by the bug.
    /// 2. `Map` siblings stay narrow — the actual regression guard.
    /// 3. The same `Map`-sibling schema scanned via `with_fragments`,
    ///    the path `optimize_indices` uses for delta training scans.
    /// 4. When the caller *does* request a Map column, the Map's
    ///    internal children (entries struct + key/value) are still
    ///    preserved (the original intent of PR #5349).
    #[tokio::test]
    async fn scan_training_data_does_not_pull_unrelated_map_siblings() {
        use arrow_array::types::Int32Type;
        use lance_datagen::ByteCount;
        use lance_file::version::LanceFileVersion;

        const FRAGMENTS: u32 = 2;
        const ROWS_PER_FRAGMENT: u32 = 32;
        const BINARY_BYTES: u64 = 20;

        async fn projection_columns(dataset: &Dataset, column: &str) -> Vec<String> {
            let mut scan = dataset.scan();
            scan.order_by(Some(vec![ColumnOrdering::asc_nulls_first(
                column.to_string(),
            )]))
            .unwrap();
            scan.with_row_id();
            scan.project_with_transform(&[(VALUE_COLUMN_NAME, column)])
                .unwrap();
            let plan = scan.explain_plan(false).await.unwrap();
            // FilteredReadExec's Display emits `projection=[col1, col2, ...]`;
            // pluck the column list out of the line. We do not depend on
            // ordering or whitespace beyond the literal `projection=[` /
            // `]` markers so the assertion stays robust to unrelated plan
            // formatting changes.
            let line = plan
                .lines()
                .find(|l| l.contains("projection=["))
                .unwrap_or_else(|| panic!("LanceRead line missing in plan:\n{}", plan));
            let start = line.find("projection=[").unwrap() + "projection=[".len();
            let rest = &line[start..];
            let end = rest.find(']').unwrap();
            rest[..end]
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect()
        }

        // Variant 1: plain `Binary` siblings — sanity check that the prior
        // narrow-projection behaviour still holds for non-Map schemas.
        let bin_dataset = lance_datagen::gen_batch()
            .col(
                "bin_a",
                array::rand_fixedbin(ByteCount::from(BINARY_BYTES), false),
            )
            .col(
                "bin_b",
                array::rand_fixedbin(ByteCount::from(BINARY_BYTES), false),
            )
            .col(
                "bin_c",
                array::rand_fixedbin(ByteCount::from(BINARY_BYTES), false),
            )
            .col(
                "bin_d",
                array::rand_fixedbin(ByteCount::from(BINARY_BYTES), false),
            )
            .col("idx", array::step::<Int32Type>())
            .into_ram_dataset(
                FragmentCount::from(FRAGMENTS),
                FragmentRowCount::from(ROWS_PER_FRAGMENT),
            )
            .await
            .unwrap();
        assert_eq!(
            projection_columns(&bin_dataset, "idx").await,
            vec!["idx".to_string()],
            "binary-siblings schema must project only the requested column"
        );

        // Variant 2: `Map` siblings + a fixed-size `Binary` index column —
        // the actual regression guard. Multiple Map types with different
        // value shapes (`Utf8`, `List<Utf8>`, `Float64`) exercise the
        // children-clone codepath across a few representative shapes.
        let map_dir = TempStrDir::default();
        let map_uri = format!("{}/maps", map_dir.as_str());
        let map_params = crate::dataset::WriteParams {
            data_storage_version: Some(LanceFileVersion::V2_2),
            max_rows_per_file: ROWS_PER_FRAGMENT as usize,
            ..Default::default()
        };
        let map_dataset = lance_datagen::gen_batch()
            .col("map_a", array::rand_map(&DataType::Utf8, &DataType::Utf8))
            .col(
                "map_b",
                array::rand_map(
                    &DataType::Utf8,
                    &DataType::List(Arc::new(arrow_schema::Field::new(
                        "item",
                        DataType::Utf8,
                        true,
                    ))),
                ),
            )
            .col(
                "map_c",
                array::rand_map(&DataType::Utf8, &DataType::Float64),
            )
            .col(
                "map_d",
                array::rand_map(&DataType::Utf8, &DataType::Float64),
            )
            .col(
                "indexed",
                array::rand_fixedbin(ByteCount::from(BINARY_BYTES), false),
            )
            .into_dataset_with_params(
                &map_uri,
                FragmentCount::from(FRAGMENTS),
                FragmentRowCount::from(ROWS_PER_FRAGMENT),
                Some(map_params),
            )
            .await
            .unwrap();
        assert_eq!(
            projection_columns(&map_dataset, "indexed").await,
            vec!["indexed".to_string()],
            "map-siblings schema must not pull unrelated Map columns into LanceRead"
        );

        // Variant 3: same dataset, exercising the `with_fragments` delta
        // path used by `optimize_indices` for incremental BTree updates.
        let frag = map_dataset.fragments().first().cloned().unwrap();
        let mut scan = map_dataset.scan();
        scan.order_by(Some(vec![ColumnOrdering::asc_nulls_first(
            "indexed".to_string(),
        )]))
        .unwrap();
        scan.with_row_id();
        scan.with_fragments(vec![frag]);
        scan.project_with_transform(&[(VALUE_COLUMN_NAME, "indexed")])
            .unwrap();
        let plan = scan.explain_plan(false).await.unwrap();
        let line = plan
            .lines()
            .find(|l| l.contains("projection=["))
            .unwrap_or_else(|| panic!("LanceRead line missing:\n{}", plan));
        assert!(
            line.contains("projection=[indexed]"),
            "with_fragments delta scan must also project only the indexed column; got:\n{}",
            line
        );

        // Variant 4: when the Map column itself is requested, its internal
        // children (entries struct with key/value) must still be preserved
        // — this is the original intent of PR #5349 and the reason
        // `apply_projection` clones the children whole-cloth for selected
        // Map fields.
        let mut scan = map_dataset.scan();
        scan.project(&["map_c"]).unwrap();
        let projected_schema = scan.schema().await.unwrap();
        let projected = projected_schema.field_with_name("map_c").unwrap();
        match projected.data_type() {
            DataType::Map(entries, _) => match entries.data_type() {
                DataType::Struct(children) => {
                    assert_eq!(
                        children.len(),
                        2,
                        "selected Map column must keep its key/value entries struct"
                    );
                }
                other => panic!("Map entries should be Struct, got {:?}", other),
            },
            other => panic!("expected Map type, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_open_scalar_index_coalesces_concurrent_cold_opens() {
        use crate::dataset::builder::DatasetBuilder;
        use arrow::datatypes::Int64Type;
        use futures::future::try_join_all;
        use lance_index::metrics::LocalMetricsCollector;
        use std::sync::atomic::Ordering;

        let dir = TempStrDir::default();
        let uri = dir.as_ref();
        let mut ds = lance_datagen::gen_batch()
            .col("id", array::step::<Int64Type>())
            .into_dataset(uri, FragmentCount::from(1), FragmentRowCount::from(200))
            .await
            .unwrap();

        let params = ScalarIndexParams::for_builtin(BuiltinIndexType::BTree);
        ds.create_index(&["id"], IndexType::Scalar, None, &params, false)
            .await
            .unwrap();

        // Reopen so the index cache is cold for the concurrent opens below.
        let ds = DatasetBuilder::from_uri(uri).load().await.unwrap();
        let id_field = ds.schema().field("id").unwrap().id;
        let index_meta = ds
            .load_indices()
            .await
            .unwrap()
            .iter()
            .find(|idx| idx.fields == [id_field])
            .cloned()
            .expect("btree index on `id`");

        // Eight concurrent cold opens; single-flight means the loader (which calls
        // `record_index_load`) runs exactly once, not once per open.
        let metrics = LocalMetricsCollector::default();
        let indices =
            try_join_all((0..8).map(|_| open_scalar_index(&ds, "id", &index_meta, &metrics)))
                .await
                .unwrap();

        assert_eq!(indices.len(), 8);
        assert_eq!(metrics.index_loads.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn test_load_training_data_addr_sort() {
        // Create test data using lance_datagen
        let dataset = lance_datagen::gen_batch()
            .col("values", array::step::<Int32Type>())
            .into_ram_dataset(FragmentCount::from(4), FragmentRowCount::from(10))
            .await
            .unwrap();

        // Test scan_aligned_chunks with different chunk sizes
        log::info!("Testing with chunk_size=10:");
        let stream = load_training_data(
            &dataset,
            "values",
            &TrainingCriteria::new(TrainingOrdering::Addresses).with_row_addr(),
            None,
            true,
            None,
        )
        .await
        .unwrap();

        // Row addresses should be strictly increasing and ending with fragment id=3
        let mut max_frag_id_seen = 0;
        let mut last_rowaddr = 0;
        for batch in stream.try_collect::<Vec<_>>().await.unwrap() {
            for rowaddr in batch
                .column_by_name(ROW_ADDR)
                .unwrap()
                .as_primitive::<UInt64Type>()
                .values()
            {
                assert!(last_rowaddr == 0 || *rowaddr > last_rowaddr);
                last_rowaddr = *rowaddr;
                let frag_id = RowAddress::from(*rowaddr).fragment_id();
                max_frag_id_seen = frag_id;
            }
        }
        assert_eq!(max_frag_id_seen, 3);
    }

    #[tokio::test]
    async fn test_initialize_scalar_index_btree() {
        use crate::dataset::Dataset;
        use crate::index::DatasetIndexExt;
        use arrow_array::types::Float32Type;
        use lance_datagen::{BatchCount, RowCount, array};
        use lance_index::metrics::NoOpMetricsCollector;
        use lance_index::scalar::ScalarIndexParams;

        let test_dir = TempStrDir::default();
        let source_uri = format!("{}/source", test_dir.as_str());
        let target_uri = format!("{}/target", test_dir.as_str());

        // Create source dataset with BTree index
        let source_reader = lance_datagen::gen_batch()
            .col("id", array::step::<Int32Type>())
            .col("value", array::rand::<Float32Type>())
            .into_reader_rows(RowCount::from(100), BatchCount::from(1));
        let mut source_dataset = Dataset::write(source_reader, &source_uri, None)
            .await
            .unwrap();

        // Create BTree index on source with custom zone_size
        use lance_index::scalar::btree::BTreeParameters;

        let btree_params = BTreeParameters {
            zone_size: Some(50),
            range_id: None,
        };
        let params_json = serde_json::to_value(&btree_params).unwrap();
        let index_params =
            ScalarIndexParams::for_builtin(lance_index::scalar::BuiltinIndexType::BTree)
                .with_params(&params_json);

        source_dataset
            .create_index(
                &["id"],
                IndexType::BTree,
                Some("id_btree".to_string()),
                &index_params,
                false,
            )
            .await
            .unwrap();

        // Reload to get updated metadata
        let source_dataset = Dataset::open(&source_uri).await.unwrap();
        let source_indices = source_dataset.load_indices().await.unwrap();
        let source_index = source_indices
            .iter()
            .find(|idx| idx.name == "id_btree")
            .unwrap();

        // Create target dataset with same schema
        let target_reader = lance_datagen::gen_batch()
            .col("id", array::step::<Int32Type>())
            .col("value", array::rand::<Float32Type>())
            .into_reader_rows(RowCount::from(100), BatchCount::from(1));
        let mut target_dataset = Dataset::write(target_reader, &target_uri, None)
            .await
            .unwrap();

        // Initialize BTree index on target
        super::initialize_scalar_index(&mut target_dataset, &source_dataset, source_index, &["id"])
            .await
            .unwrap();

        // Verify index was created
        let target_indices = target_dataset.load_indices().await.unwrap();
        assert_eq!(target_indices.len(), 1, "Target should have 1 index");
        assert_eq!(
            target_indices[0].name, "id_btree",
            "Index name should match"
        );
        assert_eq!(
            target_indices[0].fields,
            vec![0],
            "Index should be on field 0 (id)"
        );

        // Verify the index type is correct
        let target_scalar_index = target_dataset
            .open_scalar_index("id", &target_indices[0].uuid, &NoOpMetricsCollector)
            .await
            .unwrap();

        assert_eq!(
            target_scalar_index.index_type(),
            IndexType::BTree,
            "Index type should be BTree"
        );

        // Verify BTree parameters are preserved
        let derived_params = target_scalar_index.derive_index_params().unwrap();
        if let Some(params_json) = derived_params.params {
            let params: BTreeParameters = serde_json::from_str(&params_json).unwrap();
            assert_eq!(params.zone_size, Some(50), "BTree zone_size should be 50");
        } else {
            panic!("BTree index should have parameters");
        }
    }

    #[tokio::test]
    async fn test_optimize_scalar_index_btree() {
        use crate::dataset::Dataset;
        use crate::index::DatasetIndexExt;
        use arrow_array::types::Float32Type;
        use lance_datagen::{BatchCount, RowCount, array};
        use lance_index::metrics::NoOpMetricsCollector;
        use lance_index::scalar::ScalarIndexParams;

        let test_dir = TempStrDir::default();
        let uri = format!("{}/source", test_dir.as_str());

        // Create source dataset with BTree index
        let reader = lance_datagen::gen_batch()
            .col("id", array::step::<Int32Type>())
            .col("value", array::rand::<Float32Type>())
            .into_reader_rows(RowCount::from(100), BatchCount::from(1));
        let mut dataset = Dataset::write(reader, &uri, None).await.unwrap();

        // Create BTree index on source with custom zone_size
        use lance_index::scalar::btree::BTreeParameters;

        let btree_params = BTreeParameters {
            zone_size: Some(50),
            range_id: None,
        };
        let params_json = serde_json::to_value(&btree_params).unwrap();
        let index_params =
            ScalarIndexParams::for_builtin(lance_index::scalar::BuiltinIndexType::BTree)
                .with_params(&params_json);

        dataset
            .create_index(
                &["id"],
                IndexType::BTree,
                Some("id_btree".to_string()),
                &index_params,
                false,
            )
            .await
            .unwrap();

        // Verify index was created
        let indices = dataset.load_indices().await.unwrap();
        assert_eq!(indices.len(), 1, "Target should have 1 index");
        assert_eq!(indices[0].name, "id_btree", "Index name should match");
        assert_eq!(
            indices[0].fields,
            vec![0],
            "Index should be on field 0 (id)"
        );

        // Verify the index type is correct
        let scalar_index = dataset
            .open_scalar_index("id", &indices[0].uuid, &NoOpMetricsCollector)
            .await
            .unwrap();

        assert_eq!(
            scalar_index.index_type(),
            IndexType::BTree,
            "Index type should be BTree"
        );

        // Verify BTree parameters are preserved
        let derived_params = scalar_index.derive_index_params().unwrap();
        if let Some(params_json) = derived_params.params {
            let params: BTreeParameters = serde_json::from_str(&params_json).unwrap();
            assert_eq!(params.zone_size, Some(50), "BTree zone_size should be 50");
        } else {
            panic!("BTree index should have parameters");
        }

        // Append more data to dataset
        let reader = lance_datagen::gen_batch()
            .col("id", array::step::<Int32Type>())
            .col("value", array::rand::<Float32Type>())
            .into_reader_rows(RowCount::from(200), BatchCount::from(1));
        dataset.append(reader, None).await.unwrap();

        // Optimize BTree index
        let optimize_index_options =
            OptimizeOptions::new().index_names(vec!["id_btree".to_string()]);
        dataset
            .optimize_indices(&optimize_index_options)
            .await
            .unwrap();

        // Verify BTree parameters are same after optimization
        let indices = dataset.load_indices().await.unwrap();
        assert_eq!(indices.len(), 1, "Target should have 1 index");
        assert_eq!(indices[0].name, "id_btree", "Index name should match");
        assert_eq!(
            indices[0].fields,
            vec![0],
            "Index should be on field 0 (id)"
        );

        let scalar_index = dataset
            .open_scalar_index("id", &indices[0].uuid, &NoOpMetricsCollector)
            .await
            .unwrap();

        assert_eq!(
            scalar_index.index_type(),
            IndexType::BTree,
            "Index type should be BTree"
        );

        let derived_params = scalar_index.derive_index_params().unwrap();
        if let Some(params_json) = derived_params.params {
            let params: BTreeParameters = serde_json::from_str(&params_json).unwrap();
            assert_eq!(params.zone_size, Some(50), "BTree zone_size should be 50");
        } else {
            panic!("BTree index should have parameters");
        }
    }

    #[tokio::test]
    async fn test_initialize_scalar_index_bitmap() {
        use crate::dataset::Dataset;
        use crate::index::DatasetIndexExt;
        use arrow_array::types::Float32Type;
        use lance_datagen::{BatchCount, RowCount, array};
        use lance_index::scalar::ScalarIndexParams;

        let test_dir = TempStrDir::default();
        let source_uri = format!("{}/source", test_dir.as_str());
        let target_uri = format!("{}/target", test_dir.as_str());

        // Create source dataset with low cardinality column for bitmap index
        let source_reader = lance_datagen::gen_batch()
            .col(
                "category",
                array::cycle::<Int32Type>((0..10).collect::<Vec<_>>()),
            )
            .col("value", array::rand::<Float32Type>())
            .into_reader_rows(RowCount::from(100), BatchCount::from(1));
        let mut source_dataset = Dataset::write(source_reader, &source_uri, None)
            .await
            .unwrap();

        // Create Bitmap index on source
        source_dataset
            .create_index(
                &["category"],
                IndexType::Bitmap,
                Some("category_bitmap".to_string()),
                &ScalarIndexParams::default(),
                false,
            )
            .await
            .unwrap();

        // Reload to get updated metadata
        let source_dataset = Dataset::open(&source_uri).await.unwrap();
        let source_indices = source_dataset.load_indices().await.unwrap();
        let source_index = source_indices
            .iter()
            .find(|idx| idx.name == "category_bitmap")
            .unwrap();

        // Create target dataset with same schema
        let target_reader = lance_datagen::gen_batch()
            .col(
                "category",
                array::cycle::<Int32Type>((0..10).collect::<Vec<_>>()),
            )
            .col("value", array::rand::<Float32Type>())
            .into_reader_rows(RowCount::from(100), BatchCount::from(1));
        let mut target_dataset = Dataset::write(target_reader, &target_uri, None)
            .await
            .unwrap();

        // Initialize Bitmap index on target
        super::initialize_scalar_index(
            &mut target_dataset,
            &source_dataset,
            source_index,
            &["category"],
        )
        .await
        .unwrap();

        // Verify index was created
        let target_indices = target_dataset.load_indices().await.unwrap();
        assert_eq!(target_indices.len(), 1, "Target should have 1 index");
        assert_eq!(
            target_indices[0].name, "category_bitmap",
            "Index name should match"
        );
        assert_eq!(
            target_indices[0].fields,
            vec![0],
            "Index should be on field 0 (category)"
        );
    }

    #[tokio::test]
    async fn test_initialize_scalar_index_inverted() {
        use crate::dataset::Dataset;
        use crate::index::DatasetIndexExt;
        use lance_datagen::{BatchCount, ByteCount, RowCount, array};
        use lance_index::metrics::NoOpMetricsCollector;
        use lance_index::scalar::inverted::tokenizer::InvertedIndexParams;

        let test_dir = TempStrDir::default();
        let source_uri = format!("{}/source", test_dir.as_str());
        let target_uri = format!("{}/target", test_dir.as_str());

        // Create source dataset with text column for inverted index
        let source_reader = lance_datagen::gen_batch()
            .col("text", array::rand_utf8(ByteCount::from(50), false))
            .col("id", array::step::<Int32Type>())
            .into_reader_rows(RowCount::from(100), BatchCount::from(1));
        let mut source_dataset = Dataset::write(source_reader, &source_uri, None)
            .await
            .unwrap();

        // Create Inverted (FTS) index on source with custom parameters
        let inverted_params = InvertedIndexParams::default()
            .base_tokenizer("whitespace".to_string())
            .with_position(true)
            .max_token_length(Some(128))
            .lower_case(false)
            .stem(false)
            .remove_stop_words(false)
            .ascii_folding(false);

        source_dataset
            .create_index(
                &["text"],
                IndexType::Inverted,
                Some("text_fts".to_string()),
                &inverted_params,
                false,
            )
            .await
            .unwrap();

        // Reload to get updated metadata
        let source_dataset = Dataset::open(&source_uri).await.unwrap();
        let source_indices = source_dataset.load_indices().await.unwrap();
        let source_index = source_indices
            .iter()
            .find(|idx| idx.name == "text_fts")
            .unwrap();

        // Create target dataset with same schema
        let target_reader = lance_datagen::gen_batch()
            .col("text", array::rand_utf8(ByteCount::from(50), false))
            .col("id", array::step::<Int32Type>())
            .into_reader_rows(RowCount::from(100), BatchCount::from(1));
        let mut target_dataset = Dataset::write(target_reader, &target_uri, None)
            .await
            .unwrap();

        // Initialize Inverted index on target
        super::initialize_scalar_index(
            &mut target_dataset,
            &source_dataset,
            source_index,
            &["text"],
        )
        .await
        .unwrap();

        // Verify index was created
        let target_indices = target_dataset.load_indices().await.unwrap();
        assert_eq!(target_indices.len(), 1, "Target should have 1 index");
        assert_eq!(
            target_indices[0].name, "text_fts",
            "Index name should match"
        );
        assert_eq!(
            target_indices[0].fields,
            vec![0],
            "Index should be on field 0 (text)"
        );

        // Verify the index type is correct
        let target_scalar_index = target_dataset
            .open_scalar_index("text", &target_indices[0].uuid, &NoOpMetricsCollector)
            .await
            .unwrap();

        assert_eq!(
            target_scalar_index.index_type(),
            IndexType::Inverted,
            "Index type should be Inverted"
        );

        // Verify parameters are preserved by parsing the JSON params
        let derived_params = target_scalar_index.derive_index_params().unwrap();
        assert!(
            derived_params.params.is_some(),
            "Inverted index should have parameters"
        );

        // Parse the JSON parameters to verify specific fields
        let params_json = derived_params.params.unwrap();
        let params: serde_json::Value = serde_json::from_str(&params_json).unwrap();

        // Verify all the custom parameters we set
        assert_eq!(
            params["base_tokenizer"].as_str().unwrap(),
            "whitespace",
            "Base tokenizer should be whitespace"
        );
        assert!(
            params["with_position"].as_bool().unwrap(),
            "with_position should be true"
        );
        assert_eq!(
            params["max_token_length"].as_u64().unwrap(),
            128,
            "max_token_length should be 128"
        );
        assert!(
            !params["lower_case"].as_bool().unwrap(),
            "lower_case should be false"
        );
        assert!(!params["stem"].as_bool().unwrap(), "stem should be false");
        assert!(
            !params["remove_stop_words"].as_bool().unwrap(),
            "remove_stop_words should be false"
        );
        assert!(
            !params["ascii_folding"].as_bool().unwrap(),
            "ascii_folding should be false"
        );
    }

    #[tokio::test]
    async fn test_initialize_scalar_index_zonemap() {
        use crate::dataset::Dataset;
        use crate::index::DatasetIndexExt;
        use arrow_array::types::Float32Type;
        use lance_datagen::{BatchCount, RowCount, array};
        use lance_index::metrics::NoOpMetricsCollector;
        use lance_index::scalar::ScalarIndexParams;
        use lance_index::scalar::zonemap::ZoneMapIndexBuilderParams;

        let test_dir = TempStrDir::default();
        let source_uri = format!("{}/source", test_dir.as_str());
        let target_uri = format!("{}/target", test_dir.as_str());

        // Create source dataset with ZoneMap index
        let source_reader = lance_datagen::gen_batch()
            .col("value", array::rand::<Float32Type>())
            .col("id", array::step::<Int32Type>())
            .into_reader_rows(RowCount::from(1000), BatchCount::from(1));
        let mut source_dataset = Dataset::write(source_reader, &source_uri, None)
            .await
            .unwrap();

        // Create ZoneMap index on source with custom rows_per_zone
        let zonemap_params = ZoneMapIndexBuilderParams::new(200);
        let params_json = serde_json::to_value(&zonemap_params).unwrap();
        let index_params =
            ScalarIndexParams::for_builtin(lance_index::scalar::BuiltinIndexType::ZoneMap)
                .with_params(&params_json);

        source_dataset
            .create_index(
                &["value"],
                IndexType::ZoneMap,
                Some("value_zonemap".to_string()),
                &index_params,
                false,
            )
            .await
            .unwrap();

        // Reload to get updated metadata
        let source_dataset = Dataset::open(&source_uri).await.unwrap();
        let source_indices = source_dataset.load_indices().await.unwrap();
        let source_index = source_indices
            .iter()
            .find(|idx| idx.name == "value_zonemap")
            .unwrap();

        // Create target dataset with same schema
        let target_reader = lance_datagen::gen_batch()
            .col("value", array::rand::<Float32Type>())
            .col("id", array::step::<Int32Type>())
            .into_reader_rows(RowCount::from(1000), BatchCount::from(1));
        let mut target_dataset = Dataset::write(target_reader, &target_uri, None)
            .await
            .unwrap();

        // Initialize ZoneMap index on target
        super::initialize_scalar_index(
            &mut target_dataset,
            &source_dataset,
            source_index,
            &["value"],
        )
        .await
        .unwrap();

        // Verify index was created
        let target_indices = target_dataset.load_indices().await.unwrap();
        assert_eq!(target_indices.len(), 1, "Target should have 1 index");
        assert_eq!(
            target_indices[0].name, "value_zonemap",
            "Index name should match"
        );
        assert_eq!(
            target_indices[0].fields,
            vec![0],
            "Index should be on field 0 (value)"
        );

        // Verify the index type is correct
        let target_scalar_index = target_dataset
            .open_scalar_index("value", &target_indices[0].uuid, &NoOpMetricsCollector)
            .await
            .unwrap();

        assert_eq!(
            target_scalar_index.index_type(),
            IndexType::ZoneMap,
            "Index type should be ZoneMap"
        );

        // Verify ZoneMap statistics show correct rows_per_zone
        let stats = target_scalar_index.statistics().unwrap();
        let rows_per_zone = stats["rows_per_zone"].as_u64().unwrap();
        assert_eq!(rows_per_zone, 200, "ZoneMap rows_per_zone should be 200");
    }

    #[tokio::test]
    async fn test_zonemap_with_deletions() {
        let deletion_predicates = [
            "NOT value",            // every other row
            "id > 8191 or id < 10", // Second zone of each fragment
            "id < 9190 ",           // Most of first zone
        ];
        let query_predicates = ["value", "id <= 8191", "id >= 1"];

        async fn filter_query(ds: &Dataset, query_pred: &str) -> arrow_array::RecordBatch {
            ds.scan()
                .filter(query_pred)
                .unwrap()
                .try_into_batch()
                .await
                .unwrap()
        }

        for del_pred in &deletion_predicates {
            // We use 2 * 8192 so each fragment has two zones.
            let mut ds = lance_datagen::gen_batch()
                .col("id", array::step::<UInt64Type>())
                .col("value", array::cycle_bool(vec![true, false]))
                .into_ram_dataset(FragmentCount::from(2), FragmentRowCount::from(2 * 8192))
                .await
                .unwrap();

            // Create zonemap index on "value" column
            let params = ScalarIndexParams::for_builtin(BuiltinIndexType::ZoneMap);
            ds.create_index_builder(&["value"], IndexType::Scalar, &params)
                .name("value_zone_map".into())
                .await
                .unwrap();
            ds.create_index_builder(&["id"], IndexType::Scalar, &params)
                .name("id_zone_map".into())
                .await
                .unwrap();

            ds.delete(del_pred).await.unwrap();
            let mut result_before = Vec::new();
            for query_pred in &query_predicates {
                let batch = filter_query(&ds, query_pred).await;
                result_before.push(batch);
            }
            ds.drop_index("value_zone_map").await.unwrap();
            ds.drop_index("id_zone_map").await.unwrap();

            let mut expected = Vec::new();
            for query_pred in &query_predicates {
                let batch = filter_query(&ds, query_pred).await;
                expected.push(batch);
            }

            for (before, expected) in result_before.iter().zip(expected.iter()) {
                assert_eq!(
                    before, expected,
                    "Zonemap index with deletions returned wrong results for deletion predicate '{}'",
                    del_pred
                );
            }

            // Now recreate the indexes for the next iteration
            ds.create_index_builder(&["value"], IndexType::Scalar, &params)
                .name("value_zone_map".into())
                .await
                .unwrap();
            ds.create_index_builder(&["id"], IndexType::Scalar, &params)
                .name("id_zone_map".into())
                .await
                .unwrap();
            let mut result_after = Vec::new();
            for query_pred in &query_predicates {
                let batch = filter_query(&ds, query_pred).await;
                result_after.push(batch);
            }

            for (after, expected) in result_after.iter().zip(expected.iter()) {
                assert_eq!(
                    after, expected,
                    "Zonemap index with deletions returned wrong results for deletion predicate '{}' after re-creating the index",
                    del_pred
                );
            }
        }
    }

    #[tokio::test]
    async fn test_zonemap_deletion_then_index() {
        use arrow::datatypes::UInt64Type;
        use lance_datagen::array;
        use lance_index::IndexType;
        use lance_index::scalar::{BuiltinIndexType, ScalarIndexParams};

        // Create dataset with 10 rows in two fragments: alternating boolean values
        // Rows 0,2,4,6,8 have value=true, rows 1,3,5,7,9 have value=false
        let mut ds = lance_datagen::gen_batch()
            .col("id", array::step::<UInt64Type>())
            .col("value", array::cycle_bool(vec![true, false]))
            .into_ram_dataset(FragmentCount::from(2), FragmentRowCount::from(5))
            .await
            .unwrap();

        // Delete rows where value=false (rows 1, 3, 5, 7, 9)
        ds.delete("NOT value").await.unwrap();

        // Verify data before index creation: should have 5 rows with value=true
        let before_index = ds
            .scan()
            .filter("value")
            .unwrap()
            .try_into_batch()
            .await
            .unwrap();

        let before_ids = before_index["id"]
            .as_any()
            .downcast_ref::<arrow_array::UInt64Array>()
            .unwrap()
            .values();

        assert_eq!(
            before_ids,
            &[0, 2, 4, 6, 8],
            "Before index: should have 5 rows"
        );

        // Create zonemap index on "value" column
        let params = ScalarIndexParams::for_builtin(BuiltinIndexType::ZoneMap);
        ds.create_index(&["value"], IndexType::Scalar, None, &params, false)
            .await
            .unwrap();

        let after_index = ds
            .scan()
            .filter("value")
            .unwrap()
            .try_into_batch()
            .await
            .unwrap();

        let after_ids = after_index["id"]
            .as_any()
            .downcast_ref::<arrow_array::UInt64Array>()
            .unwrap()
            .values();

        // This assertion will FAIL if bug #4758 is present
        assert_eq!(
            after_ids.len(),
            5,
            "Expected 5 rows after index creation, got {}. Only {:?} returned instead of [0, 2, 4, 6, 8]",
            after_ids.len(),
            after_ids
        );
        assert_eq!(
            after_ids,
            &[0, 2, 4, 6, 8],
            "Zonemap index with deletions returns wrong results"
        );
    }

    #[tokio::test]
    async fn test_dataset_column_value_range() {
        use crate::index::DatasetIndexExt;
        use arrow::datatypes::Int64Type;
        use datafusion::scalar::ScalarValue;
        use lance_datagen::array;
        use lance_index::IndexType;
        use lance_index::scalar::{BuiltinIndexType, ScalarIndexParams};

        // 2 fragments x 5 rows: `id` and `other` both step 0..9.
        let mut ds = lance_datagen::gen_batch()
            .col("id", array::step::<Int64Type>())
            .col("other", array::step::<Int64Type>())
            .into_ram_dataset(FragmentCount::from(2), FragmentRowCount::from(5))
            .await
            .unwrap();

        let params = ScalarIndexParams::for_builtin(BuiltinIndexType::ZoneMap);
        ds.create_index(&["id"], IndexType::Scalar, None, &params, false)
            .await
            .unwrap();

        // Folded straight from the ZoneMap summaries: global [0, 9].
        assert_eq!(
            ds.statistics().column_value_range("id").await.unwrap(),
            Some((ScalarValue::Int64(Some(0)), ScalarValue::Int64(Some(9))))
        );
        // `other` has no ZoneMap index -> no precomputed range.
        assert_eq!(
            ds.statistics().column_value_range("other").await.unwrap(),
            None
        );
    }

    #[tokio::test]
    async fn test_column_value_range_none_when_index_misses_fragments() {
        use crate::index::DatasetIndexExt;
        use arrow::datatypes::Int64Type;
        use lance_datagen::{BatchCount, RowCount, array};
        use lance_index::IndexType;
        use lance_index::scalar::{BuiltinIndexType, ScalarIndexParams};

        // Index covers the initial 2 fragments.
        let mut ds = lance_datagen::gen_batch()
            .col("id", array::step::<Int64Type>())
            .into_ram_dataset(FragmentCount::from(2), FragmentRowCount::from(5))
            .await
            .unwrap();
        let params = ScalarIndexParams::for_builtin(BuiltinIndexType::ZoneMap);
        ds.create_index(&["id"], IndexType::Scalar, None, &params, false)
            .await
            .unwrap();
        assert!(
            ds.statistics()
                .column_value_range("id")
                .await
                .unwrap()
                .is_some()
        );

        // Append a fragment the index doesn't cover. Its rows may hold values
        // outside the indexed range, so any non-None range would be a subset
        // unsound to prune with -> must return None until the index is rebuilt.
        let reader = lance_datagen::gen_batch()
            .col("id", array::step::<Int64Type>())
            .into_reader_rows(RowCount::from(5), BatchCount::from(1));
        ds.append(reader, None).await.unwrap();

        assert_eq!(
            ds.statistics().column_value_range("id").await.unwrap(),
            None
        );
    }

    #[tokio::test]
    async fn test_column_value_range_folds_multiple_segments() {
        use crate::index::DatasetIndexExt;
        use arrow::datatypes::Int64Type;
        use datafusion::scalar::ScalarValue;
        use lance_datagen::array;
        use lance_index::IndexType;
        use lance_index::scalar::{BuiltinIndexType, ScalarIndexParams};

        // 2 fragments x 5 rows (`id` steps 0..9). Build one ZoneMap segment per
        // fragment, then commit them as a single multi-segment logical index.
        let mut ds = lance_datagen::gen_batch()
            .col("id", array::step::<Int64Type>())
            .into_ram_dataset(FragmentCount::from(2), FragmentRowCount::from(5))
            .await
            .unwrap();

        let params = ScalarIndexParams::for_builtin(BuiltinIndexType::ZoneMap);
        let columns = ["id"];
        let mut segments = Vec::new();
        for fragment_id in [0_u32, 1_u32] {
            let mut builder = ds
                .create_index_builder(&columns, IndexType::Scalar, &params)
                .name("id_idx".to_string())
                .fragments(vec![fragment_id]);
            segments.push(builder.execute_uncommitted().await.unwrap());
        }
        // execute_uncommitted yields ready IndexMetadata segments (IntoIndexSegment);
        // build_all is vector-only, so commit the per-fragment segments directly.
        ds.commit_existing_index_segments("id_idx", "id", segments)
            .await
            .unwrap();

        // Two disjoint segments jointly cover every live fragment -> the fold
        // spans both and yields the global range, not None.
        assert_eq!(ds.load_indices_by_name("id_idx").await.unwrap().len(), 2);
        assert_eq!(
            ds.statistics().column_value_range("id").await.unwrap(),
            Some((ScalarValue::Int64(Some(0)), ScalarValue::Int64(Some(9))))
        );
    }

    #[tokio::test]
    async fn test_zonemap_index_then_deletion() {
        // Tests the opposite scenario: create index FIRST, then perform deletions
        // Verifies that zonemap index properly handles deletions that occur after index creation
        use arrow::datatypes::UInt64Type;
        use lance_datagen::array;
        use lance_index::IndexType;
        use lance_index::scalar::{BuiltinIndexType, ScalarIndexParams};

        // Create dataset with 10 rows: alternating boolean values
        // Rows 0,2,4,6,8 have value=true, rows 1,3,5,7,9 have value=false
        let mut ds = lance_datagen::gen_batch()
            .col("id", array::step::<UInt64Type>())
            .col("value", array::cycle_bool(vec![true, false]))
            .into_ram_dataset(FragmentCount::from(2), FragmentRowCount::from(5))
            .await
            .unwrap();

        // Verify initial data: should have 10 rows
        let initial_data = ds.scan().try_into_batch().await.unwrap();

        let initial_count: usize = initial_data["id"].len();
        assert_eq!(initial_count, 10, "Should start with 10 rows");

        // CREATE INDEX FIRST (before deletion)
        let params = ScalarIndexParams::for_builtin(BuiltinIndexType::ZoneMap);
        ds.create_index(&["value"], IndexType::Scalar, None, &params, false)
            .await
            .unwrap();

        // Query with index before deletion - should return all 5 rows with value=true
        let before_deletion = ds
            .scan()
            .filter("value")
            .unwrap()
            .try_into_batch()
            .await
            .unwrap();

        let before_deletion_ids = before_deletion["id"]
            .as_any()
            .downcast_ref::<arrow_array::UInt64Array>()
            .unwrap()
            .values();

        assert_eq!(
            before_deletion_ids,
            &[0, 2, 4, 6, 8],
            "Before deletion: should return 5 rows with value=true"
        );

        // NOW DELETE rows where value=false (rows 1, 3, 5, 7, 9)
        ds.delete("NOT value").await.unwrap();

        // Query after deletion - should still return 5 rows with value=true
        let after_deletion = ds
            .scan()
            .filter("value")
            .unwrap()
            .try_into_batch()
            .await
            .unwrap();

        let after_deletion_ids = after_deletion["id"]
            .as_any()
            .downcast_ref::<arrow_array::UInt64Array>()
            .unwrap()
            .values();

        // Verify we get the correct data after deletion
        assert_eq!(
            after_deletion_ids.len(),
            5,
            "After deletion: Expected 5 rows, got {}",
            after_deletion_ids.len()
        );
        assert_eq!(
            after_deletion_ids,
            &[0, 2, 4, 6, 8],
            "After deletion: Should return rows [0, 2, 4, 6, 8] with value=true"
        );

        // Verify the actual values are correct
        let after_deletion_values: Vec<bool> = after_deletion["value"]
            .as_any()
            .downcast_ref::<arrow_array::BooleanArray>()
            .unwrap()
            .iter()
            .flatten()
            .collect();

        assert_eq!(
            after_deletion_values,
            vec![true, true, true, true, true],
            "All returned rows should have value=true"
        );

        // Count rows matching "value = true"
        let count_true = ds
            .scan()
            .filter("value")
            .unwrap()
            .try_into_batch()
            .await
            .unwrap();
        let count_true_rows: usize = count_true.num_rows();

        // Count rows matching "value = false" (should be 0 after deletion)
        let count_false = ds
            .scan()
            .filter("NOT value")
            .unwrap()
            .try_into_batch()
            .await
            .unwrap();
        let count_false_rows: usize = count_false.num_rows();

        // The key assertions: filtered queries should return correct data
        assert_eq!(
            count_true_rows, 5,
            "Should have exactly 5 rows with value=true"
        );
        assert_eq!(
            count_false_rows, 0,
            "Should have 0 rows with value=false after deletion"
        );
    }

    #[tokio::test]
    async fn test_bloomfilter_deletion_then_index() {
        // Reproduces the same bug as #4758 but for bloom filter indexes
        // After deleting rows and creating a bloom filter index, queries return fewer results than expected
        use arrow::datatypes::UInt64Type;
        use lance_datagen::array;
        use lance_index::IndexType;
        use lance_index::scalar::{BuiltinIndexType, ScalarIndexParams};

        // Create dataset with 10 rows: alternating string values "apple" and "banana"
        // Rows 0,2,4,6,8 have value="apple", rows 1,3,5,7,9 have value="banana"
        let mut ds = lance_datagen::gen_batch()
            .col("id", array::step::<UInt64Type>())
            .col("value", array::cycle_utf8_literals(&["apple", "banana"]))
            .into_ram_dataset(FragmentCount::from(2), FragmentRowCount::from(5))
            .await
            .unwrap();

        // Delete rows where value="banana" (rows 1, 3, 5, 7, 9)
        ds.delete("value = 'banana'").await.unwrap();

        // Verify data before index creation: should have 5 rows with value="apple"
        let before_index = ds
            .scan()
            .filter("value = 'apple'")
            .unwrap()
            .try_into_batch()
            .await
            .unwrap();

        let before_ids = before_index["id"]
            .as_any()
            .downcast_ref::<arrow_array::UInt64Array>()
            .unwrap()
            .values();

        assert_eq!(
            before_ids,
            &[0, 2, 4, 6, 8],
            "Before index: should have 5 rows"
        );

        // Create bloom filter index on "value" column with small zone size to ensure the bug is triggered
        #[derive(serde::Serialize)]
        struct BloomParams {
            number_of_items: u64,
            probability: f64,
        }
        let params = ScalarIndexParams::for_builtin(BuiltinIndexType::BloomFilter).with_params(
            &BloomParams {
                number_of_items: 5, // Small zone size to ensure multiple zones
                probability: 0.01,
            },
        );
        ds.create_index(&["value"], IndexType::Scalar, None, &params, false)
            .await
            .unwrap();

        let after_index = ds
            .scan()
            .filter("value = 'apple'")
            .unwrap()
            .try_into_batch()
            .await
            .unwrap();

        let after_ids = after_index["id"]
            .as_any()
            .downcast_ref::<arrow_array::UInt64Array>()
            .unwrap()
            .values();

        // This assertion verifies the fix works
        assert_eq!(
            after_ids.len(),
            5,
            "Expected 5 rows after index creation, got {}. Only {:?} returned instead of [0, 2, 4, 6, 8]",
            after_ids.len(),
            after_ids
        );
        assert_eq!(
            after_ids,
            &[0, 2, 4, 6, 8],
            "Bloom filter index with deletions returns wrong results"
        );
    }

    #[tokio::test]
    async fn test_bloomfilter_index_then_deletion() {
        // Tests the opposite scenario: create bloom filter index FIRST, then perform deletions
        // Verifies that bloom filter index properly handles deletions that occur after index creation
        use arrow::datatypes::UInt64Type;
        use lance_datagen::array;
        use lance_index::IndexType;
        use lance_index::scalar::{BuiltinIndexType, ScalarIndexParams};

        // Create dataset with 10 rows: alternating string values "apple" and "banana"
        // Rows 0,2,4,6,8 have value="apple", rows 1,3,5,7,9 have value="banana"
        let mut ds = lance_datagen::gen_batch()
            .col("id", array::step::<UInt64Type>())
            .col("value", array::cycle_utf8_literals(&["apple", "banana"]))
            .into_ram_dataset(FragmentCount::from(2), FragmentRowCount::from(5))
            .await
            .unwrap();

        // Verify initial data: should have 10 rows
        let initial_data = ds.scan().try_into_batch().await.unwrap();

        let initial_count: usize = initial_data.num_rows();
        assert_eq!(initial_count, 10, "Should start with 10 rows");

        // CREATE INDEX FIRST (before deletion)
        #[derive(serde::Serialize)]
        struct BloomParams {
            number_of_items: u64,
            probability: f64,
        }
        let params = ScalarIndexParams::for_builtin(BuiltinIndexType::BloomFilter).with_params(
            &BloomParams {
                number_of_items: 5, // Small zone size to ensure multiple zones
                probability: 0.01,
            },
        );
        ds.create_index(&["value"], IndexType::Scalar, None, &params, false)
            .await
            .unwrap();

        // Query with index before deletion - should return all 5 rows with value="apple"
        let before_deletion = ds
            .scan()
            .filter("value = 'apple'")
            .unwrap()
            .try_into_batch()
            .await
            .unwrap();

        let before_deletion_ids = before_deletion["id"]
            .as_any()
            .downcast_ref::<arrow_array::UInt64Array>()
            .unwrap()
            .values();

        assert_eq!(
            before_deletion_ids,
            &[0, 2, 4, 6, 8],
            "Before deletion: should return 5 rows with value='apple'"
        );

        // NOW DELETE rows where value="banana" (rows 1, 3, 5, 7, 9)
        ds.delete("value = 'banana'").await.unwrap();

        // Query after deletion - should still return 5 rows with value="apple"
        let after_deletion = ds
            .scan()
            .filter("value = 'apple'")
            .unwrap()
            .try_into_batch()
            .await
            .unwrap();

        let after_deletion_ids = after_deletion["id"]
            .as_any()
            .downcast_ref::<arrow_array::UInt64Array>()
            .unwrap()
            .values();

        // Verify we get the correct data after deletion
        assert_eq!(
            after_deletion_ids.len(),
            5,
            "After deletion: Expected 5 rows, got {}",
            after_deletion_ids.len()
        );
        assert_eq!(
            after_deletion_ids,
            &[0, 2, 4, 6, 8],
            "After deletion: Should return rows [0, 2, 4, 6, 8] with value='apple'"
        );

        // Verify the actual values are correct
        let after_deletion_values: Vec<&str> = after_deletion["value"]
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .unwrap()
            .iter()
            .flatten()
            .collect();

        assert_eq!(
            after_deletion_values,
            vec!["apple", "apple", "apple", "apple", "apple"],
            "All returned rows should have value='apple'"
        );

        // Count rows matching "value = 'apple'"
        let count_apple = ds
            .scan()
            .filter("value = 'apple'")
            .unwrap()
            .try_into_batch()
            .await
            .unwrap();
        let count_apple_rows: usize = count_apple.num_rows();

        // Count rows matching "value = 'banana'" (should be 0 after deletion)
        let count_banana = ds
            .scan()
            .filter("value = 'banana'")
            .unwrap()
            .try_into_batch()
            .await
            .unwrap();
        let count_banana_rows: usize = count_banana.num_rows();

        // The key assertions: filtered queries should return correct data
        assert_eq!(
            count_apple_rows, 5,
            "Should have exactly 5 rows with value='apple'"
        );
        assert_eq!(
            count_banana_rows, 0,
            "Should have 0 rows with value='banana' after deletion"
        );
    }

    // End-to-end: create index → delete a whole fragment → search (via index) →
    // update index → search again.  No deleted rows should ever appear.
    #[tokio::test]
    async fn test_zonemap_search_with_deleted_fragment_before_and_after_update() {
        use arrow::datatypes::Int32Type;
        use lance_datagen::array;
        use lance_index::IndexType;
        use lance_index::optimize::OptimizeOptions;
        use lance_index::scalar::{BuiltinIndexType, ScalarIndexParams};

        // 3 fragments × 10 rows: id 0-9 (frag 0), 10-19 (frag 1), 20-29 (frag 2).
        let mut ds = lance_datagen::gen_batch()
            .col("id", array::step::<Int32Type>())
            .into_ram_dataset(FragmentCount::from(3), FragmentRowCount::from(10))
            .await
            .unwrap();

        let params = ScalarIndexParams::for_builtin(BuiltinIndexType::ZoneMap);
        ds.create_index(&["id"], IndexType::Scalar, None, &params, false)
            .await
            .unwrap();

        // Delete the middle fragment entirely.
        ds.delete("id >= 10 AND id < 20").await.unwrap();

        // Helper: run a filter scan and return the sorted id values.
        async fn live_ids(ds: &crate::Dataset) -> Vec<i32> {
            let batch = ds
                .scan()
                .filter("id >= 0")
                .unwrap()
                .try_into_batch()
                .await
                .unwrap();
            let mut ids: Vec<i32> = batch["id"]
                .as_any()
                .downcast_ref::<arrow_array::Int32Array>()
                .unwrap()
                .values()
                .to_vec();
            ids.sort_unstable();
            ids
        }

        // --- Before index update ---
        let ids = live_ids(&ds).await;
        assert_eq!(ids.len(), 20, "expected 20 live rows before index update");
        assert!(
            ids.iter().all(|&id| !(10..20).contains(&id)),
            "deleted fragment rows (id 10-19) must not appear before index update; got: {:?}",
            ids
        );

        // Update the zone map index to reflect the deletion.
        ds.optimize_indices(&OptimizeOptions::new()).await.unwrap();

        // --- After index update ---
        let ids = live_ids(&ds).await;
        assert_eq!(ids.len(), 20, "expected 20 live rows after index update");
        assert!(
            ids.iter().all(|&id| !(10..20).contains(&id)),
            "deleted fragment rows (id 10-19) must not appear after index update; got: {:?}",
            ids
        );
    }
}
