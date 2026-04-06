// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use std::cmp::Ordering;
use std::collections::{BTreeSet, HashSet};
use std::sync::Arc;

use arrow_array::{Array, FixedSizeListArray, Float32Array, RecordBatch, RecordBatchOptions};
use arrow_schema::{DataType, Field as ArrowField, Schema as ArrowSchema};
use futures::{FutureExt, StreamExt, TryStreamExt};
use lance_core::ROW_ID;
use lance_core::cache::LanceCache;
use lance_core::datatypes::Schema as LanceSchema;
use lance_encoding::decoder::FilterExpression;
use lance_file::reader::{FileReader, FileReaderOptions, ReaderProjection};
use lance_io::ReadBatchParams;
use lance_io::object_store::ObjectStore;
use lance_io::scheduler::{ScanScheduler, SchedulerConfig};
use lance_table::format::{DataFile, Fragment, Manifest, RowIdMeta};
use lance_table::io::deletion::read_deletion_file;
use lance_table::io::manifest::read_manifest;
use lance_table::rowids::{RowIdSequence, read_row_ids};
use lance_table::utils::stream::{
    ReadBatchTask, ReadBatchTaskStream, RowIdAndDeletesConfig, merge_streams,
    wrap_with_row_id_and_delete,
};
use object_store::DynObjectStore;
use object_store::path::Path;
use sqlparser::ast::Expr;
use url::Url;

use crate::browser_expr::{
    RESULT_DISTANCE_COLUMN, RESULT_RELEVANCE_COLUMN, RESULT_SCORE_COLUMN, RowContext, RowScores,
    collect_referenced_columns, evaluate_expression, evaluate_filter, infer_expression_type,
    parse_expression, scalar_to_array,
};
use crate::local_error::{Error, Result};
use crate::{
    MANIFEST_PATH, OpenTableOptions, SearchDistanceType, SearchRequest, SelectRequest, TextRequest,
    build_open_store, object_store_root_url, object_store_table_path, preflight_manifest_check,
};

const DEFAULT_BATCH_SIZE: u32 = 1024;
const DEFAULT_VECTOR_LIMIT: usize = 10;
const HYBRID_RRF_K: f32 = 60.0;

#[derive(Clone)]
pub struct BrowserTable {
    manifest: Manifest,
    object_store: Arc<ObjectStore>,
    scan_scheduler: Arc<ScanScheduler>,
    table_path: Path,
    metadata_cache: LanceCache,
    max_concurrent_ranges: usize,
}

impl BrowserTable {
    pub async fn open(
        table_url: &str,
        options: &OpenTableOptions,
        manifest_url: &str,
    ) -> Result<Self> {
        let parsed_url = Url::parse(table_url).map_err(|source| Error::InvalidInput {
            message: format!("invalid table URL '{table_url}': {source}"),
        })?;
        let table_path = object_store_table_path(&parsed_url)?;
        let manifest_path = table_path.child(MANIFEST_PATH);

        let (http_store, wrapper) =
            build_open_store(&parsed_url, &table_path, manifest_url, options)?;
        let preflight_store = wrapper
            .as_ref()
            .map(|wrapper| wrapper.wrap("lancedb-wasm-preflight", http_store.clone()))
            .unwrap_or_else(|| http_store.clone());
        preflight_manifest_check(preflight_store, &manifest_path).await?;

        let object_store = Arc::new(ObjectStore::new(
            http_store as Arc<DynObjectStore>,
            object_store_root_url(&parsed_url),
            None,
            wrapper,
            false,
            true,
            options
                .max_concurrent_ranges
                .filter(|value| *value > 0)
                .unwrap_or(lance_io::object_store::DEFAULT_CLOUD_IO_PARALLELISM),
            lance_io::object_store::DEFAULT_DOWNLOAD_RETRY_COUNT,
            None,
        ));
        let manifest_meta = object_store.inner.head(&manifest_path).await?;
        let manifest = read_manifest(
            object_store.as_ref(),
            &manifest_path,
            Some(manifest_meta.size),
        )
        .await
        .map_err(Error::from)?;
        let scan_scheduler = ScanScheduler::new(
            object_store.clone(),
            SchedulerConfig::max_bandwidth(&object_store),
        );
        let metadata_cache = match options.cache_bytes {
            Some(capacity) if capacity > 0 => LanceCache::with_capacity(capacity),
            _ => LanceCache::no_cache(),
        };

        Ok(Self {
            manifest,
            object_store,
            scan_scheduler,
            table_path,
            metadata_cache,
            max_concurrent_ranges: options.max_concurrent_ranges.unwrap_or(4).max(1),
        })
    }

    pub fn schema(&self) -> &LanceSchema {
        &self.manifest.schema
    }

    pub fn version(&self) -> u64 {
        self.manifest.version
    }

    pub async fn search_batches(&self, request: SearchRequest) -> Result<Vec<RecordBatch>> {
        let mode = SearchMode::from_request(&self.manifest.schema, &request)?;
        let filter_expr = request
            .filter
            .as_deref()
            .map(parse_expression)
            .transpose()?;
        let output_plan = OutputPlan::new(
            &self.manifest.schema,
            &mode,
            request.select.as_ref(),
            request.with_row_id.unwrap_or(false),
            filter_expr.as_ref(),
        )?;
        let offset = request.offset.unwrap_or(0);
        let limit = requested_limit(&mode, request.limit);
        let prefilter = request.prefilter.unwrap_or(true);

        let mut vector_rows = Vec::new();
        let mut text_rows = Vec::new();
        let mut scan_rows = Vec::new();
        let mut ordinal = 0_u64;

        for fragment in self.manifest.fragments.iter() {
            let batches = self
                .read_fragment_batches(
                    fragment,
                    &output_plan.scan_projection,
                    output_plan.requires_scan_row_id,
                )
                .await?;
            for batch in batches {
                let batch = Arc::new(batch);
                self.collect_rows_from_batch(
                    batch,
                    &mode,
                    filter_expr.as_ref(),
                    prefilter,
                    &mut ordinal,
                    &mut vector_rows,
                    &mut text_rows,
                    &mut scan_rows,
                )?;
            }
        }

        let rows = match mode {
            SearchMode::Scan => apply_offset_limit(
                scan_rows
                    .into_iter()
                    .filter(|row| row.filter_pass)
                    .collect::<Vec<_>>(),
                limit,
                offset,
            ),
            SearchMode::Vector { distance_type, .. } => finalize_ranked_rows(
                vector_rows,
                limit,
                offset,
                filter_expr.is_some() && !prefilter,
                sort_vector_rows(distance_type),
            ),
            SearchMode::Text(_) => finalize_ranked_rows(
                text_rows,
                limit,
                offset,
                filter_expr.is_some() && !prefilter,
                sort_by_text_score,
            ),
            SearchMode::Hybrid(plan) => {
                let requested_limit = limit.unwrap_or(DEFAULT_VECTOR_LIMIT);
                let fusion_window = requested_limit.saturating_add(offset);
                let ranked_vector = finalize_hybrid_source_rows(
                    vector_rows,
                    fusion_window,
                    filter_expr.is_some() && !prefilter,
                    sort_vector_rows(plan.distance_type),
                );
                let ranked_text = finalize_hybrid_source_rows(
                    text_rows,
                    fusion_window,
                    filter_expr.is_some() && !prefilter,
                    sort_by_text_score,
                );
                apply_offset_limit(
                    combine_hybrid_rows(ranked_vector, ranked_text, fusion_window)?,
                    Some(requested_limit),
                    offset,
                )
            }
        };

        rows.into_iter()
            .map(|row| output_plan.materialize_row(&row))
            .collect()
    }

    pub fn result_schema(&self, request: &SearchRequest) -> Result<ArrowSchema> {
        let mode = SearchMode::from_request(&self.manifest.schema, request)?;
        let filter_expr = request
            .filter
            .as_deref()
            .map(parse_expression)
            .transpose()?;
        Ok(OutputPlan::new(
            &self.manifest.schema,
            &mode,
            request.select.as_ref(),
            request.with_row_id.unwrap_or(false),
            filter_expr.as_ref(),
        )?
        .schema)
    }

    fn collect_rows_from_batch(
        &self,
        batch: Arc<RecordBatch>,
        mode: &SearchMode,
        filter_expr: Option<&Expr>,
        prefilter: bool,
        ordinal: &mut u64,
        vector_rows: &mut Vec<ResultRow>,
        text_rows: &mut Vec<ResultRow>,
        scan_rows: &mut Vec<ResultRow>,
    ) -> Result<()> {
        let row_id_index = batch.schema().index_of(ROW_ID).ok();
        let vector_index = match mode {
            SearchMode::Vector { vector_column, .. }
            | SearchMode::Hybrid(HybridPlan { vector_column, .. }) => Some(
                batch
                    .schema()
                    .index_of(vector_column)
                    .map_err(Error::from)?,
            ),
            _ => None,
        };

        for row_index in 0..batch.num_rows() {
            let filter_pass = match filter_expr {
                Some(expr) => evaluate_filter(
                    expr,
                    &RowContext::new(&batch, row_index, RowScores::default()),
                )?,
                None => true,
            };
            if prefilter && !filter_pass {
                *ordinal += 1;
                continue;
            }

            let row_id = row_id_index
                .map(|index| extract_row_id(batch.column(index).as_ref(), row_index))
                .transpose()?;

            match mode {
                SearchMode::Scan => {
                    scan_rows.push(ResultRow {
                        ordinal: *ordinal,
                        batch: batch.clone(),
                        row_index,
                        row_id,
                        filter_pass,
                        distance: None,
                        text_score: None,
                        relevance_score: None,
                    });
                }
                SearchMode::Vector {
                    query_vector,
                    distance_type,
                    ..
                } => {
                    let distance = vector_distance(
                        &batch,
                        vector_index.expect("vector index present"),
                        row_index,
                        query_vector,
                        *distance_type,
                    )?;
                    vector_rows.push(ResultRow {
                        ordinal: *ordinal,
                        batch: batch.clone(),
                        row_index,
                        row_id,
                        filter_pass,
                        distance: Some(distance),
                        text_score: None,
                        relevance_score: None,
                    });
                }
                SearchMode::Text(plan) => {
                    let score = text_score(plan, &batch, row_index)?;
                    if score > 0.0 {
                        text_rows.push(ResultRow {
                            ordinal: *ordinal,
                            batch: batch.clone(),
                            row_index,
                            row_id,
                            filter_pass,
                            distance: None,
                            text_score: Some(score),
                            relevance_score: None,
                        });
                    }
                }
                SearchMode::Hybrid(plan) => {
                    let distance = vector_distance(
                        &batch,
                        vector_index.expect("vector index present"),
                        row_index,
                        &plan.query_vector,
                        plan.distance_type,
                    )?;
                    vector_rows.push(ResultRow {
                        ordinal: *ordinal,
                        batch: batch.clone(),
                        row_index,
                        row_id,
                        filter_pass,
                        distance: Some(distance),
                        text_score: None,
                        relevance_score: None,
                    });

                    let score = text_score(&plan.text, &batch, row_index)?;
                    if score > 0.0 {
                        text_rows.push(ResultRow {
                            ordinal: *ordinal,
                            batch: batch.clone(),
                            row_index,
                            row_id,
                            filter_pass,
                            distance: None,
                            text_score: Some(score),
                            relevance_score: None,
                        });
                    }
                }
            }

            *ordinal += 1;
        }

        Ok(())
    }

    async fn read_fragment_batches(
        &self,
        fragment: &Fragment,
        projection: &LanceSchema,
        with_row_id: bool,
    ) -> Result<Vec<RecordBatch>> {
        let (streams, num_rows) = self.open_fragment_streams(fragment, projection).await?;
        if streams.is_empty() {
            return Ok(vec![]);
        }

        let deletion_vector = match fragment.deletion_file.as_ref() {
            Some(deletion_file) => {
                if deletion_file.base_id.is_some() {
                    return Err(Error::NotSupported {
                        message:
                            "external deletion file bases are not yet supported in the browser path"
                                .to_string(),
                    });
                }
                Some(Arc::new(
                    read_deletion_file(
                        fragment.id,
                        deletion_file,
                        &self.table_path,
                        self.object_store.as_ref(),
                    )
                    .await
                    .map_err(Error::from)?,
                ))
            }
            None => None,
        };
        let row_id_sequence = if with_row_id {
            load_row_id_sequence(fragment, self.object_store.as_ref(), &self.table_path).await?
        } else {
            None
        };

        let data = if streams.len() == 1 {
            streams.into_iter().next().unwrap()
        } else {
            merge_streams(streams)
        };
        let config = RowIdAndDeletesConfig {
            params: ReadBatchParams::RangeFull,
            with_row_id,
            with_row_addr: false,
            with_row_last_updated_at_version: false,
            with_row_created_at_version: false,
            deletion_vector,
            row_id_sequence,
            last_updated_at_sequence: None,
            created_at_sequence: None,
            make_deletions_null: false,
            total_num_rows: num_rows,
        };

        wrap_with_row_id_and_delete(data, fragment.id as u32, config)
            .buffered(self.max_concurrent_ranges)
            .try_collect::<Vec<_>>()
            .await
            .map_err(Error::from)
    }

    async fn open_fragment_streams(
        &self,
        fragment: &Fragment,
        projection: &LanceSchema,
    ) -> Result<(Vec<ReadBatchTaskStream>, u32)> {
        let mut streams = Vec::new();
        let mut field_ids_in_files = HashSet::new();
        let mut num_rows = fragment.physical_rows.map(|rows| rows as u32);

        for data_file in &fragment.files {
            if data_file.base_id.is_some() {
                return Err(Error::NotSupported {
                    message: "external data file bases are not yet supported in the browser path"
                        .to_string(),
                });
            }
            if data_file.is_legacy_file() {
                return Err(Error::NotSupported {
                    message: "legacy Lance data files are not yet supported in the browser path"
                        .to_string(),
                });
            }

            let data_file_schema = data_file.schema(&self.manifest.schema);
            let schema_per_file = Arc::new(
                projection
                    .intersection_ignore_types(&data_file_schema)
                    .map_err(Error::from)?,
            );
            if schema_per_file.fields.is_empty() {
                continue;
            }
            field_ids_in_files.extend(
                schema_per_file
                    .field_ids()
                    .into_iter()
                    .filter(|field_id| *field_id >= 0),
            );

            let (stream, file_num_rows) = self
                .open_data_file_stream(data_file, schema_per_file)
                .await?;
            match num_rows {
                Some(existing) if existing != file_num_rows => {
                    return Err(Error::Runtime {
                        message: format!(
                            "fragment {} contained inconsistent file row counts ({existing} vs {file_num_rows})",
                            fragment.id
                        ),
                    });
                }
                None => num_rows = Some(file_num_rows),
                _ => {}
            }
            streams.push(stream);
        }

        let num_rows = num_rows.ok_or_else(|| Error::Runtime {
            message: format!(
                "fragment {} did not advertise a physical row count",
                fragment.id
            ),
        })?;

        let mut missing_fields = projection.field_ids();
        missing_fields.retain(|field_id| *field_id >= 0 && !field_ids_in_files.contains(field_id));
        if !missing_fields.is_empty() {
            let missing_projection = Arc::new(projection.project_by_ids(&missing_fields, true));
            streams.push(null_task_stream(
                missing_projection,
                num_rows,
                DEFAULT_BATCH_SIZE,
            ));
        }

        Ok((streams, num_rows))
    }

    async fn open_data_file_stream(
        &self,
        data_file: &DataFile,
        projection: Arc<LanceSchema>,
    ) -> Result<(ReadBatchTaskStream, u32)> {
        let path = self.table_path.child("data").child(data_file.path.as_str());
        let file_scheduler = self
            .scan_scheduler
            .open_file(&path, &data_file.file_size_bytes)
            .await
            .map_err(Error::from)?;
        let reader = FileReader::try_open(
            file_scheduler.clone(),
            None,
            Arc::default(),
            &self.metadata_cache,
            FileReaderOptions::default(),
        )
        .await
        .map_err(Error::from)?;
        let row_count = reader.metadata().num_rows as u32;

        let field_id_to_column_index = std::collections::BTreeMap::from_iter(
            data_file
                .fields
                .iter()
                .copied()
                .zip(data_file.column_indices.iter().copied())
                .filter_map(|(field_id, column_index)| {
                    (column_index >= 0).then_some((field_id as u32, column_index as u32))
                }),
        );
        let reader_projection = ReaderProjection::from_field_ids(
            reader.metadata().version(),
            projection.as_ref(),
            &field_id_to_column_index,
        )
        .map_err(Error::from)?;
        let stream = reader
            .read_tasks(
                ReadBatchParams::RangeFull,
                DEFAULT_BATCH_SIZE,
                Some(reader_projection),
                FilterExpression::no_filter(),
            )
            .map_err(Error::from)?
            .map(|task| ReadBatchTask {
                task: task.task,
                num_rows: task.num_rows,
            })
            .boxed();

        Ok((stream, row_count))
    }
}

#[derive(Debug, Clone)]
enum SearchMode {
    Scan,
    Vector {
        vector_column: String,
        query_vector: Vec<f32>,
        distance_type: SearchDistanceType,
    },
    Text(TextPlan),
    Hybrid(HybridPlan),
}

impl SearchMode {
    fn from_request(full_schema: &LanceSchema, request: &SearchRequest) -> Result<Self> {
        if request.fast_search.unwrap_or(false) {
            return Err(Error::NotSupported {
                message: "fastSearch is not supported in the browser execution path".to_string(),
            });
        }
        if matches!(request.distance_type, Some(SearchDistanceType::Hamming)) {
            return Err(Error::NotSupported {
                message:
                    "the browser path currently supports L2, cosine, and dot vector distance only"
                        .to_string(),
            });
        }
        match (&request.vector, &request.text) {
            (Some(vector), Some(text)) => Ok(Self::Hybrid(HybridPlan {
                vector_column: request.vector_column.clone().ok_or_else(|| {
                    Error::InvalidInput {
                        message: "vector searches require a vectorColumn".to_string(),
                    }
                })?,
                query_vector: vector.clone(),
                distance_type: request.distance_type.unwrap_or_default(),
                text: TextPlan::from_request(full_schema, text.clone())?,
            })),
            (Some(vector), None) => Ok(Self::Vector {
                vector_column: request.vector_column.clone().ok_or_else(|| {
                    Error::InvalidInput {
                        message: "vector searches require a vectorColumn".to_string(),
                    }
                })?,
                query_vector: vector.clone(),
                distance_type: request.distance_type.unwrap_or_default(),
            }),
            (None, Some(text)) => Ok(Self::Text(TextPlan::from_request(
                full_schema,
                text.clone(),
            )?)),
            (None, None) => Ok(Self::Scan),
        }
    }

    fn available_score_columns(&self) -> Vec<&'static str> {
        match self {
            Self::Scan => vec![],
            Self::Vector { .. } => vec![RESULT_DISTANCE_COLUMN],
            Self::Text(_) => vec![RESULT_SCORE_COLUMN],
            Self::Hybrid(_) => vec![
                RESULT_DISTANCE_COLUMN,
                RESULT_SCORE_COLUMN,
                RESULT_RELEVANCE_COLUMN,
            ],
        }
    }

    fn requires_internal_row_id(&self) -> bool {
        matches!(self, Self::Hybrid(_))
    }
}

#[derive(Debug, Clone)]
struct TextPlan {
    raw_query: String,
    columns: Vec<String>,
    terms: Vec<String>,
}

impl TextPlan {
    fn from_request(full_schema: &LanceSchema, request: TextRequest) -> Result<Self> {
        let (raw_query, columns) = match request {
            TextRequest::Query(query) => (query, string_columns(full_schema)),
            TextRequest::Structured { query, columns } => (
                query,
                columns.unwrap_or_else(|| string_columns(full_schema)),
            ),
        };
        let raw_query = raw_query.trim().to_string();
        if raw_query.is_empty() {
            return Err(Error::InvalidInput {
                message: "text searches require a non-empty query".to_string(),
            });
        }
        if columns.is_empty() {
            return Err(Error::InvalidInput {
                message: "text searches require at least one target column".to_string(),
            });
        }
        for column in &columns {
            let field = full_schema
                .field(column)
                .ok_or_else(|| Error::InvalidInput {
                    message: format!("unknown text search column '{column}'"),
                })?;
            if !matches!(field.data_type(), DataType::Utf8 | DataType::LargeUtf8) {
                return Err(Error::InvalidInput {
                    message: format!(
                        "text search column '{column}' must be Utf8 or LargeUtf8, found {:?}",
                        field.data_type()
                    ),
                });
            }
        }

        let mut terms = tokenize_text(&raw_query);
        if terms.is_empty() {
            terms.push(raw_query.to_ascii_lowercase());
        }

        Ok(Self {
            raw_query,
            columns,
            terms,
        })
    }
}

#[derive(Debug, Clone)]
struct HybridPlan {
    vector_column: String,
    query_vector: Vec<f32>,
    distance_type: SearchDistanceType,
    text: TextPlan,
}

#[derive(Debug, Clone)]
struct OutputPlan {
    items: Vec<OutputItem>,
    schema: ArrowSchema,
    scan_projection: LanceSchema,
    requires_scan_row_id: bool,
}

impl OutputPlan {
    fn new(
        full_schema: &LanceSchema,
        mode: &SearchMode,
        select: Option<&SelectRequest>,
        with_row_id: bool,
        filter_expr: Option<&Expr>,
    ) -> Result<Self> {
        let available_score_columns = mode
            .available_score_columns()
            .into_iter()
            .map(ToOwned::to_owned)
            .collect::<BTreeSet<_>>();

        let mut scan_columns = Vec::new();
        let mut items = Vec::new();
        let mut names = HashSet::new();

        match select {
            Some(SelectRequest::Columns(columns)) => {
                for column in columns {
                    append_column_or_score(
                        full_schema,
                        &available_score_columns,
                        &mut items,
                        &mut names,
                        column,
                    )?;
                    if !is_score_column(column) {
                        push_unique(&mut scan_columns, column.clone());
                    }
                }
            }
            Some(SelectRequest::Dynamic(columns)) => {
                for (alias, expression) in columns {
                    let expr = parse_expression(expression)?;
                    let data_type = infer_expression_type(
                        &expr,
                        &ArrowSchema::from(full_schema),
                        &available_score_columns,
                    )?;
                    let mut referenced = BTreeSet::new();
                    collect_referenced_columns(&expr, &mut referenced);
                    for column in referenced {
                        if is_score_column(&column) {
                            if !available_score_columns.contains(&column) {
                                return Err(Error::InvalidInput {
                                    message: format!(
                                        "browser select expression references score column '{column}' but that score is not available for this search"
                                    ),
                                });
                            }
                        } else {
                            push_unique(&mut scan_columns, column);
                        }
                    }
                    if names.insert(alias.clone()) {
                        items.push(OutputItem::Dynamic {
                            name: alias.clone(),
                            expr,
                            data_type,
                        });
                    }
                }
            }
            None => {
                for field in &full_schema.fields {
                    let name = field.name.clone();
                    push_unique(&mut scan_columns, name.clone());
                    if names.insert(name.clone()) {
                        items.push(OutputItem::Column {
                            name,
                            data_type: field.data_type().clone(),
                        });
                    }
                }
            }
        }

        if with_row_id {
            push_unique(&mut scan_columns, ROW_ID.to_string());
            if names.insert(ROW_ID.to_string()) {
                items.push(OutputItem::Column {
                    name: ROW_ID.to_string(),
                    data_type: DataType::UInt64,
                });
            }
        }

        for score_column in mode.available_score_columns() {
            if names.insert(score_column.to_string()) {
                items.push(OutputItem::Score {
                    name: score_column.to_string(),
                    data_type: DataType::Float32,
                });
            }
        }

        if let Some(filter_expr) = filter_expr {
            let mut referenced = BTreeSet::new();
            collect_referenced_columns(filter_expr, &mut referenced);
            for column in referenced {
                if is_score_column(&column) {
                    return Err(Error::NotSupported {
                        message: format!(
                            "browser filters do not currently support score column '{column}'"
                        ),
                    });
                }
                push_unique(&mut scan_columns, column);
            }
        }

        match mode {
            SearchMode::Vector { vector_column, .. } => {
                push_unique(&mut scan_columns, vector_column.clone());
            }
            SearchMode::Text(plan) => {
                for column in &plan.columns {
                    push_unique(&mut scan_columns, column.clone());
                }
            }
            SearchMode::Hybrid(plan) => {
                push_unique(&mut scan_columns, plan.vector_column.clone());
                for column in &plan.text.columns {
                    push_unique(&mut scan_columns, column.clone());
                }
            }
            SearchMode::Scan => {}
        }

        let requires_scan_row_id = with_row_id
            || mode.requires_internal_row_id()
            || scan_columns.iter().any(|column| column == ROW_ID);
        if requires_scan_row_id {
            push_unique(&mut scan_columns, ROW_ID.to_string());
        }

        let scan_projection = full_schema
            .project_preserve_system_columns(&scan_columns)
            .map_err(Error::from)?;
        let schema = ArrowSchema::new(items.iter().map(OutputItem::field).collect::<Vec<_>>());

        Ok(Self {
            items,
            schema,
            scan_projection,
            requires_scan_row_id,
        })
    }

    fn materialize_row(&self, row: &ResultRow) -> Result<RecordBatch> {
        let row_scores = RowScores {
            distance: row.distance,
            text_score: row.text_score,
            relevance_score: row.relevance_score,
        };
        let context = RowContext::new(&row.batch, row.row_index, row_scores);
        let columns = self
            .items
            .iter()
            .map(|item| item.materialize(&context, &row.batch, row.row_index))
            .collect::<Result<Vec<_>>>()?;
        RecordBatch::try_new_with_options(
            Arc::new(self.schema.clone()),
            columns,
            &RecordBatchOptions::new().with_row_count(Some(1)),
        )
        .map_err(Error::from)
    }
}

#[derive(Debug, Clone)]
enum OutputItem {
    Column {
        name: String,
        data_type: DataType,
    },
    Dynamic {
        name: String,
        expr: Expr,
        data_type: DataType,
    },
    Score {
        name: String,
        data_type: DataType,
    },
}

impl OutputItem {
    fn field(&self) -> Arc<ArrowField> {
        match self {
            Self::Column { name, data_type }
            | Self::Dynamic {
                name, data_type, ..
            }
            | Self::Score { name, data_type } => {
                Arc::new(ArrowField::new(name, data_type.clone(), true))
            }
        }
    }

    fn materialize(
        &self,
        context: &RowContext<'_>,
        batch: &RecordBatch,
        row_index: usize,
    ) -> Result<Arc<dyn Array>> {
        match self {
            Self::Column { name, .. } => {
                let index = batch.schema().index_of(name).map_err(Error::from)?;
                Ok(batch.column(index).slice(row_index, 1))
            }
            Self::Dynamic {
                expr, data_type, ..
            } => {
                let value = evaluate_expression(expr, context, Some(data_type))?;
                scalar_to_array(value, data_type)
            }
            Self::Score { name, data_type } => {
                let value = context.value_for(name)?;
                scalar_to_array(value, data_type)
            }
        }
    }
}

#[derive(Debug, Clone)]
struct ResultRow {
    ordinal: u64,
    batch: Arc<RecordBatch>,
    row_index: usize,
    row_id: Option<u64>,
    filter_pass: bool,
    distance: Option<f32>,
    text_score: Option<f32>,
    relevance_score: Option<f32>,
}

fn requested_limit(mode: &SearchMode, request_limit: Option<usize>) -> Option<usize> {
    match mode {
        SearchMode::Vector { .. } | SearchMode::Hybrid(_) => {
            Some(request_limit.unwrap_or(DEFAULT_VECTOR_LIMIT))
        }
        SearchMode::Text(_) | SearchMode::Scan => request_limit,
    }
}

fn apply_offset_limit(
    mut rows: Vec<ResultRow>,
    limit: Option<usize>,
    offset: usize,
) -> Vec<ResultRow> {
    if offset > 0 {
        rows = rows.into_iter().skip(offset).collect();
    }
    if let Some(limit) = limit {
        rows.truncate(limit);
    }
    rows
}

fn finalize_ranked_rows(
    mut rows: Vec<ResultRow>,
    limit: Option<usize>,
    offset: usize,
    postfilter: bool,
    comparator: impl Fn(&ResultRow, &ResultRow) -> Ordering,
) -> Vec<ResultRow> {
    rows.sort_by(comparator);
    let window = limit.map(|limit| limit.saturating_add(offset));
    let mut rows = if postfilter {
        take_window(rows, window)
            .into_iter()
            .filter(|row| row.filter_pass)
            .collect::<Vec<_>>()
    } else {
        rows.into_iter()
            .filter(|row| row.filter_pass)
            .collect::<Vec<_>>()
    };
    if !postfilter && let Some(window) = window {
        rows.truncate(window);
    }
    apply_offset_limit(rows, limit, offset)
}

fn finalize_hybrid_source_rows(
    mut rows: Vec<ResultRow>,
    window: usize,
    postfilter: bool,
    comparator: impl Fn(&ResultRow, &ResultRow) -> Ordering,
) -> Vec<ResultRow> {
    rows.sort_by(comparator);
    let mut rows = if postfilter {
        take_window(rows, Some(window))
            .into_iter()
            .filter(|row| row.filter_pass)
            .collect::<Vec<_>>()
    } else {
        rows.into_iter()
            .filter(|row| row.filter_pass)
            .collect::<Vec<_>>()
    };
    if !postfilter {
        rows.truncate(window);
    }
    rows
}

fn take_window(rows: Vec<ResultRow>, window: Option<usize>) -> Vec<ResultRow> {
    match window {
        Some(window) => rows.into_iter().take(window).collect(),
        None => rows,
    }
}

fn combine_hybrid_rows(
    vector_rows: Vec<ResultRow>,
    text_rows: Vec<ResultRow>,
    limit: usize,
) -> Result<Vec<ResultRow>> {
    let mut combined = std::collections::BTreeMap::<u64, ResultRow>::new();

    for (rank, row) in vector_rows.into_iter().enumerate() {
        let row_id = row.row_id.ok_or_else(|| Error::Runtime {
            message: "hybrid browser search requires row ids".to_string(),
        })?;
        let entry = combined.entry(row_id).or_insert(ResultRow {
            relevance_score: Some(0.0),
            ..row.clone()
        });
        entry.distance = row.distance;
        entry.filter_pass = entry.filter_pass && row.filter_pass;
        entry.relevance_score =
            Some(entry.relevance_score.unwrap_or(0.0) + 1.0 / (rank as f32 + HYBRID_RRF_K));
        entry.ordinal = entry.ordinal.min(row.ordinal);
    }

    for (rank, row) in text_rows.into_iter().enumerate() {
        let row_id = row.row_id.ok_or_else(|| Error::Runtime {
            message: "hybrid browser search requires row ids".to_string(),
        })?;
        let entry = combined.entry(row_id).or_insert(ResultRow {
            relevance_score: Some(0.0),
            ..row.clone()
        });
        entry.text_score = row.text_score;
        entry.filter_pass = entry.filter_pass && row.filter_pass;
        entry.relevance_score =
            Some(entry.relevance_score.unwrap_or(0.0) + 1.0 / (rank as f32 + HYBRID_RRF_K));
        entry.ordinal = entry.ordinal.min(row.ordinal);
    }

    let mut rows = combined.into_values().collect::<Vec<_>>();
    rows.sort_by(sort_by_relevance_score);
    rows.truncate(limit);
    Ok(rows)
}

fn sort_by_distance(left: &ResultRow, right: &ResultRow) -> Ordering {
    left.distance
        .expect("distance available")
        .total_cmp(&right.distance.expect("distance available"))
        .then_with(|| left.ordinal.cmp(&right.ordinal))
}

fn sort_by_distance_desc(left: &ResultRow, right: &ResultRow) -> Ordering {
    right
        .distance
        .expect("distance available")
        .total_cmp(&left.distance.expect("distance available"))
        .then_with(|| left.ordinal.cmp(&right.ordinal))
}

fn sort_vector_rows(distance_type: SearchDistanceType) -> fn(&ResultRow, &ResultRow) -> Ordering {
    match distance_type {
        SearchDistanceType::L2 | SearchDistanceType::Cosine => sort_by_distance,
        SearchDistanceType::Dot => sort_by_distance_desc,
        SearchDistanceType::Hamming => sort_by_distance,
    }
}

fn sort_by_text_score(left: &ResultRow, right: &ResultRow) -> Ordering {
    right
        .text_score
        .expect("text score available")
        .total_cmp(&left.text_score.expect("text score available"))
        .then_with(|| left.ordinal.cmp(&right.ordinal))
}

fn sort_by_relevance_score(left: &ResultRow, right: &ResultRow) -> Ordering {
    right
        .relevance_score
        .expect("relevance score available")
        .total_cmp(&left.relevance_score.expect("relevance score available"))
        .then_with(|| left.ordinal.cmp(&right.ordinal))
}

fn append_column_or_score(
    full_schema: &LanceSchema,
    available_score_columns: &BTreeSet<String>,
    items: &mut Vec<OutputItem>,
    names: &mut HashSet<String>,
    column: &str,
) -> Result<()> {
    if !names.insert(column.to_string()) {
        return Ok(());
    }

    if is_score_column(column) {
        if !available_score_columns.contains(column) {
            return Err(Error::InvalidInput {
                message: format!(
                    "browser search cannot project score column '{column}' for this search"
                ),
            });
        }
        items.push(OutputItem::Score {
            name: column.to_string(),
            data_type: DataType::Float32,
        });
        return Ok(());
    }

    let field = full_schema
        .field(column)
        .ok_or_else(|| Error::InvalidInput {
            message: format!("unknown projection column '{column}'"),
        })?;
    items.push(OutputItem::Column {
        name: column.to_string(),
        data_type: field.data_type().clone(),
    });
    Ok(())
}

fn push_unique(columns: &mut Vec<String>, column: String) {
    if !columns.iter().any(|existing| existing == &column) {
        columns.push(column);
    }
}

fn is_score_column(name: &str) -> bool {
    matches!(
        name,
        RESULT_DISTANCE_COLUMN | RESULT_SCORE_COLUMN | RESULT_RELEVANCE_COLUMN
    )
}

fn string_columns(schema: &LanceSchema) -> Vec<String> {
    schema
        .fields
        .iter()
        .filter(|field| matches!(field.data_type(), DataType::Utf8 | DataType::LargeUtf8))
        .map(|field| field.name.clone())
        .collect()
}

fn tokenize_text(query: &str) -> Vec<String> {
    query
        .split(|character: char| !character.is_alphanumeric())
        .filter(|term| !term.is_empty())
        .map(|term| term.to_ascii_lowercase())
        .collect()
}

fn text_score(plan: &TextPlan, batch: &RecordBatch, row_index: usize) -> Result<f32> {
    let mut score = 0_u32;
    let query_lower = plan.raw_query.to_ascii_lowercase();
    for column in &plan.columns {
        let index = batch.schema().index_of(column).map_err(Error::from)?;
        let column = batch.column(index);
        if column.is_null(row_index) {
            continue;
        }
        let value = extract_text(column.as_ref(), row_index)?;
        let lower = value.to_ascii_lowercase();
        if lower.contains(&query_lower) {
            score = score.saturating_add(2);
        }
        for term in &plan.terms {
            score = score.saturating_add(count_occurrences(&lower, term) as u32);
        }
    }
    Ok(score as f32)
}

fn count_occurrences(haystack: &str, needle: &str) -> usize {
    if needle.is_empty() {
        return 0;
    }
    let mut count = 0;
    let mut remainder = haystack;
    while let Some(index) = remainder.find(needle) {
        count += 1;
        remainder = &remainder[index + needle.len()..];
    }
    count
}

fn extract_text(column: &dyn Array, row_index: usize) -> Result<&str> {
    match column.data_type() {
        DataType::Utf8 => column
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .map(|array| array.value(row_index))
            .ok_or_else(|| Error::Runtime {
                message: "failed to decode Utf8 text column".to_string(),
            }),
        DataType::LargeUtf8 => column
            .as_any()
            .downcast_ref::<arrow_array::LargeStringArray>()
            .map(|array| array.value(row_index))
            .ok_or_else(|| Error::Runtime {
                message: "failed to decode LargeUtf8 text column".to_string(),
            }),
        other => Err(Error::InvalidInput {
            message: format!("expected a text column but found {other:?}"),
        }),
    }
}

fn extract_row_id(column: &dyn Array, row_index: usize) -> Result<u64> {
    if column.is_null(row_index) {
        return Err(Error::Runtime {
            message: "encountered a null row id in browser search".to_string(),
        });
    }
    column
        .as_any()
        .downcast_ref::<arrow_array::UInt64Array>()
        .map(|array| array.value(row_index))
        .ok_or_else(|| Error::Runtime {
            message: "failed to decode UInt64 row id column".to_string(),
        })
}

fn vector_distance(
    batch: &RecordBatch,
    vector_index: usize,
    row_index: usize,
    query_vector: &[f32],
    distance_type: SearchDistanceType,
) -> Result<f32> {
    let vectors = batch
        .column(vector_index)
        .as_any()
        .downcast_ref::<FixedSizeListArray>()
        .ok_or_else(|| Error::NotSupported {
            message:
                "the browser path currently supports FixedSizeList<Float32> vector columns only"
                    .to_string(),
        })?;
    if vectors.is_null(row_index) {
        return Err(Error::NotSupported {
            message: "vector rows with null values are not yet supported in the browser path"
                .to_string(),
        });
    }
    let values = vectors
        .values()
        .as_any()
        .downcast_ref::<Float32Array>()
        .ok_or_else(|| Error::NotSupported {
            message: "the browser path currently supports Float32 vector columns only".to_string(),
        })?;
    let dimension = vectors.value_length() as usize;
    if dimension != query_vector.len() {
        return Err(Error::InvalidInput {
            message: format!(
                "query vector has dimension {}, but column '{}' has dimension {}",
                query_vector.len(),
                batch.schema().field(vector_index).name(),
                dimension
            ),
        });
    }

    let base_offset = row_index * dimension;
    let mut dot = 0.0_f32;
    let mut value_norm = 0.0_f32;
    let mut query_norm = 0.0_f32;
    let mut l2 = 0.0_f32;
    for (index, expected) in query_vector.iter().enumerate() {
        let value_index = base_offset + index;
        if values.is_null(value_index) {
            return Err(Error::NotSupported {
                message: "vector rows with null elements are not yet supported in the browser path"
                    .to_string(),
            });
        }
        let value = values.value(value_index);
        let delta = value - expected;
        dot += value * expected;
        value_norm += value * value;
        query_norm += expected * expected;
        l2 += delta * delta;
    }

    match distance_type {
        SearchDistanceType::L2 => Ok(l2),
        SearchDistanceType::Cosine => {
            if value_norm == 0.0 || query_norm == 0.0 {
                return Err(Error::InvalidInput {
                    message: "cosine distance requires non-zero vectors".to_string(),
                });
            }
            Ok(1.0 - dot / (value_norm.sqrt() * query_norm.sqrt()))
        }
        SearchDistanceType::Dot => Ok(dot),
        SearchDistanceType::Hamming => Err(Error::NotSupported {
            message: "the browser path currently supports L2, cosine, and dot vector distance only"
                .to_string(),
        }),
    }
}

fn null_task_stream(
    projection: Arc<LanceSchema>,
    num_rows: u32,
    batch_size: u32,
) -> ReadBatchTaskStream {
    let schema = Arc::new(ArrowSchema::from(projection.as_ref()));
    let mut remaining_rows = num_rows as usize;

    let tasks = std::iter::from_fn(move || {
        if remaining_rows == 0 {
            return None;
        }

        let this_batch_size = remaining_rows.min(batch_size as usize);
        remaining_rows -= this_batch_size;
        let columns = schema
            .fields()
            .iter()
            .map(|field| arrow_array::new_null_array(field.data_type(), this_batch_size))
            .collect::<Vec<_>>();
        let batch = RecordBatch::try_new_with_options(
            schema.clone(),
            columns,
            &RecordBatchOptions::new().with_row_count(Some(this_batch_size)),
        )
        .expect("null batch construction should succeed");

        Some(ReadBatchTask {
            task: futures::future::ready(Ok(batch)).boxed(),
            num_rows: this_batch_size as u32,
        })
    })
    .collect::<Vec<_>>();
    futures::stream::iter(tasks).boxed()
}

async fn load_row_id_sequence(
    fragment: &Fragment,
    object_store: &ObjectStore,
    table_path: &Path,
) -> Result<Option<Arc<RowIdSequence>>> {
    match &fragment.row_id_meta {
        None => Ok(None),
        Some(RowIdMeta::Inline(data)) => {
            Ok(Some(Arc::new(read_row_ids(data).map_err(Error::from)?)))
        }
        Some(RowIdMeta::External(file_slice)) => {
            let path = table_path.child(file_slice.path.as_str());
            let range = file_slice.offset as usize..(file_slice.offset + file_slice.size) as usize;
            let bytes = object_store
                .open(&path)
                .await
                .map_err(Error::from)?
                .get_range(range)
                .await
                .map_err(Error::from)?;
            Ok(Some(Arc::new(read_row_ids(&bytes).map_err(Error::from)?)))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::SocketAddr;

    use arrow_array::cast::AsArray;
    use arrow_array::types::Float32Type;
    use arrow_array::{FixedSizeListArray, Int32Array, StringArray};
    use arrow_schema::{Field, Schema};
    use axum::{Router, serve};
    use lancedb::connect;
    use lancedb::index::Index;
    use lancedb::index::scalar::FtsIndexBuilder;
    use tempfile::TempDir;
    use tokio::net::TcpListener;
    use tower_http::services::ServeDir;

    struct TestFixture {
        _root: TempDir,
        base_url: String,
        _server_task: tokio::task::JoinHandle<()>,
    }

    impl TestFixture {
        async fn new() -> Self {
            let root = tempfile::tempdir().unwrap();
            let db = connect(root.path().to_str().unwrap())
                .execute()
                .await
                .unwrap();
            let table = db
                .create_table("search_table", make_batch())
                .execute()
                .await
                .unwrap();
            table
                .create_index(&["doc"], Index::FTS(FtsIndexBuilder::default()))
                .execute()
                .await
                .unwrap();

            let app = Router::new().fallback_service(ServeDir::new(root.path()));
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr: SocketAddr = listener.local_addr().unwrap();
            let server_task = tokio::spawn(async move {
                serve(listener, app).await.unwrap();
            });

            Self {
                _root: root,
                base_url: format!("http://{addr}"),
                _server_task: server_task,
            }
        }

        fn table_url(&self) -> String {
            format!("{}/search_table.lance/", self.base_url)
        }
    }

    fn make_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new(
                "vector",
                DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), 2),
                true,
            ),
            Field::new("doc", DataType::Utf8, false),
        ]));

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(
                    FixedSizeListArray::from_iter_primitive::<Float32Type, _, _>(
                        [
                            Some(vec![Some(0.0), Some(0.0)]),
                            Some(vec![Some(1.0), Some(1.0)]),
                            Some(vec![Some(0.1), Some(0.1)]),
                        ],
                        2,
                    ),
                ),
                Arc::new(StringArray::from(vec![
                    "apple pie",
                    "banana split",
                    "apple tart",
                ])),
            ],
        )
        .unwrap()
    }

    fn vector_request() -> SearchRequest {
        SearchRequest {
            vector: Some(vec![0.0, 0.0]),
            text: None,
            distance_type: None,
            filter: None,
            select: None,
            limit: None,
            offset: None,
            vector_column: Some("vector".into()),
            prefilter: None,
            with_row_id: None,
            fast_search: None,
        }
    }

    #[tokio::test]
    async fn browser_path_supports_filter_scan_queries() {
        let fixture = TestFixture::new().await;
        let table = BrowserTable::open(
            &fixture.table_url(),
            &OpenTableOptions::default(),
            &format!("{}_latest.manifest", fixture.table_url()),
        )
        .await
        .unwrap();

        let batches = table
            .search_batches(SearchRequest {
                vector: None,
                text: None,
                distance_type: None,
                filter: Some("id >= 2".into()),
                select: Some(SelectRequest::Columns(vec!["id".into()])),
                limit: Some(1),
                offset: Some(1),
                vector_column: None,
                prefilter: None,
                with_row_id: None,
                fast_search: None,
            })
            .await
            .unwrap();

        let ids = batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column_by_name("id")
                    .unwrap()
                    .as_primitive::<arrow_array::types::Int32Type>()
                    .values()
                    .iter()
                    .copied()
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        assert_eq!(ids, vec![3]);
    }

    #[tokio::test]
    async fn browser_path_supports_text_and_hybrid_queries() {
        let fixture = TestFixture::new().await;
        let table = BrowserTable::open(
            &fixture.table_url(),
            &OpenTableOptions::default(),
            &format!("{}_latest.manifest", fixture.table_url()),
        )
        .await
        .unwrap();

        let text_batches = table
            .search_batches(SearchRequest {
                vector: None,
                text: Some(TextRequest::Structured {
                    query: "apple".into(),
                    columns: Some(vec!["doc".into()]),
                }),
                distance_type: None,
                filter: None,
                select: Some(SelectRequest::Columns(vec!["id".into(), "doc".into()])),
                limit: Some(2),
                offset: None,
                vector_column: None,
                prefilter: None,
                with_row_id: None,
                fast_search: None,
            })
            .await
            .unwrap();
        assert!(
            text_batches[0]
                .column_by_name(RESULT_SCORE_COLUMN)
                .is_some()
        );

        let hybrid_batches = table
            .search_batches(SearchRequest {
                vector: Some(vec![0.0, 0.0]),
                text: Some(TextRequest::Structured {
                    query: "apple".into(),
                    columns: Some(vec!["doc".into()]),
                }),
                distance_type: None,
                filter: None,
                select: Some(SelectRequest::Columns(vec!["id".into()])),
                limit: Some(2),
                offset: None,
                vector_column: Some("vector".into()),
                prefilter: Some(true),
                with_row_id: None,
                fast_search: None,
            })
            .await
            .unwrap();
        assert!(
            hybrid_batches[0]
                .column_by_name(RESULT_RELEVANCE_COLUMN)
                .is_some()
        );
    }

    #[tokio::test]
    async fn browser_path_supports_dynamic_selects() {
        let fixture = TestFixture::new().await;
        let table = BrowserTable::open(
            &fixture.table_url(),
            &OpenTableOptions::default(),
            &format!("{}_latest.manifest", fixture.table_url()),
        )
        .await
        .unwrap();

        let batches = table
            .search_batches(SearchRequest {
                vector: None,
                text: None,
                distance_type: None,
                filter: Some("doc LIKE '%apple%'".into()),
                select: Some(SelectRequest::Dynamic(std::collections::BTreeMap::from([
                    ("id2".into(), "id * 2".into()),
                    ("score_plus".into(), "CAST(1 AS INT) + 1".into()),
                ]))),
                limit: Some(1),
                offset: None,
                vector_column: None,
                prefilter: None,
                with_row_id: None,
                fast_search: None,
            })
            .await
            .unwrap();

        let batch = &batches[0];
        let id2 = batch
            .column_by_name("id2")
            .unwrap()
            .as_primitive::<arrow_array::types::Int32Type>()
            .value(0);
        let score_plus = batch
            .column_by_name("score_plus")
            .unwrap()
            .as_primitive::<arrow_array::types::Int32Type>()
            .value(0);
        assert_eq!(id2, 2);
        assert_eq!(score_plus, 2);
    }

    #[test]
    fn browser_path_rejects_fast_search() {
        let schema = LanceSchema::try_from(make_batch().schema().as_ref()).unwrap();
        let mut request = vector_request();
        request.fast_search = Some(true);

        let error = SearchMode::from_request(&schema, &request).unwrap_err();

        assert!(matches!(error, Error::NotSupported { .. }));
        assert!(error.to_string().contains("fastSearch"));
    }

    #[test]
    fn browser_path_rejects_hamming_distance() {
        let schema = LanceSchema::try_from(make_batch().schema().as_ref()).unwrap();
        let mut request = vector_request();
        request.distance_type = Some(SearchDistanceType::Hamming);

        let error = SearchMode::from_request(&schema, &request).unwrap_err();

        assert!(matches!(error, Error::NotSupported { .. }));
        assert!(error.to_string().contains("L2, cosine, and dot"));
    }

    #[test]
    fn hybrid_pagination_applies_offset_after_fusion() {
        let empty_batch = Arc::new(RecordBatch::new_empty(Arc::new(Schema::empty())));
        let make_row = |row_id: u64,
                        ordinal: u64,
                        distance: Option<f32>,
                        text_score: Option<f32>|
         -> ResultRow {
            ResultRow {
                ordinal,
                batch: empty_batch.clone(),
                row_index: 0,
                row_id: Some(row_id),
                filter_pass: true,
                distance,
                text_score,
                relevance_score: None,
            }
        };

        let ranked_vector = finalize_hybrid_source_rows(
            vec![
                make_row(10, 0, Some(0.0), None),
                make_row(20, 1, Some(1.0), None),
            ],
            2,
            false,
            sort_by_distance,
        );
        let ranked_text = finalize_hybrid_source_rows(
            vec![
                make_row(20, 1, None, Some(10.0)),
                make_row(10, 0, None, Some(9.0)),
            ],
            2,
            false,
            sort_by_text_score,
        );

        let fused = combine_hybrid_rows(ranked_vector, ranked_text, 2).unwrap();
        let page = apply_offset_limit(fused, Some(1), 1);

        assert_eq!(page.len(), 1);
        assert_eq!(page[0].row_id, Some(20));
    }
}
