// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

#![allow(clippy::redundant_pub_crate)]

use std::{collections::BTreeMap, sync::Arc};

use arrow_array::cast::AsArray;
use arrow_array::{
    Array, ArrayRef, LargeStringArray, RecordBatch, StringArray, StringViewArray, UInt32Array,
    UInt64Array,
};
use arrow_schema::{DataType, Field as ArrowField, Schema as ArrowSchema};
use datafusion::error::DataFusionError;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use futures::StreamExt;
use lance_core::{
    ROW_ID,
    datatypes::{Field, LogicalType, Schema, format_field_path, parse_field_path},
};
use lance_index::metrics::NoOpMetricsCollector;
use lance_index::pbold::InvertedIndexDetails;
use lance_index::scalar::index_files_to_table;
use lance_index::scalar::inverted::{
    DocumentGranularity, InvertedIndex, InvertedIndexParams, doc_index_storage_column,
};
use lance_index::scalar::lance_format::LanceIndexStore;
use lance_index::scalar::registry::VALUE_COLUMN_NAME;
use lance_table::format::IndexMetadata;
use prost::Message;
use roaring::RoaringBitmap;
use uuid::Uuid;

use crate::{
    Dataset, Error, Result,
    dataset::index::LanceIndexStoreExt,
    index::{DatasetIndexExt, scalar::fetch_index_details},
};

#[derive(Debug, Clone)]
enum FtsTraversal {
    Text,
    Struct {
        child_index: usize,
        child: Box<Self>,
    },
    List {
        child: Box<Self>,
    },
}

impl FtsTraversal {
    fn list_depth(&self) -> usize {
        match self {
            Self::Text => 0,
            Self::Struct { child, .. } => child.list_depth(),
            Self::List { child } => 1 + child.list_depth(),
        }
    }
}

/// Schema-derived form of an FTS field path.
#[derive(Debug, Clone)]
pub(crate) struct ResolvedFtsField {
    pub final_field_id: i32,
    pub root_column: String,
    pub canonical_path: String,
    pub document_granularity: DocumentGranularity,
    traversal: FtsTraversal,
    list_depth: usize,
}

/// One logical document extracted from a dataset row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FtsDocument {
    pub row_index: usize,
    pub text: String,
    pub doc_index: Vec<u32>,
}

impl ResolvedFtsField {
    pub fn has_lists(&self) -> bool {
        self.list_depth > 0
    }

    pub fn coordinate_rank(&self) -> usize {
        if self.document_granularity.is_list_element() {
            self.list_depth
        } else {
            0
        }
    }

    pub fn documents_from_batch(&self, batch: &RecordBatch) -> Result<Vec<FtsDocument>> {
        let column = batch.column_by_name(&self.root_column).ok_or_else(|| {
            Error::invalid_input(format!(
                "FTS root column '{}' is missing from the input batch",
                self.root_column
            ))
        })?;
        self.documents_from_array(column, batch.num_rows())
    }

    fn documents_from_array(&self, column: &ArrayRef, num_rows: usize) -> Result<Vec<FtsDocument>> {
        let mut documents = Vec::new();
        match self.document_granularity {
            DocumentGranularity::Row => {
                documents.reserve(num_rows);
                for row_index in 0..num_rows {
                    let mut text = String::new();
                    append_row_text(&self.traversal, column.as_ref(), row_index, &mut text)?;
                    documents.push(FtsDocument {
                        row_index,
                        text,
                        doc_index: Vec::new(),
                    });
                }
            }
            DocumentGranularity::ListElement => {
                for row_index in 0..num_rows {
                    collect_element_documents(
                        &self.traversal,
                        column.as_ref(),
                        row_index,
                        row_index,
                        0,
                        self.list_depth,
                        &mut Vec::with_capacity(self.list_depth),
                        &mut documents,
                    )?;
                }
            }
        }
        Ok(documents)
    }
}

fn string_value(array: &dyn Array, index: usize) -> Result<Option<&str>> {
    if array.is_null(index) {
        return Ok(None);
    }
    match array.data_type() {
        DataType::Utf8 => Ok(Some(
            array
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("Utf8 array type")
                .value(index),
        )),
        DataType::LargeUtf8 => Ok(Some(
            array
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .expect("LargeUtf8 array type")
                .value(index),
        )),
        DataType::Utf8View => Ok(Some(
            array
                .as_any()
                .downcast_ref::<StringViewArray>()
                .expect("Utf8View array type")
                .value(index),
        )),
        data_type => Err(Error::internal(format!(
            "FTS traversal expected a string array, got {data_type}"
        ))),
    }
}

fn append_text(output: &mut String, text: &str) {
    if !output.is_empty() {
        output.push(' ');
    }
    output.push_str(text);
}

fn append_row_text(
    traversal: &FtsTraversal,
    array: &dyn Array,
    index: usize,
    output: &mut String,
) -> Result<()> {
    match traversal {
        FtsTraversal::Text => {
            if let Some(text) = string_value(array, index)? {
                append_text(output, text);
            }
        }
        FtsTraversal::Struct { child_index, child } => {
            if !array.is_null(index) {
                let structs = array.as_struct();
                append_row_text(child, structs.column(*child_index).as_ref(), index, output)?;
            }
        }
        FtsTraversal::List { child } => match array.data_type() {
            DataType::List(_) => {
                let lists = array.as_list::<i32>();
                if !lists.is_null(index) {
                    let offsets = lists.value_offsets();
                    let start = offsets[index] as usize;
                    let end = offsets[index + 1] as usize;
                    for element_index in start..end {
                        append_row_text(child, lists.values().as_ref(), element_index, output)?;
                    }
                }
            }
            DataType::LargeList(_) => {
                let lists = array.as_list::<i64>();
                if !lists.is_null(index) {
                    let offsets = lists.value_offsets();
                    let start = offsets[index] as usize;
                    let end = offsets[index + 1] as usize;
                    for element_index in start..end {
                        append_row_text(child, lists.values().as_ref(), element_index, output)?;
                    }
                }
            }
            data_type => {
                return Err(Error::internal(format!(
                    "FTS traversal expected a list array, got {data_type}"
                )));
            }
        },
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn collect_element_documents(
    traversal: &FtsTraversal,
    array: &dyn Array,
    index: usize,
    row_index: usize,
    list_depth: usize,
    boundary_depth: usize,
    doc_index: &mut Vec<u32>,
    documents: &mut Vec<FtsDocument>,
) -> Result<()> {
    match traversal {
        FtsTraversal::Text => Err(Error::internal(
            "ListElement FTS traversal did not encounter its list boundary".to_string(),
        )),
        FtsTraversal::Struct { child_index, child } => {
            if !array.is_null(index) {
                let structs = array.as_struct();
                collect_element_documents(
                    child,
                    structs.column(*child_index).as_ref(),
                    index,
                    row_index,
                    list_depth,
                    boundary_depth,
                    doc_index,
                    documents,
                )?;
            }
            Ok(())
        }
        FtsTraversal::List { child } => {
            let mut visit = |values: &ArrayRef, start: usize, end: usize| -> Result<()> {
                for (ordinal, element_index) in (start..end).enumerate() {
                    let ordinal = u32::try_from(ordinal).map_err(|_| {
                        Error::invalid_input(format!(
                            "FTS element ordinal overflow for row index {row_index}"
                        ))
                    })?;
                    doc_index.push(ordinal);
                    if list_depth + 1 == boundary_depth {
                        let mut text = String::new();
                        append_row_text(child, values.as_ref(), element_index, &mut text)?;
                        documents.push(FtsDocument {
                            row_index,
                            text,
                            doc_index: doc_index.clone(),
                        });
                    } else {
                        collect_element_documents(
                            child,
                            values.as_ref(),
                            element_index,
                            row_index,
                            list_depth + 1,
                            boundary_depth,
                            doc_index,
                            documents,
                        )?;
                    }
                    doc_index.pop();
                }
                Ok(())
            };
            match array.data_type() {
                DataType::List(_) => {
                    let lists = array.as_list::<i32>();
                    if !lists.is_null(index) {
                        let offsets = lists.value_offsets();
                        visit(
                            lists.values(),
                            offsets[index] as usize,
                            offsets[index + 1] as usize,
                        )?;
                    }
                }
                DataType::LargeList(_) => {
                    let lists = array.as_list::<i64>();
                    if !lists.is_null(index) {
                        let offsets = lists.value_offsets();
                        visit(
                            lists.values(),
                            offsets[index] as usize,
                            offsets[index + 1] as usize,
                        )?;
                    }
                }
                data_type => {
                    return Err(Error::internal(format!(
                        "FTS traversal expected a list array, got {data_type}"
                    )));
                }
            }
            Ok(())
        }
    }
}

fn find_child_case_insensitive<'a>(field: &'a Field, name: &str) -> Option<(usize, &'a Field)> {
    field
        .children
        .iter()
        .enumerate()
        .find(|(_, child)| child.name == name)
        .or_else(|| {
            field
                .children
                .iter()
                .enumerate()
                .find(|(_, child)| child.name.eq_ignore_ascii_case(name))
        })
}

fn build_fts_traversal(
    field: &Field,
    remaining_names: &[String],
    canonical_names: &mut Vec<String>,
    final_field_id: &mut i32,
) -> Result<FtsTraversal> {
    match field.data_type() {
        DataType::List(_) | DataType::LargeList(_) => {
            let child = field.children.first().ok_or_else(|| {
                Error::invalid_input(format!(
                    "FTS list field '{}' does not have an item field",
                    field.name
                ))
            })?;
            Ok(FtsTraversal::List {
                child: Box::new(build_fts_traversal(
                    child,
                    remaining_names,
                    canonical_names,
                    final_field_id,
                )?),
            })
        }
        DataType::Struct(_) => {
            let Some((name, rest)) = remaining_names.split_first() else {
                return Err(Error::invalid_input(format!(
                    "FTS field path ends at struct '{}'; specify the final text field",
                    field.name
                )));
            };
            let (child_index, child) =
                find_child_case_insensitive(field, name).ok_or_else(|| {
                    Error::index(format!(
                        "FTS field '{}' does not contain child '{}'",
                        field.name, name
                    ))
                })?;
            canonical_names.push(child.name.clone());
            *final_field_id = child.id;
            Ok(FtsTraversal::Struct {
                child_index,
                child: Box::new(build_fts_traversal(
                    child,
                    rest,
                    canonical_names,
                    final_field_id,
                )?),
            })
        }
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View if remaining_names.is_empty() => {
            Ok(FtsTraversal::Text)
        }
        _ if field.logical_type == LogicalType::from("json") && remaining_names.is_empty() => {
            Ok(FtsTraversal::Text)
        }
        data_type if !remaining_names.is_empty() => Err(Error::invalid_input(format!(
            "FTS field '{}' has type {data_type} and cannot contain child '{}'",
            field.name, remaining_names[0]
        ))),
        data_type => Err(Error::invalid_input(format!(
            "FTS field '{}' must resolve to Utf8, LargeUtf8, Utf8View, or JSON, got {data_type}",
            field.name
        ))),
    }
}

/// Resolve a public FTS field path and derive all list traversal from schema.
pub(crate) fn resolve_fts_field(
    schema: &Schema,
    path: &str,
    document_granularity: DocumentGranularity,
) -> Result<ResolvedFtsField> {
    let names = parse_field_path(path)?;
    let (root_name, remaining_names) = names
        .split_first()
        .ok_or_else(|| Error::invalid_input("FTS field path cannot be empty".to_string()))?;
    let root = schema
        .fields
        .iter()
        .find(|field| field.name == *root_name)
        .or_else(|| {
            schema
                .fields
                .iter()
                .find(|field| field.name.eq_ignore_ascii_case(root_name))
        })
        .ok_or_else(|| {
            Error::index(format!(
                "FTS field path '{path}' does not exist in the dataset schema"
            ))
        })?;

    let mut canonical_names = vec![root.name.clone()];
    let mut final_field_id = root.id;
    let traversal = build_fts_traversal(
        root,
        remaining_names,
        &mut canonical_names,
        &mut final_field_id,
    )?;
    let list_depth = traversal.list_depth();
    if document_granularity.is_list_element() && list_depth == 0 {
        return Err(Error::invalid_input(format!(
            "FTS field path '{}' has no List layer and cannot use ListElement document granularity",
            format_field_path(
                &canonical_names
                    .iter()
                    .map(String::as_str)
                    .collect::<Vec<_>>()
            )
        )));
    }
    if list_depth > 0 && root.logical_type == LogicalType::from("json") {
        return Err(Error::invalid_input(
            "nested List traversal is not supported for JSON FTS sources".to_string(),
        ));
    }
    let canonical_path = format_field_path(
        &canonical_names
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>(),
    );
    Ok(ResolvedFtsField {
        final_field_id,
        root_column: root.name.clone(),
        canonical_path,
        document_granularity,
        traversal,
        list_depth,
    })
}

fn find_public_path_by_id(field: &Field, field_id: i32, path: &mut Vec<String>) -> bool {
    if field.id == field_id {
        return true;
    }
    match field.data_type() {
        DataType::List(_) | DataType::LargeList(_) => field
            .children
            .first()
            .is_some_and(|child| find_physical_path_by_id(child, field_id, path)),
        DataType::Struct(_) => field.children.iter().any(|child| {
            path.push(child.name.clone());
            let found = find_public_path_by_id(child, field_id, path);
            if !found {
                path.pop();
            }
            found
        }),
        _ => false,
    }
}

fn find_physical_path_by_id(field: &Field, field_id: i32, path: &mut Vec<String>) -> bool {
    if field.id == field_id {
        return false;
    }
    match field.data_type() {
        DataType::List(_) | DataType::LargeList(_) => field
            .children
            .first()
            .is_some_and(|child| find_physical_path_by_id(child, field_id, path)),
        DataType::Struct(_) => field.children.iter().any(|child| {
            path.push(child.name.clone());
            let found = find_public_path_by_id(child, field_id, path);
            if !found {
                path.pop();
            }
            found
        }),
        _ => false,
    }
}

/// Resolve an indexed final field id against the current schema, preserving
/// routing across field renames.
pub(crate) fn resolve_fts_field_by_id(
    schema: &Schema,
    field_id: i32,
    document_granularity: DocumentGranularity,
) -> Result<ResolvedFtsField> {
    for root in &schema.fields {
        let mut path = vec![root.name.clone()];
        if find_public_path_by_id(root, field_id, &mut path) {
            let path_refs = path.iter().map(String::as_str).collect::<Vec<_>>();
            return resolve_fts_field(schema, &format_field_path(&path_refs), document_granularity);
        }
    }
    Err(Error::invalid_input(format!(
        "FTS index refers to missing or Arrow-internal field id {field_id}"
    )))
}

/// Return the persisted FTS document granularity for each logical index on a
/// public field path. Multiple physical segments with the same index name are
/// collapsed after validating that their metadata agrees.
pub(crate) async fn indexed_fts_document_granularities(
    dataset: &Dataset,
    column: &str,
) -> Result<Vec<(String, DocumentGranularity)>> {
    let resolved = resolve_fts_field(dataset.schema(), column, DocumentGranularity::Row)?;
    let mut by_name = BTreeMap::new();

    for index in dataset.load_indices().await?.iter() {
        if index.fields.as_slice() != [resolved.final_field_id] {
            continue;
        }
        let details_any = fetch_index_details(dataset, &resolved.canonical_path, index).await?;
        if !details_any.type_url.ends_with("InvertedIndexDetails") {
            continue;
        }
        let details =
            InvertedIndexDetails::decode(details_any.value.as_slice()).map_err(|error| {
                Error::corrupt_file(
                    dataset.indices_dir().join(index.uuid.to_string()),
                    format!(
                        "failed to decode InvertedIndexDetails for FTS index '{}': {error}",
                        index.name
                    ),
                )
            })?;
        let document_granularity = DocumentGranularity::try_from(details.document_granularity)?;
        if let Some(existing) = by_name.insert(index.name.clone(), document_granularity)
            && existing != document_granularity
        {
            return Err(Error::corrupt_file(
                dataset.indices_dir().join(index.uuid.to_string()),
                format!(
                    "FTS index '{}' has inconsistent document granularity across segments: \
                     {existing:?} and {document_granularity:?}",
                    index.name
                ),
            ));
        }
    }

    Ok(by_name.into_iter().collect())
}

/// Resolve an optional query granularity against persisted FTS index metadata.
///
/// A unique indexed granularity is authoritative and also controls flat search
/// over unindexed fragments. An explicit request selects between coexisting
/// row and list-element indexes, but cannot contradict the only indexed
/// granularity. With no index, the established row default is retained.
pub(crate) async fn resolve_query_document_granularity(
    dataset: &Dataset,
    column: &str,
    requested: Option<DocumentGranularity>,
) -> Result<DocumentGranularity> {
    let indices = indexed_fts_document_granularities(dataset, column).await?;
    let mut available = indices
        .iter()
        .map(|(_, document_granularity)| *document_granularity)
        .collect::<Vec<_>>();
    available.sort_by_key(|document_granularity| match document_granularity {
        DocumentGranularity::Row => 0,
        DocumentGranularity::ListElement => 1,
    });
    available.dedup();

    let resolved = match requested {
        Some(requested) if available.is_empty() || available.contains(&requested) => requested,
        Some(requested) => {
            let indexed = indices
                .iter()
                .map(|(name, document_granularity)| format!("'{name}' ({document_granularity:?})"))
                .collect::<Vec<_>>()
                .join(", ");
            return Err(Error::invalid_input(format!(
                "FTS query for field '{column}' requested {requested:?} document granularity, \
                 but the existing FTS index uses a different granularity: {indexed}"
            )));
        }
        None if available.is_empty() => DocumentGranularity::Row,
        None if available.len() == 1 => available[0],
        None => {
            let indexed = indices
                .iter()
                .map(|(name, document_granularity)| format!("'{name}' ({document_granularity:?})"))
                .collect::<Vec<_>>()
                .join(", ");
            return Err(Error::invalid_input(format!(
                "FTS query for field '{column}' is ambiguous because Row and ListElement \
                 indexes coexist: {indexed}; specify document_granularity"
            )));
        }
    };

    resolve_fts_field(dataset.schema(), column, resolved)?;
    Ok(resolved)
}

pub(crate) fn fts_document_schema(coordinate_rank: usize) -> Arc<ArrowSchema> {
    let mut fields = vec![
        ArrowField::new(VALUE_COLUMN_NAME, DataType::Utf8, false),
        ArrowField::new(ROW_ID, DataType::UInt64, false),
    ];
    fields.extend(
        (0..coordinate_rank)
            .map(|rank| ArrowField::new(doc_index_storage_column(rank), DataType::UInt32, false)),
    );
    Arc::new(ArrowSchema::new(fields))
}

pub(crate) fn transform_fts_document_stream(
    input: SendableRecordBatchStream,
    resolved: ResolvedFtsField,
) -> Result<SendableRecordBatchStream> {
    let input_schema = input.schema();
    if (input_schema
        .column_with_name(&resolved.root_column)
        .is_none()
        && input_schema.column_with_name(VALUE_COLUMN_NAME).is_none())
        || input_schema.column_with_name(ROW_ID).is_none()
    {
        return Err(Error::internal(
            "FTS document input must contain the root source column and _rowid".to_string(),
        ));
    }
    let output_schema = fts_document_schema(resolved.coordinate_rank());
    let stream_schema = output_schema.clone();
    let stream = input.map(move |batch| {
        let batch = batch?;
        let source = batch
            .column_by_name(&resolved.root_column)
            .or_else(|| batch.column_by_name(VALUE_COLUMN_NAME))
            .expect("FTS document input schema was validated");
        let documents = resolved
            .documents_from_array(source, batch.num_rows())
            .map_err(DataFusionError::from)?;
        let input_row_ids = batch[ROW_ID].as_primitive::<arrow_array::types::UInt64Type>();
        let texts =
            StringArray::from_iter_values(documents.iter().map(|document| document.text.as_str()));
        let row_ids = UInt64Array::from_iter_values(
            documents
                .iter()
                .map(|document| input_row_ids.value(document.row_index)),
        );
        let mut columns = vec![Arc::new(texts) as ArrayRef, Arc::new(row_ids) as ArrayRef];
        for rank in 0..resolved.coordinate_rank() {
            columns.push(Arc::new(UInt32Array::from_iter_values(
                documents.iter().map(|document| document.doc_index[rank]),
            )) as ArrayRef);
        }
        RecordBatch::try_new(stream_schema.clone(), columns).map_err(DataFusionError::from)
    });
    Ok(Box::pin(RecordBatchStreamAdapter::new(
        output_schema,
        stream,
    )))
}

/// Fill legacy posting-format metadata while leaving protobuf-default row
/// document granularity untouched.
pub(crate) fn normalize_inverted_details(
    index: &IndexMetadata,
    mut details: InvertedIndexDetails,
) -> Result<InvertedIndexDetails> {
    if !matches!(index.index_version, 0..=3) {
        return Err(Error::invalid_input(format!(
            "FTS index '{}' has unsupported index_version {}; expected 0, 1, 2, or 3",
            index.name, index.index_version
        )));
    }
    if details.posting_format_version.is_none() {
        let posting_format_version = match index.index_version {
            0 | 1 => 1,
            2 => 2,
            3 => 3,
            _ => unreachable!("index version was validated above"),
        };
        details.posting_format_version = Some(posting_format_version);
    }
    Ok(details)
}

/// Build an empty update stream for the inverted merge API.
///
/// `InvertedIndex::merge_segments` is shaped as "merge old segments plus new
/// rows", so even a pure segment merge needs a stream with the document column
/// and `_rowid` fields. The stream intentionally contains no batches.
fn empty_inverted_update_stream(
    dataset: &Dataset,
    resolved: &ResolvedFtsField,
) -> Result<SendableRecordBatchStream> {
    let field = dataset
        .schema()
        .field_by_id(resolved.final_field_id)
        .ok_or_else(|| {
            Error::invalid_input(format!(
                "merge_existing_index_segments: field id {} does not exist",
                resolved.final_field_id
            ))
        })?;
    let schema = if resolved.has_lists() {
        fts_document_schema(resolved.coordinate_rank())
    } else {
        Arc::new(ArrowSchema::new(vec![
            ArrowField::new(VALUE_COLUMN_NAME, field.data_type(), true),
            ArrowField::new(ROW_ID, arrow_schema::DataType::UInt64, false),
        ]))
    };
    Ok(Box::pin(RecordBatchStreamAdapter::new(
        schema,
        futures::stream::empty(),
    )))
}

pub(crate) async fn finalize_segment_files_if_needed(
    dataset: &Dataset,
    segment: &IndexMetadata,
) -> Result<()> {
    let index_dir = dataset.indices_dir().join(segment.uuid.to_string());
    let metadata_path = index_dir
        .clone()
        .join(lance_index::scalar::inverted::METADATA_FILE);
    if dataset.object_store.as_ref().exists(&metadata_path).await? {
        return Ok(());
    }

    let store = Arc::new(LanceIndexStore::from_dataset_for_new(
        dataset,
        &segment.uuid,
    )?);
    lance_index::scalar::inverted::builder::merge_index_files(
        dataset.object_store.as_ref(),
        &index_dir,
        store,
        lance_index::progress::noop_progress(),
    )
    .await
}

/// Merge one caller-defined group of source FTS segments into a single segment.
pub(crate) async fn merge_segments(
    dataset: &Dataset,
    segments: Vec<IndexMetadata>,
) -> Result<IndexMetadata> {
    if segments.is_empty() {
        return Err(Error::index("No segment metadata was provided".to_string()));
    }

    let field_id = *segments[0].fields.first().ok_or_else(|| {
        Error::invalid_input(format!(
            "CreateIndex: segment {} is missing field ids",
            segments[0].uuid
        ))
    })?;
    let details = match segments[0].index_details.as_ref() {
        Some(details_any) => {
            let details =
                InvertedIndexDetails::decode(details_any.value.as_slice()).map_err(|error| {
                    Error::io(format!(
                        "failed to decode InvertedIndexDetails payload: {error}"
                    ))
                })?;
            normalize_inverted_details(&segments[0], details)?
        }
        None => normalize_inverted_details(&segments[0], InvertedIndexDetails::default())?,
    };
    let document_granularity = DocumentGranularity::try_from(details.document_granularity)?;
    let resolved = resolve_fts_field_by_id(dataset.schema(), field_id, document_granularity)?;
    load_segment_details(dataset, &resolved.canonical_path, &segments).await?;

    let mut source_indices = Vec::with_capacity(segments.len());
    let mut fragment_bitmap = RoaringBitmap::new();
    for segment in &segments {
        finalize_segment_files_if_needed(dataset, segment).await?;
        fragment_bitmap |= segment.fragment_bitmap.as_ref().cloned().ok_or_else(|| {
            Error::invalid_input(format!(
                "CreateIndex: segment {} is missing fragment coverage",
                segment.uuid
            ))
        })?;
        if segment.fields != segments[0].fields {
            return Err(Error::invalid_input(format!(
                "FTS index {} has inconsistent fields across segments",
                segments[0].name
            )));
        }
        let scalar_index = super::open_scalar_index(
            dataset,
            &resolved.canonical_path,
            segment,
            &NoOpMetricsCollector,
        )
        .await?;
        let inverted_index = scalar_index
            .as_any()
            .downcast_ref::<InvertedIndex>()
            .ok_or_else(|| {
                Error::index(format!(
                    "merge_existing_index_segments: expected inverted segment {}, got {:?}",
                    segment.uuid,
                    scalar_index.index_type()
                ))
            })?;
        source_indices.push(Arc::new(inverted_index.clone()));
    }

    let new_uuid = Uuid::new_v4();
    let new_store = LanceIndexStore::from_dataset_for_new(dataset, &new_uuid)?;
    let created_index = InvertedIndex::merge_segments(
        &source_indices,
        empty_inverted_update_stream(dataset, &resolved)?,
        &new_store,
        None,
        lance_index::progress::noop_progress(),
    )
    .await?;

    Ok(IndexMetadata {
        uuid: new_uuid,
        fields: vec![field_id],
        dataset_version: dataset.manifest.version,
        fragment_bitmap: Some(fragment_bitmap),
        index_details: Some(Arc::new(created_index.index_details)),
        index_version: created_index.index_version as i32,
        created_at: Some(chrono::Utc::now()),
        base_id: None,
        files: Some(index_files_to_table(created_index.files)),
        ..segments[0].clone()
    })
}

/// Load all committed inverted-index segments that belong to the same named
/// FTS index on `column`.
///
/// Returns `Ok(None)` if no FTS index exists on the column. When an index
/// exists, the returned vector contains every committed segment's
/// [`IndexMetadata`] (UUID, fragment coverage, index details). All segments
/// must share the same indexed fields; mismatched fields return an error.
pub async fn load_segments(
    dataset: &Dataset,
    column: &str,
    document_granularity: DocumentGranularity,
) -> Result<Option<Vec<IndexMetadata>>> {
    let Some(index_meta) = dataset
        .load_scalar_index(
            lance_index::IndexCriteria::default()
                .for_column(column)
                .supports_fts()
                .with_fts_document_granularity(document_granularity),
        )
        .await?
    else {
        return Ok(None);
    };

    let indices = dataset.load_indices_by_name(&index_meta.name).await?;
    if indices.is_empty() {
        return Ok(None);
    }

    let expected_fields = indices[0].fields.clone();
    for meta in &indices {
        if meta.fields != expected_fields {
            return Err(Error::invalid_input(format!(
                "FTS index {} has inconsistent fields across segments",
                index_meta.name
            )));
        }
    }

    Ok(Some(indices))
}

/// Load and validate the shared [`InvertedIndexDetails`] across committed
/// segments returned by [`load_segments`].
///
/// All segments are required to agree on their semantic `InvertedIndexDetails`
/// payload (tokenizer, position settings, etc.); inconsistent
/// segments return an error. Details are canonicalized before comparison so
/// legacy segments that omit default fields remain compatible with newly
/// written text FTS segments. Returns the canonical details that may be used
/// when constructing a tokenizer or running a query against the index.
pub async fn load_segment_details(
    dataset: &Dataset,
    column: &str,
    segments: &[IndexMetadata],
) -> Result<InvertedIndexDetails> {
    let mut expected_details: Option<InvertedIndexDetails> = None;
    for meta in segments {
        let details_any = fetch_index_details(dataset, column, meta).await?;
        let details =
            InvertedIndexDetails::decode(details_any.value.as_slice()).map_err(|err| {
                Error::io(format!(
                    "failed to decode InvertedIndexDetails payload: {err}"
                ))
            })?;
        let details = canonicalize_inverted_index_details(details)?;
        match &expected_details {
            Some(expected) if expected != &details => {
                return Err(Error::invalid_input(format!(
                    "FTS index {} has inconsistent inverted index details across segments",
                    meta.name
                )));
            }
            Some(_) => {}
            None => expected_details = Some(details),
        }
    }
    expected_details.ok_or_else(|| {
        Error::invalid_input(format!(
            "FTS index for column {} requires at least one segment",
            column
        ))
    })
}

fn canonicalize_inverted_index_details(
    details: InvertedIndexDetails,
) -> Result<InvertedIndexDetails> {
    let params = InvertedIndexParams::try_from(&details)?;
    InvertedIndexDetails::try_from(&params)
}

/// Read one segment's [`InvertedIndexParams`]
pub async fn load_segment_params(
    dataset: &Dataset,
    segment: &IndexMetadata,
) -> Result<InvertedIndexParams> {
    let store = LanceIndexStore::from_dataset_for_existing(dataset, segment).await?;
    InvertedIndex::load_params(&store).await
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fts_test_schema() -> Schema {
        let schema = ArrowSchema::new(vec![
            ArrowField::new("text", DataType::Utf8, true),
            ArrowField::new(
                "tags",
                DataType::List(Arc::new(ArrowField::new("item", DataType::Utf8, true))),
                true,
            ),
        ]);
        Schema::try_from(&schema).unwrap()
    }

    #[test]
    fn decode_legacy_inverted_details_type_url() {
        let mut details_any = prost_types::Any::from_msg(&InvertedIndexDetails::default()).unwrap();
        details_any.type_url = "/lance.index.pb.InvertedIndexDetails".to_string();

        let decoded = InvertedIndexDetails::decode(details_any.value.as_slice()).unwrap();
        assert_eq!(decoded, InvertedIndexDetails::default());
    }

    #[test]
    fn resolve_element_document_field() {
        let schema = fts_test_schema();
        let tags = schema.field("tags").unwrap();
        let resolved =
            resolve_fts_field(&schema, "tags", DocumentGranularity::ListElement).unwrap();
        assert_eq!(resolved.final_field_id, tags.id);
        assert_eq!(resolved.root_column, "tags");
        assert_eq!(resolved.canonical_path, "tags");
        assert_eq!(resolved.coordinate_rank(), 1);

        let err = resolve_fts_field(&schema, "text", DocumentGranularity::ListElement).unwrap_err();
        assert!(err.to_string().contains("has no List layer"), "{err}");

        let err =
            resolve_fts_field(&schema, "tags[*]", DocumentGranularity::ListElement).unwrap_err();
        assert!(err.to_string().contains("does not exist"), "{err}");
    }

    #[test]
    fn normalize_legacy_metadata_as_row_document() {
        let metadata = IndexMetadata {
            uuid: Uuid::new_v4(),
            fields: vec![1],
            name: "tags_idx".to_string(),
            dataset_version: 1,
            fragment_bitmap: None,
            index_details: None,
            index_version: 3,
            created_at: None,
            base_id: None,
            files: None,
        };
        let details =
            normalize_inverted_details(&metadata, InvertedIndexDetails::default()).unwrap();
        assert_eq!(
            DocumentGranularity::try_from(details.document_granularity).unwrap(),
            DocumentGranularity::Row
        );
        assert_eq!(details.posting_format_version, Some(3));
    }

    #[test]
    fn normalize_rejects_unreleased_v4_metadata() {
        let metadata = IndexMetadata {
            uuid: Uuid::new_v4(),
            fields: vec![1],
            name: "tags_idx".to_string(),
            dataset_version: 1,
            fragment_bitmap: None,
            index_details: None,
            index_version: 4,
            created_at: None,
            base_id: None,
            files: None,
        };
        let details = InvertedIndexDetails::try_from(&InvertedIndexParams::default()).unwrap();

        let err = normalize_inverted_details(&metadata, details).unwrap_err();

        assert!(err.to_string().contains("unsupported index_version 4"));
    }

    #[test]
    fn canonicalize_inverted_details_accepts_legacy_empty_details() {
        let legacy = InvertedIndexDetails::default();
        let current = InvertedIndexDetails::try_from(&InvertedIndexParams::default()).unwrap();

        assert_ne!(legacy, current);
        assert_eq!(
            canonicalize_inverted_index_details(legacy).unwrap(),
            canonicalize_inverted_index_details(current).unwrap()
        );
    }
}
