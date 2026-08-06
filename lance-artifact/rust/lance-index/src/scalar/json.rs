// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use lance_core::utils::row_addr_remap::RowAddrRemap;
use std::{
    ops::Bound,
    sync::{Arc, Mutex},
};

use arrow_array::{Array, LargeBinaryArray, RecordBatch, StructArray, UInt8Array};
use arrow_schema::{DataType, Field, Schema, SortOptions};
use async_trait::async_trait;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::{
    execution::SendableRecordBatchStream,
    physical_plan::{ExecutionPlan, projection::ProjectionExec, sorts::sort::SortExec},
};
use datafusion_common::{ScalarValue, config::ConfigOptions};
use datafusion_expr::{Expr, Operator, ScalarUDF};
use datafusion_physical_expr::{
    PhysicalExpr, PhysicalSortExpr, ScalarFunctionExpr,
    expressions::{Column, Literal},
};
use futures::StreamExt;
use lance_core::deepsize::DeepSizeOf;
use lance_datafusion::exec::{
    LanceExecutionOptions, OneShotExec, execute_plan, get_session_context,
};
use lance_datafusion::udf::json::JsonbType;
use prost::Message;
use roaring::RoaringBitmap;
use serde::{Deserialize, Serialize};

use lance_core::{Error, Result, cache::LanceCache, error::LanceOptionExt};

use crate::{
    Index, IndexType,
    metrics::MetricsCollector,
    registry::IndexPluginRegistry,
    scalar::{
        AnyQuery, CreatedIndex, IndexStore, RowIdRemapper, ScalarIndex, SearchResult,
        UpdateCriteria,
        expression::{IndexedExpression, ScalarIndexExpr, ScalarIndexSearch, ScalarQueryParser},
        registry::{
            BasicTrainer, ScalarIndexPlugin, TrainingCriteria, TrainingOrdering, TrainingRequest,
            VALUE_COLUMN_NAME,
        },
    },
};

const JSON_INDEX_VERSION: u32 = 0;

/// A JSON index that indexes a field in a JSON column
///
/// The underlying index can be any other type of scalar index
#[derive(Debug)]
pub struct JsonIndex {
    target_index: Arc<dyn ScalarIndex>,
    path: String,
}

impl JsonIndex {
    pub fn new(target_index: Arc<dyn ScalarIndex>, path: String) -> Self {
        Self { target_index, path }
    }
}

impl DeepSizeOf for JsonIndex {
    fn deep_size_of_children(&self, context: &mut lance_core::deepsize::Context) -> usize {
        self.target_index.deep_size_of_children(context) + self.path.deep_size_of_children(context)
    }
}

#[async_trait]
impl Index for JsonIndex {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_index(self: Arc<Self>) -> Arc<dyn Index> {
        self
    }

    fn index_type(&self) -> IndexType {
        // TODO: This causes the index to appear as btree in list_indices call.  Need better logic
        // in list_indices to use details instead of index_type.
        IndexType::Scalar
    }

    async fn prewarm(&self) -> Result<()> {
        self.target_index.prewarm().await
    }

    fn statistics(&self) -> Result<serde_json::Value> {
        todo!()
    }

    async fn calculate_included_frags(&self) -> Result<RoaringBitmap> {
        self.target_index.calculate_included_frags().await
    }
}

#[async_trait]
impl ScalarIndex for JsonIndex {
    async fn search(
        &self,
        query: &dyn AnyQuery,
        metrics: &dyn MetricsCollector,
    ) -> Result<SearchResult> {
        let query = query.as_any().downcast_ref::<JsonQuery>().unwrap();
        self.target_index
            .search(query.target_query.as_ref(), metrics)
            .await
    }

    fn can_remap(&self) -> bool {
        self.target_index.can_remap()
    }

    async fn remap(
        &self,
        mapping: &RowAddrRemap,
        dest_store: &dyn IndexStore,
    ) -> Result<CreatedIndex> {
        let target_created = self.target_index.remap(mapping, dest_store).await?;
        let json_details = crate::pb::JsonIndexDetails {
            path: self.path.clone(),
            target_details: Some(target_created.index_details),
        };
        Ok(CreatedIndex {
            index_details: prost_types::Any::from_msg(&json_details)?,
            // TODO: We should store the target index version in the details
            index_version: JSON_INDEX_VERSION,
            files: target_created.files,
        })
    }

    async fn update(
        &self,
        new_data: SendableRecordBatchStream,
        dest_store: &dyn IndexStore,
        old_data_filter: Option<super::OldIndexDataFilter>,
    ) -> Result<CreatedIndex> {
        let target_created = self
            .target_index
            .update(new_data, dest_store, old_data_filter)
            .await?;
        let json_details = crate::pb::JsonIndexDetails {
            path: self.path.clone(),
            target_details: Some(target_created.index_details),
        };
        Ok(CreatedIndex {
            index_details: prost_types::Any::from_msg(&json_details)?,
            // TODO: We should store the target index version in the details
            index_version: JSON_INDEX_VERSION,
            files: target_created.files,
        })
    }

    fn update_criteria(&self) -> UpdateCriteria {
        self.target_index.update_criteria()
    }

    fn derive_index_params(&self) -> Result<super::ScalarIndexParams> {
        self.target_index.derive_index_params()
    }
}

/// Parameters for a [`JsonIndex`]
#[derive(Debug, Serialize, Deserialize)]
pub struct JsonIndexParameters {
    target_index_type: String,
    target_index_parameters: Option<String>,
    path: String,
}

// TODO: Do we really need to wrap the query or could we just return the target query directly?
//
// I think the only thing we really gain is a different format impl (e.g. it shows up as a json query
// in the explain plan) but I don't know if that helps the user much.
#[derive(Debug, Clone)]
pub struct JsonQuery {
    target_query: Arc<dyn AnyQuery>,
    path: String,
}

impl JsonQuery {
    pub fn new(target_query: Arc<dyn AnyQuery>, path: String) -> Self {
        Self { target_query, path }
    }
}

impl PartialEq for JsonQuery {
    fn eq(&self, other: &Self) -> bool {
        self.target_query.dyn_eq(other.target_query.as_ref()) && self.path == other.path
    }
}

impl AnyQuery for JsonQuery {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn format(&self, col: &str) -> String {
        format!("Json({}->{})", self.target_query.format(col), self.path)
    }

    fn to_expr(&self, _col: String) -> Expr {
        todo!()
    }

    fn dyn_eq(&self, other: &dyn AnyQuery) -> bool {
        match other.as_any().downcast_ref::<Self>() {
            Some(o) => self == o,
            None => false,
        }
    }
}

#[derive(Debug)]
pub struct JsonQueryParser {
    path: String,
    target_parser: Box<dyn ScalarQueryParser>,
}

impl JsonQueryParser {
    pub fn new(path: String, target_parser: Box<dyn ScalarQueryParser>) -> Self {
        Self {
            path,
            target_parser,
        }
    }

    fn wrap_search(&self, target_expr: IndexedExpression) -> IndexedExpression {
        if let Some(scalar_query) = target_expr.scalar_query {
            let scalar_query = match scalar_query {
                ScalarIndexExpr::Query(ScalarIndexSearch {
                    column,
                    index_name,
                    index_type,
                    query,
                    needs_recheck,
                    fragment_bitmap,
                }) => ScalarIndexExpr::Query(ScalarIndexSearch {
                    column,
                    index_name,
                    index_type,
                    query: Arc::new(JsonQuery::new(query, self.path.clone())),
                    needs_recheck,
                    fragment_bitmap,
                }),
                // This code path should only be hit on leaf expr
                _ => unreachable!(),
            };
            IndexedExpression {
                scalar_query: Some(scalar_query),
                refine_expr: target_expr.refine_expr,
            }
        } else {
            target_expr
        }
    }
}

impl ScalarQueryParser for JsonQueryParser {
    fn visit_between(
        &self,
        column: &str,
        low: &Bound<ScalarValue>,
        high: &Bound<ScalarValue>,
    ) -> Option<IndexedExpression> {
        self.target_parser
            .visit_between(column, low, high)
            .map(|target_expr| self.wrap_search(target_expr))
    }
    fn visit_in_list(&self, column: &str, in_list: &[ScalarValue]) -> Option<IndexedExpression> {
        self.target_parser
            .visit_in_list(column, in_list)
            .map(|target_expr| self.wrap_search(target_expr))
    }
    fn visit_is_bool(&self, column: &str, value: bool) -> Option<IndexedExpression> {
        self.target_parser
            .visit_is_bool(column, value)
            .map(|target_expr| self.wrap_search(target_expr))
    }
    fn visit_is_null(&self, column: &str) -> Option<IndexedExpression> {
        self.target_parser
            .visit_is_null(column)
            .map(|target_expr| self.wrap_search(target_expr))
    }
    fn visit_comparison(
        &self,
        column: &str,
        value: &ScalarValue,
        op: &Operator,
    ) -> Option<IndexedExpression> {
        self.target_parser
            .visit_comparison(column, value, op)
            .map(|target_expr| self.wrap_search(target_expr))
    }
    fn visit_scalar_function(
        &self,
        column: &str,
        data_type: &DataType,
        func: &ScalarUDF,
        args: &[Expr],
    ) -> Option<IndexedExpression> {
        self.target_parser
            .visit_scalar_function(column, data_type, func, args)
            .map(|target_expr| self.wrap_search(target_expr))
    }

    // TODO: maybe we should address it by https://github.com/lance-format/lance/issues/4624
    fn is_valid_reference(&self, func: &Expr, _data_type: &DataType) -> Option<DataType> {
        match func {
            Expr::ScalarFunction(udf) => {
                // Support multiple JSON extraction functions
                let json_functions = [
                    "json_extract",
                    "json_get",
                    "json_get_int",
                    "json_get_float",
                    "json_get_bool",
                    "json_get_string",
                ];
                if !json_functions.contains(&udf.name()) {
                    return None;
                }
                if udf.args.len() != 2 {
                    return None;
                }
                // We already know index 0 is a column reference to the column so we just need to
                // ensure that index 1 matches our path
                match &udf.args[1] {
                    Expr::Literal(ScalarValue::Utf8(Some(path)), _) => {
                        if path == &self.path {
                            // Return the appropriate type based on the function
                            match udf.name() {
                                "json_get_int" => Some(DataType::Int64),
                                "json_get_float" => Some(DataType::Float64),
                                "json_get_bool" => Some(DataType::Boolean),
                                "json_get_string" | "json_extract" => Some(DataType::Utf8),
                                _ => None,
                            }
                        } else {
                            None
                        }
                    }
                    _ => None,
                }
            }
            _ => None,
        }
    }
}

pub struct JsonTrainingRequest {
    parameters: JsonIndexParameters,
    target_request: Box<dyn TrainingRequest>,
    criteria: TrainingCriteria,
}

impl JsonTrainingRequest {
    pub fn new(parameters: JsonIndexParameters, target_request: Box<dyn TrainingRequest>) -> Self {
        let target_criteria = target_request.criteria();
        // The scanner can only sort its output by the raw JSON column, not by the value
        // at `path` that this plugin extracts from it, so a `Values`-ordered scan here
        // would sort by the wrong key and still need re-sorting after extraction. Ask
        // for unordered input instead and let `train_index` sort the extracted value
        // stream itself, once, right before handing it to the target trainer.
        //
        // Preserve `Addresses` so callers must still provide the ordering required by
        // target trainers such as ZoneMap, even though the dataset scanner currently
        // returns rows in row-address order by default.
        let ordering = match target_criteria.ordering {
            TrainingOrdering::Values => TrainingOrdering::None,
            ordering => ordering,
        };
        let mut criteria = TrainingCriteria::new(ordering);
        criteria.needs_row_ids = target_criteria.needs_row_ids;
        criteria.needs_row_addrs = target_criteria.needs_row_addrs;
        Self {
            parameters,
            target_request,
            criteria,
        }
    }
}

impl TrainingRequest for JsonTrainingRequest {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn criteria(&self) -> &TrainingCriteria {
        &self.criteria
    }
}

/// Plugin implementation for a [`JsonIndex`]
#[derive(Default)]
pub struct JsonIndexPlugin {
    registry: Mutex<Option<Arc<IndexPluginRegistry>>>,
}

impl std::fmt::Debug for JsonIndexPlugin {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "JsonIndexPlugin")
    }
}

impl JsonIndexPlugin {
    fn registry(&self) -> Result<Arc<IndexPluginRegistry>> {
        Ok(self.registry.lock().unwrap().as_ref().expect_ok()?.clone())
    }

    /// Extract JSON with type information using the new UDF
    async fn extract_json_with_type_info(
        data: SendableRecordBatchStream,
        path: String,
    ) -> Result<(SendableRecordBatchStream, DataType)> {
        let input = Arc::new(OneShotExec::new(data));
        let input_schema = input.schema();
        let value_column_idx = input_schema
            .column_with_name(VALUE_COLUMN_NAME)
            .expect_ok()?
            .0;

        // Call json_extract_with_type UDF
        let mut exprs: Vec<(Arc<dyn PhysicalExpr>, String)> =
            Vec::with_capacity(input_schema.fields().len());
        exprs.push((
            Arc::new(ScalarFunctionExpr::try_new(
                Arc::new(lance_datafusion::udf::json::json_extract_with_type_udf()),
                vec![
                    Arc::new(Column::new(VALUE_COLUMN_NAME, value_column_idx)),
                    Arc::new(Literal::new(ScalarValue::Utf8(Some(path)))),
                ],
                &input_schema,
                Arc::new(ConfigOptions::default()),
            )?) as Arc<dyn PhysicalExpr>,
            "json_result".to_string(),
        ));
        for (column_idx, field) in input_schema.fields().iter().enumerate() {
            if field.name() != VALUE_COLUMN_NAME {
                exprs.push((
                    Arc::new(Column::new(field.name(), column_idx)) as Arc<dyn PhysicalExpr>,
                    field.name().clone(),
                ));
            }
        }

        let project = ProjectionExec::try_new(exprs, input)?;
        let ctx = get_session_context(&LanceExecutionOptions::default());
        let mut stream = project.execute(0, ctx.task_ctx())?;

        // Collect batches and determine type from first non-null value
        let mut all_batches = Vec::new();
        let mut inferred_type: Option<DataType> = None;

        while let Some(batch_result) = stream.next().await {
            let batch = batch_result?;

            // Determine type from first non-null value if not yet set
            if inferred_type.is_none()
                && let Some(json_result_column) = batch.column_by_name("json_result")
                && let Some(struct_array) =
                    json_result_column.as_any().downcast_ref::<StructArray>()
                && let Some(type_array) = struct_array.column_by_name("type_tag")
                && let Some(uint8_array) = type_array.as_any().downcast_ref::<UInt8Array>()
            {
                // Find first non-null value to determine type
                for i in 0..uint8_array.len() {
                    if !uint8_array.is_null(i) {
                        let type_tag = uint8_array.value(i);
                        let jsonb_type = JsonbType::from_u8(type_tag).ok_or_else(|| {
                            Error::invalid_input_source(
                                format!("Invalid type tag: {}", type_tag).into(),
                            )
                        })?;

                        // Map JsonbType to Arrow DataType
                        inferred_type = Some(match jsonb_type {
                            JsonbType::Null => continue, // Skip null values
                            JsonbType::Boolean => DataType::Boolean,
                            JsonbType::Int64 => DataType::Int64,
                            JsonbType::Float64 => DataType::Float64,
                            JsonbType::String => DataType::Utf8,
                            JsonbType::Array => DataType::LargeBinary,
                            JsonbType::Object => DataType::LargeBinary,
                        });
                        break;
                    }
                }
            }

            all_batches.push(batch);
        }

        // If no type was inferred (all nulls), default to String
        let inferred_type = inferred_type.unwrap_or(DataType::Utf8);

        // Recreate stream from collected batches
        let schema = all_batches
            .first()
            .map(|b| b.schema())
            .ok_or_else(|| Error::invalid_input_source("No batches in stream".into()))?;

        let recreated_stream = Box::pin(RecordBatchStreamAdapter::new(
            schema,
            futures::stream::iter(all_batches.into_iter().map(Ok)),
        )) as SendableRecordBatchStream;

        Ok((recreated_stream, inferred_type))
    }

    /// Convert the stream with JSONB values and type tags to properly typed values
    async fn convert_stream_by_type(
        data: SendableRecordBatchStream,
        target_type: DataType,
    ) -> Result<SendableRecordBatchStream> {
        let input = Arc::new(OneShotExec::new(data));
        let input_schema = input.schema();
        let passthrough_indices = input_schema
            .fields()
            .iter()
            .enumerate()
            .filter_map(|(column_idx, field)| (field.name() != "json_result").then_some(column_idx))
            .collect::<Vec<_>>();
        let output_schema = Arc::new(Schema::new(
            std::iter::once(Field::new(VALUE_COLUMN_NAME, target_type.clone(), true))
                .chain(
                    passthrough_indices
                        .iter()
                        .map(|column_idx| input_schema.field(*column_idx).clone()),
                )
                .collect::<Vec<_>>(),
        ));
        let ctx = get_session_context(&LanceExecutionOptions::default());
        let mut stream = input.execute(0, ctx.task_ctx())?;

        let mut converted_batches = Vec::new();

        while let Some(batch_result) = stream.next().await {
            let batch = batch_result?;

            // Extract the struct column containing value and type_tag
            let json_result_column = batch
                .column_by_name("json_result")
                .ok_or_else(|| Error::invalid_input_source("Missing json_result column".into()))?;

            let struct_array = json_result_column
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| Error::invalid_input_source("json_result is not a struct".into()))?;

            let value_array = struct_array.column_by_name("value").ok_or_else(|| {
                Error::invalid_input_source("Missing value column in struct".into())
            })?;

            let binary_array = value_array
                .as_any()
                .downcast_ref::<LargeBinaryArray>()
                .ok_or_else(|| Error::invalid_input_source("value is not LargeBinary".into()))?;

            // Convert based on target type using serde deserialization
            let converted_array: Arc<dyn Array> =
                match target_type {
                    DataType::Boolean => {
                        let mut builder =
                            arrow_array::builder::BooleanBuilder::with_capacity(binary_array.len());
                        for i in 0..binary_array.len() {
                            if binary_array.is_null(i) {
                                builder.append_null();
                            } else if let Some(bytes) = binary_array.value(i).into() {
                                let raw_jsonb = jsonb::RawJsonb::new(bytes);
                                // Try to deserialize directly to bool
                                match jsonb::from_raw_jsonb::<bool>(&raw_jsonb) {
                                    Ok(bool_val) => builder.append_value(bool_val),
                                    Err(e) => {
                                        return Err(Error::invalid_input_source(format!(
                                        "Failed to deserialize JSONB to bool at index {}: {}",
                                        i, e
                                    )
                                    .into()));
                                    }
                                }
                            } else {
                                builder.append_null();
                            }
                        }
                        Arc::new(builder.finish())
                    }
                    DataType::Int64 => {
                        let mut builder =
                            arrow_array::builder::Int64Builder::with_capacity(binary_array.len());
                        for i in 0..binary_array.len() {
                            if binary_array.is_null(i) {
                                builder.append_null();
                            } else if let Some(bytes) = binary_array.value(i).into() {
                                let raw_jsonb = jsonb::RawJsonb::new(bytes);
                                // Try to deserialize directly to i64
                                match jsonb::from_raw_jsonb::<i64>(&raw_jsonb) {
                                    Ok(int_val) => builder.append_value(int_val),
                                    Err(e) => {
                                        return Err(Error::invalid_input_source(format!(
                                        "Failed to deserialize JSONB to i64 at index {}: {}",
                                        i, e
                                    )
                                    .into()));
                                    }
                                }
                            } else {
                                builder.append_null();
                            }
                        }
                        Arc::new(builder.finish())
                    }
                    DataType::Float64 => {
                        let mut builder =
                            arrow_array::builder::Float64Builder::with_capacity(binary_array.len());
                        for i in 0..binary_array.len() {
                            if binary_array.is_null(i) {
                                builder.append_null();
                            } else if let Some(bytes) = binary_array.value(i).into() {
                                let raw_jsonb = jsonb::RawJsonb::new(bytes);
                                // Try to deserialize directly to f64 (serde handles int->float conversion)
                                match jsonb::from_raw_jsonb::<f64>(&raw_jsonb) {
                                    Ok(float_val) => builder.append_value(float_val),
                                    Err(e) => {
                                        return Err(Error::invalid_input_source(format!(
                                        "Failed to deserialize JSONB to f64 at index {}: {}",
                                        i, e
                                    )
                                    .into()));
                                    }
                                }
                            } else {
                                builder.append_null();
                            }
                        }
                        Arc::new(builder.finish())
                    }
                    DataType::Utf8 => {
                        let mut builder = arrow_array::builder::StringBuilder::with_capacity(
                            binary_array.len(),
                            1024,
                        );
                        for i in 0..binary_array.len() {
                            if binary_array.is_null(i) {
                                builder.append_null();
                            } else if let Some(bytes) = binary_array.value(i).into() {
                                let raw_jsonb = jsonb::RawJsonb::new(bytes);
                                // Try to deserialize to String, or use to_string() for any type
                                match jsonb::from_raw_jsonb::<String>(&raw_jsonb) {
                                    Ok(str_val) => builder.append_value(&str_val),
                                    Err(_) => {
                                        // For non-string types, convert to string representation
                                        builder.append_value(raw_jsonb.to_string());
                                    }
                                }
                            } else {
                                builder.append_null();
                            }
                        }
                        Arc::new(builder.finish())
                    }
                    DataType::LargeBinary => {
                        // Keep as binary for array/object types
                        value_array.clone()
                    }
                    _ => {
                        return Err(Error::invalid_input_source(
                            format!("Unsupported target type: {:?}", target_type).into(),
                        ));
                    }
                };

            let mut columns = Vec::with_capacity(output_schema.fields().len());
            columns.push(converted_array);
            columns.extend(
                passthrough_indices
                    .iter()
                    .map(|column_idx| batch.column(*column_idx).clone()),
            );
            let new_batch = RecordBatch::try_new(output_schema.clone(), columns)?;

            converted_batches.push(new_batch);
        }

        // Create stream from converted batches
        if converted_batches.is_empty() {
            return Err(Error::invalid_input_source("No batches to convert".into()));
        }
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            output_schema,
            futures::stream::iter(converted_batches.into_iter().map(Ok)),
        )))
    }

    /// Sort a value stream and its row-location columns ascending by value.
    ///
    /// Target index types that require `TrainingOrdering::Values` (e.g. btree, whose
    /// per-page min/max stats are taken from the first/last row of each page) need this
    /// as the only sort in the JSON training path: `JsonTrainingRequest` requests
    /// unordered input from the scanner, since the scanner can only sort on the raw
    /// JSON column, not on the value at `path`.
    async fn sort_stream_by_value(
        data: SendableRecordBatchStream,
    ) -> Result<SendableRecordBatchStream> {
        let input = Arc::new(OneShotExec::new(data));
        let value_idx = input.schema().index_of(VALUE_COLUMN_NAME)?;
        let sort_expr = PhysicalSortExpr {
            expr: Arc::new(Column::new(VALUE_COLUMN_NAME, value_idx)),
            options: SortOptions {
                descending: false,
                nulls_first: true,
            },
        };
        let plan = Arc::new(SortExec::new([sort_expr].into(), input));
        execute_plan(
            plan,
            LanceExecutionOptions {
                use_spilling: true,
                ..Default::default()
            },
        )
    }
}

#[async_trait]
impl BasicTrainer for JsonIndexPlugin {
    fn new_training_request(
        &self,
        params: &str,
        field: &Field,
    ) -> Result<Box<dyn TrainingRequest>> {
        if !matches!(field.data_type(), DataType::Binary | DataType::LargeBinary) {
            return Err(Error::invalid_input_source(
                "A JSON index can only be created on a Binary or LargeBinary field.".into(),
            ));
        }

        // Initially use Utf8, will be refined during training with type inference
        let target_type = DataType::Utf8;

        let params = serde_json::from_str::<JsonIndexParameters>(params)?;
        let registry = self.registry()?;
        let target_plugin = registry.get_plugin_by_name(&params.target_index_type)?;
        let target_trainer = target_plugin.basic_trainer().ok_or_else(|| {
            Error::invalid_input_source(
                format!("The '{}' index type does not support basic training, please refer to the index's documentation for more details on how to create this index.", params.target_index_type).into(),
            )
        })?;
        let target_request = target_trainer.new_training_request(
            params.target_index_parameters.as_deref().unwrap_or("{}"),
            &Field::new("", target_type, true),
        )?;

        Ok(Box::new(JsonTrainingRequest::new(params, target_request)))
    }

    async fn train_index(
        &self,
        data: SendableRecordBatchStream,
        index_store: &dyn IndexStore,
        request: Box<dyn TrainingRequest>,
        fragment_ids: Option<Vec<u32>>,
        progress: Arc<dyn crate::progress::IndexBuildProgress>,
    ) -> Result<CreatedIndex> {
        let request = (request as Box<dyn std::any::Any>)
            .downcast::<JsonTrainingRequest>()
            .unwrap();
        let path = request.parameters.path.clone();

        // Extract JSON with type information
        let (data_stream, inferred_type) =
            Self::extract_json_with_type_info(data, path.clone()).await?;

        // Convert the stream to properly typed values based on inferred type
        let converted_stream =
            Self::convert_stream_by_type(data_stream, inferred_type.clone()).await?;

        // `JsonTrainingRequest::criteria()` asked the scanner for unordered input (see
        // its constructor), since the scanner can only sort on the raw JSON column, not
        // on the value at `path`. If the target index needs value-ordered input, this is
        // the one place that sort happens: on the extracted value, after extraction.
        //
        // Deliberately `request.target_request.criteria()` here, not `request.criteria()`:
        // the latter is `JsonTrainingRequest`'s own criteria, which is always `None` (that's
        // what asked the scanner for unordered input above) and would never take this branch.
        let converted_stream =
            if request.target_request.criteria().ordering == TrainingOrdering::Values {
                Self::sort_stream_by_value(converted_stream).await?
            } else {
                converted_stream
            };

        // Update the target request with inferred type
        let registry = self.registry()?;
        let target_plugin = registry.get_plugin_by_name(&request.parameters.target_index_type)?;

        // Create a new training request with the inferred type
        let target_trainer = target_plugin.basic_trainer().ok_or_else(|| {
            Error::invalid_input_source(
                format!("The '{}' index type does not support basic training, please refer to the index's documentation for more details on how to create this index.", request.parameters.target_index_type).into(),
            )
        })?;
        let target_request = target_trainer.new_training_request(
            request
                .parameters
                .target_index_parameters
                .as_deref()
                .unwrap_or("{}"),
            &Field::new("", inferred_type, true),
        )?;

        let target_index = target_trainer
            .train_index(
                converted_stream,
                index_store,
                target_request,
                fragment_ids,
                progress,
            )
            .await?;

        let index_details = crate::pb::JsonIndexDetails {
            path,
            target_details: Some(target_index.index_details),
        };
        Ok(CreatedIndex {
            index_details: prost_types::Any::from_msg(&index_details)?,
            index_version: JSON_INDEX_VERSION,
            files: target_index.files,
        })
    }
}

#[async_trait]
impl ScalarIndexPlugin for JsonIndexPlugin {
    fn basic_trainer(&self) -> Option<&dyn BasicTrainer> {
        Some(self)
    }

    fn name(&self) -> &str {
        "Json"
    }

    fn provides_exact_answer(&self) -> bool {
        // TODO: Need to lookup target plugin via details to figure this out correctly
        true
    }

    fn attach_registry(&self, registry: Arc<IndexPluginRegistry>) {
        let mut reg_ref = self.registry.lock().unwrap();
        *reg_ref = Some(registry);
    }

    fn version(&self) -> u32 {
        JSON_INDEX_VERSION
    }

    fn new_query_parser(
        &self,
        index_name: String,
        index_details: &prost_types::Any,
    ) -> Option<Box<dyn ScalarQueryParser>> {
        // TODO: Allow return Result here
        let registry = self.registry().unwrap();
        let json_details =
            crate::pb::JsonIndexDetails::decode(index_details.value.as_slice()).unwrap();
        let target_details = json_details.target_details.as_ref().expect_ok().unwrap();
        let target_plugin = registry.get_plugin_by_details(target_details).unwrap();
        // TODO: Use something like ${index_name}_${path} for the index name?  Don't have access to path here tho
        let target_parser = target_plugin.new_query_parser(index_name, index_details)?;
        Some(Box::new(JsonQueryParser::new(
            json_details.path.clone(),
            target_parser,
        )) as Box<dyn ScalarQueryParser>)
    }

    async fn load_index(
        &self,
        index_store: Arc<dyn IndexStore>,
        index_details: &prost_types::Any,
        frag_reuse_index: Option<Arc<dyn RowIdRemapper>>,
        cache: &LanceCache,
    ) -> Result<Arc<dyn ScalarIndex>> {
        let registry = self.registry().unwrap();
        let json_details = crate::pb::JsonIndexDetails::decode(index_details.value.as_slice())?;
        let target_details = json_details.target_details.as_ref().expect_ok()?;
        let target_plugin = registry.get_plugin_by_details(target_details).unwrap();
        let target_index = target_plugin
            .load_index(index_store, target_details, frag_reuse_index, cache)
            .await?;
        Ok(Arc::new(JsonIndex::new(target_index, json_details.path)))
    }

    fn details_as_json(&self, details: &prost_types::Any) -> Result<serde_json::Value> {
        let registry = self.registry().unwrap();
        let json_details = crate::pb::JsonIndexDetails::decode(details.value.as_slice())?;
        let target_details = json_details.target_details.as_ref().expect_ok()?;
        let target_plugin = registry.get_plugin_by_details(target_details).unwrap();
        let target_details_json = target_plugin.details_as_json(target_details)?;
        Ok(serde_json::json!({
            "path": json_details.path,
            "target_details": target_details_json,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scalar::{SargableQuery, TextQuery};
    use arrow_array::{ArrayRef, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use lance_core::{ROW_ADDR, ROW_ID, utils::address::RowAddress};
    use rstest::rstest;
    use std::ops::Bound;
    use std::sync::Arc;

    // Note: The old test_detect_json_value_type test has been removed as we now use
    // JSONB's inherent type information instead of string-based type detection

    #[tokio::test]
    async fn test_json_extract_with_type_info() {
        use arrow_array::{LargeBinaryArray, UInt64Array};
        use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
        use futures::stream;

        // Create test JSONB data
        let json_data = vec![
            r#"{"name": "Alice", "age": 30, "active": true}"#,
            r#"{"name": "Bob", "age": 25, "active": false}"#,
            r#"{"name": "Charlie", "age": 35, "active": true}"#,
        ];

        // Convert JSON strings to JSONB binary format
        let mut jsonb_values = Vec::new();
        for json_str in &json_data {
            let owned_jsonb: jsonb::OwnedJsonb = json_str.parse().unwrap();
            jsonb_values.push(Some(owned_jsonb.to_vec()));
        }

        // Create test batch with JSONB data
        let schema = Arc::new(Schema::new(vec![
            Field::new(VALUE_COLUMN_NAME, DataType::LargeBinary, true),
            Field::new(ROW_ID, DataType::UInt64, false),
        ]));

        let jsonb_array = LargeBinaryArray::from(
            jsonb_values
                .iter()
                .map(|v| v.as_deref())
                .collect::<Vec<_>>(),
        );
        let row_ids = UInt64Array::from(vec![1, 2, 3]);

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(jsonb_array) as ArrayRef,
                Arc::new(row_ids) as ArrayRef,
            ],
        )
        .unwrap();

        let stream = Box::pin(RecordBatchStreamAdapter::new(
            schema.clone(),
            stream::iter(vec![Ok(batch)]),
        )) as SendableRecordBatchStream;

        // Test type inference for integer field
        let (_result_stream, inferred_type) =
            JsonIndexPlugin::extract_json_with_type_info(stream, "$.age".to_string())
                .await
                .unwrap();

        assert_eq!(inferred_type, DataType::Int64);

        // Create new test stream for boolean field
        let batch2 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(LargeBinaryArray::from(vec![
                    json_data[0]
                        .parse::<jsonb::OwnedJsonb>()
                        .ok()
                        .map(|j| j.to_vec())
                        .as_deref(),
                    json_data[1]
                        .parse::<jsonb::OwnedJsonb>()
                        .ok()
                        .map(|j| j.to_vec())
                        .as_deref(),
                    json_data[2]
                        .parse::<jsonb::OwnedJsonb>()
                        .ok()
                        .map(|j| j.to_vec())
                        .as_deref(),
                ])) as ArrayRef,
                Arc::new(UInt64Array::from(vec![1, 2, 3])) as ArrayRef,
            ],
        )
        .unwrap();

        let stream2 = Box::pin(RecordBatchStreamAdapter::new(
            schema.clone(),
            stream::iter(vec![Ok(batch2)]),
        )) as SendableRecordBatchStream;

        // Test type inference for boolean field
        let (_, inferred_type) =
            JsonIndexPlugin::extract_json_with_type_info(stream2, "$.active".to_string())
                .await
                .unwrap();

        assert_eq!(inferred_type, DataType::Boolean);

        // Create test stream for string field
        let batch3 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(LargeBinaryArray::from(vec![
                    json_data[0]
                        .parse::<jsonb::OwnedJsonb>()
                        .ok()
                        .map(|j| j.to_vec())
                        .as_deref(),
                    json_data[1]
                        .parse::<jsonb::OwnedJsonb>()
                        .ok()
                        .map(|j| j.to_vec())
                        .as_deref(),
                    json_data[2]
                        .parse::<jsonb::OwnedJsonb>()
                        .ok()
                        .map(|j| j.to_vec())
                        .as_deref(),
                ])) as ArrayRef,
                Arc::new(UInt64Array::from(vec![1, 2, 3])) as ArrayRef,
            ],
        )
        .unwrap();

        let stream3 = Box::pin(RecordBatchStreamAdapter::new(
            schema,
            stream::iter(vec![Ok(batch3)]),
        )) as SendableRecordBatchStream;

        // Test type inference for string field
        let (_, inferred_type) =
            JsonIndexPlugin::extract_json_with_type_info(stream3, "$.name".to_string())
                .await
                .unwrap();

        assert_eq!(inferred_type, DataType::Utf8);
    }

    /// Trains a JSON-path index of `target_index_type` over `json_docs` (fed to the
    /// trainer in exactly the given order, with row ids `0..json_docs.len()`) and
    /// returns the loaded index. `store` is a caller-owned `LanceIndexStore` so the
    /// caller controls how long the backing `TempObjDir` stays alive.
    async fn train_and_load_json_index(
        store: Arc<dyn IndexStore>,
        target_index_type: &str,
        expected_ordering: TrainingOrdering,
        path: &str,
        json_docs: &[&str],
    ) -> Arc<dyn ScalarIndex> {
        use crate::progress::noop_progress;
        use arrow_array::{LargeBinaryArray, UInt64Array};
        use futures::stream;

        let registry = IndexPluginRegistry::with_default_plugins();
        let plugin = registry.get_plugin_by_name("json").unwrap();
        let trainer = plugin.basic_trainer().unwrap();
        let params = format!(r#"{{"target_index_type":"{target_index_type}","path":"{path}"}}"#);
        let request = trainer
            .new_training_request(
                &params,
                &Field::new(VALUE_COLUMN_NAME, DataType::LargeBinary, true),
            )
            .unwrap();

        assert_eq!(request.criteria().ordering, expected_ordering);

        let jsonb: Vec<Vec<u8>> = json_docs
            .iter()
            .map(|s| s.parse::<jsonb::OwnedJsonb>().unwrap().to_vec())
            .collect();

        let mut fields = Vec::with_capacity(3);
        fields.push(Field::new(VALUE_COLUMN_NAME, DataType::LargeBinary, true));
        let mut columns = Vec::with_capacity(3);
        columns.push(Arc::new(LargeBinaryArray::from(
            jsonb.iter().map(|v| Some(v.as_slice())).collect::<Vec<_>>(),
        )) as ArrayRef);
        if request.criteria().needs_row_ids {
            fields.push(Field::new(ROW_ID, DataType::UInt64, false));
            columns.push(Arc::new(UInt64Array::from(
                (0..json_docs.len() as u64).collect::<Vec<_>>(),
            )) as ArrayRef);
        }
        if request.criteria().needs_row_addrs {
            fields.push(Field::new(ROW_ADDR, DataType::UInt64, false));
            columns.push(Arc::new(UInt64Array::from(
                (0..json_docs.len())
                    .map(|row_idx| {
                        RowAddress::new_from_parts((row_idx / 2) as u32, (row_idx % 2) as u32)
                            .into()
                    })
                    .collect::<Vec<u64>>(),
            )) as ArrayRef);
        }
        let schema = Arc::new(Schema::new(fields));
        let batch = RecordBatch::try_new(schema.clone(), columns).unwrap();
        let data = Box::pin(RecordBatchStreamAdapter::new(
            schema,
            stream::iter(vec![Ok(batch)]),
        )) as SendableRecordBatchStream;

        let created = trainer
            .train_index(data, store.as_ref(), request, None, noop_progress())
            .await
            .unwrap();

        plugin
            .load_index(store, &created.index_details, None, &LanceCache::no_cache())
            .await
            .unwrap()
    }

    fn local_json_index_store() -> (Arc<dyn IndexStore>, lance_core::utils::tempfile::TempObjDir) {
        use crate::scalar::lance_format::LanceIndexStore;
        use lance_core::utils::tempfile::TempObjDir;
        use lance_io::object_store::ObjectStore;

        let tmpdir = TempObjDir::default();
        let store = Arc::new(LanceIndexStore::new(
            Arc::new(ObjectStore::local()),
            tmpdir.clone(),
            Arc::new(LanceCache::no_cache()),
        )) as Arc<dyn IndexStore>;
        (store, tmpdir)
    }

    /// Regression test for https://github.com/lance-format/lance/issues/7859.
    #[rstest]
    #[case::zonemap("zonemap", TrainingOrdering::Addresses)]
    #[case::fm("fm", TrainingOrdering::None)]
    #[tokio::test]
    async fn test_json_index_preserves_row_addresses(
        #[case] target_index_type: &str,
        #[case] expected_ordering: TrainingOrdering,
    ) {
        let (store, _tmpdir) = local_json_index_store();
        let index = train_and_load_json_index(
            store,
            target_index_type,
            expected_ordering,
            "value",
            &[
                r#"{"value": "alpha"}"#,
                r#"{"value": "bravo"}"#,
                r#"{"value": "charlie"}"#,
            ],
        )
        .await;

        assert_eq!(
            index.calculate_included_frags().await.unwrap(),
            RoaringBitmap::from_iter([0, 1])
        );
    }

    /// Regression test for https://github.com/lance-format/lance/issues/7485.
    ///
    /// A JSON-path btree index over float values returned wrong results because the
    /// btree trainer assumes its input arrives sorted by value (page min/max come from
    /// the first/last row of each page), but the value at `path` is extracted by this
    /// plugin *after* the scanner has already produced its rows, so a scan sorted on
    /// the raw JSON column does not sort the extracted value. This exercises the fix
    /// end to end: `JsonTrainingRequest::criteria()` must ask for unordered input (so
    /// the scanner does not waste time sorting on the wrong key), and `train_index` must
    /// sort the extracted value stream itself before training the target btree.
    ///
    /// Rows are fed in raw storage order (not sorted by value) to simulate what an
    /// unordered scan would produce.
    ///
    /// Each case below runs a spilling `SortExec` that reserves a non-spillable merge
    /// buffer from the process-wide cached DataFusion memory pool (see
    /// `get_session_context`); running the cases concurrently contends for that shared
    /// pool and can spuriously exhaust it, so this guard serializes them.
    static FLOAT_INDEX_CASE_GUARD: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    #[rstest]
    #[case::range_gt_zero(
        SargableQuery::Range(Bound::Excluded(ScalarValue::Float64(Some(0.0))), Bound::Unbounded),
        vec![0, 1]
    )]
    #[case::range_gte_page_min(
        SargableQuery::Range(Bound::Included(ScalarValue::Float64(Some(10.5))), Bound::Unbounded),
        vec![0, 1]
    )]
    #[case::equals_non_exact_float(
        SargableQuery::Equals(ScalarValue::Float64(Some(40.1))),
        vec![1]
    )]
    #[case::equals_exact_float(SargableQuery::Equals(ScalarValue::Float64(Some(10.5))), vec![0])]
    #[case::range_covers_all(
        SargableQuery::Range(Bound::Unbounded, Bound::Excluded(ScalarValue::Float64(Some(100.0)))),
        vec![0, 1, 2]
    )]
    #[tokio::test]
    async fn test_json_float_btree_index_unsorted_input(
        #[case] query: SargableQuery,
        #[case] expected: Vec<u64>,
    ) {
        let _guard = FLOAT_INDEX_CASE_GUARD.lock().await;
        use crate::metrics::NoOpMetricsCollector;
        use lance_select::RowAddrTreeMap;

        // row0=10.5, row1=40.1, row2=-3.2: storage order does not match ascending value
        // order (-3.2, 10.5, 40.1), so a btree trained on this order without an explicit
        // value sort would record a corrupted page max of -3.2.
        let (store, _tmpdir) = local_json_index_store();
        let index = train_and_load_json_index(
            store,
            "btree",
            TrainingOrdering::None,
            "latitude",
            &[
                r#"{"latitude": 10.5}"#,
                r#"{"latitude": 40.1}"#,
                r#"{"latitude": -3.2}"#,
            ],
        )
        .await;

        let json_query = JsonQuery::new(Arc::new(query.clone()), "latitude".to_string());
        let result = index
            .search(&json_query, &NoOpMetricsCollector)
            .await
            .unwrap();
        assert_eq!(
            result,
            SearchResult::exact(RowAddrTreeMap::from_iter(expected.iter().copied())),
            "query {query:?}"
        );
    }

    /// Regression test for a null value at `path` surviving `sort_stream_by_value`.
    ///
    /// `sort_stream_by_value` sorts the extracted `(value, row_id)` stream with
    /// `nulls_first: true`. This checks that a null row's row_id stays paired with its
    /// (null) value through that sort -- if the sort ever reordered values without their
    /// row_ids, a null-valued row could be attributed to the wrong id -- and that the
    /// resulting btree still answers `IsNull` and non-null range/equality queries
    /// correctly with nulls mixed in and fed out of value order.
    ///
    /// Row 1's `path` is missing entirely, which is what actually produces a null in the
    /// extracted value column (`extract_json_path_with_type` returns `None`, which
    /// `json_extract_with_type_impl` turns into an arrow-null). An explicit JSON `null`
    /// literal at `path` (e.g. `{"v": null}`) is a different, pre-existing case that
    /// `convert_stream_by_type` does not yet handle (it tries to deserialize the JSONB
    /// `null` bytes as the inferred type and errors) -- unrelated to this fix, so it's
    /// out of scope here.
    #[tokio::test]
    async fn test_json_btree_index_null_at_path() {
        use crate::metrics::NoOpMetricsCollector;
        use lance_select::RowAddrTreeMap;

        let _guard = FLOAT_INDEX_CASE_GUARD.lock().await;
        let (store, _tmpdir) = local_json_index_store();
        let index = train_and_load_json_index(
            store,
            "btree",
            TrainingOrdering::None,
            "v",
            &[
                r#"{"v": 40.1}"#,  // row 0
                r#"{"other": 1}"#, // row 1: path missing -> null
                r#"{"v": -3.2}"#,  // row 2
                r#"{"v": 10.5}"#,  // row 3
            ],
        )
        .await;

        let search = |query: SargableQuery| {
            let index = index.clone();
            let json_query = JsonQuery::new(Arc::new(query), "v".to_string());
            async move {
                index
                    .search(&json_query, &NoOpMetricsCollector)
                    .await
                    .unwrap()
            }
        };

        // Range/equality queries carry row 1 in `nulls` (three-valued logic: `NULL > 0`
        // is unknown, not false -- see `NullableRowAddrSet`), which is pre-existing
        // btree/framework behavior. Asserting it exactly here is exactly the property
        // this test targets: row 1's row_id must stay paired with its null value
        // through `sort_stream_by_value`, not just be excluded from `selected`.
        assert_eq!(
            search(SargableQuery::IsNull()).await,
            SearchResult::exact(RowAddrTreeMap::from_iter([1u64])),
            "IsNull"
        );
        assert_eq!(
            search(SargableQuery::Range(
                Bound::Excluded(ScalarValue::Float64(Some(0.0))),
                Bound::Unbounded,
            ))
            .await,
            SearchResult::exact(RowAddrTreeMap::from_iter([0u64, 3]))
                .with_nulls(RowAddrTreeMap::from_iter([1u64])),
            "> 0"
        );
        assert_eq!(
            search(SargableQuery::Equals(ScalarValue::Float64(Some(40.1)))).await,
            SearchResult::exact(RowAddrTreeMap::from_iter([0u64]))
                .with_nulls(RowAddrTreeMap::from_iter([1u64])),
            "= 40.1"
        );
        assert_eq!(
            search(SargableQuery::Range(
                Bound::Unbounded,
                Bound::Excluded(ScalarValue::Float64(Some(100.0))),
            ))
            .await,
            SearchResult::exact(RowAddrTreeMap::from_iter([0u64, 2, 3]))
                .with_nulls(RowAddrTreeMap::from_iter([1u64])),
            "< 100 (null is neither < 100 nor >= 100)"
        );
    }

    /// Regression coverage for the non-`Values`-ordering branch in `train_index`: a
    /// JSON-path index over a target that does not need value-ordered input (ngram
    /// requires `TrainingOrdering::None`) must skip `sort_stream_by_value` entirely and
    /// still produce correct results from rows fed out of value order.
    #[tokio::test]
    async fn test_json_ngram_index_skips_value_sort() {
        use crate::metrics::NoOpMetricsCollector;
        use lance_select::RowAddrTreeMap;

        let (store, _tmpdir) = local_json_index_store();
        let index = train_and_load_json_index(
            store,
            "ngram",
            TrainingOrdering::None,
            "tag",
            &[
                r#"{"tag": "unique-charlie"}"#,
                r#"{"tag": "unique-alpha"}"#,
                r#"{"tag": "unique-bravo"}"#,
            ],
        )
        .await;

        let json_query = JsonQuery::new(
            Arc::new(TextQuery::StringContains("unique-bravo".to_string())),
            "tag".to_string(),
        );
        let result = index
            .search(&json_query, &NoOpMetricsCollector)
            .await
            .unwrap();
        assert_eq!(
            result,
            SearchResult::at_most(RowAddrTreeMap::from_iter([2u64])),
        );
    }
}
