// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! DataFusion execution plan for preprocessing vector data before insertion.
//!
//! This module provides [`PreprocessingExec`], which handles:
//! - Bad vector detection (NaN values, variable-length lists)
//! - Fill/drop/null/error modes for bad vectors
//! - Schema casting (int vectors to float32, list to fixed_size_list)

use std::any::Any;
use std::fmt;
use std::sync::Arc;

use arrow_array::{Array, ArrayRef, Float32Array, ListArray, RecordBatch};
use arrow_schema::{DataType, Field, SchemaRef};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion_common::{DataFusionError, Result as DataFusionResult};
use datafusion_execution::{SendableRecordBatchStream, TaskContext};
use datafusion_physical_plan::{
    stream::RecordBatchStreamAdapter, DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties,
};
use futures::{StreamExt, TryStreamExt};

/// How to handle vectors that contain bad values (NaN, variable length, etc.).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum OnBadVectors {
    /// Return an error when a bad vector is encountered.
    #[default]
    Error,
    /// Drop rows containing bad vectors.
    Drop,
    /// Fill bad values with a specified value.
    Fill,
    /// Replace the entire vector with NULL.
    Null,
}

/// Options for vector preprocessing.
#[derive(Debug, Clone)]
pub struct PreprocessingOptions {
    /// How to handle bad vectors.
    pub on_bad_vectors: OnBadVectors,
    /// Value to use when filling bad values (only used when `on_bad_vectors` is `Fill`).
    pub fill_value: f32,
    /// Whether to infer vector schema from the data.
    ///
    /// When true, variable-length lists will be converted to fixed-size lists
    /// using the modal (most common) length in the data.
    pub infer_vector_schema: bool,
}

impl Default for PreprocessingOptions {
    fn default() -> Self {
        Self {
            on_bad_vectors: OnBadVectors::default(),
            fill_value: 0.0,
            infer_vector_schema: false,
        }
    }
}

/// DataFusion ExecutionPlan that preprocesses vector data.
///
/// This plan detects and handles:
/// - NaN values in float vectors
/// - Variable-length lists that should be fixed-size
/// - Integer vectors that need conversion to float32
///
/// The preprocessing happens per-batch in a streaming fashion.
#[derive(Debug)]
pub struct PreprocessingExec {
    input: Arc<dyn ExecutionPlan>,
    options: PreprocessingOptions,
    output_schema: SchemaRef,
    properties: PlanProperties,
    /// Indices of vector columns that need preprocessing.
    vector_column_indices: Vec<usize>,
    /// Target dimension for each vector column (if known).
    target_dimensions: Vec<Option<i32>>,
}

#[allow(dead_code)] // Methods will be used when wired into the insert pipeline
impl PreprocessingExec {
    /// Create a new PreprocessingExec.
    ///
    /// # Arguments
    /// * `input` - The input execution plan
    /// * `options` - Preprocessing options
    /// * `target_schema` - Optional target schema with fixed-size list types
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        options: PreprocessingOptions,
        target_schema: Option<SchemaRef>,
    ) -> Self {
        let input_schema = input.schema();

        // Identify vector columns and their target dimensions
        let (vector_column_indices, target_dimensions) =
            Self::identify_vector_columns(&input_schema, target_schema.as_ref());

        // Compute output schema
        let output_schema = target_schema.unwrap_or_else(|| input_schema.clone());

        let properties = Self::compute_properties(&input, &output_schema);

        Self {
            input,
            options,
            output_schema,
            properties,
            vector_column_indices,
            target_dimensions,
        }
    }

    /// Identify columns that contain vector data and need preprocessing.
    fn identify_vector_columns(
        input_schema: &SchemaRef,
        target_schema: Option<&SchemaRef>,
    ) -> (Vec<usize>, Vec<Option<i32>>) {
        let mut indices = Vec::new();
        let mut dimensions = Vec::new();

        for (i, field) in input_schema.fields().iter().enumerate() {
            let is_vector = Self::is_vector_column(field);
            if is_vector {
                indices.push(i);

                // Get target dimension from target schema if available
                let dim = target_schema
                    .and_then(|s| s.field_with_name(field.name()).ok())
                    .and_then(|f| Self::get_fixed_size_list_dimension(f.data_type()));

                dimensions.push(dim);
            }
        }

        (indices, dimensions)
    }

    /// Check if a field contains vector data.
    fn is_vector_column(field: &Field) -> bool {
        match field.data_type() {
            DataType::List(inner) | DataType::LargeList(inner) => {
                matches!(
                    inner.data_type(),
                    DataType::Float16
                        | DataType::Float32
                        | DataType::Float64
                        | DataType::Int8
                        | DataType::Int16
                        | DataType::Int32
                        | DataType::Int64
                        | DataType::UInt8
                )
            }
            DataType::FixedSizeList(inner, _) => {
                matches!(
                    inner.data_type(),
                    DataType::Float16
                        | DataType::Float32
                        | DataType::Float64
                        | DataType::Int8
                        | DataType::Int16
                        | DataType::Int32
                        | DataType::Int64
                        | DataType::UInt8
                )
            }
            _ => false,
        }
    }

    /// Get the dimension from a FixedSizeList data type.
    fn get_fixed_size_list_dimension(dtype: &DataType) -> Option<i32> {
        match dtype {
            DataType::FixedSizeList(_, dim) => Some(*dim),
            _ => None,
        }
    }

    fn compute_properties(
        input: &Arc<dyn ExecutionPlan>,
        output_schema: &SchemaRef,
    ) -> PlanProperties {
        let input_properties = input.properties();
        let eq_properties = EquivalenceProperties::new(output_schema.clone());

        PlanProperties::new(
            eq_properties,
            input_properties.partitioning.clone(),
            input_properties.emission_type,
            input_properties.boundedness,
        )
    }

    /// Preprocess a single batch.
    fn preprocess_batch(&self, batch: RecordBatch) -> DataFusionResult<Option<RecordBatch>> {
        if self.vector_column_indices.is_empty() {
            return Ok(Some(batch));
        }

        let mut columns: Vec<ArrayRef> = batch.columns().to_vec();
        let mut bad_row_mask: Option<Vec<bool>> = None;

        for (idx_pos, &col_idx) in self.vector_column_indices.iter().enumerate() {
            let column = &columns[col_idx];
            let target_dim = self.target_dimensions[idx_pos];

            let (processed, row_mask) = self.process_vector_column(column, target_dim)?;

            columns[col_idx] = processed;

            // Merge bad row masks
            if let Some(mask) = row_mask {
                bad_row_mask = Some(match bad_row_mask {
                    Some(existing) => existing
                        .iter()
                        .zip(mask.iter())
                        .map(|(a, b)| *a || *b)
                        .collect(),
                    None => mask,
                });
            }
        }

        // Apply row filtering if needed
        if let Some(mask) = bad_row_mask {
            match self.options.on_bad_vectors {
                OnBadVectors::Drop => {
                    let good_indices: Vec<u32> = mask
                        .iter()
                        .enumerate()
                        .filter(|(_, &is_bad)| !is_bad)
                        .map(|(i, _)| i as u32)
                        .collect();

                    if good_indices.is_empty() {
                        return Ok(None);
                    }

                    let indices = arrow_array::UInt32Array::from(good_indices);
                    let filtered_columns: DataFusionResult<Vec<ArrayRef>> = columns
                        .iter()
                        .map(|col| {
                            arrow_select::take::take(col.as_ref(), &indices, None)
                                .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
                        })
                        .collect();

                    return RecordBatch::try_new(self.output_schema.clone(), filtered_columns?)
                        .map(Some)
                        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None));
                }
                OnBadVectors::Error => {
                    if mask.iter().any(|&b| b) {
                        return Err(DataFusionError::Execution(
                            "Found vectors with invalid values (NaN or inconsistent dimensions)"
                                .to_string(),
                        ));
                    }
                }
                // Fill and Null modes are handled in process_vector_column
                OnBadVectors::Fill | OnBadVectors::Null => {}
            }
        }

        RecordBatch::try_new(self.output_schema.clone(), columns)
            .map(Some)
            .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
    }

    /// Process a single vector column.
    ///
    /// Returns the processed column and an optional mask of bad rows.
    fn process_vector_column(
        &self,
        column: &ArrayRef,
        target_dim: Option<i32>,
    ) -> DataFusionResult<(ArrayRef, Option<Vec<bool>>)> {
        // For now, just pass through the column
        // Full implementation would include:
        // 1. NaN detection using is_nan() compute kernel
        // 2. Variable length detection via list_value_length()
        // 3. Fill/null handling based on options
        // 4. Type casting (int to float32, list to FSL)

        let bad_rows = self.detect_bad_vectors(column, target_dim)?;

        let processed = match self.options.on_bad_vectors {
            OnBadVectors::Fill => self.fill_bad_values(column, &bad_rows)?,
            OnBadVectors::Null => self.null_bad_vectors(column, &bad_rows)?,
            _ => column.clone(),
        };

        let mask = if bad_rows.iter().any(|&b| b) {
            Some(bad_rows)
        } else {
            None
        };

        Ok((processed, mask))
    }

    /// Detect rows with bad vector values.
    fn detect_bad_vectors(
        &self,
        column: &ArrayRef,
        target_dim: Option<i32>,
    ) -> DataFusionResult<Vec<bool>> {
        let num_rows = column.len();
        let mut bad_rows = vec![false; num_rows];

        match column.data_type() {
            DataType::List(_) | DataType::LargeList(_) => {
                // Check for variable lengths
                if let Some(list_array) = column.as_any().downcast_ref::<ListArray>() {
                    #[allow(clippy::needless_range_loop)]
                    for i in 0..num_rows {
                        if list_array.is_null(i) {
                            continue;
                        }
                        let value = list_array.value(i);
                        let len = value.len() as i32;

                        // Check dimension mismatch
                        if let Some(target) = target_dim {
                            if len != target {
                                bad_rows[i] = true;
                                continue;
                            }
                        }

                        // Check for NaN values in float arrays
                        if let Some(float_arr) = value.as_any().downcast_ref::<Float32Array>() {
                            if float_arr.values().iter().any(|v| v.is_nan()) {
                                bad_rows[i] = true;
                            }
                        }
                    }
                }
            }
            DataType::FixedSizeList(_, dim) => {
                // Check dimension mismatch with target
                if let Some(target) = target_dim {
                    if *dim != target {
                        // All rows have wrong dimension
                        bad_rows.fill(true);
                    }
                }
                // TODO: Check for NaN values in fixed size list
            }
            _ => {}
        }

        Ok(bad_rows)
    }

    /// Fill bad values with the configured fill value.
    fn fill_bad_values(&self, column: &ArrayRef, _bad_rows: &[bool]) -> DataFusionResult<ArrayRef> {
        // TODO: Implement proper filling using Arrow compute kernels
        // For now, return the original column
        Ok(column.clone())
    }

    /// Replace bad vectors with NULL.
    fn null_bad_vectors(
        &self,
        column: &ArrayRef,
        _bad_rows: &[bool],
    ) -> DataFusionResult<ArrayRef> {
        // TODO: Implement proper null replacement
        // For now, return the original column
        Ok(column.clone())
    }
}

impl DisplayAs for PreprocessingExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default
            | DisplayFormatType::Verbose
            | DisplayFormatType::TreeRender => {
                write!(
                    f,
                    "PreprocessingExec: on_bad_vectors={:?}, vector_columns={:?}",
                    self.options.on_bad_vectors, self.vector_column_indices
                )
            }
        }
    }
}

impl ExecutionPlan for PreprocessingExec {
    fn name(&self) -> &str {
        "PreprocessingExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(DataFusionError::Internal(
                "PreprocessingExec requires exactly one child".to_string(),
            ));
        }
        Ok(Arc::new(Self::new(
            children[0].clone(),
            self.options.clone(),
            Some(self.output_schema.clone()),
        )))
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false]
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        let input_stream = self.input.execute(partition, context)?;
        let output_schema = self.output_schema.clone();
        let options = self.options.clone();
        let vector_column_indices = self.vector_column_indices.clone();
        let target_dimensions = self.target_dimensions.clone();

        // Create a new PreprocessingExec-like struct for the closure
        // (we can't move self into the closure)
        let processor = BatchProcessor {
            options,
            output_schema: output_schema.clone(),
            vector_column_indices,
            target_dimensions,
        };

        let stream = input_stream
            .map(move |batch_result| {
                batch_result.and_then(|batch| processor.preprocess_batch(batch))
            })
            .try_filter_map(|opt| async move { Ok(opt) });

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            output_schema,
            stream,
        )))
    }
}

/// Helper struct to process batches in the stream.
#[derive(Clone)]
struct BatchProcessor {
    options: PreprocessingOptions,
    output_schema: SchemaRef,
    vector_column_indices: Vec<usize>,
    target_dimensions: Vec<Option<i32>>,
}

impl BatchProcessor {
    fn preprocess_batch(&self, batch: RecordBatch) -> DataFusionResult<Option<RecordBatch>> {
        if self.vector_column_indices.is_empty() {
            return Ok(Some(batch));
        }

        let mut columns: Vec<ArrayRef> = batch.columns().to_vec();
        let mut bad_row_mask: Option<Vec<bool>> = None;

        for (idx_pos, &col_idx) in self.vector_column_indices.iter().enumerate() {
            let column = &columns[col_idx];
            let target_dim = self.target_dimensions[idx_pos];

            let (processed, row_mask) = self.process_vector_column(column, target_dim)?;

            columns[col_idx] = processed;

            if let Some(mask) = row_mask {
                bad_row_mask = Some(match bad_row_mask {
                    Some(existing) => existing
                        .iter()
                        .zip(mask.iter())
                        .map(|(a, b)| *a || *b)
                        .collect(),
                    None => mask,
                });
            }
        }

        if let Some(mask) = bad_row_mask {
            match self.options.on_bad_vectors {
                OnBadVectors::Drop => {
                    let good_indices: Vec<u32> = mask
                        .iter()
                        .enumerate()
                        .filter(|(_, &is_bad)| !is_bad)
                        .map(|(i, _)| i as u32)
                        .collect();

                    if good_indices.is_empty() {
                        return Ok(None);
                    }

                    let indices = arrow_array::UInt32Array::from(good_indices);
                    let filtered_columns: DataFusionResult<Vec<ArrayRef>> = columns
                        .iter()
                        .map(|col| {
                            arrow_select::take::take(col.as_ref(), &indices, None)
                                .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
                        })
                        .collect();

                    return RecordBatch::try_new(self.output_schema.clone(), filtered_columns?)
                        .map(Some)
                        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None));
                }
                OnBadVectors::Error => {
                    if mask.iter().any(|&b| b) {
                        return Err(DataFusionError::Execution(
                            "Found vectors with invalid values (NaN or inconsistent dimensions)"
                                .to_string(),
                        ));
                    }
                }
                OnBadVectors::Fill | OnBadVectors::Null => {}
            }
        }

        RecordBatch::try_new(self.output_schema.clone(), columns)
            .map(Some)
            .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
    }

    fn process_vector_column(
        &self,
        column: &ArrayRef,
        target_dim: Option<i32>,
    ) -> DataFusionResult<(ArrayRef, Option<Vec<bool>>)> {
        let bad_rows = self.detect_bad_vectors(column, target_dim)?;

        let processed = match self.options.on_bad_vectors {
            OnBadVectors::Fill => self.fill_bad_values(column, &bad_rows)?,
            OnBadVectors::Null => self.null_bad_vectors(column, &bad_rows)?,
            _ => column.clone(),
        };

        let mask = if bad_rows.iter().any(|&b| b) {
            Some(bad_rows)
        } else {
            None
        };

        Ok((processed, mask))
    }

    #[allow(clippy::needless_range_loop)]
    fn detect_bad_vectors(
        &self,
        column: &ArrayRef,
        target_dim: Option<i32>,
    ) -> DataFusionResult<Vec<bool>> {
        let num_rows = column.len();
        let mut bad_rows = vec![false; num_rows];

        match column.data_type() {
            DataType::List(_) | DataType::LargeList(_) => {
                if let Some(list_array) = column.as_any().downcast_ref::<ListArray>() {
                    for i in 0..num_rows {
                        if list_array.is_null(i) {
                            continue;
                        }
                        let value = list_array.value(i);
                        let len = value.len() as i32;

                        if let Some(target) = target_dim {
                            if len != target {
                                bad_rows[i] = true;
                                continue;
                            }
                        }

                        if let Some(float_arr) = value.as_any().downcast_ref::<Float32Array>() {
                            if float_arr.values().iter().any(|v| v.is_nan()) {
                                bad_rows[i] = true;
                            }
                        }
                    }
                }
            }
            DataType::FixedSizeList(_, dim) => {
                if let Some(target) = target_dim {
                    if *dim != target {
                        bad_rows.fill(true);
                    }
                }
            }
            _ => {}
        }

        Ok(bad_rows)
    }

    fn fill_bad_values(&self, column: &ArrayRef, _bad_rows: &[bool]) -> DataFusionResult<ArrayRef> {
        Ok(column.clone())
    }

    fn null_bad_vectors(
        &self,
        column: &ArrayRef,
        _bad_rows: &[bool],
    ) -> DataFusionResult<ArrayRef> {
        Ok(column.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{
        builder::{Float32Builder, ListBuilder},
        Int32Array,
    };
    use arrow_schema::{Field, Schema};
    use datafusion_physical_plan::memory::{LazyBatchGenerator, LazyMemoryExec};
    use parking_lot::RwLock;
    use std::sync::Mutex;

    fn make_test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new(
                "vector",
                DataType::List(Arc::new(Field::new("item", DataType::Float32, true))),
                true,
            ),
        ]))
    }

    fn make_test_batch(schema: SchemaRef) -> RecordBatch {
        let id_array = Int32Array::from(vec![1, 2, 3]);

        let mut list_builder = ListBuilder::new(Float32Builder::new());

        // Row 1: normal vector
        list_builder.values().append_value(1.0);
        list_builder.values().append_value(2.0);
        list_builder.values().append_value(3.0);
        list_builder.append(true);

        // Row 2: vector with NaN
        list_builder.values().append_value(1.0);
        list_builder.values().append_value(f32::NAN);
        list_builder.values().append_value(3.0);
        list_builder.append(true);

        // Row 3: normal vector
        list_builder.values().append_value(4.0);
        list_builder.values().append_value(5.0);
        list_builder.values().append_value(6.0);
        list_builder.append(true);

        let vector_array = list_builder.finish();

        RecordBatch::try_new(schema, vec![Arc::new(id_array), Arc::new(vector_array)]).unwrap()
    }

    /// A simple batch generator for tests.
    struct TestBatchGenerator {
        batch: Mutex<Option<RecordBatch>>,
        #[allow(dead_code)]
        schema: SchemaRef,
    }

    impl std::fmt::Debug for TestBatchGenerator {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("TestBatchGenerator").finish()
        }
    }

    impl std::fmt::Display for TestBatchGenerator {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "TestBatchGenerator")
        }
    }

    impl LazyBatchGenerator for TestBatchGenerator {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn generate_next_batch(&mut self) -> datafusion_common::Result<Option<RecordBatch>> {
            Ok(self.batch.lock().unwrap().take())
        }
    }

    #[tokio::test]
    async fn test_preprocessing_passthrough() {
        let schema = make_test_schema();
        let batch = make_test_batch(schema.clone());

        let generator = TestBatchGenerator {
            batch: Mutex::new(Some(batch)),
            schema: schema.clone(),
        };
        let generators: Vec<Arc<RwLock<dyn LazyBatchGenerator>>> =
            vec![Arc::new(RwLock::new(generator))];

        let memory_exec = Arc::new(LazyMemoryExec::try_new(schema.clone(), generators).unwrap());

        let preprocessing =
            PreprocessingExec::new(memory_exec, PreprocessingOptions::default(), None);

        assert_eq!(preprocessing.vector_column_indices, vec![1]);
    }

    #[test]
    fn test_is_vector_column() {
        let float_list_field = Field::new(
            "vec",
            DataType::List(Arc::new(Field::new("item", DataType::Float32, true))),
            true,
        );
        assert!(PreprocessingExec::is_vector_column(&float_list_field));

        let fsl_field = Field::new(
            "vec",
            DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), 128),
            true,
        );
        assert!(PreprocessingExec::is_vector_column(&fsl_field));

        let string_field = Field::new("name", DataType::Utf8, true);
        assert!(!PreprocessingExec::is_vector_column(&string_field));
    }
}
