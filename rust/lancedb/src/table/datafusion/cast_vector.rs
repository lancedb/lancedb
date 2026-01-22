// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Physical expression for casting variable-length lists to fixed-size lists.
//!
//! This is used to convert vector columns from `List<T>` to `FixedSizeList<T, N>`
//! when inserting data into tables with known vector dimensions.

use std::any::Any;
use std::fmt::{self, Formatter};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use arrow::buffer::NullBuffer;
use arrow_array::{
    new_null_array, Array, ArrayRef, FixedSizeListArray, Float32Array, ListArray,
};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion_common::{exec_err, DataFusionError, Result as DataFusionResult};
use datafusion_expr::ColumnarValue;
use arrow::buffer::OffsetBuffer;
use datafusion_physical_plan::expressions::{cast, Column};
use datafusion_physical_plan::projection::ProjectionExec;
use datafusion_physical_plan::{ExecutionPlan, PhysicalExpr};

/// A DataFusion PhysicalExpr that casts List/LargeList to FixedSizeList.
///
/// This expression handles:
/// - List<T> or LargeList<T> → FixedSizeList<T, dimension>
/// - NaN detection (returns NULL for rows with NaN values)
/// - Dimension validation (returns NULL for wrong-sized lists)
#[derive(Debug)]
pub struct CastToFixedSizeListExpr {
    /// The child expression that provides the source data
    source: Arc<dyn PhysicalExpr>,
    /// Target dimension for the FixedSizeList
    dimension: i32,
    /// Inner element type
    inner_type: DataType,
    /// The output data type
    return_type: DataType,
}

impl CastToFixedSizeListExpr {
    /// Create a new CastToFixedSizeListExpr.
    ///
    /// # Arguments
    /// * `source` - The physical expression that provides the List column
    /// * `dimension` - The target fixed size list dimension
    /// * `inner_type` - The element type of the list
    pub fn new(source: Arc<dyn PhysicalExpr>, dimension: i32, inner_type: DataType) -> Self {
        let return_type = DataType::FixedSizeList(
            Arc::new(Field::new("item", inner_type.clone(), true)),
            dimension,
        );
        Self {
            source,
            dimension,
            inner_type,
            return_type,
        }
    }

    /// Create from source expression and target FixedSizeList type.
    pub fn try_new_from_target_type(
        source: Arc<dyn PhysicalExpr>,
        target_type: &DataType,
    ) -> DataFusionResult<Self> {
        match target_type {
            DataType::FixedSizeList(field, dim) => {
                Ok(Self::new(source, *dim, field.data_type().clone()))
            }
            other => exec_err!(
                "CastToFixedSizeListExpr target must be FixedSizeList, got {:?}",
                other
            ),
        }
    }

    /// Convert a ListArray to a FixedSizeListArray.
    fn list_to_fsl(&self, list_array: &ListArray) -> DataFusionResult<FixedSizeListArray> {
        let num_rows = list_array.len();
        let dim_usize = self.dimension as usize;

        // Build validity mask and collect values
        let mut validity = vec![true; num_rows];
        let mut all_values: Vec<ArrayRef> = Vec::with_capacity(num_rows);

        for i in 0..num_rows {
            if list_array.is_null(i) {
                validity[i] = false;
                all_values.push(new_null_array(&self.inner_type, dim_usize));
                continue;
            }

            let value = list_array.value(i);
            let len = value.len();

            // Check dimension
            if len != dim_usize {
                validity[i] = false;
                all_values.push(new_null_array(&self.inner_type, dim_usize));
                continue;
            }

            // Check for NaN values in float arrays
            if Self::contains_nan(&value) {
                validity[i] = false;
                all_values.push(new_null_array(&self.inner_type, dim_usize));
                continue;
            }

            all_values.push(value);
        }

        // Concatenate all values into a single array
        let value_refs: Vec<&dyn Array> = all_values.iter().map(|a| a.as_ref()).collect();
        let mut values = arrow_select::concat::concat(&value_refs)
            .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;

        // Cast inner values to target type if needed (e.g., Float64 -> Float32)
        if values.data_type() != &self.inner_type {
            values = arrow_cast::cast(&values, &self.inner_type)
                .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
        }

        // Build null buffer
        let null_buffer = NullBuffer::from(validity);

        // Create FixedSizeListArray
        let field = Arc::new(Field::new("item", self.inner_type.clone(), true));
        FixedSizeListArray::try_new(field, self.dimension, values, Some(null_buffer))
            .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
    }

    /// Check if an array contains NaN values.
    fn contains_nan(array: &ArrayRef) -> bool {
        if let Some(float_arr) = array.as_any().downcast_ref::<Float32Array>() {
            float_arr.values().iter().any(|v| v.is_nan())
        } else if let Some(float_arr) = array
            .as_any()
            .downcast_ref::<arrow_array::Float64Array>()
        {
            float_arr.values().iter().any(|v| v.is_nan())
        } else {
            false
        }
    }

    /// Handle FixedSizeListArray input (validate NaN and dimension).
    fn fsl_to_fsl(&self, fsl_array: &FixedSizeListArray) -> DataFusionResult<FixedSizeListArray> {
        let input_dim = fsl_array.value_length();
        let num_rows = fsl_array.len();

        if input_dim == self.dimension {
            // Same dimension - just check for NaN values
            let mut validity: Vec<bool> = (0..num_rows).map(|i| !fsl_array.is_null(i)).collect();

            let mut has_changes = false;
            for i in 0..num_rows {
                if validity[i] && Self::contains_nan(&fsl_array.value(i)) {
                    validity[i] = false;
                    has_changes = true;
                }
            }

            // Cast inner values if needed (e.g., Float64 -> Float32)
            let mut values = fsl_array.values().clone();
            let needs_inner_cast = values.data_type() != &self.inner_type;
            if needs_inner_cast {
                values = arrow_cast::cast(&values, &self.inner_type)
                    .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
                has_changes = true;
            }

            // If no changes needed, return clone
            if !has_changes {
                return Ok(fsl_array.clone());
            }

            // Rebuild with new validity and/or cast values
            let null_buffer = NullBuffer::from(validity);
            let field = Arc::new(Field::new("item", self.inner_type.clone(), true));
            FixedSizeListArray::try_new(field, self.dimension, values, Some(null_buffer))
                .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
        } else {
            // Dimension mismatch - all rows become null
            let field = Arc::new(Field::new("item", self.inner_type.clone(), true));
            let values = new_null_array(&self.inner_type, num_rows * self.dimension as usize);
            let null_buffer = NullBuffer::from(vec![false; num_rows]);
            FixedSizeListArray::try_new(field, self.dimension, values, Some(null_buffer))
                .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
        }
    }
}

impl std::fmt::Display for CastToFixedSizeListExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "cast_to_fsl({}, dim={})",
            self.source, self.dimension
        )
    }
}

impl PartialEq for CastToFixedSizeListExpr {
    fn eq(&self, other: &Self) -> bool {
        self.dimension == other.dimension
            && self.inner_type == other.inner_type
            && self.source.eq(&other.source)
    }
}

impl Eq for CastToFixedSizeListExpr {}

impl Hash for CastToFixedSizeListExpr {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.dimension.hash(state);
        self.inner_type.hash(state);
        self.source.hash(state);
    }
}

impl PhysicalExpr for CastToFixedSizeListExpr {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self, _input_schema: &Schema) -> DataFusionResult<DataType> {
        Ok(self.return_type.clone())
    }

    fn nullable(&self, _input_schema: &Schema) -> DataFusionResult<bool> {
        // Always nullable since we null out bad rows
        Ok(true)
    }

    fn fmt_sql(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "cast_to_fixed_size_list({}, {})",
            self.source, self.dimension
        )
    }

    fn evaluate(&self, batch: &arrow_array::RecordBatch) -> DataFusionResult<ColumnarValue> {
        let source_value = self.source.evaluate(batch)?;

        let source_array = match source_value {
            ColumnarValue::Array(arr) => arr,
            ColumnarValue::Scalar(s) => s.to_array_of_size(batch.num_rows())?,
        };

        let result: ArrayRef = match source_array.data_type() {
            DataType::List(_) => {
                let list_array = source_array
                    .as_any()
                    .downcast_ref::<ListArray>()
                    .ok_or_else(|| {
                        DataFusionError::Internal("Expected ListArray".to_string())
                    })?;
                Arc::new(self.list_to_fsl(list_array)?)
            }
            DataType::LargeList(_) => {
                // Convert LargeList to List first
                let large_list = source_array
                    .as_any()
                    .downcast_ref::<arrow_array::LargeListArray>()
                    .ok_or_else(|| {
                        DataFusionError::Internal("Expected LargeListArray".to_string())
                    })?;

                // Convert offsets from i64 to i32
                let offsets: Vec<i32> = large_list.offsets().iter().map(|&o| o as i32).collect();
                let offsets = OffsetBuffer::new(offsets.into());

                let list_array = ListArray::try_new(
                    Arc::new(Field::new("item", self.inner_type.clone(), true)),
                    offsets,
                    large_list.values().clone(),
                    large_list.nulls().cloned(),
                )
                .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;

                Arc::new(self.list_to_fsl(&list_array)?)
            }
            DataType::FixedSizeList(_, _) => {
                let fsl_array = source_array
                    .as_any()
                    .downcast_ref::<FixedSizeListArray>()
                    .ok_or_else(|| {
                        DataFusionError::Internal("Expected FixedSizeListArray".to_string())
                    })?;
                Arc::new(self.fsl_to_fsl(fsl_array)?)
            }
            other => {
                return exec_err!(
                    "CastToFixedSizeListExpr input must be List, LargeList, or FixedSizeList, got {:?}",
                    other
                );
            }
        };

        Ok(ColumnarValue::Array(result))
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.source]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> DataFusionResult<Arc<dyn PhysicalExpr>> {
        if children.len() != 1 {
            return exec_err!("CastToFixedSizeListExpr requires exactly 1 child");
        }
        Ok(Arc::new(CastToFixedSizeListExpr::new(
            children[0].clone(),
            self.dimension,
            self.inner_type.clone(),
        )))
    }
}

// ============================================================================
// Helper functions for building schema cast projections
// ============================================================================

/// Create a projection plan that adapts the input schema to the target schema.
///
/// This handles:
/// - Field reordering (matches fields by name)
/// - Simple type casts via DataFusion CAST
/// - List → FixedSizeList conversion via CastToFixedSizeListExpr
///
/// # Arguments
/// * `input` - The input execution plan
/// * `target_schema` - The target schema to adapt to
///
/// # Returns
/// A ProjectionExec that transforms batches to match the target schema
pub fn create_schema_cast_projection(
    input: Arc<dyn ExecutionPlan>,
    target_schema: SchemaRef,
) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
    let input_schema = input.schema();

    // Check if schemas are identical
    if schemas_equal(&input_schema, &target_schema) {
        return Ok(input);
    }

    // Build projection expressions for each field in the INPUT schema
    // (Lance doesn't require field order to match, so we keep input order)
    let mut exprs: Vec<(Arc<dyn PhysicalExpr>, String)> = Vec::new();

    for (idx, input_field) in input_schema.fields().iter().enumerate() {
        let field_name = input_field.name();

        // Find corresponding target field
        let target_field = target_schema.field_with_name(field_name).map_err(|_| {
            DataFusionError::Plan(format!("Field '{}' not found in target schema", field_name))
        })?;

        let input_col: Arc<dyn PhysicalExpr> = Arc::new(Column::new(field_name, idx));

        let expr: Arc<dyn PhysicalExpr> =
            if input_field.data_type() == target_field.data_type() {
                // No conversion needed
                input_col
            } else if needs_fsl_conversion(input_field.data_type(), target_field.data_type()) {
                // Use CastToFixedSizeListExpr for List → FixedSizeList
                Arc::new(CastToFixedSizeListExpr::try_new_from_target_type(
                    input_col,
                    target_field.data_type(),
                )?)
            } else if is_compatible_subset(input_field.data_type(), target_field.data_type()) {
                // Input is a compatible subset of target (e.g., struct with fewer fields)
                // Lance will handle adding missing fields with null values
                input_col
            } else {
                // Use standard CAST for other type conversions
                cast(input_col, input_schema.as_ref(), target_field.data_type().clone())?
            };

        exprs.push((expr, field_name.clone()));
    }

    Ok(Arc::new(ProjectionExec::try_new(exprs, input)?))
}

/// Check if two schemas are equal (ignoring metadata).
fn schemas_equal(a: &SchemaRef, b: &SchemaRef) -> bool {
    if a.fields().len() != b.fields().len() {
        return false;
    }
    // Check that all fields in a exist in b with same type
    // (order doesn't matter for Lance)
    for field_a in a.fields() {
        match b.field_with_name(field_a.name()) {
            Ok(field_b) => {
                if field_a.data_type() != field_b.data_type() {
                    return false;
                }
            }
            Err(_) => return false,
        }
    }
    true
}

/// Check if the input type is a compatible subset of the target type.
///
/// This handles cases where the input has fewer fields/elements than the target
/// but the existing fields are compatible. For example:
/// - Struct(a: Int64) is a compatible subset of Struct(a: Int64, b: String)
/// - List(Struct(a)) is a compatible subset of List(Struct(a, b))
fn is_compatible_subset(from: &DataType, to: &DataType) -> bool {
    match (from, to) {
        // Struct: input fields must be subset of target fields with same types
        (DataType::Struct(from_fields), DataType::Struct(to_fields)) => {
            // All fields in `from` must exist in `to` with compatible types
            from_fields.iter().all(|from_field| {
                to_fields
                    .iter()
                    .find(|to_field| to_field.name() == from_field.name())
                    .is_some_and(|to_field| {
                        from_field.data_type() == to_field.data_type()
                            || is_compatible_subset(from_field.data_type(), to_field.data_type())
                    })
            })
        }
        // List: check inner type compatibility
        (DataType::List(from_field), DataType::List(to_field)) => {
            is_compatible_subset(from_field.data_type(), to_field.data_type())
        }
        // LargeList: check inner type compatibility
        (DataType::LargeList(from_field), DataType::LargeList(to_field)) => {
            is_compatible_subset(from_field.data_type(), to_field.data_type())
        }
        // FixedSizeList: check inner type and dimension
        (
            DataType::FixedSizeList(from_field, from_dim),
            DataType::FixedSizeList(to_field, to_dim),
        ) => {
            from_dim == to_dim
                && is_compatible_subset(from_field.data_type(), to_field.data_type())
        }
        // Other types: not a subset relationship
        _ => false,
    }
}

/// Check if conversion should use CastToFixedSizeListExpr.
///
/// This handles:
/// - List/LargeList → FixedSizeList
/// - FixedSizeList → FixedSizeList (different inner types or dimensions)
fn needs_fsl_conversion(from: &DataType, to: &DataType) -> bool {
    match (from, to) {
        // Variable-length list to fixed-size list
        (DataType::List(_), DataType::FixedSizeList(_, _)) => true,
        (DataType::LargeList(_), DataType::FixedSizeList(_, _)) => true,
        // FSL to FSL with different inner types or dimensions
        (DataType::FixedSizeList(from_field, from_dim), DataType::FixedSizeList(to_field, to_dim)) => {
            from_field.data_type() != to_field.data_type() || from_dim != to_dim
        }
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion_physical_plan::expressions::Column;

    fn make_list_array(data: Vec<Option<Vec<f32>>>) -> ListArray {
        let mut builder =
            arrow_array::builder::ListBuilder::new(arrow_array::builder::Float32Builder::new());
        for item in data {
            match item {
                Some(values) => {
                    for v in values {
                        builder.values().append_value(v);
                    }
                    builder.append(true);
                }
                None => {
                    builder.append(false);
                }
            }
        }
        builder.finish()
    }

    #[test]
    fn test_list_to_fsl_basic() {
        let list_array = make_list_array(vec![
            Some(vec![1.0, 2.0, 3.0]),
            Some(vec![4.0, 5.0, 6.0]),
            Some(vec![7.0, 8.0, 9.0]),
        ]);

        let expr = CastToFixedSizeListExpr::new(
            Arc::new(Column::new("test", 0)),
            3,
            DataType::Float32,
        );

        let result = expr.list_to_fsl(&list_array).unwrap();

        assert_eq!(result.len(), 3);
        assert_eq!(result.value_length(), 3);
        assert!(!result.is_null(0));
        assert!(!result.is_null(1));
        assert!(!result.is_null(2));
    }

    #[test]
    fn test_list_to_fsl_wrong_dimension() {
        let list_array = make_list_array(vec![
            Some(vec![1.0, 2.0, 3.0]),
            Some(vec![4.0, 5.0]), // Wrong dimension
            Some(vec![7.0, 8.0, 9.0]),
        ]);

        let expr = CastToFixedSizeListExpr::new(
            Arc::new(Column::new("test", 0)),
            3,
            DataType::Float32,
        );

        let result = expr.list_to_fsl(&list_array).unwrap();

        assert_eq!(result.len(), 3);
        assert!(!result.is_null(0));
        assert!(result.is_null(1)); // Marked as null due to wrong dimension
        assert!(!result.is_null(2));
    }

    #[test]
    fn test_list_to_fsl_with_nan() {
        let list_array = make_list_array(vec![
            Some(vec![1.0, 2.0, 3.0]),
            Some(vec![4.0, f32::NAN, 6.0]), // Contains NaN
            Some(vec![7.0, 8.0, 9.0]),
        ]);

        let expr = CastToFixedSizeListExpr::new(
            Arc::new(Column::new("test", 0)),
            3,
            DataType::Float32,
        );

        let result = expr.list_to_fsl(&list_array).unwrap();

        assert_eq!(result.len(), 3);
        assert!(!result.is_null(0));
        assert!(result.is_null(1)); // Marked as null due to NaN
        assert!(!result.is_null(2));
    }

    #[test]
    fn test_list_to_fsl_with_nulls() {
        let list_array = make_list_array(vec![
            Some(vec![1.0, 2.0, 3.0]),
            None, // Null row
            Some(vec![7.0, 8.0, 9.0]),
        ]);

        let expr = CastToFixedSizeListExpr::new(
            Arc::new(Column::new("test", 0)),
            3,
            DataType::Float32,
        );

        let result = expr.list_to_fsl(&list_array).unwrap();

        assert_eq!(result.len(), 3);
        assert!(!result.is_null(0));
        assert!(result.is_null(1)); // Already null
        assert!(!result.is_null(2));
    }
}
