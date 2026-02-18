// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! A DataFusion projection that rejects vectors containing NaN values.

use std::any::Any;
use std::sync::{Arc, LazyLock};

use arrow_array::{Array, FixedSizeListArray};
use arrow_schema::{DataType, Field, FieldRef};
use datafusion_common::config::ConfigOptions;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};
use datafusion_physical_expr::ScalarFunctionExpr;
use datafusion_physical_plan::expressions::Column;
use datafusion_physical_plan::projection::ProjectionExec;
use datafusion_physical_plan::{ExecutionPlan, PhysicalExpr};

use crate::{Error, Result};

static REJECT_NAN_UDF: LazyLock<Arc<datafusion_expr::ScalarUDF>> =
    LazyLock::new(|| Arc::new(datafusion_expr::ScalarUDF::from(RejectNanUdf::new())));

/// Returns true if the field is a vector column: FixedSizeList<Float16/32/64>.
fn is_vector_field(field: &Field) -> bool {
    if let DataType::FixedSizeList(child, _) = field.data_type() {
        matches!(
            child.data_type(),
            DataType::Float16 | DataType::Float32 | DataType::Float64
        )
    } else {
        false
    }
}

/// Wraps the input plan with a projection that checks vector columns for NaN values.
///
/// Non-vector columns pass through unchanged. Vector columns are wrapped with a
/// UDF that returns the column as-is if no NaNs are present, or errors otherwise.
pub fn reject_nan_vectors(input: Arc<dyn ExecutionPlan>) -> Result<Arc<dyn ExecutionPlan>> {
    let schema = input.schema();
    let config = Arc::new(ConfigOptions::default());
    let udf = REJECT_NAN_UDF.clone();

    let mut has_vector_cols = false;
    let mut exprs: Vec<(Arc<dyn PhysicalExpr>, String)> = Vec::new();

    for (idx, field) in schema.fields().iter().enumerate() {
        let col_expr: Arc<dyn PhysicalExpr> = Arc::new(Column::new(field.name(), idx));

        if is_vector_field(field) {
            has_vector_cols = true;
            let wrapped: Arc<dyn PhysicalExpr> = Arc::new(ScalarFunctionExpr::new(
                &format!("reject_nan({})", field.name()),
                udf.clone(),
                vec![col_expr],
                Arc::clone(field) as FieldRef,
                config.clone(),
            ));
            exprs.push((wrapped, field.name().clone()));
        } else {
            exprs.push((col_expr, field.name().clone()));
        }
    }

    if !has_vector_cols {
        return Ok(input);
    }

    let projection = ProjectionExec::try_new(exprs, input).map_err(Error::from)?;
    Ok(Arc::new(projection))
}

/// A scalar UDF that passes through FixedSizeList arrays unchanged, but errors
/// if any float values in the list are NaN.
#[derive(Debug, Hash, PartialEq, Eq)]
struct RejectNanUdf {
    signature: Signature,
}

impl RejectNanUdf {
    fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for RejectNanUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "reject_nan"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> datafusion_common::Result<DataType> {
        Ok(arg_types[0].clone())
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion_common::Result<ColumnarValue> {
        let arg = &args.args[0];
        match arg {
            ColumnarValue::Array(array) => {
                check_no_nans(array)?;
                Ok(ColumnarValue::Array(array.clone()))
            }
            ColumnarValue::Scalar(_) => Ok(arg.clone()),
        }
    }
}

fn check_no_nans(array: &dyn Array) -> datafusion_common::Result<()> {
    let fsl = array
        .as_any()
        .downcast_ref::<FixedSizeListArray>()
        .ok_or_else(|| {
            datafusion_common::DataFusionError::Internal(
                "reject_nan expected FixedSizeList".to_string(),
            )
        })?;

    // Only inspect elements that are both in a valid parent row and non-null
    // themselves. Values backing null parent rows or null child elements may
    // contain garbage (including NaN) per the Arrow spec.
    let has_nan = (0..fsl.len()).filter(|i| fsl.is_valid(*i)).any(|i| {
        let row = fsl.value(i);
        match row.data_type() {
            DataType::Float16 => row
                .as_any()
                .downcast_ref::<arrow_array::Float16Array>()
                .unwrap()
                .iter()
                .any(|v| v.is_some_and(|v| v.is_nan())),
            DataType::Float32 => row
                .as_any()
                .downcast_ref::<arrow_array::Float32Array>()
                .unwrap()
                .iter()
                .any(|v| v.is_some_and(|v| v.is_nan())),
            DataType::Float64 => row
                .as_any()
                .downcast_ref::<arrow_array::Float64Array>()
                .unwrap()
                .iter()
                .any(|v| v.is_some_and(|v| v.is_nan())),
            _ => false,
        }
    });

    if has_nan {
        return Err(datafusion_common::DataFusionError::ArrowError(
            Box::new(arrow_schema::ArrowError::ComputeError(
                "Vector column contains NaN values".to_string(),
            )),
            None,
        ));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::Float32Array;

    #[test]
    fn test_passes_clean_vectors() {
        let fsl = FixedSizeListArray::try_new(
            Arc::new(Field::new("item", DataType::Float32, true)),
            2,
            Arc::new(Float32Array::from(vec![1.0, 2.0, 3.0, 4.0])),
            None,
        )
        .unwrap();
        assert!(check_no_nans(&fsl).is_ok());
    }

    #[test]
    fn test_rejects_nan_vectors() {
        let fsl = FixedSizeListArray::try_new(
            Arc::new(Field::new("item", DataType::Float32, true)),
            2,
            Arc::new(Float32Array::from(vec![1.0, f32::NAN, 3.0, 4.0])),
            None,
        )
        .unwrap();
        assert!(check_no_nans(&fsl).is_err());
    }

    #[test]
    fn test_skips_null_rows() {
        // Values backing null rows may contain NaN per the Arrow spec.
        // We should not reject a batch just because of garbage in null slots.
        let values = Float32Array::from(vec![1.0, 2.0, f32::NAN, f32::NAN]);
        let fsl = FixedSizeListArray::try_new(
            Arc::new(Field::new("item", DataType::Float32, true)),
            2,
            Arc::new(values),
            // Row 0 is valid [1.0, 2.0], row 1 is null [NAN, NAN]
            Some(vec![true, false].into()),
        )
        .unwrap();
        assert!(fsl.is_null(1));
        assert!(check_no_nans(&fsl).is_ok());
    }

    #[test]
    fn test_skips_null_elements_within_valid_row() {
        // A valid row with null child elements: the underlying buffer may hold
        // NaN but the null bitmap says they're absent — should not reject.
        let values = Float32Array::from(vec![
            Some(1.0),
            None, // null element — buffer may contain NaN
            Some(3.0),
            None, // null element
        ]);
        let fsl = FixedSizeListArray::try_new(
            Arc::new(Field::new("item", DataType::Float32, true)),
            2,
            Arc::new(values),
            None, // both rows are valid
        )
        .unwrap();
        assert!(check_no_nans(&fsl).is_ok());
    }

    #[test]
    fn test_rejects_nan_in_valid_row_with_nulls_present() {
        // Row 0 is null, row 1 is valid but contains NaN — should reject.
        let values = Float32Array::from(vec![0.0, 0.0, 1.0, f32::NAN]);
        let fsl = FixedSizeListArray::try_new(
            Arc::new(Field::new("item", DataType::Float32, true)),
            2,
            Arc::new(values),
            Some(vec![false, true].into()),
        )
        .unwrap();
        assert!(check_no_nans(&fsl).is_err());
    }

    #[test]
    fn test_is_vector_field() {
        assert!(is_vector_field(&Field::new(
            "v",
            DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), 4),
            false,
        )));
        assert!(is_vector_field(&Field::new(
            "v",
            DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float64, true)), 4),
            false,
        )));
        assert!(!is_vector_field(&Field::new("id", DataType::Int32, false)));
        assert!(!is_vector_field(&Field::new(
            "v",
            DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Int32, true)), 4),
            false,
        )));
    }
}
