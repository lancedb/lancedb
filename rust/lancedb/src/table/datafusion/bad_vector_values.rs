// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Handles bad (NaN-containing) vectors with configurable strategies.

use std::any::Any;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use arrow::buffer::NullBuffer;
use arrow_array::{Array, ArrayRef, BooleanArray, FixedSizeListArray};
use arrow_array::{Float16Array, Float32Array, Float64Array};
use arrow_schema::{DataType, Field, FieldRef};
use datafusion_common::ScalarValue;
use datafusion_common::config::ConfigOptions;
use datafusion_expr::{
    ColumnarValue, Operator, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use datafusion_physical_expr::ScalarFunctionExpr;
use datafusion_physical_expr::expressions::{BinaryExpr, Column};
use datafusion_physical_plan::filter::FilterExec;
use datafusion_physical_plan::projection::ProjectionExec;
use datafusion_physical_plan::{ExecutionPlan, PhysicalExpr};

use crate::table::add_data::BadVectorValueHandling;
use crate::{Error, Result};

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

fn row_has_nan(row: &dyn Array) -> bool {
    match row.data_type() {
        DataType::Float16 => row
            .as_any()
            .downcast_ref::<Float16Array>()
            .unwrap()
            .iter()
            .any(|v| v.is_some_and(|v| v.is_nan())),
        DataType::Float32 => row
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap()
            .iter()
            .any(|v| v.is_some_and(|v| v.is_nan())),
        DataType::Float64 => row
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .iter()
            .any(|v| v.is_some_and(|v| v.is_nan())),
        _ => false,
    }
}

fn child_field_of(fsl: &FixedSizeListArray) -> FieldRef {
    if let DataType::FixedSizeList(f, _) = fsl.data_type() {
        f.clone()
    } else {
        unreachable!()
    }
}

/// Wraps `input` with a plan that applies `handling` to all vector columns.
///
/// Vector columns are `FixedSizeList<Float16|Float32|Float64>`. Non-vector
/// columns pass through unchanged.
pub fn handle_bad_vector_values(
    input: Arc<dyn ExecutionPlan>,
    handling: &BadVectorValueHandling,
) -> Result<Arc<dyn ExecutionPlan>> {
    let schema = input.schema();
    let has_vector_cols = schema.fields().iter().any(|f| is_vector_field(f));
    if !has_vector_cols {
        return Ok(input);
    }

    let config = Arc::new(ConfigOptions::default());

    match handling {
        BadVectorValueHandling::Error => {
            let udf = Arc::new(datafusion_expr::ScalarUDF::from(RejectNanUdf::new()));
            let exprs = build_projection_exprs(&schema, |col_expr, field| {
                Arc::new(ScalarFunctionExpr::new(
                    &format!("reject_nan({})", field.name()),
                    udf.clone(),
                    vec![col_expr],
                    Arc::clone(field) as FieldRef,
                    config.clone(),
                )) as Arc<dyn PhysicalExpr>
            });
            Ok(Arc::new(
                ProjectionExec::try_new(exprs, input).map_err(Error::from)?,
            ))
        }
        BadVectorValueHandling::Drop => {
            let udf = Arc::new(datafusion_expr::ScalarUDF::from(HasNoNanUdf::new()));
            let bool_field = Arc::new(Field::new("", DataType::Boolean, false));
            let mut predicate: Option<Arc<dyn PhysicalExpr>> = None;

            for (idx, field) in schema.fields().iter().enumerate() {
                if !is_vector_field(field) {
                    continue;
                }
                let col_expr: Arc<dyn PhysicalExpr> = Arc::new(Column::new(field.name(), idx));
                let check: Arc<dyn PhysicalExpr> = Arc::new(ScalarFunctionExpr::new(
                    &format!("has_no_nan({})", field.name()),
                    udf.clone(),
                    vec![col_expr],
                    bool_field.clone(),
                    config.clone(),
                ));
                predicate = Some(match predicate {
                    None => check,
                    Some(prev) => Arc::new(BinaryExpr::new(prev, Operator::And, check)),
                });
            }

            Ok(Arc::new(
                FilterExec::try_new(predicate.unwrap(), input).map_err(Error::from)?,
            ))
        }
        BadVectorValueHandling::Null => {
            let udf = Arc::new(datafusion_expr::ScalarUDF::from(NullIfNanUdf::new()));
            let exprs = build_projection_exprs(&schema, |col_expr, field| {
                let nullable_field =
                    Arc::new(Field::new(field.name(), field.data_type().clone(), true));
                Arc::new(ScalarFunctionExpr::new(
                    &format!("null_if_nan({})", field.name()),
                    udf.clone(),
                    vec![col_expr],
                    nullable_field,
                    config.clone(),
                )) as Arc<dyn PhysicalExpr>
            });
            Ok(Arc::new(
                ProjectionExec::try_new(exprs, input).map_err(Error::from)?,
            ))
        }
        BadVectorValueHandling::Fill(v) => {
            let udf = Arc::new(datafusion_expr::ScalarUDF::from(FillNanUdf::new(*v)));
            let exprs = build_projection_exprs(&schema, |col_expr, field| {
                Arc::new(ScalarFunctionExpr::new(
                    &format!("fill_nan({})", field.name()),
                    udf.clone(),
                    vec![col_expr],
                    Arc::clone(field) as FieldRef,
                    config.clone(),
                )) as Arc<dyn PhysicalExpr>
            });
            Ok(Arc::new(
                ProjectionExec::try_new(exprs, input).map_err(Error::from)?,
            ))
        }
        BadVectorValueHandling::Keep => Ok(input),
    }
}

fn build_projection_exprs(
    schema: &arrow_schema::Schema,
    wrap_fn: impl Fn(Arc<dyn PhysicalExpr>, &FieldRef) -> Arc<dyn PhysicalExpr>,
) -> Vec<(Arc<dyn PhysicalExpr>, String)> {
    schema
        .fields()
        .iter()
        .enumerate()
        .map(|(idx, field)| {
            let col_expr: Arc<dyn PhysicalExpr> = Arc::new(Column::new(field.name(), idx));
            let name = field.name().clone();
            if is_vector_field(field) {
                (wrap_fn(col_expr, field), name)
            } else {
                (col_expr, name)
            }
        })
        .collect()
}

// --- UDF: RejectNan (Error strategy) ---

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
        "reject_nan_values"
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
                check_no_nans(array.as_ref())?;
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

    let has_nan = (0..fsl.len()).filter(|i| fsl.is_valid(*i)).any(|i| {
        let row = fsl.value(i);
        row_has_nan(row.as_ref())
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

// --- UDF: HasNoNan (Drop strategy predicate — true = keep) ---

#[derive(Debug, Hash, PartialEq, Eq)]
struct HasNoNanUdf {
    signature: Signature,
}

impl HasNoNanUdf {
    fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for HasNoNanUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "has_no_nan"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> datafusion_common::Result<DataType> {
        Ok(DataType::Boolean)
    }
    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion_common::Result<ColumnarValue> {
        match &args.args[0] {
            ColumnarValue::Array(array) => {
                let fsl = array
                    .as_any()
                    .downcast_ref::<FixedSizeListArray>()
                    .ok_or_else(|| {
                        datafusion_common::DataFusionError::Internal(
                            "has_no_nan expected FixedSizeList".to_string(),
                        )
                    })?;
                let keep: BooleanArray = (0..fsl.len())
                    .map(|i| {
                        if fsl.is_null(i) {
                            Some(true) // null rows are fine
                        } else {
                            let row = fsl.value(i);
                            Some(!row_has_nan(row.as_ref()))
                        }
                    })
                    .collect();
                Ok(ColumnarValue::Array(Arc::new(keep)))
            }
            ColumnarValue::Scalar(_) => Ok(ColumnarValue::Scalar(ScalarValue::Boolean(Some(true)))),
        }
    }
}

// --- UDF: NullIfNan (Null strategy) ---

#[derive(Debug, Hash, PartialEq, Eq)]
struct NullIfNanUdf {
    signature: Signature,
}

impl NullIfNanUdf {
    fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for NullIfNanUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "null_if_nan"
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
        match &args.args[0] {
            ColumnarValue::Array(array) => {
                let fsl = array
                    .as_any()
                    .downcast_ref::<FixedSizeListArray>()
                    .ok_or_else(|| {
                        datafusion_common::DataFusionError::Internal(
                            "null_if_nan expected FixedSizeList".to_string(),
                        )
                    })?;
                let validity: Vec<bool> = (0..fsl.len())
                    .map(|i| {
                        if fsl.is_null(i) {
                            false
                        } else {
                            let row = fsl.value(i);
                            !row_has_nan(row.as_ref())
                        }
                    })
                    .collect();
                let null_buf = NullBuffer::from(validity);
                let new_fsl = FixedSizeListArray::try_new(
                    child_field_of(fsl),
                    fsl.value_length(),
                    fsl.values().clone(),
                    Some(null_buf),
                )
                .map_err(|e| datafusion_common::DataFusionError::ArrowError(Box::new(e), None))?;
                Ok(ColumnarValue::Array(Arc::new(new_fsl)))
            }
            ColumnarValue::Scalar(s) => Ok(ColumnarValue::Scalar(s.clone())),
        }
    }
}

// --- UDF: FillNan (Fill strategy) ---

#[derive(Debug)]
struct FillNanUdf {
    signature: Signature,
    fill_bits: u64,
}

impl FillNanUdf {
    fn new(fill: f64) -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
            fill_bits: fill.to_bits(),
        }
    }

    fn fill_value(&self) -> f64 {
        f64::from_bits(self.fill_bits)
    }
}

impl Hash for FillNanUdf {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.fill_bits.hash(state);
    }
}
impl PartialEq for FillNanUdf {
    fn eq(&self, other: &Self) -> bool {
        self.fill_bits == other.fill_bits
    }
}
impl Eq for FillNanUdf {}

impl ScalarUDFImpl for FillNanUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "fill_nan"
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
        match &args.args[0] {
            ColumnarValue::Array(array) => {
                let fsl = array
                    .as_any()
                    .downcast_ref::<FixedSizeListArray>()
                    .ok_or_else(|| {
                        datafusion_common::DataFusionError::Internal(
                            "fill_nan expected FixedSizeList".to_string(),
                        )
                    })?;
                let fill = self.fill_value();
                let new_values = fill_nans_in_values(fsl.values(), fill)?;
                let new_fsl = FixedSizeListArray::try_new(
                    child_field_of(fsl),
                    fsl.value_length(),
                    new_values,
                    fsl.nulls().cloned(),
                )
                .map_err(|e| datafusion_common::DataFusionError::ArrowError(Box::new(e), None))?;
                Ok(ColumnarValue::Array(Arc::new(new_fsl)))
            }
            ColumnarValue::Scalar(s) => Ok(ColumnarValue::Scalar(s.clone())),
        }
    }
}

fn fill_nans_in_values(values: &ArrayRef, fill: f64) -> datafusion_common::Result<ArrayRef> {
    match values.data_type() {
        DataType::Float16 => {
            let fill_f16 = half::f16::from_f64(fill);
            let arr = values.as_any().downcast_ref::<Float16Array>().unwrap();
            let new_arr: Float16Array = arr
                .iter()
                .map(|v| match v {
                    Some(x) if x.is_nan() => Some(fill_f16),
                    other => other,
                })
                .collect();
            Ok(Arc::new(new_arr))
        }
        DataType::Float32 => {
            let fill_f32 = fill as f32;
            let arr = values.as_any().downcast_ref::<Float32Array>().unwrap();
            let new_arr: Float32Array = arr
                .iter()
                .map(|v| match v {
                    Some(x) if x.is_nan() => Some(fill_f32),
                    other => other,
                })
                .collect();
            Ok(Arc::new(new_arr))
        }
        DataType::Float64 => {
            let arr = values.as_any().downcast_ref::<Float64Array>().unwrap();
            let new_arr: Float64Array = arr
                .iter()
                .map(|v| match v {
                    Some(x) if x.is_nan() => Some(fill),
                    other => other,
                })
                .collect();
            Ok(Arc::new(new_arr))
        }
        _ => Ok(values.clone()),
    }
}
