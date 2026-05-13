// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Handles vectors with wrong dimensions before the List→FixedSizeList cast step.

use std::any::Any;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use arrow::buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow_array::{Array, ArrayRef, BooleanArray, Float16Array, Float32Array, Float64Array};
use arrow_array::{LargeListArray, ListArray};
use arrow_schema::{DataType, Field, FieldRef, Schema};
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

use crate::table::add_data::BadVectorDimensionHandling;
use crate::{Error, Result};

struct AffectedCol {
    input_idx: usize,
    name: String,
    expected_dim: i32,
    is_large_list: bool,
}

/// Finds input columns that are variable-length float lists whose corresponding
/// table column is a FixedSizeList, requiring a dimension check before casting.
fn find_affected_cols(input_schema: &Schema, table_schema: &Schema) -> Vec<AffectedCol> {
    let mut cols = Vec::new();
    for (input_idx, input_field) in input_schema.fields().iter().enumerate() {
        let Ok(table_field) = table_schema.field_with_name(input_field.name()) else {
            continue;
        };
        let expected_dim = match table_field.data_type() {
            DataType::FixedSizeList(child, dim)
                if matches!(
                    child.data_type(),
                    DataType::Float16 | DataType::Float32 | DataType::Float64
                ) =>
            {
                *dim
            }
            _ => continue,
        };
        let is_large_list = match input_field.data_type() {
            DataType::List(child)
                if matches!(
                    child.data_type(),
                    DataType::Float16 | DataType::Float32 | DataType::Float64
                ) =>
            {
                false
            }
            DataType::LargeList(child)
                if matches!(
                    child.data_type(),
                    DataType::Float16 | DataType::Float32 | DataType::Float64
                ) =>
            {
                true
            }
            _ => continue,
        };
        cols.push(AffectedCol {
            input_idx,
            name: input_field.name().clone(),
            expected_dim,
            is_large_list,
        });
    }
    cols
}

/// Wraps `input` with a plan that applies `handling` to variable-length float-list
/// columns that will be cast to FixedSizeList by the table schema.
///
/// Must run *before* the cast step so that wrong-dimension rows are handled
/// before the `List → FixedSizeList` cast fails on them.
pub fn handle_bad_vector_dimensions(
    input: Arc<dyn ExecutionPlan>,
    table_schema: &Schema,
    handling: &BadVectorDimensionHandling,
) -> Result<Arc<dyn ExecutionPlan>> {
    let input_schema = input.schema();
    let affected = find_affected_cols(&input_schema, table_schema);
    if affected.is_empty() {
        return Ok(input);
    }

    let config = Arc::new(ConfigOptions::default());

    match handling {
        BadVectorDimensionHandling::Error => {
            let exprs = wrap_affected_cols(&input_schema, &affected, |ac, col_expr, field| {
                let udf = Arc::new(datafusion_expr::ScalarUDF::from(ErrorOnWrongDimUdf::new(
                    ac.expected_dim,
                    ac.is_large_list,
                )));
                Arc::new(ScalarFunctionExpr::new(
                    &format!("error_wrong_dim({})", ac.name),
                    udf,
                    vec![col_expr],
                    Arc::clone(field) as FieldRef,
                    config.clone(),
                )) as Arc<dyn PhysicalExpr>
            });
            Ok(Arc::new(
                ProjectionExec::try_new(exprs, input).map_err(Error::from)?,
            ))
        }
        BadVectorDimensionHandling::Drop => {
            let bool_field = Arc::new(Field::new("", DataType::Boolean, false));
            let mut predicate: Option<Arc<dyn PhysicalExpr>> = None;
            for ac in &affected {
                let col: Arc<dyn PhysicalExpr> = Arc::new(Column::new(&ac.name, ac.input_idx));
                let udf = Arc::new(datafusion_expr::ScalarUDF::from(HasCorrectDimUdf::new(
                    ac.expected_dim,
                    ac.is_large_list,
                )));
                let check: Arc<dyn PhysicalExpr> = Arc::new(ScalarFunctionExpr::new(
                    &format!("has_correct_dim({})", ac.name),
                    udf,
                    vec![col],
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
        BadVectorDimensionHandling::Null => {
            let exprs = wrap_affected_cols(&input_schema, &affected, |ac, col_expr, field| {
                let udf = Arc::new(datafusion_expr::ScalarUDF::from(NullIfWrongDimUdf::new(
                    ac.expected_dim,
                    ac.is_large_list,
                )));
                let nullable_field =
                    Arc::new(Field::new(field.name(), field.data_type().clone(), true));
                Arc::new(ScalarFunctionExpr::new(
                    &format!("null_if_wrong_dim({})", ac.name),
                    udf,
                    vec![col_expr],
                    nullable_field,
                    config.clone(),
                )) as Arc<dyn PhysicalExpr>
            });
            Ok(Arc::new(
                ProjectionExec::try_new(exprs, input).map_err(Error::from)?,
            ))
        }
        BadVectorDimensionHandling::Fill(v) => {
            let exprs = wrap_affected_cols(&input_schema, &affected, |ac, col_expr, field| {
                let udf = Arc::new(datafusion_expr::ScalarUDF::from(FillWrongDimUdf::new(
                    ac.expected_dim,
                    ac.is_large_list,
                    *v,
                )));
                Arc::new(ScalarFunctionExpr::new(
                    &format!("fill_wrong_dim({})", ac.name),
                    udf,
                    vec![col_expr],
                    Arc::clone(field) as FieldRef,
                    config.clone(),
                )) as Arc<dyn PhysicalExpr>
            });
            Ok(Arc::new(
                ProjectionExec::try_new(exprs, input).map_err(Error::from)?,
            ))
        }
    }
}

fn wrap_affected_cols(
    schema: &arrow_schema::Schema,
    affected: &[AffectedCol],
    make_expr: impl Fn(&AffectedCol, Arc<dyn PhysicalExpr>, &FieldRef) -> Arc<dyn PhysicalExpr>,
) -> Vec<(Arc<dyn PhysicalExpr>, String)> {
    schema
        .fields()
        .iter()
        .enumerate()
        .map(|(idx, field)| {
            let col: Arc<dyn PhysicalExpr> = Arc::new(Column::new(field.name(), idx));
            let name = field.name().clone();
            if let Some(ac) = affected.iter().find(|a| a.input_idx == idx) {
                (make_expr(ac, col, field), name)
            } else {
                (col, name)
            }
        })
        .collect()
}

// ──────────────────────────────────────────────────────────────
// Shared helpers
// ──────────────────────────────────────────────────────────────

fn list_row_count(array: &dyn Array) -> usize {
    array.len()
}

fn list_row_len(array: &dyn Array, row: usize) -> i32 {
    match array.data_type() {
        DataType::List(_) => array
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap()
            .value_length(row),
        DataType::LargeList(_) => array
            .as_any()
            .downcast_ref::<LargeListArray>()
            .unwrap()
            .value_length(row) as i32,
        _ => 0,
    }
}

fn extract_item_field(array: &dyn Array) -> FieldRef {
    match array.data_type() {
        DataType::List(f) | DataType::LargeList(f) => f.clone(),
        _ => unreachable!(),
    }
}

// ──────────────────────────────────────────────────────────────
// UDF: ErrorOnWrongDim
// ──────────────────────────────────────────────────────────────

#[derive(Debug, Hash, PartialEq, Eq)]
struct ErrorOnWrongDimUdf {
    signature: Signature,
    expected_dim: i32,
    is_large_list: bool,
}

impl ErrorOnWrongDimUdf {
    fn new(expected_dim: i32, is_large_list: bool) -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
            expected_dim,
            is_large_list,
        }
    }
}

impl ScalarUDFImpl for ErrorOnWrongDimUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "error_wrong_dim"
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
                for i in 0..list_row_count(array.as_ref()) {
                    if !array.is_null(i) {
                        let len = list_row_len(array.as_ref(), i);
                        if len != self.expected_dim {
                            return Err(datafusion_common::DataFusionError::ArrowError(
                                Box::new(arrow_schema::ArrowError::InvalidArgumentError(format!(
                                    "Vector has wrong dimension: expected {}, got {}",
                                    self.expected_dim, len
                                ))),
                                None,
                            ));
                        }
                    }
                }
                Ok(ColumnarValue::Array(array.clone()))
            }
            ColumnarValue::Scalar(s) => Ok(ColumnarValue::Scalar(s.clone())),
        }
    }
}

// ──────────────────────────────────────────────────────────────
// UDF: HasCorrectDim (Drop — true = keep)
// ──────────────────────────────────────────────────────────────

#[derive(Debug, Hash, PartialEq, Eq)]
struct HasCorrectDimUdf {
    signature: Signature,
    expected_dim: i32,
    is_large_list: bool,
}

impl HasCorrectDimUdf {
    fn new(expected_dim: i32, is_large_list: bool) -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
            expected_dim,
            is_large_list,
        }
    }
}

impl ScalarUDFImpl for HasCorrectDimUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "has_correct_dim"
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
                let n = list_row_count(array.as_ref());
                let keep: BooleanArray = (0..n)
                    .map(|i| {
                        if array.is_null(i) {
                            Some(true)
                        } else {
                            Some(list_row_len(array.as_ref(), i) == self.expected_dim)
                        }
                    })
                    .collect();
                Ok(ColumnarValue::Array(Arc::new(keep)))
            }
            ColumnarValue::Scalar(_) => Ok(ColumnarValue::Scalar(ScalarValue::Boolean(Some(true)))),
        }
    }
}

// ──────────────────────────────────────────────────────────────
// UDF: NullIfWrongDim
// ──────────────────────────────────────────────────────────────

#[derive(Debug, Hash, PartialEq, Eq)]
struct NullIfWrongDimUdf {
    signature: Signature,
    expected_dim: i32,
    is_large_list: bool,
}

impl NullIfWrongDimUdf {
    fn new(expected_dim: i32, is_large_list: bool) -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
            expected_dim,
            is_large_list,
        }
    }
}

impl ScalarUDFImpl for NullIfWrongDimUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "null_if_wrong_dim"
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
                let n = list_row_count(array.as_ref());
                let validity: Vec<bool> = (0..n)
                    .map(|i| {
                        !array.is_null(i) && list_row_len(array.as_ref(), i) == self.expected_dim
                    })
                    .collect();
                let null_buf = NullBuffer::from(validity);
                let item_field = extract_item_field(array.as_ref());
                let new_array: ArrayRef = if self.is_large_list {
                    let list =
                        array
                            .as_any()
                            .downcast_ref::<LargeListArray>()
                            .ok_or_else(|| {
                                datafusion_common::DataFusionError::Internal(
                                    "null_if_wrong_dim: expected LargeListArray".to_string(),
                                )
                            })?;
                    Arc::new(LargeListArray::new(
                        item_field,
                        list.offsets().clone(),
                        list.values().clone(),
                        Some(null_buf),
                    ))
                } else {
                    let list = array.as_any().downcast_ref::<ListArray>().ok_or_else(|| {
                        datafusion_common::DataFusionError::Internal(
                            "null_if_wrong_dim: expected ListArray".to_string(),
                        )
                    })?;
                    Arc::new(ListArray::new(
                        item_field,
                        list.offsets().clone(),
                        list.values().clone(),
                        Some(null_buf),
                    ))
                };
                Ok(ColumnarValue::Array(new_array))
            }
            ColumnarValue::Scalar(s) => Ok(ColumnarValue::Scalar(s.clone())),
        }
    }
}

// ──────────────────────────────────────────────────────────────
// UDF: FillWrongDim
// ──────────────────────────────────────────────────────────────

#[derive(Debug)]
struct FillWrongDimUdf {
    signature: Signature,
    expected_dim: i32,
    is_large_list: bool,
    fill_bits: u64,
}

impl FillWrongDimUdf {
    fn new(expected_dim: i32, is_large_list: bool, fill: f64) -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
            expected_dim,
            is_large_list,
            fill_bits: fill.to_bits(),
        }
    }

    fn fill_value(&self) -> f64 {
        f64::from_bits(self.fill_bits)
    }
}

impl Hash for FillWrongDimUdf {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.expected_dim.hash(state);
        self.is_large_list.hash(state);
        self.fill_bits.hash(state);
    }
}
impl PartialEq for FillWrongDimUdf {
    fn eq(&self, other: &Self) -> bool {
        self.expected_dim == other.expected_dim
            && self.is_large_list == other.is_large_list
            && self.fill_bits == other.fill_bits
    }
}
impl Eq for FillWrongDimUdf {}

impl ScalarUDFImpl for FillWrongDimUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "fill_wrong_dim"
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
                let fill = self.fill_value();
                let item_field = extract_item_field(array.as_ref());
                let new_array = fill_wrong_dim(
                    array,
                    &item_field,
                    self.expected_dim,
                    fill,
                    self.is_large_list,
                )?;
                Ok(ColumnarValue::Array(new_array))
            }
            ColumnarValue::Scalar(s) => Ok(ColumnarValue::Scalar(s.clone())),
        }
    }
}

// ──────────────────────────────────────────────────────────────
// Fill implementation
// ──────────────────────────────────────────────────────────────

fn fill_wrong_dim(
    array: &ArrayRef,
    item_field: &FieldRef,
    expected_dim: i32,
    fill: f64,
    is_large_list: bool,
) -> datafusion_common::Result<ArrayRef> {
    match item_field.data_type() {
        DataType::Float32 => {
            fill_list_f32(array, item_field, expected_dim, fill as f32, is_large_list)
        }
        DataType::Float64 => fill_list_f64(array, item_field, expected_dim, fill, is_large_list),
        DataType::Float16 => fill_list_f16(
            array,
            item_field,
            expected_dim,
            half::f16::from_f64(fill),
            is_large_list,
        ),
        _ => Ok(array.clone()),
    }
}

macro_rules! impl_fill_list {
    ($fn_name:ident, $FloatArr:ty, $float_ty:ty) => {
        fn $fn_name(
            array: &ArrayRef,
            item_field: &FieldRef,
            expected_dim: i32,
            fill_val: $float_ty,
            is_large_list: bool,
        ) -> datafusion_common::Result<ArrayRef> {
            let n = array.len();
            let mut flat: Vec<Option<$float_ty>> = Vec::new();
            let mut offsets_i32: Vec<i32> = vec![0];
            let mut offsets_i64: Vec<i64> = vec![0];
            let mut validity: Vec<bool> = Vec::with_capacity(n);

            for i in 0..n {
                if array.is_null(i) {
                    if is_large_list {
                        offsets_i64.push(*offsets_i64.last().unwrap());
                    } else {
                        offsets_i32.push(*offsets_i32.last().unwrap());
                    }
                    validity.push(false);
                } else {
                    let row_len = list_row_len(array.as_ref(), i);
                    if row_len == expected_dim {
                        let row: ArrayRef = if is_large_list {
                            array
                                .as_any()
                                .downcast_ref::<LargeListArray>()
                                .unwrap()
                                .value(i)
                        } else {
                            array.as_any().downcast_ref::<ListArray>().unwrap().value(i)
                        };
                        let typed = row.as_any().downcast_ref::<$FloatArr>().ok_or_else(|| {
                            datafusion_common::DataFusionError::Internal(
                                concat!(
                                    "fill_list: unexpected item type in ",
                                    stringify!($fn_name)
                                )
                                .to_string(),
                            )
                        })?;
                        for v in typed.iter() {
                            flat.push(v);
                        }
                    } else {
                        for _ in 0..expected_dim {
                            flat.push(Some(fill_val));
                        }
                    }
                    if is_large_list {
                        offsets_i64.push(*offsets_i64.last().unwrap() + expected_dim as i64);
                    } else {
                        offsets_i32.push(*offsets_i32.last().unwrap() + expected_dim);
                    }
                    validity.push(true);
                }
            }

            let item_arr: $FloatArr = flat.into_iter().collect();
            let null_buf = NullBuffer::from(validity);

            if is_large_list {
                let offsets = OffsetBuffer::new(ScalarBuffer::from(offsets_i64));
                Ok(Arc::new(LargeListArray::new(
                    item_field.clone(),
                    offsets,
                    Arc::new(item_arr),
                    Some(null_buf),
                )))
            } else {
                let offsets = OffsetBuffer::new(ScalarBuffer::from(offsets_i32));
                Ok(Arc::new(ListArray::new(
                    item_field.clone(),
                    offsets,
                    Arc::new(item_arr),
                    Some(null_buf),
                )))
            }
        }
    };
}

impl_fill_list!(fill_list_f32, Float32Array, f32);
impl_fill_list!(fill_list_f64, Float64Array, f64);
impl_fill_list!(fill_list_f16, Float16Array, half::f16);
