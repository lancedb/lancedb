// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Building blocks for the expressions the write path synthesizes.
//!
//! Two DataFusion details are easy to get wrong and were previously got wrong independently
//! in each place that assembles a struct:
//!
//! * `named_struct` returns a struct with no null bitmap, and the `get_field` calls feeding
//!   it read each child without applying the parent's validity. A struct assembled from
//!   per-child expressions therefore has to have its source's nulls put back, or a null
//!   input struct comes out non-null with its masked children exposed. That has to happen
//!   at the array level: selecting a typed null for the null rows would nullify their
//!   children too, which Lance rejects for a non-nullable child even where the parent masks
//!   it.
//! * DataFusion derives a projection's output schema from [`PhysicalExpr::return_field`], not
//!   from any field the planner carries alongside. An expression whose return field is built
//!   from a bare `DataType` loses the field's metadata, and an extension column is nothing
//!   but that metadata.
//!
//! [`build_struct`] and [`cast_to_field`] are the only sanctioned way to do either, so the
//! call sites cannot drift apart again.

use std::sync::{Arc, LazyLock};

use arrow_array::StructArray;
use arrow_array::cast::AsArray;
use arrow_schema::{DataType, Field, FieldRef};
use datafusion::functions::core::{get_field, named_struct};
use datafusion_common::config::ConfigOptions;
use datafusion_common::metadata::FieldMetadata;
use datafusion_common::{DataFusionError, Result as DFResult, ScalarValue};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};
use datafusion_physical_expr::ScalarFunctionExpr;
use datafusion_physical_expr::expressions::{CastExpr, Literal};
use datafusion_physical_plan::PhysicalExpr;

use crate::{Error, Result};

/// One child of a struct being assembled.
pub(super) struct StructChild {
    pub field: FieldRef,
    pub value: Arc<dyn PhysicalExpr>,
}

/// Assemble a struct column from per-child expressions.
///
/// `nulls_from` is the struct column the result takes its validity from, for a rebuild that
/// reshapes an input struct. Pass `None` when the result is synthesized from something that
/// is not a struct, so that there is no input validity to carry over.
pub(super) fn build_struct(
    children: Vec<StructChild>,
    output_field: &FieldRef,
    nulls_from: Option<Arc<dyn PhysicalExpr>>,
    config: &Arc<ConfigOptions>,
) -> Result<Arc<dyn PhysicalExpr>> {
    let mut args: Vec<Arc<dyn PhysicalExpr>> = Vec::with_capacity(children.len() * 2);
    for child in &children {
        args.push(Arc::new(Literal::new(ScalarValue::from(
            child.field.name().as_str(),
        ))));
        args.push(child.value.clone());
    }

    let assembled: Arc<dyn PhysicalExpr> = Arc::new(ScalarFunctionExpr::new(
        &format!("named_struct({})", output_field.name()),
        named_struct(),
        args,
        output_field.clone(),
        config.clone(),
    ));

    let Some(source) = nulls_from else {
        return Ok(assembled);
    };

    Ok(Arc::new(ScalarFunctionExpr::new(
        &format!("restore_validity({})", output_field.name()),
        RESTORE_VALIDITY_UDF.clone(),
        vec![assembled, source],
        output_field.clone(),
        config.clone(),
    )))
}

static RESTORE_VALIDITY_UDF: LazyLock<Arc<datafusion_expr::ScalarUDF>> =
    LazyLock::new(|| Arc::new(datafusion_expr::ScalarUDF::from(RestoreValidityUdf::new())));

/// Returns its first argument, a struct, carrying the null buffer of its second.
///
/// The children come through byte for byte, placeholder values in the masked slots included;
/// see the note on validity at the top of this module for why that matters.
#[derive(Debug, Hash, PartialEq, Eq)]
struct RestoreValidityUdf {
    signature: Signature,
}

impl RestoreValidityUdf {
    fn new() -> Self {
        Self {
            signature: Signature::any(2, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for RestoreValidityUdf {
    fn name(&self) -> &str {
        "restore_validity"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(arg_types[0].clone())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let rows = args.number_rows;
        let assembled = args.args[0].to_array(rows)?;
        let nulls_from = args.args[1].to_array(rows)?;

        let assembled = assembled.as_struct_opt().ok_or_else(|| {
            DataFusionError::Internal(format!(
                "restore_validity expects a struct, got {}",
                assembled.data_type()
            ))
        })?;

        let nulls = nulls_from.logical_nulls();
        let (fields, columns, _) = assembled.clone().into_parts();
        let restored = StructArray::try_new_with_length(fields, columns, nulls, rows)?;
        Ok(ColumnarValue::Array(Arc::new(restored)))
    }
}

/// Read `child` out of a struct column.
pub(super) fn get_field_expr(
    parent: Arc<dyn PhysicalExpr>,
    child: &Field,
    config: &Arc<ConfigOptions>,
) -> Arc<dyn PhysicalExpr> {
    Arc::new(ScalarFunctionExpr::new(
        &format!("get_field({})", child.name()),
        get_field(),
        vec![
            parent,
            Arc::new(Literal::new(ScalarValue::from(child.name().as_str()))),
        ],
        Arc::new(child.clone()),
        config.clone(),
    ))
}

/// Cast `expr` so that it reports `field` verbatim as its output.
///
/// Use this rather than [`CastExpr::new`] wherever the target is a known field: the metadata
/// that identifies an extension column survives, and the expression's return field then
/// matches the field the planner records for it. Casting to the type it already has is a
/// no-op in arrow, so this doubles as a way to stamp a field onto an expression.
pub(super) fn cast_to_field(
    expr: Arc<dyn PhysicalExpr>,
    field: &FieldRef,
) -> Arc<dyn PhysicalExpr> {
    // safe: false (the default) means overflow and truncation surface at execution time.
    Arc::new(CastExpr::new_with_target_field(expr, field.clone(), None))
}

/// A null literal of `field`'s type, carrying `field`'s metadata.
pub(super) fn typed_null(field: &Field) -> Result<Arc<dyn PhysicalExpr>> {
    let scalar = ScalarValue::try_new_null(field.data_type()).map_err(|e| Error::InvalidInput {
        message: format!(
            "cannot build a null literal for column '{}' of type {}: {e}",
            field.name(),
            field.data_type()
        ),
    })?;
    Ok(Arc::new(Literal::new_with_metadata(
        scalar,
        Some(FieldMetadata::from(field)),
    )))
}
