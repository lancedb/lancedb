// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Projection helpers shared by the LSM vector search, point lookup, and
//! scan planners.
//!
//! `MemTableScanner::project()` only special-cases `_rowid`; passing other
//! system columns through it errors. And cross-LSM values for system
//! columns aren't comparable (a `_rowid` of 5 in the base and in an
//! SSTable refer to different rows).
//!
//! - [`build_scanner_projection`] — strips system / `_distance` cols, appends PKs.
//! - [`canonical_output_schema`] — final schema honoring user order; system
//!   cols become nullable `UInt64`, `_distance` becomes nullable `Float32`.
//! - [`project_to_canonical`] — wraps a plan to emit `target_schema`,
//!   NULL-filling system / `_distance` cols missing from the source.

use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::expressions::{Column, Literal};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::scalar::ScalarValue;
use lance_core::{ROW_ADDR, ROW_ID, Result, is_system_column};

/// Column name for distance in vector search results.
pub const DISTANCE_COLUMN: &str = "_distance";

/// Did the caller list `_rowid` in their projection?
pub fn wants_row_id(projection: Option<&[String]>) -> bool {
    projection
        .map(|p| p.iter().any(|c| c == ROW_ID))
        .unwrap_or(false)
}

/// Did the caller list `_rowaddr` in their projection?
pub fn wants_row_address(projection: Option<&[String]>) -> bool {
    projection
        .map(|p| p.iter().any(|c| c == ROW_ADDR))
        .unwrap_or(false)
}

/// Auto-managed by the planner; must never reach `scanner.project()`.
fn is_auto_managed(col: &str) -> bool {
    col == DISTANCE_COLUMN || is_system_column(col)
}

/// Projection to pass to underlying scanners: user cols minus
/// system/`_distance`, with PKs appended for dedup/staleness.
pub fn build_scanner_projection(
    user_projection: Option<&[String]>,
    base_schema: &SchemaRef,
    pk_columns: &[String],
) -> Vec<String> {
    let mut cols: Vec<String> = if let Some(p) = user_projection {
        p.iter().filter(|c| !is_auto_managed(c)).cloned().collect()
    } else {
        base_schema
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect()
    };

    for pk in pk_columns {
        if !cols.contains(pk) {
            cols.push(pk.clone());
        }
    }

    cols
}

/// Validate user-facing projection names before constructing a canonical schema.
pub fn validate_projection_names(
    user_projection: Option<&[String]>,
    base_schema: &SchemaRef,
    extra_columns: &[&str],
) -> Result<()> {
    let Some(user_projection) = user_projection else {
        return Ok(());
    };
    for name in user_projection {
        if is_system_column(name)
            || extra_columns.contains(&name.as_str())
            || base_schema.field_with_name(name).is_ok()
        {
            continue;
        }
        return Err(lance_core::Error::invalid_input(format!(
            "Column '{}' not found in schema",
            name
        )));
    }
    Ok(())
}

/// Canonical output schema honoring user column order.
///
/// System cols → nullable `UInt64` at user position (filled by
/// `project_to_canonical`). `_distance` (when `include_distance`) →
/// nullable `Float32` at user position, appended if absent. PKs appended.
/// Callers should run [`validate_projection_names`] before using this with a
/// user-supplied projection.
pub fn canonical_output_schema(
    user_projection: Option<&[String]>,
    base_schema: &SchemaRef,
    pk_columns: &[String],
    include_distance: bool,
) -> SchemaRef {
    let mut ordered: Vec<String> = if let Some(p) = user_projection {
        p.to_vec()
    } else {
        base_schema
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect()
    };

    for pk in pk_columns {
        if !ordered.contains(pk) {
            ordered.push(pk.clone());
        }
    }

    if include_distance && !ordered.iter().any(|c| c == DISTANCE_COLUMN) {
        ordered.push(DISTANCE_COLUMN.to_string());
    }

    let fields: Vec<Arc<Field>> = ordered
        .iter()
        .filter_map(|name| {
            if name == DISTANCE_COLUMN {
                include_distance
                    .then(|| Arc::new(Field::new(DISTANCE_COLUMN, DataType::Float32, true)))
            } else if is_system_column(name) {
                Some(Arc::new(Field::new(name.clone(), DataType::UInt64, true)))
            } else {
                base_schema
                    .field_with_name(name)
                    .ok()
                    .map(|f| Arc::new(f.clone()))
            }
        })
        .collect();

    Arc::new(Schema::new(fields))
}

/// Wrap `plan` so the named columns become typed NULL literals; all
/// other columns are forwarded unchanged. Schema is preserved (same
/// fields, same dtypes). Useful for stripping the *value* of an
/// internal column after it has served its purpose (e.g. `_rowaddr`
/// after the per-arm local sort) without breaking downstream schema
/// matching.
pub fn null_columns(
    plan: Arc<dyn ExecutionPlan>,
    names: &[&str],
) -> Result<Arc<dyn ExecutionPlan>> {
    let input_schema = plan.schema();
    let mut project_exprs: Vec<(Arc<dyn PhysicalExpr>, String)> =
        Vec::with_capacity(input_schema.fields().len());
    for (idx, field) in input_schema.fields().iter().enumerate() {
        let name = field.name();
        let expr: Arc<dyn PhysicalExpr> = if names.contains(&name.as_str()) {
            Arc::new(Literal::new(
                ScalarValue::try_from(field.data_type()).map_err(|e| {
                    lance_core::Error::internal(format!(
                        "Cannot build NULL literal for {}: {}",
                        field.data_type(),
                        e
                    ))
                })?,
            ))
        } else {
            Arc::new(Column::new(name, idx))
        };
        project_exprs.push((expr, name.clone()));
    }
    let projection_exec = ProjectionExec::try_new(project_exprs, plan).map_err(|e| {
        lance_core::Error::internal(format!(
            "Failed to build null_columns ProjectionExec: {}",
            e
        ))
    })?;
    Ok(Arc::new(projection_exec))
}

/// Wrap `plan` to emit exactly `target_schema`. Source columns are
/// forwarded by name; system / `_distance` cols missing from the source
/// are NULL-filled. Other missing columns are an internal error.
pub fn project_to_canonical(
    plan: Arc<dyn ExecutionPlan>,
    target_schema: &SchemaRef,
) -> Result<Arc<dyn ExecutionPlan>> {
    let input_schema = plan.schema();
    let mut project_exprs: Vec<(Arc<dyn PhysicalExpr>, String)> =
        Vec::with_capacity(target_schema.fields().len());
    for field in target_schema.fields() {
        let name = field.name();
        let expr: Arc<dyn PhysicalExpr> = match input_schema.column_with_name(name) {
            Some((idx, _)) => Arc::new(Column::new(name, idx)),
            None if is_system_column(name) => Arc::new(Literal::new(ScalarValue::UInt64(None))),
            None if name == DISTANCE_COLUMN => Arc::new(Literal::new(ScalarValue::Float32(None))),
            None => {
                return Err(lance_core::Error::internal(format!(
                    "Column '{}' missing from canonical projection source schema (have: {:?})",
                    name,
                    input_schema
                        .fields()
                        .iter()
                        .map(|f| f.name().clone())
                        .collect::<Vec<_>>()
                )));
            }
        };
        project_exprs.push((expr, name.clone()));
    }
    let projection_exec = ProjectionExec::try_new(project_exprs, plan).map_err(|e| {
        lance_core::Error::internal(format!("Failed to build canonical ProjectionExec: {}", e))
    })?;
    Ok(Arc::new(projection_exec))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::Schema as ArrowSchema;

    fn schema() -> SchemaRef {
        Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("vector", DataType::Float32, true),
        ]))
    }

    #[test]
    fn scanner_projection_strips_system_and_distance() {
        let s = schema();
        let pks = vec!["id".to_string()];
        let user = vec![
            "_distance".to_string(),
            "vector".to_string(),
            "_rowid".to_string(),
            "_rowaddr".to_string(),
        ];
        let cols = build_scanner_projection(Some(&user), &s, &pks);
        assert_eq!(cols, vec!["vector".to_string(), "id".to_string()]);
    }

    #[test]
    fn scanner_projection_default_uses_base_schema() {
        let s = schema();
        let pks = vec!["id".to_string()];
        let cols = build_scanner_projection(None, &s, &pks);
        assert_eq!(
            cols,
            vec!["id".to_string(), "name".to_string(), "vector".to_string()]
        );
    }

    #[test]
    fn canonical_schema_honors_user_order_for_distance() {
        let s = schema();
        let pks = vec!["id".to_string()];
        let user = vec!["_distance".to_string(), "vector".to_string()];
        let out = canonical_output_schema(Some(&user), &s, &pks, true);
        let names: Vec<&str> = out.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(names, vec!["_distance", "vector", "id"]);
        assert_eq!(
            out.field_with_name("_distance").unwrap().data_type(),
            &DataType::Float32
        );
    }

    #[test]
    fn canonical_schema_includes_system_cols_as_nullable_uint64() {
        let s = schema();
        let pks = vec!["id".to_string()];
        let user = vec![
            "vector".to_string(),
            "_rowid".to_string(),
            "_rowaddr".to_string(),
            "_rowoffset".to_string(),
        ];
        let out = canonical_output_schema(Some(&user), &s, &pks, false);
        let names: Vec<&str> = out.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(
            names,
            vec!["vector", "_rowid", "_rowaddr", "_rowoffset", "id"]
        );
        for sys in ["_rowid", "_rowaddr", "_rowoffset"] {
            let field = out.field_with_name(sys).unwrap();
            assert_eq!(field.data_type(), &DataType::UInt64);
            assert!(field.is_nullable(), "{sys} must be nullable for NULL fill");
        }
    }

    #[test]
    fn canonical_schema_appends_distance_when_missing() {
        let s = schema();
        let pks = vec!["id".to_string()];
        let user = vec!["vector".to_string()];
        let out = canonical_output_schema(Some(&user), &s, &pks, true);
        let names: Vec<&str> = out.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(names, vec!["vector", "id", "_distance"]);
    }

    #[test]
    fn canonical_schema_drops_distance_when_not_requested() {
        let s = schema();
        let pks = vec!["id".to_string()];
        let user = vec!["_distance".to_string(), "vector".to_string()];
        let out = canonical_output_schema(Some(&user), &s, &pks, false);
        let names: Vec<&str> = out.fields().iter().map(|f| f.name().as_str()).collect();
        // _distance dropped because include_distance=false (e.g. point lookup / scan).
        assert_eq!(names, vec!["vector", "id"]);
    }
}
