// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Immutable DataFusion logical plans for client DataFrame bindings.
//!
//! This module is the language-neutral implementation behind LanceDB's
//! DataFrame APIs. Bindings translate language values to [`crate::expr::DfExpr`]
//! and delegate planning and Substrait serialization to [`DataFrame`].
//!
//! ```
//! use std::sync::Arc;
//! use arrow_schema::{DataType, Field, Schema};
//! use lancedb::dataframe::DataFrame;
//! use lancedb::expr::{col, lit};
//!
//! let source = DataFrame::from_table(
//!     "events",
//!     Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)])),
//! )?;
//! let plan = source.filter(col("id").gt(lit(10_i64)))?.to_substrait()?;
//! assert!(!plan.bytes().is_empty());
//! # Ok::<(), lancedb::Error>(())
//! ```

use std::sync::Arc;

use arrow_schema::Schema;
use datafusion::{
    dataframe::DataFrame as DfDataFrame,
    datasource::provider_as_source,
    execution::context::SessionContext,
    logical_expr::{JoinType as DfJoinType, LogicalPlanBuilder},
};
use datafusion_catalog::empty::EmptyTable;
use datafusion_common::{Column, TableReference};
use datafusion_expr::{Expr, SortExpr};
use datafusion_functions_aggregate::expr_fn::{avg, count, max, min, sum};
use datafusion_substrait::{logical_plan::producer::to_substrait_plan, substrait::proto::Plan};
use prost::Message;

use crate::{Error, Result};

fn planning_error(error: impl std::fmt::Display) -> Error {
    Error::InvalidInput {
        message: error.to_string(),
    }
}

/// Join behavior supported by the language-neutral DataFrame planner.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum JoinType {
    /// Keep rows whose keys match on both sides.
    Inner,
    /// Keep all left rows and matching right rows.
    Left,
    /// Keep matching left rows and all right rows.
    Right,
    /// Keep all rows from both sides.
    Full,
    /// Keep left rows that have a match, without right columns.
    LeftSemi,
    /// Keep right rows that have a match, without left columns.
    RightSemi,
    /// Keep left rows that have no match.
    LeftAnti,
    /// Keep right rows that have no match.
    RightAnti,
}

impl From<JoinType> for DfJoinType {
    fn from(value: JoinType) -> Self {
        match value {
            JoinType::Inner => Self::Inner,
            JoinType::Left => Self::Left,
            JoinType::Right => Self::Right,
            JoinType::Full => Self::Full,
            JoinType::LeftSemi => Self::LeftSemi,
            JoinType::RightSemi => Self::RightSemi,
            JoinType::LeftAnti => Self::LeftAnti,
            JoinType::RightAnti => Self::RightAnti,
        }
    }
}

/// Serialized Substrait plan and the version declared by the plan.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EncodedSubstraitPlan {
    bytes: Vec<u8>,
    version: String,
}

impl EncodedSubstraitPlan {
    /// Return the encoded Substrait protobuf.
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// Return the Substrait version declared by the plan.
    pub fn version(&self) -> &str {
        &self.version
    }

    /// Consume the value into its encoded bytes and version.
    pub fn into_parts(self) -> (Vec<u8>, String) {
        (self.bytes, self.version)
    }
}

/// An immutable DataFusion logical plan suitable for language bindings.
#[derive(Clone)]
pub struct DataFrame {
    inner: DfDataFrame,
}

impl DataFrame {
    fn wrap(result: datafusion_common::Result<DfDataFrame>) -> Result<Self> {
        result.map(|inner| Self { inner }).map_err(planning_error)
    }

    /// Create a lazy named-table scan with the supplied Arrow schema.
    pub fn from_table(name: impl Into<String>, schema: Arc<Schema>) -> Result<Self> {
        let context = SessionContext::new();
        let source = provider_as_source(Arc::new(EmptyTable::new(schema)));
        let plan = LogicalPlanBuilder::scan(TableReference::bare(name.into()), source, None)
            .and_then(LogicalPlanBuilder::build)
            .map_err(planning_error)?;
        Ok(Self {
            inner: DfDataFrame::new(context.state(), plan),
        })
    }

    /// Project expressions into a new DataFrame.
    pub fn select(&self, expressions: Vec<Expr>) -> Result<Self> {
        Self::wrap(self.inner.clone().select(expressions))
    }

    /// Keep rows matching a predicate.
    pub fn filter(&self, predicate: Expr) -> Result<Self> {
        Self::wrap(self.inner.clone().filter(predicate))
    }

    /// Group rows and calculate aggregate expressions.
    pub fn aggregate(&self, groups: Vec<Expr>, aggregates: Vec<Expr>) -> Result<Self> {
        Self::wrap(self.inner.clone().aggregate(groups, aggregates))
    }

    /// Sort by expressions expressed as `(expression, ascending, nulls_first)`.
    pub fn sort(&self, expressions: Vec<(Expr, bool, bool)>) -> Result<Self> {
        let expressions: Vec<SortExpr> = expressions
            .into_iter()
            .map(|(expr, ascending, nulls_first)| expr.sort(ascending, nulls_first))
            .collect();
        Self::wrap(self.inner.clone().sort(expressions))
    }

    /// Limit the result to `count` rows after `offset` rows.
    pub fn limit(&self, count: usize, offset: usize) -> Result<Self> {
        Self::wrap(self.inner.clone().limit(offset, Some(count)))
    }

    /// Remove duplicate rows.
    pub fn distinct(&self) -> Result<Self> {
        Self::wrap(self.inner.clone().distinct())
    }

    /// Assign a relation alias, typically before a self join.
    pub fn alias(&self, name: impl Into<String>) -> Result<Self> {
        let name = name.into();
        Self::wrap(self.inner.clone().alias(&name))
    }

    /// Resolve a literal field name to a relation-qualified expression.
    pub fn column(&self, name: &str) -> Result<Expr> {
        let field = self
            .inner
            .schema()
            .qualified_field_with_unqualified_name(name)
            .map_err(planning_error)?;
        Ok(Expr::Column(Column::from(field)))
    }

    /// Add or replace a column.
    pub fn with_column(&self, name: &str, expression: Expr) -> Result<Self> {
        Self::wrap(self.inner.clone().with_column(name, expression))
    }

    /// Drop literal field names, rejecting missing or ambiguous fields.
    pub fn drop_columns(&self, columns: &[String]) -> Result<Self> {
        let columns = columns
            .iter()
            .map(|name| {
                self.inner
                    .schema()
                    .qualified_field_with_unqualified_name(name)
                    .map(Column::from)
                    .map_err(planning_error)
            })
            .collect::<Result<Vec<_>>>()?;
        Self::wrap(self.inner.clone().drop_columns(&columns))
    }

    /// Rename a literal field while retaining its relation qualifier.
    pub fn with_column_renamed(&self, old_name: &str, new_name: &str) -> Result<Self> {
        let old_column = self
            .inner
            .schema()
            .qualified_field_with_unqualified_name(old_name)
            .map(Column::from)
            .map_err(planning_error)?;
        let projection = self
            .inner
            .schema()
            .iter()
            .map(|(qualifier, field)| {
                let column = Column::new(qualifier.cloned(), field.name());
                let expression = Expr::Column(column.clone());
                if column == old_column {
                    expression.alias_qualified(qualifier.cloned(), new_name)
                } else {
                    expression
                }
            })
            .collect::<Vec<_>>();
        Self::wrap(self.inner.clone().select(projection))
    }

    /// Join two plans using corresponding equality keys.
    pub fn join(
        &self,
        other: &Self,
        left_on: &[String],
        right_on: &[String],
        how: JoinType,
    ) -> Result<Self> {
        let left_on = left_on.iter().map(String::as_str).collect::<Vec<_>>();
        let right_on = right_on.iter().map(String::as_str).collect::<Vec<_>>();
        Self::wrap(self.inner.clone().join(
            other.inner.clone(),
            how.into(),
            &left_on,
            &right_on,
            None,
        ))
    }

    /// Union two compatible plans, preserving duplicates when `all` is true.
    pub fn union(&self, other: &Self, all: bool) -> Result<Self> {
        if all {
            Self::wrap(self.inner.clone().union(other.inner.clone()))
        } else {
            Self::wrap(self.inner.clone().union_distinct(other.inner.clone()))
        }
    }

    /// Intersect two compatible plans, preserving duplicates when `all` is true.
    pub fn intersect(&self, other: &Self, all: bool) -> Result<Self> {
        if all {
            Self::wrap(self.inner.clone().intersect(other.inner.clone()))
        } else {
            Self::wrap(self.inner.clone().intersect_distinct(other.inner.clone()))
        }
    }

    /// Subtract a compatible plan, preserving duplicates when `all` is true.
    pub fn except(&self, other: &Self, all: bool) -> Result<Self> {
        if all {
            Self::wrap(self.inner.clone().except(other.inner.clone()))
        } else {
            Self::wrap(self.inner.clone().except_distinct(other.inner.clone()))
        }
    }

    /// Return the current output Arrow schema.
    pub fn schema(&self) -> Schema {
        self.inner.schema().as_arrow().clone()
    }

    /// Serialize this logical plan to Substrait.
    pub fn to_substrait(&self) -> Result<EncodedSubstraitPlan> {
        let (state, logical_plan) = self.inner.clone().into_parts();
        let plan = to_substrait_plan(&logical_plan, &state).map_err(planning_error)?;
        let version = plan
            .version
            .as_ref()
            .map(|version| {
                format!(
                    "{}.{}.{}",
                    version.major_number, version.minor_number, version.patch_number
                )
            })
            .ok_or_else(|| Error::Runtime {
                message: "generated Substrait plan has no version".to_string(),
            })?;
        Ok(EncodedSubstraitPlan {
            bytes: plan.encode_to_vec(),
            version,
        })
    }

    /// Render the logical plan for diagnostics.
    pub fn display(&self) -> String {
        self.inner.logical_plan().display_indent().to_string()
    }
}

/// Read the declared version from a serialized Substrait plan.
pub fn plan_version(plan: &[u8]) -> Result<String> {
    let plan = Plan::decode(plan).map_err(planning_error)?;
    let version = plan.version.ok_or_else(|| Error::InvalidInput {
        message: "Substrait plan does not contain a version".to_string(),
    })?;
    Ok(format!(
        "{}.{}.{}",
        version.major_number, version.minor_number, version.patch_number
    ))
}

/// Build a `SUM` aggregate expression.
pub fn aggregate_sum(expr: Expr) -> Expr {
    sum(expr)
}

/// Build an `AVG` aggregate expression.
pub fn aggregate_avg(expr: Expr) -> Expr {
    avg(expr)
}

/// Build a `MIN` aggregate expression.
pub fn aggregate_min(expr: Expr) -> Expr {
    min(expr)
}

/// Build a `MAX` aggregate expression.
pub fn aggregate_max(expr: Expr) -> Expr {
    max(expr)
}

/// Build a `COUNT` aggregate expression.
pub fn aggregate_count(expr: Expr) -> Expr {
    count(expr)
}

#[cfg(test)]
mod tests {
    use arrow_schema::{DataType, Field};

    use super::*;
    use crate::expr::{col, lit};

    fn events() -> DataFrame {
        DataFrame::from_table(
            "events",
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("value", DataType::Int64, false),
            ])),
        )
        .unwrap()
    }

    #[test]
    fn builds_and_serializes_an_immutable_plan() {
        let frame = events()
            .filter(col("value").gt(lit(5_i64)))
            .unwrap()
            .select(vec![col("id"), col("value")])
            .unwrap()
            .sort(vec![(col("value"), false, true)])
            .unwrap()
            .limit(10, 2)
            .unwrap();

        assert_eq!(frame.schema().fields().len(), 2);
        let encoded = frame.to_substrait().unwrap();
        assert!(!encoded.bytes().is_empty());
        assert_eq!(plan_version(encoded.bytes()).unwrap(), encoded.version());
    }

    #[test]
    fn qualified_renames_survive_aliased_self_joins() {
        let source = events();
        let left = source
            .alias("left")
            .unwrap()
            .with_column_renamed("value", "renamed")
            .unwrap();
        let right = source
            .alias("right")
            .unwrap()
            .with_column_renamed("value", "renamed")
            .unwrap();
        let joined = left
            .join(
                &right,
                &["id".to_string()],
                &["id".to_string()],
                JoinType::Inner,
            )
            .unwrap()
            .select(vec![
                left.column("renamed").unwrap().alias("left_value"),
                right.column("renamed").unwrap().alias("right_value"),
            ])
            .unwrap();

        assert_eq!(joined.schema().fields().len(), 2);
        assert!(!joined.to_substrait().unwrap().bytes().is_empty());
    }

    #[test]
    fn dotted_names_are_literal_and_missing_names_error() {
        let frame = DataFrame::from_table(
            "dotted",
            Arc::new(Schema::new(vec![Field::new(
                "left.value",
                DataType::Int64,
                true,
            )])),
        )
        .unwrap();

        assert!(frame.column("left.value").is_ok());
        assert_eq!(
            frame
                .with_column_renamed("left.value", "value")
                .unwrap()
                .schema()
                .field(0)
                .name(),
            "value"
        );
        assert!(frame.drop_columns(&["left.value".to_string()]).is_ok());
        assert!(frame.drop_columns(&["missing".to_string()]).is_err());
    }
}
