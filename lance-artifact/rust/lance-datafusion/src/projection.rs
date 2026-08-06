// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use arrow_array::RecordBatch;
use arrow_schema::{DataType, Field as ArrowField, Schema as ArrowSchema};
use datafusion::{logical_expr::Expr, physical_plan::projection::ProjectionExec};
use datafusion_common::{Column, DFSchema};
use datafusion_physical_expr::PhysicalExpr;
use futures::TryStreamExt;
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};
use tracing::instrument;

use lance_core::{
    Error, ROW_ADDR, ROW_CREATED_AT_VERSION, ROW_ID, ROW_LAST_UPDATED_AT_VERSION, ROW_OFFSET,
    Result, WILDCARD,
    datatypes::{OnMissing, Projectable, Projection, Schema},
};

use crate::{
    exec::{LanceExecutionOptions, OneShotExec, execute_plan},
    planner::Planner,
};

const SCORING_COLUMNS: [&str; 2] = ["_distance", "_score"];

fn canonical_scoring_column(name: &str) -> Option<&'static str> {
    SCORING_COLUMNS
        .into_iter()
        .find(|scoring_column| name.eq_ignore_ascii_case(scoring_column))
}

struct ProjectionBuilder {
    base: Arc<dyn Projectable>,
    planner: Planner,
    output: HashMap<String, Expr>,
    output_cols: Vec<OutputColumn>,
    scoring_exprs: HashMap<String, String>,
    physical_cols_set: HashSet<String>,
    physical_cols: Vec<String>,
    needs_row_id: bool,
    needs_row_addr: bool,
    needs_row_last_updated_at: bool,
    needs_row_created_at: bool,
    must_add_row_offset: bool,
    has_wildcard: bool,
}

impl ProjectionBuilder {
    fn new(base: Arc<dyn Projectable>) -> Self {
        let full_schema = Arc::new(Projection::full(base.clone()).to_arrow_schema());
        let full_schema = Arc::new(ProjectionPlan::add_system_columns(&full_schema));
        let planner = Planner::new(full_schema);

        Self {
            base,
            planner,
            output: HashMap::default(),
            output_cols: Vec::default(),
            scoring_exprs: HashMap::default(),
            physical_cols_set: HashSet::default(),
            physical_cols: Vec::default(),
            needs_row_id: false,
            needs_row_addr: false,
            needs_row_created_at: false,
            needs_row_last_updated_at: false,
            must_add_row_offset: false,
            has_wildcard: false,
        }
    }

    fn check_duplicate_column(&self, name: &str) -> Result<()> {
        if self.output.contains_key(name) {
            return Err(Error::invalid_input(format!(
                "Duplicate column name: {}",
                name
            )));
        }
        Ok(())
    }

    fn add_column(&mut self, output_name: &str, raw_expr: &str) -> Result<()> {
        self.check_duplicate_column(output_name)?;

        let expr = self.planner.parse_expr(raw_expr)?;
        let expr = if Self::references_scoring_column(&expr) {
            // A scoring name can refer to either a stored column or a search-generated
            // Float32 column. Reparse and coerce once the physical input schema disambiguates it.
            self.scoring_exprs
                .insert(output_name.to_string(), raw_expr.to_string());
            expr
        } else {
            // Run simplification + coercion so that expressions like `coalesce(...)`
            // (which DataFusion's physical evaluator expects to have been rewritten
            // into a `CASE` expression by the simplifier) work correctly.
            self.planner.optimize_expr(expr)?
        };

        // If the expression is a bare column reference to a system column, mark that we need it
        if let Expr::Column(Column {
            name,
            relation: None,
            ..
        }) = &expr
        {
            if name == ROW_ID {
                self.needs_row_id = true;
            } else if name == ROW_ADDR {
                self.needs_row_addr = true;
            } else if name == ROW_OFFSET {
                self.must_add_row_offset = true;
            } else if name == ROW_LAST_UPDATED_AT_VERSION {
                self.needs_row_last_updated_at = true;
            } else if name == ROW_CREATED_AT_VERSION {
                self.needs_row_created_at = true;
            }
        }

        for col in Planner::column_names_in_expr(&expr) {
            // Discovery can bind an exact provisional scoring field beside a mixed-case stored
            // field. Load the stored field too so final-schema replanning can select the stored
            // or search-generated field from the physical input.
            let physical_col = if canonical_scoring_column(&col).is_some() {
                self.base
                    .schema()
                    .field_case_insensitive(&col)
                    .map(|field| field.name.clone())
                    .unwrap_or(col)
            } else {
                col
            };
            if self.physical_cols_set.contains(&physical_col) {
                continue;
            }
            self.physical_cols.push(physical_col.clone());
            self.physical_cols_set.insert(physical_col);
        }
        self.output.insert(output_name.to_string(), expr.clone());

        self.output_cols.push(OutputColumn {
            expr,
            name: output_name.to_string(),
        });

        Ok(())
    }

    fn references_scoring_column(expr: &Expr) -> bool {
        Planner::column_names_in_expr(expr)
            .iter()
            .any(|name| canonical_scoring_column(name).is_some())
    }

    fn add_columns(&mut self, columns: &[(impl AsRef<str>, impl AsRef<str>)]) -> Result<()> {
        for (output_name, raw_expr) in columns {
            if raw_expr.as_ref() == WILDCARD {
                self.has_wildcard = true;
                for col in self.base.schema().fields.iter().map(|f| f.name.as_str()) {
                    self.check_duplicate_column(col)?;
                    self.output_cols.push(OutputColumn {
                        expr: Expr::Column(Column::from_name(col)),
                        name: col.to_string(),
                    });
                    // Throw placeholder expr in self.output, this will trigger error on duplicates
                    self.output.insert(col.to_string(), Expr::default());
                }
            } else {
                self.add_column(output_name.as_ref(), raw_expr.as_ref())?;
            }
        }
        Ok(())
    }

    fn build(self) -> Result<ProjectionPlan> {
        // Now, calculate the physical projection from the columns referenced by the expressions
        //
        // If a column is missing it might be a system column (_rowid, _distance, etc.) and so
        // we ignore it.  We don't need to load that column from disk at least, which is all we are
        // trying to calculate here.
        let mut physical_projection = if self.has_wildcard {
            Projection::full(self.base.clone())
        } else {
            Projection::empty(self.base.clone())
                .union_columns(&self.physical_cols, OnMissing::Ignore)?
        };

        physical_projection.with_row_id = self.needs_row_id;
        physical_projection.with_row_addr = self.needs_row_addr || self.must_add_row_offset;
        physical_projection.with_row_last_updated_at_version = self.needs_row_last_updated_at;
        physical_projection.with_row_created_at_version = self.needs_row_created_at;

        Ok(ProjectionPlan {
            physical_projection,
            must_add_row_offset: self.must_add_row_offset,
            requested_output_expr: self.output_cols,
            scoring_exprs: self.scoring_exprs,
        })
    }
}

#[derive(Clone, Debug)]
pub struct OutputColumn {
    /// The expression that represents the output column
    pub expr: Expr,
    /// The name of the output column
    pub name: String,
}

#[derive(Clone, Debug)]
pub struct ProjectionPlan {
    /// The physical schema that must be loaded from the dataset
    pub physical_projection: Projection,

    /// Needs the row address converted into a row offset
    pub must_add_row_offset: bool,

    /// The desired output columns
    pub requested_output_expr: Vec<OutputColumn>,

    /// Original SQL for scoring expressions that must be replanned against the physical schema.
    scoring_exprs: HashMap<String, String>,
}

impl ProjectionPlan {
    fn add_system_columns(schema: &ArrowSchema) -> ArrowSchema {
        let mut fields = Vec::from_iter(schema.fields.iter().cloned());
        fields.push(Arc::new(ArrowField::new(ROW_ID, DataType::UInt64, true)));
        fields.push(Arc::new(ArrowField::new(ROW_ADDR, DataType::UInt64, true)));
        fields.push(Arc::new(ArrowField::new(
            ROW_OFFSET,
            DataType::UInt64,
            true,
        )));
        fields.push(Arc::new(
            (*lance_core::ROW_LAST_UPDATED_AT_VERSION_FIELD).clone(),
        ));
        fields.push(Arc::new(
            (*lance_core::ROW_CREATED_AT_VERSION_FIELD).clone(),
        ));
        // Exact scoring fields are needed for initial parsing of schema-dependent functions, even
        // beside a mixed-case stored field. The stored field is carried into the physical
        // projection separately, and scoring expressions are replanned against the final schema.
        for name in SCORING_COLUMNS {
            if schema.field_with_name(name).is_err() {
                fields.push(Arc::new(ArrowField::new(name, DataType::Float32, true)));
            }
        }
        ArrowSchema::new(fields)
    }

    /// Set the projection from SQL expressions
    pub fn from_expressions(
        base: Arc<dyn Projectable>,
        columns: &[(impl AsRef<str>, impl AsRef<str>)],
    ) -> Result<Self> {
        let mut builder = ProjectionBuilder::new(base);
        builder.add_columns(columns)?;
        builder.build()
    }

    /// Set the projection from a schema
    ///
    /// This plan will have no complex expressions, the schema must be a subset of the dataset schema.
    ///
    /// With this approach it is possible to refer to portions of nested fields.
    ///
    /// For example, if the schema is:
    ///
    /// ```ignore
    /// {
    ///   "metadata": {
    ///     "location": {
    ///       "x": f32,
    ///       "y": f32,
    ///     },
    ///     "age": i32,
    ///   }
    /// }
    /// ```
    ///
    /// It is possible to project a partial schema that drops `y` like:
    ///
    /// ```ignore
    /// {
    ///   "metadata": {
    ///     "location": {
    ///       "x": f32,
    ///     },
    ///     "age": i32,
    ///   }
    /// }
    /// ```
    ///
    /// This is something that cannot be done easily using expressions.
    pub fn from_schema(base: Arc<dyn Projectable>, projection: &Schema) -> Result<Self> {
        // Separate data columns from system columns
        // System columns (_rowid, _rowaddr, etc.) are handled via flags in Projection,
        // not as fields in the Schema
        let mut data_fields = Vec::new();
        let mut with_row_id = false;
        let mut with_row_addr = false;
        let mut must_add_row_offset = false;
        let mut with_row_last_updated_at_version = false;
        let mut with_row_created_at_version = false;

        for field in projection.fields.iter() {
            if lance_core::is_system_column(&field.name) {
                // Handle known system columns that can be included in projections
                if field.name == ROW_ID {
                    with_row_id = true;
                    must_add_row_offset = true;
                } else if field.name == ROW_ADDR {
                    with_row_addr = true;
                } else if field.name == ROW_OFFSET {
                    with_row_addr = true;
                    must_add_row_offset = true;
                } else if field.name == ROW_LAST_UPDATED_AT_VERSION {
                    with_row_last_updated_at_version = true;
                } else if field.name == ROW_CREATED_AT_VERSION {
                    with_row_created_at_version = true;
                }
            } else {
                // Regular data column - validate it exists in base schema
                if base.schema().field(&field.name).is_none() {
                    return Err(Error::invalid_input(format!(
                        "Column '{}' not found in schema",
                        field.name
                    )));
                }
                data_fields.push(field.clone());
            }
        }

        // Create a schema with only data columns for the physical projection
        let data_schema = Schema {
            fields: data_fields,
            metadata: projection.metadata.clone(),
        };

        // Calculate the physical projection from data columns only
        let mut physical_projection = Projection::empty(base).union_schema(&data_schema);
        physical_projection.with_row_id = with_row_id;
        physical_projection.with_row_addr = with_row_addr;
        physical_projection.with_row_last_updated_at_version = with_row_last_updated_at_version;
        physical_projection.with_row_created_at_version = with_row_created_at_version;

        // Build output expressions preserving the original order (including system columns)
        let exprs = projection
            .fields
            .iter()
            .map(|f| OutputColumn {
                expr: Expr::Column(Column::from_name(&f.name)),
                name: f.name.clone(),
            })
            .collect::<Vec<_>>();

        Ok(Self {
            physical_projection,
            requested_output_expr: exprs,
            must_add_row_offset,
            scoring_exprs: HashMap::default(),
        })
    }

    pub fn full(base: Arc<dyn Projectable>) -> Result<Self> {
        let physical_cols: Vec<&str> = base
            .schema()
            .fields
            .iter()
            .map(|f| f.name.as_ref())
            .collect::<Vec<_>>();

        let physical_projection =
            Projection::empty(base.clone()).union_columns(&physical_cols, OnMissing::Ignore)?;

        let requested_output_expr = physical_cols
            .into_iter()
            .map(|col_name| OutputColumn {
                expr: Expr::Column(Column::from_name(col_name)),
                name: col_name.to_string(),
            })
            .collect();

        Ok(Self {
            physical_projection,
            must_add_row_offset: false,
            requested_output_expr,
            scoring_exprs: HashMap::default(),
        })
    }

    /// Convert the projection to a list of physical expressions
    ///
    /// This is used to apply the final projection (including dynamic expressions) to the data.
    pub fn to_physical_exprs(
        &self,
        current_schema: &ArrowSchema,
    ) -> Result<Vec<(Arc<dyn PhysicalExpr>, String)>> {
        let physical_df_schema = Arc::new(DFSchema::try_from(current_schema.clone())?);
        self.requested_output_expr
            .iter()
            .map(|output_column| {
                let expr = if let Some(raw_expr) = self.scoring_exprs.get(&output_column.name) {
                    let planner = Planner::new(Arc::new(current_schema.clone()));
                    let expr = planner.parse_expr(raw_expr)?;
                    planner.optimize_expr(expr)?
                } else {
                    output_column.expr.clone()
                };
                Ok((
                    datafusion::physical_expr::create_physical_expr(
                        &expr,
                        physical_df_schema.as_ref(),
                        &Default::default(),
                    )?,
                    output_column.name.clone(),
                ))
            })
            .collect::<Result<Vec<_>>>()
    }

    /// Include the row id in the output
    pub fn include_row_id(&mut self) {
        self.physical_projection.with_row_id = true;
        if !self
            .requested_output_expr
            .iter()
            .any(|OutputColumn { name, .. }| name == ROW_ID)
        {
            self.requested_output_expr.push(OutputColumn {
                expr: Expr::Column(Column::from_name(ROW_ID)),
                name: ROW_ID.to_string(),
            });
        }
    }

    /// Include the row address in the output
    pub fn include_row_addr(&mut self) {
        self.physical_projection.with_row_addr = true;
        if !self
            .requested_output_expr
            .iter()
            .any(|OutputColumn { name, .. }| name == ROW_ADDR)
        {
            self.requested_output_expr.push(OutputColumn {
                expr: Expr::Column(Column::from_name(ROW_ADDR)),
                name: ROW_ADDR.to_string(),
            });
        }
    }

    /// Check if the projection has any output columns
    ///
    /// This doesn't mean there is a physical projection.  For example, we may someday support
    /// something like `SELECT 1 AS foo` which would have an output column (foo) but no physical projection
    pub fn has_output_cols(&self) -> bool {
        !self.requested_output_expr.is_empty()
    }

    pub fn output_schema(&self) -> Result<ArrowSchema> {
        let physical_schema = self.physical_projection.to_arrow_schema();
        let exprs = self.to_physical_exprs(&physical_schema)?;
        let fields = exprs
            .iter()
            .map(|(expr, name)| {
                let metadata = expr.return_field(&physical_schema)?.metadata().clone();
                Ok(ArrowField::new(
                    name,
                    expr.data_type(&physical_schema)?,
                    expr.nullable(&physical_schema)?,
                )
                .with_metadata(metadata))
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(ArrowSchema::new_with_metadata(
            fields,
            physical_schema.metadata().clone(),
        ))
    }

    #[instrument(skip_all, level = "debug")]
    pub async fn project_batch(&self, batch: RecordBatch) -> Result<RecordBatch> {
        let src = Arc::new(OneShotExec::from_batch(batch));

        // Need to add ROW_OFFSET to get filterable schema
        let extra_columns = vec![
            ArrowField::new(ROW_ADDR, DataType::UInt64, true),
            ArrowField::new(ROW_OFFSET, DataType::UInt64, true),
        ];
        let mut filterable_schema = self.physical_projection.to_schema();
        filterable_schema = filterable_schema.merge(&ArrowSchema::new(extra_columns))?;

        let physical_exprs = self.to_physical_exprs(&(&filterable_schema).into())?;
        let projection = Arc::new(ProjectionExec::try_new(physical_exprs, src)?);

        // Run dummy plan to execute projection, do not log the plan run
        let stream = execute_plan(
            projection,
            LanceExecutionOptions {
                skip_logging: true,
                ..Default::default()
            },
        )?;
        let batches = stream.try_collect::<Vec<_>>().await?;
        if batches.len() != 1 {
            Err(Error::internal("Expected exactly one batch".to_string()))
        } else {
            Ok(batches.into_iter().next().unwrap())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use arrow_array::{ArrayRef, Float32Array, Int64Array};
    use lance_arrow::json::{is_json_field, json_field};

    #[test]
    fn test_scoring_column_expression() {
        for scoring_column in ["_distance", "_score"] {
            for has_stored_column in [false, true] {
                let base = if has_stored_column {
                    Arc::new(
                        Schema::try_from(&ArrowSchema::new(vec![ArrowField::new(
                            scoring_column,
                            DataType::Float64,
                            true,
                        )]))
                        .unwrap(),
                    )
                } else {
                    Arc::new(Schema::default())
                };
                let expression = format!("1 - {scoring_column}");
                let plan =
                    ProjectionPlan::from_expressions(base, &[("inverted", expression.as_str())])
                        .unwrap();

                if has_stored_column {
                    let stored_output = plan.output_schema().unwrap();
                    assert_eq!(stored_output.field(0).data_type(), &DataType::Float64);
                }

                let batch = RecordBatch::try_from_iter([(
                    scoring_column,
                    Arc::new(Float32Array::from(vec![0.25, 0.75])) as ArrayRef,
                )])
                .unwrap();

                let physical_exprs = plan.to_physical_exprs(batch.schema().as_ref()).unwrap();
                let values = physical_exprs[0]
                    .0
                    .evaluate(&batch)
                    .unwrap()
                    .into_array(batch.num_rows())
                    .unwrap();

                assert_eq!(
                    values.as_ref(),
                    &Float32Array::from(vec![0.75, 0.25]),
                    "unexpected result for {scoring_column}",
                );
            }
        }
    }

    #[test]
    fn test_stored_scoring_column_does_not_break_other_expressions() {
        for scoring_column in ["_distance", "_score"] {
            let base = Arc::new(
                Schema::try_from(&ArrowSchema::new(vec![
                    ArrowField::new("id", DataType::Int64, false),
                    ArrowField::new(scoring_column, DataType::Float64, true),
                ]))
                .unwrap(),
            );

            ProjectionPlan::from_expressions(base, &[("incremented", "id + 1")]).unwrap();
        }
    }

    #[test]
    fn test_stored_scoring_column_is_case_insensitive() {
        for (stored_name, requested_name) in [("_Distance", "_distance"), ("_Score", "_score")] {
            let base = Arc::new(
                Schema::try_from(&ArrowSchema::new(vec![ArrowField::new(
                    stored_name,
                    DataType::Float64,
                    true,
                )]))
                .unwrap(),
            );
            let plan =
                ProjectionPlan::from_expressions(base, &[("stored", requested_name)]).unwrap();

            assert_eq!(
                plan.output_schema().unwrap().field(0).data_type(),
                &DataType::Float64,
            );

            let batch = RecordBatch::try_from_iter([(
                requested_name,
                Arc::new(Float32Array::from(vec![0.25, 0.75])) as ArrayRef,
            )])
            .unwrap();
            let physical_exprs = plan.to_physical_exprs(batch.schema().as_ref()).unwrap();
            assert_eq!(
                physical_exprs[0]
                    .0
                    .data_type(batch.schema().as_ref())
                    .unwrap(),
                DataType::Float32,
            );
        }
    }

    #[test]
    fn test_generated_scoring_function_with_mixed_case_stored_column() {
        for (stored_name, generated_name) in [("_Distance", "_distance"), ("_Score", "_score")] {
            let base = Arc::new(
                Schema::try_from(&ArrowSchema::new(vec![ArrowField::new(
                    stored_name,
                    DataType::Float64,
                    true,
                )]))
                .unwrap(),
            );
            let expression = format!("coalesce(1 - {generated_name}, 0)");
            let plan =
                ProjectionPlan::from_expressions(base, &[("normalized", expression.as_str())])
                    .unwrap();
            let batch = RecordBatch::try_from_iter([(
                generated_name,
                Arc::new(Float32Array::from(vec![Some(0.25), None])) as ArrayRef,
            )])
            .unwrap();

            let physical_exprs = plan.to_physical_exprs(batch.schema().as_ref()).unwrap();
            let values = physical_exprs[0]
                .0
                .evaluate(&batch)
                .unwrap()
                .into_array(batch.num_rows())
                .unwrap();
            assert_eq!(values.as_ref(), &Float32Array::from(vec![0.75, 0.0]));
        }
    }

    #[test]
    fn test_scoring_column_function_expression() {
        for scoring_column in ["_distance", "_score"] {
            let expression = format!("coalesce(1 - {scoring_column}, 0)");
            let plan = ProjectionPlan::from_expressions(
                Arc::new(Schema::default()),
                &[("normalized", expression.as_str())],
            )
            .unwrap();
            let batch = RecordBatch::try_from_iter([(
                scoring_column,
                Arc::new(Float32Array::from(vec![Some(0.25), None])) as ArrayRef,
            )])
            .unwrap();

            let physical_exprs = plan.to_physical_exprs(batch.schema().as_ref()).unwrap();
            let values = physical_exprs[0]
                .0
                .evaluate(&batch)
                .unwrap()
                .into_array(batch.num_rows())
                .unwrap();
            assert_eq!(values.as_ref(), &Float32Array::from(vec![0.75, 0.0]));
        }
    }

    #[tokio::test]
    async fn test_coalesce_in_column_map() {
        // Regression test: `coalesce` in a column-map expression used to fail with
        // "coalesce should have been simplified to case" because the parsed expression
        // was passed straight to `create_physical_expr` without running the simplifier.
        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("col_a", DataType::Int64, true),
            ArrowField::new("col_b", DataType::Int64, true),
        ]));
        let base_schema = Schema::try_from(arrow_schema.as_ref()).unwrap();
        let base = Arc::new(base_schema);

        let plan =
            ProjectionPlan::from_expressions(base, &[("foo", "coalesce(col_a, col_b)")]).unwrap();

        let batch = RecordBatch::try_new(
            arrow_schema,
            vec![
                Arc::new(Int64Array::from(vec![Some(1), None, Some(3), None])),
                Arc::new(Int64Array::from(vec![Some(10), Some(20), None, None])),
            ],
        )
        .unwrap();

        let projected = plan.project_batch(batch).await.unwrap();
        let foo = projected
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(
            foo.iter().collect::<Vec<_>>(),
            vec![Some(1), Some(20), Some(3), None],
        );
    }

    #[test]
    fn test_output_schema_preserves_json_extension_metadata() {
        let arrow_schema = ArrowSchema::new(vec![
            ArrowField::new("id", DataType::Int32, false),
            json_field("meta", true),
        ]);
        let base_schema = Schema::try_from(&arrow_schema).unwrap();
        let base = Arc::new(base_schema.clone());

        let plan = ProjectionPlan::from_schema(base, &base_schema).unwrap();

        let physical = plan.physical_projection.to_arrow_schema();
        assert!(is_json_field(physical.field_with_name("meta").unwrap()));

        let output = plan.output_schema().unwrap();
        let output_field = output.field_with_name("meta").unwrap();
        assert!(is_json_field(output_field));
    }
}
