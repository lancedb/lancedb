// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::sync::{Arc, OnceLock};

use arrow_schema::{DataType, Field, FieldRef, Fields, Schema as ArrowSchema};
use datafusion::config::ConfigOptions;
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::ScalarUDF;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::scalar::ScalarValue;
use datafusion_functions::core::getfield::GetFieldFunc;
use datafusion_functions::core::named_struct::NamedStructFunc;
use datafusion_physical_expr::expressions::{Column, Literal};
use datafusion_physical_expr::{PhysicalExpr, ScalarFunctionExpr};

fn get_field_func() -> Arc<ScalarUDF> {
    static GET_FIELD_FUNC: OnceLock<Arc<ScalarUDF>> = OnceLock::new();
    GET_FIELD_FUNC
        .get_or_init(|| Arc::new(ScalarUDF::new_from_impl(GetFieldFunc::new())))
        .clone()
}
fn get_make_struct_func() -> Arc<ScalarUDF> {
    static MAKE_STRUCT_FUNC: OnceLock<Arc<ScalarUDF>> = OnceLock::new();
    MAKE_STRUCT_FUNC
        .get_or_init(|| Arc::new(ScalarUDF::new_from_impl(NamedStructFunc::new())))
        .clone()
}

/// Make a DataFusion projection node from a schema.
///
/// The `projection` schema must be a subset of the input schema. This can be
/// used to select a subset of fields, either at the top-level or within
/// nested structs.
pub fn project(input: Arc<dyn ExecutionPlan>, projection: &ArrowSchema) -> Result<ProjectionExec> {
    // Use input schema and projection schema to create a list of physical expressions.
    let mut exprs: Vec<(Arc<dyn PhysicalExpr>, String)> =
        Vec::with_capacity(projection.fields.len());

    let input_schema = input.schema();

    let selections = compute_projection(input_schema.fields(), &projection.fields)?;

    let field_names = projection.fields().iter().map(|f| f.name()).cloned();

    for (name, selection) in field_names.zip(selections) {
        let expr = selection_as_expr(&selection, input_schema.fields(), None);
        exprs.push((expr, name));
    }

    ProjectionExec::try_new(exprs, input)
}

fn selection_as_expr(
    selection: &Selection,
    parent_fields: &Fields,
    parent_expr: Option<Arc<dyn PhysicalExpr>>,
) -> Arc<dyn PhysicalExpr> {
    match selection {
        Selection::FullField(field_name) => {
            let (field_index, field) = &parent_fields.find(field_name).unwrap();
            if let Some(expr) = parent_expr {
                // We are extracting a child field.
                sub_field(expr, field)
            } else {
                // This is a top-level field
                Arc::new(Column::new(field.name(), *field_index))
            }
        }
        Selection::StructProjection(field_name, sub_selections) => {
            let (field_index, field) = &parent_fields.find(field_name).unwrap();
            let parent = if let Some(grandparent) = parent_expr {
                sub_field(grandparent, field)
            } else {
                Arc::new(Column::new(field_name, *field_index))
            };
            let DataType::Struct(fields) = field.data_type() else {
                panic!("Expected struct");
            };
            let mut sub_exprs = Vec::with_capacity(2 * sub_selections.len());
            for sub_selection in sub_selections {
                sub_exprs.push(Arc::new(Literal::new(ScalarValue::Utf8(Some(
                    sub_selection.name().to_string(),
                )))) as Arc<dyn PhysicalExpr>);
                sub_exprs.push(selection_as_expr(
                    sub_selection,
                    fields,
                    Some(parent.clone()),
                ));
            }

            let make_struct = get_make_struct_func();
            Arc::new(ScalarFunctionExpr::new(
                make_struct.name(),
                make_struct.clone(),
                sub_exprs,
                project_field(field, selection),
                Arc::new(ConfigOptions::default()),
            ))
        }
    }
}

fn sub_field(parent_expr: Arc<dyn PhysicalExpr>, field: &FieldRef) -> Arc<dyn PhysicalExpr> {
    let func = get_field_func();
    Arc::new(ScalarFunctionExpr::new(
        func.name(),
        func.clone(),
        vec![
            parent_expr,
            Arc::new(Literal::new(ScalarValue::Utf8(Some(field.name().clone())))),
        ],
        field.clone(),
        Arc::new(ConfigOptions::default()),
    ))
}

fn project_field(field: &FieldRef, selection: &Selection) -> FieldRef {
    match selection {
        Selection::FullField(_) => {
            // If we project, it's always null (for some reason).
            Arc::new(field.as_ref().clone().with_nullable(true))
        }
        Selection::StructProjection(_, sub_selections) => {
            if let DataType::Struct(fields) = field.data_type() {
                let mut projected_fields = Vec::with_capacity(sub_selections.len());
                for sub_selection in sub_selections {
                    let field_name = sub_selection.name();
                    let field = fields.iter().find(|f| f.name() == field_name).unwrap();
                    let projected_field = project_field(field, sub_selection);
                    projected_fields.push(projected_field);
                }
                Arc::new(
                    Field::new(
                        field.name(),
                        DataType::Struct(projected_fields.into()),
                        true,
                    )
                    .with_metadata(field.metadata().clone()),
                )
            } else {
                panic!("Expected struct")
            }
        }
    }
}

/// Represents selection of fields from a struct / schema.
#[derive(Debug)]
pub enum Selection<'a> {
    /// Selects this fields and all subfields
    FullField(&'a str),
    /// For a struct, selections of subfields
    StructProjection(&'a str, Vec<Self>),
}

impl Selection<'_> {
    /// Returns the name of the field being selected.
    pub fn name(&self) -> &str {
        match self {
            Selection::FullField(name) => name,
            Selection::StructProjection(name, _) => name,
        }
    }
}

/// Resolves the projection into a list of selections.
pub fn compute_projection<'a>(
    schema: &'_ Fields,
    projection: &'a Fields,
) -> Result<Vec<Selection<'a>>> {
    let mut selections = Vec::with_capacity(projection.len());
    for projected_field in projection {
        match projected_field.data_type() {
            DataType::Struct(fields) => {
                let original_field = schema
                    .iter()
                    .find(|f| f.name() == projected_field.name())
                    .ok_or_else(|| {
                        DataFusionError::Internal(format!(
                            "compute_projection: projected field {} not found in schema {:?}",
                            projected_field.name(),
                            schema
                        ))
                    })?;
                match original_field.data_type() {
                    DataType::Struct(input_fields) => {
                        let sub_selections = compute_projection(input_fields, fields)?;

                        if fields.len() == input_fields.len()
                            && sub_selections
                                .iter()
                                .all(|s| matches!(s, Selection::FullField(_)))
                        {
                            selections.push(Selection::FullField(projected_field.name()));
                        } else {
                            selections.push(Selection::StructProjection(
                                projected_field.name(),
                                sub_selections,
                            ));
                        }
                    }
                    DataType::LargeBinary => {
                        // This can happen when loading blob fields.  The projection we load from disk
                        // is the blob description (struct) but the actual result / schema field is large_binary
                        selections.push(Selection::FullField(projected_field.name()));
                    }
                    _ => {
                        return Err(DataFusionError::Internal(format!(
                            "Field '{}' expected struct or LargeBinary, found {:?}",
                            original_field.name(),
                            original_field.data_type()
                        )));
                    }
                }
            }
            _ => {
                selections.push(Selection::FullField(projected_field.name()));
            }
        }
    }
    Ok(selections)
}

#[cfg(test)]
mod tests {
    use arrow_array::{ArrayRef, Int32Array, RecordBatch, StructArray};
    use datafusion::prelude::SessionContext;
    use futures::TryStreamExt;
    use lance_core::datatypes::Schema;
    use lance_datafusion::exec::OneShotExec;

    use super::*;

    fn sample_nested_data() -> RecordBatch {
        let schema = Arc::new(
            ArrowSchema::new(vec![
                Field::new("a", DataType::Int32, true),
                Field::new(
                    "b",
                    DataType::Struct(
                        vec![
                            Field::new("c", DataType::Int32, true),
                            Field::new(
                                "d",
                                DataType::Struct(
                                    vec![
                                        Field::new("e", DataType::Int32, true),
                                        Field::new("f", DataType::Int32, true),
                                    ]
                                    .into(),
                                ),
                                true,
                            ),
                        ]
                        .into(),
                    ),
                    true,
                ),
            ])
            .with_metadata([("key".into(), "value".into())].into()),
        );

        // Only needs to be one row
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1])) as ArrayRef,
                Arc::new(StructArray::from(vec![
                    (
                        Arc::new(Field::new("c", DataType::Int32, true)),
                        Arc::new(Int32Array::from(vec![2])) as ArrayRef,
                    ),
                    (
                        Arc::new(Field::new(
                            "d",
                            DataType::Struct(
                                vec![
                                    Field::new("e", DataType::Int32, true),
                                    Field::new("f", DataType::Int32, true),
                                ]
                                .into(),
                            ),
                            true,
                        )),
                        Arc::new(StructArray::from(vec![
                            (
                                Arc::new(Field::new("e", DataType::Int32, true)),
                                Arc::new(Int32Array::from(vec![3])) as ArrayRef,
                            ),
                            (
                                Arc::new(Field::new("f", DataType::Int32, true)),
                                Arc::new(Int32Array::from(vec![4])),
                            ),
                        ])),
                    ),
                ])),
            ],
        )
        .unwrap()
    }

    async fn apply_to_batch(batch: RecordBatch, projection: &ArrowSchema) -> Result<RecordBatch> {
        let memory_exec = OneShotExec::from_batch(batch);
        let exec = project(Arc::new(memory_exec), projection)?;
        let claimed_schema = exec.schema();
        let session = SessionContext::new();
        let task_ctx = session.task_ctx();
        let stream = exec.execute(0, task_ctx)?;
        assert_eq!(stream.schema().as_ref(), claimed_schema.as_ref());
        let batches = stream.try_collect::<Vec<_>>().await?;
        assert_eq!(batches.len(), 1);
        Ok(batches.into_iter().next().unwrap())
    }

    #[tokio::test]
    async fn test_project_preserves_field_metadata() {
        use arrow_array::LargeBinaryArray;

        let meta_field = Field::new("meta", DataType::LargeBinary, true).with_metadata(
            std::collections::HashMap::from([(
                lance_arrow::ARROW_EXT_NAME_KEY.to_string(),
                "lance.json".to_string(),
            )]),
        );
        let x_field = Field::new("x", DataType::Int32, true);

        let schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "b",
            DataType::Struct(vec![meta_field.clone(), x_field.clone()].into()),
            true,
        )]));

        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(StructArray::from(vec![
                (
                    Arc::new(meta_field.clone()),
                    Arc::new(LargeBinaryArray::from(vec![Some(b"{}".as_slice())])) as ArrayRef,
                ),
                (Arc::new(x_field), Arc::new(Int32Array::from(vec![1]))),
            ]))],
        )
        .unwrap();

        let projection = ArrowSchema::new(vec![Field::new(
            "b",
            DataType::Struct(vec![meta_field].into()),
            true,
        )]);
        let result = apply_to_batch(batch, &projection).await.unwrap();
        assert_eq!(result.schema().as_ref(), &projection);
    }

    #[tokio::test]
    async fn test_project_node() {
        let sample_data = sample_nested_data();
        let schema = sample_data.schema();
        // Project that schema with nested fields selected
        let lance_schema = Schema::try_from(schema.as_ref()).unwrap();

        let projections: [&[i32]; 4] = [
            &[0, 2, 4, 5], // All leaves
            &[0, 1],       // Top-level fields
            &[2],          // Partial first level struct
            &[4],          // Partial second level struct
        ];

        for projection in projections {
            let projected_schema = lance_schema.project_by_ids(projection, true);
            let projected_arrow_schema = (&projected_schema).into();

            let result = apply_to_batch(sample_data.clone(), &projected_arrow_schema)
                .await
                .unwrap();

            assert_eq!(result.schema().as_ref(), &projected_arrow_schema);
        }
    }
}
