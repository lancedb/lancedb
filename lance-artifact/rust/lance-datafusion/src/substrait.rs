// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use arrow_schema::{DataType, Schema as ArrowSchema};
use datafusion::{execution::SessionState, logical_expr::Expr};

use crate::aggregate::Aggregate;
use datafusion_common::DFSchema;
use datafusion_substrait::extensions::Extensions;
use datafusion_substrait::logical_plan::consumer::{
    DefaultSubstraitConsumer, from_substrait_agg_func, from_substrait_rex, from_substrait_sorts,
};
use datafusion_substrait::substrait::proto::{
    AggregateRel, Expression, ExpressionReference, ExtendedExpression, NamedStruct, Plan, Type,
    expression::{
        RexType,
        field_reference::{ReferenceType, RootType},
        reference_segment,
    },
    expression_reference::ExprType,
    function_argument::ArgType,
    rel::RelType,
    r#type::{Kind, Struct},
};
use lance_core::{Error, Result};
use prost::Message;
use std::collections::HashMap;
use std::sync::Arc;

/// FixedSizeList has no Substrait producer support in datafusion-substrait.
/// Other unsupported types (Null, Float16) are encoded as UserDefined and
/// handled by `remove_extension_types` on the decode side.
fn is_substrait_compatible(data_type: &DataType) -> bool {
    match data_type {
        DataType::FixedSizeList(_, _) => false,
        DataType::List(inner) => is_substrait_compatible(inner.data_type()),
        DataType::Struct(fields) => fields
            .iter()
            .all(|f| is_substrait_compatible(f.data_type())),
        _ => true,
    }
}

/// Removes top-level fields that contain data types that the Substrait
/// producer cannot encode (currently only FixedSizeList).
pub fn prune_schema_for_substrait(schema: &ArrowSchema) -> ArrowSchema {
    ArrowSchema::new(
        schema
            .fields()
            .iter()
            .filter(|f| is_substrait_compatible(f.data_type()))
            .cloned()
            .collect::<Vec<_>>(),
    )
}

/// Convert a DF Expr into a Substrait ExtendedExpressions message
///
/// The schema needs to contain all of the fields that are referenced in the expression.
/// It is ok if the schema has more fields than are required.  However, we cannot currently
/// convert all field types (e.g. extension types, FSL) and if these fields are present then
/// the conversion will fail.
///
/// As a result, it may be a good idea for now to remove those types from the schema before
/// calling this function.
pub fn encode_substrait(
    expr: Expr,
    schema: Arc<ArrowSchema>,
    state: &SessionState,
) -> Result<Vec<u8>> {
    use arrow_schema::Field;
    use datafusion::logical_expr::ExprSchemable;
    use datafusion_common::DFSchema;

    let df_schema = Arc::new(DFSchema::try_from(schema)?);
    let output_type = expr.get_type(&df_schema)?;
    // Nullability doesn't matter
    let output_field = Field::new("output", output_type, /*nullable=*/ true);
    let extended_expr = datafusion_substrait::logical_plan::producer::to_substrait_extended_expr(
        &[(&expr, &output_field)],
        &df_schema,
        state,
    )?;

    Ok(extended_expr.encode_to_vec())
}

fn count_fields(dtype: &Type) -> usize {
    match dtype.kind.as_ref().unwrap() {
        Kind::Struct(struct_type) => struct_type.types.iter().map(count_fields).sum::<usize>() + 1,
        Kind::List(list_type) => {
            // Recursively count fields in the list's child type
            // This is critical for schemas with List<Struct> patterns
            count_fields(list_type.r#type.as_ref().unwrap())
        }
        _ => 1,
    }
}

fn remove_extension_types(
    substrait_schema: &NamedStruct,
    arrow_schema: Arc<ArrowSchema>,
) -> Result<(NamedStruct, Arc<ArrowSchema>, HashMap<usize, usize>)> {
    let fields = substrait_schema.r#struct.as_ref().unwrap();
    if fields.types.len() != arrow_schema.fields.len() {
        return Err(Error::invalid_input_source("the number of fields in the provided substrait schema did not match the number of fields in the input schema.".into()));
    }
    let mut kept_substrait_fields = Vec::with_capacity(fields.types.len());
    let mut kept_arrow_fields = Vec::with_capacity(arrow_schema.fields.len());
    let mut index_mapping = HashMap::with_capacity(arrow_schema.fields.len());
    let mut field_counter = 0;
    let mut field_index = 0;
    // TODO: this logic doesn't catch user defined fields inside of struct fields
    for (substrait_field, arrow_field) in fields.types.iter().zip(arrow_schema.fields.iter()) {
        let num_fields = count_fields(substrait_field);

        let kind = substrait_field.kind.as_ref().unwrap();
        let is_user_defined = match kind {
            Kind::UserDefined(_) => true,
            // Keep compatibility with older Substrait plans.
            #[allow(deprecated)]
            Kind::UserDefinedTypeReference(_) => true,
            _ => false,
        };

        if !substrait_schema.names[field_index].starts_with("__unlikely_name_placeholder")
            && !is_user_defined
        {
            kept_substrait_fields.push(substrait_field.clone());
            kept_arrow_fields.push(arrow_field.clone());
            for i in 0..num_fields {
                index_mapping.insert(field_index + i, field_counter + i);
            }
            field_counter += num_fields;
        }
        field_index += num_fields;
    }
    let mut names = vec![String::new(); index_mapping.len()];
    for (old_idx, old_name) in substrait_schema.names.iter().enumerate() {
        if let Some(new_idx) = index_mapping.get(&old_idx) {
            names[*new_idx] = old_name.clone();
        }
    }
    let new_arrow_schema = Arc::new(ArrowSchema::new(kept_arrow_fields));
    let new_substrait_schema = NamedStruct {
        names,
        r#struct: Some(Struct {
            nullability: fields.nullability,
            type_variation_reference: fields.type_variation_reference,
            types: kept_substrait_fields,
        }),
    };
    Ok((new_substrait_schema, new_arrow_schema, index_mapping))
}

fn remap_expr_references(expr: &mut Expression, mapping: &HashMap<usize, usize>) -> Result<()> {
    match expr.rex_type.as_mut().unwrap() {
        // Simple, no field references possible
        RexType::Literal(_) | RexType::Nested(_) | RexType::DynamicParameter(_) => Ok(()),
        // Enum literals are deprecated in Substrait and should only appear in older plans.
        #[allow(deprecated)]
        RexType::Enum(_) => Ok(()),
        // Complex operators not supported in filters
        RexType::WindowFunction(_) | RexType::Subquery(_) => Err(Error::invalid_input(
            "Window functions or subqueries not allowed in filter expression",
        )),
        RexType::Lambda(_) | RexType::LambdaInvocation(_) => Err(Error::invalid_input(
            "Lambda expressions not allowed in filter expression",
        )),
        // Pass through operators, nested children may have field references
        RexType::ScalarFunction(func) => {
            #[allow(deprecated)]
            for arg in &mut func.args {
                remap_expr_references(arg, mapping)?;
            }
            for arg in &mut func.arguments {
                match arg.arg_type.as_mut().unwrap() {
                    ArgType::Value(expr) => remap_expr_references(expr, mapping)?,
                    ArgType::Enum(_) | ArgType::Type(_) => {}
                }
            }
            Ok(())
        }
        RexType::IfThen(ifthen) => {
            for clause in ifthen.ifs.iter_mut() {
                remap_expr_references(clause.r#if.as_mut().unwrap(), mapping)?;
                remap_expr_references(clause.then.as_mut().unwrap(), mapping)?;
            }
            remap_expr_references(ifthen.r#else.as_mut().unwrap(), mapping)?;
            Ok(())
        }
        RexType::SwitchExpression(switch) => {
            for clause in switch.ifs.iter_mut() {
                remap_expr_references(clause.then.as_mut().unwrap(), mapping)?;
            }
            remap_expr_references(switch.r#else.as_mut().unwrap(), mapping)?;
            Ok(())
        }
        RexType::SingularOrList(orlist) => {
            for opt in orlist.options.iter_mut() {
                remap_expr_references(opt, mapping)?;
            }
            remap_expr_references(orlist.value.as_mut().unwrap(), mapping)?;
            Ok(())
        }
        RexType::MultiOrList(orlist) => {
            for opt in orlist.options.iter_mut() {
                for field in opt.fields.iter_mut() {
                    remap_expr_references(field, mapping)?;
                }
            }
            for val in orlist.value.iter_mut() {
                remap_expr_references(val, mapping)?;
            }
            Ok(())
        }
        RexType::Cast(cast) => {
            remap_expr_references(cast.input.as_mut().unwrap(), mapping)?;
            Ok(())
        }
        RexType::Selection(sel) => {
            // Finally, the selection, which might actually have field references
            let root_type = sel.root_type.as_mut().unwrap();
            // These types of references do not reference input fields so no remap needed
            if matches!(
                root_type,
                RootType::Expression(_) | RootType::OuterReference(_)
            ) {
                return Ok(());
            }
            match sel.reference_type.as_mut().unwrap() {
                ReferenceType::DirectReference(direct) => {
                    match direct.reference_type.as_mut().unwrap() {
                        reference_segment::ReferenceType::ListElement(_)
                        | reference_segment::ReferenceType::MapKey(_) => Err(Error::invalid_input(
                            "map/list nested references not supported in pushdown filters",
                        )),
                        reference_segment::ReferenceType::StructField(field) => {
                            if field.child.is_some() {
                                Err(Error::invalid_input(
                                    "nested references in pushdown filters not yet supported",
                                ))
                            } else {
                                if let Some(new_index) = mapping.get(&(field.field as usize)) {
                                    field.field = *new_index as i32;
                                } else {
                                    return Err(Error::invalid_input(
                                        "pushdown filter referenced a field that is not yet supported by Substrait conversion",
                                    ));
                                }
                                Ok(())
                            }
                        }
                    }
                }
                ReferenceType::MaskedReference(_) => Err(Error::invalid_input(
                    "masked references not yet supported in filter expressions",
                )),
            }
        }
    }
}

/// Convert a Substrait ExtendedExpressions message into a DF Expr
///
/// The ExtendedExpressions message must contain a single scalar expression
pub async fn parse_substrait(
    expr: &[u8],
    input_schema: Arc<ArrowSchema>,
    state: &SessionState,
) -> Result<Expr> {
    let envelope = ExtendedExpression::decode(expr)?;
    if envelope.referred_expr.is_empty() {
        return Err(Error::invalid_input_source(
            "the provided substrait expression is empty (contains no expressions)".into(),
        ));
    }
    if envelope.referred_expr.len() > 1 {
        return Err(Error::invalid_input_source(
            format!(
                "the provided substrait expression had {} expressions when only 1 was expected",
                envelope.referred_expr.len()
            )
            .into(),
        ));
    }
    let mut expr = match &envelope.referred_expr[0].expr_type {
        None => Err(Error::invalid_input_source(
            "the provided substrait had an expression but was missing an expr_type".into(),
        )),
        Some(ExprType::Expression(expr)) => Ok(expr.clone()),
        _ => Err(Error::invalid_input_source(
            "the provided substrait was not a scalar expression".into(),
        )),
    }?;

    // The Substrait may have come from a producer that uses extension types that DF doesn't support (e.g.
    // from pyarrow) so we need to remove them and remap expr references (since they are indexes into the
    // schema and we may have removed some fields)
    let substrait_schema = if envelope.base_schema.as_ref().unwrap().r#struct.is_some() {
        let (substrait_schema, _, index_mapping) =
            remove_extension_types(envelope.base_schema.as_ref().unwrap(), input_schema.clone())?;

        if substrait_schema.r#struct.as_ref().unwrap().types.len()
            != envelope
                .base_schema
                .as_ref()
                .unwrap()
                .r#struct
                .as_ref()
                .unwrap()
                .types
                .len()
        {
            remap_expr_references(&mut expr, &index_mapping)?;
        }

        substrait_schema
    } else {
        envelope.base_schema.as_ref().unwrap().clone()
    };

    let extended_expr = ExtendedExpression {
        base_schema: Some(substrait_schema),
        referred_expr: vec![ExpressionReference {
            output_names: envelope.referred_expr[0].output_names.clone(),
            expr_type: Some(ExprType::Expression(expr)),
        }],
        ..envelope
    };

    let mut expr_container =
        datafusion_substrait::logical_plan::consumer::from_substrait_extended_expr(
            state,
            &extended_expr,
        )
        .await?;

    if expr_container.exprs.is_empty() {
        return Err(Error::invalid_input(
            "Substrait expression did not contain any expressions",
        ));
    }

    if expr_container.exprs.len() > 1 {
        return Err(Error::invalid_input(
            "Substrait expression contained multiple expressions",
        ));
    }

    Ok(expr_container.exprs.pop().unwrap().0)
}

/// Parse Substrait Plan bytes containing an AggregateRel.
pub async fn parse_substrait_aggregate(
    bytes: &[u8],
    input_schema: Arc<ArrowSchema>,
    state: &SessionState,
) -> Result<Aggregate> {
    let plan = Plan::decode(bytes)?;
    let (aggregate_rel, output_names) = extract_aggregate_from_plan(&plan)?;
    let extensions = Extensions::try_from(&plan.extensions)?;

    let mut agg =
        parse_aggregate_rel_with_extensions(&aggregate_rel, input_schema, state, &extensions)
            .await?;

    // Apply aliases from RelRoot.names to expressions
    if !output_names.is_empty() {
        let num_groups = agg.group_by.len();
        for (i, expr) in agg.group_by.iter_mut().enumerate() {
            if i < output_names.len() {
                *expr = expr.clone().alias(&output_names[i]);
            }
        }
        for (i, expr) in agg.aggregates.iter_mut().enumerate() {
            let name_idx = num_groups + i;
            if name_idx < output_names.len() {
                *expr = expr.clone().alias(&output_names[name_idx]);
            }
        }
    }

    Ok(agg)
}

fn extract_aggregate_from_plan(plan: &Plan) -> Result<(Box<AggregateRel>, Vec<String>)> {
    if plan.relations.is_empty() {
        return Err(Error::invalid_input("Substrait Plan has no relations"));
    }

    let plan_rel = &plan.relations[0];
    let (rel, output_names) = match &plan_rel.rel_type {
        Some(datafusion_substrait::substrait::proto::plan_rel::RelType::Root(root)) => {
            (root.input.as_ref(), root.names.clone())
        }
        Some(datafusion_substrait::substrait::proto::plan_rel::RelType::Rel(rel)) => {
            (Some(rel), vec![])
        }
        None => (None, vec![]),
    };

    let rel = rel.ok_or_else(|| Error::invalid_input("Plan relation has no input"))?;

    match &rel.rel_type {
        Some(RelType::Aggregate(agg)) => Ok((agg.clone(), output_names)),
        Some(other) => Err(Error::invalid_input(format!(
            "Expected Substrait AggregateRel, got {:?}",
            std::mem::discriminant(other)
        ))),
        None => Err(Error::invalid_input("Substrait Rel has no rel_type")),
    }
}

/// Parse an AggregateRel proto with provided extensions.
pub async fn parse_aggregate_rel_with_extensions(
    aggregate_rel: &AggregateRel,
    input_schema: Arc<ArrowSchema>,
    state: &SessionState,
    extensions: &Extensions,
) -> Result<Aggregate> {
    let df_schema = DFSchema::try_from(input_schema.as_ref().clone())?;
    let consumer = DefaultSubstraitConsumer::new(extensions, state);
    let group_by = parse_groupings(aggregate_rel, &df_schema, &consumer).await?;
    let aggregates = parse_measures(aggregate_rel, &df_schema, &consumer).await?;

    Ok(Aggregate::new(group_by, aggregates))
}

/// Parse an AggregateRel proto with default extensions.
pub async fn parse_aggregate_rel(
    aggregate_rel: &AggregateRel,
    input_schema: Arc<ArrowSchema>,
    state: &SessionState,
) -> Result<Aggregate> {
    let extensions = Extensions::default();
    parse_aggregate_rel_with_extensions(aggregate_rel, input_schema, state, &extensions).await
}

async fn parse_groupings(
    agg_rel: &AggregateRel,
    schema: &DFSchema,
    consumer: &DefaultSubstraitConsumer<'_>,
) -> Result<Vec<Expr>> {
    let mut group_exprs = Vec::new();

    // First, handle the new-style grouping_expressions + expression_references
    if !agg_rel.grouping_expressions.is_empty() {
        for grouping in &agg_rel.groupings {
            for expr_ref in &grouping.expression_references {
                let idx = *expr_ref as usize;
                if idx >= agg_rel.grouping_expressions.len() {
                    return Err(Error::invalid_input(format!(
                        "Grouping expression reference {} out of bounds (max: {})",
                        idx,
                        agg_rel.grouping_expressions.len()
                    )));
                }
                let expr = &agg_rel.grouping_expressions[idx];
                let df_expr = from_substrait_rex(consumer, expr, schema)
                    .await
                    .map_err(|e| {
                        Error::invalid_input(format!("Failed to parse grouping expression: {}", e))
                    })?;
                group_exprs.push(df_expr);
            }
        }
    } else {
        // Fallback to deprecated inline grouping_expressions within each Grouping
        #[allow(deprecated)]
        for grouping in &agg_rel.groupings {
            for expr in &grouping.grouping_expressions {
                let df_expr = from_substrait_rex(consumer, expr, schema)
                    .await
                    .map_err(|e| {
                        Error::invalid_input(format!("Failed to parse grouping expression: {}", e))
                    })?;
                group_exprs.push(df_expr);
            }
        }
    }

    Ok(group_exprs)
}

async fn parse_measures(
    agg_rel: &AggregateRel,
    schema: &DFSchema,
    consumer: &DefaultSubstraitConsumer<'_>,
) -> Result<Vec<Expr>> {
    let mut aggregates = Vec::new();

    for measure in &agg_rel.measures {
        if let Some(agg_func) = &measure.measure {
            // Parse optional filter
            let filter = if let Some(filter_expr) = &measure.filter {
                let df_filter = from_substrait_rex(consumer, filter_expr, schema)
                    .await
                    .map_err(|e| {
                        Error::invalid_input(format!("Failed to parse measure filter: {}", e))
                    })?;
                Some(Box::new(df_filter))
            } else {
                None
            };

            // Parse ordering (for ordered aggregates like ARRAY_AGG)
            let order_by = from_substrait_sorts(consumer, &agg_func.sorts, schema)
                .await
                .map_err(|e| {
                    Error::invalid_input(format!("Failed to parse aggregate sorts: {}", e))
                })?;

            // Check for DISTINCT invocation
            let distinct = matches!(
                agg_func.invocation,
                i if i == datafusion_substrait::substrait::proto::aggregate_function::AggregationInvocation::Distinct as i32
            );

            // Convert Substrait AggregateFunction to DataFusion Expr
            let df_expr =
                from_substrait_agg_func(consumer, agg_func, schema, filter, order_by, distinct)
                    .await
                    .map_err(|e| {
                        Error::invalid_input(format!("Failed to parse aggregate function: {}", e))
                    })?;

            aggregates.push(df_expr.as_ref().clone());
        }
    }

    Ok(aggregates)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_schema::{DataType, Field, Schema};
    use datafusion::{
        execution::SessionState,
        logical_expr::{BinaryExpr, Operator},
        prelude::{Expr, SessionContext},
    };
    use datafusion_common::{Column, ScalarValue};
    use datafusion_substrait::substrait::proto::{
        Expression, ExpressionReference, ExtendedExpression, FunctionArgument, NamedStruct, Type,
        Version,
        expression::{
            FieldReference, Literal, ReferenceSegment, RexType, ScalarFunction,
            field_reference::{ReferenceType, RootReference, RootType},
            literal::LiteralType,
            reference_segment::{self, StructField},
        },
        expression_reference::ExprType,
        extensions::{
            SimpleExtensionDeclaration, SimpleExtensionUrn,
            simple_extension_declaration::{ExtensionFunction, MappingType},
        },
        function_argument::ArgType,
        r#type::{Boolean, I32, Kind, Nullability, Struct},
    };
    use prost::Message;

    use crate::substrait::{encode_substrait, parse_substrait};

    fn session_state() -> SessionState {
        let ctx = SessionContext::new();
        ctx.state()
    }

    #[tokio::test]
    async fn test_substrait_conversion() {
        let expr = ExtendedExpression {
            version: Some(Version {
                major_number: 0,
                minor_number: 63,
                patch_number: 1,
                git_hash: "".to_string(),
                producer: "unit-test".to_string(),
            }),
            extension_urns: vec![
                SimpleExtensionUrn {
                    extension_urn_anchor: 1,
                    urn: "https://github.com/substrait-io/substrait/blob/main/extensions/functions_comparison.yaml".to_string(),
                }
            ],
            extensions: vec![
                SimpleExtensionDeclaration {
                    mapping_type: Some(MappingType::ExtensionFunction(ExtensionFunction {
                        extension_urn_reference: 1,
                        function_anchor: 1,
                        name: "lt".to_string(),
                    })),
                }
            ],
            referred_expr: vec![ExpressionReference {
                output_names: vec!["filter_mask".to_string()],
                expr_type: Some(ExprType::Expression(Expression {
                    rex_type: Some(RexType::ScalarFunction(ScalarFunction {
                        function_reference: 1,
                        arguments: vec![
                            FunctionArgument {
                                arg_type: Some(ArgType::Value(Expression {
                                    rex_type: Some(RexType::Selection(Box::new(FieldReference {
                                        reference_type: Some(ReferenceType::DirectReference(ReferenceSegment {
                                            reference_type: Some(reference_segment::ReferenceType::StructField(Box::new(StructField { field: 0, child: None })))
                                        })),
                                        root_type: Some(RootType::RootReference(RootReference {}))
                                    })))
                                }))
                            },
                            FunctionArgument {
                                arg_type: Some(ArgType::Value(Expression {
                                    rex_type: Some(RexType::Literal(Literal {
                                        nullable: false,
                                        type_variation_reference: 0,
                                        literal_type: Some(LiteralType::I32(0))
                                    }))
                                }))
                            }
                        ],
                        options: vec![],
                        output_type: Some(Type {
                            kind: Some(Kind::Bool(Boolean {
                                type_variation_reference: 0,
                                nullability: Nullability::Required as i32,
                            })),
                        }),
                        #[allow(deprecated)]
                        args: vec![],
                    }))
                })),
            }],
            base_schema: Some(NamedStruct {
                names: vec!["x".to_string()],
                r#struct: Some(Struct {
                    types: vec![Type {
                        kind: Some(Kind::I32(I32 {
                            type_variation_reference: 0,
                            nullability: Nullability::Nullable as i32,
                        })),
                    }],
                    type_variation_reference: 0,
                    nullability: Nullability::Required as i32,
                }),
            }),
            advanced_extensions: None,
            expected_type_urls: vec![],
        };
        let expr_bytes = expr.encode_to_vec();

        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, true)]));

        let df_expr = parse_substrait(expr_bytes.as_slice(), schema, &session_state())
            .await
            .unwrap();

        let expected = Expr::BinaryExpr(BinaryExpr {
            left: Box::new(Expr::Column(Column::new_unqualified("x"))),
            op: Operator::Lt,
            right: Box::new(Expr::Literal(ScalarValue::Int32(Some(0)), None)),
        });
        assert_eq!(df_expr, expected);
    }

    #[tokio::test]
    async fn test_expr_substrait_roundtrip() {
        let schema = arrow_schema::Schema::new(vec![Field::new("x", DataType::Int32, true)]);
        let expr = Expr::BinaryExpr(BinaryExpr {
            left: Box::new(Expr::Column(Column::new_unqualified("x"))),
            op: Operator::Lt,
            right: Box::new(Expr::Literal(ScalarValue::Int32(Some(0)), None)),
        });

        let bytes =
            encode_substrait(expr.clone(), Arc::new(schema.clone()), &session_state()).unwrap();

        let decoded = parse_substrait(bytes.as_slice(), Arc::new(schema.clone()), &session_state())
            .await
            .unwrap();
        assert_eq!(decoded, expr);
    }

    /// Helper to create a simple equality filter on the "id" field
    fn id_filter(value: &str) -> Expr {
        Expr::BinaryExpr(BinaryExpr {
            left: Box::new(Expr::Column(Column::new_unqualified("id"))),
            op: Operator::Eq,
            right: Box::new(Expr::Literal(
                ScalarValue::Utf8(Some(value.to_string())),
                None,
            )),
        })
    }

    /// Helper to test substrait roundtrip encode/decode
    async fn assert_substrait_roundtrip(schema: Schema, expr: Expr) {
        let schema = Arc::new(schema);
        let bytes = encode_substrait(expr.clone(), schema.clone(), &session_state()).unwrap();
        let decoded = parse_substrait(bytes.as_slice(), schema, &session_state())
            .await
            .unwrap();
        assert_eq!(decoded, expr);
    }

    /// Helper to create List<Struct> field
    fn list_of_struct(name: &str, fields: Vec<Field>) -> Field {
        Field::new(
            name,
            DataType::List(Arc::new(Field::new(
                "item",
                DataType::Struct(fields.into()),
                true,
            ))),
            true,
        )
    }

    #[tokio::test]
    async fn test_substrait_roundtrip_with_list_of_struct() {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            list_of_struct(
                "top_previous_companies",
                vec![
                    Field::new("company_id", DataType::Int64, true),
                    Field::new("company_name", DataType::Utf8, true),
                ],
            ),
            Field::new("name", DataType::Utf8, true),
        ]);

        assert_substrait_roundtrip(schema, id_filter("test-id")).await;
    }

    #[tokio::test]
    async fn test_substrait_roundtrip_with_list_struct_struct() {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            list_of_struct(
                "employees_count_breakdown_by_month",
                vec![
                    Field::new("date", DataType::Utf8, true),
                    Field::new(
                        "breakdown",
                        DataType::Struct(
                            vec![
                                Field::new("employees_count_owner", DataType::Int64, true),
                                Field::new("employees_count_founder", DataType::Int64, true),
                                Field::new("employees_count_clevel", DataType::Int64, true),
                            ]
                            .into(),
                        ),
                        true,
                    ),
                ],
            ),
            Field::new("name", DataType::Utf8, true),
        ]);

        assert_substrait_roundtrip(schema, id_filter("test-id")).await;
    }

    #[tokio::test]
    async fn test_substrait_roundtrip_with_many_nested_columns() {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new(
                "location",
                DataType::Struct(
                    vec![
                        Field::new("city", DataType::Utf8, true),
                        Field::new("country", DataType::Utf8, true),
                    ]
                    .into(),
                ),
                true,
            ),
            list_of_struct(
                "top_previous_companies",
                vec![
                    Field::new("company_id", DataType::Int64, true),
                    Field::new("company_name", DataType::Utf8, true),
                ],
            ),
            list_of_struct(
                "employees_by_month",
                vec![
                    Field::new("date", DataType::Utf8, true),
                    Field::new(
                        "breakdown",
                        DataType::Struct(
                            vec![
                                Field::new("count_owner", DataType::Int64, true),
                                Field::new("count_founder", DataType::Int64, true),
                            ]
                            .into(),
                        ),
                        true,
                    ),
                ],
            ),
            Field::new("name", DataType::Utf8, true),
        ]);

        assert_substrait_roundtrip(schema, id_filter("test-id")).await;
    }

    #[tokio::test]
    async fn test_substrait_roundtrip_with_null_and_float16_columns() {
        // Float16 and Null are encoded as UserDefined types in Substrait.
        // The decode side (remove_extension_types) strips them and remaps
        // field references, so filters on other columns still work.
        let schema = Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("embedding", DataType::Float16, true),
            Field::new("empty", DataType::Null, true),
            Field::new("name", DataType::Utf8, true),
        ]);

        assert_substrait_roundtrip(schema, id_filter("test-id")).await;
    }

    #[tokio::test]
    async fn test_substrait_roundtrip_with_fixed_size_list_column() {
        // FixedSizeList has no Substrait producer support, so it must be
        // pruned from the schema before encoding. Verify that a schema with
        // FSL columns works when the filter references a different column.
        use crate::substrait::prune_schema_for_substrait;

        let schema = Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new(
                "vector",
                DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), 128),
                true,
            ),
            Field::new("name", DataType::Utf8, true),
        ]);

        // Encoding with the full schema would fail, but pruning removes the FSL column
        let pruned = prune_schema_for_substrait(&schema);
        assert_eq!(pruned.fields().len(), 2); // id and name only
        assert_substrait_roundtrip(pruned, id_filter("test-id")).await;
    }

    // ==================== Aggregate parsing tests ====================

    use datafusion_substrait::substrait::proto::{
        AggregateFunction, AggregateRel, Plan, PlanRel, Rel, RelRoot,
        aggregate_function::AggregationInvocation,
        aggregate_rel::{Grouping, Measure},
        rel::RelType,
    };

    /// Helper to create a field reference expression for a column index
    fn agg_field_ref(field_index: i32) -> Expression {
        Expression {
            rex_type: Some(RexType::Selection(Box::new(FieldReference {
                reference_type: Some(ReferenceType::DirectReference(ReferenceSegment {
                    reference_type: Some(reference_segment::ReferenceType::StructField(Box::new(
                        StructField {
                            field: field_index,
                            child: None,
                        },
                    ))),
                })),
                root_type: Some(RootType::RootReference(RootReference {})),
            }))),
        }
    }

    /// Create extension declaration for an aggregate function
    fn agg_extension(anchor: u32, name: &str) -> SimpleExtensionDeclaration {
        SimpleExtensionDeclaration {
            mapping_type: Some(MappingType::ExtensionFunction(ExtensionFunction {
                extension_urn_reference: 1,
                function_anchor: anchor,
                name: name.to_string(),
            })),
        }
    }

    /// Helper to create a Substrait Plan with AggregateRel
    fn create_aggregate_plan(
        measures: Vec<Measure>,
        grouping_expressions: Vec<Expression>,
        groupings: Vec<Grouping>,
        extensions: Vec<SimpleExtensionDeclaration>,
    ) -> Vec<u8> {
        let aggregate_rel = AggregateRel {
            common: None,
            input: None, // Input is ignored for pushdown
            groupings,
            measures,
            grouping_expressions,
            advanced_extension: None,
        };

        let rel = Rel {
            rel_type: Some(RelType::Aggregate(Box::new(aggregate_rel))),
        };

        // Wrap in a Plan to include extensions
        let plan = Plan {
            version: Some(Version {
                major_number: 0,
                minor_number: 63,
                patch_number: 0,
                git_hash: String::new(),
                producer: "lance-test".to_string(),
            }),
            extension_urns: vec![SimpleExtensionUrn {
                extension_urn_anchor: 1,
                urn: "https://github.com/substrait-io/substrait/blob/main/extensions/functions_aggregate_generic.yaml".to_string(),
            }],
            extensions,
            relations: vec![PlanRel {
                rel_type: Some(
                    datafusion_substrait::substrait::proto::plan_rel::RelType::Root(RelRoot {
                        input: Some(rel),
                        names: vec![],
                    }),
                ),
            }],
            advanced_extensions: None,
            expected_type_urls: vec![],
            parameter_bindings: vec![],
            type_aliases: vec![],
        };

        plan.encode_to_vec()
    }

    /// Create a COUNT(*) measure
    fn count_star_measure(function_ref: u32) -> Measure {
        Measure {
            measure: Some(AggregateFunction {
                function_reference: function_ref,
                arguments: vec![],
                options: vec![],
                output_type: None,
                phase: 0,
                sorts: vec![],
                invocation: AggregationInvocation::All as i32,
                #[allow(deprecated)]
                args: vec![],
            }),
            filter: None,
        }
    }

    /// Create a SUM/AVG/MIN/MAX measure on a column
    fn simple_agg_measure(function_ref: u32, column_index: i32) -> Measure {
        Measure {
            measure: Some(AggregateFunction {
                function_reference: function_ref,
                arguments: vec![FunctionArgument {
                    arg_type: Some(ArgType::Value(agg_field_ref(column_index))),
                }],
                options: vec![],
                output_type: None,
                phase: 0,
                sorts: vec![],
                invocation: AggregationInvocation::All as i32,
                #[allow(deprecated)]
                args: vec![],
            }),
            filter: None,
        }
    }

    #[tokio::test]
    async fn test_parse_substrait_aggregate_count_star() {
        let bytes = create_aggregate_plan(
            vec![count_star_measure(0)],
            vec![],
            vec![],
            vec![agg_extension(0, "count")],
        );

        let schema = Arc::new(Schema::new(vec![
            Field::new("x", DataType::Int32, true),
            Field::new("y", DataType::Int64, true),
        ]));

        let result =
            crate::substrait::parse_substrait_aggregate(&bytes, schema, &session_state()).await;

        let agg = result.expect("Failed to parse COUNT(*) aggregate");
        assert!(agg.group_by.is_empty(), "COUNT(*) should have no group by");
        assert_eq!(agg.aggregates.len(), 1, "Should have exactly one aggregate");

        // Verify it's a COUNT aggregate
        let agg_expr = &agg.aggregates[0];
        assert!(
            agg_expr.schema_name().to_string().contains("count"),
            "Expected COUNT aggregate, got: {}",
            agg_expr.schema_name()
        );
    }

    #[tokio::test]
    async fn test_parse_substrait_aggregate_sum() {
        let bytes = create_aggregate_plan(
            vec![simple_agg_measure(0, 1)], // SUM on column index 1 (y)
            vec![],
            vec![],
            vec![agg_extension(0, "sum")],
        );

        let schema = Arc::new(Schema::new(vec![
            Field::new("x", DataType::Int32, true),
            Field::new("y", DataType::Int64, true),
        ]));

        let result =
            crate::substrait::parse_substrait_aggregate(&bytes, schema, &session_state()).await;

        let agg = result.expect("Failed to parse SUM aggregate");
        assert!(agg.group_by.is_empty(), "SUM should have no group by");
        assert_eq!(agg.aggregates.len(), 1, "Should have exactly one aggregate");

        // Verify it's a SUM aggregate
        let agg_expr = &agg.aggregates[0];
        assert!(
            agg_expr.schema_name().to_string().contains("sum"),
            "Expected SUM aggregate, got: {}",
            agg_expr.schema_name()
        );
    }

    #[tokio::test]
    async fn test_parse_substrait_aggregate_sum_with_group_by() {
        // SUM(y) GROUP BY x
        let bytes = create_aggregate_plan(
            vec![simple_agg_measure(0, 1)], // SUM on column index 1 (y)
            vec![agg_field_ref(0)],         // Group by column index 0 (x)
            vec![Grouping {
                #[allow(deprecated)]
                grouping_expressions: vec![],
                expression_references: vec![0], // Reference to first grouping_expression
            }],
            vec![agg_extension(0, "sum")],
        );

        let schema = Arc::new(Schema::new(vec![
            Field::new("x", DataType::Int32, true),
            Field::new("y", DataType::Int64, true),
        ]));

        let result =
            crate::substrait::parse_substrait_aggregate(&bytes, schema, &session_state()).await;

        let agg = result.expect("Failed to parse SUM with GROUP BY");
        assert_eq!(
            agg.group_by.len(),
            1,
            "Should have exactly one group by expression"
        );
        assert_eq!(agg.aggregates.len(), 1, "Should have exactly one aggregate");

        // Verify group by is column x
        let group_expr = &agg.group_by[0];
        assert!(
            group_expr.schema_name().to_string().contains('x'),
            "Expected group by on column x, got: {}",
            group_expr.schema_name()
        );

        // Verify it's a SUM aggregate
        let agg_expr = &agg.aggregates[0];
        assert!(
            agg_expr.schema_name().to_string().contains("sum"),
            "Expected SUM aggregate, got: {}",
            agg_expr.schema_name()
        );
    }

    #[tokio::test]
    async fn test_parse_substrait_aggregate_multiple_aggregates() {
        // COUNT(*) and SUM(y)
        let bytes = create_aggregate_plan(
            vec![count_star_measure(0), simple_agg_measure(1, 1)],
            vec![],
            vec![],
            vec![agg_extension(0, "count"), agg_extension(1, "sum")],
        );

        let schema = Arc::new(Schema::new(vec![
            Field::new("x", DataType::Int32, true),
            Field::new("y", DataType::Int64, true),
        ]));

        let result =
            crate::substrait::parse_substrait_aggregate(&bytes, schema, &session_state()).await;

        let agg = result.expect("Failed to parse multiple aggregates");
        assert!(agg.group_by.is_empty(), "Should have no group by");
        assert_eq!(agg.aggregates.len(), 2, "Should have two aggregates");

        // Verify COUNT
        assert!(
            agg.aggregates[0]
                .schema_name()
                .to_string()
                .contains("count"),
            "Expected COUNT aggregate, got: {}",
            agg.aggregates[0].schema_name()
        );

        // Verify SUM
        assert!(
            agg.aggregates[1].schema_name().to_string().contains("sum"),
            "Expected SUM aggregate, got: {}",
            agg.aggregates[1].schema_name()
        );
    }

    // ==================== LIKE and starts_with tests ====================

    #[tokio::test]
    async fn test_substrait_roundtrip_like() {
        use datafusion::logical_expr::Like;

        let schema = Schema::new(vec![Field::new("name", DataType::Utf8, true)]);

        let like_expr = Expr::Like(Like {
            negated: false,
            expr: Box::new(Expr::Column(Column::new_unqualified("name"))),
            pattern: Box::new(Expr::Literal(
                ScalarValue::Utf8(Some("test%".to_string())),
                None,
            )),
            escape_char: None,
            case_insensitive: false,
        });

        assert_substrait_roundtrip(schema, like_expr).await;
    }

    #[tokio::test]
    async fn test_substrait_roundtrip_starts_with() {
        use datafusion::functions::string::starts_with;

        let schema = Schema::new(vec![Field::new("name", DataType::Utf8, true)]);

        let starts_with_expr = starts_with().call(vec![
            Expr::Column(Column::new_unqualified("name")),
            Expr::Literal(ScalarValue::Utf8(Some("prefix".to_string())), None),
        ]);

        assert_substrait_roundtrip(schema, starts_with_expr).await;
    }
}
