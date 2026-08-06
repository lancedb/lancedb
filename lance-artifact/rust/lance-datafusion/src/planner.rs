// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Exec plan planner

use std::borrow::Cow;
use std::collections::{BTreeSet, VecDeque};
use std::sync::Arc;

use crate::exec::{LanceExecutionOptions, get_session_context};
use crate::expr::safe_coerce_scalar;
use crate::logical_expr::{coerce_filter_type_to_boolean, get_as_string_scalar_opt, resolve_expr};
use crate::sql::{parse_sql_expr, parse_sql_filter};
use arrow::compute::CastOptions;
use arrow_array::ListArray;
use arrow_buffer::OffsetBuffer;
use arrow_cast::cast_with_options;
use arrow_schema::{DataType as ArrowDataType, Field, SchemaRef, TimeUnit};
use arrow_select::concat::concat;
use datafusion::common::DFSchema;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion, TreeNodeVisitor};
use datafusion::config::ConfigOptions;
use datafusion::error::Result as DFResult;
use datafusion::execution::context::SessionState;
use datafusion::logical_expr::expr::ScalarFunction;
use datafusion::logical_expr::planner::{ExprPlanner, PlannerResult, RawFieldAccessExpr};
use datafusion::logical_expr::{
    AggregateUDF, ColumnarValue, GetFieldAccess, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl,
    Signature, Volatility, WindowUDF,
};
use datafusion::optimizer::simplify_expressions::SimplifyContext;
use datafusion::sql::planner::{
    ContextProvider, NullOrdering, ParserOptions, PlannerContext, SqlToRel,
};
use datafusion::sql::sqlparser::ast::{
    AccessExpr, Array as SQLArray, BinaryOperator, DataType as SQLDataType, ExactNumberInfo,
    Expr as SQLExpr, Function, FunctionArg, FunctionArgExpr, FunctionArguments, Ident,
    ObjectNamePart, Subscript, TimezoneInfo, TypedString, UnaryOperator, Value, ValueWithSpan,
};
use datafusion::{
    common::Column,
    logical_expr::{Between, BinaryExpr, Like, Operator},
    physical_plan::PhysicalExpr,
    prelude::Expr,
    scalar::ScalarValue,
};
use datafusion_functions::core::getfield::GetFieldFunc;
use lance_core::datatypes::Schema;
use lance_core::error::LanceOptionExt;

use chrono::Utc;
use lance_core::{Error, Result};

/// Encode a JSON string into a JSONB `LargeBinary` literal expression.
fn encode_jsonb(json_str: &str) -> Result<Expr> {
    let bytes = lance_arrow::json::encode_json(json_str)
        .map_err(|e| Error::invalid_input(format!("Failed to encode JSONB: {e}")))?;
    Ok(Expr::Literal(ScalarValue::LargeBinary(Some(bytes)), None))
}

// The escape in `LIKE/ILIKE ... ESCAPE '<char>'` must be exactly one character.
// Reject empty or multi-character escape strings rather than silently treating
// them as "no escape" or truncating to the first character.
fn parse_like_escape_char(escape_char: &Option<ValueWithSpan>) -> Result<Option<char>> {
    let Some(value) = escape_char else {
        return Ok(None);
    };
    let ValueWithSpan {
        value: Value::SingleQuotedString(escape),
        ..
    } = value
    else {
        return Err(Error::invalid_input(format!(
            "Invalid escape character in LIKE expression. Expected a single character wrapped with single quotes, got {value}"
        )));
    };
    let mut chars = escape.chars();
    match (chars.next(), chars.next()) {
        (Some(c), None) => Ok(Some(c)),
        _ => Err(Error::invalid_input(format!(
            "Invalid escape character in LIKE expression. Expected a single character, got '{escape}'"
        ))),
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
struct CastListF16Udf {
    signature: Signature,
}

impl CastListF16Udf {
    pub fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for CastListF16Udf {
    fn name(&self) -> &str {
        "_cast_list_f16"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[ArrowDataType]) -> DFResult<ArrowDataType> {
        let input = &arg_types[0];
        match input {
            ArrowDataType::FixedSizeList(field, size) => {
                if field.data_type() != &ArrowDataType::Float32
                    && field.data_type() != &ArrowDataType::Float16
                {
                    return Err(datafusion::error::DataFusionError::Execution(
                        "cast_list_f16 only supports list of float32 or float16".to_string(),
                    ));
                }
                Ok(ArrowDataType::FixedSizeList(
                    Arc::new(Field::new(
                        field.name(),
                        ArrowDataType::Float16,
                        field.is_nullable(),
                    )),
                    *size,
                ))
            }
            ArrowDataType::List(field) => {
                if field.data_type() != &ArrowDataType::Float32
                    && field.data_type() != &ArrowDataType::Float16
                {
                    return Err(datafusion::error::DataFusionError::Execution(
                        "cast_list_f16 only supports list of float32 or float16".to_string(),
                    ));
                }
                Ok(ArrowDataType::List(Arc::new(Field::new(
                    field.name(),
                    ArrowDataType::Float16,
                    field.is_nullable(),
                ))))
            }
            _ => Err(datafusion::error::DataFusionError::Execution(
                "cast_list_f16 only supports FixedSizeList/List arguments".to_string(),
            )),
        }
    }

    fn invoke_with_args(&self, func_args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let ColumnarValue::Array(arr) = &func_args.args[0] else {
            return Err(datafusion::error::DataFusionError::Execution(
                "cast_list_f16 only supports array arguments".to_string(),
            ));
        };

        let to_type = match arr.data_type() {
            ArrowDataType::FixedSizeList(field, size) => ArrowDataType::FixedSizeList(
                Arc::new(Field::new(
                    field.name(),
                    ArrowDataType::Float16,
                    field.is_nullable(),
                )),
                *size,
            ),
            ArrowDataType::List(field) => ArrowDataType::List(Arc::new(Field::new(
                field.name(),
                ArrowDataType::Float16,
                field.is_nullable(),
            ))),
            _ => {
                return Err(datafusion::error::DataFusionError::Execution(
                    "cast_list_f16 only supports array arguments".to_string(),
                ));
            }
        };

        let res = cast_with_options(arr.as_ref(), &to_type, &CastOptions::default())?;
        Ok(ColumnarValue::Array(res))
    }
}

// Adapter that instructs datafusion how lance expects expressions to be interpreted
struct LanceContextProvider {
    options: datafusion::config::ConfigOptions,
    state: SessionState,
    expr_planners: Vec<Arc<dyn ExprPlanner>>,
}

impl Default for LanceContextProvider {
    fn default() -> Self {
        let ctx = get_session_context(&LanceExecutionOptions::default());
        let state = ctx.state();
        let expr_planners = state.expr_planners().to_vec();

        Self {
            options: ConfigOptions::default(),
            state,
            expr_planners,
        }
    }
}

impl ContextProvider for LanceContextProvider {
    fn get_table_source(
        &self,
        name: datafusion::sql::TableReference,
    ) -> DFResult<Arc<dyn datafusion::logical_expr::TableSource>> {
        Err(datafusion::error::DataFusionError::NotImplemented(format!(
            "Attempt to reference inner table {} not supported",
            name
        )))
    }

    fn get_aggregate_meta(&self, name: &str) -> Option<Arc<AggregateUDF>> {
        self.state.aggregate_functions().get(name).cloned()
    }

    fn get_window_meta(&self, name: &str) -> Option<Arc<WindowUDF>> {
        self.state.window_functions().get(name).cloned()
    }

    fn get_higher_order_meta(
        &self,
        name: &str,
    ) -> Option<Arc<datafusion::logical_expr::HigherOrderUDF>> {
        self.state.higher_order_functions().get(name).cloned()
    }

    fn get_function_meta(&self, f: &str) -> Option<Arc<ScalarUDF>> {
        match f {
            // TODO: cast should go thru CAST syntax instead of UDF
            // Going thru UDF makes it hard for the optimizer to find no-ops
            "_cast_list_f16" => Some(Arc::new(ScalarUDF::new_from_impl(CastListF16Udf::new()))),
            _ => self.state.scalar_functions().get(f).cloned(),
        }
    }

    fn get_variable_type(&self, _: &[String]) -> Option<ArrowDataType> {
        // Variables (things like @@LANGUAGE) not supported
        None
    }

    fn options(&self) -> &datafusion::config::ConfigOptions {
        &self.options
    }

    fn udf_names(&self) -> Vec<String> {
        self.state.scalar_functions().keys().cloned().collect()
    }

    fn udaf_names(&self) -> Vec<String> {
        self.state.aggregate_functions().keys().cloned().collect()
    }

    fn udwf_names(&self) -> Vec<String> {
        self.state.window_functions().keys().cloned().collect()
    }

    fn higher_order_function_names(&self) -> Vec<String> {
        self.state
            .higher_order_functions()
            .keys()
            .cloned()
            .collect()
    }

    fn get_expr_planners(&self) -> &[Arc<dyn ExprPlanner>] {
        &self.expr_planners
    }
}

pub struct Planner {
    schema: SchemaRef,
    context_provider: LanceContextProvider,
    enable_relations: bool,
}

impl Planner {
    pub fn new(schema: SchemaRef) -> Self {
        Self {
            schema,
            context_provider: LanceContextProvider::default(),
            enable_relations: false,
        }
    }

    /// If passed with `true`, then the first identifier in column reference
    /// is parsed as the relation. For example, `table.field.inner` will be
    /// read as the nested field `field.inner` (`inner` on struct field `field`)
    /// on the `table` relation. If `false` (the default), then no relations
    /// are used and all identifiers are assumed to be a nested column path.
    pub fn with_enable_relations(mut self, enable_relations: bool) -> Self {
        self.enable_relations = enable_relations;
        self
    }

    /// Resolve a column name using case-insensitive matching against the schema.
    /// Returns the actual field name if found, otherwise returns the original name.
    fn resolve_column_name(&self, name: &str) -> String {
        // Try exact match first
        if self.schema.field_with_name(name).is_ok() {
            return name.to_string();
        }
        // Fall back to case-insensitive match
        for field in self.schema.fields() {
            if field.name().eq_ignore_ascii_case(name) {
                return field.name().clone();
            }
        }
        // Not found in schema - return original (might be computed column, system column, etc.)
        name.to_string()
    }

    fn column(&self, idents: &[Ident]) -> Expr {
        fn handle_remaining_idents(expr: &mut Expr, idents: &[Ident]) {
            for ident in idents {
                *expr = Expr::ScalarFunction(ScalarFunction {
                    args: vec![
                        std::mem::take(expr),
                        Expr::Literal(ScalarValue::Utf8(Some(ident.value.clone())), None),
                    ],
                    func: Arc::new(ScalarUDF::new_from_impl(GetFieldFunc::default())),
                });
            }
        }

        if self.enable_relations && idents.len() > 1 {
            // Create qualified column reference (relation.column)
            let relation = &idents[0].value;
            let column_name = self.resolve_column_name(&idents[1].value);
            let column = Expr::Column(Column::new(Some(relation.clone()), column_name));
            let mut result = column;
            handle_remaining_idents(&mut result, &idents[2..]);
            result
        } else {
            // Default behavior - treat as struct field access
            // Use resolved column name to handle case-insensitive matching
            let resolved_name = self.resolve_column_name(&idents[0].value);
            let mut column = Expr::Column(Column::from_name(resolved_name));
            handle_remaining_idents(&mut column, &idents[1..]);
            column
        }
    }

    fn binary_op(&self, op: &BinaryOperator) -> Result<Operator> {
        Ok(match op {
            BinaryOperator::Plus => Operator::Plus,
            BinaryOperator::Minus => Operator::Minus,
            BinaryOperator::Multiply => Operator::Multiply,
            BinaryOperator::Divide => Operator::Divide,
            BinaryOperator::Modulo => Operator::Modulo,
            BinaryOperator::StringConcat => Operator::StringConcat,
            BinaryOperator::Gt => Operator::Gt,
            BinaryOperator::Lt => Operator::Lt,
            BinaryOperator::GtEq => Operator::GtEq,
            BinaryOperator::LtEq => Operator::LtEq,
            BinaryOperator::Eq => Operator::Eq,
            BinaryOperator::NotEq => Operator::NotEq,
            BinaryOperator::And => Operator::And,
            BinaryOperator::Or => Operator::Or,
            _ => {
                return Err(Error::invalid_input(format!(
                    "Operator {op} is not supported"
                )));
            }
        })
    }

    fn is_logical_binary_op(op: &BinaryOperator) -> bool {
        matches!(op, BinaryOperator::And | BinaryOperator::Or)
    }

    fn is_same_logical_binary_op(left: &BinaryOperator, right: &BinaryOperator) -> bool {
        matches!(
            (left, right),
            (BinaryOperator::And, BinaryOperator::And) | (BinaryOperator::Or, BinaryOperator::Or)
        )
    }

    fn flatten_logical_binary_exprs<'a>(
        left: &'a SQLExpr,
        op: &BinaryOperator,
        right: &'a SQLExpr,
    ) -> Vec<&'a SQLExpr> {
        let mut leaves = Vec::new();
        let mut stack = vec![right, left];

        while let Some(expr) = stack.pop() {
            match expr {
                SQLExpr::BinaryOp {
                    left,
                    op: child_op,
                    right,
                } if Self::is_same_logical_binary_op(op, child_op) => {
                    stack.push(right.as_ref());
                    stack.push(left.as_ref());
                }
                _ => leaves.push(expr),
            }
        }

        leaves
    }

    fn balanced_binary_expr(mut exprs: VecDeque<Expr>, op: Operator) -> Result<Expr> {
        if exprs.is_empty() {
            return Err(Error::invalid_input("Binary expression has no operands"));
        }

        while exprs.len() > 1 {
            let mut next = VecDeque::with_capacity(exprs.len().div_ceil(2));
            while let Some(left) = exprs.pop_front() {
                if let Some(right) = exprs.pop_front() {
                    next.push_back(Expr::BinaryExpr(BinaryExpr::new(
                        Box::new(left),
                        op,
                        Box::new(right),
                    )));
                } else {
                    next.push_back(left);
                }
            }
            exprs = next;
        }

        exprs
            .pop_front()
            .ok_or_else(|| Error::invalid_input("Binary expression has no operands"))
    }

    fn binary_expr(&self, left: &SQLExpr, op: &BinaryOperator, right: &SQLExpr) -> Result<Expr> {
        let df_op = self.binary_op(op)?;
        if Self::is_logical_binary_op(op) {
            let leaves = Self::flatten_logical_binary_exprs(left, op, right);
            let mut exprs = VecDeque::with_capacity(leaves.len());
            for leaf in leaves {
                exprs.push_back(self.parse_sql_expr(leaf)?);
            }
            return Self::balanced_binary_expr(exprs, df_op);
        }

        Ok(Expr::BinaryExpr(BinaryExpr::new(
            Box::new(self.parse_sql_expr(left)?),
            df_op,
            Box::new(self.parse_sql_expr(right)?),
        )))
    }

    fn unary_expr(&self, op: &UnaryOperator, expr: &SQLExpr) -> Result<Expr> {
        Ok(match op {
            UnaryOperator::Not | UnaryOperator::BitwiseNot => {
                Expr::Not(Box::new(self.parse_sql_expr(expr)?))
            }

            UnaryOperator::Minus => {
                use datafusion::logical_expr::lit;
                match expr {
                    SQLExpr::Value(ValueWithSpan { value: Value::Number(n, _), ..}) => match n.parse::<i64>() {
                        Ok(n) => lit(-n),
                        Err(_) => lit(-n
                            .parse::<f64>()
                            .map_err(|_e| {
                                Error::invalid_input(format!("negative operator can be only applied to integer and float operands, got: {n}"))
                            })?),
                    },
                    _ => {
                        Expr::Negative(Box::new(self.parse_sql_expr(expr)?))
                    }
                }
            }

            _ => {
                return Err(Error::invalid_input(format!(
                    "Unary operator '{:?}' is not supported",
                    op
                )));
            }
        })
    }

    // See datafusion `sqlToRel::parse_sql_number()`
    fn number(&self, value: &str, negative: bool) -> Result<Expr> {
        use datafusion::logical_expr::lit;
        let value: Cow<str> = if negative {
            Cow::Owned(format!("-{}", value))
        } else {
            Cow::Borrowed(value)
        };
        if let Ok(n) = value.parse::<i64>() {
            Ok(lit(n))
        } else if let Ok(n) = value.parse::<u64>() {
            Ok(lit(n))
        } else {
            value.parse::<f64>().map(lit).map_err(|_| {
                Error::invalid_input(format!("'{value}' is not supported number value."))
            })
        }
    }

    fn value(&self, value: &Value) -> Result<Expr> {
        Ok(match value {
            Value::Number(v, _) => self.number(v.as_str(), false)?,
            Value::SingleQuotedString(s) => Expr::Literal(ScalarValue::Utf8(Some(s.clone())), None),
            Value::HexStringLiteral(hsl) => {
                Expr::Literal(ScalarValue::Binary(Self::try_decode_hex_literal(hsl)), None)
            }
            Value::DoubleQuotedString(s) => Expr::Literal(ScalarValue::Utf8(Some(s.clone())), None),
            Value::Boolean(v) => Expr::Literal(ScalarValue::Boolean(Some(*v)), None),
            Value::Null => Expr::Literal(ScalarValue::Null, None),
            _ => todo!(),
        })
    }

    fn parse_function_args(&self, func_args: &FunctionArg) -> Result<Expr> {
        match func_args {
            FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => self.parse_sql_expr(expr),
            _ => Err(Error::invalid_input(format!(
                "Unsupported function args: {:?}",
                func_args
            ))),
        }
    }

    // We now use datafusion to parse functions.  This allows us to use datafusion's
    // entire collection of functions (previously we had just hard-coded support for two functions).
    //
    // Unfortunately, one of those two functions was is_valid and the reason we needed it was because
    // this is a function that comes from duckdb.  Datafusion does not consider is_valid to be a function
    // but rather an AST node (Expr::IsNotNull) and so we need to handle this case specially.
    fn legacy_parse_function(&self, func: &Function) -> Result<Expr> {
        match &func.args {
            FunctionArguments::List(args) => {
                if func.name.0.len() != 1 {
                    return Err(Error::invalid_input(format!(
                        "Function name must have 1 part, got: {:?}",
                        func.name.0
                    )));
                }
                Ok(Expr::IsNotNull(Box::new(
                    self.parse_function_args(&args.args[0])?,
                )))
            }
            _ => Err(Error::invalid_input(format!(
                "Unsupported function args: {:?}",
                func.args
            ))),
        }
    }

    fn parse_function(&self, function: SQLExpr) -> Result<Expr> {
        if let SQLExpr::Function(function) = &function
            && let Some(ObjectNamePart::Identifier(name)) = &function.name.0.first()
            && &name.value == "is_valid"
        {
            return self.legacy_parse_function(function);
        }
        let sql_to_rel = SqlToRel::new_with_options(
            &self.context_provider,
            ParserOptions {
                parse_float_as_decimal: false,
                enable_ident_normalization: false,
                support_varchar_with_length: false,
                enable_options_value_normalization: false,
                collect_spans: false,
                map_string_types_to_utf8view: false,
                default_null_ordering: NullOrdering::NullsMax,
            },
        );

        let mut planner_context = PlannerContext::default();
        let schema = DFSchema::try_from(self.schema.as_ref().clone())?;
        sql_to_rel
            .sql_to_expr(function, &schema, &mut planner_context)
            .map_err(|e| Error::invalid_input(format!("Error parsing function: {e}")))
    }

    fn parse_type(&self, data_type: &SQLDataType) -> Result<ArrowDataType> {
        const SUPPORTED_TYPES: [&str; 13] = [
            "int [unsigned]",
            "tinyint [unsigned]",
            "smallint [unsigned]",
            "bigint [unsigned]",
            "float",
            "double",
            "string",
            "binary",
            "date",
            "timestamp(precision)",
            "datetime(precision)",
            "decimal(precision,scale)",
            "boolean",
        ];
        match data_type {
            SQLDataType::String(_) => Ok(ArrowDataType::Utf8),
            SQLDataType::Binary(_) => Ok(ArrowDataType::Binary),
            SQLDataType::Float(_) => Ok(ArrowDataType::Float32),
            SQLDataType::Double(_) => Ok(ArrowDataType::Float64),
            SQLDataType::Boolean => Ok(ArrowDataType::Boolean),
            SQLDataType::TinyInt(_) => Ok(ArrowDataType::Int8),
            SQLDataType::SmallInt(_) => Ok(ArrowDataType::Int16),
            SQLDataType::Int(_) | SQLDataType::Integer(_) => Ok(ArrowDataType::Int32),
            SQLDataType::BigInt(_) => Ok(ArrowDataType::Int64),
            SQLDataType::TinyIntUnsigned(_) => Ok(ArrowDataType::UInt8),
            SQLDataType::SmallIntUnsigned(_) => Ok(ArrowDataType::UInt16),
            SQLDataType::IntUnsigned(_) | SQLDataType::IntegerUnsigned(_) => {
                Ok(ArrowDataType::UInt32)
            }
            SQLDataType::BigIntUnsigned(_) => Ok(ArrowDataType::UInt64),
            SQLDataType::Date => Ok(ArrowDataType::Date32),
            SQLDataType::Timestamp(resolution, tz) => {
                match tz {
                    TimezoneInfo::None => {}
                    _ => {
                        return Err(Error::invalid_input(
                            "Timezone not supported in timestamp".to_string(),
                        ));
                    }
                };
                let time_unit = match resolution {
                    // Default to microsecond to match PyArrow
                    None => TimeUnit::Microsecond,
                    Some(0) => TimeUnit::Second,
                    Some(3) => TimeUnit::Millisecond,
                    Some(6) => TimeUnit::Microsecond,
                    Some(9) => TimeUnit::Nanosecond,
                    _ => {
                        return Err(Error::invalid_input(format!(
                            "Unsupported datetime resolution: {:?}",
                            resolution
                        )));
                    }
                };
                Ok(ArrowDataType::Timestamp(time_unit, None))
            }
            SQLDataType::Datetime(resolution) => {
                let time_unit = match resolution {
                    None => TimeUnit::Microsecond,
                    Some(0) => TimeUnit::Second,
                    Some(3) => TimeUnit::Millisecond,
                    Some(6) => TimeUnit::Microsecond,
                    Some(9) => TimeUnit::Nanosecond,
                    _ => {
                        return Err(Error::invalid_input(format!(
                            "Unsupported datetime resolution: {:?}",
                            resolution
                        )));
                    }
                };
                Ok(ArrowDataType::Timestamp(time_unit, None))
            }
            SQLDataType::Decimal(number_info) => match number_info {
                ExactNumberInfo::PrecisionAndScale(precision, scale) => {
                    Ok(ArrowDataType::Decimal128(*precision as u8, *scale as i8))
                }
                _ => Err(Error::invalid_input(format!(
                    "Must provide precision and scale for decimal: {:?}",
                    number_info
                ))),
            },
            _ => Err(Error::invalid_input(format!(
                "Unsupported data type: {:?}. Supported types: {:?}",
                data_type, SUPPORTED_TYPES
            ))),
        }
    }

    fn plan_field_access(&self, mut field_access_expr: RawFieldAccessExpr) -> Result<Expr> {
        let df_schema = DFSchema::try_from(self.schema.as_ref().clone())?;
        for planner in self.context_provider.get_expr_planners() {
            match planner.plan_field_access(field_access_expr, &df_schema)? {
                PlannerResult::Planned(expr) => return Ok(expr),
                PlannerResult::Original(expr) => {
                    field_access_expr = expr;
                }
            }
        }
        Err(Error::invalid_input("Field access could not be planned"))
    }

    fn parse_sql_expr(&self, expr: &SQLExpr) -> Result<Expr> {
        match expr {
            SQLExpr::Identifier(id) => {
                // Users can pass string literals wrapped in `"`.
                // (Normally SQL only allows single quotes.)
                if id.quote_style == Some('"') {
                    Ok(Expr::Literal(
                        ScalarValue::Utf8(Some(id.value.clone())),
                        None,
                    ))
                // Users can wrap identifiers with ` to reference non-standard
                // names, such as uppercase or spaces.
                } else if id.quote_style == Some('`') {
                    Ok(Expr::Column(Column::from_name(id.value.clone())))
                } else {
                    Ok(self.column(vec![id.clone()].as_slice()))
                }
            }
            SQLExpr::CompoundIdentifier(ids) => Ok(self.column(ids.as_slice())),
            SQLExpr::BinaryOp { left, op, right } => self.binary_expr(left, op, right),
            SQLExpr::UnaryOp { op, expr } => self.unary_expr(op, expr),
            SQLExpr::Value(value) => self.value(&value.value),
            SQLExpr::Array(SQLArray { elem, .. }) => {
                let mut values = vec![];

                let array_literal_error = |pos: usize, value: &_| {
                    Err(Error::invalid_input(format!(
                        "Expected a literal value in array, instead got {} at position {}",
                        value, pos
                    )))
                };

                for (pos, expr) in elem.iter().enumerate() {
                    match expr {
                        SQLExpr::Value(value) => {
                            if let Expr::Literal(value, _) = self.value(&value.value)? {
                                values.push(value);
                            } else {
                                return array_literal_error(pos, expr);
                            }
                        }
                        SQLExpr::UnaryOp {
                            op: UnaryOperator::Minus,
                            expr,
                        } => {
                            if let SQLExpr::Value(ValueWithSpan {
                                value: Value::Number(number, _),
                                ..
                            }) = expr.as_ref()
                            {
                                if let Expr::Literal(value, _) = self.number(number, true)? {
                                    values.push(value);
                                } else {
                                    return array_literal_error(pos, expr);
                                }
                            } else {
                                return array_literal_error(pos, expr);
                            }
                        }
                        _ => {
                            return array_literal_error(pos, expr);
                        }
                    }
                }

                let field = if !values.is_empty() {
                    let data_type = values[0].data_type();

                    for value in &mut values {
                        if value.data_type() != data_type {
                            *value = safe_coerce_scalar(value, &data_type).ok_or_else(|| Error::invalid_input(format!("Array expressions must have a consistent datatype. Expected: {}, got: {}", data_type, value.data_type())))?;
                        }
                    }
                    Field::new("item", data_type, true)
                } else {
                    Field::new("item", ArrowDataType::Null, true)
                };

                let values = values
                    .into_iter()
                    .map(|v| v.to_array().map_err(Error::from))
                    .collect::<Result<Vec<_>>>()?;
                let array_refs = values.iter().map(|v| v.as_ref()).collect::<Vec<_>>();
                let values = concat(&array_refs)?;
                let values = ListArray::try_new(
                    field.into(),
                    OffsetBuffer::from_lengths([values.len()]),
                    values,
                    None,
                )?;

                Ok(Expr::Literal(ScalarValue::List(Arc::new(values)), None))
            }
            // JSONB literal: jsonb '{"key": "value"}'
            SQLExpr::TypedString(TypedString {
                data_type: SQLDataType::JSONB,
                value,
                ..
            }) => match &value.value {
                Value::SingleQuotedString(s) | Value::DoubleQuotedString(s) => encode_jsonb(s),
                _ => Err(Error::invalid_input(
                    "Expected a string value for JSONB literal",
                )),
            },
            // For example, DATE '2020-01-01'
            SQLExpr::TypedString(TypedString {
                data_type, value, ..
            }) => {
                let value = value.clone().into_string().expect_ok()?;
                Ok(Expr::Cast(datafusion::logical_expr::Cast::new(
                    Box::new(Expr::Literal(ScalarValue::Utf8(Some(value)), None)),
                    self.parse_type(data_type)?,
                )))
            }
            SQLExpr::IsFalse(expr) => Ok(Expr::IsFalse(Box::new(self.parse_sql_expr(expr)?))),
            SQLExpr::IsNotFalse(expr) => Ok(Expr::IsNotFalse(Box::new(self.parse_sql_expr(expr)?))),
            SQLExpr::IsTrue(expr) => Ok(Expr::IsTrue(Box::new(self.parse_sql_expr(expr)?))),
            SQLExpr::IsNotTrue(expr) => Ok(Expr::IsNotTrue(Box::new(self.parse_sql_expr(expr)?))),
            SQLExpr::IsNull(expr) => Ok(Expr::IsNull(Box::new(self.parse_sql_expr(expr)?))),
            SQLExpr::IsNotNull(expr) => Ok(Expr::IsNotNull(Box::new(self.parse_sql_expr(expr)?))),
            SQLExpr::InList {
                expr,
                list,
                negated,
            } => {
                let value_expr = self.parse_sql_expr(expr)?;
                let list_exprs = list
                    .iter()
                    .map(|e| self.parse_sql_expr(e))
                    .collect::<Result<Vec<_>>>()?;
                Ok(value_expr.in_list(list_exprs, *negated))
            }
            SQLExpr::Nested(inner) => self.parse_sql_expr(inner.as_ref()),
            SQLExpr::Function(_) => self.parse_function(expr.clone()),
            SQLExpr::ILike {
                negated,
                expr,
                pattern,
                escape_char,
                any: _,
            } => Ok(Expr::Like(Like::new(
                *negated,
                Box::new(self.parse_sql_expr(expr)?),
                Box::new(self.parse_sql_expr(pattern)?),
                parse_like_escape_char(escape_char)?,
                true,
            ))),
            SQLExpr::Like {
                negated,
                expr,
                pattern,
                escape_char,
                any: _,
            } => Ok(Expr::Like(Like::new(
                *negated,
                Box::new(self.parse_sql_expr(expr)?),
                Box::new(self.parse_sql_expr(pattern)?),
                parse_like_escape_char(escape_char)?,
                false,
            ))),
            // JSONB cast: CAST('...' AS JSONB) or '...'::jsonb
            SQLExpr::Cast {
                data_type: SQLDataType::JSONB,
                expr: inner,
                ..
            } => match inner.as_ref() {
                SQLExpr::Value(ValueWithSpan {
                    value: Value::SingleQuotedString(s) | Value::DoubleQuotedString(s),
                    ..
                }) => encode_jsonb(s),
                _ => Err(Error::invalid_input(
                    "CAST to JSONB only supports string literals",
                )),
            },
            SQLExpr::Cast {
                expr,
                data_type,
                kind,
                ..
            } => match kind {
                datafusion::sql::sqlparser::ast::CastKind::TryCast
                | datafusion::sql::sqlparser::ast::CastKind::SafeCast => {
                    Ok(Expr::TryCast(datafusion::logical_expr::TryCast::new(
                        Box::new(self.parse_sql_expr(expr)?),
                        self.parse_type(data_type)?,
                    )))
                }
                _ => Ok(Expr::Cast(datafusion::logical_expr::Cast::new(
                    Box::new(self.parse_sql_expr(expr)?),
                    self.parse_type(data_type)?,
                ))),
            },
            SQLExpr::JsonAccess { .. } => Err(Error::invalid_input("JSON access is not supported")),
            SQLExpr::CompoundFieldAccess { root, access_chain } => {
                let mut expr = self.parse_sql_expr(root)?;

                for access in access_chain {
                    let field_access = match access {
                        // x.y or x['y']
                        AccessExpr::Dot(SQLExpr::Identifier(Ident { value: s, .. }))
                        | AccessExpr::Subscript(Subscript::Index {
                            index:
                                SQLExpr::Value(ValueWithSpan {
                                    value:
                                        Value::SingleQuotedString(s) | Value::DoubleQuotedString(s),
                                    ..
                                }),
                        }) => GetFieldAccess::NamedStructField {
                            name: ScalarValue::from(s.as_str()),
                        },
                        AccessExpr::Subscript(Subscript::Index { index }) => {
                            let key = Box::new(self.parse_sql_expr(index)?);
                            GetFieldAccess::ListIndex { key }
                        }
                        AccessExpr::Subscript(Subscript::Slice { .. }) => {
                            return Err(Error::invalid_input("Slice subscript is not supported"));
                        }
                        _ => {
                            // Handle other cases like JSON access
                            // Note: JSON access is not supported in lance
                            return Err(Error::invalid_input(
                                "Only dot notation or index access is supported for field access",
                            ));
                        }
                    };

                    let field_access_expr = RawFieldAccessExpr { expr, field_access };
                    expr = self.plan_field_access(field_access_expr)?;
                }

                Ok(expr)
            }
            SQLExpr::Between {
                expr,
                negated,
                low,
                high,
            } => {
                // Parse the main expression and bounds
                let expr = self.parse_sql_expr(expr)?;
                let low = self.parse_sql_expr(low)?;
                let high = self.parse_sql_expr(high)?;

                let between = Expr::Between(Between::new(
                    Box::new(expr),
                    *negated,
                    Box::new(low),
                    Box::new(high),
                ));
                Ok(between)
            }
            _ => Err(Error::invalid_input(format!(
                "Expression '{expr}' is not supported SQL in lance"
            ))),
        }
    }

    /// Create Logical [Expr] from a SQL filter clause.
    ///
    /// Note: the returned expression must be passed through `optimize_expr()`
    /// before being passed to `create_physical_expr()`.
    pub fn parse_filter(&self, filter: &str) -> Result<Expr> {
        // Allow sqlparser to parse filter as part of ONE SQL statement.
        let ast_expr = parse_sql_filter(filter)?;
        let expr = self.parse_sql_expr(&ast_expr)?;
        let schema = Schema::try_from(self.schema.as_ref())?;
        let resolved = resolve_expr(&expr, &schema).map_err(|e| {
            Error::invalid_input(format!("Error resolving filter expression {filter}: {e}"))
        })?;

        Ok(coerce_filter_type_to_boolean(resolved))
    }

    /// Create Logical [Expr] from a SQL expression.
    ///
    /// Note: the returned expression must be passed through `optimize_filter()`
    /// before being passed to `create_physical_expr()`.
    pub fn parse_expr(&self, expr: &str) -> Result<Expr> {
        // First check if it's a simple column reference (no operators, functions, etc.)
        // resolve_column_name tries exact match first, then falls back to case-insensitive
        let resolved_name = self.resolve_column_name(expr);
        if self.schema.field_with_name(&resolved_name).is_ok() {
            return Ok(Expr::Column(Column::from_name(resolved_name)));
        }

        // Parse as SQL expression
        let ast_expr = parse_sql_expr(expr)?;
        let expr = self.parse_sql_expr(&ast_expr)?;
        let schema = Schema::try_from(self.schema.as_ref())?;
        let resolved = resolve_expr(&expr, &schema)?;
        Ok(resolved)
    }

    /// Try to decode bytes from hex literal string.
    ///
    /// Copied from datafusion because this is not public.
    ///
    /// TODO: use SqlToRel from Datafusion directly?
    fn try_decode_hex_literal(s: &str) -> Option<Vec<u8>> {
        let hex_bytes = s.as_bytes();
        let mut decoded_bytes = Vec::with_capacity(hex_bytes.len().div_ceil(2));

        let start_idx = hex_bytes.len() % 2;
        if start_idx > 0 {
            // The first byte is formed of only one char.
            decoded_bytes.push(Self::try_decode_hex_char(hex_bytes[0])?);
        }

        for i in (start_idx..hex_bytes.len()).step_by(2) {
            let high = Self::try_decode_hex_char(hex_bytes[i])?;
            let low = Self::try_decode_hex_char(hex_bytes[i + 1])?;
            decoded_bytes.push((high << 4) | low);
        }

        Some(decoded_bytes)
    }

    /// Try to decode a byte from a hex char.
    ///
    /// None will be returned if the input char is hex-invalid.
    const fn try_decode_hex_char(c: u8) -> Option<u8> {
        match c {
            b'A'..=b'F' => Some(c - b'A' + 10),
            b'a'..=b'f' => Some(c - b'a' + 10),
            b'0'..=b'9' => Some(c - b'0'),
            _ => None,
        }
    }

    /// Optimize the filter expression and coerce data types.
    pub fn optimize_expr(&self, expr: Expr) -> Result<Expr> {
        let df_schema = Arc::new(DFSchema::try_from(self.schema.as_ref().clone())?);

        // DataFusion needs the coerce and simplify passes to be applied before
        // expressions can be handled by the physical planner.
        let simplify_context = SimplifyContext::builder()
            .with_schema(df_schema.clone())
            .with_query_execution_start_time(Some(Utc::now()))
            .build();
        let simplifier =
            datafusion::optimizer::simplify_expressions::ExprSimplifier::new(simplify_context);

        // Coerce before simplify to match DataFusion's analyzer-before-optimizer pipeline.
        let expr = simplifier.coerce(expr, &df_schema)?;
        let expr = simplifier.simplify(expr)?;

        Ok(expr)
    }

    /// Create the [`PhysicalExpr`] from a logical [`Expr`]
    pub fn create_physical_expr(&self, expr: &Expr) -> Result<Arc<dyn PhysicalExpr>> {
        let df_schema = Arc::new(DFSchema::try_from(self.schema.as_ref().clone())?);
        Ok(datafusion::physical_expr::create_physical_expr(
            expr,
            df_schema.as_ref(),
            &Default::default(),
        )?)
    }

    /// Collect the columns in the expression.
    ///
    /// The columns are returned in sorted order.
    ///
    /// If the expr refers to nested columns these will be returned
    /// as dotted paths (x.y.z)
    pub fn column_names_in_expr(expr: &Expr) -> Vec<String> {
        let mut visitor = ColumnCapturingVisitor {
            current_path: VecDeque::new(),
            columns: BTreeSet::new(),
        };
        expr.visit(&mut visitor).unwrap();
        visitor.columns.into_iter().collect()
    }
}

struct ColumnCapturingVisitor {
    // Current column path. If this is empty, we are not in a column expression.
    current_path: VecDeque<String>,
    columns: BTreeSet<String>,
}

impl TreeNodeVisitor<'_> for ColumnCapturingVisitor {
    type Node = Expr;

    fn f_down(&mut self, node: &Self::Node) -> DFResult<TreeNodeRecursion> {
        match node {
            Expr::Column(Column { name, .. }) => {
                // Build the field path from the column name and any nested fields
                // The nested field names from get_field already come as literal strings,
                // so we just need to concatenate them properly
                let mut path = name.clone();
                for part in self.current_path.drain(..) {
                    path.push('.');
                    // Check if the part needs quoting (contains dots)
                    if part.contains('.') || part.contains('`') {
                        // Quote the field name with backticks and escape any existing backticks
                        let escaped = part.replace('`', "``");
                        path.push('`');
                        path.push_str(&escaped);
                        path.push('`');
                    } else {
                        path.push_str(&part);
                    }
                }
                self.columns.insert(path);
                self.current_path.clear();
            }
            Expr::ScalarFunction(udf) if udf.name() == GetFieldFunc::default().name() => {
                if let Some(name) = get_as_string_scalar_opt(&udf.args[1]) {
                    self.current_path.push_front(name.to_string())
                } else {
                    self.current_path.clear();
                }
            }
            _ => {
                self.current_path.clear();
            }
        }

        Ok(TreeNodeRecursion::Continue)
    }
}

#[cfg(test)]
mod tests {

    use crate::logical_expr::ExprExt;

    use super::*;

    use arrow::datatypes::Float64Type;
    use arrow_array::{
        ArrayRef, BooleanArray, Float32Array, Int32Array, Int64Array, RecordBatch, StringArray,
        StructArray, TimestampMicrosecondArray, TimestampMillisecondArray,
        TimestampNanosecondArray, TimestampSecondArray,
    };
    use arrow_schema::{DataType, Fields, Schema};
    use datafusion::{
        logical_expr::{Cast, col, lit},
        prelude::{array_element, get_field},
    };
    use datafusion_functions::core::expr_ext::FieldAccessor;

    #[test]
    fn test_parse_filter_simple() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("i", DataType::Int32, false),
            Field::new("s", DataType::Utf8, true),
            Field::new(
                "st",
                DataType::Struct(Fields::from(vec![
                    Field::new("x", DataType::Float32, false),
                    Field::new("y", DataType::Float32, false),
                ])),
                true,
            ),
        ]));

        let planner = Planner::new(schema.clone());

        let expected = col("i")
            .gt(lit(3_i32))
            .and(col("st").field_newstyle("x").lt_eq(lit(5.0_f32)))
            .and(
                col("s")
                    .eq(lit("str-4"))
                    .or(col("s").in_list(vec![lit("str-4"), lit("str-5")], false)),
            );

        // double quotes
        let expr = planner
            .parse_filter("i > 3 AND st.x <= 5.0 AND (s == 'str-4' OR s in ('str-4', 'str-5'))")
            .unwrap();
        assert_eq!(expr, expected);

        // single quote
        let expr = planner
            .parse_filter("i > 3 AND st.x <= 5.0 AND (s = 'str-4' OR s in ('str-4', 'str-5'))")
            .unwrap();

        let physical_expr = planner.create_physical_expr(&expr).unwrap();

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from_iter_values(0..10)) as ArrayRef,
                Arc::new(StringArray::from_iter_values(
                    (0..10).map(|v| format!("str-{}", v)),
                )),
                Arc::new(StructArray::from(vec![
                    (
                        Arc::new(Field::new("x", DataType::Float32, false)),
                        Arc::new(Float32Array::from_iter_values((0..10).map(|v| v as f32)))
                            as ArrayRef,
                    ),
                    (
                        Arc::new(Field::new("y", DataType::Float32, false)),
                        Arc::new(Float32Array::from_iter_values(
                            (0..10).map(|v| (v * 10) as f32),
                        )),
                    ),
                ])),
            ],
        )
        .unwrap();
        let predicates = physical_expr.evaluate(&batch).unwrap();
        assert_eq!(
            predicates.into_array(0).unwrap().as_ref(),
            &BooleanArray::from(vec![
                false, false, false, false, true, true, false, false, false, false
            ])
        );
    }

    #[test]
    fn test_parse_filter_uint64_literal_above_i64_max() {
        let value = u64::MAX - 1;
        let batch = arrow_array::record_batch!(("id", UInt64, [1, value])).unwrap();
        let planner = Planner::new(batch.schema());

        let expr = planner.parse_filter(&format!("id = {value}")).unwrap();
        assert_eq!(expr, col("id").eq(lit(value)));

        let physical_expr = planner.create_physical_expr(&expr).unwrap();
        let predicates = physical_expr.evaluate(&batch).unwrap();
        assert_eq!(
            predicates.into_array(0).unwrap().as_ref(),
            &BooleanArray::from(vec![false, true])
        );
    }

    #[test]
    fn test_parse_deep_logical_filter() {
        let planner = Planner::new(Arc::new(Schema::empty()));

        for op in ["AND", "OR"] {
            let filter = std::iter::repeat_n("true", 1000)
                .collect::<Vec<_>>()
                .join(&format!(" {op} "));

            let expr = planner.parse_filter(&filter).unwrap();
            let optimized = planner.optimize_expr(expr).unwrap();

            assert_eq!(optimized, lit(true));
        }
    }

    #[derive(Debug, Eq, PartialEq, Hash)]
    struct StrictFloat64Udf {
        signature: Signature,
    }

    impl StrictFloat64Udf {
        fn new() -> Self {
            Self {
                signature: Signature::exact(vec![DataType::Float64], Volatility::Immutable),
            }
        }
    }

    impl ScalarUDFImpl for StrictFloat64Udf {
        fn name(&self) -> &str {
            "strict_float64"
        }

        fn signature(&self) -> &Signature {
            &self.signature
        }

        fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
            Ok(DataType::Float64)
        }

        fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
            let data_type = args.args[0].data_type();
            assert_eq!(
                data_type,
                DataType::Float64,
                "strict_float64 expected Float64, got {data_type}"
            );
            Ok(ColumnarValue::Scalar(ScalarValue::Float64(Some(0.0))))
        }
    }

    #[test]
    fn test_coerce_before_simplify() {
        let planner = Planner::new(Arc::new(Schema::empty()));
        let strict_float64 = Arc::new(ScalarUDF::new_from_impl(StrictFloat64Udf::new()));
        let expr = Expr::ScalarFunction(ScalarFunction::new_udf(strict_float64, vec![lit(0_i64)]))
            .eq(lit(0.0_f64));

        let optimized = planner.optimize_expr(expr).unwrap();

        planner.create_physical_expr(&optimized).unwrap();
    }

    #[test]
    fn test_nested_col_refs() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("s0", DataType::Utf8, true),
            Field::new(
                "st",
                DataType::Struct(Fields::from(vec![
                    Field::new("s1", DataType::Utf8, true),
                    Field::new(
                        "st",
                        DataType::Struct(Fields::from(vec![Field::new(
                            "s2",
                            DataType::Utf8,
                            true,
                        )])),
                        true,
                    ),
                ])),
                true,
            ),
        ]));

        let planner = Planner::new(schema);

        fn assert_column_eq(planner: &Planner, expr: &str, expected: &Expr) {
            let expr = planner.parse_filter(&format!("{expr} = 'val'")).unwrap();
            assert!(matches!(
                expr,
                Expr::BinaryExpr(BinaryExpr {
                    left: _,
                    op: Operator::Eq,
                    right: _
                })
            ));
            if let Expr::BinaryExpr(BinaryExpr { left, .. }) = expr {
                assert_eq!(left.as_ref(), expected);
            }
        }

        let expected = Expr::Column(Column::new_unqualified("s0"));
        assert_column_eq(&planner, "s0", &expected);
        assert_column_eq(&planner, "`s0`", &expected);

        let expected = Expr::ScalarFunction(ScalarFunction {
            func: Arc::new(ScalarUDF::new_from_impl(GetFieldFunc::default())),
            args: vec![
                Expr::Column(Column::new_unqualified("st")),
                Expr::Literal(ScalarValue::Utf8(Some("s1".to_string())), None),
            ],
        });
        assert_column_eq(&planner, "st.s1", &expected);
        assert_column_eq(&planner, "`st`.`s1`", &expected);
        assert_column_eq(&planner, "st.`s1`", &expected);

        let expected = Expr::ScalarFunction(ScalarFunction {
            func: Arc::new(ScalarUDF::new_from_impl(GetFieldFunc::default())),
            args: vec![
                Expr::ScalarFunction(ScalarFunction {
                    func: Arc::new(ScalarUDF::new_from_impl(GetFieldFunc::default())),
                    args: vec![
                        Expr::Column(Column::new_unqualified("st")),
                        Expr::Literal(ScalarValue::Utf8(Some("st".to_string())), None),
                    ],
                }),
                Expr::Literal(ScalarValue::Utf8(Some("s2".to_string())), None),
            ],
        });

        assert_column_eq(&planner, "st.st.s2", &expected);
        assert_column_eq(&planner, "`st`.`st`.`s2`", &expected);
        assert_column_eq(&planner, "st.st.`s2`", &expected);
        assert_column_eq(&planner, "st['st'][\"s2\"]", &expected);
    }

    #[test]
    fn test_nested_list_refs() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "l",
            DataType::List(Arc::new(Field::new(
                "item",
                DataType::Struct(Fields::from(vec![Field::new("f1", DataType::Utf8, true)])),
                true,
            ))),
            true,
        )]));

        let planner = Planner::new(schema);

        let expected = array_element(col("l"), lit(0_i64));
        let expr = planner.parse_expr("l[0]").unwrap();
        assert_eq!(expr, expected);

        let expected = get_field(array_element(col("l"), lit(0_i64)), "f1");
        let expr = planner.parse_expr("l[0]['f1']").unwrap();
        assert_eq!(expr, expected);

        // FIXME: This should work, but sqlparser doesn't recognize anything
        // after the period for some reason.
        // let expr = planner.parse_expr("l[0].f1").unwrap();
        // assert_eq!(expr, expected);
    }

    #[test]
    fn test_negative_expressions() {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));

        let planner = Planner::new(schema.clone());

        let expected = col("x")
            .gt(lit(-3_i64))
            .and(col("x").lt(-(lit(-5_i64) + lit(3_i64))));

        let expr = planner.parse_filter("x > -3 AND x < -(-5 + 3)").unwrap();

        assert_eq!(expr, expected);

        let physical_expr = planner.create_physical_expr(&expr).unwrap();

        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int64Array::from_iter_values(-5..5)) as ArrayRef],
        )
        .unwrap();
        let predicates = physical_expr.evaluate(&batch).unwrap();
        assert_eq!(
            predicates.into_array(0).unwrap().as_ref(),
            &BooleanArray::from(vec![
                false, false, false, true, true, true, true, false, false, false
            ])
        );
    }

    #[test]
    fn test_negative_array_expressions() {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));

        let planner = Planner::new(schema);

        let expected = Expr::Literal(
            ScalarValue::List(Arc::new(
                ListArray::from_iter_primitive::<Float64Type, _, _>(vec![Some(
                    [-1_f64, -2.0, -3.0, -4.0, -5.0].map(Some),
                )]),
            )),
            None,
        );

        let expr = planner
            .parse_expr("[-1.0, -2.0, -3.0, -4.0, -5.0]")
            .unwrap();

        assert_eq!(expr, expected);
    }

    #[test]
    fn test_sql_like() {
        let schema = Arc::new(Schema::new(vec![Field::new("s", DataType::Utf8, true)]));

        let planner = Planner::new(schema.clone());

        let expected = col("s").like(lit("str-4"));
        // single quote
        let expr = planner.parse_filter("s LIKE 'str-4'").unwrap();
        assert_eq!(expr, expected);
        let physical_expr = planner.create_physical_expr(&expr).unwrap();

        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(StringArray::from_iter_values(
                (0..10).map(|v| format!("str-{}", v)),
            ))],
        )
        .unwrap();
        let predicates = physical_expr.evaluate(&batch).unwrap();
        assert_eq!(
            predicates.into_array(0).unwrap().as_ref(),
            &BooleanArray::from(vec![
                false, false, false, false, true, false, false, false, false, false
            ])
        );
    }

    #[test]
    fn test_not_like() {
        let schema = Arc::new(Schema::new(vec![Field::new("s", DataType::Utf8, true)]));

        let planner = Planner::new(schema.clone());

        let expected = col("s").not_like(lit("str-4"));
        // single quote
        let expr = planner.parse_filter("s NOT LIKE 'str-4'").unwrap();
        assert_eq!(expr, expected);
        let physical_expr = planner.create_physical_expr(&expr).unwrap();

        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(StringArray::from_iter_values(
                (0..10).map(|v| format!("str-{}", v)),
            ))],
        )
        .unwrap();
        let predicates = physical_expr.evaluate(&batch).unwrap();
        assert_eq!(
            predicates.into_array(0).unwrap().as_ref(),
            &BooleanArray::from(vec![
                true, true, true, true, false, true, true, true, true, true
            ])
        );
    }

    #[test]
    fn test_like_escape_char() {
        let schema = Arc::new(Schema::new(vec![Field::new("s", DataType::Utf8, true)]));
        let planner = Planner::new(schema);

        // A valid single-character escape is captured for both LIKE and ILIKE.
        for filter in ["s LIKE 'a!%' ESCAPE '!'", "s ILIKE 'a!%' ESCAPE '!'"] {
            match planner.parse_filter(filter).unwrap() {
                Expr::Like(like) => assert_eq!(like.escape_char, Some('!'), "{filter}"),
                other => panic!("expected a LIKE expression for `{filter}`, got {other:?}"),
            }
        }

        // Empty and multi-character escapes are rejected rather than silently
        // dropped or truncated to the first character.
        for filter in [
            "s LIKE 'x' ESCAPE ''",
            "s LIKE 'x' ESCAPE 'ab'",
            "s ILIKE 'x' ESCAPE ''",
            "s ILIKE 'x' ESCAPE 'ab'",
        ] {
            let err = planner.parse_filter(filter).unwrap_err();
            assert!(
                err.to_string()
                    .contains("Invalid escape character in LIKE expression"),
                "unexpected error for `{filter}`: {err}"
            );
        }
    }

    #[test]
    fn test_sql_is_in() {
        let schema = Arc::new(Schema::new(vec![Field::new("s", DataType::Utf8, true)]));

        let planner = Planner::new(schema.clone());

        let expected = col("s").in_list(vec![lit("str-4"), lit("str-5")], false);
        // single quote
        let expr = planner.parse_filter("s IN ('str-4', 'str-5')").unwrap();
        assert_eq!(expr, expected);
        let physical_expr = planner.create_physical_expr(&expr).unwrap();

        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(StringArray::from_iter_values(
                (0..10).map(|v| format!("str-{}", v)),
            ))],
        )
        .unwrap();
        let predicates = physical_expr.evaluate(&batch).unwrap();
        assert_eq!(
            predicates.into_array(0).unwrap().as_ref(),
            &BooleanArray::from(vec![
                false, false, false, false, true, true, false, false, false, false
            ])
        );
    }

    #[test]
    fn test_sql_is_null() {
        let schema = Arc::new(Schema::new(vec![Field::new("s", DataType::Utf8, true)]));

        let planner = Planner::new(schema.clone());

        let expected = col("s").is_null();
        let expr = planner.parse_filter("s IS NULL").unwrap();
        assert_eq!(expr, expected);
        let physical_expr = planner.create_physical_expr(&expr).unwrap();

        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(StringArray::from_iter((0..10).map(|v| {
                if v % 3 == 0 {
                    Some(format!("str-{}", v))
                } else {
                    None
                }
            })))],
        )
        .unwrap();
        let predicates = physical_expr.evaluate(&batch).unwrap();
        assert_eq!(
            predicates.into_array(0).unwrap().as_ref(),
            &BooleanArray::from(vec![
                false, true, true, false, true, true, false, true, true, false
            ])
        );

        let expr = planner.parse_filter("s IS NOT NULL").unwrap();
        let physical_expr = planner.create_physical_expr(&expr).unwrap();
        let predicates = physical_expr.evaluate(&batch).unwrap();
        assert_eq!(
            predicates.into_array(0).unwrap().as_ref(),
            &BooleanArray::from(vec![
                true, false, false, true, false, false, true, false, false, true,
            ])
        );
    }

    #[test]
    fn test_sql_invert() {
        let schema = Arc::new(Schema::new(vec![Field::new("s", DataType::Boolean, true)]));

        let planner = Planner::new(schema.clone());

        let expr = planner.parse_filter("NOT s").unwrap();
        let physical_expr = planner.create_physical_expr(&expr).unwrap();

        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(BooleanArray::from_iter(
                (0..10).map(|v| Some(v % 3 == 0)),
            ))],
        )
        .unwrap();
        let predicates = physical_expr.evaluate(&batch).unwrap();
        assert_eq!(
            predicates.into_array(0).unwrap().as_ref(),
            &BooleanArray::from(vec![
                false, true, true, false, true, true, false, true, true, false
            ])
        );
    }

    #[test]
    fn test_sql_cast() {
        let cases = &[
            (
                "x = cast('2021-01-01 00:00:00' as timestamp)",
                ArrowDataType::Timestamp(TimeUnit::Microsecond, None),
            ),
            (
                "x = cast('2021-01-01 00:00:00' as timestamp(0))",
                ArrowDataType::Timestamp(TimeUnit::Second, None),
            ),
            (
                "x = cast('2021-01-01 00:00:00.123' as timestamp(9))",
                ArrowDataType::Timestamp(TimeUnit::Nanosecond, None),
            ),
            (
                "x = cast('2021-01-01 00:00:00.123' as datetime(9))",
                ArrowDataType::Timestamp(TimeUnit::Nanosecond, None),
            ),
            ("x = cast('2021-01-01' as date)", ArrowDataType::Date32),
            (
                "x = cast('1.238' as decimal(9,3))",
                ArrowDataType::Decimal128(9, 3),
            ),
            ("x = cast(1 as float)", ArrowDataType::Float32),
            ("x = cast(1 as double)", ArrowDataType::Float64),
            ("x = cast(1 as tinyint)", ArrowDataType::Int8),
            ("x = cast(1 as smallint)", ArrowDataType::Int16),
            ("x = cast(1 as int)", ArrowDataType::Int32),
            ("x = cast(1 as integer)", ArrowDataType::Int32),
            ("x = cast(1 as bigint)", ArrowDataType::Int64),
            ("x = cast(1 as tinyint unsigned)", ArrowDataType::UInt8),
            ("x = cast(1 as smallint unsigned)", ArrowDataType::UInt16),
            ("x = cast(1 as int unsigned)", ArrowDataType::UInt32),
            ("x = cast(1 as integer unsigned)", ArrowDataType::UInt32),
            ("x = cast(1 as bigint unsigned)", ArrowDataType::UInt64),
            ("x = cast(1 as boolean)", ArrowDataType::Boolean),
            ("x = cast(1 as string)", ArrowDataType::Utf8),
        ];

        for (sql, expected_data_type) in cases {
            let schema = Arc::new(Schema::new(vec![Field::new(
                "x",
                expected_data_type.clone(),
                true,
            )]));
            let planner = Planner::new(schema.clone());
            let expr = planner.parse_filter(sql).unwrap();

            // Get the thing after 'cast(` but before ' as'.
            let expected_value_str = sql
                .split("cast(")
                .nth(1)
                .unwrap()
                .split(" as")
                .next()
                .unwrap();
            // Remove any quote marks
            let expected_value_str = expected_value_str.trim_matches('\'');

            match expr {
                Expr::BinaryExpr(BinaryExpr { right, .. }) => match right.as_ref() {
                    Expr::Cast(Cast { expr, field }) => {
                        match expr.as_ref() {
                            Expr::Literal(ScalarValue::Utf8(Some(value_str)), _) => {
                                assert_eq!(value_str, expected_value_str);
                            }
                            Expr::Literal(ScalarValue::Int64(Some(value)), _) => {
                                assert_eq!(*value, 1);
                            }
                            _ => panic!("Expected cast to be applied to literal"),
                        }
                        assert_eq!(field.data_type(), expected_data_type);
                    }
                    _ => panic!("Expected right to be a cast"),
                },
                _ => panic!("Expected binary expression"),
            }
        }
    }

    #[test]
    fn test_sql_literals() {
        let cases = &[
            (
                "x = timestamp '2021-01-01 00:00:00'",
                ArrowDataType::Timestamp(TimeUnit::Microsecond, None),
            ),
            (
                "x = timestamp(0) '2021-01-01 00:00:00'",
                ArrowDataType::Timestamp(TimeUnit::Second, None),
            ),
            (
                "x = timestamp(9) '2021-01-01 00:00:00.123'",
                ArrowDataType::Timestamp(TimeUnit::Nanosecond, None),
            ),
            ("x = date '2021-01-01'", ArrowDataType::Date32),
            ("x = decimal(9,3) '1.238'", ArrowDataType::Decimal128(9, 3)),
        ];

        for (sql, expected_data_type) in cases {
            let schema = Arc::new(Schema::new(vec![Field::new(
                "x",
                expected_data_type.clone(),
                true,
            )]));
            let planner = Planner::new(schema.clone());
            let expr = planner.parse_filter(sql).unwrap();

            let expected_value_str = sql.split('\'').nth(1).unwrap();

            match expr {
                Expr::BinaryExpr(BinaryExpr { right, .. }) => match right.as_ref() {
                    Expr::Cast(Cast { expr, field }) => {
                        match expr.as_ref() {
                            Expr::Literal(ScalarValue::Utf8(Some(value_str)), _) => {
                                assert_eq!(value_str, expected_value_str);
                            }
                            _ => panic!("Expected cast to be applied to literal"),
                        }
                        assert_eq!(field.data_type(), expected_data_type);
                    }
                    _ => panic!("Expected right to be a cast"),
                },
                _ => panic!("Expected binary expression"),
            }
        }
    }

    #[test]
    fn test_sql_array_literals() {
        let cases = [
            (
                "x = [1, 2, 3]",
                ArrowDataType::List(Arc::new(Field::new("item", ArrowDataType::Int64, true))),
            ),
            (
                "x = [1, 2, 3]",
                ArrowDataType::FixedSizeList(
                    Arc::new(Field::new("item", ArrowDataType::Int64, true)),
                    3,
                ),
            ),
        ];

        for (sql, expected_data_type) in cases {
            let schema = Arc::new(Schema::new(vec![Field::new(
                "x",
                expected_data_type.clone(),
                true,
            )]));
            let planner = Planner::new(schema.clone());
            let expr = planner.parse_filter(sql).unwrap();
            let expr = planner.optimize_expr(expr).unwrap();

            match expr {
                Expr::BinaryExpr(BinaryExpr { right, .. }) => match right.as_ref() {
                    Expr::Literal(value, _) => {
                        assert_eq!(&value.data_type(), &expected_data_type);
                    }
                    _ => panic!("Expected right to be a literal"),
                },
                _ => panic!("Expected binary expression"),
            }
        }
    }

    #[test]
    fn test_sql_between() {
        use arrow_array::{Float64Array, Int32Array, TimestampMicrosecondArray};
        use arrow_schema::{DataType, Field, Schema, TimeUnit};
        use std::sync::Arc;

        let schema = Arc::new(Schema::new(vec![
            Field::new("x", DataType::Int32, false),
            Field::new("y", DataType::Float64, false),
            Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                false,
            ),
        ]));

        let planner = Planner::new(schema.clone());

        // Test integer BETWEEN
        let expr = planner
            .parse_filter("x BETWEEN CAST(3 AS INT) AND CAST(7 AS INT)")
            .unwrap();
        let physical_expr = planner.create_physical_expr(&expr).unwrap();

        // Create timestamp array with values representing:
        // 2024-01-01 00:00:00 to 2024-01-01 00:00:09 (in microseconds)
        let base_ts = 1704067200000000_i64; // 2024-01-01 00:00:00
        let ts_array = TimestampMicrosecondArray::from_iter_values(
            (0..10).map(|i| base_ts + i * 1_000_000), // Each value is 1 second apart
        );

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from_iter_values(0..10)) as ArrayRef,
                Arc::new(Float64Array::from_iter_values((0..10).map(|v| v as f64))),
                Arc::new(ts_array),
            ],
        )
        .unwrap();

        let predicates = physical_expr.evaluate(&batch).unwrap();
        assert_eq!(
            predicates.into_array(0).unwrap().as_ref(),
            &BooleanArray::from(vec![
                false, false, false, true, true, true, true, true, false, false
            ])
        );

        // Test NOT BETWEEN
        let expr = planner
            .parse_filter("x NOT BETWEEN CAST(3 AS INT) AND CAST(7 AS INT)")
            .unwrap();
        let physical_expr = planner.create_physical_expr(&expr).unwrap();

        let predicates = physical_expr.evaluate(&batch).unwrap();
        assert_eq!(
            predicates.into_array(0).unwrap().as_ref(),
            &BooleanArray::from(vec![
                true, true, true, false, false, false, false, false, true, true
            ])
        );

        // Test floating point BETWEEN
        let expr = planner.parse_filter("y BETWEEN 2.5 AND 6.5").unwrap();
        let physical_expr = planner.create_physical_expr(&expr).unwrap();

        let predicates = physical_expr.evaluate(&batch).unwrap();
        assert_eq!(
            predicates.into_array(0).unwrap().as_ref(),
            &BooleanArray::from(vec![
                false, false, false, true, true, true, true, false, false, false
            ])
        );

        // Test timestamp BETWEEN
        let expr = planner
            .parse_filter(
                "ts BETWEEN timestamp '2024-01-01 00:00:03' AND timestamp '2024-01-01 00:00:07'",
            )
            .unwrap();
        let physical_expr = planner.create_physical_expr(&expr).unwrap();

        let predicates = physical_expr.evaluate(&batch).unwrap();
        assert_eq!(
            predicates.into_array(0).unwrap().as_ref(),
            &BooleanArray::from(vec![
                false, false, false, true, true, true, true, true, false, false
            ])
        );
    }

    #[test]
    fn test_sql_comparison() {
        // Create a batch with all data types
        let batch: Vec<(&str, ArrayRef)> = vec![
            (
                "timestamp_s",
                Arc::new(TimestampSecondArray::from_iter_values(0..10)),
            ),
            (
                "timestamp_ms",
                Arc::new(TimestampMillisecondArray::from_iter_values(0..10)),
            ),
            (
                "timestamp_us",
                Arc::new(TimestampMicrosecondArray::from_iter_values(0..10)),
            ),
            (
                "timestamp_ns",
                Arc::new(TimestampNanosecondArray::from_iter_values(4995..5005)),
            ),
        ];
        let batch = RecordBatch::try_from_iter(batch).unwrap();

        let planner = Planner::new(batch.schema());

        // Each expression is meant to select the final 5 rows
        let expressions = &[
            "timestamp_s >= TIMESTAMP '1970-01-01 00:00:05'",
            "timestamp_ms >= TIMESTAMP '1970-01-01 00:00:00.005'",
            "timestamp_us >= TIMESTAMP '1970-01-01 00:00:00.000005'",
            "timestamp_ns >= TIMESTAMP '1970-01-01 00:00:00.000005'",
        ];

        let expected: ArrayRef = Arc::new(BooleanArray::from_iter(
            std::iter::repeat_n(Some(false), 5).chain(std::iter::repeat_n(Some(true), 5)),
        ));
        for expression in expressions {
            // convert to physical expression
            let logical_expr = planner.parse_filter(expression).unwrap();
            let logical_expr = planner.optimize_expr(logical_expr).unwrap();
            let physical_expr = planner.create_physical_expr(&logical_expr).unwrap();

            // Evaluate and assert they have correct results
            let result = physical_expr.evaluate(&batch).unwrap();
            let result = result.into_array(batch.num_rows()).unwrap();
            assert_eq!(&expected, &result, "unexpected result for {}", expression);
        }
    }

    #[test]
    fn test_columns_in_expr() {
        let expr = col("s0").gt(lit("value")).and(
            col("st")
                .field("st")
                .field("s2")
                .eq(lit("value"))
                .or(col("st")
                    .field("s1")
                    .in_list(vec![lit("value 1"), lit("value 2")], false)),
        );

        let columns = Planner::column_names_in_expr(&expr);
        assert_eq!(columns, vec!["s0", "st.s1", "st.st.s2"]);
    }

    #[test]
    fn test_parse_binary_expr() {
        let bin_str = "x'616263'";

        let schema = Arc::new(Schema::new(vec![Field::new(
            "binary",
            DataType::Binary,
            true,
        )]));
        let planner = Planner::new(schema);
        let expr = planner.parse_expr(bin_str).unwrap();
        assert_eq!(
            expr,
            Expr::Literal(ScalarValue::Binary(Some(vec![b'a', b'b', b'c'])), None)
        );
    }

    #[test]
    fn test_lance_context_provider_expr_planners() {
        let ctx_provider = LanceContextProvider::default();
        assert!(!ctx_provider.get_expr_planners().is_empty());
    }

    #[test]
    fn test_regexp_match_and_non_empty_captions() {
        // Repro for a bug where regexp_match inside an AND chain wasn't coerced to boolean,
        // causing planning/evaluation failures. This should evaluate successfully.
        let schema = Arc::new(Schema::new(vec![
            Field::new("keywords", DataType::Utf8, true),
            Field::new("natural_caption", DataType::Utf8, true),
            Field::new("poetic_caption", DataType::Utf8, true),
        ]));

        let planner = Planner::new(schema.clone());

        let expr = planner
            .parse_filter(
                "regexp_match(keywords, 'Liberty|revolution') AND \
                 (natural_caption IS NOT NULL AND natural_caption <> '' AND \
                  poetic_caption IS NOT NULL AND poetic_caption <> '')",
            )
            .unwrap();

        let physical_expr = planner.create_physical_expr(&expr).unwrap();

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![
                    Some("Liberty for all"),
                    Some("peace"),
                    Some("revolution now"),
                    Some("Liberty"),
                    Some("revolutionary"),
                    Some("none"),
                ])) as ArrayRef,
                Arc::new(StringArray::from(vec![
                    Some("a"),
                    Some("b"),
                    None,
                    Some(""),
                    Some("c"),
                    Some("d"),
                ])) as ArrayRef,
                Arc::new(StringArray::from(vec![
                    Some("x"),
                    Some(""),
                    Some("y"),
                    Some("z"),
                    None,
                    Some("w"),
                ])) as ArrayRef,
            ],
        )
        .unwrap();

        let result = physical_expr.evaluate(&batch).unwrap();
        assert_eq!(
            result.into_array(0).unwrap().as_ref(),
            &BooleanArray::from(vec![true, false, false, false, false, false])
        );
    }

    #[test]
    fn test_regexp_match_infer_error_without_boolean_coercion() {
        // With the fix applied, using parse_filter should coerce regexp_match to boolean
        // even when nested in a larger AND expression, so this should plan successfully.
        let schema = Arc::new(Schema::new(vec![
            Field::new("keywords", DataType::Utf8, true),
            Field::new("natural_caption", DataType::Utf8, true),
            Field::new("poetic_caption", DataType::Utf8, true),
        ]));

        let planner = Planner::new(schema);

        let expr = planner
            .parse_filter(
                "regexp_match(keywords, 'Liberty|revolution') AND \
                 (natural_caption IS NOT NULL AND natural_caption <> '' AND \
                  poetic_caption IS NOT NULL AND poetic_caption <> '')",
            )
            .unwrap();

        // Should not panic
        let _physical = planner.create_physical_expr(&expr).unwrap();
    }

    #[test]
    fn test_jsonb_literals() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "j",
            DataType::LargeBinary,
            true,
        )]));
        let planner = Planner::new(schema);

        let cases = [
            ("jsonb '{\"key\": \"value\"}'", r#"{"key":"value"}"#),
            ("cast('{\"a\": 1}' as jsonb)", r#"{"a":1}"#),
            ("'{\"a\": 1}'::jsonb", r#"{"a":1}"#),
        ];
        for (sql, expected) in cases {
            let ast = parse_sql_expr(sql).unwrap();
            let expr = planner.parse_sql_expr(&ast).unwrap();
            match expr {
                Expr::Literal(ScalarValue::LargeBinary(Some(bytes)), _) => {
                    assert_eq!(
                        lance_arrow::json::decode_json(&bytes),
                        expected,
                        "failed for: {sql}"
                    );
                }
                other => panic!("Expected LargeBinary literal for '{sql}', got: {other:?}"),
            }
        }
    }

    #[test]
    fn test_jsonb_literal_errors() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "j",
            DataType::LargeBinary,
            true,
        )]));
        let planner = Planner::new(schema);

        // Invalid JSON content
        let ast = parse_sql_expr("jsonb 'not valid json'").unwrap();
        let err = planner.parse_sql_expr(&ast).unwrap_err();
        assert!(
            err.to_string().contains("Failed to encode JSONB"),
            "expected JSONB encoding error, got: {err}"
        );

        // CAST with non-literal expression
        let ast = parse_sql_expr("cast(j as jsonb)").unwrap();
        let err = planner.parse_sql_expr(&ast).unwrap_err();
        assert!(
            err.to_string()
                .contains("CAST to JSONB only supports string literals"),
            "got: {err}"
        );
    }
}
