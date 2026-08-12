// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use std::collections::HashSet;
use std::fmt;

use arrow::array::{ArrayData, ArrayRef, make_array};
use arrow::datatypes::DataType;
use arrow::pyarrow::{FromPyArrow, ToPyArrow};
use lancedb::function::{
    FunctionArgument, FunctionCall, FunctionCapability, FunctionDefinition, FunctionOutput,
    FunctionParameter, FunctionSignature, PythonFunctionDefinition,
};
use pyo3::{
    Bound, Py, PyAny, PyResult, Python,
    exceptions::{PyRuntimeError, PyTypeError, PyValueError},
    pyclass, pyfunction, pymethods,
    types::{
        PyAnyMethods, PyBool, PyDict, PyDictMethods, PyList, PyListMethods, PyTuple, PyTupleMethods,
    },
};

use crate::error::PythonErrorExt;
use crate::expr::{DirectExprView, PyExpr};

/// Immutable first-class Function handle backed by the exact Rust value.
#[pyclass(frozen, skip_from_py_object)]
#[derive(Clone)]
pub struct Function {
    inner: lancedb::function::Function,
}

impl Function {
    pub(crate) fn new(inner: lancedb::function::Function) -> Self {
        Self { inner }
    }

    /// Crate-private accessor for later call-authoring slices.
    #[allow(dead_code)]
    pub(crate) fn inner(&self) -> &lancedb::function::Function {
        &self.inner
    }
}

#[pymethods]
impl Function {
    #[getter]
    fn id(&self) -> &str {
        self.inner.id().as_str()
    }

    #[getter]
    fn parameters<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyTuple>> {
        let parameters = self.inner.signature().parameters();
        let mut pairs = Vec::with_capacity(parameters.len());
        for parameter in parameters {
            let data_type = parameter.data_type().to_pyarrow(py)?;
            pairs.push((parameter.name(), data_type));
        }
        PyTuple::new(py, pairs)
    }

    #[getter]
    fn output_type(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.inner
            .signature()
            .output()
            .data_type()
            .to_pyarrow(py)
            .map(|obj| obj.unbind())
    }

    #[getter]
    fn output_nullable(&self) -> bool {
        self.inner.signature().output().nullable()
    }

    fn __repr__(&self) -> String {
        format!("Function(id={:?})", self.inner.id().as_str())
    }

    /// Author an unresolved function call expression (FF-028).
    ///
    /// Keyword-only. Does not execute. Returns a private frozen authoring value
    /// that owns this exact Function and signature-ordered unresolved bindings.
    #[pyo3(signature = (*args, **kwargs))]
    fn __call__(
        &self,
        py: Python<'_>,
        args: &Bound<'_, PyTuple>,
        kwargs: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<AuthoredFunctionCall> {
        if !args.is_empty() {
            return Err(PyTypeError::new_err(
                "Function.__call__ accepts keyword arguments only",
            ));
        }
        let kwargs = match kwargs {
            Some(dict) => dict.clone(),
            None => PyDict::new(py),
        };
        AuthoredFunctionCall::try_bind(py, &self.inner, &kwargs)
    }
}

/// Signature-ordered unresolved binding for Function call authoring.
///
/// Field bindings keep a case-sensitive column name until a later table API
/// resolves stable field identity in a pinned snapshot. Literal bindings are
/// already canonical [`FunctionArgument`] literals (never field args).
#[derive(Clone)]
pub(crate) enum UnresolvedArgument {
    Field {
        column_name: String,
    },
    /// Canonical typed literal; read by the later table-binding slice.
    #[allow(dead_code)]
    Literal(FunctionArgument),
}

impl UnresolvedArgument {
    /// Structural binding text for repr/Debug only.
    ///
    /// Literal bindings expose exact Arrow [`DataType`] and one-row nullness.
    /// Never formats literal values, array Debug, IPC bytes, or payload text.
    fn format_binding(&self, name: &str) -> String {
        match self {
            Self::Field { column_name } => {
                format!("{name}=field({column_name:?})")
            }
            Self::Literal(argument) => {
                format!(
                    "{name}=literal({}, null={})",
                    argument.data_type(),
                    argument.is_typed_null()
                )
            }
        }
    }
}

/// Private, frozen owner of an exact Function plus unresolved call bindings.
///
/// Exposed to Python as `lancedb._lancedb._FunctionCall`. Not constructible
/// from Python, not a catalog/Job/wire/resource, and not serializable.
#[pyclass(
    name = "_FunctionCall",
    module = "lancedb._lancedb",
    frozen,
    skip_from_py_object
)]
#[derive(Clone)]
pub struct AuthoredFunctionCall {
    function: lancedb::function::Function,
    bindings: Vec<(String, UnresolvedArgument)>,
}

impl AuthoredFunctionCall {
    pub(crate) fn try_bind(
        py: Python<'_>,
        function: &lancedb::function::Function,
        kwargs: &Bound<'_, PyDict>,
    ) -> PyResult<Self> {
        let parameters = function.signature().parameters();
        let mut seen = HashSet::with_capacity(kwargs.len());
        let mut by_name = std::collections::HashMap::with_capacity(kwargs.len());

        for (key, value) in kwargs.iter() {
            let name: String = key.extract().map_err(|_| {
                PyTypeError::new_err("Function.__call__ keyword names must be strings")
            })?;
            if !seen.insert(name.clone()) {
                return Err(PyTypeError::new_err(format!(
                    "duplicate Function argument for parameter `{name}`"
                )));
            }
            by_name.insert(name, value);
        }

        if by_name.len() != parameters.len() {
            // Prefer precise missing/unknown diagnostics over a bare arity error.
            for parameter in parameters {
                if !by_name.contains_key(parameter.name()) {
                    return Err(PyTypeError::new_err(format!(
                        "missing Function argument for parameter `{}`",
                        parameter.name()
                    )));
                }
            }
            if let Some(unknown) = by_name.keys().find(|name| {
                !parameters
                    .iter()
                    .any(|parameter| parameter.name() == name.as_str())
            }) {
                return Err(PyTypeError::new_err(format!(
                    "unknown Function argument `{unknown}`"
                )));
            }
            return Err(PyTypeError::new_err(format!(
                "Function.__call__ requires exactly {} arguments, got {}",
                parameters.len(),
                by_name.len()
            )));
        }

        let mut bindings = Vec::with_capacity(parameters.len());
        for parameter in parameters {
            let Some(value) = by_name.remove(parameter.name()) else {
                return Err(PyTypeError::new_err(format!(
                    "missing Function argument for parameter `{}`",
                    parameter.name()
                )));
            };
            let argument = bind_argument(py, parameter, &value)?;
            bindings.push((parameter.name().to_string(), argument));
        }

        if let Some(unknown) = by_name.keys().next() {
            return Err(PyTypeError::new_err(format!(
                "unknown Function argument `{unknown}`"
            )));
        }

        Ok(Self {
            function: function.clone(),
            bindings,
        })
    }

    /// Crate-private accessor for the exact Function owned by this authored call.
    pub(crate) fn function(&self) -> &lancedb::function::Function {
        &self.function
    }

    /// Crate-private accessor for signature-ordered unresolved bindings.
    #[allow(dead_code)]
    pub(crate) fn bindings(&self) -> &[(String, UnresolvedArgument)] {
        &self.bindings
    }

    /// Bind unresolved authoring arguments against exactly one table snapshot.
    ///
    /// Calls [`lancedb::Table::generated_column_binding_snapshot`] once so the
    /// returned `source_table_version` and canonical [`FunctionCall`] share the
    /// same field identities and Arrow types. Source version remains outside the
    /// call for the create/change Job envelope.
    pub(crate) async fn bind_to_table(
        &self,
        table: &lancedb::Table,
    ) -> lancedb::Result<(u64, FunctionCall)> {
        let snapshot = table.generated_column_binding_snapshot().await?;
        self.bind_against_snapshot(&snapshot)
    }

    /// Resolve authoring bindings against an already-fetched binding snapshot.
    ///
    /// Field names use exact case-sensitive top-level lookup; a name containing
    /// `.` is literal, not a nested path. Used only by [`Self::bind_to_table`]
    /// and narrowly scoped Rust tests that need a dotted top-level name Native
    /// Lance cannot create on a real table.
    fn bind_against_snapshot(
        &self,
        snapshot: &lancedb::function::GeneratedColumnBindingSnapshot,
    ) -> lancedb::Result<(u64, FunctionCall)> {
        let mut bindings = Vec::with_capacity(self.bindings.len());
        for (parameter_name, unresolved) in &self.bindings {
            let argument = match unresolved {
                UnresolvedArgument::Field { column_name } => {
                    let Some(entry) = snapshot.field(column_name) else {
                        return Err(lancedb::Error::InvalidInput {
                            message: format!(
                                "missing table field `{column_name}` for Function parameter `{parameter_name}`"
                            ),
                        });
                    };
                    FunctionArgument::try_field(
                        entry.field_id(),
                        entry.field().data_type().clone(),
                    )?
                }
                UnresolvedArgument::Literal(argument) => argument.clone(),
            };
            bindings.push((parameter_name.clone(), argument));
        }

        let call = FunctionCall::try_new(&self.function, bindings)?;
        snapshot.validate_field_arguments(&call)?;
        Ok((snapshot.version(), call))
    }
}

impl fmt::Debug for AuthoredFunctionCall {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Never format literal payloads; literals are type + nullness only.
        f.debug_struct("_FunctionCall")
            .field("function_id", &self.function.id().as_str())
            .field(
                "bindings",
                &self
                    .bindings
                    .iter()
                    .map(|(name, binding)| binding.format_binding(name))
                    .collect::<Vec<_>>(),
            )
            .finish()
    }
}

#[pymethods]
impl AuthoredFunctionCall {
    fn __repr__(&self) -> String {
        let bindings = self
            .bindings
            .iter()
            .map(|(name, binding)| binding.format_binding(name))
            .collect::<Vec<_>>()
            .join(", ");
        format!(
            "_FunctionCall(function_id={:?}, bindings=[{bindings}])",
            self.function.id().as_str()
        )
    }
}

fn bind_argument(
    py: Python<'_>,
    parameter: &FunctionParameter,
    value: &Bound<'_, PyAny>,
) -> PyResult<UnresolvedArgument> {
    if let Some(py_expr) = extract_public_expr_inner(py, value)? {
        return match py_expr.as_direct_column_or_literal() {
            Some(DirectExprView::UnqualifiedColumn(name)) => Ok(UnresolvedArgument::Field {
                column_name: name.to_string(),
            }),
            Some(DirectExprView::Literal(scalar)) => {
                let expected = parameter.data_type();
                let actual = scalar.data_type();
                if &actual != expected {
                    return Err(PyTypeError::new_err(format!(
                        "literal expression type mismatch for parameter `{}`: expected {expected}, got {actual}",
                        parameter.name()
                    )));
                }
                let array = scalar_to_one_row_array(scalar, parameter.name(), expected)?;
                let argument = FunctionArgument::try_literal(array)
                    .map_err(|_| conversion_error(parameter.name(), expected))?;
                Ok(UnresolvedArgument::Literal(argument))
            }
            None => Err(PyTypeError::new_err(format!(
                "parameter `{}` requires a direct column reference or literal",
                parameter.name()
            ))),
        };
    }

    let argument =
        python_value_to_literal_argument(py, value, parameter.name(), parameter.data_type())?;
    Ok(UnresolvedArgument::Literal(argument))
}

fn extract_public_expr_inner<'py>(
    py: Python<'py>,
    value: &Bound<'py, PyAny>,
) -> PyResult<Option<PyExpr>> {
    let expr_cls = py.import("lancedb.expr")?.getattr("Expr")?;
    if !value.is_instance(&expr_cls)? {
        return Ok(None);
    }
    let inner = value.getattr("_inner")?;
    let py_expr: PyExpr = inner
        .extract()
        .map_err(|_| PyTypeError::new_err("lancedb.expr.Expr must wrap a native PyExpr"))?;
    Ok(Some(py_expr))
}

fn python_value_to_literal_argument(
    py: Python<'_>,
    value: &Bound<'_, PyAny>,
    parameter_name: &str,
    data_type: &DataType,
) -> PyResult<FunctionArgument> {
    let pa = py.import("pyarrow")?;
    let type_obj = data_type
        .to_pyarrow(py)
        .map_err(|_| conversion_error(parameter_name, data_type))?;
    let values = PyList::new(py, std::slice::from_ref(value))
        .map_err(|_| conversion_error(parameter_name, data_type))?;
    let kwargs = PyDict::new(py);
    kwargs
        .set_item("type", type_obj)
        .map_err(|_| conversion_error(parameter_name, data_type))?;
    let array_obj = pa
        .call_method("array", (values,), Some(&kwargs))
        .map_err(|_| conversion_error(parameter_name, data_type))?;
    let array_data = ArrayData::from_pyarrow_bound(&array_obj)
        .map_err(|_| conversion_error(parameter_name, data_type))?;
    let array: ArrayRef = make_array(array_data);
    FunctionArgument::try_literal(array).map_err(|_| conversion_error(parameter_name, data_type))
}

fn scalar_to_one_row_array(
    scalar: &datafusion_common::ScalarValue,
    parameter_name: &str,
    data_type: &DataType,
) -> PyResult<ArrayRef> {
    scalar
        .to_array_of_size(1)
        .map_err(|_| conversion_error(parameter_name, data_type))
}

fn conversion_error(parameter_name: &str, data_type: &DataType) -> pyo3::PyErr {
    PyValueError::new_err(format!(
        "cannot convert argument for parameter `{parameter_name}` to type {data_type}"
    ))
}

/// Private, frozen owner of the exact Rust [`FunctionDefinition`].
///
/// Exposed to Python as `lancedb._lancedb._FunctionDefinition`. Not
/// constructible from Python. Sensitive fields are omitted from `__repr__`
/// via the Rust `Debug` redaction contract.
#[pyclass(
    name = "_FunctionDefinition",
    module = "lancedb._lancedb",
    frozen,
    skip_from_py_object
)]
pub struct PyFunctionDefinition {
    inner: FunctionDefinition,
}

impl PyFunctionDefinition {
    pub(crate) fn new(inner: FunctionDefinition) -> Self {
        Self { inner }
    }

    /// Crate-private accessor for registration submit bindings.
    pub(crate) fn inner(&self) -> &FunctionDefinition {
        &self.inner
    }
}

#[pymethods]
impl PyFunctionDefinition {
    fn _to_json(&self) -> PyResult<String> {
        // Use the existing serde wire. Never log or format payload data into errors.
        serde_json::to_string(&self.inner)
            .map_err(|_| PyRuntimeError::new_err("failed to serialize function definition"))
    }

    fn __repr__(&self) -> String {
        format!("{:?}", self.inner)
    }
}

/// Build a private [`PyFunctionDefinition`] from normalized keyword inputs.
///
/// Arity mirrors the private Python FFI surface produced by
/// `_build_function_definition`; keep distinct keyword parameters at this boundary.
#[pyfunction(signature = (
    *,
    parameters,
    output_type,
    output_nullable,
    module,
    callable_name,
    source,
    python,
    packages,
    capabilities,
))]
#[allow(clippy::too_many_arguments)]
pub fn _new_function_definition(
    parameters: Bound<'_, PyAny>,
    output_type: Bound<'_, PyAny>,
    output_nullable: Bound<'_, PyAny>,
    module: String,
    callable_name: String,
    source: String,
    python: String,
    packages: Bound<'_, PyAny>,
    capabilities: Bound<'_, PyAny>,
) -> PyResult<PyFunctionDefinition> {
    let parameters = parse_parameters(&parameters)?;
    let output_data_type = parse_data_type(&output_type, "output_type")?;
    let output_nullable = parse_exact_bool(&output_nullable, "output_nullable")?;
    let packages = parse_string_list(&packages, "packages")?;
    let capabilities = parse_capabilities(&capabilities)?;

    let signature = FunctionSignature::try_new(
        parameters,
        FunctionOutput::new(output_data_type, output_nullable),
    )
    .infer_error()?;
    let python_definition =
        PythonFunctionDefinition::try_new(module, callable_name, source, python, packages)
            .infer_error()?;
    let definition =
        FunctionDefinition::try_new(signature, python_definition, capabilities).infer_error()?;
    Ok(PyFunctionDefinition::new(definition))
}

fn parse_exact_bool(value: &Bound<'_, PyAny>, field: &str) -> PyResult<bool> {
    if !value.is_instance_of::<PyBool>() {
        return Err(PyTypeError::new_err(format!("{field} must be a bool")));
    }
    value.extract()
}

fn parse_data_type(value: &Bound<'_, PyAny>, field: &str) -> PyResult<DataType> {
    DataType::from_pyarrow_bound(value)
        .map_err(|_| PyTypeError::new_err(format!("{field} must be a pyarrow DataType")))
}

fn parse_parameters(parameters: &Bound<'_, PyAny>) -> PyResult<Vec<FunctionParameter>> {
    let list = parameters.cast_exact::<PyList>().map_err(|_| {
        PyTypeError::new_err("parameters must be a list of (name, data_type) pairs")
    })?;
    let mut out = Vec::with_capacity(list.len());
    for i in 0..list.len() {
        let item = list.get_item(i)?;
        let pair = item
            .cast_exact::<PyTuple>()
            .map_err(|_| PyTypeError::new_err("each parameter must be a (name, data_type) pair"))?;
        if pair.len() != 2 {
            return Err(PyTypeError::new_err(
                "each parameter must be a (name, data_type) pair",
            ));
        }
        let name: String = pair
            .get_item(0)?
            .extract()
            .map_err(|_| PyTypeError::new_err("parameter name must be a string"))?;
        let data_type = parse_data_type(&pair.get_item(1)?, "parameter data_type")?;
        out.push(FunctionParameter::new(name, data_type));
    }
    Ok(out)
}

fn parse_string_list(value: &Bound<'_, PyAny>, field: &str) -> PyResult<Vec<String>> {
    let list = value
        .cast_exact::<PyList>()
        .map_err(|_| PyTypeError::new_err(format!("{field} must be a list of strings")))?;
    let mut out = Vec::with_capacity(list.len());
    for i in 0..list.len() {
        let item = list.get_item(i)?;
        let package: String = item
            .extract()
            .map_err(|_| PyTypeError::new_err(format!("{field} must contain only strings")))?;
        out.push(package);
    }
    Ok(out)
}

fn parse_capabilities(capabilities: &Bound<'_, PyAny>) -> PyResult<Vec<FunctionCapability>> {
    let list = capabilities
        .cast_exact::<PyList>()
        .map_err(|_| PyTypeError::new_err("capabilities must be a list of capability triples"))?;
    let mut out = Vec::with_capacity(list.len());
    for i in 0..list.len() {
        out.push(parse_capability_triple(&list.get_item(i)?)?);
    }
    Ok(out)
}

fn parse_capability_triple(item: &Bound<'_, PyAny>) -> PyResult<FunctionCapability> {
    let triple = item.cast_exact::<PyTuple>().map_err(|_| {
        PyTypeError::new_err(
            "each capability must be a 3-tuple of (kind, value, environment_variable)",
        )
    })?;
    if triple.len() != 3 {
        return Err(PyTypeError::new_err(
            "each capability must be a 3-tuple of (kind, value, environment_variable)",
        ));
    }

    let kind: String = triple
        .get_item(0)?
        .extract()
        .map_err(|_| PyTypeError::new_err("capability kind must be a string"))?;
    let primary: String = triple
        .get_item(1)?
        .extract()
        .map_err(|_| PyTypeError::new_err("capability value must be a string"))?;
    let env_obj = triple.get_item(2)?;
    let environment_variable = if env_obj.is_none() {
        None
    } else {
        Some(env_obj.extract::<String>().map_err(|_| {
            PyTypeError::new_err("capability environment_variable must be a string or None")
        })?)
    };

    match kind.as_str() {
        "network" => {
            if environment_variable.is_some() {
                // Fail closed without echoing kind, origin, or any env value.
                return Err(PyValueError::new_err(
                    "network capability must not include an environment variable",
                ));
            }
            FunctionCapability::try_network(primary).infer_error()
        }
        "secret" => {
            let Some(environment_variable) = environment_variable else {
                // Fail closed without echoing kind or secret reference.
                return Err(PyValueError::new_err(
                    "secret capability requires an environment variable",
                ));
            };
            FunctionCapability::try_secret(primary, environment_variable).infer_error()
        }
        // Fail closed: never echo the supplied kind, source, or secret reference.
        _ => Err(PyValueError::new_err("unsupported capability kind")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{Field, Schema};
    use arrow::record_batch::RecordBatch;
    use lancedb::connect;
    use lancedb::error::Error as LanceDbError;
    use lancedb::expr::col as ldb_col;
    use lancedb::expr::lit as ldb_lit;
    use lancedb::function::{FunctionId, FunctionOutput, FunctionParameter, FunctionSignature};

    fn sample_function() -> lancedb::function::Function {
        let signature = FunctionSignature::try_new(
            vec![
                FunctionParameter::new("text", DataType::Utf8),
                FunctionParameter::new("limit", DataType::Int32),
            ],
            FunctionOutput::new(DataType::Utf8, true),
        )
        .expect("signature");
        lancedb::function::Function::new(
            FunctionId::try_new("fn.exact.call-handle").expect("id"),
            signature,
        )
    }

    fn block_on_test<F: std::future::Future>(fut: F) -> F::Output {
        crate::runtime::block_on(fut)
    }

    async fn memory_table(name: &str, schema: Schema, columns: Vec<ArrayRef>) -> lancedb::Table {
        let db = connect("memory://").execute().await.expect("connect");
        let batch = RecordBatch::try_new(Arc::new(schema), columns).expect("batch");
        db.create_table(name, batch).execute().await.expect("table")
    }

    fn int_literal(value: Option<i32>) -> FunctionArgument {
        FunctionArgument::try_literal(Arc::new(Int32Array::from(vec![value])) as ArrayRef)
            .expect("int literal")
    }

    fn utf8_literal(value: Option<&str>) -> FunctionArgument {
        FunctionArgument::try_literal(Arc::new(StringArray::from(vec![value])) as ArrayRef)
            .expect("utf8 literal")
    }

    #[test]
    fn authored_call_normalizes_binding_order_and_preserves_column_case() {
        let function = sample_function();
        let int_lit =
            FunctionArgument::try_literal(Arc::new(Int32Array::from(vec![8])) as ArrayRef)
                .expect("literal");
        let authored = AuthoredFunctionCall {
            function: function.clone(),
            bindings: vec![
                (
                    "text".to_string(),
                    UnresolvedArgument::Field {
                        column_name: "firstName".to_string(),
                    },
                ),
                ("limit".to_string(), UnresolvedArgument::Literal(int_lit)),
            ],
        };

        assert_eq!(authored.function().id().as_str(), "fn.exact.call-handle");
        assert_eq!(authored.bindings().len(), 2);
        assert_eq!(authored.bindings()[0].0, "text");
        match &authored.bindings()[0].1 {
            UnresolvedArgument::Field { column_name } => assert_eq!(column_name, "firstName"),
            UnresolvedArgument::Literal(_) => panic!("expected field binding, got literal"),
        }
        match &authored.bindings()[1].1 {
            UnresolvedArgument::Literal(argument) => {
                assert_eq!(argument.data_type(), &DataType::Int32);
                assert!(!argument.is_typed_null());
            }
            UnresolvedArgument::Field { .. } => panic!("expected literal binding, got field"),
        }

        let rendered = authored.__repr__();
        assert!(rendered.starts_with("_FunctionCall(function_id="));
        assert!(
            rendered.find("text=field(\"firstName\")").unwrap()
                < rendered.find("limit=literal(Int32, null=false)").unwrap()
        );
        assert!(!rendered.contains('8'));
    }

    #[test]
    fn authored_call_repr_and_debug_omit_literal_payload() {
        let function = sample_function();
        let sentinel = FunctionArgument::try_literal(Arc::new(arrow::array::StringArray::from(
            vec![Some("LITERAL_PAYLOAD_SENTINEL_call_xyz_42")],
        )) as ArrayRef)
        .expect("literal");
        let authored = AuthoredFunctionCall {
            function,
            bindings: vec![
                ("text".to_string(), UnresolvedArgument::Literal(sentinel)),
                (
                    "limit".to_string(),
                    UnresolvedArgument::Literal(
                        FunctionArgument::try_literal(Arc::new(Int32Array::from(vec![Some(
                            2_147_000_123,
                        )])) as ArrayRef)
                        .expect("int literal"),
                    ),
                ),
            ],
        };
        let rendered = format!("{authored:?}\n{}", authored.__repr__());
        assert!(!rendered.contains("LITERAL_PAYLOAD_SENTINEL_call_xyz_42"));
        assert!(!rendered.contains("2147000123"));
        assert!(rendered.contains("text=literal(Utf8, null=false)"));
        assert!(rendered.contains("limit=literal(Int32, null=false)"));
    }

    #[test]
    fn typed_null_literal_argument_round_trips_type() {
        let null =
            FunctionArgument::try_literal(
                Arc::new(Int32Array::from(vec![None as Option<i32>])) as ArrayRef
            )
            .expect("typed null");
        assert!(null.is_typed_null());
        assert_eq!(null.data_type(), &DataType::Int32);
    }

    #[test]
    fn direct_expr_view_accepts_column_and_literal_only() {
        let column = PyExpr(ldb_col("firstName"));
        match column.as_direct_column_or_literal() {
            Some(DirectExprView::UnqualifiedColumn(name)) => assert_eq!(name, "firstName"),
            _ => panic!("expected unqualified column"),
        }

        let literal = PyExpr(ldb_lit(8i64));
        match literal.as_direct_column_or_literal() {
            Some(DirectExprView::Literal(value)) => {
                assert_eq!(value.data_type(), DataType::Int64);
            }
            _ => panic!("expected literal"),
        }

        let complex = PyExpr(ldb_col("text").eq(ldb_lit("x")));
        assert!(complex.as_direct_column_or_literal().is_none());
    }

    #[test]
    fn bind_to_table_field_and_literal_uses_one_snapshot_version_and_signature_order() {
        block_on_test(async {
            let table = memory_table(
                "bind_happy",
                Schema::new(vec![
                    Field::new("source_text", DataType::Utf8, true),
                    Field::new("ignored_score", DataType::Int32, false),
                ]),
                vec![
                    Arc::new(StringArray::from(vec![Some("hello")])) as ArrayRef,
                    Arc::new(Int32Array::from(vec![7])) as ArrayRef,
                ],
            )
            .await;
            let snapshot = table
                .generated_column_binding_snapshot()
                .await
                .expect("snapshot");
            let text_entry = snapshot.field("source_text").expect("source_text");

            let function = sample_function();
            // Intentionally reverse parameter order versus Function signature.
            let authored = AuthoredFunctionCall {
                function: function.clone(),
                bindings: vec![
                    (
                        "limit".to_string(),
                        UnresolvedArgument::Literal(int_literal(Some(8))),
                    ),
                    (
                        "text".to_string(),
                        UnresolvedArgument::Field {
                            column_name: "source_text".to_string(),
                        },
                    ),
                ],
            };

            let (source_version, call) =
                authored.bind_to_table(&table).await.expect("bind_to_table");
            assert_eq!(source_version, snapshot.version());
            assert_eq!(source_version, table.version().await.expect("version"));
            assert_eq!(call.function_id().as_str(), function.id().as_str());

            let arguments = call.arguments();
            assert_eq!(arguments.len(), 2);
            assert_eq!(arguments[0].0, "text");
            assert_eq!(arguments[1].0, "limit");
            assert_eq!(arguments[0].1.field_id(), Some(text_entry.field_id()));
            assert_eq!(arguments[0].1.data_type(), text_entry.field().data_type());
            assert_eq!(arguments[1].1.data_type(), &DataType::Int32);
            assert!(!arguments[1].1.is_typed_null());

            let wire = serde_json::to_value(&call).expect("serde FunctionCall");
            let encoded = wire.to_string();
            assert!(encoded.contains("\"field_id\""));
            assert!(encoded.contains("\"parameter\":\"text\""));
            assert!(encoded.contains("\"parameter\":\"limit\""));
            assert!(!encoded.contains("source_text"));
            assert!(!encoded.contains("source_table_version"));
            assert!(!encoded.contains("bind_happy"));
            assert!(!encoded.contains("table_version"));
            assert_eq!(wire.get("source_table_version"), None);
            assert_eq!(wire.get("version"), None);
        });
    }

    #[test]
    fn bind_to_table_preserves_typed_null_and_redacts_sentinel_literal() {
        block_on_test(async {
            let table = memory_table(
                "bind_null",
                Schema::new(vec![Field::new("source_text", DataType::Utf8, true)]),
                vec![Arc::new(StringArray::from(vec![Some("row")])) as ArrayRef],
            )
            .await;
            const SENTINEL: &str = "LITERAL_PAYLOAD_SENTINEL_bind_xyz_99";
            let function = sample_function();
            let authored = AuthoredFunctionCall {
                function,
                bindings: vec![
                    (
                        "text".to_string(),
                        UnresolvedArgument::Field {
                            column_name: "source_text".to_string(),
                        },
                    ),
                    (
                        "limit".to_string(),
                        UnresolvedArgument::Literal(int_literal(None)),
                    ),
                ],
            };
            // Keep a sibling authored value whose literal carries the sentinel for redaction.
            let sentinel_authored = AuthoredFunctionCall {
                function: sample_function(),
                bindings: vec![
                    (
                        "text".to_string(),
                        UnresolvedArgument::Literal(utf8_literal(Some(SENTINEL))),
                    ),
                    (
                        "limit".to_string(),
                        UnresolvedArgument::Literal(int_literal(None)),
                    ),
                ],
            };
            let rendered = format!("{sentinel_authored:?}\n{}", sentinel_authored.__repr__());
            assert!(!rendered.contains(SENTINEL));

            let (_version, call) = authored.bind_to_table(&table).await.expect("bind");
            assert!(call.arguments()[1].1.is_typed_null());
            assert_eq!(call.arguments()[1].1.data_type(), &DataType::Int32);

            let missing = AuthoredFunctionCall {
                function: sample_function(),
                bindings: vec![
                    (
                        "text".to_string(),
                        UnresolvedArgument::Literal(utf8_literal(Some(SENTINEL))),
                    ),
                    (
                        "limit".to_string(),
                        UnresolvedArgument::Field {
                            column_name: "missing_col".to_string(),
                        },
                    ),
                ],
            };
            let err = missing
                .bind_to_table(&table)
                .await
                .expect_err("missing field");
            let message = format!("{err:?}\n{err}");
            assert!(!message.contains(SENTINEL));
            assert!(matches!(err, LanceDbError::InvalidInput { .. }));
        });
    }

    #[test]
    fn bind_to_table_missing_and_case_mismatch_are_invalid_input() {
        block_on_test(async {
            let table = memory_table(
                "bind_missing",
                Schema::new(vec![Field::new("source_text", DataType::Utf8, true)]),
                vec![Arc::new(StringArray::from(vec![Some("row")])) as ArrayRef],
            )
            .await;
            let version_before = table.version().await.expect("version");

            for column_name in ["absent_column", "Source_Text", "SOURCE_TEXT"] {
                let authored = AuthoredFunctionCall {
                    function: sample_function(),
                    bindings: vec![
                        (
                            "text".to_string(),
                            UnresolvedArgument::Field {
                                column_name: column_name.to_string(),
                            },
                        ),
                        (
                            "limit".to_string(),
                            UnresolvedArgument::Literal(int_literal(Some(1))),
                        ),
                    ],
                };
                let err = authored
                    .bind_to_table(&table)
                    .await
                    .expect_err("exact name required");
                match err {
                    LanceDbError::InvalidInput { message } => {
                        assert!(message.contains(column_name));
                        assert!(message.contains("text"));
                    }
                    other => panic!("expected InvalidInput, got {other:?}"),
                }
            }

            assert_eq!(table.version().await.expect("version"), version_before);
        });
    }

    #[test]
    fn bind_to_table_field_type_mismatch_fails_through_try_new() {
        block_on_test(async {
            let table = memory_table(
                "bind_type_mismatch",
                Schema::new(vec![
                    Field::new("source_text", DataType::Int32, true),
                    Field::new("unused", DataType::Utf8, true),
                ]),
                vec![
                    Arc::new(Int32Array::from(vec![1])) as ArrayRef,
                    Arc::new(StringArray::from(vec![Some("x")])) as ArrayRef,
                ],
            )
            .await;
            let authored = AuthoredFunctionCall {
                function: sample_function(),
                bindings: vec![
                    (
                        "text".to_string(),
                        UnresolvedArgument::Field {
                            column_name: "source_text".to_string(),
                        },
                    ),
                    (
                        "limit".to_string(),
                        UnresolvedArgument::Literal(int_literal(Some(3))),
                    ),
                ],
            };
            let err = authored
                .bind_to_table(&table)
                .await
                .expect_err("Utf8 parameter vs Int32 field");
            match err {
                LanceDbError::InvalidInput { message } => {
                    assert!(message.contains("type mismatch") || message.contains("text"));
                    assert!(!message.contains("hello"));
                }
                other => panic!("expected InvalidInput, got {other:?}"),
            }
        });
    }

    #[test]
    fn bind_to_table_literal_only_ignores_table_columns() {
        block_on_test(async {
            let table = memory_table(
                "bind_literal_only",
                Schema::new(vec![Field::new("unrelated", DataType::Float64, true)]),
                vec![Arc::new(arrow::array::Float64Array::from(vec![1.5])) as ArrayRef],
            )
            .await;
            let function = lancedb::function::Function::new(
                FunctionId::try_new("fn.exact.literal-only").expect("id"),
                FunctionSignature::try_new(
                    vec![
                        FunctionParameter::new("left", DataType::Int32),
                        FunctionParameter::new("right", DataType::Int32),
                    ],
                    FunctionOutput::new(DataType::Int32, true),
                )
                .expect("signature"),
            );
            let authored = AuthoredFunctionCall {
                function: function.clone(),
                bindings: vec![
                    (
                        "left".to_string(),
                        UnresolvedArgument::Literal(int_literal(None)),
                    ),
                    (
                        "right".to_string(),
                        UnresolvedArgument::Literal(int_literal(Some(4))),
                    ),
                ],
            };
            let (version, call) = authored.bind_to_table(&table).await.expect("literal-only");
            assert_eq!(version, table.version().await.expect("version"));
            assert_eq!(call.function_id().as_str(), function.id().as_str());
            assert!(call.arguments()[0].1.is_typed_null());
            assert_eq!(call.arguments()[0].1.field_id(), None);
            assert_eq!(call.arguments()[1].1.field_id(), None);
        });
    }

    #[test]
    fn bind_to_table_dotted_name_is_literal_not_nested_path() {
        block_on_test(async {
            // Native Lance rejects creating a top-level field whose name contains
            // `.`. On a real table, prove a dotted authoring name does not resolve
            // through a nested struct path.
            let nested_fields =
                arrow::datatypes::Fields::from(vec![Field::new("b", DataType::Utf8, true)]);
            let table = memory_table(
                "bind_dotted_reject_path",
                Schema::new(vec![
                    Field::new("a", DataType::Struct(nested_fields.clone()), true),
                    Field::new("source_text", DataType::Utf8, true),
                ]),
                vec![
                    Arc::new(arrow::array::StructArray::new(
                        nested_fields,
                        vec![Arc::new(StringArray::from(vec![Some("nested")])) as ArrayRef],
                        None,
                    )) as ArrayRef,
                    Arc::new(StringArray::from(vec![Some("top")])) as ArrayRef,
                ],
            )
            .await;
            assert!(
                table
                    .generated_column_binding_snapshot()
                    .await
                    .expect("snapshot")
                    .field("a.b")
                    .is_none()
            );

            let nested_path = AuthoredFunctionCall {
                function: sample_function(),
                bindings: vec![
                    (
                        "text".to_string(),
                        UnresolvedArgument::Field {
                            column_name: "a.b".to_string(),
                        },
                    ),
                    (
                        "limit".to_string(),
                        UnresolvedArgument::Literal(int_literal(Some(2))),
                    ),
                ],
            };
            let err = nested_path
                .bind_to_table(&table)
                .await
                .expect_err("dotted name is not a nested path");
            match err {
                LanceDbError::InvalidInput { message } => {
                    assert!(message.contains("a.b"));
                    assert!(message.contains("text"));
                }
                other => panic!("expected InvalidInput, got {other:?}"),
            }
        });

        // Same binder code path: a snapshot entry whose top-level name literally
        // contains `.` binds by exact name (Remote/FF-029 projection shape).
        let snapshot = lancedb::function::GeneratedColumnBindingSnapshot::try_new(
            17,
            vec![
                Arc::new(Field::new("a.b", DataType::Utf8, true)),
                Arc::new(Field::new("a", DataType::Utf8, true)),
            ],
            vec![11, 12],
        )
        .expect("snapshot");
        let authored = AuthoredFunctionCall {
            function: sample_function(),
            bindings: vec![
                (
                    "text".to_string(),
                    UnresolvedArgument::Field {
                        column_name: "a.b".to_string(),
                    },
                ),
                (
                    "limit".to_string(),
                    UnresolvedArgument::Literal(int_literal(Some(2))),
                ),
            ],
        };
        let (version, call) = authored
            .bind_against_snapshot(&snapshot)
            .expect("literal dotted top-level");
        assert_eq!(version, 17);
        assert_eq!(call.arguments()[0].1.field_id(), Some(11));
        assert_ne!(call.arguments()[0].1.field_id(), Some(12));
    }
}
