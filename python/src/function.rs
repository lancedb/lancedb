// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use arrow::datatypes::DataType;
use arrow::pyarrow::{FromPyArrow, ToPyArrow};
use lancedb::function::{
    FunctionCapability, FunctionDefinition, FunctionOutput, FunctionParameter, FunctionSignature,
    PythonFunctionDefinition,
};
use pyo3::{
    Bound, Py, PyAny, PyResult, Python,
    exceptions::{PyRuntimeError, PyTypeError, PyValueError},
    pyclass, pyfunction, pymethods,
    types::{PyAnyMethods, PyBool, PyList, PyListMethods, PyTuple, PyTupleMethods},
};

use crate::error::PythonErrorExt;

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
