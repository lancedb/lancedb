// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use crate::error::PythonErrorExt;
use crate::runtime::future_into_py;
use lancedb::authz::*;
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::types::{PyAnyMethods, PyBytes, PyString};
use pyo3::{Bound, PyAny, PyRef, PyResult, pyclass, pymethods};
use std::sync::Arc;

#[pyclass]
pub struct AuthorizationClient {
    pub(crate) inner: Arc<dyn Authorization>,
}

#[pymethods]
impl AuthorizationClient {
    fn list_acls<'py>(self_: PyRef<'py, Self>, request: &str) -> PyResult<Bound<'py, PyAny>> {
        let request: ListAccessControlEntriesRequest = serde_json::from_str(request)
            .map_err(|_| PyValueError::new_err("Invalid authorization request"))?;
        let inner = self_.inner.clone();
        future_into_py(self_.py(), async move {
            let result = inner.list_acls(request).await.infer_error()?;
            serde_json::to_string(&result).map_err(|err| PyRuntimeError::new_err(err.to_string()))
        })
    }
    fn add_acl<'py>(self_: PyRef<'py, Self>, request: &str) -> PyResult<Bound<'py, PyAny>> {
        let request: AccessControlEntryRequest = serde_json::from_str(request)
            .map_err(|_| PyValueError::new_err("Invalid authorization request"))?;
        let inner = self_.inner.clone();
        future_into_py(self_.py(), async move {
            inner.add_acl(request).await.infer_error()
        })
    }
    fn delete_acl<'py>(self_: PyRef<'py, Self>, request: &str) -> PyResult<Bound<'py, PyAny>> {
        let request: AccessControlEntryRequest = serde_json::from_str(request)
            .map_err(|_| PyValueError::new_err("Invalid authorization request"))?;
        let inner = self_.inner.clone();
        future_into_py(self_.py(), async move {
            inner.delete_acl(request).await.infer_error()
        })
    }
    fn list_role_bindings<'py>(
        self_: PyRef<'py, Self>,
        request: &str,
    ) -> PyResult<Bound<'py, PyAny>> {
        let request: ListRoleBindingsRequest = serde_json::from_str(request)
            .map_err(|_| PyValueError::new_err("Invalid authorization request"))?;
        request.validate().infer_error()?;
        let inner = self_.inner.clone();
        future_into_py(self_.py(), async move {
            let result = inner.list_role_bindings(request).await.infer_error()?;
            serde_json::to_string(&result).map_err(|err| PyRuntimeError::new_err(err.to_string()))
        })
    }
    fn add_role_binding<'py>(
        self_: PyRef<'py, Self>,
        request: &str,
    ) -> PyResult<Bound<'py, PyAny>> {
        let request: RoleBindingRequest = serde_json::from_str(request)
            .map_err(|_| PyValueError::new_err("Invalid authorization request"))?;
        request.validate().infer_error()?;
        let inner = self_.inner.clone();
        future_into_py(self_.py(), async move {
            inner.add_role_binding(request).await.infer_error()
        })
    }
    fn delete_role_binding<'py>(
        self_: PyRef<'py, Self>,
        request: &str,
    ) -> PyResult<Bound<'py, PyAny>> {
        let request: RoleBindingRequest = serde_json::from_str(request)
            .map_err(|_| PyValueError::new_err("Invalid authorization request"))?;
        request.validate().infer_error()?;
        let inner = self_.inner.clone();
        future_into_py(self_.py(), async move {
            inner.delete_role_binding(request).await.infer_error()
        })
    }
    fn get_principal<'py>(self_: PyRef<'py, Self>, request: &str) -> PyResult<Bound<'py, PyAny>> {
        let request: GetPrincipalRequest = serde_json::from_str(request)
            .map_err(|_| PyValueError::new_err("Invalid authorization request"))?;
        request.validate().infer_error()?;
        let inner = self_.inner.clone();
        future_into_py(self_.py(), async move {
            let result = inner.get_principal(request).await.infer_error()?;
            serde_json::to_string(&result).map_err(|err| PyRuntimeError::new_err(err.to_string()))
        })
    }
    fn get_group<'py>(self_: PyRef<'py, Self>, request: &str) -> PyResult<Bound<'py, PyAny>> {
        let request: GetGroupRequest = serde_json::from_str(request)
            .map_err(|_| PyValueError::new_err("Invalid authorization request"))?;
        request.validate().infer_error()?;
        let inner = self_.inner.clone();
        future_into_py(self_.py(), async move {
            let result = inner.get_group(request).await.infer_error()?;
            serde_json::to_string(&result).map_err(|err| PyRuntimeError::new_err(err.to_string()))
        })
    }
}

/// The shared privilege parser and canonical wire names.
#[pyclass(frozen)]
pub struct AuthzPrivilege {
    inner: Privilege,
}

#[pymethods]
impl AuthzPrivilege {
    #[new]
    fn new(value: &str) -> PyResult<Self> {
        Ok(Self {
            inner: value.parse().map_err(PyValueError::new_err)?,
        })
    }

    #[staticmethod]
    fn known() -> Vec<String> {
        Privilege::KNOWN.iter().map(ToString::to_string).collect()
    }

    fn __str__(&self) -> String {
        self.inner.to_string()
    }
}

#[pyclass(frozen)]
pub struct AuthzSubject {
    inner: Subject,
}

#[pymethods]
impl AuthzSubject {
    #[new]
    fn new(kind: &str, value: &Bound<'_, PyAny>) -> PyResult<Self> {
        let value: String = value
            .extract()
            .map_err(|_| PyValueError::new_err("Subject value must be a nonempty string"))?;
        Ok(Self {
            inner: Subject::new(kind.parse().infer_error()?, value).infer_error()?,
        })
    }

    #[staticmethod]
    fn from_canonical(value: &str) -> PyResult<Self> {
        Ok(Self {
            inner: Subject::from_canonical(value).infer_error()?,
        })
    }

    #[getter]
    fn kind(&self) -> &str {
        self.inner.kind().as_str()
    }
    #[getter]
    fn value(&self) -> &str {
        self.inner.value()
    }
    #[getter]
    fn display_value(&self) -> &str {
        self.inner.display_value()
    }

    #[pyo3(signature = (context=None))]
    fn to_wire(&self, context: Option<&str>) -> PyResult<String> {
        match context {
            None => {}
            Some("principal") => self.inner.validate_principal().infer_error()?,
            Some("group") => self.inner.validate_group().infer_error()?,
            Some("role_member") => self.inner.validate_role_member().infer_error()?,
            Some(_) => return Err(PyValueError::new_err("Invalid subject context")),
        }
        Ok(self.inner.to_wire())
    }

    fn __repr__(&self) -> String {
        format!("{:?}", self.inner)
    }
    fn __str__(&self) -> String {
        self.inner.to_string()
    }
}

#[pyclass(frozen)]
pub struct AuthzObject {
    inner: Object,
}

impl AuthzObject {
    fn validated(inner: Object) -> PyResult<Self> {
        inner.validate().map_err(PyValueError::new_err)?;
        Ok(Self { inner })
    }
}

fn namespace_path(value: &Bound<'_, PyAny>) -> PyResult<NamespacePath> {
    if value.is_instance_of::<PyString>() || value.is_instance_of::<PyBytes>() {
        return Err(PyValueError::new_err(
            "namespace must be a sequence of components",
        ));
    }
    let components = value
        .extract()
        .map_err(|_| PyValueError::new_err("namespace must be a sequence of string components"))?;
    NamespacePath::new(components).map_err(PyValueError::new_err)
}

#[pymethods]
impl AuthzObject {
    #[new]
    fn new(value: &str) -> PyResult<Self> {
        Ok(Self {
            inner: value.parse().infer_error()?,
        })
    }

    #[staticmethod]
    fn system() -> Self {
        Self {
            inner: Object::System,
        }
    }

    #[staticmethod]
    fn database(name: &str) -> PyResult<Self> {
        Self::validated(Object::Database(DatabaseObject::new(name.into())))
    }

    #[staticmethod]
    fn namespace(database: &str, namespace: &Bound<'_, PyAny>) -> PyResult<Self> {
        Self::validated(Object::Namespace(NamespaceObject {
            database: database.into(),
            namespace: namespace_path(namespace)?,
        }))
    }

    #[staticmethod]
    fn table(database: &str, namespace: &Bound<'_, PyAny>, name: &str) -> PyResult<Self> {
        Self::validated(Object::Table(TableObject {
            database: database.into(),
            namespace: namespace_path(namespace)?,
            table: name.into(),
        }))
    }

    #[staticmethod]
    fn view(database: &str, namespace: &Bound<'_, PyAny>, name: &str) -> PyResult<Self> {
        Self::validated(Object::View(ViewObject {
            database: database.into(),
            namespace: namespace_path(namespace)?,
            view: name.into(),
        }))
    }

    #[staticmethod]
    fn secret(database: &str, namespace: &Bound<'_, PyAny>, name: &str) -> PyResult<Self> {
        Self::validated(Object::Secret(SecretObject {
            database: database.into(),
            namespace: namespace_path(namespace)?,
            secret: name.into(),
        }))
    }

    #[staticmethod]
    fn function(database: &str, namespace: &Bound<'_, PyAny>, name: &str) -> PyResult<Self> {
        Self::validated(Object::Function(FunctionObject {
            database: database.into(),
            namespace: namespace_path(namespace)?,
            function: name.into(),
        }))
    }

    fn __str__(&self) -> String {
        self.inner.to_string()
    }
    fn __repr__(&self) -> String {
        format!("{:?}", self.inner)
    }
}
