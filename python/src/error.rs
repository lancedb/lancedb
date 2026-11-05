// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use pyo3::{
    Bound, PyAny, PyErr, PyResult, Python,
    exceptions::{PyIOError, PyNotImplementedError, PyOSError, PyRuntimeError, PyValueError},
    intern,
    types::{PyAnyMethods, PyNone},
};

use lancedb::error::{Error as LanceError, HttpErrorDetails, RemoteErrorMetadata};

pub trait PythonErrorExt<T> {
    /// Convert to a python error based on the Lance error type
    fn infer_error(self) -> PyResult<T>;
    /// Convert to OSError
    fn os_error(self) -> PyResult<T>;
    /// Convert to RuntimeError
    fn runtime_error(self) -> PyResult<T>;
    /// Convert to ValueError
    fn value_error(self) -> PyResult<T>;
}

impl<T> PythonErrorExt<T> for std::result::Result<T, LanceError> {
    fn infer_error(self) -> PyResult<T> {
        match &self {
            Ok(_) => Ok(self.unwrap()),
            Err(err) => match err {
                LanceError::InvalidInput { .. }
                | LanceError::InvalidTableName { .. }
                | LanceError::NotAMaterializedView { .. }
                | LanceError::Schema { .. }
                | LanceError::DatabaseNotFound { .. }
                | LanceError::DatabaseAlreadyExists { .. }
                | LanceError::TableAlreadyExists { .. } => self.value_error(),
                LanceError::TableNotFound { source, .. } => {
                    let Some(
                        request_error @ LanceError::Http {
                            request_id,
                            status_code,
                            source: http_source,
                        },
                    ) = source.downcast_ref::<LanceError>()
                    else {
                        return self.value_error();
                    };
                    Python::attach(|py| {
                        let status_code = status_code.map(|s| s.as_u16());
                        let metadata = request_error.remote_error_metadata();
                        let details = request_error.http_error_details();
                        let py_err = if details.is_some()
                            || metadata.is_some_and(|metadata| metadata.error_code.is_some())
                        {
                            new_py_http_error(
                                py,
                                http_source.to_string(),
                                request_id,
                                status_code,
                                details,
                                metadata,
                                false,
                            )?
                        } else {
                            let cls = py
                                .import(intern!(py, "lancedb.remote.errors"))?
                                .getattr(intern!(py, "TableNotFoundError"))?;
                            let error = cls.call1((err.to_string(), request_id, status_code))?;
                            error.setattr(
                                "job_id",
                                metadata.and_then(|value| value.job_id.as_deref()),
                            )?;
                            error.setattr(
                                "job_request_id",
                                metadata.and_then(|value| value.job_request_id.as_deref()),
                            )?;
                            error
                        };
                        let cause =
                            http_from_rust_error(py, source.as_ref(), request_id, status_code)?;
                        py_err.setattr(intern!(py, "__cause__"), cause)?;
                        Err(PyErr::from_value(py_err))
                    })
                }
                LanceError::ColumnNotFound { .. } | LanceError::FieldNotFound { .. } => {
                    Python::attach(|py| {
                        let cls = py
                            .import(intern!(py, "lancedb.exceptions"))?
                            .getattr(intern!(py, "ColumnNotFoundError"))?;
                        Err(PyErr::from_value(cls.call1((err.to_string(),))?))
                    })
                }
                LanceError::CreateDir { .. } => self.os_error(),
                LanceError::ObjectStore { .. } => Err(PyIOError::new_err(err.to_string())),
                LanceError::NotSupported { .. } => {
                    Err(PyNotImplementedError::new_err(err.to_string()))
                }
                LanceError::Http {
                    request_id,
                    source,
                    status_code,
                } => Python::attach(|py| {
                    let message = if err
                        .remote_error_metadata()
                        .is_some_and(|metadata| metadata.error_code.is_some())
                    {
                        source.to_string()
                    } else {
                        err.to_string()
                    };
                    let err = new_py_http_error(
                        py,
                        message,
                        request_id,
                        status_code.map(|s| s.as_u16()),
                        err.http_error_details(),
                        err.remote_error_metadata(),
                        is_transport_error(source.as_ref()),
                    )?;

                    if let Some(cause) = source.source() {
                        // The HTTP error already includes the first cause. But
                        // we can add the rest of the chain if there is any more.
                        let cause_err = http_from_rust_error(
                            py,
                            cause,
                            request_id,
                            status_code.map(|s| s.as_u16()),
                        )?;
                        err.setattr(intern!(py, "__cause__"), cause_err)?;
                    }

                    Err(PyErr::from_value(err))
                }),
                LanceError::Retry {
                    request_id,
                    request_failures,
                    max_request_failures,
                    connect_failures,
                    max_connect_failures,
                    read_failures,
                    max_read_failures,
                    source,
                    status_code,
                } => Python::attach(|py| {
                    let cause_err = http_from_rust_error(
                        py,
                        source.as_ref(),
                        request_id,
                        status_code.map(|s| s.as_u16()),
                    )?;

                    let message = err.to_string();
                    let retry_error_cls = py
                        .import(intern!(py, "lancedb.remote.errors"))?
                        .getattr("RetryError")?;
                    let err = retry_error_cls.call1((
                        message,
                        request_id,
                        *request_failures,
                        *connect_failures,
                        *read_failures,
                        *max_request_failures,
                        *max_connect_failures,
                        *max_read_failures,
                        status_code.map(|s| s.as_u16()),
                    ))?;

                    err.setattr(intern!(py, "__cause__"), cause_err)?;
                    Err(PyErr::from_value(err))
                }),
                LanceError::Sql {
                    message,
                    request_id,
                    grpc_code,
                    metadata,
                } => Python::attach(|py| {
                    Err(PyErr::from_value(new_py_sql_error(
                        py, message, request_id, *grpc_code, metadata,
                    )?))
                }),
                LanceError::JobFailed { job_id, failure } => Python::attach(|py| {
                    let cls = py
                        .import(intern!(py, "lancedb.exceptions"))?
                        .getattr(intern!(py, "JobFailedError"))?;
                    let sql_source = match failure.source.as_deref() {
                        Some(LanceError::Sql {
                            message,
                            request_id,
                            grpc_code,
                            metadata,
                        }) => Some((
                            new_py_sql_error(py, message, request_id, *grpc_code, metadata)?,
                            request_id.as_str(),
                            metadata.job_request_id.as_deref(),
                            sql_has_resource_error(metadata, *grpc_code),
                        )),
                        _ => None,
                    };
                    let error = cls.call1((
                        err.to_string(),
                        job_id.clone(),
                        failure.sql_job_id.clone(),
                        sql_source.as_ref().map(|(_, request_id, _, _)| *request_id),
                        sql_source
                            .as_ref()
                            .and_then(|(_, _, request_id, _)| *request_id),
                    ))?;
                    if let Some((cause, _, _, _)) = sql_source {
                        error.setattr(intern!(py, "__cause__"), cause)?;
                    }
                    Err(PyErr::from_value(error))
                }),
                LanceError::JobCancelled { .. } => Python::attach(|py| {
                    let cls = py
                        .import(intern!(py, "lancedb.exceptions"))?
                        .getattr(intern!(py, "JobCancelledError"))?;
                    Err(PyErr::from_value(cls.call1((err.to_string(),))?))
                }),
                LanceError::JobNotFound { .. } => Python::attach(|py| {
                    let cls = py
                        .import(intern!(py, "lancedb.exceptions"))?
                        .getattr(intern!(py, "JobNotFoundError"))?;
                    Err(PyErr::from_value(cls.call1((err.to_string(),))?))
                }),
                _ => self.runtime_error(),
            },
        }
    }

    fn os_error(self) -> PyResult<T> {
        self.map_err(|err| PyOSError::new_err(err.to_string()))
    }

    fn runtime_error(self) -> PyResult<T> {
        self.map_err(|err| PyRuntimeError::new_err(err.to_string()))
    }

    fn value_error(self) -> PyResult<T> {
        self.map_err(|err| PyValueError::new_err(err.to_string()))
    }
}

fn http_from_rust_error(
    py: Python<'_>,
    err: &(dyn std::error::Error + 'static),
    request_id: &str,
    status_code: Option<u16>,
) -> PyResult<PyErr> {
    let message = err.to_string();
    let details = err
        .downcast_ref::<LanceError>()
        .and_then(LanceError::http_error_details);
    let py_err = new_py_http_error(
        py,
        message,
        request_id,
        status_code,
        details,
        err.downcast_ref::<LanceError>()
            .and_then(LanceError::remote_error_metadata),
        is_transport_error(err),
    )?;

    // Reset the traceback since it doesn't provide additional information.
    let py_err = py_err.call_method1(intern!(py, "with_traceback"), (PyNone::get(py),))?;

    if let Some(cause) = err.source() {
        let cause_err = http_from_rust_error(py, cause, request_id, status_code)?;
        py_err.setattr(intern!(py, "__cause__"), cause_err)?;
    }

    Ok(PyErr::from_value(py_err))
}

fn new_py_http_error<'py>(
    py: Python<'py>,
    message: String,
    request_id: &str,
    status_code: Option<u16>,
    details: Option<&HttpErrorDetails>,
    metadata: Option<&RemoteErrorMetadata>,
    is_transport_error: bool,
) -> PyResult<Bound<'py, PyAny>> {
    let error_code = metadata
        .and_then(|metadata| metadata.error_code.as_deref())
        .or_else(|| details.map(|details| details.error_code.as_str()));
    let resource_kind = details.map(|details| details.resource_kind.as_str());
    let resource_name = details.map(|details| details.resource_name.as_str());
    let namespace_path = details.map(|details| details.namespace_path.clone());

    py.import("lancedb.remote.errors")?
        .getattr("HttpError")?
        .call1((
            message,
            request_id,
            status_code,
            error_code,
            resource_kind,
            resource_name,
            namespace_path,
            is_transport_error,
            metadata.and_then(|metadata| metadata.job_id.as_deref()),
            metadata.and_then(|metadata| metadata.job_request_id.as_deref()),
        ))
}

fn new_py_sql_error<'py>(
    py: Python<'py>,
    message: &str,
    request_id: &str,
    grpc_code: i32,
    metadata: &RemoteErrorMetadata,
) -> PyResult<Bound<'py, PyAny>> {
    if sql_has_resource_error(metadata, grpc_code) {
        return new_py_sql_resource_error(py, message, request_id, grpc_code, metadata);
    }
    py.import("lancedb.remote.errors")?
        .getattr("SqlError")?
        .call1((
            message,
            request_id,
            grpc_code,
            metadata.error_code.as_deref(),
            metadata.job_id.as_deref(),
            metadata.job_request_id.as_deref(),
        ))
}

fn new_py_sql_resource_error<'py>(
    py: Python<'py>,
    message: &str,
    request_id: &str,
    grpc_code: i32,
    metadata: &RemoteErrorMetadata,
) -> PyResult<Bound<'py, PyAny>> {
    let resource = metadata.resource.as_ref().unwrap();
    py.import("lancedb.remote.errors")?
        .getattr("HttpError")?
        .call1((
            message,
            request_id,
            grpc_code_to_http_status(grpc_code),
            metadata.error_code.as_deref(),
            resource.kind.as_str(),
            resource.name.as_str(),
            resource.namespace_path.clone(),
            false,
            metadata.job_id.as_deref(),
            metadata.job_request_id.as_deref(),
        ))
}

fn sql_has_resource_error(metadata: &RemoteErrorMetadata, grpc_code: i32) -> bool {
    metadata.resource.is_some()
        && matches!(
            (metadata.error_code.as_deref(), grpc_code),
            // These are gRPC status codes, not HTTP status codes.
            (Some("not_found"), 5) | (Some("already_exists"), 6)
        )
}

fn grpc_code_to_http_status(grpc_code: i32) -> Option<u16> {
    match grpc_code {
        3 => Some(400),
        5 => Some(404),
        6 => Some(409),
        7 => Some(403),
        9 => Some(400),
        12 => Some(501),
        _ => None,
    }
}

// Preserve the typed transport classification across the Python boundary.
// A missing status code alone also covers malformed job responses, which
// waiting longer cannot repair.
fn is_transport_error(err: &(dyn std::error::Error + 'static)) -> bool {
    if let Some(error) = err.downcast_ref::<reqwest::Error>() {
        return error.is_connect()
            || error.is_timeout()
            || error.is_request()
            || error.is_body()
            || error.is_decode();
    }
    err.source().is_some_and(is_transport_error)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use lancedb::error::{JobFailure, RemoteResourceIdentity};
    use pyo3::types::PyAnyMethods;

    use super::*;

    #[test]
    fn generic_job_failure_retains_the_named_resource_as_its_cause() {
        Python::initialize();
        let source = LanceError::Sql {
            message: "Function 'missing' was not found".into(),
            request_id: "current-request".into(),
            grpc_code: 5,
            metadata: Box::new({
                let mut metadata = RemoteErrorMetadata::default();
                metadata.error_code = Some("not_found".into());
                metadata.job_id = Some("outer-job".into());
                metadata.job_request_id = Some("worker-request".into());
                metadata.resource = Some(RemoteResourceIdentity::new(
                    "function",
                    "missing",
                    vec!["ns".into()],
                ));
                metadata
            }),
        };
        let error = LanceError::JobFailed {
            job_id: Some("outer-job".into()),
            failure: Box::new(JobFailure {
                message: Some("Function 'missing' was not found".into()),
                traceback: None,
                sql_job_id: Some("outer-job".into()),
                source: Some(Arc::new(source)),
                ..Default::default()
            }),
        };

        let py_err = Python::attach(|_| {
            let result: std::result::Result<(), LanceError> = Err(error);
            result.infer_error().unwrap_err()
        });
        Python::attach(|py| {
            let value = py_err.value(py);
            let job_failed = py
                .import("lancedb.exceptions")
                .unwrap()
                .getattr("JobFailedError")
                .unwrap();
            assert!(value.is_instance(&job_failed).unwrap(), "{py_err:?}");
            let cause = value.getattr("__cause__").unwrap();
            let function_not_found = py
                .import("lancedb.remote.errors")
                .unwrap()
                .getattr("FunctionNotFoundError")
                .unwrap();
            assert!(
                cause.is_instance(&function_not_found).unwrap(),
                "{py_err:?}"
            );
            assert_eq!(
                cause
                    .getattr("error_code")
                    .unwrap()
                    .extract::<String>()
                    .unwrap(),
                "not_found"
            );
            assert_eq!(
                cause
                    .getattr("resource_kind")
                    .unwrap()
                    .extract::<String>()
                    .unwrap(),
                "function"
            );
            assert_eq!(
                cause
                    .getattr("resource_name")
                    .unwrap()
                    .extract::<String>()
                    .unwrap(),
                "missing"
            );
            assert_eq!(
                cause
                    .getattr("namespace_path")
                    .unwrap()
                    .extract::<Vec<String>>()
                    .unwrap(),
                ["ns"]
            );
            assert_eq!(
                cause
                    .getattr("status_code")
                    .unwrap()
                    .extract::<u16>()
                    .unwrap(),
                404
            );
            assert_eq!(
                cause
                    .getattr("request_id")
                    .unwrap()
                    .extract::<String>()
                    .unwrap(),
                "current-request"
            );
            assert_eq!(
                cause
                    .getattr("job_id")
                    .unwrap()
                    .extract::<String>()
                    .unwrap(),
                "outer-job"
            );
            assert_eq!(
                cause
                    .getattr("job_request_id")
                    .unwrap()
                    .extract::<String>()
                    .unwrap(),
                "worker-request"
            );
        });
    }

    #[test]
    fn direct_catalog_lookup_is_raised_as_the_named_resource_error() {
        Python::initialize();
        for (kind, name, namespace_path, exception) in [
            (
                "function",
                "missing",
                vec!["ns".into()],
                "FunctionNotFoundError",
            ),
            ("namespace", "ns", Vec::new(), "NamespaceNotFoundError"),
        ] {
            let mut metadata = RemoteErrorMetadata::default();
            metadata.error_code = Some("not_found".into());
            metadata.resource = Some(RemoteResourceIdentity::new(kind, name, namespace_path));
            let error = LanceError::Sql {
                message: format!("{kind} '{name}' was not found"),
                request_id: "current-request".into(),
                grpc_code: 5,
                metadata: Box::new(metadata),
            };
            let py_err = Python::attach(|_| {
                let result: std::result::Result<(), LanceError> = Err(error);
                result.infer_error().unwrap_err()
            });
            Python::attach(|py| {
                let value = py_err.value(py);
                let class = py
                    .import("lancedb.remote.errors")
                    .unwrap()
                    .getattr(exception)
                    .unwrap();
                assert!(value.is_instance(&class).unwrap(), "{py_err:?}");
                assert_eq!(
                    value
                        .getattr("resource_name")
                        .unwrap()
                        .extract::<String>()
                        .unwrap(),
                    name
                );
            });
        }
    }
}
