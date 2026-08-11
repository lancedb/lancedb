// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use std::fmt::{self, Display, Formatter};
use std::sync::{Arc, PoisonError};

use arrow_schema::ArrowError;
use datafusion_common::DataFusionError;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use snafu::Snafu;

pub(crate) type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// Stable Function error category (FF-006).
///
/// The known variants serialize to fixed JSON strings. Any other wire string
/// decodes as [`Self::Unrecognized`] with the exact value preserved, and
/// re-serializes unchanged. Category judgment is structural: do not infer a
/// code from diagnostic message text, HTTP status, job phase, or retryability.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum FunctionErrorCode {
    /// Function definition failed validation.
    DefinitionValidationFailure,
    /// A named Function or Function reference was not found.
    NameOrFunctionNotFound,
    /// A Function name conflicts with an existing name.
    NameConflict,
    /// The requested runtime or capability is not supported.
    UnsupportedRuntimeOrCapability,
    /// The Function has been revoked and cannot be used.
    RevokedFunction,
    /// User-defined Function execution failed.
    UdfExecutionFailure,
    /// A generated column was not fully materialized.
    GeneratedColumnIncomplete,
    /// Input was stale or conflicted with the current state.
    StaleOrConflictingInput,
    /// A wire string this client version does not recognize.
    ///
    /// The inner value is preserved exactly for forward compatibility.
    Unrecognized(String),
}

impl FunctionErrorCode {
    /// The stable JSON / wire string for this code.
    pub fn as_str(&self) -> &str {
        match self {
            Self::DefinitionValidationFailure => "definition_validation_failure",
            Self::NameOrFunctionNotFound => "name_or_function_not_found",
            Self::NameConflict => "name_conflict",
            Self::UnsupportedRuntimeOrCapability => "unsupported_runtime_or_capability",
            Self::RevokedFunction => "revoked_function",
            Self::UdfExecutionFailure => "udf_execution_failure",
            Self::GeneratedColumnIncomplete => "generated_column_incomplete",
            Self::StaleOrConflictingInput => "stale_or_conflicting_input",
            Self::Unrecognized(raw) => raw.as_str(),
        }
    }

    fn from_wire(value: &str) -> Self {
        match value {
            "definition_validation_failure" => Self::DefinitionValidationFailure,
            "name_or_function_not_found" => Self::NameOrFunctionNotFound,
            "name_conflict" => Self::NameConflict,
            "unsupported_runtime_or_capability" => Self::UnsupportedRuntimeOrCapability,
            "revoked_function" => Self::RevokedFunction,
            "udf_execution_failure" => Self::UdfExecutionFailure,
            "generated_column_incomplete" => Self::GeneratedColumnIncomplete,
            "stale_or_conflicting_input" => Self::StaleOrConflictingInput,
            other => Self::Unrecognized(other.to_string()),
        }
    }
}

impl Display for FunctionErrorCode {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl Serialize for FunctionErrorCode {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for FunctionErrorCode {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        Ok(Self::from_wire(String::deserialize(deserializer)?.as_str()))
    }
}

/// Why a job failed, to whatever precision the backend provides.
///
/// A job run in this process carries the error it failed with in [`Self::source`].
/// A job run remotely carries whatever the server reported, which older servers
/// do not report at all. Every field is absent rather than invented when the
/// backend does not supply it.
#[derive(Debug, Clone, Default)]
pub struct JobFailure {
    /// Stable Function error category, when the backend supplied one.
    ///
    /// Present only when copied from [`Error::Function`] or decoded from a
    /// remote `error_code` field. Never inferred from message, phase,
    /// retryable, HTTP status, or other diagnostics.
    pub error_code: Option<FunctionErrorCode>,
    /// The stage the job was in, when known.
    pub phase: Option<String>,
    /// A human-readable reason, when known.
    pub message: Option<String>,
    /// Whether a retry could clear the failure, when known.
    pub retryable: Option<bool>,
    /// The error the job failed with, when it ran in this process.
    pub source: Option<Arc<Error>>,
}

impl JobFailure {
    /// A failure whose only known detail is the error that caused it.
    ///
    /// When `source` is [`Error::Function`], [`Self::error_code`] is copied
    /// from that error. Other error kinds leave `error_code` as [`None`].
    pub(crate) fn from_source(source: Arc<Error>) -> Self {
        let error_code = match source.as_ref() {
            Error::Function { code, .. } => Some(code.clone()),
            _ => None,
        };
        Self {
            error_code,
            message: Some(source.to_string()),
            source: Some(source),
            ..Default::default()
        }
    }
}

impl Display for JobFailure {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match (&self.message, &self.phase) {
            (Some(message), Some(phase)) => write!(f, ": {message} (in {phase})"),
            (Some(message), None) => write!(f, ": {message}"),
            (None, Some(phase)) => write!(f, " in {phase}"),
            (None, None) => Ok(()),
        }
    }
}

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum Error {
    #[snafu(display("Invalid table name (\"{name}\"): {reason}"))]
    InvalidTableName { name: String, reason: String },
    #[snafu(display("Invalid input, {message}"))]
    InvalidInput { message: String },
    #[snafu(display("Table '{name}' was not found"))]
    TableNotFound { name: String, source: BoxError },
    #[snafu(display(
        "Table '{name}' exists but could not be loaded (it may be corrupt or incomplete): {source}"
    ))]
    TableCorrupted { name: String, source: BoxError },
    #[snafu(display("Database '{name}' was not found"))]
    DatabaseNotFound { name: String },
    #[snafu(display("Database '{name}' already exists."))]
    DatabaseAlreadyExists { name: String },
    #[snafu(display("Index '{name}' was not found"))]
    IndexNotFound { name: String },
    #[snafu(display("Embedding function '{name}' was not found. : {reason}"))]
    EmbeddingFunctionNotFound { name: String, reason: String },

    #[snafu(display("Table '{name}' already exists"))]
    TableAlreadyExists { name: String },
    #[snafu(display("Unable to created lance dataset at {path}: {source}"))]
    CreateDir {
        path: String,
        source: std::io::Error,
    },
    #[snafu(display("Schema Error: {message}"))]
    Schema { message: String },
    #[snafu(display("Runtime error: {message}"))]
    Runtime { message: String },
    #[snafu(display("Timeout error: {message}"))]
    Timeout { message: String },
    #[snafu(display("Job{} failed{failure}", job_id.as_ref().map(|id| format!(" {id}")).unwrap_or_default()))]
    JobFailed {
        job_id: Option<String>,
        failure: JobFailure,
    },
    #[snafu(display("Job{} was cancelled", job_id.as_ref().map(|id| format!(" {id}")).unwrap_or_default()))]
    JobCancelled { job_id: Option<String> },
    /// A first-class Function operation failed with a stable category.
    ///
    /// [`Self::Function::code`] is the semantic category. [`Self::Function::message`]
    /// is diagnostic only and must not be used to recover or override the code.
    #[snafu(display("Function error ({code}): {message}"))]
    Function {
        code: FunctionErrorCode,
        message: String,
    },

    // 3rd party / external errors
    #[snafu(display("object_store error: {source}"))]
    ObjectStore { source: object_store::Error },
    #[snafu(display("lance error: {source}"))]
    Lance { source: lance::Error },
    #[cfg(feature = "remote")]
    #[snafu(display("Http error: (request_id={request_id}) {source}"))]
    Http {
        #[snafu(source(from(reqwest::Error, Box::new)))]
        source: Box<dyn std::error::Error + Send + Sync>,
        request_id: String,
        /// Status code associated with the error, if available.
        /// This is not always available, for example when the error is due to a
        /// connection failure. It may also be missing if the request was
        /// successful but there was an error decoding the response.
        status_code: Option<reqwest::StatusCode>,
    },
    #[cfg(feature = "remote")]
    #[snafu(display(
        "Hit retry limit for request_id={request_id} (\
        request_failures={request_failures}/{max_request_failures}, \
        connect_failures={connect_failures}/{max_connect_failures}, \
        read_failures={read_failures}/{max_read_failures})"
    ))]
    Retry {
        request_id: String,
        request_failures: u8,
        max_request_failures: u8,
        connect_failures: u8,
        max_connect_failures: u8,
        read_failures: u8,
        max_read_failures: u8,
        #[snafu(source(from(reqwest::Error, Box::new)))]
        source: Box<dyn std::error::Error + Send + Sync>,
        status_code: Option<reqwest::StatusCode>,
    },
    #[snafu(display("Arrow error: {source}"))]
    Arrow { source: ArrowError },
    #[snafu(display("LanceDBError: not supported: {message}"))]
    NotSupported { message: String },
    /// External error pass through from user code.
    #[snafu(transparent)]
    External { source: BoxError },
    #[snafu(whatever, display("{message}"))]
    Other {
        message: String,
        #[snafu(source(from(Box<dyn std::error::Error + Send + Sync>, Some)))]
        source: Option<Box<dyn std::error::Error + Send + Sync>>,
    },
}

pub type Result<T> = std::result::Result<T, Error>;

impl From<ArrowError> for Error {
    fn from(source: ArrowError) -> Self {
        match source {
            ArrowError::ExternalError(source) => Self::from_box_error(source),
            _ => Self::Arrow { source },
        }
    }
}

impl From<DataFusionError> for Error {
    fn from(source: DataFusionError) -> Self {
        match source {
            DataFusionError::ArrowError(source, _) => (*source).into(),
            DataFusionError::External(source) => Self::from_box_error(source),
            other => Self::External {
                source: Box::new(other),
            },
        }
    }
}

impl From<lance::Error> for Error {
    fn from(source: lance::Error) -> Self {
        if has_unsupported_local_filesystem_source(&source) {
            return Self::NotSupported {
                message: "the filesystem does not support an operation required for safe Lance commits (such as atomic rename). Object-storage mounts such as Mountpoint for Amazon S3 are not supported; use the native object-store URI (for example, s3://bucket/path) instead".to_string(),
            };
        }

        // Try to unwrap external errors that were wrapped by lance
        match source {
            lance::Error::Wrapped { error, .. } => Self::from_box_error(error),
            lance::Error::External { source } => Self::from_box_error(source),
            lance::Error::InvalidInput { source, .. } => Self::InvalidInput {
                message: source.to_string(),
            },
            _ => Self::Lance { source },
        }
    }
}

fn has_unsupported_local_filesystem_source(error: &(dyn std::error::Error + 'static)) -> bool {
    let mut current = Some(error);
    let mut is_local_filesystem = false;
    let mut is_unsupported = false;
    while let Some(error) = current {
        is_local_filesystem |= error
            .downcast_ref::<object_store::Error>()
            .is_some_and(|error| {
                matches!(error, object_store::Error::Generic { store, .. } if *store == "LocalFileSystem")
            });
        is_unsupported |= error
            .downcast_ref::<std::io::Error>()
            .is_some_and(|error| error.kind() == std::io::ErrorKind::Unsupported);
        if is_local_filesystem && is_unsupported {
            return true;
        }
        current = error.source();
    }
    false
}

impl Error {
    fn from_box_error(mut source: Box<dyn std::error::Error + Send + Sync>) -> Self {
        source = match source.downcast::<Self>() {
            Ok(e) => match *e {
                Self::External { source } => return Self::from_box_error(source),
                other => return other,
            },
            Err(source) => source,
        };

        source = match source.downcast::<lance::Error>() {
            Ok(e) => match *e {
                lance::Error::Wrapped { error, .. } => return Self::from_box_error(error),
                other => return other.into(),
            },
            Err(source) => source,
        };

        source = match source.downcast::<ArrowError>() {
            Ok(e) => match *e {
                ArrowError::ExternalError(source) => return Self::from_box_error(source),
                other => return other.into(),
            },
            Err(source) => source,
        };

        source = match source.downcast::<DataFusionError>() {
            Ok(e) => match *e {
                DataFusionError::ArrowError(source, _) => return (*source).into(),
                DataFusionError::External(source) => return Self::from_box_error(source),
                other => return other.into(),
            },
            Err(source) => source,
        };

        Self::External { source }
    }
}

impl From<object_store::Error> for Error {
    fn from(source: object_store::Error) -> Self {
        Self::ObjectStore { source }
    }
}

impl From<object_store::path::Error> for Error {
    fn from(source: object_store::path::Error) -> Self {
        Self::ObjectStore {
            source: object_store::Error::InvalidPath { source },
        }
    }
}

impl<T> From<PoisonError<T>> for Error {
    fn from(e: PoisonError<T>) -> Self {
        Self::Runtime {
            message: e.to_string(),
        }
    }
}

#[cfg(feature = "polars")]
impl From<polars::prelude::PolarsError> for Error {
    fn from(source: polars::prelude::PolarsError) -> Self {
        Self::Other {
            message: "Error in Polars DataFrame integration.".to_string(),
            source: Some(Box::new(source)),
        }
    }
}

#[cfg(feature = "sentence-transformers")]
impl From<hf_hub::api::sync::ApiError> for Error {
    fn from(source: hf_hub::api::sync::ApiError) -> Self {
        Self::Other {
            message: "Error in Sentence Transformers integration.".to_string(),
            source: Some(Box::new(source)),
        }
    }
}
#[cfg(feature = "sentence-transformers")]
impl From<candle_core::Error> for Error {
    fn from(source: candle_core::Error) -> Self {
        Self::Other {
            message: "Error in 'candle_core'.".to_string(),
            source: Some(Box::new(source)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unsupported_filesystem_operations_have_actionable_error() {
        let object_store_error = object_store::Error::Generic {
            store: "LocalFileSystem",
            source: Box::new(std::io::Error::from(std::io::ErrorKind::Unsupported)),
        };
        let lance_error = lance::Error::io_source(Box::new(object_store_error));

        let error = Error::from(lance_error);

        assert!(matches!(
            error,
            Error::NotSupported { message }
                if message.contains("Mountpoint for Amazon S3")
                    && message.contains("s3://bucket/path")
        ));
    }

    #[test]
    fn other_io_errors_remain_lance_errors() {
        let object_store_error = object_store::Error::Generic {
            store: "LocalFileSystem",
            source: Box::new(std::io::Error::from(std::io::ErrorKind::PermissionDenied)),
        };
        let lance_error = lance::Error::io_source(Box::new(object_store_error));

        assert!(matches!(Error::from(lance_error), Error::Lance { .. }));
    }

    #[test]
    fn unsupported_non_filesystem_errors_remain_lance_errors() {
        let lance_error = lance::Error::io_source(Box::new(std::io::Error::from(
            std::io::ErrorKind::Unsupported,
        )));

        assert!(matches!(Error::from(lance_error), Error::Lance { .. }));
    }
}
