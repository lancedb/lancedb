// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use std::fmt::{self, Display, Formatter};
use std::sync::{Arc, PoisonError};

use arrow_schema::ArrowError;
use datafusion_common::DataFusionError;
#[cfg(feature = "remote")]
use serde::Deserialize;
use snafu::Snafu;

pub(crate) type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// Stable resource details carried by a structured remote HTTP error.
#[cfg(feature = "remote")]
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct HttpErrorDetails {
    /// Machine-readable error code from the server.
    pub error_code: String,
    /// The kind of resource named by the error.
    pub resource_kind: String,
    /// The leaf name of the resource.
    pub resource_name: String,
    /// The parent namespace path of the resource.
    pub namespace_path: Vec<String>,
}

/// Stable resource identity reported with a remote Flight SQL failure.
#[cfg(feature = "remote")]
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct RemoteResourceIdentity {
    /// The kind of resource named by the error.
    pub kind: String,
    /// The leaf name of the resource.
    pub name: String,
    /// The parent namespace path of the resource.
    pub namespace_path: Vec<String>,
}

#[cfg(feature = "remote")]
impl RemoteResourceIdentity {
    /// Construct a resource identity as reported by a remote service.
    pub fn new(
        kind: impl Into<String>,
        name: impl Into<String>,
        namespace_path: Vec<String>,
    ) -> Self {
        Self {
            kind: kind.into(),
            name: name.into(),
            namespace_path,
        }
    }
}

/// Correlation and classification metadata reported by a remote server.
///
/// Fields are absent when the server did not supply them. Diagnostic text is
/// never used to infer an error code or a job identity.
#[cfg(feature = "remote")]
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct RemoteErrorMetadata {
    /// Machine-readable input error code, when available.
    pub error_code: Option<String>,
    /// SQL job that failed, when the request ran in an isolated job.
    pub job_id: Option<String>,
    /// Request executed by the isolated SQL worker.
    pub job_request_id: Option<String>,
    /// Structured resource identity supplied by the server, when available.
    pub resource: Option<RemoteResourceIdentity>,
}

#[cfg(feature = "remote")]
impl RemoteErrorMetadata {
    pub(crate) fn from_headers(headers: &reqwest::header::HeaderMap) -> Self {
        let header = |name| {
            headers
                .get(name)
                .and_then(|value| value.to_str().ok())
                .map(str::to_owned)
        };
        Self {
            error_code: header("x-lancedb-error-code"),
            job_id: header("x-lancedb-sql-job-id"),
            job_request_id: header("x-job-request-id"),
            resource: header("x-lancedb-resource")
                .as_deref()
                .and_then(parse_flight_resource_identity),
        }
    }
}

#[cfg(feature = "remote")]
#[derive(Deserialize)]
struct FlightResourceIdentity {
    kind: String,
    name: String,
    namespace_path: Vec<String>,
}

/// Decode the ASCII-safe structured resource metadata used by Flight SQL.
#[cfg(feature = "remote")]
pub(crate) fn parse_flight_resource_identity(value: &str) -> Option<RemoteResourceIdentity> {
    use base64::Engine as _;

    let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(value)
        .ok()?;
    let identity = serde_json::from_slice::<FlightResourceIdentity>(&bytes).ok()?;
    Some(RemoteResourceIdentity {
        kind: identity.kind,
        name: identity.name,
        namespace_path: identity.namespace_path,
    })
}

#[cfg(feature = "remote")]
#[derive(Debug)]
pub(crate) struct HttpErrorSource {
    message: String,
    details: Option<HttpErrorDetails>,
    metadata: RemoteErrorMetadata,
}

#[cfg(feature = "remote")]
impl HttpErrorSource {
    pub(crate) fn new(
        message: String,
        response_body: Option<&str>,
        metadata: RemoteErrorMetadata,
    ) -> Self {
        let details = response_body.and_then(parse_http_error_details);
        let message = if metadata.error_code.is_some() {
            response_body
                .and_then(|body| serde_json::from_str::<serde_json::Value>(body).ok())
                .and_then(|body| body.get("error")?.as_str().map(str::to_owned))
                .unwrap_or(message)
        } else {
            message
        };
        Self {
            message,
            details,
            metadata,
        }
    }
}

#[cfg(feature = "remote")]
impl Display for HttpErrorSource {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

#[cfg(feature = "remote")]
impl std::error::Error for HttpErrorSource {}

#[cfg(feature = "remote")]
#[derive(Deserialize)]
struct HttpResponseBody {
    error_code: String,
    resource: HttpResponseResource,
}

#[cfg(feature = "remote")]
#[derive(Deserialize)]
struct HttpResponseResource {
    kind: String,
    name: String,
    namespace_path: Vec<String>,
}

#[cfg(feature = "remote")]
fn parse_http_error_details(response_body: &str) -> Option<HttpErrorDetails> {
    let body: HttpResponseBody = serde_json::from_str(response_body).ok()?;
    Some(HttpErrorDetails {
        error_code: body.error_code,
        resource_kind: body.resource.kind,
        resource_name: body.resource.name,
        namespace_path: body.resource.namespace_path,
    })
}

// reqwest's Display omits the timeout cause. Add guidance at display time so
// callers still receive the original typed source and its full error chain.
#[cfg(feature = "remote")]
fn http_timeout_hint(source: &(dyn std::error::Error + 'static)) -> &'static str {
    match source.downcast_ref::<reqwest::Error>() {
        Some(error) if error.is_timeout() && error.is_connect() => {
            "; connection timed out. Check client_config.timeout_config.connect_timeout \
             (LANCE_CLIENT_CONNECT_TIMEOUT) and client_config.timeout_config.timeout \
             (LANCE_CLIENT_TIMEOUT) if configured"
        }
        Some(error) if error.is_timeout() => {
            "; request timed out. For slow uploads or responses, check \
             client_config.timeout_config.read_timeout (LANCE_CLIENT_READ_TIMEOUT) \
             and client_config.timeout_config.timeout (LANCE_CLIENT_TIMEOUT) if configured"
        }
        _ => "",
    }
}

/// The error that ended a streamed response.
///
/// It is shared so that a copy of [`Error::ResponseInterrupted`] keeps it:
/// DataFusion hands one error to every partition that reads the failed input.
/// It dereferences to the underlying error, which is also the next error in the
/// source chain.
#[cfg(feature = "remote")]
#[derive(Debug, Clone)]
pub struct InterruptionSource(Arc<dyn std::error::Error + Send + Sync>);

#[cfg(feature = "remote")]
impl From<BoxError> for InterruptionSource {
    fn from(source: BoxError) -> Self {
        Self(source.into())
    }
}

#[cfg(feature = "remote")]
impl std::ops::Deref for InterruptionSource {
    type Target = dyn std::error::Error + Send + Sync;

    fn deref(&self) -> &Self::Target {
        self.0.as_ref()
    }
}

#[cfg(feature = "remote")]
impl Display for InterruptionSource {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

// Not `Error`: an `Arc` in the source chain hides the error it holds from
// `downcast_ref`, and so would a wrapper that implements `Error`.
#[cfg(feature = "remote")]
impl snafu::AsErrorSource for InterruptionSource {
    fn as_error_source(&self) -> &(dyn std::error::Error + 'static) {
        self.0.as_ref()
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
    /// The stage the job was in, when known.
    pub phase: Option<String>,
    /// A human-readable reason, when known.
    pub message: Option<String>,
    /// User-code traceback, possibly truncated by the server.
    pub traceback: Option<String>,
    /// Whether a retry could clear the failure, when known.
    pub retryable: Option<bool>,
    /// Isolated SQL job that raised this failure, when the server supplies
    /// one across the Flight SQL boundary.
    pub sql_job_id: Option<String>,
    /// The underlying error from a job run in this process or a structured
    /// remote Flight SQL failure.
    pub source: Option<Arc<Error>>,
}

impl JobFailure {
    /// A failure whose only known detail is the error that caused it.
    pub(crate) fn from_source(source: Arc<Error>) -> Self {
        Self {
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
    #[snafu(display("Column '{name}' was not found"))]
    ColumnNotFound { name: String },
    /// A missing query field with schema-qualified candidate paths.
    #[snafu(display("{message}"))]
    FieldNotFound {
        name: String,
        message: String,
        source: BoxError,
    },
    #[snafu(display("Column '{name}' already exists"))]
    ColumnAlreadyExists { name: String },
    #[snafu(display("Column '{name}' is not a computed column"))]
    NotAComputedColumn { name: String },
    #[snafu(display("Table '{name}' is not a materialized view"))]
    NotAMaterializedView { name: String },
    #[snafu(display("Invalid expression for column '{column}': {message}"))]
    InvalidExpression { column: String, message: String },

    #[snafu(display("Table '{name}' already exists"))]
    TableAlreadyExists { name: String },
    #[snafu(display("Unable to created lance dataset at {path}: {source}"))]
    CreateDir {
        path: String,
        source: std::io::Error,
    },
    #[snafu(display("Schema Error: {message}"))]
    Schema { message: String },
    /// A SQL response retaining its gRPC status and server metadata.
    #[cfg(feature = "remote")]
    #[snafu(display("{message} (request_id={request_id})"))]
    Sql {
        message: String,
        request_id: String,
        grpc_code: i32,
        metadata: Box<RemoteErrorMetadata>,
    },
    #[snafu(display("Runtime error: {message}"))]
    Runtime { message: String },
    #[snafu(display("Timeout error: {message}"))]
    Timeout { message: String },
    #[snafu(display("Job{} failed{failure}", job_id.as_ref().map(|id| format!(" {id}")).unwrap_or_default()))]
    JobFailed {
        job_id: Option<String>,
        failure: Box<JobFailure>,
    },
    #[snafu(display("Job{} was cancelled", job_id.as_ref().map(|id| format!(" {id}")).unwrap_or_default()))]
    JobCancelled { job_id: Option<String> },
    #[snafu(display("Job '{job_id}' was not found"))]
    JobNotFound { job_id: String },

    // 3rd party / external errors
    #[snafu(display("object_store error: {source}"))]
    ObjectStore { source: object_store::Error },
    #[snafu(display("lance error: {source}"))]
    Lance { source: lance::Error },
    #[cfg(feature = "remote")]
    #[snafu(display(
        "Http error: (request_id={request_id}) {source}{}",
        http_timeout_hint(source.as_ref())
    ))]
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
        read_failures={read_failures}/{max_read_failures}): {source}{}",
        http_timeout_hint(source.as_ref())
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
    /// A streamed response stopped before the server signalled that it was complete.
    ///
    /// The batches already returned are valid, but the result is incomplete. The
    /// request is not retried, because re-issuing it would return those batches
    /// again. `table_version` is the table version the response reflected, when
    /// the server reported one, so a caller can re-issue the request against it.
    #[cfg(feature = "remote")]
    #[snafu(display(
        "Response for request_id={request_id} interrupted after {batches} batches \
        ({rows} rows): {source}{}",
        http_timeout_hint(&**source)
    ))]
    ResponseInterrupted {
        request_id: String,
        /// Batches returned before the interruption.
        batches: u64,
        /// Rows in those batches.
        rows: u64,
        table_version: Option<u64>,
        source: InterruptionSource,
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
            // DataFusion adds context to an error, and shares one error between
            // the partitions that read a failed input. An error of ours beneath
            // either keeps its type; other errors keep the wrapper's message.
            DataFusionError::Context(_, inner) if find_error(inner.as_ref()).is_some() => {
                (*inner).into()
            }
            DataFusionError::Shared(shared) => match Arc::try_unwrap(shared) {
                Ok(inner) => inner.into(),
                Err(shared) => find_error(shared.as_ref())
                    .and_then(Self::clone_shared)
                    .unwrap_or_else(|| Self::External {
                        source: Box::new(DataFusionError::Shared(shared)),
                    }),
            },
            other => Self::External {
                source: Box::new(other),
            },
        }
    }
}

/// The first error of ours in an error's source chain.
fn find_error<'a>(error: &'a (dyn std::error::Error + 'static)) -> Option<&'a Error> {
    std::iter::successors(Some(error), |error| error.source())
        .find_map(|error| error.downcast_ref::<Error>())
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
            lance::Error::InvalidInput {
                source, location, ..
            } => {
                if let Some(DataFusionError::SchemaError(error, _)) = source
                    .downcast_ref::<DataFusionError>()
                    .map(|error| error.find_root())
                    && let datafusion_common::SchemaError::FieldNotFound { field, .. } =
                        error.as_ref()
                {
                    // Preserve the known field identity before this boundary
                    // turns other input diagnostics into plain messages.
                    log::info!("Query input rejected at {location}: {source}");
                    Self::ColumnNotFound {
                        name: field.name.clone(),
                    }
                } else {
                    Self::InvalidInput {
                        message: source.to_string(),
                    }
                }
            }
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
    /// HTTP status associated with a remote request error.
    ///
    /// Retry wrappers preserve the underlying status. Other error variants do
    /// not represent an HTTP response and return `None`.
    #[cfg(feature = "remote")]
    pub fn status_code(&self) -> Option<reqwest::StatusCode> {
        match self {
            Self::Http { status_code, .. } => *status_code,
            Self::Retry { status_code, .. } => *status_code,
            Self::TableNotFound { source, .. } => {
                source.downcast_ref::<Self>().and_then(Self::status_code)
            }
            Self::JobFailed { failure, .. } => {
                failure.source.as_deref().and_then(Self::status_code)
            }
            _ => None,
        }
    }

    /// Structured resource details reported by a remote HTTP error.
    ///
    /// Older servers do not include these fields, in which case this returns
    /// `None` and callers should treat the error as a generic HTTP failure.
    #[cfg(feature = "remote")]
    pub fn http_error_details(&self) -> Option<&HttpErrorDetails> {
        match self {
            Self::Http { source, .. } => source
                .downcast_ref::<HttpErrorSource>()
                .and_then(|source| source.details.as_ref()),
            Self::Retry { source, .. } => source
                .downcast_ref::<Self>()
                .and_then(Self::http_error_details),
            _ => None,
        }
    }

    /// Classification and correlation metadata supplied by a remote server.
    ///
    /// Unlike parsing [`Self::to_string`], this preserves absence and does not
    /// mistake a user-provided identifier for an error classification.
    ///
    /// ```
    /// # #[cfg(feature = "remote")]
    /// fn reported_job(error: &lancedb::Error) -> Option<&str> {
    ///     error.remote_error_metadata()?.job_id.as_deref()
    /// }
    /// ```
    #[cfg(feature = "remote")]
    pub fn remote_error_metadata(&self) -> Option<&RemoteErrorMetadata> {
        match self {
            Self::Http { source, .. } => source
                .downcast_ref::<HttpErrorSource>()
                .map(|source| &source.metadata),
            Self::Retry { source, .. } => source
                .downcast_ref::<Self>()
                .and_then(Self::remote_error_metadata),
            Self::TableNotFound { source, .. } => source
                .downcast_ref::<Self>()
                .and_then(Self::remote_error_metadata),
            Self::JobFailed { failure, .. } => failure
                .source
                .as_deref()
                .and_then(Self::remote_error_metadata),
            Self::Sql { metadata, .. } => Some(metadata),
            _ => None,
        }
    }

    /// A copy of an error that other partitions of a DataFusion plan still hold.
    /// Only an interrupted response can be copied, because it shares its source.
    fn clone_shared(&self) -> Option<Self> {
        #[cfg(feature = "remote")]
        if let Self::ResponseInterrupted {
            request_id,
            batches,
            rows,
            table_version,
            source,
        } = self
        {
            return Some(Self::ResponseInterrupted {
                request_id: request_id.clone(),
                batches: *batches,
                rows: *rows,
                table_version: *table_version,
                source: source.clone(),
            });
        }
        None
    }

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
    fn missing_filter_column_retains_its_type_across_lance_conversion() {
        let field_error = DataFusionError::SchemaError(
            Box::new(datafusion_common::SchemaError::FieldNotFound {
                field: Box::new(datafusion_common::Column::from_name("missing")),
                valid_fields: vec![datafusion_common::Column::from_name("id")],
            }),
            Box::new(None),
        );
        let source =
            lance::Error::invalid_input_source(Box::new(field_error.context("binding filter")));
        assert!(matches!(Error::from(source), Error::ColumnNotFound { name } if name == "missing"));

        let source = lance::Error::invalid_input("No field named missing");
        assert!(matches!(Error::from(source), Error::InvalidInput { .. }));
    }

    /// DataFusion shares an error between the partitions that read a failed
    /// input, and may add context to it. Whether or not another partition still
    /// holds it, an interrupted response keeps its fields and its source.
    #[cfg(feature = "remote")]
    #[rstest::rstest]
    fn shared_interruption_keeps_its_type_and_source(
        #[values(false, true)] still_held: bool,
        #[values(false, true)] with_context: bool,
    ) {
        let reset = BoxError::from(std::io::Error::from(std::io::ErrorKind::ConnectionReset));
        let interrupted = Error::ResponseInterrupted {
            request_id: "req-1".into(),
            batches: 2,
            rows: 2048,
            table_version: Some(7),
            source: reset.into(),
        };
        let shared = Arc::new(DataFusionError::External(Box::new(interrupted)));
        let held = still_held.then(|| shared.clone());
        let mut error = DataFusionError::Shared(shared);
        if with_context {
            error = error.context("merging query results");
        }

        let error = Error::from(error);

        assert!(
            matches!(
                &error,
                Error::ResponseInterrupted {
                    request_id,
                    batches: 2,
                    rows: 2048,
                    table_version: Some(7),
                    ..
                } if request_id == "req-1"
            ),
            "{error:?}"
        );
        let source = std::error::Error::source(&error).unwrap();
        assert_eq!(
            source.downcast_ref::<std::io::Error>().map(|e| e.kind()),
            Some(std::io::ErrorKind::ConnectionReset)
        );
        drop(held);
    }

    #[test]
    fn other_shared_errors_keep_their_message() {
        let shared = Arc::new(DataFusionError::Execution("boom".into()));
        let _held = shared.clone();

        let error = Error::from(DataFusionError::Shared(shared).context("merging"));

        assert!(matches!(error, Error::External { .. }), "{error:?}");
        assert_eq!(
            error.to_string(),
            "merging\ncaused by\nExecution error: boom"
        );
    }

    #[cfg(feature = "remote")]
    #[test]
    fn non_timeout_http_error_message_is_unchanged() {
        let error = Error::Http {
            source: "server error".into(),
            request_id: "test".into(),
            status_code: Some(reqwest::StatusCode::INTERNAL_SERVER_ERROR),
        };
        assert_eq!(
            error.to_string(),
            "Http error: (request_id=test) server error"
        );
    }

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
