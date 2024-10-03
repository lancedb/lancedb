// Copyright 2023 Lance Developers.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::PoisonError;

use arrow_schema::ArrowError;
use snafu::Snafu;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum Error {
    #[snafu(display("Invalid table name (\"{name}\"): {reason}"))]
    InvalidTableName { name: String, reason: String },
    #[snafu(display("Invalid input, {message}"))]
    InvalidInput { message: String },
    #[snafu(display("Table '{name}' was not found"))]
    TableNotFound { name: String },
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
        Self::Arrow { source }
    }
}

impl From<lance::Error> for Error {
    fn from(source: lance::Error) -> Self {
        // TODO: Once Lance is changed to preserve ObjectStore, DataFusion, and Arrow errors, we can
        // pass those variants through here as well.
        Self::Lance { source }
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
