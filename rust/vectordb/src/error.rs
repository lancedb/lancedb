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
    #[snafu(display("LanceDBError: Invalid table name: {name}"))]
    InvalidTableName { name: String },
    #[snafu(display("LanceDBError: Table '{name}' was not found"))]
    TableNotFound { name: String },
    #[snafu(display("LanceDBError: Table '{name}' already exists"))]
    TableAlreadyExists { name: String },
    #[snafu(display("LanceDBError: Unable to created lance dataset at {path}: {source}"))]
    CreateDir {
        path: String,
        source: std::io::Error,
    },
    #[snafu(display("LanceDBError: {message}"))]
    Store { message: String },
    #[snafu(display("LanceDBError: {message}"))]
    Lance { message: String },
    #[snafu(display("LanceDB Schema Error: {message}"))]
    Schema { message: String },
    #[snafu(display("Runtime error: {message}"))]
    Runtime { message: String },
}

pub type Result<T> = std::result::Result<T, Error>;

impl From<ArrowError> for Error {
    fn from(e: ArrowError) -> Self {
        Self::Lance {
            message: e.to_string(),
        }
    }
}

impl From<lance::Error> for Error {
    fn from(e: lance::Error) -> Self {
        Self::Lance {
            message: e.to_string(),
        }
    }
}

impl From<object_store::Error> for Error {
    fn from(e: object_store::Error) -> Self {
        Self::Store {
            message: e.to_string(),
        }
    }
}

impl From<object_store::path::Error> for Error {
    fn from(e: object_store::path::Error) -> Self {
        Self::Store {
            message: e.to_string(),
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
