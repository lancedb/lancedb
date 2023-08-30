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
    #[snafu(display("Bad query: {message}"))]
    InvalidQuery { message: String },
}

impl Error {
    pub fn invalid_table_name(name: &str) -> Self {
        Self::InvalidTableName {
            name: name.to_string(),
        }
    }

    pub fn table_not_found(name: &str) -> Self {
        Self::TableNotFound {
            name: name.to_string(),
        }
    }

    pub fn table_already_exists(name: &str) -> Self {
        Self::TableAlreadyExists {
            name: name.to_string(),
        }
    }

    pub fn invalid_query(message: &str) -> Self {
        Self::InvalidQuery {
            message: message.to_string(),
        }
    }

    pub fn create_dir(path: &str, source: std::io::Error) -> Self {
        Self::CreateDir {
            path: path.to_string(),
            source,
        }
    }
}
pub type Result<T> = std::result::Result<T, Error>;

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
