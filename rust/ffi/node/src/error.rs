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

use arrow_schema::ArrowError;
use neon::context::Context;
use neon::prelude::NeonResult;
use snafu::Snafu;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum Error {
    #[snafu(display("column '{name}' is missing"))]
    MissingColumn { name: String },
    #[snafu(display("{name}: {message}"))]
    OutOfRange { name: String, message: String },
    #[snafu(display("{index_type} is not a valid index type"))]
    InvalidIndexType { index_type: String },

    #[snafu(display("{message}"))]
    LanceDB { message: String },
    #[snafu(display("{message}"))]
    Neon { message: String },
}

pub type Result<T> = std::result::Result<T, Error>;

impl From<vectordb::error::Error> for Error {
    fn from(e: vectordb::error::Error) -> Self {
        Self::LanceDB {
            message: e.to_string(),
        }
    }
}

impl From<lance::Error> for Error {
    fn from(e: lance::Error) -> Self {
        Self::LanceDB {
            message: e.to_string(),
        }
    }
}

impl From<ArrowError> for Error {
    fn from(value: ArrowError) -> Self {
        Self::LanceDB {
            message: value.to_string(),
        }
    }
}

impl From<neon::result::Throw> for Error {
    fn from(value: neon::result::Throw) -> Self {
        Self::Neon {
            message: value.to_string(),
        }
    }
}

impl<T> From<std::sync::mpsc::SendError<T>> for Error {
    fn from(value: std::sync::mpsc::SendError<T>) -> Self {
        Self::Neon {
            message: value.to_string(),
        }
    }
}

/// ResultExt is used to transform a [`Result`] into a [`NeonResult`],
/// so it can be returned as a JavaScript error
/// Copied from [Neon](https://github.com/neon-bindings/neon/blob/4c2e455a9e6814f1ba0178616d63caec7f4df317/crates/neon/src/result/mod.rs#L88)
pub trait ResultExt<T> {
    fn or_throw<'a, C: Context<'a>>(self, cx: &mut C) -> NeonResult<T>;
}

/// Implement ResultExt for the std Result so it can be used any Result type
impl<T, E> ResultExt<T> for std::result::Result<T, E>
where
    E: std::fmt::Display,
{
    fn or_throw<'a, C: Context<'a>>(self, cx: &mut C) -> NeonResult<T> {
        match self {
            Ok(value) => Ok(value),
            Err(error) => cx.throw_error(error.to_string()),
        }
    }
}
