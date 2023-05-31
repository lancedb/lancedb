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

#[derive(Debug)]
pub enum Error {
    IO(String),
    Lance(String),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let (catalog, message) = match self {
            Self::IO(s) => ("I/O", s.as_str()),
            Self::Lance(s) => ("Lance", s.as_str()),
        };
        write!(f, "LanceDBError({catalog}): {message}")
    }
}

pub type Result<T> = std::result::Result<T, Error>;

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Self::IO(e.to_string())
    }
}

impl From<lance::Error> for Error {
    fn from(e: lance::Error) -> Self {
        Self::Lance(e.to_string())
    }
}

impl From<object_store::Error> for Error {
    fn from(e: object_store::Error) -> Self {
        Self::IO(e.to_string())
    }
}

impl From<object_store::path::Error> for Error {
    fn from(e: object_store::path::Error) -> Self {
        Self::IO(e.to_string())
    }
}
