// Copyright 2024 Lance Developers.
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

use connection::Connection;
use napi_derive::*;

mod connection;
mod index;
mod iterator;
mod query;
mod table;

#[napi(object)]
pub struct ConnectionOptions {
    pub uri: String,
    pub api_key: Option<String>,
    pub host_override: Option<String>,
}

/// Write mode for writing a table.
#[napi(string_enum)]
pub enum WriteMode {
    Create,
    Append,
    Overwrite,
}

/// Write options when creating a Table.
#[napi(object)]
pub struct WriteOptions {
    pub mode: Option<WriteMode>,
}

#[napi]
pub async fn connect(options: ConnectionOptions) -> napi::Result<Connection> {
    Connection::new(options.uri.clone()).await
}
