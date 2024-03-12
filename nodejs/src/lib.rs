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
mod error;
mod index;
mod iterator;
mod query;
mod table;

#[napi(object)]
#[derive(Debug)]
pub struct ConnectionOptions {
    pub api_key: Option<String>,
    pub host_override: Option<String>,
    /// (For LanceDB OSS only): The interval, in seconds, at which to check for
    /// updates to the table from other processes. If None, then consistency is not
    /// checked. For performance reasons, this is the default. For strong
    /// consistency, set this to zero seconds. Then every read will check for
    /// updates from other processes. As a compromise, you can set this to a
    /// non-zero value for eventual consistency. If more than that interval
    /// has passed since the last check, then the table will be checked for updates.
    /// Note: this consistency only applies to read operations. Write operations are
    /// always consistent.
    pub read_consistency_interval: Option<f64>,
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
pub async fn connect(uri: String, options: ConnectionOptions) -> napi::Result<Connection> {
    Connection::new(uri, options).await
}
