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

//! This module contains a remote client for a LanceDB server.  This is used
//! to communicate with LanceDB cloud.  It can also serve as an example for
//! building client/server applications with LanceDB or as a client for some
//! other custom LanceDB service.

pub(crate) mod client;
pub(crate) mod db;
pub(crate) mod table;
pub(crate) mod util;

const ARROW_STREAM_CONTENT_TYPE: &str = "application/vnd.apache.arrow.stream";
const JSON_CONTENT_TYPE: &str = "application/json";

pub use client::{ClientConfig, RetryConfig, TimeoutConfig};
