// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#![doc = include_str!("../README.md")]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![cfg_attr(docsrs, doc(auto_cfg))]
#![deny(missing_docs)]

mod backend;
mod config;
mod copier;
mod core;
mod deleter;
mod lister;
mod reader;
mod writer;

pub use backend::S3Builder as S3;
pub use config::S3Config;

/// URI scheme used for service registration and scheme-driven construction.
pub const S3_SCHEME: &str = "s3";

/// Register this service's URI scheme or schemes with an operator registry.
///
/// Registration enables scheme-driven construction through
/// [`opendal_core::Operator::from_uri`] and
/// [`opendal_core::Operator::via_iter`]. Direct construction through
/// [`opendal_core::Operator::new`] does not require registration.
pub fn register_s3_service(registry: &opendal_core::OperatorRegistry) {
    registry.register::<S3>(S3_SCHEME);
}
