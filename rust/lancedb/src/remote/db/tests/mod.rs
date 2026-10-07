// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use super::{NamespaceHeaderProviderContext, build_cache_key};
use crate::connection::ConnectBuilder;
use crate::database::Database;
use crate::materialized_view::CreateMaterializedViewRequest;
use crate::{
    Connection, Error,
    database::CreateTableMode,
    job::JobEventsRequest,
    remote::{ARROW_STREAM_CONTENT_TYPE, ClientConfig, HeaderProvider, JSON_CONTENT_TYPE},
};
use arrow_array::{Int32Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use lance_namespace_impls::{DynamicContextProvider, OperationInfo};
use rstest::rstest;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};

mod catalog;
mod jobs;
mod namespace;
mod tables;
