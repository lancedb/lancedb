// Copyright 2024 LanceDB Developers.
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

use std::sync::Arc;

use async_trait::async_trait;
use reqwest::header::CONTENT_TYPE;
use serde::Deserialize;
use tokio::task::spawn_blocking;

use crate::connection::{
    ConnectionInternal, CreateTableBuilder, OpenTableBuilder, TableNamesBuilder,
};
use crate::error::Result;
use crate::Table;

use super::client::RestfulLanceDbClient;
use super::table::RemoteTable;
use super::util::batches_to_ipc_bytes;

const ARROW_STREAM_CONTENT_TYPE: &str = "application/vnd.apache.arrow.stream";

#[derive(Deserialize)]
struct ListTablesResponse {
    tables: Vec<String>,
}

#[derive(Debug)]
pub struct RemoteDatabase {
    client: RestfulLanceDbClient,
}

impl RemoteDatabase {
    pub fn try_new(
        uri: &str,
        api_key: &str,
        region: &str,
        host_override: Option<String>,
    ) -> Result<Self> {
        let client = RestfulLanceDbClient::try_new(uri, api_key, region, host_override)?;
        Ok(Self { client })
    }
}

impl std::fmt::Display for RemoteDatabase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "RemoteDatabase(host={})", self.client.host())
    }
}

#[async_trait]
impl ConnectionInternal for RemoteDatabase {
    async fn table_names(&self, options: TableNamesBuilder) -> Result<Vec<String>> {
        let mut req = self.client.get("/v1/table/");
        if let Some(limit) = options.limit {
            req = req.query(&[("limit", limit)]);
        }
        if let Some(start_after) = options.start_after {
            req = req.query(&[("page_token", start_after)]);
        }
        let rsp = req.send().await?;
        let rsp = self.client.check_response(rsp).await?;
        Ok(rsp.json::<ListTablesResponse>().await?.tables)
    }

    async fn do_create_table(&self, options: CreateTableBuilder<true>) -> Result<Table> {
        let data = options.data.unwrap();
        // TODO: https://github.com/lancedb/lancedb/issues/1026
        // We should accept data from an async source.  In the meantime, spawn this as blocking
        // to make sure we don't block the tokio runtime if the source is slow.
        let data_buffer = spawn_blocking(move || batches_to_ipc_bytes(data))
            .await
            .unwrap()?;

        self.client
            .post(&format!("/v1/table/{}/create", options.name))
            .body(data_buffer)
            .header(CONTENT_TYPE, ARROW_STREAM_CONTENT_TYPE)
            // This is currently expected by LanceDb cloud but will be removed soon.
            .header("x-request-id", "na")
            .send()
            .await?;

        Ok(Table::new(Arc::new(RemoteTable::new(
            self.client.clone(),
            options.name,
        ))))
    }

    async fn do_open_table(&self, _options: OpenTableBuilder) -> Result<Table> {
        todo!()
    }

    async fn drop_table(&self, _name: &str) -> Result<()> {
        todo!()
    }

    async fn drop_db(&self) -> Result<()> {
        todo!()
    }
}
