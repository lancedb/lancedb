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

use async_trait::async_trait;
use serde::Deserialize;

use crate::connection::{ConnectionInternal, CreateTableBuilder, OpenTableBuilder};
use crate::error::Result;
use crate::TableRef;

use super::client::RestfulLanceDbClient;

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

#[async_trait]
impl ConnectionInternal for RemoteDatabase {
    async fn table_names(&self) -> Result<Vec<String>> {
        let rsp = self
            .client
            .get("/v1/table/")
            .query(&[("limit", 10)])
            .query(&[("page_token", "")])
            .send()
            .await?;
        let rsp = self.client.check_response(rsp).await?;
        Ok(rsp.json::<ListTablesResponse>().await?.tables)
    }

    async fn do_create_table(&self, _options: CreateTableBuilder<true>) -> Result<TableRef> {
        todo!()
    }

    async fn do_open_table(&self, _options: OpenTableBuilder) -> Result<TableRef> {
        todo!()
    }

    async fn drop_table(&self, _name: &str) -> Result<()> {
        todo!()
    }

    async fn drop_db(&self) -> Result<()> {
        todo!()
    }
}
