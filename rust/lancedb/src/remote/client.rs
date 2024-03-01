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

use std::time::Duration;

use reqwest::{
    header::{HeaderMap, HeaderValue},
    RequestBuilder, Response,
};

use crate::error::{Error, Result};

#[derive(Clone, Debug)]
pub struct RestfulLanceDbClient {
    client: reqwest::Client,
    host: String,
}

impl RestfulLanceDbClient {
    fn default_headers(
        api_key: &str,
        region: &str,
        db_name: &str,
        has_host_override: bool,
    ) -> Result<HeaderMap> {
        let mut headers = HeaderMap::new();
        headers.insert(
            "x-api-key",
            HeaderValue::from_str(api_key).map_err(|_| Error::Http {
                message: "non-ascii api key provided".to_string(),
            })?,
        );
        if region == "local" {
            let host = format!("{}.local.api.lancedb.com", db_name);
            headers.insert(
                "Host",
                HeaderValue::from_str(&host).map_err(|_| Error::Http {
                    message: format!("non-ascii database name '{}' provided", db_name),
                })?,
            );
        }
        if has_host_override {
            headers.insert(
                "x-lancedb-database",
                HeaderValue::from_str(db_name).map_err(|_| Error::Http {
                    message: format!("non-ascii database name '{}' provided", db_name),
                })?,
            );
        }

        Ok(headers)
    }

    pub fn try_new(
        db_url: &str,
        api_key: &str,
        region: &str,
        host_override: Option<String>,
    ) -> Result<Self> {
        let parsed_url = url::Url::parse(db_url)?;
        debug_assert_eq!(parsed_url.scheme(), "db");
        if !parsed_url.has_host() {
            return Err(Error::Http {
                message: format!("Invalid database URL (missing host) '{}'", db_url),
            });
        }
        let db_name = parsed_url.host_str().unwrap();
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .default_headers(Self::default_headers(
                api_key,
                region,
                db_name,
                host_override.is_some(),
            )?)
            .build()?;
        let host = match host_override {
            Some(host_override) => host_override,
            None => format!("https://{}.{}.api.lancedb.com", db_name, region),
        };
        Ok(Self { client, host })
    }

    pub fn get(&self, uri: &str) -> RequestBuilder {
        let full_uri = format!("{}{}", self.host, uri);
        self.client.get(full_uri)
    }

    pub fn post(&self, uri: &str) -> RequestBuilder {
        let full_uri = format!("{}{}", self.host, uri);
        self.client.post(full_uri)
    }

    async fn rsp_to_str(response: Response) -> String {
        let status = response.status();
        response.text().await.unwrap_or_else(|_| status.to_string())
    }

    pub async fn check_response(&self, response: Response) -> Result<Response> {
        let status_int: u16 = u16::from(response.status());
        if (400..500).contains(&status_int) {
            Err(Error::InvalidInput {
                message: Self::rsp_to_str(response).await,
            })
        } else if status_int != 200 {
            Err(Error::Runtime {
                message: Self::rsp_to_str(response).await,
            })
        } else {
            Ok(response)
        }
    }
}
