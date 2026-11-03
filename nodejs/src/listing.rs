// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use crate::error::NapiErrorExt;
use futures::{StreamExt, future::poll_fn};
use lancedb::listing::{Listing, ListingOptions};
use napi_derive::napi;
use std::sync::Mutex;

pub fn options(page_token: Option<String>, page_limit: Option<u32>) -> ListingOptions {
    let mut options = ListingOptions::default();
    options.page_token = page_token;
    options.page_limit = page_limit;
    options
}

#[napi]
pub struct NameListing {
    inner: Mutex<Listing<String>>,
    next_lock: futures::lock::Mutex<()>,
}
impl NameListing {
    pub fn new(inner: Listing<String>) -> Self {
        Self {
            inner: Mutex::new(inner),
            next_lock: futures::lock::Mutex::new(()),
        }
    }
}
#[napi]
impl NameListing {
    #[napi]
    pub fn num_page_results(&self) -> u32 {
        self.inner.lock().unwrap().num_page_results() as u32
    }
    #[napi]
    pub fn page_token(&self) -> Option<String> {
        self.inner.lock().unwrap().page_token().map(str::to_owned)
    }
    #[napi]
    pub async fn next(&self) -> napi::Result<Option<String>> {
        let _guard = self.next_lock.lock().await;
        let value = poll_fn(|cx| self.inner.lock().unwrap().poll_next_unpin(cx))
            .await
            .transpose()
            .default_error()?;
        Ok(value)
    }
}

#[napi]
pub struct JobListing {
    inner: Mutex<Listing<lancedb::database::JobInfo>>,
    next_lock: futures::lock::Mutex<()>,
}
impl JobListing {
    pub fn new(inner: Listing<lancedb::database::JobInfo>) -> Self {
        Self {
            inner: Mutex::new(inner),
            next_lock: futures::lock::Mutex::new(()),
        }
    }
}
#[napi]
impl JobListing {
    #[napi]
    pub fn num_page_results(&self) -> u32 {
        self.inner.lock().unwrap().num_page_results() as u32
    }
    #[napi]
    pub fn page_token(&self) -> Option<String> {
        self.inner.lock().unwrap().page_token().map(str::to_owned)
    }
    #[napi]
    pub async fn next(&self) -> napi::Result<Option<crate::job::JobInfo>> {
        let _guard = self.next_lock.lock().await;
        let value = poll_fn(|cx| self.inner.lock().unwrap().poll_next_unpin(cx))
            .await
            .transpose()
            .default_error()?;
        Ok(value.map(|value| value.into()))
    }
}
