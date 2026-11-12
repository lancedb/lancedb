// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use std::future::{Future, IntoFuture};
use std::pin::Pin;
use std::sync::Arc;

use crate::Result;
use crate::database::Database;
use crate::utils::validate_secret_reference;

/// Configure creation of a named Secret with [`super::Connection::create_secret`].
#[must_use]
pub struct CreateSecretBuilder {
    database: Arc<dyn Database>,
    name: String,
    value: String,
    namespace_path: Vec<String>,
    exist_ok: bool,
}

impl CreateSecretBuilder {
    pub(super) fn new(
        database: Arc<dyn Database>,
        name: String,
        value: String,
        namespace_path: Vec<String>,
    ) -> Self {
        Self {
            database,
            name,
            value,
            namespace_path,
            exist_ok: false,
        }
    }

    /// Succeed without changing the stored value when the Secret already exists.
    /// Defaults to false, which reports an error for an existing name.
    pub fn exist_ok(mut self, exist_ok: bool) -> Self {
        self.exist_ok = exist_ok;
        self
    }

    /// Create the Secret, preserving any existing value when `exist_ok` is true.
    pub async fn execute(self) -> Result<()> {
        validate_secret_reference(&self.name, &self.namespace_path)?;
        self.database
            .create_secret(&self.name, &self.value, &self.namespace_path, self.exist_ok)
            .await
    }
}

impl IntoFuture for CreateSecretBuilder {
    type Output = Result<()>;
    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(self.execute())
    }
}
