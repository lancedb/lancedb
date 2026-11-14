// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use std::future::{Future, IntoFuture};
use std::pin::Pin;
use std::sync::Arc;

use crate::Result;
use crate::database::Database;
use crate::utils::validate_view_reference;
use crate::view::ViewDescription;

/// Configure creation of a view with [`super::Connection::create_view`].
#[must_use]
pub struct CreateViewBuilder {
    database: Arc<dyn Database>,
    name: String,
    query: String,
    namespace_path: Vec<String>,
    exist_ok: bool,
}

impl CreateViewBuilder {
    pub(super) fn new(
        database: Arc<dyn Database>,
        name: String,
        query: String,
        namespace_path: Vec<String>,
    ) -> Self {
        Self {
            database,
            name,
            query,
            namespace_path,
            exist_ok: false,
        }
    }

    /// Return the existing view without changing its definition if it exists.
    /// Defaults to false, which reports an error for an existing name.
    pub fn exist_ok(mut self, exist_ok: bool) -> Self {
        self.exist_ok = exist_ok;
        self
    }

    /// Create the view or return the existing view when `exist_ok` is true.
    pub async fn execute(self) -> Result<ViewDescription> {
        validate_view_reference(&self.name, &self.namespace_path)?;
        self.database
            .create_view(&self.name, &self.query, &self.namespace_path, self.exist_ok)
            .await
    }
}

impl IntoFuture for CreateViewBuilder {
    type Output = Result<ViewDescription>;
    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(self.execute())
    }
}
