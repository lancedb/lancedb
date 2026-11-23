// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use arrow_array::{Array, StringArray, UInt64Array};
use futures::TryStreamExt;
use serde::de::DeserializeOwned;

use super::{cleanup, optimize, prewarm};
use crate::job::Job;
use crate::remote::client::{HttpSend, RestfulLanceDbClient};
use crate::remote::job::RemoteJob;
use crate::sql::Query;
use crate::{Error, Result};

#[derive(Clone, Copy)]
pub(super) enum JobReceipt {
    Compact,
    Prewarm,
    Cleanup,
}

impl JobReceipt {
    fn invalid(self) -> Error {
        let receipt = match self {
            Self::Compact => "OPTIMIZE did not return a single nonempty job_id receipt",
            Self::Prewarm => "PREWARM did not return a single (job_id, table_version) receipt",
            Self::Cleanup => "VACUUM did not return a single nonempty job_id receipt",
        };
        Error::Runtime {
            message: format!(
                "{receipt}; inspect jobs before retrying because work may already have been submitted"
            ),
        }
    }

    /// A registry job can already be running when its SQL response is lost.
    /// Only construct the handle after reading one valid row and the end of the stream.
    pub(super) async fn accept<T, S>(
        self,
        submission: Result<Query>,
        client: &RestfulLanceDbClient<S>,
    ) -> Result<Job<T>>
    where
        T: Clone + DeserializeOwned + Send + Sync + 'static,
        S: HttpSend,
    {
        let map_error = match self {
            Self::Compact => optimize::map_optimize_error,
            Self::Prewarm => prewarm::map_error,
            Self::Cleanup => cleanup::map_error,
        };
        let query = submission.map_err(map_error)?;
        let mut reader = query.reader().await.map_err(map_error)?;
        let mut receipt = None;
        let mut rows = 0usize;
        while let Some(batch) = reader.try_next().await.map_err(map_error)? {
            rows = rows.saturating_add(batch.num_rows());
            if rows == 1 && batch.num_rows() == 1 {
                receipt = Some(batch);
            }
        }
        let batch = receipt
            .filter(|_| rows == 1)
            .ok_or_else(|| self.invalid())?;
        let ids = batch
            .column_by_name("job_id")
            .and_then(|column| column.as_any().downcast_ref::<StringArray>())
            .filter(|ids| !ids.is_null(0) && !ids.value(0).trim().is_empty())
            .ok_or_else(|| self.invalid())?;
        if matches!(self, Self::Prewarm) {
            batch
                .column_by_name("table_version")
                .and_then(|column| column.as_any().downcast_ref::<UInt64Array>())
                .filter(|versions| !versions.is_null(0))
                .ok_or_else(|| self.invalid())?;
        }
        let job_id = ids.value(0).to_owned();
        Ok(Job::new_typed(Box::new(RemoteJob::new(
            client.clone(),
            job_id,
        ))))
    }
}
