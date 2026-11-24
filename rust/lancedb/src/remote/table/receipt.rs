// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use arrow_array::{Array, UInt64Array};
use serde::de::DeserializeOwned;

use super::{cleanup, optimize, prewarm};
use crate::job::Job;
use crate::remote::client::{HttpSend, RestfulLanceDbClient};
use crate::remote::job::RemoteJob;
use crate::remote::sql::receipt::ReceiptRow;
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
        let row = ReceiptRow::read(&query)
            .await
            .map_err(map_error)?
            .ok_or_else(|| self.invalid())?;
        let Ok(Some(job_id)) = row.job_id() else {
            return Err(self.invalid());
        };
        if matches!(self, Self::Prewarm) {
            row.column("table_version")
                .and_then(|column| column.as_any().downcast_ref::<UInt64Array>())
                .filter(|versions| !versions.is_null(0))
                .ok_or_else(|| self.invalid())?;
        }
        Ok(Job::new_typed(Box::new(RemoteJob::new(
            client.clone(),
            job_id.to_owned(),
        ))))
    }
}
