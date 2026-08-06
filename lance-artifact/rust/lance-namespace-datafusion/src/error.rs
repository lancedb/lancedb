// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use datafusion::error::DataFusionError;
use lance::Error;

/// Converts a lance error into a datafusion error.
pub fn to_datafusion_error(error: Error) -> DataFusionError {
    DataFusionError::External(error.into())
}
