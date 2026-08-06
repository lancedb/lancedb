// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::sync::Arc;

use arrow_array::RecordBatchReader;
use datafusion::{
    execution::SendableRecordBatchStream,
    physical_plan::{ExecutionPlan, stream::RecordBatchStreamAdapter},
};
use datafusion_common::DataFusionError;
use futures::TryStreamExt;
use lance_core::Error;
use lance_datagen::{BatchCount, BatchGeneratorBuilder, ByteCount, RoundingBehavior, RowCount};

use crate::exec::OneShotExec;

pub trait DatafusionDatagenExt {
    fn into_df_stream(
        self,
        batch_size: RowCount,
        num_batches: BatchCount,
    ) -> SendableRecordBatchStream;

    fn into_df_stream_bytes(
        self,
        batch_size: ByteCount,
        num_batches: BatchCount,
        rounding_behavior: RoundingBehavior,
    ) -> Result<SendableRecordBatchStream, Error>;

    fn into_df_exec(self, batch_size: RowCount, num_batches: BatchCount) -> Arc<dyn ExecutionPlan>;
}

impl DatafusionDatagenExt for BatchGeneratorBuilder {
    fn into_df_stream(
        self,
        batch_size: RowCount,
        num_batches: BatchCount,
    ) -> SendableRecordBatchStream {
        let (stream, schema) = self.into_reader_stream(batch_size, num_batches);
        let stream = stream.map_err(DataFusionError::from);
        Box::pin(RecordBatchStreamAdapter::new(schema, stream))
    }

    fn into_df_stream_bytes(
        self,
        batch_size: ByteCount,
        num_batches: BatchCount,
        rounding_behavior: RoundingBehavior,
    ) -> Result<SendableRecordBatchStream, Error> {
        let stream = self.into_reader_bytes(batch_size, num_batches, rounding_behavior)?;
        let schema = stream.schema();
        let stream = futures::stream::iter(stream).map_err(DataFusionError::from);
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }

    fn into_df_exec(self, batch_size: RowCount, num_batches: BatchCount) -> Arc<dyn ExecutionPlan> {
        let stream = self.into_df_stream(batch_size, num_batches);
        Arc::new(OneShotExec::new(stream))
    }
}
