// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use core::fmt;
use std::sync::{Arc, Mutex};

use datafusion_common::{DataFusionError, Result as DFResult, Statistics, stats::Precision};
use datafusion_execution::{SendableRecordBatchStream, TaskContext};
use datafusion_physical_expr::{EquivalenceProperties, Partitioning};
use datafusion_physical_plan::stream::RecordBatchStreamAdapter;
use datafusion_physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, execution_plan::EmissionType,
};
use futures::TryStreamExt;

use crate::table::write_progress::WriteProgressTracker;
use crate::{arrow::SendableRecordBatchStreamExt, data::scannable::Scannable};

pub(crate) struct ScannableExec {
    // We don't require Scannable to be Sync, so we wrap it in a Mutex to allow safe concurrent access.
    source: Mutex<Box<dyn Scannable>>,
    num_rows: Option<usize>,
    properties: Arc<PlanProperties>,
    tracker: Option<Arc<WriteProgressTracker>>,
}

impl std::fmt::Debug for ScannableExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ScannableExec")
            .field("schema", &self.schema())
            .field("num_rows", &self.num_rows)
            .finish()
    }
}

impl ScannableExec {
    pub fn new(source: Box<dyn Scannable>, tracker: Option<Arc<WriteProgressTracker>>) -> Self {
        let schema = source.schema();
        let eq_properties = EquivalenceProperties::new(schema);
        let properties = PlanProperties::new(
            eq_properties,
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            datafusion_physical_plan::execution_plan::Boundedness::Bounded,
        );

        let num_rows = source.num_rows();
        let source = Mutex::new(source);
        Self {
            source,
            num_rows,
            properties: Arc::new(properties),
            tracker,
        }
    }
}

impl DisplayAs for ScannableExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ScannableExec: num_rows={:?}", self.num_rows)
    }
}

impl ExecutionPlan for ScannableExec {
    fn name(&self) -> &str {
        "ScannableExec"
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        if !children.is_empty() {
            return Err(DataFusionError::Internal(
                "ScannableExec does not have children".to_string(),
            ));
        }
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Internal(format!(
                "ScannableExec only supports partition 0, got {}",
                partition
            )));
        }

        let stream = match self.source.lock() {
            Ok(mut guard) => guard.scan_as_stream(),
            Err(poison) => poison.into_inner().scan_as_stream(),
        };

        let tracker = self.tracker.clone();
        let stream = stream.into_df_stream().map_ok(move |batch| {
            if let Some(ref t) = tracker {
                t.record_batch(batch.num_rows(), batch.get_array_memory_size());
            }
            batch
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            stream,
        )))
    }

    fn partition_statistics(&self, _partition: Option<usize>) -> DFResult<Statistics> {
        Ok(Statistics {
            num_rows: self
                .num_rows
                .map(Precision::Exact)
                .unwrap_or(Precision::Absent),
            total_byte_size: Precision::Absent,
            column_statistics: vec![],
        })
    }
}
