// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use std::sync::Arc;

use arrow_array::{BooleanArray, UInt64Array};
use arrow_select::filter::filter_record_batch;
use datafusion::prelude::{SessionConfig, SessionContext, col};
use datafusion_common::{Result, plan_err};
use datafusion_execution::{SendableRecordBatchStream, TaskContext};
use datafusion_physical_expr::{EquivalenceProperties, Partitioning};
use datafusion_physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties,
    coalesce_partitions::CoalescePartitionsExec,
    execution_plan::{Boundedness, EmissionType},
    stream::RecordBatchStreamAdapter,
};
use futures::{TryStreamExt, stream};
use lance_datafusion::exec::SessionContextExt;

#[derive(Debug)]
pub(super) struct CanonicalPairsExec {
    input: Arc<dyn ExecutionPlan>,
    properties: Arc<PlanProperties>,
}
impl CanonicalPairsExec {
    pub fn new(input: Arc<dyn ExecutionPlan>) -> Self {
        Self {
            properties: Arc::new(PlanProperties::new(
                EquivalenceProperties::new(input.schema()),
                Partitioning::UnknownPartitioning(1),
                EmissionType::Final,
                Boundedness::Bounded,
            )),
            input,
        }
    }
}
impl DisplayAs for CanonicalPairsExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("CanonicalPairsExec")
    }
}
impl ExecutionPlan for CanonicalPairsExec {
    fn name(&self) -> &str {
        "CanonicalPairsExec"
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return plan_err!("canonical pairs requires one input");
        }
        Ok(Arc::new(Self::new(children[0].clone())))
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return plan_err!("invalid canonical pair partition");
        }
        let input = CoalescePartitionsExec::new(self.input.clone()).execute(0, context.clone())?;
        let schema = self.schema();
        let stream = stream::once(async move {
            let ctx = SessionContext::new_with_config_rt(
                SessionConfig::default(),
                context.runtime_env().clone(),
            );
            let sorted = ctx
                .read_one_shot(input)?
                .sort_by(vec![col("row_id_a"), col("row_id_b"), col("distance")])?
                .execute_stream()
                .await?;
            Ok::<_, datafusion_common::DataFusionError>(stream::try_unfold(
                (sorted, None),
                |(mut sorted, mut previous)| async move {
                    match sorted.try_next().await? {
                        None => Ok(None),
                        Some(batch) => {
                            let a = batch
                                .column(0)
                                .as_any()
                                .downcast_ref::<UInt64Array>()
                                .expect("native pair ID");
                            let b = batch
                                .column(1)
                                .as_any()
                                .downcast_ref::<UInt64Array>()
                                .expect("native pair ID");
                            let keep = BooleanArray::from_iter_values(
                                a.values().iter().zip(b.values()).map(|(&a, &b)| {
                                    let pair = (a, b);
                                    let keep = previous != Some(pair);
                                    previous = Some(pair);
                                    keep
                                }),
                            );
                            Ok(Some((
                                filter_record_batch(&batch, &keep)?,
                                (sorted, previous),
                            )))
                        }
                    }
                },
            ))
        })
        .try_flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}
