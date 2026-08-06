// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! BloomFilterGuardExec - Guards child execution with bloom filter check.
//!
//! Used in point lookup queries to skip generations that definitely don't contain the key.

use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use datafusion::error::Result as DFResult;
use datafusion::execution::TaskContext;
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
    SendableRecordBatchStream,
};
use futures::Stream;
use lance_core::utils::bloomfilter::sbbf::Sbbf;

/// Guards a child execution node with a bloom filter check.
///
/// Given a primary key hash, checks the bloom filter before executing the child.
/// If the bloom filter returns negative (key definitely not present), returns
/// empty without executing the child. If the bloom filter returns positive
/// (key may be present), executes the child normally.
///
/// # Use Case
///
/// For point lookup in LSM tree:
/// - Check bloom filter of each generation before scanning
/// - Skip generations that definitely don't contain the key
/// - Reduces I/O by avoiding unnecessary scans
///
/// # Example
///
/// ```text
/// CoalesceFirstExec
///   BloomFilterGuardExec: gen3, pk_hash=12345
///     GlobalLimitExec: limit=1 (gen3)
///   BloomFilterGuardExec: gen2, pk_hash=12345
///     GlobalLimitExec: limit=1 (gen2)
///   GlobalLimitExec: limit=1 (base_table)
/// ```
#[derive(Debug)]
pub struct BloomFilterGuardExec {
    /// Child execution plan to conditionally execute.
    input: Arc<dyn ExecutionPlan>,
    /// Bloom filter to check.
    bloom_filter: Arc<Sbbf>,
    /// Primary key hash to check.
    pk_hash: u64,
    /// Generation number (for display purposes).
    generation: u64,
    /// Output schema.
    schema: SchemaRef,
    /// Plan properties.
    properties: Arc<PlanProperties>,
}

impl BloomFilterGuardExec {
    /// Create a new BloomFilterGuardExec.
    ///
    /// # Arguments
    ///
    /// * `input` - Child plan to conditionally execute
    /// * `bloom_filter` - Bloom filter to check
    /// * `pk_hash` - Primary key hash to check
    /// * `generation` - Generation number (for display)
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        bloom_filter: Arc<Sbbf>,
        pk_hash: u64,
        generation: u64,
    ) -> Self {
        let schema = input.schema();

        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(1),
            input.pipeline_behavior(),
            input.boundedness(),
        ));

        Self {
            input,
            bloom_filter,
            pk_hash,
            generation,
            schema,
            properties,
        }
    }

    /// Check if the key might be in this generation.
    pub fn might_contain(&self) -> bool {
        self.bloom_filter.check_hash(self.pk_hash)
    }

    /// Get the generation number.
    pub fn generation(&self) -> u64 {
        self.generation
    }

    /// Get the primary key hash.
    pub fn pk_hash(&self) -> u64 {
        self.pk_hash
    }
}

impl DisplayAs for BloomFilterGuardExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default
            | DisplayFormatType::Verbose
            | DisplayFormatType::TreeRender => {
                write!(
                    f,
                    "BloomFilterGuardExec: gen={}, pk_hash={}",
                    self.generation, self.pk_hash
                )
            }
        }
    }
}

impl ExecutionPlan for BloomFilterGuardExec {
    fn name(&self) -> &str {
        "BloomFilterGuardExec"
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
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
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(datafusion::error::DataFusionError::Internal(
                "BloomFilterGuardExec requires exactly one child".to_string(),
            ));
        }
        Ok(Arc::new(Self::new(
            children[0].clone(),
            self.bloom_filter.clone(),
            self.pk_hash,
            self.generation,
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        if !self.might_contain() {
            return Ok(Box::pin(EmptyStream::new(self.schema.clone())));
        }
        self.input.execute(partition, context)
    }
}

/// Empty stream that returns no batches.
struct EmptyStream {
    schema: SchemaRef,
}

impl EmptyStream {
    fn new(schema: SchemaRef) -> Self {
        Self { schema }
    }
}

impl Stream for EmptyStream {
    type Item = DFResult<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Ready(None)
    }
}

impl datafusion::physical_plan::RecordBatchStream for EmptyStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

/// Compute hash for a primary key value.
///
/// Must stay byte-for-byte consistent with [`super::compute_pk_hash`]: for each
/// scalar, hash `is_null` first, then hash the inner value only when not-null.
/// This includes the typed Option(None) branches — they represent a NULL of a
/// known type and must hash the same as a row-side NULL of the same column.
pub fn compute_pk_hash_from_scalars(values: &[datafusion::common::ScalarValue]) -> u64 {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    let mut hasher = DefaultHasher::new();

    fn hash_opt<T: Hash>(hasher: &mut DefaultHasher, v: &Option<T>) {
        v.is_none().hash(hasher);
        if let Some(val) = v {
            val.hash(hasher);
        }
    }

    for value in values {
        match value {
            datafusion::common::ScalarValue::Null => {
                true.hash(&mut hasher); // is_null = true
            }
            datafusion::common::ScalarValue::Int8(v) => hash_opt(&mut hasher, v),
            datafusion::common::ScalarValue::Int16(v) => hash_opt(&mut hasher, v),
            datafusion::common::ScalarValue::Int32(v) => hash_opt(&mut hasher, v),
            datafusion::common::ScalarValue::Int64(v) => hash_opt(&mut hasher, v),
            datafusion::common::ScalarValue::UInt8(v) => hash_opt(&mut hasher, v),
            datafusion::common::ScalarValue::UInt16(v) => hash_opt(&mut hasher, v),
            datafusion::common::ScalarValue::UInt32(v) => hash_opt(&mut hasher, v),
            datafusion::common::ScalarValue::UInt64(v) => hash_opt(&mut hasher, v),
            datafusion::common::ScalarValue::Boolean(v) => hash_opt(&mut hasher, v),
            datafusion::common::ScalarValue::Utf8(v)
            | datafusion::common::ScalarValue::LargeUtf8(v) => hash_opt(&mut hasher, v),
            datafusion::common::ScalarValue::Binary(v)
            | datafusion::common::ScalarValue::LargeBinary(v) => hash_opt(&mut hasher, v),
            // Unsupported types: validated out at the scanner boundary, but
            // distinguish by value rather than collapse if reached.
            _ => {
                false.hash(&mut hasher);
                format!("{:?}", value).hash(&mut hasher);
            }
        }
    }

    hasher.finish()
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int32Array, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use datafusion::prelude::SessionContext;
    use datafusion_physical_plan::test::TestMemoryExec;
    use futures::TryStreamExt;

    fn create_test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]))
    }

    fn create_test_batch(schema: &Schema, ids: &[i32]) -> RecordBatch {
        let names: Vec<String> = ids.iter().map(|id| format!("name_{}", id)).collect();
        RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![
                Arc::new(Int32Array::from(ids.to_vec())),
                Arc::new(StringArray::from(names)),
            ],
        )
        .unwrap()
    }

    fn create_bloom_filter_with_hash(hash: u64) -> Arc<Sbbf> {
        let mut bf = Sbbf::with_ndv_fpp(100, 0.01).unwrap();
        bf.insert_hash(hash);
        Arc::new(bf)
    }

    #[tokio::test]
    async fn test_bloom_guard_passes_when_key_present() {
        let schema = create_test_schema();
        let batch = create_test_batch(&schema, &[1, 2, 3]);

        let pk_hash =
            compute_pk_hash_from_scalars(&[datafusion::common::ScalarValue::Int32(Some(1))]);
        let bf = create_bloom_filter_with_hash(pk_hash);

        let input = TestMemoryExec::try_new_exec(&[vec![batch]], schema.clone(), None).unwrap();
        let guard = BloomFilterGuardExec::new(input, bf, pk_hash, 1);

        assert!(guard.might_contain());

        let ctx = SessionContext::new();
        let stream = guard.execute(0, ctx.task_ctx()).unwrap();
        let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();

        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 3);
    }

    #[tokio::test]
    async fn test_bloom_guard_skips_when_key_absent() {
        let schema = create_test_schema();
        let batch = create_test_batch(&schema, &[1, 2, 3]);

        // Create bloom filter with different hash
        let bf_hash =
            compute_pk_hash_from_scalars(&[datafusion::common::ScalarValue::Int32(Some(999))]);
        let bf = create_bloom_filter_with_hash(bf_hash);

        // Query for a different key
        let query_hash =
            compute_pk_hash_from_scalars(&[datafusion::common::ScalarValue::Int32(Some(1))]);

        let input = TestMemoryExec::try_new_exec(&[vec![batch]], schema.clone(), None).unwrap();
        let guard = BloomFilterGuardExec::new(input, bf, query_hash, 1);

        assert!(!guard.might_contain());

        let ctx = SessionContext::new();
        let stream = guard.execute(0, ctx.task_ctx()).unwrap();
        let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();

        // Should return empty (child not executed)
        assert!(batches.is_empty());
    }

    #[test]
    fn test_pk_hash_consistency() {
        // Test that same values produce same hash
        let hash1 =
            compute_pk_hash_from_scalars(&[datafusion::common::ScalarValue::Int32(Some(42))]);
        let hash2 =
            compute_pk_hash_from_scalars(&[datafusion::common::ScalarValue::Int32(Some(42))]);
        assert_eq!(hash1, hash2);

        // Different values produce different hashes
        let hash3 =
            compute_pk_hash_from_scalars(&[datafusion::common::ScalarValue::Int32(Some(43))]);
        assert_ne!(hash1, hash3);
    }

    #[test]
    fn test_pk_hash_with_multiple_columns() {
        let hash1 = compute_pk_hash_from_scalars(&[
            datafusion::common::ScalarValue::Int32(Some(1)),
            datafusion::common::ScalarValue::Utf8(Some("foo".to_string())),
        ]);
        let hash2 = compute_pk_hash_from_scalars(&[
            datafusion::common::ScalarValue::Int32(Some(1)),
            datafusion::common::ScalarValue::Utf8(Some("bar".to_string())),
        ]);
        assert_ne!(hash1, hash2);
    }

    #[test]
    fn test_display() {
        let schema = create_test_schema();
        let batch = RecordBatch::new_empty(schema.clone());
        let input = TestMemoryExec::try_new_exec(&[vec![batch]], schema, None).unwrap();

        let bf = Sbbf::with_ndv_fpp(100, 0.01).unwrap();
        let guard = BloomFilterGuardExec::new(input, Arc::new(bf), 12345, 2);

        // Verify it doesn't panic
        let _ = format!("{:?}", guard);
    }
}
