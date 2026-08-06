// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Bloom Filter Index
//!
//! Bloom Filter is a probabilistic data structure that allows for fast membership testing.
//! It is a space-efficient data structure that can be used to test whether an element is a member of a set.
//! It's an inexact filter - they may include false positives that require rechecking.

use crate::pb;
use crate::scalar::expression::{BloomFilterQueryParser, ScalarQueryParser};
use crate::scalar::registry::{
    BasicTrainer, ScalarIndexPlugin, TrainingCriteria, TrainingOrdering, TrainingRequest,
};
use crate::scalar::{
    BloomFilterQuery, BuiltinIndexType, CreatedIndex, IndexFile, ScalarIndexParams, UpdateCriteria,
};
use arrow_array::{Array, UInt64Array};
use arrow_schema::{DataType, Field, Schema};
use futures::TryStreamExt;
use lance_arrow_stats::StatisticsAccumulator;
use lance_core::utils::bloomfilter::as_bytes;
use lance_core::utils::bloomfilter::sbbf::{Sbbf, SbbfBuilder};
use lance_core::utils::row_addr_remap::RowAddrRemap;
use lance_select::RowAddrTreeMap;
use serde::{Deserialize, Serialize};
use std::any::Any;

use std::collections::HashMap;
use std::sync::LazyLock;

use datafusion::execution::SendableRecordBatchStream;
use std::sync::Arc;

use crate::scalar::{
    AnyQuery, IndexStore, MetricsCollector, RowIdRemapper, ScalarIndex, SearchResult,
};
use crate::{Index, IndexType};
use arrow_array::{ArrayRef, RecordBatch};
use async_trait::async_trait;
use lance_core::Error;
use lance_core::Result;
use lance_core::cache::LanceCache;
use lance_core::deepsize::DeepSizeOf;
use roaring::RoaringBitmap;

use super::zoned::{ZoneBound, ZoneProcessor, ZoneTrainer, rebuild_zones, search_zones};

const BLOOMFILTER_FILENAME: &str = "bloomfilter.lance";
const BLOOMFILTER_ITEM_META_KEY: &str = "bloomfilter_item";
const NULL_BITMAP_META_KEY: &str = "null_bitmap";
const BLOOMFILTER_PROBABILITY_META_KEY: &str = "bloomfilter_probability";
/// Upper bound on the total serialized bytes packed into a single bloom filter
/// `BinaryArray`. Its offsets are `i32`, so the concatenated payload cannot exceed
/// `i32::MAX`. We reserve a 1 MiB margin below that hard limit so per-row Arrow
/// bookkeeping (offset and validity buffers) cannot push a batch over the edge.
const MAX_BLOOMFILTER_ARRAY_LENGTH: usize = i32::MAX as usize - 1024 * 1024;
const BLOOMFILTER_INDEX_VERSION: u32 = 0;

#[derive(Debug, Clone)]
struct BloomFilterStatistics {
    // Bound of this zone within the fragment. Persisted as three separate columns
    // (fragment_id, zone_start, zone_length) in the index file.
    bound: ZoneBound,
    // Whether this zone contains any null values
    has_null: bool,
    // The actual bloom filter (SBBF) for efficient querying
    bloom_filter: Sbbf,
}

impl DeepSizeOf for BloomFilterStatistics {
    fn deep_size_of_children(&self, _context: &mut lance_core::deepsize::Context) -> usize {
        // Estimate the size of the bloom filter
        // We could try to get the actual size from the Sbbf if it has a method for that,
        // but for now we'll estimate based on the number of bytes it serializes to
        self.bloom_filter.to_bytes().len()
    }
}

impl AsRef<ZoneBound> for BloomFilterStatistics {
    fn as_ref(&self) -> &ZoneBound {
        &self.bound
    }
}

#[derive(Debug, Clone)]
pub struct BloomFilterIndex {
    zones: Vec<BloomFilterStatistics>,
    // Number of items in the filter
    number_of_items: u64,
    // Probability of false positives, fraction between 0 and 1
    probability: f64,
    // Exact set of null row addresses; None for older indices without this bitmap.
    null_rows: Option<RowAddrTreeMap>,
    frag_reuse_index: Option<Arc<dyn RowIdRemapper>>,
}

impl DeepSizeOf for BloomFilterIndex {
    fn deep_size_of_children(&self, context: &mut lance_core::deepsize::Context) -> usize {
        self.zones.deep_size_of_children(context) + self.null_rows.deep_size_of_children(context)
    }
}

impl BloomFilterIndex {
    async fn load(
        store: Arc<dyn IndexStore>,
        fri: Option<Arc<dyn RowIdRemapper>>,
        index_cache: &LanceCache,
    ) -> Result<Arc<Self>> {
        Self::load_with_max_array_length(store, fri, index_cache, MAX_BLOOMFILTER_ARRAY_LENGTH)
            .await
    }

    async fn load_with_max_array_length(
        store: Arc<dyn IndexStore>,
        fri: Option<Arc<dyn RowIdRemapper>>,
        _index_cache: &LanceCache,
        max_array_length: usize,
    ) -> Result<Arc<Self>> {
        let index_file = store.open_index_file(BLOOMFILTER_FILENAME).await?;
        let file_schema = index_file.schema();

        let number_of_items: u64 = file_schema
            .metadata
            .get(BLOOMFILTER_ITEM_META_KEY)
            .and_then(|bs| bs.parse().ok())
            .unwrap_or(*DEFAULT_NUMBER_OF_ITEMS);

        let probability: f64 = file_schema
            .metadata
            .get(BLOOMFILTER_PROBABILITY_META_KEY)
            .and_then(|bs| bs.parse().ok())
            .unwrap_or(*DEFAULT_PROBABILITY);

        let null_rows = if let Some(idx_str) = file_schema.metadata.get(NULL_BITMAP_META_KEY) {
            let idx = idx_str.parse::<u32>().map_err(|e| {
                Error::invalid_input(format!("invalid null bitmap buffer index: {e}"))
            })?;
            let bytes = index_file.read_global_buffer(idx).await?;
            Some(RowAddrTreeMap::deserialize_from(bytes.as_ref())?)
        } else {
            None
        };

        let read_batch_size =
            Self::read_batch_size(number_of_items, probability, max_array_length)?;

        let mut zones = Vec::with_capacity(index_file.num_rows());
        for start in (0..index_file.num_rows()).step_by(read_batch_size) {
            let end = (start + read_batch_size).min(index_file.num_rows());
            let mut bloom_data = index_file.read_range_stream(start..end, None).await?;
            while let Some(batch) = bloom_data.try_next().await? {
                zones.extend(Self::try_from_serialized(batch, max_array_length)?);
            }
        }

        Ok(Arc::new(Self {
            zones,
            number_of_items,
            probability,
            null_rows,
            frag_reuse_index: fri,
        }))
    }

    fn read_batch_size(
        number_of_items: u64,
        probability: f64,
        max_array_length: usize,
    ) -> Result<usize> {
        // Bloom filters are stored in an Arrow BinaryArray, whose offsets are i32.
        // The serialized filter size is fixed by the index parameters, so bound
        // reads by total serialized bytes instead of row count alone.
        let params = BloomFilterIndexBuilderParams {
            number_of_items,
            probability,
        };
        let filter_size = BloomFilterProcessor::build_filter(&params)?.size_bytes();
        if filter_size > max_array_length {
            return Err(Error::invalid_input(format!(
                "Serialized bloom filter size {} exceeds max supported batch bytes {}",
                filter_size, max_array_length
            )));
        }
        Ok((max_array_length / filter_size).max(1))
    }

    fn try_from_serialized(
        data: RecordBatch,
        max_array_length: usize,
    ) -> Result<Vec<BloomFilterStatistics>> {
        if data.num_rows() == 0 {
            return Ok(Vec::new());
        }

        let fragment_id_col = data
            .column_by_name("fragment_id")
            .ok_or_else(|| Error::invalid_input("BloomFilterIndex: missing 'fragment_id' column"))?
            .as_any()
            .downcast_ref::<arrow_array::UInt64Array>()
            .ok_or_else(|| {
                Error::invalid_input("BloomFilterIndex: 'fragment_id' column is not UInt64")
            })?;

        let zone_start_col = data
            .column_by_name("zone_start")
            .ok_or_else(|| Error::invalid_input("BloomFilterIndex: missing 'zone_start' column"))?
            .as_any()
            .downcast_ref::<arrow_array::UInt64Array>()
            .ok_or_else(|| {
                Error::invalid_input("BloomFilterIndex: 'zone_start' column is not UInt64")
            })?;

        let zone_length_col = data
            .column_by_name("zone_length")
            .ok_or_else(|| Error::invalid_input("BloomFilterIndex: missing 'zone_length' column"))?
            .as_any()
            .downcast_ref::<arrow_array::UInt64Array>()
            .ok_or_else(|| {
                Error::invalid_input("BloomFilterIndex: 'zone_length' column is not UInt64")
            })?;

        let bloom_filter_data_col = data
            .column_by_name("bloom_filter_data")
            .ok_or_else(|| {
                Error::invalid_input("BloomFilterIndex: missing 'bloom_filter_data' column")
            })?
            .as_any()
            .downcast_ref::<arrow_array::BinaryArray>()
            .ok_or_else(|| {
                Error::invalid_input("BloomFilterIndex: 'bloom_filter_data' column is not Binary")
            })?;

        // Enforce the i32-offset cap on read, symmetric to the write side. A batch this
        // large means the read chunking was bypassed; reject it before it overflows the
        // BinaryArray offsets instead of panicking deep inside Arrow.
        let offsets = bloom_filter_data_col.value_offsets();
        let batch_bytes = (offsets[offsets.len() - 1] - offsets[0]) as usize;
        if batch_bytes > max_array_length {
            return Err(Error::invalid_input(format!(
                "Serialized bloom filter batch size {} exceeds max supported batch bytes {}",
                batch_bytes, max_array_length
            )));
        }

        let has_null_col = data
            .column_by_name("has_null")
            .ok_or_else(|| Error::invalid_input("BloomFilterIndex: missing 'has_null' column"))?
            .as_any()
            .downcast_ref::<arrow_array::BooleanArray>()
            .ok_or_else(|| {
                Error::invalid_input("BloomFilterIndex: 'has_null' column is not Boolean")
            })?;

        let num_blocks = data.num_rows();
        let mut blocks = Vec::with_capacity(num_blocks);

        for i in 0..num_blocks {
            let bloom_filter_bytes = if bloom_filter_data_col.is_valid(i) {
                bloom_filter_data_col.value(i).to_vec()
            } else {
                Vec::new()
            };

            let bloom_filter = Sbbf::new(&bloom_filter_bytes).map_err(|e| {
                Error::invalid_input(format!("Failed to deserialize bloom filter: {:?}", e))
            })?;

            blocks.push(BloomFilterStatistics {
                bound: ZoneBound {
                    fragment_id: fragment_id_col.value(i),
                    start: zone_start_col.value(i),
                    length: zone_length_col.value(i) as usize,
                },
                has_null: has_null_col.value(i),
                bloom_filter,
            });
        }

        Ok(blocks)
    }

    fn evaluate_block_against_query(
        &self,
        block: &BloomFilterStatistics,
        query: &BloomFilterQuery,
    ) -> Result<bool> {
        let sbbf = &block.bloom_filter;

        match query {
            BloomFilterQuery::IsNull() => {
                // Use the has_null information to determine if this block contains nulls
                Ok(block.has_null)
            }
            BloomFilterQuery::Equals(target) => {
                if target.is_null() {
                    // Handle null values using has_null information
                    return Ok(block.has_null);
                }

                // Check the bloom filter for the target value
                match target {
                    // Signed integers
                    datafusion_common::ScalarValue::Int8(Some(val)) => Ok(sbbf.check(val)),
                    datafusion_common::ScalarValue::Int16(Some(val)) => Ok(sbbf.check(val)),
                    datafusion_common::ScalarValue::Int32(Some(val)) => Ok(sbbf.check(val)),
                    datafusion_common::ScalarValue::Int64(Some(val)) => Ok(sbbf.check(val)),
                    // Unsigned integers
                    datafusion_common::ScalarValue::UInt8(Some(val)) => Ok(sbbf.check(val)),
                    datafusion_common::ScalarValue::UInt16(Some(val)) => Ok(sbbf.check(val)),
                    datafusion_common::ScalarValue::UInt32(Some(val)) => Ok(sbbf.check(val)),
                    datafusion_common::ScalarValue::UInt64(Some(val)) => Ok(sbbf.check(val)),
                    // Floating point
                    datafusion_common::ScalarValue::Float32(Some(val)) => Ok(sbbf.check(val)),
                    datafusion_common::ScalarValue::Float64(Some(val)) => Ok(sbbf.check(val)),
                    // String types
                    datafusion_common::ScalarValue::Utf8(Some(val)) => Ok(sbbf.check(val.as_str())),
                    datafusion_common::ScalarValue::LargeUtf8(Some(val)) => {
                        Ok(sbbf.check(val.as_str()))
                    }
                    // Binary types
                    datafusion_common::ScalarValue::Binary(Some(val)) => {
                        Ok(sbbf.check(val.as_slice()))
                    }
                    datafusion_common::ScalarValue::LargeBinary(Some(val)) => {
                        Ok(sbbf.check(val.as_slice()))
                    }
                    // Date and time types
                    datafusion_common::ScalarValue::Date32(Some(val)) => Ok(sbbf.check(val)),
                    datafusion_common::ScalarValue::Date64(Some(val)) => Ok(sbbf.check(val)),
                    datafusion_common::ScalarValue::Time32Second(Some(val)) => Ok(sbbf.check(val)),
                    datafusion_common::ScalarValue::Time32Millisecond(Some(val)) => {
                        Ok(sbbf.check(val))
                    }
                    datafusion_common::ScalarValue::Time64Microsecond(Some(val)) => {
                        Ok(sbbf.check(val))
                    }
                    datafusion_common::ScalarValue::Time64Nanosecond(Some(val)) => {
                        Ok(sbbf.check(val))
                    }
                    datafusion_common::ScalarValue::TimestampSecond(Some(val), _) => {
                        Ok(sbbf.check(val))
                    }
                    datafusion_common::ScalarValue::TimestampMillisecond(Some(val), _) => {
                        Ok(sbbf.check(val))
                    }
                    datafusion_common::ScalarValue::TimestampMicrosecond(Some(val), _) => {
                        Ok(sbbf.check(val))
                    }
                    datafusion_common::ScalarValue::TimestampNanosecond(Some(val), _) => {
                        Ok(sbbf.check(val))
                    }
                    _ => Err(Error::invalid_input_source(
                        format!("Unsupported data type in bloom filter query: {:?}", target).into(),
                    )),
                }
            }
            BloomFilterQuery::IsIn(values) => {
                // Check if any value in the set is in the bloom filter
                for value in values {
                    if value.is_null() {
                        // Handle null values using has_null information
                        if block.has_null {
                            return Ok(true);
                        }
                        continue;
                    }

                    let found = match value {
                        // Signed integers
                        datafusion_common::ScalarValue::Int8(Some(val)) => sbbf.check(val),
                        datafusion_common::ScalarValue::Int16(Some(val)) => sbbf.check(val),
                        datafusion_common::ScalarValue::Int32(Some(val)) => sbbf.check(val),
                        datafusion_common::ScalarValue::Int64(Some(val)) => sbbf.check(val),
                        // Unsigned integers
                        datafusion_common::ScalarValue::UInt8(Some(val)) => sbbf.check(val),
                        datafusion_common::ScalarValue::UInt16(Some(val)) => sbbf.check(val),
                        datafusion_common::ScalarValue::UInt32(Some(val)) => sbbf.check(val),
                        datafusion_common::ScalarValue::UInt64(Some(val)) => sbbf.check(val),
                        // Floating point
                        datafusion_common::ScalarValue::Float32(Some(val)) => sbbf.check(val),
                        datafusion_common::ScalarValue::Float64(Some(val)) => sbbf.check(val),
                        // String types
                        datafusion_common::ScalarValue::Utf8(Some(val)) => sbbf.check(val.as_str()),
                        datafusion_common::ScalarValue::LargeUtf8(Some(val)) => {
                            sbbf.check(val.as_str())
                        }
                        // Binary types
                        datafusion_common::ScalarValue::Binary(Some(val)) => {
                            sbbf.check(val.as_slice())
                        }
                        datafusion_common::ScalarValue::LargeBinary(Some(val)) => {
                            sbbf.check(val.as_slice())
                        }
                        // Date and time types
                        datafusion_common::ScalarValue::Date32(Some(val)) => sbbf.check(val),
                        datafusion_common::ScalarValue::Date64(Some(val)) => sbbf.check(val),
                        datafusion_common::ScalarValue::Time32Second(Some(val)) => sbbf.check(val),
                        datafusion_common::ScalarValue::Time32Millisecond(Some(val)) => {
                            sbbf.check(val)
                        }
                        datafusion_common::ScalarValue::Time64Microsecond(Some(val)) => {
                            sbbf.check(val)
                        }
                        datafusion_common::ScalarValue::Time64Nanosecond(Some(val)) => {
                            sbbf.check(val)
                        }
                        datafusion_common::ScalarValue::TimestampSecond(Some(val), _) => {
                            sbbf.check(val)
                        }
                        datafusion_common::ScalarValue::TimestampMillisecond(Some(val), _) => {
                            sbbf.check(val)
                        }
                        datafusion_common::ScalarValue::TimestampMicrosecond(Some(val), _) => {
                            sbbf.check(val)
                        }
                        datafusion_common::ScalarValue::TimestampNanosecond(Some(val), _) => {
                            sbbf.check(val)
                        }
                        _ => {
                            return Err(Error::invalid_input_source(
                                format!("Unsupported data type in bloom filter query: {:?}", value)
                                    .into(),
                            ));
                        }
                    };

                    if found {
                        return Ok(true);
                    }
                }
                Ok(false) // None of the values were found
            }
        }
    }
}

#[async_trait]
impl Index for BloomFilterIndex {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_index(self: Arc<Self>) -> Arc<dyn Index> {
        self
    }

    async fn prewarm(&self) -> Result<()> {
        Ok(())
    }

    fn statistics(&self) -> Result<serde_json::Value> {
        Ok(serde_json::json!({
            "type": "BloomFilter",
            "num_blocks": self.zones.len(),
            "number_of_items": self.number_of_items,
            "probability": self.probability,
        }))
    }

    fn index_type(&self) -> IndexType {
        IndexType::BloomFilter
    }

    async fn calculate_included_frags(&self) -> Result<RoaringBitmap> {
        let mut frag_ids = RoaringBitmap::new();

        // Loop through zones and add unique fragment IDs to the bitmap
        for block in &self.zones {
            frag_ids.insert(block.bound.fragment_id as u32);
        }

        Ok(frag_ids)
    }
}

#[async_trait]
impl ScalarIndex for BloomFilterIndex {
    async fn search(
        &self,
        query: &dyn AnyQuery,
        metrics: &dyn MetricsCollector,
    ) -> Result<SearchResult> {
        let query = query.as_any().downcast_ref::<BloomFilterQuery>().unwrap();
        if let BloomFilterQuery::IsNull() = query
            && let Some(null_rows) = &self.null_rows
        {
            return Ok(SearchResult::exact(null_rows.clone()));
        }

        search_zones(&self.zones, metrics, |block| {
            self.evaluate_block_against_query(block, query)
        })
    }

    fn results_are_row_addresses(&self) -> bool {
        true
    }

    fn can_remap(&self) -> bool {
        false
    }

    async fn remap(
        &self,
        _mapping: &RowAddrRemap,
        _dest_store: &dyn IndexStore,
    ) -> Result<CreatedIndex> {
        Err(Error::invalid_input_source(
            "BloomFilter does not support remap".into(),
        ))
    }

    async fn update(
        &self,
        new_data: SendableRecordBatchStream,
        dest_store: &dyn IndexStore,
        _old_data_filter: Option<super::OldIndexDataFilter>,
    ) -> Result<CreatedIndex> {
        // Re-train bloom filters for the appended data using the shared trainer
        let params = BloomFilterIndexBuilderParams {
            number_of_items: self.number_of_items,
            probability: self.probability,
        };

        let processor = BloomFilterProcessor::new(params.clone())?;
        let trainer = ZoneTrainer::new(processor, params.number_of_items)?;
        let (updated_blocks, new_null_rows) = rebuild_zones(&self.zones, trainer, new_data).await?;

        // Merge existing and new null rows.  If the existing index had no null bitmap
        // (legacy format — null positions unknown), preserve that None: updating cannot
        // recover the missing information, and claiming the result has zero nulls would
        // be a false negative.  Only a full retrain produces a fresh, complete bitmap.
        let merged_null_rows = self.null_rows.as_ref().map(|existing| {
            let mut merged = existing.clone();
            merged |= &new_null_rows;
            merged
        });

        // Write the combined zones back to storage
        let mut builder = BloomFilterIndexBuilder::try_new(params)?;
        builder.blocks = updated_blocks;
        builder.null_rows = merged_null_rows;
        let files = builder.write_index(dest_store).await?;

        Ok(CreatedIndex {
            index_details: prost_types::Any::from_msg(&pb::BloomFilterIndexDetails::default())?,
            index_version: BLOOMFILTER_INDEX_VERSION,
            files,
        })
    }

    fn update_criteria(&self) -> UpdateCriteria {
        UpdateCriteria::only_new_data(
            TrainingCriteria::new(TrainingOrdering::Addresses).with_row_addr(),
        )
    }

    fn derive_index_params(&self) -> Result<ScalarIndexParams> {
        let params = serde_json::to_value(BloomFilterIndexBuilderParams {
            number_of_items: self.number_of_items,
            probability: self.probability,
        })?;
        Ok(ScalarIndexParams::for_builtin(BuiltinIndexType::BloomFilter).with_params(&params))
    }
}

fn remap_zone(
    zone: &BloomFilterStatistics,
    remapper: &dyn RowIdRemapper,
) -> Vec<BloomFilterStatistics> {
    let zone_start = (zone.bound.fragment_id << 32).saturating_add(zone.bound.start);
    let mut remapped = (0..zone.bound.length as u64)
        .filter_map(|offset| remapper.remap_row_id(zone_start.saturating_add(offset)))
        .collect::<Vec<_>>();
    remapped.sort_unstable();
    remapped.dedup();

    let mut zones = Vec::new();
    let mut run_start = None;
    let mut previous = 0u64;
    for row_id in remapped {
        if run_start.is_none() {
            run_start = Some(row_id);
        } else if row_id != previous.saturating_add(1) || row_id >> 32 != previous >> 32 {
            let start = run_start.take().unwrap();
            zones.push(BloomFilterStatistics {
                bound: ZoneBound {
                    fragment_id: start >> 32,
                    start: start & u64::from(u32::MAX),
                    length: (previous - start + 1) as usize,
                },
                has_null: zone.has_null,
                bloom_filter: zone.bloom_filter.clone(),
            });
            run_start = Some(row_id);
        }
        previous = row_id;
    }
    if let Some(start) = run_start {
        zones.push(BloomFilterStatistics {
            bound: ZoneBound {
                fragment_id: start >> 32,
                start: start & u64::from(u32::MAX),
                length: (previous - start + 1) as usize,
            },
            has_null: zone.has_null,
            bloom_filter: zone.bloom_filter.clone(),
        });
    }
    zones
}

/// Merge caller-selected BloomFilter segments into one self-contained segment.
pub async fn merge_bloomfilter_indices(
    source_indices: &[(&BloomFilterIndex, &RoaringBitmap)],
    dest_store: &dyn IndexStore,
) -> Result<CreatedIndex> {
    let first = source_indices
        .iter()
        .find(|(_, fragment_filter)| !fragment_filter.is_empty())
        .or_else(|| source_indices.first())
        .ok_or_else(|| {
            Error::invalid_input("merge_bloomfilter_indices requires at least one source index")
        })?;
    let params = BloomFilterIndexBuilderParams {
        number_of_items: first.0.number_of_items,
        probability: first.0.probability,
    };

    let mut blocks = Vec::new();
    let mut merged_null_rows = RowAddrTreeMap::new();
    let mut has_missing_null_bitmap = false;
    for (source, fragment_filter) in source_indices {
        if fragment_filter.is_empty() {
            continue;
        }
        if source.number_of_items != params.number_of_items
            || source.probability != params.probability
        {
            return Err(Error::invalid_input(format!(
                "cannot merge BloomFilter segments with different parameters: \
                 number_of_items={}, probability={} and number_of_items={}, probability={}",
                params.number_of_items,
                params.probability,
                source.number_of_items,
                source.probability
            )));
        }
        let source_zones = source.zones.iter().flat_map(|block| {
            source.frag_reuse_index.as_deref().map_or_else(
                || vec![block.clone()],
                |remapper| remap_zone(block, remapper),
            )
        });
        blocks.extend(source_zones.filter(|block| {
            u32::try_from(block.bound.fragment_id)
                .is_ok_and(|fragment_id| fragment_filter.contains(fragment_id))
        }));
        match &source.null_rows {
            Some(null_rows) => {
                let mut filtered = source.frag_reuse_index.as_deref().map_or_else(
                    || null_rows.clone(),
                    |remapper| remapper.remap_row_addrs_tree_map(null_rows),
                );
                filtered.retain_fragments(fragment_filter.iter());
                merged_null_rows |= &filtered;
            }
            None => has_missing_null_bitmap = true,
        }
    }
    blocks.sort_by_key(|block| (block.bound.fragment_id, block.bound.start));

    let mut builder = BloomFilterIndexBuilder::try_new(params)?;
    builder.blocks = blocks;
    if !has_missing_null_bitmap {
        builder.null_rows = Some(merged_null_rows);
    }
    let files = builder.write_index(dest_store).await?;

    Ok(CreatedIndex {
        index_details: prost_types::Any::from_msg(&pb::BloomFilterIndexDetails::default())?,
        index_version: BLOOMFILTER_INDEX_VERSION,
        files,
    })
}

fn default_number_of_items() -> u64 {
    *DEFAULT_NUMBER_OF_ITEMS
}

fn default_probability() -> f64 {
    *DEFAULT_PROBABILITY
}

// NumberOfItems: 8192 + Probability: 0.00057(1 in 1754) -> NumberOfBytes: 16384(16KiB) + 8 SALT values
// reference: https://hur.st/bloomfilter/?n=8192&p=&m=16KiB&k=8
static DEFAULT_NUMBER_OF_ITEMS: LazyLock<u64> = LazyLock::new(|| {
    std::env::var("LANCE_BLOOMFILTER_DEFAULT_NUMBER_OF_ITEMS")
        .unwrap_or_else(|_| "8192".to_string())
        .parse()
        .expect("failed to parse Lance_BLOOMFILTER_DEFAULT_NUMBER_OF_ITEMS")
});

#[allow(clippy::manual_inspect)]
static DEFAULT_PROBABILITY: LazyLock<f64> = LazyLock::new(|| {
    std::env::var("LANCE_BLOOMFILTER_DEFAULT_PROBABILITY")
        // 0.00057 ≈ 1 in 1754 false positive rate
        .unwrap_or_else(|_| "0.00057".to_string())
        .parse()
        .map(|prob: f64| {
            assert!(
                (0.0..=1.0).contains(&prob),
                "LANCE_BLOOMFILTER_DEFAULT_PROBABILITY must be between 0 and 1, got {}",
                prob
            );
            prob
        })
        .expect("failed to parse LANCE_BLOOMFILTER_DEFAULT_PROBABILITY")
});

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BloomFilterIndexBuilderParams {
    #[serde(default = "default_number_of_items")]
    number_of_items: u64,
    #[serde(default = "default_probability")]
    probability: f64,
}

impl Default for BloomFilterIndexBuilderParams {
    fn default() -> Self {
        Self {
            number_of_items: *DEFAULT_NUMBER_OF_ITEMS,
            probability: *DEFAULT_PROBABILITY,
        }
    }
}

impl BloomFilterIndexBuilderParams {
    #[cfg(test)]
    fn new(number_of_items: u64, probability: f64) -> Self {
        Self {
            number_of_items,
            probability,
        }
    }
}

pub struct BloomFilterIndexBuilder {
    params: BloomFilterIndexBuilderParams,
    blocks: Vec<BloomFilterStatistics>,
    // None means "legacy index — null positions unknown"; Some means a complete bitmap.
    // write_index omits the null-bitmap global buffer when this is None, preserving the
    // legacy format so that downstream searches remain conservative.
    null_rows: Option<RowAddrTreeMap>,
}

impl BloomFilterIndexBuilder {
    pub fn try_new(params: BloomFilterIndexBuilderParams) -> Result<Self> {
        Ok(Self {
            params,
            blocks: Vec::new(),
            null_rows: None,
        })
    }

    /// Train the builder using the shared ZoneTrainer. The input stream is expected to
    /// contain the value column followed by `_rowaddr`, matching the order emitted by
    /// the scalar index training pipeline.
    pub async fn train(&mut self, batches_source: SendableRecordBatchStream) -> Result<()> {
        let processor = BloomFilterProcessor::new(self.params.clone())?;
        let trainer = ZoneTrainer::new(processor, self.params.number_of_items)?;
        let (blocks, null_rows) = trainer.train(batches_source).await?;
        self.blocks = blocks;
        self.null_rows = Some(null_rows);
        Ok(())
    }

    fn bloomfilter_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("fragment_id", DataType::UInt64, false),
            Field::new("zone_start", DataType::UInt64, false),
            Field::new("zone_length", DataType::UInt64, false),
            Field::new("has_null", DataType::Boolean, false),
            Field::new("bloom_filter_data", DataType::Binary, false),
        ]))
    }

    fn bloomfilter_stats_as_batch(
        fragment_ids: Vec<u64>,
        zone_starts: Vec<u64>,
        zone_lengths: Vec<u64>,
        has_nulls: Vec<bool>,
        binary_data: Vec<Vec<u8>>,
    ) -> Result<RecordBatch> {
        let fragment_ids = UInt64Array::from(fragment_ids);
        let zone_starts = UInt64Array::from(zone_starts);
        let zone_lengths = UInt64Array::from(zone_lengths);
        let has_nulls = arrow_array::BooleanArray::from(has_nulls);
        let bloom_filter_data = if binary_data.is_empty() {
            Arc::new(arrow_array::BinaryArray::new_null(0)) as ArrayRef
        } else {
            let binary_refs: Vec<Option<&[u8]>> = binary_data
                .iter()
                .map(|bytes| Some(bytes.as_slice()))
                .collect();
            Arc::new(arrow_array::BinaryArray::from_opt_vec(binary_refs)) as ArrayRef
        };

        let columns: Vec<ArrayRef> = vec![
            Arc::new(fragment_ids) as ArrayRef,
            Arc::new(zone_starts) as ArrayRef,
            Arc::new(zone_lengths) as ArrayRef,
            Arc::new(has_nulls) as ArrayRef,
            bloom_filter_data,
        ];

        Ok(RecordBatch::try_new(Self::bloomfilter_schema(), columns)?)
    }

    /// Serialize the trained bloom filter zone statistics into an index file in
    /// `index_store`, returning the resulting [`IndexFile`]s.
    ///
    /// Zones are flushed as one or more record batches, each bounded by
    /// `MAX_BLOOMFILTER_ARRAY_LENGTH` serialized bytes so the underlying Arrow
    /// `BinaryArray` never overflows its `i32` offsets. Any optional null-row bitmap
    /// is persisted as a global buffer on the same [`IndexFile`] via [`IndexStore`].
    pub async fn write_index(self, index_store: &dyn IndexStore) -> Result<Vec<IndexFile>> {
        self.write_index_with_max_array_length(index_store, MAX_BLOOMFILTER_ARRAY_LENGTH)
            .await
    }

    async fn write_index_with_max_array_length(
        self,
        index_store: &dyn IndexStore,
        max_array_length: usize,
    ) -> Result<Vec<IndexFile>> {
        let mut file_schema = Self::bloomfilter_schema().as_ref().clone();
        file_schema.metadata.insert(
            BLOOMFILTER_ITEM_META_KEY.to_string(),
            self.params.number_of_items.to_string(),
        );
        file_schema.metadata.insert(
            BLOOMFILTER_PROBABILITY_META_KEY.to_string(),
            self.params.probability.to_string(),
        );

        let index_file = index_store
            .new_index_file(BLOOMFILTER_FILENAME, Arc::new(file_schema))
            .await?;

        let mut writer = BloomFilterBatchWriter::new(index_file, max_array_length);
        for block in self.blocks {
            writer.emit(block).await?;
        }
        let bloomfilter_file = writer.finish(self.null_rows).await?;
        Ok(vec![bloomfilter_file])
    }
}

/// Buffers serialized bloom filter zone statistics and flushes them as record batches
/// to the index file, respecting the `max_array_length` limit.
struct BloomFilterBatchWriter {
    file: Box<dyn super::IndexWriter>,
    max_array_length: usize,
    fragment_ids: Vec<u64>,
    zone_starts: Vec<u64>,
    zone_lengths: Vec<u64>,
    has_nulls: Vec<bool>,
    bloom_filter_data: Vec<Vec<u8>>,
    current_bytes: usize,
    has_written: bool,
}

impl BloomFilterBatchWriter {
    fn new(file: Box<dyn super::IndexWriter>, max_array_length: usize) -> Self {
        Self {
            file,
            max_array_length,
            fragment_ids: Vec::new(),
            zone_starts: Vec::new(),
            zone_lengths: Vec::new(),
            has_nulls: Vec::new(),
            bloom_filter_data: Vec::new(),
            current_bytes: 0,
            has_written: false,
        }
    }

    async fn emit(&mut self, block: BloomFilterStatistics) -> Result<()> {
        let serialized_filter = block.bloom_filter.to_bytes();
        let serialized_len = serialized_filter.len();

        if serialized_len > self.max_array_length {
            return Err(Error::invalid_input(format!(
                "Serialized bloom filter size {} exceeds max supported batch bytes {}",
                serialized_len, self.max_array_length
            )));
        }

        let next_bytes = self
            .current_bytes
            .checked_add(serialized_len)
            .ok_or_else(|| {
                Error::invalid_input(format!(
                    "Bloom filter batch size overflow when adding {} bytes to {} bytes",
                    serialized_len, self.current_bytes
                ))
            })?;

        if !self.bloom_filter_data.is_empty() && next_bytes > self.max_array_length {
            self.flush().await?;
        }

        self.fragment_ids.push(block.bound.fragment_id);
        self.zone_starts.push(block.bound.start);
        self.zone_lengths.push(block.bound.length as u64);
        self.has_nulls.push(block.has_null);
        self.bloom_filter_data.push(serialized_filter);
        self.current_bytes += serialized_len;
        Ok(())
    }

    async fn flush(&mut self) -> Result<()> {
        if self.bloom_filter_data.is_empty() {
            return Ok(());
        }

        let batch = BloomFilterIndexBuilder::bloomfilter_stats_as_batch(
            std::mem::take(&mut self.fragment_ids),
            std::mem::take(&mut self.zone_starts),
            std::mem::take(&mut self.zone_lengths),
            std::mem::take(&mut self.has_nulls),
            std::mem::take(&mut self.bloom_filter_data),
        )?;
        self.file.write_record_batch(batch).await?;
        self.current_bytes = 0;
        self.has_written = true;
        Ok(())
    }

    async fn finish(mut self, null_rows: Option<RowAddrTreeMap>) -> Result<IndexFile> {
        self.flush().await?;
        if !self.has_written {
            self.file
                .write_record_batch(BloomFilterIndexBuilder::bloomfilter_stats_as_batch(
                    Vec::new(),
                    Vec::new(),
                    Vec::new(),
                    Vec::new(),
                    Vec::new(),
                )?)
                .await?;
        }

        if let Some(null_rows) = null_rows {
            let mut null_bitmap_bytes = Vec::with_capacity(null_rows.serialized_size());
            null_rows.serialize_into(&mut null_bitmap_bytes)?;
            let null_bitmap_idx = self
                .file
                .add_global_buffer(bytes::Bytes::from(null_bitmap_bytes))
                .await?;
            self.file
                .finish_with_metadata(HashMap::from([(
                    NULL_BITMAP_META_KEY.to_string(),
                    null_bitmap_idx.to_string(),
                )]))
                .await
        } else {
            self.file.finish_with_metadata(HashMap::new()).await
        }
    }
}

/// Index-specific processor that inserts values into the split block Bloom filter.
struct BloomFilterProcessor {
    params: BloomFilterIndexBuilderParams,
    sbbf: Option<Sbbf>,
    statistics: Option<StatisticsAccumulator>,
}

impl BloomFilterProcessor {
    fn new(params: BloomFilterIndexBuilderParams) -> Result<Self> {
        let mut processor = Self {
            params,
            sbbf: None,
            statistics: None,
        };
        processor.reset()?;
        Ok(processor)
    }

    fn build_filter(params: &BloomFilterIndexBuilderParams) -> Result<Sbbf> {
        SbbfBuilder::new()
            .expected_items(params.number_of_items)
            .false_positive_probability(params.probability)
            .build()
            .map_err(|e| {
                Error::invalid_input_source(format!("Failed to build SBBF: {:?}", e).into())
            })
    }

    fn process_primitive_array<T>(sbbf: &mut Sbbf, array: &arrow_array::PrimitiveArray<T>) -> bool
    where
        T: arrow_array::ArrowPrimitiveType,
        T::Native: as_bytes::AsBytes,
    {
        let mut has_null = false;
        for i in 0..array.len() {
            if array.is_valid(i) {
                sbbf.insert(&array.value(i));
            } else {
                has_null = true;
            }
        }
        has_null
    }

    fn process_string_array(sbbf: &mut Sbbf, array: &arrow_array::StringArray) -> bool {
        let mut has_null = false;
        for i in 0..array.len() {
            if array.is_valid(i) {
                sbbf.insert(array.value(i));
            } else {
                has_null = true;
            }
        }
        has_null
    }

    fn process_large_string_array(sbbf: &mut Sbbf, array: &arrow_array::LargeStringArray) -> bool {
        let mut has_null = false;
        for i in 0..array.len() {
            if array.is_valid(i) {
                sbbf.insert(array.value(i));
            } else {
                has_null = true;
            }
        }
        has_null
    }

    fn process_binary_array(sbbf: &mut Sbbf, array: &arrow_array::BinaryArray) -> bool {
        let mut has_null = false;
        for i in 0..array.len() {
            if array.is_valid(i) {
                sbbf.insert(array.value(i));
            } else {
                has_null = true;
            }
        }
        has_null
    }

    fn process_large_binary_array(sbbf: &mut Sbbf, array: &arrow_array::LargeBinaryArray) -> bool {
        let mut has_null = false;
        for i in 0..array.len() {
            if array.is_valid(i) {
                sbbf.insert(array.value(i));
            } else {
                has_null = true;
            }
        }
        has_null
    }
}

impl ZoneProcessor for BloomFilterProcessor {
    type ZoneStatistics = BloomFilterStatistics;

    fn process_chunk(&mut self, array: &ArrayRef) -> Result<()> {
        let sbbf = self.sbbf.as_mut().ok_or_else(|| {
            Error::invalid_input("BloomFilterProcessor did not initialize bloom filter")
        })?;

        let statistics = self
            .statistics
            .get_or_insert_with(|| StatisticsAccumulator::new(array.data_type()));
        statistics.update(array)?;

        let has_null = match array.data_type() {
            // Signed integers
            DataType::Int8 => {
                let typed_array = array
                    .as_any()
                    .downcast_ref::<arrow_array::Int8Array>()
                    .unwrap();
                Self::process_primitive_array(sbbf, typed_array)
            }
            DataType::Int16 => {
                let typed_array = array
                    .as_any()
                    .downcast_ref::<arrow_array::Int16Array>()
                    .unwrap();
                Self::process_primitive_array(sbbf, typed_array)
            }
            DataType::Int32 => {
                let typed_array = array
                    .as_any()
                    .downcast_ref::<arrow_array::Int32Array>()
                    .unwrap();
                Self::process_primitive_array(sbbf, typed_array)
            }
            DataType::Int64 => {
                let typed_array = array
                    .as_any()
                    .downcast_ref::<arrow_array::Int64Array>()
                    .unwrap();
                Self::process_primitive_array(sbbf, typed_array)
            }
            // Unsigned integers
            DataType::UInt8 => {
                let typed_array = array
                    .as_any()
                    .downcast_ref::<arrow_array::UInt8Array>()
                    .unwrap();
                Self::process_primitive_array(sbbf, typed_array)
            }
            DataType::UInt16 => {
                let typed_array = array
                    .as_any()
                    .downcast_ref::<arrow_array::UInt16Array>()
                    .unwrap();
                Self::process_primitive_array(sbbf, typed_array)
            }
            DataType::UInt32 => {
                let typed_array = array
                    .as_any()
                    .downcast_ref::<arrow_array::UInt32Array>()
                    .unwrap();
                Self::process_primitive_array(sbbf, typed_array)
            }
            DataType::UInt64 => {
                let typed_array = array
                    .as_any()
                    .downcast_ref::<arrow_array::UInt64Array>()
                    .unwrap();
                Self::process_primitive_array(sbbf, typed_array)
            }
            // Floating point numbers
            DataType::Float32 => {
                let typed_array = array
                    .as_any()
                    .downcast_ref::<arrow_array::Float32Array>()
                    .unwrap();
                Self::process_primitive_array(sbbf, typed_array)
            }
            DataType::Float64 => {
                let typed_array = array
                    .as_any()
                    .downcast_ref::<arrow_array::Float64Array>()
                    .unwrap();
                Self::process_primitive_array(sbbf, typed_array)
            }
            // Date and time types (stored as i32 internally)
            DataType::Date32 => {
                let typed_array = array
                    .as_any()
                    .downcast_ref::<arrow_array::Date32Array>()
                    .unwrap();
                Self::process_primitive_array(sbbf, typed_array)
            }
            DataType::Time32(time_unit) => match time_unit {
                arrow_schema::TimeUnit::Second => {
                    let typed_array = array
                        .as_any()
                        .downcast_ref::<arrow_array::Time32SecondArray>()
                        .unwrap();
                    Self::process_primitive_array(sbbf, typed_array)
                }
                arrow_schema::TimeUnit::Millisecond => {
                    let typed_array = array
                        .as_any()
                        .downcast_ref::<arrow_array::Time32MillisecondArray>()
                        .unwrap();
                    Self::process_primitive_array(sbbf, typed_array)
                }
                _ => {
                    return Err(Error::invalid_input_source(
                        format!("Unsupported Time32 unit: {:?}", time_unit).into(),
                    ));
                }
            },
            // Date and time types (stored as i64 internally)
            DataType::Date64 => {
                let typed_array = array
                    .as_any()
                    .downcast_ref::<arrow_array::Date64Array>()
                    .unwrap();
                Self::process_primitive_array(sbbf, typed_array)
            }
            DataType::Time64(time_unit) => match time_unit {
                arrow_schema::TimeUnit::Microsecond => {
                    let typed_array = array
                        .as_any()
                        .downcast_ref::<arrow_array::Time64MicrosecondArray>()
                        .unwrap();
                    Self::process_primitive_array(sbbf, typed_array)
                }
                arrow_schema::TimeUnit::Nanosecond => {
                    let typed_array = array
                        .as_any()
                        .downcast_ref::<arrow_array::Time64NanosecondArray>()
                        .unwrap();
                    Self::process_primitive_array(sbbf, typed_array)
                }
                _ => {
                    return Err(Error::invalid_input_source(
                        format!("Unsupported Time64 unit: {:?}", time_unit).into(),
                    ));
                }
            },
            DataType::Timestamp(time_unit, _) => match time_unit {
                arrow_schema::TimeUnit::Second => {
                    let typed_array = array
                        .as_any()
                        .downcast_ref::<arrow_array::TimestampSecondArray>()
                        .unwrap();
                    Self::process_primitive_array(sbbf, typed_array)
                }
                arrow_schema::TimeUnit::Millisecond => {
                    let typed_array = array
                        .as_any()
                        .downcast_ref::<arrow_array::TimestampMillisecondArray>()
                        .unwrap();
                    Self::process_primitive_array(sbbf, typed_array)
                }
                arrow_schema::TimeUnit::Microsecond => {
                    let typed_array = array
                        .as_any()
                        .downcast_ref::<arrow_array::TimestampMicrosecondArray>()
                        .unwrap();
                    Self::process_primitive_array(sbbf, typed_array)
                }
                arrow_schema::TimeUnit::Nanosecond => {
                    let typed_array = array
                        .as_any()
                        .downcast_ref::<arrow_array::TimestampNanosecondArray>()
                        .unwrap();
                    Self::process_primitive_array(sbbf, typed_array)
                }
            },
            DataType::Utf8 => {
                let typed_array = array
                    .as_any()
                    .downcast_ref::<arrow_array::StringArray>()
                    .unwrap();
                Self::process_string_array(sbbf, typed_array)
            }
            DataType::LargeUtf8 => {
                let typed_array = array
                    .as_any()
                    .downcast_ref::<arrow_array::LargeStringArray>()
                    .unwrap();
                Self::process_large_string_array(sbbf, typed_array)
            }
            DataType::Binary => {
                let typed_array = array
                    .as_any()
                    .downcast_ref::<arrow_array::BinaryArray>()
                    .unwrap();
                Self::process_binary_array(sbbf, typed_array)
            }
            DataType::LargeBinary => {
                let typed_array = array
                    .as_any()
                    .downcast_ref::<arrow_array::LargeBinaryArray>()
                    .unwrap();
                Self::process_large_binary_array(sbbf, typed_array)
            }
            _ => {
                return Err(Error::invalid_input_source(
                    format!(
                        "Bloom filter does not support data type: {:?}",
                        array.data_type()
                    )
                    .into(),
                ));
            }
        };

        // Update the current zone's null tracking
        debug_assert_eq!(has_null, array.null_count() > 0);
        Ok(())
    }

    fn finish_zone(&mut self, bound: ZoneBound) -> Result<Self::ZoneStatistics> {
        let bloom_filter = self.sbbf.as_ref().ok_or_else(|| {
            Error::invalid_input("BloomFilterProcessor did not initialize bloom filter")
        })?;
        let has_null = self
            .statistics
            .as_ref()
            .map(|statistics| statistics.statistics().null_count > 0)
            .unwrap_or(false);
        Ok(BloomFilterStatistics {
            bound,
            has_null,
            bloom_filter: bloom_filter.clone(),
        })
    }

    fn reset(&mut self) -> Result<()> {
        self.sbbf = Some(Self::build_filter(&self.params)?);
        self.statistics = None;
        Ok(())
    }
}

#[derive(Debug, Default)]
pub struct BloomFilterIndexPlugin;

impl BloomFilterIndexPlugin {
    async fn train_bloomfilter_index(
        batches_source: SendableRecordBatchStream,
        index_store: &dyn IndexStore,
        options: Option<BloomFilterIndexBuilderParams>,
    ) -> Result<Vec<IndexFile>> {
        let mut builder = BloomFilterIndexBuilder::try_new(options.unwrap_or_default())?;

        builder.train(batches_source).await?;

        builder.write_index(index_store).await
    }
}

#[async_trait]
impl BasicTrainer for BloomFilterIndexPlugin {
    fn new_training_request(
        &self,
        params: &str,
        field: &Field,
    ) -> Result<Box<dyn TrainingRequest>> {
        if field.data_type().is_nested() {
            return Err(Error::invalid_input_source(
                "A bloom filter index can only be created on a non-nested field.".into(),
            ));
        }

        // Check if the data type is supported by bloom filter
        match field.data_type() {
            // Signed integers
            DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            // Unsigned integers
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            // Floating point
            | DataType::Float32
            | DataType::Float64
            // String types
            | DataType::Utf8
            | DataType::LargeUtf8
            // Binary types
            | DataType::Binary
            | DataType::LargeBinary
            // Date and time types
            | DataType::Date32
            | DataType::Date64
            | DataType::Time32(_)
            | DataType::Time64(_)
            | DataType::Timestamp(_, _) => {
                // Type is supported, continue
            }
            _ => {
                return Err(Error::invalid_input_source(format!(
                    "Bloom filter index does not support data type: {:?}. Supported types: Int8, Int16, Int32, Int64, UInt8, UInt16, UInt32, UInt64, Float32, Float64, Utf8, LargeUtf8, Binary, LargeBinary, Date32, Date64, Time32, Time64, Timestamp",
                    field.data_type()
                ).into()));
            }
        }

        let params = serde_json::from_str::<BloomFilterIndexBuilderParams>(params)?;

        Ok(Box::new(BloomFilterIndexTrainingRequest::new(params)))
    }

    async fn train_index(
        &self,
        data: SendableRecordBatchStream,
        index_store: &dyn IndexStore,
        request: Box<dyn TrainingRequest>,
        _fragment_ids: Option<Vec<u32>>,
        _progress: Arc<dyn crate::progress::IndexBuildProgress>,
    ) -> Result<CreatedIndex> {
        let request = (request as Box<dyn std::any::Any>)
            .downcast::<BloomFilterIndexTrainingRequest>()
            .map_err(|_| {
                Error::invalid_input_source(
                    "must provide training request created by new_training_request".into(),
                )
            })?;
        let files = Self::train_bloomfilter_index(data, index_store, Some(request.params)).await?;
        Ok(CreatedIndex {
            index_details: prost_types::Any::from_msg(&pb::BloomFilterIndexDetails::default())
                .unwrap(),
            index_version: BLOOMFILTER_INDEX_VERSION,
            files,
        })
    }
}

#[async_trait]
impl ScalarIndexPlugin for BloomFilterIndexPlugin {
    fn basic_trainer(&self) -> Option<&dyn BasicTrainer> {
        Some(self)
    }

    fn provides_exact_answer(&self) -> bool {
        false
    }

    fn version(&self) -> u32 {
        BLOOMFILTER_INDEX_VERSION
    }

    fn name(&self) -> &str {
        "BloomFilter"
    }

    fn new_query_parser(
        &self,
        index_name: String,
        _index_details: &prost_types::Any,
    ) -> Option<Box<dyn ScalarQueryParser>> {
        Some(Box::new(BloomFilterQueryParser::new(
            index_name,
            self.name().to_string(),
            true,
        )))
    }

    async fn load_index(
        &self,
        index_store: Arc<dyn IndexStore>,
        _index_details: &prost_types::Any,
        frag_reuse_index: Option<Arc<dyn RowIdRemapper>>,
        cache: &LanceCache,
    ) -> Result<Arc<dyn ScalarIndex>> {
        Ok(
            BloomFilterIndex::load(index_store, frag_reuse_index, cache).await?
                as Arc<dyn ScalarIndex>,
        )
    }

    async fn load_statistics(
        &self,
        _index_store: Arc<dyn IndexStore>,
        _index_details: &prost_types::Any,
    ) -> Result<Option<serde_json::Value>> {
        Ok(None)
    }
}

#[derive(Debug)]
pub struct BloomFilterIndexTrainingRequest {
    pub params: BloomFilterIndexBuilderParams,
    pub criteria: TrainingCriteria,
}

impl BloomFilterIndexTrainingRequest {
    pub fn new(params: BloomFilterIndexBuilderParams) -> Self {
        Self {
            params,
            criteria: TrainingCriteria::new(TrainingOrdering::Addresses).with_row_addr(),
        }
    }
}

impl TrainingRequest for BloomFilterIndexTrainingRequest {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn criteria(&self) -> &TrainingCriteria {
        &self.criteria
    }
}

#[cfg(test)]
mod tests {
    use crate::frag_reuse::{FragReuseIndex, FragReuseIndexDetails, FragReuseIndexHandle};
    use crate::scalar::registry::VALUE_COLUMN_NAME;
    use std::collections::HashMap;
    use std::sync::Arc;

    use crate::scalar::bloomfilter::BloomFilterIndexPlugin;
    use arrow_array::{RecordBatch, UInt64Array, record_batch};
    use arrow_schema::{DataType, Field, Schema};
    use datafusion::execution::SendableRecordBatchStream;
    use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
    use datafusion_common::ScalarValue;
    use futures::{StreamExt, stream};
    use lance_core::{Error, ROW_ADDR, cache::LanceCache, utils::tempfile::TempObjDir};
    use lance_io::object_store::ObjectStore;
    use lance_select::RowAddrTreeMap;

    use crate::scalar::{
        BloomFilterQuery, IndexStore, ScalarIndex, SearchResult,
        bloomfilter::{
            BloomFilterIndex, BloomFilterIndexBuilder, BloomFilterIndexBuilderParams,
            merge_bloomfilter_indices,
        },
        lance_format::LanceIndexStore,
    };
    use lance_core::utils::bloomfilter::sbbf::Sbbf;

    use crate::Index; // Import Index trait to access calculate_included_frags
    use crate::metrics::NoOpMetricsCollector;
    use roaring::RoaringBitmap; // Import RoaringBitmap for the test

    // Adds a _rowaddr column emulating each batch as a new fragment
    fn add_row_addr(stream: SendableRecordBatchStream) -> SendableRecordBatchStream {
        let schema = stream.schema();
        let schema_with_row_addr = Arc::new(Schema::new(vec![
            schema.field(0).clone(),
            Field::new(ROW_ADDR, DataType::UInt64, false),
        ]));
        let schema = schema_with_row_addr.clone();
        let stream = stream.enumerate().map(move |(frag_id, batch)| {
            let batch = batch.unwrap();
            let row_addr = Arc::new(UInt64Array::from_iter_values(
                (0..batch.num_rows() as u64).map(|off| off + ((frag_id as u64) << 32)),
            ));
            Ok(RecordBatch::try_new(
                schema_with_row_addr.clone(),
                vec![batch.column(0).clone(), row_addr],
            )?)
        });
        Box::pin(RecordBatchStreamAdapter::new(schema, stream))
    }

    #[tokio::test]
    async fn test_empty_bloomfilter_index() {
        let tmpdir = TempObjDir::default();
        let test_store = Arc::new(LanceIndexStore::new(
            Arc::new(ObjectStore::local()),
            tmpdir.clone(),
            Arc::new(LanceCache::no_cache()),
        ));

        let data = arrow_array::Int32Array::from(Vec::<i32>::new());
        let schema = Arc::new(Schema::new(vec![Field::new(
            VALUE_COLUMN_NAME,
            DataType::Int32,
            false,
        )]));
        let data = RecordBatch::try_new(schema.clone(), vec![Arc::new(data)]).unwrap();

        let data_stream: SendableRecordBatchStream = Box::pin(RecordBatchStreamAdapter::new(
            schema,
            stream::once(std::future::ready(Ok(data))),
        ));
        let data_stream = add_row_addr(data_stream);

        BloomFilterIndexPlugin::train_bloomfilter_index(data_stream, test_store.as_ref(), None)
            .await
            .unwrap();

        log::debug!("Successfully wrote the index file");

        // Read the index file back and check its contents
        let index = BloomFilterIndex::load(test_store.clone(), None, &LanceCache::no_cache())
            .await
            .expect("Failed to load BloomFilterIndex");
        assert_eq!(index.zones.len(), 0);
        assert_eq!(index.number_of_items, 8192);
        assert_eq!(index.probability, 0.00057); // Default probability

        // Equals query: null (should match nothing, as there are no nulls in empty index)
        let query = BloomFilterQuery::Equals(ScalarValue::Int32(None));
        let result = index.search(&query, &NoOpMetricsCollector).await.unwrap();
        assert_eq!(result, SearchResult::at_most(RowAddrTreeMap::new()));
    }

    #[tokio::test]
    async fn test_basic_bloomfilter_index() {
        let tmpdir = TempObjDir::default();
        let test_store = Arc::new(LanceIndexStore::new(
            Arc::new(ObjectStore::local()),
            tmpdir.clone(),
            Arc::new(LanceCache::no_cache()),
        ));

        let data = arrow_array::Int32Array::from_iter_values(0..100);
        let schema = Arc::new(Schema::new(vec![Field::new(
            VALUE_COLUMN_NAME,
            DataType::Int32,
            false,
        )]));
        let data = RecordBatch::try_new(schema.clone(), vec![Arc::new(data)]).unwrap();
        let data_stream: SendableRecordBatchStream = Box::pin(RecordBatchStreamAdapter::new(
            schema,
            stream::once(std::future::ready(Ok(data))),
        ));
        let data_stream = add_row_addr(data_stream);

        BloomFilterIndexPlugin::train_bloomfilter_index(
            data_stream,
            test_store.as_ref(),
            Some(BloomFilterIndexBuilderParams::new(100, 0.01)), // ~1% false positive rate
        )
        .await
        .unwrap();

        log::debug!("Successfully wrote the index file");

        // Read the index file back and check its contents
        let index = BloomFilterIndex::load(test_store.clone(), None, &LanceCache::no_cache())
            .await
            .expect("Failed to load BloomFilterIndex");

        assert_eq!(index.zones.len(), 1);
        assert_eq!(index.number_of_items, 100);
        assert_eq!(index.probability, 0.01);

        // Check that we have one zone (since 100 items fit exactly in one zone of size 100)
        assert_eq!(index.zones[0].bound.fragment_id, 0u64);
        assert_eq!(index.zones[0].bound.start, 0u64);
        assert_eq!(index.zones[0].bound.length, 100);

        // Test search functionality
        // The bloom filter should work correctly and find the value
        let query = BloomFilterQuery::Equals(ScalarValue::Int32(Some(50)));
        let result = index.search(&query, &NoOpMetricsCollector).await.unwrap();

        // Should match the block since value 50 is in the range [0, 100)
        let mut expected = RowAddrTreeMap::new();
        expected.insert_range(0..100);
        assert_eq!(result, SearchResult::at_most(expected));

        // Test search for a value that shouldn't exist
        let query = BloomFilterQuery::Equals(ScalarValue::Int32(Some(500))); // Value not in [0, 100)
        let result = index.search(&query, &NoOpMetricsCollector).await.unwrap();

        // Should return empty result since bloom filter correctly filters out this value
        assert_eq!(result, SearchResult::at_most(RowAddrTreeMap::new()));

        // Test calculate_included_frags
        assert_eq!(
            index.calculate_included_frags().await.unwrap(),
            RoaringBitmap::from_iter(0..1)
        );
    }

    #[tokio::test]
    async fn test_multiple_fragments_bloomfilter() {
        let tmpdir = TempObjDir::default();
        let test_store = Arc::new(LanceIndexStore::new(
            Arc::new(ObjectStore::local()),
            tmpdir.clone(),
            Arc::new(LanceCache::no_cache()),
        ));

        let schema = Arc::new(Schema::new(vec![Field::new(
            VALUE_COLUMN_NAME,
            DataType::Int64,
            false,
        )]));

        // Create multiple fragments with data
        // Fragment 0: values 0-99
        let fragment0_data = arrow_array::Int64Array::from_iter_values(0..100);
        let fragment0_batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(fragment0_data)]).unwrap();

        // Fragment 1: values 100-199
        let fragment1_data = arrow_array::Int64Array::from_iter_values(100..200);
        let fragment1_batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(fragment1_data)]).unwrap();

        // Create a stream with multiple batches (fragments)
        let data_stream: SendableRecordBatchStream = Box::pin(RecordBatchStreamAdapter::new(
            schema.clone(),
            stream::iter(vec![
                Ok(fragment0_batch.clone()),
                Ok(fragment1_batch.clone()),
            ]),
        ));
        let data_stream = add_row_addr(data_stream);

        BloomFilterIndexPlugin::train_bloomfilter_index(
            data_stream,
            test_store.as_ref(),
            Some(BloomFilterIndexBuilderParams::new(50, 0.05)), // ~5% false positive rate
        )
        .await
        .unwrap();

        // Read the index file back and check its contents
        let index = BloomFilterIndex::load(test_store.clone(), None, &LanceCache::no_cache())
            .await
            .expect("Failed to load BloomFilterIndex");

        // Should have 4 zones total (2 zones per fragment)
        assert_eq!(index.zones.len(), 4);

        // Check fragment 0 zones
        assert_eq!(index.zones[0].bound.fragment_id, 0u64);
        assert_eq!(index.zones[0].bound.start, 0u64);
        assert_eq!(index.zones[0].bound.length, 50);

        assert_eq!(index.zones[1].bound.fragment_id, 0u64);
        assert_eq!(index.zones[1].bound.start, 50u64);
        assert_eq!(index.zones[1].bound.length, 50);

        // Check fragment 1 zones
        assert_eq!(index.zones[2].bound.fragment_id, 1u64);
        assert_eq!(index.zones[2].bound.start, 0u64);
        assert_eq!(index.zones[2].bound.length, 50);

        assert_eq!(index.zones[3].bound.fragment_id, 1u64);
        assert_eq!(index.zones[3].bound.start, 50u64);
        assert_eq!(index.zones[3].bound.length, 50);

        // Test search functionality
        let query = BloomFilterQuery::Equals(ScalarValue::Int64(Some(150)));
        let result = index.search(&query, &NoOpMetricsCollector).await.unwrap();

        // Should only match fragment 1 blocks since bloom filter correctly filters
        // Value 150 is only in fragment 1 (values 100-199), not in fragment 0 (values 0-99)
        let mut expected = RowAddrTreeMap::new();
        expected.insert_range((1u64 << 32) + 50..((1u64 << 32) + 100)); // Only the block containing 150
        assert_eq!(result, SearchResult::at_most(expected));

        // Test calculate_included_frags
        assert_eq!(
            index.calculate_included_frags().await.unwrap(),
            RoaringBitmap::from_iter(0..2)
        );
    }

    #[tokio::test]
    async fn test_nan_bloomfilter_index() {
        let tmpdir = TempObjDir::default();
        let test_store = Arc::new(LanceIndexStore::new(
            Arc::new(ObjectStore::local()),
            tmpdir.clone(),
            Arc::new(LanceCache::no_cache()),
        ));

        // Create deterministic data with NaN values
        // Pattern: [1.0, 2.0, NaN, 3.0, 4.0, 5.0, NaN, 6.0, 7.0, 8.0, ...]
        let mut values = Vec::new();
        for i in 0..500 {
            if i % 5 == 2 {
                values.push(f32::NAN);
            } else {
                values.push(i as f32);
            }
        }

        let float_data = arrow_array::Float32Array::from(values);
        let schema = Arc::new(Schema::new(vec![Field::new(
            VALUE_COLUMN_NAME,
            DataType::Float32,
            true,
        )]));
        let data =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(float_data.clone())]).unwrap();
        let data_stream: SendableRecordBatchStream = Box::pin(RecordBatchStreamAdapter::new(
            schema,
            stream::once(std::future::ready(Ok(data))),
        ));
        let data_stream = add_row_addr(data_stream);

        BloomFilterIndexPlugin::train_bloomfilter_index(
            data_stream,
            test_store.as_ref(),
            Some(BloomFilterIndexBuilderParams::new(100, 0.01)), // ~1% false positive rate
        )
        .await
        .unwrap();

        // Load the index
        let index = BloomFilterIndex::load(test_store.clone(), None, &LanceCache::no_cache())
            .await
            .expect("Failed to load BloomFilterIndex");

        // Should have 5 zones since we have 500 rows and zone size is 100
        assert_eq!(index.zones.len(), 5);

        // Test search for NaN values using Equals with NaN
        let query = BloomFilterQuery::Equals(ScalarValue::Float32(Some(f32::NAN)));
        let result = index.search(&query, &NoOpMetricsCollector).await.unwrap();

        // Should match all blocks since they all contain NaN values
        let mut expected = RowAddrTreeMap::new();
        expected.insert_range(0..500); // All rows since NaN is in every block
        assert_eq!(result, SearchResult::at_most(expected));

        // Test search for a specific finite value that exists in the data
        let query = BloomFilterQuery::Equals(ScalarValue::Float32(Some(5.0)));
        let result = index.search(&query, &NoOpMetricsCollector).await.unwrap();

        // Should match only the first block since 5.0 only exists in rows 0-99
        let mut expected = RowAddrTreeMap::new();
        expected.insert_range(0..100);
        assert_eq!(result, SearchResult::at_most(expected));

        // Test search for a value that doesn't exist but is within expected range
        let query = BloomFilterQuery::Equals(ScalarValue::Float32(Some(250.0)));
        let result = index.search(&query, &NoOpMetricsCollector).await.unwrap();

        // Should match the third block since 250.0 would be in that range if it existed
        let mut expected = RowAddrTreeMap::new();
        expected.insert_range(200..300);
        assert_eq!(result, SearchResult::at_most(expected));

        // Test search for a value way outside the range
        let query = BloomFilterQuery::Equals(ScalarValue::Float32(Some(10000.0)));
        let result = index.search(&query, &NoOpMetricsCollector).await.unwrap();

        // Should return empty since bloom filter correctly filters out this value
        assert_eq!(result, SearchResult::at_most(RowAddrTreeMap::new()));

        // Test IsIn query with NaN and finite values
        let query = BloomFilterQuery::IsIn(vec![
            ScalarValue::Float32(Some(f32::NAN)),
            ScalarValue::Float32(Some(5.0)),
            ScalarValue::Float32(Some(150.0)), // This value exists in the second block
        ]);
        let result = index.search(&query, &NoOpMetricsCollector).await.unwrap();

        // Should match all blocks since they all contain NaN values
        let mut expected = RowAddrTreeMap::new();
        expected.insert_range(0..500);
        assert_eq!(result, SearchResult::at_most(expected));
    }

    #[tokio::test]
    async fn test_complex_bloomfilter_index() {
        let tmpdir = TempObjDir::default();
        let test_store = Arc::new(LanceIndexStore::new(
            Arc::new(ObjectStore::local()),
            tmpdir.clone(),
            Arc::new(LanceCache::no_cache()),
        ));

        // Create data that will produce multiple blocks
        let data_size = 10000;
        let data = arrow_array::Int64Array::from_iter_values(0..data_size as i64);
        let schema = Arc::new(Schema::new(vec![Field::new(
            VALUE_COLUMN_NAME,
            DataType::Int64,
            false,
        )]));
        let data = RecordBatch::try_new(schema.clone(), vec![Arc::new(data)]).unwrap();
        let data_stream: SendableRecordBatchStream = Box::pin(RecordBatchStreamAdapter::new(
            schema,
            stream::once(std::future::ready(Ok(data))),
        ));
        let data_stream = add_row_addr(data_stream);

        BloomFilterIndexPlugin::train_bloomfilter_index(
            data_stream,
            test_store.as_ref(),
            Some(BloomFilterIndexBuilderParams::new(1000, 0.001)), // 10 blocks total
        )
        .await
        .unwrap();

        // Load the index
        let index = BloomFilterIndex::load(test_store.clone(), None, &LanceCache::no_cache())
            .await
            .expect("Failed to load BloomFilterIndex");

        // Should have 10 zones since we have 10000 rows and zone size is 1000
        assert_eq!(index.zones.len(), 10);
        assert_eq!(index.number_of_items, 1000);
        assert_eq!(index.probability, 0.001);

        // Verify zone structure
        for (i, block) in index.zones.iter().enumerate() {
            assert_eq!(block.bound.fragment_id, 0u64);
            assert_eq!(block.bound.start, (i * 1000) as u64);
            assert_eq!(block.bound.length, 1000);
            // Check that the bloom filter has some data (non-zero bytes when serialized)
            assert!(!block.bloom_filter.to_bytes().is_empty());
        }

        // Test search for a value in a specific zone
        let query = BloomFilterQuery::Equals(ScalarValue::Int64(Some(2500))); // In zone 2 (2000-2999)
        let result = index.search(&query, &NoOpMetricsCollector).await.unwrap();

        // Should match zone 2
        let mut expected = RowAddrTreeMap::new();
        expected.insert_range(2000..3000);
        assert_eq!(result, SearchResult::at_most(expected));

        // Test search for a value way outside the range
        let query = BloomFilterQuery::Equals(ScalarValue::Int64(Some(50000)));
        let result = index.search(&query, &NoOpMetricsCollector).await.unwrap();

        // Should return empty since bloom filter correctly filters out this value
        assert_eq!(result, SearchResult::at_most(RowAddrTreeMap::new()));

        // Test IsIn query with values from different zones
        let query = BloomFilterQuery::IsIn(vec![
            ScalarValue::Int64(Some(500)),   // Zone 0 (0-999)
            ScalarValue::Int64(Some(2500)),  // Zone 2 (2000-2999)
            ScalarValue::Int64(Some(7500)),  // Zone 7 (7000-7999)
            ScalarValue::Int64(Some(50000)), // Not in any zone
        ]);
        let result = index.search(&query, &NoOpMetricsCollector).await.unwrap();

        // Should match zones 0, 2, and 7
        let mut expected = RowAddrTreeMap::new();
        expected.insert_range(0..1000); // Zone 0
        expected.insert_range(2000..3000); // Zone 2
        expected.insert_range(7000..8000); // Zone 7
        assert_eq!(result, SearchResult::at_most(expected));

        // Test calculate_included_frags
        assert_eq!(
            index.calculate_included_frags().await.unwrap(),
            RoaringBitmap::from_iter(0..1)
        );
    }

    #[tokio::test]
    async fn test_string_bloomfilter_index() {
        let tmpdir = TempObjDir::default();
        let test_store = Arc::new(LanceIndexStore::new(
            Arc::new(ObjectStore::local()),
            tmpdir.clone(),
            Arc::new(LanceCache::no_cache()),
        ));

        // Create string data
        let string_values: Vec<String> = (0..200).map(|i| format!("value_{:03}", i)).collect();
        let string_data = arrow_array::StringArray::from_iter_values(string_values.iter());
        let schema = Arc::new(Schema::new(vec![Field::new(
            VALUE_COLUMN_NAME,
            DataType::Utf8,
            false,
        )]));
        let data = RecordBatch::try_new(schema.clone(), vec![Arc::new(string_data)]).unwrap();
        let data_stream: SendableRecordBatchStream = Box::pin(RecordBatchStreamAdapter::new(
            schema,
            stream::once(std::future::ready(Ok(data))),
        ));
        let data_stream = add_row_addr(data_stream);

        BloomFilterIndexPlugin::train_bloomfilter_index(
            data_stream,
            test_store.as_ref(),
            Some(BloomFilterIndexBuilderParams::new(100, 0.01)), // ~1% false positive rate
        )
        .await
        .unwrap();

        // Load the index
        let index = BloomFilterIndex::load(test_store.clone(), None, &LanceCache::no_cache())
            .await
            .expect("Failed to load BloomFilterIndex");

        // Should have 2 zones since we have 200 rows and zone size is 100
        assert_eq!(index.zones.len(), 2);

        // Test search for a value in the first zone
        let query = BloomFilterQuery::Equals(ScalarValue::Utf8(Some("value_050".to_string())));
        let result = index.search(&query, &NoOpMetricsCollector).await.unwrap();

        // Should match the first zone
        let mut expected = RowAddrTreeMap::new();
        expected.insert_range(0..100);
        assert_eq!(result, SearchResult::at_most(expected));

        // Test search for a value in the second zone
        let query = BloomFilterQuery::Equals(ScalarValue::Utf8(Some("value_150".to_string())));
        let result = index.search(&query, &NoOpMetricsCollector).await.unwrap();

        // Should match the second zone
        let mut expected = RowAddrTreeMap::new();
        expected.insert_range(100..200);
        assert_eq!(result, SearchResult::at_most(expected));

        // Test search for a value that doesn't exist
        let query =
            BloomFilterQuery::Equals(ScalarValue::Utf8(Some("nonexistent_value".to_string())));
        let result = index.search(&query, &NoOpMetricsCollector).await.unwrap();

        // Should return empty since bloom filter correctly filters out this value
        assert_eq!(result, SearchResult::at_most(RowAddrTreeMap::new()));

        // Test IsIn query with string values
        let query = BloomFilterQuery::IsIn(vec![
            ScalarValue::Utf8(Some("value_025".to_string())), // First zone
            ScalarValue::Utf8(Some("value_175".to_string())), // Second zone
            ScalarValue::Utf8(Some("nonexistent".to_string())), // Not present
        ]);
        let result = index.search(&query, &NoOpMetricsCollector).await.unwrap();

        // Should match both zones
        let mut expected = RowAddrTreeMap::new();
        expected.insert_range(0..200);
        assert_eq!(result, SearchResult::at_most(expected));
    }

    #[tokio::test]
    async fn test_binary_bloomfilter_index() {
        let tmpdir = TempObjDir::default();
        let test_store = Arc::new(LanceIndexStore::new(
            Arc::new(ObjectStore::local()),
            tmpdir.clone(),
            Arc::new(LanceCache::no_cache()),
        ));

        // Create binary data
        let binary_values: Vec<Vec<u8>> = (0..100)
            .map(|i| vec![i as u8, (i + 1) as u8, (i + 2) as u8])
            .collect();
        let binary_data = arrow_array::BinaryArray::from_iter_values(binary_values.iter());
        let schema = Arc::new(Schema::new(vec![Field::new(
            VALUE_COLUMN_NAME,
            DataType::Binary,
            false,
        )]));
        let data = RecordBatch::try_new(schema.clone(), vec![Arc::new(binary_data)]).unwrap();
        let data_stream: SendableRecordBatchStream = Box::pin(RecordBatchStreamAdapter::new(
            schema,
            stream::once(std::future::ready(Ok(data))),
        ));
        let data_stream = add_row_addr(data_stream);

        BloomFilterIndexPlugin::train_bloomfilter_index(
            data_stream,
            test_store.as_ref(),
            Some(BloomFilterIndexBuilderParams::new(50, 0.05)),
        )
        .await
        .unwrap();

        // Load the index
        let index = BloomFilterIndex::load(test_store.clone(), None, &LanceCache::no_cache())
            .await
            .expect("Failed to load BloomFilterIndex");

        // Should have 2 zones since we have 100 rows and zone size is 50
        assert_eq!(index.zones.len(), 2);

        // Test search for a value in the first zone
        let query = BloomFilterQuery::Equals(ScalarValue::Binary(Some(vec![25, 26, 27])));
        let result = index.search(&query, &NoOpMetricsCollector).await.unwrap();

        // Should match the first zone
        let mut expected = RowAddrTreeMap::new();
        expected.insert_range(0..50);
        assert_eq!(result, SearchResult::at_most(expected));

        // Test search for a value in the second zone
        let query = BloomFilterQuery::Equals(ScalarValue::Binary(Some(vec![75, 76, 77])));
        let result = index.search(&query, &NoOpMetricsCollector).await.unwrap();

        // Should match the second zone
        let mut expected = RowAddrTreeMap::new();
        expected.insert_range(50..100);
        assert_eq!(result, SearchResult::at_most(expected));

        // Test search for a value that doesn't exist
        let query = BloomFilterQuery::Equals(ScalarValue::Binary(Some(vec![255, 254, 253])));
        let result = index.search(&query, &NoOpMetricsCollector).await.unwrap();

        // Should return empty since bloom filter correctly filters out this value
        assert_eq!(result, SearchResult::at_most(RowAddrTreeMap::new()));
    }

    #[tokio::test]
    async fn test_large_data_types_bloomfilter_index() {
        let tmpdir = TempObjDir::default();
        let test_store = Arc::new(LanceIndexStore::new(
            Arc::new(ObjectStore::local()),
            tmpdir.clone(),
            Arc::new(LanceCache::no_cache()),
        ));

        // Test LargeUtf8 data type
        let large_string_values: Vec<String> =
            (0..100).map(|i| format!("large_value_{:05}", i)).collect();
        let large_string_data =
            arrow_array::LargeStringArray::from_iter_values(large_string_values.iter());
        let schema = Arc::new(Schema::new(vec![Field::new(
            VALUE_COLUMN_NAME,
            DataType::LargeUtf8,
            false,
        )]));
        let data = RecordBatch::try_new(schema.clone(), vec![Arc::new(large_string_data)]).unwrap();
        let data_stream: SendableRecordBatchStream = Box::pin(RecordBatchStreamAdapter::new(
            schema,
            stream::once(std::future::ready(Ok(data))),
        ));
        let data_stream = add_row_addr(data_stream);

        BloomFilterIndexPlugin::train_bloomfilter_index(
            data_stream,
            test_store.as_ref(),
            Some(BloomFilterIndexBuilderParams::new(50, 0.05)),
        )
        .await
        .unwrap();

        // Load the index
        let index = BloomFilterIndex::load(test_store.clone(), None, &LanceCache::no_cache())
            .await
            .expect("Failed to load BloomFilterIndex");

        assert_eq!(index.zones.len(), 2);

        // Test search functionality
        let query = BloomFilterQuery::Equals(ScalarValue::LargeUtf8(Some(
            "large_value_00025".to_string(),
        )));
        let result = index.search(&query, &NoOpMetricsCollector).await.unwrap();

        // Should match the first zone
        let mut expected = RowAddrTreeMap::new();
        expected.insert_range(0..50);
        assert_eq!(result, SearchResult::at_most(expected));

        // Test search for a value that doesn't exist
        let query = BloomFilterQuery::Equals(ScalarValue::LargeUtf8(Some(
            "nonexistent_large_value".to_string(),
        )));
        let result = index.search(&query, &NoOpMetricsCollector).await.unwrap();

        // Should return empty since bloom filter correctly filters out this value
        assert_eq!(result, SearchResult::at_most(RowAddrTreeMap::new()));
    }

    #[tokio::test]
    async fn test_timestamp_bloomfilter_index() {
        let tmpdir = TempObjDir::default();
        let test_store = Arc::new(LanceIndexStore::new(
            Arc::new(ObjectStore::local()),
            tmpdir.clone(),
            Arc::new(LanceCache::no_cache()),
        ));

        // Test Date32 (days since Unix epoch)
        let date32_values: Vec<i32> = (0..100).collect(); // Days since Unix epoch
        let date32_data = arrow_array::Date32Array::from(date32_values.clone());
        let schema = Arc::new(Schema::new(vec![Field::new(
            VALUE_COLUMN_NAME,
            DataType::Date32,
            false,
        )]));
        let data = RecordBatch::try_new(schema.clone(), vec![Arc::new(date32_data)]).unwrap();
        let data_stream: SendableRecordBatchStream = Box::pin(RecordBatchStreamAdapter::new(
            schema,
            stream::once(std::future::ready(Ok(data))),
        ));
        let data_stream = add_row_addr(data_stream);

        BloomFilterIndexPlugin::train_bloomfilter_index(
            data_stream,
            test_store.as_ref(),
            Some(BloomFilterIndexBuilderParams::new(50, 0.01)),
        )
        .await
        .unwrap();

        // Load the Date32 index
        let index = BloomFilterIndex::load(test_store.clone(), None, &LanceCache::no_cache())
            .await
            .expect("Failed to load Date32 BloomFilterIndex");

        assert_eq!(index.zones.len(), 2); // 100 rows, zone size 50

        // Test search for Date32 value in first zone
        let query = BloomFilterQuery::Equals(ScalarValue::Date32(Some(25)));
        let result = index.search(&query, &NoOpMetricsCollector).await.unwrap();
        let mut expected = RowAddrTreeMap::new();
        expected.insert_range(0..50);
        assert_eq!(result, SearchResult::at_most(expected));

        // Test search for Date32 value in second zone
        let query = BloomFilterQuery::Equals(ScalarValue::Date32(Some(75)));
        let result = index.search(&query, &NoOpMetricsCollector).await.unwrap();
        let mut expected = RowAddrTreeMap::new();
        expected.insert_range(50..100);
        assert_eq!(result, SearchResult::at_most(expected));

        // Test search for Date32 value that doesn't exist
        let query = BloomFilterQuery::Equals(ScalarValue::Date32(Some(500)));
        let result = index.search(&query, &NoOpMetricsCollector).await.unwrap();
        assert_eq!(result, SearchResult::at_most(RowAddrTreeMap::new()));
    }

    #[tokio::test]
    async fn test_timestamp_types_bloomfilter_index() {
        let tmpdir = TempObjDir::default();
        let test_store = Arc::new(LanceIndexStore::new(
            Arc::new(ObjectStore::local()),
            tmpdir.clone(),
            Arc::new(LanceCache::no_cache()),
        ));

        // Test Timestamp with nanosecond precision - use simple incrementing values
        let timestamp_values: Vec<i64> = (0..100).map(|i| 1_000_000_000i64 + (i as i64)).collect();

        let timestamp_data = arrow_array::TimestampNanosecondArray::from(timestamp_values.clone());
        let schema = Arc::new(Schema::new(vec![Field::new(
            VALUE_COLUMN_NAME,
            DataType::Timestamp(arrow_schema::TimeUnit::Nanosecond, None),
            false,
        )]));
        let data = RecordBatch::try_new(schema.clone(), vec![Arc::new(timestamp_data)]).unwrap();
        let data_stream: SendableRecordBatchStream = Box::pin(RecordBatchStreamAdapter::new(
            schema,
            stream::once(std::future::ready(Ok(data))),
        ));
        let data_stream = add_row_addr(data_stream);

        BloomFilterIndexPlugin::train_bloomfilter_index(
            data_stream,
            test_store.as_ref(),
            Some(BloomFilterIndexBuilderParams::new(50, 0.01)),
        )
        .await
        .unwrap();

        // Load the Timestamp index
        let index = BloomFilterIndex::load(test_store.clone(), None, &LanceCache::no_cache())
            .await
            .expect("Failed to load Timestamp BloomFilterIndex");

        assert_eq!(index.zones.len(), 2); // 100 rows, zone size 50

        // Test search for Timestamp value in first zone
        let first_timestamp = timestamp_values[25];
        let query = BloomFilterQuery::Equals(ScalarValue::TimestampNanosecond(
            Some(first_timestamp),
            None,
        ));
        let result = index.search(&query, &NoOpMetricsCollector).await.unwrap();
        let mut expected = RowAddrTreeMap::new();
        expected.insert_range(0..50);
        assert_eq!(result, SearchResult::at_most(expected));

        // Test search for Timestamp value in second zone
        let second_timestamp = timestamp_values[75];
        let query = BloomFilterQuery::Equals(ScalarValue::TimestampNanosecond(
            Some(second_timestamp),
            None,
        ));
        let result = index.search(&query, &NoOpMetricsCollector).await.unwrap();
        let mut expected = RowAddrTreeMap::new();
        expected.insert_range(50..100);
        assert_eq!(result, SearchResult::at_most(expected));

        // Test search for Timestamp value that doesn't exist
        let query =
            BloomFilterQuery::Equals(ScalarValue::TimestampNanosecond(Some(999_999_999i64), None));
        let result = index.search(&query, &NoOpMetricsCollector).await.unwrap();
        assert_eq!(result, SearchResult::at_most(RowAddrTreeMap::new()));

        // Test IsIn query with multiple timestamp values
        let query = BloomFilterQuery::IsIn(vec![
            ScalarValue::TimestampNanosecond(Some(timestamp_values[10]), None), // First zone
            ScalarValue::TimestampNanosecond(Some(timestamp_values[85]), None), // Second zone
            ScalarValue::TimestampNanosecond(Some(999_999_999i64), None),       // Not present
        ]);
        let result = index.search(&query, &NoOpMetricsCollector).await.unwrap();
        let mut expected = RowAddrTreeMap::new();
        expected.insert_range(0..100); // Should match both zones
        assert_eq!(result, SearchResult::at_most(expected));
    }

    #[tokio::test]
    async fn test_time_types_bloomfilter_index() {
        let tmpdir = TempObjDir::default();
        let test_store = Arc::new(LanceIndexStore::new(
            Arc::new(ObjectStore::local()),
            tmpdir.clone(),
            Arc::new(LanceCache::no_cache()),
        ));

        // Test Time64 with microsecond precision (stored as i64)
        let time_values: Vec<i64> = (0..100)
            .map(|i| (i as i64) * 3_600_000_000) // Hours in microseconds
            .collect();

        let time_data = arrow_array::Time64MicrosecondArray::from(time_values.clone());
        let schema = Arc::new(Schema::new(vec![Field::new(
            VALUE_COLUMN_NAME,
            DataType::Time64(arrow_schema::TimeUnit::Microsecond),
            false,
        )]));
        let data = RecordBatch::try_new(schema.clone(), vec![Arc::new(time_data)]).unwrap();
        let data_stream: SendableRecordBatchStream = Box::pin(RecordBatchStreamAdapter::new(
            schema,
            stream::once(std::future::ready(Ok(data))),
        ));
        let data_stream = add_row_addr(data_stream);

        BloomFilterIndexPlugin::train_bloomfilter_index(
            data_stream,
            test_store.as_ref(),
            Some(BloomFilterIndexBuilderParams::new(25, 0.05)),
        )
        .await
        .unwrap();

        // Load the Time64 index
        let index = BloomFilterIndex::load(test_store.clone(), None, &LanceCache::no_cache())
            .await
            .expect("Failed to load Time64 BloomFilterIndex");

        assert_eq!(index.zones.len(), 4); // 100 rows, zone size 25

        // Test search for Time64 value in first zone
        let first_time = time_values[10];
        let query = BloomFilterQuery::Equals(ScalarValue::Time64Microsecond(Some(first_time)));
        let result = index.search(&query, &NoOpMetricsCollector).await.unwrap();
        let mut expected = RowAddrTreeMap::new();
        expected.insert_range(0..25);
        assert_eq!(result, SearchResult::at_most(expected));

        // Test search for Time64 value that doesn't exist
        let query = BloomFilterQuery::Equals(ScalarValue::Time64Microsecond(Some(999_999_999i64)));
        let result = index.search(&query, &NoOpMetricsCollector).await.unwrap();
        assert_eq!(result, SearchResult::at_most(RowAddrTreeMap::new()));
    }

    #[tokio::test]
    async fn test_bloomfilter_supported_operations() {
        let tmpdir = TempObjDir::default();
        let test_store = Arc::new(LanceIndexStore::new(
            Arc::new(ObjectStore::local()),
            tmpdir.clone(),
            Arc::new(LanceCache::no_cache()),
        ));

        let data = arrow_array::Int32Array::from_iter_values(0..1000);
        let schema = Arc::new(Schema::new(vec![Field::new(
            VALUE_COLUMN_NAME,
            DataType::Int32,
            false,
        )]));
        let data = RecordBatch::try_new(schema.clone(), vec![Arc::new(data)]).unwrap();
        let data_stream: SendableRecordBatchStream = Box::pin(RecordBatchStreamAdapter::new(
            schema,
            stream::once(std::future::ready(Ok(data))),
        ));
        let data_stream = add_row_addr(data_stream);

        BloomFilterIndexPlugin::train_bloomfilter_index(
            data_stream,
            test_store.as_ref(),
            Some(BloomFilterIndexBuilderParams::new(250, 0.01)), // 4 zones total
        )
        .await
        .unwrap();

        // Load the index
        let index = BloomFilterIndex::load(test_store.clone(), None, &LanceCache::no_cache())
            .await
            .expect("Failed to load BloomFilterIndex");

        assert_eq!(index.zones.len(), 4);

        // Test that bloom filters support the operations they are designed for
        // Test a specific equality query
        let query = BloomFilterQuery::Equals(ScalarValue::Int32(Some(500)));
        let result = index.search(&query, &NoOpMetricsCollector).await.unwrap();
        let mut expected = RowAddrTreeMap::new();
        expected.insert_range(500..750); // Should match the zone containing 500
        assert_eq!(result, SearchResult::at_most(expected));

        // Test IsNull query (no nulls in data, should return exact empty set)
        let query = BloomFilterQuery::IsNull();
        let result = index.search(&query, &NoOpMetricsCollector).await.unwrap();
        assert_eq!(result, SearchResult::exact(RowAddrTreeMap::new()));

        // Test IsIn query
        let query = BloomFilterQuery::IsIn(vec![
            ScalarValue::Int32(Some(100)),
            ScalarValue::Int32(Some(600)),
        ]);
        let result = index.search(&query, &NoOpMetricsCollector).await.unwrap();
        let mut expected = RowAddrTreeMap::new();
        expected.insert_range(0..250); // Zone containing 100
        expected.insert_range(500..750); // Zone containing 600
        assert_eq!(result, SearchResult::at_most(expected));
    }

    #[tokio::test]
    async fn test_bloomfilter_null_handling_in_queries() {
        // Test that bloomfilter index correctly returns null_list for queries
        let tmpdir = TempObjDir::default();
        let store = Arc::new(LanceIndexStore::new(
            Arc::new(ObjectStore::local()),
            tmpdir.clone(),
            Arc::new(LanceCache::no_cache()),
        ));

        // Create test data: [0, 5, null]
        let batch = record_batch!(
            (VALUE_COLUMN_NAME, Int64, [Some(0), Some(5), None]),
            (ROW_ADDR, UInt64, [0, 1, 2])
        )
        .unwrap();
        let schema = batch.schema();
        let stream = stream::once(async move { Ok(batch) });
        let stream = Box::pin(RecordBatchStreamAdapter::new(schema, stream));

        // Train and write the bloomfilter index
        BloomFilterIndexPlugin::train_bloomfilter_index(stream, store.as_ref(), None)
            .await
            .unwrap();

        let cache = LanceCache::with_capacity(1024 * 1024);
        let index = BloomFilterIndex::load(store.clone(), None, &cache)
            .await
            .unwrap();

        // Test 1: Search for value 5 - bloomfilter should return at_most with all rows
        // Like ZoneMap, BloomFilter returns AtMost (superset) and includes nulls
        let query = BloomFilterQuery::Equals(ScalarValue::Int64(Some(5)));
        let result = index.search(&query, &NoOpMetricsCollector).await.unwrap();

        match result {
            SearchResult::AtMost(row_addrs) => {
                // Bloomfilter returns all rows in the zone including nulls
                let all_rows: Vec<u64> = row_addrs
                    .true_rows()
                    .row_addrs()
                    .unwrap()
                    .map(u64::from)
                    .collect();
                assert_eq!(
                    all_rows,
                    vec![0, 1, 2],
                    "Should return all rows (including nulls) since BloomFilter is inexact"
                );

                // For AtMost results, nulls are included in the superset
            }
            _ => panic!("Expected AtMost search result from bloomfilter"),
        }

        // Test 2: IsIn query - should also return all rows
        let query = BloomFilterQuery::IsIn(vec![
            ScalarValue::Int64(Some(0)),
            ScalarValue::Int64(Some(10)),
        ]);
        let result = index.search(&query, &NoOpMetricsCollector).await.unwrap();

        match result {
            SearchResult::AtMost(row_addrs) => {
                let all_rows: Vec<u64> = row_addrs
                    .true_rows()
                    .row_addrs()
                    .unwrap()
                    .map(u64::from)
                    .collect();
                assert_eq!(
                    all_rows,
                    vec![0, 1, 2],
                    "Should return all rows in zone as possible matches"
                );
            }
            _ => panic!("Expected AtMost search result from bloomfilter"),
        }
    }

    #[test]
    fn test_bloomfilter_read_batch_size_is_byte_bounded() {
        let number_of_items = 1;
        let probability = 0.25;
        let max_test_batch_bytes = 48;
        let filter_bytes = Sbbf::with_ndv_fpp(number_of_items, probability)
            .unwrap()
            .to_bytes();

        assert!(filter_bytes.len() <= max_test_batch_bytes);
        assert!(filter_bytes.len() * 2 > max_test_batch_bytes);

        assert_eq!(
            BloomFilterIndex::read_batch_size(number_of_items, probability, max_test_batch_bytes,)
                .unwrap(),
            1
        );
    }

    #[test]
    fn test_bloomfilter_read_batch_size_rejects_oversized_filter() {
        let number_of_items = 1;
        let probability = 0.25;
        let filter_bytes = Sbbf::with_ndv_fpp(number_of_items, probability)
            .unwrap()
            .to_bytes();

        let error =
            BloomFilterIndex::read_batch_size(number_of_items, probability, filter_bytes.len() - 1)
                .unwrap_err();
        assert!(
            matches!(error, Error::InvalidInput { .. }),
            "unexpected error variant: {error:?}"
        );
        assert!(
            error
                .to_string()
                .contains("exceeds max supported batch bytes"),
            "unexpected error: {error}"
        );
    }

    #[tokio::test]
    async fn test_bloomfilter_chunked_write_and_load() {
        let tmpdir = TempObjDir::default();
        let test_store = Arc::new(LanceIndexStore::new(
            Arc::new(ObjectStore::local()),
            tmpdir.clone(),
            Arc::new(LanceCache::no_cache()),
        ));

        let row_count = 5_000;
        // Sprinkle nulls at known positions (none of which are asserted individually
        // below) so the chunked write must carry a non-empty null-row bitmap through
        // multiple flushes and reload it correctly via add_global_buffer.
        let expected_null_count = (0..row_count).filter(|&i| i % 100 == 50).count();
        let values = (0..row_count)
            .map(|i| (i % 100 != 50).then_some(i))
            .collect::<Vec<_>>();
        let data = record_batch!((VALUE_COLUMN_NAME, Int32, values)).unwrap();
        let schema = data.schema();
        let data_stream: SendableRecordBatchStream = Box::pin(RecordBatchStreamAdapter::new(
            schema,
            stream::once(std::future::ready(Ok(data))),
        ));
        let data_stream = add_row_addr(data_stream);

        let mut builder =
            BloomFilterIndexBuilder::try_new(BloomFilterIndexBuilderParams::new(1, 0.25)).unwrap();
        builder.train(data_stream).await.unwrap();
        assert_eq!(builder.blocks.len(), row_count as usize);

        // Small byte limit forces the writer to emit many on-disk batches and the
        // reader to chunk its reads. The total payload is far larger than the limit,
        // so a load that concatenated everything into one BinaryArray would trip the
        // read-side cap; a correctly chunked load must still succeed.
        let max_array_length = 64;
        let filter_bytes = Sbbf::with_ndv_fpp(1, 0.25).unwrap().size_bytes();
        assert!(filter_bytes * row_count as usize > max_array_length);

        builder
            .write_index_with_max_array_length(test_store.as_ref(), max_array_length)
            .await
            .unwrap();

        let index = BloomFilterIndex::load_with_max_array_length(
            test_store.clone(),
            None,
            &LanceCache::no_cache(),
            max_array_length,
        )
        .await
        .expect("Failed to load chunked BloomFilterIndex");

        assert_eq!(index.zones.len(), row_count as usize);
        assert_eq!(index.zones[0].bound.start, 0);
        assert_eq!(index.zones[4096].bound.start, 4096);
        assert_eq!(index.zones[4999].bound.start, 4999);
        assert_eq!(index.zones[4999].bound.length, 1);
        assert!(!index.zones[4096].bloom_filter.to_bytes().is_empty());

        // The null-row bitmap is stored as a global buffer, independent of the chunked
        // zone batches. Verify it survives the multi-flush write and reloads intact.
        let null_rows = index
            .null_rows
            .as_ref()
            .expect("chunked write must preserve the null-row bitmap");
        let loaded_null_count = null_rows
            .row_addrs()
            .map(|addrs| addrs.count())
            .unwrap_or(0);
        assert_eq!(loaded_null_count, expected_null_count);
    }

    #[tokio::test]
    async fn test_bloomfilter_load_rejects_unchunked_oversized_read() {
        // Guards the read-side invariant directly: a batch whose concatenated filter
        // payload exceeds the cap must be rejected rather than building an oversized
        // BinaryArray. This is what protects `load` if its chunking ever regresses.
        let filter_bytes = Sbbf::with_ndv_fpp(1, 0.25).unwrap().to_bytes();
        let batch = BloomFilterIndexBuilder::bloomfilter_stats_as_batch(
            vec![0, 0],
            vec![0, 1],
            vec![1, 1],
            vec![false, false],
            vec![filter_bytes.clone(), filter_bytes.clone()],
        )
        .unwrap();

        // One filter fits, two together do not.
        let max_array_length = filter_bytes.len();
        let error = BloomFilterIndex::try_from_serialized(batch, max_array_length).unwrap_err();
        assert!(
            matches!(error, Error::InvalidInput { .. }),
            "unexpected error variant: {error:?}"
        );
        assert!(
            error
                .to_string()
                .contains("exceeds max supported batch bytes"),
            "unexpected error: {error}"
        );
    }

    #[tokio::test]
    async fn test_bloomfilter_chunked_write_rejects_oversized_filter() {
        let tmpdir = TempObjDir::default();
        let test_store = Arc::new(LanceIndexStore::new(
            Arc::new(ObjectStore::local()),
            tmpdir.clone(),
            Arc::new(LanceCache::no_cache()),
        ));

        let data = record_batch!((VALUE_COLUMN_NAME, Int32, [0])).unwrap();
        let schema = data.schema();
        let data_stream: SendableRecordBatchStream = Box::pin(RecordBatchStreamAdapter::new(
            schema,
            stream::once(std::future::ready(Ok(data))),
        ));
        let data_stream = add_row_addr(data_stream);

        let mut builder =
            BloomFilterIndexBuilder::try_new(BloomFilterIndexBuilderParams::new(1, 0.25)).unwrap();
        builder.train(data_stream).await.unwrap();

        let error = builder
            .write_index_with_max_array_length(test_store.as_ref(), 16)
            .await
            .unwrap_err();
        assert!(
            matches!(error, Error::InvalidInput { .. }),
            "unexpected error variant: {error:?}"
        );
        assert!(
            error
                .to_string()
                .contains("exceeds max supported batch bytes 16"),
            "unexpected error: {error}"
        );
    }

    // Writes a bloomfilter file in the legacy format (no null bitmap global buffer),
    // simulating an index created before the null bitmap feature was added.
    async fn write_legacy_bloomfilter(store: &dyn IndexStore, has_null: bool) {
        use crate::scalar::bloomfilter::{
            BLOOMFILTER_FILENAME, BLOOMFILTER_ITEM_META_KEY, BLOOMFILTER_PROBABILITY_META_KEY,
        };
        use arrow_array::BooleanArray;
        let schema = Arc::new(Schema::new(vec![
            Field::new("fragment_id", DataType::UInt64, false),
            Field::new("zone_start", DataType::UInt64, false),
            Field::new("zone_length", DataType::UInt64, false),
            Field::new("has_null", DataType::Boolean, false),
            Field::new("bloom_filter_data", DataType::Binary, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(UInt64Array::from(vec![0u64])) as _,
                Arc::new(UInt64Array::from(vec![0u64])) as _,
                Arc::new(UInt64Array::from(vec![3u64])) as _,
                Arc::new(BooleanArray::from(vec![has_null])) as _,
                Arc::new(arrow_array::BinaryArray::from_vec(vec![b"".as_ref()])) as _,
            ],
        )
        .unwrap();
        let mut file_schema = schema.as_ref().clone();
        file_schema
            .metadata
            .insert(BLOOMFILTER_ITEM_META_KEY.to_string(), "1000".to_string());
        file_schema.metadata.insert(
            BLOOMFILTER_PROBABILITY_META_KEY.to_string(),
            "0.01".to_string(),
        );
        let mut writer = store
            .new_index_file(BLOOMFILTER_FILENAME, Arc::new(file_schema))
            .await
            .unwrap();
        writer.write_record_batch(batch).await.unwrap();
        writer.finish().await.unwrap();
    }

    // Updating a legacy (null_rows = None) index must not silently treat None as
    // "no nulls".  The bug: `self.null_rows.clone().unwrap_or_default()` collapses
    // None into an empty RowAddrTreeMap; after the merge the updated index has
    // `null_rows = Some(empty)`, so an IsNull search returns `exact(empty)` — a
    // false negative even though the legacy zone recorded has_null = true.
    #[tokio::test]
    async fn test_update_legacy_none_null_rows_not_treated_as_no_nulls() {
        let tmpdir = TempObjDir::default();
        let store = Arc::new(LanceIndexStore::new(
            Arc::new(ObjectStore::local()),
            tmpdir.clone(),
            Arc::new(LanceCache::no_cache()),
        ));

        // Write a legacy-format index (no null bitmap) with has_null=true in its zone.
        write_legacy_bloomfilter(store.as_ref(), true).await;

        let index = BloomFilterIndex::load(store.clone(), None, &LanceCache::no_cache())
            .await
            .unwrap();
        assert!(
            index.null_rows.is_none(),
            "precondition: legacy null_rows is None"
        );

        // Update with new data from fragment 1 (no nulls).  The destination is the
        // same store so we can reload from it afterwards.
        let new_schema = Arc::new(Schema::new(vec![
            Field::new(VALUE_COLUMN_NAME, DataType::Int32, true),
            Field::new(ROW_ADDR, DataType::UInt64, false),
        ]));
        let new_batch = RecordBatch::try_new(
            new_schema.clone(),
            vec![
                Arc::new(arrow_array::Int32Array::from(vec![
                    Some(10i32),
                    Some(20),
                    Some(30),
                ])) as _,
                Arc::new(UInt64Array::from_iter_values(
                    (0u64..3).map(|i| (1u64 << 32) | i),
                )) as _,
            ],
        )
        .unwrap();
        let new_stream: SendableRecordBatchStream = Box::pin(RecordBatchStreamAdapter::new(
            new_schema,
            stream::once(std::future::ready(Ok(new_batch))),
        ));

        index
            .update(new_stream, store.as_ref(), None)
            .await
            .unwrap();

        let updated_index = BloomFilterIndex::load(store.clone(), None, &LanceCache::no_cache())
            .await
            .unwrap();

        // The legacy zone had has_null=true, so there ARE nulls at unknown positions.
        // An IsNull search on the updated index must NOT claim "no nulls" (exact empty).
        // It must be conservative and return AtMost, falling back to the has_null scan.
        let result = updated_index
            .search(&BloomFilterQuery::IsNull(), &NoOpMetricsCollector)
            .await
            .unwrap();

        // With the bug: null_rows = Some(empty) → returns exact(empty) ← FALSE NEGATIVE
        // With the fix: null_rows = None        → falls through to has_null scan → AtMost
        assert!(
            !result.is_exact(),
            "IsNull on an updated legacy index must not return exact(empty); \
             the legacy zone had has_null=true so nulls exist at unknown positions"
        );
    }

    #[tokio::test]
    async fn test_legacy_bloomfilter_no_null_bitmap() {
        let tmpdir = TempObjDir::default();
        let store = Arc::new(LanceIndexStore::new(
            Arc::new(ObjectStore::local()),
            tmpdir.clone(),
            Arc::new(LanceCache::no_cache()),
        ));

        write_legacy_bloomfilter(store.as_ref(), true).await;

        let index = BloomFilterIndex::load(store, None, &LanceCache::no_cache())
            .await
            .expect("failed to load legacy bloomfilter");

        assert!(
            index.null_rows.is_none(),
            "legacy index should have no null bitmap"
        );

        // IS NULL should fall back to the has_null zone scan and return AtMost, not Exact.
        let result = index
            .search(&BloomFilterQuery::IsNull(), &NoOpMetricsCollector)
            .await
            .unwrap();
        assert!(
            !result.is_exact(),
            "IS NULL on a legacy index should not be exact"
        );
    }

    #[tokio::test]
    async fn test_merge_bloomfilter_indices_preserves_exact_and_legacy_nulls() {
        fn create_store() -> (TempObjDir, Arc<LanceIndexStore>) {
            let tmpdir = TempObjDir::default();
            let store = Arc::new(LanceIndexStore::new(
                Arc::new(ObjectStore::local()),
                tmpdir.clone(),
                Arc::new(LanceCache::no_cache()),
            ));
            (tmpdir, store)
        }

        let (_first_tmpdir, first_store) = create_store();
        let (_second_tmpdir, second_store) = create_store();
        let (_merged_tmpdir, merged_store) = create_store();
        let params = BloomFilterIndexBuilderParams::new(1000, 0.01);
        for (fragment_id, store) in [(0_u64, &first_store), (1_u64, &second_store)] {
            let row_base = fragment_id << 32;
            let batch = record_batch!(
                (VALUE_COLUMN_NAME, Int64, [Some(10), None, Some(30)]),
                (ROW_ADDR, UInt64, [row_base, row_base + 1, row_base + 2])
            )
            .unwrap();
            let schema = batch.schema();
            let stream = Box::pin(RecordBatchStreamAdapter::new(
                schema,
                stream::once(async move { Ok(batch) }),
            ));
            BloomFilterIndexPlugin::train_bloomfilter_index(
                stream,
                store.as_ref(),
                Some(params.clone()),
            )
            .await
            .unwrap();
        }

        let first = BloomFilterIndex::load(first_store.clone(), None, &LanceCache::no_cache())
            .await
            .unwrap();
        let second = BloomFilterIndex::load(second_store, None, &LanceCache::no_cache())
            .await
            .unwrap();
        merge_bloomfilter_indices(
            &[
                (first.as_ref(), &RoaringBitmap::from_iter([0])),
                (second.as_ref(), &RoaringBitmap::from_iter([1])),
            ],
            merged_store.as_ref(),
        )
        .await
        .unwrap();
        let merged = BloomFilterIndex::load(merged_store.clone(), None, &LanceCache::no_cache())
            .await
            .unwrap();
        let mut expected_nulls = RowAddrTreeMap::new();
        expected_nulls.insert(1);
        expected_nulls.insert((1_u64 << 32) + 1);
        assert_eq!(
            merged
                .search(&BloomFilterQuery::IsNull(), &NoOpMetricsCollector)
                .await
                .unwrap(),
            SearchResult::exact(expected_nulls)
        );

        let remapped_base = 2_u64 << 32;
        let remapper = FragReuseIndexHandle(Arc::new(FragReuseIndex::new(
            uuid::Uuid::new_v4(),
            vec![HashMap::from([
                (0, Some(remapped_base)),
                (1, Some(remapped_base + 1)),
                (2, Some(remapped_base + 2)),
            ])],
            FragReuseIndexDetails { versions: vec![] },
        )));
        let remapped_first = BloomFilterIndex::load(
            first_store,
            Some(Arc::new(remapper)),
            &LanceCache::no_cache(),
        )
        .await
        .unwrap();
        let (_remapped_tmpdir, remapped_store) = create_store();
        merge_bloomfilter_indices(
            &[(remapped_first.as_ref(), &RoaringBitmap::from_iter([2]))],
            remapped_store.as_ref(),
        )
        .await
        .unwrap();
        let remapped = BloomFilterIndex::load(remapped_store, None, &LanceCache::no_cache())
            .await
            .unwrap();
        let mut expected_remapped_nulls = RowAddrTreeMap::new();
        expected_remapped_nulls.insert(remapped_base + 1);
        assert_eq!(
            remapped
                .search(&BloomFilterQuery::IsNull(), &NoOpMetricsCollector)
                .await
                .unwrap(),
            SearchResult::exact(expected_remapped_nulls)
        );
        let candidates = remapped
            .search(
                &BloomFilterQuery::Equals(ScalarValue::Int64(Some(10))),
                &NoOpMetricsCollector,
            )
            .await
            .unwrap();
        assert!(
            candidates
                .row_addrs()
                .true_rows()
                .row_addrs()
                .unwrap()
                .map(u64::from)
                .any(|row_id| row_id == remapped_base)
        );

        let (_legacy_tmpdir, legacy_store) = create_store();
        let (_legacy_merged_tmpdir, legacy_merged_store) = create_store();
        write_legacy_bloomfilter(legacy_store.as_ref(), true).await;
        let legacy = BloomFilterIndex::load(legacy_store, None, &LanceCache::no_cache())
            .await
            .unwrap();
        merge_bloomfilter_indices(
            &[
                (legacy.as_ref(), &RoaringBitmap::from_iter([0])),
                (second.as_ref(), &RoaringBitmap::from_iter([1])),
            ],
            legacy_merged_store.as_ref(),
        )
        .await
        .unwrap();
        let legacy_merged =
            BloomFilterIndex::load(legacy_merged_store, None, &LanceCache::no_cache())
                .await
                .unwrap();
        assert!(
            !legacy_merged
                .search(&BloomFilterQuery::IsNull(), &NoOpMetricsCollector)
                .await
                .unwrap()
                .is_exact()
        );

        let (_trimmed_tmpdir, trimmed_store) = create_store();
        let mut ignored_legacy = legacy.as_ref().clone();
        ignored_legacy.probability = 0.5;
        merge_bloomfilter_indices(
            &[
                (&ignored_legacy, &RoaringBitmap::new()),
                (second.as_ref(), &RoaringBitmap::from_iter([1])),
            ],
            trimmed_store.as_ref(),
        )
        .await
        .unwrap();
        let trimmed = BloomFilterIndex::load(trimmed_store, None, &LanceCache::no_cache())
            .await
            .unwrap();
        let mut expected_nulls = RowAddrTreeMap::new();
        expected_nulls.insert((1_u64 << 32) + 1);
        assert_eq!(
            trimmed
                .search(&BloomFilterQuery::IsNull(), &NoOpMetricsCollector)
                .await
                .unwrap(),
            SearchResult::exact(expected_nulls)
        );

        let (_empty_tmpdir, empty_store) = create_store();
        merge_bloomfilter_indices(
            &[(&ignored_legacy, &RoaringBitmap::new())],
            empty_store.as_ref(),
        )
        .await
        .unwrap();
        let empty = BloomFilterIndex::load(empty_store, None, &LanceCache::no_cache())
            .await
            .unwrap();
        assert_eq!(
            empty
                .search(&BloomFilterQuery::IsNull(), &NoOpMetricsCollector)
                .await
                .unwrap(),
            SearchResult::exact(RowAddrTreeMap::new())
        );
    }
}
