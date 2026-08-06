// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::{collections::BTreeMap, collections::HashMap, ops::Range, pin::Pin, sync::Arc};

use crate::dataset::fragment::FragReadConfig;
use crate::dataset::rowids::get_row_id_index;
use crate::io::exec::AddRowOffsetExec;
use crate::{Error, Result};
use arrow::{compute::concat_batches, datatypes::UInt64Type};
use arrow_array::cast::AsArray;
use arrow_array::{Array, ArrayRef, RecordBatch, StructArray, UInt64Array};
use arrow_buffer::{ArrowNativeType, BooleanBuffer, Buffer, NullBuffer};
use arrow_schema::Field as ArrowField;
use datafusion::common::Column;
use datafusion::error::DataFusionError;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion_expr::Expr;
use futures::{Future, Stream, StreamExt, TryStreamExt};
use lance_arrow::RecordBatchExt;
use lance_arrow::json::convert_lance_json_to_arrow;
use lance_core::datatypes::Schema;
use lance_core::utils::address::RowAddress;
use lance_core::utils::deletion::OffsetMapper;
use lance_core::{ROW_ADDR, ROW_OFFSET};
use lance_datafusion::projection::{OutputColumn, ProjectionPlan};

use super::ProjectionRequest;
use super::{Dataset, fragment::FileFragment, scanner::DatasetRecordBatchStream};

/// Convert a list of row offsets to a list of row addresses
///
/// A row offset is a 64-bit integer in the range [0, num_rows_in_dataset]
///
/// For example, if there are two fragments, each with 100 rows, then the row offset
/// 150 maps to the address (1, 50), the 50th row in the second fragment
///
/// Row offsets are useful for sampling because you don't need to know the ids / addresses
/// up front (you can just use the range [0, num_rows_in_dataset]) and they can be cheaply
/// converted to row addresses in a single pass through fragment sizes (which is what this method does)
///
/// This method accounts for deletions.  If there is one fragment with 100 rows and rows 50-59 are
/// deleted then the row offset 70 will map to the address (0, 80) and the row offset 100 will map
/// to the address (1, 10), assuming the second fragment starts with 10 undeleted rows.
///
/// If any offsets are beyond the end of the dataset, they will be mapped to a tombstone row address.
pub async fn row_offsets_to_row_addresses(
    fragments: &[FileFragment],
    row_indices: &[u64],
) -> Result<Vec<u64>> {
    let mut perm = permutation::sort(row_indices);
    let sorted_offsets = perm.apply_slice(row_indices);

    let mut frag_iter = fragments.iter();
    let mut cur_frag = frag_iter.next();
    let mut cur_frag_rows = if let Some(cur_frag) = cur_frag {
        cur_frag.count_rows(None).await? as u64
    } else {
        0
    };
    let mut offset_mapper = if let Some(cur_frag) = cur_frag {
        let deletion_vector = cur_frag.get_deletion_vector().await?;
        deletion_vector.map(OffsetMapper::new)
    } else {
        None
    };
    let mut frag_offset = 0;

    let mut addrs: Vec<u64> = Vec::with_capacity(sorted_offsets.len());
    for sorted_offset in sorted_offsets.into_iter() {
        while cur_frag.is_some() && sorted_offset >= frag_offset + cur_frag_rows {
            frag_offset += cur_frag_rows;
            cur_frag = frag_iter.next();
            cur_frag_rows = if let Some(cur_frag) = cur_frag {
                cur_frag.count_rows(None).await? as u64
            } else {
                0
            };
            offset_mapper = if let Some(cur_frag) = cur_frag {
                let deletion_vector = cur_frag.get_deletion_vector().await?;
                deletion_vector.map(OffsetMapper::new)
            } else {
                None
            };
        }
        let Some(cur_frag) = cur_frag else {
            addrs.push(RowAddress::TOMBSTONE_ROW);
            continue;
        };

        let mut local_offset = (sorted_offset - frag_offset) as u32;
        if let Some(offset_mapper) = &mut offset_mapper {
            local_offset = offset_mapper.map_offset(local_offset);
        };
        let row_addr = RowAddress::new_from_parts(cur_frag.id() as u32, local_offset);
        addrs.push(u64::from(row_addr));
    }

    // Restore the original order
    perm.apply_inv_slice_in_place(&mut addrs);
    Ok(addrs)
}

pub async fn take(
    dataset: &Dataset,
    offsets: &[u64],
    projection: ProjectionRequest,
) -> Result<RecordBatch> {
    let projection = projection.into_projection_plan(Arc::new(dataset.clone()))?;
    if offsets.is_empty() {
        return to_logical_json_batch(RecordBatch::new_empty(Arc::new(
            projection.output_schema()?,
        )));
    }

    // First, convert the dataset offsets into row addresses
    let fragments = dataset.get_fragments();
    let addrs = row_offsets_to_row_addresses(&fragments, offsets).await?;

    let builder = TakeBuilder::try_new_from_addresses(
        Arc::new(dataset.clone()),
        addrs,
        Arc::new(projection),
    )?;

    take_rows(builder).await
}

/// Take rows by the internal ROW ids.
#[allow(clippy::needless_question_mark)]
async fn do_take_rows(
    mut builder: TakeBuilder,
    projection: Arc<ProjectionPlan>,
) -> Result<RecordBatch> {
    // If we need row addresses in output, add to projection's output expressions
    let projection = if builder.with_row_address {
        let mut proj = (*projection).clone();
        // Add _rowaddr to output if not already present
        if !proj
            .requested_output_expr
            .iter()
            .any(|c| c.name == ROW_ADDR)
        {
            proj.requested_output_expr.push(OutputColumn {
                expr: Expr::Column(Column::from_name(ROW_ADDR)),
                name: ROW_ADDR.to_string(),
            });
        }
        Arc::new(proj)
    } else {
        projection
    };

    let with_row_id_in_projection = projection.physical_projection.with_row_id;
    let with_row_addr_in_projection = projection.physical_projection.with_row_addr;
    let with_row_created_at_version_in_projection =
        projection.physical_projection.with_row_created_at_version;
    let with_row_last_updated_at_version_in_projection = projection
        .physical_projection
        .with_row_last_updated_at_version;

    let row_addrs = builder.get_row_addrs().await?.clone();

    if row_addrs.is_empty() {
        // It is possible that `row_id_index` returns None when a fragment has been wholly deleted
        let empty_batch = RecordBatch::new_empty(Arc::new(builder.projection.output_schema()?));
        // If row addresses were requested, add an empty row address column.
        // This ensures callers that expect the _rowaddr column don't panic.
        if builder.with_row_address {
            let row_addr_col = Arc::new(UInt64Array::from(Vec::<u64>::new()));
            let row_addr_field =
                ArrowField::new(ROW_ADDR, arrow::datatypes::DataType::UInt64, false);
            return to_logical_json_batch(
                empty_batch.try_with_column(row_addr_field, row_addr_col)?,
            );
        }
        return to_logical_json_batch(empty_batch);
    }

    let row_addr_stats = check_row_addrs(&row_addrs);

    // This method is mostly to annotate the send bound to avoid the
    // higher-order lifetime error.
    // manually implemented async for Send bound
    #[allow(clippy::manual_async_fn)]
    fn do_take(
        fragment: FileFragment,
        row_offsets: Vec<u32>,
        projection: Arc<Schema>,
        with_row_id: bool,
        with_row_addresses: bool,
        with_row_created_at_version: bool,
        with_row_last_updated_at_version: bool,
    ) -> impl Future<Output = Result<RecordBatch>> + Send {
        async move {
            fragment
                .take_rows(
                    &row_offsets,
                    projection.as_ref(),
                    with_row_id,
                    with_row_addresses,
                    with_row_created_at_version,
                    with_row_last_updated_at_version,
                )
                .await
        }
    }

    let physical_schema = Arc::new(projection.physical_projection.to_bare_schema());
    let mut batch = if row_addr_stats.contiguous {
        // Fastest path: Can use `read_range` directly
        let start = row_addrs.first().expect("empty range passed to take_rows");
        let fragment_id = (start >> 32) as usize;
        let range_start = *start as u32 as usize;
        let range_end = *row_addrs.last().expect("empty range passed to take_rows") as u32 as usize;
        let range = range_start..(range_end + 1);

        let fragment = builder.dataset.get_fragment(fragment_id).ok_or_else(|| {
            Error::invalid_input(format!(
                "rowaddr start: {} belongs to non-existent fragment: {}",
                start, fragment_id
            ))
        })?;

        let read_config = FragReadConfig::default()
            .with_row_id(with_row_id_in_projection)
            .with_row_address(with_row_addr_in_projection)
            .with_row_created_at_version(with_row_created_at_version_in_projection)
            .with_row_last_updated_at_version(with_row_last_updated_at_version_in_projection);
        let reader = fragment.open(&physical_schema, read_config).await?;
        reader.legacy_read_range_as_batch(range).await
    } else if row_addr_stats.sorted {
        // Don't need to re-arrange data, just concatenate
        let mut batches: Vec<_> = Vec::new();
        let mut current_fragment = row_addrs[0] >> 32;
        let mut current_start = 0;
        let mut row_addr_iter = row_addrs.iter().enumerate();
        'outer: loop {
            let (fragment_id, range) = loop {
                if let Some((i, row_addr)) = row_addr_iter.next() {
                    let fragment_id = row_addr >> 32;
                    if fragment_id != current_fragment {
                        let next = (current_fragment, current_start..i);
                        current_fragment = fragment_id;
                        current_start = i;
                        break next;
                    }
                } else if current_start != row_addrs.len() {
                    let next = (current_fragment, current_start..row_addrs.len());
                    current_start = row_addrs.len();
                    break next;
                } else {
                    break 'outer;
                }
            };

            let fragment = builder
                .dataset
                .get_fragment(fragment_id as usize)
                .ok_or_else(|| {
                    Error::invalid_input(format!(
                        "rowaddr {} belongs to non-existent fragment: {}",
                        row_addrs[range.start], fragment_id
                    ))
                })?;
            let row_offsets: Vec<u32> = row_addrs[range].iter().map(|x| *x as u32).collect();

            let batch_fut = do_take(
                fragment,
                row_offsets,
                physical_schema.clone(),
                with_row_id_in_projection,
                with_row_addr_in_projection,
                with_row_created_at_version_in_projection,
                with_row_last_updated_at_version_in_projection,
            );
            batches.push(batch_fut);
        }
        let batches: Vec<RecordBatch> = futures::stream::iter(batches)
            .buffered(builder.dataset.object_store.io_parallelism())
            .try_collect()
            .await?;
        Ok(concat_batches(&batches[0].schema(), &batches)?)
    } else {
        // Slow case: need to re-map data into expected order
        let mut sorted_row_addrs = row_addrs.clone();
        sorted_row_addrs.sort();
        // Go ahead and dedup, we will reinsert duplicates during the remapping
        sorted_row_addrs.dedup();
        // Group ROW Ids by the fragment
        let mut row_addrs_per_fragment: BTreeMap<u32, Vec<u32>> = BTreeMap::new();
        sorted_row_addrs.iter().for_each(|row_addr| {
            let row_addr = RowAddress::from(*row_addr);
            let fragment_id = row_addr.fragment_id();
            let offset = row_addr.row_offset();
            row_addrs_per_fragment
                .entry(fragment_id)
                .and_modify(|v| v.push(offset))
                .or_insert_with(|| vec![offset]);
        });

        let addressed_ids: Vec<u32> = row_addrs_per_fragment.keys().copied().collect();
        let fragments = builder
            .dataset
            .get_existing_fragments_from_ids(&addressed_ids);
        let fragment_and_indices = fragments.into_iter().filter_map(|f| {
            let row_offset = row_addrs_per_fragment.remove(&(f.id() as u32))?;
            Some((f, row_offset))
        });

        let mut batches = futures::stream::iter(fragment_and_indices)
            .map(|(fragment, indices)| {
                do_take(
                    fragment,
                    indices,
                    physical_schema.clone(),
                    with_row_id_in_projection,
                    true,
                    with_row_created_at_version_in_projection,
                    with_row_last_updated_at_version_in_projection,
                )
            })
            .buffered(builder.dataset.object_store.io_parallelism())
            .try_collect::<Vec<_>>()
            .await?;
        let one_batch = if batches.len() > 1 {
            concat_batches(&batches[0].schema(), &batches)?
        } else {
            batches.pop().unwrap()
        };
        // Note: one_batch may contains fewer rows than the number of requested
        // row ids because some rows may have been deleted. Because of this, we
        // get the results with row ids so that we can re-order the results
        // to match the requested order.

        let returned_row_addr = one_batch
            .column_by_name(ROW_ADDR)
            .ok_or_else(|| Error::internal("_rowaddr column not found"))?
            .as_primitive::<UInt64Type>()
            .values();

        let addr_to_pos: HashMap<u64, u64> = returned_row_addr
            .iter()
            .enumerate()
            .map(|(i, addr)| (*addr, i as u64))
            .collect();
        let remapping_index: UInt64Array = row_addrs
            .iter()
            .filter_map(|o| addr_to_pos.get(o).copied())
            .collect();

        // remapping_index may be greater than the number of rows in one_batch
        // if there are duplicates in the requested row ids. This is expected.
        debug_assert!(remapping_index.len() >= one_batch.num_rows());

        // There's a bug in arrow_select::take::take, that it doesn't handle empty struct correctly,
        // so we need to handle it manually here.
        // TODO: remove this once the bug is fixed.
        let struct_arr: StructArray = one_batch.into();
        let reordered = take_struct_array(&struct_arr, &remapping_index)?;
        Ok(reordered.into())
    }?;

    if builder.with_row_address || projection.must_add_row_offset {
        // compile `ROW_ADDR` column
        if batch.num_rows() != row_addrs.len() {
            return Err(Error::not_supported_source(format!(
                "Expected {} rows, got {}.  A take operation that includes row addresses must not target deleted rows.",
                row_addrs.len(),
                batch.num_rows()
            ).into()));
        }

        let row_addr_col: ArrayRef = Arc::new(UInt64Array::from(row_addrs));

        if projection.must_add_row_offset {
            // compile and inject `ROW_OFFSET` column
            let row_offset_col =
                AddRowOffsetExec::compute_row_offset_array(&row_addr_col, builder.dataset).await?;
            let row_offset_field =
                ArrowField::new(ROW_OFFSET, arrow::datatypes::DataType::UInt64, false);
            if batch.schema().column_with_name(ROW_OFFSET).is_none() {
                batch = batch.try_with_column(row_offset_field, row_offset_col)?;
            }
        }

        if builder.with_row_address {
            // inject `ROW_ADDR` column
            let row_addr_field =
                ArrowField::new(ROW_ADDR, arrow::datatypes::DataType::UInt64, false);
            if batch.schema().column_with_name(ROW_ADDR).is_none() {
                batch = batch.try_with_column(row_addr_field, row_addr_col)?;
            }
        }
    }

    to_logical_json_batch(projection.project_batch(batch).await?)
}

async fn take_rows(builder: TakeBuilder) -> Result<RecordBatch> {
    if builder.is_empty() {
        return to_logical_json_batch(RecordBatch::new_empty(Arc::new(
            builder.projection.output_schema()?,
        )));
    }

    let projection = builder.projection.clone();

    do_take_rows(builder, projection).await
}

fn to_logical_json_batch(batch: RecordBatch) -> Result<RecordBatch> {
    Ok(convert_lance_json_to_arrow(&batch)?)
}

/// Get a stream of batches based on iterator of ranges of row numbers.
///
/// This is an experimental API. It may change at any time.
pub fn take_scan(
    dataset: &Dataset,
    row_ranges: Pin<Box<dyn Stream<Item = Result<Range<u64>>> + Send>>,
    projection: Arc<Schema>,
    batch_readahead: usize,
) -> DatasetRecordBatchStream {
    let arrow_schema = Arc::new(projection.as_ref().into());
    let dataset = Arc::new(dataset.clone());
    let batch_stream = row_ranges
        .map(move |res| {
            let dataset = dataset.clone();
            let projection = projection.clone();
            let fut = async move {
                let range = res.map_err(|err| DataFusionError::External(Box::new(err)))?;
                let row_pos: Vec<u64> = (range.start..range.end).collect();
                dataset
                    .take(&row_pos, ProjectionRequest::Schema(projection.clone()))
                    .await
                    .map_err(|err| DataFusionError::External(Box::new(err)))
            };
            async move { tokio::task::spawn(fut).await.unwrap() }
        })
        .buffered(batch_readahead);

    DatasetRecordBatchStream::new(Box::pin(RecordBatchStreamAdapter::new(
        arrow_schema,
        batch_stream,
    )))
}

struct RowAddressStats {
    sorted: bool,
    contiguous: bool,
}

fn check_row_addrs(row_addrs: &[u64]) -> RowAddressStats {
    let mut sorted = true;
    let mut contiguous = true;

    if row_addrs.is_empty() {
        return RowAddressStats { sorted, contiguous };
    }

    let mut last_offset = row_addrs[0];
    let first_fragment_id = row_addrs[0] >> 32;

    for addr in row_addrs.iter().skip(1) {
        sorted &= *addr >= last_offset;
        contiguous &= *addr == last_offset + 1;
        // Contiguous also requires the fragment ids are all the same
        contiguous &= (*addr >> 32) == first_fragment_id;
        last_offset = *addr;
    }

    RowAddressStats { sorted, contiguous }
}

/// Builder for the `take` operation.
#[derive(Clone, Debug)]
pub struct TakeBuilder {
    dataset: Arc<Dataset>,
    row_ids: Option<Vec<u64>>,
    row_addrs: Option<Vec<u64>>,
    projection: Arc<ProjectionPlan>,
    with_row_address: bool,
    missing_row_policy: MissingRowPolicy,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(super) enum MissingRowPolicy {
    #[default]
    Ignore,
    Error,
}

impl TakeBuilder {
    /// Create a new `TakeBuilder` for taking by id
    pub fn try_new_from_ids(
        dataset: Arc<Dataset>,
        row_ids: Vec<u64>,
        projection: ProjectionRequest,
    ) -> Result<Self> {
        Ok(Self {
            row_ids: Some(row_ids),
            row_addrs: None,
            projection: Arc::new(projection.into_projection_plan(dataset.clone())?),
            dataset,
            with_row_address: false,
            missing_row_policy: MissingRowPolicy::default(),
        })
    }

    /// Create a new `TakeBuilder` for taking by address
    pub fn try_new_from_addresses(
        dataset: Arc<Dataset>,
        addresses: Vec<u64>,
        projection: Arc<ProjectionPlan>,
    ) -> Result<Self> {
        Ok(Self {
            row_ids: None,
            row_addrs: Some(addresses),
            projection,
            dataset,
            with_row_address: false,
            missing_row_policy: MissingRowPolicy::default(),
        })
    }

    /// Adds row addresses to the output
    pub fn with_row_address(mut self, with_row_address: bool) -> Self {
        self.with_row_address = with_row_address;
        self
    }

    pub(super) fn with_missing_row_policy(mut self, policy: MissingRowPolicy) -> Self {
        self.missing_row_policy = policy;
        self
    }

    /// Execute the take operation and return a single batch
    pub async fn execute(self) -> Result<RecordBatch> {
        take_rows(self).await
    }

    pub fn is_empty(&self) -> bool {
        match (self.row_ids.as_ref(), self.row_addrs.as_ref()) {
            (Some(ids), _) => ids.is_empty(),
            (_, Some(addrs)) => addrs.is_empty(),
            _ => unreachable!(),
        }
    }

    async fn get_row_addrs(&mut self) -> Result<&Vec<u64>> {
        if self.row_addrs.is_none() {
            let row_ids = self
                .row_ids
                .as_ref()
                .expect("row_ids must be set if row_addrs is not");
            let addrs = if let Some(row_id_index) = get_row_id_index(&self.dataset).await? {
                let resolved = row_id_index.get_many(row_ids);
                if self.missing_row_policy == MissingRowPolicy::Error
                    && let Some(first_missing_index) =
                        resolved.iter().position(|address| address.is_none())
                {
                    let missing_count = resolved.iter().filter(|address| address.is_none()).count();
                    return Err(Error::invalid_input(format!(
                        "Could not resolve all requested row IDs: requested {}, resolved {}; first missing row ID {} was deleted or not found",
                        row_ids.len(),
                        row_ids.len() - missing_count,
                        row_ids[first_missing_index]
                    )));
                }
                resolved
                    .into_iter()
                    .filter_map(|opt| opt.map(|address| address.into()))
                    .collect::<Vec<_>>()
            } else {
                row_ids.clone()
            };
            self.row_addrs = Some(addrs);
        }
        Ok(self.row_addrs.as_ref().unwrap())
    }
}

fn take_struct_array(array: &StructArray, indices: &UInt64Array) -> Result<StructArray> {
    let nulls = array.nulls().map(|nulls| {
        let is_valid = indices.iter().map(|index| {
            if let Some(index) = index {
                nulls.is_valid(index.to_usize().unwrap())
            } else {
                false
            }
        });
        NullBuffer::new(BooleanBuffer::new(
            Buffer::from_iter(is_valid),
            0,
            indices.len(),
        ))
    });

    if array.fields().is_empty() {
        return Ok(StructArray::new_empty_fields(indices.len(), nulls));
    }

    let arrays = array
        .columns()
        .iter()
        .map(|array| {
            let array = match array.data_type() {
                arrow::datatypes::DataType::Struct(_) => {
                    Arc::new(take_struct_array(array.as_struct(), indices)?)
                }
                _ => arrow_select::take::take(array, indices, None)?,
            };
            Ok(array)
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(StructArray::new(array.fields().clone(), arrays, nulls))
}

#[cfg(test)]
mod test {
    use arrow_array::{
        Int32Array, LargeBinaryArray, ListArray, RecordBatchIterator, StringArray, StructArray,
    };
    use arrow_buffer::{OffsetBuffer, ScalarBuffer};
    use arrow_schema::{DataType, Fields, Schema as ArrowSchema};
    use lance_arrow::ARROW_EXT_NAME_KEY;
    use lance_arrow::json::{ARROW_JSON_EXT_NAME, is_arrow_json_field};
    use lance_core::{ROW_ADDR_FIELD, ROW_ID_FIELD};
    use lance_file::version::LanceFileVersion;
    use pretty_assertions::assert_eq;
    use rstest::rstest;
    use std::collections::HashMap;

    use crate::dataset::{WriteParams, scanner::test_dataset::TestVectorDataset};

    use super::*;

    // Used to validate that futures returned are Send.
    fn require_send<T: Send>(t: T) -> T {
        t
    }

    fn test_batch(i_range: Range<i32>) -> RecordBatch {
        let schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("i", DataType::Int32, false),
            ArrowField::new("s", DataType::Utf8, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from_iter_values(i_range.clone())),
                Arc::new(StringArray::from_iter_values(
                    i_range.map(|i| format!("str-{}", i)),
                )),
            ],
        )
        .unwrap()
    }

    fn nested_arrow_json_batch() -> RecordBatch {
        let uri_field = Arc::new(ArrowField::new("uri", DataType::Utf8, false));
        let mut metadata = HashMap::new();
        metadata.insert(
            ARROW_EXT_NAME_KEY.to_string(),
            ARROW_JSON_EXT_NAME.to_string(),
        );
        let extra_field =
            Arc::new(ArrowField::new("extra", DataType::Utf8, true).with_metadata(metadata));
        let item_fields = Fields::from(vec![uri_field, extra_field]);
        let values = StructArray::new(
            item_fields.clone(),
            vec![
                Arc::new(StringArray::from(vec![Some("a.jpg"), Some("b.jpg")])) as ArrayRef,
                Arc::new(StringArray::from(vec![
                    Some(r#"{"codec":"h264"}"#),
                    None::<&str>,
                ])) as ArrayRef,
            ],
            None,
        );
        let item = Arc::new(ArrowField::new("item", DataType::Struct(item_fields), true));
        let media = ListArray::new(
            item,
            OffsetBuffer::new(ScalarBuffer::from(vec![0, 1, 2])),
            Arc::new(values),
            None,
        );
        let schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
            "media",
            media.data_type().clone(),
            true,
        )]));

        RecordBatch::try_new(schema, vec![Arc::new(media) as ArrayRef]).unwrap()
    }

    fn assert_nested_arrow_json_schema(batch: &RecordBatch) {
        let schema = batch.schema();
        let DataType::List(item) = schema.field(0).data_type() else {
            panic!("expected list field");
        };
        let DataType::Struct(fields) = item.data_type() else {
            panic!("expected struct item");
        };
        assert!(is_arrow_json_field(&fields[1]));
    }

    fn assert_first_nested_json_value(batch: &RecordBatch) {
        let media: &ListArray = batch.column(0).as_list();
        let values = media.values().as_struct();
        let uri = values
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let extra = values
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        assert_eq!(uri.value(0), "a.jpg");
        assert!(extra.value(0).contains("h264"));
    }

    #[rstest]
    #[tokio::test]
    async fn test_take(
        #[values(LanceFileVersion::Legacy, LanceFileVersion::Stable)]
        data_storage_version: LanceFileVersion,
        #[values(false, true)] enable_stable_row_ids: bool,
    ) {
        let data = test_batch(0..400);
        let write_params = WriteParams {
            max_rows_per_file: 40,
            max_rows_per_group: 10,
            data_storage_version: Some(data_storage_version),
            enable_stable_row_ids,
            ..Default::default()
        };
        let batches = RecordBatchIterator::new([Ok(data.clone())], data.schema());
        let dataset = Dataset::write(batches, "memory://", Some(write_params))
            .await
            .unwrap();

        assert_eq!(dataset.count_rows(None).await.unwrap(), 400);
        let projection = Schema::try_from(data.schema().as_ref()).unwrap();
        let values = dataset
            .take(
                &[
                    200, // 200
                    199, // 199
                    39,  // 39
                    40,  // 40
                    199, // 40
                    40,  // 40
                    125, // 125
                ],
                projection,
            )
            .await
            .unwrap();
        assert_eq!(
            RecordBatch::try_new(
                data.schema(),
                vec![
                    Arc::new(Int32Array::from_iter_values([
                        200, 199, 39, 40, 199, 40, 125
                    ])),
                    Arc::new(StringArray::from_iter_values(
                        [200, 199, 39, 40, 199, 40, 125]
                            .iter()
                            .map(|v| format!("str-{v}"))
                    )),
                ],
            )
            .unwrap(),
            values
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_take_nested_arrow_json_returns_logical_schema(
        #[values(LanceFileVersion::V2_1, LanceFileVersion::V2_2, LanceFileVersion::V2_3)]
        data_storage_version: LanceFileVersion,
    ) {
        let data = nested_arrow_json_batch();
        let write_params = WriteParams {
            data_storage_version: Some(data_storage_version),
            enable_stable_row_ids: false,
            ..Default::default()
        };
        let batches = RecordBatchIterator::new([Ok(data.clone())], data.schema());
        let dataset = Dataset::write(batches, "memory://", Some(write_params))
            .await
            .unwrap();
        let projection = Schema::try_from(data.schema().as_ref()).unwrap();

        let values = dataset.take(&[0], projection.clone()).await.unwrap();
        assert_nested_arrow_json_schema(&values);
        assert_first_nested_json_value(&values);

        let empty = dataset.take(&[], projection.clone()).await.unwrap();
        assert_eq!(empty.num_rows(), 0);
        assert_nested_arrow_json_schema(&empty);

        let values = dataset.take_rows(&[0], projection.clone()).await.unwrap();
        assert_nested_arrow_json_schema(&values);
        assert_first_nested_json_value(&values);

        let empty = dataset.take_rows(&[], projection).await.unwrap();
        assert_eq!(empty.num_rows(), 0);
        assert_nested_arrow_json_schema(&empty);
    }

    #[tokio::test]
    async fn test_take_with_deletion() {
        let data = test_batch(0..120);
        let write_params = WriteParams {
            max_rows_per_file: 40,
            max_rows_per_group: 10,
            ..Default::default()
        };
        let batches = RecordBatchIterator::new([Ok(data.clone())], data.schema());
        let mut dataset = Dataset::write(batches, "memory://", Some(write_params))
            .await
            .unwrap();

        dataset.delete("i in (40, 77, 78, 79)").await.unwrap();

        let projection = Schema::try_from(data.schema().as_ref()).unwrap();
        let values = dataset
            .take(
                &[
                    0,   // 0
                    39,  // 39
                    40,  // 41
                    75,  // 76
                    76,  // 80
                    77,  // 81
                    115, // 119
                ],
                projection,
            )
            .await
            .unwrap();

        assert_eq!(
            RecordBatch::try_new(
                data.schema(),
                vec![
                    Arc::new(Int32Array::from_iter_values([0, 39, 41, 76, 80, 81, 119])),
                    Arc::new(StringArray::from_iter_values(
                        [0, 39, 41, 76, 80, 81, 119]
                            .iter()
                            .map(|v| format!("str-{v}"))
                    )),
                ],
            )
            .unwrap(),
            values
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_take_with_projection(
        #[values(LanceFileVersion::Legacy, LanceFileVersion::Stable)]
        data_storage_version: LanceFileVersion,
        #[values(false, true)] enable_stable_row_ids: bool,
    ) {
        let data = test_batch(0..400);
        let write_params = WriteParams {
            data_storage_version: Some(data_storage_version),
            enable_stable_row_ids,
            ..Default::default()
        };
        let batches = RecordBatchIterator::new([Ok(data.clone())], data.schema());
        let dataset = Dataset::write(batches, "memory://", Some(write_params))
            .await
            .unwrap();

        assert_eq!(dataset.count_rows(None).await.unwrap(), 400);
        let projection = ProjectionRequest::from_sql(vec![("foo", "i"), ("bar", "i*2")]);
        let values = dataset
            .take(&[10, 50, 100], projection.clone())
            .await
            .unwrap();

        let expected_schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("foo", DataType::Int32, false),
            ArrowField::new("bar", DataType::Int32, false),
        ]));
        assert_eq!(
            RecordBatch::try_new(
                expected_schema,
                vec![
                    Arc::new(Int32Array::from_iter_values([10, 50, 100])),
                    Arc::new(Int32Array::from_iter_values([20, 100, 200])),
                ],
            )
            .unwrap(),
            values
        );

        let values2 = dataset.take_rows(&[10, 50, 100], projection).await.unwrap();
        assert_eq!(values, values2);
    }

    #[tokio::test]
    async fn test_reject_legacy_blob_schema_on_v2_2() {
        let mut metadata = HashMap::new();
        metadata.insert(lance_arrow::BLOB_META_KEY.to_string(), "true".to_string());

        let schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("blob", DataType::LargeBinary, true).with_metadata(metadata),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(LargeBinaryArray::from(vec![Some(
                b"hello".as_slice(),
            )]))],
        )
        .unwrap();

        let write_params = WriteParams {
            data_storage_version: Some(LanceFileVersion::V2_2),
            ..Default::default()
        };
        let batches = RecordBatchIterator::new([Ok(batch)], schema);
        let err = Dataset::write(batches, "memory://", Some(write_params))
            .await
            .unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("Legacy blob columns"));
        assert!(msg.contains("lance.blob.v2"));
    }

    #[tokio::test]
    async fn test_take_blob_v2_from_blob_v2_struct_on_v2_2() {
        let schema = Arc::new(ArrowSchema::new(vec![crate::blob::blob_field(
            "blob", true,
        )]));
        let mut builder = crate::blob::BlobArrayBuilder::new(1);
        builder.push_bytes(b"hello").unwrap();
        let array = builder.finish().unwrap();

        let batch = RecordBatch::try_new(schema.clone(), vec![array]).unwrap();
        let write_params = WriteParams {
            data_storage_version: Some(LanceFileVersion::V2_2),
            ..Default::default()
        };
        let batches = RecordBatchIterator::new([Ok(batch)], schema);
        let dataset = crate::dataset::write::InsertBuilder::new("memory://")
            .with_params(&write_params)
            .execute_stream(batches)
            .await
            .unwrap();

        let proj = ProjectionRequest::from_columns(["blob"], dataset.schema());
        let values = dataset.take(&[0u64], proj).await.unwrap();

        let struct_arr = values.column(0).as_struct();
        assert_eq!(struct_arr.fields().len(), 5);
        assert_eq!(struct_arr.fields()[0].name(), "kind");
        assert_eq!(struct_arr.fields()[1].name(), "position");
        assert_eq!(struct_arr.fields()[2].name(), "size");
        assert_eq!(struct_arr.fields()[3].name(), "blob_id");
        assert_eq!(struct_arr.fields()[4].name(), "blob_uri");
    }

    #[tokio::test]
    async fn test_projection_plan_accepts_unloaded_legacy_blob_schema() {
        let mut metadata = HashMap::new();
        metadata.insert(lance_arrow::BLOB_META_KEY.to_string(), "true".to_string());
        let schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("blob", DataType::LargeBinary, true).with_metadata(metadata),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(LargeBinaryArray::from(vec![Some(
                b"hello".as_slice(),
            )]))],
        )
        .unwrap();
        let write_params = WriteParams {
            data_storage_version: Some(LanceFileVersion::Legacy),
            ..Default::default()
        };
        let batches = RecordBatchIterator::new([Ok(batch)], schema);
        let dataset = Dataset::write(batches, "memory://", Some(write_params))
            .await
            .unwrap();

        let mut projection = dataset.schema().project(&["blob"]).unwrap();
        projection.fields[0].unloaded_mut();

        let projection = ProjectionRequest::from_schema(projection)
            .into_projection_plan(Arc::new(dataset))
            .unwrap();

        let output_schema = projection.output_schema().unwrap();
        let blob_field = output_schema.field_with_name("blob").unwrap();
        let DataType::Struct(fields) = blob_field.data_type() else {
            panic!("expected blob output schema to be a struct, got {blob_field:?}");
        };
        assert_eq!(fields.len(), 2);
        assert_eq!(fields[0].name(), "position");
        assert_eq!(fields[1].name(), "size");
    }

    #[rstest]
    #[tokio::test]
    async fn test_take_rowid_rowaddr_with_projection_enable_stable_row_ids_projection_from_sql(
        #[values(LanceFileVersion::Legacy, LanceFileVersion::Stable)]
        data_storage_version: LanceFileVersion,
    ) {
        let data = test_batch(0..400);
        let write_params = WriteParams {
            data_storage_version: Some(data_storage_version),
            enable_stable_row_ids: true,
            max_rows_per_file: 50,
            ..Default::default()
        };
        let batches = RecordBatchIterator::new([Ok(data.clone())], data.schema());
        let dataset = Dataset::write(batches, "memory://", Some(write_params))
            .await
            .unwrap();

        assert_eq!(dataset.count_rows(None).await.unwrap(), 400);
        let projection = ProjectionRequest::from_sql(vec![
            ("foo", "i"),
            ("bar", "i*2"),
            ("_rowid", "_rowid"),
            ("_rowaddr", "_rowaddr"),
        ]);
        let values = dataset
            .take(&[10, 50, 100], projection.clone())
            .await
            .unwrap();
        let expected_schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("foo", DataType::Int32, false),
            ArrowField::new("bar", DataType::Int32, false),
            ROW_ID_FIELD.clone(),
            ROW_ADDR_FIELD.clone(),
        ]));
        assert_eq!(
            RecordBatch::try_new(
                expected_schema,
                vec![
                    Arc::new(Int32Array::from_iter_values([10, 50, 100])),
                    Arc::new(Int32Array::from_iter_values([20, 100, 200])),
                    Arc::new(UInt64Array::from_iter_values([10, 50, 100])),
                    Arc::new(UInt64Array::from_iter_values([10, 4294967296, 8589934592])),
                ],
            )
            .unwrap(),
            values
        );

        let values2 = dataset.take_rows(&[10, 50, 100], projection).await.unwrap();
        assert_eq!(values, values2);
    }

    #[rstest]
    #[tokio::test]
    async fn test_take_rowid_rowaddr_with_projection_enable_stable_row_ids_projection_from_columns(
        #[values(LanceFileVersion::Legacy, LanceFileVersion::Stable)]
        data_storage_version: LanceFileVersion,
    ) {
        let data = test_batch(0..400);
        let write_params = WriteParams {
            data_storage_version: Some(data_storage_version),
            enable_stable_row_ids: true,
            max_rows_per_file: 50,
            ..Default::default()
        };
        let batches = RecordBatchIterator::new([Ok(data.clone())], data.schema());
        let dataset = Dataset::write(batches, "memory://", Some(write_params))
            .await
            .unwrap();

        assert_eq!(dataset.count_rows(None).await.unwrap(), 400);
        let projection =
            ProjectionRequest::from_columns(["_rowid", "_rowaddr", "i"], dataset.schema());

        let values = dataset
            .take(&[10, 50, 100], projection.clone())
            .await
            .unwrap();
        let expected_schema = Arc::new(ArrowSchema::new(vec![
            ROW_ID_FIELD.clone(),
            ROW_ADDR_FIELD.clone(),
            ArrowField::new("i", DataType::Int32, false),
        ]));
        assert_eq!(
            RecordBatch::try_new(
                expected_schema.clone(),
                vec![
                    Arc::new(UInt64Array::from_iter_values([10, 50, 100])),
                    Arc::new(UInt64Array::from_iter_values([10, 4294967296, 8589934592])),
                    Arc::new(Int32Array::from_iter_values([10, 50, 100])),
                ],
            )
            .unwrap(),
            values
        );

        let values2 = dataset
            .take_rows(&[10, 50, 100], projection.clone())
            .await
            .unwrap();
        assert_eq!(values, values2);

        let values3 = dataset
            .take(&[50, 100, 10], projection.clone())
            .await
            .unwrap();
        assert_eq!(
            RecordBatch::try_new(
                expected_schema,
                vec![
                    Arc::new(UInt64Array::from_iter_values([50, 100, 10])),
                    Arc::new(UInt64Array::from_iter_values([4294967296, 8589934592, 10])),
                    Arc::new(Int32Array::from_iter_values([50, 100, 10])),
                ],
            )
            .unwrap(),
            values3
        );
        let values4 = dataset.take_rows(&[50, 100, 10], projection).await.unwrap();
        assert_eq!(values3, values4);
    }

    #[rstest]
    #[tokio::test]
    async fn test_take_rowid_rowaddr_with_projection_disable_stable_row_ids_projection_from_sql(
        #[values(LanceFileVersion::Legacy, LanceFileVersion::Stable)]
        data_storage_version: LanceFileVersion,
    ) {
        let data = test_batch(0..400);
        let write_params = WriteParams {
            data_storage_version: Some(data_storage_version),
            enable_stable_row_ids: false,
            max_rows_per_file: 50,
            ..Default::default()
        };
        let batches = RecordBatchIterator::new([Ok(data.clone())], data.schema());
        let dataset = Dataset::write(batches, "memory://", Some(write_params))
            .await
            .unwrap();

        assert_eq!(dataset.count_rows(None).await.unwrap(), 400);
        let projection = ProjectionRequest::from_sql(vec![
            ("foo", "i"),
            ("bar", "i*2"),
            ("_rowid", "_rowid"),
            ("_rowaddr", "_rowaddr"),
        ]);
        let values = dataset
            .take(&[10, 50, 100], projection.clone())
            .await
            .unwrap();
        let expected_schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("foo", DataType::Int32, false),
            ArrowField::new("bar", DataType::Int32, false),
            ROW_ID_FIELD.clone(),
            ROW_ADDR_FIELD.clone(),
        ]));
        assert_eq!(
            RecordBatch::try_new(
                expected_schema,
                vec![
                    Arc::new(Int32Array::from_iter_values([10, 50, 100])),
                    Arc::new(Int32Array::from_iter_values([20, 100, 200])),
                    Arc::new(UInt64Array::from_iter_values([10, 4294967296, 8589934592])),
                    Arc::new(UInt64Array::from_iter_values([10, 4294967296, 8589934592])),
                ],
            )
            .unwrap(),
            values
        );

        let values2 = dataset
            .take_rows(&[10, 4294967296, 8589934592], projection)
            .await
            .unwrap();
        assert_eq!(values, values2);
    }

    #[rstest]
    #[tokio::test]
    async fn test_take_rowid_rowaddr_with_projection_disable_stable_row_ids_projection_from_columns(
        #[values(LanceFileVersion::Legacy, LanceFileVersion::Stable)]
        data_storage_version: LanceFileVersion,
    ) {
        let data = test_batch(0..400);
        let write_params = WriteParams {
            data_storage_version: Some(data_storage_version),
            enable_stable_row_ids: false,
            max_rows_per_file: 50,
            ..Default::default()
        };
        let batches = RecordBatchIterator::new([Ok(data.clone())], data.schema());
        let dataset = Dataset::write(batches, "memory://", Some(write_params))
            .await
            .unwrap();

        assert_eq!(dataset.count_rows(None).await.unwrap(), 400);

        let projection =
            ProjectionRequest::from_columns(["_rowid", "_rowaddr", "i"], dataset.schema());
        let values = dataset
            .take(&[10, 50, 100], projection.clone())
            .await
            .unwrap();

        let expected_schema = Arc::new(ArrowSchema::new(vec![
            ROW_ID_FIELD.clone(),
            ROW_ADDR_FIELD.clone(),
            ArrowField::new("i", DataType::Int32, false),
        ]));

        assert_eq!(
            RecordBatch::try_new(
                expected_schema.clone(),
                vec![
                    Arc::new(UInt64Array::from_iter_values([10, 4294967296, 8589934592])),
                    Arc::new(UInt64Array::from_iter_values([10, 4294967296, 8589934592])),
                    Arc::new(Int32Array::from_iter_values([10, 50, 100])),
                ],
            )
            .unwrap(),
            values
        );
        let values2 = dataset
            .take_rows(&[10, 4294967296, 8589934592], projection.clone())
            .await
            .unwrap();
        assert_eq!(values, values2);

        let values3 = dataset
            .take(&[50, 100, 10], projection.clone())
            .await
            .unwrap();
        assert_eq!(
            RecordBatch::try_new(
                expected_schema,
                vec![
                    Arc::new(UInt64Array::from_iter_values([4294967296, 8589934592, 10])),
                    Arc::new(UInt64Array::from_iter_values([4294967296, 8589934592, 10])),
                    Arc::new(Int32Array::from_iter_values([50, 100, 10])),
                ],
            )
            .unwrap(),
            values3
        );
        let values4 = dataset
            .take_rows(&[4294967296, 8589934592, 10], projection)
            .await
            .unwrap();
        assert_eq!(values3, values4);
    }

    #[rstest]
    #[tokio::test]
    async fn test_take_rows_out_of_bound(
        #[values(LanceFileVersion::Legacy, LanceFileVersion::Stable)]
        data_storage_version: LanceFileVersion,
    ) {
        // a dataset with 1 fragment and 400 rows
        let test_ds = TestVectorDataset::new(data_storage_version, false)
            .await
            .unwrap();
        let ds = test_ds.dataset;

        // take the last row of first fragment
        // this triggers the contiguous branch
        let indices = &[(1 << 32) - 1];
        let fut = require_send(ds.take_rows(indices, ds.schema().clone()));
        let err = fut.await.unwrap_err();
        assert!(
            err.to_string().contains("Invalid read params"),
            "{}",
            err.to_string()
        );

        // this triggers the sorted branch, but not contiguous
        let indices = &[(1 << 32) - 3, (1 << 32) - 1];
        let err = ds
            .take_rows(indices, ds.schema().clone())
            .await
            .unwrap_err();
        assert!(
            err.to_string()
                .contains("Invalid read params Indices(4294967293,4294967295)"),
            "{}",
            err.to_string()
        );

        // this triggers the catch all branch
        let indices = &[(1 << 32) - 1, (1 << 32) - 3];
        let err = ds
            .take_rows(indices, ds.schema().clone())
            .await
            .unwrap_err();
        assert!(
            err.to_string()
                .contains("Invalid read params Indices(4294967293,4294967295)"),
            "{}",
            err.to_string()
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_take_rows(
        #[values(LanceFileVersion::Legacy, LanceFileVersion::Stable)]
        data_storage_version: LanceFileVersion,
    ) {
        let data = test_batch(0..400);
        let write_params = WriteParams {
            max_rows_per_file: 40,
            max_rows_per_group: 10,
            data_storage_version: Some(data_storage_version),
            ..Default::default()
        };
        let batches = RecordBatchIterator::new([Ok(data.clone())], data.schema());
        let mut dataset = Dataset::write(batches, "memory://", Some(write_params))
            .await
            .unwrap();

        assert_eq!(dataset.count_rows(None).await.unwrap(), 400);
        let projection = Schema::try_from(data.schema().as_ref()).unwrap();
        let indices = &[
            5_u64 << 32,        // 200
            (4_u64 << 32) + 39, // 199
            39,                 // 39
            1_u64 << 32,        // 40
            (2_u64 << 32) + 20, // 100
        ];
        let values = dataset
            .take_rows(indices, projection.clone())
            .await
            .unwrap();
        assert_eq!(
            RecordBatch::try_new(
                data.schema(),
                vec![
                    Arc::new(Int32Array::from_iter_values([200, 199, 39, 40, 100])),
                    Arc::new(StringArray::from_iter_values(
                        [200, 199, 39, 40, 100].iter().map(|v| format!("str-{v}"))
                    )),
                ],
            )
            .unwrap(),
            values
        );

        // Delete some rows from a fragment
        dataset.delete("i in (199, 100)").await.unwrap();
        dataset.validate().await.unwrap();
        let values = dataset
            .take_rows(indices, projection.clone())
            .await
            .unwrap();
        assert_eq!(
            RecordBatch::try_new(
                data.schema(),
                vec![
                    Arc::new(Int32Array::from_iter_values([200, 39, 40])),
                    Arc::new(StringArray::from_iter_values(
                        [200, 39, 40].iter().map(|v| format!("str-{v}"))
                    )),
                ],
            )
            .unwrap(),
            values
        );

        // Take an empty selection.
        let values = dataset.take_rows(&[], projection).await.unwrap();
        assert_eq!(RecordBatch::new_empty(data.schema()), values);
    }

    #[rstest]
    #[tokio::test]
    async fn take_scan_dataset(
        #[values(LanceFileVersion::Legacy, LanceFileVersion::Stable)]
        data_storage_version: LanceFileVersion,
    ) {
        use arrow::datatypes::Int32Type;

        let data = test_batch(1..5);
        let write_params = WriteParams {
            max_rows_per_group: 2,
            data_storage_version: Some(data_storage_version),
            ..Default::default()
        };
        let batches = RecordBatchIterator::new([Ok(data.clone())], data.schema());
        let dataset = Dataset::write(batches, "memory://", Some(write_params))
            .await
            .unwrap();

        let projection = Arc::new(dataset.schema().project(&["i"]).unwrap());
        let ranges = [0_u64..3, 1..4, 0..1];
        let range_stream = futures::stream::iter(ranges).map(Ok).boxed();
        let results = dataset
            .take_scan(range_stream, projection.clone(), 10)
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        let expected_schema = projection.as_ref().into();
        for batch in &results {
            assert_eq!(batch.schema().as_ref(), &expected_schema);
        }
        assert_eq!(results.len(), 3);
        assert_eq!(
            results[0].column(0).as_primitive::<Int32Type>().values(),
            &[1, 2, 3],
        );
        assert_eq!(
            results[1].column(0).as_primitive::<Int32Type>().values(),
            &[2, 3, 4],
        );
        assert_eq!(
            results[2].column(0).as_primitive::<Int32Type>().values(),
            &[1],
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_take_rows_with_row_ids(
        #[values(LanceFileVersion::Legacy, LanceFileVersion::Stable)]
        data_storage_version: LanceFileVersion,
    ) {
        let data = test_batch(0..8);
        let write_params = WriteParams {
            max_rows_per_group: 2,
            data_storage_version: Some(data_storage_version),
            enable_stable_row_ids: true,
            ..Default::default()
        };
        let batches = RecordBatchIterator::new([Ok(data.clone())], data.schema());
        let mut dataset = Dataset::write(batches, "memory://", Some(write_params))
            .await
            .unwrap();

        dataset.delete("i in (1, 2, 3, 7)").await.unwrap();

        let indices = &[0, 4, 6, 5];
        let result = dataset
            .take_rows(indices, dataset.schema().clone())
            .await
            .unwrap();
        assert_eq!(
            RecordBatch::try_new(
                data.schema(),
                vec![
                    Arc::new(Int32Array::from_iter_values(
                        indices.iter().map(|x| *x as i32)
                    )),
                    Arc::new(StringArray::from_iter_values(
                        indices.iter().map(|v| format!("str-{v}"))
                    )),
                ],
            )
            .unwrap(),
            result
        );
    }
}
