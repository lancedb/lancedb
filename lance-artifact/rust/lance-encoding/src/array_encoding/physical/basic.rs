// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::sync::Arc;

use arrow_schema::DataType;
use futures::{FutureExt, future::BoxFuture};
use log::trace;

use crate::{
    EncodingsIo,
    data::{AllNullDataBlock, BlockInfo, DataBlock, NullableDataBlock},
    decoder::{PageScheduler, PrimitivePageDecoder},
    encoder::{ArrayEncoder, EncodedArray},
    format::ProtobufUtils,
};

use lance_core::Result;

struct DataDecoders {
    validity: Box<dyn PrimitivePageDecoder>,
    values: Box<dyn PrimitivePageDecoder>,
}

enum DataNullStatus {
    // Neither validity nor values
    All,
    // Values only
    None(Box<dyn PrimitivePageDecoder>),
    // Validity and values
    Some(DataDecoders),
}

#[derive(Debug)]
struct DataSchedulers {
    validity: Box<dyn PageScheduler>,
    values: Box<dyn PageScheduler>,
}

#[derive(Debug)]
enum SchedulerNullStatus {
    // Values only
    None(Box<dyn PageScheduler>),
    // Validity and values
    Some(DataSchedulers),
    // Neither validity nor values
    All,
}

impl SchedulerNullStatus {
    fn values_scheduler(&self) -> Option<&dyn PageScheduler> {
        match self {
            Self::All => None,
            Self::None(values) => Some(values.as_ref()),
            Self::Some(schedulers) => Some(schedulers.values.as_ref()),
        }
    }
}

/// A physical scheduler for "basic" fields.  These are fields that have an optional
/// validity bitmap and some kind of values buffer.
///
/// No actual decoding happens here, we are simply aggregating the two buffers.
///
/// If everything is null then there are no data buffers at all.
// TODO: Add support/tests for primitive nulls
// TODO: Add tests for the all-null case
//
// Right now this is always present on primitive fields.  In the future we may use a
// sentinel encoding instead.
#[derive(Debug)]
pub struct BasicPageScheduler {
    mode: SchedulerNullStatus,
}

impl BasicPageScheduler {
    /// Creates a new instance that expects a validity bitmap
    pub fn new_nullable(
        validity_decoder: Box<dyn PageScheduler>,
        values_decoder: Box<dyn PageScheduler>,
    ) -> Self {
        Self {
            mode: SchedulerNullStatus::Some(DataSchedulers {
                validity: validity_decoder,
                values: values_decoder,
            }),
        }
    }

    /// Create a new instance that does not need a validity bitmap because no item is null
    pub fn new_non_nullable(values_decoder: Box<dyn PageScheduler>) -> Self {
        Self {
            mode: SchedulerNullStatus::None(values_decoder),
        }
    }

    /// Create a new instance where all values are null
    ///
    /// It may seem strange we need `values_decoder` here but Arrow requires that value
    /// buffers still be allocated / sized even if everything is null.  So we need the value
    /// decoder to calculate the capacity of the garbage buffer.
    pub fn new_all_null() -> Self {
        Self {
            mode: SchedulerNullStatus::All,
        }
    }
}

impl PageScheduler for BasicPageScheduler {
    fn schedule_ranges(
        &self,
        ranges: &[std::ops::Range<u64>],
        scheduler: &Arc<dyn EncodingsIo>,
        top_level_row: u64,
    ) -> BoxFuture<'static, Result<Box<dyn PrimitivePageDecoder>>> {
        let validity_future = match &self.mode {
            SchedulerNullStatus::None(_) | SchedulerNullStatus::All => None,
            SchedulerNullStatus::Some(schedulers) => Some(schedulers.validity.schedule_ranges(
                ranges,
                scheduler,
                top_level_row,
            )),
        };

        let values_future = if let Some(values_scheduler) = self.mode.values_scheduler() {
            Some(
                values_scheduler
                    .schedule_ranges(ranges, scheduler, top_level_row)
                    .boxed(),
            )
        } else {
            trace!("No values fetch needed since values all null");
            None
        };

        async move {
            let mode = match (values_future, validity_future) {
                (None, None) => DataNullStatus::All,
                (Some(values_future), None) => DataNullStatus::None(values_future.await?),
                (Some(values_future), Some(validity_future)) => {
                    DataNullStatus::Some(DataDecoders {
                        values: values_future.await?,
                        validity: validity_future.await?,
                    })
                }
                _ => unreachable!(),
            };
            Ok(Box::new(BasicPageDecoder { mode }) as Box<dyn PrimitivePageDecoder>)
        }
        .boxed()
    }
}

struct BasicPageDecoder {
    mode: DataNullStatus,
}

impl PrimitivePageDecoder for BasicPageDecoder {
    fn decode(&self, rows_to_skip: u64, num_rows: u64) -> Result<DataBlock> {
        match &self.mode {
            DataNullStatus::Some(decoders) => {
                let validity = decoders.validity.decode(rows_to_skip, num_rows)?;
                let validity = validity.as_fixed_width().unwrap();
                let values = decoders.values.decode(rows_to_skip, num_rows)?;
                Ok(DataBlock::Nullable(NullableDataBlock {
                    data: Box::new(values),
                    nulls: validity.data,
                    block_info: BlockInfo::new(),
                }))
            }
            DataNullStatus::All => Ok(DataBlock::AllNull(AllNullDataBlock {
                num_values: num_rows,
            })),
            DataNullStatus::None(values) => values.decode(rows_to_skip, num_rows),
        }
    }
}

#[derive(Debug)]
pub struct BasicEncoder {
    values_encoder: Box<dyn ArrayEncoder>,
}

impl BasicEncoder {
    pub fn new(values_encoder: Box<dyn ArrayEncoder>) -> Self {
        Self { values_encoder }
    }
}

impl ArrayEncoder for BasicEncoder {
    fn encode(
        &self,
        data: DataBlock,
        data_type: &DataType,
        buffer_index: &mut u32,
    ) -> Result<EncodedArray> {
        match data {
            DataBlock::AllNull(_) => {
                let encoding = ProtobufUtils::basic_all_null_encoding();
                Ok(EncodedArray { data, encoding })
            }
            DataBlock::Nullable(nullable) => {
                let validity_buffer_index = *buffer_index;
                *buffer_index += 1;

                let validity_desc = ProtobufUtils::flat_encoding(
                    1,
                    validity_buffer_index,
                    /*compression=*/ None,
                );
                let encoded_values =
                    self.values_encoder
                        .encode(*nullable.data, data_type, buffer_index)?;
                let encoding =
                    ProtobufUtils::basic_some_null_encoding(validity_desc, encoded_values.encoding);
                let encoded = DataBlock::Nullable(NullableDataBlock {
                    data: Box::new(encoded_values.data),
                    nulls: nullable.nulls,
                    block_info: BlockInfo::new(),
                });
                Ok(EncodedArray {
                    data: encoded,
                    encoding,
                })
            }
            _ => {
                let encoded_values = self.values_encoder.encode(data, data_type, buffer_index)?;
                let encoding = ProtobufUtils::basic_no_null_encoding(encoded_values.encoding);
                Ok(EncodedArray {
                    data: encoded_values.data,
                    encoding,
                })
            }
        }
    }
}
