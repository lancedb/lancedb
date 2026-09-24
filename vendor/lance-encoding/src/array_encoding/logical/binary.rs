// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::sync::Arc;

use arrow_array::{
    Array, ArrayRef, GenericByteArray, GenericListArray,
    cast::AsArray,
    types::{BinaryType, ByteArrayType, LargeBinaryType, LargeUtf8Type, UInt8Type, Utf8Type},
};

use arrow_schema::DataType;
use futures::{FutureExt, future::BoxFuture};
use lance_core::{Error, Result};
use log::trace;

use crate::{
    decoder::{
        DecodeArrayTask, FilterExpression, MessageType, NextDecodeTask, PriorityRange,
        ScheduledScanLine, SchedulerContext,
    },
    decoder::{DecoderReady, DrainLimit, FieldScheduler, LogicalPageDecoder, SchedulingJob},
};

/// Wraps a varbin scheduler and uses a BinaryPageDecoder to cast
/// the result to the appropriate type
#[derive(Debug)]
pub struct BinarySchedulingJob<'a> {
    scheduler: &'a BinaryFieldScheduler,
    inner: Box<dyn SchedulingJob + 'a>,
}

impl SchedulingJob for BinarySchedulingJob<'_> {
    fn schedule_next(
        &mut self,
        context: &mut SchedulerContext,
        priority: &dyn PriorityRange,
    ) -> Result<ScheduledScanLine> {
        let inner_scan = self.inner.schedule_next(context, priority)?;
        let wrapped_decoders = inner_scan
            .decoders
            .into_iter()
            .map(|message| {
                let decoder = message.into_array();
                MessageType::DecoderReady(DecoderReady {
                    path: decoder.path,
                    decoder: Box::new(BinaryPageDecoder {
                        inner: decoder.decoder,
                        data_type: self.scheduler.data_type.clone(),
                    }),
                })
            })
            .collect::<Vec<_>>();
        Ok(ScheduledScanLine {
            decoders: wrapped_decoders,
            rows_scheduled: inner_scan.rows_scheduled,
        })
    }

    fn num_rows(&self) -> u64 {
        self.inner.num_rows()
    }
}

/// A logical scheduler for utf8/binary pages which assumes the data are encoded as `List<u8>`
#[derive(Debug)]
pub struct BinaryFieldScheduler {
    varbin_scheduler: Arc<dyn FieldScheduler>,
    data_type: DataType,
}

impl BinaryFieldScheduler {
    // Create a new ListPageScheduler
    pub fn new(varbin_scheduler: Arc<dyn FieldScheduler>, data_type: DataType) -> Self {
        Self {
            varbin_scheduler,
            data_type,
        }
    }
}

impl FieldScheduler for BinaryFieldScheduler {
    fn schedule_ranges<'a>(
        &'a self,
        ranges: &[std::ops::Range<u64>],
        filter: &FilterExpression,
    ) -> Result<Box<dyn SchedulingJob + 'a>> {
        trace!("Scheduling binary for {} ranges", ranges.len());
        let varbin_job = self.varbin_scheduler.schedule_ranges(ranges, filter)?;
        Ok(Box::new(BinarySchedulingJob {
            scheduler: self,
            inner: varbin_job,
        }))
    }

    fn num_rows(&self) -> u64 {
        self.varbin_scheduler.num_rows()
    }

    fn initialize<'a>(
        &'a self,
        _filter: &'a FilterExpression,
        _context: &'a SchedulerContext,
    ) -> BoxFuture<'a, Result<()>> {
        // 2.0 schedulers do not need to initialize
        std::future::ready(Ok(())).boxed()
    }
}

#[derive(Debug)]
pub struct BinaryPageDecoder {
    inner: Box<dyn LogicalPageDecoder>,
    data_type: DataType,
}

impl LogicalPageDecoder for BinaryPageDecoder {
    fn wait_for_loaded(&mut self, num_rows: u64) -> BoxFuture<'_, Result<()>> {
        self.inner.wait_for_loaded(num_rows)
    }

    fn drain(&mut self, num_rows: u64) -> Result<NextDecodeTask> {
        let inner_task = self.inner.drain(num_rows)?;
        Ok(NextDecodeTask {
            num_rows: inner_task.num_rows,
            task: Box::new(BinaryArrayDecoder {
                inner: inner_task.task,
                data_type: self.data_type.clone(),
            }),
        })
    }

    fn max_rows_to_drain(&self, num_rows: u64, byte_budget: u64) -> Result<DrainLimit> {
        self.inner.max_rows_to_drain(num_rows, byte_budget)
    }

    fn data_type(&self) -> &DataType {
        &self.data_type
    }

    fn rows_loaded(&self) -> u64 {
        self.inner.rows_loaded()
    }

    fn num_rows(&self) -> u64 {
        self.inner.num_rows()
    }

    fn rows_drained(&self) -> u64 {
        self.inner.rows_drained()
    }
}

pub struct BinaryArrayDecoder {
    inner: Box<dyn DecodeArrayTask>,
    data_type: DataType,
}

impl BinaryArrayDecoder {
    fn from_list_array<T: ByteArrayType>(array: &GenericListArray<T::Offset>) -> Result<ArrayRef> {
        let values = array
            .values()
            .as_primitive::<UInt8Type>()
            .values()
            .inner()
            .clone();
        let offsets = array.offsets().clone();
        let array = GenericByteArray::<T>::try_new(offsets, values, array.nulls().cloned())?;
        Ok(Arc::new(array))
    }
}

impl DecodeArrayTask for BinaryArrayDecoder {
    fn decode(self: Box<Self>) -> Result<(ArrayRef, u64)> {
        let data_type = self.data_type;
        let (arr, _) = self.inner.decode()?;
        let result = match data_type {
            DataType::Binary => Self::from_list_array::<BinaryType>(arr.as_list::<i32>())?,
            DataType::LargeBinary => {
                Self::from_list_array::<LargeBinaryType>(arr.as_list::<i64>())?
            }
            DataType::Utf8 => Self::from_list_array::<Utf8Type>(arr.as_list::<i32>())?,
            DataType::LargeUtf8 => Self::from_list_array::<LargeUtf8Type>(arr.as_list::<i64>())?,
            other => {
                return Err(Error::internal(format!(
                    "Binary decoder does not support data type {other}"
                )));
            }
        };
        // data_size is only tracked in the v2.1 structural decode path; the v2.0 array
        // v2.0 path does not need it so we return 0.
        Ok((result, 0))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{ListArray, UInt8Array};
    use arrow_buffer::OffsetBuffer;
    use arrow_schema::Field;

    use super::*;
    use crate::decoder::DecodeArrayTask;

    struct StubDecodeTask {
        array: ArrayRef,
    }

    impl DecodeArrayTask for StubDecodeTask {
        fn decode(self: Box<Self>) -> Result<(ArrayRef, u64)> {
            Ok((self.array, 0))
        }
    }

    fn make_single_byte_list(value: u8) -> ListArray {
        let offsets = OffsetBuffer::from_lengths([1_usize]);
        let values: ArrayRef = Arc::new(UInt8Array::from(vec![value]));
        ListArray::try_new(
            Arc::new(Field::new("item", DataType::UInt8, false)),
            offsets,
            values,
            None,
        )
        .unwrap()
    }

    #[test]
    fn logical_utf8_decode_preserves_non_overflow_arrow_error() {
        let list = make_single_byte_list(0xFF_u8);
        let decoder = BinaryArrayDecoder {
            inner: Box::new(StubDecodeTask {
                array: Arc::new(list),
            }),
            data_type: DataType::Utf8,
        };

        let error = Box::new(decoder).decode().unwrap_err();
        let message = error.to_string();
        assert!(!message.contains("more than 2GiB of string/binary data"));
        assert!(message.to_lowercase().contains("utf"));
    }

    #[test]
    fn logical_binary_decode_returns_internal_error_for_unsupported_type() {
        let list = make_single_byte_list(b'x');
        let decoder = BinaryArrayDecoder {
            inner: Box::new(StubDecodeTask {
                array: Arc::new(list),
            }),
            data_type: DataType::Int32,
        };

        let error = Box::new(decoder).decode().unwrap_err();
        assert!(matches!(error, Error::Internal { .. }));
        assert!(
            error
                .to_string()
                .contains("Binary decoder does not support data type Int32")
        );
    }
}
