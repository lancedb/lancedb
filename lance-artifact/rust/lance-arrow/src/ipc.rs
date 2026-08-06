// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Zero-copy Arrow IPC stream read/write utilities.
//!
//! Provides helpers for serializing and deserializing [`RecordBatch`]es as
//! self-delimiting Arrow IPC streams using synchronous [`Read`]/[`Write`] I/O.
//!
//! These are designed for embedding IPC streams inside larger binary formats
//! (e.g. a cache entry that contains multiple IPC sections). Each stream is
//! self-delimiting (schema + batches + EOS marker) and can be read back
//! independently.
//!
//! # Zero-copy reads
//!
//! [`read_ipc_stream`] and [`read_ipc_stream_single`] take `&Bytes` and use
//! [`Bytes::slice`] to produce each message buffer. Because `Bytes::slice`
//! increments a reference count rather than copying, the resulting
//! [`Buffer`]s — and the array data decoded from them by [`FileDecoder`] —
//! are all backed by the same allocation as the input.

use std::io::{Read, Write};
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_buffer::Buffer;
use arrow_ipc::convert::fb_to_schema;
use arrow_ipc::reader::FileDecoder;
use arrow_ipc::root_as_message;
use arrow_ipc::writer::StreamWriter;
use arrow_schema::ArrowError;
use bytes::Bytes;

// ---------------------------------------------------------------------------
// Length-prefixed byte utilities
// ---------------------------------------------------------------------------

/// Write `data` prefixed by its length as a little-endian `u64`.
///
/// Paired with [`read_len_prefixed_bytes`].
pub fn write_len_prefixed_bytes(writer: &mut dyn Write, data: &[u8]) -> Result<(), ArrowError> {
    writer
        .write_all(&(data.len() as u64).to_le_bytes())
        .map_err(|e| ArrowError::IoError(e.to_string(), e))?;
    writer
        .write_all(data)
        .map_err(|e| ArrowError::IoError(e.to_string(), e))
}

/// Read a byte slice written by [`write_len_prefixed_bytes`].
///
/// Reads an 8-byte little-endian length then exactly that many bytes.
pub fn read_len_prefixed_bytes(reader: &mut dyn Read) -> Result<Vec<u8>, ArrowError> {
    let mut len_buf = [0u8; 8];
    reader
        .read_exact(&mut len_buf)
        .map_err(|e| ArrowError::IoError(e.to_string(), e))?;
    let len = u64::from_le_bytes(len_buf) as usize;
    let mut buf = vec![0u8; len];
    reader
        .read_exact(&mut buf)
        .map_err(|e| ArrowError::IoError(e.to_string(), e))?;
    Ok(buf)
}

// ---------------------------------------------------------------------------
// IPC stream utilities
// ---------------------------------------------------------------------------

// 4-byte continuation marker used by modern Arrow IPC streams.
const IPC_CONTINUATION: [u8; 4] = [0xff; 4];

/// Write `batch` as a single-batch Arrow IPC stream to `writer`.
pub fn write_ipc_stream(batch: &RecordBatch, writer: &mut dyn Write) -> Result<(), ArrowError> {
    let mut sw = StreamWriter::try_new(&mut *writer, batch.schema_ref())?;
    sw.write(batch)?;
    sw.finish()
}

/// Write all batches from `iter` as a single Arrow IPC stream to `writer`.
///
/// `iter` must yield at least one batch; the schema is inferred from the first
/// batch. Returns `ArrowError::InvalidArgumentError` if the iterator is empty.
/// If you need to write an empty stream (schema only, no rows), construct a
/// `StreamWriter` directly.
pub fn write_ipc_stream_batches<I>(iter: I, writer: &mut dyn Write) -> Result<(), ArrowError>
where
    I: IntoIterator<Item = RecordBatch>,
{
    let mut iter = iter.into_iter();
    let first = iter
        .next()
        .ok_or_else(|| ArrowError::InvalidArgumentError("no batches to serialize".into()))?;
    let mut sw = StreamWriter::try_new(&mut *writer, first.schema_ref())?;
    sw.write(&first)?;
    for batch in iter {
        sw.write(&batch)?;
    }
    sw.finish()
}

/// Read one complete Arrow IPC stream message from `data` as a zero-copy [`Buffer`].
///
/// Parses the first message starting at byte 0 of `data`. Returns `None` on
/// EOS (size field == 0) or empty input. The returned [`Buffer`] is backed by
/// `data`'s allocation — no bytes are copied.
///
/// The caller should advance its position by `buf.len()` after each call.
fn read_one_ipc_message(data: &Bytes) -> Result<Option<Buffer>, ArrowError> {
    let bytes = data.as_ref();

    if bytes.is_empty() {
        return Ok(None);
    }
    if bytes.len() < 4 {
        return Err(ArrowError::IoError(
            "IPC: truncated header".into(),
            std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "truncated IPC header"),
        ));
    }

    let has_continuation = bytes[..4] == IPC_CONTINUATION;
    let (size_bytes, prefix_len): ([u8; 4], usize) = if has_continuation {
        if bytes.len() < 8 {
            return Err(ArrowError::IoError(
                "IPC: truncated header after continuation".into(),
                std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "truncated after continuation",
                ),
            ));
        }
        (bytes[4..8].try_into().unwrap(), 8)
    } else {
        (bytes[..4].try_into().unwrap(), 4)
    };

    let meta_size = u32::from_le_bytes(size_bytes) as usize;
    if meta_size == 0 {
        return Ok(None); // EOS
    }

    let meta_end = prefix_len + meta_size;
    if bytes.len() < meta_end {
        return Err(ArrowError::IoError(
            "IPC: truncated metadata".into(),
            std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "truncated IPC metadata"),
        ));
    }

    let msg = root_as_message(&bytes[prefix_len..meta_end])
        .map_err(|e| ArrowError::ParseError(format!("IPC message parse error: {e}")))?;
    let body_len = msg.bodyLength() as usize;

    let total = meta_end + body_len;
    if bytes.len() < total {
        return Err(ArrowError::IoError(
            "IPC: truncated body".into(),
            std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "truncated IPC body"),
        ));
    }

    // Zero-copy: Bytes::slice shares the backing allocation; Buffer::from
    // wraps it without copying.
    Ok(Some(Buffer::from(data.slice(0..total))))
}

/// Read a length-prefixed byte slice at `offset` in `data`, advancing `offset`.
///
/// Reads an 8-byte little-endian length, then slices exactly that many bytes
/// from `data`. The returned [`Bytes`] is zero-copy (shares `data`'s allocation).
pub fn read_len_prefixed_bytes_at(data: &Bytes, offset: &mut usize) -> Result<Bytes, ArrowError> {
    let bytes = data.as_ref();
    let len_end = offset
        .checked_add(8)
        .filter(|&e| e <= bytes.len())
        .ok_or_else(|| {
            ArrowError::IoError(
                "length-prefixed bytes: truncated length field".into(),
                std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "truncated length"),
            )
        })?;
    let len = u64::from_le_bytes(bytes[*offset..len_end].try_into().unwrap()) as usize;
    *offset = len_end;
    let data_end = offset
        .checked_add(len)
        .filter(|&e| e <= bytes.len())
        .ok_or_else(|| {
            ArrowError::IoError(
                "length-prefixed bytes: truncated data".into(),
                std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "truncated data"),
            )
        })?;
    let result = data.slice(*offset..data_end);
    *offset = data_end;
    Ok(result)
}

/// Read all [`RecordBatch`]es from one Arrow IPC stream starting at `offset`,
/// advancing `offset` past the stream (including the EOS marker).
///
/// Zero-copy: array buffers borrow from `data`'s allocation.
pub fn read_ipc_stream_at(
    data: &Bytes,
    offset: &mut usize,
) -> Result<Vec<RecordBatch>, ArrowError> {
    let batches = read_ipc_stream(&data.slice(*offset..))?;

    // Recompute how many bytes were consumed by re-parsing message sizes.
    // We can't get this from read_ipc_stream directly, so we re-walk the
    // message headers (metadata only, no body re-read) to sum up lengths.
    let slice = &data.as_ref()[*offset..];
    let mut consumed = 0usize;
    loop {
        let rem = &slice[consumed..];
        if rem.is_empty() {
            break;
        }
        let has_cont = rem.len() >= 4 && rem[..4] == IPC_CONTINUATION;
        let (size_bytes, prefix_len): ([u8; 4], usize) = if has_cont {
            if rem.len() < 8 {
                break;
            }
            (rem[4..8].try_into().unwrap(), 8)
        } else {
            if rem.len() < 4 {
                break;
            }
            (rem[..4].try_into().unwrap(), 4)
        };
        let meta_size = u32::from_le_bytes(size_bytes) as usize;
        if meta_size == 0 {
            // EOS — consume it and stop.
            consumed += prefix_len;
            break;
        }
        let meta_end = prefix_len + meta_size;
        if rem.len() < meta_end {
            break;
        }
        let msg = root_as_message(&rem[prefix_len..meta_end])
            .map_err(|e| ArrowError::ParseError(format!("IPC message parse error: {e}")))?;
        let body_len = msg.bodyLength() as usize;
        consumed += meta_end + body_len;
    }
    *offset += consumed;

    Ok(batches)
}

/// Read exactly one [`RecordBatch`] from one Arrow IPC stream starting at `offset`,
/// advancing `offset` past the stream (including the EOS marker).
///
/// Zero-copy: array buffers borrow from `data`'s allocation.
pub fn read_ipc_stream_single_at(
    data: &Bytes,
    offset: &mut usize,
) -> Result<RecordBatch, ArrowError> {
    let mut batches = read_ipc_stream_at(data, offset)?;
    match batches.len() {
        1 => Ok(batches.remove(0)),
        n => Err(ArrowError::ParseError(format!(
            "expected exactly 1 IPC record batch, got {n}"
        ))),
    }
}

/// Extract the prefix length and metadata size from a raw IPC message buffer.
///
/// Modern IPC streams have an 8-byte prefix `[continuation: 4][size: 4]`.
/// Legacy streams have a 4-byte prefix `[size: 4]`. Returns `(prefix_len, meta_size)`.
fn parse_ipc_message_prefix(buf: &Buffer) -> Result<(usize, usize), ArrowError> {
    let has_continuation = buf.len() >= 4 && buf[..4] == IPC_CONTINUATION;
    if has_continuation {
        if buf.len() < 8 {
            return Err(ArrowError::ParseError(
                "IPC message buffer too short".into(),
            ));
        }
        let meta_size = u32::from_le_bytes(buf[4..8].try_into().unwrap()) as usize;
        Ok((8, meta_size))
    } else {
        if buf.len() < 4 {
            return Err(ArrowError::ParseError(
                "IPC message buffer too short".into(),
            ));
        }
        let meta_size = u32::from_le_bytes(buf[..4].try_into().unwrap()) as usize;
        Ok((4, meta_size))
    }
}

/// Read all [`RecordBatch`]es from one Arrow IPC stream.
///
/// Zero-copy: each batch's array data buffers are borrowed from the input
/// message buffer(s) and not copied during decoding.
///
/// Uses [`FileDecoder`] directly (rather than `StreamDecoder`) to avoid a
/// known edge case where `StreamDecoder` does not produce a batch for messages
/// with a zero-length body when the message exactly fills the decode buffer.
pub fn read_ipc_stream(data: &Bytes) -> Result<Vec<RecordBatch>, ArrowError> {
    let mut offset = 0usize;

    let schema_buf = read_one_ipc_message(&data.slice(offset..))?.ok_or_else(|| {
        ArrowError::ParseError("IPC stream: expected schema message, got EOS".into())
    })?;
    offset += schema_buf.len();

    let (prefix_len, meta_size) = parse_ipc_message_prefix(&schema_buf)?;
    let schema_msg = root_as_message(&schema_buf[prefix_len..prefix_len + meta_size])
        .map_err(|e| ArrowError::ParseError(format!("IPC schema parse error: {e}")))?;
    let schema = Arc::new(fb_to_schema(schema_msg.header_as_schema().ok_or_else(
        || ArrowError::ParseError("IPC stream: first message is not a schema".into()),
    )?));
    let mut decoder = FileDecoder::new(schema, schema_msg.version());

    let mut batches = Vec::new();

    loop {
        let Some(buf) = read_one_ipc_message(&data.slice(offset..))? else {
            break;
        };
        offset += buf.len();

        let (prefix_len, meta_size) = parse_ipc_message_prefix(&buf)?;
        let msg = root_as_message(&buf[prefix_len..prefix_len + meta_size])
            .map_err(|e| ArrowError::ParseError(format!("IPC message parse error: {e}")))?;
        let body_len = msg.bodyLength() as usize;

        // Block offset = 0 since the buffer starts at the message boundary.
        // metaDataLength = prefix_len + meta_size (prefix + flatbuf + padding).
        let block = arrow_ipc::Block::new(0, (prefix_len + meta_size) as i32, body_len as i64);

        match msg.header_type() {
            arrow_ipc::MessageHeader::RecordBatch => {
                if let Some(batch) = decoder.read_record_batch(&block, &buf)? {
                    batches.push(batch);
                }
            }
            arrow_ipc::MessageHeader::DictionaryBatch => {
                decoder.read_dictionary(&block, &buf)?;
            }
            _ => break,
        }
    }

    Ok(batches)
}

/// Read exactly one [`RecordBatch`] from one Arrow IPC stream.
pub fn read_ipc_stream_single(data: &Bytes) -> Result<RecordBatch, ArrowError> {
    let mut batches = read_ipc_stream(data)?;
    match batches.len() {
        1 => Ok(batches.remove(0)),
        n => Err(ArrowError::ParseError(format!(
            "expected exactly 1 IPC record batch, got {n}"
        ))),
    }
}

// ---------------------------------------------------------------------------
// Aligned IPC sections
// ---------------------------------------------------------------------------

/// Byte alignment that each IPC section's stream start is padded to.
///
/// When several IPC streams are concatenated into one larger blob (e.g. a
/// cache entry), a section that starts at an arbitrary offset would leave its
/// array data misaligned. [`FileDecoder`] with `require_alignment = false`
/// then silently copies each buffer into a freshly aligned allocation on
/// every read, defeating zero-copy. Padding each section start to a 64-byte
/// boundary keeps the decoded buffers borrowed directly from the input.
pub const IPC_SECTION_ALIGNMENT: usize = 64;

/// Number of zero-padding bytes needed to advance `pos` to the next
/// [`IPC_SECTION_ALIGNMENT`] boundary.
fn section_padding(pos: usize) -> usize {
    (IPC_SECTION_ALIGNMENT - (pos % IPC_SECTION_ALIGNMENT)) % IPC_SECTION_ALIGNMENT
}

/// A [`Write`] adapter that counts the bytes written through it.
struct CountingWriter<'a> {
    inner: &'a mut dyn Write,
    count: usize,
}

impl Write for CountingWriter<'_> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let n = self.inner.write(buf)?;
        self.count += n;
        Ok(n)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

/// Write zero padding so the next byte lands on an [`IPC_SECTION_ALIGNMENT`]
/// boundary, advancing `pos` past it.
fn write_section_padding(writer: &mut dyn Write, pos: &mut usize) -> Result<(), ArrowError> {
    let pad = section_padding(*pos);
    if pad > 0 {
        const ZEROS: [u8; IPC_SECTION_ALIGNMENT] = [0u8; IPC_SECTION_ALIGNMENT];
        writer
            .write_all(&ZEROS[..pad])
            .map_err(|e| ArrowError::IoError(e.to_string(), e))?;
        *pos += pad;
    }
    Ok(())
}

/// Write `batch` as a 64-byte-aligned single-batch Arrow IPC section.
///
/// `pos` is the absolute byte offset of `writer` within the enclosing blob.
/// Zero padding is written first so the IPC stream begins on an
/// [`IPC_SECTION_ALIGNMENT`] boundary, then the stream itself. `pos` is
/// advanced past both the padding and the stream so the caller can write
/// further aligned sections.
///
/// Paired with [`read_ipc_section_at`]. For the decoded buffers to be borrowed
/// zero-copy, the blob must ultimately be read back from a buffer whose base
/// address is at least 64-byte aligned.
pub fn write_ipc_section(
    writer: &mut dyn Write,
    pos: &mut usize,
    batch: &RecordBatch,
) -> Result<(), ArrowError> {
    write_section_padding(writer, pos)?;

    let mut counting = CountingWriter {
        inner: writer,
        count: 0,
    };
    write_ipc_stream(batch, &mut counting)?;
    *pos += counting.count;
    Ok(())
}

/// Read a single [`RecordBatch`] from an aligned IPC section at `offset`.
///
/// Skips the alignment padding written by [`write_ipc_section`], then reads
/// the stream, advancing `offset` past the section (padding + stream + EOS).
///
/// Zero-copy: array buffers borrow from `data`'s allocation when `data`'s base
/// address is at least 64-byte aligned (see [`write_ipc_section`]).
pub fn read_ipc_section_at(data: &Bytes, offset: &mut usize) -> Result<RecordBatch, ArrowError> {
    *offset += section_padding(*offset);
    read_ipc_stream_single_at(data, offset)
}

/// Write `batches` as a single 64-byte-aligned multi-batch Arrow IPC section.
///
/// Like [`write_ipc_section`] but emits every batch from `iter` into one IPC
/// stream (schema + N batches + EOS). `iter` must yield at least one batch.
/// Paired with [`read_ipc_section_batches_at`].
pub fn write_ipc_section_batches<I>(
    writer: &mut dyn Write,
    pos: &mut usize,
    iter: I,
) -> Result<(), ArrowError>
where
    I: IntoIterator<Item = RecordBatch>,
{
    write_section_padding(writer, pos)?;

    let mut counting = CountingWriter {
        inner: writer,
        count: 0,
    };
    write_ipc_stream_batches(iter, &mut counting)?;
    *pos += counting.count;
    Ok(())
}

/// Read all [`RecordBatch`]es from an aligned multi-batch IPC section at
/// `offset`, advancing `offset` past the section (padding + stream + EOS).
///
/// Zero-copy: array buffers borrow from `data`'s allocation when `data`'s base
/// address is at least 64-byte aligned (see [`write_ipc_section_batches`]).
pub fn read_ipc_section_batches_at(
    data: &Bytes,
    offset: &mut usize,
) -> Result<Vec<RecordBatch>, ArrowError> {
    *offset += section_padding(*offset);
    read_ipc_stream_at(data, offset)
}

#[cfg(test)]
mod tests {
    use arrow_array::{ArrayRef, record_batch};

    use super::*;

    #[test]
    fn test_ipc_roundtrip() {
        let batch1 = record_batch!(
            ("int", Int32, [1, 2, 3]),
            ("str", Utf8, ["foo", "bar", "baz"])
        )
        .unwrap();
        let batch2 = record_batch!(("int", Int32, [4, 5]), ("str", Utf8, ["qux", "quux"])).unwrap();
        let batches = vec![batch1.clone(), batch2.clone()];

        let mut buf = Vec::new();
        write_ipc_stream_batches(batches, &mut buf).unwrap();

        let data = Bytes::from(buf);

        let batches = read_ipc_stream(&data).unwrap();
        assert_eq!(batches.len(), 2);
        assert_eq!(batches[0], batch1);
        assert_eq!(batches[1], batch2);

        let data_base = data.as_ptr() as usize;
        let data_end = data_base + data.len();
        let assert_col_zero_copy = |array: &ArrayRef| {
            for buffer in array.to_data().buffers() {
                let ptr = buffer.as_ptr() as usize;
                assert!(
                    ptr >= data_base && ptr < data_end,
                    "buffer at {ptr:#x} is not backed by the input Bytes allocation \
                     [{data_base:#x}..{data_end:#x})"
                );
            }
        };

        for batch in &batches {
            assert_eq!(batch.schema(), batch1.schema());
            assert_col_zero_copy(batch.column(0));
            assert_col_zero_copy(batch.column(1));
        }
    }

    /// Allocate a [`Bytes`] whose base address is 64-byte aligned, modelling a
    /// backend that reads cache entries into an aligned buffer. A plain
    /// `Bytes::from(vec)` only guarantees the allocator's alignment for `u8`.
    fn aligned_bytes(payload: &[u8]) -> Bytes {
        let mut v = vec![0u8; payload.len() + IPC_SECTION_ALIGNMENT];
        let pad = section_padding(v.as_ptr() as usize);
        v[pad..pad + payload.len()].copy_from_slice(payload);
        Bytes::from(v).slice(pad..pad + payload.len())
    }

    #[test]
    fn test_aligned_ipc_sections_are_zero_copy() {
        // A LargeBinary column exercises the i64-offset buffer whose 8-byte
        // alignment requirement triggers a realigning memcpy when misaligned.
        let blocks = arrow_array::LargeBinaryArray::from_vec(vec![&b"hello"[..], b"world"]);
        let section_a = RecordBatch::try_from_iter([("a", Arc::new(blocks) as ArrayRef)]).unwrap();
        let section_b = record_batch!(("b", Int64, [10i64, 20, 30, 40, 50])).unwrap();

        let mut buf = Vec::new();
        // Arbitrary, deliberately non-64-aligned preamble so the first section
        // must be padded rather than landing at offset 0 by luck.
        buf.extend_from_slice(&[0xABu8; 7]);
        let mut pos = buf.len();
        // The first section's stream begins after padding the 7-byte preamble
        // up to the next 64-byte boundary.
        assert_eq!(7 + section_padding(7), IPC_SECTION_ALIGNMENT);
        write_ipc_section(&mut buf, &mut pos, &section_a).unwrap();
        write_ipc_section(&mut buf, &mut pos, &section_b).unwrap();

        let data = aligned_bytes(&buf);
        assert_eq!(
            section_padding(data.as_ptr() as usize),
            0,
            "base not aligned"
        );

        let mut offset = 7;
        let read_a = read_ipc_section_at(&data, &mut offset).unwrap();
        let read_b = read_ipc_section_at(&data, &mut offset).unwrap();
        assert_eq!(read_a, section_a);
        assert_eq!(read_b, section_b);

        let data_base = data.as_ptr() as usize;
        let data_end = data_base + data.len();
        for batch in [&read_a, &read_b] {
            for buffer in batch.column(0).to_data().buffers() {
                let ptr = buffer.as_ptr() as usize;
                assert!(
                    ptr >= data_base && ptr < data_end,
                    "section buffer at {ptr:#x} was realigned out of the input \
                     [{data_base:#x}..{data_end:#x}) — misaligned section",
                );
            }
        }
    }

    #[test]
    fn test_aligned_multi_batch_section_roundtrip_zero_copy() {
        // A multi-batch section (e.g. IVF SQ storage chunks) must round-trip
        // every batch and decode the first batch's buffers zero-copy.
        let b1 = record_batch!(("v", Int64, [1i64, 2, 3])).unwrap();
        let b2 = record_batch!(("v", Int64, [4i64, 5])).unwrap();
        let b3 = record_batch!(("v", Int64, [6i64])).unwrap();

        let mut buf = vec![0xCDu8; 5];
        let mut pos = buf.len();
        write_ipc_section_batches(&mut buf, &mut pos, [b1.clone(), b2.clone(), b3.clone()])
            .unwrap();

        let data = aligned_bytes(&buf);
        let mut offset = 5;
        let read = read_ipc_section_batches_at(&data, &mut offset).unwrap();
        assert_eq!(read, vec![b1, b2, b3]);
        assert_eq!(offset, buf.len(), "offset should land at section end");

        let data_base = data.as_ptr() as usize;
        let data_end = data_base + data.len();
        for buffer in read[0].column(0).to_data().buffers() {
            let ptr = buffer.as_ptr() as usize;
            assert!(
                ptr >= data_base && ptr < data_end,
                "first batch buffer at {ptr:#x} was realigned out of the input",
            );
        }
    }
}
