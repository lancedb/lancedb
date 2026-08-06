// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Streaming readers/writers for cache entry bodies.
//!
//! [`CacheCodecImpl`](super::CacheCodecImpl) bodies are written and read
//! through these wrappers. They keep serialization streaming (no buffering of
//! the whole entry) and reads zero-copy (sections borrow from the input
//! [`Bytes`]), while tracking the byte position needed to keep Arrow IPC
//! sections 64-byte aligned (see [`lance_arrow::ipc`]).
//!
//! Body layout primitives:
//!
//! ```text
//! HEADER    : [header_len: u32 LE][header proto bytes]
//! ARROW_IPC : [pad to 64B][self-delimiting IPC stream]
//! RAW_BLOB  : [len: u64 LE][bytes]
//! ```

use std::io::Write;

use arrow_array::RecordBatch;
use bytes::Bytes;
use prost::Message;

use crate::{Error, Result};

/// Writes a cache entry body: a header followed by sections, streaming
/// directly to the underlying writer.
///
/// The envelope is written by the [`CacheCodec`](super::CacheCodec) wrapper
/// before this writer is handed to
/// [`CacheCodecImpl::serialize`](super::CacheCodecImpl::serialize).
pub struct CacheEntryWriter<'a> {
    writer: &'a mut dyn Write,
    /// Absolute byte offset within the entry, used to align IPC sections.
    pos: usize,
}

impl<'a> CacheEntryWriter<'a> {
    /// Create a writer positioned at the start of an entry (offset 0).
    ///
    /// Use this for nested serialization into a standalone buffer. The
    /// envelope-aware entry point is [`CacheCodec::serialize`](super::CacheCodec::serialize).
    pub fn new(writer: &'a mut dyn Write) -> Self {
        Self { writer, pos: 0 }
    }

    /// Create a writer whose section alignment accounts for `pos` bytes
    /// already written ahead of the body (i.e. the envelope).
    pub(crate) fn with_pos(writer: &'a mut dyn Write, pos: usize) -> Self {
        Self { writer, pos }
    }

    /// Write a single discriminant byte (e.g. a variant tag).
    pub fn write_u8(&mut self, value: u8) -> Result<()> {
        self.writer.write_all(&[value])?;
        self.pos += 1;
        Ok(())
    }

    /// Write a protobuf header as `[len: u32 LE][bytes]`.
    pub fn write_header<P: Message>(&mut self, header: &P) -> Result<()> {
        let bytes = header.encode_to_vec();
        let len = u32::try_from(bytes.len())
            .map_err(|_| Error::io(format!("cache header too large: {} bytes", bytes.len())))?;
        self.writer.write_all(&len.to_le_bytes())?;
        self.writer.write_all(&bytes)?;
        self.pos += 4 + bytes.len();
        Ok(())
    }

    /// Write `batch` as a 64-byte-aligned Arrow IPC section.
    pub fn write_ipc(&mut self, batch: &RecordBatch) -> Result<()> {
        lance_arrow::ipc::write_ipc_section(self.writer, &mut self.pos, batch)
            .map_err(|e| Error::io(e.to_string()))
    }

    /// Write `batches` as a single 64-byte-aligned multi-batch Arrow IPC
    /// section. The iterator must yield at least one batch.
    pub fn write_ipc_batches<I>(&mut self, batches: I) -> Result<()>
    where
        I: IntoIterator<Item = RecordBatch>,
    {
        lance_arrow::ipc::write_ipc_section_batches(self.writer, &mut self.pos, batches)
            .map_err(|e| Error::io(e.to_string()))
    }

    /// Write a raw blob as `[len: u64 LE][bytes]`.
    ///
    /// Only for byte payloads that already have their own stable, portable
    /// encoding (e.g. a roaring bitmap, a varint-packed stream).
    pub fn write_raw(&mut self, bytes: &[u8]) -> Result<()> {
        lance_arrow::ipc::write_len_prefixed_bytes(self.writer, bytes)
            .map_err(|e| Error::io(e.to_string()))?;
        self.pos += 8 + bytes.len();
        Ok(())
    }

    /// The underlying writer, for a payload that carries its own framing.
    ///
    /// Use this only when the codec writes a self-delimiting or whole-body
    /// payload — e.g. streaming a roaring bitmap as the entire body, where the
    /// length prefix of [`write_raw`](Self::write_raw) would be redundant and
    /// buffering to measure that length would force an extra copy. For
    /// structured bodies prefer [`write_header`](Self::write_header) /
    /// [`write_ipc`](Self::write_ipc) / [`write_raw`](Self::write_raw), which
    /// give you versioning and 64-byte IPC alignment.
    ///
    /// Bytes written through this do **not** advance the section-alignment
    /// position, so it must not be interleaved with [`write_ipc`](Self::write_ipc).
    pub fn raw_writer(&mut self) -> &mut dyn Write {
        self.writer
    }
}

/// Reads a cache entry body, tracking an offset into the input and exposing
/// the entry's `type_version` so implementors can branch for backward compat.
///
/// All reads are zero-copy: returned [`Bytes`] and the buffers behind decoded
/// [`RecordBatch`]es borrow from the input allocation.
pub struct CacheEntryReader<'a> {
    data: &'a Bytes,
    offset: usize,
    version: u32,
}

impl<'a> CacheEntryReader<'a> {
    /// Create a reader over `data`, starting at body byte `offset`, for an
    /// entry written at `version`.
    pub fn new(data: &'a Bytes, offset: usize, version: u32) -> Self {
        Self {
            data,
            offset,
            version,
        }
    }

    /// The `type_version` from the envelope. Branch on this for backward compat.
    pub fn version(&self) -> u32 {
        self.version
    }

    /// Read a single discriminant byte written by [`CacheEntryWriter::write_u8`].
    pub fn read_u8(&mut self) -> Result<u8> {
        let bytes = self.data.as_ref();
        let v = *bytes
            .get(self.offset)
            .ok_or_else(|| Error::io("cache entry: truncated, missing tag byte".to_string()))?;
        self.offset += 1;
        Ok(v)
    }

    /// Read a protobuf header written by [`CacheEntryWriter::write_header`].
    pub fn read_header<P: Message + Default>(&mut self) -> Result<P> {
        let bytes = self.data.as_ref();
        let len_end = self
            .offset
            .checked_add(4)
            .filter(|&e| e <= bytes.len())
            .ok_or_else(|| Error::io("cache header: truncated length prefix".to_string()))?;
        let len = u32::from_le_bytes(bytes[self.offset..len_end].try_into().unwrap()) as usize;
        let data_end = len_end
            .checked_add(len)
            .filter(|&e| e <= bytes.len())
            .ok_or_else(|| Error::io("cache header: truncated body".to_string()))?;
        let msg = P::decode(&bytes[len_end..data_end])
            .map_err(|e| Error::io(format!("cache header decode failed: {e}")))?;
        self.offset = data_end;
        Ok(msg)
    }

    /// Read one [`RecordBatch`] from a 64-byte-aligned IPC section.
    pub fn read_ipc(&mut self) -> Result<RecordBatch> {
        lance_arrow::ipc::read_ipc_section_at(self.data, &mut self.offset)
            .map_err(|e| Error::io(e.to_string()))
    }

    /// Read all [`RecordBatch`]es from a 64-byte-aligned multi-batch IPC
    /// section written by [`CacheEntryWriter::write_ipc_batches`].
    pub fn read_ipc_batches(&mut self) -> Result<Vec<RecordBatch>> {
        lance_arrow::ipc::read_ipc_section_batches_at(self.data, &mut self.offset)
            .map_err(|e| Error::io(e.to_string()))
    }

    /// Read a raw blob written by [`CacheEntryWriter::write_raw`], zero-copy.
    pub fn read_raw(&mut self) -> Result<Bytes> {
        lance_arrow::ipc::read_len_prefixed_bytes_at(self.data, &mut self.offset)
            .map_err(|e| Error::io(e.to_string()))
    }

    /// The not-yet-consumed body bytes as a zero-copy slice.
    ///
    /// For a payload that carries its own framing and is parsed with the
    /// codec's own cursor — the read counterpart of
    /// [`CacheEntryWriter::raw_writer`]. For structured bodies prefer
    /// [`read_header`](Self::read_header) / [`read_ipc`](Self::read_ipc) /
    /// [`read_raw`](Self::read_raw).
    pub fn body(&self) -> Bytes {
        self.data.slice(self.offset..)
    }
}
