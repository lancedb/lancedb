// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Frame IPC bodies before decoding, avoiding StreamDecoder's growing scratch buffer.

use arrow_buffer::Buffer;
use arrow_schema::ArrowError;
use bytes::Bytes;
use futures::StreamExt;
use futures::stream::BoxStream;

use crate::error::BoxError;

pub(super) enum FramingError {
    Interrupted(BoxError),
    Arrow(ArrowError),
}

#[derive(Clone, Copy)]
enum Frame {
    Header {
        bytes: [u8; 4],
        read: usize,
        continuation: bool,
    },
    Metadata(usize),
    Body(usize),
    End,
}

impl Default for Frame {
    fn default() -> Self {
        Self::Header {
            bytes: [0; 4],
            read: 0,
            continuation: false,
        }
    }
}

pub(super) struct IpcFraming {
    chunks: BoxStream<'static, Result<Bytes, BoxError>>,
    chunk: Buffer,
    frame: Frame,
}

impl IpcFraming {
    pub(super) fn new(chunks: BoxStream<'static, Result<Bytes, BoxError>>) -> Self {
        Self {
            chunks,
            chunk: Buffer::default(),
            frame: Frame::default(),
        }
    }

    pub(super) async fn next_buffer(&mut self) -> Result<Option<Buffer>, FramingError> {
        let buffer = match self.frame {
            Frame::Header {
                mut bytes,
                mut read,
                continuation,
            } => {
                // Forward even a partial prefix immediately. StreamDecoder may
                // need that byte to yield the preceding zero-body record batch.
                self.fill_chunk().await?;
                let take = self.chunk.len().min(4 - read);
                let buffer = self.chunk.slice_with_length(0, take);
                self.chunk.advance(take);
                bytes[read..read + take].copy_from_slice(&buffer);
                read += take;
                self.frame = if read == 4 {
                    match u32::from_le_bytes(bytes) {
                        u32::MAX if !continuation => Frame::Header {
                            bytes: [0; 4],
                            read: 0,
                            continuation: true,
                        },
                        0 => Frame::End,
                        length => Frame::Metadata(length as usize),
                    }
                } else {
                    Frame::Header {
                        bytes,
                        read,
                        continuation,
                    }
                };
                buffer
            }
            Frame::Metadata(length) => {
                let metadata = self.read_exact(length).await?;
                let message = arrow_ipc::root_as_message(&metadata).map_err(|error| {
                    FramingError::Arrow(ArrowError::IpcError(format!(
                        "Invalid IPC message: {error}"
                    )))
                })?;
                let length = usize::try_from(message.bodyLength()).map_err(|_| {
                    FramingError::Arrow(ArrowError::IpcError(
                        "IPC message body length is negative or exceeds addressable memory".into(),
                    ))
                })?;
                self.frame = if length == 0 {
                    Frame::default()
                } else {
                    Frame::Body(length)
                };
                metadata
            }
            Frame::Body(length) => {
                let body = self.read_exact(length).await?;
                self.frame = Frame::default();
                body
            }
            Frame::End => {
                // Forward trailing bytes to the decoder so it rejects data after EOS.
                if !self.chunk.is_empty() {
                    return Ok(Some(std::mem::take(&mut self.chunk)));
                }
                return self
                    .chunks
                    .next()
                    .await
                    .transpose()
                    .map(|chunk| chunk.map(Buffer::from))
                    .map_err(FramingError::Interrupted);
            }
        };
        Ok(Some(buffer))
    }

    async fn read_exact(&mut self, length: usize) -> Result<Buffer, FramingError> {
        self.fill_chunk().await?;
        if self.chunk.len() >= length {
            let buffer = self.chunk.slice_with_length(0, length);
            self.chunk.advance(length);
            return Ok(buffer);
        }
        // The declared length is untrusted. Arrow's infallible aligned allocator
        // can panic on its layout or abort on OOM; Vec exposes both as errors.
        // StreamDecoder handles any array-specific alignment when decoding.
        let mut buffer = Vec::new();
        buffer.try_reserve_exact(length).map_err(|error| {
            FramingError::Arrow(ArrowError::IpcError(format!(
                "cannot allocate IPC message of {length} bytes: {error}"
            )))
        })?;
        while buffer.len() < length {
            self.fill_chunk().await?;
            let take = self.chunk.len().min(length - buffer.len());
            buffer.extend_from_slice(&self.chunk[..take]);
            self.chunk.advance(take);
        }
        Ok(Buffer::from_vec(buffer))
    }

    async fn fill_chunk(&mut self) -> Result<(), FramingError> {
        while self.chunk.is_empty() {
            self.chunk = self
                .chunks
                .next()
                .await
                .ok_or_else(|| {
                    FramingError::Interrupted(
                        "the response ended before the Arrow end-of-stream marker".into(),
                    )
                })?
                .map_err(FramingError::Interrupted)?
                .into();
        }
        Ok(())
    }
}
