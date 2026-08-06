// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use futures::Future;
use lance_core::Result;
use lance_core::cache::{CacheBackend, CacheCodec, CacheEntry, InternalCacheKey, MokaCacheBackend};

#[derive(Debug)]
struct SerializedEntry {
    bytes: Bytes,
    size_bytes: usize,
}

#[derive(Debug, Default)]
struct SerializedStore {
    entries: tokio::sync::Mutex<HashMap<InternalCacheKey, SerializedEntry>>,
    insert_counts: tokio::sync::Mutex<HashMap<&'static str, usize>>,
}

/// Test-only cache backend that forces codec-backed entries through bytes.
///
/// The serialized store can survive [`restart`](Self::restart), while entries
/// without a codec live only in the in-memory L1. This deliberately models the
/// persistence boundary, not a production cache:
/// - it has no capacity limit or eviction policy for serialized entries;
/// - concurrent serialized misses are not single-flight;
/// - accounting uses the logical size supplied by the cache layer.
#[derive(Debug)]
pub struct SerializingCacheBackend {
    serialized: Arc<SerializedStore>,
    l1: MokaCacheBackend,
}

impl SerializingCacheBackend {
    pub fn new() -> Self {
        Self {
            serialized: Arc::new(SerializedStore::default()),
            l1: MokaCacheBackend::with_capacity(256 * 1024 * 1024),
        }
    }

    /// Recreate the backend over the same serialized bytes and an empty L1.
    pub fn restart(&self) -> Self {
        Self {
            serialized: self.serialized.clone(),
            l1: MokaCacheBackend::with_capacity(256 * 1024 * 1024),
        }
    }

    pub async fn serialized_entry_count(&self) -> usize {
        self.serialized.entries.lock().await.len()
    }

    pub async fn serialized_insert_count(&self, type_id: &'static str) -> usize {
        self.serialized
            .insert_counts
            .lock()
            .await
            .get(type_id)
            .copied()
            .unwrap_or(0)
    }

    pub async fn l1_entry_count(&self) -> usize {
        self.l1.num_entries().await
    }
}

#[async_trait]
impl CacheBackend for SerializingCacheBackend {
    async fn get(&self, key: &InternalCacheKey, codec: Option<CacheCodec>) -> Option<CacheEntry> {
        let Some(codec) = codec else {
            return self.l1.get(key, None).await;
        };
        let bytes = self
            .serialized
            .entries
            .lock()
            .await
            .get(key)
            .map(|entry| entry.bytes.clone())?;
        codec.deserialize(&bytes).hit()
    }

    async fn insert(
        &self,
        key: &InternalCacheKey,
        entry: CacheEntry,
        size_bytes: usize,
        codec: Option<CacheCodec>,
    ) {
        let Some(codec) = codec else {
            self.l1.insert(key, entry, size_bytes, None).await;
            return;
        };
        let mut bytes = Vec::new();
        codec
            .serialize(&entry, &mut bytes)
            .expect("test cache entry serialization should succeed");
        let mut insert_counts = self.serialized.insert_counts.lock().await;
        *insert_counts.entry(codec.type_id()).or_default() += 1;
        drop(insert_counts);
        self.serialized.entries.lock().await.insert(
            *key,
            SerializedEntry {
                bytes: Bytes::from(bytes),
                size_bytes,
            },
        );
    }

    async fn get_or_insert<'a>(
        &self,
        key: &InternalCacheKey,
        loader: Pin<Box<dyn Future<Output = Result<(CacheEntry, usize)>> + Send + 'a>>,
        codec: Option<CacheCodec>,
    ) -> Result<(CacheEntry, bool)> {
        if let Some(entry) = self.get(key, codec).await {
            return Ok((entry, true));
        }
        let (entry, size_bytes) = loader.await?;
        self.insert(key, entry.clone(), size_bytes, codec).await;
        Ok((entry, false))
    }

    async fn clear(&self) {
        self.serialized.entries.lock().await.clear();
        self.l1.clear().await;
    }

    async fn num_entries(&self) -> usize {
        self.serialized.entries.lock().await.len() + self.l1.num_entries().await
    }

    async fn size_bytes(&self) -> usize {
        let serialized_size = self
            .serialized
            .entries
            .lock()
            .await
            .values()
            .map(|entry| entry.size_bytes)
            .sum::<usize>();
        serialized_size.saturating_add(self.l1.size_bytes().await)
    }
}

#[cfg(test)]
mod tests {
    use lance_core::cache::{CacheCodecImpl, CacheEntryReader, CacheEntryWriter};
    use lance_core::{Error, Result};

    use super::*;

    #[derive(Debug)]
    struct StoredValue(u32);

    impl CacheCodecImpl for StoredValue {
        const TYPE_ID: &'static str = "test.LookupCodec";
        const CURRENT_VERSION: u32 = 1;

        fn serialize(&self, writer: &mut CacheEntryWriter<'_>) -> Result<()> {
            writer.write_raw(&self.0.to_le_bytes())
        }

        fn deserialize(_reader: &mut CacheEntryReader<'_>) -> Result<Self> {
            Err(Error::internal(
                "the codec retained at insert time must not be used for lookup",
            ))
        }
    }

    #[derive(Debug, PartialEq)]
    struct LookupValue(u32);

    impl CacheCodecImpl for LookupValue {
        const TYPE_ID: &'static str = "test.LookupCodec";
        const CURRENT_VERSION: u32 = 1;

        fn serialize(&self, writer: &mut CacheEntryWriter<'_>) -> Result<()> {
            writer.write_raw(&self.0.to_le_bytes())
        }

        fn deserialize(reader: &mut CacheEntryReader<'_>) -> Result<Self> {
            let bytes = reader.read_raw()?;
            let value = u32::from_le_bytes(
                bytes
                    .as_ref()
                    .try_into()
                    .map_err(|_| Error::internal("invalid test cache value"))?,
            );
            Ok(Self(value))
        }
    }

    #[tokio::test]
    async fn restart_keeps_bytes_uses_lookup_codec_and_discards_l1() {
        let backend = SerializingCacheBackend::new();
        let serialized_key = InternalCacheKey::from_bytes([1; 16]);
        backend
            .insert(
                &serialized_key,
                Arc::new(StoredValue(42)),
                4,
                Some(CacheCodec::from_impl::<StoredValue>()),
            )
            .await;
        let l1_key = InternalCacheKey::from_bytes([2; 16]);
        backend.insert(&l1_key, Arc::new(7_u32), 4, None).await;

        let restarted = backend.restart();
        assert_eq!(restarted.serialized_entry_count().await, 1);
        assert_eq!(
            restarted
                .serialized_insert_count(StoredValue::TYPE_ID)
                .await,
            1
        );
        assert_eq!(restarted.l1_entry_count().await, 0);
        assert!(restarted.get(&l1_key, None).await.is_none());

        let decoded = restarted
            .get(
                &serialized_key,
                Some(CacheCodec::from_impl::<LookupValue>()),
            )
            .await
            .unwrap()
            .downcast::<LookupValue>()
            .unwrap();
        assert_eq!(*decoded, LookupValue(42));
        assert_eq!(
            restarted
                .serialized_insert_count(StoredValue::TYPE_ID)
                .await,
            1
        );
    }
}
