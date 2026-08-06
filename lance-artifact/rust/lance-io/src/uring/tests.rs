// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Tests for io_uring reader implementation.

use crate::object_store::ObjectStore;
use lance_core::Result;
use std::io::Write;
use std::time::Duration;
use tempfile::NamedTempFile;

/// Helper to create a temporary file with test data
fn create_test_file(size: usize) -> Result<(NamedTempFile, Vec<u8>)> {
    let mut file = NamedTempFile::new()?;
    let data: Vec<u8> = (0..size).map(|i| (i % 256) as u8).collect();
    file.write_all(&data)?;
    file.flush()?;
    Ok((file, data))
}

#[tokio::test]
async fn test_read_small_file() -> Result<()> {
    let (file, expected_data) = create_test_file(1024)?;
    let file_path = file.path().to_str().unwrap();
    let uri = format!("file+uring://{}", file_path);

    let (store, path) = ObjectStore::from_uri(&uri).await?;
    let reader = store.open(&path).await?;

    // Read entire file
    let data = reader.get_all().await.unwrap();
    assert_eq!(data.as_ref(), expected_data.as_slice());

    Ok(())
}

#[tokio::test]
async fn test_read_range() -> Result<()> {
    let (file, expected_data) = create_test_file(4096)?;
    let file_path = file.path().to_str().unwrap();
    let uri = format!("file+uring://{}", file_path);

    let (store, path) = ObjectStore::from_uri(&uri).await?;
    let reader = store.open(&path).await?;

    // Read a range in the middle
    let range = 1000..2000;
    let data = reader.get_range(range.clone()).await.unwrap();
    assert_eq!(data.as_ref(), &expected_data[range]);

    Ok(())
}

#[tokio::test]
async fn test_read_multiple_ranges() -> Result<()> {
    let (file, expected_data) = create_test_file(8192)?;
    let file_path = file.path().to_str().unwrap();
    let uri = format!("file+uring://{}", file_path);

    let (store, path) = ObjectStore::from_uri(&uri).await?;
    let reader = store.open(&path).await?;

    // Read multiple ranges
    let ranges = vec![0..100, 500..600, 2000..3000];
    for range in ranges {
        let data = reader.get_range(range.clone()).await.unwrap();
        assert_eq!(data.as_ref(), &expected_data[range]);
    }

    Ok(())
}

#[tokio::test]
async fn test_file_size() -> Result<()> {
    let size = 5000;
    let (file, _) = create_test_file(size)?;
    let file_path = file.path().to_str().unwrap();
    let uri = format!("file+uring://{}", file_path);

    let (store, path) = ObjectStore::from_uri(&uri).await?;
    let reader = store.open(&path).await?;

    assert_eq!(reader.size().await.unwrap(), size);

    Ok(())
}

#[tokio::test]
async fn test_concurrent_reads() -> Result<()> {
    let (file, expected_data) = create_test_file(16384)?;
    let file_path = file.path().to_str().unwrap();
    let uri = format!("file+uring://{}", file_path);

    let (store, path) = ObjectStore::from_uri(&uri).await?;

    // Perform multiple concurrent reads
    let mut tasks = vec![];
    for i in 0..10 {
        let reader_clone = store.open(&path).await?;
        let expected = expected_data.clone();
        tasks.push(tokio::spawn(async move {
            let range = (i * 1000)..((i + 1) * 1000);
            let data = reader_clone.get_range(range.clone()).await.unwrap();
            assert_eq!(data.as_ref(), &expected[range]);
        }));
    }

    // Wait for all tasks
    for task in tasks {
        task.await.unwrap();
    }

    Ok(())
}

#[tokio::test]
async fn test_large_file_read() -> Result<()> {
    // Test with a larger file (1MB)
    let size = 1024 * 1024;
    let (file, expected_data) = create_test_file(size)?;
    let file_path = file.path().to_str().unwrap();
    let uri = format!("file+uring://{}", file_path);

    let (store, path) = ObjectStore::from_uri(&uri).await?;
    let reader = store.open(&path).await?;

    // Read entire file
    let data = reader.get_all().await.unwrap();
    assert_eq!(data.len(), size);
    assert_eq!(data.as_ref(), expected_data.as_slice());

    Ok(())
}

#[tokio::test]
async fn test_read_edge_cases() -> Result<()> {
    let (file, expected_data) = create_test_file(4096)?;
    let file_path = file.path().to_str().unwrap();
    let uri = format!("file+uring://{}", file_path);

    let (store, path) = ObjectStore::from_uri(&uri).await?;
    let reader = store.open(&path).await?;

    // Read from start
    let data = reader.get_range(0..100).await.unwrap();
    assert_eq!(data.as_ref(), &expected_data[0..100]);

    // Read to end
    let data = reader.get_range(4000..4096).await.unwrap();
    assert_eq!(data.as_ref(), &expected_data[4000..4096]);

    // Read single byte
    let data = reader.get_range(2000..2001).await.unwrap();
    assert_eq!(data.as_ref(), &expected_data[2000..2001]);

    Ok(())
}

#[tokio::test]
async fn test_file_not_found() {
    let uri = "file+uring:///nonexistent/file.dat";
    let (store, path) = ObjectStore::from_uri(uri).await.unwrap();

    // Should fail to open non-existent file
    let result = store.open(&path).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_block_size_and_parallelism() -> Result<()> {
    let (file, _) = create_test_file(1024)?;
    let file_path = file.path().to_str().unwrap();
    let uri = format!("file+uring://{}", file_path);

    let (store, path) = ObjectStore::from_uri(&uri).await?;
    let reader = store.open(&path).await?;

    // Check default values (or configured values)
    assert!(reader.block_size() > 0);
    assert!(reader.io_parallelism() > 0);

    Ok(())
}

#[tokio::test]
async fn test_path() -> Result<()> {
    let (file, _) = create_test_file(1024)?;
    let file_path = file.path().to_str().unwrap();
    let uri = format!("file+uring://{}", file_path);

    let (store, path) = ObjectStore::from_uri(&uri).await?;
    let reader = store.open(&path).await?;

    // Verify path is preserved
    assert_eq!(reader.path(), &path);

    Ok(())
}

/// Test that reading past EOF returns an error.
///
/// This exercises the case where `known_size` passed to `open_with_size` is larger
/// than the actual file, causing io_uring to hit EOF before the full read completes.
#[tokio::test]
async fn test_short_read_get_all() -> Result<()> {
    let actual_size: usize = 8192;
    let (file, _expected_data) = create_test_file(actual_size)?;
    let file_path = file.path().to_str().unwrap();
    let uri = format!("file+uring://{}", file_path);

    let (store, path) = ObjectStore::from_uri(&uri).await?;

    // Open with inflated known_size — the reader will think the file is 2x its real size
    let inflated_size = actual_size * 2;
    let reader = store.open_with_size(&path, inflated_size).await?;

    // get_all() will submit a read for inflated_size bytes from an actual_size file.
    // The kernel reads actual_size bytes then returns 0 (EOF) — this should be an error.
    let result = reader.get_all().await;
    assert!(result.is_err(), "reading past EOF should return an error");

    Ok(())
}

/// Test that a range read extending past EOF returns an error.
#[tokio::test]
async fn test_short_read_get_range_past_eof() -> Result<()> {
    let actual_size: usize = 8192;
    let (file, _expected_data) = create_test_file(actual_size)?;
    let file_path = file.path().to_str().unwrap();
    let uri = format!("file+uring://{}", file_path);

    let (store, path) = ObjectStore::from_uri(&uri).await?;
    let reader = store.open(&path).await?;

    // Request a range that starts inside the file but extends past EOF.
    // File is 8192 bytes; reading 4096..16384 hits EOF — this should be an error.
    let range_start = 4096;
    let range_end = actual_size * 2; // 16384, well past EOF
    let result = reader.get_range(range_start..range_end).await;
    assert!(
        result.is_err(),
        "range extending past EOF should return an error"
    );

    Ok(())
}

/// Test that when push_to_sq fails (SQ full), the request's future returns
/// an error instead of hanging forever.
///
/// This directly tests the thread-path scenario: create an IoUring with
/// queue_depth=2, fill the SQ, then try to push a 3rd request. The 3rd
/// request's future should return an error within the timeout.
///
/// BUG: currently the failed push silently drops the request, so the
/// future hangs and the timeout fires.
#[tokio::test]
async fn test_retry_sq_full_thread() -> Result<()> {
    use super::future::UringReadFuture;
    use super::requests::{IoRequest, RequestState};
    use super::thread::push_to_sq;
    use bytes::BytesMut;
    use io_uring::IoUring;
    use std::collections::HashMap;
    use std::os::unix::io::AsRawFd;
    use std::sync::{Arc, Mutex};

    let (file, _) = create_test_file(4096)?;
    let fd = file.as_file().as_raw_fd();

    // Create a tiny ring with queue_depth=2
    let mut ring = IoUring::new(2).unwrap();
    let mut pending: HashMap<u64, Arc<IoRequest>> = HashMap::new();

    // Helper to create a request
    let make_request = || {
        Arc::new(IoRequest {
            fd,
            offset: 0,
            length: 4096,
            thread_id: std::thread::current().id(),
            state: Mutex::new(RequestState {
                completed: false,
                waker: None,
                err: None,
                buffer: BytesMut::zeroed(4096),
                bytes_read: 0,
            }),
        })
    };

    // Fill the SQ (capacity=2)
    let _r1 = make_request();
    let _r2 = make_request();
    push_to_sq(&mut ring, &mut pending, _r1).unwrap();
    push_to_sq(&mut ring, &mut pending, _r2).unwrap();

    // 3rd push should fail — SQ is full
    let r3 = make_request();
    let push_result = push_to_sq(&mut ring, &mut pending, r3.clone());
    assert!(push_result.is_err(), "3rd push should fail (SQ full)");

    // r3's future should return an error, not hang forever.
    // BUG: currently nobody sets completed=true or err on r3, so the future hangs.
    let future = UringReadFuture { request: r3 };
    let result = tokio::time::timeout(Duration::from_secs(2), future).await;
    assert!(
        result.is_ok(),
        "future timed out — request was dropped without error on SQ-full push failure"
    );

    Ok(())
}

/// Test that when push_to_sq fails (SQ full) on the current-thread path,
/// the request's future returns an error instead of hanging forever.
///
/// Uses UringCurrentThreadFuture (which will be a no-op poller since the
/// thread-local URING has no knowledge of this request) after push_to_sq
/// has already completed the request with an error.
#[tokio::test(flavor = "current_thread")]
async fn test_retry_sq_full_current_thread() -> Result<()> {
    use super::current_thread_future::UringCurrentThreadFuture;
    use super::requests::{IoRequest, RequestState};
    use super::thread::push_to_sq;
    use bytes::BytesMut;
    use io_uring::IoUring;
    use std::collections::HashMap;
    use std::os::unix::io::AsRawFd;
    use std::sync::{Arc, Mutex};

    let (file, _) = create_test_file(4096)?;
    let fd = file.as_file().as_raw_fd();

    // Create a tiny ring with queue_depth=2
    let mut ring = IoUring::new(2).unwrap();
    let mut pending: HashMap<u64, Arc<IoRequest>> = HashMap::new();

    let make_request = || {
        Arc::new(IoRequest {
            fd,
            offset: 0,
            length: 4096,
            thread_id: std::thread::current().id(),
            state: Mutex::new(RequestState {
                completed: false,
                waker: None,
                err: None,
                buffer: BytesMut::zeroed(4096),
                bytes_read: 0,
            }),
        })
    };

    // Fill the SQ (capacity=2)
    push_to_sq(&mut ring, &mut pending, make_request()).unwrap();
    push_to_sq(&mut ring, &mut pending, make_request()).unwrap();

    // 3rd push should fail — SQ is full
    let r3 = make_request();
    let push_result = push_to_sq(&mut ring, &mut pending, r3.clone());
    assert!(push_result.is_err(), "3rd push should fail (SQ full)");

    // r3's future should return an error, not hang forever.
    let future = UringCurrentThreadFuture::new(r3);
    let result = tokio::time::timeout(Duration::from_secs(2), future).await;
    assert!(
        result.is_ok(),
        "future timed out — request was dropped without error on SQ-full push failure"
    );

    Ok(())
}

#[tokio::test]
async fn test_uring_not_enabled_with_file_scheme() -> Result<()> {
    // Verify that files opened with file:// don't use uring
    let (file, expected_data) = create_test_file(1024)?;
    let file_path = file.path().to_str().unwrap();
    // Use regular file:// scheme, should NOT use uring
    let uri = format!("file://{}", file_path);

    let (store, path) = ObjectStore::from_uri(&uri).await?;
    let reader = store.open(&path).await?;

    // Should still be able to read, just won't use uring
    let data = reader.get_all().await.unwrap();
    assert_eq!(data.as_ref(), expected_data.as_slice());

    Ok(())
}
