// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! These utilities make it possible to mock out the "current time" when running
//! unit tests.  Anywhere in production code where we need to get the current time
//! we should use the below methods and types instead of the builtin methods and types

use chrono::{DateTime, TimeZone, Utc};
#[cfg(test)]
use mock_instant::thread_local::{SystemTime as NativeSystemTime, UNIX_EPOCH};

#[cfg(not(test))]
use std::time::{SystemTime as NativeSystemTime, UNIX_EPOCH};

pub type SystemTime = NativeSystemTime;

/// Mirror function that mimics DateTime<Utc>::now() with the exception that it
/// uses the potentially mocked system time.
pub fn utc_now() -> DateTime<Utc> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time before Unix epoch");
    let naive = DateTime::from_timestamp(now.as_secs() as i64, now.subsec_nanos())
        .expect("DateTime::from_timestamp")
        .naive_utc();
    Utc.from_utc_datetime(&naive)
}

pub fn timestamp_to_nanos(timestamp: Option<SystemTime>) -> u128 {
    let timestamp = timestamp.unwrap_or_else(SystemTime::now);
    timestamp
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_nanos()
}
