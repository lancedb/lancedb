// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Test helper that wraps an object store and injects failures for specific
//! method/path combinations. Used to exercise cleanup paths in write code that
//! handle storage failures after partial data files have been written.

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

use lance_core::utils::testing::{ProxyObjectStore, ProxyObjectStorePolicy};
use lance_io::object_store::WrappingObjectStore;

/// Wraps the object store so that calls matching a configured method+path
/// substring return a synthetic error.
#[derive(Debug)]
pub struct FailingProxyStore {
    policy: Arc<Mutex<ProxyObjectStorePolicy>>,
}

impl Default for FailingProxyStore {
    fn default() -> Self {
        Self::new()
    }
}

impl FailingProxyStore {
    pub fn new() -> Self {
        Self {
            policy: Arc::new(Mutex::new(ProxyObjectStorePolicy::new())),
        }
    }

    /// Fail calls to `method` on paths containing `path_substr` only after
    /// the first `skip` matching calls have passed through. Useful when an
    /// earlier write to the same prefix must succeed (e.g. `reserve_fragment_ids`)
    /// before we want a later write to fail.
    pub fn fail_after_n(
        &self,
        method: &'static str,
        path_substr: &'static str,
        skip: usize,
        error_message: &'static str,
    ) {
        let mut policy = self.policy.lock().unwrap();
        let counter = Arc::new(AtomicUsize::new(0));
        let policy_name = format!("fail_after_{}_{}_{}", method, path_substr, skip);
        policy.set_before_policy(
            Box::leak(policy_name.into_boxed_str()),
            Arc::new(move |called_method, path| {
                if called_method == method && path.as_ref().contains(path_substr) {
                    let count = counter.fetch_add(1, Ordering::SeqCst);
                    if count >= skip {
                        return Err(object_store::Error::Generic {
                            store: "FailingProxyStore",
                            source: error_message.into(),
                        }
                        .into());
                    }
                }
                Ok(())
            }),
        );
    }

    /// Fail every call to `method` whose path contains `path_substr` with a
    /// generic store error carrying `error_message`.
    pub fn fail_when(
        &self,
        method: &'static str,
        path_substr: &'static str,
        error_message: &'static str,
    ) {
        let mut policy = self.policy.lock().unwrap();
        let policy_name = format!("fail_{}_{}", method, path_substr);
        policy.set_before_policy(
            Box::leak(policy_name.into_boxed_str()),
            Arc::new(move |called_method, path| {
                if called_method == method && path.as_ref().contains(path_substr) {
                    Err(object_store::Error::Generic {
                        store: "FailingProxyStore",
                        source: error_message.into(),
                    }
                    .into())
                } else {
                    Ok(())
                }
            }),
        );
    }

    /// Stop failing calls configured by [`Self::fail_when`].
    pub fn clear_fail_when(&self, method: &str, path_substr: &str) {
        let mut policy = self.policy.lock().unwrap();
        policy.clear_before_policy(&format!("fail_{}_{}", method, path_substr));
    }
}

impl WrappingObjectStore for FailingProxyStore {
    fn wrap(
        &self,
        _storage_prefix: &str,
        original: Arc<dyn object_store::ObjectStore>,
    ) -> Arc<dyn object_store::ObjectStore> {
        Arc::new(ProxyObjectStore::new(original, self.policy.clone()))
    }
}
