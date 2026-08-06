// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use async_cell::sync::AsyncCell;
use futures::Future;
use std::sync::Arc;
use tracing::Instrument;

/// An async background task whose output can be shared across threads (via cloning)
///
/// SharedPrerequisite is very similar to a shared future except:
///  * It must be created by spawning a new task (runs in the background)
///  * Shared future doesn't support Result.  This class handles errors by
///    serializing them to string.
///  * This class can optionally cache the output so that it can be accessed synchronously
pub struct SharedPrerequisite<T: Clone>(Arc<AsyncCell<std::result::Result<T, String>>>);

impl<T: Clone> SharedPrerequisite<T> {
    /// Synchronously get a cloned copy of the cached output
    ///
    /// Must be called after a call to `wait_ready`
    pub fn get_ready(&self) -> T {
        self.0
            .try_get()
            // There was no call to wait_ready and the value was accessed to early
            .expect("SharedPrerequisite cached value accessed without call to wait_ready")
            // There was no call to wait_ready and the value was actually ready, but failed
            .expect("SharedPrerequisite cached value accessed without call to wait_ready")
    }

    /// Asynchronously wait for the output to be ready
    ///
    /// Must be called before `get_ready``
    pub async fn wait_ready(&self) -> crate::Result<()> {
        self.0
            .get()
            .await
            .map(|_| ())
            .map_err(|err| crate::Error::prerequisite_failed(err))
    }

    /// Launch a background task (using tokio::spawn) and get a shareable handle to the eventual result
    pub fn spawn<F>(future: F) -> Arc<Self>
    where
        T: Clone + Send + 'static,
        F: Future<Output = crate::Result<T>> + Send + 'static,
    {
        let cell = AsyncCell::<std::result::Result<T, String>>::shared();
        let dst = cell.clone();
        tokio::spawn(
            (async move {
                let res = future.await;
                dst.set(res.map_err(|err| err.to_string()));
            })
            .in_current_span(),
        );
        Arc::new(Self(cell))
    }
}

#[cfg(test)]
mod tests {

    use std::future;

    use super::*;

    #[tokio::test]
    async fn test_spawn_prereq() {
        // On success
        let fut = future::ready(crate::Result::Ok(7_u32));
        let prereq = SharedPrerequisite::spawn(fut);

        let mut tasks = Vec::with_capacity(10);
        for _ in 0..10 {
            let instance = prereq.clone();
            tasks.push(tokio::spawn(async move {
                instance.wait_ready().await.unwrap();
                assert_eq!(instance.get_ready(), 7_u32);
            }));
        }
        for task in tasks {
            task.await.unwrap();
        }

        // On error
        let fut = future::ready(crate::Result::Err(crate::Error::invalid_input("xyz")));
        let prereq = SharedPrerequisite::<u32>::spawn(fut);

        let mut tasks = Vec::with_capacity(10);
        for _ in 0..10 {
            let instance = prereq.clone();
            tasks.push(tokio::spawn(async move {
                let err = instance.wait_ready().await.unwrap_err();
                assert!(err.to_string().contains("xyz"));
                assert!(err.to_string().contains("task failed"));
            }));
        }
        for task in tasks {
            task.await.unwrap();
        }
    }
}
