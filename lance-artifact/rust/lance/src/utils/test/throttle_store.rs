// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::sync::Arc;

use lance_io::object_store::WrappingObjectStore;
use object_store::{
    ObjectStore,
    throttle::{ThrottleConfig, ThrottledStore},
};

#[derive(Debug, Clone, Default)]
pub struct ThrottledStoreWrapper {
    pub config: ThrottleConfig,
}

impl WrappingObjectStore for ThrottledStoreWrapper {
    fn wrap(&self, _prefix: &str, original: Arc<dyn ObjectStore>) -> Arc<dyn ObjectStore> {
        let throttle_store = ThrottledStore::new(original, self.config);
        Arc::new(throttle_store)
    }
}
