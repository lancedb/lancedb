// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

/// Define a test-only progress recorder that captures stage events in memory.
#[macro_export]
macro_rules! define_stage_event_progress {
    ($name:ident, $progress_trait:path, $result:ty) => {
        #[derive(Debug, Default)]
        struct $name {
            events: ::std::sync::Mutex<Vec<(String, String, u64)>>,
        }

        impl $name {
            fn recorded_events(&self) -> Vec<(String, String, u64)> {
                self.events
                    .lock()
                    .expect("recording progress mutex poisoned")
                    .clone()
            }
        }

        #[::async_trait::async_trait]
        impl $progress_trait for $name {
            async fn stage_start(&self, stage: &str, total: Option<u64>, _unit: &str) -> $result {
                self.events
                    .lock()
                    .expect("recording progress mutex poisoned")
                    .push(("start".to_string(), stage.to_string(), total.unwrap_or(0)));
                Ok(())
            }

            async fn stage_progress(&self, stage: &str, completed: u64) -> $result {
                self.events
                    .lock()
                    .expect("recording progress mutex poisoned")
                    .push(("progress".to_string(), stage.to_string(), completed));
                Ok(())
            }

            async fn stage_complete(&self, stage: &str) -> $result {
                self.events
                    .lock()
                    .expect("recording progress mutex poisoned")
                    .push(("complete".to_string(), stage.to_string(), 0));
                Ok(())
            }
        }
    };
}
