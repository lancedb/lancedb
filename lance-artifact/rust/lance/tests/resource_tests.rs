// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

// The memory tests don't work currently on MacOS because they rely on thread
// local storage in the allocator, which seems to have some issues on MacOS.
#[cfg(target_os = "linux")]
mod resource_test;
