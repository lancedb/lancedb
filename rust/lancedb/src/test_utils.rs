// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

pub mod connection;
pub mod datagen;
pub mod embeddings;

#[derive(Debug)]
pub struct TestCustomError;

impl std::fmt::Display for TestCustomError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "TestCustomError occurred")
    }
}

impl std::error::Error for TestCustomError {}
