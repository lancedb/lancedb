// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Unstable integration contracts used by LanceDB Enterprise components.
//!
//! Nothing in this module is a supported SDK surface. Types and functions may
//! change without a semver-major release.

pub mod fragment_publication {
    pub use crate::table::fragment_publication::{
        CommitOutcome, CommitReceipt, FragmentInputBinding, FragmentOutputBinding,
        FragmentPublicationBasis, FragmentPublicationOptions, FragmentPublisher,
    };
}
