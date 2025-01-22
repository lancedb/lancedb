// Copyright 2024 Lance Developers.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Core utilities for LanceDB
//!
//! This crate contains the core utilities used by LanceDB.  It is not intended to be used directly by
//! users of LanceDB.  Instead, it is used by LanceDB itself.
//!
//! ## Error Handling
//!
//! The `error` module contains the error type used by LanceDB.  This error type is used throughout LanceDB
//! and is returned by all public functions.

use std::fmt::Display;

use lance_linalg::distance::DistanceType as LanceDistanceType;
use serde::{Deserialize, Serialize};

pub mod arrow;
pub mod error;
#[cfg(feature = "polars")]
pub mod polars_arrow_convertors;
#[cfg(feature = "remote")]
pub mod remote;
pub mod tabledef;

#[derive(Debug, Copy, Clone, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
#[serde(rename_all = "lowercase")]
pub enum DistanceType {
    /// Euclidean distance. This is a very common distance metric that
    /// accounts for both magnitude and direction when determining the distance
    /// between vectors. L2 distance has a range of [0, ∞).
    L2,
    /// Cosine distance.  Cosine distance is a distance metric
    /// calculated from the cosine similarity between two vectors. Cosine
    /// similarity is a measure of similarity between two non-zero vectors of an
    /// inner product space. It is defined to equal the cosine of the angle
    /// between them.  Unlike L2, the cosine distance is not affected by the
    /// magnitude of the vectors.  Cosine distance has a range of [0, 2].
    ///
    /// Note: the cosine distance is undefined when one (or both) of the vectors
    /// are all zeros (there is no direction).  These vectors are invalid and may
    /// never be returned from a vector search.
    Cosine,
    /// Dot product. Dot distance is the dot product of two vectors. Dot
    /// distance has a range of (-∞, ∞). If the vectors are normalized (i.e. their
    /// L2 norm is 1), then dot distance is equivalent to the cosine distance.
    Dot,
    /// Hamming distance. Hamming distance is a distance metric that measures
    /// the number of positions at which the corresponding elements are different.
    Hamming,
}

impl Default for DistanceType {
    fn default() -> Self {
        Self::L2
    }
}

impl From<DistanceType> for LanceDistanceType {
    fn from(value: DistanceType) -> Self {
        match value {
            DistanceType::L2 => Self::L2,
            DistanceType::Cosine => Self::Cosine,
            DistanceType::Dot => Self::Dot,
            DistanceType::Hamming => Self::Hamming,
        }
    }
}

impl From<LanceDistanceType> for DistanceType {
    fn from(value: LanceDistanceType) -> Self {
        match value {
            LanceDistanceType::L2 => Self::L2,
            LanceDistanceType::Cosine => Self::Cosine,
            LanceDistanceType::Dot => Self::Dot,
            LanceDistanceType::Hamming => Self::Hamming,
        }
    }
}

impl<'a> TryFrom<&'a str> for DistanceType {
    type Error = <LanceDistanceType as TryFrom<&'a str>>::Error;

    fn try_from(value: &str) -> std::prelude::v1::Result<Self, Self::Error> {
        LanceDistanceType::try_from(value).map(Self::from)
    }
}

impl Display for DistanceType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        LanceDistanceType::from(*self).fmt(f)
    }
}
