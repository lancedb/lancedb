// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Scalar indices are exact indices that are used to quickly satisfy a variety of filters
//! against a column of scalar values.
//!
//! Scalar indices are currently supported on numeric, string, boolean, and temporal columns.
//!
//! A scalar index will help with queries with filters like `x > 10`, `x < 10`, `x = 10`,
//! etc.  Scalar indices can also speed up prefiltering for vector searches.  A single
//! vector search with prefiltering can use both a scalar index and a vector index.

/// Builder for a btree index
///
/// A btree index is an index on scalar columns.  The index stores a copy of the column
/// in sorted order.  A header entry is created for each block of rows (currently the
/// block size is fixed at 4096).  These header entries are stored in a separate
/// cacheable structure (a btree).  To search for data the header is used to determine
/// which blocks need to be read from disk.
///
/// For example, a btree index in a table with 1Bi rows requires sizeof(Scalar) * 256Ki
/// bytes of memory and will generally need to read sizeof(Scalar) * 4096 bytes to find
/// the correct row ids.
///
/// This index is good for scalar columns with mostly distinct values and does best when
/// the query is highly selective.
///
/// The btree index does not currently have any parameters though parameters such as the
/// block size may be added in the future.
#[derive(Default, Debug, Clone)]
pub struct BTreeIndexBuilder {}

impl BTreeIndexBuilder {}

/// Builder for a Bitmap index.
///
/// It is a scalar index that stores a bitmap for each possible value
///
/// This index works best for low-cardinality (i.e., less than 1000 unique values) columns,
/// where the number of unique values is small.
/// The bitmap stores a list of row ids where the value is present.
#[derive(Debug, Clone, Default)]
pub struct BitmapIndexBuilder {}

/// Builder for LabelList index.
///
/// [LabeListIndexBuilder] is a scalar index that can be used on `List<T>` columns to
/// support queries with `array_contains_all` and `array_contains_any`
/// using an underlying bitmap index.
///
#[derive(Debug, Clone, Default)]
pub struct LabelListIndexBuilder {}

pub use lance_index::scalar::inverted::query::*;
pub use lance_index::scalar::FullTextSearchQuery;
pub use lance_index::scalar::InvertedIndexParams as FtsIndexBuilder;
pub use lance_index::scalar::InvertedIndexParams;
