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
#[derive(Default, Debug, Clone, serde::Serialize)]
pub struct BTreeIndexBuilder {}

impl BTreeIndexBuilder {}

/// Builder for a Bitmap index.
///
/// It is a scalar index that stores a bitmap for each possible value
///
/// This index works best for low-cardinality (i.e., less than 1000 unique values) columns,
/// where the number of unique values is small.
/// The bitmap stores a list of row ids where the value is present.
#[derive(Debug, Clone, Default, serde::Serialize)]
pub struct BitmapIndexBuilder {}

/// Builder for LabelList index.
///
/// [LabeListIndexBuilder] is a scalar index that can be used on `List<T>` columns to
/// support queries with `array_contains_all` and `array_contains_any`
/// using an underlying bitmap index.
///
#[derive(Debug, Clone, Default, serde::Serialize)]
pub struct LabelListIndexBuilder {}

/// Builder for an FM-Index.
///
/// An FM-Index (Ferragina–Manzini) is a scalar index over string/binary columns
/// that accelerates substring search, i.e. `contains(col, 'needle')`. Unlike an
/// inverted (FTS) index it matches arbitrary substrings of the raw bytes rather
/// than tokenized words.
#[derive(Debug, Clone, Default, serde::Serialize)]
pub struct FmIndexBuilder {}

/// Builder for a ZoneMap index.
///
/// A ZoneMap index stores min/max summaries for ranges of rows and can
/// accelerate range predicates by pruning zones that cannot match.
///
/// ```
/// use lancedb::{
///     index::{scalar::ZoneMapIndexBuilder, Index},
///     Table,
/// };
///
/// # async fn create_zonemap_index(table: &Table) -> lancedb::Result<()> {
/// table
///     .create_index(&["timestamp"], Index::ZoneMap(ZoneMapIndexBuilder::default()))
///     .execute()
///     .await?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone, Default, serde::Serialize)]
pub struct ZoneMapIndexBuilder {}

/// Builder for an NGram index over UTF-8 strings.
///
/// This index accelerates substring, `LIKE`, and regular-expression filters.
/// It uses Lance's default trigram parameters.
///
/// ```
/// use lancedb::index::{Index, scalar::NGramIndexBuilder};
/// # async fn example(table: &lancedb::Table) -> lancedb::Result<()> {
/// table.create_index(&["text"], Index::NGram(NGramIndexBuilder::default()))
///     .execute().await?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone, Default, serde::Serialize)]
pub struct NGramIndexBuilder {}

/// Builder for a Bloom filter index on scalar values.
///
/// Bloom filters accelerate equality and membership filters by skipping groups
/// of rows that cannot match. Candidate rows are checked to remove false positives.
/// Unset parameters use Lance's defaults.
///
/// ```
/// use lancedb::index::{Index, scalar::BloomFilterIndexBuilder};
/// # async fn example(table: &lancedb::Table) -> lancedb::Result<()> {
/// let params = BloomFilterIndexBuilder::default()
///     .number_of_items(4096)?
///     .probability(0.01)?;
/// table.create_index(&["id"], Index::BloomFilter(params))
///     .execute().await?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone, Default, serde::Serialize)]
pub struct BloomFilterIndexBuilder {
    #[serde(skip_serializing_if = "Option::is_none")]
    number_of_items: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    probability: Option<f64>,
}

impl BloomFilterIndexBuilder {
    /// Set the number of rows covered by each Bloom filter.
    ///
    /// Must be greater than zero. Defaults to 8192, unless overridden by Lance's
    /// `LANCE_BLOOMFILTER_DEFAULT_NUMBER_OF_ITEMS` environment variable.
    pub fn number_of_items(mut self, number_of_items: u64) -> crate::Result<Self> {
        if number_of_items == 0 {
            return Err(crate::Error::InvalidInput {
                message: "BloomFilter number_of_items must be greater than zero".into(),
            });
        }
        self.number_of_items = Some(number_of_items);
        Ok(self)
    }

    /// Set the desired false-positive probability for each Bloom filter.
    ///
    /// Must be finite and strictly between zero and one. Lower values use more
    /// space. Defaults to 0.00057, unless overridden by Lance's
    /// `LANCE_BLOOMFILTER_DEFAULT_PROBABILITY` environment variable.
    pub fn probability(mut self, probability: f64) -> crate::Result<Self> {
        if !probability.is_finite() || probability <= 0.0 || probability >= 1.0 {
            return Err(crate::Error::InvalidInput {
                message: "BloomFilter probability must be finite and strictly between zero and one"
                    .into(),
            });
        }
        self.probability = Some(probability);
        Ok(self)
    }
}

/// Builder for an R-tree index on GeoArrow geometry columns.
///
/// This index accelerates spatial intersection filters using geometry bounding
/// boxes. Unset parameters use Lance's defaults. Native creation requires
/// the `geo` feature; remote creation requires server support.
///
/// ```
/// use lancedb::index::{Index, scalar::RTreeIndexBuilder};
/// # async fn example(table: &lancedb::Table) -> lancedb::Result<()> {
/// let params = RTreeIndexBuilder::default().page_size(1024)?;
/// table.create_index(&["geometry"], Index::RTree(params))
///     .execute().await?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone, Default, serde::Serialize)]
pub struct RTreeIndexBuilder {
    #[serde(skip_serializing_if = "Option::is_none")]
    page_size: Option<u32>,
}

impl RTreeIndexBuilder {
    /// Set the maximum number of entries in each R-tree page.
    ///
    /// Must be at least 2. Defaults to 4096.
    pub fn page_size(mut self, page_size: u32) -> crate::Result<Self> {
        if page_size < 2 {
            return Err(crate::Error::InvalidInput {
                message: "RTree page_size must be at least 2".into(),
            });
        }
        self.page_size = Some(page_size);
        Ok(self)
    }
}

pub use lance_index::scalar::FullTextSearchQuery;
pub use lance_index::scalar::InvertedIndexParams as FtsIndexBuilder;
pub use lance_index::scalar::InvertedIndexParams;
pub use lance_index::scalar::inverted::DocumentGranularity;
pub use lance_index::scalar::inverted::query::*;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scalar_index_parameters() {
        assert_eq!(
            serde_json::to_value(BloomFilterIndexBuilder::default()).unwrap(),
            serde_json::json!({})
        );
        assert_eq!(
            serde_json::to_value(RTreeIndexBuilder::default()).unwrap(),
            serde_json::json!({})
        );
        assert_eq!(
            serde_json::to_value(
                BloomFilterIndexBuilder::default()
                    .number_of_items(1)
                    .unwrap()
            )
            .unwrap(),
            serde_json::json!({"number_of_items": 1})
        );
        assert_eq!(
            serde_json::to_value(
                BloomFilterIndexBuilder::default()
                    .probability(0.01)
                    .unwrap()
            )
            .unwrap(),
            serde_json::json!({"probability": 0.01})
        );
        assert!(
            BloomFilterIndexBuilder::default()
                .number_of_items(0)
                .is_err()
        );
        for probability in [
            f64::NAN,
            f64::INFINITY,
            f64::NEG_INFINITY,
            -0.1,
            0.0,
            1.0,
            1.1,
        ] {
            assert!(
                BloomFilterIndexBuilder::default()
                    .probability(probability)
                    .is_err()
            );
        }
        for page_size in [0, 1] {
            assert!(RTreeIndexBuilder::default().page_size(page_size).is_err());
        }
        assert!(RTreeIndexBuilder::default().page_size(2).is_ok());
    }
}
