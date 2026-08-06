// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Key existence tracking for merge insert conflict detection.

use std::collections::HashSet;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use arrow_array::cast::AsArray;
use arrow_array::{
    Array, BinaryArray, LargeBinaryArray, LargeListArray, LargeStringArray, ListArray, RecordBatch,
    StringArray, StructArray,
};
use arrow_schema::DataType;
use lance_core::Result;
use lance_core::deepsize::DeepSizeOf;
use lance_core::utils::bloomfilter::sbbf::{Sbbf, SbbfBuilder};
use lance_table::format::pb;

// Default bloom filter config: 8192 items @ 0.00057 fpp -> 16KiB filter
pub const BLOOM_FILTER_DEFAULT_NUMBER_OF_ITEMS: u64 = 8192;
pub const BLOOM_FILTER_DEFAULT_PROBABILITY: f64 = 0.00057;

/// Key value for conflict detection.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum KeyValue {
    String(String),
    Int64(i64),
    UInt64(u64),
    Binary(Vec<u8>),
    List(Vec<Self>),
    Struct(Vec<Self>),
    Composite(Vec<Self>),
}

impl KeyValue {
    pub fn to_bytes(&self) -> Vec<u8> {
        match self {
            Self::String(s) => s.as_bytes().to_vec(),
            Self::Int64(i) => i.to_le_bytes().to_vec(),
            Self::UInt64(u) => u.to_le_bytes().to_vec(),
            Self::Binary(b) => b.clone(),
            Self::List(values) | Self::Struct(values) | Self::Composite(values) => {
                let mut result = Vec::new();
                for value in values {
                    result.extend_from_slice(&value.to_bytes());
                    result.push(0);
                }
                result
            }
        }
    }

    pub fn hash_value(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        self.to_bytes().hash(&mut hasher);
        hasher.finish()
    }
}

/// Builder for KeyExistenceFilter using Split Block Bloom Filter.
#[derive(Debug, Clone)]
pub struct KeyExistenceFilterBuilder {
    sbbf: Sbbf,
    field_ids: Vec<i32>,
    item_count: usize,
}

impl KeyExistenceFilterBuilder {
    pub fn new(field_ids: Vec<i32>) -> Self {
        let sbbf = SbbfBuilder::new()
            .expected_items(BLOOM_FILTER_DEFAULT_NUMBER_OF_ITEMS)
            .false_positive_probability(BLOOM_FILTER_DEFAULT_PROBABILITY)
            .build()
            .expect("Failed to build SBBF");
        Self {
            sbbf,
            field_ids,
            item_count: 0,
        }
    }

    pub fn insert(&mut self, key: KeyValue) -> Result<()> {
        self.sbbf.insert(&key.to_bytes()[..]);
        self.item_count += 1;
        Ok(())
    }

    pub fn contains(&self, key: &KeyValue) -> bool {
        self.sbbf.check(&key.to_bytes()[..])
    }

    pub fn might_intersect(&self, other: &Self) -> Result<bool> {
        self.sbbf
            .might_intersect(&other.sbbf)
            .map_err(|e| lance_core::Error::invalid_input(e.to_string()))
    }

    pub fn field_ids(&self) -> &[i32] {
        &self.field_ids
    }

    pub fn estimated_size_bytes(&self) -> usize {
        self.sbbf.size_bytes()
    }

    pub fn len(&self) -> usize {
        self.item_count
    }

    pub fn is_empty(&self) -> bool {
        self.item_count == 0
    }

    pub fn build(&self) -> KeyExistenceFilter {
        KeyExistenceFilter {
            field_ids: self.field_ids.clone(),
            filter: FilterType::Bloom {
                bitmap: self.sbbf.to_bytes(),
                num_bits: (self.sbbf.size_bytes() as u32) * 8,
                number_of_items: BLOOM_FILTER_DEFAULT_NUMBER_OF_ITEMS,
                probability: BLOOM_FILTER_DEFAULT_PROBABILITY,
            },
        }
    }
}

impl From<&KeyExistenceFilterBuilder> for pb::transaction::KeyExistenceFilter {
    fn from(builder: &KeyExistenceFilterBuilder) -> Self {
        Self {
            field_ids: builder.field_ids.clone(),
            data: Some(pb::transaction::key_existence_filter::Data::Bloom(
                pb::transaction::BloomFilter {
                    bitmap: builder.sbbf.to_bytes(),
                    num_bits: (builder.sbbf.size_bytes() as u32) * 8,
                    number_of_items: BLOOM_FILTER_DEFAULT_NUMBER_OF_ITEMS,
                    probability: BLOOM_FILTER_DEFAULT_PROBABILITY,
                },
            )),
        }
    }
}

/// Filter type for key existence data.
#[derive(Debug, Clone, DeepSizeOf, PartialEq)]
pub enum FilterType {
    ExactSet(HashSet<u64>),
    Bloom {
        bitmap: Vec<u8>,
        num_bits: u32,
        number_of_items: u64,
        probability: f64,
    },
}

/// Tracks keys of inserted rows for conflict detection.
/// Only created when ON columns match the schema's unenforced primary key.
#[derive(Debug, Clone, DeepSizeOf, PartialEq)]
pub struct KeyExistenceFilter {
    pub field_ids: Vec<i32>,
    pub filter: FilterType,
}

impl KeyExistenceFilter {
    pub fn from_bloom_filter(bloom: &KeyExistenceFilterBuilder) -> Self {
        bloom.build()
    }

    /// Check if two filters intersect. Returns (has_intersection, might_be_false_positive).
    /// Errors if bloom filter configs don't match.
    pub fn intersects(&self, other: &Self) -> Result<(bool, bool)> {
        match (&self.filter, &other.filter) {
            (FilterType::ExactSet(a), FilterType::ExactSet(b)) => {
                Ok((a.iter().any(|h| b.contains(h)), false))
            }
            (FilterType::ExactSet(_), FilterType::Bloom { .. })
            | (FilterType::Bloom { .. }, FilterType::ExactSet(_)) => {
                // Can't compare different hash schemes, assume intersection
                Ok((true, true))
            }
            (
                FilterType::Bloom {
                    bitmap: a_bits,
                    number_of_items: a_num_items,
                    probability: a_prob,
                    ..
                },
                FilterType::Bloom {
                    bitmap: b_bits,
                    number_of_items: b_num_items,
                    probability: b_prob,
                    ..
                },
            ) => {
                if a_num_items != b_num_items || (a_prob - b_prob).abs() > f64::EPSILON {
                    return Err(lance_core::Error::invalid_input(format!(
                        "Bloom filter config mismatch: ({}, {}) vs ({}, {})",
                        a_num_items, a_prob, b_num_items, b_prob
                    )));
                }
                let has = Sbbf::bytes_might_intersect(a_bits, b_bits)
                    .map_err(|e| lance_core::Error::invalid_input(e.to_string()))?;
                Ok((has, has))
            }
        }
    }
}

impl From<&KeyExistenceFilter> for pb::transaction::KeyExistenceFilter {
    fn from(filter: &KeyExistenceFilter) -> Self {
        match &filter.filter {
            FilterType::ExactSet(hashes) => Self {
                field_ids: filter.field_ids.clone(),
                data: Some(pb::transaction::key_existence_filter::Data::Exact(
                    pb::transaction::ExactKeySetFilter {
                        key_hashes: hashes.iter().copied().collect(),
                    },
                )),
            },
            FilterType::Bloom {
                bitmap,
                num_bits,
                number_of_items,
                probability,
            } => Self {
                field_ids: filter.field_ids.clone(),
                data: Some(pb::transaction::key_existence_filter::Data::Bloom(
                    pb::transaction::BloomFilter {
                        bitmap: bitmap.clone(),
                        num_bits: *num_bits,
                        number_of_items: *number_of_items,
                        probability: *probability,
                    },
                )),
            },
        }
    }
}

impl TryFrom<&pb::transaction::KeyExistenceFilter> for KeyExistenceFilter {
    type Error = lance_core::Error;

    fn try_from(message: &pb::transaction::KeyExistenceFilter) -> Result<Self> {
        let filter = match message.data.as_ref() {
            Some(pb::transaction::key_existence_filter::Data::Exact(exact)) => {
                FilterType::ExactSet(exact.key_hashes.iter().copied().collect())
            }
            Some(pb::transaction::key_existence_filter::Data::Bloom(b)) => {
                // Use defaults for backwards compatibility
                let number_of_items = if b.number_of_items == 0 {
                    BLOOM_FILTER_DEFAULT_NUMBER_OF_ITEMS
                } else {
                    b.number_of_items
                };
                let probability = if b.probability == 0.0 {
                    BLOOM_FILTER_DEFAULT_PROBABILITY
                } else {
                    b.probability
                };
                FilterType::Bloom {
                    bitmap: b.bitmap.clone(),
                    num_bits: b.num_bits,
                    number_of_items,
                    probability,
                }
            }
            None => FilterType::ExactSet(HashSet::new()),
        };
        Ok(Self {
            field_ids: message.field_ids.clone(),
            filter,
        })
    }
}

/// Extract key value from a batch row. Returns None if null or unsupported type.
pub fn extract_key_value_from_batch(
    batch: &RecordBatch,
    row_idx: usize,
    on_columns: &[String],
) -> Option<KeyValue> {
    let mut parts: Vec<KeyValue> = Vec::with_capacity(on_columns.len());

    for col_name in on_columns {
        let (col_idx, _) = batch.schema().column_with_name(col_name)?;
        let column = batch.column(col_idx);

        if column.is_null(row_idx) {
            return None;
        }

        let key_part = extract_key_value(column, row_idx)?;
        parts.push(key_part);
    }

    if parts.is_empty() {
        None
    } else if parts.len() == 1 {
        Some(parts.into_iter().next().unwrap())
    } else {
        Some(KeyValue::Composite(parts))
    }
}

fn extract_key_value(array: &dyn Array, row_idx: usize) -> Option<KeyValue> {
    let v = match array.data_type() {
        DataType::Utf8 => {
            let arr = array.as_any().downcast_ref::<StringArray>()?;
            KeyValue::String(arr.value(row_idx).to_string())
        }
        DataType::LargeUtf8 => {
            let arr = array.as_any().downcast_ref::<LargeStringArray>()?;
            KeyValue::String(arr.value(row_idx).to_string())
        }
        DataType::UInt64 => {
            let arr = array.as_primitive::<arrow_array::types::UInt64Type>();
            KeyValue::UInt64(arr.value(row_idx))
        }
        DataType::Int64 => {
            let arr = array.as_primitive::<arrow_array::types::Int64Type>();
            KeyValue::Int64(arr.value(row_idx))
        }
        DataType::UInt32 => {
            let arr = array.as_primitive::<arrow_array::types::UInt32Type>();
            KeyValue::UInt64(arr.value(row_idx) as u64)
        }
        DataType::Int32 => {
            let arr = array.as_primitive::<arrow_array::types::Int32Type>();
            KeyValue::Int64(arr.value(row_idx) as i64)
        }
        DataType::Binary => {
            let arr = array.as_any().downcast_ref::<BinaryArray>()?;
            KeyValue::Binary(arr.value(row_idx).to_vec())
        }
        DataType::LargeBinary => {
            let arr = array.as_any().downcast_ref::<LargeBinaryArray>()?;
            KeyValue::Binary(arr.value(row_idx).to_vec())
        }
        DataType::List(_) => {
            let list_array = array.as_any().downcast_ref::<ListArray>().unwrap();
            let values = list_array.value(row_idx);

            let mut elements = Vec::with_capacity(values.len());
            for i in 0..values.len() {
                if values.is_null(i) {
                    return None;
                }
                let element = extract_key_value(&values, i)?;
                elements.push(element);
            }
            KeyValue::List(elements)
        }
        DataType::LargeList(_) => {
            let list_array = array.as_any().downcast_ref::<LargeListArray>().unwrap();
            let values = list_array.value(row_idx);

            let mut elements = Vec::with_capacity(values.len());
            for i in 0..values.len() {
                if values.is_null(i) {
                    return None;
                }
                let element = extract_key_value(&values, i)?;
                elements.push(element);
            }
            KeyValue::List(elements)
        }
        DataType::Struct(_) => {
            let struct_array = array.as_any().downcast_ref::<StructArray>()?;
            let mut elements = Vec::with_capacity(struct_array.num_columns());
            for i in 0..struct_array.num_columns() {
                let child = struct_array.column(i);
                if child.is_null(row_idx) {
                    return None;
                }
                let field_value = extract_key_value(child.as_ref(), row_idx)?;
                elements.push(field_value);
            }
            KeyValue::Struct(elements)
        }
        _ => return None,
    };
    Some(v)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use arrow_array::builder::{Int32Builder, ListBuilder, StringBuilder};
    use arrow_array::{Int32Array, RecordBatch, StringArray, StructArray};
    use arrow_schema::{Field, Schema};

    #[test]
    fn test_extract_key_value_from_batch_list_int() {
        let values_builder = Int32Builder::new();
        let mut list_builder = ListBuilder::new(values_builder);

        list_builder.append_value([Some(1), Some(2)]);
        list_builder.append_value([Some(3), Some(4), Some(5)]);

        let list_array = list_builder.finish();

        let schema = Arc::new(Schema::new(vec![Field::new(
            "id",
            list_array.data_type().clone(),
            false,
        )]));

        let batch = RecordBatch::try_new(schema, vec![Arc::new(list_array)])
            .expect("batch should be valid");

        let key0 = extract_key_value_from_batch(&batch, 0, &[String::from("id")])
            .expect("first row should produce a key");
        let key1 = extract_key_value_from_batch(&batch, 1, &[String::from("id")])
            .expect("second row should produce a key");

        match &key0 {
            KeyValue::List(values) => {
                assert_eq!(values.len(), 2);
                assert_eq!(values[0], KeyValue::Int64(1));
                assert_eq!(values[1], KeyValue::Int64(2));
            }
            other => panic!("expected list key, got {:?}", other),
        }

        match &key1 {
            KeyValue::List(values) => {
                assert_eq!(values.len(), 3);
                assert_eq!(values[0], KeyValue::Int64(3));
                assert_eq!(values[1], KeyValue::Int64(4));
                assert_eq!(values[2], KeyValue::Int64(5));
            }
            other => panic!("expected list key, got {:?}", other),
        }

        assert_ne!(
            key0.hash_value(),
            key1.hash_value(),
            "different list values should hash differently",
        );
    }

    #[test]
    fn test_extract_key_value_from_batch_empty_list() {
        let values_builder = Int32Builder::new();
        let mut list_builder = ListBuilder::new(values_builder);

        list_builder.append_value(std::iter::empty::<Option<i32>>());

        let list_array = list_builder.finish();

        let schema = Arc::new(Schema::new(vec![Field::new(
            "id",
            list_array.data_type().clone(),
            false,
        )]));

        let batch = RecordBatch::try_new(schema, vec![Arc::new(list_array)])
            .expect("batch should be valid");

        let key = extract_key_value_from_batch(&batch, 0, &[String::from("id")])
            .expect("empty list should still produce a key");

        match key {
            KeyValue::List(values) => {
                assert!(values.is_empty(), "expected empty list");
            }
            other => panic!("expected list key, got {:?}", other),
        }
    }

    #[test]
    fn test_extract_key_value_from_batch_list_utf8() {
        let values_builder = StringBuilder::new();
        let mut list_builder = ListBuilder::new(values_builder);

        list_builder.append_value([Some("a"), Some("bc")]);
        list_builder.append_value([Some("de")]);

        let list_array = list_builder.finish();

        let schema = Arc::new(Schema::new(vec![Field::new(
            "id",
            list_array.data_type().clone(),
            false,
        )]));

        let batch = RecordBatch::try_new(schema, vec![Arc::new(list_array)])
            .expect("batch should be valid");

        let key0 = extract_key_value_from_batch(&batch, 0, &[String::from("id")])
            .expect("first row should produce a key");
        let key1 = extract_key_value_from_batch(&batch, 1, &[String::from("id")])
            .expect("second row should produce a key");

        match &key0 {
            KeyValue::List(values) => {
                assert_eq!(values.len(), 2);
                assert_eq!(values[0], KeyValue::String("a".to_string()));
                assert_eq!(values[1], KeyValue::String("bc".to_string()));
            }
            other => panic!("expected list key, got {:?}", other),
        }

        match &key1 {
            KeyValue::List(values) => {
                assert_eq!(values.len(), 1);
                assert_eq!(values[0], KeyValue::String("de".to_string()));
            }
            other => panic!("expected list key, got {:?}", other),
        }

        assert_ne!(
            key0.hash_value(),
            key1.hash_value(),
            "different list values should hash differently",
        );
    }

    #[test]
    fn test_extract_key_value_from_batch_list_with_null_child() {
        let values_builder = Int32Builder::new();
        let mut list_builder = ListBuilder::new(values_builder);

        list_builder.append_value([Some(1), Some(2)]);
        list_builder.append_value([Some(3), None]);

        let list_array = list_builder.finish();

        let schema = Arc::new(Schema::new(vec![Field::new(
            "id",
            list_array.data_type().clone(),
            false,
        )]));

        let batch = RecordBatch::try_new(schema, vec![Arc::new(list_array)])
            .expect("batch should be valid");

        let key0 = extract_key_value_from_batch(&batch, 0, &[String::from("id")])
            .expect("first row should produce a key");
        let key1 = extract_key_value_from_batch(&batch, 1, &[String::from("id")]);

        match &key0 {
            KeyValue::List(values) => {
                assert_eq!(values.len(), 2);
                assert_eq!(values[0], KeyValue::Int64(1));
                assert_eq!(values[1], KeyValue::Int64(2));
            }
            other => panic!("expected list key, got {:?}", other),
        }

        assert!(
            key1.is_none(),
            "list row with a null child should not produce a key",
        );
    }

    #[test]
    fn test_extract_key_value_from_batch_struct_int() {
        let a_values = Int32Array::from(vec![1, 3]);
        let b_values = Int32Array::from(vec![2, 4]);

        let struct_array = StructArray::from(vec![
            (
                Arc::new(Field::new("a", arrow_schema::DataType::Int32, false)),
                Arc::new(a_values) as Arc<dyn arrow_array::Array>,
            ),
            (
                Arc::new(Field::new("b", arrow_schema::DataType::Int32, false)),
                Arc::new(b_values) as Arc<dyn arrow_array::Array>,
            ),
        ]);

        let schema = Arc::new(Schema::new(vec![Field::new(
            "id",
            struct_array.data_type().clone(),
            false,
        )]));

        let batch = RecordBatch::try_new(schema, vec![Arc::new(struct_array)])
            .expect("batch should be valid");

        let key0 = extract_key_value_from_batch(&batch, 0, &[String::from("id")])
            .expect("first row should produce a key");
        let key1 = extract_key_value_from_batch(&batch, 1, &[String::from("id")])
            .expect("second row should produce a key");

        match &key0 {
            KeyValue::Struct(values) => {
                assert_eq!(values.len(), 2);
                assert_eq!(values[0], KeyValue::Int64(1));
                assert_eq!(values[1], KeyValue::Int64(2));
            }
            other => panic!("expected struct key, got {:?}", other),
        }

        match &key1 {
            KeyValue::Struct(values) => {
                assert_eq!(values.len(), 2);
                assert_eq!(values[0], KeyValue::Int64(3));
                assert_eq!(values[1], KeyValue::Int64(4));
            }
            other => panic!("expected struct key, got {:?}", other),
        }

        assert_ne!(
            key0.hash_value(),
            key1.hash_value(),
            "different struct values should hash differently",
        );
    }

    #[test]
    fn test_extract_key_value_from_batch_struct_utf8() {
        let first_names = StringArray::from(vec!["alice", "bob"]);
        let last_names = StringArray::from(vec!["smith", "jones"]);

        let struct_array = StructArray::from(vec![
            (
                Arc::new(Field::new("first", arrow_schema::DataType::Utf8, false)),
                Arc::new(first_names) as Arc<dyn arrow_array::Array>,
            ),
            (
                Arc::new(Field::new("last", arrow_schema::DataType::Utf8, false)),
                Arc::new(last_names) as Arc<dyn arrow_array::Array>,
            ),
        ]);

        let schema = Arc::new(Schema::new(vec![Field::new(
            "id",
            struct_array.data_type().clone(),
            false,
        )]));

        let batch = RecordBatch::try_new(schema, vec![Arc::new(struct_array)])
            .expect("batch should be valid");

        let key0 = extract_key_value_from_batch(&batch, 0, &[String::from("id")])
            .expect("first row should produce a key");
        let key1 = extract_key_value_from_batch(&batch, 1, &[String::from("id")])
            .expect("second row should produce a key");

        match &key0 {
            KeyValue::Struct(values) => {
                assert_eq!(values.len(), 2);
                assert_eq!(values[0], KeyValue::String("alice".to_string()));
                assert_eq!(values[1], KeyValue::String("smith".to_string()));
            }
            other => panic!("expected struct key, got {:?}", other),
        }

        match &key1 {
            KeyValue::Struct(values) => {
                assert_eq!(values.len(), 2);
                assert_eq!(values[0], KeyValue::String("bob".to_string()));
                assert_eq!(values[1], KeyValue::String("jones".to_string()));
            }
            other => panic!("expected struct key, got {:?}", other),
        }

        assert_ne!(
            key0.hash_value(),
            key1.hash_value(),
            "different struct values should hash differently",
        );
    }

    #[test]
    fn test_extract_key_value_from_batch_struct_with_null_child() {
        let a_values = Int32Array::from(vec![Some(1), None]);
        let b_values = Int32Array::from(vec![Some(2), Some(3)]);

        let struct_array = StructArray::from(vec![
            (
                Arc::new(Field::new("a", arrow_schema::DataType::Int32, true)),
                Arc::new(a_values) as Arc<dyn arrow_array::Array>,
            ),
            (
                Arc::new(Field::new("b", arrow_schema::DataType::Int32, true)),
                Arc::new(b_values) as Arc<dyn arrow_array::Array>,
            ),
        ]);

        let schema = Arc::new(Schema::new(vec![Field::new(
            "id",
            struct_array.data_type().clone(),
            false,
        )]));

        let batch = RecordBatch::try_new(schema, vec![Arc::new(struct_array)])
            .expect("batch should be valid");

        let key0 = extract_key_value_from_batch(&batch, 0, &[String::from("id")])
            .expect("first row should produce a key");
        let key1 = extract_key_value_from_batch(&batch, 1, &[String::from("id")]);

        match &key0 {
            KeyValue::Struct(values) => {
                assert_eq!(values.len(), 2);
                assert_eq!(values[0], KeyValue::Int64(1));
                assert_eq!(values[1], KeyValue::Int64(2));
            }
            other => panic!("expected struct key, got {:?}", other),
        }

        assert!(
            key1.is_none(),
            "struct row with a null child should not produce a key",
        );
    }
}
