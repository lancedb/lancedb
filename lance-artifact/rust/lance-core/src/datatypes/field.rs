// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Lance Schema Field

use std::{
    cmp::{Ordering, max},
    collections::{HashMap, VecDeque},
    fmt,
    sync::Arc,
};

use crate::deepsize::DeepSizeOf;
use arrow_array::{
    ArrayRef,
    cast::AsArray,
    types::{
        Int8Type, Int16Type, Int32Type, Int64Type, UInt8Type, UInt16Type, UInt32Type, UInt64Type,
    },
};
use arrow_schema::{DataType, Field as ArrowField};
use lance_arrow::{
    ARROW_EXT_NAME_KEY, BLOB_META_KEY, BLOB_V2_EXT_NAME, DataTypeExt,
    json::{is_arrow_json_field, is_json_field},
};

use super::{
    Dictionary, LogicalType, Projection,
    schema::{compare_fields, explain_fields_difference},
};
use crate::{
    Error, Result,
    datatypes::{BLOB_DESC_LANCE_FIELD, BLOB_V2_DESC_LANCE_FIELD},
};

/// Use this config key in Arrow field metadata to indicate a column is a part of the primary key.
/// The value can be any true values like `true`, `1`, `yes` (case-insensitive).
/// A primary key column must satisfy:
/// (1) The field, and all its ancestors must not be nullable.
/// (2) The field must be a leaf without child (i.e. it is a primitive data type).
/// (3) The field must not be within a list type.
pub const LANCE_UNENFORCED_PRIMARY_KEY: &str = "lance-schema:unenforced-primary-key";

/// Use this config key in Arrow field metadata to specify the position of a primary key column.
/// The value is a 1-based integer indicating the order within the composite primary key.
/// When specified, primary key fields are ordered by this position value.
/// When not specified, primary key fields are ordered by their schema field id.
pub const LANCE_UNENFORCED_PRIMARY_KEY_POSITION: &str =
    "lance-schema:unenforced-primary-key:position";

/// Use this config key in Arrow field metadata to specify the position of a clustering key column.
/// The value is a 1-based integer indicating the order within the composite clustering key.
/// Clustering key fields are ordered by this position value.
pub const LANCE_UNENFORCED_CLUSTERING_KEY_POSITION: &str =
    "lance-schema:unenforced-clustering-key:position";

/// Use this config key in Arrow field metadata to specify the field id of the lance field.
/// The value should be non-negative i32 value. Any negative value will be seen as -1.
pub const LANCE_FIELD_ID_KEY: &str = "lance:field_id";

const PACKED_KEYS: [&str; 2] = ["packed", "lance-encoding:packed"];

fn has_blob_v2_extension(field: &ArrowField) -> bool {
    field
        .metadata()
        .get(ARROW_EXT_NAME_KEY)
        .map(|name| name == BLOB_V2_EXT_NAME)
        .unwrap_or(false)
}

#[derive(Debug, Default)]
pub enum NullabilityComparison {
    // If the nullabilities don't match then the fields don't match
    #[default]
    Strict,
    // If the expected schema is nullable then a non-nullable version of the field is allowed
    OneWay,
    // Nullability is ignored when comparing fields
    Ignore,
}

#[derive(Default)]
pub struct SchemaCompareOptions {
    /// Should the metadata be compared (default false)
    pub compare_metadata: bool,
    /// Should the dictionaries be compared (default false)
    pub compare_dictionary: bool,
    /// Should the field ids be compared (default false)
    pub compare_field_ids: bool,
    /// Should nullability be compared (default Strict)
    pub compare_nullability: NullabilityComparison,
    /// Allow fields in the expected schema to be missing from the schema being tested if
    /// they are nullable (default false)
    ///
    /// Fields in the schema being tested must always be present in the expected schema
    /// regardless of this flag.
    pub allow_missing_if_nullable: bool,
    /// Allow out of order fields (default false)
    pub ignore_field_order: bool,
    /// Allow the source schema to be a subset of the target schema (default false)
    pub allow_subschema: bool,
}

/// Blob column format version.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum BlobVersion {
    /// Legacy blob format (position / size only).
    #[default]
    V1,
    /// Blob v2 struct format.
    V2,
}
/// Encoding enum.
#[derive(Debug, Clone, PartialEq, Eq, DeepSizeOf)]
pub enum Encoding {
    /// Plain encoding.
    Plain,
    /// Binary encoding.
    VarBinary,
    /// Dictionary encoding.
    Dictionary,
    /// RLE encoding.
    RLE,
}

/// What to do on a merge operation if the types of the fields don't match
#[derive(Debug, Clone, Copy, PartialEq, Eq, DeepSizeOf)]
pub enum OnTypeMismatch {
    TakeSelf,
    Error,
}

/// Lance Schema Field
///
#[derive(Debug, Clone, PartialEq, DeepSizeOf)]
pub struct Field {
    pub name: String,
    pub id: i32,
    // TODO: Find way to move these next three fields to private
    pub parent_id: i32,
    pub logical_type: LogicalType,
    pub metadata: HashMap<String, String>,
    pub encoding: Option<Encoding>,
    pub nullable: bool,

    pub children: Vec<Self>,

    /// Dictionary value array if this field is dictionary.
    pub dictionary: Option<Dictionary>,

    /// Position of this field in the primary key (1-based).
    /// None means the field is not part of the primary key.
    /// Some(n) means this field is the nth column in the primary key.
    pub unenforced_primary_key_position: Option<u32>,

    /// Position of this field in the clustering key (1-based).
    /// None means the field is not part of the clustering key.
    /// Some(n) means this field is the nth column in the clustering key.
    pub unenforced_clustering_key_position: Option<u32>,
}

impl Field {
    /// Shortcut for creating a field with no field id (i.e. from the same info
    /// needed to create an Arrow field)
    pub fn new_arrow(name: &str, data_type: DataType, nullable: bool) -> Result<Self> {
        Self::try_from(ArrowField::new(name, data_type, nullable))
    }

    /// Returns arrow data type.
    pub fn data_type(&self) -> DataType {
        match &self.logical_type {
            lt if lt.is_list() => DataType::List(Arc::new(ArrowField::from(&self.children[0]))),
            lt if lt.is_large_list() => {
                DataType::LargeList(Arc::new(ArrowField::from(&self.children[0])))
            }
            lt if lt.is_fixed_size_list_struct() => {
                // Parse size from "fixed_size_list:struct:N"
                let size: i32 =
                    lt.0.split(':')
                        .next_back()
                        .expect("fixed_size_list:struct logical type missing size suffix")
                        .parse()
                        .expect("fixed_size_list:struct logical type has invalid size");
                DataType::FixedSizeList(Arc::new(ArrowField::from(&self.children[0])), size)
            }
            lt if lt.is_struct() => {
                DataType::Struct(self.children.iter().map(ArrowField::from).collect())
            }
            lt if lt.is_map() => {
                DataType::Map(Arc::new(ArrowField::from(&self.children[0])), false)
            }
            lt => DataType::try_from(lt).unwrap(),
        }
    }

    pub fn has_dictionary_types(&self) -> bool {
        matches!(self.data_type(), DataType::Dictionary(_, _))
            || self.children.iter().any(Self::has_dictionary_types)
    }

    /// Merge a field with another field using a reference field to ensure
    /// the correct order of fields
    ///
    /// For each child in the reference field we look for a matching child
    /// in self and other.
    ///
    /// If we find a match in both we recursively merge the children.
    /// If we find a match in one but not the other we take the matching child.
    ///
    /// Primitive fields we simply clone self and return.
    ///
    /// Matches are determined using field names and so ids are not required.
    pub fn merge_with_reference(&self, other: &Self, reference: &Self) -> Self {
        let mut new_children = Vec::with_capacity(reference.children.len());
        let mut self_children_itr = self.children.iter().peekable();
        let mut other_children_itr = other.children.iter().peekable();
        for ref_child in &reference.children {
            match (self_children_itr.peek(), other_children_itr.peek()) {
                (Some(&only_child), None) => {
                    // other is exhausted so just check if self matches
                    if only_child.name == ref_child.name {
                        new_children.push(only_child.clone());
                        self_children_itr.next();
                    }
                }
                (None, Some(&only_child)) => {
                    // Self is exhausted so just check if other matches
                    if only_child.name == ref_child.name {
                        new_children.push(only_child.clone());
                        other_children_itr.next();
                    }
                }
                (Some(&self_child), Some(&other_child)) => {
                    // Both iterators have potential, see if any match
                    match (
                        ref_child.name.cmp(&self_child.name),
                        ref_child.name.cmp(&other_child.name),
                    ) {
                        (Ordering::Equal, Ordering::Equal) => {
                            // Both match, recursively merge
                            new_children
                                .push(self_child.merge_with_reference(other_child, ref_child));
                            self_children_itr.next();
                            other_children_itr.next();
                        }
                        (Ordering::Equal, _) => {
                            // Self matches, other doesn't, use self as-is
                            new_children.push(self_child.clone());
                            self_children_itr.next();
                        }
                        (_, Ordering::Equal) => {
                            // Other matches, self doesn't, use other as-is
                            new_children.push(other_child.clone());
                            other_children_itr.next();
                        }
                        _ => {
                            // Neither match, field is projected out
                        }
                    }
                }
                (None, None) => {
                    // Both iterators are exhausted, we can quit, all remaining fields projected out
                    break;
                }
            }
        }
        Self {
            children: new_children,
            ..self.clone()
        }
    }

    pub fn apply_projection(&self, projection: &Projection) -> Option<Self> {
        // Maps and blob descriptors are atomic physical layouts. Map children
        // must remain together, while projected blob descriptor children may
        // have synthetic IDs that cannot be selected independently.
        let is_atomic_layout = self.logical_type.is_map() || self.is_blob();
        if is_atomic_layout && !projection.contains_field_id(self.id) {
            return None;
        }

        let children = if is_atomic_layout {
            self.children.clone()
        } else {
            self.children
                .iter()
                .filter_map(|c| c.apply_projection(projection))
                .collect::<Vec<_>>()
        };

        // The following case is invalid:
        // - This is a nested field (has children)
        // - All children were projected away
        // - Caller is asking for the parent field
        assert!(
            // One of the following must be true
            !children.is_empty() // Some children were projected
                || !projection.contains_field_id(self.id) // Caller is not asking for this field
                || self.children.is_empty() // This isn't a nested field
        );

        if children.is_empty() && !projection.contains_field_id(self.id) {
            None
        } else {
            let mut new_field = self.clone();
            new_field.children = children;
            Some(projection.blob_handling.unload_if_needed(new_field))
        }
    }

    pub(crate) fn explain_differences(
        &self,
        expected: &Self,
        options: &SchemaCompareOptions,
        path: Option<&str>,
    ) -> Vec<String> {
        let mut differences = Vec::new();
        let self_name = path
            .map(|path| {
                let mut self_name = path.to_owned();
                self_name.push('.');
                self_name.push_str(&self.name);
                self_name
            })
            .unwrap_or_else(|| self.name.clone());
        if self.name != expected.name {
            let expected_path = path
                .map(|path| {
                    let mut expected_path = path.to_owned();
                    expected_path.push('.');
                    expected_path.push_str(&expected.name);
                    expected_path
                })
                .unwrap_or_else(|| expected.name.clone());
            differences.push(format!(
                "expected name '{}' but name was '{}'",
                expected_path, self_name
            ));
        }
        if options.compare_field_ids && self.id != expected.id {
            differences.push(format!(
                "`{}` should have id {} but id was {}",
                self_name, expected.id, self.id
            ));
        }
        if self.logical_type != expected.logical_type {
            differences.push(format!(
                "`{}` should have type {} but type was {}",
                self_name, expected.logical_type, self.logical_type
            ));
        }
        if !Self::compare_nullability(expected.nullable, self.nullable, options) {
            differences.push(format!(
                "`{}` should have nullable={} but nullable={}",
                self_name, expected.nullable, self.nullable
            ))
        }
        if options.compare_dictionary && self.dictionary != expected.dictionary {
            differences.push(format!(
                "dictionary for `{}` did not match expected dictionary",
                self_name
            ));
        }
        if options.compare_metadata && self.metadata != expected.metadata {
            differences.push(format!(
                "metadata for `{}` did not match expected metadata",
                self_name
            ));
        }
        let children_differences = explain_fields_difference(
            &self.children,
            &expected.children,
            options,
            Some(&self_name),
        );
        if !children_differences.is_empty() {
            let children_differences = format!(
                "`{}` had mismatched children: {}",
                self_name,
                children_differences.join(", ")
            );
            differences.push(children_differences);
        }
        differences
    }

    pub fn explain_difference(
        &self,
        expected: &Self,
        options: &SchemaCompareOptions,
    ) -> Option<String> {
        let differences = self.explain_differences(expected, options, None);
        if differences.is_empty() {
            None
        } else {
            Some(differences.join(", "))
        }
    }

    pub fn compare_nullability(
        expected_nullability: bool,
        actual_nullability: bool,
        options: &SchemaCompareOptions,
    ) -> bool {
        match options.compare_nullability {
            NullabilityComparison::Strict => expected_nullability == actual_nullability,
            NullabilityComparison::OneWay => expected_nullability || !actual_nullability,
            NullabilityComparison::Ignore => true,
        }
    }

    pub fn compare_with_options(&self, expected: &Self, options: &SchemaCompareOptions) -> bool {
        self.name == expected.name
            && self.logical_type == expected.logical_type
            && Self::compare_nullability(expected.nullable, self.nullable, options)
            && compare_fields(&self.children, &expected.children, options)
            && (!options.compare_field_ids || self.id == expected.id)
            && (!options.compare_dictionary || self.dictionary == expected.dictionary)
            && (!options.compare_metadata || self.metadata == expected.metadata)
    }

    pub fn extension_name(&self) -> Option<&str> {
        self.metadata.get(ARROW_EXT_NAME_KEY).map(String::as_str)
    }

    pub fn child(&self, name: &str) -> Option<&Self> {
        self.children.iter().find(|f| f.name == name)
    }

    pub fn child_mut(&mut self, name: &str) -> Option<&mut Self> {
        self.children.iter_mut().find(|f| f.name == name)
    }

    /// Attach the Dictionary's value array, so that we can later serialize
    /// the dictionary to the manifest.
    pub fn set_dictionary_values(&mut self, arr: &ArrayRef) {
        assert!(self.data_type().is_dictionary());
        // offset / length are set to 0 and recomputed when the dictionary is persisted to disk
        self.dictionary = Some(Dictionary {
            offset: 0,
            length: 0,
            values: Some(arr.clone()),
        });
    }

    pub fn set_dictionary(&mut self, arr: &ArrayRef) {
        let data_type = self.data_type();
        match data_type {
            DataType::Dictionary(key_type, _) => match key_type.as_ref() {
                DataType::Int8 => {
                    self.set_dictionary_values(arr.as_dictionary::<Int8Type>().values())
                }
                DataType::Int16 => {
                    self.set_dictionary_values(arr.as_dictionary::<Int16Type>().values())
                }
                DataType::Int32 => {
                    self.set_dictionary_values(arr.as_dictionary::<Int32Type>().values())
                }
                DataType::Int64 => {
                    self.set_dictionary_values(arr.as_dictionary::<Int64Type>().values())
                }
                DataType::UInt8 => {
                    self.set_dictionary_values(arr.as_dictionary::<UInt8Type>().values())
                }
                DataType::UInt16 => {
                    self.set_dictionary_values(arr.as_dictionary::<UInt16Type>().values())
                }
                DataType::UInt32 => {
                    self.set_dictionary_values(arr.as_dictionary::<UInt32Type>().values())
                }
                DataType::UInt64 => {
                    self.set_dictionary_values(arr.as_dictionary::<UInt64Type>().values())
                }
                _ => {
                    panic!("Unsupported dictionary key type: {}", key_type);
                }
            },
            DataType::Struct(subfields) => {
                for (i, f) in subfields.iter().enumerate() {
                    let lance_field = self
                        .children
                        .iter_mut()
                        .find(|c| c.name == *f.name())
                        .unwrap();
                    let struct_arr = arr.as_struct();
                    lance_field.set_dictionary(struct_arr.column(i));
                }
            }
            DataType::List(_) => {
                let list_arr = arr.as_list::<i32>();
                self.children[0].set_dictionary(list_arr.values());
            }
            DataType::LargeList(_) => {
                let list_arr = arr.as_list::<i64>();
                self.children[0].set_dictionary(list_arr.values());
            }
            _ => {
                // Field types that don't support dictionaries
            }
        }
    }

    pub fn sub_field(&self, path_components: &[&str]) -> Option<&Self> {
        if path_components.is_empty() {
            Some(self)
        } else {
            let first = path_components[0];
            self.children
                .iter()
                .find(|c| c.name == first)
                .and_then(|c| c.sub_field(&path_components[1..]))
        }
    }

    pub fn sub_field_mut(&mut self, path_components: &[&str]) -> Option<&mut Self> {
        if path_components.is_empty() {
            Some(self)
        } else {
            let first = path_components[0];
            self.children
                .iter_mut()
                .find(|c| c.name == first)
                .and_then(|c| c.sub_field_mut(&path_components[1..]))
        }
    }

    /// Check if the user has labeled the field as a blob
    ///
    /// Blob fields will load descriptions by default
    pub fn is_blob(&self) -> bool {
        self.metadata.contains_key(BLOB_META_KEY)
            || self
                .metadata
                .get(ARROW_EXT_NAME_KEY)
                .map(|name| name == BLOB_V2_EXT_NAME)
                .unwrap_or(false)
    }

    /// Returns true if the field is explicitly marked as blob v2 extension.
    pub fn is_blob_v2(&self) -> bool {
        self.metadata
            .get(ARROW_EXT_NAME_KEY)
            .map(|name| name == BLOB_V2_EXT_NAME)
            .unwrap_or(false)
            || self.is_blob_v2_descriptor()
    }

    fn is_blob_v2_descriptor(&self) -> bool {
        self.metadata.contains_key(BLOB_META_KEY)
            && self.logical_type == BLOB_V2_DESC_LANCE_FIELD.logical_type
            && self.children.len() == BLOB_V2_DESC_LANCE_FIELD.children.len()
            && self
                .children
                .iter()
                .zip(BLOB_V2_DESC_LANCE_FIELD.children.iter())
                .all(|(child, expected)| {
                    child.name == expected.name && child.data_type() == expected.data_type()
                })
    }

    // Blob columns intentionally have two schema representations:
    // the loaded value view (legacy LargeBinary or blob v2 struct) and the unloaded
    // descriptor view used by projection/planning. Schema set operations need to
    // treat them as the same logical column instead of ordinary incompatible types.
    fn is_compatible_blob_projection(&self, other: &Self) -> bool {
        self.is_blob() && other.is_blob() && self.is_blob_v2() == other.is_blob_v2()
    }

    /// If the field is a blob, update this field with the same name and id
    /// but with the data type set to a struct of the blob description fields.
    ///
    /// If the field is not a blob, return the field itself.
    pub fn unloaded_mut(&mut self) {
        if self.is_blob_v2() {
            self.logical_type = BLOB_V2_DESC_LANCE_FIELD.logical_type.clone();
            self.children = BLOB_V2_DESC_LANCE_FIELD.children.clone();
            self.metadata = BLOB_V2_DESC_LANCE_FIELD.metadata.clone();
        } else if self.is_blob() {
            self.logical_type = BLOB_DESC_LANCE_FIELD.logical_type.clone();
            self.children = BLOB_DESC_LANCE_FIELD.children.clone();
            self.metadata = BLOB_DESC_LANCE_FIELD.metadata.clone();
        }
    }

    /// Convert a blob field to the materialized binary payload view.
    ///
    /// The field keeps its name and id but uses `LargeBinary` with no children.
    /// Blob v2 fields retain their extension marker internally so scan planning
    /// can recognize the binary view before exposing a plain Arrow binary field.
    pub fn binary_blob_mut(&mut self) {
        if !self.is_blob() {
            return;
        }
        let is_blob_v2 = self.is_blob_v2();

        self.logical_type = LogicalType::try_from(&DataType::LargeBinary)
            .expect("LargeBinary is always a valid logical type");
        self.children.clear();
        self.encoding = Some(Encoding::VarBinary);
        if is_blob_v2 {
            self.metadata.remove(BLOB_META_KEY);
            for key in PACKED_KEYS {
                self.metadata.remove(key);
            }
            self.metadata
                .insert(ARROW_EXT_NAME_KEY.to_string(), BLOB_V2_EXT_NAME.to_string());
        }
    }

    /// Convert blob v2 fields in this field tree to their descriptor view.
    pub fn unload_blobs_recursive(&mut self) {
        if self.is_blob_v2() {
            self.unloaded_mut();
            return;
        }

        for child in &mut self.children {
            child.unload_blobs_recursive();
        }
    }

    pub fn project(&self, path_components: &[&str]) -> Result<Self> {
        let mut f = Self {
            name: self.name.clone(),
            id: self.id,
            parent_id: self.parent_id,
            logical_type: self.logical_type.clone(),
            metadata: self.metadata.clone(),
            encoding: self.encoding.clone(),
            nullable: self.nullable,
            children: vec![],
            dictionary: self.dictionary.clone(),
            unenforced_primary_key_position: self.unenforced_primary_key_position,
            unenforced_clustering_key_position: self.unenforced_clustering_key_position,
        };
        if path_components.is_empty() {
            // Project stops here, copy all the remaining children.
            f.children.clone_from(&self.children)
        } else {
            let first = path_components[0];
            for c in self.children.as_slice() {
                if c.name == first {
                    let projected = c.project(&path_components[1..])?;
                    f.children.push(projected);
                    break;
                }
            }
        }
        Ok(f)
    }

    /// Create a new field by removing all fields that do not match the filter.
    ///
    /// If a child field matches the filter then the parent will be kept even if
    /// it does not match the filter.
    ///
    /// Returns None if the field itself does not match the filter.
    pub fn project_by_filter<F: Fn(&Self) -> bool>(&self, filter: &F) -> Option<Self> {
        let children = self
            .children
            .iter()
            .filter_map(|c| c.project_by_filter(filter))
            .collect::<Vec<_>>();
        if !children.is_empty() || filter(self) {
            Some(Self {
                children,
                ..self.clone()
            })
        } else {
            None
        }
    }

    /// Create a new field by selecting fields by their ids.
    ///
    /// If a field has it's id in the list of ids then it will be included
    /// in the new field. If a field is selected, all of it's parents will be
    /// and all of it's children will be included.
    ///
    /// For example, for the schema:
    ///
    /// ```text
    /// 0: x struct {
    ///     1: y int32
    ///     2: l list {
    ///         3: z int32
    ///     }
    /// }
    /// ```
    ///
    /// If the ids are `[2]`, then this will include the parent `0` and the
    /// child `3`.
    pub(crate) fn project_by_ids(&self, ids: &[i32], include_all_children: bool) -> Option<Self> {
        let children = self
            .children
            .iter()
            .filter_map(|c| c.project_by_ids(ids, include_all_children))
            .collect::<Vec<_>>();
        if ids.contains(&self.id) && (children.is_empty() || include_all_children) {
            Some(self.clone())
        } else if !children.is_empty() {
            Some(Self {
                children,
                ..self.clone()
            })
        } else {
            None
        }
    }

    /// Project by a field.
    ///
    pub fn project_by_field(&self, other: &Self, on_type_mismatch: OnTypeMismatch) -> Result<Self> {
        if self.name != other.name {
            return Err(Error::schema(format!(
                "Attempt to project field by different names: {} and {}",
                self.name, other.name,
            )));
        };

        if self.is_compatible_blob_projection(other) {
            return Ok(self.clone());
        }

        match (self.data_type(), other.data_type()) {
            (DataType::Boolean, DataType::Boolean) => Ok(self.clone()),
            (dt, other_dt)
                if (dt.is_primitive() && other_dt.is_primitive())
                    || (dt.is_binary_like() && other_dt.is_binary_like()) =>
            {
                if dt != other_dt {
                    return Err(Error::schema(format!(
                        "Attempt to project field by different types: {} and {}",
                        dt, other_dt,
                    )));
                }
                Ok(self.clone())
            }
            (DataType::Struct(_), DataType::Struct(_)) => {
                // Blob v2 columns are special: they can have different struct layouts
                // (logical input vs. descriptor struct). We treat blob v2 structs like primitive
                // fields (e.g. a binary column) during schema set operations (union/subtract).
                if self.is_blob() {
                    return Ok(self.clone());
                }
                let mut fields = vec![];
                for other_field in other.children.iter() {
                    let Some(child) = self.child(&other_field.name) else {
                        return Err(Error::schema(format!(
                            "Attempt to project non-existed field: {} on {}",
                            other_field.name, self,
                        )));
                    };
                    fields.push(child.project_by_field(other_field, on_type_mismatch)?);
                }
                let mut cloned = self.clone();
                cloned.children = fields;
                Ok(cloned)
            }
            (DataType::List(_), DataType::List(_))
            | (DataType::LargeList(_), DataType::LargeList(_))
            | (DataType::Map(_, _), DataType::Map(_, _)) => {
                let projected =
                    self.children[0].project_by_field(&other.children[0], on_type_mismatch)?;
                let mut cloned = self.clone();
                cloned.children = vec![projected];
                Ok(cloned)
            }
            (DataType::FixedSizeList(dt, n), DataType::FixedSizeList(other_dt, m))
                if dt == other_dt && n == m =>
            {
                Ok(self.clone())
            }
            (
                DataType::Dictionary(self_key, self_value),
                DataType::Dictionary(other_key, other_value),
            ) if self_key == other_key && self_value == other_value => Ok(self.clone()),
            (DataType::Null, DataType::Null) => Ok(self.clone()),
            (DataType::FixedSizeBinary(self_width), DataType::FixedSizeBinary(other_width))
                if self_width == other_width =>
            {
                Ok(self.clone())
            }
            _ => match on_type_mismatch {
                OnTypeMismatch::Error => Err(Error::schema(format!(
                    "Attempt to project incompatible fields: {} and {}",
                    self, other
                ))),
                OnTypeMismatch::TakeSelf => Ok(self.clone()),
            },
        }
    }

    pub(crate) fn resolve<'a>(
        &'a self,
        split: &mut VecDeque<&str>,
        fields: &mut Vec<&'a Self>,
    ) -> bool {
        fields.push(self);
        if split.is_empty() {
            return true;
        }
        let first = split.pop_front().unwrap();
        if let Some(child) = self.children.iter().find(|c| c.name == first) {
            child.resolve(split, fields)
        } else {
            false
        }
    }

    /// Case-insensitive version of resolve.
    /// First tries exact match for each child, then falls back to case-insensitive.
    pub(crate) fn resolve_case_insensitive<'a>(
        &'a self,
        split: &mut VecDeque<&str>,
        fields: &mut Vec<&'a Self>,
    ) -> bool {
        fields.push(self);
        if split.is_empty() {
            return true;
        }
        let first = split.pop_front().unwrap();
        // Try exact match first
        if let Some(child) = self.children.iter().find(|c| c.name == first) {
            return child.resolve_case_insensitive(split, fields);
        }
        // Fall back to case-insensitive match
        if let Some(child) = self
            .children
            .iter()
            .find(|c| c.name.eq_ignore_ascii_case(first))
        {
            return child.resolve_case_insensitive(split, fields);
        }
        false
    }

    pub(crate) fn do_intersection(&self, other: &Self, ignore_types: bool) -> Result<Self> {
        if self.name != other.name {
            return Err(Error::arrow(format!(
                "Attempt to intersect different fields: {} and {}",
                self.name, other.name,
            )));
        }

        if self.is_blob() != other.is_blob() {
            if ignore_types {
                return Ok(if self.id >= 0 {
                    self.clone()
                } else {
                    other.clone()
                });
            }
            return Err(Error::arrow(format!(
                "Attempt to intersect blob and non-blob field: {}",
                self.name
            )));
        }

        if self.is_compatible_blob_projection(other) {
            return Ok(if self.id >= 0 {
                self.clone()
            } else {
                other.clone()
            });
        }

        let self_type = self.data_type();
        let other_type = other.data_type();

        if matches!(
            (&self_type, &other_type),
            (DataType::Struct(_), DataType::Struct(_))
                | (DataType::List(_), DataType::List(_))
                | (DataType::Map(_, _), DataType::Map(_, _))
        ) {
            // Blob v2 uses a struct logical type for descriptors, which differs from the logical
            // input struct (data/uri). When intersecting schemas for projection we want to keep
            // the projected blob layout instead of intersecting by child names.
            if self.is_blob() {
                return Ok(self.clone());
            }

            let children = self
                .children
                .iter()
                .filter_map(|c| {
                    if let Some(other_child) = other.child(&c.name) {
                        let intersection = c.do_intersection(other_child, ignore_types).ok()?;
                        Some(intersection)
                    } else {
                        None
                    }
                })
                .collect::<Vec<_>>();
            let f = Self {
                name: self.name.clone(),
                id: if self.id >= 0 { self.id } else { other.id },
                parent_id: self.parent_id,
                logical_type: self.logical_type.clone(),
                metadata: self.metadata.clone(),
                encoding: self.encoding.clone(),
                nullable: self.nullable,
                children,
                dictionary: self.dictionary.clone(),
                unenforced_primary_key_position: self.unenforced_primary_key_position,
                unenforced_clustering_key_position: self.unenforced_clustering_key_position,
            };
            return Ok(f);
        }

        if (!ignore_types && self_type != other_type) || self.name != other.name {
            return Err(Error::arrow(format!(
                "Attempt to intersect different fields: ({}, {}) and ({}, {})",
                self.name, self_type, other.name, other_type
            )));
        }

        Ok(if self.id >= 0 {
            self.clone()
        } else {
            other.clone()
        })
    }

    /// Intersection of two [`Field`]s.
    ///
    pub fn intersection(&self, other: &Self) -> Result<Self> {
        self.do_intersection(other, false)
    }

    /// Intersection of two [`Field`]s, ignoring data types.
    pub fn intersection_ignore_types(&self, other: &Self) -> Result<Self> {
        self.do_intersection(other, true)
    }

    pub fn exclude(&self, other: &Self) -> Option<Self> {
        if !self.data_type().is_nested() {
            return None;
        }
        let children = self
            .children
            .iter()
            .map(|c| {
                if let Some(other_child) = other.child(&c.name) {
                    c.exclude(other_child)
                } else {
                    Some(c.clone())
                }
            })
            .filter(Option::is_some)
            .flatten()
            .collect::<Vec<_>>();
        if children.is_empty() {
            None
        } else {
            Some(Self {
                name: self.name.clone(),
                id: self.id,
                parent_id: self.parent_id,
                logical_type: self.logical_type.clone(),
                metadata: self.metadata.clone(),
                encoding: self.encoding.clone(),
                nullable: self.nullable,
                children,
                dictionary: self.dictionary.clone(),
                unenforced_primary_key_position: self.unenforced_primary_key_position,
                unenforced_clustering_key_position: self.unenforced_clustering_key_position,
            })
        }
    }

    /// Merge the children of other field into this one.
    pub(super) fn merge(&mut self, other: &Self) -> Result<()> {
        match (self.data_type(), other.data_type()) {
            (DataType::Struct(_), DataType::Struct(_)) => {
                for other_child in other.children.as_slice() {
                    if let Some(field) = self.child_mut(&other_child.name) {
                        field.merge(other_child)?;
                    } else {
                        self.children.push(other_child.clone());
                    }
                }
            }
            (DataType::List(_), DataType::List(_))
            | (DataType::LargeList(_), DataType::LargeList(_)) => {
                self.children[0].merge(&other.children[0])?;
            }
            (
                DataType::FixedSizeList(_, self_list_size),
                DataType::FixedSizeList(_, other_list_size),
            ) if self_list_size == other_list_size => {
                // do nothing
            }
            (DataType::FixedSizeBinary(self_size), DataType::FixedSizeBinary(other_size))
                if self_size == other_size =>
            {
                // do nothing
            }
            _ => {
                if self.data_type() != other.data_type() {
                    return Err(Error::schema(format!(
                        "Attempt to merge incompatible fields: {} and {}",
                        self, other
                    )));
                }
            }
        }
        Ok(())
    }

    // Get the max field id of itself and all children.
    pub(super) fn max_id(&self) -> i32 {
        max(
            self.id,
            self.children.iter().map(|c| c.max_id()).max().unwrap_or(-1),
        )
    }

    /// Recursively set field ID and parent ID for this field and all its children.
    pub fn set_id(&mut self, parent_id: i32, id_seed: &mut i32) {
        self.parent_id = parent_id;
        if self.id < 0 {
            self.id = *id_seed;
            *id_seed += 1;
        }
        self.children
            .iter_mut()
            .for_each(|f| f.set_id(self.id, id_seed));
    }

    /// Recursively reset field ID for this field and all its children.
    pub(super) fn reset_id(&mut self) {
        self.id = -1;
        self.children.iter_mut().for_each(Self::reset_id);
    }

    pub fn field_by_id_mut(&mut self, id: impl Into<i32>) -> Option<&mut Self> {
        let id = id.into();
        for child in self.children.as_mut_slice() {
            if child.id == id {
                return Some(child);
            }
            if let Some(grandchild) = child.field_by_id_mut(id) {
                return Some(grandchild);
            }
        }
        None
    }

    pub fn field_by_id(&self, id: impl Into<i32>) -> Option<&Self> {
        let id = id.into();
        for child in self.children.as_slice() {
            if child.id == id {
                return Some(child);
            }
            if let Some(grandchild) = child.field_by_id(id) {
                return Some(grandchild);
            }
        }
        None
    }

    // Find any nested child with a specific field id
    pub(super) fn mut_field_by_id(&mut self, id: impl Into<i32>) -> Option<&mut Self> {
        let id = id.into();
        for child in self.children.as_mut_slice() {
            if child.id == id {
                return Some(child);
            }
            if let Some(grandchild) = child.mut_field_by_id(id) {
                return Some(grandchild);
            }
        }
        None
    }

    // Check if field has metadata `packed` set to true, this check is case insensitive.
    pub fn is_packed_struct(&self) -> bool {
        PACKED_KEYS.iter().any(|key| {
            self.metadata
                .get(*key)
                .map(|value| value.eq_ignore_ascii_case("true"))
                .unwrap_or(false)
        })
    }

    /// Return true if the field is a leaf field.
    ///
    /// A leaf field is a field that is not a struct or a list.
    pub fn is_leaf(&self) -> bool {
        self.children.is_empty()
    }

    /// Return true if the field is part of the (unenforced) primary key.
    pub fn is_unenforced_primary_key(&self) -> bool {
        self.unenforced_primary_key_position.is_some()
    }

    pub fn is_unenforced_clustering_key(&self) -> bool {
        self.unenforced_clustering_key_position.is_some()
    }
}

impl fmt::Display for Field {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Field(id={}, name={}, type={}",
            self.id, self.name, self.logical_type.0,
        )?;

        if let Some(dictionary) = &self.dictionary {
            write!(f, ", dictionary={:?}", dictionary)?;
        }

        if !self.children.is_empty() {
            write!(f, ", children=[")?;
            for child in self.children.iter() {
                write!(f, "{}, ", child)?;
            }
            write!(f, "]")?;
        }

        write!(f, ")")
    }
}

impl TryFrom<&ArrowField> for Field {
    type Error = Error;

    fn try_from(field: &ArrowField) -> Result<Self> {
        let mut metadata = field.metadata().clone();
        let id = match metadata.remove(LANCE_FIELD_ID_KEY) {
            Some(val) => val
                .parse::<i32>()
                .map_err(|e| Error::invalid_input(e.to_string()))?
                .max(-1),
            None => -1,
        };

        let children = match field.data_type() {
            DataType::Struct(children) => children
                .iter()
                .map(|f| Self::try_from(f.as_ref()))
                .collect::<Result<_>>()?,
            DataType::List(item) => vec![Self::try_from(item.as_ref())?],
            DataType::LargeList(item) => vec![Self::try_from(item.as_ref())?],
            DataType::FixedSizeList(item, _) if matches!(item.data_type(), DataType::Struct(_)) => {
                vec![Self::try_from(item.as_ref())?]
            }
            DataType::Map(entries, keys_sorted) => {
                // TODO: We only support keys_sorted=false for now,
                //  because converting a rust arrow map field to the python arrow field will
                //  lose the keys_sorted property.
                if *keys_sorted {
                    return Err(Error::schema(
                        "Unsupported map field with keys_sorted=true".to_string(),
                    ));
                }
                // Validate Map entries follow Arrow specification
                let DataType::Struct(struct_fields) = entries.data_type() else {
                    return Err(Error::schema(
                        "Map entries field must be a Struct<key, value>".to_string(),
                    ));
                };
                if struct_fields.len() < 2 {
                    return Err(Error::schema(
                        "Map entries struct must contain both key and value fields".to_string(),
                    ));
                }
                let key_field = &struct_fields[0];
                if key_field.is_nullable() {
                    return Err(Error::schema(format!(
                        "Map key field '{}' must be non-nullable according to Arrow Map specification",
                        key_field.name()
                    )));
                }
                vec![Self::try_from(entries.as_ref())?]
            }
            _ => vec![],
        };
        let unenforced_primary_key_position = metadata
            .get(LANCE_UNENFORCED_PRIMARY_KEY_POSITION)
            .and_then(|s| s.parse::<u32>().ok())
            .or_else(|| {
                // Backward compatibility: use 0 for legacy boolean flag
                metadata
                    .get(LANCE_UNENFORCED_PRIMARY_KEY)
                    .filter(|s| matches!(s.to_lowercase().as_str(), "true" | "1" | "yes"))
                    .map(|_| 0)
            });
        let unenforced_clustering_key_position = metadata
            .get(LANCE_UNENFORCED_CLUSTERING_KEY_POSITION)
            .and_then(|s| s.parse::<u32>().ok());
        let is_blob_v2 = has_blob_v2_extension(field);

        if is_blob_v2 {
            metadata
                .entry(ARROW_EXT_NAME_KEY.to_string())
                .or_insert_with(|| BLOB_V2_EXT_NAME.to_string());
        }

        // Check for JSON extension types (both Arrow and Lance)
        let logical_type = if is_arrow_json_field(field) || is_json_field(field) {
            LogicalType::from("json")
        } else if is_blob_v2 {
            LogicalType::from("struct")
        } else {
            LogicalType::try_from(field.data_type())?
        };

        Ok(Self {
            id,
            parent_id: -1,
            name: field.name().clone(),
            logical_type,
            encoding: match field.data_type() {
                dt if dt.is_fixed_stride() => Some(Encoding::Plain),
                dt if dt.is_binary_like() => Some(Encoding::VarBinary),
                DataType::Dictionary(_, _) => Some(Encoding::Dictionary),
                // Use plain encoder to store the offsets of list and map.
                DataType::List(_) | DataType::LargeList(_) | DataType::Map(_, _) => {
                    Some(Encoding::Plain)
                }
                _ => None,
            },
            metadata,
            nullable: field.is_nullable(),
            children,
            dictionary: None,
            unenforced_primary_key_position,
            unenforced_clustering_key_position,
        })
    }
}

impl TryFrom<ArrowField> for Field {
    type Error = Error;

    fn try_from(field: ArrowField) -> Result<Self> {
        Self::try_from(&field)
    }
}

impl From<&Field> for ArrowField {
    fn from(field: &Field) -> Self {
        let out = Self::new(&field.name, field.data_type(), field.nullable);
        let mut metadata = field.metadata.clone();

        if field.logical_type.is_blob() {
            metadata
                .entry(BLOB_META_KEY.to_string())
                .or_insert_with(|| "true".to_string());
        }

        // Add JSON extension metadata if this is a JSON field
        if field.logical_type.0 == "json" {
            metadata.insert(
                ARROW_EXT_NAME_KEY.to_string(),
                lance_arrow::json::JSON_EXT_NAME.to_string(),
            );
        }

        out.with_metadata(metadata)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use arrow_array::{DictionaryArray, StringArray, UInt32Array};
    use arrow_schema::{Fields, TimeUnit};
    use lance_arrow::BLOB_META_KEY;
    use std::collections::HashMap;

    #[test]
    fn arrow_field_to_field_metadata() {
        let mut metadata = HashMap::new();
        metadata.insert(LANCE_FIELD_ID_KEY.to_string(), "42".to_string());
        metadata.insert("custom".to_string(), "value".to_string());

        let arrow_field =
            ArrowField::new("a", DataType::Int32, false).with_metadata(metadata.clone());
        let field = Field::try_from(&arrow_field).unwrap();

        assert_eq!(field.id, 42);
        assert!(!field.metadata.contains_key(LANCE_FIELD_ID_KEY));
        assert_eq!(
            field.metadata.get("custom").map(String::as_str),
            Some("value")
        );
    }

    #[test]
    fn arrow_field_to_field() {
        for (name, data_type) in [
            ("null", DataType::Null),
            ("bool", DataType::Boolean),
            ("int8", DataType::Int8),
            ("uint8", DataType::UInt8),
            ("int16", DataType::Int16),
            ("uint16", DataType::UInt16),
            ("int32", DataType::Int32),
            ("uint32", DataType::UInt32),
            ("int64", DataType::Int64),
            ("uint64", DataType::UInt64),
            ("float16", DataType::Float16),
            ("float32", DataType::Float32),
            ("float64", DataType::Float64),
            ("decimal128:7:3", DataType::Decimal128(7, 3)),
            ("timestamp:s:-", DataType::Timestamp(TimeUnit::Second, None)),
            (
                "timestamp:ms:-",
                DataType::Timestamp(TimeUnit::Millisecond, None),
            ),
            (
                "timestamp:us:-",
                DataType::Timestamp(TimeUnit::Microsecond, None),
            ),
            (
                "timestamp:ns:-",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
            ),
            (
                "timestamp:s:America/New_York",
                DataType::Timestamp(TimeUnit::Second, Some("America/New_York".into())),
            ),
            ("time32:s", DataType::Time32(TimeUnit::Second)),
            ("time32:ms", DataType::Time32(TimeUnit::Millisecond)),
            ("time64:us", DataType::Time64(TimeUnit::Microsecond)),
            ("time64:ns", DataType::Time64(TimeUnit::Nanosecond)),
            ("duration:s", DataType::Duration(TimeUnit::Second)),
            ("duration:ms", DataType::Duration(TimeUnit::Millisecond)),
            ("duration:us", DataType::Duration(TimeUnit::Microsecond)),
            ("duration:ns", DataType::Duration(TimeUnit::Nanosecond)),
            ("fixed_size_binary:100", DataType::FixedSizeBinary(100)),
            (
                "fixed_size_list:int32:10",
                DataType::FixedSizeList(
                    Arc::new(ArrowField::new("item", DataType::Int32, true)),
                    10,
                ),
            ),
        ] {
            let arrow_field = ArrowField::new(name, data_type.clone(), true);
            let field = Field::try_from(&arrow_field).unwrap();
            assert_eq!(field.name, name);
            assert_eq!(field.data_type(), data_type);
            assert_eq!(ArrowField::from(&field), arrow_field);
        }
    }

    #[test]
    fn test_view_types_stored_as_lance_base_types() {
        let field = Field::try_from(&ArrowField::new("s", DataType::Utf8View, true)).unwrap();
        assert_eq!(field.data_type(), DataType::Utf8);
        assert_eq!(
            LogicalType::try_from(&DataType::Utf8View).unwrap().0,
            "string"
        );

        let field = Field::try_from(&ArrowField::new("b", DataType::BinaryView, true)).unwrap();
        assert_eq!(field.data_type(), DataType::Binary);
        assert_eq!(
            LogicalType::try_from(&DataType::BinaryView).unwrap().0,
            "binary"
        );
    }

    #[test]
    fn test_nested_types() {
        assert_eq!(
            LogicalType::try_from(&DataType::List(Arc::new(ArrowField::new(
                "item",
                DataType::Binary,
                false
            ))))
            .unwrap()
            .0,
            "list"
        );
        assert_eq!(
            LogicalType::try_from(&DataType::List(Arc::new(ArrowField::new(
                "item",
                DataType::Struct(Fields::empty()),
                false
            ))))
            .unwrap()
            .0,
            "list.struct"
        );
        assert_eq!(
            LogicalType::try_from(&DataType::Struct(Fields::from(vec![ArrowField::new(
                "item",
                DataType::Binary,
                false
            )])))
            .unwrap()
            .0,
            "struct"
        );

        assert_eq!(
            LogicalType::try_from(&DataType::Map(
                Arc::new(ArrowField::new(
                    "entries",
                    DataType::Struct(Fields::from(vec![
                        ArrowField::new("key", DataType::Utf8, false),
                        ArrowField::new("value", DataType::Int32, true),
                    ])),
                    true
                )),
                false
            ))
            .unwrap()
            .0,
            "map"
        );
    }

    #[test]
    fn struct_field() {
        let arrow_field = ArrowField::new(
            "struct",
            DataType::Struct(Fields::from(vec![ArrowField::new(
                "a",
                DataType::Int32,
                true,
            )])),
            false,
        );
        let field = Field::try_from(&arrow_field).unwrap();
        assert_eq!(field.name, "struct");
        assert_eq!(&field.data_type(), arrow_field.data_type());
        assert_eq!(ArrowField::from(&field), arrow_field);
    }

    #[test]
    fn map_key_must_be_non_nullable() {
        let entries_field = Arc::new(ArrowField::new(
            "entries",
            DataType::Struct(Fields::from(vec![
                ArrowField::new("key", DataType::Utf8, true), // invalid: nullable key
                ArrowField::new("value", DataType::Int32, true),
            ])),
            false,
        ));
        let arrow_field = ArrowField::new("props", DataType::Map(entries_field, false), true);

        let result = Field::try_from(&arrow_field);
        assert!(result.is_err(), "Nullable map key should be rejected");
    }

    #[test]
    fn map_keys_sorted_unsupported() {
        let entries_field = Arc::new(ArrowField::new(
            "entries",
            DataType::Struct(Fields::from(vec![
                ArrowField::new("key", DataType::Utf8, false),
                ArrowField::new("value", DataType::Int32, true),
            ])),
            false,
        ));

        // Test that keys_sorted=true is rejected
        let arrow_field_sorted = ArrowField::new(
            "map_field",
            DataType::Map(entries_field.clone(), true),
            true,
        );
        let result = Field::try_from(&arrow_field_sorted);
        assert!(result.is_err(), "keys_sorted=true should be rejected");
        assert!(result.unwrap_err().to_string().contains("keys_sorted=true"));

        // Test that keys_sorted=false is supported
        let arrow_field_unsorted =
            ArrowField::new("map_field", DataType::Map(entries_field, false), true);
        let lance_field_unsorted = Field::try_from(&arrow_field_unsorted).unwrap();

        // Verify conversion back to ArrowField preserves keys_sorted=false
        let converted_field_unsorted = ArrowField::from(&lance_field_unsorted);
        match converted_field_unsorted.data_type() {
            DataType::Map(_, keys_sorted) => assert!(!keys_sorted, "keys_sorted should be false"),
            _ => panic!("Expected Map type"),
        }
    }

    #[test]
    fn map_entries_must_be_struct() {
        let entries_field = Arc::new(ArrowField::new("entries", DataType::Utf8, false));
        let arrow_field = ArrowField::new("map_field", DataType::Map(entries_field, false), true);

        let err = Field::try_from(&arrow_field).unwrap_err();
        assert!(
            err.to_string()
                .contains("Map entries field must be a Struct"),
            "Expected struct requirement error, got {err}"
        );
    }

    #[test]
    fn map_entries_struct_needs_key_and_value() {
        let entries_field = Arc::new(ArrowField::new(
            "entries",
            DataType::Struct(Fields::from(vec![ArrowField::new(
                "key",
                DataType::Utf8,
                false,
            )])),
            false,
        ));
        let arrow_field = ArrowField::new("map_field", DataType::Map(entries_field, false), true);

        let err = Field::try_from(&arrow_field).unwrap_err();
        assert!(
            err.to_string().contains("must contain both key and value"),
            "Expected both fields requirement error, got {err}"
        );
    }

    #[test]
    fn test_project_by_field_null_type() {
        let f1: Field = ArrowField::new("a", DataType::Null, true)
            .try_into()
            .unwrap();
        let f2: Field = ArrowField::new("a", DataType::Null, true)
            .try_into()
            .unwrap();
        let p1 = f1.project_by_field(&f2, OnTypeMismatch::Error).unwrap();

        assert_eq!(p1, f1);

        let f3: Field = ArrowField::new("b", DataType::Null, true)
            .try_into()
            .unwrap();
        assert!(f1.project_by_field(&f3, OnTypeMismatch::Error).is_err());

        let f4: Field = ArrowField::new("a", DataType::Int32, true)
            .try_into()
            .unwrap();
        assert!(f1.project_by_field(&f4, OnTypeMismatch::Error).is_err());
    }

    #[test]
    fn test_field_intersection() {
        let f1: Field = ArrowField::new("a", DataType::Int32, true)
            .try_into()
            .unwrap();
        let f2: Field = ArrowField::new("a", DataType::Int32, true)
            .try_into()
            .unwrap();
        let i1 = f1.intersection(&f2).unwrap();

        assert_eq!(i1, f1);

        let f3: Field = ArrowField::new("b", DataType::Int32, true)
            .try_into()
            .unwrap();
        assert!(f1.intersection(&f3).is_err());
    }

    #[test]
    fn test_struct_field_intersection() {
        let f1: Field = ArrowField::new(
            "a",
            DataType::Struct(Fields::from(vec![
                ArrowField::new("b", DataType::Int32, true),
                ArrowField::new("c", DataType::Int32, true),
            ])),
            true,
        )
        .try_into()
        .unwrap();
        let f2: Field = ArrowField::new(
            "a",
            DataType::Struct(Fields::from(vec![
                ArrowField::new("c", DataType::Int32, true),
                ArrowField::new("a", DataType::Int32, true),
            ])),
            true,
        )
        .try_into()
        .unwrap();
        let actual = f1.intersection(&f2).unwrap();

        let expected: Field = ArrowField::new(
            "a",
            DataType::Struct(Fields::from(vec![ArrowField::new(
                "c",
                DataType::Int32,
                true,
            )])),
            true,
        )
        .try_into()
        .unwrap();
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_compare() {
        let opts = SchemaCompareOptions::default();

        let mut expected: Field = ArrowField::new(
            "a",
            DataType::Dictionary(Box::new(DataType::UInt32), Box::new(DataType::Utf8)),
            true,
        )
        .try_into()
        .unwrap();
        let keys = UInt32Array::from_iter_values(vec![0, 1]);
        let values: ArrayRef = Arc::new(StringArray::from_iter_values([
            "a".to_string(),
            "b".to_string(),
        ]));
        let dictionary: ArrayRef = Arc::new(DictionaryArray::new(keys, values));
        expected.set_dictionary(&dictionary);

        let no_dictionary: Field = ArrowField::new(
            "a",
            DataType::Dictionary(Box::new(DataType::UInt32), Box::new(DataType::Utf8)),
            true,
        )
        .try_into()
        .unwrap();

        // By default, do not compare dictionary
        assert!(no_dictionary.compare_with_options(&expected, &opts));

        let compare_dict = SchemaCompareOptions {
            compare_dictionary: true,
            ..Default::default()
        };
        assert!(!no_dictionary.compare_with_options(&expected, &compare_dict));

        let metadata = HashMap::<_, _>::from_iter(vec![("foo".to_string(), "bar".to_string())]);
        let expected: Field = ArrowField::new("a", DataType::UInt32, true)
            .with_metadata(metadata)
            .try_into()
            .unwrap();

        let no_metadata: Field = ArrowField::new("a", DataType::UInt32, true)
            .try_into()
            .unwrap();

        // By default, do not compare metadata
        assert!(no_metadata.compare_with_options(&expected, &opts));

        let compare_metadata = SchemaCompareOptions {
            compare_metadata: true,
            ..Default::default()
        };
        assert!(!no_metadata.compare_with_options(&expected, &compare_metadata));

        let mut expected: Field = ArrowField::new("a", DataType::UInt32, true)
            .try_into()
            .unwrap();
        let mut seed = 0;
        expected.set_id(-1, &mut seed);

        let no_id: Field = ArrowField::new("a", DataType::UInt32, true)
            .try_into()
            .unwrap();
        // Do not compare ids by default
        assert!(no_id.compare_with_options(&expected, &opts));

        let compare_ids = SchemaCompareOptions {
            compare_field_ids: true,
            ..Default::default()
        };
        assert!(!no_id.compare_with_options(&expected, &compare_ids));
    }

    #[test]
    fn test_explain_difference() {
        let expected: Field = ArrowField::new(
            "a",
            DataType::Struct(Fields::from(vec![
                ArrowField::new("b", DataType::Int32, true),
                ArrowField::new("c", DataType::Int32, true),
            ])),
            true,
        )
        .try_into()
        .unwrap();

        let opts = SchemaCompareOptions::default();
        assert_eq!(expected.explain_difference(&expected, &opts), None);

        let wrong_name: Field = ArrowField::new(
            "b",
            DataType::Struct(Fields::from(vec![
                ArrowField::new("b", DataType::Int32, true),
                ArrowField::new("c", DataType::Int32, true),
            ])),
            true,
        )
        .try_into()
        .unwrap();

        assert_eq!(
            wrong_name.explain_difference(&expected, &opts),
            Some("expected name 'a' but name was 'b'".to_string())
        );

        let wrong_child: Field = ArrowField::new(
            "a",
            DataType::Struct(Fields::from(vec![
                ArrowField::new("b", DataType::Int32, false),
                ArrowField::new("c", DataType::Int32, true),
            ])),
            true,
        )
        .try_into()
        .unwrap();
        assert_eq!(
            wrong_child.explain_difference(&expected, &opts),
            Some(
                "`a` had mismatched children: `a.b` should have nullable=true but nullable=false"
                    .to_string()
            )
        );

        let mismatched_children: Field = ArrowField::new(
            "a",
            DataType::Struct(Fields::from(vec![
                ArrowField::new("d", DataType::Int32, false),
                ArrowField::new("b", DataType::Int32, true),
            ])),
            true,
        )
        .try_into()
        .unwrap();
        assert_eq!(
            mismatched_children.explain_difference(&expected, &opts),
            Some("`a` had mismatched children: fields did not match, missing=[a.c], unexpected=[a.d]".to_string())
        );

        let reordered_children: Field = ArrowField::new(
            "a",
            DataType::Struct(Fields::from(vec![
                ArrowField::new("c", DataType::Int32, true),
                ArrowField::new("b", DataType::Int32, true),
            ])),
            true,
        )
        .try_into()
        .unwrap();
        assert_eq!(
            reordered_children.explain_difference(&expected, &opts),
            Some("`a` had mismatched children: fields in different order, expected: [b, c], actual: [c, b]".to_string())
        );

        let multiple_wrongs: Field = ArrowField::new(
            "c",
            DataType::Struct(Fields::from(vec![
                ArrowField::new("b", DataType::Int32, true),
                ArrowField::new("c", DataType::Float32, true),
            ])),
            true,
        )
        .try_into()
        .unwrap();
        assert_eq!(
            multiple_wrongs.explain_difference(&expected, &opts),
            Some(
                "expected name 'a' but name was 'c', `c` had mismatched children: `c.c` should have type int32 but type was float"
                    .to_string()
            )
        );

        let mut expected: Field = ArrowField::new(
            "a",
            DataType::Dictionary(Box::new(DataType::UInt32), Box::new(DataType::Utf8)),
            true,
        )
        .try_into()
        .unwrap();
        let keys = UInt32Array::from_iter_values(vec![0, 1]);
        let values: ArrayRef = Arc::new(StringArray::from_iter_values([
            "a".to_string(),
            "b".to_string(),
        ]));
        let dictionary: ArrayRef = Arc::new(DictionaryArray::new(keys, values));
        expected.set_dictionary(&dictionary);

        let no_dictionary: Field = ArrowField::new(
            "a",
            DataType::Dictionary(Box::new(DataType::UInt32), Box::new(DataType::Utf8)),
            true,
        )
        .try_into()
        .unwrap();

        // By default, do not compare dictionary
        assert_eq!(no_dictionary.explain_difference(&expected, &opts), None);

        let compare_dict = SchemaCompareOptions {
            compare_dictionary: true,
            ..Default::default()
        };
        assert_eq!(
            no_dictionary.explain_difference(&expected, &compare_dict),
            Some("dictionary for `a` did not match expected dictionary".to_string())
        );

        let metadata = HashMap::<_, _>::from_iter(vec![("foo".to_string(), "bar".to_string())]);
        let expected: Field = ArrowField::new("a", DataType::UInt32, true)
            .with_metadata(metadata)
            .try_into()
            .unwrap();

        let no_metadata: Field = ArrowField::new("a", DataType::UInt32, true)
            .try_into()
            .unwrap();

        // By default, do not compare metadata
        assert_eq!(no_metadata.explain_difference(&expected, &opts), None);

        let compare_metadata = SchemaCompareOptions {
            compare_metadata: true,
            ..Default::default()
        };
        assert_eq!(
            no_metadata.explain_difference(&expected, &compare_metadata),
            Some("metadata for `a` did not match expected metadata".to_string())
        );

        let mut expected: Field = ArrowField::new("a", DataType::UInt32, true)
            .try_into()
            .unwrap();
        let mut seed = 0;
        expected.set_id(-1, &mut seed);

        let no_id: Field = ArrowField::new("a", DataType::UInt32, true)
            .try_into()
            .unwrap();
        // Do not compare ids by default
        assert_eq!(no_id.explain_difference(&expected, &opts), None);

        let compare_ids = SchemaCompareOptions {
            compare_field_ids: true,
            ..Default::default()
        };
        assert_eq!(
            no_id.explain_difference(&expected, &compare_ids),
            Some("`a` should have id 0 but id was -1".to_string())
        );
    }

    #[test]
    pub fn test_nullability_comparison() {
        let f1 = Field::try_from(&ArrowField::new("a", DataType::Int32, true)).unwrap();
        let f2 = Field::try_from(&ArrowField::new("a", DataType::Int32, false)).unwrap();

        // By default, nullability difference is not allowed
        assert!(!f1.compare_with_options(&f2, &SchemaCompareOptions::default()));

        let ignore_nullability = SchemaCompareOptions {
            compare_nullability: NullabilityComparison::Ignore,
            ..Default::default()
        };
        let oneway_nullability = SchemaCompareOptions {
            compare_nullability: NullabilityComparison::OneWay,
            ..Default::default()
        };
        let strict_nullability = SchemaCompareOptions {
            compare_nullability: NullabilityComparison::Strict,
            ..Default::default()
        };

        // By default, nullability difference is not allowed
        assert!(!f1.compare_with_options(&f2, &strict_nullability));
        assert!(!f2.compare_with_options(&f1, &strict_nullability));
        // One way nullability will allow the difference if expected is nullable
        assert!(!f1.compare_with_options(&f2, &oneway_nullability));
        assert!(f2.compare_with_options(&f1, &oneway_nullability));
        // Finally, ignore will ignore
        assert!(f1.compare_with_options(&f2, &ignore_nullability));
        assert!(f2.compare_with_options(&f1, &ignore_nullability));
    }

    #[test]
    fn blob_unloaded_mut_selects_layout_from_metadata() {
        let metadata = HashMap::from([(BLOB_META_KEY.to_string(), "true".to_string())]);
        let mut binary_field: Field = ArrowField::new("blob", DataType::LargeBinary, true)
            .with_metadata(metadata.clone())
            .try_into()
            .unwrap();
        binary_field.binary_blob_mut();
        assert!(binary_field.metadata.contains_key(BLOB_META_KEY));
        assert!(!binary_field.is_blob_v2());

        let mut field: Field = ArrowField::new("blob", DataType::LargeBinary, true)
            .with_metadata(metadata)
            .try_into()
            .unwrap();
        field.unloaded_mut();
        assert_eq!(field.children.len(), 2);
        assert_eq!(field.logical_type, BLOB_DESC_LANCE_FIELD.logical_type);
        assert!(field.is_blob());
        assert!(!field.is_blob_v2());
        field.unloaded_mut();
        assert_eq!(field.children.len(), 2);
        assert_eq!(field.logical_type, BLOB_DESC_LANCE_FIELD.logical_type);
        assert!(!field.is_blob_v2());

        let metadata =
            HashMap::from([(ARROW_EXT_NAME_KEY.to_string(), BLOB_V2_EXT_NAME.to_string())]);
        let mut field: Field = ArrowField::new(
            "blob",
            DataType::Struct(
                vec![
                    ArrowField::new("data", DataType::LargeBinary, true),
                    ArrowField::new("uri", DataType::Utf8, true),
                ]
                .into(),
            ),
            true,
        )
        .with_metadata(metadata)
        .try_into()
        .unwrap();
        field.unloaded_mut();
        assert_eq!(field.children.len(), 5);
        assert_eq!(field.logical_type, BLOB_V2_DESC_LANCE_FIELD.logical_type);
        assert!(!field.metadata.contains_key(ARROW_EXT_NAME_KEY));
        assert!(field.is_blob_v2());
        field.unloaded_mut();
        assert_eq!(field.children.len(), 5);
        assert_eq!(field.logical_type, BLOB_V2_DESC_LANCE_FIELD.logical_type);
        assert!(!field.metadata.contains_key(ARROW_EXT_NAME_KEY));
    }

    #[test]
    fn unload_blobs_recursive_only_unloads_blob_v2() {
        let legacy_metadata = HashMap::from([(BLOB_META_KEY.to_string(), "true".to_string())]);
        let blob_v2_metadata =
            HashMap::from([(ARROW_EXT_NAME_KEY.to_string(), BLOB_V2_EXT_NAME.to_string())]);

        let mut field: Field = ArrowField::new(
            "parent",
            DataType::Struct(Fields::from(vec![
                ArrowField::new("legacy_blob", DataType::LargeBinary, true)
                    .with_metadata(legacy_metadata),
                ArrowField::new(
                    "blob_v2",
                    DataType::Struct(
                        vec![
                            ArrowField::new("data", DataType::LargeBinary, true),
                            ArrowField::new("uri", DataType::Utf8, true),
                        ]
                        .into(),
                    ),
                    true,
                )
                .with_metadata(blob_v2_metadata),
            ])),
            true,
        )
        .try_into()
        .unwrap();

        field.unload_blobs_recursive();

        let legacy_blob = field
            .children
            .iter()
            .find(|f| f.name == "legacy_blob")
            .unwrap();
        assert_eq!(
            legacy_blob.logical_type,
            LogicalType::try_from(&DataType::LargeBinary).unwrap()
        );
        assert_eq!(legacy_blob.children.len(), 0);
        assert!(legacy_blob.metadata.contains_key(BLOB_META_KEY));

        let blob_v2 = field.children.iter().find(|f| f.name == "blob_v2").unwrap();
        assert_eq!(blob_v2.logical_type, BLOB_V2_DESC_LANCE_FIELD.logical_type);
        assert_eq!(blob_v2.children.len(), 5);
    }

    #[test]
    fn project_by_field_accepts_blob_descriptor_projection() {
        let metadata = HashMap::from([(BLOB_META_KEY.to_string(), "true".to_string())]);
        let field: Field = ArrowField::new("blob", DataType::LargeBinary, true)
            .with_metadata(metadata)
            .try_into()
            .unwrap();
        let mut unloaded = field.clone();
        unloaded.unloaded_mut();

        let projected = field
            .project_by_field(&unloaded, OnTypeMismatch::Error)
            .unwrap();
        assert_eq!(projected, field);

        let unloaded_projected = unloaded
            .project_by_field(&field, OnTypeMismatch::Error)
            .unwrap();
        assert_eq!(unloaded_projected, unloaded);
    }

    #[test]
    fn blob_descriptor_projection_preserves_synthetic_children() {
        let metadata =
            HashMap::from([(ARROW_EXT_NAME_KEY.to_string(), BLOB_V2_EXT_NAME.to_string())]);
        let mut blob: Field = ArrowField::new(
            "blob",
            DataType::Struct(
                vec![
                    ArrowField::new("data", DataType::LargeBinary, true),
                    ArrowField::new("uri", DataType::Utf8, true),
                ]
                .into(),
            ),
            true,
        )
        .with_metadata(metadata)
        .try_into()
        .unwrap();
        let mut next_id = 0;
        blob.set_id(-1, &mut next_id);

        let schema = Arc::new(crate::datatypes::Schema {
            fields: vec![blob],
            metadata: HashMap::new(),
        });
        let descriptor_schema = Projection::full(schema)
            .with_blob_handling(crate::datatypes::BlobHandling::BlobsDescriptions)
            .to_bare_schema();
        assert!(
            descriptor_schema.fields[0]
                .children
                .iter()
                .all(|child| child.id == -1)
        );

        let projected = Projection::full(Arc::new(descriptor_schema)).to_bare_schema();
        assert_eq!(projected.fields[0].children.len(), 5);
    }
}
