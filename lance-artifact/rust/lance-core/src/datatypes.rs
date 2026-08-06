// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Lance data types, [Schema] and [Field]

use std::collections::HashMap;
use std::fmt::{self, Debug, Formatter};
use std::sync::{Arc, LazyLock};

use crate::deepsize::DeepSizeOf;
use arrow_array::ArrayRef;
use arrow_schema::{DataType, Field as ArrowField, Fields, TimeUnit};
use lance_arrow::bfloat16::{BFLOAT16_EXT_NAME, is_bfloat16_field};
use lance_arrow::{ARROW_EXT_META_KEY, ARROW_EXT_NAME_KEY};

mod field;
mod schema;

use crate::{Error, Result};
pub use field::{
    BlobVersion, Encoding, Field, LANCE_UNENFORCED_CLUSTERING_KEY_POSITION,
    LANCE_UNENFORCED_PRIMARY_KEY, LANCE_UNENFORCED_PRIMARY_KEY_POSITION, NullabilityComparison,
    OnTypeMismatch, SchemaCompareOptions,
};
pub use schema::{
    BlobHandling, FieldRef, OnMissing, Projectable, Projection, Schema,
    escape_field_path_for_project, format_field_path, format_field_path_minimal, parse_field_path,
    validate_fixed_size_list_dimensions,
};

pub static BLOB_DESC_FIELDS: LazyLock<Fields> = LazyLock::new(|| {
    Fields::from(vec![
        ArrowField::new("position", DataType::UInt64, true),
        ArrowField::new("size", DataType::UInt64, true),
    ])
});

pub static BLOB_DESC_TYPE: LazyLock<DataType> =
    LazyLock::new(|| DataType::Struct(BLOB_DESC_FIELDS.clone()));

pub static BLOB_DESC_FIELD: LazyLock<ArrowField> = LazyLock::new(|| {
    ArrowField::new("description", BLOB_DESC_TYPE.clone(), true).with_metadata(HashMap::from([(
        lance_arrow::BLOB_META_KEY.to_string(),
        "true".to_string(),
    )]))
});

pub static BLOB_DESC_LANCE_FIELD: LazyLock<Field> =
    LazyLock::new(|| Field::try_from(&*BLOB_DESC_FIELD).unwrap());

/// The minimal logical blob v2 fields accepted from writers.
///
/// Logical values may also use [`BLOB_V2_LOGICAL_FIELDS`] when an external
/// object range is present.
pub static BLOB_V2_LOGICAL_MINIMAL_FIELDS: LazyLock<Fields> = LazyLock::new(|| {
    Fields::from(vec![
        ArrowField::new("data", DataType::LargeBinary, true),
        ArrowField::new("uri", DataType::Utf8, true),
    ])
});

/// The complete logical blob v2 fields used for writer input and rewrite output.
///
/// `position` and `size` are an optional range within the external object named
/// by `uri`. They do not describe Lance-managed data, packed, or dedicated
/// storage.
pub static BLOB_V2_LOGICAL_FIELDS: LazyLock<Fields> = LazyLock::new(|| {
    let mut fields = BLOB_V2_LOGICAL_MINIMAL_FIELDS
        .iter()
        .cloned()
        .collect::<Vec<_>>();
    fields.extend([
        Arc::new(ArrowField::new("position", DataType::UInt64, true)),
        Arc::new(ArrowField::new("size", DataType::UInt64, true)),
    ]);
    Fields::from(fields)
});

/// The complete logical blob v2 struct type.
pub static BLOB_V2_LOGICAL_TYPE: LazyLock<DataType> =
    LazyLock::new(|| DataType::Struct(BLOB_V2_LOGICAL_FIELDS.clone()));

/// Writer-prepared blob v2 fields consumed by the structural encoder.
///
/// The populated fields depend on [`BlobKind`]:
///
/// - [`BlobKind::Inline`] carries `data`; the encoder derives the stored
///   `position` and `size` from the out-of-line buffer it creates.
/// - [`BlobKind::Packed`] carries `blob_id`, `position`, and `blob_size`.
/// - [`BlobKind::Dedicated`] carries `blob_id` and `blob_size`; its stored
///   `position` is zero.
/// - [`BlobKind::External`] carries `uri`, optional `blob_id`, `position`, and
///   `blob_size`. A zero `blob_size` is resolved to the complete external object
///   length when read.
///
/// `blob_size` is distinct from the logical `size`, which is only an optional
/// external-object range before preparation. For external blobs, `uri` is
/// normalized into the stable stored `blob_uri` field.
pub static BLOB_V2_PREPARED_FIELDS: LazyLock<Fields> = LazyLock::new(|| {
    Fields::from(vec![
        ArrowField::new("kind", DataType::UInt8, true),
        ArrowField::new("data", DataType::LargeBinary, true),
        ArrowField::new("uri", DataType::Utf8, true),
        ArrowField::new("blob_id", DataType::UInt32, true),
        ArrowField::new("blob_size", DataType::UInt64, true),
        ArrowField::new("position", DataType::UInt64, true),
    ])
});

/// The writer-prepared blob v2 struct type.
pub static BLOB_V2_PREPARED_TYPE: LazyLock<DataType> =
    LazyLock::new(|| DataType::Struct(BLOB_V2_PREPARED_FIELDS.clone()));

/// Stored blob v2 descriptor fields.
///
/// These field names are part of the stable file format. Their meaning depends
/// on `kind`:
///
/// - [`BlobKind::Inline`]: `position` and `size` locate an out-of-line buffer in
///   the Lance data file.
/// - [`BlobKind::Packed`]: `blob_id` identifies a shared packed blob file, and
///   `position` and `size` locate a range within it.
/// - [`BlobKind::Dedicated`]: `blob_id` identifies a dedicated raw blob file,
///   `position` is zero, and `size` is the complete file length.
/// - [`BlobKind::External`]: `blob_uri` and `blob_id` identify the object, while
///   `position` and `size` select a range. A zero `size` is resolved to the
///   object's complete length when read.
pub static BLOB_V2_DESC_FIELDS: LazyLock<Fields> = LazyLock::new(|| {
    Fields::from(vec![
        ArrowField::new("kind", DataType::UInt8, false),
        ArrowField::new("position", DataType::UInt64, false),
        ArrowField::new("size", DataType::UInt64, false),
        ArrowField::new("blob_id", DataType::UInt32, false),
        ArrowField::new("blob_uri", DataType::Utf8, false),
    ])
});

pub static BLOB_V2_DESC_TYPE: LazyLock<DataType> =
    LazyLock::new(|| DataType::Struct(BLOB_V2_DESC_FIELDS.clone()));

pub static BLOB_V2_DESC_FIELD: LazyLock<ArrowField> = LazyLock::new(|| {
    ArrowField::new("description", BLOB_V2_DESC_TYPE.clone(), false).with_metadata(HashMap::from([
        (lance_arrow::BLOB_META_KEY.to_string(), "true".to_string()),
        ("lance-encoding:packed".to_string(), "true".to_string()),
    ]))
});

pub static BLOB_V2_DESC_LANCE_FIELD: LazyLock<Field> =
    LazyLock::new(|| Field::try_from(&*BLOB_V2_DESC_FIELD).unwrap());

/// The in-memory representation of a blob v2 struct.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BlobV2Layout {
    /// Writer input or rewrite output.
    ///
    /// Both the minimal `data, uri` fields and the complete
    /// `data, uri, position, size` fields have this layout.
    Logical,
    /// Kind-aware writer intermediate consumed by the structural encoder.
    Prepared,
    /// Stable descriptor stored in Lance files and returned by descriptor scans.
    Descriptor,
}

impl BlobV2Layout {
    /// Classify blob v2 child fields by name, type, order, and layout-specific
    /// nullability requirements.
    ///
    /// Child metadata is not part of the representation. The complete logical
    /// layout also accepts non-nullable `position` and `size` fields, matching
    /// the existing writer-input contract. Descriptor child nullability is
    /// ignored because it changed across released schemas; row nullness has
    /// been represented by either parent struct validity or a nullable `kind`
    /// child.
    pub fn classify(fields: &Fields) -> Option<Self> {
        if logical_blob_v2_fields_match(fields) {
            Some(Self::Logical)
        } else if blob_v2_fields_match(fields, &BLOB_V2_PREPARED_FIELDS, true) {
            Some(Self::Prepared)
        } else if blob_v2_fields_match(fields, &BLOB_V2_DESC_FIELDS, false) {
            Some(Self::Descriptor)
        } else {
            None
        }
    }
}

impl fmt::Display for BlobV2Layout {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Logical => write!(f, "logical"),
            Self::Prepared => write!(f, "prepared"),
            Self::Descriptor => write!(f, "descriptor"),
        }
    }
}

fn blob_v2_field_matches(
    actual: &ArrowField,
    expected: &ArrowField,
    compare_nullability: bool,
) -> bool {
    actual.name() == expected.name()
        && actual.data_type() == expected.data_type()
        && (!compare_nullability || actual.is_nullable() == expected.is_nullable())
}

fn blob_v2_fields_match(actual: &Fields, expected: &Fields, compare_nullability: bool) -> bool {
    actual.len() == expected.len()
        && actual
            .iter()
            .zip(expected.iter())
            .all(|(actual, expected)| {
                blob_v2_field_matches(actual.as_ref(), expected.as_ref(), compare_nullability)
            })
}

fn logical_blob_v2_fields_match(fields: &Fields) -> bool {
    if blob_v2_fields_match(fields, &BLOB_V2_LOGICAL_MINIMAL_FIELDS, true) {
        return true;
    }
    fields.len() == BLOB_V2_LOGICAL_FIELDS.len()
        && fields
            .iter()
            .zip(BLOB_V2_LOGICAL_FIELDS.iter())
            .enumerate()
            .all(|(index, (actual, expected))| {
                blob_v2_field_matches(actual.as_ref(), expected.as_ref(), index < 2)
            })
}

/// Deprecated name for [`BLOB_V2_LOGICAL_FIELDS`].
#[deprecated(note = "use BLOB_V2_LOGICAL_FIELDS")]
pub use self::BLOB_V2_LOGICAL_FIELDS as BLOB_V2_USER_FIELDS;

/// Deprecated name for [`BLOB_V2_LOGICAL_TYPE`].
#[deprecated(note = "use BLOB_V2_LOGICAL_TYPE")]
pub use self::BLOB_V2_LOGICAL_TYPE as BLOB_V2_USER_TYPE;

pub const BLOB_LOGICAL_TYPE: &str = "blob";

/// LogicalType is a string presentation of arrow type.
/// to be serialized into protobuf.
#[derive(Debug, Clone, PartialEq, DeepSizeOf)]
pub struct LogicalType(String);

impl fmt::Display for LogicalType {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl LogicalType {
    fn is_list(&self) -> bool {
        self.0 == "list" || self.0 == "list.struct"
    }

    fn is_large_list(&self) -> bool {
        self.0 == "large_list" || self.0 == "large_list.struct"
    }

    fn is_fixed_size_list_struct(&self) -> bool {
        self.0.starts_with("fixed_size_list:struct:")
    }

    pub fn is_struct(&self) -> bool {
        self.0 == "struct"
    }

    fn is_blob(&self) -> bool {
        self.0 == BLOB_LOGICAL_TYPE
    }

    fn is_map(&self) -> bool {
        self.0 == "map"
    }
}

impl From<&str> for LogicalType {
    fn from(s: &str) -> Self {
        Self(s.to_string())
    }
}

fn timeunit_to_str(unit: &TimeUnit) -> &'static str {
    match unit {
        TimeUnit::Second => "s",
        TimeUnit::Millisecond => "ms",
        TimeUnit::Microsecond => "us",
        TimeUnit::Nanosecond => "ns",
    }
}

fn is_supported_fixed_size_list_child(data_type: &DataType, nested: bool) -> bool {
    match data_type {
        DataType::Struct(_) => !nested,
        DataType::List(_) | DataType::LargeList(_) | DataType::Map(_, _) => false,
        DataType::FixedSizeList(field, _) => {
            is_supported_fixed_size_list_child(field.data_type(), true)
        }
        _ => true,
    }
}

fn parse_timeunit(unit: &str) -> Result<TimeUnit> {
    match unit {
        "s" => Ok(TimeUnit::Second),
        "ms" => Ok(TimeUnit::Millisecond),
        "us" => Ok(TimeUnit::Microsecond),
        "ns" => Ok(TimeUnit::Nanosecond),
        _ => Err(Error::arrow(format!("Unsupported TimeUnit: {unit}"))),
    }
}

impl TryFrom<&DataType> for LogicalType {
    type Error = Error;

    fn try_from(dt: &DataType) -> Result<Self> {
        let type_str = match dt {
            DataType::Null => "null".to_string(),
            DataType::Boolean => "bool".to_string(),
            DataType::Int8 => "int8".to_string(),
            DataType::UInt8 => "uint8".to_string(),
            DataType::Int16 => "int16".to_string(),
            DataType::UInt16 => "uint16".to_string(),
            DataType::Int32 => "int32".to_string(),
            DataType::UInt32 => "uint32".to_string(),
            DataType::Int64 => "int64".to_string(),
            DataType::UInt64 => "uint64".to_string(),
            DataType::Float16 => "halffloat".to_string(),
            DataType::Float32 => "float".to_string(),
            DataType::Float64 => "double".to_string(),
            DataType::Decimal128(precision, scale) => format!("decimal:128:{precision}:{scale}"),
            DataType::Decimal256(precision, scale) => format!("decimal:256:{precision}:{scale}"),
            DataType::Utf8 | DataType::Utf8View => "string".to_string(),
            DataType::Binary | DataType::BinaryView => "binary".to_string(),
            DataType::LargeUtf8 => "large_string".to_string(),
            DataType::LargeBinary => "large_binary".to_string(),
            DataType::Date32 => "date32:day".to_string(),
            DataType::Date64 => "date64:ms".to_string(),
            DataType::Time32(tu) => format!("time32:{}", timeunit_to_str(tu)),
            DataType::Time64(tu) => format!("time64:{}", timeunit_to_str(tu)),
            DataType::Timestamp(tu, tz) => format!(
                "timestamp:{}:{}",
                timeunit_to_str(tu),
                tz.as_ref()
                    .map(|v| v.to_string())
                    .unwrap_or("-".to_string())
            ),
            DataType::Duration(tu) => format!("duration:{}", timeunit_to_str(tu)),
            DataType::Struct(_) => "struct".to_string(),
            DataType::Dictionary(key_type, value_type) => {
                format!(
                    "dict:{}:{}:{}",
                    Self::try_from(value_type.as_ref())?.0,
                    Self::try_from(key_type.as_ref())?.0,
                    // Arrow C++ Dictionary has "ordered:bool" field, but it does not exist in `arrow-rs`.
                    false
                )
            }
            DataType::List(elem) => match elem.data_type() {
                DataType::Struct(_) => "list.struct".to_string(),
                _ => "list".to_string(),
            },
            DataType::LargeList(elem) => match elem.data_type() {
                DataType::Struct(_) => "large_list.struct".to_string(),
                _ => "large_list".to_string(),
            },
            DataType::FixedSizeList(field, len) => {
                if is_bfloat16_field(field) {
                    // Don't want to directly use `bfloat16`, in case a built-in type is added
                    // that isn't identical to our extension type.
                    format!("fixed_size_list:lance.bfloat16:{}", *len)
                } else if !is_supported_fixed_size_list_child(field.data_type(), false) {
                    return Err(Error::schema(format!("Unsupported data type: {:?}", dt)));
                } else {
                    format!(
                        "fixed_size_list:{}:{}",
                        Self::try_from(field.data_type())?.0,
                        *len
                    )
                }
            }
            DataType::FixedSizeBinary(len) => format!("fixed_size_binary:{}", *len),
            DataType::Map(_, keys_sorted) => {
                // TODO: We only support keys_sorted=false for now,
                //  because converting a rust arrow map field to the python arrow field will
                //  lose the keys_sorted property.
                if *keys_sorted {
                    return Err(Error::schema(format!(
                        "Unsupported map data type with keys_sorted=true: {:?}",
                        dt
                    )));
                }
                "map".to_string()
            }
            _ => {
                return Err(Error::schema(format!("Unsupported data type: {:?}", dt)));
            }
        };

        Ok(Self(type_str))
    }
}

impl TryFrom<&LogicalType> for DataType {
    type Error = Error;

    fn try_from(lt: &LogicalType) -> Result<Self> {
        use DataType::*;
        if let Some(t) = match lt.0.as_str() {
            "null" => Some(Null),
            "bool" => Some(Boolean),
            "int8" => Some(Int8),
            "uint8" => Some(UInt8),
            "int16" => Some(Int16),
            "uint16" => Some(UInt16),
            "int32" => Some(Int32),
            "uint32" => Some(UInt32),
            "int64" => Some(Int64),
            "uint64" => Some(UInt64),
            "halffloat" => Some(Float16),
            "float" => Some(Float32),
            "double" => Some(Float64),
            "string" => Some(Utf8),
            "binary" => Some(Binary),
            "large_string" => Some(LargeUtf8),
            "large_binary" => Some(LargeBinary),
            BLOB_LOGICAL_TYPE => Some(LargeBinary),
            "json" => Some(LargeBinary),
            "date32:day" => Some(Date32),
            "date64:ms" => Some(Date64),
            "time32:s" => Some(Time32(TimeUnit::Second)),
            "time32:ms" => Some(Time32(TimeUnit::Millisecond)),
            "time64:us" => Some(Time64(TimeUnit::Microsecond)),
            "time64:ns" => Some(Time64(TimeUnit::Nanosecond)),
            "duration:s" => Some(Duration(TimeUnit::Second)),
            "duration:ms" => Some(Duration(TimeUnit::Millisecond)),
            "duration:us" => Some(Duration(TimeUnit::Microsecond)),
            "duration:ns" => Some(Duration(TimeUnit::Nanosecond)),
            _ => None,
        } {
            Ok(t)
        } else {
            let splits = lt.0.split(':').collect::<Vec<_>>();
            match splits[0] {
                "fixed_size_list" => {
                    if splits.len() < 3 {
                        return Err(Error::schema(format!("Unsupported logical type: {}", lt)));
                    }

                    let size: i32 = splits
                        .last()
                        .unwrap()
                        .parse::<i32>()
                        .map_err(|e: _| Error::schema(e.to_string()))?;

                    let inner_type = splits[1..splits.len() - 1].join(":");

                    match inner_type.as_str() {
                        BFLOAT16_EXT_NAME => {
                            let field = ArrowField::new("item", Self::FixedSizeBinary(2), true)
                                .with_metadata(
                                    [
                                        (ARROW_EXT_NAME_KEY.into(), BFLOAT16_EXT_NAME.into()),
                                        (ARROW_EXT_META_KEY.into(), "".into()),
                                    ]
                                    .into(),
                                );
                            Ok(FixedSizeList(Arc::new(field), size))
                        }
                        data_type => {
                            let elem_type = (&LogicalType(data_type.to_string())).try_into()?;

                            Ok(FixedSizeList(
                                Arc::new(ArrowField::new("item", elem_type, true)),
                                size,
                            ))
                        }
                    }
                }
                "fixed_size_binary" => {
                    if splits.len() != 2 {
                        Err(Error::schema(format!("Unsupported logical type: {}", lt)))
                    } else {
                        let size: i32 = splits[1]
                            .parse::<i32>()
                            .map_err(|e: _| Error::schema(e.to_string()))?;
                        Ok(FixedSizeBinary(size))
                    }
                }
                "dict" => {
                    if splits.len() != 4 {
                        Err(Error::schema(format!(
                            "Unsupported dictionary type: {}",
                            lt
                        )))
                    } else {
                        let value_type: Self = (&LogicalType::from(splits[1])).try_into()?;
                        let index_type: Self = (&LogicalType::from(splits[2])).try_into()?;
                        Ok(Dictionary(Box::new(index_type), Box::new(value_type)))
                    }
                }
                "decimal" => {
                    if splits.len() != 4 {
                        Err(Error::schema(format!("Unsupported decimal type: {}", lt)))
                    } else {
                        let bits: i16 = splits[1]
                            .parse::<i16>()
                            .map_err(|err| Error::schema(err.to_string()))?;
                        let precision: u8 = splits[2]
                            .parse::<u8>()
                            .map_err(|err| Error::schema(err.to_string()))?;
                        let scale: i8 = splits[3]
                            .parse::<i8>()
                            .map_err(|err| Error::schema(err.to_string()))?;

                        if bits == 128 {
                            Ok(Decimal128(precision, scale))
                        } else if bits == 256 {
                            Ok(Decimal256(precision, scale))
                        } else {
                            Err(Error::schema(format!(
                                "Only Decimal128 and Decimal256 is supported. Found {bits}"
                            )))
                        }
                    }
                }
                "timestamp" => {
                    if splits.len() != 3 {
                        Err(Error::schema(format!("Unsupported timestamp type: {}", lt)))
                    } else {
                        let timeunit = parse_timeunit(splits[1])?;
                        let tz: Option<Arc<str>> = if splits[2] == "-" {
                            None
                        } else {
                            Some(splits[2].into())
                        };
                        Ok(Timestamp(timeunit, tz))
                    }
                }
                _ => Err(Error::schema(format!("Unsupported logical type: {}", lt))),
            }
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct Dictionary {
    pub offset: usize,

    pub length: usize,

    pub values: Option<ArrayRef>,
}

impl DeepSizeOf for Dictionary {
    fn deep_size_of_children(&self, context: &mut crate::deepsize::Context) -> usize {
        self.values
            .as_ref()
            .map(|v| (v.as_ref() as &dyn arrow_array::Array).deep_size_of_children(context))
            .unwrap_or(0)
    }
}

impl PartialEq for Dictionary {
    fn eq(&self, other: &Self) -> bool {
        match (&self.values, &other.values) {
            (Some(a), Some(b)) => a == b,
            _ => false,
        }
    }
}

/// Physical storage mode for blob v2 descriptors (one byte, stored in the packed struct column).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum BlobKind {
    /// Stored in the main data file’s out-of-line buffer; `position`/`size` point into that file.
    Inline = 0,
    /// Stored in a shared packed blob file; `position`/`size` locate the slice, `blob_id` selects the file.
    Packed = 1,
    /// Stored in a dedicated raw blob file; `blob_id` identifies the file, `size` is the full file length.
    Dedicated = 2,
    /// Not stored by Lance data files.
    ///
    /// For external blobs:
    /// - `blob_id == 0` means `blob_uri` is an absolute external URI.
    /// - `blob_id > 0` means `blob_uri` is a path relative to `manifest.base_paths[blob_id]`.
    ///
    /// External blobs can have a position and a size. If the position is not set,
    /// it defaults to 0, which points to the beginning of the blob.
    External = 3,
}

impl TryFrom<u8> for BlobKind {
    type Error = Error;

    fn try_from(value: u8) -> Result<Self> {
        match value {
            0 => Ok(Self::Inline),
            1 => Ok(Self::Packed),
            2 => Ok(Self::Dedicated),
            3 => Ok(Self::External),
            other => Err(Error::invalid_input_source(
                format!("Unknown blob kind {other:?}").into(),
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_classify_blob_v2_layouts() {
        assert_eq!(
            BlobV2Layout::classify(&BLOB_V2_LOGICAL_MINIMAL_FIELDS),
            Some(BlobV2Layout::Logical)
        );
        assert_eq!(
            BlobV2Layout::classify(&BLOB_V2_LOGICAL_FIELDS),
            Some(BlobV2Layout::Logical)
        );
        assert_eq!(
            BlobV2Layout::classify(&BLOB_V2_PREPARED_FIELDS),
            Some(BlobV2Layout::Prepared)
        );
        assert_eq!(
            BlobV2Layout::classify(&BLOB_V2_DESC_FIELDS),
            Some(BlobV2Layout::Descriptor)
        );
    }

    #[test]
    fn test_classify_blob_v2_layout_uses_structural_contract() {
        let logical_with_required_range = Fields::from(vec![
            ArrowField::new("data", DataType::LargeBinary, true),
            ArrowField::new("uri", DataType::Utf8, true),
            ArrowField::new("position", DataType::UInt64, false),
            ArrowField::new("size", DataType::UInt64, false),
        ]);
        assert_eq!(
            BlobV2Layout::classify(&logical_with_required_range),
            Some(BlobV2Layout::Logical)
        );

        let prepared_with_child_metadata =
            Fields::from(
                BLOB_V2_PREPARED_FIELDS
                    .iter()
                    .map(|field| {
                        Arc::new(field.as_ref().clone().with_metadata(HashMap::from([(
                            "source".to_string(),
                            "test".to_string(),
                        )])))
                    })
                    .collect::<Vec<_>>(),
            );
        assert_eq!(
            BlobV2Layout::classify(&prepared_with_child_metadata),
            Some(BlobV2Layout::Prepared)
        );

        let nullable_descriptor = Fields::from(
            BLOB_V2_DESC_FIELDS
                .iter()
                .map(|field| Arc::new(field.as_ref().clone().with_nullable(true)))
                .collect::<Vec<_>>(),
        );
        assert_eq!(
            BlobV2Layout::classify(&nullable_descriptor),
            Some(BlobV2Layout::Descriptor)
        );

        let malformed_descriptor = Fields::from(vec![
            ArrowField::new("kind", DataType::UInt8, false),
            ArrowField::new("position", DataType::UInt64, false),
            ArrowField::new("size", DataType::UInt32, false),
            ArrowField::new("blob_id", DataType::UInt32, false),
            ArrowField::new("blob_uri", DataType::Utf8, false),
        ]);
        assert_eq!(BlobV2Layout::classify(&malformed_descriptor), None);
    }
}
