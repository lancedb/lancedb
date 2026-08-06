// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use lance_arrow::ARROW_EXT_NAME_KEY;
use lance_core::datatypes::{Dictionary, Encoding, Field, LogicalType, Schema};
use lance_core::{Error, Result};
use std::collections::HashMap;

use crate::format::pb;

#[allow(clippy::fallible_impl_from)]
impl From<&pb::Field> for Field {
    fn from(field: &pb::Field) -> Self {
        let lance_metadata: HashMap<String, String> = field
            .metadata
            .iter()
            .map(|(key, value)| {
                let string_value = String::from_utf8_lossy(value).to_string();
                (key.clone(), string_value)
            })
            .collect();
        let mut lance_metadata = lance_metadata;
        if !field.extension_name.is_empty() {
            lance_metadata.insert(ARROW_EXT_NAME_KEY.to_string(), field.extension_name.clone());
        }
        Self {
            name: field.name.clone(),
            id: field.id,
            parent_id: field.parent_id,
            logical_type: LogicalType::from(field.logical_type.as_str()),
            metadata: lance_metadata,
            encoding: match field.encoding {
                1 => Some(Encoding::Plain),
                2 => Some(Encoding::VarBinary),
                3 => Some(Encoding::Dictionary),
                4 => Some(Encoding::RLE),
                _ => None,
            },
            nullable: field.nullable,
            children: vec![],
            dictionary: field.dictionary.as_ref().map(Dictionary::from),
            unenforced_primary_key_position: if field.unenforced_primary_key_position > 0 {
                Some(field.unenforced_primary_key_position)
            } else if field.unenforced_primary_key {
                Some(0)
            } else {
                None
            },
            unenforced_clustering_key_position: if field.unenforced_clustering_key_position > 0 {
                Some(field.unenforced_clustering_key_position)
            } else {
                None
            },
        }
    }
}

impl From<&Field> for pb::Field {
    fn from(field: &Field) -> Self {
        let pb_metadata = field
            .metadata
            .iter()
            .map(|(key, value)| (key.clone(), value.clone().into_bytes()))
            .collect();
        Self {
            id: field.id,
            parent_id: field.parent_id,
            name: field.name.clone(),
            logical_type: field.logical_type.to_string(),
            encoding: match field.encoding {
                Some(Encoding::Plain) => 1,
                Some(Encoding::VarBinary) => 2,
                Some(Encoding::Dictionary) => 3,
                Some(Encoding::RLE) => 4,
                _ => 0,
            },
            nullable: field.nullable,
            dictionary: field.dictionary.as_ref().map(pb::Dictionary::from),
            metadata: pb_metadata,
            extension_name: field
                .extension_name()
                .map(|name| name.to_owned())
                .unwrap_or_default(),
            r#type: 0,
            unenforced_primary_key: field.unenforced_primary_key_position.is_some(),
            unenforced_primary_key_position: field.unenforced_primary_key_position.unwrap_or(0),
            unenforced_clustering_key: false,
            unenforced_clustering_key_position: field
                .unenforced_clustering_key_position
                .unwrap_or(0),
        }
    }
}

pub struct Fields(pub Vec<pb::Field>);

struct FieldNode {
    field: Field,
    child_indices: Vec<usize>,
}

/// Searches in pre-order depth-first order and returns the first matching node,
/// preserving the legacy parent tie-break for duplicate field IDs.
fn first_field_index_by_id(
    nodes: &[FieldNode],
    root_indices: &[usize],
    field_id: i32,
) -> Option<usize> {
    let mut to_visit = Vec::with_capacity(nodes.len());
    to_visit.extend(root_indices.iter().rev().copied());

    while let Some(node_index) = to_visit.pop() {
        let node = &nodes[node_index];
        if node.field.id == field_id {
            return Some(node_index);
        }
        to_visit.extend(node.child_indices.iter().rev().copied());
    }

    None
}

impl From<&Field> for Fields {
    fn from(field: &Field) -> Self {
        let mut protos = vec![pb::Field::from(field)];
        protos.extend(field.children.iter().flat_map(|val| Self::from(val).0));
        Self(protos)
    }
}

/// Reconstruct a schema from a flat, pre-order protobuf field list.
///
/// Parent fields must appear before their children. Historical manifests may
/// contain duplicate field IDs, so an ID may not identify a unique parent. For
/// those references, reconstruction preserves the legacy
/// [`Schema::mut_field_by_id`] tie-break by selecting the first matching field
/// in pre-order depth-first traversal.
///
/// # Examples
///
/// ```
/// use lance_core::datatypes::Schema;
/// use lance_file::{datatypes::Fields, format::pb};
///
/// let field = pb::Field {
///     id: 0,
///     parent_id: -1,
///     name: "value".to_owned(),
///     logical_type: "int32".to_owned(),
///     ..Default::default()
/// };
/// let fields = Fields(vec![field]);
/// let schema = Schema::try_from(&fields)?;
/// assert_eq!(schema.fields[0].name, "value");
/// # Ok::<(), lance_core::Error>(())
/// ```
impl TryFrom<&Fields> for Schema {
    type Error = Error;

    fn try_from(fields: &Fields) -> Result<Self> {
        let mut nodes: Vec<FieldNode> = Vec::with_capacity(fields.0.len());
        let mut root_indices = Vec::with_capacity(fields.0.len());
        let mut field_indices: HashMap<i32, Option<usize>> = HashMap::with_capacity(fields.0.len());

        for proto_field in &fields.0 {
            let parent_index = if proto_field.parent_id == -1 {
                None
            } else {
                let parent_index = match field_indices.get(&proto_field.parent_id) {
                    Some(Some(parent_index)) => *parent_index,
                    Some(None) => {
                        // Duplicate IDs are invalid but occur in historical
                        // manifests. Match the legacy tree traversal only for
                        // these ambiguous parent references so valid schemas
                        // retain the linear fast path.
                        first_field_index_by_id(&nodes, &root_indices, proto_field.parent_id)
                            .ok_or_else(|| {
                                Error::internal(format!(
                                    "Duplicate field id {} has no existing arena node",
                                    proto_field.parent_id
                                ))
                            })?
                    }
                    None => {
                        return Err(Error::schema(format!(
                            "Field '{}' (id={}) references parent id {}, which must appear earlier in the protobuf field list",
                            proto_field.name, proto_field.id, proto_field.parent_id
                        )));
                    }
                };
                Some(parent_index)
            };

            let node_index = nodes.len();
            if let Some(parent_index) = parent_index {
                nodes[parent_index].child_indices.push(node_index);
            } else {
                root_indices.push(node_index);
            }
            nodes.push(FieldNode {
                field: Field::from(proto_field),
                child_indices: Vec::new(),
            });

            field_indices
                .entry(proto_field.id)
                .and_modify(|field_index| *field_index = None)
                .or_insert(Some(node_index));
        }

        let mut fields_by_node = Vec::with_capacity(nodes.len());
        fields_by_node.resize_with(nodes.len(), || None);
        for (node_index, mut node) in nodes.into_iter().enumerate().rev() {
            node.field.children.reserve(node.child_indices.len());
            for child_index in node.child_indices {
                let child = fields_by_node
                    .get_mut(child_index)
                    .and_then(Option::take)
                    .ok_or_else(|| {
                        Error::internal(format!(
                            "Schema field arena node {child_index} was not materialized before its parent"
                        ))
                    })?;
                node.field.children.push(child);
            }
            fields_by_node[node_index] = Some(node.field);
        }

        let fields = root_indices
            .into_iter()
            .map(|root_index| {
                fields_by_node[root_index].take().ok_or_else(|| {
                    Error::internal(format!(
                        "Schema field arena root node {root_index} was not materialized"
                    ))
                })
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(Self {
            fields,
            metadata: HashMap::default(),
        })
    }
}

pub struct FieldsWithMeta {
    pub fields: Fields,
    pub metadata: HashMap<String, Vec<u8>>,
}

/// Reconstruct a schema from flat protobuf fields and schema metadata.
///
/// # Examples
///
/// ```
/// use std::collections::HashMap;
///
/// use lance_core::datatypes::Schema;
/// use lance_file::datatypes::{Fields, FieldsWithMeta};
///
/// let fields = FieldsWithMeta {
///     fields: Fields(Vec::new()),
///     metadata: HashMap::from([("owner".to_owned(), b"lance".to_vec())]),
/// };
/// let schema = Schema::try_from(fields)?;
/// assert_eq!(schema.metadata["owner"], "lance");
/// # Ok::<(), lance_core::Error>(())
/// ```
impl TryFrom<FieldsWithMeta> for Schema {
    type Error = Error;

    fn try_from(fields_with_meta: FieldsWithMeta) -> Result<Self> {
        let lance_metadata = fields_with_meta
            .metadata
            .into_iter()
            .map(|(key, value)| {
                let string_value = String::from_utf8_lossy(&value).to_string();
                (key, string_value)
            })
            .collect();

        let schema_with_fields = Self::try_from(&fields_with_meta.fields)?;
        Ok(Self {
            fields: schema_with_fields.fields,
            metadata: lance_metadata,
        })
    }
}

/// Convert a Schema to a list of protobuf Field.
impl From<&Schema> for Fields {
    fn from(schema: &Schema) -> Self {
        let mut protos = vec![];
        schema.fields.iter().for_each(|f| {
            protos.extend(Self::from(f).0);
        });
        Self(protos)
    }
}

/// Convert a Schema to a list of protobuf Field and Metadata
impl From<&Schema> for FieldsWithMeta {
    fn from(schema: &Schema) -> Self {
        let fields = schema.into();
        let metadata = schema
            .metadata
            .clone()
            .into_iter()
            .map(|(key, value)| (key, value.into_bytes()))
            .collect();
        Self { fields, metadata }
    }
}

impl From<&pb::Dictionary> for Dictionary {
    fn from(proto: &pb::Dictionary) -> Self {
        Self {
            offset: proto.offset as usize,
            length: proto.length as usize,
            values: None,
        }
    }
}

impl From<&Dictionary> for pb::Dictionary {
    fn from(d: &Dictionary) -> Self {
        Self {
            offset: d.offset as i64,
            length: d.length as i64,
        }
    }
}

impl From<Encoding> for pb::Encoding {
    fn from(e: Encoding) -> Self {
        match e {
            Encoding::Plain => Self::Plain,
            Encoding::VarBinary => Self::VarBinary,
            Encoding::Dictionary => Self::Dictionary,
            Encoding::RLE => Self::Rle,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use arrow_schema::DataType;
    use arrow_schema::Field as ArrowField;
    use arrow_schema::Fields as ArrowFields;
    use arrow_schema::Schema as ArrowSchema;
    use lance_core::Error;
    use lance_core::datatypes::Schema;

    use super::{Fields, FieldsWithMeta};
    use crate::format::pb;

    fn proto_field(id: i32, parent_id: i32, name: String, logical_type: &str) -> pb::Field {
        pb::Field {
            id,
            parent_id,
            name,
            logical_type: logical_type.to_owned(),
            ..Default::default()
        }
    }

    #[test]
    fn test_schema_set_ids() {
        let arrow_schema = ArrowSchema::new(vec![
            ArrowField::new("a", DataType::Int32, false),
            ArrowField::new(
                "b",
                DataType::Struct(ArrowFields::from(vec![
                    ArrowField::new("f1", DataType::Utf8, true),
                    ArrowField::new("f2", DataType::Boolean, false),
                    ArrowField::new("f3", DataType::Float32, false),
                ])),
                true,
            ),
            ArrowField::new("c", DataType::Float64, false),
        ]);
        let schema = Schema::try_from(&arrow_schema).unwrap();

        let protos: Fields = (&schema).into();
        assert_eq!(
            protos.0.iter().map(|p| p.id).collect::<Vec<_>>(),
            (0..6).collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_schema_metadata() {
        let mut metadata: HashMap<String, String> = HashMap::new();
        metadata.insert(String::from("k1"), String::from("v1"));
        metadata.insert(String::from("k2"), String::from("v2"));

        let arrow_schema = ArrowSchema::new_with_metadata(
            vec![ArrowField::new("a", DataType::Int32, false)],
            metadata,
        );

        let expected_schema = Schema::try_from(&arrow_schema).unwrap();
        let fields_with_meta: FieldsWithMeta = (&expected_schema).into();

        let schema = Schema::try_from(fields_with_meta).unwrap();
        assert_eq!(expected_schema, schema);
    }

    #[test]
    fn test_reconstruct_wide_nested_schema() {
        const NUM_STRUCTS: usize = 4096;

        let mut proto_fields = Vec::with_capacity(NUM_STRUCTS * 3);
        for struct_index in 0..NUM_STRUCTS {
            let parent_id = (struct_index * 3) as i32;
            proto_fields.push(proto_field(
                parent_id,
                -1,
                format!("struct_{struct_index}"),
                "struct",
            ));
            proto_fields.push(proto_field(
                parent_id + 1,
                parent_id,
                format!("left_{struct_index}"),
                "int32",
            ));
            proto_fields.push(proto_field(
                parent_id + 2,
                parent_id,
                format!("right_{struct_index}"),
                "int32",
            ));
        }

        let fields = Fields(proto_fields);
        let schema = Schema::try_from(&fields).unwrap();
        assert_eq!(schema.fields.len(), NUM_STRUCTS);
        for (struct_index, field) in schema.fields.iter().enumerate() {
            let parent_id = (struct_index * 3) as i32;
            assert_eq!(field.id, parent_id);
            assert_eq!(field.name, format!("struct_{struct_index}"));
            assert_eq!(field.children.len(), 2);
            assert_eq!(field.children[0].id, parent_id + 1);
            assert_eq!(field.children[0].name, format!("left_{struct_index}"));
            assert_eq!(field.children[1].id, parent_id + 2);
            assert_eq!(field.children[1].name, format!("right_{struct_index}"));
        }
    }

    #[test]
    fn test_reconstruct_deep_nested_schema() {
        const DEPTH: usize = 1024;

        let proto_fields = (0..DEPTH)
            .map(|depth| {
                proto_field(
                    depth as i32,
                    if depth == 0 { -1 } else { depth as i32 - 1 },
                    format!("level_{depth}"),
                    if depth + 1 == DEPTH {
                        "int32"
                    } else {
                        "struct"
                    },
                )
            })
            .collect();

        let fields = Fields(proto_fields);
        let schema = Schema::try_from(&fields).unwrap();
        assert_eq!(schema.fields.len(), 1);
        let mut field = &schema.fields[0];
        for depth in 0..DEPTH {
            assert_eq!(field.id, depth as i32);
            assert_eq!(field.name, format!("level_{depth}"));
            if depth + 1 == DEPTH {
                assert!(field.children.is_empty());
            } else {
                assert_eq!(field.children.len(), 1);
                field = &field.children[0];
            }
        }
    }

    #[test]
    fn test_reconstruct_schema_reports_missing_parent() {
        let fields = Fields(vec![proto_field(7, 42, "child".to_owned(), "int32")]);

        let error = Schema::try_from(&fields).unwrap_err();
        assert!(matches!(&error, Error::Schema { .. }));
        assert!(
            error.to_string().contains(
                "Field 'child' (id=7) references parent id 42, which must appear earlier"
            )
        );
    }

    #[test]
    fn test_reconstruct_schema_preserves_legacy_duplicate_id_match() {
        let fields = Fields(vec![
            proto_field(1, -1, "root_a".to_owned(), "struct"),
            proto_field(2, -1, "root_b".to_owned(), "struct"),
            proto_field(2, 1, "nested_duplicate".to_owned(), "struct"),
            proto_field(3, 2, "child".to_owned(), "int32"),
        ]);

        let schema = Schema::try_from(&fields).unwrap();
        assert_eq!(schema.fields.len(), 2);
        assert_eq!(schema.fields[0].name, "root_a");
        assert_eq!(schema.fields[0].children.len(), 1);
        assert_eq!(schema.fields[0].children[0].name, "nested_duplicate");
        assert_eq!(schema.fields[0].children[0].children.len(), 1);
        assert_eq!(schema.fields[0].children[0].children[0].name, "child");
        assert_eq!(schema.fields[1].name, "root_b");
        assert!(schema.fields[1].children.is_empty());
    }

    #[test]
    fn test_clustering_key_roundtrip() {
        let arrow_schema = ArrowSchema::new(vec![
            ArrowField::new("region", DataType::Utf8, true).with_metadata(
                vec![(
                    "lance-schema:unenforced-clustering-key:position".to_owned(),
                    "1".to_owned(),
                )]
                .into_iter()
                .collect::<HashMap<_, _>>(),
            ),
            ArrowField::new("date", DataType::Int32, false).with_metadata(
                vec![(
                    "lance-schema:unenforced-clustering-key:position".to_owned(),
                    "2".to_owned(),
                )]
                .into_iter()
                .collect::<HashMap<_, _>>(),
            ),
            ArrowField::new("value", DataType::Float64, true),
        ]);

        let schema = Schema::try_from(&arrow_schema).unwrap();
        let ck = schema.unenforced_clustering_key();
        assert_eq!(ck.len(), 2);
        assert_eq!(ck[0].name, "region");
        assert_eq!(ck[1].name, "date");

        // Round-trip through protobuf
        let fields_with_meta: FieldsWithMeta = (&schema).into();
        let restored = Schema::try_from(fields_with_meta).unwrap();

        let ck2 = restored.unenforced_clustering_key();
        assert_eq!(ck2.len(), 2);
        assert_eq!(ck2[0].name, "region");
        assert_eq!(ck2[1].name, "date");
        assert_eq!(ck2[0].unenforced_clustering_key_position, Some(1));
        assert_eq!(ck2[1].unenforced_clustering_key_position, Some(2));

        // Non-clustering-key field should not have position
        let value_field = restored.field("value").unwrap();
        assert!(!value_field.is_unenforced_clustering_key());
    }
}
