// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Phase one of the write path: which fields should Lance actually receive?
//!
//! Kept apart from expression building so that the question "what shape do we want?" is a
//! pure function over fields, answerable and testable without a DataFusion plan. A new
//! extension type that only needs a different target shape belongs here; one that needs its
//! values synthesized differently belongs in a [`super::cast`] rule.

use std::sync::Arc;

use arrow_schema::{DataType, Field, FieldRef};
use lance_arrow::json::has_json_fields;

use super::extension::{ExtensionKind, arrow_json_field, arrow_json_storage_type};

/// The field the write path should hand to Lance for `table_field`, given what the input
/// supplies for it.
///
/// Almost always the table's own field: the input is what has to change, not the table. The
/// exception is JSON. Lance-core encodes JSON text into JSONB as it writes, but only for
/// leaves labelled `arrow.json`; casting the text to a json column's `LargeBinary` storage
/// type instead relabels raw text as JSONB, which appends without error and leaves the
/// column unreadable. So a json leaf the input supplies as text is asked for as `arrow.json`
/// and the writer does the encoding, however deeply the leaf is nested.
pub(super) fn resolve_write_field(input_field: &Field, table_field: &FieldRef) -> FieldRef {
    match relabel_json_leaves(input_field, table_field) {
        Some(relabelled) => Arc::new(relabelled),
        None => table_field.clone(),
    }
}

/// `table_field` with every json leaf the input supplies as text turned into an `arrow.json`
/// leaf, or `None` if there is no such leaf.
fn relabel_json_leaves(input_field: &Field, table_field: &Field) -> Option<Field> {
    if ExtensionKind::of(table_field) == ExtensionKind::Json {
        // Input that is already labelled keeps its own storage type; there is nothing to
        // convert, only the target to agree with.
        let storage = if ExtensionKind::of(input_field) == ExtensionKind::ArrowJson {
            input_field.data_type().clone()
        } else {
            arrow_json_storage_type(input_field.data_type())?
        };
        return Some(arrow_json_field(
            table_field.name(),
            storage,
            table_field.is_nullable(),
        ));
    }

    if !has_json_fields(table_field) {
        return None;
    }

    let relabelled = match (input_field.data_type(), table_field.data_type()) {
        (
            DataType::List(input_item)
            | DataType::LargeList(input_item)
            | DataType::FixedSizeList(input_item, _),
            DataType::List(table_item)
            | DataType::LargeList(table_item)
            | DataType::FixedSizeList(table_item, _),
        ) => {
            let item: FieldRef = Arc::new(relabel_json_leaves(input_item, table_item)?);
            match table_field.data_type() {
                DataType::List(_) => DataType::List(item),
                DataType::LargeList(_) => DataType::LargeList(item),
                DataType::FixedSizeList(_, len) => DataType::FixedSizeList(item, *len),
                _ => unreachable!("matched a list type above"),
            }
        }
        (DataType::Map(input_entries, _), DataType::Map(table_entries, sorted)) => {
            let entries = relabel_json_leaves(input_entries, table_entries)?;
            DataType::Map(Arc::new(entries), *sorted)
        }
        (DataType::Struct(input_children), DataType::Struct(table_children)) => {
            let mut children = Vec::with_capacity(table_children.len());
            let mut relabelled_any = false;
            for table_child in table_children {
                let relabelled_child = input_children
                    .iter()
                    .find(|f| f.name() == table_child.name())
                    .and_then(|input_child| relabel_json_leaves(input_child, table_child));
                match relabelled_child {
                    Some(child) => {
                        relabelled_any = true;
                        children.push(Arc::new(child));
                    }
                    None => children.push(table_child.clone()),
                }
            }
            if !relabelled_any {
                return None;
            }
            DataType::Struct(children.into())
        }
        _ => return None,
    };

    Some(
        Field::new(table_field.name(), relabelled, table_field.is_nullable())
            .with_metadata(table_field.metadata().clone()),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::blob::blob;
    use lance_arrow::json::{is_arrow_json_field, json_field};

    fn resolve(input: Field, table: Field) -> FieldRef {
        resolve_write_field(&input, &Arc::new(table))
    }

    fn list_of(item: Field) -> DataType {
        DataType::List(Arc::new(item))
    }

    #[rstest::rstest]
    #[case::utf8(DataType::Utf8, DataType::Utf8)]
    #[case::large_utf8(DataType::LargeUtf8, DataType::LargeUtf8)]
    #[case::utf8_view(DataType::Utf8View, DataType::Utf8)]
    fn json_text_is_asked_for_as_arrow_json(
        #[case] input_type: DataType,
        #[case] expected: DataType,
    ) {
        let resolved = resolve(Field::new("j", input_type, true), json_field("j", true));
        assert_eq!(resolved.data_type(), &expected);
        assert!(is_arrow_json_field(&resolved));
    }

    #[test]
    fn already_labelled_json_keeps_its_storage_type() {
        let input = arrow_json_field("j", DataType::LargeUtf8, true);
        let resolved = resolve(input, json_field("j", true));
        assert_eq!(resolved.data_type(), &DataType::LargeUtf8);
        assert!(is_arrow_json_field(&resolved));
    }

    #[test]
    fn json_leaf_is_relabelled_inside_a_list() {
        let resolved = resolve(
            Field::new(
                "docs",
                list_of(Field::new("item", DataType::Utf8, true)),
                true,
            ),
            Field::new("docs", list_of(json_field("item", true)), true),
        );
        let DataType::List(item) = resolved.data_type() else {
            panic!("expected a list, got {}", resolved.data_type());
        };
        assert!(is_arrow_json_field(item));
    }

    #[test]
    fn json_leaf_is_relabelled_inside_a_struct() {
        let resolved = resolve(
            Field::new(
                "info",
                DataType::Struct(
                    vec![
                        Field::new("id", DataType::Int64, true),
                        Field::new("value", DataType::Utf8, true),
                    ]
                    .into(),
                ),
                true,
            ),
            Field::new(
                "info",
                DataType::Struct(
                    vec![
                        Field::new("id", DataType::Int64, true),
                        json_field("value", true),
                    ]
                    .into(),
                ),
                true,
            ),
        );
        let DataType::Struct(children) = resolved.data_type() else {
            panic!("expected a struct, got {}", resolved.data_type());
        };
        assert_eq!(children[0].data_type(), &DataType::Int64);
        assert!(is_arrow_json_field(&children[1]));
    }

    /// An all-null column carries no text to encode, so the json column is asked for as it
    /// is stored and the writer receives typed nulls.
    #[test]
    fn null_input_leaves_the_json_column_alone() {
        let table = json_field("j", true);
        let resolved = resolve(Field::new("j", DataType::Null, true), table.clone());
        assert_eq!(resolved.as_ref(), &table);
    }

    #[rstest::rstest]
    #[case::plain(
        Field::new("x", DataType::Int32, true),
        Field::new("x", DataType::Int64, true)
    )]
    #[case::blob(Field::new("b", DataType::Binary, true), blob("b", true))]
    #[case::binary_into_json(Field::new("j", DataType::Binary, true), json_field("j", true))]
    fn everything_else_is_asked_for_as_the_table_declares_it(
        #[case] input: Field,
        #[case] table: Field,
    ) {
        let resolved = resolve(input, table.clone());
        assert_eq!(resolved.as_ref(), &table);
    }
}
