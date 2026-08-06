// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use crate::Dataset;
use crate::dataset::{NewColumnTransform, WriteMode, WriteParams};
use arrow_array::{
    Array, ArrayRef, FixedSizeListArray, Int32Array, ListArray, NullArray, RecordBatch,
    RecordBatchIterator, StringArray, StructArray,
};
use arrow_schema::{
    DataType, Field as ArrowField, Field, Fields as ArrowFields, Fields, Schema as ArrowSchema,
};
use lance_file::version::LanceFileVersion;
use rstest::rstest;
use std::collections::HashMap;
use std::sync::Arc;

#[rstest]
#[tokio::test]
async fn test_add_sub_column_to_packed_struct_col(
    #[values(LanceFileVersion::V2_2)] version: LanceFileVersion,
) {
    let mut dataset = prepare_packed_struct_col(version).await;

    // Construct sub-column record batch.
    let food_array = StringArray::from(vec!["omnivore"]);
    let struct_array = StructArray::new(
        ArrowFields::from(vec![ArrowField::new("food", DataType::Utf8, false)]),
        vec![Arc::new(food_array) as ArrayRef],
        None,
    );

    let new_added_struct_field = ArrowField::new(
        "animal",
        DataType::Struct(ArrowFields::from(vec![ArrowField::new(
            "food",
            DataType::Utf8,
            false,
        )])),
        false,
    );
    let new_schema = Arc::new(ArrowSchema::new(vec![new_added_struct_field]));
    let batch = RecordBatch::try_new(new_schema.clone(), vec![Arc::new(struct_array)]).unwrap();

    // Verify add sub-column.
    let error = dataset
        .add_columns(
            NewColumnTransform::Reader(Box::new(RecordBatchIterator::new(
                vec![Ok(batch)],
                new_schema,
            ))),
            None,
            None,
        )
        .await
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("Column animal is packed struct and already exists in the dataset")
    );
}

#[rstest]
#[tokio::test]
async fn test_add_sub_column_to_struct_col_unsupported(
    #[values(
        LanceFileVersion::Legacy,
        LanceFileVersion::V2_0,
        LanceFileVersion::V2_1
    )]
    version: LanceFileVersion,
) {
    let mut dataset = prepare_initial_dataset_with_struct_col(version, 3).await;

    // add 2 sub-column of animal
    let batch = prepare_sub_column_batch(3).await;
    let new_schema = batch.schema();

    let err = dataset
        .add_columns(
            NewColumnTransform::Reader(Box::new(RecordBatchIterator::new(
                vec![Ok(batch)],
                new_schema,
            ))),
            None,
            None,
        )
        .await
        .unwrap_err();
    assert!(
        err.to_string()
            .contains("is a struct col, add sub column is not supported in Lance file version")
    );
}

#[rstest]
#[tokio::test]
async fn test_add_sub_column_to_struct_col(
    #[values(LanceFileVersion::V2_2)] version: LanceFileVersion,
) {
    let mut dataset = prepare_initial_dataset_with_struct_col(version, 3).await;

    // add 2 sub-columns of animal
    let batch = prepare_sub_column_batch(3).await;
    let new_schema = batch.schema();

    dataset
        .add_columns(
            NewColumnTransform::Reader(Box::new(RecordBatchIterator::new(
                vec![Ok(batch)],
                new_schema,
            ))),
            None,
            None,
        )
        .await
        .unwrap();

    // Verify schema
    // root
    //  - fixed_list
    //  - list
    //  - struct
    //    - level_1
    //      - level_0
    //        - leaf
    //        - new_col
    //      - new_col
    //    - new_col
    assert_eq!(dataset.schema().fields.len(), 1);
    assert_eq!(dataset.schema().fields[0].name, "root");

    let field = &dataset.schema().fields[0];
    assert_eq!(field.children[0].name, "fixed_list");
    assert_eq!(field.children[1].name, "list");
    assert_eq!(field.children[2].name, "struct");

    let field = &field.children[2];
    assert_eq!(field.children[0].name, "level_1");
    assert_eq!(field.children[1].name, "new_col");

    let field = &field.children[0];
    assert_eq!(field.children[0].name, "level_0");
    assert_eq!(field.children[1].name, "new_col");

    let field = &field.children[0];
    assert_eq!(field.children[0].name, "leaf");
    assert_eq!(field.children[1].name, "new_col");

    // verify data is updated
    let batch = dataset
        .scan()
        .project(&[
            "root.struct.level_1.level_0.leaf",
            "root.struct.new_col",
            "root.struct.level_1.new_col",
            "root.struct.level_1.level_0.new_col",
        ])
        .unwrap()
        .try_into_batch()
        .await
        .unwrap();
    assert_eq!(batch.num_rows(), 1);
    assert_eq!(batch.num_columns(), 4);

    let col = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(col.value(0), 42);

    for i in 1..4 {
        let col = batch
            .column(i)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(col.value(0), 100);
    }
}

async fn prepare_sub_column_batch(nested_level: usize) -> RecordBatch {
    // add a sub-column of new_col
    let leaf_col = ArrowField::new(String::from("new_col"), DataType::Int32, false);
    let leaf_array = Arc::new(Int32Array::from(vec![100])) as ArrayRef;

    let mut current_field = leaf_col.clone();
    let mut current_struct_array = leaf_array.clone();

    for i in 0..nested_level {
        if i == 0 {
            let struct_array = StructArray::try_new(
                Fields::from(vec![current_field.clone()]),
                vec![current_struct_array],
                None,
            )
            .unwrap();

            current_struct_array = Arc::new(struct_array) as ArrayRef;
            current_field = ArrowField::new(
                format!("level_{}", i),
                DataType::Struct(ArrowFields::from(vec![current_field])),
                false,
            );
        } else {
            let struct_array = StructArray::try_new(
                Fields::from(vec![current_field.clone(), leaf_col.clone()]),
                vec![current_struct_array, leaf_array.clone()],
                None,
            )
            .unwrap();

            current_struct_array = Arc::new(struct_array) as ArrayRef;
            current_field = ArrowField::new(
                format!("level_{}", i),
                DataType::Struct(ArrowFields::from(vec![current_field, leaf_col.clone()])),
                false,
            );
        };
    }

    let current_field = ArrowField::new("struct", current_struct_array.data_type().clone(), false);
    let root_struct_array = Arc::new(
        StructArray::try_new(
            Fields::from(vec![current_field]),
            vec![current_struct_array],
            None,
        )
        .unwrap(),
    ) as ArrayRef;

    let root_field = Field::new("root", root_struct_array.data_type().clone(), true);

    let schema = Arc::new(ArrowSchema::new(vec![root_field]));
    RecordBatch::try_new(schema, vec![Arc::new(root_struct_array)]).unwrap()
}

async fn prepare_initial_dataset_with_struct_col(
    version: LanceFileVersion,
    nested_level: usize,
) -> Dataset {
    // nested column
    let mut current_field = ArrowField::new(String::from("leaf"), DataType::Int32, false);
    let mut current_array = Arc::new(Int32Array::from(vec![42])) as ArrayRef;

    for i in 0..nested_level {
        let struct_array = StructArray::try_new(
            Fields::from(vec![current_field.clone()]),
            vec![current_array],
            None,
        )
        .unwrap();

        current_array = Arc::new(struct_array) as ArrayRef;
        current_field = ArrowField::new(
            format!("level_{}", i),
            DataType::Struct(ArrowFields::from(vec![current_field])),
            false,
        );
    }

    // list column
    let values = Int32Array::from(vec![1]);
    let offsets =
        arrow_buffer::OffsetBuffer::new(arrow_buffer::ScalarBuffer::from(vec![0i32, 1i32]));
    let list_data_type = DataType::Int32;
    let list_array = ListArray::new(
        Arc::new(ArrowField::new("list", list_data_type, false)),
        offsets,
        Arc::new(values),
        None,
    );

    // fixed list column
    let values = Int32Array::from(vec![1, 2, 3, 4, 5, 6]);
    let field = Arc::new(Field::new_list_field(DataType::Int32, true));
    let fixed_size_list_array = FixedSizeListArray::new(field, 6, Arc::new(values), None);

    // Root field
    let root_fields = Fields::from(vec![
        Field::new(
            "fixed_list",
            fixed_size_list_array.data_type().clone(),
            true,
        ),
        Field::new("list", list_array.data_type().clone(), true),
        Field::new("struct", current_array.data_type().clone(), true),
    ]);
    let root_struct_array = StructArray::new(
        root_fields.clone(),
        vec![
            Arc::new(fixed_size_list_array) as ArrayRef,
            Arc::new(list_array) as ArrayRef,
            Arc::new(current_array) as ArrayRef,
        ],
        None,
    );
    let root_field = ArrowField::new("root", root_struct_array.data_type().clone(), false);

    // create schema with struct column
    let schema = Arc::new(ArrowSchema::new(vec![root_field]));
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(root_struct_array)]).unwrap();

    let reader = RecordBatchIterator::new(vec![Ok(batch.clone())], schema.clone());
    let write_params = WriteParams {
        mode: WriteMode::Create,
        data_storage_version: Some(version),
        ..Default::default()
    };
    let mut dataset = Dataset::write(reader, "memory://test", Some(write_params))
        .await
        .unwrap();

    // verify initial schema
    assert_eq!(dataset.schema().fields.len(), 1);

    // add conflict sub-column
    let res = dataset
        .add_columns(
            NewColumnTransform::Reader(Box::new(RecordBatchIterator::new(vec![Ok(batch)], schema))),
            None,
            None,
        )
        .await;
    assert!(res.is_err());

    dataset
}

async fn prepare_packed_struct_col(version: LanceFileVersion) -> Dataset {
    let mut metadata = HashMap::new();
    metadata.insert("lance-encoding:packed".to_string(), "true".to_string());

    // create schema with struct column
    let mut animal_struct_field = ArrowField::new(
        "animal",
        DataType::Struct(ArrowFields::from(vec![ArrowField::new(
            "name",
            DataType::Utf8,
            false,
        )])),
        false,
    );
    animal_struct_field.set_metadata(metadata);
    let schema = Arc::new(ArrowSchema::new(vec![animal_struct_field]));

    // create data with one record
    let name_array = StringArray::from(vec!["bear"]);
    let struct_array = StructArray::new(
        ArrowFields::from(vec![ArrowField::new("name", DataType::Utf8, false)]),
        vec![Arc::new(name_array) as ArrayRef],
        None,
    );
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(struct_array)]).unwrap();

    let reader = RecordBatchIterator::new(vec![Ok(batch.clone())], schema.clone());
    let write_params = WriteParams {
        mode: WriteMode::Create,
        data_storage_version: Some(version),
        ..Default::default()
    };
    let dataset = Dataset::write(reader, "memory://test", Some(write_params))
        .await
        .unwrap();

    // verify initial schema
    assert_eq!(dataset.schema().fields.len(), 1);
    assert_eq!(dataset.schema().fields[0].name, "animal");

    dataset
}

#[rstest]
#[tokio::test]
async fn test_add_sub_column_to_list_struct_col(
    #[values(LanceFileVersion::V2_2)] version: LanceFileVersion,
) {
    let mut dataset = prepare_initial_dataset_with_list_struct_col(version).await;

    // Prepare sub-column data to add to the struct inside list.
    let all_cars = StringArray::from(vec!["Toyota", "Honda", "Mercedes", "Audi", "BMW", "Tesla"]);

    let car_struct = StructArray::new(
        ArrowFields::from(vec![ArrowField::new("car", DataType::Utf8, false)]),
        vec![Arc::new(all_cars) as ArrayRef],
        None,
    );

    let car_list = ListArray::new(
        Arc::new(ArrowField::new(
            "item",
            DataType::Struct(ArrowFields::from(vec![ArrowField::new(
                "car",
                DataType::Utf8,
                false,
            )])),
            false,
        )),
        arrow_buffer::OffsetBuffer::new(arrow_buffer::ScalarBuffer::from(vec![
            0i32, 2i32, 5i32, 6i32,
        ])),
        Arc::new(car_struct),
        None,
    );

    let new_added_field = ArrowField::new("people", car_list.data_type().clone(), false);
    let new_schema = Arc::new(ArrowSchema::new(vec![new_added_field]));
    let batch = RecordBatch::try_new(new_schema.clone(), vec![Arc::new(car_list)]).unwrap();

    // Add sub-column to the struct inside list.
    dataset
        .add_columns(
            NewColumnTransform::Reader(Box::new(RecordBatchIterator::new(
                vec![Ok(batch)],
                new_schema,
            ))),
            None,
            None,
        )
        .await
        .unwrap();

    // Verify schema
    // root
    //  - id
    //  - people
    //    - name
    //    - age
    //    - city
    //    - car
    assert_eq!(dataset.schema().fields.len(), 2);
    assert_eq!(dataset.schema().fields[0].name, "id");
    assert_eq!(dataset.schema().fields[1].name, "people");

    let field = &dataset.schema().fields[1];
    assert_eq!(field.children[0].name, "item");

    let field = &field.children[0];
    assert_eq!(field.children[0].name, "name");
    assert_eq!(field.children[1].name, "age");
    assert_eq!(field.children[2].name, "city");
    assert_eq!(field.children[3].name, "car");

    // Verify the data
    let batch = dataset.scan().try_into_batch().await.unwrap();
    assert_eq!(batch.num_rows(), 3);
    assert_eq!(batch.num_columns(), 2);

    let list_array = batch
        .column(1)
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    let list_value = list_array.value(0);
    let struct_array = list_value.as_any().downcast_ref::<StructArray>().unwrap();
    let name = struct_array
        .column_by_name("name")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let car = struct_array
        .column_by_name("car")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(name.value(0), "Alice");
    assert_eq!(car.value(0), "Toyota");
}

async fn prepare_initial_dataset_with_list_struct_col(version: LanceFileVersion) -> Dataset {
    // Create struct type for person
    let person_struct_type = DataType::Struct(ArrowFields::from(vec![
        ArrowField::new("name", DataType::Utf8, false),
        ArrowField::new("age", DataType::Int32, false),
        ArrowField::new("city", DataType::Utf8, false),
    ]));

    // Create list of struct type
    let list_of_struct_type = DataType::List(Arc::new(ArrowField::new(
        "item",
        person_struct_type.clone(),
        false,
    )));

    // Create schema
    let schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("id", DataType::Int32, false),
        ArrowField::new("people", list_of_struct_type.clone(), false),
    ]));

    // Create data - 3 rows as in the Python test
    let all_names = StringArray::from(vec!["Alice", "Bob", "Charlie", "David", "Eve", "Frank"]);
    let all_ages = Int32Array::from(vec![25, 30, 35, 28, 32, 40]);
    let all_cities = StringArray::from(vec![
        "Beijing",
        "Shanghai",
        "Guangzhou",
        "Shenzhen",
        "Hangzhou",
        "Chengdu",
    ]);
    let all_struct = StructArray::new(
        ArrowFields::from(vec![
            ArrowField::new("name", DataType::Utf8, false),
            ArrowField::new("age", DataType::Int32, false),
            ArrowField::new("city", DataType::Utf8, false),
        ]),
        vec![
            Arc::new(all_names) as ArrayRef,
            Arc::new(all_ages) as ArrayRef,
            Arc::new(all_cities) as ArrayRef,
        ],
        None,
    );
    let all_people = ListArray::new(
        Arc::new(ArrowField::new("item", person_struct_type, false)),
        arrow_buffer::OffsetBuffer::new(arrow_buffer::ScalarBuffer::from(vec![
            0i32, 2i32, 5i32, 6i32,
        ])),
        Arc::new(all_struct),
        None,
    );

    let ids = Int32Array::from(vec![1, 2, 3]);
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(ids) as ArrayRef, Arc::new(all_people) as ArrayRef],
    )
    .unwrap();

    let reader = RecordBatchIterator::new(vec![Ok(batch)], schema);
    let write_params = WriteParams {
        mode: WriteMode::Create,
        data_storage_version: Some(version),
        ..Default::default()
    };
    let dataset = Dataset::write(reader, "memory://test", Some(write_params))
        .await
        .unwrap();

    // verify initial schema
    assert_eq!(dataset.schema().fields.len(), 2);
    assert_eq!(dataset.schema().fields[0].name, "id");
    assert_eq!(dataset.schema().fields[1].name, "people");

    dataset
}

/// Reproduces ENT-990: panic in `adjust_child_validity` when reading a dataset where:
/// - Fragment 0 has `meta.extra: Null` (Arrow infers DataType::Null when the user inserts
///   rows where every value in `extra` is null, e.g. from Python/pandas with an all-None column)
/// - Fragment 1 (appended later) has a nullable `meta` struct with null rows, but no `extra`
///   sub-field (Lance allows this because `extra: Null` is nullable in the dataset schema)
///
/// When Fragment 1 is read, Lance adds a `NullReader` for the missing `meta.extra: Null`
/// sub-field. `MergeStream` calls `RecordBatchExt::merge` on the real batch (with null `meta`
/// rows) and the `NullReader` batch (all-null `meta` struct). The recursive merge descends into
/// `meta`, where the parent's null validity is non-empty and the child column has `DataType::Null`
/// — causing `ArrayData::try_new` to panic.
#[tokio::test]
async fn test_scan_with_null_typed_struct_subfield_across_fragments() {
    // Fragment 0: struct column with an `extra` sub-field of DataType::Null.
    // This simulates a user inserting rows from Python/pandas where `extra` is all None.
    let meta0 = StructArray::new(
        ArrowFields::from(vec![
            ArrowField::new("name", DataType::Utf8, true),
            ArrowField::new("extra", DataType::Null, true),
        ]),
        vec![
            Arc::new(StringArray::from(vec![Some("alice"), Some("bob")])) as ArrayRef,
            Arc::new(NullArray::new(2)) as ArrayRef,
        ],
        None,
    );
    let schema0 = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("id", DataType::Int32, false),
        ArrowField::new("meta", meta0.data_type().clone(), true),
    ]));
    let batch0 = RecordBatch::try_new(
        schema0.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef,
            Arc::new(meta0) as ArrayRef,
        ],
    )
    .unwrap();

    let mut ds = Dataset::write(
        RecordBatchIterator::new(vec![Ok(batch0)], schema0),
        "memory://",
        Some(WriteParams::default()),
    )
    .await
    .unwrap();

    // Fragment 1: same struct column but WITHOUT `extra`. Lance's `allow_missing_if_nullable`
    // permits omitting `extra` (it's nullable), so a NullReader will fill it when reading
    // Fragment 1. The struct has null rows, which is what exposes the bug.
    let meta1 = StructArray::new(
        ArrowFields::from(vec![ArrowField::new("name", DataType::Utf8, true)]),
        vec![Arc::new(StringArray::from(vec![Some("charlie"), None])) as ArrayRef],
        Some(vec![true, false].into()), // row 1 is a null struct
    );
    let schema1 = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("id", DataType::Int32, false),
        ArrowField::new("meta", meta1.data_type().clone(), true),
    ]));
    let batch1 = RecordBatch::try_new(
        schema1.clone(),
        vec![
            Arc::new(Int32Array::from(vec![3, 4])) as ArrayRef,
            Arc::new(meta1) as ArrayRef,
        ],
    )
    .unwrap();

    ds.append(
        RecordBatchIterator::new(vec![Ok(batch1)], schema1),
        Some(WriteParams {
            mode: WriteMode::Append,
            ..Default::default()
        }),
    )
    .await
    .unwrap();

    // Scanning reads both fragments. Fragment 1 is missing `meta.extra: Null`, so Lance adds
    // a NullReader for it. MergeStream merges the real batch (with null struct rows) and the
    // NullReader batch (all-null `meta` struct). The recursive merge in `merge()` descends into
    // `meta`, where `right_validity` (from the all-null NullReader struct) has non-zero null
    // count and the child column has DataType::Null — previously panicked:
    // "Arrays of type Null cannot contain a null bitmask".
    let result = ds.scan().try_into_batch().await.unwrap();
    assert_eq!(result.num_rows(), 4);
}
