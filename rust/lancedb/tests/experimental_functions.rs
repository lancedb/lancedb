// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

#![cfg(not(feature = "experimental-functions"))]
#![recursion_limit = "256"]

use std::collections::HashMap;

use arrow_array::record_batch;
use futures::TryStreamExt;
use lancedb::materialized_view::{
    DEFINITION_META_KEY, definition_metadata_from_sql, prepare_declaration,
};
use lancedb::table::NewColumnTransform;
use lancedb::table::computed_columns::{
    COMPUTED_COLUMN_META_KEY, EXPRESSION_META_KEY, INPUTS_META_KEY, KIND_META_KEY, computed_columns,
};
use lancedb::{Error, MaterializedView, Result, connect};

fn assert_disabled<T>(result: Result<T>) {
    match result {
        Err(Error::NotSupported { message }) => {
            assert!(message.contains("experimental-functions"), "{message}");
        }
        _ => panic!("expected the experimental-functions feature error"),
    }
}

#[tokio::test]
async fn computed_operations_fail_before_mutation_or_job_creation() {
    let db = connect("memory://").execute().await.unwrap();
    let table = db
        .create_table("source", record_batch!(("x", Int32, [1, 2])).unwrap())
        .execute()
        .await
        .unwrap();
    let version = table.version().await.unwrap();
    assert_disabled(
        table
            .add_columns()
            .computed("doubled", "x * 2")
            .execute()
            .await,
    );
    assert_disabled(table.refresh_column("x").await);
    assert_disabled(table.refresh_column_async("x").await);
    assert_eq!(table.version().await.unwrap(), version);
    assert_eq!(table.schema().await.unwrap().fields().len(), 1);

    // The eager SQL transform remains an ordinary schema-evolution operation.
    table
        .add_columns()
        .transform(NewColumnTransform::SqlExpressions(vec![(
            "eager".into(),
            "x * 2".into(),
        )]))
        .execute()
        .await
        .unwrap();
    assert_eq!(
        table
            .count_rows(Some("eager = x * 2".into()))
            .await
            .unwrap(),
        2
    );
}

#[tokio::test]
async fn existing_computed_metadata_remains_readable_and_protected() {
    let db = connect("memory://").execute().await.unwrap();
    let table = db
        .create_table(
            "source",
            record_batch!(("x", Int32, [1, 2]), ("doubled", Int32, [2, 4])).unwrap(),
        )
        .execute()
        .await
        .unwrap();
    // Model a table previously written by an enabled build, without enabling
    // the feature in this test binary through Cargo feature unification.
    let wrapper = table.dataset().unwrap();
    let mut dataset = wrapper.get().await.unwrap().as_ref().clone();
    let field_id = dataset.schema().field("doubled").unwrap().id as u32;
    dataset
        .replace_field_metadata(vec![(
            field_id,
            HashMap::from([
                (COMPUTED_COLUMN_META_KEY.into(), "true".into()),
                (KIND_META_KEY.into(), "sql".into()),
                (EXPRESSION_META_KEY.into(), "x * 2".into()),
                (INPUTS_META_KEY.into(), "[\"x\"]".into()),
            ]),
        )])
        .await
        .unwrap();
    wrapper.update(dataset);
    let version = table.version().await.unwrap();
    assert_eq!(computed_columns(&table.schema().await.unwrap()).len(), 1);
    assert_eq!(
        table
            .count_rows(Some("doubled = x * 2".into()))
            .await
            .unwrap(),
        2
    );
    assert_disabled(table.refresh_column("doubled").await);
    assert_disabled(table.refresh_column_async("doubled").await);
    assert!(
        table
            .update()
            .column("doubled", "42")
            .execute()
            .await
            .is_err()
    );
    assert!(table.drop_columns(&["x"]).await.is_err());
    assert_eq!(table.version().await.unwrap(), version);
    table.drop_columns(&["doubled"]).await.unwrap();
}

#[tokio::test]
async fn views_cannot_be_created_or_refreshed_but_existing_data_is_accessible() {
    let db = connect("memory://").execute().await.unwrap();
    let source = db
        .create_table("source", record_batch!(("x", Int32, [1, 2])).unwrap())
        .execute()
        .await
        .unwrap();
    for with_no_data in [false, true] {
        assert_disabled(
            db.create_materialized_view("blocked", "source")
                .with_no_data(with_no_data)
                .execute()
                .await,
        );
        assert_disabled(
            db.create_materialized_view("blocked", "source")
                .with_no_data(with_no_data)
                .execute_async()
                .await,
        );
    }
    let prepared = prepare_declaration(&source, None, None, None)
        .await
        .unwrap();
    assert_disabled(prepared.clone().create("blocked").await);
    assert_disabled(prepared.create_in(&[], "blocked").await);
    assert!(matches!(
        db.open_table("blocked").execute().await,
        Err(Error::TableNotFound { .. })
    ));

    let table = db
        .create_table("existing", record_batch!(("x", Int32, [1, 2])).unwrap())
        .execute()
        .await
        .unwrap();
    let wrapper = table.dataset().unwrap();
    let mut dataset = wrapper.get().await.unwrap().as_ref().clone();
    dataset
        .update_schema_metadata(vec![(
            DEFINITION_META_KEY.into(),
            Some(definition_metadata_from_sql("SELECT x FROM source")),
        )])
        .await
        .unwrap();
    wrapper.update(dataset);
    let version = table.version().await.unwrap();
    let view = MaterializedView::from_table(table.clone()).await.unwrap();
    assert_disabled(view.refresh().execute().await);
    assert_disabled(view.refresh().execute_async().await);
    assert_eq!(table.version().await.unwrap(), version);
    assert_eq!(view.table().count_rows(None).await.unwrap(), 2);
    assert_eq!(
        db.open_materialized_view("existing")
            .await
            .unwrap()
            .definition_sql(),
        "SELECT x FROM source"
    );
    assert_eq!(
        db.list_materialized_views(Default::default())
            .try_collect::<Vec<_>>()
            .await
            .unwrap(),
        vec!["existing"]
    );
    assert!(
        table
            .add(record_batch!(("x", Int32, [3])).unwrap())
            .execute()
            .await
            .is_err()
    );
    assert!(table.delete("true").await.is_err());
    db.drop_materialized_view("existing", &[]).await.unwrap();
}
