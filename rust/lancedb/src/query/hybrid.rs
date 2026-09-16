// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use arrow::compute::{
    kernels::numeric::{div, sub},
    max, min,
};
use arrow::row::{RowConverter, SortField};
use arrow_array::{Float32Array, RecordBatch, UInt64Array, cast::downcast_array};
use arrow_schema::{DataType, Field, Schema, SortOptions};
use lance::dataset::ROW_ID;
use lance_arrow::RecordBatchExt;
use lance_index::{scalar::inverted::SCORE_COL, vector::DIST_COL};
use std::collections::HashMap;
use std::sync::Arc;

use crate::error::{Error, Result};

/// Converts results's score column to a rank.
///
/// Expects the `column` argument to be type Float32 and will panic if it's not
pub fn rank(results: RecordBatch, column: &str, ascending: Option<bool>) -> Result<RecordBatch> {
    let scores = results.column_by_name(column).ok_or(Error::InvalidInput {
        message: format!(
            "expected column {} not found in rank. found columns {:?}",
            column,
            results
                .schema()
                .fields()
                .iter()
                .map(|f| f.name())
                .collect::<Vec<_>>(),
        ),
    })?;

    if results.num_rows() == 0 {
        return Ok(results);
    }

    let scores: Float32Array = downcast_array(scores);
    let ranks = Float32Array::from_iter_values(
        arrow::compute::kernels::rank::rank(
            &scores,
            Some(SortOptions {
                descending: !ascending.unwrap_or(true),
                ..Default::default()
            }),
        )?
        .iter()
        .map(|i| *i as f32),
    );

    let schema = results.schema();
    let (column_idx, _) = schema.column_with_name(column).unwrap();
    let mut columns = results.columns().to_vec();
    columns[column_idx] = Arc::new(ranks);

    let results = RecordBatch::try_new(results.schema(), columns)?;

    Ok(results)
}

/// Get the query schemas needed when combining the search results.
///
/// If either of the record batches are empty, then we create a schema from the
/// other record batch, and replace the score/distance column. If both record
/// batches are empty, create empty schemas.
pub fn query_schemas(
    fts_results: &[RecordBatch],
    vec_results: &[RecordBatch],
) -> (Arc<Schema>, Arc<Schema>) {
    let (fts_schema, vec_schema) = match (
        fts_results.first().map(|r| r.schema()),
        vec_results.first().map(|r| r.schema()),
    ) {
        (Some(fts_schema), Some(vec_schema)) => (fts_schema, vec_schema),
        (None, Some(vec_schema)) => {
            let fts_schema = with_field_name_replaced(&vec_schema, DIST_COL, SCORE_COL);
            (Arc::new(fts_schema), vec_schema)
        }
        (Some(fts_schema), None) => {
            let vec_schema = with_field_name_replaced(&fts_schema, DIST_COL, SCORE_COL);
            (fts_schema, Arc::new(vec_schema))
        }
        (None, None) => (Arc::new(empty_fts_schema()), Arc::new(empty_vec_schema())),
    };

    (fts_schema, vec_schema)
}

pub fn empty_fts_schema() -> Schema {
    Schema::new(vec![
        Arc::new(Field::new(SCORE_COL, DataType::Float32, false)),
        Arc::new(Field::new(ROW_ID, DataType::UInt64, false)),
    ])
}

pub fn empty_vec_schema() -> Schema {
    Schema::new(vec![
        Arc::new(Field::new(DIST_COL, DataType::Float32, false)),
        Arc::new(Field::new(ROW_ID, DataType::UInt64, false)),
    ])
}

pub fn with_field_name_replaced(schema: &Schema, target: &str, replacement: &str) -> Schema {
    let field_idx = schema.fields().iter().enumerate().find_map(|(i, field)| {
        if field.name() == target {
            Some(i)
        } else {
            None
        }
    });

    let mut fields = schema.fields().to_vec();
    if let Some(idx) = field_idx {
        let new_field = (*fields[idx]).clone().with_name(replacement);
        fields[idx] = Arc::new(new_field);
    }

    Schema::new(fields)
}

/// Normalize the scores column to have values between 0 and 1.
///
/// Expects the `column` argument to be type Float32 and will panic if it's not
pub fn normalize_scores(
    results: RecordBatch,
    column: &str,
    invert: Option<bool>,
) -> Result<RecordBatch> {
    let scores = results.column_by_name(column).ok_or(Error::InvalidInput {
        message: format!(
            "expected column {} not found in rank. found columns {:?}",
            column,
            results
                .schema()
                .fields()
                .iter()
                .map(|f| f.name())
                .collect::<Vec<_>>(),
        ),
    })?;

    if results.num_rows() == 0 {
        return Ok(results);
    }
    let mut scores: Float32Array = downcast_array(scores);

    let max = max(&scores).unwrap_or(0.0);
    let min = min(&scores).unwrap_or(0.0);

    // this is equivalent to np.isclose which is used in python
    let rng = if max - min < 10e-5 { max } else { max - min };

    // if rng is 0, then min and max are both 0 so we just leave the scores as is
    if rng != 0.0 {
        let tmp = div(
            &sub(&scores, &Float32Array::new_scalar(min))?,
            &Float32Array::new_scalar(rng),
        )?;
        scores = downcast_array(&tmp);
    }

    if invert.unwrap_or(false) {
        let tmp = sub(&Float32Array::new_scalar(1.0), &scores)?;
        scores = downcast_array(&tmp);
    }

    let schema = results.schema();
    let (column_idx, _) = schema.column_with_name(column).unwrap();
    let mut columns = results.columns().to_vec();
    columns[column_idx] = Arc::new(scores);

    let results = RecordBatch::try_new(results.schema(), columns).unwrap();

    Ok(results)
}

/// Replace each batch's join column with a dense surrogate derived from
/// `pk_columns`, in place and in the order given.
///
/// The fusion joins its two legs on [`ROW_ID`], which a MemWAL table cannot
/// supply — the fresh tier has no stable row id. It only ever compares the
/// column for equality, though (`merge_results` dedups, the rerankers group and
/// the score restore looks up), so any injective mapping of the primary key
/// serves. Ids are assigned in first-seen order across `batches`, so passing
/// them in fusion order keeps the dedup's "first occurrence wins" and its tie
/// break byte-identical to what a real row id would have produced.
///
/// Writing it under the [`ROW_ID`] name is what lets every reranker, including
/// third-party implementations that hardcode the name, run unmodified. The
/// column never reaches the caller: the hybrid query drops it before returning.
pub fn stamp_surrogate_row_ids(batches: &mut [RecordBatch], pk_columns: &[String]) -> Result<()> {
    if pk_columns.is_empty() {
        return Err(Error::InvalidInput {
            message: "hybrid search on a MemWAL table needs an unenforced primary key to \
                      deduplicate on, and this table declares none"
                .to_string(),
        });
    }

    let mut converter: Option<RowConverter> = None;
    let mut ids: HashMap<Vec<u8>, u64> = HashMap::new();

    for batch in batches.iter_mut() {
        // Both legs empty: `query_schemas` synthesizes a schema carrying only
        // the score and join columns, so there is no key to read — and no row
        // that would need one.
        let row_ids: UInt64Array = if batch.num_rows() == 0 {
            UInt64Array::from(Vec::<u64>::new())
        } else {
            let key_columns = pk_columns
                .iter()
                .map(|name| {
                    batch
                        .column_by_name(name)
                        .cloned()
                        .ok_or_else(|| Error::InvalidInput {
                            message: format!(
                                "hybrid search could not deduplicate: primary key column {} \
                                 is missing from a result set with {} rows",
                                name,
                                batch.num_rows()
                            ),
                        })
                })
                .collect::<Result<Vec<_>>>()?;

            let converter = match converter {
                Some(ref converter) => converter,
                None => converter.insert(RowConverter::new(
                    key_columns
                        .iter()
                        .map(|column| SortField::new(column.data_type().clone()))
                        .collect(),
                )?),
            };

            let rows = converter.convert_columns(&key_columns)?;
            let next = &mut ids;
            UInt64Array::from_iter_values(rows.iter().map(|row| {
                let id = next.len() as u64;
                *next.entry(row.as_ref().to_vec()).or_insert(id)
            }))
        };

        *batch = batch.try_with_column(
            Field::new(ROW_ID, DataType::UInt64, false),
            Arc::new(row_ids),
        )?;
    }

    Ok(())
}

#[cfg(test)]
mod test {
    use super::*;
    use arrow_array::StringArray;
    use arrow_schema::{DataType, Field, Schema};

    #[test]
    fn test_rank() {
        let schema = Arc::new(Schema::new(vec![
            Arc::new(Field::new("name", DataType::Utf8, false)),
            Arc::new(Field::new("score", DataType::Float32, false)),
        ]));

        let names = StringArray::from(vec!["foo", "bar", "baz", "bean", "dog"]);
        let scores = Float32Array::from(vec![0.2, 0.4, 0.1, 0.6, 0.45]);

        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(names), Arc::new(scores)]).unwrap();

        let result = rank(batch.clone(), "score", Some(false)).unwrap();
        assert_eq!(2, result.schema().fields().len());
        assert_eq!("name", result.schema().field(0).name());
        assert_eq!("score", result.schema().field(1).name());

        let names: StringArray = downcast_array(result.column(0));
        assert_eq!(
            names.iter().map(|e| e.unwrap()).collect::<Vec<_>>(),
            vec!["foo", "bar", "baz", "bean", "dog"]
        );
        let scores: Float32Array = downcast_array(result.column(1));
        assert_eq!(
            scores.iter().map(|e| e.unwrap()).collect::<Vec<_>>(),
            vec![4.0, 3.0, 5.0, 1.0, 2.0]
        );

        // check sort ascending
        let result = rank(batch.clone(), "score", Some(true)).unwrap();
        let names: StringArray = downcast_array(result.column(0));
        assert_eq!(
            names.iter().map(|e| e.unwrap()).collect::<Vec<_>>(),
            vec!["foo", "bar", "baz", "bean", "dog"]
        );
        let scores: Float32Array = downcast_array(result.column(1));
        assert_eq!(
            scores.iter().map(|e| e.unwrap()).collect::<Vec<_>>(),
            vec![2.0, 3.0, 1.0, 5.0, 4.0]
        );

        // ensure default sort is ascending
        let result = rank(batch.clone(), "score", None).unwrap();
        let names: StringArray = downcast_array(result.column(0));
        assert_eq!(
            names.iter().map(|e| e.unwrap()).collect::<Vec<_>>(),
            vec!["foo", "bar", "baz", "bean", "dog"]
        );
        let scores: Float32Array = downcast_array(result.column(1));
        assert_eq!(
            scores.iter().map(|e| e.unwrap()).collect::<Vec<_>>(),
            vec![2.0, 3.0, 1.0, 5.0, 4.0]
        );

        // check it can handle an empty batch
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(Vec::<&str>::new())),
                Arc::new(Float32Array::from(Vec::<f32>::new())),
            ],
        )
        .unwrap();
        let result = rank(batch.clone(), "score", None).unwrap();
        assert_eq!(0, result.num_rows());
        assert_eq!(2, result.schema().fields().len());
        assert_eq!("name", result.schema().field(0).name());
        assert_eq!("score", result.schema().field(1).name());

        // check it returns the expected error when there's no column
        let result = rank(batch.clone(), "bad_col", None);
        match result {
            Err(Error::InvalidInput { message }) => {
                assert_eq!(
                    "expected column bad_col not found in rank. found columns [\"name\", \"score\"]",
                    message
                );
            }
            _ => {
                panic!("expected invalid input error, received {:?}", result)
            }
        }
    }

    #[test]
    fn test_normalize_scores() {
        let schema = Arc::new(Schema::new(vec![
            Arc::new(Field::new("name", DataType::Utf8, false)),
            Arc::new(Field::new("score", DataType::Float32, false)),
        ]));

        let names = Arc::new(StringArray::from(vec!["foo", "bar", "baz", "bean", "dog"]));
        let scores = Arc::new(Float32Array::from(vec![-4.0, 2.0, 0.0, 3.0, 6.0]));

        let batch =
            RecordBatch::try_new(schema.clone(), vec![names.clone(), scores.clone()]).unwrap();

        let result = normalize_scores(batch.clone(), "score", Some(false)).unwrap();
        let names: StringArray = downcast_array(result.column(0));
        assert_eq!(
            names.iter().map(|e| e.unwrap()).collect::<Vec<_>>(),
            vec!["foo", "bar", "baz", "bean", "dog"]
        );
        let scores: Float32Array = downcast_array(result.column(1));
        assert_eq!(
            scores.iter().map(|e| e.unwrap()).collect::<Vec<_>>(),
            vec![0.0, 0.6, 0.4, 0.7, 1.0]
        );

        // check it can invert the normalization
        let result = normalize_scores(batch.clone(), "score", Some(true)).unwrap();
        let scores: Float32Array = downcast_array(result.column(1));
        assert_eq!(
            scores.iter().map(|e| e.unwrap()).collect::<Vec<_>>(),
            vec![1.0, 1.0 - 0.6, 0.6, 0.3, 0.0]
        );

        // check that the default is not inverted
        let result = normalize_scores(batch.clone(), "score", None).unwrap();
        let scores: Float32Array = downcast_array(result.column(1));
        assert_eq!(
            scores.iter().map(|e| e.unwrap()).collect::<Vec<_>>(),
            vec![0.0, 0.6, 0.4, 0.7, 1.0]
        );

        // check that it will function correctly if all the values are the same
        let names = Arc::new(StringArray::from(vec!["foo", "bar", "baz", "bean", "dog"]));
        let scores = Arc::new(Float32Array::from(vec![2.1, 2.1, 2.1, 2.1, 2.1]));
        let batch =
            RecordBatch::try_new(schema.clone(), vec![names.clone(), scores.clone()]).unwrap();
        let result = normalize_scores(batch.clone(), "score", None).unwrap();
        let scores: Float32Array = downcast_array(result.column(1));
        assert_eq!(
            scores.iter().map(|e| e.unwrap()).collect::<Vec<_>>(),
            vec![0.0, 0.0, 0.0, 0.0, 0.0]
        );

        // check it keeps floating point rounding errors for same score normalized the same
        // e.g., the behaviour is consistent with python
        let scores = Arc::new(Float32Array::from(vec![1.0, 1.0, 1.0, 1.0, 0.9999999]));
        let batch =
            RecordBatch::try_new(schema.clone(), vec![names.clone(), scores.clone()]).unwrap();
        let result = normalize_scores(batch.clone(), "score", None).unwrap();
        let scores: Float32Array = downcast_array(result.column(1));
        assert_eq!(
            scores.iter().map(|e| e.unwrap()).collect::<Vec<_>>(),
            vec![
                1.0 - 0.9999999,
                1.0 - 0.9999999,
                1.0 - 0.9999999,
                1.0 - 0.9999999,
                0.0
            ]
        );

        // check that it can handle if all the scores are 0
        let scores = Arc::new(Float32Array::from(vec![0.0, 0.0, 0.0, 0.0, 0.0]));
        let batch =
            RecordBatch::try_new(schema.clone(), vec![names.clone(), scores.clone()]).unwrap();
        let result = normalize_scores(batch.clone(), "score", None).unwrap();
        let scores: Float32Array = downcast_array(result.column(1));
        assert_eq!(
            scores.iter().map(|e| e.unwrap()).collect::<Vec<_>>(),
            vec![0.0, 0.0, 0.0, 0.0, 0.0]
        );
    }

    fn batch(ids: Vec<&str>, score: &str) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Arc::new(Field::new("id", DataType::Utf8, false)),
            Arc::new(Field::new(score, DataType::Float32, false)),
        ]));
        let scores = Float32Array::from(vec![0.0_f32; ids.len()]);
        RecordBatch::try_new(
            schema,
            vec![Arc::new(StringArray::from(ids)), Arc::new(scores)],
        )
        .unwrap()
    }

    fn row_ids(batch: &RecordBatch) -> Vec<u64> {
        let ids: UInt64Array = downcast_array(batch.column_by_name(ROW_ID).unwrap());
        ids.values().to_vec()
    }

    /// The same key in both legs has to land on the same id or the fusion will
    /// not dedup, and ids must ascend in fusion order so the tie break holds.
    #[test]
    fn test_surrogate_row_ids_are_shared_across_legs_in_first_seen_order() {
        let mut batches = vec![
            batch(vec!["a", "b", "c"], DIST_COL),
            batch(vec!["b", "d"], SCORE_COL),
        ];
        stamp_surrogate_row_ids(&mut batches, &["id".to_string()]).unwrap();

        assert_eq!(row_ids(&batches[0]), vec![0, 1, 2]);
        assert_eq!(
            row_ids(&batches[1]),
            vec![1, 3],
            "b keeps the id the vector leg gave it"
        );
    }

    /// A string key is the common case and cannot be cast to the u64 the
    /// rerankers read, which is the whole reason for the surrogate.
    #[test]
    fn test_surrogate_row_ids_repeat_within_a_leg() {
        let mut batches = vec![batch(vec!["a", "a", "b"], DIST_COL)];
        stamp_surrogate_row_ids(&mut batches, &["id".to_string()]).unwrap();
        assert_eq!(row_ids(&batches[0]), vec![0, 0, 1]);
    }

    #[test]
    fn test_surrogate_row_ids_support_a_composite_key() {
        let schema = Arc::new(Schema::new(vec![
            Arc::new(Field::new("tenant", DataType::Utf8, false)),
            Arc::new(Field::new("id", DataType::Int32, false)),
        ]));
        let make = |tenants: Vec<&str>, ids: Vec<i32>| {
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(StringArray::from(tenants)),
                    Arc::new(arrow_array::Int32Array::from(ids)),
                ],
            )
            .unwrap()
        };
        let mut batches = vec![make(vec!["x", "y"], vec![1, 1]), make(vec!["y"], vec![1])];
        stamp_surrogate_row_ids(&mut batches, &["tenant".to_string(), "id".to_string()]).unwrap();

        assert_eq!(
            row_ids(&batches[0]),
            vec![0, 1],
            "same id, different tenant"
        );
        assert_eq!(row_ids(&batches[1]), vec![1]);
    }

    /// Both legs empty: `query_schemas` synthesizes a schema with no key column,
    /// and there is no row that would need one.
    #[test]
    fn test_surrogate_row_ids_tolerate_the_both_empty_schema() {
        let mut batches = vec![
            RecordBatch::new_empty(Arc::new(empty_vec_schema())),
            RecordBatch::new_empty(Arc::new(empty_fts_schema())),
        ];
        // The synthesized schemas already carry ROW_ID, so stamping would
        // duplicate the field name; drop it first the way the caller does.
        for b in batches.iter_mut() {
            let keep: Vec<_> = b
                .schema()
                .fields()
                .iter()
                .filter(|f| f.name() != ROW_ID)
                .map(|f| b.schema().index_of(f.name()).unwrap())
                .collect();
            *b = b.project(&keep).unwrap();
        }
        stamp_surrogate_row_ids(&mut batches, &["id".to_string()]).unwrap();
        assert_eq!(row_ids(&batches[0]), Vec::<u64>::new());
    }

    /// Rows with no key would silently fuse into one bucket, so refuse.
    #[test]
    fn test_surrogate_row_ids_reject_a_missing_key_column() {
        let mut batches = vec![batch(vec!["a"], DIST_COL)];
        let err = stamp_surrogate_row_ids(&mut batches, &["nope".to_string()]).unwrap_err();
        assert!(err.to_string().contains("nope"), "{err}");
    }

    #[test]
    fn test_surrogate_row_ids_reject_a_table_with_no_primary_key() {
        let mut batches = vec![batch(vec!["a"], DIST_COL)];
        let err = stamp_surrogate_row_ids(&mut batches, &[]).unwrap_err();
        assert!(err.to_string().contains("unenforced primary key"), "{err}");
    }
}
