// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use arrow::compute::{
    kernels::numeric::{div, sub},
    max, min,
};
use arrow_array::{cast::downcast_array, Float32Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SortOptions};
use lance::dataset::ROW_ID;
use lance_index::{scalar::inverted::SCORE_COL, vector::DIST_COL};
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
                assert_eq!("expected column bad_col not found in rank. found columns [\"name\", \"score\"]", message);
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
}
