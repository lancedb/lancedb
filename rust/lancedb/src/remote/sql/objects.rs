// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use super::{SqlClient, materialized_view::quote_identifier};
use crate::{
    database::ExecuteQueryRequest,
    error::{Error, Result},
    job::Job,
    listing::{ListingOptions, ListingPage},
    utils::validate_namespace,
};
use arrow_array::{Array, LargeStringArray, ListArray, RecordBatch, StringArray, StringViewArray};
use futures::TryStreamExt;
use serde::{Serialize, de::DeserializeOwned};
use std::sync::Arc;

const DEFAULT_SQL_LISTING_LIMIT: u32 = 1_000;
const MAX_SQL_LISTING_LIMIT: u32 = 10_000;
const NEXT_PAGE_TOKEN_METADATA_KEY: &str = "lancedb.next_page_token";

impl SqlClient {
    pub(in crate::remote) async fn listing_page(
        &self,
        statement: &str,
        namespace: &[String],
        options: ListingOptions,
    ) -> Result<(Vec<RecordBatch>, Option<String>)> {
        validate_namespace(namespace)?;
        let limit = options.page_limit.unwrap_or(DEFAULT_SQL_LISTING_LIMIT);
        if limit > MAX_SQL_LISTING_LIMIT {
            return Err(Error::InvalidInput {
                message: format!("page limit must be between 0 and {MAX_SQL_LISTING_LIMIT}"),
            });
        }
        if options.page_token.as_ref().is_some_and(String::is_empty) {
            return Err(Error::InvalidInput {
                message: "page token must not be empty".into(),
            });
        }
        if limit == 0 {
            return Ok((Vec::new(), None));
        }
        let schema = if namespace.is_empty() {
            "public".to_owned()
        } else {
            namespace.join("$")
        };
        let mut statement = format!("{statement} IN {} LIMIT {limit}", quote_identifier(&schema));
        if let Some(token) = options.page_token {
            statement.push_str(&format!(" FROM {}", literal(&token)));
        }
        let query = self
            .execute(ExecuteQueryRequest {
                query: statement,
                default_namespace_path: namespace.to_vec(),
                parameters: None,
            })
            .await?;
        let reader = query.reader().await?;
        let token = reader
            .schema()
            .metadata()
            .get(NEXT_PAGE_TOKEN_METADATA_KEY)
            .cloned()
            .filter(|token| !token.is_empty());
        let batches = reader.try_collect::<Vec<_>>().await?;
        if batches.iter().map(RecordBatch::num_rows).sum::<usize>() > limit as usize {
            return Err(Error::Runtime {
                message: "SQL listing exceeded its requested page size".into(),
            });
        }
        Ok((batches, token))
    }

    pub(in crate::remote) async fn list_names(
        &self,
        statement: &str,
        namespace: &[String],
        options: ListingOptions,
    ) -> Result<ListingPage<String>> {
        let (batches, page_token) = self.listing_page(statement, namespace, options).await?;
        Ok(ListingPage {
            items: strings(&batches, "name")?,
            page_token,
        })
    }
}

pub(super) fn literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}
pub(super) fn strings(batches: &[RecordBatch], name: &str) -> Result<Vec<String>> {
    let mut result = Vec::new();
    for batch in batches {
        let column = batch.column_by_name(name).ok_or_else(|| Error::Runtime {
            message: format!("SQL result is missing string column '{name}'"),
        })?;
        if column.null_count() != 0 {
            return Err(Error::Runtime {
                message: format!("SQL result contains null '{name}'"),
            });
        }
        if let Some(values) = column.as_any().downcast_ref::<StringArray>() {
            result.extend(values.iter().map(|value| value.unwrap().to_owned()));
        } else if let Some(values) = column.as_any().downcast_ref::<LargeStringArray>() {
            result.extend(values.iter().map(|value| value.unwrap().to_owned()));
        } else if let Some(values) = column.as_any().downcast_ref::<StringViewArray>() {
            result.extend(values.iter().map(|value| value.unwrap().to_owned()));
        } else {
            return Err(Error::Runtime {
                message: format!("SQL result column '{name}' is not a string"),
            });
        }
    }
    Ok(result)
}

/// Read an optional `List<Utf8>` column. Older servers do not return the
/// column, so a missing column means no warnings rather than a bad result.
pub(super) fn string_lists(batches: &[RecordBatch], name: &str) -> Result<Vec<String>> {
    let mut values = Vec::new();
    for batch in batches {
        let Some(column) = batch.column_by_name(name) else {
            continue;
        };
        let lists = column
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| Error::Runtime {
                message: format!("SQL result column '{name}' is not a string list"),
            })?;
        for index in 0..lists.len() {
            if lists.is_null(index) {
                return Err(Error::Runtime {
                    message: format!("SQL result contains null '{name}'"),
                });
            }
            let list = lists.value(index);
            let strings =
                list.as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| Error::Runtime {
                        message: format!("SQL result column '{name}' contains a non-string value"),
                    })?;
            if strings.null_count() != 0 {
                return Err(Error::Runtime {
                    message: format!("SQL result column '{name}' contains null values"),
                });
            }
            values.extend(strings.iter().map(|value| value.unwrap().to_owned()));
        }
    }
    Ok(values)
}

pub(super) fn definition<T: DeserializeOwned>(batches: Vec<RecordBatch>) -> Result<T> {
    let values = strings(&batches, "definition")?;
    let [value]: [String; 1] = values.try_into().map_err(|_| Error::Runtime {
        message: "SQL definition command must return exactly one row".into(),
    })?;
    serde_json::from_str(&value).map_err(|error| Error::Runtime {
        message: format!("invalid SQL definition: {error}"),
    })
}
impl SqlClient {
    pub(super) async fn object_job<T, F>(
        &self,
        request: ExecuteQueryRequest,
        decode: F,
    ) -> Result<Job<T>>
    where
        T: Clone + Serialize + DeserializeOwned + Send + Sync + 'static,
        F: FnOnce(Vec<RecordBatch>) -> Result<T> + Send + 'static,
    {
        let query = Arc::new(self.execute(request).await?);
        let id = query.id();
        let execution = query.clone();
        let task = tokio::spawn(async move {
            let reader = execution.reader().await?;
            decode(reader.try_collect().await?)
        });
        Ok(Job::from_query_task(id, task, move || {
            let query = query.clone();
            async move { query.cancel().await }
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::{string_lists, strings};
    use arrow_array::builder::{ListBuilder, StringBuilder};
    use arrow_array::{ArrayRef, LargeStringArray, RecordBatch, StringArray, StringViewArray};
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

    fn warning_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![Field::new(
            "warnings",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            true,
        )]))
    }

    #[test]
    fn listing_strings_accept_all_arrow_string_representations() {
        for values in [
            Arc::new(StringArray::from(vec!["a", "z"])) as ArrayRef,
            Arc::new(LargeStringArray::from(vec!["a", "z"])) as ArrayRef,
            Arc::new(StringViewArray::from(vec!["a", "z"])) as ArrayRef,
        ] {
            let batch = RecordBatch::try_from_iter(vec![("name", values)]).unwrap();
            assert_eq!(strings(&[batch], "name").unwrap(), ["a", "z"]);
        }
        let batch = RecordBatch::try_from_iter(vec![(
            "name",
            Arc::new(StringViewArray::from(vec![Some("a"), None])) as ArrayRef,
        )])
        .unwrap();
        assert!(strings(&[batch], "name").is_err());
    }

    #[test]
    fn reads_warnings_after_an_empty_first_batch() {
        let schema = warning_schema();
        let empty = RecordBatch::new_empty(schema.clone());
        let mut builder = ListBuilder::new(StringBuilder::new());
        builder.values().append_value(
            "materialized view \"pinned\" references Function \"embed\" version \"1\"",
        );
        builder.append(true);
        let non_empty =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(builder.finish()) as ArrayRef])
                .unwrap();

        assert_eq!(
            string_lists(&[empty, non_empty], "warnings").unwrap(),
            ["materialized view \"pinned\" references Function \"embed\" version \"1\""]
        );
    }

    #[test]
    fn missing_warning_column_remains_an_empty_result() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "status",
            DataType::Utf8,
            false,
        )]));
        let batch = RecordBatch::new_empty(schema);

        assert_eq!(
            string_lists(&[batch], "warnings").unwrap(),
            Vec::<String>::new()
        );
    }

    #[test]
    fn null_or_non_list_warning_values_are_rejected() {
        let mut builder = ListBuilder::new(StringBuilder::new());
        builder.append(false);
        let null_list = RecordBatch::try_new(
            warning_schema(),
            vec![Arc::new(builder.finish()) as ArrayRef],
        )
        .unwrap();
        assert!(string_lists(&[null_list], "warnings").is_err());

        let non_list_schema = Arc::new(Schema::new(vec![Field::new(
            "warnings",
            DataType::Utf8,
            false,
        )]));
        let non_list = RecordBatch::new_empty(non_list_schema);
        assert!(string_lists(&[non_list], "warnings").is_err());
    }
}
