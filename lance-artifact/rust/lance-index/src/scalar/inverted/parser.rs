// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use super::DocumentGranularity;
use super::query::{
    BooleanQuery, BoostQuery, FtsQuery, MatchQuery, MultiMatchQuery, Occur, Operator, PhraseQuery,
};
use lance_core::{Error, Result};
use serde_json::Value;

pub trait JsonParser {
    fn from_json(value: &Value) -> Result<Self>
    where
        Self: Sized;
}

impl JsonParser for MatchQuery {
    fn from_json(value: &Value) -> Result<Self> {
        let column = value["column"].as_str().map(String::from);
        let terms = value["terms"]
            .as_str()
            .ok_or_else(|| Error::invalid_input("missing terms in match query"))?
            .to_string();
        let boost = value["boost"]
            .as_f64()
            .map(|v| v as f32)
            .unwrap_or(Self::default_boost());
        let fuzziness = match &value["fuzziness"] {
            Value::Number(num) if num.is_u64() => Some(num.as_u64().unwrap() as u32),
            Value::String(s) if s.as_str() == "auto" => None,
            Value::Null => None,
            _ => Some(0),
        };
        let max_expansions = value["max_expansions"]
            .as_u64()
            .map(|v| v as usize)
            .unwrap_or(Self::default_max_expansions());
        let operator = value["operator"]
            .as_str()
            .map(|s| s.try_into().unwrap_or(Operator::default()))
            .unwrap_or_default();
        let prefix_length = value["prefix_length"]
            .as_u64()
            .map(|v| v as u32)
            .unwrap_or(0);
        let document_granularity = parse_document_granularity(value)?;

        Ok(Self {
            column,
            terms,
            boost,
            fuzziness,
            max_expansions,
            operator,
            prefix_length,
            document_granularity,
        })
    }
}

impl JsonParser for PhraseQuery {
    fn from_json(value: &Value) -> Result<Self> {
        let column = value["column"].as_str().map(String::from);
        let terms = value["terms"]
            .as_str()
            .ok_or_else(|| Error::invalid_input("missing terms in phrase query"))?
            .to_string();
        let slop = value["slop"].as_u64().map(|v| v as u32).unwrap_or(0);
        let document_granularity = parse_document_granularity(value)?;

        Ok(Self {
            column,
            terms,
            slop,
            document_granularity,
        })
    }
}

fn parse_document_granularity(value: &Value) -> Result<Option<DocumentGranularity>> {
    match value.get("document_granularity") {
        None | Some(Value::Null) => Ok(None),
        Some(granularity) => serde_json::from_value(granularity.clone())
            .map(Some)
            .map_err(|error| {
                Error::invalid_input(format!(
                    "invalid document_granularity in FTS query: {error}"
                ))
            }),
    }
}

impl JsonParser for BoostQuery {
    fn from_json(value: &Value) -> Result<Self> {
        let positive = value["positive"]
            .as_object()
            .ok_or_else(|| Error::invalid_input("missing positive in boost query"))?;
        let positive_query = from_json_value(&Value::Object(positive.clone()))?;

        let negative = value["negative"]
            .as_object()
            .ok_or_else(|| Error::invalid_input("missing negative in boost query"))?;
        let negative_query = from_json_value(&Value::Object(negative.clone()))?;

        let negative_boost = value["negative_boost"].as_f64().map(|v| v as f32);

        Ok(Self::new(positive_query, negative_query, negative_boost))
    }
}

impl JsonParser for MultiMatchQuery {
    fn from_json(value: &Value) -> Result<Self> {
        let query = value["match_queries"]
            .as_array()
            .ok_or_else(|| Error::invalid_input("missing match_queries in multi_match query"))?;
        let query = query
            .iter()
            .map(MatchQuery::from_json)
            .collect::<Result<Vec<MatchQuery>>>()?;

        if query.is_empty() {
            return Err(Error::invalid_input("empty multi_match query"));
        }

        Ok(Self {
            match_queries: query,
        })
    }
}

impl JsonParser for BooleanQuery {
    fn from_json(value: &Value) -> Result<Self> {
        let mut clauses = Vec::new();

        if let Some(must) = value["must"].as_array() {
            for query_val in must {
                let query = from_json_value(query_val)?;
                clauses.push((Occur::Must, query));
            }
        }

        if let Some(should) = value["should"].as_array() {
            for query_val in should {
                let query = from_json_value(query_val)?;
                clauses.push((Occur::Should, query));
            }
        }

        if let Some(must_not) = value["must_not"].as_array() {
            for query_val in must_not {
                let query = from_json_value(query_val)?;
                clauses.push((Occur::MustNot, query));
            }
        }

        Ok(Self::new(clauses))
    }
}

fn from_json_value(value: &Value) -> Result<FtsQuery> {
    let value = value
        .as_object()
        .ok_or_else(|| Error::invalid_input("value must be a JSON object"))?;
    if value.len() != 1 {
        return Err(Error::invalid_input("value must be a single JSON object"));
    }

    let (query_type, query_val) = value.into_iter().next().unwrap();
    match query_type.as_str() {
        "match" => Ok(FtsQuery::Match(MatchQuery::from_json(query_val)?)),
        "phrase" => Ok(FtsQuery::Phrase(PhraseQuery::from_json(query_val)?)),
        "boost" => Ok(FtsQuery::Boost(BoostQuery::from_json(query_val)?)),
        "multi_match" => Ok(FtsQuery::MultiMatch(MultiMatchQuery::from_json(query_val)?)),
        "boolean" => Ok(FtsQuery::Boolean(BooleanQuery::from_json(query_val)?)),
        _ => Err(Error::invalid_input(format!(
            "unknown fts query type: {}",
            query_type
        ))),
    }
}

pub fn from_json(json: &str) -> Result<FtsQuery> {
    let value: Value = serde_json::from_str(json)
        .map_err(|e| Error::invalid_input(format!("invalid json: {}", e)))?;
    from_json_value(&value)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_from_json_match() {
        let json = r#"
        {
            "match": {
                "column": "text",
                "terms": "hello world",
                "boost": 2.0,
                "fuzziness": 1,
                "max_expansions": 10,
                "operator": "and",
                "prefix_length": 2
            }
        }
        "#;
        let fts_query = from_json(json).unwrap();
        let expected_query = FtsQuery::Match(MatchQuery {
            column: Some("text".to_string()),
            terms: "hello world".to_string(),
            boost: 2.0,
            fuzziness: Some(1),
            max_expansions: 10,
            operator: Operator::And,
            prefix_length: 2,
            document_granularity: None,
        });
        assert_eq!(fts_query, expected_query);
    }

    #[test]
    fn test_from_json_phrase() {
        let json = r#"
        {
            "phrase": {
                "column": "text",
                "terms": "hello world",
                "slop": 1
            }
        }"#;
        let fts_query = from_json(json).unwrap();
        let expected_query = FtsQuery::Phrase(PhraseQuery {
            column: Some("text".to_string()),
            terms: "hello world".to_string(),
            slop: 1,
            document_granularity: None,
        });
        assert_eq!(fts_query, expected_query);
    }

    #[test]
    fn test_from_json_boost() {
        let json = r#"
        {
            "boost": {
                "positive": {
                    "match": {
                        "column": "title",
                        "terms": "hello"
                    }
                },
                "negative": {
                    "phrase": {
                        "column": "body",
                        "terms": "world"
                    }
                },
                "negative_boost": 0.5
            }
        }"#;
        let fts_query = from_json(json).unwrap();
        let positive_query = Box::new(FtsQuery::Match(
            MatchQuery::new("hello".to_string())
                .with_column(Some("title".to_string()))
                .with_fuzziness(None),
        ));
        let negative_query = Box::new(FtsQuery::Phrase(
            PhraseQuery::new("world".to_string()).with_column(Some("body".to_string())),
        ));
        let expected_query = FtsQuery::Boost(BoostQuery {
            positive: positive_query,
            negative: negative_query,
            negative_boost: 0.5,
        });
        assert_eq!(fts_query, expected_query);
    }

    #[test]
    fn test_from_json_multi_match() {
        let json = r#"
        {
            "multi_match": {
                "match_queries": [
                    {
                        "column": "title",
                        "terms": "hello"
                    },
                    {
                        "column": "body",
                        "terms": "world"
                    }
                ]
            }
        }"#;
        let fts_query = from_json(json).unwrap();
        let match_queries = vec![
            MatchQuery::new("hello".to_string())
                .with_column(Some("title".to_string()))
                .with_fuzziness(None),
            MatchQuery::new("world".to_string())
                .with_column(Some("body".to_string()))
                .with_fuzziness(None),
        ];
        let expected_query = FtsQuery::MultiMatch(MultiMatchQuery { match_queries });
        assert_eq!(fts_query, expected_query);
    }

    #[test]
    fn test_from_json_boolean() {
        let json = r#"{
            "boolean": {
                "must": [
                    {
                        "match": {
                            "column": "text",
                            "terms": "hello"
                        }
                    }
                ],
                "should": [
                    {
                        "phrase": {
                            "column": "text",
                            "terms": "world"
                        }
                    }
                ],
                "must_not": []
            }
        }"#;
        let fts_query = from_json(json).unwrap();
        let must_query = FtsQuery::Match(
            MatchQuery::new("hello".to_string())
                .with_column(Some("text".to_string()))
                .with_fuzziness(None),
        );
        let should_query = FtsQuery::Phrase(
            PhraseQuery::new("world".to_string()).with_column(Some("text".to_string())),
        );

        let expected_query = FtsQuery::Boolean(BooleanQuery::new(vec![
            (Occur::Must, must_query),
            (Occur::Should, should_query),
        ]));
        assert_eq!(fts_query, expected_query);
    }
}
