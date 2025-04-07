// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use lancedb::index::scalar::{BoostQuery, FtsQuery, MatchQuery, MultiMatchQuery, PhraseQuery};
use lancedb::DistanceType;

pub fn parse_distance_type(distance_type: impl AsRef<str>) -> napi::Result<DistanceType> {
    match distance_type.as_ref().to_lowercase().as_str() {
        "l2" => Ok(DistanceType::L2),
        "cosine" => Ok(DistanceType::Cosine),
        "dot" => Ok(DistanceType::Dot),
        "hamming" => Ok(DistanceType::Hamming),
        _ => Err(napi::Error::from_reason(format!(
            "Invalid distance type '{}'.  Must be one of l2, cosine, dot, or hamming",
            distance_type.as_ref()
        ))),
    }
}

pub fn parse_fts_query(query: &napi::JsObject) -> napi::Result<FtsQuery> {
    let query_type = query
        .get_property_names()?
        .get_element::<napi::JsString>(0)?;
    let query_type = query_type.into_utf8()?.into_owned()?;
    let query_value =
        query
            .get::<_, napi::JsObject>(&query_type)?
            .ok_or(napi::Error::from_reason(format!(
                "query value {} not found",
                query_type
            )))?;

    match query_type.as_str() {
        "match" => {
            let column = query_value
                .get_property_names()?
                .get_element::<napi::JsString>(0)?
                .into_utf8()?
                .into_owned()?;
            let params =
                query_value
                    .get::<_, napi::JsObject>(&column)?
                    .ok_or(napi::Error::from_reason(format!(
                        "column {} not found",
                        column
                    )))?;

            let query = params
                .get::<_, napi::JsString>("query")?
                .ok_or(napi::Error::from_reason("query not found"))?
                .into_utf8()?
                .into_owned()?;
            let boost = params
                .get::<_, napi::JsNumber>("boost")?
                .ok_or(napi::Error::from_reason("boost not found"))?
                .get_double()? as f32;
            let fuzziness = params
                .get::<_, napi::JsNumber>("fuzziness")?
                .map(|f| f.get_uint32())
                .transpose()?;
            let max_expansions = params
                .get::<_, napi::JsNumber>("max_expansions")?
                .ok_or(napi::Error::from_reason("max_expansions not found"))?
                .get_uint32()? as usize;

            let query = MatchQuery::new(query)
                .with_column(Some(column))
                .with_boost(boost)
                .with_fuzziness(fuzziness)
                .with_max_expansions(max_expansions);
            Ok(query.into())
        }

        "match_phrase" => {
            let column = query_value
                .get_property_names()?
                .get_element::<napi::JsString>(0)?
                .into_utf8()?
                .into_owned()?;
            let query = query_value
                .get::<_, napi::JsString>(&column)?
                .ok_or(napi::Error::from_reason(format!(
                    "column {} not found",
                    column
                )))?
                .into_utf8()?
                .into_owned()?;

            let query = PhraseQuery::new(query).with_column(Some(column));
            Ok(query.into())
        }

        "boost" => {
            let positive = query_value
                .get::<_, napi::JsObject>("positive")?
                .ok_or(napi::Error::from_reason("positive not found"))?;

            let negative = query_value
                .get::<_, napi::JsObject>("negative")?
                .ok_or(napi::Error::from_reason("negative not found"))?;
            let negative_boost = query_value
                .get::<_, napi::JsNumber>("negative_boost")?
                .ok_or(napi::Error::from_reason("negative_boost not found"))?
                .get_double()? as f32;

            let positive = parse_fts_query(&positive)?;
            let negative = parse_fts_query(&negative)?;
            let query = BoostQuery::new(positive, negative, Some(negative_boost));
            Ok(query.into())
        }

        "multi_match" => {
            let query = query_value
                .get::<_, napi::JsString>("query")?
                .ok_or(napi::Error::from_reason("query not found"))?
                .into_utf8()?
                .into_owned()?;
            let columns_array = query_value
                .get::<_, napi::JsTypedArray>("columns")?
                .ok_or(napi::Error::from_reason("columns not found"))?;
            let columns_num = columns_array.get_array_length()?;
            let mut columns = Vec::with_capacity(columns_num as usize);
            for i in 0..columns_num {
                let column = columns_array
                    .get_element::<napi::JsString>(i)?
                    .into_utf8()?
                    .into_owned()?;
                columns.push(column);
            }
            let boost_array = query_value
                .get::<_, napi::JsTypedArray>("boost")?
                .ok_or(napi::Error::from_reason("boost not found"))?;
            if boost_array.get_array_length()? != columns_num {
                return Err(napi::Error::from_reason(format!(
                    "boost array length ({}) does not match columns length ({})",
                    boost_array.get_array_length()?,
                    columns_num
                )));
            }
            let mut boost = Vec::with_capacity(columns_num as usize);
            for i in 0..columns_num {
                let b = boost_array.get_element::<napi::JsNumber>(i)?.get_double()? as f32;
                boost.push(b);
            }

            let query =
                MultiMatchQuery::try_new_with_boosts(query, columns, boost).map_err(|e| {
                    napi::Error::from_reason(format!("Error creating MultiMatchQuery: {}", e))
                })?;

            Ok(query.into())
        }

        _ => Err(napi::Error::from_reason(format!(
            "Unsupported query type: {}",
            query_type
        ))),
    }
}
