// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use arrow_schema::{DataType, Field};
use lance_arrow::ARROW_EXT_NAME_KEY;
use lance_arrow::json::JSON_EXT_NAME;
use lance_tokenizer::{BoxTokenStream, TextAnalyzer, Token, TokenStream};
use serde_json::Value;

/// Document type for full text search.
#[derive(Debug, Clone)]
pub enum DocType {
    Text,
    Json,
}

impl AsRef<str> for DocType {
    fn as_ref(&self) -> &str {
        match self {
            Self::Text => "text",
            Self::Json => "json",
        }
    }
}

impl TryFrom<&Field> for DocType {
    type Error = lance_core::Error;

    fn try_from(field: &Field) -> Result<Self, Self::Error> {
        match field.data_type() {
            DataType::Utf8 | DataType::LargeUtf8 => Ok(Self::Text),
            DataType::List(field) | DataType::LargeList(field)
                if matches!(field.data_type(), DataType::Utf8 | DataType::LargeUtf8) =>
            {
                Ok(Self::Text)
            }
            DataType::LargeBinary => match field.metadata().get(ARROW_EXT_NAME_KEY) {
                Some(name) if name.as_str() == JSON_EXT_NAME => Ok(Self::Json),
                _ => Err(lance_core::Error::invalid_input_source(
                    format!("field {} is not json", field.name()).into(),
                )),
            },
            _ => Err(lance_core::Error::invalid_input_source(
                format!("field {} is not json", field.name()).into(),
            )),
        }
    }
}

impl DocType {
    /// Get the length of the prefix before value.
    ///  - JSON Token: path,type,value
    ///  - Text Token: value
    pub fn prefix_len(&self, token: &str) -> usize {
        match self {
            Self::Json => {
                if let Some(pos) = token.find(',')
                    && let Some(second_pos) = token[pos + 1..].find(',')
                {
                    return pos + second_pos + 2;
                }
                panic!("json token must be in format of <path>,<type>,<value>")
            }
            Self::Text => 0,
        }
    }
}

/// Lance full text search tokenizer.
///
/// `LanceTokenizer` defines 2 methods for tokenization, normally they are the same, but sometimes
/// tokenizer needs different behavior for search and index. Take json document as an example:
/// 1. Query text is a triplet <path,type,value>, something like `a.b,str,123`. We shouldn't use
///    json in search, because it would be too complicated.
/// 2. Document text is a json string.
pub trait LanceTokenizer: Send + Sync + std::fmt::Debug {
    /// Tokenize query text for search.
    fn token_stream_for_search<'a>(&'a mut self, query_text: &'a str) -> BoxTokenStream<'a>;
    /// Tokenize document text for index.
    fn token_stream_for_doc<'a>(&'a mut self, text: &'a str) -> BoxTokenStream<'a>;
    /// Clone the tokenizer.
    fn box_clone(&self) -> Box<dyn LanceTokenizer>;
    /// Get document type.
    fn doc_type(&self) -> DocType;
}

impl Clone for Box<dyn LanceTokenizer> {
    fn clone(&self) -> Self {
        self.box_clone()
    }
}

#[derive(Clone)]
pub struct TextTokenizer {
    tokenizer: TextAnalyzer,
}

impl std::fmt::Debug for TextTokenizer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "TextTokenizer")
    }
}

impl TextTokenizer {
    pub fn new(tokenizer: TextAnalyzer) -> Self {
        Self { tokenizer }
    }
}

impl LanceTokenizer for TextTokenizer {
    fn token_stream_for_search<'a>(&'a mut self, query_text: &'a str) -> BoxTokenStream<'a> {
        self.tokenizer.token_stream(query_text)
    }

    fn token_stream_for_doc<'a>(&'a mut self, text: &'a str) -> BoxTokenStream<'a> {
        self.tokenizer.token_stream(text)
    }

    fn box_clone(&self) -> Box<dyn LanceTokenizer> {
        Box::new(self.clone())
    }

    fn doc_type(&self) -> DocType {
        DocType::Text
    }
}

#[derive(Clone)]
pub struct JsonTokenizer {
    tokenizer: TextAnalyzer,
}

impl JsonTokenizer {
    pub fn new(tokenizer: TextAnalyzer) -> Self {
        Self { tokenizer }
    }
}

impl std::fmt::Debug for JsonTokenizer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "JsonTokenizer")
    }
}

impl LanceTokenizer for JsonTokenizer {
    fn token_stream_for_search<'a>(&'a mut self, query_text: &'a str) -> BoxTokenStream<'a> {
        let tokens = flatten_triplet(query_text, &mut self.tokenizer).unwrap();
        BoxTokenStream::new(TTStream { tokens, index: 0 })
    }

    fn token_stream_for_doc<'a>(&'a mut self, text: &'a str) -> BoxTokenStream<'a> {
        let value: Value = match serde_json::from_slice(text.as_bytes()) {
            Ok(v) => v,
            Err(e) => {
                panic!("JSON parse error: {:?}", e);
            }
        };
        let mut tokens = vec![];
        let mut position = 0;
        flatten_json(&value, "", &mut tokens, &mut position, &mut self.tokenizer);
        BoxTokenStream::new(TTStream { tokens, index: 0 })
    }

    fn box_clone(&self) -> Box<dyn LanceTokenizer> {
        Box::new(self.clone())
    }

    fn doc_type(&self) -> DocType {
        DocType::Json
    }
}

fn flatten_triplet(text: &str, tokenizer: &mut TextAnalyzer) -> lance_core::Result<Vec<Token>> {
    let mut token_vec = Vec::new();
    let mut idx = 0;

    for triple in text.split(';') {
        let parts: Vec<&str> = triple.splitn(3, ',').collect();
        if parts.len() != 3 {
            return Err(lance_core::Error::invalid_input_source(
                format!("Invalid triple format: {}", triple).into(),
            ));
        }
        let field = parts[0];
        let v_type = parts[1];
        let value = parts[2];

        match v_type {
            "number" | "bool" | "null" => {
                let token = Token {
                    offset_from: 0,
                    offset_to: 0,
                    position: idx,
                    text: format!("{},{},{}", field, v_type, value),
                    position_length: 1,
                };
                token_vec.push(token);
                idx += 1;
            }
            "str" => {
                let mut tokens = tokenizer.token_stream(value);
                while let Some(token) = tokens.next() {
                    token_vec.push(Token {
                        offset_from: 0,
                        offset_to: 0,
                        position: idx,
                        text: format!("{},{},{}", field, v_type, token.text),
                        position_length: 1,
                    });
                    idx += 1;
                }
            }
            _ => {
                return Err(lance_core::Error::invalid_input_source(
                    format!("Invalid triple type: {}", v_type).into(),
                ));
            }
        }
    }
    Ok(token_vec)
}

fn flatten_json(
    value: &Value,
    prefix: &str,
    out: &mut Vec<Token>,
    position: &mut usize,
    tokenizer: &mut TextAnalyzer,
) {
    match value {
        Value::Object(map) => {
            for (k, v) in map {
                let next_prefix = if prefix.is_empty() {
                    k.clone()
                } else {
                    format!("{}.{}", prefix, k)
                };
                flatten_json(v, &next_prefix, out, position, tokenizer);
            }
        }
        Value::Array(arr) => {
            for v in arr.iter() {
                flatten_json(v, prefix, out, position, tokenizer);
            }
        }
        Value::String(text) => {
            let mut tokens = tokenizer.token_stream(text);
            while let Some(token) = tokens.next() {
                let token = Token {
                    offset_from: 0,
                    offset_to: 0,
                    position: *position,
                    text: format!("{},{},{}", prefix, "str", token.text),
                    position_length: 1,
                };
                *position += 1;
                out.push(token);
            }
        }
        _ => {
            let value_type = match value {
                Value::Null => "null",
                Value::Bool(_) => "bool",
                Value::Number(_) => "number",
                _ => unreachable!(),
            };
            let token = Token {
                offset_from: 0,
                offset_to: 0,
                position: *position,
                text: format!("{},{},{}", prefix, value_type, value),
                position_length: 1,
            };
            *position += 1;
            out.push(token);
        }
    }
}

struct TTStream {
    tokens: Vec<Token>,
    index: usize,
}

impl TokenStream for TTStream {
    fn advance(&mut self) -> bool {
        if self.index < self.tokens.len() {
            self.index += 1;
            true
        } else {
            false
        }
    }

    fn token(&self) -> &Token {
        &self.tokens[self.index - 1]
    }

    fn token_mut(&mut self) -> &mut Token {
        &mut self.tokens[self.index - 1]
    }
}

#[cfg(test)]
mod tests {
    use crate::scalar::inverted::tokenizer::document_tokenizer::{
        JsonTokenizer, LanceTokenizer, flatten_json, flatten_triplet,
    };
    use lance_tokenizer::{SimpleTokenizer, TextAnalyzer, Token};
    use serde_json::Value;

    #[test]
    fn test_json_tokenizer() {
        let text = r#"{
          "a": 1,
          "b": [
            {"c": "d"},
            {"c": "e"}
          ]
        }"#;
        let mut tokenizer =
            JsonTokenizer::new(TextAnalyzer::builder(SimpleTokenizer::default()).build());
        let mut stream = tokenizer.token_stream_for_doc(text);

        let mut tokens: Vec<Token> = vec![];
        while let Some(token) = stream.next() {
            tokens.push(token.clone());
        }

        assert_eq!(tokens.len(), 3);
        assert_token(&tokens[0], 0, "a,number,1");
        assert_token(&tokens[1], 1, "b.c,str,d");
        assert_token(&tokens[2], 2, "b.c,str,e");
    }

    #[test]
    fn test_flatten_json_text() {
        let json = r#"{
              "a": 1,
              "b": [
                {"c": "hello world"},
                {"c": "e"}
              ],
              "c": true,
              "d": null,
              "e": {
                "f": 1.0
              }
          }"#;
        let value: Value = serde_json::from_str(json).unwrap();

        let mut tokens = vec![];
        let mut tokenizer = TextAnalyzer::builder(SimpleTokenizer::default()).build();
        let mut position = 0;
        flatten_json(&value, "", &mut tokens, &mut position, &mut tokenizer);

        assert_eq!(7, tokens.len());
        assert_token(&tokens[0], 0, "a,number,1");
        assert_token(&tokens[1], 1, "b.c,str,hello");
        assert_token(&tokens[2], 2, "b.c,str,world");
        assert_token(&tokens[3], 3, "b.c,str,e");
        assert_token(&tokens[4], 4, "c,bool,true");
        assert_token(&tokens[5], 5, "d,null,null");
        assert_token(&tokens[6], 6, "e.f,number,1.0");
    }

    #[test]
    fn test_flatten_triplet() {
        let text = r#"a,number,1;b.c,str,d;b.c,str,e;d,str,hello world;e,number,1.0"#;
        let mut tokenizer = TextAnalyzer::builder(SimpleTokenizer::default()).build();
        let tokens = flatten_triplet(text, &mut tokenizer).unwrap();

        assert_eq!(tokens.len(), 6);
        assert_token(&tokens[0], 0, "a,number,1");
        assert_token(&tokens[1], 1, "b.c,str,d");
        assert_token(&tokens[2], 2, "b.c,str,e");
        assert_token(&tokens[3], 3, "d,str,hello");
        assert_token(&tokens[4], 4, "d,str,world");
        assert_token(&tokens[5], 5, "e,number,1.0");
    }

    fn assert_token(token: &Token, position: usize, text: &str) {
        assert_eq!(
            token.position, position,
            "expected position {position} but {token:?}"
        );
        assert_eq!(
            token.text.as_str(),
            text,
            "expected text {text} but {token:?}"
        );
    }
}
