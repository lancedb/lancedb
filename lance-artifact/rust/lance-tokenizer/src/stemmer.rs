// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors
// SPDX-License-Identifier: MIT
// Adapted from Tantivy v0.24.2 stemmer filter.
// Copyright (c) 2017-present Tantivy contributors.

use std::borrow::Cow;
use std::mem;

use frostem::Algorithm;
use serde::{Deserialize, Serialize};

use crate::{Token, TokenFilter, TokenStream, Tokenizer};

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Copy, Clone)]
pub enum Language {
    Arabic,
    Danish,
    Dutch,
    English,
    Finnish,
    French,
    German,
    Greek,
    Hungarian,
    Italian,
    Norwegian,
    Portuguese,
    Romanian,
    Russian,
    Spanish,
    Swedish,
    Tamil,
    Turkish,
}

impl Language {
    fn algorithm(self) -> Algorithm {
        match self {
            Self::Arabic => Algorithm::Arabic,
            Self::Danish => Algorithm::Danish,
            Self::Dutch => Algorithm::Dutch,
            Self::English => Algorithm::English,
            Self::Finnish => Algorithm::Finnish,
            Self::French => Algorithm::French,
            Self::German => Algorithm::German,
            Self::Greek => Algorithm::Greek,
            Self::Hungarian => Algorithm::Hungarian,
            Self::Italian => Algorithm::Italian,
            Self::Norwegian => Algorithm::Norwegian,
            Self::Portuguese => Algorithm::Portuguese,
            Self::Romanian => Algorithm::Romanian,
            Self::Russian => Algorithm::Russian,
            Self::Spanish => Algorithm::Spanish,
            Self::Swedish => Algorithm::Swedish,
            Self::Tamil => Algorithm::Tamil,
            Self::Turkish => Algorithm::Turkish,
        }
    }
}

#[derive(Clone)]
pub struct Stemmer {
    stemmer_algorithm: Algorithm,
}

impl Stemmer {
    pub fn new(language: Language) -> Self {
        Self {
            stemmer_algorithm: language.algorithm(),
        }
    }
}

impl Default for Stemmer {
    fn default() -> Self {
        Self::new(Language::English)
    }
}

impl TokenFilter for Stemmer {
    type Tokenizer<T: Tokenizer> = StemmerFilter<T>;

    fn transform<T: Tokenizer>(self, tokenizer: T) -> Self::Tokenizer<T> {
        StemmerFilter {
            stemmer_algorithm: self.stemmer_algorithm,
            inner: tokenizer,
        }
    }
}

#[derive(Clone)]
pub struct StemmerFilter<T> {
    stemmer_algorithm: Algorithm,
    inner: T,
}

impl<T: Tokenizer> Tokenizer for StemmerFilter<T> {
    type TokenStream<'a> = StemmerTokenStream<T::TokenStream<'a>>;

    fn token_stream<'a>(&'a mut self, text: &'a str) -> Self::TokenStream<'a> {
        StemmerTokenStream {
            tail: self.inner.token_stream(text),
            stemmer: frostem::Stemmer::new(self.stemmer_algorithm),
            buffer: String::new(),
        }
    }
}

pub struct StemmerTokenStream<T> {
    tail: T,
    stemmer: frostem::Stemmer,
    buffer: String,
}

impl<T: TokenStream> TokenStream for StemmerTokenStream<T> {
    fn advance(&mut self) -> bool {
        if !self.tail.advance() {
            return false;
        }
        let token = self.tail.token_mut();
        let stemmed = self.stemmer.stem(&token.text);
        match stemmed {
            Cow::Owned(stemmed) => token.text = stemmed,
            Cow::Borrowed(stemmed) => {
                self.buffer.clear();
                self.buffer.push_str(stemmed);
                mem::swap(&mut token.text, &mut self.buffer);
            }
        }
        true
    }

    fn token(&self) -> &Token {
        self.tail.token()
    }

    fn token_mut(&mut self) -> &mut Token {
        self.tail.token_mut()
    }
}

#[cfg(test)]
mod tests {
    use crate::{Language, RawTokenizer, Stemmer, TextAnalyzer, TokenStream};

    #[test]
    fn test_greek_stemmer_handles_multibyte_suffixes() {
        let mut analyzer = TextAnalyzer::builder(RawTokenizer::default())
            .filter(Stemmer::new(Language::Greek))
            .build();
        let mut stream = analyzer.token_stream("αντιθετε");

        assert!(stream.advance());
        assert_eq!(stream.token().text, "ανετ");
    }
}
