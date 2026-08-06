// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors
// SPDX-License-Identifier: MIT
// Adapted from Tantivy v0.24.2 tokenizer analyzer.
// Copyright (c) 2017-present Tantivy contributors.

use crate::{BoxTokenStream, TokenFilter, Tokenizer};

#[derive(Clone)]
pub struct TextAnalyzer {
    tokenizer: Box<dyn BoxableTokenizer>,
}

impl<T: Tokenizer + Clone> From<T> for TextAnalyzer {
    fn from(tokenizer: T) -> Self {
        Self::builder(tokenizer).build()
    }
}

impl Default for TextAnalyzer {
    fn default() -> Self {
        Self::from(crate::RawTokenizer::default())
    }
}

impl TextAnalyzer {
    pub fn builder<T: Tokenizer>(tokenizer: T) -> TextAnalyzerBuilder<T> {
        TextAnalyzerBuilder { tokenizer }
    }

    pub fn token_stream<'a>(&'a mut self, text: &'a str) -> BoxTokenStream<'a> {
        self.tokenizer.token_stream(text)
    }
}

pub trait BoxableTokenizer: 'static + Send + Sync {
    fn box_token_stream<'a>(&'a mut self, text: &'a str) -> BoxTokenStream<'a>;

    fn box_clone(&self) -> Box<dyn BoxableTokenizer>;
}

impl<T: Tokenizer> BoxableTokenizer for T {
    fn box_token_stream<'a>(&'a mut self, text: &'a str) -> BoxTokenStream<'a> {
        BoxTokenStream::new(self.token_stream(text))
    }

    fn box_clone(&self) -> Box<dyn BoxableTokenizer> {
        Box::new(self.clone())
    }
}

impl Clone for Box<dyn BoxableTokenizer> {
    fn clone(&self) -> Self {
        (**self).box_clone()
    }
}

impl Tokenizer for Box<dyn BoxableTokenizer> {
    type TokenStream<'a> = BoxTokenStream<'a>;

    fn token_stream<'a>(&'a mut self, text: &'a str) -> Self::TokenStream<'a> {
        (**self).box_token_stream(text)
    }
}

pub struct TextAnalyzerBuilder<T = Box<dyn BoxableTokenizer>> {
    tokenizer: T,
}

impl<T: Tokenizer> TextAnalyzerBuilder<T> {
    pub fn filter<F: TokenFilter>(self, token_filter: F) -> TextAnalyzerBuilder<F::Tokenizer<T>> {
        TextAnalyzerBuilder {
            tokenizer: token_filter.transform(self.tokenizer),
        }
    }

    pub fn dynamic(self) -> TextAnalyzerBuilder {
        TextAnalyzerBuilder {
            tokenizer: Box::new(self.tokenizer),
        }
    }

    pub fn filter_dynamic<F: TokenFilter>(self, token_filter: F) -> TextAnalyzerBuilder {
        self.filter(token_filter).dynamic()
    }

    pub fn build(self) -> TextAnalyzer {
        TextAnalyzer {
            tokenizer: Box::new(self.tokenizer),
        }
    }
}
