// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors
// SPDX-License-Identifier: MIT
// Adapted from lindera-tantivy v2.0.0.
// Copyright (c) lindera-tantivy contributors.

use std::path::Path;

use lindera::token::Token as LinderaToken;
use lindera::tokenizer::{Tokenizer as LinderaCoreTokenizer, TokenizerBuilder};

use crate::{TextAnalyzer, TextAnalyzerBuilder, Token, TokenStream, Tokenizer};

#[derive(Clone)]
pub struct LinderaTokenizer {
    tokenizer: LinderaCoreTokenizer,
    token: Token,
}

impl LinderaTokenizer {
    pub fn new() -> std::io::Result<Self> {
        let builder = TokenizerBuilder::new().map_err(invalid_data)?;
        let tokenizer = builder.build().map_err(invalid_data)?;
        Ok(Self {
            tokenizer,
            token: Token::default(),
        })
    }

    pub fn from_file(file_path: &Path) -> std::io::Result<Self> {
        let builder = TokenizerBuilder::from_file(file_path).map_err(invalid_data)?;
        let tokenizer = builder.build().map_err(invalid_data)?;
        Ok(Self {
            tokenizer,
            token: Token::default(),
        })
    }

    pub fn from_segmenter(segmenter: lindera::segmenter::Segmenter) -> Self {
        Self {
            tokenizer: LinderaCoreTokenizer::new(segmenter),
            token: Token::default(),
        }
    }

    pub fn analyzer(self) -> TextAnalyzer {
        TextAnalyzer::builder(self).build()
    }

    pub fn analyzer_builder(self) -> TextAnalyzerBuilder {
        TextAnalyzer::builder(self).dynamic()
    }
}

pub struct LinderaTokenStream<'a> {
    tokens: Vec<LinderaToken<'a>>,
    token: &'a mut Token,
}

impl<'a> TokenStream for LinderaTokenStream<'a> {
    fn advance(&mut self) -> bool {
        if self.tokens.is_empty() {
            return false;
        }
        let token = self.tokens.remove(0);
        self.token.text = token.surface.to_string();
        self.token.offset_from = token.byte_start;
        self.token.offset_to = token.byte_end;
        self.token.position = token.position;
        self.token.position_length = token.position_length;
        true
    }

    fn token(&self) -> &Token {
        self.token
    }

    fn token_mut(&mut self) -> &mut Token {
        self.token
    }
}

impl Tokenizer for LinderaTokenizer {
    type TokenStream<'a> = LinderaTokenStream<'a>;

    fn token_stream<'a>(&'a mut self, text: &'a str) -> Self::TokenStream<'a> {
        self.token.reset();
        LinderaTokenStream {
            tokens: self.tokenizer.tokenize(text).unwrap_or_default(),
            token: &mut self.token,
        }
    }
}

fn invalid_data(err: impl std::fmt::Debug) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidData, format!("{err:?}"))
}
