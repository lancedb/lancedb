// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors
// SPDX-License-Identifier: MIT
// Adapted from Tantivy v0.24.2 whitespace tokenizer.
// Copyright (c) 2017-present Tantivy contributors.

use std::str::CharIndices;

use crate::{Token, TokenStream, Tokenizer};

#[derive(Clone, Default)]
pub struct WhitespaceTokenizer {
    token: Token,
}

pub struct WhitespaceTokenStream<'a> {
    text: &'a str,
    chars: CharIndices<'a>,
    token: &'a mut Token,
}

impl Tokenizer for WhitespaceTokenizer {
    type TokenStream<'a> = WhitespaceTokenStream<'a>;

    fn token_stream<'a>(&'a mut self, text: &'a str) -> Self::TokenStream<'a> {
        self.token.reset();
        WhitespaceTokenStream {
            text,
            chars: text.char_indices(),
            token: &mut self.token,
        }
    }
}

impl WhitespaceTokenStream<'_> {
    fn search_token_end(&mut self) -> usize {
        (&mut self.chars)
            .filter(|(_, c)| c.is_ascii_whitespace())
            .map(|(offset, _)| offset)
            .next()
            .unwrap_or(self.text.len())
    }
}

impl TokenStream for WhitespaceTokenStream<'_> {
    fn advance(&mut self) -> bool {
        self.token.text.clear();
        self.token.position = self.token.position.wrapping_add(1);
        while let Some((offset_from, c)) = self.chars.next() {
            if !c.is_ascii_whitespace() {
                let offset_to = self.search_token_end();
                self.token.offset_from = offset_from;
                self.token.offset_to = offset_to;
                self.token.text.push_str(&self.text[offset_from..offset_to]);
                return true;
            }
        }
        false
    }

    fn token(&self) -> &Token {
        self.token
    }

    fn token_mut(&mut self) -> &mut Token {
        self.token
    }
}
