// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors
// SPDX-License-Identifier: MIT
// Adapted from Tantivy v0.24.2 alphanum-only filter.
// Copyright (c) 2017-present Tantivy contributors.

use crate::{Token, TokenFilter, TokenStream, Tokenizer};

#[derive(Clone)]
pub struct AlphaNumOnlyFilter;

pub struct AlphaNumOnlyFilterStream<T> {
    tail: T,
}

impl<T> AlphaNumOnlyFilterStream<T> {
    fn predicate(&self, token: &Token) -> bool {
        token.text.chars().all(|ch| ch.is_ascii_alphanumeric())
    }
}

impl TokenFilter for AlphaNumOnlyFilter {
    type Tokenizer<T: Tokenizer> = AlphaNumOnlyFilterWrapper<T>;

    fn transform<T: Tokenizer>(self, tokenizer: T) -> Self::Tokenizer<T> {
        AlphaNumOnlyFilterWrapper(tokenizer)
    }
}

#[derive(Clone)]
pub struct AlphaNumOnlyFilterWrapper<T>(T);

impl<T: Tokenizer> Tokenizer for AlphaNumOnlyFilterWrapper<T> {
    type TokenStream<'a> = AlphaNumOnlyFilterStream<T::TokenStream<'a>>;

    fn token_stream<'a>(&'a mut self, text: &'a str) -> Self::TokenStream<'a> {
        AlphaNumOnlyFilterStream {
            tail: self.0.token_stream(text),
        }
    }
}

impl<T: TokenStream> TokenStream for AlphaNumOnlyFilterStream<T> {
    fn advance(&mut self) -> bool {
        while self.tail.advance() {
            if self.predicate(self.tail.token()) {
                return true;
            }
        }
        false
    }

    fn token(&self) -> &Token {
        self.tail.token()
    }

    fn token_mut(&mut self) -> &mut Token {
        self.tail.token_mut()
    }
}
