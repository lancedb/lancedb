// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors
// SPDX-License-Identifier: MIT
// Adapted from Tantivy v0.24.2 remove-long filter.
// Copyright (c) 2017-present Tantivy contributors.

use crate::{Token, TokenFilter, TokenStream, Tokenizer};

#[derive(Clone)]
pub struct RemoveLongFilter {
    length_limit: usize,
}

impl RemoveLongFilter {
    pub fn limit(length_limit: usize) -> Self {
        Self { length_limit }
    }
}

impl TokenFilter for RemoveLongFilter {
    type Tokenizer<T: Tokenizer> = RemoveLongFilterWrapper<T>;

    fn transform<T: Tokenizer>(self, tokenizer: T) -> Self::Tokenizer<T> {
        RemoveLongFilterWrapper {
            length_limit: self.length_limit,
            inner: tokenizer,
        }
    }
}

#[derive(Clone)]
pub struct RemoveLongFilterWrapper<T: Tokenizer> {
    length_limit: usize,
    inner: T,
}

impl<T: Tokenizer> Tokenizer for RemoveLongFilterWrapper<T> {
    type TokenStream<'a> = RemoveLongFilterStream<T::TokenStream<'a>>;

    fn token_stream<'a>(&'a mut self, text: &'a str) -> Self::TokenStream<'a> {
        RemoveLongFilterStream {
            token_length_limit: self.length_limit,
            tail: self.inner.token_stream(text),
        }
    }
}

pub struct RemoveLongFilterStream<T> {
    token_length_limit: usize,
    tail: T,
}

impl<T> RemoveLongFilterStream<T> {
    fn predicate(&self, token: &Token) -> bool {
        token.text.len() < self.token_length_limit
    }
}

impl<T: TokenStream> TokenStream for RemoveLongFilterStream<T> {
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
