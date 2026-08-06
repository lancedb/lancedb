// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors
// SPDX-License-Identifier: MIT
// Adapted from Tantivy v0.24.2 raw tokenizer.
// Copyright (c) 2017-present Tantivy contributors.

use crate::{Token, TokenStream, Tokenizer};

#[derive(Clone, Default)]
pub struct RawTokenizer {
    token: Token,
}

pub struct RawTokenStream<'a> {
    token: &'a mut Token,
    has_token: bool,
}

impl Tokenizer for RawTokenizer {
    type TokenStream<'a> = RawTokenStream<'a>;

    fn token_stream<'a>(&'a mut self, text: &'a str) -> Self::TokenStream<'a> {
        self.token.reset();
        self.token.position = 0;
        self.token.position_length = 1;
        self.token.offset_from = 0;
        self.token.offset_to = text.len();
        self.token.text.clear();
        self.token.text.push_str(text);
        RawTokenStream {
            token: &mut self.token,
            has_token: true,
        }
    }
}

impl TokenStream for RawTokenStream<'_> {
    fn advance(&mut self) -> bool {
        let has_token = self.has_token;
        self.has_token = false;
        has_token
    }

    fn token(&self) -> &Token {
        self.token
    }

    fn token_mut(&mut self) -> &mut Token {
        self.token
    }
}
