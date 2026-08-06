// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors
// SPDX-License-Identifier: MIT
// Adapted from Tantivy v0.24.2 tokenizer API.
// Copyright (c) 2017-present Tantivy contributors.

use std::borrow::{Borrow, BorrowMut};
use std::ops::{Deref, DerefMut};

/// Token emitted by a tokenizer.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Token {
    /// Byte offset of the first character.
    pub offset_from: usize,
    /// Byte offset after the last character.
    pub offset_to: usize,
    /// Logical token position.
    pub position: usize,
    /// Token text.
    pub text: String,
    /// Position length measured in original tokens.
    pub position_length: usize,
}

impl Default for Token {
    fn default() -> Self {
        Self {
            offset_from: 0,
            offset_to: 0,
            position: usize::MAX,
            text: String::new(),
            position_length: 1,
        }
    }
}

impl Token {
    /// Reset the token to its default state.
    pub fn reset(&mut self) {
        self.offset_from = 0;
        self.offset_to = 0;
        self.position = usize::MAX;
        self.text.clear();
        self.position_length = 1;
    }
}

/// Tokenizer splits text into a token stream.
pub trait Tokenizer: 'static + Clone + Send + Sync {
    /// Stream type emitted by the tokenizer.
    type TokenStream<'a>: TokenStream;

    /// Create a token stream for the provided text.
    fn token_stream<'a>(&'a mut self, text: &'a str) -> Self::TokenStream<'a>;
}

/// Token stream object-safe wrapper.
pub struct BoxTokenStream<'a>(Box<dyn TokenStream + 'a>);

impl<'a> BoxTokenStream<'a> {
    pub fn new<T: TokenStream + 'a>(token_stream: T) -> Self {
        Self(Box::new(token_stream))
    }
}

impl TokenStream for BoxTokenStream<'_> {
    fn advance(&mut self) -> bool {
        self.0.advance()
    }

    fn token(&self) -> &Token {
        self.0.token()
    }

    fn token_mut(&mut self) -> &mut Token {
        self.0.token_mut()
    }
}

impl<'a> Deref for BoxTokenStream<'a> {
    type Target = dyn TokenStream + 'a;

    fn deref(&self) -> &Self::Target {
        &*self.0
    }
}

impl DerefMut for BoxTokenStream<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut *self.0
    }
}

impl<'a> TokenStream for Box<dyn TokenStream + 'a> {
    fn advance(&mut self) -> bool {
        let token_stream: &mut dyn TokenStream = self.borrow_mut();
        token_stream.advance()
    }

    fn token(&self) -> &Token {
        let token_stream: &(dyn TokenStream + 'a) = self.borrow();
        token_stream.token()
    }

    fn token_mut(&mut self) -> &mut Token {
        let token_stream: &mut (dyn TokenStream + 'a) = self.borrow_mut();
        token_stream.token_mut()
    }
}

/// Consumable token stream.
pub trait TokenStream {
    /// Advance to the next token.
    fn advance(&mut self) -> bool;

    /// Access the current token.
    fn token(&self) -> &Token;

    /// Mutate the current token.
    fn token_mut(&mut self) -> &mut Token;

    /// Iterate to the next token and return it.
    fn next(&mut self) -> Option<&Token> {
        if self.advance() {
            Some(self.token())
        } else {
            None
        }
    }

    /// Consume the remaining stream into the provided sink.
    fn process(&mut self, sink: &mut dyn FnMut(&Token)) {
        while self.advance() {
            sink(self.token());
        }
    }
}

/// Filter that wraps a tokenizer with additional token-processing behavior.
pub trait TokenFilter: 'static + Send + Sync {
    /// Tokenizer produced by this filter.
    type Tokenizer<T: Tokenizer>: Tokenizer;

    /// Wrap the tokenizer.
    fn transform<T: Tokenizer>(self, tokenizer: T) -> Self::Tokenizer<T>;
}
