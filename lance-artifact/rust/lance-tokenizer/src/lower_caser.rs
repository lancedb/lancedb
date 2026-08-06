// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors
// SPDX-License-Identifier: MIT
// Adapted from Tantivy v0.24.2 lower caser.
// Copyright (c) 2017-present Tantivy contributors.

use std::mem;

use crate::{Token, TokenFilter, TokenStream, Tokenizer};

#[derive(Clone)]
pub struct LowerCaser;

impl TokenFilter for LowerCaser {
    type Tokenizer<T: Tokenizer> = LowerCaserFilter<T>;

    fn transform<T: Tokenizer>(self, tokenizer: T) -> Self::Tokenizer<T> {
        LowerCaserFilter {
            tokenizer,
            buffer: String::new(),
        }
    }
}

#[derive(Clone)]
pub struct LowerCaserFilter<T> {
    tokenizer: T,
    buffer: String,
}

impl<T: Tokenizer> Tokenizer for LowerCaserFilter<T> {
    type TokenStream<'a> = LowerCaserTokenStream<'a, T::TokenStream<'a>>;

    fn token_stream<'a>(&'a mut self, text: &'a str) -> Self::TokenStream<'a> {
        self.buffer.clear();
        LowerCaserTokenStream {
            buffer: &mut self.buffer,
            tail: self.tokenizer.token_stream(text),
        }
    }
}

pub struct LowerCaserTokenStream<'a, T> {
    buffer: &'a mut String,
    tail: T,
}

fn to_lowercase_unicode(text: &str, output: &mut String) {
    output.clear();
    output.reserve(text.len());
    for ch in text.chars() {
        output.extend(ch.to_lowercase());
    }
}

fn is_lowercase_stable(text: &str) -> bool {
    text.chars().all(|ch| {
        let mut lower = ch.to_lowercase();
        lower.next() == Some(ch) && lower.next().is_none()
    })
}

impl<T: TokenStream> TokenStream for LowerCaserTokenStream<'_, T> {
    fn advance(&mut self) -> bool {
        if !self.tail.advance() {
            return false;
        }
        let token = self.tail.token_mut();
        if token.text.is_ascii() {
            token.text.make_ascii_lowercase();
        } else if !is_lowercase_stable(&token.text) {
            to_lowercase_unicode(&token.text, self.buffer);
            mem::swap(&mut token.text, self.buffer);
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
    use crate::{LowerCaser, RawTokenizer, TextAnalyzer, Token};

    fn collect_tokens(text: &str) -> Vec<Token> {
        let mut analyzer = TextAnalyzer::builder(RawTokenizer::default())
            .filter(LowerCaser)
            .build();
        let mut stream = analyzer.token_stream(text);
        let mut tokens = Vec::new();
        stream.process(&mut |token| tokens.push(token.clone()));
        tokens
    }

    #[test]
    fn test_lower_caser_unicode_changed() {
        let tokens = collect_tokens("İSTANBUL");
        assert_eq!(tokens[0].text, "i\u{307}stanbul");
    }

    #[test]
    fn test_lower_caser_unicode_unchanged() {
        let tokens = collect_tokens("こんにちは世界");
        assert_eq!(tokens[0].text, "こんにちは世界");
    }
}
