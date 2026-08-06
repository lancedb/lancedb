// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors
// SPDX-License-Identifier: MIT
// Adapted from Tantivy v0.24.2 ngram tokenizer.
// Copyright (c) 2017-present Tantivy contributors.

use std::fmt::{Display, Formatter};

use crate::{Token, TokenStream, Tokenizer};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NgramError {
    message: String,
}

impl NgramError {
    fn invalid_argument(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl Display for NgramError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for NgramError {}

#[derive(Clone, Debug)]
pub struct NgramTokenizer {
    min_gram: usize,
    max_gram: usize,
    prefix_only: bool,
    token: Token,
}

impl NgramTokenizer {
    pub fn new(min_gram: usize, max_gram: usize, prefix_only: bool) -> Result<Self, NgramError> {
        if min_gram == 0 {
            return Err(NgramError::invalid_argument(
                "min_gram must be greater than 0",
            ));
        }
        if min_gram > max_gram {
            return Err(NgramError::invalid_argument(
                "min_gram must not be greater than max_gram",
            ));
        }
        Ok(Self {
            min_gram,
            max_gram,
            prefix_only,
            token: Token::default(),
        })
    }

    pub fn all_ngrams(min_gram: usize, max_gram: usize) -> Result<Self, NgramError> {
        Self::new(min_gram, max_gram, false)
    }

    pub fn prefix_only(min_gram: usize, max_gram: usize) -> Result<Self, NgramError> {
        Self::new(min_gram, max_gram, true)
    }
}

pub struct NgramTokenStream<'a> {
    ngram_charidx_iterator: StutteringIterator<CodepointFrontiers<'a>>,
    prefix_only: bool,
    text: &'a str,
    token: &'a mut Token,
}

impl Tokenizer for NgramTokenizer {
    type TokenStream<'a> = NgramTokenStream<'a>;

    fn token_stream<'a>(&'a mut self, text: &'a str) -> Self::TokenStream<'a> {
        self.token.reset();
        NgramTokenStream {
            ngram_charidx_iterator: StutteringIterator::new(
                CodepointFrontiers::for_str(text),
                self.min_gram,
                self.max_gram,
            ),
            prefix_only: self.prefix_only,
            text,
            token: &mut self.token,
        }
    }
}

impl TokenStream for NgramTokenStream<'_> {
    fn advance(&mut self) -> bool {
        if let Some((offset_from, offset_to)) = self.ngram_charidx_iterator.next() {
            if self.prefix_only && offset_from > 0 {
                return false;
            }
            self.token.position = 0;
            self.token.offset_from = offset_from;
            self.token.offset_to = offset_to;
            self.token.text.clear();
            self.token.text.push_str(&self.text[offset_from..offset_to]);
            true
        } else {
            false
        }
    }

    fn token(&self) -> &Token {
        self.token
    }

    fn token_mut(&mut self) -> &mut Token {
        self.token
    }
}

struct StutteringIterator<T> {
    underlying: T,
    min_gram: usize,
    max_gram: usize,
    memory: Vec<usize>,
    cursor: usize,
    gram_len: usize,
}

impl<T> StutteringIterator<T>
where
    T: Iterator<Item = usize>,
{
    fn new(mut underlying: T, min_gram: usize, max_gram: usize) -> Self {
        debug_assert!(min_gram > 0, "min_gram must be positive");
        let memory: Vec<usize> = (&mut underlying).take(max_gram + 1).collect();
        if memory.len() <= min_gram {
            Self {
                underlying,
                min_gram: 1,
                max_gram: 0,
                memory,
                cursor: 0,
                gram_len: 0,
            }
        } else {
            Self {
                underlying,
                min_gram,
                max_gram: memory.len() - 1,
                memory,
                cursor: 0,
                gram_len: min_gram,
            }
        }
    }
}

impl<T> Iterator for StutteringIterator<T>
where
    T: Iterator<Item = usize>,
{
    type Item = (usize, usize);

    fn next(&mut self) -> Option<Self::Item> {
        if self.gram_len > self.max_gram {
            self.gram_len = self.min_gram;
            if let Some(next_val) = self.underlying.next() {
                self.memory[self.cursor] = next_val;
            } else {
                self.max_gram -= 1;
            }
            self.cursor += 1;
            if self.cursor >= self.memory.len() {
                self.cursor = 0;
            }
        }
        if self.max_gram < self.min_gram {
            return None;
        }
        let start = self.memory[self.cursor % self.memory.len()];
        let stop = self.memory[(self.cursor + self.gram_len) % self.memory.len()];
        self.gram_len += 1;
        Some((start, stop))
    }
}

struct CodepointFrontiers<'a> {
    text: &'a str,
    next_offset: Option<usize>,
}

impl<'a> CodepointFrontiers<'a> {
    fn for_str(text: &'a str) -> Self {
        Self {
            text,
            next_offset: Some(0),
        }
    }
}

impl Iterator for CodepointFrontiers<'_> {
    type Item = usize;

    fn next(&mut self) -> Option<Self::Item> {
        let offset = self.next_offset?;
        if self.text.is_empty() {
            self.next_offset = None;
        } else {
            let width = utf8_codepoint_width(self.text.as_bytes()[0]);
            self.text = &self.text[width..];
            self.next_offset = Some(offset + width);
        }
        Some(offset)
    }
}

const CODEPOINT_UTF8_WIDTH: [u8; 16] = [1, 1, 1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 2, 3, 4];

fn utf8_codepoint_width(byte: u8) -> usize {
    CODEPOINT_UTF8_WIDTH[(byte as usize) >> 4] as usize
}
