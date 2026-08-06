// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use icu_segmenter::{WordSegmenter, WordSegmenterBorrowed, options::WordBreakInvariantOptions};

use crate::{TextAnalyzer, TextAnalyzerBuilder, Token, TokenStream, Tokenizer};

#[derive(Clone)]
pub struct IcuTokenizer {
    segmenter: WordSegmenterBorrowed<'static>,
    split_on_non_alphanumeric: bool,
}

impl Default for IcuTokenizer {
    fn default() -> Self {
        Self {
            segmenter: WordSegmenter::new_dictionary(WordBreakInvariantOptions::default()),
            split_on_non_alphanumeric: false,
        }
    }
}

impl IcuTokenizer {
    /// Split ICU word segments again on simple-tokenizer delimiters.
    pub fn with_simple_split(mut self) -> Self {
        self.split_on_non_alphanumeric = true;
        self
    }

    pub fn analyzer(self) -> TextAnalyzer {
        TextAnalyzer::builder(self).build()
    }

    pub fn analyzer_builder(self) -> TextAnalyzerBuilder {
        TextAnalyzer::builder(self).dynamic()
    }
}

pub struct IcuTokenStream {
    tokens: Vec<Token>,
    index: usize,
}

fn push_token(tokens: &mut Vec<Token>, text: &str, offset_from: usize, offset_to: usize) {
    if offset_from == offset_to {
        return;
    }

    let token_text = &text[offset_from..offset_to];
    if token_text.chars().any(char::is_alphanumeric) {
        tokens.push(Token {
            offset_from,
            offset_to,
            position: tokens.len(),
            text: token_text.to_owned(),
            position_length: 1,
        });
    }
}

fn push_tokens_split_on_non_alphanumeric(
    tokens: &mut Vec<Token>,
    text: &str,
    offset_from: usize,
    offset_to: usize,
) {
    let mut part_start = offset_from;
    for (relative_offset, c) in text[offset_from..offset_to].char_indices() {
        if !c.is_alphanumeric() {
            let delimiter_offset = offset_from + relative_offset;
            push_token(tokens, text, part_start, delimiter_offset);
            part_start = delimiter_offset + c.len_utf8();
        }
    }
    push_token(tokens, text, part_start, offset_to);
}

impl TokenStream for IcuTokenStream {
    fn advance(&mut self) -> bool {
        if self.index < self.tokens.len() {
            self.index += 1;
            true
        } else {
            false
        }
    }

    fn token(&self) -> &Token {
        &self.tokens[self.index - 1]
    }

    fn token_mut(&mut self) -> &mut Token {
        &mut self.tokens[self.index - 1]
    }
}

impl Tokenizer for IcuTokenizer {
    type TokenStream<'a> = IcuTokenStream;

    fn token_stream<'a>(&'a mut self, text: &'a str) -> Self::TokenStream<'a> {
        let mut boundaries = self.segmenter.segment_str(text);
        let mut tokens = Vec::new();
        let Some(mut offset_from) = boundaries.next() else {
            return IcuTokenStream { tokens, index: 0 };
        };

        for offset_to in boundaries {
            if self.split_on_non_alphanumeric {
                push_tokens_split_on_non_alphanumeric(&mut tokens, text, offset_from, offset_to);
            } else {
                push_token(&mut tokens, text, offset_from, offset_to);
            }
            offset_from = offset_to;
        }

        IcuTokenStream { tokens, index: 0 }
    }
}

#[cfg(test)]
mod tests {
    use crate::{IcuTokenizer, Token, TokenStream, Tokenizer};

    fn collect_tokens_with_split(text: &str, split_on_non_alphanumeric: bool) -> Vec<Token> {
        let mut tokenizer = IcuTokenizer::default();
        if split_on_non_alphanumeric {
            tokenizer = tokenizer.with_simple_split();
        }
        let mut stream = tokenizer.token_stream(text);
        let mut tokens = Vec::new();
        stream.process(&mut |token| tokens.push(token.clone()));
        tokens
    }

    fn collect_tokens(text: &str) -> Vec<Token> {
        collect_tokens_with_split(text, false)
    }

    #[test]
    fn test_icu_tokenizer_segments_mixed_text() {
        let tokens = collect_tokens("Hello, こんにちは世界!");

        assert_eq!(
            tokens
                .iter()
                .map(|token| token.text.as_str())
                .collect::<Vec<_>>(),
            vec!["Hello", "こんにちは", "世界"]
        );
        assert_eq!(
            tokens
                .iter()
                .map(|token| (token.offset_from, token.offset_to, token.position))
                .collect::<Vec<_>>(),
            vec![(0, 5, 0), (7, 22, 1), (22, 28, 2)]
        );
    }

    #[test]
    fn test_icu_tokenizer_skips_non_word_segments() {
        let tokens = collect_tokens("Mark'd ye his words?");

        assert_eq!(
            tokens
                .iter()
                .map(|token| token.text.as_str())
                .collect::<Vec<_>>(),
            vec!["Mark'd", "ye", "his", "words"]
        );
    }

    #[test]
    fn test_icu_tokenizer_splits_on_non_alphanumeric_when_enabled() {
        let tokens = collect_tokens_with_split("foo_bar__baz-alpha.beta", true);

        assert_eq!(
            tokens
                .iter()
                .map(|token| token.text.as_str())
                .collect::<Vec<_>>(),
            vec!["foo", "bar", "baz", "alpha", "beta"]
        );
        assert_eq!(
            tokens
                .iter()
                .map(|token| (token.offset_from, token.offset_to, token.position))
                .collect::<Vec<_>>(),
            vec![(0, 3, 0), (4, 7, 1), (9, 12, 2), (13, 18, 3), (19, 23, 4)]
        );
    }

    #[test]
    fn test_icu_tokenizer_split_control_keeps_icu_segmentation() {
        let tokens = collect_tokens_with_split("hello_world こんにちは世界", true);

        assert_eq!(
            tokens
                .iter()
                .map(|token| token.text.as_str())
                .collect::<Vec<_>>(),
            vec!["hello", "world", "こんにちは", "世界"]
        );
    }
}
