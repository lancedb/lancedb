// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::collections::VecDeque;

use crate::{Token, TokenFilter, TokenStream, Tokenizer};

/// Splits code identifiers into subword tokens.
///
/// The original identifier can be preserved at the first subword position. If
/// a token is not a compound identifier then it is passed through unchanged.
///
/// # Examples
///
/// ```
/// use lance_tokenizer::{CodeLexTokenizer, TextAnalyzer, TokenStream, WordDelimiterFilter};
///
/// let mut analyzer = TextAnalyzer::builder(CodeLexTokenizer::new(false))
///     .filter(WordDelimiterFilter::new(true, true))
///     .build();
/// let mut stream = analyzer.token_stream("parseHTML2JSON");
///
/// let mut tokens = Vec::new();
/// stream.process(&mut |token| tokens.push(token.text.clone()));
/// assert_eq!(tokens, vec!["parseHTML2JSON", "parse", "HTML", "2", "JSON"]);
/// ```
#[derive(Clone)]
pub struct WordDelimiterFilter {
    preserve_original: bool,
    split_on_numerics: bool,
}

impl WordDelimiterFilter {
    pub fn new(preserve_original: bool, split_on_numerics: bool) -> Self {
        Self {
            preserve_original,
            split_on_numerics,
        }
    }
}

impl TokenFilter for WordDelimiterFilter {
    type Tokenizer<T: Tokenizer> = WordDelimiterFilterWrapper<T>;

    fn transform<T: Tokenizer>(self, tokenizer: T) -> Self::Tokenizer<T> {
        WordDelimiterFilterWrapper {
            tokenizer,
            preserve_original: self.preserve_original,
            split_on_numerics: self.split_on_numerics,
        }
    }
}

#[derive(Clone)]
/// Tokenizer wrapper produced by [`WordDelimiterFilter`].
pub struct WordDelimiterFilterWrapper<T> {
    tokenizer: T,
    preserve_original: bool,
    split_on_numerics: bool,
}

impl<T: Tokenizer> Tokenizer for WordDelimiterFilterWrapper<T> {
    type TokenStream<'a> = WordDelimiterTokenStream<T::TokenStream<'a>>;

    fn token_stream<'a>(&'a mut self, text: &'a str) -> Self::TokenStream<'a> {
        WordDelimiterTokenStream {
            tail: self.tokenizer.token_stream(text),
            current: Token::default(),
            pending: VecDeque::new(),
            preserve_original: self.preserve_original,
            split_on_numerics: self.split_on_numerics,
            position_offset: 0,
        }
    }
}

/// Token stream produced by [`WordDelimiterFilterWrapper`].
pub struct WordDelimiterTokenStream<T> {
    tail: T,
    current: Token,
    pending: VecDeque<Token>,
    preserve_original: bool,
    split_on_numerics: bool,
    position_offset: usize,
}

#[derive(Clone, Copy)]
struct CharInfo {
    offset: usize,
    ch: char,
}

fn is_identifier_text(text: &str) -> bool {
    text.chars().any(|ch| ch == '_' || ch.is_alphanumeric())
}

fn split_identifier(text: &str, split_on_numerics: bool) -> Vec<(usize, usize)> {
    let mut pieces = Vec::new();
    let mut segment_start = None;
    for (offset, ch) in text.char_indices() {
        if ch == '_' {
            if let Some(start) = segment_start.take() {
                split_segment(text, start, offset, split_on_numerics, &mut pieces);
            }
        } else if segment_start.is_none() {
            segment_start = Some(offset);
        }
    }
    if let Some(start) = segment_start {
        split_segment(text, start, text.len(), split_on_numerics, &mut pieces);
    }
    pieces
}

fn split_segment(
    text: &str,
    start: usize,
    end: usize,
    split_on_numerics: bool,
    pieces: &mut Vec<(usize, usize)>,
) {
    if start == end {
        return;
    }
    let chars = text[start..end]
        .char_indices()
        .map(|(offset, ch)| CharInfo {
            offset: start + offset,
            ch,
        })
        .collect::<Vec<_>>();
    let mut piece_start = start;
    for idx in 1..chars.len() {
        if is_boundary(&chars, idx, split_on_numerics) {
            pieces.push((piece_start, chars[idx].offset));
            piece_start = chars[idx].offset;
        }
    }
    pieces.push((piece_start, end));
}

fn is_boundary(chars: &[CharInfo], idx: usize, split_on_numerics: bool) -> bool {
    let prev = chars[idx - 1].ch;
    let cur = chars[idx].ch;
    if split_on_numerics && prev.is_ascii_digit() != cur.is_ascii_digit() {
        return true;
    }
    if prev.is_lowercase() && cur.is_uppercase() {
        return true;
    }
    if prev.is_uppercase()
        && cur.is_uppercase()
        && chars
            .get(idx + 1)
            .is_some_and(|next| next.ch.is_lowercase())
    {
        return true;
    }
    false
}

impl<T: TokenStream> WordDelimiterTokenStream<T> {
    fn refill(&mut self) -> bool {
        while self.pending.is_empty() {
            if !self.tail.advance() {
                return false;
            }
            self.enqueue_current_tail_token();
        }
        true
    }

    fn enqueue_current_tail_token(&mut self) {
        let token = self.tail.token();
        let adjusted_position = token.position.saturating_add(self.position_offset);
        if !is_identifier_text(&token.text) {
            let mut token = token.clone();
            token.position = adjusted_position;
            self.pending.push_back(token);
            return;
        }

        let parts = split_identifier(&token.text, self.split_on_numerics);
        if parts.len() <= 1 {
            let mut token = token.clone();
            token.position = adjusted_position;
            self.pending.push_back(token);
            return;
        }

        if self.preserve_original {
            let mut original = token.clone();
            original.position = adjusted_position;
            original.position_length = parts.len();
            self.pending.push_back(original);
        }

        for (part_idx, (start, end)) in parts.iter().copied().enumerate() {
            let mut part = token.clone();
            part.offset_from = token.offset_from + start;
            part.offset_to = token.offset_from + end;
            part.position = adjusted_position + part_idx;
            part.position_length = 1;
            part.text.clear();
            part.text.push_str(&token.text[start..end]);
            self.pending.push_back(part);
        }
        self.position_offset += parts.len() - 1;
    }
}

impl<T: TokenStream> TokenStream for WordDelimiterTokenStream<T> {
    fn advance(&mut self) -> bool {
        if !self.refill() {
            return false;
        }
        let Some(token) = self.pending.pop_front() else {
            debug_assert!(false, "pending token should be available after refill");
            return false;
        };
        self.current = token;
        true
    }

    fn token(&self) -> &Token {
        &self.current
    }

    fn token_mut(&mut self) -> &mut Token {
        &mut self.current
    }
}

#[cfg(test)]
mod tests {
    use crate::{CodeLexTokenizer, TextAnalyzer, Token, WordDelimiterFilter};

    fn collect_tokens(text: &str) -> Vec<Token> {
        let mut analyzer = TextAnalyzer::builder(CodeLexTokenizer::new(false))
            .filter(WordDelimiterFilter::new(true, true))
            .build();
        let mut stream = analyzer.token_stream(text);
        let mut tokens = Vec::new();
        stream.process(&mut |token| tokens.push(token.clone()));
        tokens
    }

    #[test]
    fn test_word_delimiter_code_identifiers() {
        let tokens = collect_tokens("getUserName XMLHttpRequest parseHTML2JSON utf8_reader");
        let texts = tokens
            .iter()
            .map(|token| token.text.as_str())
            .collect::<Vec<_>>();
        assert_eq!(
            texts,
            vec![
                "getUserName",
                "get",
                "User",
                "Name",
                "XMLHttpRequest",
                "XML",
                "Http",
                "Request",
                "parseHTML2JSON",
                "parse",
                "HTML",
                "2",
                "JSON",
                "utf8_reader",
                "utf",
                "8",
                "reader",
            ]
        );
    }

    #[test]
    fn test_word_delimiter_positions_span_compounds() {
        let tokens = collect_tokens("getUserName next");
        let positions = tokens
            .iter()
            .map(|token| (token.text.as_str(), token.position, token.position_length))
            .collect::<Vec<_>>();
        assert_eq!(
            positions,
            vec![
                ("getUserName", 0, 3),
                ("get", 0, 1),
                ("User", 1, 1),
                ("Name", 2, 1),
                ("next", 3, 1),
            ]
        );
    }
}
