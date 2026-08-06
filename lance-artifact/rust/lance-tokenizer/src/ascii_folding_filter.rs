// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::mem;

use unicode_normalization::{UnicodeNormalization, char::is_combining_mark};

use crate::{Token, TokenFilter, TokenStream, Tokenizer};

#[derive(Clone)]
pub struct AsciiFoldingFilter;

impl TokenFilter for AsciiFoldingFilter {
    type Tokenizer<T: Tokenizer> = AsciiFoldingFilterWrapper<T>;

    fn transform<T: Tokenizer>(self, tokenizer: T) -> Self::Tokenizer<T> {
        AsciiFoldingFilterWrapper {
            tokenizer,
            buffer: String::new(),
        }
    }
}

#[derive(Clone)]
pub struct AsciiFoldingFilterWrapper<T> {
    tokenizer: T,
    buffer: String,
}

impl<T: Tokenizer> Tokenizer for AsciiFoldingFilterWrapper<T> {
    type TokenStream<'a> = AsciiFoldingFilterTokenStream<'a, T::TokenStream<'a>>;

    fn token_stream<'a>(&'a mut self, text: &'a str) -> Self::TokenStream<'a> {
        self.buffer.clear();
        AsciiFoldingFilterTokenStream {
            buffer: &mut self.buffer,
            tail: self.tokenizer.token_stream(text),
        }
    }
}

pub struct AsciiFoldingFilterTokenStream<'a, T> {
    buffer: &'a mut String,
    tail: T,
}

impl<T: TokenStream> TokenStream for AsciiFoldingFilterTokenStream<'_, T> {
    fn advance(&mut self) -> bool {
        if !self.tail.advance() {
            return false;
        }
        let token = self.tail.token_mut();
        if !token.text.is_ascii() {
            to_ascii(&token.text, self.buffer);
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

fn to_ascii(text: &str, output: &mut String) {
    output.clear();
    output.reserve(text.len());
    for ch in text.chars() {
        if ch.is_ascii() {
            output.push(ch);
            continue;
        }

        if let Some(mapped) = fold_char(ch) {
            output.push_str(mapped);
            continue;
        }

        let original_len = output.len();
        for decomposed in ch.nfkd() {
            if decomposed.is_ascii() {
                output.push(decomposed);
            } else if is_combining_mark(decomposed) {
                continue;
            } else if let Some(mapped) = fold_char(decomposed) {
                output.push_str(mapped);
            }
        }

        if output.len() == original_len {
            output.push(ch);
        }
    }
}

fn fold_char(ch: char) -> Option<&'static str> {
    match ch {
        'ß' => Some("ss"),
        'ẞ' => Some("SS"),
        'Æ' => Some("AE"),
        'æ' => Some("ae"),
        'Œ' => Some("OE"),
        'œ' => Some("oe"),
        'Ø' => Some("O"),
        'ø' => Some("o"),
        'Ł' => Some("L"),
        'ł' => Some("l"),
        'Đ' | 'Ð' => Some("D"),
        'đ' | 'ð' => Some("d"),
        'Þ' => Some("TH"),
        'þ' => Some("th"),
        'Ħ' => Some("H"),
        'ħ' => Some("h"),
        'Ŧ' => Some("T"),
        'ŧ' => Some("t"),
        'Ŋ' => Some("N"),
        'ŋ' => Some("n"),
        'ı' => Some("i"),
        'ĸ' => Some("k"),
        'ſ' => Some("s"),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use crate::{AsciiFoldingFilter, RawTokenizer, TextAnalyzer, Token};

    fn collect_tokens(text: &str) -> Vec<Token> {
        let mut analyzer = TextAnalyzer::builder(RawTokenizer::default())
            .filter(AsciiFoldingFilter)
            .build();
        let mut stream = analyzer.token_stream(text);
        let mut tokens = Vec::new();
        stream.process(&mut |token| tokens.push(token.clone()));
        tokens
    }

    #[test]
    fn test_ascii_folding_accents() {
        let tokens = collect_tokens("café");
        assert_eq!(tokens[0].text, "cafe");
    }

    #[test]
    fn test_ascii_folding_sharp_s() {
        let tokens = collect_tokens("straße");
        assert_eq!(tokens[0].text, "strasse");
    }

    #[test]
    fn test_ascii_folding_cjk_unchanged() {
        let tokens = collect_tokens("こんにちは世界");
        assert_eq!(tokens[0].text, "こんにちは世界");
    }
}
