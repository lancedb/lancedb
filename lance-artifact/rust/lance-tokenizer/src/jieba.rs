// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use crate::{TextAnalyzer, TextAnalyzerBuilder, Token, TokenStream, Tokenizer};

#[derive(Clone)]
pub struct JiebaTokenizer {
    jieba: jieba_rs::Jieba,
}

impl JiebaTokenizer {
    pub fn new(jieba: jieba_rs::Jieba) -> Self {
        Self { jieba }
    }

    pub fn analyzer(self) -> TextAnalyzer {
        TextAnalyzer::builder(self).build()
    }

    pub fn analyzer_builder(self) -> TextAnalyzerBuilder {
        TextAnalyzer::builder(self).dynamic()
    }
}

pub struct JiebaTokenStream {
    tokens: Vec<Token>,
    index: usize,
}

impl TokenStream for JiebaTokenStream {
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

impl Tokenizer for JiebaTokenizer {
    type TokenStream<'a> = JiebaTokenStream;

    fn token_stream<'a>(&'a mut self, text: &'a str) -> Self::TokenStream<'a> {
        let mut indices = text.char_indices().collect::<Vec<_>>();
        indices.push((text.len(), '\0'));
        let orig_tokens = self
            .jieba
            .tokenize(text, jieba_rs::TokenizeMode::Search, true);
        let tokens = orig_tokens
            .into_iter()
            .map(|token| Token {
                offset_from: indices[token.start].0,
                offset_to: indices[token.end].0,
                position: token.start,
                text: text[indices[token.start].0..indices[token.end].0].to_owned(),
                position_length: token.end - token.start,
            })
            .collect();
        JiebaTokenStream { tokens, index: 0 }
    }
}
