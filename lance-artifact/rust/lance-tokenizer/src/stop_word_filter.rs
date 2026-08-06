// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors
// SPDX-License-Identifier: MIT
// Adapted from Tantivy v0.24.2 stop-word filter.
// Copyright (c) 2017-present Tantivy contributors.

#[path = "stop_word_filter/stopwords.rs"]
mod stopwords;

use std::collections::HashSet;
use std::sync::Arc;

use crate::{Language, Token, TokenFilter, TokenStream, Tokenizer};

fn all_stop_words() -> impl Iterator<Item = &'static str> {
    [
        stop_words::get("ar"),
        stopwords::DANISH,
        stopwords::DUTCH,
        // Use the fuller `stop-words` crate English list (~198 words) rather
        // than the local Tantivy-style list (~33 words), which omits extremely
        // common pronouns/function words (you, my, your, we, she, what, ...).
        // Those omissions let the highest-frequency English tokens through the
        // ICU stop-word path and build pathologically large posting lists.
        stop_words::get("en"),
        stopwords::FINNISH,
        stopwords::FRENCH,
        stopwords::GERMAN,
        stop_words::get("el"),
        stopwords::HUNGARIAN,
        stopwords::ITALIAN,
        stopwords::NORWEGIAN,
        stopwords::PORTUGUESE,
        stop_words::get("ro"),
        stopwords::RUSSIAN,
        stopwords::SPANISH,
        stopwords::SWEDISH,
        stop_words::get("ta"),
        stop_words::get("tr"),
        stop_words::get("zh"),
        stop_words::get("ja"),
        stop_words::get("ko"),
    ]
    .into_iter()
    .flat_map(|words| words.iter().copied())
}

#[derive(Clone)]
pub struct StopWordFilter {
    words: Arc<HashSet<String>>,
}

impl StopWordFilter {
    pub fn new(language: Language) -> Option<Self> {
        let words = match language {
            Language::Arabic => stop_words::get("ar"),
            Language::Danish => stopwords::DANISH,
            Language::Dutch => stopwords::DUTCH,
            // Use the fuller `stop-words` crate English list (~198 words); the
            // local Tantivy-style list (~33 words) omits common pronouns/function
            // words (you, my, your, we, ...) that would otherwise leak through
            // stop-word removal and build pathologically large posting lists.
            Language::English => stop_words::get("en"),
            Language::Finnish => stopwords::FINNISH,
            Language::French => stopwords::FRENCH,
            Language::German => stopwords::GERMAN,
            Language::Greek => stop_words::get("el"),
            Language::Hungarian => stopwords::HUNGARIAN,
            Language::Italian => stopwords::ITALIAN,
            Language::Norwegian => stopwords::NORWEGIAN,
            Language::Portuguese => stopwords::PORTUGUESE,
            Language::Romanian => stop_words::get("ro"),
            Language::Russian => stopwords::RUSSIAN,
            Language::Spanish => stopwords::SPANISH,
            Language::Swedish => stopwords::SWEDISH,
            Language::Tamil => stop_words::get("ta"),
            Language::Turkish => stop_words::get("tr"),
        };
        Some(Self::remove(words.iter().map(|word| (*word).to_owned())))
    }

    pub fn all() -> Self {
        Self::remove(all_stop_words().map(str::to_owned))
    }

    pub fn remove<W: IntoIterator<Item = String>>(words: W) -> Self {
        Self {
            words: Arc::new(words.into_iter().collect()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::all_stop_words;
    use crate::StopWordFilter;
    use std::collections::HashSet;

    #[test]
    fn test_external_stop_word_lists_are_available() {
        let words = all_stop_words().collect::<HashSet<_>>();
        for word in ["إلى", "και", "acesta", "அவர்", "ama", "的", "ある", "그리고"]
        {
            assert!(
                words.contains(word),
                "built-in stop words should contain {word}"
            );
        }
    }

    #[test]
    fn test_language_stop_word_lists_are_available() {
        for (language, word) in [
            (crate::Language::Arabic, "إلى"),
            (crate::Language::Greek, "και"),
            (crate::Language::Romanian, "acesta"),
            (crate::Language::Tamil, "அவர்"),
            (crate::Language::Turkish, "ama"),
        ] {
            let filter = StopWordFilter::new(language).unwrap();
            assert!(
                filter.words.contains(word),
                "{language:?} should contain {word}"
            );
        }
    }
}

impl TokenFilter for StopWordFilter {
    type Tokenizer<T: Tokenizer> = StopWordFilterWrapper<T>;

    fn transform<T: Tokenizer>(self, tokenizer: T) -> Self::Tokenizer<T> {
        StopWordFilterWrapper {
            words: self.words,
            inner: tokenizer,
        }
    }
}

#[derive(Clone)]
pub struct StopWordFilterWrapper<T> {
    words: Arc<HashSet<String>>,
    inner: T,
}

impl<T: Tokenizer> Tokenizer for StopWordFilterWrapper<T> {
    type TokenStream<'a> = StopWordFilterStream<T::TokenStream<'a>>;

    fn token_stream<'a>(&'a mut self, text: &'a str) -> Self::TokenStream<'a> {
        StopWordFilterStream {
            words: self.words.clone(),
            tail: self.inner.token_stream(text),
        }
    }
}

pub struct StopWordFilterStream<T> {
    words: Arc<HashSet<String>>,
    tail: T,
}

impl<T> StopWordFilterStream<T> {
    fn predicate(&self, token: &Token) -> bool {
        !self.words.contains(&token.text)
    }
}

impl<T: TokenStream> TokenStream for StopWordFilterStream<T> {
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
