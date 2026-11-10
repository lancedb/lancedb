// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Lazy, resumable listings of database resources.

use std::collections::HashSet;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures::future::BoxFuture;
use futures::{Future, FutureExt, Stream};

use crate::{Error, Result};

/// Pagination controls for a resource listing.
#[derive(Clone, Debug, Default)]
#[non_exhaustive]
pub struct ListingOptions {
    /// Starting continuation token; `None` starts at the beginning.
    pub page_token: Option<String>,
    /// Maximum results per request, not a limit on the entire listing.
    /// `None` uses the backend default. Must be between 1 and `i32::MAX`.
    pub page_limit: Option<u32>,
}

impl ListingOptions {
    /// Resume at a saved continuation token.
    pub fn page_token(mut self, token: impl Into<String>) -> Self {
        self.page_token = Some(token.into());
        self
    }

    /// Set the maximum number of results requested per page.
    pub fn page_limit(mut self, limit: u32) -> Self {
        self.page_limit = Some(limit);
        self
    }
}

/// One backend page of a resource listing.
#[derive(Debug)]
pub struct ListingPage<T> {
    /// Items returned on this page, possibly empty even when more pages exist.
    pub items: Vec<T>,
    /// Token for the next page; `None` or an empty string ends the listing.
    pub page_token: Option<String>,
}

type FetchPage<T> =
    Box<dyn Fn(ListingOptions) -> BoxFuture<'static, Result<ListingPage<T>>> + Send + Sync>;

/// A lazy stream of resources with inspectable pagination state.
///
/// No requests are made until the stream is polled or [`Self::next_page`] is called.
/// Item iteration skips empty pages with continuation tokens; `next_page` returns
/// them. Errors terminate the listing and retain the failed request's
/// token, allowing a new stream to retry it. Repeated tokens are rejected.
///
/// A token advances when its page is fetched. Drain [`Self::num_page_results`]
/// before saving [`Self::page_token`] to avoid skipping cached items on resumption.
/// After the final page is fetched the token is `None`, even with cached items.
/// Use [`futures::TryStreamExt::try_collect`] to collect a listing into a vector.
pub struct Listing<T> {
    fetch: FetchPage<T>,
    items: std::vec::IntoIter<T>,
    options: ListingOptions,
    done: bool,
    seen_tokens: HashSet<String>,
    pending: Option<BoxFuture<'static, Result<ListingPage<T>>>>,
}

impl<T> Listing<T> {
    pub(crate) fn new<F, Fut>(options: ListingOptions, fetch: F) -> Self
    where
        F: Fn(ListingOptions) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<ListingPage<T>>> + Send + 'static,
    {
        let seen_tokens = options
            .page_token
            .iter()
            .filter(|t| !t.is_empty())
            .cloned()
            .collect();
        Self {
            fetch: Box::new(move |options| fetch(options).boxed()),
            items: Vec::new().into_iter(),
            options,
            done: false,
            seen_tokens,
            pending: None,
        }
    }

    /// Number of items available without another backend request.
    pub fn num_page_results(&self) -> usize {
        self.items.len()
    }

    /// Token for the next request. Initially the supplied token; `None` after
    /// fetching the final page. Drain cached items before saving this token.
    pub fn page_token(&self) -> Option<&str> {
        self.options.page_token.as_deref()
    }

    /// Return exactly one page without consuming a following page.
    ///
    /// If item iteration has started, this returns the remaining items in its
    /// cached page. An empty page with a continuation token is returned as is.
    pub async fn next_page(&mut self) -> Result<Option<ListingPage<T>>> {
        if self.items.len() == 0 {
            if self.done {
                return Ok(None);
            }
            futures::future::poll_fn(|cx| self.poll_fetch_page(cx)).await?;
        }
        let items = std::mem::replace(&mut self.items, Vec::new().into_iter()).collect();
        Ok(Some(ListingPage {
            items,
            page_token: self.options.page_token.clone(),
        }))
    }

    fn poll_fetch_page(&mut self, cx: &mut Context<'_>) -> Poll<Result<()>> {
        if self
            .options
            .page_limit
            .is_some_and(|limit| limit == 0 || limit > i32::MAX as u32)
        {
            self.done = true;
            return Poll::Ready(Err(Error::InvalidInput {
                message: "Listing page limit must be between 1 and 2147483647".into(),
            }));
        }
        let pending = self
            .pending
            .get_or_insert_with(|| (self.fetch)(self.options.clone()));
        let response = futures::ready!(pending.as_mut().poll(cx));
        self.pending = None;
        match response {
            Ok(response) => {
                let token = response.page_token.filter(|token| !token.is_empty());
                if token
                    .as_ref()
                    .is_some_and(|token| !self.seen_tokens.insert(token.clone()))
                {
                    self.done = true;
                    return Poll::Ready(Err(Error::Runtime {
                        message: "Listing response repeated a page_token".into(),
                    }));
                }
                self.done = token.is_none();
                self.options.page_token = token;
                self.items = response.items.into_iter();
                Poll::Ready(Ok(()))
            }
            Err(error) => {
                self.done = true;
                Poll::Ready(Err(error))
            }
        }
    }
}

impl<T: Unpin> Stream for Listing<T> {
    type Item = Result<T>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        loop {
            if let Some(item) = this.items.next() {
                return Poll::Ready(Some(Ok(item)));
            }
            if this.done {
                return Poll::Ready(None);
            }
            if let Err(error) = futures::ready!(this.poll_fetch_page(cx)) {
                return Poll::Ready(Some(Err(error)));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::{StreamExt, TryStreamExt};
    use std::sync::{Arc, Mutex};

    #[tokio::test]
    async fn lazy_pages_empty_pages_and_resume() {
        let requests = Arc::new(Mutex::new(Vec::new()));
        let fetch = {
            let requests = requests.clone();
            move |options: ListingOptions| {
                requests.lock().unwrap().push(options.clone());
                async move {
                    let (items, token) = match options.page_token.as_deref() {
                        Some("start") => (vec![1, 2], Some("empty")),
                        Some("empty") => (vec![], Some("last")),
                        Some("last") => (vec![3, 4], Some("")),
                        _ => panic!("unexpected request"),
                    };
                    Ok(ListingPage {
                        items,
                        page_token: token.map(str::to_owned),
                    })
                }
            }
        };
        let mut listing = Listing::new(
            ListingOptions::default().page_token("start").page_limit(2),
            fetch.clone(),
        );
        assert!(requests.lock().unwrap().is_empty());
        assert_eq!(listing.page_token(), Some("start"));
        assert_eq!(listing.num_page_results(), 0);
        assert_eq!(listing.try_next().await.unwrap(), Some(1));
        assert_eq!(listing.num_page_results(), 1);
        assert_eq!(listing.page_token(), Some("empty"));
        assert_eq!(listing.try_next().await.unwrap(), Some(2));
        assert_eq!(requests.lock().unwrap().len(), 1);
        let mut resumed = Listing::new(
            ListingOptions::default()
                .page_token(listing.page_token().unwrap())
                .page_limit(2),
            fetch,
        );
        assert_eq!(resumed.try_next().await.unwrap(), Some(3));
        assert_eq!(resumed.page_token(), None);
        assert_eq!(resumed.num_page_results(), 1);
        assert_eq!(resumed.try_next().await.unwrap(), Some(4));
        assert_eq!(resumed.try_next().await.unwrap(), None);
        assert_eq!(resumed.try_next().await.unwrap(), None);
        let requests = requests.lock().unwrap();
        assert_eq!(requests.len(), 3);
        assert!(requests.iter().all(|request| request.page_limit == Some(2)));
    }

    #[tokio::test]
    async fn failure_retains_token_and_terminates() {
        let mut listing =
            Listing::<String>::new(ListingOptions::default().page_token("retry"), |_| async {
                Err(Error::Runtime {
                    message: "failed".into(),
                })
            });
        assert!(listing.next().await.unwrap().is_err());
        assert_eq!(listing.page_token(), Some("retry"));
        assert_eq!(listing.num_page_results(), 0);
        assert!(listing.next().await.is_none());
    }

    #[tokio::test]
    async fn next_page_preserves_empty_pages_and_remaining_items() {
        let requests = Arc::new(Mutex::new(0));
        let count = requests.clone();
        let mut listing = Listing::new(ListingOptions::default(), move |options| {
            *count.lock().unwrap() += 1;
            async move {
                let (items, page_token) = match options.page_token.as_deref() {
                    None => (vec![], Some("second".to_string())),
                    Some("second") => (vec!["a", "b"], Some("last".to_string())),
                    Some("last") => (vec!["c"], None),
                    _ => unreachable!(),
                };
                Ok(ListingPage { items, page_token })
            }
        });
        assert_eq!(*requests.lock().unwrap(), 0);
        assert!(listing.next_page().await.unwrap().unwrap().items.is_empty());
        assert_eq!(*requests.lock().unwrap(), 1);
        assert_eq!(listing.try_next().await.unwrap(), Some("a"));
        assert_eq!(*requests.lock().unwrap(), 2);
        let remaining = listing.next_page().await.unwrap().unwrap();
        assert_eq!(remaining.items, vec!["b"]);
        assert_eq!(remaining.page_token.as_deref(), Some("last"));
        assert_eq!(*requests.lock().unwrap(), 2);
        assert_eq!(listing.next_page().await.unwrap().unwrap().items, vec!["c"]);
        assert!(listing.next_page().await.unwrap().is_none());
        assert_eq!(*requests.lock().unwrap(), 3);
    }

    #[tokio::test]
    async fn invalid_limits_do_not_fetch() {
        for limit in [0, u32::MAX] {
            let mut listing =
                Listing::<String>::new(ListingOptions::default().page_limit(limit), |_| async {
                    panic!("invalid limits must not reach the backend")
                });
            assert!(matches!(
                listing.next().await,
                Some(Err(Error::InvalidInput { .. }))
            ));
            assert!(listing.next().await.is_none());
        }
    }

    #[tokio::test]
    async fn jobs_can_exceed_the_old_page_cap() {
        let listing = Listing::new(ListingOptions::default(), |options| async move {
            let n: usize = options
                .page_token
                .as_deref()
                .unwrap_or("0")
                .parse()
                .unwrap();
            Ok(ListingPage {
                items: vec![n],
                page_token: (n < 110).then(|| (n + 1).to_string()),
            })
        });
        assert_eq!(
            listing.try_collect::<Vec<_>>().await.unwrap(),
            (0..=110).collect::<Vec<_>>()
        );
    }
}
