// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

pub mod stream;

pub trait LanceIteratorExtension {
    fn exact_size(self, size: usize) -> ExactSize<Self>
    where
        Self: Sized;
}

impl<I: Iterator> LanceIteratorExtension for I {
    fn exact_size(self, size: usize) -> ExactSize<Self>
    where
        Self: Sized,
    {
        ExactSize { inner: self, size }
    }
}

/// A iterator that is tagged with a known size. This is useful when we are
/// able to pre-compute the size of the iterator but the iterator implementation
/// isn't able to itself. A common example is when using `flatten()`.
///
/// This is inspired by discussion in <https://github.com/rust-lang/rust/issues/68995>
pub struct ExactSize<I> {
    inner: I,
    size: usize,
}

impl<I: Iterator> Iterator for ExactSize<I> {
    type Item = I::Item;

    fn next(&mut self) -> Option<Self::Item> {
        match self.inner.next() {
            None => None,
            Some(x) => {
                self.size -= 1;
                Some(x)
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.size, Some(self.size))
    }
}

impl<T: Iterator> ExactSizeIterator for ExactSize<T> {}
