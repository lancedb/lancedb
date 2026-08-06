// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::collections::LinkedList;

use crate::deepsize::DeepSizeOf;

/// A linked list that grows exponentially. It is used to store a large number of
/// elements in a memory-efficient way. The list grows by doubling the capacity of
/// the last element when it's full, the capacity can be limited by the `limit`
/// parameter. The default value is 0, which means no limit.
#[derive(Debug, Clone, Default)]
pub struct ExpLinkedList<T> {
    inner: LinkedList<Vec<T>>,
    len: usize,
    // The maximum capacity of single node in the list.
    // If the limit is 0, there is no limit.
    // We use u16 to save memory because ExpLinkedList should not
    // be used if the limit is that large.
    limit: u16,
}

impl<T> ExpLinkedList<T> {
    /// Creates a new empty `ExpLinkedList`.
    pub fn new() -> Self {
        Self {
            inner: LinkedList::new(),
            len: 0,
            limit: 0,
        }
    }

    pub fn with_capacity(capacity: usize) -> Self {
        let mut inner = LinkedList::new();
        inner.push_back(Vec::with_capacity(capacity));
        Self {
            inner,
            len: 0,
            limit: 0,
        }
    }

    /// Creates a new `ExpLinkedList` with a specified capacity limit.
    /// The limit is the maximum capacity of a single node in the list.
    /// If the limit is 0, there is no limit.
    pub fn with_capacity_limit(mut self, limit: u16) -> Self {
        self.limit = limit;
        self
    }

    /// Pushes a new element into the list. If the last element in the list
    /// reaches its capacity, a new node is created with double capacity.
    pub fn push(&mut self, v: T) {
        match self.inner.back() {
            Some(last) => {
                if last.len() == last.capacity() {
                    let new_cap = if self.limit > 0 && last.capacity() * 2 >= self.limit as usize {
                        self.limit as usize
                    } else {
                        last.capacity() * 2
                    };
                    self.inner.push_back(Vec::with_capacity(new_cap));
                }
            }
            None => {
                self.inner.push_back(Vec::with_capacity(1));
            }
        }
        self.do_push(v);
    }

    fn do_push(&mut self, v: T) {
        self.inner.back_mut().unwrap().push(v);
        self.len += 1;
    }

    /// Removes the last element from the list.
    pub fn pop(&mut self) -> Option<T> {
        match self.inner.back_mut() {
            Some(last) => {
                if last.is_empty() {
                    self.inner.pop_back();
                    self.pop()
                } else {
                    self.len -= 1;
                    last.pop()
                }
            }
            None => None,
        }
    }

    /// Clears the list, removing all elements.
    /// This will free the memory used by the list.
    pub fn clear(&mut self) {
        self.inner.clear();
        self.len = 0;
    }

    /// Returns the number of elements in the list.
    pub fn len(&self) -> usize {
        self.len
    }

    /// Returns whether the list is empty.
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Returns the size of list, including the size of the elements and the
    /// size of the list itself, and the unused space.
    /// The element size is calculated using `std::mem::size_of::<T>()`,
    /// so it is not accurate for all types.
    /// For example, for `String`, it will return the size of the pointer,
    /// not the size of the string itself. For that you need to use `DeepSizeOf`.
    pub fn size(&self) -> usize {
        let unused_space = match self.inner.back() {
            Some(last) => last.capacity() - last.len(),
            None => 0,
        };
        (self.len() + unused_space) * std::mem::size_of::<T>()
            + std::mem::size_of::<Self>()
            + self.inner.len() * std::mem::size_of::<Vec<T>>()
    }

    /// Returns an iterator over the elements in the list.
    pub fn iter(&self) -> ExpLinkedListIter<'_, T> {
        ExpLinkedListIter::new(self)
    }

    pub fn block_iter(&self) -> impl Iterator<Item = &[T]> {
        self.inner.iter().map(|v| v.as_slice())
    }
}

impl<T: DeepSizeOf> DeepSizeOf for ExpLinkedList<T> {
    fn deep_size_of_children(&self, context: &mut crate::deepsize::Context) -> usize {
        self.inner
            .iter()
            .map(|v| v.deep_size_of_children(context))
            .sum()
    }
}

impl<T> FromIterator<T> for ExpLinkedList<T> {
    fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self {
        let iter = iter.into_iter();
        let size_hint = iter.size_hint().0;
        let cap = if size_hint > 0 { size_hint } else { 1 };
        let mut list = Self::with_capacity(cap);
        for item in iter {
            list.push(item);
        }
        list
    }
}

impl<T> PartialEq for ExpLinkedList<T>
where
    T: PartialEq,
{
    fn eq(&self, other: &Self) -> bool {
        self.iter().zip(other.iter()).all(|(a, b)| a == b)
    }
}

impl<T> Eq for ExpLinkedList<T> where T: Eq {}

pub struct ExpLinkedListIter<'a, T> {
    inner: std::collections::linked_list::Iter<'a, Vec<T>>,
    inner_iter: Option<std::slice::Iter<'a, T>>,
    len: usize,
}

impl<'a, T> ExpLinkedListIter<'a, T> {
    pub fn new(inner: &'a ExpLinkedList<T>) -> Self {
        Self {
            inner: inner.inner.iter(),
            inner_iter: None,
            len: inner.len(),
        }
    }
}

impl<'a, T> Iterator for ExpLinkedListIter<'a, T> {
    type Item = &'a T;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(inner_iter) = &mut self.inner_iter
            && let Some(v) = inner_iter.next()
        {
            return Some(v);
        }
        if let Some(inner) = self.inner.next() {
            self.inner_iter = Some(inner.iter());
            return self.next();
        }
        None
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.len, Some(self.len))
    }
}

pub struct ExpLinkedListIntoIter<T> {
    inner: std::collections::linked_list::IntoIter<Vec<T>>,
    inner_iter: Option<std::vec::IntoIter<T>>,
    len: usize,
}

impl<T> ExpLinkedListIntoIter<T> {
    pub fn new(list: ExpLinkedList<T>) -> Self {
        let len = list.len();
        Self {
            inner: list.inner.into_iter(),
            inner_iter: None,
            len,
        }
    }
}

impl<T> Iterator for ExpLinkedListIntoIter<T> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(inner_iter) = &mut self.inner_iter
            && let Some(v) = inner_iter.next()
        {
            return Some(v);
        }
        if let Some(inner) = self.inner.next() {
            self.inner_iter = Some(inner.into_iter());
            return self.next();
        }
        None
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.len, Some(self.len))
    }
}

impl<T> IntoIterator for ExpLinkedList<T> {
    type Item = T;
    type IntoIter = ExpLinkedListIntoIter<T>;

    fn into_iter(self) -> Self::IntoIter {
        ExpLinkedListIntoIter::new(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_exp_linked_list(list: &mut ExpLinkedList<usize>) {
        assert_eq!(list.len(), 100);
        assert!(!list.is_empty());

        // removes the last 50 elements
        for i in 0..50 {
            assert_eq!(list.pop(), Some(99 - i));
        }
        assert_eq!(list.len(), 50);
        assert!(!list.is_empty());

        // iterate over the list
        for (i, v) in list.iter().enumerate() {
            assert_eq!(*v, i);
        }

        // clear the list
        list.clear();
        assert_eq!(list.len(), 0);
        assert!(list.is_empty());
        assert_eq!(list.pop(), None);
    }

    #[test]
    fn test_exp_linked_list_basic() {
        let mut list = ExpLinkedList::new();
        for i in 0..100 {
            list.push(i);
            assert_eq!(list.len(), i + 1);
        }
        test_exp_linked_list(&mut list);
    }

    #[test]
    fn test_exp_linked_list_from() {
        let mut list = (0..100).collect();
        test_exp_linked_list(&mut list);
    }

    #[test]
    fn test_exp_linked_list_with_capacity_limit() {
        let mut list = ExpLinkedList::new().with_capacity_limit(10);
        for i in 0..100 {
            list.push(i);
            assert_eq!(list.len(), i + 1);
        }
        assert_eq!(list.inner.back().unwrap().capacity(), 10);
        test_exp_linked_list(&mut list);
    }
}
