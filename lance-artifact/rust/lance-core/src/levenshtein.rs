// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

/// Calculate the Levenshtein distance between two strings.
///
/// The Levenshtein distance is a measure of the number of single-character edits
/// (insertions, deletions, or substitutions) required to change one word into the other.
///
/// # Examples
///
/// ```
/// use lance_core::levenshtein::levenshtein_distance;
///
/// assert_eq!(levenshtein_distance("kitten", "sitting"), 3);
/// assert_eq!(levenshtein_distance("hello", "hello"), 0);
/// ```
pub fn levenshtein_distance(s1: &str, s2: &str) -> usize {
    let s1_chars: Vec<char> = s1.chars().collect();
    let s2_chars: Vec<char> = s2.chars().collect();
    let m = s1_chars.len();
    let n = s2_chars.len();

    if m == 0 {
        return n;
    }
    if n == 0 {
        return m;
    }

    // Use two rows instead of full matrix for space efficiency
    let mut prev_row: Vec<usize> = (0..=n).collect();
    let mut curr_row: Vec<usize> = vec![0; n + 1];

    for (i, s1_char) in s1_chars.iter().enumerate() {
        curr_row[0] = i + 1;
        for (j, s2_char) in s2_chars.iter().enumerate() {
            let cost = if s1_char == s2_char { 0 } else { 1 };
            curr_row[j + 1] = (prev_row[j + 1] + 1)
                .min(curr_row[j] + 1)
                .min(prev_row[j] + cost);
        }
        std::mem::swap(&mut prev_row, &mut curr_row);
    }

    prev_row[n]
}

/// Find the best suggestion from a list of options based on Levenshtein distance.
///
/// Returns `Some(suggestion)` if there's an option where the Levenshtein distance
/// is at most 1/3 of the length of the input string (integer division).
/// Otherwise returns `None`.
///
/// # Examples
///
/// ```
/// use lance_core::levenshtein::find_best_suggestion;
///
/// let options = vec!["vector", "id", "name"];
/// assert_eq!(find_best_suggestion("vacter", &options), Some("vector"));
/// assert_eq!(find_best_suggestion("hello", &options), None);
/// ```
pub fn find_best_suggestion<'a, 'b>(
    input: &'a str,
    options: &'b [impl AsRef<str>],
) -> Option<&'b str> {
    let input_len = input.chars().count();
    if input_len == 0 {
        return None;
    }

    let threshold = input_len / 3;
    let mut best_option: Option<(&'b str, usize)> = None;
    for option in options {
        let distance = levenshtein_distance(input, option.as_ref());
        if distance <= threshold {
            match &best_option {
                None => best_option = Some((option.as_ref(), distance)),
                Some((_, best_distance)) => {
                    if distance < *best_distance {
                        best_option = Some((option.as_ref(), distance));
                    }
                }
            }
        }
    }

    best_option.map(|(option, _)| option)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_levenshtein_distance() {
        assert_eq!(levenshtein_distance("", ""), 0);
        assert_eq!(levenshtein_distance("a", ""), 1);
        assert_eq!(levenshtein_distance("", "a"), 1);
        assert_eq!(levenshtein_distance("abc", "abc"), 0);
        assert_eq!(levenshtein_distance("abc", ""), 3);
        assert_eq!(levenshtein_distance("", "abc"), 3);
        assert_eq!(levenshtein_distance("kitten", "sitting"), 3);
        assert_eq!(levenshtein_distance("saturday", "sunday"), 3);
        assert_eq!(levenshtein_distance("vector", "vectr"), 1);
        assert_eq!(levenshtein_distance("vector", "vextor"), 1);
        assert_eq!(levenshtein_distance("vector", "vvector"), 1);
        assert_eq!(levenshtein_distance("abc", "xyz"), 3);
    }

    #[test]
    fn test_find_best_suggestion() {
        let options = vec!["vector", "id", "name", "column", "table"];

        assert_eq!(find_best_suggestion("vacter", &options), Some("vector"));
        assert_eq!(find_best_suggestion("vectr", &options), Some("vector"));
        assert_eq!(find_best_suggestion("tble", &options), Some("table"));

        // Should return None if no good match
        assert_eq!(find_best_suggestion("hello", &options), None);
        assert_eq!(find_best_suggestion("xyz", &options), None);

        // Should return None if input is too short
        assert_eq!(find_best_suggestion("v", &options), None);
        assert_eq!(find_best_suggestion("", &options), None);

        // Picks closest when multiple are close
        assert_eq!(
            find_best_suggestion("vecor", &["vector", "vendor"]),
            Some("vector")
        );
    }
}
