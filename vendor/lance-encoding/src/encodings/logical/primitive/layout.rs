// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use lance_core::Result;

use crate::repdef::MiniBlockRepDefBudget;

/// Runs automatic sparse planning only after the dense mini-block budget makes it useful.
pub(super) fn select_automatic_sparse<T>(
    requested_encoding: Option<&str>,
    dense_budget: &MiniBlockRepDefBudget,
    candidate: impl FnOnce() -> Result<Option<T>>,
) -> Result<Option<T>> {
    if requested_encoding.is_some()
        || !matches!(
            dense_budget,
            MiniBlockRepDefBudget::RequiresPageSplit(_)
                | MiniBlockRepDefBudget::SingleRowOverBudget(_)
        )
    {
        return Ok(None);
    }
    candidate()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::constants::{
        STRUCTURAL_ENCODING_FULLZIP, STRUCTURAL_ENCODING_MINIBLOCK, STRUCTURAL_ENCODING_SPARSE,
    };

    #[test]
    fn within_budget_does_not_construct_sparse_candidate() {
        let selected =
            select_automatic_sparse::<()>(None, &MiniBlockRepDefBudget::WithinBudget, || {
                panic!("within-budget pages must not construct sparse candidates")
            })
            .unwrap();
        assert!(selected.is_none());
    }

    #[test]
    fn over_budget_selects_only_eligible_candidates() {
        let split = MiniBlockRepDefBudget::RequiresPageSplit(Vec::new());
        let selected = select_automatic_sparse(None, &split, || Ok(Some(42))).unwrap();
        assert_eq!(selected, Some(42));

        let ineligible = select_automatic_sparse::<()>(None, &split, || Ok(None)).unwrap();
        assert!(ineligible.is_none());

        let unsplittable = MiniBlockRepDefBudget::SingleRowOverBudget(70_000);
        let selected = select_automatic_sparse(None, &unsplittable, || Ok(Some(7))).unwrap();
        assert_eq!(selected, Some(7));
    }

    #[test]
    fn explicit_modes_and_lance_2_2_do_not_auto_select() {
        let split = MiniBlockRepDefBudget::RequiresPageSplit(Vec::new());
        for requested in [
            STRUCTURAL_ENCODING_MINIBLOCK,
            STRUCTURAL_ENCODING_FULLZIP,
            STRUCTURAL_ENCODING_SPARSE,
        ] {
            let selected = select_automatic_sparse::<()>(Some(requested), &split, || {
                panic!("explicit modes must not invoke automatic sparse planning")
            })
            .unwrap();
            assert!(selected.is_none());
        }
    }
}
