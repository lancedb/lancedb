// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use std::sync::{Arc, Mutex};

use crate::{error::NapiErrorExt, table::Table};
use lancedb::dataloader::{
    permutation::{PermutationBuilder as LancePermutationBuilder, ShuffleStrategy},
    split::{SplitSizes, SplitStrategy},
};
use napi_derive::napi;

#[napi(object)]
pub struct SplitRandomOptions {
    pub ratios: Option<Vec<f64>>,
    pub counts: Option<Vec<i64>>,
    pub fixed: Option<i64>,
    pub seed: Option<i64>,
}

#[napi(object)]
pub struct SplitHashOptions {
    pub columns: Vec<String>,
    pub split_weights: Vec<i64>,
    pub discard_weight: Option<i64>,
}

#[napi(object)]
pub struct SplitSequentialOptions {
    pub ratios: Option<Vec<f64>>,
    pub counts: Option<Vec<i64>>,
    pub fixed: Option<i64>,
}

#[napi(object)]
pub struct ShuffleOptions {
    pub seed: Option<i64>,
    pub clump_size: Option<i64>,
}

/// Create a permutation builder for the given table
#[napi]
pub fn permutation_builder(table: &crate::table::Table, dest_table_name: String) -> napi::Result<PermutationBuilder> {
    use lancedb::dataloader::permutation::PermutationBuilder as LancePermutationBuilder;

    let inner_table = table.inner_ref()?.clone();
    let inner_builder = LancePermutationBuilder::new(inner_table);

    Ok(PermutationBuilder::new(inner_builder, dest_table_name))
}


pub struct PermutationBuilderState {
    pub builder: Option<LancePermutationBuilder>,
    pub dest_table_name: String,
}

#[napi]
pub struct PermutationBuilder {
    state: Arc<Mutex<PermutationBuilderState>>,
}

impl PermutationBuilder {
    pub fn new(builder: LancePermutationBuilder, dest_table_name: String) -> Self {
        Self {
            state: Arc::new(Mutex::new(PermutationBuilderState {
                builder: Some(builder),
                dest_table_name,
            })),
        }
    }
}

impl PermutationBuilder {
    fn modify(
        &self,
        func: impl FnOnce(LancePermutationBuilder) -> LancePermutationBuilder,
    ) -> napi::Result<Self> {
        let mut state = self.state.lock().unwrap();
        let builder = state
            .builder
            .take()
            .ok_or_else(|| napi::Error::from_reason("Builder already consumed"))?;
        state.builder = Some(func(builder));
        Ok(Self {
            state: self.state.clone(),
        })
    }
}

#[napi]
impl PermutationBuilder {
    /// Configure random splits
    #[napi]
    pub fn split_random(&self, options: SplitRandomOptions) -> napi::Result<PermutationBuilder> {
        // Check that exactly one split type is provided
        let split_args_count = [options.ratios.is_some(), options.counts.is_some(), options.fixed.is_some()]
            .iter()
            .filter(|&&x| x)
            .count();

        if split_args_count != 1 {
            return Err(napi::Error::from_reason(
                "Exactly one of 'ratios', 'counts', or 'fixed' must be provided",
            ));
        }

        let sizes = if let Some(ratios) = options.ratios {
            SplitSizes::Percentages(ratios)
        } else if let Some(counts) = options.counts {
            SplitSizes::Counts(counts.into_iter().map(|c| c as u64).collect())
        } else if let Some(fixed) = options.fixed {
            SplitSizes::Fixed(fixed as u64)
        } else {
            unreachable!("One of the split arguments must be provided");
        };

        let seed = options.seed.map(|s| s as u64);

        self.modify(|builder| builder.with_split_strategy(SplitStrategy::Random { seed, sizes }))
    }

    /// Configure hash-based splits
    #[napi]
    pub fn split_hash(&self, options: SplitHashOptions) -> napi::Result<PermutationBuilder> {
        let split_weights = options.split_weights.into_iter().map(|w| w as u64).collect();
        let discard_weight = options.discard_weight.unwrap_or(0) as u64;

        self.modify(|builder| {
            builder.with_split_strategy(SplitStrategy::Hash {
                columns: options.columns,
                split_weights,
                discard_weight,
            })
        })
    }

    /// Configure sequential splits
    #[napi]
    pub fn split_sequential(&self, options: SplitSequentialOptions) -> napi::Result<PermutationBuilder> {
        // Check that exactly one split type is provided
        let split_args_count = [options.ratios.is_some(), options.counts.is_some(), options.fixed.is_some()]
            .iter()
            .filter(|&&x| x)
            .count();

        if split_args_count != 1 {
            return Err(napi::Error::from_reason(
                "Exactly one of 'ratios', 'counts', or 'fixed' must be provided",
            ));
        }

        let sizes = if let Some(ratios) = options.ratios {
            SplitSizes::Percentages(ratios)
        } else if let Some(counts) = options.counts {
            SplitSizes::Counts(counts.into_iter().map(|c| c as u64).collect())
        } else if let Some(fixed) = options.fixed {
            SplitSizes::Fixed(fixed as u64)
        } else {
            unreachable!("One of the split arguments must be provided");
        };

        self.modify(|builder| builder.with_split_strategy(SplitStrategy::Sequential { sizes }))
    }

    /// Configure calculated splits
    #[napi]
    pub fn split_calculated(&self, calculation: String) -> napi::Result<PermutationBuilder> {
        self.modify(|builder| builder.with_split_strategy(SplitStrategy::Calculated { calculation }))
    }

    /// Configure shuffling
    #[napi]
    pub fn shuffle(&self, options: ShuffleOptions) -> napi::Result<PermutationBuilder> {
        let seed = options.seed.map(|s| s as u64);
        let clump_size = options.clump_size.map(|c| c as u64);

        self.modify(|builder| {
            builder.with_shuffle_strategy(ShuffleStrategy::Random { seed, clump_size })
        })
    }

    /// Configure filtering
    #[napi]
    pub fn filter(&self, filter: String) -> napi::Result<PermutationBuilder> {
        self.modify(|builder| builder.with_filter(filter))
    }

    /// Execute the permutation builder and create the table
    #[napi]
    pub async fn execute(&self) -> napi::Result<Table> {
        let (builder, dest_table_name) = {
            let mut state = self.state.lock().unwrap();
            let builder = state
                .builder
                .take()
                .ok_or_else(|| napi::Error::from_reason("Builder already consumed"))?;

            let dest_table_name = std::mem::take(&mut state.dest_table_name);
            (builder, dest_table_name)
        };

        let table = builder.build(&dest_table_name).await.default_error()?;
        Ok(Table::new(table))
    }
}