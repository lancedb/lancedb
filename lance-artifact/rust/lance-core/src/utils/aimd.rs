// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! AIMD (Additive Increase / Multiplicative Decrease) rate controller.
//!
//! This module provides a reusable AIMD algorithm for dynamically adjusting
//! request rates. On success windows, the rate increases additively. On
//! windows with throttle signals, the rate decreases multiplicatively.
//!
//! The algorithm operates in discrete time windows. At the end of each window,
//! the throttle ratio (throttled / total) is compared against a threshold:
//! - Above threshold: `rate = max(rate * decrease_factor, min_rate)`
//! - At or below threshold: `rate = min(rate + additive_increment, max_rate)`

use std::sync::Mutex;
use std::time::Duration;

use crate::Result;

/// Configuration for the AIMD rate controller.
///
/// Use builder methods to customize. Defaults are tuned for cloud object stores
/// and will start at about 40% of the max rate and require 10 seconds to reach
/// the max rate.
///
/// - initial_rate: 2000 req/s
/// - min_rate: 1 req/s
/// - max_rate: 5000 req/s (0.0 disables ceiling)
/// - decrease_factor: 0.5 (halve on throttle)
/// - additive_increment: 300 req/s per success window
/// - window_duration: 1 second
/// - throttle_threshold: 0.0 (any throttle triggers decrease)
#[derive(Debug, Clone)]
pub struct AimdConfig {
    pub initial_rate: f64,
    pub min_rate: f64,
    pub max_rate: f64,
    pub decrease_factor: f64,
    pub additive_increment: f64,
    pub window_duration: Duration,
    pub throttle_threshold: f64,
}

impl Default for AimdConfig {
    fn default() -> Self {
        Self {
            initial_rate: 2000.0,
            min_rate: 1.0,
            max_rate: 5000.0,
            decrease_factor: 0.5,
            additive_increment: 300.0,
            window_duration: Duration::from_secs(1),
            throttle_threshold: 0.0,
        }
    }
}

impl AimdConfig {
    pub fn with_initial_rate(self, initial_rate: f64) -> Self {
        Self {
            initial_rate,
            ..self
        }
    }

    pub fn with_min_rate(self, min_rate: f64) -> Self {
        Self { min_rate, ..self }
    }

    pub fn with_max_rate(self, max_rate: f64) -> Self {
        Self { max_rate, ..self }
    }

    pub fn with_decrease_factor(self, decrease_factor: f64) -> Self {
        Self {
            decrease_factor,
            ..self
        }
    }

    pub fn with_additive_increment(self, additive_increment: f64) -> Self {
        Self {
            additive_increment,
            ..self
        }
    }

    pub fn with_window_duration(self, window_duration: Duration) -> Self {
        Self {
            window_duration,
            ..self
        }
    }

    pub fn with_throttle_threshold(self, throttle_threshold: f64) -> Self {
        Self {
            throttle_threshold,
            ..self
        }
    }

    /// Validate that the configuration values are sensible.
    pub fn validate(&self) -> Result<()> {
        if self.initial_rate <= 0.0 {
            return Err(crate::Error::invalid_input(format!(
                "initial_rate must be positive, got {}",
                self.initial_rate
            )));
        }
        if self.min_rate <= 0.0 {
            return Err(crate::Error::invalid_input(format!(
                "min_rate must be positive, got {}",
                self.min_rate
            )));
        }
        if self.max_rate < 0.0 {
            return Err(crate::Error::invalid_input(format!(
                "max_rate must be non-negative (0.0 = no ceiling), got {}",
                self.max_rate
            )));
        }
        if self.max_rate > 0.0 && self.min_rate > self.max_rate {
            return Err(crate::Error::invalid_input(format!(
                "min_rate ({}) must not exceed max_rate ({})",
                self.min_rate, self.max_rate
            )));
        }
        if self.decrease_factor <= 0.0 || self.decrease_factor >= 1.0 {
            return Err(crate::Error::invalid_input(format!(
                "decrease_factor must be in (0, 1), got {}",
                self.decrease_factor
            )));
        }
        if self.additive_increment <= 0.0 {
            return Err(crate::Error::invalid_input(format!(
                "additive_increment must be positive, got {}",
                self.additive_increment
            )));
        }
        if self.window_duration.is_zero() {
            return Err(crate::Error::invalid_input(
                "window_duration must be non-zero",
            ));
        }
        if !(0.0..=1.0).contains(&self.throttle_threshold) {
            return Err(crate::Error::invalid_input(format!(
                "throttle_threshold must be in [0.0, 1.0], got {}",
                self.throttle_threshold
            )));
        }
        if self.max_rate > 0.0 && self.initial_rate > self.max_rate {
            return Err(crate::Error::invalid_input(format!(
                "initial_rate ({}) must not exceed max_rate ({})",
                self.initial_rate, self.max_rate
            )));
        }
        if self.initial_rate < self.min_rate {
            return Err(crate::Error::invalid_input(format!(
                "initial_rate ({}) must not be below min_rate ({})",
                self.initial_rate, self.min_rate
            )));
        }
        Ok(())
    }
}

/// Outcome of a single request, used to feed the AIMD controller.
///
/// Non-throttle errors (e.g. 404, network timeout) should be mapped to
/// `Success` since they don't indicate capacity problems.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RequestOutcome {
    Success,
    Throttled,
}

struct AimdState {
    rate: f64,
    window_start: std::time::Instant,
    success_count: u64,
    throttle_count: u64,
}

/// AIMD rate controller.
///
/// Thread-safe: uses an internal `Mutex` to protect state. The lock is held
/// only briefly during `record_outcome` and `current_rate`.
pub struct AimdController {
    config: AimdConfig,
    state: Mutex<AimdState>,
}

impl std::fmt::Debug for AimdController {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AimdController")
            .field("config", &self.config)
            .field("rate", &self.current_rate())
            .finish()
    }
}

impl AimdController {
    /// Create a new AIMD controller with the given configuration.
    pub fn new(config: AimdConfig) -> Result<Self> {
        config.validate()?;
        let rate = config.initial_rate;
        Ok(Self {
            config,
            state: Mutex::new(AimdState {
                rate,
                window_start: std::time::Instant::now(),
                success_count: 0,
                throttle_count: 0,
            }),
        })
    }

    /// Record a request outcome and return the current rate.
    ///
    /// If the current time window has expired, the rate is adjusted before
    /// recording the new outcome in a fresh window.
    pub fn record_outcome(&self, outcome: RequestOutcome) -> f64 {
        let mut state = self.state.lock().unwrap();
        self.record_outcome_inner(&mut state, outcome, std::time::Instant::now())
    }

    fn record_outcome_inner(
        &self,
        state: &mut AimdState,
        outcome: RequestOutcome,
        now: std::time::Instant,
    ) -> f64 {
        // Check if the window has expired
        let elapsed = now.duration_since(state.window_start);
        if elapsed >= self.config.window_duration {
            let total = state.success_count + state.throttle_count;
            if total > 0 {
                let throttle_ratio = state.throttle_count as f64 / total as f64;
                if throttle_ratio > self.config.throttle_threshold {
                    // Multiplicative decrease
                    state.rate =
                        (state.rate * self.config.decrease_factor).max(self.config.min_rate);
                } else {
                    // Additive increase
                    state.rate += self.config.additive_increment;
                    if self.config.max_rate > 0.0 {
                        state.rate = state.rate.min(self.config.max_rate);
                    }
                }
            }
            // Reset window
            state.window_start = now;
            state.success_count = 0;
            state.throttle_count = 0;
        }

        // Record this outcome
        match outcome {
            RequestOutcome::Success => state.success_count += 1,
            RequestOutcome::Throttled => state.throttle_count += 1,
        }

        state.rate
    }

    /// Get the current rate without recording an outcome.
    pub fn current_rate(&self) -> f64 {
        self.state.lock().unwrap().rate
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;

    #[rstest]
    #[case::zero_initial_rate(
        AimdConfig::default().with_initial_rate(0.0),
        "initial_rate must be positive"
    )]
    #[case::negative_min_rate(
        AimdConfig::default().with_min_rate(-1.0),
        "min_rate must be positive"
    )]
    #[case::negative_max_rate(
        AimdConfig::default().with_max_rate(-1.0),
        "max_rate must be non-negative"
    )]
    #[case::min_exceeds_max(
        AimdConfig::default().with_min_rate(100.0).with_max_rate(10.0),
        "min_rate (100) must not exceed max_rate (10)"
    )]
    #[case::decrease_factor_zero(
        AimdConfig::default().with_decrease_factor(0.0),
        "decrease_factor must be in (0, 1)"
    )]
    #[case::decrease_factor_one(
        AimdConfig::default().with_decrease_factor(1.0),
        "decrease_factor must be in (0, 1)"
    )]
    #[case::decrease_factor_over_one(
        AimdConfig::default().with_decrease_factor(1.5),
        "decrease_factor must be in (0, 1)"
    )]
    #[case::zero_additive_increment(
        AimdConfig::default().with_additive_increment(0.0),
        "additive_increment must be positive"
    )]
    #[case::zero_window_duration(
        AimdConfig::default().with_window_duration(Duration::ZERO),
        "window_duration must be non-zero"
    )]
    #[case::threshold_over_one(
        AimdConfig::default().with_throttle_threshold(1.1),
        "throttle_threshold must be in [0.0, 1.0]"
    )]
    #[case::threshold_negative(
        AimdConfig::default().with_throttle_threshold(-0.1),
        "throttle_threshold must be in [0.0, 1.0]"
    )]
    #[case::initial_exceeds_max(
        AimdConfig::default().with_initial_rate(6000.0),
        "initial_rate (6000) must not exceed max_rate (5000)"
    )]
    #[case::initial_below_min(
        AimdConfig::default().with_initial_rate(0.5).with_min_rate(1.0),
        "initial_rate (0.5) must not be below min_rate (1)"
    )]
    fn test_config_validation_rejects_invalid(
        #[case] config: AimdConfig,
        #[case] expected_msg: &str,
    ) {
        let err = config.validate().unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains(expected_msg),
            "Expected error containing '{}', got: {}",
            expected_msg,
            msg
        );
    }

    #[test]
    fn test_default_config_is_valid() {
        AimdConfig::default().validate().unwrap();
    }

    #[test]
    fn test_no_ceiling_config_is_valid() {
        AimdConfig::default().with_max_rate(0.0).validate().unwrap();
    }

    #[test]
    fn test_additive_increase_on_success_window() {
        let config = AimdConfig::default()
            .with_initial_rate(100.0)
            .with_additive_increment(10.0)
            .with_window_duration(Duration::from_millis(100));
        let controller = AimdController::new(config).unwrap();

        // Record some successes in the first window
        let start = std::time::Instant::now();
        {
            let mut state = controller.state.lock().unwrap();
            controller.record_outcome_inner(&mut state, RequestOutcome::Success, start);
        }

        // Advance past the window boundary and record another success
        let after_window = start + Duration::from_millis(150);
        {
            let mut state = controller.state.lock().unwrap();
            controller.record_outcome_inner(&mut state, RequestOutcome::Success, after_window);
        }

        // Rate should have increased by additive_increment
        assert_eq!(controller.current_rate(), 110.0);
    }

    #[test]
    fn test_multiplicative_decrease_on_throttle_window() {
        let config = AimdConfig::default()
            .with_initial_rate(100.0)
            .with_decrease_factor(0.5)
            .with_window_duration(Duration::from_millis(100));
        let controller = AimdController::new(config).unwrap();

        let start = std::time::Instant::now();
        {
            let mut state = controller.state.lock().unwrap();
            controller.record_outcome_inner(&mut state, RequestOutcome::Throttled, start);
        }

        // Advance past window
        let after_window = start + Duration::from_millis(150);
        {
            let mut state = controller.state.lock().unwrap();
            controller.record_outcome_inner(&mut state, RequestOutcome::Success, after_window);
        }

        assert_eq!(controller.current_rate(), 50.0);
    }

    #[test]
    fn test_floor_enforcement() {
        let config = AimdConfig::default()
            .with_initial_rate(2.0)
            .with_min_rate(1.0)
            .with_decrease_factor(0.5)
            .with_window_duration(Duration::from_millis(100));
        let controller = AimdController::new(config).unwrap();

        let start = std::time::Instant::now();
        {
            let mut state = controller.state.lock().unwrap();
            controller.record_outcome_inner(&mut state, RequestOutcome::Throttled, start);
        }

        // After decrease: 2.0 * 0.5 = 1.0 (at floor)
        let t1 = start + Duration::from_millis(150);
        {
            let mut state = controller.state.lock().unwrap();
            controller.record_outcome_inner(&mut state, RequestOutcome::Throttled, t1);
        }
        assert_eq!(controller.current_rate(), 1.0);

        // Another decrease should stay at floor
        let t2 = t1 + Duration::from_millis(150);
        {
            let mut state = controller.state.lock().unwrap();
            controller.record_outcome_inner(&mut state, RequestOutcome::Success, t2);
        }
        assert_eq!(controller.current_rate(), 1.0);
    }

    #[test]
    fn test_ceiling_enforcement() {
        let config = AimdConfig::default()
            .with_initial_rate(4990.0)
            .with_max_rate(5000.0)
            .with_additive_increment(20.0)
            .with_window_duration(Duration::from_millis(100));
        let controller = AimdController::new(config).unwrap();

        let start = std::time::Instant::now();
        {
            let mut state = controller.state.lock().unwrap();
            controller.record_outcome_inner(&mut state, RequestOutcome::Success, start);
        }

        let t1 = start + Duration::from_millis(150);
        {
            let mut state = controller.state.lock().unwrap();
            controller.record_outcome_inner(&mut state, RequestOutcome::Success, t1);
        }
        // 4990 + 20 = 5010, clamped to 5000
        assert_eq!(controller.current_rate(), 5000.0);
    }

    #[test]
    fn test_no_ceiling_allows_unbounded_growth() {
        let config = AimdConfig::default()
            .with_initial_rate(100.0)
            .with_max_rate(0.0)
            .with_additive_increment(50.0)
            .with_window_duration(Duration::from_millis(100));
        let controller = AimdController::new(config).unwrap();

        let start = std::time::Instant::now();
        let mut t = start;

        for _ in 0..5 {
            {
                let mut state = controller.state.lock().unwrap();
                controller.record_outcome_inner(&mut state, RequestOutcome::Success, t);
            }
            t += Duration::from_millis(150);
        }

        // Trigger final window evaluation
        {
            let mut state = controller.state.lock().unwrap();
            controller.record_outcome_inner(&mut state, RequestOutcome::Success, t);
        }

        // 100 + 50*5 = 350
        assert_eq!(controller.current_rate(), 350.0);
    }

    #[test]
    fn test_empty_window_no_adjustment() {
        let config = AimdConfig::default()
            .with_initial_rate(100.0)
            .with_window_duration(Duration::from_millis(100));
        let controller = AimdController::new(config).unwrap();

        // Don't record anything in the first window, just advance time
        let start = std::time::Instant::now();
        let after = start + Duration::from_millis(150);
        {
            let mut state = controller.state.lock().unwrap();
            // First outcome in a new window after empty window
            controller.record_outcome_inner(&mut state, RequestOutcome::Success, after);
        }
        // No adjustment because the expired window had 0 total
        assert_eq!(controller.current_rate(), 100.0);
    }

    #[test]
    fn test_throttle_threshold_filtering() {
        // With threshold 0.5, less than 50% throttles should still increase
        let config = AimdConfig::default()
            .with_initial_rate(100.0)
            .with_throttle_threshold(0.5)
            .with_additive_increment(10.0)
            .with_window_duration(Duration::from_millis(100));
        let controller = AimdController::new(config).unwrap();

        let start = std::time::Instant::now();
        {
            let mut state = controller.state.lock().unwrap();
            // 1 throttle out of 3 = 33% < 50% threshold
            controller.record_outcome_inner(&mut state, RequestOutcome::Success, start);
            controller.record_outcome_inner(&mut state, RequestOutcome::Success, start);
            controller.record_outcome_inner(&mut state, RequestOutcome::Throttled, start);
        }

        // Advance past window
        let t1 = start + Duration::from_millis(150);
        {
            let mut state = controller.state.lock().unwrap();
            controller.record_outcome_inner(&mut state, RequestOutcome::Success, t1);
        }

        // Should have increased because 33% <= 50%
        assert_eq!(controller.current_rate(), 110.0);
    }

    #[test]
    fn test_throttle_threshold_triggers_decrease() {
        // With threshold 0.5, >= 50% throttles should decrease
        let config = AimdConfig::default()
            .with_initial_rate(100.0)
            .with_throttle_threshold(0.5)
            .with_decrease_factor(0.5)
            .with_window_duration(Duration::from_millis(100));
        let controller = AimdController::new(config).unwrap();

        let start = std::time::Instant::now();
        {
            let mut state = controller.state.lock().unwrap();
            // 2 throttle out of 3 = 67% > 50% threshold
            controller.record_outcome_inner(&mut state, RequestOutcome::Success, start);
            controller.record_outcome_inner(&mut state, RequestOutcome::Throttled, start);
            controller.record_outcome_inner(&mut state, RequestOutcome::Throttled, start);
        }

        let t1 = start + Duration::from_millis(150);
        {
            let mut state = controller.state.lock().unwrap();
            controller.record_outcome_inner(&mut state, RequestOutcome::Success, t1);
        }

        assert_eq!(controller.current_rate(), 50.0);
    }

    #[test]
    fn test_recovery_after_decrease() {
        let config = AimdConfig::default()
            .with_initial_rate(100.0)
            .with_decrease_factor(0.5)
            .with_additive_increment(10.0)
            .with_window_duration(Duration::from_millis(100));
        let controller = AimdController::new(config).unwrap();

        let start = std::time::Instant::now();

        // Window 1: throttle → decrease to 50
        {
            let mut state = controller.state.lock().unwrap();
            controller.record_outcome_inner(&mut state, RequestOutcome::Throttled, start);
        }
        let t1 = start + Duration::from_millis(150);

        // Window 2: success → increase to 60
        {
            let mut state = controller.state.lock().unwrap();
            controller.record_outcome_inner(&mut state, RequestOutcome::Success, t1);
        }
        let t2 = t1 + Duration::from_millis(150);

        // Window 3: success → increase to 70
        {
            let mut state = controller.state.lock().unwrap();
            controller.record_outcome_inner(&mut state, RequestOutcome::Success, t2);
        }
        let t3 = t2 + Duration::from_millis(150);

        // Trigger final evaluation
        {
            let mut state = controller.state.lock().unwrap();
            controller.record_outcome_inner(&mut state, RequestOutcome::Success, t3);
        }

        assert_eq!(controller.current_rate(), 70.0);
    }

    #[test]
    fn test_within_window_no_adjustment() {
        let config = AimdConfig::default()
            .with_initial_rate(100.0)
            .with_window_duration(Duration::from_secs(10));
        let controller = AimdController::new(config).unwrap();

        // Record many outcomes but all within the same window
        for _ in 0..100 {
            controller.record_outcome(RequestOutcome::Throttled);
        }

        // Rate should still be initial since window hasn't expired
        assert_eq!(controller.current_rate(), 100.0);
    }
}
