use rand::{Rng, SeedableRng};
use std::time::Duration;

// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

/// Computes backoff as
///
/// ```text
/// backoff = base^attempt * unit + jitter
/// ```
///
/// The defaults are base=2, unit=50ms, jitter=50ms, min=0ms, max=5s. This gives
/// a backoff of 50ms, 100ms, 200ms, 400ms, 800ms, 1.6s, 3.2s, 5s, (not including jitter).
///
/// You can have non-exponential backoff by setting base=1.
pub struct Backoff {
    base: u32,
    unit: u32,
    jitter: i32,
    min: u32,
    max: u32,
    attempt: u32,
}

impl Default for Backoff {
    fn default() -> Self {
        Self {
            base: 2,
            unit: 50,
            jitter: 50,
            min: 0,
            max: 5000,
            attempt: 0,
        }
    }
}

impl Backoff {
    pub fn with_base(self, base: u32) -> Self {
        Self { base, ..self }
    }

    pub fn with_unit(self, unit: u32) -> Self {
        Self { unit, ..self }
    }

    pub fn with_jitter(self, jitter: i32) -> Self {
        Self { jitter, ..self }
    }

    pub fn with_min(self, min: u32) -> Self {
        Self { min, ..self }
    }

    pub fn with_max(self, max: u32) -> Self {
        Self { max, ..self }
    }

    pub fn next_backoff(&mut self) -> Duration {
        let backoff = self
            .base
            .saturating_pow(self.attempt)
            .saturating_mul(self.unit);
        let jitter = rand::rng().random_range(-self.jitter..=self.jitter);
        let backoff = (backoff.saturating_add_signed(jitter)).clamp(self.min, self.max);
        self.attempt += 1;
        Duration::from_millis(backoff as u64)
    }

    pub fn attempt(&self) -> u32 {
        self.attempt
    }

    pub fn reset(&mut self) {
        self.attempt = 0;
    }
}

/// Upper bound on the number of retry slots.
///
/// Slots double each attempt to spread contending writers apart, but a hundred
/// or so already exceeds any realistic number of concurrent committers, so
/// further doubling only inflates the wait without reducing collisions. Capping
/// the count also bounds a single backoff to `(MAX_SLOTS - 1) * unit` instead of
/// letting it grow without limit as `attempt` climbs.
const MAX_SLOTS: u32 = 128;

/// SlotBackoff is a backoff strategy that randomly chooses a time slot to retry.
///
/// This is useful when you have multiple tasks that can't overlap, and each
/// task takes roughly the same amount of time.
///
/// The `unit` represents the time it takes to complete one attempt. Future attempts
/// are divided into time slots, and a random slot is chosen for the retry. The number
/// of slots increases exponentially with each attempt. Initially, there are 4 slots,
/// then 8, then 16, and so on, up to a fixed cap.
///
/// Example:
/// Suppose you have 10 tasks that can't overlap, each taking 1 second. The tasks
/// don't know about each other and can't coordinate. Each task randomly picks a
/// time slot to retry. Here's how it might look:
///
/// First round (4 slots):
/// ```text
/// task id   | 1, 2, 3 | 4, 5, 6 | 7, 8, 9 | 10 |
/// status    | x, x, ✓ | x, x, ✓ | x, x, ✓ | ✓  |
/// timeline  | 0s      | 1s      | 2s      | 3s |
/// ```
/// Each slot can have one success. Here, tasks 3, 6, 9, and 10 succeed.
/// In the next round, the number of slots doubles (8):
///
/// Second round (8 slots):
/// ```text
/// task id   |  1 |  2 |    | 4, 5 |  7 |  8 |    |    |
/// status    |  ✓ |  ✓ |    | x, ✓ |  ✓ |  ✓ |    |    |
/// timeline  | 0s | 1s | 2s | 3s   | 4s | 5s | 6s | 7s |
/// ```
/// Most tasks are done now, except for task 4. It will succeed in the next round.
pub struct SlotBackoff {
    base: u32,
    unit: u32,
    starting_i: u32,
    attempt: u32,
    rng: rand::rngs::SmallRng,
}

impl Default for SlotBackoff {
    fn default() -> Self {
        Self {
            base: 2,
            unit: 50,
            starting_i: 2, // start with 4 slots
            attempt: 0,
            rng: rand::rngs::SmallRng::from_os_rng(),
        }
    }
}

impl SlotBackoff {
    pub fn with_unit(self, unit: u32) -> Self {
        Self { unit, ..self }
    }

    pub fn attempt(&self) -> u32 {
        self.attempt
    }

    pub fn next_backoff(&mut self) -> Duration {
        let num_slots = self
            .base
            .saturating_pow(self.attempt.saturating_add(self.starting_i))
            .min(MAX_SLOTS);
        let slot_i = self.rng.random_range(0..num_slots);
        self.attempt = self.attempt.saturating_add(1);
        // Widen before multiplying: `unit` is the first-attempt latency, which
        // can be large enough that a `u32` slot * unit product would overflow.
        Duration::from_millis(slot_i as u64 * self.unit as u64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_backoff() {
        let mut backoff = Backoff::default().with_jitter(0);
        assert_eq!(backoff.next_backoff().as_millis(), 50);
        assert_eq!(backoff.attempt(), 1);
        assert_eq!(backoff.next_backoff().as_millis(), 100);
        assert_eq!(backoff.attempt(), 2);
        assert_eq!(backoff.next_backoff().as_millis(), 200);
        assert_eq!(backoff.attempt(), 3);
        assert_eq!(backoff.next_backoff().as_millis(), 400);
        assert_eq!(backoff.attempt(), 4);
    }

    #[test]
    fn test_backoff_with_base() {
        let mut backoff = Backoff::default().with_base(3).with_jitter(0);
        assert_eq!(backoff.next_backoff().as_millis(), 50); // 3^0 * 50
        assert_eq!(backoff.next_backoff().as_millis(), 150); // 3^1 * 50
        assert_eq!(backoff.next_backoff().as_millis(), 450); // 3^2 * 50
    }

    #[test]
    fn test_backoff_with_unit() {
        let mut backoff = Backoff::default().with_unit(100).with_jitter(0);
        assert_eq!(backoff.next_backoff().as_millis(), 100); // 2^0 * 100
        assert_eq!(backoff.next_backoff().as_millis(), 200); // 2^1 * 100
    }

    #[test]
    fn test_backoff_with_min() {
        let mut backoff = Backoff::default().with_min(100).with_jitter(0);
        assert_eq!(backoff.next_backoff().as_millis(), 100); // clamped to min
    }

    #[test]
    fn test_backoff_with_max() {
        let mut backoff = Backoff::default().with_max(75).with_jitter(0);
        assert_eq!(backoff.next_backoff().as_millis(), 50);
        assert_eq!(backoff.next_backoff().as_millis(), 75); // clamped to max
    }

    #[test]
    fn test_backoff_reset() {
        let mut backoff = Backoff::default().with_jitter(0);
        assert_eq!(backoff.next_backoff().as_millis(), 50);
        assert_eq!(backoff.attempt(), 1);
        backoff.reset();
        assert_eq!(backoff.attempt(), 0);
        assert_eq!(backoff.next_backoff().as_millis(), 50);
    }

    #[test]
    fn test_slot_backoff() {
        #[cfg_attr(coverage, coverage(off))]
        fn assert_in(value: u128, expected: &[u128]) {
            assert!(
                expected.contains(&value),
                "value {} not in {:?}",
                value,
                expected
            );
        }

        for _ in 0..10 {
            let mut backoff = SlotBackoff::default().with_unit(100);
            assert_in(backoff.next_backoff().as_millis(), &[0, 100, 200, 300]);
            assert_eq!(backoff.attempt(), 1);
            assert_in(
                backoff.next_backoff().as_millis(),
                &[0, 100, 200, 300, 400, 500, 600, 700],
            );
            assert_eq!(backoff.attempt(), 2);
            assert_in(
                backoff.next_backoff().as_millis(),
                &(0..16).map(|i| i * 100).collect::<Vec<_>>(),
            );
            assert_eq!(backoff.attempt(), 3);
        }
    }

    #[test]
    fn test_slot_backoff_high_attempt_is_bounded() {
        // Without the slot cap the wait grows unbounded with `attempt`. The cap
        // holds every backoff to `(MAX_SLOTS - 1) * unit`.
        let unit = 100_000; // 100s first attempt
        let mut backoff = SlotBackoff::default().with_unit(unit);
        let max_backoff = Duration::from_millis((MAX_SLOTS - 1) as u64 * unit as u64);
        for _ in 0..40 {
            assert!(backoff.next_backoff() <= max_backoff);
        }
        assert_eq!(backoff.attempt(), 40);
    }

    #[test]
    fn test_slot_backoff_large_unit_does_not_overflow() {
        // With unit = u32::MAX, any slot >= 2 makes the old u32 `slot_i * unit`
        // product overflow: a debug panic, or in release a wrap to a value that
        // is no longer a multiple of unit. The u64 widening keeps every backoff
        // an exact multiple of unit. Seed the RNG so the drawn slots — and thus
        // this check — are deterministic rather than dependent on random draws.
        let unit = u32::MAX;
        let mut backoff = SlotBackoff::default().with_unit(unit);
        backoff.rng = rand::rngs::SmallRng::seed_from_u64(0);
        let mut saw_high_slot = false;
        for _ in 0..64 {
            let backoff_ms = backoff.next_backoff().as_millis();
            // `slot_i * unit` is always a multiple of unit; a wrapped u32
            // product is not.
            assert_eq!(backoff_ms % unit as u128, 0, "{backoff_ms} wrapped");
            saw_high_slot |= backoff_ms >= 2 * unit as u128;
        }
        assert!(saw_high_slot, "expected a slot >= 2 in 64 seeded draws");
    }

    #[test]
    fn test_slot_backoff_attempt_saturates() {
        // At u32::MAX the counter must stay put rather than panic (debug) or
        // wrap to 0 (release), which would restart the low-slot distribution.
        let mut backoff = SlotBackoff {
            attempt: u32::MAX,
            ..Default::default()
        };
        let _ = backoff.next_backoff();
        assert_eq!(backoff.attempt(), u32::MAX);
    }
}
