// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Batched f16 dot product with an optional AMX-FP16 tile backend.
//!
//! Used by IVF over an fp16 column under dot distance: [`dot_f16_batch_16`]
//! scores one query against 16 centroids when choosing the partitions to probe,
//! and the GEMM behind [`PackedCentroidsF16`] assigns vectors to partitions
//! while an index is built.
//!
//! [`dot_f16_batch_16`] returns the 16 raw dot products `Σ query·candidate`
//! (same value convention as [`crate::distance::dot()`]); the caller applies the
//! `1.0 - dot` distance wrapping. On Linux/x86_64 hosts with AMX-FP16 it
//! dispatches to a single tile pass; everywhere else (and on any AMX
//! unavailability) it falls back to 16 independent [`crate::distance::dot()`]
//! calls, which are bit-identical to the per-vector scalar path.
//!
//! Two gates cover all of this, and they are deliberately separate:
//! [`amx_fp16_supported`] answers whether the tile instructions can run here at
//! all, and [`amx_fp16_available`] adds the `LANCE_DISABLE_AMX` kill switch on
//! top. The kernels here are guarded by the former so tests can always reach
//! them; callers routing production work consult the latter. AMX is on by
//! default — the switch exists for A/B measurement and for getting the previous
//! path back without a rebuild.
//!
//! Unlike integer AMX kernels this is floating point: the AMX and fallback paths
//! are **not** bit-for-bit identical (tile accumulation order rounds
//! differently), but both accumulate products in f32 and agree to within fp16
//! precision — a relative error on the order of 1e-4, far below fp16's own
//! representational error, so recall is unaffected.

use half::f16;

use crate::distance::dot::dot;

/// Batched f16 dot product: the raw dot products of one `query` against the
/// first `len` of 16 `candidates`, in order. Every candidate slice must have the
/// same length as `query`, and `len` must be in `1..=16`.
///
/// A batch is shorter than 16 only in the last group of a sweep, when the
/// centroid count is not a multiple of 16. `len` keeps the `16 - len` padding
/// rows out of the kernel's staging copy; with at most one short group per
/// sweep that saves much less than it would for a caller whose batches were
/// usually partial. Lanes `len..16` are returned as `0` rather than left
/// unspecified, so both the AMX and fallback paths agree exactly on what a
/// caller that reads past `len` sees.
///
/// Safe to call unconditionally: the caller never needs to know whether AMX is
/// present. The function panics if any candidate has a different length from
/// `query`, or if `len` is out of range, rather than allowing a malformed batch
/// to reach the FFI kernel. See the module docs for the accuracy contract.
#[inline]
pub fn dot_f16_batch_16(query: &[f16], candidates: &[&[f16]; 16], len: usize) -> [f32; 16] {
    assert!(
        candidates
            .iter()
            .all(|candidate| candidate.len() == query.len()),
        "all candidate vectors must have the same length as query"
    );
    assert!(
        (1..=16).contains(&len),
        "batch length must be in 1..=16, got {len}"
    );
    #[cfg(all(
        kernel_support = "amx_fp16",
        target_arch = "x86_64",
        target_os = "linux"
    ))]
    {
        // AMX only earns its tile-config overhead once there is at least one
        // full 32-wide pass; for tiny dims the fallback is cheaper anyway.
        if query.len() >= 32 && crate::simd::amx_fp16::amx_supported() {
            return unsafe { crate::simd::amx_fp16::dot_f16_batch_16_amx(query, candidates, len) };
        }
    }
    dot_f16_batch_16_fallback(query, candidates, len)
}

/// Fallback for [`dot_f16_batch_16`]: `len` independent [`crate::distance::dot()`]
/// calls — bit-identical to the per-vector scalar path (both go through
/// `f16::dot`) — and `0` for the remaining lanes, matching what the kernel
/// leaves there. Exposed separately so tests can exercise it regardless of host.
#[inline]
pub(crate) fn dot_f16_batch_16_fallback(
    query: &[f16],
    candidates: &[&[f16]; 16],
    len: usize,
) -> [f32; 16] {
    std::array::from_fn(|i| {
        if i < len {
            dot(query, candidates[i])
        } else {
            0.0
        }
    })
}

/// Centroids pre-packed into the layout the AMX-FP16 GEMM reads its B operand
/// in, together with the scoring entry point that consumes them.
///
/// This type exists unconditionally, and construction is the only gate:
/// [`PackedCentroidsF16::new`] returns `None` on a build or host without the
/// kernel. Callers in other crates cannot see lance-linalg's `kernel_support`
/// cfg, so an `Option` at run time is the only form of the gate they can branch
/// on to keep their own fallback path.
///
/// Packing costs `O(k * dim)` and is done once here rather than per block of
/// vectors, where it would outweigh the GEMM it feeds.
pub struct PackedCentroidsF16(Packed);

/// [`PackedCentroidsF16`]'s payload — and the reason that type needs no `cfg`
/// of its own: without the kernel this is uninhabited, so `PackedCentroidsF16`
/// is too. `new` provably cannot return `Some`, which is what lets the methods
/// discharge their bodies against a value that cannot exist instead of carrying
/// a fallback implementation that could never run.
#[cfg(all(
    kernel_support = "amx_fp16",
    target_arch = "x86_64",
    target_os = "linux"
))]
struct Packed {
    /// Zero-padded `[n_padded, dim]` centroids, row-major and tight. Held
    /// because the kernel reads the unpacked centroids directly for the
    /// `dim % 32` tail dims, which are not part of the packed layout.
    centroids: Vec<f16>,
    /// `centroids` in the kernel's VNNI B-tile order.
    packed: Vec<f16>,
    n_padded: usize,
    dim: usize,
}

#[cfg(not(all(
    kernel_support = "amx_fp16",
    target_arch = "x86_64",
    target_os = "linux"
)))]
enum Packed {}

/// How many elements a buffer must hold for `m` rows of `row_len`, `stride`
/// apart — `(m - 1) * stride + row_len` — or `None` if that overflows `usize`.
///
/// The checked arithmetic is the point. Written inline, the expression wraps
/// silently in release builds, and a wrapped requirement is *small*, so it
/// satisfies the very length check it was computed for: `m = 32`,
/// `stride = 595_056_260_442_243_601`, `row_len = 32` wraps to 47, admitting a
/// 47-element slice into a kernel that then strides `data + i * stride` past its
/// end. Every caller here is a safe function guarding an FFI boundary, so an
/// overflow has to be rejected rather than folded into a comparison.
///
/// Zero rows need zero elements. Handling that here rather than leaving it to
/// the caller keeps the function total: `m - 1` would underflow, which panics in
/// debug builds and wraps to `usize::MAX` in release ones — the same class of
/// silent wrap this function exists to prevent.
pub(crate) fn strided_len(m: usize, stride: usize, row_len: usize) -> Option<usize> {
    let Some(last_row) = m.checked_sub(1) else {
        return Some(0);
    };
    last_row.checked_mul(stride)?.checked_add(row_len)
}

impl PackedCentroidsF16 {
    /// Packs `n` row-major `dim`-dimensional `centroids` for repeated scoring.
    ///
    /// `None` means the GEMM is unavailable — this build has no kernel, this
    /// host cannot run it (both are [`amx_fp16_supported`]), or the shape is
    /// empty — and the caller must keep using its own path. There is no partial
    /// mode. Whether an operator has taken the AMX paths out of service is a
    /// separate question, answered by [`amx_fp16_available`] at the caller's
    /// routing decision, so that tests can build one of these regardless.
    ///
    /// `n` is rounded up to a multiple of 32 with zero centroids, since the
    /// kernel blocks its `n` loop by 32 and has no partial-tile path. The
    /// padding is visible to [`score`](Self::score)'s output and callers must
    /// account for it; see [`num_centroids_padded`](Self::num_centroids_padded).
    ///
    /// # Panics
    /// If `centroids` does not hold exactly `n * dim` values.
    pub fn new(centroids: &[f16], n: usize, dim: usize) -> Option<Self> {
        let expected = n
            .checked_mul(dim)
            .unwrap_or_else(|| panic!("centroid shape n = {n} x dim = {dim} overflows usize"));
        assert_eq!(
            centroids.len(),
            expected,
            "centroids must hold n*dim = {expected} values, got {}",
            centroids.len()
        );
        if n == 0 || dim == 0 || !amx_fp16_supported() {
            return None;
        }
        #[cfg(all(
            kernel_support = "amx_fp16",
            target_arch = "x86_64",
            target_os = "linux"
        ))]
        {
            // Same checked-size contract as the length guards below: this
            // allocation is what the kernel later reads through a raw pointer,
            // so a shape whose padded size is not representable is rejected
            // here rather than wrapped into an allocation smaller than the rows
            // the kernel will address. `packed_centroids_len` needs no separate
            // check -- `(dim / 32) * (n_padded / 16) * 512 <= n_padded * dim`
            // for every input, so it cannot overflow once this one holds.
            let n_padded = n.checked_next_multiple_of(32).unwrap_or_else(|| {
                panic!("padding n = {n} up to a multiple of 32 overflows usize")
            });
            let padded_len = n_padded.checked_mul(dim).unwrap_or_else(|| {
                panic!("padded centroid shape {n_padded} x dim = {dim} overflows usize")
            });
            let mut padded = vec![f16::ZERO; padded_len];
            padded[..centroids.len()].copy_from_slice(centroids);
            let mut packed =
                Vec::with_capacity(crate::simd::amx_fp16::packed_centroids_len(n_padded, dim));
            crate::simd::amx_fp16::pack_centroids_vnni(&padded, n_padded, dim, &mut packed);
            return Some(Self(Packed {
                centroids: padded,
                packed,
                n_padded,
                dim,
            }));
        }
        #[allow(unreachable_code)]
        None
    }

    /// The centroid count [`score`](Self::score) actually writes per row: the
    /// `n` given to [`new`](Self::new) rounded up to a multiple of 32.
    pub fn num_centroids_padded(&self) -> usize {
        self.shape().0
    }

    /// `(padded centroid count, dim)`. The single place the uninhabited-payload
    /// build is discharged, so the methods above it read as ordinary code.
    fn shape(&self) -> (usize, usize) {
        #[cfg(all(
            kernel_support = "amx_fp16",
            target_arch = "x86_64",
            target_os = "linux"
        ))]
        {
            (self.0.n_padded, self.0.dim)
        }
        #[cfg(not(all(
            kernel_support = "amx_fp16",
            target_arch = "x86_64",
            target_os = "linux"
        )))]
        {
            match self.0 {}
        }
    }

    /// Scores `m` vectors against every centroid: `out[i * out_stride + j]` is
    /// the raw dot product `Σ data_i·centroid_j` — the same value convention as
    /// [`crate::distance::dot()`], with no `1.0 - d` distance wrapping.
    ///
    /// Row `i` of `data` starts at `i * data_stride`, so both buffers may be
    /// windows of larger ones. Columns `n..num_centroids_padded()` are the zero
    /// padding centroids' scores; they are `0.0` for any finite input, which
    /// *beats* a real centroid whose dot product is negative. A caller reducing
    /// across a row must therefore stop at its own centroid count.
    ///
    /// See the module docs for the accuracy contract against the scalar path.
    ///
    /// # Panics
    /// If `m` is not a multiple of 32 (the kernel blocks its `m` loop by 32 and
    /// has no partial-tile path), if either stride is too small, or if either
    /// slice is too short for the last row its stride reaches.
    pub fn score(
        &self,
        data: &[f16],
        m: usize,
        data_stride: usize,
        out: &mut [f32],
        out_stride: usize,
    ) {
        let (n_padded, dim) = self.shape();
        assert_eq!(m % 32, 0, "m ({m}) must be a multiple of 32");
        assert!(
            data_stride >= dim,
            "data_stride ({data_stride}) is below dim ({dim})"
        );
        assert!(
            out_stride >= n_padded,
            "out_stride ({out_stride}) is below the padded centroid count ({n_padded})"
        );
        if m == 0 {
            return;
        }
        let data_needed = strided_len(m, data_stride, dim).unwrap_or_else(|| {
            panic!("m = {m} rows of dim {dim} at stride {data_stride} overflow usize")
        });
        assert!(
            data.len() >= data_needed,
            "data ({}) holds fewer than m = {m} rows of dim {dim} at stride {data_stride}",
            data.len()
        );
        let out_needed = strided_len(m, out_stride, n_padded).unwrap_or_else(|| {
            panic!("m = {m} rows of {n_padded} at stride {out_stride} overflow usize")
        });
        assert!(
            out.len() >= out_needed,
            "out ({}) holds fewer than m = {m} rows of {n_padded} at stride {out_stride}",
            out.len()
        );
        #[cfg(all(
            kernel_support = "amx_fp16",
            target_arch = "x86_64",
            target_os = "linux"
        ))]
        // SAFETY: the kernel's preconditions are exactly the asserts above plus
        // AMX availability. `n_padded % 32 == 0` and the packing / length
        // agreement between `packed`, `centroids`, `n_padded` and `dim` hold by
        // construction in `new`, which also proved `amx_fp16_supported()` — a
        // process-wide, monotonic property (CPUID plus a one-time, idempotent
        // XTILEDATA grant), so it cannot have lapsed since.
        unsafe {
            crate::simd::amx_fp16::dot_f16_gemm_amx(
                data,
                m,
                data_stride,
                &self.0.packed,
                &self.0.centroids,
                n_padded,
                dim,
                out,
                out_stride,
            );
        }
    }
}

/// Whether this process *can* execute the AMX-FP16 kernels at all. True when
/// all of the following hold:
///
/// - the kernel was compiled in, i.e. `build.rs` found a C compiler accepting
///   `-mamx-fp16` (clang >= 16 or gcc >= 13) and set `kernel_support`;
/// - the target is Linux/x86_64;
/// - the CPU reports both the amx-tile and the amx-fp16 CPUID bits;
/// - the one-time XTILEDATA `arch_prctl` grant succeeded.
///
/// This is the safety question — without the `arch_prctl` grant the first tile
/// instruction would SIGILL — so no tile instruction in this module runs until
/// it holds. It deliberately ignores [`LANCE_DISABLE_AMX`][amx_fp16_available],
/// so kernel-level tests can exercise the kernels on any host that can run them
/// even when an operator has turned the production paths off.
///
/// Callers choosing between the AMX path and their fallback want
/// [`amx_fp16_available`] instead.
///
/// Evaluated once and cached by the hardware probe underneath; nothing is
/// recomputed per call.
pub fn amx_fp16_supported() -> bool {
    #[cfg(all(
        kernel_support = "amx_fp16",
        target_arch = "x86_64",
        target_os = "linux"
    ))]
    {
        return crate::simd::amx_fp16::amx_supported();
    }
    #[allow(unreachable_code)]
    false
}

/// Whether production work should be routed onto the AMX-FP16 kernels: this
/// host can run them ([`amx_fp16_supported`]) and no operator has turned them
/// off.
///
/// **AMX is on by default.** Dispatch is a run-time decision made from CPU
/// capability alone, so a host with the silicon uses it with nothing to enable —
/// there is no Cargo feature and no opt-in variable. `LANCE_DISABLE_AMX` is the
/// escape hatch for the cases where a capability probe is not the whole story:
/// A/B measurement, and an operator who needs the previous code path back
/// without rebuilding. Set it to `1`, `true` or `on` (case-insensitive,
/// surrounding whitespace ignored) to take the AMX paths out of service; every
/// other value, and an unset variable, leave them in.
///
/// Note this changes *which algorithm* an index build uses, not just how fast it
/// runs: without the GEMM, partition assignment falls back to an approximate
/// graph lookup (see `lance_index`'s `prefers_flat_amx_assignment`). Two indexes
/// built on either side of this variable are not interchangeable.
pub fn amx_fp16_available() -> bool {
    !amx_fp16_disabled() && amx_fp16_supported()
}

/// The `LANCE_DISABLE_AMX` kill switch on its own, read once and cached. Cached
/// because the routing decisions that consult it run per block of vectors, and
/// because a build that flipped behaviour halfway through would be far harder to
/// reason about than one that reads the environment at startup.
fn amx_fp16_disabled() -> bool {
    static DISABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *DISABLED.get_or_init(|| {
        std::env::var("LANCE_DISABLE_AMX").is_ok_and(|value| is_amx_disable_value(&value))
    })
}

/// The accepted spellings of "off". Anything else — including `0`, `false` and
/// the empty string — leaves AMX enabled, because an unrecognised value must not
/// silently disable a path the operator did not clearly ask to disable.
fn is_amx_disable_value(value: &str) -> bool {
    matches!(
        value.trim().to_ascii_lowercase().as_str(),
        "1" | "true" | "on"
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};

    /// Dims covering: < 32 (no tile pass, pure fallback), exactly 32 (one pass,
    /// no tail), non-multiples of 32 (exercise the AMX tail), and larger dims
    /// (multiple passes).
    const BATCH_DIMS: &[usize] = &[
        1, 7, 31, 32, 33, 47, 64, 96, 100, 127, 128, 200, 256, 384, 768, 1000, 1536,
    ];

    fn make_batch(dim: usize, rng: &mut StdRng) -> (Vec<f16>, Vec<Vec<f16>>) {
        let gen_vec = |rng: &mut StdRng| -> Vec<f16> {
            (0..dim)
                .map(|_| f16::from_f32(rng.random_range(-1.0f32..1.0)))
                .collect()
        };
        let query = gen_vec(rng);
        let candidates = (0..16).map(|_| gen_vec(rng)).collect();
        (query, candidates)
    }

    /// f32-accumulated reference dot product — the semantic contract both the
    /// AMX and fallback paths approximate. `f16::dot` itself accumulates in f32.
    fn ref_dot_f32(query: &[f16], cand: &[f16]) -> f32 {
        query
            .iter()
            .zip(cand.iter())
            .map(|(&q, &c)| q.to_f32() * c.to_f32())
            .sum()
    }

    /// Relative-error tolerance justification: fp16 carries ~11 bits of mantissa
    /// (~3 decimal digits). The AMX path and the f32 reference differ only in
    /// summation order of f32-widened products, so the error is many orders
    /// tighter than fp16's own representational error; 5e-3 relative is a very
    /// safe bound (observed worst case is ~2e-4).
    const REL_TOL: f32 = 5e-3;

    fn assert_close(got: f32, want: f32, ctx: &str) {
        let rel = (got - want).abs() / (want.abs() + 1e-6);
        assert!(
            rel <= REL_TOL || (got - want).abs() <= 1e-3,
            "{ctx}: got {got} want {want} rel_err {rel}"
        );
    }

    #[test]
    fn fallback_matches_reference() {
        let mut rng = StdRng::seed_from_u64(0xF16);
        for &dim in BATCH_DIMS {
            let (query, cands) = make_batch(dim, &mut rng);
            let candidates: [&[f16]; 16] = std::array::from_fn(|i| cands[i].as_slice());
            let got = dot_f16_batch_16_fallback(&query, &candidates, 16);
            for i in 0..16 {
                assert_close(
                    got[i],
                    ref_dot_f32(&query, &cands[i]),
                    &format!("fb dim={dim} i={i}"),
                );
            }
        }
    }

    #[test]
    fn dispatch_matches_reference() {
        let mut rng = StdRng::seed_from_u64(0xBEEF);
        for &dim in BATCH_DIMS {
            let (query, cands) = make_batch(dim, &mut rng);
            let candidates: [&[f16]; 16] = std::array::from_fn(|i| cands[i].as_slice());
            let got = dot_f16_batch_16(&query, &candidates, 16);
            for i in 0..16 {
                assert_close(
                    got[i],
                    ref_dot_f32(&query, &cands[i]),
                    &format!("disp dim={dim} i={i}"),
                );
            }
        }
    }

    #[test]
    #[should_panic(expected = "all candidate vectors must have the same length")]
    fn rejects_mismatched_candidate_length() {
        let query = vec![f16::from_f32(1.0); 32];
        let short = vec![f16::from_f32(1.0); 31];
        let candidates: [&[f16]; 16] = std::array::from_fn(|i| {
            if i == 0 {
                short.as_slice()
            } else {
                query.as_slice()
            }
        });
        let _ = dot_f16_batch_16(&query, &candidates, 16);
    }

    /// A live lane must be bit-identical whatever `len` the batch was issued at.
    /// Shortening the batch changes only how many rows are gathered, never a
    /// row's contents nor the order the tile passes accumulate them, so anything
    /// less than bit-exact here would mean a staging offset moved with `len` —
    /// exactly the bug a tolerance would hide. Lanes past `len` must read 0 on
    /// both paths: dispatch is host-dependent, so a caller must not be able to
    /// tell which one ran.
    #[test]
    fn partial_len_matches_full_batch_and_zeroes_the_rest() {
        let mut rng = StdRng::seed_from_u64(0x1EE);
        for &dim in BATCH_DIMS {
            let (query, cands) = make_batch(dim, &mut rng);
            let candidates: [&[f16]; 16] = std::array::from_fn(|i| cands[i].as_slice());
            let full = dot_f16_batch_16(&query, &candidates, 16);
            let full_fb = dot_f16_batch_16_fallback(&query, &candidates, 16);
            for len in 1..=16 {
                let got = dot_f16_batch_16(&query, &candidates, len);
                let got_fb = dot_f16_batch_16_fallback(&query, &candidates, len);
                for i in 0..len {
                    assert_eq!(
                        got[i].to_bits(),
                        full[i].to_bits(),
                        "dim={dim} len={len} i={i}: {} vs {}",
                        got[i],
                        full[i]
                    );
                    assert_eq!(
                        got_fb[i].to_bits(),
                        full_fb[i].to_bits(),
                        "fb dim={dim} i={i}"
                    );
                }
                for i in len..16 {
                    assert_eq!(got[i], 0.0, "dim={dim} len={len}: lane {i} must be 0");
                    assert_eq!(got_fb[i], 0.0, "fb dim={dim} len={len}: lane {i} must be 0");
                }
            }
        }
    }

    /// `len` is rejected, never clamped: the kernel indexes its staging buffer
    /// by it, and a caller that meant 16 and passed 17 should hear about it.
    #[rstest::rstest]
    #[case::zero(0)]
    #[case::seventeen(17)]
    #[should_panic(expected = "batch length must be in 1..=16")]
    fn rejects_out_of_range_len(#[case] len: usize) {
        let query = vec![f16::from_f32(1.0); 32];
        let candidates: [&[f16]; 16] = std::array::from_fn(|_| query.as_slice());
        let _ = dot_f16_batch_16(&query, &candidates, len);
    }

    /// The `LANCE_DISABLE_AMX` truth table, pinned in one place.
    ///
    /// Which spellings mean "off" is an operator-facing contract, and both ways
    /// of getting it wrong are silent. A typo accepted as a kill switch —
    /// `LANCE_DISABLE_AMX=disable` — would quietly change which algorithm an
    /// index build uses, and the only visible trace would be a recall number
    /// nobody was comparing. A deliberate `1` rejected would leave an operator
    /// who needs the old path back believing they have it.
    ///
    /// The asymmetry with the enable direction is deliberate: an unrecognised
    /// value leaves AMX **on**, because the default is on and an unparsable
    /// request to deviate from it should not be honoured by halves.
    #[test]
    fn amx_disable_flag_accepts_only_explicit_on() {
        for value in ["1", "true", "on", "TRUE", "On", " 1 ", "true\n"] {
            assert!(is_amx_disable_value(value), "{value:?} should disable AMX");
        }
        for value in ["", " ", "0", "false", "off", "no", "yes", "2", "disable"] {
            assert!(
                !is_amx_disable_value(value),
                "{value:?} should not disable AMX"
            );
        }
    }

    /// With the kill switch unset, availability is exactly hardware support.
    ///
    /// This is the "on by default" contract itself: the assertion that would
    /// fail if a Cargo feature or an opt-in variable ever crept back in front of
    /// the dispatch decision. It is derived rather than hardcoded so it holds
    /// identically on a host without AMX and in a build whose toolchain could
    /// not compile the kernel.
    #[test]
    fn amx_is_available_by_default_wherever_it_is_supported() {
        if std::env::var_os("LANCE_DISABLE_AMX").is_some() {
            return; // the switch is under test elsewhere; respect it here
        }
        assert_eq!(amx_fp16_available(), amx_fp16_supported());
    }

    /// The length arithmetic guarding the GEMM's FFI boundary must reject a
    /// shape it cannot represent, not wrap it into a small number.
    ///
    /// The first case is the one that made this necessary: unchecked,
    /// `(32 - 1) * 595_056_260_442_243_601 + 32` wraps to **47**, so a
    /// 47-element slice satisfied a check that meant to demand ~1.8e19
    /// elements — and the kernel then strode `data + i * stride` far past the
    /// end of it. A wrapped requirement is dangerous precisely because it comes
    /// out *small* enough to pass.
    ///
    /// Deliberately a plain-function test: it runs on every host, including
    /// those where `PackedCentroidsF16` cannot be constructed at all.
    #[test]
    fn strided_len_rejects_shapes_it_cannot_represent() {
        assert_eq!(strided_len(32, 595_056_260_442_243_601, 32), None);
        assert_eq!(strided_len(2, usize::MAX, 1), None);
        assert_eq!(strided_len(usize::MAX, 2, 0), None);
        // Representable shapes still come through, including the degenerate
        // single-row case where the stride is never applied.
        assert_eq!(strided_len(32, 768, 768), Some(31 * 768 + 768));
        assert_eq!(strided_len(1, usize::MAX, 5), Some(5));
        // Zero rows need zero elements, and must not underflow `m - 1`.
        assert_eq!(strided_len(0, usize::MAX, usize::MAX), Some(0));
    }

    /// End to end: the safe `score` wrapper must panic on the overflowing
    /// shape rather than hand the undersized slice to the C kernel.
    ///
    /// `strided_len_rejects_shapes_it_cannot_represent` pins the arithmetic;
    /// this pins that `score` actually consults it before the `unsafe` block.
    #[cfg(all(
        kernel_support = "amx_fp16",
        target_arch = "x86_64",
        target_os = "linux"
    ))]
    #[test]
    fn score_rejects_overflowing_stride_before_ffi() {
        use std::panic::{AssertUnwindSafe, catch_unwind};

        let centroids = vec![f16::ONE; 32 * 32];
        let Some(packed) = PackedCentroidsF16::new(&centroids, 32, 32) else {
            return; // no AMX on this host; the arithmetic test above still ran
        };
        let data = vec![f16::ZERO; 47];
        let mut out = vec![0f32; 32 * 32];

        let hook = std::panic::take_hook();
        std::panic::set_hook(Box::new(|_| {})); // the panic is the expected result
        let result = catch_unwind(AssertUnwindSafe(|| {
            packed.score(&data, 32, 595_056_260_442_243_601, &mut out, 32);
        }));
        std::panic::set_hook(hook);

        assert!(
            result.is_err(),
            "score accepted a 47-element slice for a stride whose row count overflows usize"
        );
    }

    /// On AMX-FP16 hardware, assert the AMX branch is actually selected (not a
    /// silent fallback) and agrees with the f32 reference within tolerance.
    /// Reaching the end without a SIGILL is itself proof the tile path executed.
    #[cfg(all(
        kernel_support = "amx_fp16",
        target_arch = "x86_64",
        target_os = "linux"
    ))]
    #[test]
    fn amx_path_is_active_and_close() {
        if !crate::simd::amx_fp16::amx_supported() {
            return;
        }
        let mut rng = StdRng::seed_from_u64(0xA11);
        let mut worst = 0f32;
        for &dim in BATCH_DIMS.iter().filter(|&&d| d >= 32) {
            let (query, cands) = make_batch(dim, &mut rng);
            let candidates: [&[f16]; 16] = std::array::from_fn(|i| cands[i].as_slice());
            let amx =
                unsafe { crate::simd::amx_fp16::dot_f16_batch_16_amx(&query, &candidates, 16) };
            for i in 0..16 {
                let want = ref_dot_f32(&query, &cands[i]);
                assert_close(amx[i], want, &format!("amx dim={dim} i={i}"));
                let rel = (amx[i] - want).abs() / (want.abs() + 1e-6);
                worst = worst.max(rel);
            }
        }
        assert!(worst <= REL_TOL, "worst AMX relative error: {worst:.2e}");
    }

    /// Another AMX user on this thread retiring the tile configuration must not
    /// break the next kernel call.
    ///
    /// Regression for a stale-cache bug: the kernels used to remember which
    /// configuration they had loaded and skip LDTILECFG when it matched, but that
    /// record was private to Lance while LDTILECFG and TILERELEASE are
    /// architectural per-logical-processor state. After a foreign TILERELEASE the
    /// record still said "SEARCH is live" and the hardware was back in INIT, so
    /// the kernel skipped the load and its first tile op raised #UD. Reloading on
    /// every entry is the fix; this is what would fail if a cache came back.
    ///
    /// Both halves of that design are checked here, and only one of them has any
    /// other symptom. The reload shows up as the kernel surviving a clobber; the
    /// release on exit shows up nowhere but in the tile unit itself, read back at
    /// the end.
    #[cfg(all(
        kernel_support = "amx_fp16",
        target_arch = "x86_64",
        target_os = "linux"
    ))]
    #[test]
    fn kernel_reconfigures_after_foreign_tile_release() {
        use crate::simd::amx_fp16::{
            amx_supported, clobber_tile_state_for_test, dot_f16_batch_16_amx,
            tile_config_is_live_for_test,
        };

        if !amx_supported() {
            return;
        }
        let mut rng = StdRng::seed_from_u64(0xC10B);
        let (query, cands) = make_batch(256, &mut rng);
        let candidates: [&[f16]; 16] = std::array::from_fn(|i| cands[i].as_slice());

        let before = unsafe { dot_f16_batch_16_amx(&query, &candidates, 16) };
        // SAFETY: the `amx_supported()` check above granted XTILEDATA, so
        // TILERELEASE is legal here.
        unsafe { clobber_tile_state_for_test() };
        // Reaching the end of this call at all is the assertion that matters:
        // under the bug it raised #UD and killed the test process. Comparing the
        // results additionally catches the quieter variant, where a foreign
        // configuration supplies the wrong shapes instead of none.
        let after = unsafe { dot_f16_batch_16_amx(&query, &candidates, 16) };
        assert_eq!(
            before, after,
            "kernel output changed after a foreign TILERELEASE"
        );

        // Everything above only asks "did it crash". That is too weak on its own:
        // `lance_amx_tile_ensure` reloading on entry is by itself enough to keep
        // results right, so deleting `lance_amx_tile_done`'s TILERELEASE would
        // leave every assertion so far green while the tile unit stayed held
        // against the next AMX user on this thread. Reading the hardware is what
        // catches that.
        assert!(
            !tile_config_is_live_for_test(),
            "a kernel left its tile configuration loaded after returning"
        );
    }

    /// The batch-16 kernel's tile shape, pinned byte for byte.
    ///
    /// The shape is the one thing a refactor of `amx_fp16.c` can change with no
    /// visible symptom until it runs on AMX hardware, where a wrong shape is a
    /// #UD or wrong results rather than a clean failure. Reading the
    /// configuration image back is a way to check it without executing a single
    /// tile instruction.
    ///
    /// It still needs the `amx_supported()` guard, which is easy to mistake for
    /// redundant: `lance_amx_tilecfg_image` only fills a 64-byte struct. But
    /// `amx_fp16.c` is compiled as one translation unit with
    /// `-march=sapphirerapids`, so the compiler may use instructions from that
    /// baseline anywhere in the file — entering *any* function in it faults on
    /// an older CPU. Under `qemu -cpu Nehalem` that is a SIGILL, which is
    /// exactly what the `qemu-pre-haswell` CI job runs.
    #[cfg(all(
        kernel_support = "amx_fp16",
        target_arch = "x86_64",
        target_os = "linux"
    ))]
    #[test]
    fn search_tile_config_image_is_pinned() {
        use crate::simd::amx_fp16::{AMX_CFG_SEARCH, amx_supported, tilecfg_image};

        if !amx_supported() {
            return;
        }

        // Image layout per Intel SDM: palette_id, start_row, 14 reserved bytes,
        // a u16 colsb[16] array, then a u8 rows[16] array.
        const COLSB: usize = 16;
        const ROWS: usize = 48;

        let mut want = [0u8; 64];
        want[0] = 1; // palette_id
        // C = tmm0: 16 x 1 fp32, fed by three independent (A, B) pairs so three
        // TDPFP16PS can be in flight at once: A = tmm1/3/5, each 16 x 32 fp16;
        // B = tmm2/4/6, each 16 x 2 fp16. tmm7 is left unconfigured.
        for (tmm, colsb) in [
            (0usize, 4u16),
            (1, 64),
            (2, 4),
            (3, 64),
            (4, 4),
            (5, 64),
            (6, 4),
        ] {
            want[COLSB + tmm * 2..COLSB + tmm * 2 + 2].copy_from_slice(&colsb.to_le_bytes());
            want[ROWS + tmm] = 16;
        }

        let got = tilecfg_image(AMX_CFG_SEARCH).expect("search config kind must be known");
        assert_eq!(got, want, "batch-16 tile configuration changed");
    }

    /// An unknown config kind must be rejected rather than answered with a
    /// zeroed image: an all-zero (unconfigured) shape #UDs on the first tile op.
    ///
    /// Guarded for the same reason as the test above: reaching the C side at
    /// all requires a CPU that can run its `-march=sapphirerapids` code.
    #[cfg(all(
        kernel_support = "amx_fp16",
        target_arch = "x86_64",
        target_os = "linux"
    ))]
    #[test]
    fn unknown_tile_config_kind_is_rejected() {
        if !crate::simd::amx_fp16::amx_supported() {
            return;
        }
        assert!(crate::simd::amx_fp16::tilecfg_image(-1).is_none());
    }

    // -----------------------------------------------------------------------
    // AMX-FP16 m x n GEMM
    // -----------------------------------------------------------------------

    /// `[m, dim]` vectors and `[n, dim]` centroids, both row-major and tightly
    /// packed. Separate rngs per role would make a transposition bug harder to
    /// spot, so both come off one stream.
    fn make_gemm(m: usize, n: usize, dim: usize, rng: &mut StdRng) -> (Vec<f16>, Vec<f16>) {
        let mut sample = |count: usize| -> Vec<f16> {
            (0..count)
                .map(|_| f16::from_f32(rng.random_range(-1.0f32..1.0)))
                .collect()
        };
        (sample(m * dim), sample(n * dim))
    }

    /// f32-accumulated reference GEMM — the same semantic contract as
    /// [`ref_dot_f32`], extended to every (vector, centroid) pair.
    fn ref_gemm(
        data: &[f16],
        m: usize,
        data_stride: usize,
        centroids: &[f16],
        n: usize,
        dim: usize,
    ) -> Vec<f32> {
        let mut out = vec![0f32; m * n];
        for i in 0..m {
            let row = &data[i * data_stride..i * data_stride + dim];
            for j in 0..n {
                out[i * n + j] = ref_dot_f32(row, &centroids[j * dim..j * dim + dim]);
            }
        }
        out
    }

    /// Pack + run the GEMM kernel, returning an `[m, out_stride]` buffer.
    ///
    /// The destination starts as NaN rather than 0 so a tile store that never
    /// lands (wrong stride, wrong tile index) fails the comparison instead of
    /// passing on a coincidentally-correct zero.
    #[cfg(all(
        kernel_support = "amx_fp16",
        target_arch = "x86_64",
        target_os = "linux"
    ))]
    fn run_gemm(
        data: &[f16],
        m: usize,
        data_stride: usize,
        centroids: &[f16],
        n: usize,
        dim: usize,
        out_stride: usize,
    ) -> Vec<f32> {
        use crate::simd::amx_fp16::{dot_f16_gemm_amx, pack_centroids_vnni};
        let mut packed = Vec::new();
        pack_centroids_vnni(centroids, n, dim, &mut packed);
        let mut out = vec![f32::NAN; m * out_stride];
        unsafe {
            dot_f16_gemm_amx(
                data,
                m,
                data_stride,
                &packed,
                centroids,
                n,
                dim,
                &mut out,
                out_stride,
            );
        }
        out
    }

    /// Compare a `[m, out_stride]` kernel result against a tightly-packed
    /// `[m, n]` reference.
    fn assert_gemm_close(
        got: &[f32],
        want: &[f32],
        m: usize,
        n: usize,
        out_stride: usize,
        ctx: &str,
    ) {
        for i in 0..m {
            for j in 0..n {
                assert_close(
                    got[i * out_stride + j],
                    want[i * n + j],
                    &format!("{ctx} [{i}][{j}]"),
                );
            }
        }
    }

    /// The VNNI interleave, checked against the identity it exists to satisfy.
    ///
    /// This is the one part of the GEMM with no cheap sanity signal: a wrong
    /// permutation still produces plausible-looking finite numbers, and on a
    /// host without AMX nothing else here can run at all. Asserting the
    /// element-for-element mapping keeps the layout pinned everywhere.
    #[cfg(all(
        kernel_support = "amx_fp16",
        target_arch = "x86_64",
        target_os = "linux"
    ))]
    #[test]
    fn packed_centroid_layout_matches_tile_operand_order() {
        use crate::simd::amx_fp16::{pack_centroids_vnni, packed_centroids_len};

        // Reused across shapes to also pin that packing clears rather than
        // appends — a stale prefix would silently offset every B tile.
        let mut packed = Vec::new();
        let mut rng = StdRng::seed_from_u64(0x9EC7);
        for (n, dim) in [(16usize, 32usize), (32, 64), (48, 100), (32, 31), (32, 768)] {
            let centroids: Vec<f16> = (0..n * dim)
                .map(|_| f16::from_f32(rng.random_range(-1.0f32..1.0)))
                .collect();
            pack_centroids_vnni(&centroids, n, dim, &mut packed);
            assert_eq!(
                packed.len(),
                packed_centroids_len(n, dim),
                "n={n} dim={dim}"
            );

            for kb in 0..dim / 32 {
                for jb in 0..n / 16 {
                    for k in 0..16 {
                        for nn in 0..16 {
                            for p in 0..2 {
                                let at = ((kb * (n / 16)) + jb) * 512 + k * 32 + nn * 2 + p;
                                let from = (jb * 16 + nn) * dim + kb * 32 + 2 * k + p;
                                assert_eq!(
                                    packed[at], centroids[from],
                                    "n={n} dim={dim} kb={kb} jb={jb} k={k} nn={nn} p={p}"
                                );
                            }
                        }
                    }
                }
            }
        }
    }

    /// GEMM results against the f32 reference across the shapes that exercise
    /// each loop boundary: one vs. several 32-row/32-column register blocks,
    /// one vs. many k-passes, dims with and without a scalar tail, dims too
    /// short for any tile pass at all, and a case with padded row strides on
    /// both the input and the output.
    ///
    /// Dims below 32 matter disproportionately: the kernel skips the tile loop
    /// entirely, so `out` is never written by a tile store and the scalar tail
    /// has to zero it first. Accumulating onto uninitialized memory instead
    /// would still look plausible on a freshly-allocated buffer.
    #[cfg(all(
        kernel_support = "amx_fp16",
        target_arch = "x86_64",
        target_os = "linux"
    ))]
    #[test]
    fn gemm_matches_reference() {
        if !crate::simd::amx_fp16::amx_supported() {
            return;
        }
        let mut rng = StdRng::seed_from_u64(0x6E33);
        for &m in &[32usize, 64] {
            for &n in &[32usize, 64] {
                for &dim in &[1usize, 16, 31, 32, 33, 64, 100, 768, 1000, 1536] {
                    let (data, centroids) = make_gemm(m, n, dim, &mut rng);
                    let want = ref_gemm(&data, m, dim, &centroids, n, dim);
                    let got = run_gemm(&data, m, dim, &centroids, n, dim, n);
                    assert_gemm_close(&got, &want, m, n, n, &format!("gemm m={m} n={n} dim={dim}"));
                }
            }
        }

        // Padded strides: the kernel must address rows by the caller's stride,
        // not by `dim` / `n`, so it can score a window of a larger buffer.
        let (m, n, dim) = (64usize, 32usize, 100usize);
        let (data_stride, out_stride) = (dim + 7, n + 5);
        let mut data: Vec<f16> = vec![f16::from_f32(f32::MAX); m * data_stride];
        let (tight, centroids) = make_gemm(m, n, dim, &mut rng);
        for i in 0..m {
            data[i * data_stride..i * data_stride + dim]
                .copy_from_slice(&tight[i * dim..(i + 1) * dim]);
        }
        let want = ref_gemm(&data, m, data_stride, &centroids, n, dim);
        let got = run_gemm(&data, m, data_stride, &centroids, n, dim, out_stride);
        assert_gemm_close(&got, &want, m, n, out_stride, "gemm padded strides");
    }

    /// The safe wrapper, over centroid counts that do and do not divide 32.
    ///
    /// Padding is the part a caller cannot see and must still reason about: the
    /// real centroids have to score exactly as they would unpadded, and the
    /// columns beyond them have to be the zeros that make a caller's argmin
    /// bound (`< n`) load-bearing rather than decorative.
    #[test]
    fn packed_centroids_pad_to_the_kernel_block() {
        let mut rng = StdRng::seed_from_u64(0x9AD);
        for (n, dim) in [(32usize, 64usize), (100, 100), (48, 768)] {
            let m = 64;
            let (data, centroids) = make_gemm(m, n, dim, &mut rng);
            let Some(packed) = PackedCentroidsF16::new(&centroids, n, dim) else {
                return; // no AMX-FP16 on this build or host
            };
            let n_padded = packed.num_centroids_padded();
            assert_eq!(n_padded, n.next_multiple_of(32), "n={n}");

            // A wider output stride than the padded width, so a row's tail is
            // untouched memory rather than the next row's scores.
            let out_stride = n_padded + 3;
            let mut out = vec![f32::NAN; m * out_stride];
            packed.score(&data, m, dim, &mut out, out_stride);

            let want = ref_gemm(&data, m, dim, &centroids, n, dim);
            assert_gemm_close(&out, &want, m, n, out_stride, &format!("packed n={n}"));
            for i in 0..m {
                for j in n..n_padded {
                    assert_eq!(out[i * out_stride + j], 0.0, "padding n={n} [{i}][{j}]");
                }
            }
        }
    }

    /// The GEMM kernel's tile shape, pinned byte for byte, for the same reason
    /// as [`search_tile_config_image_is_pinned`].
    #[cfg(all(
        kernel_support = "amx_fp16",
        target_arch = "x86_64",
        target_os = "linux"
    ))]
    #[test]
    fn gemm_tile_config_image_is_pinned() {
        use crate::simd::amx_fp16::{AMX_CFG_GEMM, amx_supported, tilecfg_image};

        if !amx_supported() {
            return;
        }

        const COLSB: usize = 16;
        const ROWS: usize = 48;

        let mut want = [0u8; 64];
        want[0] = 1; // palette_id
        // All eight tiles at the architectural maximum 16 x 64 B: four fp32
        // accumulators, two A panels, two VNNI-packed B panels.
        for tmm in 0..8usize {
            want[COLSB + tmm * 2..COLSB + tmm * 2 + 2].copy_from_slice(&64u16.to_le_bytes());
            want[ROWS + tmm] = 16;
        }

        let got = tilecfg_image(AMX_CFG_GEMM).expect("gemm config kind must be known");
        assert_eq!(got, want, "gemm tile configuration changed");
    }

    /// Alternate the two kernels on one thread and check both stay correct.
    ///
    /// They ask for incompatible tile shapes (7 tiles, four of them 16x4, vs. 8
    /// tiles all 16x64), so each call has to install its own shape over the one
    /// the previous call left. That is what this pins: a kernel running against
    /// the other one's tile shape reads garbage or #UDs, and only alternating
    /// the two shapes can expose it. It says nothing about how the configuration
    /// got there — `kernel_reconfigures_after_foreign_tile_release` is the test
    /// that pins the reload itself.
    #[cfg(all(
        kernel_support = "amx_fp16",
        target_arch = "x86_64",
        target_os = "linux"
    ))]
    #[test]
    fn interleaved_search_and_gemm_stay_correct() {
        if !crate::simd::amx_fp16::amx_supported() {
            return;
        }
        let mut rng = StdRng::seed_from_u64(0x11E12EA5);
        let (m, n, dim) = (32usize, 32usize, 96usize);
        for round in 0..20 {
            let (query, cands) = make_batch(dim, &mut rng);
            let candidates: [&[f16]; 16] = std::array::from_fn(|i| cands[i].as_slice());
            let batch =
                unsafe { crate::simd::amx_fp16::dot_f16_batch_16_amx(&query, &candidates, 16) };
            for i in 0..16 {
                assert_close(
                    batch[i],
                    ref_dot_f32(&query, &cands[i]),
                    &format!("interleaved batch round={round} i={i}"),
                );
            }

            let (data, centroids) = make_gemm(m, n, dim, &mut rng);
            let want = ref_gemm(&data, m, dim, &centroids, n, dim);
            let got = run_gemm(&data, m, dim, &centroids, n, dim, n);
            assert_gemm_close(
                &got,
                &want,
                m,
                n,
                n,
                &format!("interleaved gemm round={round}"),
            );
        }
    }

    /// Both kernels running concurrently on different threads.
    ///
    /// LDTILECFG is per-logical-processor state, so correctness here rests on
    /// each call building its configuration image on its own stack. A shared or
    /// `static` image would let one thread's shape reach another's tile ops,
    /// which this catches and a single-threaded test cannot.
    #[cfg(all(
        kernel_support = "amx_fp16",
        target_arch = "x86_64",
        target_os = "linux"
    ))]
    #[test]
    fn concurrent_search_and_gemm_stay_correct() {
        if !crate::simd::amx_fp16::amx_supported() {
            return;
        }
        const THREADS: u64 = 8;
        std::thread::scope(|scope| {
            for t in 0..THREADS {
                scope.spawn(move || {
                    let mut rng = StdRng::seed_from_u64(0xC0FFEE + t);
                    let dim = 128;
                    for round in 0..25 {
                        if t % 2 == 0 {
                            let (query, cands) = make_batch(dim, &mut rng);
                            let candidates: [&[f16]; 16] =
                                std::array::from_fn(|i| cands[i].as_slice());
                            let batch = unsafe {
                                crate::simd::amx_fp16::dot_f16_batch_16_amx(&query, &candidates, 16)
                            };
                            for i in 0..16 {
                                assert_close(
                                    batch[i],
                                    ref_dot_f32(&query, &cands[i]),
                                    &format!("concurrent batch t={t} round={round} i={i}"),
                                );
                            }
                        } else {
                            let (m, n) = (32usize, 32usize);
                            let (data, centroids) = make_gemm(m, n, dim, &mut rng);
                            let want = ref_gemm(&data, m, dim, &centroids, n, dim);
                            let got = run_gemm(&data, m, dim, &centroids, n, dim, n);
                            assert_gemm_close(
                                &got,
                                &want,
                                m,
                                n,
                                n,
                                &format!("concurrent gemm t={t} round={round}"),
                            );
                        }
                    }
                });
            }
        });
    }

    /// The two costs a membership-level benchmark cannot separate: packing the
    /// centroids, which happens once per call and is amortized over every block
    /// of vectors, and the GEMM's throughput as a function of the block height
    /// `m` its caller chooses.
    ///
    /// `m` sets how much f32 scratch one block writes (`m * n_padded * 4` bytes,
    /// reported per row), which is what decides whether the reduction that
    /// follows reads it out of L2 or out of memory. A caller sizing its blocks
    /// from a scratch budget is making exactly this trade, so sweeping `m` here
    /// says whether it lands on the right side of it. No knob is needed in the
    /// production path for that — `score` takes `m` directly.
    ///
    /// `#[ignore]` -- run:
    ///   cargo test -p lance-linalg --release \
    ///     packed_centroids_gemm_shape_bench -- --ignored --nocapture
    /// Tune with `BENCH_DIMS` / `BENCH_KS` (comma-separated) and `BENCH_SECONDS`
    /// (the wall-clock budget each measured point gets).
    #[test]
    #[ignore]
    #[allow(clippy::print_stderr)]
    // Without the kernel `PackedCentroidsF16` is uninhabited, so the first
    // `expect` below has type `!` and everything after it is provably dead. That
    // is the property the type is designed to have; it is not a sign the bench is
    // wrong.
    #[allow(unreachable_code, unused_variables)]
    fn packed_centroids_gemm_shape_bench() {
        use std::time::{Duration, Instant};

        // Multiples of 32 (the kernel's `m` granularity) spanning scratch that
        // fits a private L2 up to scratch that cannot.
        const BLOCK_ROWS: &[usize] = &[32, 64, 128, 256, 512, 1024, 2048];

        if !amx_fp16_supported() {
            eprintln!("[gemm_shape_bench] skipped: amx_fp16_supported=false on this build or host");
            return;
        }

        let env_list = |key: &str, default: &[usize]| -> Vec<usize> {
            std::env::var(key)
                .ok()
                .map(|s| s.split(',').filter_map(|t| t.trim().parse().ok()).collect())
                .unwrap_or_else(|| default.to_vec())
        };
        let dims = env_list("BENCH_DIMS", &[768]);
        let ks = env_list("BENCH_KS", &[256, 4096]);
        let budget = Duration::from_secs_f64(
            std::env::var("BENCH_SECONDS")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(3.0),
        );

        let mut rng = StdRng::seed_from_u64(0x6E33);
        let mut random_f16 = |count: usize| -> Vec<f16> {
            (0..count)
                .map(|_| f16::from_f32(rng.random_range(-1.0f32..1.0)))
                .collect()
        };

        eprintln!(
            "[gemm_shape_bench] budget={:.1}s amx_fp16_supported=true",
            budget.as_secs_f64()
        );
        for &dim in &dims {
            for &k in &ks {
                let centroids = random_f16(k * dim);

                let mut packs = 0usize;
                let t0 = Instant::now();
                while t0.elapsed() < budget {
                    let packed = PackedCentroidsF16::new(&centroids, k, dim);
                    std::hint::black_box(&packed);
                    packs += 1;
                }
                let pack_us = t0.elapsed().as_secs_f64() * 1e6 / packs as f64;

                let packed = PackedCentroidsF16::new(&centroids, k, dim)
                    .expect("availability was just checked");
                let n_padded = packed.num_centroids_padded();
                eprintln!(
                    "[gemm_shape_bench] dim={dim} k={k} n_padded={n_padded} pack_calls={packs} pack_us={pack_us:.1}"
                );

                let mut best_vec_per_s = 0f64;
                for &m in BLOCK_ROWS {
                    let data = random_f16(m * dim);
                    let mut out = vec![0f32; m * n_padded];
                    packed.score(&data, m, dim, &mut out, n_padded); // untimed warm-up

                    let t1 = Instant::now();
                    let mut iters = 0usize;
                    while t1.elapsed() < budget {
                        packed.score(&data, m, dim, &mut out, n_padded);
                        iters += 1;
                    }
                    let elapsed = t1.elapsed().as_secs_f64();
                    std::hint::black_box(&out);

                    let vec_per_s = (iters * m) as f64 / elapsed;
                    best_vec_per_s = best_vec_per_s.max(vec_per_s);
                    eprintln!(
                        // Pairs count the padding columns, since the kernel does
                        // the work either way — this is its true rate, not the
                        // caller's useful fraction of it.
                        "[gemm_shape_bench]   m={m:>5} scratch_kb={:>7} iters={iters:>8} vec_per_s={vec_per_s:>12.0} us_per_vec={:>8.4} Gpair_per_s={:>8.2}",
                        m * n_padded * 4 / 1024,
                        1e6 / vec_per_s,
                        vec_per_s * n_padded as f64 / 1e9,
                    );
                }
                eprintln!(
                    "[gemm_shape_bench]   pack_us={pack_us:.1} buys {:.0} vectors of scoring at the best m: packing is amortized above that",
                    pack_us * 1e-6 * best_vec_per_s,
                );
            }
        }
    }
}
