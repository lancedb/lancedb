// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use num_traits::AsPrimitive;
use rand::RngCore;

// Fast random rotation used by the RabitQ "fast" path.
//
// The transform is a composition of:
// 1) random diagonal sign flips (Rademacher variables),
// 2) FWHT-style mixing on a power-of-two window,
// 3) a Kac-style pairwise mixing step for non-power-of-two dimensions.
//
// Background:
// - Hadamard transform: https://en.wikipedia.org/wiki/Hadamard_transform
// - Fast Walsh-Hadamard transform (FWHT):
//   https://en.wikipedia.org/wiki/Fast_Walsh%E2%80%93Hadamard_transform
// - Rademacher random signs:
//   https://en.wikipedia.org/wiki/Rademacher_distribution
// - Kac-walk-based fast dimension reduction (uses fixed-angle pair rotations):
//   https://arxiv.org/abs/2003.10069
// - Givens / plane rotation:
//   https://en.wikipedia.org/wiki/Givens_rotation
const FAST_ROTATION_ROUNDS: usize = 4;

#[inline]
fn fwht_in_place(values: &mut [f32]) {
    // In-place FWHT butterfly network.
    // For each stage, pair entries (x, y) and map to (x + y, x - y).
    // Complexity: O(n log n) operations, no extra heap allocation.
    debug_assert!(values.len().is_power_of_two());
    let mut half = 1usize;
    while half < values.len() {
        let step = half * 2;
        for block in values.chunks_exact_mut(step) {
            let (left, right) = block.split_at_mut(half);
            for (x, y) in left.iter_mut().zip(right.iter_mut()) {
                let lx = *x;
                let ry = *y;
                *x = lx + ry;
                *y = lx - ry;
            }
        }
        half = step;
    }
}

#[inline]
fn flip_signs_scalar(values: &mut [f32], signs: &[u8]) {
    // Apply a random diagonal matrix with +/-1 entries by toggling the f32 sign bit.
    // One bit in `signs` controls one element in `values`.
    for (byte_idx, &mask) in signs.iter().enumerate() {
        let start = byte_idx * 8;
        if start >= values.len() {
            break;
        }
        let end = (start + 8).min(values.len());
        for (bit_idx, value) in values[start..end].iter_mut().enumerate() {
            let sign_mask = (((mask >> bit_idx) & 1) as u32) << 31;
            *value = f32::from_bits(value.to_bits() ^ sign_mask);
        }
    }
}

#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
#[target_feature(enable = "avx2")]
unsafe fn flip_signs_avx2(values: &mut [f32], signs: &[u8]) {
    #[cfg(target_arch = "x86")]
    use std::arch::x86::*;
    #[cfg(target_arch = "x86_64")]
    use std::arch::x86_64::*;

    // Vectorized variant of `flip_signs_scalar`: consume 8 f32 values per AVX2 lane.
    // The sign mask is expanded from one byte to 8 lane-wise sign-bit masks.
    let full_chunks = values.len() / 8;
    let bit_select = _mm256_setr_epi32(0x01, 0x02, 0x04, 0x08, 0x10, 0x20, 0x40, 0x80);
    let sign_flip = _mm256_set1_epi32(0x80000000u32 as i32);

    for (chunk_idx, &mask) in signs.iter().take(full_chunks).enumerate() {
        let mask = mask as i32;
        let mask_bits = _mm256_set1_epi32(mask);
        let test = _mm256_and_si256(mask_bits, bit_select);
        let cmp = _mm256_cmpeq_epi32(test, bit_select);
        let xor_mask = _mm256_and_si256(cmp, sign_flip);

        let ptr = unsafe { values.as_mut_ptr().add(chunk_idx * 8) };
        let vec = unsafe { _mm256_loadu_ps(ptr) };
        let out = _mm256_xor_ps(vec, _mm256_castsi256_ps(xor_mask));
        unsafe { _mm256_storeu_ps(ptr, out) };
    }

    if full_chunks * 8 < values.len() {
        flip_signs_scalar(&mut values[full_chunks * 8..], &signs[full_chunks..]);
    }
}

#[inline]
fn flip_signs(values: &mut [f32], signs: &[u8]) {
    debug_assert!(signs.len() * 8 >= values.len());
    #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
    {
        if std::arch::is_x86_feature_detected!("avx2") {
            // SAFETY: guarded by runtime feature detection.
            unsafe {
                flip_signs_avx2(values, signs);
            }
            return;
        }
    }
    flip_signs_scalar(values, signs);
}

#[inline]
fn kacs_walk(values: &mut [f32]) {
    // A fixed-angle (pi/4) plane-rotation-like sweep over paired coordinates:
    // (x, y) -> (x + y, x - y). Up to normalization, this is a 2x2 Hadamard block
    // and corresponds to one Kac-style mixing step.
    let half = values.len() / 2;
    let (left, right) = values.split_at_mut(half);
    for (x, y) in left.iter_mut().zip(right.iter_mut()) {
        let lx = *x;
        let ry = *y;
        *x = lx + ry;
        *y = lx - ry;
    }
}

#[inline]
fn rescale(values: &mut [f32], factor: f32) {
    // Keep the transform numerically stable and approximately orthonormal.
    for value in values.iter_mut() {
        *value *= factor;
    }
}

#[inline]
fn sign_bytes_per_round(dim: usize) -> usize {
    dim.div_ceil(8)
}

pub(crate) fn fast_rotation_signs_len(dim: usize) -> usize {
    FAST_ROTATION_ROUNDS * sign_bytes_per_round(dim)
}

pub fn random_fast_rotation_signs(dim: usize) -> Vec<u8> {
    // Each round needs one random sign bit per dimension.
    let mut signs = vec![0u8; fast_rotation_signs_len(dim)];
    rand::rng().fill_bytes(&mut signs);
    signs
}

#[inline]
pub fn apply_fast_rotation<T: AsPrimitive<f32>>(input: &[T], output: &mut [f32], signs: &[u8]) {
    // Fast random rotation pipeline, aligned with RaBitQ-Library's FhtKacRotator:
    // - power-of-two dims: repeat [random signs -> FWHT -> scale] for 4 rounds
    // - non-power-of-two dims: alternate FWHT on head/tail + Kac mixing
    //
    // This keeps the fast path matrix-free: no dense orthogonal matrix materialization.
    let dim = output.len();
    let input_len = input.len().min(dim);
    output[..input_len]
        .iter_mut()
        .zip(input[..input_len].iter())
        .for_each(|(dst, src)| *dst = src.as_());
    if input_len < dim {
        output[input_len..].fill(0.0);
    }

    apply_fast_rotation_in_place(output, signs);
}

#[inline]
pub fn apply_fast_rotation_in_place(output: &mut [f32], signs: &[u8]) {
    let dim = output.len();
    let bytes_per_round = sign_bytes_per_round(dim);
    debug_assert_eq!(signs.len(), FAST_ROTATION_ROUNDS * bytes_per_round);
    if dim == 0 {
        return;
    }

    let trunc_dim = 1usize << dim.ilog2();
    let scale = 1.0f32 / (trunc_dim as f32).sqrt();
    if trunc_dim == dim {
        for round in 0..FAST_ROTATION_ROUNDS {
            let offset = round * bytes_per_round;
            flip_signs(output, &signs[offset..offset + bytes_per_round]);
            fwht_in_place(output);
            rescale(output, scale);
        }
        return;
    }

    let start = dim - trunc_dim;
    for round in 0..FAST_ROTATION_ROUNDS {
        let offset = round * bytes_per_round;
        flip_signs(output, &signs[offset..offset + bytes_per_round]);

        if round % 2 == 0 {
            let head = &mut output[..trunc_dim];
            fwht_in_place(head);
            rescale(head, scale);
        } else {
            let tail = &mut output[start..];
            fwht_in_place(tail);
            rescale(tail, scale);
        }

        kacs_walk(output);
    }

    // Matches RaBitQ-Library FhtKacRotator behavior for non-power-of-two dimensions.
    // The extra factor compensates the alternating truncated FWHT + Kac steps above.
    rescale(output, 0.25);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fast_rotation_sign_bytes() {
        assert_eq!(random_fast_rotation_signs(128).len(), 64);
        assert_eq!(random_fast_rotation_signs(130).len(), 68);
    }

    #[test]
    fn test_fast_rotation_preserves_shape() {
        let input = vec![1.0f32; 129];
        let mut output = vec![0.0f32; 129];
        let signs = random_fast_rotation_signs(129);
        apply_fast_rotation(&input, &mut output, &signs);
        assert_eq!(output.len(), 129);
    }
}
