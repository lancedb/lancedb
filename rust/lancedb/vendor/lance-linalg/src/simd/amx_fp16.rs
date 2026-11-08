// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! AMX-FP16 accelerated f16 x f16 dot products.
//!
//! Two shapes: `dot_f16_batch_16_amx` (one query against 16 candidates, for
//! choosing the partitions a query probes) and `dot_f16_gemm_amx` (an m x n
//! GEMM, for scoring many vectors against many centroids). Both are named in
//! plain code spans rather than intra-doc links: they are
//! `kernel_support = "amx_fp16"`-gated, so a link from these unconditional
//! module docs is unresolved — and hence a rustdoc error under `-D warnings` —
//! on any build without the kernel.
//!
//! The tile math lives in `amx_fp16.c` (compiled by `build.rs` with a compiler
//! new enough for `-mamx-fp16`, which sets `kernel_support = "amx_fp16"`). This
//! module holds the FFI declarations, the B-operand packing the GEMM's tile
//! layout requires, and the runtime safety gate. Everything here is
//! crate-internal; the safe public entry points are
//! [`crate::distance::dot_f16::dot_f16_batch_16`] for the batch-16 shape and
//! `PackedCentroidsF16` for the GEMM (named, not linked, for the same reason as
//! the kernels above). The latter owns what this layer deliberately does not:
//! padding `n` up to a multiple of 32, holding the packed B operand across
//! calls, and turning the absence of AMX into an `Option` its caller — k-means
//! assignment, in another crate that cannot see `kernel_support` — can branch
//! on. Every kernel here is guarded by
//! [`crate::distance::dot_f16::amx_fp16_supported`], which is also what callers
//! consult before routing work here: one gate, decided by run-time capability
//! alone.
//!
//! ## Safety gate
//!
//! A set CPUID bit is not sufficient to run AMX tile instructions on Linux:
//! the OS must first grant the extended (XTILEDATA)
//! state via `arch_prctl(ARCH_REQ_XCOMP_PERM, XFEATURE_XTILEDATA)`; skipping
//! that SIGILLs the first tile instruction. `amx_supported` requires, and
//! caches process-wide, all of:
//!   1. `target_arch = "x86_64"` and `target_os = "linux"` (compile-time cfg),
//!   2. the amx-tile CPUID bit (leaf 7, sub-leaf 0, EDX bit 24) **and** the
//!      amx-fp16 CPUID bit (leaf 7, sub-leaf 1, EAX bit 21),
//!   3. a successful one-time `arch_prctl` permission request.
//!
//! On any failure the caller falls back to the existing AVX-512-FP16 / scalar
//! `f16::dot` path, so results are unchanged (both accumulate in f32; only the
//! summation order — hence fp16-level rounding — differs).
//!
//! ## XTILEDATA is a shared AMX state permission
//!
//! XTILEDATA (component 18) is the single dynamically-enabled XSAVE state
//! backing the physical TMM tile registers; it is requested per-*state*, not
//! per-*instruction*, so one grant covers every AMX compute instruction — see
//! Linux `Documentation/arch/x86/xstate.rst` ("Dynamically Enabled XSAVE
//! Features", AMX example) and Intel SDM Vol.1 §13.3. The syscall is
//! idempotent, so requesting an already-granted permission again is harmless.

#[cfg(all(
    kernel_support = "amx_fp16",
    target_arch = "x86_64",
    target_os = "linux"
))]
use crate::distance::dot_f16::strided_len;
#[cfg(all(
    kernel_support = "amx_fp16",
    target_arch = "x86_64",
    target_os = "linux"
))]
use half::f16;

#[cfg(all(
    kernel_support = "amx_fp16",
    target_arch = "x86_64",
    target_os = "linux"
))]
unsafe extern "C" {
    /// arch_prctl(ARCH_REQ_XCOMP_PERM, XFEATURE_XTILEDATA); 0 on success.
    fn lance_amx_fp16_request_perm() -> i32;

    /// out[i] = sum_d f32(query[d]) * f32(candidates[i][d]), i in 0..count;
    /// out[count..16] = 0. `query` is `dim` IEEE binary16 bit patterns;
    /// `candidates` is 16 pointers to `dim` of them each, of which only the
    /// first `count` (1..=16) are read; `out` holds 16 f32. The kernel gathers
    /// the rows itself, k-block by k-block. See `amx_fp16.c`.
    fn lance_amx_dot_f16_batch_16(
        query: *const u16,
        candidates: *const *const u16,
        count: usize,
        dim: usize,
        out: *mut f32,
    );

    /// out[i*out_stride + j] = sum_d f32(data[i*data_stride + d]) *
    /// f32(centroids[j*dim + d]), for i in 0..m, j in 0..n. `packed_b` is
    /// [`pack_centroids_vnni`]'s output for the same centroids; `centroids`
    /// itself is read only for the `dim % 32` unpacked tail dims. `m` and `n`
    /// must both be multiples of 32. See `amx_fp16.c`.
    fn lance_amx_dot_f16_gemm(
        data: *const u16,
        m: usize,
        data_stride: usize,
        packed_b: *const u16,
        centroids: *const u16,
        n: usize,
        dim: usize,
        out: *mut f32,
        out_stride: usize,
    );

    /// Writes the 64-byte LDTILECFG image for `cfg_kind` into `out` without
    /// loading it; 0 on success, -1 for an unknown kind. See `amx_fp16.c`.
    #[cfg(test)]
    fn lance_amx_tilecfg_image(cfg_kind: i32, out: *mut u8) -> i32;

    /// Retires the tile configuration the way a foreign AMX user sharing this
    /// thread would. See `amx_fp16.c`.
    #[cfg(test)]
    fn lance_amx_tile_clobber_for_test();

    /// Writes this logical processor's live 64-byte tile configuration to `out`
    /// via STTILECFG; `palette_id` (byte 0) is 0 when nothing is configured.
    /// See `amx_fp16.c`.
    #[cfg(test)]
    fn lance_amx_tilecfg_current_for_test(out: *mut u8);
}

/// Test-only: retire this thread's tile configuration the way another AMX user
/// on the same thread would, leaving the tile unit in INIT under a kernel that
/// is about to run.
///
/// # Safety
/// Executes TILERELEASE, so [`amx_supported`] must have returned `true` first.
#[cfg(all(
    test,
    kernel_support = "amx_fp16",
    target_arch = "x86_64",
    target_os = "linux"
))]
pub(crate) unsafe fn clobber_tile_state_for_test() {
    unsafe { lance_amx_tile_clobber_for_test() };
}

/// Test-only: `true` when a tile configuration is currently live on this logical
/// processor, read straight off the hardware with STTILECFG.
///
/// This is what distinguishes a working release path from a deleted one.
/// `lance_amx_tile_ensure` reloads on every kernel entry, so results stay right
/// with `lance_amx_tile_done`'s TILERELEASE gone and no "did it crash?" test
/// notices; what is lost is only the promise made to whoever shares the thread,
/// and that is visible nowhere but here.
#[cfg(all(
    test,
    kernel_support = "amx_fp16",
    target_arch = "x86_64",
    target_os = "linux"
))]
pub(crate) fn tile_config_is_live_for_test() -> bool {
    let mut image = [0u8; 64];
    // SAFETY: the C side writes exactly 64 bytes, and STTILECFG touches no TMM
    // register so it is legal without the XTILEDATA grant.
    unsafe { lance_amx_tilecfg_current_for_test(image.as_mut_ptr()) };
    image[0] != 0
}

/// Config kind for [`tilecfg_image`]: the batch-16 search kernel's tile shape.
/// Must match `LANCE_AMX_CFG_SEARCH` in `amx_fp16.c`.
#[cfg(all(
    test,
    kernel_support = "amx_fp16",
    target_arch = "x86_64",
    target_os = "linux"
))]
pub(crate) const AMX_CFG_SEARCH: i32 = 0;

/// Config kind for [`tilecfg_image`]: the GEMM kernel's tile shape.
/// Must match `LANCE_AMX_CFG_GEMM` in `amx_fp16.c`.
#[cfg(all(
    test,
    kernel_support = "amx_fp16",
    target_arch = "x86_64",
    target_os = "linux"
))]
pub(crate) const AMX_CFG_GEMM: i32 = 1;

/// The 64-byte LDTILECFG image a kernel would configure, without loading it.
/// `None` if `cfg_kind` is not one of the `AMX_CFG_*` constants.
///
/// Exists for the tests: a wrong tile shape never surfaces as a clean error —
/// it is a #UD or silently wrong results — so the shape is pinned directly
/// rather than inferred from kernel output.
#[cfg(all(
    test,
    kernel_support = "amx_fp16",
    target_arch = "x86_64",
    target_os = "linux"
))]
pub(crate) fn tilecfg_image(cfg_kind: i32) -> Option<[u8; 64]> {
    let mut image = [0u8; 64];
    // SAFETY: the C side writes exactly `sizeof(lance_amx_tilecfg)` bytes, which
    // a `_Static_assert` there pins to 64 — the length of `image`.
    let rc = unsafe { lance_amx_tilecfg_image(cfg_kind, image.as_mut_ptr()) };
    (rc == 0).then_some(image)
}

/// True iff AMX-FP16 tile instructions can be executed safely in this process.
/// Evaluated once and cached; the `arch_prctl` permission request (a syscall)
/// happens at most once, process-wide.
///
/// This is a hardware question only — a pure "would a tile instruction fault
/// here?" — so that kernel-level tests can exercise the kernels on any host
/// that can run them.
#[cfg(all(
    kernel_support = "amx_fp16",
    target_arch = "x86_64",
    target_os = "linux"
))]
pub(crate) fn amx_supported() -> bool {
    static SUPPORTED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *SUPPORTED.get_or_init(|| {
        if !detect_amx_fp16() {
            return false;
        }
        // Request XTILEDATA permission; without a 0 return, any tile instruction
        // would SIGILL, so treat anything else as unavailable.
        unsafe { lance_amx_fp16_request_perm() == 0 }
    })
}

/// AMX-TILE = CPUID leaf 7, sub-leaf 0, EDX bit 24. AMX-FP16 = CPUID leaf 7,
/// sub-leaf 1, EAX bit 21. Both required.
#[cfg(all(
    kernel_support = "amx_fp16",
    target_arch = "x86_64",
    target_os = "linux"
))]
fn detect_amx_fp16() -> bool {
    use std::arch::x86_64::__cpuid_count;
    // `__cpuid_count` is safe on nightly but `unsafe` on stable; allow both.
    #[allow(unused_unsafe)]
    let leaf7_0 = unsafe { __cpuid_count(7, 0) };
    let amx_tile = (leaf7_0.edx & (1 << 24)) != 0;
    #[allow(unused_unsafe)]
    let leaf7_1 = unsafe { __cpuid_count(7, 1) };
    let amx_fp16 = (leaf7_1.eax & (1 << 21)) != 0;
    amx_tile && amx_fp16
}

/// Batched AMX-FP16 dot product: one query against the first `len` of 16
/// candidates. Returns the 16 raw dot products (`Σ query·candidate`, no `1.0 -`
/// distance wrapping); lanes `len..16` are 0.
///
/// Hands the kernel 16 pointers rather than a packed `16 x dim` buffer. The
/// candidates still have to be brought together for a tile load, but the kernel
/// does it one k-block at a time into 3 KB of stack, which overlaps the copies
/// with the tile ops; packing all `16 * dim * 2` bytes here first could not
/// overlap with anything, and at dim 1024 that is 32 KB against a 48 KB L1D.
/// See the kernel comment in `amx_fp16.c` for the measurements behind this.
///
/// `len` is what keeps a partial batch cheap: the tile pass is a fixed cost for
/// 16 lanes either way, but only `len` rows are gathered.
///
/// # Safety
/// `amx_supported` must have returned `true`. `len` must be in `1..=16` — the
/// kernel does not clamp it, and a larger value walks its staging buffer off the
/// end; [`crate::distance::dot_f16::dot_f16_batch_16`] is where that is
/// rejected. Every candidate slice must have length `query.len()`, and must
/// outlive the call.
#[cfg(all(
    kernel_support = "amx_fp16",
    target_arch = "x86_64",
    target_os = "linux"
))]
pub(crate) unsafe fn dot_f16_batch_16_amx(
    query: &[f16],
    candidates: &[&[f16]; 16],
    len: usize,
) -> [f32; 16] {
    debug_assert!((1..=16).contains(&len), "len ({len}) must be in 1..=16");
    let dim = query.len();
    // half::f16 is #[repr(transparent)] over u16, so the casts below reinterpret
    // the identical IEEE binary16 bit patterns the kernel expects. All 16 slots
    // are filled even though the kernel reads only `len` of them: the caller
    // already holds 16 valid slices, so there is nothing to gain from leaving
    // the tail of the array undefined.
    let mut rows = [std::ptr::null::<u16>(); 16];
    for (i, cand) in candidates.iter().enumerate() {
        debug_assert_eq!(
            cand.len(),
            dim,
            "candidate {i} length must equal query length"
        );
        rows[i] = cand.as_ptr() as *const u16;
    }
    let mut out = [0f32; 16];
    unsafe {
        lance_amx_dot_f16_batch_16(
            query.as_ptr() as *const u16,
            rows.as_ptr(),
            len,
            dim,
            out.as_mut_ptr(),
        );
    }
    out
}

/// Halfwords in one packed B block: 16 tile rows x 32 halfwords per row.
/// Mirrors `GEMM_B_BLOCK` in `amx_fp16.c`.
#[cfg(all(
    kernel_support = "amx_fp16",
    target_arch = "x86_64",
    target_os = "linux"
))]
const GEMM_B_BLOCK: usize = 512;

/// Number of `f16` [`pack_centroids_vnni`] writes for `n` centroids of `dim`
/// dims. Only whole 32-dim k-blocks are packed; the `dim % 32` tail is left to
/// the kernel's scalar cleanup, which reads the unpacked centroids directly.
#[cfg(all(
    kernel_support = "amx_fp16",
    target_arch = "x86_64",
    target_os = "linux"
))]
pub(crate) fn packed_centroids_len(n: usize, dim: usize) -> usize {
    (dim / 32) * (n / 16) * GEMM_B_BLOCK
}

/// Interleave `[n, dim]` row-major `centroids` into the VNNI order
/// [`dot_f16_gemm_amx`]'s B tiles are loaded in, replacing `out`'s contents.
///
/// The layout is dictated by TDPFP16PS, which reads its B operand as
/// `b.row[k].fp16[2*nn + p]` and pairs it with `a.row[mm].fp16[2*k + p]`. Since
/// A is loaded straight out of the vector buffer — `a.row[mm].fp16[2*k+p]` is
/// dim `kb*32 + 2*k + p` of vector `mm` — B must satisfy
///
/// ```text
/// out[((kb * (n/16)) + jb) * 512 + k*32 + nn*2 + p]
///     == centroids[(jb*16 + nn) * dim + kb*32 + 2*k + p]
/// ```
///
/// with `kb` the 32-dim k-block and `jb` the 16-centroid block. `jb` is the
/// inner index so that the two B tiles a single k-pass consumes are adjacent.
///
/// `n` must be a multiple of 16 (one B tile covers exactly 16 centroids);
/// `centroids` must hold `n * dim` values.
#[cfg(all(
    kernel_support = "amx_fp16",
    target_arch = "x86_64",
    target_os = "linux"
))]
pub(crate) fn pack_centroids_vnni(centroids: &[f16], n: usize, dim: usize, out: &mut Vec<f16>) {
    debug_assert_eq!(n % 16, 0, "n ({n}) must be a multiple of 16");
    debug_assert_eq!(
        centroids.len(),
        n * dim,
        "centroids must hold n*dim = {} values",
        n * dim
    );
    out.clear();
    out.reserve(packed_centroids_len(n, dim));
    for kb in 0..dim / 32 {
        for jb in 0..n / 16 {
            for k in 0..16 {
                for nn in 0..16 {
                    for p in 0..2 {
                        out.push(centroids[(jb * 16 + nn) * dim + kb * 32 + 2 * k + p]);
                    }
                }
            }
        }
    }
}

/// AMX-FP16 `[m, dim] x [n, dim]^T -> [m, n]` dot-product GEMM: scores every
/// row of `data` against every centroid, writing raw dot products (no distance
/// wrapping) into `out`.
///
/// `packed_b` must come from [`pack_centroids_vnni`] over the same `centroids`,
/// `n` and `dim`; `centroids` is additionally passed through because the
/// `dim % 32` tail dims are not packed and the kernel finishes them in scalar
/// fp32. Rows of `data` are `data_stride` halfwords apart and rows of `out` are
/// `out_stride` floats apart, so a caller can hand over a window of a larger
/// buffer without copying.
///
/// Accuracy matches [`dot_f16_batch_16_amx`]'s contract: f32-accumulated to
/// within fp16 precision, not bit-exact against a sequential scalar loop.
///
/// # Safety
/// * [`crate::distance::dot_f16::amx_fp16_supported`] must have returned `true`.
/// * `m % 32 == 0` and `n % 32 == 0`. The kernel has no edge-case path for
///   partial tiles and would read and write past the ends of its buffers.
/// * `data_stride >= dim` and `out_stride >= n`, and the slices must be long
///   enough for the last row those strides reach.
#[cfg(all(
    kernel_support = "amx_fp16",
    target_arch = "x86_64",
    target_os = "linux"
))]
#[allow(clippy::too_many_arguments)]
pub(crate) unsafe fn dot_f16_gemm_amx(
    data: &[f16],
    m: usize,
    data_stride: usize,
    packed_b: &[f16],
    centroids: &[f16],
    n: usize,
    dim: usize,
    out: &mut [f32],
    out_stride: usize,
) {
    debug_assert_eq!(m % 32, 0, "m ({m}) must be a multiple of 32");
    debug_assert_eq!(n % 32, 0, "n ({n}) must be a multiple of 32");
    debug_assert!(
        data_stride >= dim,
        "data_stride ({data_stride}) < dim ({dim})"
    );
    debug_assert!(out_stride >= n, "out_stride ({out_stride}) < n ({n})");
    // Through `strided_len` rather than inline arithmetic, for the same reason
    // the safe caller uses it: `(m - 1) * stride + row_len` wraps in release
    // builds, and a wrapped requirement is small enough to satisfy the very
    // check it was computed for. These are `debug_assert`s restating a contract
    // the caller already enforced, but they should fail loudly on the
    // overflowing shape rather than quietly agree with it.
    debug_assert!(
        strided_len(m, data_stride, dim).is_some_and(|need| data.len() >= need),
        "data too short"
    );
    debug_assert!(
        strided_len(m, out_stride, n).is_some_and(|need| out.len() >= need),
        "out too short"
    );
    debug_assert_eq!(
        Some(centroids.len()),
        n.checked_mul(dim),
        "centroids must hold n*dim values"
    );
    debug_assert_eq!(
        packed_b.len(),
        packed_centroids_len(n, dim),
        "packed_b must be pack_centroids_vnni's output for this n and dim"
    );
    // half::f16 is #[repr(transparent)] over u16, so these casts reinterpret the
    // identical IEEE binary16 bit patterns the kernel expects.
    unsafe {
        lance_amx_dot_f16_gemm(
            data.as_ptr() as *const u16,
            m,
            data_stride,
            packed_b.as_ptr() as *const u16,
            centroids.as_ptr() as *const u16,
            n,
            dim,
            out.as_mut_ptr(),
            out_stride,
        );
    }
}
