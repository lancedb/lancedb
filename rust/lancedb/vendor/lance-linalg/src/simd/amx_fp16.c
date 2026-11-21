// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

// AMX-FP16 tile kernels, and the tile-configuration plumbing they share.
//
// Every kernel here computes fp16 x fp16 dot products with TDPFP16PS
// (fp16 x fp16 -> fp32 accumulate). The tile *shapes* differ per kernel and are
// declared next to each one; the code that turns a shape into a loaded
// LDTILECFG image is shared, because the one subtle step in it (the
// dead-store barrier below) fails silently and only on some compilers, so it
// must exist exactly once.
//
// Two kernels:
//   * `lance_amx_dot_f16_batch_16` -- one query against 16 candidates, for
//     choosing the IVF partitions a query probes (16 centroids per call).
//     Ported in spirit from FAISS PR
//     facebookresearch/faiss#5235's AMX-BF16 kernel: fp16 and bf16 are both
//     2-byte tile elements consumed by the same `_tile_dp*ps` shape, so only the
//     instruction (`_tile_dpbf16ps` -> `_tile_dpfp16ps`) and the element ->
//     float conversion (bf16 bit-shift -> real IEEE fp16 via F16C `_cvtsh_ss`)
//     differ. Its C tile is fed by three independent (A, B) tile pairs so three
//     TDPFP16PS can be in flight at once; see the tile roles below.
//   * `lance_amx_dot_f16_gemm` -- an m x n GEMM, for scoring many vectors
//     against many centroids at once (k-means assignment). Uses all eight tiles
//     as a 2x2 register-blocked accumulator.
//
// ## Tile configuration is reloaded on every call
//
// Each kernel has one compile-time tile shape (`SEARCH_TILES` / `GEMM_TILES`),
// so caching its LDTILECFG is tempting: that reconfiguration costs a few
// hundred cycles, against the ~64 cycles of useful tile work a dim-128 search
// call performs. It is still wrong, because Lance does not own the tile unit:
// LDTILECFG and TILERELEASE are architectural per-logical-processor state, so
// another AMX user on the thread (oneDNN under PyTorch, ONNX Runtime, same
// Python process) can retire or reshape a configuration Lance believes is live
// -- a foreign TILERELEASE leaves the tiles in INIT and the next tile op raises
// #UD, a foreign LDTILECFG silently substitutes wrong shapes. Neither is
// observable from here, and a kernel reached from arbitrary Rust and C cannot
// bound what runs between two of its own calls.
//
// So nothing is cached: every kernel configures the tiles on entry and releases
// them on exit, and pays that per call. Against a GEMM over a whole block of
// vectors it is noise; against a batch-16 search it is most of the call, and is
// spent anyway, because the alternative is a configuration whose validity this
// file has no way to establish.
//
// ## Thread safety
//
// LDTILECFG sets per-logical-processor state, and nothing here is shared
// mutably: the spec tables are `static const` and the 64-byte config *image* is
// built on the calling thread's stack, so one thread's shape can never reach
// another's tile ops -- as with the XTILECFG it is loaded into, which rides in
// the thread's own XSAVE area. That says nothing about what *other* libraries
// on this thread have done to the tile unit, which is what reconfiguring on
// every entry is for.
//
// SAFETY: executing any AMX tile instruction without first (a) confirming the
// amx-tile + amx-fp16 CPUID bits and (b) obtaining XTILEDATA permission from
// the kernel raises SIGILL. Both are the Rust caller's responsibility (see
// `simd/amx_fp16.rs`); `lance_amx_fp16_request_perm` below performs (b).

// Must precede all includes: exposes the glibc `syscall()` prototype from
// <unistd.h>, which -std=c17 otherwise hides.
#define _GNU_SOURCE

#include <immintrin.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>

#ifdef __linux__
#include <sys/syscall.h>
#include <unistd.h>
#endif

// ---------------------------------------------------------------------------
// XTILEDATA permission
// ---------------------------------------------------------------------------

// Ask the kernel to enable AMX tile data state for this process, via
// arch_prctl(ARCH_REQ_XCOMP_PERM, XFEATURE_XTILEDATA). Returns 0 on success,
// non-zero otherwise. Requesting XTILEDATA (18) implicitly also grants
// XTILECFG (17). Constants per Linux Documentation/arch/x86/xstate.rst.
//
// XTILEDATA is the single dynamically-enabled XSAVE state component backing the
// physical TMM tile registers, shared by every AMX compute instruction
// (TDPBUUD / TDPBF16PS / TDPFP16PS ...). The syscall is idempotent, so
// requesting an already-granted permission is harmless.
int lance_amx_fp16_request_perm(void) {
#ifdef __linux__
  const unsigned long ARCH_REQ_XCOMP_PERM = 0x1023;
  const unsigned long XFEATURE_XTILEDATA = 18;
  return (int)syscall(SYS_arch_prctl, ARCH_REQ_XCOMP_PERM, XFEATURE_XTILEDATA);
#else
  return -1;
#endif
}

// ---------------------------------------------------------------------------
// Shared tile configuration
// ---------------------------------------------------------------------------

// Tile configuration kinds. One per distinct tile shape, and the selector
// `lance_amx_tilecfg_image` exposes to the Rust tests so they can pin a shape
// without executing a tile instruction. Kept in sync with the `AMX_CFG_*`
// constants in `simd/amx_fp16.rs`.
#define LANCE_AMX_CFG_SEARCH 0
#define LANCE_AMX_CFG_GEMM 1

// The 64-byte tile configuration image loaded by LDTILECFG
// (`_tile_loadconfig`). Layout per Intel SDM: palette_id, start_row, 14
// reserved bytes, then a u16 colsb[16] (bytes-per-row) array and a u8 rows[16]
// array. Slots not named by a kernel's spec table stay zero.
//
// 64-byte aligned so the configuration load does not straddle a cache line.
typedef struct __attribute__((packed, aligned(64))) {
  uint8_t palette_id;
  uint8_t start_row;
  uint8_t reserved[14];
  uint16_t colsb[16];
  uint8_t rows[16];
} lance_amx_tilecfg;

// The image LDTILECFG reads is exactly 64 bytes; a layout change that altered
// the size would silently feed the instruction garbage.
_Static_assert(sizeof(lance_amx_tilecfg) == 64,
               "LDTILECFG image must be exactly 64 bytes");

// One tile register's shape. `tmm` is the register index (0..7), `colsb` its
// bytes per row, `rows` its row count.
typedef struct {
  uint8_t tmm;
  uint8_t rows;
  uint16_t colsb;
} lance_amx_tile_spec;

#define LANCE_AMX_TILE_COUNT(specs) (sizeof(specs) / sizeof((specs)[0]))

// Fill `cfg` with the LDTILECFG image described by `specs`, without loading it.
// Split from the load so `lance_amx_tilecfg_image` can hand the Rust tests the
// exact bytes a kernel would configure.
static void lance_amx_tilecfg_build(lance_amx_tilecfg *cfg,
                                    const lance_amx_tile_spec *specs,
                                    size_t n) {
  memset(cfg, 0, sizeof(*cfg));
  cfg->palette_id = 1;
  for (size_t i = 0; i < n; i++) {
    cfg->rows[specs[i].tmm] = specs[i].rows;
    cfg->colsb[specs[i].tmm] = specs[i].colsb;
  }
}

// Load the tile configuration described by `specs`. Unconditional: see the file
// header for why no state is kept about what is already loaded.
//
// The barrier is not optional: some GCC versions do not model
// _tile_loadconfig as reading the 64-byte cfg image, dead-store-eliminate the
// rows/colsb writes, and load an all-zero (unconfigured) tile shape -> #UD on
// the first tile op (documented in FAISS #5235). Keeping it here, in the one
// function that owns the image, is why kernels do not build configurations
// themselves. Harmless under clang.
static inline void lance_amx_tile_ensure(const lance_amx_tile_spec *specs,
                                         size_t n) {
  lance_amx_tilecfg cfg;
  lance_amx_tilecfg_build(&cfg, specs, n);
  __asm__ volatile("" : : "m"(cfg) : "memory");
  _tile_loadconfig(&cfg);
}

// Hand the tile unit back at the end of a kernel. Pairs with
// `lance_amx_tile_ensure`, and must run on every exit path of a kernel that
// configured tiles.
//
// Not for Lance's own benefit -- `lance_amx_tile_ensure` reloads on every entry,
// so a stale configuration could never reach one of these kernels either way. It
// is for everyone else on the thread: a live tile configuration keeps 8 KB of
// XTILEDATA in this thread's XSAVE area across every context switch, and leaves
// a shape another AMX user did not ask for sitting on hardware it also uses.
//
// Because it is nobody's correctness but the neighbours', deleting it leaves
// every result right and every "did it crash?" test green. Only
// `lance_amx_tilecfg_current_for_test`, which reads the hardware, notices.
static inline void lance_amx_tile_done(void) { _tile_release(); }

// Test hook: retire the tile configuration the way a *foreign* AMX user on this
// thread would, so a test can put the tile unit in INIT under a kernel that is
// about to run and check the kernel reconfigures instead of assuming.
//
// Hidden visibility because this manufactures the state the rest of this file
// exists to prevent: it is linked into the crate for its regression test, but is
// not something a shipped shared object should offer to whatever else is in the
// process.
//
// TILERELEASE needs no XTILEDATA grant of its own -- XFD traps only instructions
// that touch a TMM register -- but a test calling this is about to call a kernel
// that does, so it should have gone through `amx_supported()` anyway.
__attribute__((visibility("hidden"))) void lance_amx_tile_clobber_for_test(void) {
  _tile_release();
}

// Test hook: copy this logical processor's *live* tile configuration into the
// 64 bytes at `out`, via STTILECFG. A `palette_id` of 0 means the tile unit is
// in INIT state -- nothing configured.
//
// This reads the hardware rather than any record Lance keeps, which is what
// makes it able to catch the half of this design that has no other symptom:
// `lance_amx_tile_ensure` reloading on every entry is what keeps results
// correct, so dropping `lance_amx_tile_done`'s release leaves behaviour right
// and every "did it crash?" test passing, and only a live `palette_id` reported
// back here says the tile unit was never handed over.
//
// STTILECFG does not touch a TMM register, so unlike the kernels it is legal
// without the XTILEDATA grant. Hidden for the same reason as the clobber hook.
__attribute__((visibility("hidden"))) void lance_amx_tilecfg_current_for_test(
    uint8_t *out) {
  lance_amx_tilecfg cfg;
  _tile_storeconfig(&cfg);
  memcpy(out, &cfg, sizeof(cfg));
}

// ---------------------------------------------------------------------------
// Kernel: batch-16 search (one query x 16 candidates)
// ---------------------------------------------------------------------------

// Tile roles. These are immediate operands of the tile intrinsics, so they must
// be compile-time constants; `#define` rather than `enum` avoids relying on how
// strictly a compiler treats enum constants as immediates.
//
// One TDPFP16PS pass covers K = 32 fp16 dims. With N = 1 the query needs no
// VNNI repacking: B.row[k].fp16[i] = query[k*2 + i] is just the contiguous
// query halfwords, obtained by loading with a 4-byte row stride. The candidates
// form the A tile's rows; they live at unrelated addresses, so this kernel
// stages them a few k-blocks at a time into a fixed-stride scratch buffer that
// the tile load can read (see `lance_amx_stage_rows`).
//
// Three (A, B) pairs, not one. A single pair would make the loop strictly
// serial -- every TDPFP16PS waiting on the two loads that just overwrote its
// own operands -- so the tile unit would idle through each load's latency.
// Three independent pairs let three k-blocks' loads issue before the first
// TDPFP16PS needs its result, which is enough to keep the dp ops back to back.
// Seven tiles is what that costs; tmm7 is left unconfigured.
#define SEARCH_TMM_C 0   // 16 results x 1 fp32
#define SEARCH_TMM_A0 1  // 16 candidate rows x 32 fp16, k-block 3t
#define SEARCH_TMM_B0 2  // query, VNNI-packed at N = 1, k-block 3t
#define SEARCH_TMM_A1 3  // k-block 3t + 1
#define SEARCH_TMM_B1 4
#define SEARCH_TMM_A2 5  // k-block 3t + 2
#define SEARCH_TMM_B2 6

static const lance_amx_tile_spec SEARCH_TILES[] = {
    {SEARCH_TMM_C, 16, 4},   {SEARCH_TMM_A0, 16, 64}, {SEARCH_TMM_B0, 16, 4},
    {SEARCH_TMM_A1, 16, 64}, {SEARCH_TMM_B1, 16, 4},  {SEARCH_TMM_A2, 16, 64},
    {SEARCH_TMM_B2, 16, 4},
};

// Halfwords of one k-block: 32 fp16 dims, the K a single TDPFP16PS covers.
#define SEARCH_K_BLOCK 32

// Bytes one tile row spans in one k-block: 32 fp16, the A tile's full row width.
#define SEARCH_ROW_BYTES (SEARCH_K_BLOCK * (int)sizeof(uint16_t))

// K-blocks gathered per staging step. Three, so one staged buffer feeds exactly
// the three (A, B) pairs the main loop issues together.
#define SEARCH_STAGE_BLOCKS 3
#define SEARCH_STAGE_ROW_BYTES (SEARCH_STAGE_BLOCKS * SEARCH_ROW_BYTES)

// One staging buffer: the 16 rows a tile load always reads, whatever the batch
// actually holds.
#define SEARCH_STAGE_BYTES (16 * SEARCH_STAGE_ROW_BYTES)

// The furthest-reaching tile load is A2: 64 bytes at offset 128 of the last of
// 16 rows, so the last byte it touches is at 128 + 15*192 + 63 and the span it
// covers is exactly SEARCH_STAGE_BYTES. Asserted because an overrun here would
// be a stack smash with no other symptom.
//
// What this actually pins is that the three A tiles tile one staging row with
// no gap and no overlap, i.e. SEARCH_STAGE_BLOCKS == 3; it does not check
// SEARCH_K_BLOCK, nor the 64-byte A-tile width, which is hardcoded separately
// in SEARCH_TILES.
_Static_assert(2 * SEARCH_ROW_BYTES + 15 * SEARCH_STAGE_ROW_BYTES +
                       SEARCH_ROW_BYTES ==
                   SEARCH_STAGE_BYTES,
               "the three A tiles must tile a staging row exactly");

// Gather `row_bytes` starting at k-block `kb` out of each of the first `count`
// candidates into `dst`, one candidate per row.
//
// Rows are SEARCH_STAGE_ROW_BYTES apart even when `row_bytes` is smaller: an A
// tile only reads 64 bytes at its own offset within a row, so a wider stride
// simply leaves the trailing bytes unread. Keeping the stride fixed across
// every staging step is what lets rows [count, 16) be zeroed once per call --
// each step then rewrites the same rows at the same addresses.
//
// The k-blocks a row covers are adjacent inside the candidate vector, so each
// candidate costs exactly one straight-line copy no matter how many k-blocks the
// step covers. `row_bytes` should stay a compile-time constant at every call
// site: a constant size expands to inline wide moves, while a variable one
// becomes a libc `memcpy` call that `-funroll-loops` then multiplies (five PLT
// calls for the one remainder loop, measured under clang-16).
//
// Zeroing the padding rows is deliberately *not* routed through here: it writes
// rows [count, 16) rather than [0, count), and when the rows are full width it
// is one contiguous `memset` rather than a per-row loop.
static inline void lance_amx_stage_rows(uint8_t *dst,
                                        const uint16_t *const *candidates,
                                        size_t count, size_t kb,
                                        size_t row_bytes) {
  for (size_t n = 0; n < count; n++) {
    memcpy(dst + n * SEARCH_STAGE_ROW_BYTES,
           candidates[n] + kb * SEARCH_K_BLOCK, row_bytes);
  }
}

// out[i] = sum_{d in 0..dim} f32(query[d]) * f32(candidates[i][d]), i < count.
//
// `query`      -- IEEE-754 binary16 values as raw uint16_t bit patterns
//                 (half::f16 has identical layout); `dim` valid halfwords.
// `candidates` -- pointers to `dim` halfwords each; only the first `count` are
//                 read. The vectors live wherever the storage put them; this
//                 kernel owns the gather.
// `count`      -- candidates carrying a real vector. **Precondition:
//                 1 <= count <= 16**, rejected at the Rust boundary
//                 (`dot_f16_batch_16`) rather than clamped here; a larger value
//                 would run the gather off the end of a staging buffer.
// `dim`        -- vector dimension.
// `out`        -- destination for 16 fp32 dot products. Lanes [count, 16) are
//                 written as 0, not left untouched.
//
// ## Why `count` rather than always 16
//
// The caller sweeps centroids 16 at a time, so when their count is not a
// multiple of 16 the last group is short and it fills the spare slots by
// repeating a row it already holds. Staging all 16 rows would copy that row an
// extra 16 - count times; skipping them saves that much of a memcpy on at most
// one group per sweep, so `count` buys far less here than it did for a caller
// whose batches were usually partial. Only rows [0, count) are gathered, so the
// copying scales with the vectors actually scored while the tile work stays one
// fixed-cost pass. The padded rows still have to exist, since a tile load reads
// 16 rows unconditionally; they are zeroed once per call, and an all-zero A row
// yields a zero dot product.
//
// ## Why the gather is here and not in the caller
//
// A tile load reads 16 rows at one fixed stride from one base pointer, and no
// stride is guaranteed between this kernel's 16 candidate pointers, so their
// bytes have to be brought together somewhere. Doing it in the caller means
// copying 16 * dim * 2 bytes -- 24 KB at dim 768, 32 KB at dim 1024 -- and every
// one of those bytes has to land before the first TDPFP16PS can issue. Against a
// 48 KB L1D that buffer alone is half the cache, and the copy is pure exposed
// latency: nothing overlaps it.
//
// Staging inside the k-block loop instead keeps the working buffer at 3 KB and
// lets the copies for one triple run underneath the tile ops of the previous
// one. The bytes moved are identical; what changes is that they move while the
// tile unit is busy rather than before it starts. (Measured on the caller-side
// version at dim 1024: __memmove 3.7M cycles/query against 0.5M for the scalar
// kernel, IPC 1.70 -> 1.17, and a critical path 18% longer even though total CPU
// work per query was 6.5% lower. The same structure is what
// epeshared/hnswlib-amx uses for its AMX-BF16 kernel.)
//
// For dim > 32 we accumulate across floor(dim/32) tile passes into the same C
// tile, three k-blocks at a time. The tail (dim % 32 dims) is computed in
// scalar fp32 afterwards. The result is NOT bit-exact against a sequential
// scalar loop: floating-point tile-order accumulation rounds differently. It
// matches an f32-accumulated reference dot product to within fp16 precision,
// which is all the fp16 distance path requires (see amx_fp16.rs / the Rust-side
// tests).
void lance_amx_dot_f16_batch_16(const uint16_t *query,
                                const uint16_t *const *candidates, size_t count,
                                size_t dim, float *out) {
  const size_t blocks = dim / SEARCH_K_BLOCK;   // whole 32-wide tile passes
  const size_t full = blocks * SEARCH_K_BLOCK;  // dims they cover

  if (blocks > 0) {
    lance_amx_tile_ensure(SEARCH_TILES, LANCE_AMX_TILE_COUNT(SEARCH_TILES));

    // Two staging buffers, 16 rows x 192 bytes each. Double buffered so a
    // triple's gather can be issued a whole triple before the tile loads that
    // read it: a tile load cannot take its data from the store buffer, so the
    // gather's stores have to reach L1 first, and with one buffer there is
    // nothing to overlap that drain with. 3 KB each, so both sit in L1D
    // alongside the candidate rows streaming through it.
    __attribute__((aligned(64))) uint8_t stage[2][SEARCH_STAGE_BYTES];

    const size_t triples = blocks / SEARCH_STAGE_BLOCKS;
    const size_t rem = blocks % SEARCH_STAGE_BLOCKS;

    // Rows [count, 16) are padding that a tile load reads but no candidate
    // fills, so they are zeroed here and an all-zero A row then contributes a
    // zero dot product. Once per call is enough: every staging step below
    // writes rows [0, count) only, always at the same row stride, so nothing
    // disturbs the padding again.
    //
    // Only bytes some tile load actually reads are zeroed. This cost is the one
    // part of the call that does not shrink with `dim`, so zeroing the full
    // 2 x 15 x 192 bytes unconditionally would dominate short vectors:
    //   * `stage[1]` is tile-loaded only if the main loop reaches a second
    //     iteration, or the remainder lands on it (an odd `triples`). A single
    //     triple with no remainder never reads it at all.
    //   * A padding row is read to its full width only by the main loop. With
    //     `triples == 0` the remainder is the only reader, and it loads A0 --
    //     plus A1 when `rem == 2` -- so only the first `rem` k-block slots of
    //     each row are ever seen.
    if (count < 16) {
      const size_t pad_off = count * SEARCH_STAGE_ROW_BYTES;
      if (triples > 0) {
        // Full-width padding rows are contiguous: one memset covers them all.
        memset(stage[0] + pad_off, 0, SEARCH_STAGE_BYTES - pad_off);
        if (triples > 1 || rem > 0) {
          memset(stage[1] + pad_off, 0, SEARCH_STAGE_BYTES - pad_off);
        }
      } else {
        for (size_t n = count; n < 16; n++) {
          memset(stage[0] + n * SEARCH_STAGE_ROW_BYTES, 0,
                 rem * (size_t)SEARCH_ROW_BYTES);
        }
      }
    }

    _tile_zero(SEARCH_TMM_C);

    // Take ownership of the destination line now (PREFETCHW), so the RFO
    // overlaps the tile work instead of stalling TILESTORED at the end. The
    // store is 64 bytes issued as one instruction, and waiting on the RFO there
    // backs up the store queue: measured on the FAISS #5235 BF16 kernel as
    // SQ_Full 28.9% of cycles, with XQ.FULL_CYCLES 56x an AVX-512 baseline.
    _mm_prefetch((const char *)out, _MM_HINT_ET0);

    // Prologue of the software pipeline below, and the one gather in the call
    // with no tile work ahead of it to hide behind.
    if (triples > 0) {
      lance_amx_stage_rows(stage[0], candidates, count, 0,
                           SEARCH_STAGE_ROW_BYTES);

      // The in-loop prefetch below runs two triples ahead, so k-blocks
      // [3, 6) -- read by the very first iteration's gather -- would otherwise
      // be touched by nothing. Guarded on the address staying inside the
      // vectors, which also covers a remainder that follows a single triple.
      if (SEARCH_STAGE_BLOCKS < blocks) {
        for (size_t n = 0; n < count; n++) {
          _mm_prefetch(
              (const char *)(candidates[n] +
                             SEARCH_STAGE_BLOCKS * (size_t)SEARCH_K_BLOCK),
              _MM_HINT_T0);
        }
      }
    }

    size_t kb = 0;
    for (size_t t = 0; t < triples; t++, kb += SEARCH_STAGE_BLOCKS) {
      const uint8_t *st = stage[t & 1];

      // Gather the *next* triple before issuing this one's tile ops, into the
      // buffer the previous iteration's tile loads have already consumed. Those
      // stores then have a full triple of tile work to commit to L1 under,
      // instead of the tile load immediately below them waiting on the drain.
      if (t + 1 < triples) {
        // Open the candidate streams the iteration after this one will copy
        // from, so that copy finds them resident. With the rows at 16 unrelated
        // addresses there is no single stride for a hardware prefetcher to
        // latch onto; what it can do is run each candidate forward once that
        // candidate has been touched, and these touches are what start it.
        // (Measured on the FAISS #5235 BF16 kernel without any prefetch: L1D MPI
        // 2.6x and DTLB load MPI 14.6x an AVX-512 baseline.) Guarded so the last
        // iterations stay inside the vectors.
        if (kb + 2 * SEARCH_STAGE_BLOCKS < blocks) {
          const size_t ahead = (kb + 2 * SEARCH_STAGE_BLOCKS) * SEARCH_K_BLOCK;
          for (size_t n = 0; n < count; n++) {
            _mm_prefetch((const char *)(candidates[n] + ahead), _MM_HINT_T0);
          }
        }
        lance_amx_stage_rows(stage[(t + 1) & 1], candidates, count,
                             kb + SEARCH_STAGE_BLOCKS, SEARCH_STAGE_ROW_BYTES);
      }

      // All six loads first: they are independent, so the three dp ops below
      // issue back to back rather than each waiting on its own operands.
      _tile_loadd(SEARCH_TMM_A0, st + 0 * SEARCH_ROW_BYTES,
                  SEARCH_STAGE_ROW_BYTES);
      _tile_loadd(SEARCH_TMM_B0, query + kb * SEARCH_K_BLOCK, 4);
      _tile_loadd(SEARCH_TMM_A1, st + 1 * SEARCH_ROW_BYTES,
                  SEARCH_STAGE_ROW_BYTES);
      _tile_loadd(SEARCH_TMM_B1, query + (kb + 1) * SEARCH_K_BLOCK, 4);
      _tile_loadd(SEARCH_TMM_A2, st + 2 * SEARCH_ROW_BYTES,
                  SEARCH_STAGE_ROW_BYTES);
      _tile_loadd(SEARCH_TMM_B2, query + (kb + 2) * SEARCH_K_BLOCK, 4);

      // C += A * B  (fp16 x fp16 -> fp32)
      _tile_dpfp16ps(SEARCH_TMM_C, SEARCH_TMM_A0, SEARCH_TMM_B0);
      _tile_dpfp16ps(SEARCH_TMM_C, SEARCH_TMM_A1, SEARCH_TMM_B1);
      _tile_dpfp16ps(SEARCH_TMM_C, SEARCH_TMM_A2, SEARCH_TMM_B2);
    }

    // The 1 or 2 k-blocks left over when `blocks` is not a multiple of three.
    // `stage[triples & 1]` is the buffer the loop above neither wrote nor read
    // last, so filling it now cannot collide with a tile load still in flight.
    if (rem > 0) {
      uint8_t *st = stage[triples & 1];
      // Two constant-size cases rather than one `rem * 64` copy, because a
      // variable-length memcpy here does not stay one instruction: clang-16
      // emits a libc call and `-funroll-loops` then multiplies it, measured as
      // five `memcpy@PLT` calls for this one loop (the pre-`count` kernel,
      // whose loop ran to 16, paid sixteen). The main loop's gather is
      // unaffected either way -- it keeps its inline wide moves -- so this is
      // about the remainder alone. The branch costs one predictable compare.
      if (rem == 2) {
        lance_amx_stage_rows(st, candidates, count, kb, 2 * SEARCH_ROW_BYTES);
      } else {
        lance_amx_stage_rows(st, candidates, count, kb, SEARCH_ROW_BYTES);
      }

      // Loaded at the main loop's row stride even though only `rem` k-blocks are
      // live: A0 and A1 read their own 64 bytes at offsets 0 and 64, which is
      // what the staging step just filled, and the padding rows are already zero
      // at exactly this stride.
      _tile_loadd(SEARCH_TMM_A0, st, SEARCH_STAGE_ROW_BYTES);
      _tile_loadd(SEARCH_TMM_B0, query + kb * SEARCH_K_BLOCK, 4);
      if (rem == 2) {
        _tile_loadd(SEARCH_TMM_A1, st + SEARCH_ROW_BYTES,
                    SEARCH_STAGE_ROW_BYTES);
        _tile_loadd(SEARCH_TMM_B1, query + (kb + 1) * SEARCH_K_BLOCK, 4);
      }
      _tile_dpfp16ps(SEARCH_TMM_C, SEARCH_TMM_A0, SEARCH_TMM_B0);
      if (rem == 2) {
        _tile_dpfp16ps(SEARCH_TMM_C, SEARCH_TMM_A1, SEARCH_TMM_B1);
      }
    }

    _tile_stored(SEARCH_TMM_C, out, 4);  // 16 fp32, row stride 4 bytes
    // Last tile op in this call; the tail below is scalar. Hands the tile unit
    // back so an interleaved AMX user on this thread cannot be surprised by a
    // configuration it did not ask for.
    lance_amx_tile_done();
  } else {
    for (int i = 0; i < 16; i++) out[i] = 0.0f;
  }

  // Tail: the dims not covered by a full 32-wide tile, in scalar fp32. F16C
  // `_cvtsh_ss` is an exact (lossless) binary16 -> binary32 widening. Lanes at
  // or past `count` are skipped rather than accumulated onto: they are already
  // 0 (zeroed staging rows, or the no-tile-pass branch above) and must stay so.
  const size_t tail = dim - full;
  if (tail > 0) {
    for (size_t i = 0; i < count; i++) {
      float acc = 0.0f;
      const uint16_t *row = candidates[i];
      for (size_t d = full; d < dim; d++) {
        acc += _cvtsh_ss(row[d]) * _cvtsh_ss(query[d]);
      }
      out[i] += acc;
    }
  }
}

// ---------------------------------------------------------------------------
// Kernel: M x N GEMM (many vectors x many centroids)
// ---------------------------------------------------------------------------

// Tile roles for the 2x2-register-blocked GEMM. Two A tiles (32 vectors) and
// two B tiles (32 centroids) feed four C accumulators, so one k-pass issues 4
// TDPFP16PS against 4 tile loads -- the highest compute-per-load ratio the 8
// physical tiles allow, and the reason all 8 are claimed here.
#define GEMM_TMM_C00 0  // 16 vectors x 16 centroids, fp32
#define GEMM_TMM_C01 1
#define GEMM_TMM_C10 2
#define GEMM_TMM_C11 3
#define GEMM_TMM_A0 4  // 16 vector rows x 32 fp16 dims
#define GEMM_TMM_A1 5
#define GEMM_TMM_B0 6  // 32 dims x 16 centroids, VNNI-interleaved
#define GEMM_TMM_B1 7

// All eight at the architectural maximum (16 rows x 64 bytes = 1 KB), which is
// exactly the 8 KB of tile state AMX provides.
static const lance_amx_tile_spec GEMM_TILES[] = {
    {GEMM_TMM_C00, 16, 64}, {GEMM_TMM_C01, 16, 64}, {GEMM_TMM_C10, 16, 64},
    {GEMM_TMM_C11, 16, 64}, {GEMM_TMM_A0, 16, 64},  {GEMM_TMM_A1, 16, 64},
    {GEMM_TMM_B0, 16, 64},  {GEMM_TMM_B1, 16, 64},
};

// Halfwords per packed B block: 16 tile rows x 32 halfwords per row.
#define GEMM_B_BLOCK 512

// out[i*out_stride + j] = sum_{d in 0..dim} f32(data[i*data_stride + d]) *
//                                           f32(centroids[j*dim + d]).
//
// `data`        -- [m, dim] row-major fp16 bit patterns, rows `data_stride`
//                  halfwords apart (`data_stride >= dim`).
// `m`           -- number of vectors; **must be a multiple of 32**.
// `packed_b`    -- centroids pre-interleaved by `pack_centroids_vnni` (see
//                  `amx_fp16.rs`), holding only the floor(dim/32) whole
//                  32-dim k-blocks.
// `centroids`   -- the same [n, dim] row-major centroids `packed_b` was built
//                  from. Read only for the `dim % 32` tail dims, which are not
//                  worth a tile pass and so are never packed; still required
//                  when dim % 32 == 0, where it goes unread.
// `n`           -- number of centroids; **must be a multiple of 32**.
// `dim`         -- vector dimension; any value, the tail runs scalar.
// `out`         -- [m, n] row-major fp32, rows `out_stride` floats apart
//                  (`out_stride >= n`).
//
// The m and n multiple-of-32 requirements are preconditions, not something this
// kernel checks or works around: they let the register-blocked loop run with no
// edge cases, and the Rust caller is the layer that knows how to pad or split.
//
// The k dimension carries no such requirement. TDPFP16PS accumulation rounds
// differently from a sequential scalar loop, so results match an f32-accumulated
// reference to fp16 precision rather than bit-exactly -- same contract as
// `lance_amx_dot_f16_batch_16`.
//
// B's VNNI interleave is what makes A loadable straight out of `data`: with
//   packed_b[((kb*(n/16) + jb)*512) + k*32 + nn*2 + p]
//       == centroids[(jb*16 + nn)*dim + kb*32 + 2*k + p]
// TDPFP16PS's b.row[k].fp16[2*nn+p] lands on centroid (jb*16+nn) dim
// (kb*32+2*k+p), pairing it with a.row[mm].fp16[2*k+p] = the same dim of vector
// (i+mm). k-blocks are the outer index so one k-pass reads the two B tiles it
// needs from adjacent memory.
void lance_amx_dot_f16_gemm(const uint16_t *data, size_t m, size_t data_stride,
                            const uint16_t *packed_b, const uint16_t *centroids,
                            size_t n, size_t dim, float *out,
                            size_t out_stride) {
  const size_t full = (dim / 32) * 32;  // dims covered by full 32-wide passes

  if (full > 0) {
    lance_amx_tile_ensure(GEMM_TILES, LANCE_AMX_TILE_COUNT(GEMM_TILES));

    const size_t a_stride_bytes = data_stride * sizeof(uint16_t);
    const size_t c_stride_bytes = out_stride * sizeof(float);
    const size_t b_blocks_per_k = n / 16;

    for (size_t i = 0; i < m; i += 32) {
      const uint16_t *a0 = data + i * data_stride;
      const uint16_t *a1 = a0 + 16 * data_stride;
      float *c0 = out + i * out_stride;
      float *c1 = c0 + 16 * out_stride;

      for (size_t j = 0; j < n; j += 32) {
        _tile_zero(GEMM_TMM_C00);
        _tile_zero(GEMM_TMM_C01);
        _tile_zero(GEMM_TMM_C10);
        _tile_zero(GEMM_TMM_C11);

        for (size_t kbase = 0; kbase < full; kbase += 32) {
          // The B tiles for centroid blocks j/16 and j/16+1 are adjacent
          // because jb is the inner index of the packed layout.
          const uint16_t *b =
              packed_b + ((kbase / 32) * b_blocks_per_k + j / 16) * GEMM_B_BLOCK;
          _tile_loadd(GEMM_TMM_A0, a0 + kbase, a_stride_bytes);
          _tile_loadd(GEMM_TMM_A1, a1 + kbase, a_stride_bytes);
          _tile_loadd(GEMM_TMM_B0, b, 64);
          _tile_loadd(GEMM_TMM_B1, b + GEMM_B_BLOCK, 64);
          _tile_dpfp16ps(GEMM_TMM_C00, GEMM_TMM_A0, GEMM_TMM_B0);
          _tile_dpfp16ps(GEMM_TMM_C01, GEMM_TMM_A0, GEMM_TMM_B1);
          _tile_dpfp16ps(GEMM_TMM_C10, GEMM_TMM_A1, GEMM_TMM_B0);
          _tile_dpfp16ps(GEMM_TMM_C11, GEMM_TMM_A1, GEMM_TMM_B1);
        }

        _tile_stored(GEMM_TMM_C00, c0 + j, c_stride_bytes);
        _tile_stored(GEMM_TMM_C01, c0 + j + 16, c_stride_bytes);
        _tile_stored(GEMM_TMM_C10, c1 + j, c_stride_bytes);
        _tile_stored(GEMM_TMM_C11, c1 + j + 16, c_stride_bytes);
      }
    }
    // Last tile op in this call; the tail below is scalar. One LDTILECFG plus
    // one TILERELEASE against an m x n GEMM is why the per-call reconfiguration
    // the file header argues for costs this kernel nothing.
    lance_amx_tile_done();
  } else {
    // dim < 32: no tile ever stores to `out`, so the scalar tail below has to
    // accumulate onto a known-zero destination rather than whatever was there.
    for (size_t i = 0; i < m; i++) {
      memset(out + i * out_stride, 0, n * sizeof(float));
    }
  }

  const size_t tail = dim - full;
  if (tail > 0) {
    for (size_t i = 0; i < m; i++) {
      // Widen this vector's tail once per row instead of once per (row,
      // centroid) pair; `tail` is at most 31 so the buffer is a fixed 32.
      float vec_tail[32];
      const uint16_t *row = data + i * data_stride + full;
      for (size_t d = 0; d < tail; d++) vec_tail[d] = _cvtsh_ss(row[d]);

      float *out_row = out + i * out_stride;
      for (size_t j = 0; j < n; j++) {
        const uint16_t *cent = centroids + j * dim + full;
        float acc = 0.0f;
        for (size_t d = 0; d < tail; d++) acc += vec_tail[d] * _cvtsh_ss(cent[d]);
        out_row[j] += acc;
      }
    }
  }
}

// ---------------------------------------------------------------------------
// Configuration introspection
// ---------------------------------------------------------------------------

// Write the 64-byte LDTILECFG image for `cfg_kind` (a `LANCE_AMX_CFG_*`
// constant) into `out`, without loading it. Returns 0 on success, -1 for an
// unknown `cfg_kind`.
//
// Exposed so the Rust tests can pin each kernel's tile shape byte for byte. A
// wrong shape does not fail cleanly — it is a #UD or silently wrong results —
// so the shape is asserted directly rather than inferred from kernel output.
int lance_amx_tilecfg_image(int cfg_kind, uint8_t *out) {
  const lance_amx_tile_spec *specs;
  size_t n;

  switch (cfg_kind) {
    case LANCE_AMX_CFG_SEARCH:
      specs = SEARCH_TILES;
      n = LANCE_AMX_TILE_COUNT(SEARCH_TILES);
      break;
    case LANCE_AMX_CFG_GEMM:
      specs = GEMM_TILES;
      n = LANCE_AMX_TILE_COUNT(GEMM_TILES);
      break;
    default:
      return -1;
  }

  lance_amx_tilecfg cfg;
  lance_amx_tilecfg_build(&cfg, specs, n);
  memcpy(out, &cfg, sizeof(cfg));
  return 0;
}
