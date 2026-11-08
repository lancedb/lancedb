// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

#include <immintrin.h>
#include <stddef.h>
#include <stdint.h>

void sum_4bit_dist_table_32bytes_batch_avx512(const uint8_t *codes,
                                              size_t code_length,
                                              const uint8_t *dist_table,
                                              uint16_t *dists) {
  __m512i c;
  __m512i lo;
  __m512i hi;
  __m512i lut;
  __m512i res_lo;
  __m512i res_hi;

  const __m512i lo_mask = _mm512_set1_epi8(0x0f);
  __m512i accu0 = _mm512_setzero_si512();
  __m512i accu1 = _mm512_setzero_si512();
  __m512i accu2 = _mm512_setzero_si512();
  __m512i accu3 = _mm512_setzero_si512();

  for (size_t i = 0; i < code_length; i += 64) {
    const size_t remaining = code_length - i;
    if (remaining >= 64) {
      c = _mm512_loadu_si512(&codes[i]);
      lut = _mm512_loadu_si512(&dist_table[i]);
    } else {
      const __mmask64 load_mask = (UINT64_C(1) << remaining) - 1;
      c = _mm512_maskz_loadu_epi8(load_mask, &codes[i]);
      lut = _mm512_maskz_loadu_epi8(load_mask, &dist_table[i]);
    }
    lo = _mm512_and_si512(c, lo_mask);
    hi = _mm512_and_si512(_mm512_srli_epi16(c, 4), lo_mask);

    res_lo = _mm512_shuffle_epi8(lut, lo);
    res_hi = _mm512_shuffle_epi8(lut, hi);

    accu0 = _mm512_add_epi16(accu0, res_lo);
    accu1 = _mm512_add_epi16(accu1, _mm512_srli_epi16(res_lo, 8));
    accu2 = _mm512_add_epi16(accu2, res_hi);
    accu3 = _mm512_add_epi16(accu3, _mm512_srli_epi16(res_hi, 8));
  }

  accu0 = _mm512_sub_epi16(accu0, _mm512_slli_epi16(accu1, 8));
  accu2 = _mm512_sub_epi16(accu2, _mm512_slli_epi16(accu3, 8));

  __m512i ret1 = _mm512_add_epi16(_mm512_mask_blend_epi64(0xF0, accu0, accu1),
                                  _mm512_shuffle_i64x2(accu0, accu1, 0x4E));
  __m512i ret2 = _mm512_add_epi16(_mm512_mask_blend_epi64(0xF0, accu2, accu3),
                                  _mm512_shuffle_i64x2(accu2, accu3, 0x4E));
  __m512i ret = _mm512_setzero_si512();

  ret = _mm512_add_epi16(ret, _mm512_shuffle_i64x2(ret1, ret2, 0x88));
  ret = _mm512_add_epi16(ret, _mm512_shuffle_i64x2(ret1, ret2, 0xDD));

  _mm512_storeu_si512(dists, ret);
}
