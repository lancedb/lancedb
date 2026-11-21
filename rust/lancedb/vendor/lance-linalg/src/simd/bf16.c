// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

#include <stddef.h>
#include <stdint.h>
#include <math.h>
#include <string.h>

// Because we might be compiling this library multiple times, we need to
// add a suffix to each of the function names.
#define FUNC_CAT_INNER(A, B) A##B
#define FUNC_CAT(A, B) FUNC_CAT_INNER(A, B)
#define FUNC(N) FUNC_CAT(N, SUFFIX)

// Convert a bf16 value (stored as uint16_t) to float32.
// BF16 is the upper 16 bits of an IEEE 754 float32, so we just shift left.
static inline float bf16_to_f32(uint16_t v) {
  uint32_t bits = (uint32_t)v << 16;
  float f;
  memcpy(&f, &bits, sizeof(f));
  return f;
}

float FUNC(norm_l2_bf16)(const uint16_t *data, uint32_t dimension) {
  float sum = 0;

#pragma clang loop unroll(enable) vectorize(enable) interleave(enable)
  for (uint32_t i = 0; i < dimension; i++) {
    float v = bf16_to_f32(data[i]);
    sum += v * v;
  }
  return sqrtf(sum);
}

/// @brief Dot product of two bf16 vectors.
/// @param x A bf16 vector (stored as uint16_t)
/// @param y A bf16 vector (stored as uint16_t)
/// @param dimension The dimension of the vectors
/// @return The dot product of the two vectors.
float FUNC(dot_bf16)(const uint16_t *x, const uint16_t *y, uint32_t dimension) {
  float sum = 0;

#pragma clang loop unroll(enable) interleave(enable) vectorize(enable)
  for (uint32_t i = 0; i < dimension; i++) {
    sum += bf16_to_f32(x[i]) * bf16_to_f32(y[i]);
  }
  return sum;
}

float FUNC(l2_bf16)(const uint16_t *x, const uint16_t *y, uint32_t dimension) {
  float sum = 0.0;

#pragma clang loop unroll(enable) interleave(enable) vectorize(enable)
  for (uint32_t i = 0; i < dimension; i++) {
    float s = bf16_to_f32(x[i]) - bf16_to_f32(y[i]);
    sum += s * s;
  }
  return sum;
}

float FUNC(cosine_bf16)(const uint16_t *x, float x_norm, const uint16_t *y, uint32_t dimension) {
  float dot = 0.0;
  float l2_y = 0.0;

  // Combine the loop to reduce overhead of the bf16 to fp32 conversion.
#pragma clang loop unroll(enable) interleave(enable) vectorize(enable)
  for (uint32_t i = 0; i < dimension; i++) {
    float y_i = bf16_to_f32(y[i]);
    dot += bf16_to_f32(x[i]) * y_i;
    l2_y += y_i * y_i;
  }

  return 1.0 - dot / (x_norm * sqrtf(l2_y));
}
