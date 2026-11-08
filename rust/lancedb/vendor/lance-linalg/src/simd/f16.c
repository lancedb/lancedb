// Copyright 2023 Lance Developers.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <stddef.h>
#include <stdint.h>
#include <math.h>

// Because we might be compiling this library multiple times, we need to
// add a suffix to each of the function names.
#define FUNC_CAT_INNER(A, B) A##B
#define FUNC_CAT(A, B) FUNC_CAT_INNER(A, B)
#define FUNC(N) FUNC_CAT(N, SUFFIX)

#if defined(__clang__)
// Note: we use __fp16 instead of _Float16 because Clang < 15.0.0 does not
// support it well for most targets. __fp16 works for our purposes here since
// we are always casting it to float anyways. This doesn't make a difference
// in the compiled assembly code for these functions.
#define FP16 __fp16
#elif defined(__GNUC__) || defined(__GNUG__)
#define FP16 _Float16
#endif
// Note: MSVC doesn't support _Float16 yet, so we can't use it here.

// The AVX2 object uses the lane-partitioned reductions below to preserve the
// Rust reference implementation's error bound while still vectorizing each
// 16-element chunk. The AVX-512 object keeps its existing wider reduction.
#define F16_ACCUMULATOR_LANES 16

float FUNC(norm_l2_f16)(const FP16 *data, uint32_t dimension) {
#ifdef PRECISE_F16_REDUCTION
  float sums[F16_ACCUMULATOR_LANES] = {0};
  uint32_t chunked_dimension =
      dimension - dimension % F16_ACCUMULATOR_LANES;

  for (uint32_t i = 0; i < chunked_dimension;
       i += F16_ACCUMULATOR_LANES) {
#pragma clang loop unroll(full)
    for (uint32_t lane = 0; lane < F16_ACCUMULATOR_LANES; lane++) {
      float value = (float)data[i + lane];
      sums[lane] += value * value;
    }
  }

  float remainder_sum = 0;
  for (uint32_t i = chunked_dimension; i < dimension; i++) {
    float value = (float)data[i];
    remainder_sum += value * value;
  }

  float lane_sum = 0;
  for (uint32_t lane = 0; lane < F16_ACCUMULATOR_LANES; lane++) {
    lane_sum += sums[lane];
  }
  return sqrtf(remainder_sum + lane_sum);
#else
  float sum = 0;

#pragma clang loop unroll(enable) vectorize(enable) interleave(enable)
  for (uint32_t i = 0; i < dimension; i++) {
    sum += (float) data[i] * (float) data[i];
  }
  return sqrtf(sum);
#endif
}

/// @brief Dot product of two f16 vectors.
/// @param x A f16 vector
/// @param y A f16 vector
/// @param dimension The dimension of the vectors
/// @return The dot product of the two vectors.
float FUNC(dot_f16)(const FP16 *x, const FP16 *y, uint32_t dimension) {
  float sum = 0;

#pragma clang loop unroll(enable) interleave(enable) vectorize(enable)
  for (uint32_t i = 0; i < dimension; i++) {
    sum += (float) x[i] * (float) y[i];
  }
  return sum;
}

float FUNC(l2_f16)(const FP16 *x, const FP16 *y, uint32_t dimension) {
#ifdef PRECISE_F16_REDUCTION
  float sums[F16_ACCUMULATOR_LANES] = {0};
  uint32_t chunked_dimension =
      dimension - dimension % F16_ACCUMULATOR_LANES;

  for (uint32_t i = 0; i < chunked_dimension;
       i += F16_ACCUMULATOR_LANES) {
#pragma clang loop unroll(full)
    for (uint32_t lane = 0; lane < F16_ACCUMULATOR_LANES; lane++) {
      float difference = (float)x[i + lane] - (float)y[i + lane];
      sums[lane] += difference * difference;
    }
  }

  float remainder_sum = 0;
  for (uint32_t i = chunked_dimension; i < dimension; i++) {
    float difference = (float)x[i] - (float)y[i];
    remainder_sum += difference * difference;
  }

  float lane_sum = 0;
  for (uint32_t lane = 0; lane < F16_ACCUMULATOR_LANES; lane++) {
    lane_sum += sums[lane];
  }
  return remainder_sum + lane_sum;
#else
  float sum = 0.0;

#pragma clang loop unroll(enable) interleave(enable) vectorize(enable)
  for (uint32_t i = 0; i < dimension; i++) {
    float s = (float) x[i] - (float) y[i];
    sum += s * s;
  }
  return sum;
#endif
}

float FUNC(cosine_f16)(const FP16 *x, float x_norm, const FP16 *y, uint32_t dimension) {
  float dot = 0.0;
  float l2_y = 0.0;

  // Instead of using functions above, we combine the loop to reduce overhead
  // of the fp16 to fp32 conversion.
#pragma clang loop unroll(enable) interleave(enable) vectorize(enable)
  for (uint32_t i = 0; i < dimension; i++) {
    float y_i = (float) y[i];
    dot += (float) x[i] * y_i;
    l2_y += y_i * y_i;
  }

  return 1.0 - dot / (x_norm * sqrtf(l2_y));
}
