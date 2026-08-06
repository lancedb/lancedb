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

float FUNC(norm_l2_f16)(const FP16 *data, uint32_t dimension) {
  float sum = 0;

#pragma clang loop unroll(enable) vectorize(enable) interleave(enable)
  for (uint32_t i = 0; i < dimension; i++) {
    sum += (float) data[i] * (float) data[i];
  }
  return sqrtf(sum);
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
  float sum = 0.0;

#pragma clang loop unroll(enable) interleave(enable) vectorize(enable)
  for (uint32_t i = 0; i < dimension; i++) {
    float s = (float) x[i] - (float) y[i];
    sum += s * s;
  }
  return sum;
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
