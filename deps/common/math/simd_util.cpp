/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include <stdint.h>
#include "common/math/simd_util.h"

#if defined(USE_SIMD)

int mm256_extract_epi32_var_indx(const __m256i vec, const unsigned int i)
{
  __m128i idx = _mm_cvtsi32_si128(i);
  __m256i val = _mm256_permutevar8x32_epi32(vec, _mm256_castsi128_si256(idx));
  return _mm_cvtsi128_si32(_mm256_castsi256_si128(val));
}

int mm256_sum_epi32(const int *values, int size)
{
  int sum[8];
  __m256i sum256 = _mm256_setzero_si256();
  __m256i load256 = _mm256_setzero_si256();
  int i = 0;
  for (; i <= size - SIMD_WIDTH; i += SIMD_WIDTH)
  {
    load256 = _mm256_loadu_si256((const __m256i *)&values[i]);
    sum256 = _mm256_add_epi32(sum256, load256);
  }
  _mm256_storeu_si256((__m256i*)sum, sum256);
  for (int j = 1; j < SIMD_WIDTH; j++) {
    sum[0] += sum[j];
  }
  for (; i < size; i++) {
    sum[0] += values[i];
  }
  return sum[0];
  // your code here
  // int sum = 0;
  // for (int i = 0; i < size; i++) {
  //   sum += values[i];
  // }
  // return sum;
}

float mm256_sum_ps(const float *values, int size)
{
  float sum[8];
  __m256 sum256 = _mm256_setzero_ps();
  __m256 load256 = _mm256_setzero_ps();
  int i = 0;
  for (; i <= size - SIMD_WIDTH; i += SIMD_WIDTH)
  {
    load256 = _mm256_loadu_ps(&values[i]);
    sum256 = _mm256_add_ps(sum256, load256);
  }
  _mm256_storeu_ps(sum, sum256);
  for (int j = 1; j < SIMD_WIDTH; j++) {
    sum[0] += sum[j];
  }
  for (; i < size; i++) {
    sum[0] += values[i];
  }
  return sum[0];
  // your code here
  // float sum = 0;
  // for (int i = 0; i < size; i++) {
  //   sum += values[i];
  // }
  // return sum;
}

template <typename V>
void selective_load(V *memory, int offset, V *vec, __m256i &inv)
{
  int *inv_ptr = reinterpret_cast<int *>(&inv);
  for (int i = 0; i < SIMD_WIDTH; i++) {
    if (inv_ptr[i] == -1) {
      vec[i] = memory[offset++];
    }
  }
}
template void selective_load<uint32_t>(uint32_t *memory, int offset, uint32_t *vec, __m256i &inv);
template void selective_load<int>(int *memory, int offset, int *vec, __m256i &inv);
template void selective_load<float>(float *memory, int offset, float *vec, __m256i &inv);

#endif