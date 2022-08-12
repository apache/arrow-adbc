// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#ifndef NANOARROW_ARRAY_INLINE_H_INCLUDED
#define NANOARROW_ARRAY_INLINE_H_INCLUDED

#include <errno.h>
#include <stdint.h>
#include <string.h>

#include "bitmap_inline.h"
#include "buffer_inline.h"
#include "typedefs_inline.h"

#ifdef __cplusplus
extern "C" {
#endif

static inline struct ArrowBitmap* ArrowArrayValidityBitmap(struct ArrowArray* array) {
  struct ArrowArrayPrivateData* private_data =
      (struct ArrowArrayPrivateData*)array->private_data;
  return &private_data->bitmap;
}

static inline struct ArrowBuffer* ArrowArrayBuffer(struct ArrowArray* array, int64_t i) {
  struct ArrowArrayPrivateData* private_data =
      (struct ArrowArrayPrivateData*)array->private_data;
  switch (i) {
    case 0:
      return &private_data->bitmap.buffer;
    default:
      return private_data->buffers + i - 1;
  }
}

static inline ArrowErrorCode ArrowArrayFinishBuilding(struct ArrowArray* array,
                                                      char shrink_to_fit) {
  struct ArrowArrayPrivateData* private_data =
      (struct ArrowArrayPrivateData*)array->private_data;

  // Make sure the value we get with array->buffers[i] is set to the actual
  // pointer (which may have changed from the original due to reallocation)
  int result;
  for (int64_t i = 0; i < 3; i++) {
    struct ArrowBuffer* buffer = ArrowArrayBuffer(array, i);
    if (shrink_to_fit) {
      result = ArrowBufferResize(buffer, buffer->size_bytes, shrink_to_fit);
      if (result != NANOARROW_OK) {
        return result;
      }
    }

    private_data->buffer_data[i] = ArrowArrayBuffer(array, i)->data;
  }

  return NANOARROW_OK;
}

#ifdef __cplusplus
}
#endif

#endif
