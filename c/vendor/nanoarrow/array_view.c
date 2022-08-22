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

#include <errno.h>
#include <stdlib.h>
#include <string.h>

#include "nanoarrow.h"

void ArrowArrayViewInit(struct ArrowArrayView* array_view, enum ArrowType storage_type) {
  memset(array_view, 0, sizeof(struct ArrowArrayView));
  array_view->storage_type = storage_type;
  ArrowLayoutInit(&array_view->layout, storage_type);
}

ArrowErrorCode ArrowArrayViewAllocateChildren(struct ArrowArrayView* array_view,
                                              int64_t n_children) {
  if (array_view->children != NULL) {
    return EINVAL;
  }

  array_view->children =
      (struct ArrowArrayView**)ArrowMalloc(n_children * sizeof(struct ArrowArrayView*));
  if (array_view->children == NULL) {
    return ENOMEM;
  }

  for (int64_t i = 0; i < n_children; i++) {
    array_view->children[i] = NULL;
  }

  array_view->n_children = n_children;

  for (int64_t i = 0; i < n_children; i++) {
    array_view->children[i] =
        (struct ArrowArrayView*)ArrowMalloc(sizeof(struct ArrowArrayView));
    if (array_view->children[i] == NULL) {
      return ENOMEM;
    }
    ArrowArrayViewInit(array_view->children[i], NANOARROW_TYPE_UNINITIALIZED);
  }

  return NANOARROW_OK;
}

ArrowErrorCode ArrowArrayViewInitFromSchema(struct ArrowArrayView* array_view,
                                            struct ArrowSchema* schema,
                                            struct ArrowError* error) {
  struct ArrowSchemaView schema_view;
  int result = ArrowSchemaViewInit(&schema_view, schema, error);
  if (result != NANOARROW_OK) {
    return result;
  }

  ArrowArrayViewInit(array_view, schema_view.storage_data_type);
  array_view->layout = schema_view.layout;

  result = ArrowArrayViewAllocateChildren(array_view, schema->n_children);
  if (result != NANOARROW_OK) {
    ArrowArrayViewReset(array_view);
    return result;
  }

  for (int64_t i = 0; i < schema->n_children; i++) {
    result =
        ArrowArrayViewInitFromSchema(array_view->children[i], schema->children[i], error);
    if (result != NANOARROW_OK) {
      ArrowArrayViewReset(array_view);
      return result;
    }
  }

  return NANOARROW_OK;
}

void ArrowArrayViewReset(struct ArrowArrayView* array_view) {
  if (array_view->children != NULL) {
    for (int64_t i = 0; i < array_view->n_children; i++) {
      if (array_view->children[i] != NULL) {
        ArrowArrayViewReset(array_view->children[i]);
        ArrowFree(array_view->children[i]);
      }
    }

    ArrowFree(array_view->children);
  }

  ArrowArrayViewInit(array_view, NANOARROW_TYPE_UNINITIALIZED);
}

void ArrowArrayViewSetLength(struct ArrowArrayView* array_view, int64_t length) {
  for (int i = 0; i < 3; i++) {
    int64_t element_size_bytes = array_view->layout.element_size_bits[i] / 8;
    array_view->buffer_views[i].data.data = NULL;

    switch (array_view->layout.buffer_type[i]) {
      case NANOARROW_BUFFER_TYPE_VALIDITY:
        array_view->buffer_views[i].n_bytes = _ArrowBytesForBits(length);
        continue;
      case NANOARROW_BUFFER_TYPE_DATA_OFFSET:
        // Probably don't want/need to rely on the producer to have allocated an
        // offsets buffer of length 1 for a zero-size array
        array_view->buffer_views[i].n_bytes =
            (length != 0) * element_size_bytes * (length + 1);
        continue;
      case NANOARROW_BUFFER_TYPE_DATA:
        array_view->buffer_views[i].n_bytes =
            _ArrowRoundUpToMultipleOf8(array_view->layout.element_size_bits[i] * length) /
            8;
        continue;
      case NANOARROW_BUFFER_TYPE_TYPE_ID:
      case NANOARROW_BUFFER_TYPE_UNION_OFFSET:
        array_view->buffer_views[i].n_bytes = element_size_bytes * length;
        continue;
      case NANOARROW_BUFFER_TYPE_NONE:
        array_view->buffer_views[i].n_bytes = 0;
        continue;
    }
  }

  switch (array_view->storage_type) {
    case NANOARROW_TYPE_STRUCT:
    case NANOARROW_TYPE_SPARSE_UNION:
      for (int64_t i = 0; i < array_view->n_children; i++) {
        ArrowArrayViewSetLength(array_view->children[i], length);
      }
      break;
    case NANOARROW_TYPE_FIXED_SIZE_LIST:
      if (array_view->n_children >= 1) {
        ArrowArrayViewSetLength(array_view->children[0],
                                length * array_view->layout.child_size_elements);
      }
    default:
      break;
  }
}

ArrowErrorCode ArrowArrayViewSetArray(struct ArrowArrayView* array_view,
                                      struct ArrowArray* array,
                                      struct ArrowError* error) {
  array_view->array = array;
  ArrowArrayViewSetLength(array_view, array->offset + array->length);

  int64_t buffers_required = 0;
  for (int i = 0; i < 3; i++) {
    if (array_view->layout.buffer_type[i] == NANOARROW_BUFFER_TYPE_NONE) {
      break;
    }

    buffers_required++;

    // If the null_count is 0, the validity buffer can be NULL
    if (array_view->layout.buffer_type[i] == NANOARROW_BUFFER_TYPE_VALIDITY &&
        array->null_count == 0 && array->buffers[i] == NULL) {
      array_view->buffer_views[i].n_bytes = 0;
    }

    array_view->buffer_views[i].data.data = array->buffers[i];
  }

  if (buffers_required != array->n_buffers) {
    ArrowErrorSet(error, "Expected array with %d buffer(s) but found %d buffer(s)",
                  (int)buffers_required, (int)array->n_buffers);
    return EINVAL;
  }

  if (array_view->n_children != array->n_children) {
    return EINVAL;
  }

  // Check child sizes and calculate sizes that depend on data in the array buffers
  int64_t last_offset;
  switch (array_view->storage_type) {
    case NANOARROW_TYPE_STRING:
    case NANOARROW_TYPE_BINARY:
      if (array_view->buffer_views[1].n_bytes != 0) {
        last_offset =
            array_view->buffer_views[1].data.as_int32[array->offset + array->length];
        array_view->buffer_views[2].n_bytes = last_offset;
      }
      break;
    case NANOARROW_TYPE_LARGE_STRING:
    case NANOARROW_TYPE_LARGE_BINARY:
      if (array_view->buffer_views[1].n_bytes != 0) {
        last_offset =
            array_view->buffer_views[1].data.as_int64[array->offset + array->length];
        array_view->buffer_views[2].n_bytes = last_offset;
      }
      break;
    case NANOARROW_TYPE_STRUCT:
      for (int64_t i = 0; i < array_view->n_children; i++) {
        if (array->children[i]->length < (array->offset + array->length)) {
          ArrowErrorSet(
              error,
              "Expected struct child %d to have length >= %ld but found child with "
              "length %ld",
              (int)(i + 1), (long)(array->offset + array->length),
              (long)array->children[i]->length);
          return EINVAL;
        }
      }
      break;
    case NANOARROW_TYPE_LIST:
      if (array->n_children != 1) {
        ArrowErrorSet(error,
                      "Expected 1 child of list array but found %d child arrays",
                      (int)array->n_children);
        return EINVAL;
      }

      if (array_view->buffer_views[1].n_bytes != 0) {
        last_offset =
            array_view->buffer_views[1].data.as_int32[array->offset + array->length];
        if (array->children[0]->length < last_offset) {
          ArrowErrorSet(
              error,
              "Expected child of list array with length >= %ld but found array with "
              "length %ld",
              (long)last_offset, (long)array->children[0]->length);
          return EINVAL;
        }
      }
      break;
    case NANOARROW_TYPE_LARGE_LIST:
      if (array->n_children != 1) {
        ArrowErrorSet(error,
                      "Expected 1 child of large list array but found %d child arrays",
                      (int)array->n_children);
        return EINVAL;
      }

      if (array_view->buffer_views[1].n_bytes != 0) {
        last_offset =
            array_view->buffer_views[1].data.as_int64[array->offset + array->length];
        if (array->children[0]->length < last_offset) {
          ArrowErrorSet(
              error,
              "Expected child of large list array with length >= %ld but found array "
              "with length %ld",
              (long)last_offset, (long)array->children[0]->length);
          return EINVAL;
        }
      }
      break;
    case NANOARROW_TYPE_FIXED_SIZE_LIST:
      if (array->n_children != 1) {
        ArrowErrorSet(error,
                      "Expected 1 child of fixed-size array but found %d child arrays",
                      (int)array->n_children);
        return EINVAL;
      }

      last_offset =
          (array->offset + array->length) * array_view->layout.child_size_elements;
      if (array->children[0]->length < last_offset) {
        ArrowErrorSet(
            error,
            "Expected child of fixed-size list array with length >= %ld but found array "
            "with length %ld",
            (long)last_offset, (long)array->children[0]->length);
        return EINVAL;
      }
      break;
    default:
      break;
  }

  for (int64_t i = 0; i < array_view->n_children; i++) {
    NANOARROW_RETURN_NOT_OK(
        ArrowArrayViewSetArray(array_view->children[i], array->children[i], error));
  }

  return NANOARROW_OK;
}
