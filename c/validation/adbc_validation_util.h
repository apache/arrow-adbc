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

// Utilities for testing with Nanoarrow.

#pragma once

#include <cstring>
#include <optional>
#include <string>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

#include <adbc.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <nanoarrow.h>

namespace adbc_validation {

// ------------------------------------------------------------
// Helpers to print values

std::string StatusCodeToString(AdbcStatusCode code);
std::string ToString(struct AdbcError* error);
std::string ToString(struct ArrowError* error);
std::string ToString(struct ArrowArrayStream* stream);

// ------------------------------------------------------------
// Nanoarrow helpers

/// \brief Get the array offset for a particular index
int64_t ArrowArrayViewGetOffsetUnsafe(struct ArrowArrayView* array_view, int64_t i);

// ------------------------------------------------------------
// Helper to manage C Data Interface/Nanoarrow resources with RAII

template <typename T>
struct Releaser {
  static void Release(T* value) {
    if (value->release) {
      value->release(value);
    }
  }
};

template <>
struct Releaser<struct ArrowArrayView> {
  static void Release(struct ArrowArrayView* value) {
    if (value->storage_type != NANOARROW_TYPE_UNINITIALIZED) {
      ArrowArrayViewReset(value);
    }
  }
};

template <>
struct Releaser<struct AdbcConnection> {
  static void Release(struct AdbcConnection* value) {
    if (value->private_data) {
      struct AdbcError error = {};
      auto status = AdbcConnectionRelease(value, &error);
      if (status != ADBC_STATUS_OK) {
        FAIL() << StatusCodeToString(status) << ": " << ToString(&error);
      }
    }
  }
};

template <>
struct Releaser<struct AdbcDatabase> {
  static void Release(struct AdbcDatabase* value) {
    if (value->private_data) {
      struct AdbcError error = {};
      auto status = AdbcDatabaseRelease(value, &error);
      if (status != ADBC_STATUS_OK) {
        FAIL() << StatusCodeToString(status) << ": " << ToString(&error);
      }
    }
  }
};

template <>
struct Releaser<struct AdbcStatement> {
  static void Release(struct AdbcStatement* value) {
    if (value->private_data) {
      struct AdbcError error = {};
      auto status = AdbcStatementRelease(value, &error);
      if (status != ADBC_STATUS_OK) {
        FAIL() << StatusCodeToString(status) << ": " << ToString(&error);
      }
    }
  }
};

template <typename Resource>
struct Handle {
  Resource value;

  Handle() { std::memset(&value, 0, sizeof(value)); }

  ~Handle() { Releaser<Resource>::Release(&value); }

  Resource* operator->() { return &value; }
};

// ------------------------------------------------------------
// GTest/GMock helpers

/// \brief A GTest matcher for Nanoarrow/C Data Interface error codes.
class IsErrno {
 public:
  using is_gtest_matcher = void;

  explicit IsErrno(int expected, struct ArrowArrayStream* stream,
                   struct ArrowError* error);
  bool MatchAndExplain(int errcode, std::ostream* os) const;
  void DescribeTo(std::ostream* os) const;
  void DescribeNegationTo(std::ostream* os) const;

 private:
  int expected_;
  struct ArrowArrayStream* stream_;
  struct ArrowError* error_;
};

::testing::Matcher<int> IsOkErrno();
::testing::Matcher<int> IsOkErrno(Handle<struct ArrowArrayStream>* stream);
::testing::Matcher<int> IsOkErrno(struct ArrowError* error);

/// \brief A GTest matcher for ADBC status codes
class IsAdbcStatusCode {
 public:
  using is_gtest_matcher = void;

  explicit IsAdbcStatusCode(AdbcStatusCode expected, struct AdbcError* error);
  bool MatchAndExplain(AdbcStatusCode actual, std::ostream* os) const;
  void DescribeTo(std::ostream* os) const;
  void DescribeNegationTo(std::ostream* os) const;

 private:
  AdbcStatusCode expected_;
  struct AdbcError* error_;
};

::testing::Matcher<AdbcStatusCode> IsOkStatus(struct AdbcError* error = nullptr);
::testing::Matcher<AdbcStatusCode> IsStatus(AdbcStatusCode code,
                                            struct AdbcError* error = nullptr);

/// \brief Read an ArrowArrayStream with RAII safety
struct StreamReader {
  Handle<struct ArrowArrayStream> stream;
  Handle<struct ArrowSchema> schema;
  Handle<struct ArrowArray> array;
  Handle<struct ArrowArrayView> array_view;
  std::vector<struct ArrowSchemaView> fields;
  struct ArrowError na_error;
  int64_t rows_affected = 0;

  StreamReader() { std::memset(&na_error, 0, sizeof(na_error)); }

  void GetSchema() {
    ASSERT_NE(nullptr, stream->release);
    ASSERT_THAT(stream->get_schema(&stream.value, &schema.value), IsOkErrno(&stream));
    fields.resize(schema->n_children);
    for (int64_t i = 0; i < schema->n_children; i++) {
      ASSERT_THAT(ArrowSchemaViewInit(&fields[i], schema->children[i], &na_error),
                  IsOkErrno(&na_error));
    }
  }

  void Next() { ASSERT_THAT(MaybeNext(), IsErrno(0, &stream.value, &na_error)); }

  int MaybeNext() {
    if (array->release) {
      ArrowArrayViewReset(&array_view.value);
      array->release(&array.value);
    }
    int err = stream->get_next(&stream.value, &array.value);
    if (err != 0) return err;
    if (array->release) {
      err = ArrowArrayViewInitFromSchema(&array_view.value, &schema.value, &na_error);
      if (err != 0) return err;
      err = ArrowArrayViewSetArray(&array_view.value, &array.value, &na_error);
      if (err != 0) return err;
    }
    return 0;
  }
};

struct SchemaField {
  std::string name;
  ArrowType type = NANOARROW_TYPE_UNINITIALIZED;
  bool nullable = true;

  SchemaField(std::string name, ArrowType type, bool nullable)
      : name(std::move(name)), type(type), nullable(nullable) {}

  SchemaField(std::string name, ArrowType type)
      : SchemaField(std::move(name), type, /*nullable=*/true) {}
};

/// \brief Make a schema from a vector of (name, type, nullable) tuples.
int MakeSchema(struct ArrowSchema* schema, const std::vector<SchemaField>& fields);

/// \brief Make an array from a column of C types.
template <typename T>
int MakeArray(struct ArrowArray* parent, struct ArrowArray* array,
              const std::vector<std::optional<T>>& values) {
  for (const auto& v : values) {
    if (v.has_value()) {
      if constexpr (std::is_same<T, int64_t>::value) {
        if (int errno_res = ArrowArrayAppendInt(array, *v); errno_res != 0) {
          return errno_res;
        }
      } else if constexpr (std::is_same<T, double>::value) {
        if (int errno_res = ArrowArrayAppendDouble(array, *v); errno_res != 0) {
          return errno_res;
        }
      } else if constexpr (std::is_same<T, std::string>::value) {
        struct ArrowStringView view;
        view.data = v->c_str();
        view.n_bytes = v->size();
        if (int errno_res = ArrowArrayAppendString(array, view); errno_res != 0) {
          return errno_res;
        }
      } else {
        static_assert(!sizeof(T), "Not yet implemented");
        return ENOTSUP;
      }
    } else {
      if (int errno_res = ArrowArrayAppendNull(array, 1); errno_res != 0) {
        return errno_res;
      }
    }
  }
  return 0;
}

template <typename First>
int MakeBatchImpl(struct ArrowArray* batch, size_t i, struct ArrowError* error,
                  const std::vector<std::optional<First>>& first) {
  return MakeArray<First>(batch, batch->children[i], first);
}

template <typename First, typename... Rest>
int MakeBatchImpl(struct ArrowArray* batch, size_t i, struct ArrowError* error,
                  const std::vector<std::optional<First>>& first,
                  const std::vector<std::optional<Rest>>&... rest) {
  if (int errno_res = MakeArray<First>(batch, batch->children[i], first);
      errno_res != 0) {
    return errno_res;
  }
  return MakeBatchImpl(batch, i + 1, error, rest...);
}

/// \brief Make a batch from columns of C types.
template <typename... T>
int MakeBatch(struct ArrowArray* batch, struct ArrowError* error,
              const std::vector<std::optional<T>>&... columns) {
  if (int errno_res = ArrowArrayStartAppending(batch); errno_res != 0) {
    return errno_res;
  }
  if (int errno_res = MakeBatchImpl(batch, 0, error, columns...); errno_res != 0) {
    return errno_res;
  }
  for (size_t i = 0; i < static_cast<size_t>(batch->n_children); i++) {
    if (batch->length > 0 && batch->children[i]->length != batch->length) {
      ADD_FAILURE() << "Column lengths are inconsistent: column " << i << " has length "
                    << batch->children[i]->length;
      return EINVAL;
    }
    batch->length = batch->children[i]->length;
  }
  return ArrowArrayFinishBuilding(batch, error);
}

template <typename... T>
int MakeBatch(struct ArrowSchema* schema, struct ArrowArray* batch,
              struct ArrowError* error, const std::vector<std::optional<T>>&... columns) {
  if (int errno_res = ArrowArrayInitFromSchema(batch, schema, error); errno_res != 0) {
    return errno_res;
  }
  return MakeBatch(batch, error, columns...);
}

/// \brief Make a stream from a list of batches.
void MakeStream(struct ArrowArrayStream* stream, struct ArrowSchema* schema,
                std::vector<struct ArrowArray> batches);

/// \brief Compare an array for equality against a vector of values.
template <typename T>
void CompareArray(struct ArrowArrayView* array,
                  const std::vector<std::optional<T>>& values) {
  ASSERT_EQ(static_cast<int64_t>(values.size()), array->array->length);
  int64_t i = 0;
  for (const auto& v : values) {
    SCOPED_TRACE("Array index " + std::to_string(i));
    if (v.has_value()) {
      ASSERT_FALSE(ArrowArrayViewIsNull(array, i));
      if constexpr (std::is_same<T, double>::value) {
        ASSERT_NE(array->buffer_views[1].data.data, nullptr);
        ASSERT_EQ(*v, array->buffer_views[1].data.as_double[i]);
      } else if constexpr (std::is_same<T, float>::value) {
        ASSERT_NE(array->buffer_views[1].data.data, nullptr);
        ASSERT_EQ(*v, array->buffer_views[1].data.as_float[i]);
      } else if constexpr (std::is_same<T, int32_t>::value) {
        ASSERT_NE(array->buffer_views[1].data.data, nullptr);
        ASSERT_EQ(*v, array->buffer_views[1].data.as_int32[i]);
      } else if constexpr (std::is_same<T, int64_t>::value) {
        ASSERT_NE(array->buffer_views[1].data.data, nullptr);
        ASSERT_EQ(*v, array->buffer_views[1].data.as_int64[i]);
      } else if constexpr (std::is_same<T, std::string>::value) {
        struct ArrowStringView view = ArrowArrayViewGetStringUnsafe(array, i);
        std::string str(view.data, view.n_bytes);
        ASSERT_EQ(*v, str);
      } else {
        static_assert(!sizeof(T), "Not yet implemented");
      }
    } else {
      ASSERT_TRUE(ArrowArrayViewIsNull(array, i));
    }
    i++;
  }
}

/// \brief Compare a schema for equality against a vector of (name,
///   type, nullable) tuples.
void CompareSchema(
    struct ArrowSchema* schema,
    const std::vector<std::tuple<std::optional<std::string>, ArrowType, bool>>& fields);

}  // namespace adbc_validation
