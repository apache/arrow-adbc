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
