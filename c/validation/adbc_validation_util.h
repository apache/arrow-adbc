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

#include <arrow-adbc/adbc.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <nanoarrow/nanoarrow.h>

#include "common/utils.h"

namespace adbc_validation {

// ------------------------------------------------------------
// ADBC helpers

std::optional<std::string> ConnectionGetOption(struct AdbcConnection* connection,
                                               std::string_view option,
                                               struct AdbcError* error);

std::optional<std::string> StatementGetOption(struct AdbcStatement* statement,
                                              std::string_view option,
                                              struct AdbcError* error);

// ------------------------------------------------------------
// Helpers to print values

std::string StatusCodeToString(AdbcStatusCode code);
std::string ToString(struct AdbcError* error);
std::string ToString(struct ArrowError* error);
std::string ToString(struct ArrowArrayStream* stream);

// ------------------------------------------------------------
// Nanoarrow helpers

#define NULLABLE true
#define NOT_NULL false

// ------------------------------------------------------------
// Helper to manage C Data Interface/Nanoarrow resources with RAII

template <typename T>
struct Initializer {
  static void Initialize(T* value) { memset(value, 0, sizeof(T)); }
};

template <typename T>
struct Releaser {
  static void Release(T* value) {
    if (value->release) {
      value->release(value);
    }
  }
};

template <>
struct Initializer<struct ArrowBuffer> {
  static void Initialize(struct ArrowBuffer* value) { ArrowBufferInit(value); }
};

template <>
struct Releaser<struct ArrowBuffer> {
  static void Release(struct ArrowBuffer* buffer) { ArrowBufferReset(buffer); }
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

  Handle() { Initializer<Resource>::Initialize(&value); }

  ~Handle() { Releaser<Resource>::Release(&value); }

  Resource* operator->() { return &value; }
};

// ------------------------------------------------------------
// GTest/GMock helpers

#define CHECK_OK(EXPR)                                              \
  do {                                                              \
    if (auto adbc_status = (EXPR); adbc_status != ADBC_STATUS_OK) { \
      return adbc_status;                                           \
    }                                                               \
  } while (false)

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

/// \brief Read an AdbcGetInfoData struct with RAII safety
struct GetObjectsReader {
  explicit GetObjectsReader(struct ArrowArrayView* array_view) {
    // TODO: this swallows any construction errors
    get_objects_data_ = InternalAdbcGetObjectsDataInit(array_view);
  }
  ~GetObjectsReader() { InternalAdbcGetObjectsDataDelete(get_objects_data_); }

  struct AdbcGetObjectsData* operator*() { return get_objects_data_; }
  struct AdbcGetObjectsData* operator->() { return get_objects_data_; }

 private:
  struct AdbcGetObjectsData* get_objects_data_;
};

struct SchemaField {
  std::string name;
  ArrowType type = NANOARROW_TYPE_UNINITIALIZED;
  int32_t fixed_size = 0;
  bool nullable = true;
  std::vector<SchemaField> children;

  SchemaField(std::string name, ArrowType type, bool nullable)
      : name(std::move(name)), type(type), nullable(nullable) {}

  SchemaField(std::string name, ArrowType type)
      : SchemaField(std::move(name), type, /*nullable=*/true) {}

  static SchemaField Nested(std::string name, ArrowType type,
                            std::vector<SchemaField> children) {
    SchemaField out(name, type);
    out.children = std::move(children);
    return out;
  }

  static SchemaField FixedSize(std::string name, ArrowType type, int32_t fixed_size,
                               std::vector<SchemaField> children = {}) {
    SchemaField out = Nested(name, type, std::move(children));
    out.fixed_size = fixed_size;
    return out;
  }
};

/// \brief Make a schema from a vector of (name, type, nullable) tuples.
int MakeSchema(struct ArrowSchema* schema, const std::vector<SchemaField>& fields);

/// \brief Make an array from a column of C types.
template <typename T>
int MakeArray(struct ArrowArray* parent, struct ArrowArray* array,
              const std::vector<std::optional<T>>& values) {
  for (const auto& v : values) {
    if (v.has_value()) {
      if constexpr (std::is_same<T, bool>::value || std::is_same<T, int8_t>::value ||
                    std::is_same<T, int16_t>::value || std::is_same<T, int32_t>::value ||
                    std::is_same<T, int64_t>::value) {
        CHECK_OK(ArrowArrayAppendInt(array, *v));
        // XXX: cpplint gets weird here and thinks this is an unbraced if
      } else if constexpr (std::is_same<T,  // NOLINT(readability/braces)
                                        uint8_t>::value ||
                           std::is_same<T, uint16_t>::value ||
                           std::is_same<T, uint32_t>::value ||
                           std::is_same<T, uint64_t>::value) {
        CHECK_OK(ArrowArrayAppendUInt(array, *v));
      } else if constexpr (std::is_same<T, float>::value ||  // NOLINT(readability/braces)
                           std::is_same<T, double>::value) {
        CHECK_OK(ArrowArrayAppendDouble(array, *v));
      } else if constexpr (std::is_same<T, std::string>::value) {
        struct ArrowBufferView view;
        view.data.as_char = v->c_str();
        view.size_bytes = v->size();
        CHECK_OK(ArrowArrayAppendBytes(array, view));
      } else if constexpr (std::is_same<T, std::vector<std::byte>>::value) {
        static_assert(std::is_same_v<uint8_t, unsigned char>);
        struct ArrowBufferView view;
        view.data.as_uint8 = reinterpret_cast<const uint8_t*>(v->data());
        view.size_bytes = v->size();
        CHECK_OK(ArrowArrayAppendBytes(array, view));
      } else if constexpr (std::is_same<T, ArrowInterval*>::value) {
        CHECK_OK(ArrowArrayAppendInterval(array, *v));
      } else if constexpr (std::is_same<T, ArrowDecimal*>::value) {
        CHECK_OK(ArrowArrayAppendDecimal(array, *v));
      } else if constexpr (
          // Possibly a more effective way to do this using template magic
          // Not included but possible are the std::optional<> variants of this
          std::is_same<T, std::vector<bool>>::value ||
          std::is_same<T, std::vector<int8_t>>::value ||
          std::is_same<T, std::vector<int16_t>>::value ||
          std::is_same<T, std::vector<int32_t>>::value ||
          std::is_same<T, std::vector<int64_t>>::value ||
          std::is_same<T, std::vector<uint8_t>>::value ||
          std::is_same<T, std::vector<uint16_t>>::value ||
          std::is_same<T, std::vector<uint32_t>>::value ||
          std::is_same<T, std::vector<uint64_t>>::value ||
          std::is_same<T, std::vector<double>>::value ||
          std::is_same<T, std::vector<float>>::value ||
          std::is_same<T, std::vector<std::string>>::value ||
          std::is_same<T, std::vector<std::vector<std::byte>>>::value) {
        using child_t = typename T::value_type;
        std::vector<std::optional<child_t>> value_nullable;
        for (const auto& child_value : *v) {
          value_nullable.push_back(child_value);
        }
        CHECK_OK(MakeArray(array, array->children[0], value_nullable));
        CHECK_OK(ArrowArrayFinishElement(array));
      } else {
        static_assert(!sizeof(T), "Not yet implemented");
        return ENOTSUP;
      }
    } else {
      CHECK_OK(ArrowArrayAppendNull(array, 1));
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
  CHECK_OK(MakeArray<First>(batch, batch->children[i], first));
  return MakeBatchImpl(batch, i + 1, error, rest...);
}

/// \brief Make a batch from columns of C types.
template <typename... T>
int MakeBatch(struct ArrowArray* batch, struct ArrowError* error,
              const std::vector<std::optional<T>>&... columns) {
  CHECK_OK(ArrowArrayStartAppending(batch));
  CHECK_OK(MakeBatchImpl(batch, 0, error, columns...));
  for (size_t i = 0; i < static_cast<size_t>(batch->n_children); i++) {
    if (batch->length > 0 && batch->children[i]->length != batch->length) {
      ADD_FAILURE() << "Column lengths are inconsistent: column " << i << " has length "
                    << batch->children[i]->length;
      return EINVAL;
    }
    batch->length = batch->children[i]->length;
  }
  return ArrowArrayFinishBuildingDefault(batch, error);
}

template <typename... T>
int MakeBatch(struct ArrowSchema* schema, struct ArrowArray* batch,
              struct ArrowError* error, const std::vector<std::optional<T>>&... columns) {
  CHECK_OK(ArrowArrayInitFromSchema(batch, schema, error));
  return MakeBatch(batch, error, columns...);
}

/// \brief Make a stream from a list of batches.
void MakeStream(struct ArrowArrayStream* stream, struct ArrowSchema* schema,
                std::vector<struct ArrowArray> batches);

/// \brief Compare an array for equality against a vector of values.
template <typename T>
void CompareArray(struct ArrowArrayView* array,
                  const std::vector<std::optional<T>>& values, int64_t offset = 0,
                  int64_t length = -1) {
  if (length == -1) {
    length = array->length;
  }
  ASSERT_EQ(static_cast<int64_t>(values.size()), length);
  int64_t i = offset;
  for (const auto& v : values) {
    SCOPED_TRACE("Array index " + std::to_string(i));
    if (v.has_value()) {
      ASSERT_FALSE(ArrowArrayViewIsNull(array, i));
      if constexpr (std::is_same<T, float>::value || std::is_same<T, double>::value) {
        ASSERT_NE(array->buffer_views[1].data.data, nullptr);
        ASSERT_EQ(ArrowArrayViewGetDoubleUnsafe(array, i), *v);
      } else if constexpr (std::is_same<T, bool>::value ||
                           std::is_same<T, int8_t>::value ||
                           std::is_same<T, int16_t>::value ||
                           std::is_same<T, int32_t>::value ||
                           std::is_same<T, int64_t>::value) {
        ASSERT_NE(array->buffer_views[1].data.data, nullptr);
        ASSERT_EQ(ArrowArrayViewGetIntUnsafe(array, i), *v);
      } else if constexpr (std::is_same<T, uint8_t>::value ||
                           std::is_same<T, uint16_t>::value ||
                           std::is_same<T, uint32_t>::value ||
                           std::is_same<T, uint64_t>::value) {
        ASSERT_NE(array->buffer_views[1].data.data, nullptr);
        ASSERT_EQ(ArrowArrayViewGetUIntUnsafe(array, i), *v);
      } else if constexpr (std::is_same<T, std::string>::value) {
        struct ArrowStringView view = ArrowArrayViewGetStringUnsafe(array, i);
        std::string str(view.data, view.size_bytes);
        ASSERT_EQ(*v, str);
      } else if constexpr (std::is_same<T, std::vector<std::byte>>::value) {
        struct ArrowBufferView view = ArrowArrayViewGetBytesUnsafe(array, i);
        ASSERT_EQ(v->size(), view.size_bytes);
        for (int64_t byte_i = 0; byte_i < view.size_bytes; byte_i++) {
          ASSERT_EQ((*v)[byte_i], std::byte{view.data.as_uint8[byte_i]});
        }
      } else if constexpr (std::is_same<T, ArrowInterval*>::value) {
        ASSERT_NE(array->buffer_views[1].data.data, nullptr);
        struct ArrowInterval interval;
        ArrowIntervalInit(&interval, ArrowType::NANOARROW_TYPE_INTERVAL_MONTH_DAY_NANO);
        ArrowArrayViewGetIntervalUnsafe(array, i, &interval);

        ASSERT_EQ(interval.months, (*v)->months);
        ASSERT_EQ(interval.days, (*v)->days);
        ASSERT_EQ(interval.ns, (*v)->ns);

      } else if constexpr (
          // Possibly a more effective way to do this using template magic
          // Not included but possible are the std::optional<> variants of this
          std::is_same<T, std::vector<bool>>::value ||
          std::is_same<T, std::vector<int8_t>>::value ||
          std::is_same<T, std::vector<int16_t>>::value ||
          std::is_same<T, std::vector<int32_t>>::value ||
          std::is_same<T, std::vector<int64_t>>::value ||
          std::is_same<T, std::vector<uint8_t>>::value ||
          std::is_same<T, std::vector<uint16_t>>::value ||
          std::is_same<T, std::vector<uint32_t>>::value ||
          std::is_same<T, std::vector<uint64_t>>::value ||
          std::is_same<T, std::vector<double>>::value ||
          std::is_same<T, std::vector<float>>::value ||
          std::is_same<T, std::vector<std::string>>::value ||
          std::is_same<T, std::vector<std::vector<std::byte>>>::value) {
        using child_t = typename T::value_type;
        std::vector<std::optional<child_t>> value_nullable;
        for (const auto& child_value : *v) {
          value_nullable.push_back(child_value);
        }

        SCOPED_TRACE("List item");
        int64_t child_offset = ArrowArrayViewListChildOffset(array, i);
        int64_t child_length = ArrowArrayViewListChildOffset(array, i + 1) - child_offset;
        CompareArray<child_t>(array->children[0], value_nullable, child_offset,
                              child_length);
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
void CompareSchema(struct ArrowSchema* schema, const std::vector<SchemaField>& fields);

/// \brief Helper method to get the vendor version of a driver
std::string GetDriverVendorVersion(struct AdbcConnection* connection);

}  // namespace adbc_validation
