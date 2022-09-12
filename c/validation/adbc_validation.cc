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

#include "adbc_validation.h"

#include <cerrno>
#include <cstring>
#include <iostream>
#include <optional>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include <adbc.h>
#include <gmock/gmock.h>
#include <gtest/gtest-matchers.h>
#include <gtest/gtest.h>
#include <nanoarrow.h>

namespace adbc_validation {

namespace {
const char* AdbcvStatusCodeMessage(AdbcStatusCode code) {
#define CASE(CONSTANT)         \
  case ADBC_STATUS_##CONSTANT: \
    return ADBCV_STRINGIFY_VALUE(ADBC_STATUS_##CONSTANT) " (" #CONSTANT ")";

  switch (code) {
    CASE(OK);
    CASE(UNKNOWN);
    CASE(NOT_IMPLEMENTED);
    CASE(NOT_FOUND);
    CASE(ALREADY_EXISTS);
    CASE(INVALID_ARGUMENT);
    CASE(INVALID_STATE);
    CASE(INVALID_DATA);
    CASE(INTEGRITY);
    CASE(INTERNAL);
    CASE(IO);
    CASE(CANCELLED);
    CASE(TIMEOUT);
    CASE(UNAUTHENTICATED);
    CASE(UNAUTHORIZED);
    default:
      return "(invalid code)";
  }
#undef CASE
}

/// Nanoarrow helpers

#define NULLABLE true
#define NOT_NULL false

static inline int64_t ArrowArrayViewGetOffsetUnsafe(struct ArrowArrayView* array_view,
                                                    int64_t i) {
  struct ArrowBufferView* data_view = &array_view->buffer_views[1];
  i += array_view->array->offset;
  switch (array_view->storage_type) {
    case NANOARROW_TYPE_LIST:
    case NANOARROW_TYPE_MAP:
      return data_view->data.as_int32[i];
    case NANOARROW_TYPE_LARGE_LIST:
      return data_view->data.as_int64[i];
    default:
      return INT64_MAX;
  }
}

/// Assertion helpers

std::string AdbcvErrorRepr(AdbcStatusCode v) { return AdbcvStatusCodeMessage(v); }
std::string AdbcvErrorRepr(int v) {
  if (v == 0) {
    return "0 (OK)";
  }
  return std::to_string(v) + " (" + std::strerror(v) + ")";
}
std::string AdbcvErrorRepr(struct AdbcError* error) {
  if (error && error->message) {
    std::string result = "\nError message: ";
    result += error->message;
    error->release(error);
    return result;
  }
  return "";
}
std::string AdbcvErrorRepr(struct ArrowArrayStream* stream) {
  if (stream && stream->release) {
    std::string result = "\nError message: ";
    result += stream->get_last_error(stream);
    return result;
  }
  return "";
}
std::string AdbcvErrorRepr(struct ArrowError* error) {
  if (error) {
    std::string result = "\nError message: ";
    result += error->message;
    return result;
  }
  return "";
}

template <typename T, typename U>
std::string AdbcvErrorRepr(T expected, T actual, U* error) {
  std::string message = "Expected: ";
  message += AdbcvErrorRepr(expected);
  message += "\nActual:   ";
  message += AdbcvErrorRepr(actual);
  message += AdbcvErrorRepr(error);
  return message;
}

#define ADBCV_CONCAT(a, b) a##b
#define ADBCV_NAME(a, b) ADBCV_CONCAT(a, b)

/// Assertion helpers

#define CHECK_OK(EXPR)                                              \
  do {                                                              \
    if (auto adbc_status = (EXPR); adbc_status != ADBC_STATUS_OK) { \
      return adbc_status;                                           \
    }                                                               \
  } while (false)

#define ASSERT_FAILS_WITH_IMPL(ERROR, STATUS, EXPECTED, EXPR)                      \
  AdbcStatusCode STATUS = (EXPR);                                                  \
  ASSERT_EQ(EXPECTED, STATUS) << AdbcvErrorRepr<AdbcStatusCode, struct AdbcError>( \
      EXPECTED, STATUS, ERROR);
#define ASSERT_FAILS_IMPL(ERROR, STATUS, EXPR) \
  AdbcStatusCode STATUS = (EXPR);              \
  ASSERT_NE(ADBC_STATUS_OK, STATUS) << "Expected failure, but was actually OK";
#define ASSERT_OK(ERROR, EXPR)                                                         \
  ASSERT_FAILS_WITH_IMPL(ERROR, ADBCV_NAME(adbc_status_, __COUNTER__), ADBC_STATUS_OK, \
                         EXPR)
#define ASSERT_FAILS_WITH(STATUS, ERROR, EXPR)                         \
  ASSERT_FAILS_WITH_IMPL(ERROR, ADBCV_NAME(adbc_status_, __COUNTER__), \
                         ADBCV_CONCAT(ADBC_STATUS_, STATUS), EXPR)
#define ASSERT_FAILS(ERROR, EXPR) \
  ASSERT_FAILS_IMPL(ERROR, ADBCV_NAME(adbc_status_, __COUNTER__), EXPR)

#define ABI_ASSERT_FAILS_WITH_IMPL(STREAM, ERRNO, EXPECTED, EXPR)             \
  const int ERRNO = (EXPR);                                                   \
  ASSERT_EQ(EXPECTED, ERRNO) << AdbcvErrorRepr<int, struct ArrowArrayStream>( \
      EXPECTED, ERRNO, STREAM);
#define ABI_ASSERT_OK(STREAM, EXPR) \
  ABI_ASSERT_FAILS_WITH_IMPL(STREAM, ADBCV_NAME(adbc_errno_, __COUNTER__), 0, EXPR)
#define ABI_ASSERT_FAILS_WITH(ERRNO, STREAM, EXPR) \
  ABI_ASSERT_FAILS_WITH_IMPL(STREAM, ADBCV_NAME(adbc_errno_, __COUNTER__), ERRNO, EXPR)

#define NA_ASSERT_FAILS_WITH_IMPL(STREAM, ERRNO, EXPECTED, EXPR)                        \
  const int ERRNO = (EXPR);                                                             \
  ASSERT_EQ(EXPECTED, ERRNO) << AdbcvErrorRepr<int, struct ArrowError>(EXPECTED, ERRNO, \
                                                                       STREAM);
#define NA_ASSERT_OK(STREAM, EXPR) \
  NA_ASSERT_FAILS_WITH_IMPL(STREAM, ADBCV_NAME(adbc_errno_, __COUNTER__), 0, EXPR)
#define NA_ASSERT_FAILS_WITH(ERRNO, STREAM, EXPR) \
  NA_ASSERT_FAILS_WITH_IMPL(STREAM, ADBCV_NAME(adbc_errno_, __COUNTER__), ERRNO, EXPR)

/// Helper to manage C Data Interface/Nanoarrow resources with RAII

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
        FAIL() << AdbcvErrorRepr(status) << ": " << AdbcvErrorRepr(&error);
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
        FAIL() << AdbcvErrorRepr(status) << ": " << AdbcvErrorRepr(&error);
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
    ABI_ASSERT_OK(&stream.value, stream->get_schema(&stream.value, &schema.value));
    fields.resize(schema->n_children);
    for (int64_t i = 0; i < schema->n_children; i++) {
      NA_ASSERT_OK(&na_error,
                   ArrowSchemaViewInit(&fields[i], schema->children[i], &na_error));
    }
  }

  void Next() {
    if (array->release) {
      ArrowArrayViewReset(&array_view.value);
      array->release(&array.value);
    }
    ABI_ASSERT_OK(&stream.value, stream->get_next(&stream.value, &array.value));
    if (array->release) {
      NA_ASSERT_OK(&na_error, ArrowArrayViewInitFromSchema(&array_view.value,
                                                           &schema.value, &na_error));
      NA_ASSERT_OK(&na_error,
                   ArrowArrayViewSetArray(&array_view.value, &array.value, &na_error));
    }
  }
};

#define CHECK_ERRNO_IMPL(ERRNO, EXPR)   \
  if (int ERRNO = (EXPR); ERRNO != 0) { \
    return ERRNO;                       \
  }
#define CHECK_ERRNO(EXPR) CHECK_ERRNO_IMPL(ADBCV_NAME(errno_, __COUNTER__), EXPR)

int MakeSchema(struct ArrowSchema* schema,
               const std::vector<std::pair<std::string, ArrowType>>& fields) {
  CHECK_ERRNO(ArrowSchemaInit(schema, NANOARROW_TYPE_STRUCT));
  CHECK_ERRNO(ArrowSchemaAllocateChildren(schema, fields.size()));
  size_t i = 0;
  for (const std::pair<std::string, ArrowType>& field : fields) {
    CHECK_ERRNO(ArrowSchemaInit(schema->children[i], field.second));
    CHECK_ERRNO(ArrowSchemaSetName(schema->children[i], field.first.c_str()));
    i++;
  }
  return 0;
}

template <typename T>
int MakeArray(struct ArrowArray* parent, struct ArrowArray* array,
              const std::vector<std::optional<T>>& values) {
  for (const auto& v : values) {
    if (v.has_value()) {
      if constexpr (std::is_same<T, int64_t>::value) {
        CHECK_ERRNO(ArrowArrayAppendInt(array, *v));
      } else if constexpr (std::is_same<T, std::string>::value) {
        struct ArrowStringView view;
        view.data = v->c_str();
        view.n_bytes = v->size();
        CHECK_ERRNO(ArrowArrayAppendString(array, view));
      } else {
        static_assert(!sizeof(T), "Not yet implemented");
        return ENOTSUP;
      }
    } else {
      CHECK_ERRNO(ArrowArrayAppendNull(array, 1));
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
  CHECK_ERRNO(MakeArray<First>(batch, batch->children[i], first));
  return MakeBatchImpl(batch, i + 1, error, rest...);
}

template <typename... T>
int MakeBatch(struct ArrowArray* batch, struct ArrowError* error,
              const std::vector<std::optional<T>>&... columns) {
  CHECK_ERRNO(ArrowArrayStartAppending(batch));
  CHECK_ERRNO(MakeBatchImpl(batch, 0, error, columns...));
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
              struct ArrowError* error,
              const std::vector<std::pair<std::string, ArrowType>>& fields,
              const std::vector<std::optional<T>>&... columns) {
  CHECK_ERRNO(MakeSchema(schema, fields));
  CHECK_ERRNO(ArrowArrayInitFromSchema(batch, schema, error));
  return MakeBatch(batch, error, columns...);
}

class ConstantArrayStream {
 public:
  explicit ConstantArrayStream(struct ArrowSchema* schema,
                               std::vector<struct ArrowArray> batches)
      : batches_(std::move(batches)), next_index_(0) {
    schema_ = *schema;
    std::memset(schema, 0, sizeof(*schema));
  }

  static const char* GetLastError(struct ArrowArrayStream* stream) { return nullptr; }

  static int GetNext(struct ArrowArrayStream* stream, struct ArrowArray* out) {
    if (!stream || !stream->private_data || !out) return EINVAL;
    auto* self = reinterpret_cast<ConstantArrayStream*>(stream->private_data);
    if (self->next_index_ >= self->batches_.size()) {
      out->release = nullptr;
      return 0;
    }

    *out = self->batches_[self->next_index_];
    std::memset(&self->batches_[self->next_index_], 0, sizeof(struct ArrowArray));

    self->next_index_++;
    return 0;
  }

  static int GetSchema(struct ArrowArrayStream* stream, struct ArrowSchema* out) {
    if (!stream || !stream->private_data || !out) return EINVAL;
    auto* self = reinterpret_cast<ConstantArrayStream*>(stream->private_data);
    return ArrowSchemaDeepCopy(&self->schema_, out);
  }

  static void Release(struct ArrowArrayStream* stream) {
    if (!stream->private_data) return;
    auto* self = reinterpret_cast<ConstantArrayStream*>(stream->private_data);

    self->schema_.release(&self->schema_);
    for (size_t i = 0; i < self->batches_.size(); i++) {
      if (self->batches_[i].release) {
        self->batches_[i].release(&self->batches_[i]);
      }
    }

    delete self;
    std::memset(stream, 0, sizeof(*stream));
  }

 private:
  struct ArrowSchema schema_;
  std::vector<struct ArrowArray> batches_;
  size_t next_index_;
};

void MakeStream(struct ArrowArrayStream* stream, struct ArrowSchema* schema,
                std::vector<struct ArrowArray> batches) {
  stream->get_last_error = &ConstantArrayStream::GetLastError;
  stream->get_next = &ConstantArrayStream::GetNext;
  stream->get_schema = &ConstantArrayStream::GetSchema;
  stream->release = &ConstantArrayStream::Release;
  stream->private_data = new ConstantArrayStream(schema, std::move(batches));
}

void CompareSchema(
    struct ArrowSchema* schema,
    const std::vector<std::tuple<std::optional<std::string>, ArrowType, bool>>& fields) {
  struct ArrowError na_error;
  struct ArrowSchemaView view;

  NA_ASSERT_OK(&na_error, ArrowSchemaViewInit(&view, schema, &na_error));
  ASSERT_THAT(view.data_type, ::testing::AnyOf(NANOARROW_TYPE_LIST, NANOARROW_TYPE_STRUCT,
                                               NANOARROW_TYPE_DENSE_UNION));
  ASSERT_EQ(fields.size(), schema->n_children);

  for (int64_t i = 0; i < schema->n_children; i++) {
    SCOPED_TRACE("Field " + std::to_string(i));
    struct ArrowSchemaView field_view;
    NA_ASSERT_OK(&na_error,
                 ArrowSchemaViewInit(&field_view, schema->children[i], &na_error));
    ASSERT_EQ(std::get<1>(fields[i]), field_view.data_type);
    ASSERT_EQ(std::get<2>(fields[i]),
              (schema->children[i]->flags & ARROW_FLAG_NULLABLE) != 0)
        << "Nullability mismatch";
    if (std::get<0>(fields[i]).has_value()) {
      ASSERT_EQ(*std::get<0>(fields[i]), schema->children[i]->name);
    }
  }
}

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
        ASSERT_EQ(*v, array->buffer_views[1].data.as_double[i]);
      } else if constexpr (std::is_same<T, float>::value) {
        ASSERT_EQ(*v, array->buffer_views[1].data.as_float[i]);
      } else if constexpr (std::is_same<T, int32_t>::value) {
        ASSERT_EQ(*v, array->buffer_views[1].data.as_int32[i]);
      } else if constexpr (std::is_same<T, int64_t>::value) {
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

}  // namespace

//------------------------------------------------------------
// Tests of AdbcDatabase

void DatabaseTest::SetUpTest() {
  std::memset(&error, 0, sizeof(error));
  std::memset(&database, 0, sizeof(database));
}

void DatabaseTest::TearDownTest() {
  if (database.private_data) {
    ASSERT_OK(&error, AdbcDatabaseRelease(&database, &error));
  }
  if (error.release) {
    error.release(&error);
  }
}

void DatabaseTest::TestNewInit() {
  ASSERT_OK(&error, AdbcDatabaseNew(&database, &error));
  ASSERT_OK(&error, quirks()->SetupDatabase(&database, &error));
  ASSERT_OK(&error, AdbcDatabaseInit(&database, &error));
  ASSERT_NE(nullptr, database.private_data);
  ASSERT_OK(&error, AdbcDatabaseRelease(&database, &error));
  ASSERT_EQ(nullptr, database.private_data);

  ASSERT_FAILS_WITH(INVALID_STATE, &error, AdbcDatabaseRelease(&database, &error));
}

void DatabaseTest::TestRelease() {
  ASSERT_FAILS_WITH(INVALID_STATE, &error, AdbcDatabaseRelease(&database, &error));

  ASSERT_OK(&error, AdbcDatabaseNew(&database, &error));
  ASSERT_OK(&error, AdbcDatabaseRelease(&database, &error));
  ASSERT_EQ(nullptr, database.private_data);
}

//------------------------------------------------------------
// Tests of AdbcConnection

void ConnectionTest::SetUpTest() {
  std::memset(&error, 0, sizeof(error));
  std::memset(&database, 0, sizeof(database));
  std::memset(&connection, 0, sizeof(connection));

  ASSERT_OK(&error, AdbcDatabaseNew(&database, &error));
  ASSERT_OK(&error, quirks()->SetupDatabase(&database, &error));
  ASSERT_OK(&error, AdbcDatabaseInit(&database, &error));
}

void ConnectionTest::TearDownTest() {
  if (connection.private_data) {
    ASSERT_OK(&error, AdbcConnectionRelease(&connection, &error));
  }
  ASSERT_OK(&error, AdbcDatabaseRelease(&database, &error));
  if (error.release) {
    error.release(&error);
  }
}

void ConnectionTest::TestNewInit() {
  ASSERT_OK(&error, AdbcConnectionNew(&connection, &error));
  ASSERT_OK(&error, AdbcConnectionInit(&connection, &database, &error));
  ASSERT_OK(&error, AdbcConnectionRelease(&connection, &error));
  ASSERT_EQ(NULL, connection.private_data);

  ASSERT_FAILS_WITH(INVALID_STATE, &error, AdbcConnectionRelease(&connection, &error));
}

void ConnectionTest::TestRelease() {
  ASSERT_FAILS_WITH(INVALID_STATE, &error, AdbcConnectionRelease(&connection, &error));

  ASSERT_OK(&error, AdbcConnectionNew(&connection, &error));
  ASSERT_OK(&error, AdbcConnectionRelease(&connection, &error));
  ASSERT_EQ(NULL, connection.private_data);

  // TODO: what should happen if we Release() with open connections?
}

void ConnectionTest::TestConcurrent() {
  struct AdbcConnection connection2;
  memset(&connection2, 0, sizeof(connection2));

  ASSERT_OK(&error, AdbcConnectionNew(&connection, &error));
  ASSERT_OK(&error, AdbcConnectionInit(&connection, &database, &error));

  ASSERT_OK(&error, AdbcConnectionNew(&connection2, &error));
  ASSERT_OK(&error, AdbcConnectionInit(&connection2, &database, &error));

  ASSERT_OK(&error, AdbcConnectionRelease(&connection, &error));
  ASSERT_OK(&error, AdbcConnectionRelease(&connection2, &error));
}

//------------------------------------------------------------
// Tests of autocommit (without data)

void ConnectionTest::TestAutocommitDefault() {
  ASSERT_OK(&error, AdbcConnectionNew(&connection, &error));
  ASSERT_OK(&error, AdbcConnectionInit(&connection, &database, &error));

  // Even if not supported, the driver should act as if autocommit is
  // enabled, and return INVALID_STATE if the client tries to commit
  // or rollback
  ASSERT_FAILS_WITH(INVALID_STATE, &error, AdbcConnectionCommit(&connection, &error));
  ASSERT_FAILS_WITH(INVALID_STATE, &error, AdbcConnectionRollback(&connection, &error));

  // Invalid option value
  ASSERT_FAILS(&error,
               AdbcConnectionSetOption(&connection, ADBC_CONNECTION_OPTION_AUTOCOMMIT,
                                       "invalid", &error));
}

void ConnectionTest::TestAutocommitToggle() {
  ASSERT_OK(&error, AdbcConnectionNew(&connection, &error));
  ASSERT_OK(&error, AdbcConnectionInit(&connection, &database, &error));

  // It is OK to enable autocommit when it is already enabled
  ASSERT_OK(&error,
            AdbcConnectionSetOption(&connection, ADBC_CONNECTION_OPTION_AUTOCOMMIT,
                                    ADBC_OPTION_VALUE_ENABLED, &error));
  ASSERT_OK(&error,
            AdbcConnectionSetOption(&connection, ADBC_CONNECTION_OPTION_AUTOCOMMIT,
                                    ADBC_OPTION_VALUE_DISABLED, &error));
  // It is OK to disable autocommit when it is already enabled
  ASSERT_OK(&error,
            AdbcConnectionSetOption(&connection, ADBC_CONNECTION_OPTION_AUTOCOMMIT,
                                    ADBC_OPTION_VALUE_DISABLED, &error));
}

//------------------------------------------------------------
// Tests of metadata

void IngestSampleTable(struct AdbcConnection* connection, struct AdbcError* error) {
  Handle<struct ArrowSchema> schema;
  Handle<struct ArrowArray> array;
  struct ArrowError na_error;
  NA_ASSERT_OK(nullptr,
               (MakeBatch<int64_t, std::string>(
                   &schema.value, &array.value, &na_error,
                   {{"int64s", NANOARROW_TYPE_INT64}, {"strings", NANOARROW_TYPE_STRING}},
                   {42, -42, std::nullopt}, {"foo", std::nullopt, ""})));

  struct AdbcStatement statement;
  std::memset(&statement, 0, sizeof(statement));
  ASSERT_OK(error, AdbcStatementNew(connection, &statement, error));
  ASSERT_OK(error, AdbcStatementSetOption(&statement, ADBC_INGEST_OPTION_TARGET_TABLE,
                                          "bulk_ingest", error));
  ASSERT_OK(error, AdbcStatementBind(&statement, &array.value, &schema.value, error));
  ASSERT_OK(error, AdbcStatementExecuteQuery(&statement, nullptr, nullptr, error));
  ASSERT_OK(error, AdbcStatementRelease(&statement, error));
}

void ConnectionTest::TestMetadataGetInfo() {
  ASSERT_OK(&error, AdbcConnectionNew(&connection, &error));
  ASSERT_OK(&error, AdbcConnectionInit(&connection, &database, &error));

  StreamReader reader;
  std::vector<uint32_t> info = {
      ADBC_INFO_DRIVER_NAME,
      ADBC_INFO_DRIVER_VERSION,
      ADBC_INFO_VENDOR_NAME,
      ADBC_INFO_VENDOR_VERSION,
  };

  ASSERT_OK(&error, AdbcConnectionGetInfo(&connection, info.data(), info.size(),
                                          &reader.stream.value, &error));
  ASSERT_NO_FATAL_FAILURE(reader.GetSchema());
  ASSERT_NO_FATAL_FAILURE(CompareSchema(
      &reader.schema.value, {
                                {"info_name", NANOARROW_TYPE_UINT32, NOT_NULL},
                                {"info_value", NANOARROW_TYPE_DENSE_UNION, NULLABLE},
                            }));
  ASSERT_NO_FATAL_FAILURE(
      CompareSchema(reader.schema->children[1],
                    {
                        {"string_value", NANOARROW_TYPE_STRING, NULLABLE},
                        {"bool_value", NANOARROW_TYPE_BOOL, NULLABLE},
                        {"int64_value", NANOARROW_TYPE_INT64, NULLABLE},
                        {"int32_bitmask", NANOARROW_TYPE_INT32, NULLABLE},
                        {"string_list", NANOARROW_TYPE_LIST, NULLABLE},
                        {"int32_to_int32_list_map", NANOARROW_TYPE_MAP, NULLABLE},
                    }));

  std::vector<uint32_t> seen;
  while (true) {
    ASSERT_NO_FATAL_FAILURE(reader.Next());
    if (!reader.array->release) break;

    for (int64_t row = 0; row < reader.array->length; row++) {
      ASSERT_FALSE(ArrowArrayViewIsNull(reader.array_view->children[0], row));
      const uint32_t code =
          reader.array_view->children[0]->buffer_views[1].data.as_uint32[row];
      seen.push_back(code);

      switch (code) {
        case ADBC_INFO_DRIVER_NAME:
        case ADBC_INFO_DRIVER_VERSION:
        case ADBC_INFO_VENDOR_NAME:
        case ADBC_INFO_VENDOR_VERSION:
          // UTF8
          ASSERT_EQ(uint8_t(0),
                    reader.array_view->children[1]->buffer_views[0].data.as_uint8[row]);
        default:
          // Ignored
          break;
      }
    }
  }
  ASSERT_THAT(seen, ::testing::UnorderedElementsAreArray(info));
}

void ConnectionTest::TestMetadataGetTableSchema() {
  ASSERT_OK(&error, AdbcConnectionNew(&connection, &error));
  ASSERT_OK(&error, AdbcConnectionInit(&connection, &database, &error));
  ASSERT_OK(&error, quirks()->DropTable(&connection, "bulk_ingest", &error));
  ASSERT_NO_FATAL_FAILURE(IngestSampleTable(&connection, &error));

  Handle<ArrowSchema> schema;
  ASSERT_OK(&error, AdbcConnectionGetTableSchema(&connection, /*catalog=*/nullptr,
                                                 /*db_schema=*/nullptr, "bulk_ingest",
                                                 &schema.value, &error));

  ASSERT_NO_FATAL_FAILURE(
      CompareSchema(&schema.value, {{"int64s", NANOARROW_TYPE_INT64, NULLABLE},
                                    {"strings", NANOARROW_TYPE_STRING, NULLABLE}}));
}

void ConnectionTest::TestMetadataGetTableTypes() {
  ASSERT_OK(&error, AdbcConnectionNew(&connection, &error));
  ASSERT_OK(&error, AdbcConnectionInit(&connection, &database, &error));

  StreamReader reader;
  ASSERT_OK(&error,
            AdbcConnectionGetTableTypes(&connection, &reader.stream.value, &error));
  ASSERT_NO_FATAL_FAILURE(reader.GetSchema());
  ASSERT_NO_FATAL_FAILURE(CompareSchema(
      &reader.schema.value, {{"table_type", NANOARROW_TYPE_STRING, NOT_NULL}}));
  ASSERT_NO_FATAL_FAILURE(reader.Next());
}

void CheckGetObjectsSchema(struct ArrowSchema* schema) {
  ASSERT_NO_FATAL_FAILURE(
      CompareSchema(schema, {
                                {"catalog_name", NANOARROW_TYPE_STRING, NULLABLE},
                                {"catalog_db_schemas", NANOARROW_TYPE_LIST, NULLABLE},
                            }));
  struct ArrowSchema* db_schema_schema = schema->children[1]->children[0];
  ASSERT_NO_FATAL_FAILURE(CompareSchema(
      db_schema_schema, {
                            {"db_schema_name", NANOARROW_TYPE_STRING, NULLABLE},
                            {"db_schema_tables", NANOARROW_TYPE_LIST, NULLABLE},
                        }));
  struct ArrowSchema* table_schema = db_schema_schema->children[1]->children[0];
  ASSERT_NO_FATAL_FAILURE(CompareSchema(
      table_schema, {
                        {"table_name", NANOARROW_TYPE_STRING, NOT_NULL},
                        {"table_type", NANOARROW_TYPE_STRING, NOT_NULL},
                        {"table_columns", NANOARROW_TYPE_LIST, NULLABLE},
                        {"table_constraints", NANOARROW_TYPE_LIST, NULLABLE},
                    }));
  struct ArrowSchema* column_schema = table_schema->children[2]->children[0];
  ASSERT_NO_FATAL_FAILURE(CompareSchema(
      column_schema, {
                         {"column_name", NANOARROW_TYPE_STRING, NOT_NULL},
                         {"ordinal_position", NANOARROW_TYPE_INT32, NULLABLE},
                         {"remarks", NANOARROW_TYPE_STRING, NULLABLE},
                         {"xdbc_data_type", NANOARROW_TYPE_INT16, NULLABLE},
                         {"xdbc_type_name", NANOARROW_TYPE_STRING, NULLABLE},
                         {"xdbc_column_size", NANOARROW_TYPE_INT32, NULLABLE},
                         {"xdbc_decimal_digits", NANOARROW_TYPE_INT16, NULLABLE},
                         {"xdbc_num_prec_radix", NANOARROW_TYPE_INT16, NULLABLE},
                         {"xdbc_nullable", NANOARROW_TYPE_INT16, NULLABLE},
                         {"xdbc_column_def", NANOARROW_TYPE_STRING, NULLABLE},
                         {"xdbc_sql_data_type", NANOARROW_TYPE_INT16, NULLABLE},
                         {"xdbc_datetime_sub", NANOARROW_TYPE_INT16, NULLABLE},
                         {"xdbc_char_octet_length", NANOARROW_TYPE_INT32, NULLABLE},
                         {"xdbc_is_nullable", NANOARROW_TYPE_STRING, NULLABLE},
                         {"xdbc_scope_catalog", NANOARROW_TYPE_STRING, NULLABLE},
                         {"xdbc_scope_schema", NANOARROW_TYPE_STRING, NULLABLE},
                         {"xdbc_scope_table", NANOARROW_TYPE_STRING, NULLABLE},
                         {"xdbc_is_autoincrement", NANOARROW_TYPE_BOOL, NULLABLE},
                         {"xdbc_is_generatedcolumn", NANOARROW_TYPE_BOOL, NULLABLE},
                     }));

  struct ArrowSchema* constraint_schema = table_schema->children[3]->children[0];
  ASSERT_NO_FATAL_FAILURE(CompareSchema(
      constraint_schema, {
                             {"constraint_name", NANOARROW_TYPE_STRING, NULLABLE},
                             {"constraint_type", NANOARROW_TYPE_STRING, NOT_NULL},
                             {"constraint_column_names", NANOARROW_TYPE_LIST, NOT_NULL},
                             {"constraint_column_usage", NANOARROW_TYPE_LIST, NULLABLE},
                         }));
  ASSERT_NO_FATAL_FAILURE(CompareSchema(
      constraint_schema->children[2], {
                                          {std::nullopt, NANOARROW_TYPE_STRING, NULLABLE},
                                      }));

  struct ArrowSchema* usage_schema = constraint_schema->children[3]->children[0];
  ASSERT_NO_FATAL_FAILURE(
      CompareSchema(usage_schema, {
                                      {"fk_catalog", NANOARROW_TYPE_STRING, NULLABLE},
                                      {"fk_db_schema", NANOARROW_TYPE_STRING, NULLABLE},
                                      {"fk_table", NANOARROW_TYPE_STRING, NULLABLE},
                                      {"fk_column_name", NANOARROW_TYPE_STRING, NULLABLE},
                                  }));
}

void ConnectionTest::TestMetadataGetObjectsCatalogs() {
  ASSERT_OK(&error, AdbcConnectionNew(&connection, &error));
  ASSERT_OK(&error, AdbcConnectionInit(&connection, &database, &error));

  {
    StreamReader reader;
    ASSERT_OK(&error, AdbcConnectionGetObjects(&connection, ADBC_OBJECT_DEPTH_CATALOGS,
                                               nullptr, nullptr, nullptr, nullptr,
                                               nullptr, &reader.stream.value, &error));
    ASSERT_NO_FATAL_FAILURE(reader.GetSchema());
    ASSERT_NO_FATAL_FAILURE(CheckGetObjectsSchema(&reader.schema.value));
    // We requested catalogs, so expect at least one catalog, and
    // 'catalog_db_schemas' should be null
    ASSERT_NO_FATAL_FAILURE(reader.Next());
    ASSERT_NE(nullptr, reader.array->release);
    ASSERT_GT(reader.array->length, 0);
    do {
      for (int64_t row = 0; row < reader.array->length; row++) {
        ASSERT_TRUE(ArrowArrayViewIsNull(reader.array_view->children[1], row))
            << "Row " << row << " should have null catalog_db_schemas";
      }
      ASSERT_NO_FATAL_FAILURE(reader.Next());
    } while (reader.array->release);
  }

  {
    // Filter with a nonexistent catalog - we should get nothing
    StreamReader reader;
    ASSERT_OK(&error,
              AdbcConnectionGetObjects(&connection, ADBC_OBJECT_DEPTH_CATALOGS,
                                       "this catalog does not exist", nullptr, nullptr,
                                       nullptr, nullptr, &reader.stream.value, &error));
    ASSERT_NO_FATAL_FAILURE(reader.GetSchema());
    ASSERT_NO_FATAL_FAILURE(CheckGetObjectsSchema(&reader.schema.value));
    ASSERT_NO_FATAL_FAILURE(reader.Next());
    if (reader.array->release) {
      ASSERT_EQ(0, reader.array->length);
      ASSERT_NO_FATAL_FAILURE(reader.Next());
      ASSERT_EQ(nullptr, reader.array->release);
    }
  }
}

void ConnectionTest::TestMetadataGetObjectsDbSchemas() {
  ASSERT_OK(&error, AdbcConnectionNew(&connection, &error));
  ASSERT_OK(&error, AdbcConnectionInit(&connection, &database, &error));

  {
    // Expect at least one catalog, at least one schema, and tables should be null
    StreamReader reader;
    ASSERT_OK(&error, AdbcConnectionGetObjects(&connection, ADBC_OBJECT_DEPTH_DB_SCHEMAS,
                                               nullptr, nullptr, nullptr, nullptr,
                                               nullptr, &reader.stream.value, &error));
    ASSERT_NO_FATAL_FAILURE(reader.GetSchema());
    ASSERT_NO_FATAL_FAILURE(CheckGetObjectsSchema(&reader.schema.value));
    ASSERT_NO_FATAL_FAILURE(reader.Next());
    ASSERT_NE(nullptr, reader.array->release);
    ASSERT_GT(reader.array->length, 0);
    do {
      for (int64_t row = 0; row < reader.array->length; row++) {
        // type: list<db_schema_schema>
        struct ArrowArrayView* catalog_db_schemas_list = reader.array_view->children[1];
        // type: db_schema_schema (struct)
        struct ArrowArrayView* catalog_db_schemas = catalog_db_schemas_list->children[0];
        // type: list<table_schema>
        struct ArrowArrayView* db_schema_tables_list = catalog_db_schemas->children[1];

        ASSERT_FALSE(ArrowArrayViewIsNull(catalog_db_schemas_list, row))
            << "Row " << row << " should have non-null catalog_db_schemas";

        for (int64_t list_index =
                 ArrowArrayViewGetOffsetUnsafe(catalog_db_schemas_list, row);
             list_index < ArrowArrayViewGetOffsetUnsafe(catalog_db_schemas_list, row + 1);
             list_index++) {
          ASSERT_TRUE(ArrowArrayViewIsNull(db_schema_tables_list, row + list_index))
              << "Row " << row << " should have null db_schema_tables";
        }
      }
      ASSERT_NO_FATAL_FAILURE(reader.Next());
    } while (reader.array->release);
  }

  {
    // Filter with a nonexistent DB schema - we should get nothing
    StreamReader reader;
    ASSERT_OK(&error,
              AdbcConnectionGetObjects(&connection, ADBC_OBJECT_DEPTH_DB_SCHEMAS, nullptr,
                                       "this schema does not exist", nullptr, nullptr,
                                       nullptr, &reader.stream.value, &error));
    ASSERT_NO_FATAL_FAILURE(reader.GetSchema());
    ASSERT_NO_FATAL_FAILURE(CheckGetObjectsSchema(&reader.schema.value));
    ASSERT_NO_FATAL_FAILURE(reader.Next());
    ASSERT_NE(nullptr, reader.array->release);
    ASSERT_GT(reader.array->length, 0);
    do {
      for (int64_t row = 0; row < reader.array->length; row++) {
        struct ArrowArrayView* catalog_db_schemas_list = reader.array_view->children[1];
        ASSERT_FALSE(ArrowArrayViewIsNull(catalog_db_schemas_list, row))
            << "Row " << row << " should have non-null catalog_db_schemas";

        const int64_t start_offset =
            ArrowArrayViewGetOffsetUnsafe(catalog_db_schemas_list, row);
        const int64_t end_offset =
            ArrowArrayViewGetOffsetUnsafe(catalog_db_schemas_list, row + 1);
        ASSERT_EQ(start_offset, end_offset);
      }
      ASSERT_NO_FATAL_FAILURE(reader.Next());
    } while (reader.array->release);
  }
}

void ConnectionTest::TestMetadataGetObjectsTables() {
  ASSERT_OK(&error, AdbcConnectionNew(&connection, &error));
  ASSERT_OK(&error, AdbcConnectionInit(&connection, &database, &error));
  ASSERT_OK(&error, quirks()->DropTable(&connection, "bulk_ingest", &error));
  ASSERT_NO_FATAL_FAILURE(IngestSampleTable(&connection, &error));

  std::vector<std::pair<const char*, bool>> test_cases = {
      {nullptr, true}, {"bulk_%", true}, {"asdf%", false}};
  for (const auto& expected : test_cases) {
    std::string scope = "Filter: ";
    scope += expected.first ? expected.first : "(no filter)";
    scope += "; table should exist? ";
    scope += expected.second ? "true" : "false";
    SCOPED_TRACE(scope);

    StreamReader reader;
    ASSERT_OK(&error, AdbcConnectionGetObjects(&connection, ADBC_OBJECT_DEPTH_TABLES,
                                               nullptr, nullptr, expected.first, nullptr,
                                               nullptr, &reader.stream.value, &error));
    ASSERT_NO_FATAL_FAILURE(reader.GetSchema());
    ASSERT_NO_FATAL_FAILURE(CheckGetObjectsSchema(&reader.schema.value));
    ASSERT_NO_FATAL_FAILURE(reader.Next());
    ASSERT_NE(nullptr, reader.array->release);
    ASSERT_GT(reader.array->length, 0);
    bool found_expected_table = false;
    do {
      for (int64_t row = 0; row < reader.array->length; row++) {
        // type: list<db_schema_schema>
        struct ArrowArrayView* catalog_db_schemas_list = reader.array_view->children[1];
        // type: db_schema_schema (struct)
        struct ArrowArrayView* catalog_db_schemas = catalog_db_schemas_list->children[0];
        // type: list<table_schema>
        struct ArrowArrayView* db_schema_tables_list = catalog_db_schemas->children[1];
        // type: table_schema (struct)
        struct ArrowArrayView* db_schema_tables = db_schema_tables_list->children[0];

        ASSERT_FALSE(ArrowArrayViewIsNull(catalog_db_schemas_list, row))
            << "Row " << row << " should have non-null catalog_db_schemas";

        for (int64_t db_schemas_index =
                 ArrowArrayViewGetOffsetUnsafe(catalog_db_schemas_list, row);
             db_schemas_index <
             ArrowArrayViewGetOffsetUnsafe(catalog_db_schemas_list, row + 1);
             db_schemas_index++) {
          ASSERT_FALSE(
              ArrowArrayViewIsNull(db_schema_tables_list, row + db_schemas_index))
              << "Row " << row << " should have non-null db_schema_tables";

          for (int64_t tables_index = ArrowArrayViewGetOffsetUnsafe(
                   db_schema_tables_list, row + db_schemas_index);
               tables_index < ArrowArrayViewGetOffsetUnsafe(db_schema_tables_list,
                                                            row + db_schemas_index + 1);
               tables_index++) {
            ArrowStringView table_name = ArrowArrayViewGetStringUnsafe(
                db_schema_tables->children[0], tables_index);
            if (std::strcmp(table_name.data, "bulk_ingest") == 0) {
              found_expected_table = true;
            }

            ASSERT_TRUE(ArrowArrayViewIsNull(db_schema_tables->children[2], tables_index))
                << "Row " << row << " should have null table_columns";
            ASSERT_TRUE(ArrowArrayViewIsNull(db_schema_tables->children[3], tables_index))
                << "Row " << row << " should have null table_constraints";
          }
        }
      }
      ASSERT_NO_FATAL_FAILURE(reader.Next());
    } while (reader.array->release);

    ASSERT_EQ(expected.second, found_expected_table)
        << "Did (not) find table in metadata";
  }
}

void ConnectionTest::TestMetadataGetObjectsTablesTypes() {
  ASSERT_OK(&error, AdbcConnectionNew(&connection, &error));
  ASSERT_OK(&error, AdbcConnectionInit(&connection, &database, &error));
  ASSERT_OK(&error, quirks()->DropTable(&connection, "bulk_ingest", &error));
  ASSERT_NO_FATAL_FAILURE(IngestSampleTable(&connection, &error));

  std::vector<const char*> table_types(2);
  table_types[0] = "this_table_type_does_not_exist";
  table_types[1] = nullptr;
  {
    StreamReader reader;
    ASSERT_OK(&error,
              AdbcConnectionGetObjects(&connection, ADBC_OBJECT_DEPTH_TABLES, nullptr,
                                       nullptr, nullptr, table_types.data(), nullptr,
                                       &reader.stream.value, &error));
    ASSERT_NO_FATAL_FAILURE(reader.GetSchema());
    ASSERT_NO_FATAL_FAILURE(CheckGetObjectsSchema(&reader.schema.value));
    ASSERT_NO_FATAL_FAILURE(reader.Next());
    ASSERT_NE(nullptr, reader.array->release);
    ASSERT_GT(reader.array->length, 0);
    bool found_expected_table = false;
    do {
      for (int64_t row = 0; row < reader.array->length; row++) {
        // type: list<db_schema_schema>
        struct ArrowArrayView* catalog_db_schemas_list = reader.array_view->children[1];
        // type: db_schema_schema (struct)
        struct ArrowArrayView* catalog_db_schemas = catalog_db_schemas_list->children[0];
        // type: list<table_schema>
        struct ArrowArrayView* db_schema_tables_list = catalog_db_schemas->children[1];
        // type: table_schema (struct)
        struct ArrowArrayView* db_schema_tables = db_schema_tables_list->children[0];

        ASSERT_FALSE(ArrowArrayViewIsNull(catalog_db_schemas_list, row))
            << "Row " << row << " should have non-null catalog_db_schemas";

        for (int64_t db_schemas_index =
                 ArrowArrayViewGetOffsetUnsafe(catalog_db_schemas_list, row);
             db_schemas_index <
             ArrowArrayViewGetOffsetUnsafe(catalog_db_schemas_list, row + 1);
             db_schemas_index++) {
          ASSERT_FALSE(ArrowArrayViewIsNull(db_schema_tables_list, db_schemas_index))
              << "Row " << row << " should have non-null db_schema_tables";

          for (int64_t tables_index = ArrowArrayViewGetOffsetUnsafe(
                   db_schema_tables_list, row + db_schemas_index);
               tables_index <
               ArrowArrayViewGetOffsetUnsafe(db_schema_tables_list, db_schemas_index + 1);
               tables_index++) {
            ArrowStringView table_name = ArrowArrayViewGetStringUnsafe(
                db_schema_tables->children[0], tables_index);
            if (std::strcmp(table_name.data, "bulk_ingest") == 0) {
              found_expected_table = true;
            }

            ASSERT_TRUE(ArrowArrayViewIsNull(db_schema_tables->children[2], tables_index))
                << "Row " << row << " should have null table_columns";
            ASSERT_TRUE(ArrowArrayViewIsNull(db_schema_tables->children[3], tables_index))
                << "Row " << row << " should have null table_constraints";
          }
        }
      }
      ASSERT_NO_FATAL_FAILURE(reader.Next());
    } while (reader.array->release);

    ASSERT_FALSE(found_expected_table) << "Should not find table in metadata";
  }
}

void ConnectionTest::TestMetadataGetObjectsColumns() {
  // TODO: test could be more robust if we ingested a few tables
  ASSERT_EQ(ADBC_OBJECT_DEPTH_COLUMNS, ADBC_OBJECT_DEPTH_ALL);

  ASSERT_OK(&error, AdbcConnectionNew(&connection, &error));
  ASSERT_OK(&error, AdbcConnectionInit(&connection, &database, &error));
  ASSERT_OK(&error, quirks()->DropTable(&connection, "bulk_ingest", &error));
  ASSERT_NO_FATAL_FAILURE(IngestSampleTable(&connection, &error));

  struct TestCase {
    std::optional<std::string> filter;
    std::vector<std::string> column_names;
    std::vector<int32_t> ordinal_positions;
  };

  std::vector<TestCase> test_cases;
  test_cases.push_back({std::nullopt, {"int64s", "strings"}, {1, 2}});
  test_cases.push_back({"in%", {"int64s"}, {1}});

  for (const auto& test_case : test_cases) {
    StreamReader reader;
    std::vector<std::string> column_names;
    std::vector<int32_t> ordinal_positions;

    ASSERT_OK(
        &error,
        AdbcConnectionGetObjects(
            &connection, ADBC_OBJECT_DEPTH_COLUMNS, nullptr, nullptr, nullptr, nullptr,
            test_case.filter.has_value() ? test_case.filter->c_str() : nullptr,
            &reader.stream.value, &error));
    ASSERT_NO_FATAL_FAILURE(reader.GetSchema());
    ASSERT_NO_FATAL_FAILURE(CheckGetObjectsSchema(&reader.schema.value));
    ASSERT_NO_FATAL_FAILURE(reader.Next());
    ASSERT_NE(nullptr, reader.array->release);
    ASSERT_GT(reader.array->length, 0);
    bool found_expected_table = false;
    do {
      for (int64_t row = 0; row < reader.array->length; row++) {
        // type: list<db_schema_schema>
        struct ArrowArrayView* catalog_db_schemas_list = reader.array_view->children[1];
        // type: db_schema_schema (struct)
        struct ArrowArrayView* catalog_db_schemas = catalog_db_schemas_list->children[0];
        // type: list<table_schema>
        struct ArrowArrayView* db_schema_tables_list = catalog_db_schemas->children[1];
        // type: table_schema (struct)
        struct ArrowArrayView* db_schema_tables = db_schema_tables_list->children[0];
        // type: list<column_schema>
        struct ArrowArrayView* table_columns_list = db_schema_tables->children[2];
        // type: column_schema (struct)
        struct ArrowArrayView* table_columns = table_columns_list->children[0];
        // type: list<usage_schema>
        struct ArrowArrayView* table_constraints_list = db_schema_tables->children[3];

        ASSERT_FALSE(ArrowArrayViewIsNull(catalog_db_schemas_list, row))
            << "Row " << row << " should have non-null catalog_db_schemas";

        for (int64_t db_schemas_index =
                 ArrowArrayViewGetOffsetUnsafe(catalog_db_schemas_list, row);
             db_schemas_index <
             ArrowArrayViewGetOffsetUnsafe(catalog_db_schemas_list, row + 1);
             db_schemas_index++) {
          ASSERT_FALSE(
              ArrowArrayViewIsNull(db_schema_tables_list, row + db_schemas_index))
              << "Row " << row << " should have non-null db_schema_tables";

          for (int64_t tables_index =
                   ArrowArrayViewGetOffsetUnsafe(db_schema_tables_list, db_schemas_index);
               tables_index <
               ArrowArrayViewGetOffsetUnsafe(db_schema_tables_list, db_schemas_index + 1);
               tables_index++) {
            ArrowStringView table_name = ArrowArrayViewGetStringUnsafe(
                db_schema_tables->children[0], tables_index);

            ASSERT_FALSE(ArrowArrayViewIsNull(table_columns_list, tables_index))
                << "Row " << row << " should have non-null table_columns";
            ASSERT_FALSE(ArrowArrayViewIsNull(table_constraints_list, tables_index))
                << "Row " << row << " should have non-null table_constraints";

            if (std::strcmp(table_name.data, "bulk_ingest") == 0) {
              found_expected_table = true;

              for (int64_t columns_index =
                       ArrowArrayViewGetOffsetUnsafe(table_columns_list, tables_index);
                   columns_index <
                   ArrowArrayViewGetOffsetUnsafe(table_columns_list, tables_index + 1);
                   columns_index++) {
                ArrowStringView name = ArrowArrayViewGetStringUnsafe(
                    table_columns->children[0], columns_index);
                column_names.push_back(std::string(name.data, name.n_bytes));
                ordinal_positions.push_back(ArrowArrayViewGetIntUnsafe(
                    table_columns->children[1], columns_index));
              }
            }
          }
        }
      }
      ASSERT_NO_FATAL_FAILURE(reader.Next());
    } while (reader.array->release);

    ASSERT_TRUE(found_expected_table) << "Did (not) find table in metadata";
    ASSERT_EQ(test_case.column_names, column_names);
    ASSERT_EQ(test_case.ordinal_positions, ordinal_positions);
  }
}

void ConnectionTest::TestMetadataGetObjectsConstraints() {
  // TODO: can't be done portably (need to create tables with primary keys and such)
}

//------------------------------------------------------------
// Tests of AdbcStatement

void StatementTest::SetUpTest() {
  std::memset(&error, 0, sizeof(error));
  std::memset(&database, 0, sizeof(database));
  std::memset(&connection, 0, sizeof(connection));
  std::memset(&statement, 0, sizeof(statement));

  ASSERT_OK(&error, AdbcDatabaseNew(&database, &error));
  ASSERT_OK(&error, quirks()->SetupDatabase(&database, &error));
  ASSERT_OK(&error, AdbcDatabaseInit(&database, &error));

  ASSERT_OK(&error, AdbcConnectionNew(&connection, &error));
  ASSERT_OK(&error, AdbcConnectionInit(&connection, &database, &error));
}

void StatementTest::TearDownTest() {
  if (statement.private_data) {
    ASSERT_OK(&error, AdbcStatementRelease(&statement, &error));
  }
  ASSERT_OK(&error, AdbcConnectionRelease(&connection, &error));
  ASSERT_OK(&error, AdbcDatabaseRelease(&database, &error));
  if (error.release) {
    error.release(&error);
  }
}

void StatementTest::TestNewInit() {
  ASSERT_OK(&error, AdbcStatementNew(&connection, &statement, &error));
  ASSERT_OK(&error, AdbcStatementRelease(&statement, &error));
  ASSERT_EQ(NULL, statement.private_data);

  ASSERT_FAILS_WITH(INVALID_STATE, &error, AdbcStatementRelease(&statement, &error));

  ASSERT_OK(&error, AdbcStatementNew(&connection, &statement, &error));
  // Cannot execute
  ASSERT_FAILS_WITH(INVALID_STATE, &error,
                    AdbcStatementExecuteQuery(&statement, nullptr, nullptr, &error));
}

void StatementTest::TestRelease() {
  ASSERT_FAILS_WITH(INVALID_STATE, &error, AdbcStatementRelease(&statement, &error));

  ASSERT_OK(&error, AdbcStatementNew(&connection, &statement, &error));
  ASSERT_OK(&error, AdbcStatementRelease(&statement, &error));
  ASSERT_EQ(NULL, statement.private_data);
}

void StatementTest::TestSqlIngestInts() {
  ASSERT_OK(&error, quirks()->DropTable(&connection, "bulk_ingest", &error));

  Handle<struct ArrowSchema> schema;
  Handle<struct ArrowArray> array;
  struct ArrowError na_error;
  NA_ASSERT_OK(nullptr, MakeBatch<int64_t>(&schema.value, &array.value, &na_error,
                                           {{"int64s", NANOARROW_TYPE_INT64}},
                                           {42, -42, std::nullopt}));

  ASSERT_OK(&error, AdbcStatementNew(&connection, &statement, &error));
  ASSERT_OK(&error, AdbcStatementSetOption(&statement, ADBC_INGEST_OPTION_TARGET_TABLE,
                                           "bulk_ingest", &error));
  ASSERT_OK(&error, AdbcStatementBind(&statement, &array.value, &schema.value, &error));

  int64_t rows_affected = 0;
  ASSERT_OK(&error,
            AdbcStatementExecuteQuery(&statement, nullptr, &rows_affected, &error));
  ASSERT_THAT(rows_affected, ::testing::AnyOf(::testing::Eq(3), ::testing::Eq(-1)));

  ASSERT_OK(&error,
            AdbcStatementSetSqlQuery(&statement, "SELECT * FROM bulk_ingest", &error));
  {
    StreamReader reader;
    ASSERT_OK(&error, AdbcStatementExecuteQuery(&statement, &reader.stream.value,
                                                &reader.rows_affected, &error));
    ASSERT_THAT(reader.rows_affected,
                ::testing::AnyOf(::testing::Eq(3), ::testing::Eq(-1)));

    ASSERT_NO_FATAL_FAILURE(reader.GetSchema());
    ASSERT_NO_FATAL_FAILURE(CompareSchema(&reader.schema.value,
                                          {{"int64s", NANOARROW_TYPE_INT64, NULLABLE}}));

    ASSERT_NO_FATAL_FAILURE(reader.Next());
    ASSERT_NE(nullptr, reader.array->release);
    ASSERT_EQ(3, reader.array->length);
    ASSERT_EQ(1, reader.array->n_children);

    ASSERT_NO_FATAL_FAILURE(
        CompareArray<int64_t>(reader.array_view->children[0], {42, -42, std::nullopt}));

    ASSERT_NO_FATAL_FAILURE(reader.Next());
    ASSERT_EQ(nullptr, reader.array->release);
  }
}

void StatementTest::TestSqlIngestAppend() {
  // Ingest
  ASSERT_OK(&error, quirks()->DropTable(&connection, "bulk_ingest", &error));

  Handle<struct ArrowSchema> schema;
  Handle<struct ArrowArray> array;
  struct ArrowError na_error;
  NA_ASSERT_OK(nullptr, MakeBatch<int64_t>(&schema.value, &array.value, &na_error,
                                           {{"int64s", NANOARROW_TYPE_INT64}}, {42}));

  ASSERT_OK(&error, AdbcStatementNew(&connection, &statement, &error));
  ASSERT_OK(&error, AdbcStatementSetOption(&statement, ADBC_INGEST_OPTION_TARGET_TABLE,
                                           "bulk_ingest", &error));
  ASSERT_OK(&error, AdbcStatementBind(&statement, &array.value, &schema.value, &error));

  int64_t rows_affected = 0;
  ASSERT_OK(&error,
            AdbcStatementExecuteQuery(&statement, nullptr, &rows_affected, &error));
  ASSERT_THAT(rows_affected, ::testing::AnyOf(::testing::Eq(1), ::testing::Eq(-1)));

  // Now append

  // Re-initialize since Bind() should take ownership of data
  NA_ASSERT_OK(nullptr, MakeBatch<int64_t>(&schema.value, &array.value, &na_error,
                                           {{"int64s", NANOARROW_TYPE_INT64}},
                                           {-42, std::nullopt}));

  ASSERT_OK(&error, AdbcStatementSetOption(&statement, ADBC_INGEST_OPTION_TARGET_TABLE,
                                           "bulk_ingest", &error));
  ASSERT_OK(&error, AdbcStatementSetOption(&statement, ADBC_INGEST_OPTION_MODE,
                                           ADBC_INGEST_OPTION_MODE_APPEND, &error));
  ASSERT_OK(&error, AdbcStatementBind(&statement, &array.value, &schema.value, &error));

  ASSERT_OK(&error,
            AdbcStatementExecuteQuery(&statement, nullptr, &rows_affected, &error));
  ASSERT_THAT(rows_affected, ::testing::AnyOf(::testing::Eq(2), ::testing::Eq(-1)));

  // Read data back
  ASSERT_OK(&error,
            AdbcStatementSetSqlQuery(&statement, "SELECT * FROM bulk_ingest", &error));
  {
    StreamReader reader;
    ASSERT_OK(&error, AdbcStatementExecuteQuery(&statement, &reader.stream.value,
                                                &reader.rows_affected, &error));
    ASSERT_THAT(reader.rows_affected,
                ::testing::AnyOf(::testing::Eq(3), ::testing::Eq(-1)));

    ASSERT_NO_FATAL_FAILURE(reader.GetSchema());
    ASSERT_NO_FATAL_FAILURE(CompareSchema(&reader.schema.value,
                                          {{"int64s", NANOARROW_TYPE_INT64, NULLABLE}}));

    ASSERT_NO_FATAL_FAILURE(reader.Next());
    ASSERT_NE(nullptr, reader.array->release);
    ASSERT_EQ(3, reader.array->length);
    ASSERT_EQ(1, reader.array->n_children);

    ASSERT_NO_FATAL_FAILURE(
        CompareArray<int64_t>(reader.array_view->children[0], {42, -42, std::nullopt}));

    ASSERT_NO_FATAL_FAILURE(reader.Next());
    ASSERT_EQ(nullptr, reader.array->release);
  }

  ASSERT_OK(&error, AdbcStatementRelease(&statement, &error));
}

void StatementTest::TestSqlIngestErrors() {
  // Ingest without bind
  ASSERT_OK(&error, AdbcStatementNew(&connection, &statement, &error));
  ASSERT_OK(&error, AdbcStatementSetOption(&statement, ADBC_INGEST_OPTION_TARGET_TABLE,
                                           "bulk_ingest", &error));
  ASSERT_FAILS_WITH(INVALID_STATE, &error,
                    AdbcStatementExecuteQuery(&statement, nullptr, nullptr, &error));
  if (error.release) error.release(&error);

  ASSERT_OK(&error, quirks()->DropTable(&connection, "bulk_ingest", &error));

  // Append to nonexistent table
  Handle<struct ArrowSchema> schema;
  Handle<struct ArrowArray> array;
  struct ArrowError na_error;

  ASSERT_OK(&error, AdbcStatementSetOption(&statement, ADBC_INGEST_OPTION_TARGET_TABLE,
                                           "bulk_ingest", &error));
  ASSERT_OK(&error, AdbcStatementSetOption(&statement, ADBC_INGEST_OPTION_MODE,
                                           ADBC_INGEST_OPTION_MODE_APPEND, &error));
  NA_ASSERT_OK(&na_error, MakeBatch<int64_t>(&schema.value, &array.value, &na_error,
                                             {{"int64s", NANOARROW_TYPE_INT64}},
                                             {-42, std::nullopt}));
  ASSERT_OK(&error, AdbcStatementBind(&statement, &array.value, &schema.value, &error));

  ASSERT_FAILS(&error, AdbcStatementExecuteQuery(&statement, nullptr, nullptr, &error));
  if (error.release) error.release(&error);

  // Ingest...
  ASSERT_OK(&error, AdbcStatementSetOption(&statement, ADBC_INGEST_OPTION_MODE,
                                           ADBC_INGEST_OPTION_MODE_CREATE, &error));
  NA_ASSERT_OK(&na_error, MakeBatch<int64_t>(&schema.value, &array.value, &na_error,
                                             {{"int64s", NANOARROW_TYPE_INT64}},
                                             {-42, std::nullopt}));
  ASSERT_OK(&error, AdbcStatementBind(&statement, &array.value, &schema.value, &error));
  ASSERT_OK(&error, AdbcStatementExecuteQuery(&statement, nullptr, nullptr, &error));

  // ...then try to overwrite it
  NA_ASSERT_OK(&na_error, MakeBatch<int64_t>(&schema.value, &array.value, &na_error,
                                             {{"int64s", NANOARROW_TYPE_INT64}},
                                             {-42, std::nullopt}));
  ASSERT_OK(&error, AdbcStatementBind(&statement, &array.value, &schema.value, &error));
  ASSERT_FAILS(&error, AdbcStatementExecuteQuery(&statement, nullptr, nullptr, &error));
  if (error.release) error.release(&error);

  // ...then try to append an incompatible schema
  NA_ASSERT_OK(
      &na_error,
      (MakeBatch<int64_t, int64_t>(
          &schema.value, &array.value, &na_error,
          {{"int64s", NANOARROW_TYPE_INT64}, {"coltwo", NANOARROW_TYPE_INT64}}, {}, {})));

  ASSERT_OK(&error, AdbcStatementBind(&statement, &array.value, &schema.value, &error));
  ASSERT_OK(&error, AdbcStatementSetOption(&statement, ADBC_INGEST_OPTION_MODE,
                                           ADBC_INGEST_OPTION_MODE_APPEND, &error));
  ASSERT_FAILS(&error, AdbcStatementExecuteQuery(&statement, nullptr, nullptr, &error));
}

void StatementTest::TestSqlIngestMultipleConnections() {
  ASSERT_OK(&error, quirks()->DropTable(&connection, "bulk_ingest", &error));

  Handle<struct ArrowSchema> schema;
  Handle<struct ArrowArray> array;
  struct ArrowError na_error;
  NA_ASSERT_OK(nullptr, MakeBatch<int64_t>(&schema.value, &array.value, &na_error,
                                           {{"int64s", NANOARROW_TYPE_INT64}},
                                           {42, -42, std::nullopt}));

  ASSERT_OK(&error, AdbcStatementNew(&connection, &statement, &error));
  ASSERT_OK(&error, AdbcStatementSetOption(&statement, ADBC_INGEST_OPTION_TARGET_TABLE,
                                           "bulk_ingest", &error));
  ASSERT_OK(&error, AdbcStatementBind(&statement, &array.value, &schema.value, &error));

  int64_t rows_affected = 0;
  ASSERT_OK(&error,
            AdbcStatementExecuteQuery(&statement, nullptr, &rows_affected, &error));
  ASSERT_THAT(rows_affected, ::testing::AnyOf(::testing::Eq(3), ::testing::Eq(-1)));
  ASSERT_OK(&error, AdbcStatementRelease(&statement, &error));

  {
    struct AdbcConnection connection2;
    ASSERT_OK(&error, AdbcConnectionNew(&connection2, &error));
    ASSERT_OK(&error, AdbcConnectionInit(&connection2, &database, &error));
    ASSERT_OK(&error, AdbcStatementNew(&connection2, &statement, &error));

    ASSERT_OK(&error,
              AdbcStatementSetSqlQuery(&statement, "SELECT * FROM bulk_ingest", &error));

    {
      StreamReader reader;
      ASSERT_OK(&error, AdbcStatementExecuteQuery(&statement, &reader.stream.value,
                                                  &reader.rows_affected, &error));
      ASSERT_THAT(reader.rows_affected,
                  ::testing::AnyOf(::testing::Eq(3), ::testing::Eq(-1)));

      ASSERT_NO_FATAL_FAILURE(reader.GetSchema());
      ASSERT_NO_FATAL_FAILURE(CompareSchema(
          &reader.schema.value, {{"int64s", NANOARROW_TYPE_INT64, NULLABLE}}));

      ASSERT_NO_FATAL_FAILURE(reader.Next());
      ASSERT_NE(nullptr, reader.array->release);
      ASSERT_EQ(3, reader.array->length);
      ASSERT_EQ(1, reader.array->n_children);

      ASSERT_NO_FATAL_FAILURE(
          CompareArray<int64_t>(reader.array_view->children[0], {42, -42, std::nullopt}));

      ASSERT_NO_FATAL_FAILURE(reader.Next());
      ASSERT_EQ(nullptr, reader.array->release);
    }

    ASSERT_OK(&error, AdbcStatementRelease(&statement, &error));
    ASSERT_OK(&error, AdbcConnectionRelease(&connection2, &error));
  }
}

void StatementTest::TestSqlPrepareSelectNoParams() {
  ASSERT_OK(&error, AdbcStatementNew(&connection, &statement, &error));
  ASSERT_OK(&error, AdbcStatementSetSqlQuery(&statement, "SELECT 1", &error));
  ASSERT_OK(&error, AdbcStatementPrepare(&statement, &error));

  StreamReader reader;
  ASSERT_OK(&error, AdbcStatementExecuteQuery(&statement, &reader.stream.value,
                                              &reader.rows_affected, &error));
  ASSERT_THAT(reader.rows_affected,
              ::testing::AnyOf(::testing::Eq(1), ::testing::Eq(-1)));

  ASSERT_NO_FATAL_FAILURE(reader.GetSchema());
  ASSERT_EQ(1, reader.schema->n_children);

  ASSERT_NO_FATAL_FAILURE(reader.Next());
  ASSERT_NE(nullptr, reader.array->release);
  ASSERT_EQ(1, reader.array->length);
  ASSERT_EQ(1, reader.array->n_children);

  switch (reader.fields[0].data_type) {
    case NANOARROW_TYPE_INT32:
      ASSERT_NO_FATAL_FAILURE(CompareArray<int32_t>(reader.array_view->children[0], {1}));
      break;
    case NANOARROW_TYPE_INT64:
      ASSERT_NO_FATAL_FAILURE(CompareArray<int64_t>(reader.array_view->children[0], {1}));
      break;
    default:
      FAIL() << "Unexpected data type: " << reader.fields[0].data_type;
  }

  ASSERT_NO_FATAL_FAILURE(reader.Next());
  ASSERT_EQ(nullptr, reader.array->release);
}

void StatementTest::TestSqlPrepareSelectParams() {
  ASSERT_OK(&error, AdbcStatementNew(&connection, &statement, &error));
  ASSERT_OK(&error, AdbcStatementSetSqlQuery(&statement, "SELECT ?, ?", &error));
  ASSERT_OK(&error, AdbcStatementPrepare(&statement, &error));

  Handle<struct ArrowSchema> schema;
  Handle<struct ArrowArray> array;
  struct ArrowError na_error;
  NA_ASSERT_OK(nullptr,
               (MakeBatch<int64_t, std::string>(
                   &schema.value, &array.value, &na_error,
                   {{"int64s", NANOARROW_TYPE_INT64}, {"strings", NANOARROW_TYPE_STRING}},
                   {42, -42, std::nullopt}, {"", std::nullopt, "bar"})));
  ASSERT_OK(&error, AdbcStatementBind(&statement, &array.value, &schema.value, &error));

  StreamReader reader;
  ASSERT_OK(&error, AdbcStatementExecuteQuery(&statement, &reader.stream.value,
                                              &reader.rows_affected, &error));
  ASSERT_THAT(reader.rows_affected,
              ::testing::AnyOf(::testing::Eq(1), ::testing::Eq(-1)));

  ASSERT_NO_FATAL_FAILURE(reader.GetSchema());
  ASSERT_EQ(2, reader.schema->n_children);

  ASSERT_NO_FATAL_FAILURE(reader.Next());
  ASSERT_NE(nullptr, reader.array->release);
  ASSERT_EQ(3, reader.array->length);
  ASSERT_EQ(2, reader.array->n_children);

  switch (reader.fields[0].data_type) {
    case NANOARROW_TYPE_INT32:
      ASSERT_NO_FATAL_FAILURE(
          CompareArray<int32_t>(reader.array_view->children[0], {42, -42, std::nullopt}));
      break;
    case NANOARROW_TYPE_INT64:
      ASSERT_NO_FATAL_FAILURE(
          CompareArray<int64_t>(reader.array_view->children[0], {42, -42, std::nullopt}));
      break;
    default:
      FAIL() << "Unexpected data type: " << reader.fields[0].data_type;
  }
  ASSERT_NO_FATAL_FAILURE(CompareArray<std::string>(reader.array_view->children[1],
                                                    {"", std::nullopt, "bar"}));

  ASSERT_NO_FATAL_FAILURE(reader.Next());
  ASSERT_EQ(nullptr, reader.array->release);
}

void StatementTest::TestSqlPrepareUpdate() {
  ASSERT_OK(&error, AdbcStatementNew(&connection, &statement, &error));
  ASSERT_OK(&error, quirks()->DropTable(&connection, "bulk_ingest", &error));

  Handle<struct ArrowSchema> schema;
  Handle<struct ArrowArray> array;
  struct ArrowError na_error;

  // Create table
  ASSERT_OK(&error, AdbcStatementSetOption(&statement, ADBC_INGEST_OPTION_TARGET_TABLE,
                                           "bulk_ingest", &error));
  NA_ASSERT_OK(nullptr, MakeBatch<int64_t>(&schema.value, &array.value, &na_error,
                                           {{"int64s", NANOARROW_TYPE_INT64}},
                                           {42, -42, std::nullopt}));
  ASSERT_OK(&error, AdbcStatementBind(&statement, &array.value, &schema.value, &error));

  ASSERT_OK(&error, AdbcStatementExecuteQuery(&statement, nullptr, nullptr, &error));

  // Prepare
  std::string query =
      "INSERT INTO bulk_ingest VALUES (" + quirks()->BindParameter(0) + ")";
  ASSERT_OK(&error, AdbcStatementSetSqlQuery(&statement, query.c_str(), &error));
  ASSERT_OK(&error, AdbcStatementPrepare(&statement, &error));

  // Bind and execute
  NA_ASSERT_OK(nullptr, MakeBatch<int64_t>(&schema.value, &array.value, &na_error,
                                           {{"int64s", NANOARROW_TYPE_INT64}},
                                           {42, -42, std::nullopt}));
  ASSERT_OK(&error, AdbcStatementBind(&statement, &array.value, &schema.value, &error));
  ASSERT_OK(&error, AdbcStatementExecuteQuery(&statement, nullptr, nullptr, &error));

  // Read data back
  ASSERT_OK(&error,
            AdbcStatementSetSqlQuery(&statement, "SELECT * FROM bulk_ingest", &error));
  {
    StreamReader reader;
    ASSERT_OK(&error, AdbcStatementExecuteQuery(&statement, &reader.stream.value,
                                                &reader.rows_affected, &error));
    ASSERT_THAT(reader.rows_affected,
                ::testing::AnyOf(::testing::Eq(6), ::testing::Eq(-1)));

    ASSERT_NO_FATAL_FAILURE(reader.GetSchema());
    ASSERT_NO_FATAL_FAILURE(CompareSchema(&reader.schema.value,
                                          {{"int64s", NANOARROW_TYPE_INT64, NULLABLE}}));

    ASSERT_NO_FATAL_FAILURE(reader.Next());
    ASSERT_NE(nullptr, reader.array->release);
    ASSERT_EQ(6, reader.array->length);
    ASSERT_EQ(1, reader.array->n_children);

    ASSERT_NO_FATAL_FAILURE(CompareArray<int64_t>(
        reader.array_view->children[0], {42, -42, std::nullopt, 42, -42, std::nullopt}));

    ASSERT_NO_FATAL_FAILURE(reader.Next());
    ASSERT_EQ(nullptr, reader.array->release);
  }
}

void StatementTest::TestSqlPrepareUpdateNoParams() {
  // TODO: prepare something like INSERT 1, then execute it and confirm it's executed once

  // TODO: then bind a table with 0 cols and X rows and confirm it executes multiple times
}

void StatementTest::TestSqlPrepareUpdateStream() {
  ASSERT_OK(&error, AdbcStatementNew(&connection, &statement, &error));
  ASSERT_OK(&error, quirks()->DropTable(&connection, "bulk_ingest", &error));
  struct ArrowError na_error;

  const std::vector<std::pair<std::string, ArrowType>> fields = {
      {"ints", NANOARROW_TYPE_INT64}};

  // Create table
  {
    Handle<struct ArrowSchema> schema;
    Handle<struct ArrowArray> array;

    ASSERT_OK(&error, AdbcStatementSetOption(&statement, ADBC_INGEST_OPTION_TARGET_TABLE,
                                             "bulk_ingest", &error));
    NA_ASSERT_OK(nullptr,
                 MakeBatch<int64_t>(&schema.value, &array.value, &na_error, fields, {}));
    ASSERT_OK(&error, AdbcStatementBind(&statement, &array.value, &schema.value, &error));
    ASSERT_OK(&error, AdbcStatementExecuteQuery(&statement, nullptr, nullptr, &error));
  }

  // Generate stream
  struct ArrowArrayStream stream;
  struct ArrowSchema schema;
  std::vector<struct ArrowArray> batches(2);

  ASSERT_NO_FATAL_FAILURE(MakeSchema(&schema, fields));
  NA_ASSERT_OK(&na_error, ArrowArrayInitFromSchema(&batches[0], &schema, &na_error));
  NA_ASSERT_OK(&na_error,
               MakeBatch<int64_t>(&batches[0], &na_error, {1, 2, std::nullopt, 3}));
  NA_ASSERT_OK(&na_error, ArrowArrayInitFromSchema(&batches[1], &schema, &na_error));
  NA_ASSERT_OK(&na_error, MakeBatch<int64_t>(&batches[1], &na_error, {std::nullopt, 3}));

  ASSERT_NO_FATAL_FAILURE(MakeStream(&stream, &schema, std::move(batches)));

  // Prepare
  std::string query =
      "INSERT INTO bulk_ingest VALUES (" + quirks()->BindParameter(0) + ")";
  ASSERT_OK(&error, AdbcStatementSetSqlQuery(&statement, query.c_str(), &error));
  ASSERT_OK(&error, AdbcStatementPrepare(&statement, &error));

  // Bind and execute
  ASSERT_OK(&error, AdbcStatementBindStream(&statement, &stream, &error));
  ASSERT_OK(&error, AdbcStatementExecuteQuery(&statement, nullptr, nullptr, &error));

  // Read data back
  ASSERT_OK(&error,
            AdbcStatementSetSqlQuery(&statement, "SELECT * FROM bulk_ingest", &error));
  {
    StreamReader reader;
    ASSERT_OK(&error, AdbcStatementExecuteQuery(&statement, &reader.stream.value,
                                                &reader.rows_affected, &error));
    ASSERT_THAT(reader.rows_affected,
                ::testing::AnyOf(::testing::Eq(6), ::testing::Eq(-1)));

    ASSERT_NO_FATAL_FAILURE(reader.GetSchema());
    ASSERT_NO_FATAL_FAILURE(
        CompareSchema(&reader.schema.value, {{"ints", NANOARROW_TYPE_INT64, NULLABLE}}));

    ASSERT_NO_FATAL_FAILURE(reader.Next());
    ASSERT_NE(nullptr, reader.array->release);
    ASSERT_EQ(6, reader.array->length);
    ASSERT_EQ(1, reader.array->n_children);

    ASSERT_NO_FATAL_FAILURE(CompareArray<int64_t>(
        reader.array_view->children[0], {1, 2, std::nullopt, 3, std::nullopt, 3}));

    ASSERT_NO_FATAL_FAILURE(reader.Next());
    ASSERT_EQ(nullptr, reader.array->release);
  }

  // TODO: import released stream

  // TODO: stream that errors on get_schema

  // TODO: stream that errors on get_next (first call)

  // TODO: stream that errors on get_next (second call)
}

void StatementTest::TestSqlPrepareErrorNoQuery() {
  ASSERT_OK(&error, AdbcStatementNew(&connection, &statement, &error));
  ASSERT_FAILS_WITH(INVALID_STATE, &error, AdbcStatementPrepare(&statement, &error));
  if (error.release) error.release(&error);
}

void StatementTest::TestSqlPrepareErrorParamCountMismatch() {
  Handle<struct ArrowSchema> schema;
  Handle<struct ArrowArray> array;
  struct ArrowError na_error;
  StreamReader reader;

  std::string query = "SELECT ";
  query += quirks()->BindParameter(0);
  query += ", ";
  query += quirks()->BindParameter(1);

  ASSERT_OK(&error, AdbcStatementNew(&connection, &statement, &error));
  ASSERT_OK(&error, AdbcStatementSetSqlQuery(&statement, query.c_str(), &error));
  ASSERT_OK(&error, AdbcStatementPrepare(&statement, &error));
  NA_ASSERT_OK(nullptr, MakeBatch<int64_t>(&schema.value, &array.value, &na_error,
                                           {{"int64s", NANOARROW_TYPE_INT64}},
                                           {42, -42, std::nullopt}));

  ASSERT_FAILS(&error, ([&]() -> AdbcStatusCode {
    CHECK_OK(AdbcStatementBind(&statement, &array.value, &schema.value, &error));
    CHECK_OK(AdbcStatementExecuteQuery(&statement, &reader.stream.value,
                                       &reader.rows_affected, &error));
    return ADBC_STATUS_OK;
  })());
}

void StatementTest::TestSqlQueryInts() {
  ASSERT_OK(&error, AdbcStatementNew(&connection, &statement, &error));
  ASSERT_OK(&error, AdbcStatementSetSqlQuery(&statement, "SELECT 42", &error));

  {
    StreamReader reader;
    ASSERT_OK(&error, AdbcStatementExecuteQuery(&statement, &reader.stream.value,
                                                &reader.rows_affected, &error));
    ASSERT_THAT(reader.rows_affected,
                ::testing::AnyOf(::testing::Eq(1), ::testing::Eq(-1)));

    ASSERT_NO_FATAL_FAILURE(reader.GetSchema());
    ASSERT_EQ(1, reader.schema->n_children);

    ASSERT_NO_FATAL_FAILURE(reader.Next());
    ASSERT_NE(nullptr, reader.array->release);
    ASSERT_EQ(1, reader.array->length);
    ASSERT_EQ(1, reader.array->n_children);

    switch (reader.fields[0].data_type) {
      case NANOARROW_TYPE_INT32:
        ASSERT_NO_FATAL_FAILURE(
            CompareArray<int32_t>(reader.array_view->children[0], {42}));
        break;
      case NANOARROW_TYPE_INT64:
        ASSERT_NO_FATAL_FAILURE(
            CompareArray<int64_t>(reader.array_view->children[0], {42}));
        break;
      default:
        FAIL() << "Unexpected data type: " << reader.fields[0].data_type;
    }

    ASSERT_NO_FATAL_FAILURE(reader.Next());
    ASSERT_EQ(nullptr, reader.array->release);
  }

  ASSERT_OK(&error, AdbcStatementRelease(&statement, &error));
}

void StatementTest::TestSqlQueryFloats() {
  ASSERT_OK(&error, AdbcStatementNew(&connection, &statement, &error));
  ASSERT_OK(&error,
            AdbcStatementSetSqlQuery(&statement, "SELECT CAST(1.0 AS REAL)", &error));

  {
    StreamReader reader;
    ASSERT_OK(&error, AdbcStatementExecuteQuery(&statement, &reader.stream.value,
                                                &reader.rows_affected, &error));
    ASSERT_THAT(reader.rows_affected,
                ::testing::AnyOf(::testing::Eq(1), ::testing::Eq(-1)));

    ASSERT_NO_FATAL_FAILURE(reader.GetSchema());
    ASSERT_EQ(1, reader.schema->n_children);

    ASSERT_NO_FATAL_FAILURE(reader.Next());
    ASSERT_NE(nullptr, reader.array->release);
    ASSERT_EQ(1, reader.array->length);
    ASSERT_EQ(1, reader.array->n_children);

    ASSERT_FALSE(ArrowArrayViewIsNull(&reader.array_view.value, 0));
    ASSERT_FALSE(ArrowArrayViewIsNull(reader.array_view->children[0], 0));
    switch (reader.fields[0].data_type) {
      case NANOARROW_TYPE_FLOAT:
        ASSERT_NO_FATAL_FAILURE(
            CompareArray<float>(reader.array_view->children[0], {1.0f}));
        break;
      case NANOARROW_TYPE_DOUBLE:
        ASSERT_NO_FATAL_FAILURE(
            CompareArray<double>(reader.array_view->children[0], {1.0}));
        break;
      default:
        FAIL() << "Unexpected data type: " << reader.fields[0].data_type;
    }

    ASSERT_NO_FATAL_FAILURE(reader.Next());
    ASSERT_EQ(nullptr, reader.array->release);
  }

  ASSERT_OK(&error, AdbcStatementRelease(&statement, &error));
}

void StatementTest::TestSqlQueryStrings() {
  ASSERT_OK(&error, AdbcStatementNew(&connection, &statement, &error));
  ASSERT_OK(&error, AdbcStatementSetSqlQuery(&statement, "SELECT 'SaShiSuSeSo'", &error));

  {
    StreamReader reader;
    ASSERT_OK(&error, AdbcStatementExecuteQuery(&statement, &reader.stream.value,
                                                &reader.rows_affected, &error));
    ASSERT_THAT(reader.rows_affected,
                ::testing::AnyOf(::testing::Eq(1), ::testing::Eq(-1)));

    ASSERT_NO_FATAL_FAILURE(reader.GetSchema());
    ASSERT_EQ(1, reader.schema->n_children);

    ASSERT_NO_FATAL_FAILURE(reader.Next());
    ASSERT_NE(nullptr, reader.array->release);
    ASSERT_EQ(1, reader.array->length);
    ASSERT_EQ(1, reader.array->n_children);

    ASSERT_FALSE(ArrowArrayViewIsNull(&reader.array_view.value, 0));
    ASSERT_FALSE(ArrowArrayViewIsNull(reader.array_view->children[0], 0));
    switch (reader.fields[0].data_type) {
      case NANOARROW_TYPE_STRING: {
        ASSERT_NO_FATAL_FAILURE(
            CompareArray<std::string>(reader.array_view->children[0], {"SaShiSuSeSo"}));
        break;
      }
      default:
        FAIL() << "Unexpected data type: " << reader.fields[0].data_type;
    }

    ASSERT_NO_FATAL_FAILURE(reader.Next());
    ASSERT_EQ(nullptr, reader.array->release);
  }

  ASSERT_OK(&error, AdbcStatementRelease(&statement, &error));
}

void StatementTest::TestSqlQueryErrors() {
  // Invalid query
  ASSERT_OK(&error, AdbcStatementNew(&connection, &statement, &error));
  AdbcStatusCode code =
      AdbcStatementSetSqlQuery(&statement, "this is not a query", &error);
  if (code == ADBC_STATUS_OK) {
    code = AdbcStatementExecuteQuery(&statement, nullptr, nullptr, &error);
  }
  ASSERT_NE(ADBC_STATUS_OK, code);
}

void StatementTest::TestTransactions() {
  ASSERT_OK(&error, quirks()->DropTable(&connection, "bulk_ingest", &error));

  Handle<struct AdbcConnection> connection2;
  ASSERT_OK(&error, AdbcConnectionNew(&connection2.value, &error));
  ASSERT_OK(&error, AdbcConnectionInit(&connection2.value, &database, &error));

  ASSERT_OK(&error,
            AdbcConnectionSetOption(&connection, ADBC_CONNECTION_OPTION_AUTOCOMMIT,
                                    ADBC_OPTION_VALUE_DISABLED, &error));

  // Uncommitted change
  ASSERT_NO_FATAL_FAILURE(IngestSampleTable(&connection, &error));

  // Query on first connection should succeed
  {
    Handle<struct AdbcStatement> statement;
    StreamReader reader;

    ASSERT_OK(&error, AdbcStatementNew(&connection, &statement.value, &error));
    ASSERT_OK(&error, AdbcStatementSetSqlQuery(&statement.value,
                                               "SELECT * FROM bulk_ingest", &error));
    ASSERT_OK(&error, AdbcStatementExecuteQuery(&statement.value, &reader.stream.value,
                                                &reader.rows_affected, &error));
    ASSERT_NO_FATAL_FAILURE(reader.GetSchema());
  }

  if (error.release) error.release(&error);

  // Query on second connection should fail
  ASSERT_FAILS(&error, ([&]() -> AdbcStatusCode {
    Handle<struct AdbcStatement> statement;
    StreamReader reader;

    CHECK_OK(AdbcStatementNew(&connection2.value, &statement.value, &error));
    CHECK_OK(
        AdbcStatementSetSqlQuery(&statement.value, "SELECT * FROM bulk_ingest", &error));
    CHECK_OK(AdbcStatementExecuteQuery(&statement.value, &reader.stream.value,
                                       &reader.rows_affected, &error));
    return ADBC_STATUS_OK;
  })());

  if (error.release) {
    std::cerr << "Failure message: " << error.message << std::endl;
    error.release(&error);
  }

  // Rollback
  ASSERT_OK(&error, AdbcConnectionRollback(&connection, &error));

  // Query on first connection should fail
  ASSERT_FAILS(&error, ([&]() -> AdbcStatusCode {
    Handle<struct AdbcStatement> statement;
    StreamReader reader;

    CHECK_OK(AdbcStatementNew(&connection, &statement.value, &error));
    CHECK_OK(
        AdbcStatementSetSqlQuery(&statement.value, "SELECT * FROM bulk_ingest", &error));
    CHECK_OK(AdbcStatementExecuteQuery(&statement.value, &reader.stream.value,
                                       &reader.rows_affected, &error));
    return ADBC_STATUS_OK;
  })());

  // Commit
  ASSERT_NO_FATAL_FAILURE(IngestSampleTable(&connection, &error));
  ASSERT_OK(&error, AdbcConnectionCommit(&connection, &error));

  // Query on second connection should succeed
  {
    Handle<struct AdbcStatement> statement;
    StreamReader reader;

    ASSERT_OK(&error, AdbcStatementNew(&connection2.value, &statement.value, &error));
    ASSERT_OK(&error, AdbcStatementSetSqlQuery(&statement.value,
                                               "SELECT * FROM bulk_ingest", &error));
    ASSERT_OK(&error, AdbcStatementExecuteQuery(&statement.value, &reader.stream.value,
                                                &reader.rows_affected, &error));
    ASSERT_NO_FATAL_FAILURE(reader.GetSchema());
  }
}

void StatementTest::TestConcurrentStatements() {
  Handle<struct AdbcStatement> statement1;
  Handle<struct AdbcStatement> statement2;

  ASSERT_OK(&error, AdbcStatementNew(&connection, &statement1.value, &error));
  ASSERT_OK(&error, AdbcStatementNew(&connection, &statement2.value, &error));

  ASSERT_OK(&error,
            AdbcStatementSetSqlQuery(&statement1.value, "SELECT 'SaShiSuSeSo'", &error));
  ASSERT_OK(&error,
            AdbcStatementSetSqlQuery(&statement2.value, "SELECT 'SaShiSuSeSo'", &error));

  StreamReader reader1;
  StreamReader reader2;
  ASSERT_OK(&error, AdbcStatementExecuteQuery(&statement1.value, &reader1.stream.value,
                                              &reader1.rows_affected, &error));

  if (quirks()->supports_concurrent_statements()) {
    ASSERT_OK(&error, AdbcStatementExecuteQuery(&statement2.value, &reader2.stream.value,
                                                &reader2.rows_affected, &error));
    ASSERT_NO_FATAL_FAILURE(reader2.GetSchema());
  } else {
    ASSERT_FAILS(&error,
                 AdbcStatementExecuteQuery(&statement2.value, &reader2.stream.value,
                                           &reader2.rows_affected, &error));
    ASSERT_EQ(nullptr, reader2.stream.value.release);
  }
  // Original stream should still be valid
  ASSERT_NO_FATAL_FAILURE(reader1.GetSchema());
}

void StatementTest::TestResultInvalidation() {
  // Start reading from a statement, then overwrite it
  ASSERT_OK(&error, AdbcStatementNew(&connection, &statement, &error));
  ASSERT_OK(&error, AdbcStatementSetSqlQuery(&statement, "SELECT 42", &error));

  StreamReader reader1;
  StreamReader reader2;
  ASSERT_OK(&error, AdbcStatementExecuteQuery(&statement, &reader1.stream.value,
                                              &reader1.rows_affected, &error));
  ASSERT_NO_FATAL_FAILURE(reader1.GetSchema());

  ASSERT_OK(&error, AdbcStatementExecuteQuery(&statement, &reader2.stream.value,
                                              &reader2.rows_affected, &error));
  ASSERT_NO_FATAL_FAILURE(reader2.GetSchema());

  // First reader should not fail, but may give no data
  ASSERT_NO_FATAL_FAILURE(reader1.Next());
}

#undef ADBCV_CONCAT
#undef ADBCV_NAME
#undef ADBCV_FAILS_WITH_IMPL
#undef ADBCV_ASSERT_OK
#undef ADBCV_ASSERT_FAILS_WITH
}  // namespace adbc_validation
