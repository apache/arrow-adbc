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

#include "adbc_validation_util.h"
#include <arrow-adbc/adbc.h>

#include "adbc_validation.h"

namespace adbc_validation {

std::optional<std::string> ConnectionGetOption(struct AdbcConnection* connection,
                                               std::string_view option,
                                               struct AdbcError* error) {
  char buffer[128];
  size_t buffer_size = sizeof(buffer);
  AdbcStatusCode status =
      AdbcConnectionGetOption(connection, option.data(), buffer, &buffer_size, error);
  EXPECT_THAT(status, IsOkStatus(error));
  if (status != ADBC_STATUS_OK) return std::nullopt;
  EXPECT_GT(buffer_size, 0);
  if (buffer_size == 0) return std::nullopt;
  return std::string(buffer, buffer_size - 1);
}

std::optional<std::string> StatementGetOption(struct AdbcStatement* statement,
                                              std::string_view option,
                                              struct AdbcError* error) {
  char buffer[128];
  size_t buffer_size = sizeof(buffer);
  AdbcStatusCode status =
      AdbcStatementGetOption(statement, option.data(), buffer, &buffer_size, error);
  EXPECT_THAT(status, IsOkStatus(error));
  if (status != ADBC_STATUS_OK) return std::nullopt;
  EXPECT_GT(buffer_size, 0);
  if (buffer_size == 0) return std::nullopt;
  return std::string(buffer, buffer_size - 1);
}

std::string StatusCodeToString(AdbcStatusCode code) {
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
      return "(unknown code)";
  }
#undef CASE
}
std::string ToString(struct AdbcError* error) {
  if (error && error->message) {
    std::string result = error->message;
    error->release(error);
    return result;
  }
  return "";
}
std::string ToString(struct ArrowError* error) { return error ? error->message : ""; }
std::string ToString(struct ArrowArrayStream* stream) {
  if (stream && stream->get_last_error) {
    const char* error = stream->get_last_error(stream);
    if (error) return error;
  }
  return "";
}

IsErrno::IsErrno(int expected, struct ArrowArrayStream* stream, struct ArrowError* error)
    : expected_(expected), stream_(stream), error_(error) {}

bool IsErrno::MatchAndExplain(int errcode, std::ostream* os) const {
  if (errcode != expected_) {
    if (os) {
      *os << std::strerror(errcode);
      if (stream_) *os << "\nError message: " << ToString(stream_);
      if (error_) *os << "\nError message: " << ToString(error_);
    }
    return false;
  }
  return true;
}

void IsErrno::DescribeTo(std::ostream* os) const { *os << "is errno " << expected_; }

void IsErrno::DescribeNegationTo(std::ostream* os) const {
  *os << "is not errno " << expected_;
}

::testing::Matcher<int> IsOkErrno() { return IsErrno(0, nullptr, nullptr); }
::testing::Matcher<int> IsOkErrno(Handle<struct ArrowArrayStream>* stream) {
  return IsErrno(0, &stream->value, nullptr);
}
::testing::Matcher<int> IsOkErrno(struct ArrowError* error) {
  return IsErrno(0, nullptr, error);
}

IsAdbcStatusCode::IsAdbcStatusCode(AdbcStatusCode expected, struct AdbcError* error)
    : expected_(expected), error_(error) {}

bool IsAdbcStatusCode::MatchAndExplain(AdbcStatusCode actual, std::ostream* os) const {
  if (actual != expected_) {
    if (os) {
      *os << StatusCodeToString(actual);
      if (error_) {
        if (error_->message) *os << "\nError message: " << error_->message;
        if (error_->sqlstate[0]) *os << "\nSQLSTATE: " << error_->sqlstate;
        if (error_->vendor_code) *os << "\nVendor code: " << error_->vendor_code;

        if (error_->release) error_->release(error_);
      }
    }
    return false;
  }
  return true;
}

void IsAdbcStatusCode::DescribeTo(std::ostream* os) const {
  *os << "is " << StatusCodeToString(expected_);
}

void IsAdbcStatusCode::DescribeNegationTo(std::ostream* os) const {
  *os << "is not " << StatusCodeToString(expected_);
}

::testing::Matcher<AdbcStatusCode> IsOkStatus(struct AdbcError* error) {
  return IsStatus(ADBC_STATUS_OK, error);
}
::testing::Matcher<AdbcStatusCode> IsStatus(AdbcStatusCode code,
                                            struct AdbcError* error) {
  return IsAdbcStatusCode(code, error);
}

#define CHECK_ERRNO(EXPR)                             \
  do {                                                \
    if (int adbcv_errno = (EXPR); adbcv_errno != 0) { \
      return adbcv_errno;                             \
    }                                                 \
  } while (false);

static int MakeSchemaColumnImpl(struct ArrowSchema* column, const SchemaField& field) {
  switch (field.type) {
    case NANOARROW_TYPE_FIXED_SIZE_BINARY:
    case NANOARROW_TYPE_FIXED_SIZE_LIST:
      CHECK_ERRNO(ArrowSchemaSetTypeFixedSize(column, field.type, field.fixed_size));
      break;
    default:
      CHECK_ERRNO(ArrowSchemaSetType(column, field.type));
      break;
  }

  CHECK_ERRNO(ArrowSchemaSetName(column, field.name.c_str()));

  if (!field.nullable) {
    column->flags &= ~ARROW_FLAG_NULLABLE;
  }

  if (static_cast<size_t>(column->n_children) != field.children.size()) {
    return EINVAL;
  }

  switch (field.type) {
    // SetType for a list will allocate and initialize children
    case NANOARROW_TYPE_LIST:
    case NANOARROW_TYPE_LARGE_LIST:
    case NANOARROW_TYPE_FIXED_SIZE_LIST:
    case NANOARROW_TYPE_MAP: {
      size_t i = 0;
      for (const SchemaField& child : field.children) {
        CHECK_ERRNO(MakeSchemaColumnImpl(column->children[i], child));
        ++i;
      }
      break;
    }
    default:
      break;
  }

  return 0;
}

int MakeSchema(struct ArrowSchema* schema, const std::vector<SchemaField>& fields) {
  ArrowSchemaInit(schema);
  CHECK_ERRNO(ArrowSchemaSetTypeStruct(schema, fields.size()));
  size_t i = 0;
  for (const SchemaField& field : fields) {
    CHECK_ERRNO(MakeSchemaColumnImpl(schema->children[i], field));
    i++;
  }
  return 0;
}

#undef CHECK_ERRNO

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

void CompareSchema(struct ArrowSchema* schema, const std::vector<SchemaField>& fields) {
  struct ArrowError na_error;
  struct ArrowSchemaView view;

  ASSERT_THAT(ArrowSchemaViewInit(&view, schema, &na_error), IsOkErrno(&na_error));
  ASSERT_THAT(view.type,
              ::testing::AnyOf(NANOARROW_TYPE_LIST, NANOARROW_TYPE_MAP,
                               NANOARROW_TYPE_STRUCT, NANOARROW_TYPE_DENSE_UNION));
  ASSERT_EQ(fields.size(), schema->n_children);

  for (int64_t i = 0; i < schema->n_children; i++) {
    SCOPED_TRACE("Field " + std::to_string(i));
    struct ArrowSchemaView field_view;
    ASSERT_THAT(ArrowSchemaViewInit(&field_view, schema->children[i], &na_error),
                IsOkErrno(&na_error));
    ASSERT_EQ(fields[i].type, field_view.type);
    ASSERT_EQ(fields[i].nullable, (schema->children[i]->flags & ARROW_FLAG_NULLABLE) != 0)
        << "Nullability mismatch";
    if (fields[i].name != "") {
      ASSERT_STRCASEEQ(fields[i].name.c_str(), schema->children[i]->name);
    }
  }
}

std::string GetDriverVendorVersion(struct AdbcConnection* connection) {
  const uint32_t info_code = ADBC_INFO_VENDOR_VERSION;
  const uint32_t info[] = {info_code};

  adbc_validation::StreamReader reader;
  struct AdbcError error = ADBC_ERROR_INIT;
  AdbcConnectionGetInfo(connection, info, 1, &reader.stream.value, &error);
  reader.GetSchema();
  if (error.release) {
    error.release(&error);
    throw std::runtime_error("error occured calling AdbcConnectionGetInfo!");
  }

  reader.Next();
  const ArrowStringView raw_version =
      ArrowArrayViewGetStringUnsafe(reader.array_view->children[1]->children[0], 0);
  const std::string version(raw_version.data, raw_version.size_bytes);

  return version;
}

}  // namespace adbc_validation
