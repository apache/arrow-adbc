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
#include <adbc.h>

#include "adbc_validation.h"

namespace adbc_validation {
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

int64_t ArrowArrayViewGetOffsetUnsafe(struct ArrowArrayView* array_view, int64_t i) {
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

int MakeSchema(struct ArrowSchema* schema, const std::vector<SchemaField>& fields) {
  CHECK_ERRNO(ArrowSchemaInit(schema, NANOARROW_TYPE_STRUCT));
  CHECK_ERRNO(ArrowSchemaAllocateChildren(schema, fields.size()));
  size_t i = 0;
  for (const SchemaField& field : fields) {
    CHECK_ERRNO(ArrowSchemaInit(schema->children[i], field.type));
    CHECK_ERRNO(ArrowSchemaSetName(schema->children[i], field.name.c_str()));
    if (!field.nullable) {
      schema->children[i]->flags &= ~ARROW_FLAG_NULLABLE;
    }
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

void CompareSchema(
    struct ArrowSchema* schema,
    const std::vector<std::tuple<std::optional<std::string>, ArrowType, bool>>& fields) {
  struct ArrowError na_error;
  struct ArrowSchemaView view;

  ASSERT_THAT(ArrowSchemaViewInit(&view, schema, &na_error), IsOkErrno(&na_error));
  ASSERT_THAT(view.data_type, ::testing::AnyOf(NANOARROW_TYPE_LIST, NANOARROW_TYPE_STRUCT,
                                               NANOARROW_TYPE_DENSE_UNION));
  ASSERT_EQ(fields.size(), schema->n_children);

  for (int64_t i = 0; i < schema->n_children; i++) {
    SCOPED_TRACE("Field " + std::to_string(i));
    struct ArrowSchemaView field_view;
    ASSERT_THAT(ArrowSchemaViewInit(&field_view, schema->children[i], &na_error),
                IsOkErrno(&na_error));
    ASSERT_EQ(std::get<1>(fields[i]), field_view.data_type);
    ASSERT_EQ(std::get<2>(fields[i]),
              (schema->children[i]->flags & ARROW_FLAG_NULLABLE) != 0)
        << "Nullability mismatch";
    if (std::get<0>(fields[i]).has_value()) {
      ASSERT_EQ(*std::get<0>(fields[i]), schema->children[i]->name);
    }
  }
}

}  // namespace adbc_validation
