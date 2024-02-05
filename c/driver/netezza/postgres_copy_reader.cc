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

#include "postgres_copy_reader.h"

namespace adbcpq {

  void PostgresCopyFieldReader::Init(const NetezzaType& pg_type) { pg_type_ = pg_type; }

  const NetezzaType& PostgresCopyFieldReader::InputType() const { return pg_type_; }

  ArrowErrorCode PostgresCopyFieldReader::InitSchema(ArrowSchema* schema) {
    NANOARROW_RETURN_NOT_OK(ArrowSchemaViewInit(&schema_view_, schema, nullptr));
    return NANOARROW_OK;
  }

  ArrowErrorCode PostgresCopyFieldReader::InitArray(ArrowArray* array) {
    // Cache some buffer pointers
    validity_ = ArrowArrayValidityBitmap(array);
    for (int32_t i = 0; i < 3; i++) {
      switch (schema_view_.layout.buffer_type[i]) {
        case NANOARROW_BUFFER_TYPE_DATA_OFFSET:
          if (schema_view_.layout.element_size_bits[i] == 32) {
            offsets_ = ArrowArrayBuffer(array, i);
          }
          break;
        case NANOARROW_BUFFER_TYPE_DATA:
          data_ = ArrowArrayBuffer(array, i);
          break;
        default:
          break;
      }
    }

    return NANOARROW_OK;
  }

  ArrowErrorCode PostgresCopyFieldReader::Read(ArrowBufferView* data, int32_t field_size_bytes,
                              ArrowArray* array, ArrowError* error) {
    return ENOTSUP;
  }

  ArrowErrorCode PostgresCopyFieldReader::FinishArray(ArrowArray* array, ArrowError* error) {
    return NANOARROW_OK;
  }

  ArrowErrorCode PostgresCopyFieldReader::AppendValid(ArrowArray* array) {
    if (validity_->buffer.data != nullptr) {
      NANOARROW_RETURN_NOT_OK(ArrowBitmapAppend(validity_, true, 1));
    }

    array->length++;
    return NANOARROW_OK;
  }

// Reader for a Postgres boolean (one byte -> bitmap)

  ArrowErrorCode PostgresCopyBooleanFieldReader::Read(ArrowBufferView* data, int32_t field_size_bytes, ArrowArray* array,
                      ArrowError* error) {
    if (field_size_bytes <= 0) {
      return ArrowArrayAppendNull(array, 1);
    }

    if (field_size_bytes != 1) {
      ArrowErrorSet(error, "Expected field with one byte but found field with %d bytes",
                    static_cast<int>(field_size_bytes));  // NOLINT(runtime/int)
      return EINVAL;
    }

    int64_t bytes_required = _ArrowBytesForBits(array->length + 1);
    if (bytes_required > data_->size_bytes) {
      NANOARROW_RETURN_NOT_OK(
          ArrowBufferAppendFill(data_, 0, bytes_required - data_->size_bytes));
    }

    if (ReadUnsafe<int8_t>(data)) {
      ArrowBitSet(data_->data, array->length);
    } else {
      ArrowBitClear(data_->data, array->length);
    }

    return AppendValid(array);
  }


// Reader for Pg->Arrow conversions whose representations are identical minus
// the bswap from network endian. This includes all integral and float types.
  template <typename T, T kOffset>
  ArrowErrorCode PostgresCopyNetworkEndianFieldReader<T, kOffset>::Read(ArrowBufferView* data, int32_t field_size_bytes, ArrowArray* array,
                      ArrowError* error) {
    if (field_size_bytes <= 0) {
      return ArrowArrayAppendNull(array, 1);
    }

    if (field_size_bytes != static_cast<int32_t>(sizeof(T))) {
      ArrowErrorSet(error, "Expected field with %d bytes but found field with %d bytes",
                    static_cast<int>(sizeof(T)),
                    static_cast<int>(field_size_bytes));  // NOLINT(runtime/int)
      return EINVAL;
    }

    T value = kOffset + ReadUnsafe<T>(data);
    NANOARROW_RETURN_NOT_OK(ArrowBufferAppend(data_, &value, sizeof(T)));
    return AppendValid(array);
  }

// Reader for Intervals
  ArrowErrorCode PostgresCopyIntervalFieldReader::Read(ArrowBufferView* data, int32_t field_size_bytes, ArrowArray* array,
                      ArrowError* error) {
    if (field_size_bytes <= 0) {
      return ArrowArrayAppendNull(array, 1);
    }

    if (field_size_bytes != 16) {
      ArrowErrorSet(error, "Expected field with %d bytes but found field with %d bytes",
                    16,
                    static_cast<int>(field_size_bytes));  // NOLINT(runtime/int)
      return EINVAL;
    }

    // postgres stores time as usec, arrow stores as ns
    const int64_t time_usec = ReadUnsafe<int64_t>(data);
    int64_t time;

    if (time_usec > kMaxSafeMicrosToNanos || time_usec < kMinSafeMicrosToNanos) {
      ArrowErrorSet(error,
                    "[libpq] Interval with time value %" PRId64
                    " usec would overflow when converting to nanoseconds",
                    time_usec);
      return EINVAL;
    }

    time = time_usec * 1000;

    const int32_t days = ReadUnsafe<int32_t>(data);
    const int32_t months = ReadUnsafe<int32_t>(data);

    NANOARROW_RETURN_NOT_OK(ArrowBufferAppend(data_, &months, sizeof(int32_t)));
    NANOARROW_RETURN_NOT_OK(ArrowBufferAppend(data_, &days, sizeof(int32_t)));
    NANOARROW_RETURN_NOT_OK(ArrowBufferAppend(data_, &time, sizeof(int64_t)));
    return AppendValid(array);
  }


// // Converts COPY resulting from the Postgres NUMERIC type into a string.
// Rewritten based on the Postgres implementation of NUMERIC cast to string in
// src/backend/utils/adt/numeric.c : get_str_from_var() (Note that in the initial source,
// DEC_DIGITS is always 4 and DBASE is always 10000).
//
// Briefly, the Postgres representation of "numeric" is an array of int16_t ("digits")
// from most significant to least significant. Each "digit" is a value between 0000 and
// 9999. There are weight + 1 digits before the decimal point and dscale digits after the
// decimal point. Both of those values can be zero or negative. A "sign" component
// encodes the positive or negativeness of the value and is also used to encode special
// values (inf, -inf, and nan).
  ArrowErrorCode PostgresCopyNumericFieldReader::Read(ArrowBufferView* data, int32_t field_size_bytes, ArrowArray* array,
                      ArrowError* error) {
    // -1 for NULL
    if (field_size_bytes < 0) {
      return ArrowArrayAppendNull(array, 1);
    }

    // Read the input
    if (data->size_bytes < static_cast<int64_t>(4 * sizeof(int16_t))) {
      ArrowErrorSet(error,
                    "Expected at least %d bytes of field data for numeric copy data but "
                    "only %d bytes of input remain",
                    static_cast<int>(4 * sizeof(int16_t)),
                    static_cast<int>(data->size_bytes));  // NOLINT(runtime/int)
      return EINVAL;
    }

    int16_t ndigits = ReadUnsafe<int16_t>(data);
    int16_t weight = ReadUnsafe<int16_t>(data);
    uint16_t sign = ReadUnsafe<uint16_t>(data);
    uint16_t dscale = ReadUnsafe<uint16_t>(data);

    if (data->size_bytes < static_cast<int64_t>(ndigits * sizeof(int16_t))) {
      ArrowErrorSet(error,
                    "Expected at least %d bytes of field data for numeric digits copy "
                    "data but only %d bytes of input remain",
                    static_cast<int>(ndigits * sizeof(int16_t)),
                    static_cast<int>(data->size_bytes));  // NOLINT(runtime/int)
      return EINVAL;
    }

    digits_.clear();
    for (int16_t i = 0; i < ndigits; i++) {
      digits_.push_back(ReadUnsafe<int16_t>(data));
    }

    // Handle special values
    std::string special_value;
    switch (sign) {
      case kNumericNAN:
        special_value = std::string("nan");
        break;
      case kNumericPinf:
        special_value = std::string("inf");
        break;
      case kNumericNinf:
        special_value = std::string("-inf");
        break;
      case kNumericPos:
      case kNumericNeg:
        special_value = std::string("");
        break;
      default:
        ArrowErrorSet(error,
                      "Unexpected value for sign read from Postgres numeric field: %d",
                      static_cast<int>(sign));
        return EINVAL;
    }

    if (!special_value.empty()) {
      NANOARROW_RETURN_NOT_OK(
          ArrowBufferAppend(data_, special_value.data(), special_value.size()));
      NANOARROW_RETURN_NOT_OK(ArrowBufferAppendInt32(offsets_, data_->size_bytes));
      return AppendValid(array);
    }

    // Calculate string space requirement
    int64_t max_chars_required = std::max<int64_t>(1, (weight + 1) * kDecDigits);
    max_chars_required += dscale + kDecDigits + 2;
    NANOARROW_RETURN_NOT_OK(ArrowBufferReserve(data_, max_chars_required));
    char* out0 = reinterpret_cast<char*>(data_->data + data_->size_bytes);
    char* out = out0;

    // Build output string in-place, starting with the negative sign
    if (sign == kNumericNeg) {
      *out++ = '-';
    }

    // ...then digits before the decimal point
    int d;
    int d1;
    int16_t dig;

    if (weight < 0) {
      d = weight + 1;
      *out++ = '0';
    } else {
      for (d = 0; d <= weight; d++) {
        if (d < ndigits) {
          dig = digits_[d];
        } else {
          dig = 0;
        }

        // To strip leading zeroes
        int append = (d > 0);

        for (const auto pow10 : {1000, 100, 10, 1}) {
          d1 = dig / pow10;
          dig -= d1 * pow10;
          append |= (d1 > 0);
          if (append) {
            *out++ = d1 + '0';
          }
        }
      }
    }

    // ...then the decimal point + digits after it. This may write more digits
    // than specified by dscale so we need to keep track of how many we want to
    // keep here.
    int64_t actual_chars_required = out - out0;

    if (dscale > 0) {
      *out++ = '.';
      actual_chars_required += dscale + 1;

      for (int i = 0; i < dscale; i++, d++, i += kDecDigits) {
        if (d >= 0 && d < ndigits) {
          dig = digits_[d];
        } else {
          dig = 0;
        }

        for (const auto pow10 : {1000, 100, 10, 1}) {
          d1 = dig / pow10;
          dig -= d1 * pow10;
          *out++ = d1 + '0';
        }
      }
    }

    // Update data buffer size and add offsets
    data_->size_bytes += actual_chars_required;
    NANOARROW_RETURN_NOT_OK(ArrowBufferAppendInt32(offsets_, data_->size_bytes));
    return AppendValid(array);
  }



// Reader for Pg->Arrow conversions whose Arrow representation is simply the
// bytes of the field representation. This can be used with binary and string
// Arrow types and any Postgres type.
  ArrowErrorCode PostgresCopyBinaryFieldReader::Read(ArrowBufferView* data, int32_t field_size_bytes, ArrowArray* array,
                      ArrowError* error) {
    // -1 for NULL (0 would be empty string)
    if (field_size_bytes < 0) {
      return ArrowArrayAppendNull(array, 1);
    }

    if (field_size_bytes > data->size_bytes) {
      ArrowErrorSet(error, "Expected %d bytes of field data but got %d bytes of input",
                    static_cast<int>(field_size_bytes),
                    static_cast<int>(data->size_bytes));  // NOLINT(runtime/int)
      return EINVAL;
    }

    NANOARROW_RETURN_NOT_OK(ArrowBufferAppend(data_, data->data.data, field_size_bytes));
    data->data.as_uint8 += field_size_bytes;
    data->size_bytes -= field_size_bytes;

    int32_t* offsets = reinterpret_cast<int32_t*>(offsets_->data);
    NANOARROW_RETURN_NOT_OK(
        ArrowBufferAppendInt32(offsets_, offsets[array->length] + field_size_bytes));

    return AppendValid(array);
  }


  void PostgresCopyArrayFieldReader::InitChild(std::unique_ptr<PostgresCopyFieldReader> child) {
    child_ = std::move(child);
    child_->Init(pg_type_.child(0));
  }

  ArrowErrorCode PostgresCopyArrayFieldReader::InitSchema(ArrowSchema* schema)  {
    NANOARROW_RETURN_NOT_OK(PostgresCopyFieldReader::InitSchema(schema));
    NANOARROW_RETURN_NOT_OK(child_->InitSchema(schema->children[0]));
    return NANOARROW_OK;
  }

  ArrowErrorCode PostgresCopyArrayFieldReader::InitArray(ArrowArray* array)  {
    NANOARROW_RETURN_NOT_OK(PostgresCopyFieldReader::InitArray(array));
    NANOARROW_RETURN_NOT_OK(child_->InitArray(array->children[0]));
    return NANOARROW_OK;
  }

  ArrowErrorCode PostgresCopyArrayFieldReader::Read(ArrowBufferView* data, int32_t field_size_bytes, ArrowArray* array,
                      ArrowError* error)  {
    if (field_size_bytes <= 0) {
      return ArrowArrayAppendNull(array, 1);
    }

    // Keep the cursor where we start to parse the array so we can check
    // the number of bytes read against the field size when finished
    const uint8_t* data0 = data->data.as_uint8;

    int32_t n_dim;
    NANOARROW_RETURN_NOT_OK(ReadChecked<int32_t>(data, &n_dim, error));
    int32_t flags;
    NANOARROW_RETURN_NOT_OK(ReadChecked<int32_t>(data, &flags, error));
    uint32_t element_type_oid;
    NANOARROW_RETURN_NOT_OK(ReadChecked<uint32_t>(data, &element_type_oid, error));

    // We could validate the OID here, but this is a poor fit for all cases
    // (e.g. testing) since the OID can be specific to each database

    if (n_dim < 0) {
      ArrowErrorSet(error, "Expected array n_dim > 0 but got %d",
                    static_cast<int>(n_dim));  // NOLINT(runtime/int)
      return EINVAL;
    }

    // This is apparently allowed
    if (n_dim == 0) {
      NANOARROW_RETURN_NOT_OK(ArrowArrayFinishElement(array));
      return NANOARROW_OK;
    }

    int64_t n_items = 1;
    for (int32_t i = 0; i < n_dim; i++) {
      int32_t dim_size;
      NANOARROW_RETURN_NOT_OK(ReadChecked<int32_t>(data, &dim_size, error));
      n_items *= dim_size;

      int32_t lower_bound;
      NANOARROW_RETURN_NOT_OK(ReadChecked<int32_t>(data, &lower_bound, error));
      if (lower_bound != 1) {
        ArrowErrorSet(error, "Array value with lower bound != 1 is not supported");
        return EINVAL;
      }
    }

    for (int64_t i = 0; i < n_items; i++) {
      int32_t child_field_size_bytes;
      NANOARROW_RETURN_NOT_OK(ReadChecked<int32_t>(data, &child_field_size_bytes, error));
      NANOARROW_RETURN_NOT_OK(
          child_->Read(data, child_field_size_bytes, array->children[0], error));
    }

    int64_t bytes_read = data->data.as_uint8 - data0;
    if (bytes_read != field_size_bytes) {
      ArrowErrorSet(error, "Expected to read %d bytes from array field but read %d bytes",
                    static_cast<int>(field_size_bytes),
                    static_cast<int>(bytes_read));  // NOLINT(runtime/int)
      return EINVAL;
    }

    NANOARROW_RETURN_NOT_OK(ArrowArrayFinishElement(array));
    return NANOARROW_OK;
  }


  void PostgresCopyRecordFieldReader::AppendChild(std::unique_ptr<PostgresCopyFieldReader> child) {
    int64_t child_i = static_cast<int64_t>(children_.size());
    children_.push_back(std::move(child));
    children_[child_i]->Init(pg_type_.child(child_i));
  }

  ArrowErrorCode PostgresCopyRecordFieldReader::InitSchema(ArrowSchema* schema)  {
    NANOARROW_RETURN_NOT_OK(PostgresCopyFieldReader::InitSchema(schema));
    for (int64_t i = 0; i < schema->n_children; i++) {
      NANOARROW_RETURN_NOT_OK(children_[i]->InitSchema(schema->children[i]));
    }

    return NANOARROW_OK;
  }

  ArrowErrorCode PostgresCopyRecordFieldReader::InitArray(ArrowArray* array)  {
    NANOARROW_RETURN_NOT_OK(PostgresCopyFieldReader::InitArray(array));
    for (int64_t i = 0; i < array->n_children; i++) {
      NANOARROW_RETURN_NOT_OK(children_[i]->InitArray(array->children[i]));
    }

    return NANOARROW_OK;
  }

  ArrowErrorCode PostgresCopyRecordFieldReader::Read(ArrowBufferView* data, int32_t field_size_bytes, ArrowArray* array,
                      ArrowError* error)  {
    if (field_size_bytes < 0) {
      return ArrowArrayAppendNull(array, 1);
    }

    // Keep the cursor where we start to parse the field so we can check
    // the number of bytes read against the field size when finished
    const uint8_t* data0 = data->data.as_uint8;

    int32_t n_fields;
    NANOARROW_RETURN_NOT_OK(ReadChecked<int32_t>(data, &n_fields, error));
    if (n_fields != array->n_children) {
      ArrowErrorSet(error, "Expected nested record type to have %ld fields but got %d",
                    static_cast<long>(array->n_children),  // NOLINT(runtime/int)
                    static_cast<int>(n_fields));           // NOLINT(runtime/int)
      return EINVAL;
    }

    for (int32_t i = 0; i < n_fields; i++) {
      uint32_t child_oid;
      NANOARROW_RETURN_NOT_OK(ReadChecked<uint32_t>(data, &child_oid, error));

      int32_t child_field_size_bytes;
      NANOARROW_RETURN_NOT_OK(ReadChecked<int32_t>(data, &child_field_size_bytes, error));
      int result =
          children_[i]->Read(data, child_field_size_bytes, array->children[i], error);

      // On overflow, pretend all previous children for this struct were never
      // appended to. This leaves array in a valid state in the specific case
      // where EOVERFLOW was returned so that a higher level caller can attempt
      // to try again after creating a new array.
      if (result == EOVERFLOW) {
        for (int16_t j = 0; j < i; j++) {
          array->children[j]->length--;
        }
      }

      if (result != NANOARROW_OK) {
        return result;
      }
    }

    // field size == -1 means don't check (e.g., for a top-level row tuple)
    int64_t bytes_read = data->data.as_uint8 - data0;
    if (field_size_bytes != -1 && bytes_read != field_size_bytes) {
      ArrowErrorSet(error,
                    "Expected to read %d bytes from record field but read %d bytes",
                    static_cast<int>(field_size_bytes),
                    static_cast<int>(bytes_read));  // NOLINT(runtime/int)
      return EINVAL;
    }

    array->length++;
    return NANOARROW_OK;
  }

 

// Subtely different from a Record field item: field count is an int16_t
// instead of an int32_t and each field is not prefixed by its OID.
  void PostgresCopyFieldTupleReader::AppendChild(std::unique_ptr<PostgresCopyFieldReader> child) {
    int64_t child_i = static_cast<int64_t>(children_.size());
    children_.push_back(std::move(child));
    children_[child_i]->Init(pg_type_.child(child_i));
  }

  ArrowErrorCode PostgresCopyFieldTupleReader::InitSchema(ArrowSchema* schema)  {
    NANOARROW_RETURN_NOT_OK(PostgresCopyFieldReader::InitSchema(schema));
    for (int64_t i = 0; i < schema->n_children; i++) {
      NANOARROW_RETURN_NOT_OK(children_[i]->InitSchema(schema->children[i]));
    }

    return NANOARROW_OK;
  }

  ArrowErrorCode PostgresCopyFieldTupleReader::InitArray(ArrowArray* array)  {
    NANOARROW_RETURN_NOT_OK(PostgresCopyFieldReader::InitArray(array));
    for (int64_t i = 0; i < array->n_children; i++) {
      NANOARROW_RETURN_NOT_OK(children_[i]->InitArray(array->children[i]));
    }

    return NANOARROW_OK;
  }

  ArrowErrorCode PostgresCopyFieldTupleReader::Read(ArrowBufferView* data, int32_t field_size_bytes, ArrowArray* array,
                      ArrowError* error)  {
    int16_t n_fields;
    NANOARROW_RETURN_NOT_OK(ReadChecked<int16_t>(data, &n_fields, error));
    if (n_fields == -1) {
      return ENODATA;
    } else if (n_fields != array->n_children) {
      ArrowErrorSet(error,
                    "Expected -1 for end-of-stream or number of fields in output array "
                    "(%ld) but got %d",
                    static_cast<long>(array->n_children),  // NOLINT(runtime/int)
                    static_cast<int>(n_fields));           // NOLINT(runtime/int)
      return EINVAL;
    }

    for (int16_t i = 0; i < n_fields; i++) {
      int32_t child_field_size_bytes;
      NANOARROW_RETURN_NOT_OK(ReadChecked<int32_t>(data, &child_field_size_bytes, error));
      int result =
          children_[i]->Read(data, child_field_size_bytes, array->children[i], error);

      // On overflow, pretend all previous children for this struct were never
      // appended to. This leaves array in a valid state in the specific case
      // where EOVERFLOW was returned so that a higher level caller can attemptgit submodule update --init --recursive
      // to try again after creating a new array.
      if (result == EOVERFLOW) {
        for (int16_t j = 0; j < i; j++) {
          array->children[j]->length--;
        }
      }

      if (result != NANOARROW_OK) {
        return result;
      }
    }

    array->length++;
    return NANOARROW_OK;
  }

// Factory for a PostgresCopyFieldReader that instantiates the proper subclass
// and gives a nice error for Postgres type -> Arrow type conversions that aren't
// supported.
 ArrowErrorCode ErrorCantConvert(ArrowError* error,
                                              const NetezzaType& pg_type,
                                              const ArrowSchemaView& schema_view) {
  ArrowErrorSet(error, "Can't convert Postgres type '%s' to Arrow type '%s'",
                pg_type.typname().c_str(),
                ArrowTypeString(schema_view.type));  // NOLINT(runtime/int)
  return EINVAL;
}

 ArrowErrorCode MakeCopyFieldReader(const NetezzaType& pg_type,
                                                 ArrowSchema* schema,
                                                 PostgresCopyFieldReader** out,
                                                 ArrowError* error) {
  ArrowSchemaView schema_view;
  NANOARROW_RETURN_NOT_OK(ArrowSchemaViewInit(&schema_view, schema, nullptr));

  switch (schema_view.type) {
    case NANOARROW_TYPE_BOOL:
      switch (pg_type.type_id()) {
        case NetezzaTypeId::kBool:
          *out = new PostgresCopyBooleanFieldReader();
          return NANOARROW_OK;
        default:
          return ErrorCantConvert(error, pg_type, schema_view);
      }

    case NANOARROW_TYPE_INT16:
      switch (pg_type.type_id()) {
        case NetezzaTypeId::kInt2:
          *out = new PostgresCopyNetworkEndianFieldReader<int16_t>();
          return NANOARROW_OK;
        default:
          return ErrorCantConvert(error, pg_type, schema_view);
      }

    case NANOARROW_TYPE_INT32:
      switch (pg_type.type_id()) {
        case NetezzaTypeId::kInt4:
        case NetezzaTypeId::kOid:
        case NetezzaTypeId::kRegproc:
          *out = new PostgresCopyNetworkEndianFieldReader<int32_t>();
          return NANOARROW_OK;
        default:
          return ErrorCantConvert(error, pg_type, schema_view);
      }

    case NANOARROW_TYPE_INT64:
      switch (pg_type.type_id()) {
        case NetezzaTypeId::kInt8:
          *out = new PostgresCopyNetworkEndianFieldReader<int64_t>();
          return NANOARROW_OK;
        default:
          return ErrorCantConvert(error, pg_type, schema_view);
      }

    case NANOARROW_TYPE_FLOAT:
      switch (pg_type.type_id()) {
        case NetezzaTypeId::kFloat4:
          *out = new PostgresCopyNetworkEndianFieldReader<uint32_t>();
          return NANOARROW_OK;
        default:
          return ErrorCantConvert(error, pg_type, schema_view);
      }

    case NANOARROW_TYPE_DOUBLE:
      switch (pg_type.type_id()) {
        case NetezzaTypeId::kFloat8:
          *out = new PostgresCopyNetworkEndianFieldReader<uint64_t>();
          return NANOARROW_OK;
        default:
          return ErrorCantConvert(error, pg_type, schema_view);
      }

    case NANOARROW_TYPE_STRING:
      switch (pg_type.type_id()) {
        case NetezzaTypeId::kChar:
        case NetezzaTypeId::kVarchar:
        case NetezzaTypeId::kText:
        case NetezzaTypeId::kBpchar:
        case NetezzaTypeId::kName:
          *out = new PostgresCopyBinaryFieldReader();
          return NANOARROW_OK;
        case NetezzaTypeId::kNumeric:
          *out = new PostgresCopyNumericFieldReader();
          return NANOARROW_OK;
        default:
          return ErrorCantConvert(error, pg_type, schema_view);
      }

    case NANOARROW_TYPE_BINARY:
      // No need to check pg_type here: we can return the bytes of any
      // Postgres type as binary.
      *out = new PostgresCopyBinaryFieldReader();
      return NANOARROW_OK;

    case NANOARROW_TYPE_LIST:
      switch (pg_type.type_id()) {
        case NetezzaTypeId::kUnkbinary: {
          if (pg_type.n_children() != 1) {
            ArrowErrorSet(
                error, "Expected Postgres array type to have one child but found %ld",
                static_cast<long>(pg_type.n_children()));  // NOLINT(runtime/int)
            return EINVAL;
          }

          auto array_reader = std::unique_ptr<PostgresCopyArrayFieldReader>(
              new PostgresCopyArrayFieldReader());
          array_reader->Init(pg_type);

          PostgresCopyFieldReader* child_reader;
          NANOARROW_RETURN_NOT_OK(MakeCopyFieldReader(
              pg_type.child(0), schema->children[0], &child_reader, error));
          array_reader->InitChild(std::unique_ptr<PostgresCopyFieldReader>(child_reader));

          *out = array_reader.release();
          return NANOARROW_OK;
        }
        default:
          return ErrorCantConvert(error, pg_type, schema_view);
      }

    case NANOARROW_TYPE_STRUCT:
      switch (pg_type.type_id()) {
        case NetezzaTypeId::kUnkbinary: {
          if (pg_type.n_children() != schema->n_children) {
            ArrowErrorSet(error,
                          "Can't convert Postgres record type with %ld chlidren to Arrow "
                          "struct type with %ld children",
                          static_cast<long>(pg_type.n_children()),  // NOLINT(runtime/int)
                          static_cast<long>(schema->n_children));   // NOLINT(runtime/int)
            return EINVAL;
          }

          auto record_reader = std::unique_ptr<PostgresCopyRecordFieldReader>(
              new PostgresCopyRecordFieldReader());
          record_reader->Init(pg_type);

          for (int64_t i = 0; i < pg_type.n_children(); i++) {
            PostgresCopyFieldReader* child_reader;
            NANOARROW_RETURN_NOT_OK(MakeCopyFieldReader(
                pg_type.child(i), schema->children[i], &child_reader, error));
            record_reader->AppendChild(
                std::unique_ptr<PostgresCopyFieldReader>(child_reader));
          }

          *out = record_reader.release();
          return NANOARROW_OK;
        }
        default:
          return ErrorCantConvert(error, pg_type, schema_view);
      }

    case NANOARROW_TYPE_DATE32: {
      // 2000-01-01
      constexpr int32_t kPostgresDateEpoch = 10957;
      *out = new PostgresCopyNetworkEndianFieldReader<int32_t, kPostgresDateEpoch>();
      return NANOARROW_OK;
    }

    case NANOARROW_TYPE_TIME64: {
      *out = new PostgresCopyNetworkEndianFieldReader<int64_t>();
      return NANOARROW_OK;
    }

    case NANOARROW_TYPE_TIMESTAMP:
      switch (pg_type.type_id()) {
        case NetezzaTypeId::kTimestamp: {
          // 2000-01-01 00:00:00.000000 in microseconds
          constexpr int64_t kPostgresTimestampEpoch = 946684800000000;
          *out = new PostgresCopyNetworkEndianFieldReader<int64_t,
                                                          kPostgresTimestampEpoch>();
          return NANOARROW_OK;
        }
        default:
          return ErrorCantConvert(error, pg_type, schema_view);
      }
    case NANOARROW_TYPE_INTERVAL_MONTH_DAY_NANO:
      switch (pg_type.type_id()) {
        case NetezzaTypeId::kInterval: {
          *out = new PostgresCopyIntervalFieldReader();
          return NANOARROW_OK;
        }
        default:
          return ErrorCantConvert(error, pg_type, schema_view);
      }

    default:
      return ErrorCantConvert(error, pg_type, schema_view);
  }
}

  ArrowErrorCode PostgresCopyStreamReader::Init(NetezzaType pg_type) {

    pg_type_ = std::move(pg_type);
    root_reader_.Init(pg_type_);
    array_size_approx_bytes_ = 0;
    return NANOARROW_OK;
  }

  int64_t PostgresCopyStreamReader::array_size_approx_bytes() const { return array_size_approx_bytes_; }

  ArrowErrorCode PostgresCopyStreamReader::SetOutputSchema(ArrowSchema* schema, ArrowError* error) {
    if (std::string(schema_->format) != "+s") {
      ArrowErrorSet(
          error,
          "Expected output schema of type struct but got output schema with format '%s'",
          schema_->format);  // NOLINT(runtime/int)
      return EINVAL;
    }

    if (schema_->n_children != root_reader_.InputType().n_children()) {
      ArrowErrorSet(error,
                    "Expected output schema with %ld columns to match Postgres input but "
                    "got schema with %ld columns",
                    static_cast<long>(  // NOLINT(runtime/int)
                        root_reader_.InputType().n_children()),
                    static_cast<long>(schema->n_children));  // NOLINT(runtime/int)
      return EINVAL;
    }

    schema_.reset(schema);
    return NANOARROW_OK;
  }

  ArrowErrorCode PostgresCopyStreamReader::InferOutputSchema(ArrowError* error) {
    schema_.reset();
    ArrowSchemaInit(schema_.get());
    NANOARROW_RETURN_NOT_OK(root_reader_.InputType().SetSchema(schema_.get()));
    return NANOARROW_OK;
  }

  ArrowErrorCode PostgresCopyStreamReader::InitFieldReaders(ArrowError* error) {
    if (schema_->release == nullptr) {
      return EINVAL;
    }

    const NetezzaType& root_type = root_reader_.InputType();

    for (int64_t i = 0; i < root_type.n_children(); i++) {
      const NetezzaType& child_type = root_type.child(i);
      PostgresCopyFieldReader* child_reader;
      MakeCopyFieldReader(child_type, schema_->children[i], &child_reader, error);
      //NANOARROW_RETURN_NOT_OK(MakeCopyFieldReader(child_type, schema_->children[i], &child_reader, error));
      root_reader_.AppendChild(std::unique_ptr<PostgresCopyFieldReader>(child_reader));
    }

    NANOARROW_RETURN_NOT_OK(root_reader_.InitSchema(schema_.get()));
    return NANOARROW_OK;
  }

  /*ArrowErrorCode PostgresCopyStreamReader::ReadHeader(ArrowBufferView* data, ArrowError* error) {
    // Commented as unused, kept for reference.
    if (data->size_bytes < static_cast<int64_t>(sizeof(kPgCopyBinarySignature))) {
      ArrowErrorSet(
          error,
          "Expected PGCOPY signature of %ld bytes at beginning of stream but "
          "found %ld bytes of input",
          static_cast<long>(sizeof(kPgCopyBinarySignature)),  // NOLINT(runtime/int)
          static_cast<long>(data->size_bytes));               // NOLINT(runtime/int)
      return EINVAL;
    }

    if (memcmp(data->data.data, kPgCopyBinarySignature, sizeof(kPgCopyBinarySignature)) !=
        0) {
      ArrowErrorSet(error, "Invalid PGCOPY signature at beginning of stream");
      return EINVAL;
    }

    data->data.as_uint8 += sizeof(kPgCopyBinarySignature);
    data->size_bytes -= sizeof(kPgCopyBinarySignature);

    uint32_t flags;
    NANOARROW_RETURN_NOT_OK(ReadChecked<uint32_t>(data, &flags, error));
    uint32_t extension_length;
    NANOARROW_RETURN_NOT_OK(ReadChecked<uint32_t>(data, &extension_length, error));

    if (data->size_bytes < static_cast<int64_t>(extension_length)) {
      ArrowErrorSet(error,
                    "Expected %ld bytes of extension metadata at start of stream but "
                    "found %ld bytes of input",
                    static_cast<long>(extension_length),   // NOLINT(runtime/int)
                    static_cast<long>(data->size_bytes));  // NOLINT(runtime/int)
      return EINVAL;
    }

    data->data.as_uint8 += extension_length;
    data->size_bytes -= extension_length;
    return NANOARROW_OK;
  }*/

  ArrowErrorCode PostgresCopyStreamReader::ReadRecord(ArrowBufferView* data, ArrowError* error) {
    if (array_->release == nullptr) {
      NANOARROW_RETURN_NOT_OK(ArrowArrayInitFromSchema(array_.get(), schema_.get(), error));
      NANOARROW_RETURN_NOT_OK(ArrowArrayStartAppending(array_.get()));
      NANOARROW_RETURN_NOT_OK(root_reader_.InitArray(array_.get()));
      array_size_approx_bytes_ = 0;
    }

    const uint8_t* start = data->data.as_uint8;
    NANOARROW_RETURN_NOT_OK(root_reader_.Read(data, -1, array_.get(), error));
    array_size_approx_bytes_ += (data->data.as_uint8 - start);
    return NANOARROW_OK;
  }

  ArrowErrorCode PostgresCopyStreamReader::GetSchema(ArrowSchema* out) {
    return ArrowSchemaDeepCopy(schema_.get(), out);
  }

  ArrowErrorCode PostgresCopyStreamReader::GetArray(ArrowArray* out, ArrowError* error) {
    if (array_->release == nullptr) {
      return EINVAL;
    }

    NANOARROW_RETURN_NOT_OK(ArrowArrayFinishBuildingDefault(array_.get(), error));
    ArrowArrayMove(array_.get(), out);
    return NANOARROW_OK;
  }

  const NetezzaType& PostgresCopyStreamReader::pg_type() const { return pg_type_; }


  void PostgresCopyFieldWriter::Init(struct ArrowArrayView* array_view) { array_view_ = array_view; }

  ArrowErrorCode PostgresCopyFieldWriter::Write(ArrowBuffer* buffer, int64_t index, ArrowError* error) {
    return ENOTSUP;
  }

  void PostgresCopyFieldTupleWriter::AppendChild(std::unique_ptr<PostgresCopyFieldWriter> child) {
    int64_t child_i = static_cast<int64_t>(children_.size());
    children_.push_back(std::move(child));
    children_[child_i]->Init(array_view_->children[child_i]);
  }

  ArrowErrorCode PostgresCopyFieldTupleWriter::Write(ArrowBuffer* buffer, int64_t index, ArrowError* error)  {
    if (index >= array_view_->length) {
      return ENODATA;
    }

    const int16_t n_fields = children_.size();
    NANOARROW_RETURN_NOT_OK(WriteChecked<int16_t>(buffer, n_fields, error));

    for (int16_t i = 0; i < n_fields; i++) {
      const int8_t is_null = ArrowArrayViewIsNull(array_view_->children[i], index);
      if (is_null) {
        constexpr int32_t field_size_bytes = -1;
        NANOARROW_RETURN_NOT_OK(WriteChecked<int32_t>(buffer, field_size_bytes, error));
      } else {
        children_[i]->Write(buffer, index, error);
      }
    }

    return NANOARROW_OK;
  } 

  ArrowErrorCode PostgresCopyBooleanFieldWriter::Write(ArrowBuffer* buffer, int64_t index, ArrowError* error)  {
    constexpr int32_t field_size_bytes = 1;
    NANOARROW_RETURN_NOT_OK(WriteChecked<int32_t>(buffer, field_size_bytes, error));
    const int8_t value =
        static_cast<int8_t>(ArrowArrayViewGetIntUnsafe(array_view_, index));
    NANOARROW_RETURN_NOT_OK(WriteChecked<int8_t>(buffer, value, error));

    return ADBC_STATUS_OK;
  }

template <typename T, T kOffset>
  ArrowErrorCode PostgresCopyNetworkEndianFieldWriter<T, kOffset>::Write(ArrowBuffer* buffer, int64_t index, ArrowError* error)  {
    constexpr int32_t field_size_bytes = sizeof(T);
    NANOARROW_RETURN_NOT_OK(WriteChecked<int32_t>(buffer, field_size_bytes, error));
    const T value =
        static_cast<T>(ArrowArrayViewGetIntUnsafe(array_view_, index)) - kOffset;
    NANOARROW_RETURN_NOT_OK(WriteChecked<T>(buffer, value, error));

    return ADBC_STATUS_OK;
  }

  ArrowErrorCode PostgresCopyFloatFieldWriter::Write(ArrowBuffer* buffer, int64_t index, ArrowError* error)  {
    constexpr int32_t field_size_bytes = sizeof(uint32_t);
    NANOARROW_RETURN_NOT_OK(WriteChecked<int32_t>(buffer, field_size_bytes, error));

    uint32_t value;
    float raw_value = ArrowArrayViewGetDoubleUnsafe(array_view_, index);
    std::memcpy(&value, &raw_value, sizeof(uint32_t));
    NANOARROW_RETURN_NOT_OK(WriteChecked<uint32_t>(buffer, value, error));

    return ADBC_STATUS_OK;
  }

  ArrowErrorCode PostgresCopyDoubleFieldWriter::Write(ArrowBuffer* buffer, int64_t index, ArrowError* error)  {
    constexpr int32_t field_size_bytes = sizeof(uint64_t);
    NANOARROW_RETURN_NOT_OK(WriteChecked<int32_t>(buffer, field_size_bytes, error));

    uint64_t value;
    double raw_value = ArrowArrayViewGetDoubleUnsafe(array_view_, index);
    std::memcpy(&value, &raw_value, sizeof(uint64_t));
    NANOARROW_RETURN_NOT_OK(WriteChecked<uint64_t>(buffer, value, error));

    return ADBC_STATUS_OK;
  }

  ArrowErrorCode PostgresCopyIntervalFieldWriter::Write(ArrowBuffer* buffer, int64_t index, ArrowError* error) {
    constexpr int32_t field_size_bytes = 16;
    NANOARROW_RETURN_NOT_OK(WriteChecked<int32_t>(buffer, field_size_bytes, error));

    struct ArrowInterval interval;
    ArrowIntervalInit(&interval, NANOARROW_TYPE_INTERVAL_MONTH_DAY_NANO);
    ArrowArrayViewGetIntervalUnsafe(array_view_, index, &interval);
    const int64_t ms = interval.ns / 1000;
    NANOARROW_RETURN_NOT_OK(WriteChecked<int64_t>(buffer, ms, error));
    NANOARROW_RETURN_NOT_OK(WriteChecked<int32_t>(buffer, interval.days, error));
    NANOARROW_RETURN_NOT_OK(WriteChecked<int32_t>(buffer, interval.months, error));

    return ADBC_STATUS_OK;
  }

template <enum ArrowTimeUnit TU>
  ArrowErrorCode PostgresCopyDurationFieldWriter<TU>::Write(ArrowBuffer* buffer, int64_t index, ArrowError* error) {
    constexpr int32_t field_size_bytes = 16;
    NANOARROW_RETURN_NOT_OK(WriteChecked<int32_t>(buffer, field_size_bytes, error));

    int64_t raw_value = ArrowArrayViewGetIntUnsafe(array_view_, index);
    int64_t value;

    bool overflow_safe = true;
    switch (TU) {
      case NANOARROW_TIME_UNIT_SECOND:
        if ((overflow_safe = raw_value <= kMaxSafeSecondsToMicros &&
                             raw_value >= kMinSafeSecondsToMicros)) {
          value = raw_value * 1000000;
        }
        break;
      case NANOARROW_TIME_UNIT_MILLI:
        if ((overflow_safe = raw_value <= kMaxSafeMillisToMicros &&
                             raw_value >= kMinSafeMillisToMicros)) {
          value = raw_value * 1000;
        }
        break;
      case NANOARROW_TIME_UNIT_MICRO:
        value = raw_value;
        break;
      case NANOARROW_TIME_UNIT_NANO:
        value = raw_value / 1000;
        break;
    }

    if (!overflow_safe) {
      ArrowErrorSet(
          error, "Row %" PRId64 " duration value %" PRId64 " with unit %d would overflow",
          index, raw_value, TU);
      return ADBC_STATUS_INVALID_ARGUMENT;
    }

    // 2000-01-01 00:00:00.000000 in microseconds
    constexpr uint32_t days = 0;
    constexpr uint32_t months = 0;
    NANOARROW_RETURN_NOT_OK(WriteChecked<int64_t>(buffer, value, error));
    NANOARROW_RETURN_NOT_OK(WriteChecked<int32_t>(buffer, days, error));
    NANOARROW_RETURN_NOT_OK(WriteChecked<int32_t>(buffer, months, error));

    return ADBC_STATUS_OK;
  }

  ArrowErrorCode PostgresCopyBinaryFieldWriter::Write(ArrowBuffer* buffer, int64_t index, ArrowError* error)  {
    struct ArrowBufferView buffer_view = ArrowArrayViewGetBytesUnsafe(array_view_, index);
    NANOARROW_RETURN_NOT_OK(WriteChecked<int32_t>(buffer, buffer_view.size_bytes, error));
    NANOARROW_RETURN_NOT_OK(
        ArrowBufferAppend(buffer, buffer_view.data.as_uint8, buffer_view.size_bytes));

    return ADBC_STATUS_OK;
  }

  ArrowErrorCode PostgresCopyBinaryDictFieldWriter::Write(ArrowBuffer* buffer, int64_t index, ArrowError* error)  {
    int64_t dict_index = ArrowArrayViewGetIntUnsafe(array_view_, index);
    if (ArrowArrayViewIsNull(array_view_->dictionary, dict_index)) {
      constexpr int32_t field_size_bytes = -1;
      NANOARROW_RETURN_NOT_OK(WriteChecked<int32_t>(buffer, field_size_bytes, error));
    } else {
      struct ArrowBufferView buffer_view =
          ArrowArrayViewGetBytesUnsafe(array_view_->dictionary, dict_index);
      NANOARROW_RETURN_NOT_OK(
          WriteChecked<int32_t>(buffer, buffer_view.size_bytes, error));
      NANOARROW_RETURN_NOT_OK(
          ArrowBufferAppend(buffer, buffer_view.data.as_uint8, buffer_view.size_bytes));
    }

    return ADBC_STATUS_OK;
  }

template <enum ArrowTimeUnit TU>
  ArrowErrorCode PostgresCopyTimestampFieldWriter<TU>::Write(ArrowBuffer* buffer, int64_t index, ArrowError* error)  {
    constexpr int32_t field_size_bytes = sizeof(int64_t);
    NANOARROW_RETURN_NOT_OK(WriteChecked<int32_t>(buffer, field_size_bytes, error));

    int64_t raw_value = ArrowArrayViewGetIntUnsafe(array_view_, index);
    int64_t value;

    bool overflow_safe = true;
    switch (TU) {
      case NANOARROW_TIME_UNIT_SECOND:
        if ((overflow_safe = raw_value <= kMaxSafeSecondsToMicros &&
                             raw_value >= kMinSafeSecondsToMicros)) {
          value = raw_value * 1000000;
        }
        break;
      case NANOARROW_TIME_UNIT_MILLI:
        if ((overflow_safe = raw_value <= kMaxSafeMillisToMicros &&
                             raw_value >= kMinSafeMillisToMicros)) {
          value = raw_value * 1000;
        }
        break;
      case NANOARROW_TIME_UNIT_MICRO:
        value = raw_value;
        break;
      case NANOARROW_TIME_UNIT_NANO:
        value = raw_value / 1000;
        break;
    }

    if (!overflow_safe) {
      ArrowErrorSet(error,
                    "[libpq] Row %" PRId64 " timestamp value %" PRId64
                    " with unit %d would overflow",
                    index, raw_value, TU);
      return ADBC_STATUS_INVALID_ARGUMENT;
    }

    if (value < std::numeric_limits<int64_t>::min() + kPostgresTimestampEpoch) {
      ArrowErrorSet(error,
                    "[libpq] Row %" PRId64 " timestamp value %" PRId64
                    " with unit %d would underflow",
                    index, raw_value, TU);
      return ADBC_STATUS_INVALID_ARGUMENT;
    }

    const int64_t scaled = value - kPostgresTimestampEpoch;
    NANOARROW_RETURN_NOT_OK(WriteChecked<int64_t>(buffer, scaled, error));

    return ADBC_STATUS_OK;
  }


 ArrowErrorCode MakeCopyFieldWriter(struct ArrowSchema* schema,
                                                 PostgresCopyFieldWriter** out,
                                                 ArrowError* error) {
  struct ArrowSchemaView schema_view;
  NANOARROW_RETURN_NOT_OK(ArrowSchemaViewInit(&schema_view, schema, error));

  switch (schema_view.type) {
    case NANOARROW_TYPE_BOOL:
      *out = new PostgresCopyBooleanFieldWriter();
      return NANOARROW_OK;
    case NANOARROW_TYPE_INT8:
    case NANOARROW_TYPE_INT16:
      *out = new PostgresCopyNetworkEndianFieldWriter<int16_t>();
      return NANOARROW_OK;
    case NANOARROW_TYPE_INT32:
      *out = new PostgresCopyNetworkEndianFieldWriter<int32_t>();
      return NANOARROW_OK;
    case NANOARROW_TYPE_INT64:
      *out = new PostgresCopyNetworkEndianFieldWriter<int64_t>();
      return NANOARROW_OK;
    case NANOARROW_TYPE_DATE32: {
      constexpr int32_t kPostgresDateEpoch = 10957;
      *out = new PostgresCopyNetworkEndianFieldWriter<int32_t, kPostgresDateEpoch>();
      return NANOARROW_OK;
    }
    case NANOARROW_TYPE_FLOAT:
      *out = new PostgresCopyFloatFieldWriter();
      return NANOARROW_OK;
    case NANOARROW_TYPE_DOUBLE:
      *out = new PostgresCopyDoubleFieldWriter();
      return NANOARROW_OK;
    case NANOARROW_TYPE_BINARY:
    case NANOARROW_TYPE_STRING:
    case NANOARROW_TYPE_LARGE_STRING:
      *out = new PostgresCopyBinaryFieldWriter();
      return NANOARROW_OK;
    case NANOARROW_TYPE_TIMESTAMP: {
      switch (schema_view.time_unit) {
        case NANOARROW_TIME_UNIT_NANO:
          *out = new PostgresCopyTimestampFieldWriter<NANOARROW_TIME_UNIT_NANO>();
          break;
        case NANOARROW_TIME_UNIT_MILLI:
          *out = new PostgresCopyTimestampFieldWriter<NANOARROW_TIME_UNIT_MILLI>();
          break;
        case NANOARROW_TIME_UNIT_MICRO:
          *out = new PostgresCopyTimestampFieldWriter<NANOARROW_TIME_UNIT_MICRO>();
          break;
        case NANOARROW_TIME_UNIT_SECOND:
          *out = new PostgresCopyTimestampFieldWriter<NANOARROW_TIME_UNIT_SECOND>();
          break;
      }
      return NANOARROW_OK;
    }
    case NANOARROW_TYPE_INTERVAL_MONTH_DAY_NANO:
      *out = new PostgresCopyIntervalFieldWriter();
      return NANOARROW_OK;
    case NANOARROW_TYPE_DURATION: {
      switch (schema_view.time_unit) {
        case NANOARROW_TIME_UNIT_SECOND:
          *out = new PostgresCopyDurationFieldWriter<NANOARROW_TIME_UNIT_SECOND>();
          break;
        case NANOARROW_TIME_UNIT_MILLI:
          *out = new PostgresCopyDurationFieldWriter<NANOARROW_TIME_UNIT_MILLI>();
          break;
        case NANOARROW_TIME_UNIT_MICRO:
          *out = new PostgresCopyDurationFieldWriter<NANOARROW_TIME_UNIT_MICRO>();

          break;
        case NANOARROW_TIME_UNIT_NANO:
          *out = new PostgresCopyDurationFieldWriter<NANOARROW_TIME_UNIT_NANO>();
          break;
      }
      return NANOARROW_OK;
    }
    case NANOARROW_TYPE_DICTIONARY: {
      struct ArrowSchemaView value_view;
      NANOARROW_RETURN_NOT_OK(
          ArrowSchemaViewInit(&value_view, schema->dictionary, error));
      switch (value_view.type) {
        case NANOARROW_TYPE_BINARY:
        case NANOARROW_TYPE_STRING:
        case NANOARROW_TYPE_LARGE_BINARY:
        case NANOARROW_TYPE_LARGE_STRING:
          *out = new PostgresCopyBinaryDictFieldWriter();
          return NANOARROW_OK;
        default:
          break;
      }
    }
    default:
      break;
  }

  ArrowErrorSet(error, "COPY Writer not implemented for type %d", schema_view.type);
  return EINVAL;
}

  ArrowErrorCode PostgresCopyStreamWriter::Init(struct ArrowSchema* schema) {
    schema_ = schema;
    NANOARROW_RETURN_NOT_OK(
        ArrowArrayViewInitFromSchema(&array_view_.value, schema, nullptr));
    root_writer_.Init(&array_view_.value);
    ArrowBufferInit(&buffer_.value);
    return NANOARROW_OK;
  }

  ArrowErrorCode PostgresCopyStreamWriter::SetArray(struct ArrowArray* array) {
    NANOARROW_RETURN_NOT_OK(ArrowArrayViewSetArray(&array_view_.value, array, nullptr));
    return NANOARROW_OK;
  }

  ArrowErrorCode PostgresCopyStreamWriter::WriteHeader(ArrowError* error) {
    // Commented as unused, kept for reference.
    // NANOARROW_RETURN_NOT_OK(ArrowBufferAppend(&buffer_.value, kPgCopyBinarySignature,
    //                                           sizeof(kPgCopyBinarySignature)));

    // const uint32_t flag_fields = 0;
    // NANOARROW_RETURN_NOT_OK(
    //     ArrowBufferAppend(&buffer_.value, &flag_fields, sizeof(flag_fields)));

    // const uint32_t extension_bytes = 0;
    // NANOARROW_RETURN_NOT_OK(
    //     ArrowBufferAppend(&buffer_.value, &extension_bytes, sizeof(extension_bytes)));

    return NANOARROW_OK;
  }

  ArrowErrorCode PostgresCopyStreamWriter::WriteRecord(ArrowError* error) {
    NANOARROW_RETURN_NOT_OK(root_writer_.Write(&buffer_.value, records_written_, error));
    records_written_++;
    return NANOARROW_OK;
  }

  ArrowErrorCode PostgresCopyStreamWriter::InitFieldWriters(ArrowError* error) {
    if (schema_->release == nullptr) {
      return EINVAL;
    }

    for (int64_t i = 0; i < schema_->n_children; i++) {
      PostgresCopyFieldWriter* child_writer = nullptr;
      NANOARROW_RETURN_NOT_OK(
          MakeCopyFieldWriter(schema_->children[i], &child_writer, error));
      root_writer_.AppendChild(std::unique_ptr<PostgresCopyFieldWriter>(child_writer));
    }

    return NANOARROW_OK;
  }

  const struct ArrowBuffer& PostgresCopyStreamWriter::WriteBuffer() const { return buffer_.value; }

  void PostgresCopyStreamWriter::Rewind() {
    records_written_ = 0;
    buffer_->size_bytes = 0;
  }

}  // namespace adbcpq
