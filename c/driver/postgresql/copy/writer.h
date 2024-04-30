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

#pragma once

#include <charconv>
#include <cinttypes>
#include <limits>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <nanoarrow/nanoarrow.hpp>

#include "../postgres_util.h"
#include "copy_common.h"

namespace adbcpq {

// The maximum value in seconds that can be converted into microseconds
// without overflow
constexpr int64_t kMaxSafeSecondsToMicros = 9223372036854L;

// The minimum value in seconds that can be converted into microseconds
// without overflow
constexpr int64_t kMinSafeSecondsToMicros = -9223372036854L;

// The maximum value in milliseconds that can be converted into microseconds
// without overflow
constexpr int64_t kMaxSafeMillisToMicros = 9223372036854775L;

// The minimum value in milliseconds that can be converted into microseconds
// without overflow
constexpr int64_t kMinSafeMillisToMicros = -9223372036854775L;

// 2000-01-01 00:00:00.000000 in microseconds
constexpr int64_t kPostgresTimestampEpoch = 946684800000000L;

// Write a value to a buffer without checking the buffer size. Advances
// the cursor of buffer and reduces it by sizeof(T)
template <typename T>
inline void WriteUnsafe(ArrowBuffer* buffer, T in) {
  const T value = SwapNetworkToHost(in);
  ArrowBufferAppendUnsafe(buffer, &value, sizeof(T));
}

template <>
inline void WriteUnsafe(ArrowBuffer* buffer, int8_t in) {
  ArrowBufferAppendUnsafe(buffer, &in, sizeof(int8_t));
}

template <>
inline void WriteUnsafe(ArrowBuffer* buffer, int16_t in) {
  WriteUnsafe<uint16_t>(buffer, in);
}

template <>
inline void WriteUnsafe(ArrowBuffer* buffer, int32_t in) {
  WriteUnsafe<uint32_t>(buffer, in);
}

template <>
inline void WriteUnsafe(ArrowBuffer* buffer, int64_t in) {
  WriteUnsafe<uint64_t>(buffer, in);
}

template <typename T>
ArrowErrorCode WriteChecked(ArrowBuffer* buffer, T in, ArrowError* error) {
  NANOARROW_RETURN_NOT_OK(ArrowBufferReserve(buffer, sizeof(T)));
  WriteUnsafe<T>(buffer, in);
  return NANOARROW_OK;
}

class PostgresCopyFieldWriter {
 public:
  virtual ~PostgresCopyFieldWriter() {}

  void Init(struct ArrowArrayView* array_view) { array_view_ = array_view; };

  virtual ArrowErrorCode Write(ArrowBuffer* buffer, int64_t index, ArrowError* error) {
    return ENOTSUP;
  }

 protected:
  struct ArrowArrayView* array_view_;
  std::vector<std::unique_ptr<PostgresCopyFieldWriter>> children_;
};

class PostgresCopyFieldTupleWriter : public PostgresCopyFieldWriter {
 public:
  void AppendChild(std::unique_ptr<PostgresCopyFieldWriter> child) {
    int64_t child_i = static_cast<int64_t>(children_.size());
    children_.push_back(std::move(child));
    children_[child_i]->Init(array_view_->children[child_i]);
  }

  ArrowErrorCode Write(ArrowBuffer* buffer, int64_t index, ArrowError* error) override {
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

 private:
  std::vector<std::unique_ptr<PostgresCopyFieldWriter>> children_;
};

class PostgresCopyBooleanFieldWriter : public PostgresCopyFieldWriter {
 public:
  ArrowErrorCode Write(ArrowBuffer* buffer, int64_t index, ArrowError* error) override {
    constexpr int32_t field_size_bytes = 1;
    NANOARROW_RETURN_NOT_OK(WriteChecked<int32_t>(buffer, field_size_bytes, error));
    const int8_t value =
        static_cast<int8_t>(ArrowArrayViewGetIntUnsafe(array_view_, index));
    NANOARROW_RETURN_NOT_OK(WriteChecked<int8_t>(buffer, value, error));

    return ADBC_STATUS_OK;
  }
};

template <typename T, T kOffset = 0>
class PostgresCopyNetworkEndianFieldWriter : public PostgresCopyFieldWriter {
 public:
  ArrowErrorCode Write(ArrowBuffer* buffer, int64_t index, ArrowError* error) override {
    constexpr int32_t field_size_bytes = sizeof(T);
    NANOARROW_RETURN_NOT_OK(WriteChecked<int32_t>(buffer, field_size_bytes, error));
    const T value =
        static_cast<T>(ArrowArrayViewGetIntUnsafe(array_view_, index)) - kOffset;
    NANOARROW_RETURN_NOT_OK(WriteChecked<T>(buffer, value, error));

    return ADBC_STATUS_OK;
  }
};

class PostgresCopyFloatFieldWriter : public PostgresCopyFieldWriter {
 public:
  ArrowErrorCode Write(ArrowBuffer* buffer, int64_t index, ArrowError* error) override {
    constexpr int32_t field_size_bytes = sizeof(uint32_t);
    NANOARROW_RETURN_NOT_OK(WriteChecked<int32_t>(buffer, field_size_bytes, error));

    uint32_t value;
    float raw_value = ArrowArrayViewGetDoubleUnsafe(array_view_, index);
    std::memcpy(&value, &raw_value, sizeof(uint32_t));
    NANOARROW_RETURN_NOT_OK(WriteChecked<uint32_t>(buffer, value, error));

    return ADBC_STATUS_OK;
  }
};

class PostgresCopyDoubleFieldWriter : public PostgresCopyFieldWriter {
 public:
  ArrowErrorCode Write(ArrowBuffer* buffer, int64_t index, ArrowError* error) override {
    constexpr int32_t field_size_bytes = sizeof(uint64_t);
    NANOARROW_RETURN_NOT_OK(WriteChecked<int32_t>(buffer, field_size_bytes, error));

    uint64_t value;
    double raw_value = ArrowArrayViewGetDoubleUnsafe(array_view_, index);
    std::memcpy(&value, &raw_value, sizeof(uint64_t));
    NANOARROW_RETURN_NOT_OK(WriteChecked<uint64_t>(buffer, value, error));

    return ADBC_STATUS_OK;
  }
};

class PostgresCopyIntervalFieldWriter : public PostgresCopyFieldWriter {
 public:
  ArrowErrorCode Write(ArrowBuffer* buffer, int64_t index, ArrowError* error) override {
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
};

// Inspiration for this taken from get_str_from_var in the pg source
// src/backend/utils/adt/numeric.c
template <enum ArrowType T>
class PostgresCopyNumericFieldWriter : public PostgresCopyFieldWriter {
 public:
  PostgresCopyNumericFieldWriter<T>(int32_t precision, int32_t scale)
      : precision_{precision}, scale_{scale} {}

  ArrowErrorCode Write(ArrowBuffer* buffer, int64_t index, ArrowError* error) override {
    struct ArrowDecimal decimal;
    ArrowDecimalInit(&decimal, bitwidth_, precision_, scale_);
    ArrowArrayViewGetDecimalUnsafe(array_view_, index, &decimal);

    const int16_t sign = ArrowDecimalSign(&decimal) > 0 ? kNumericPos : kNumericNeg;

    // Number of decimal digits per Postgres digit
    constexpr int kDecDigits = 4;
    std::vector<int16_t> pg_digits;
    int16_t weight = -(scale_ / kDecDigits);
    int16_t dscale = scale_;
    bool seen_decimal = scale_ == 0;
    bool truncating_trailing_zeros = true;

    char decimal_string[max_decimal_digits_ + 1];
    int digits_remaining = DecimalToString<bitwidth_>(&decimal, decimal_string);
    do {
      const int start_pos =
          digits_remaining < kDecDigits ? 0 : digits_remaining - kDecDigits;
      const size_t len = digits_remaining < 4 ? digits_remaining : kDecDigits;
      const std::string_view substr{decimal_string + start_pos, len};
      int16_t val{};
      std::from_chars(substr.data(), substr.data() + substr.size(), val);

      if (val == 0) {
        if (!seen_decimal && truncating_trailing_zeros) {
          dscale -= kDecDigits;
        }
      } else {
        pg_digits.insert(pg_digits.begin(), val);
        if (!seen_decimal && truncating_trailing_zeros) {
          if (val % 1000 == 0) {
            dscale -= 3;
          } else if (val % 100 == 0) {
            dscale -= 2;
          } else if (val % 10 == 0) {
            dscale -= 1;
          }
        }
        truncating_trailing_zeros = false;
      }
      digits_remaining -= kDecDigits;
      if (digits_remaining <= 0) {
        break;
      }
      weight++;

      if (start_pos <= static_cast<int>(std::strlen(decimal_string)) - scale_) {
        seen_decimal = true;
      }
    } while (true);

    int16_t ndigits = pg_digits.size();
    int32_t field_size_bytes = sizeof(ndigits) + sizeof(weight) + sizeof(sign) +
                               sizeof(dscale) + ndigits * sizeof(int16_t);

    NANOARROW_RETURN_NOT_OK(WriteChecked<int32_t>(buffer, field_size_bytes, error));
    NANOARROW_RETURN_NOT_OK(WriteChecked<int16_t>(buffer, ndigits, error));
    NANOARROW_RETURN_NOT_OK(WriteChecked<int16_t>(buffer, weight, error));
    NANOARROW_RETURN_NOT_OK(WriteChecked<int16_t>(buffer, sign, error));
    NANOARROW_RETURN_NOT_OK(WriteChecked<int16_t>(buffer, dscale, error));

    const size_t pg_digit_bytes = sizeof(int16_t) * pg_digits.size();
    NANOARROW_RETURN_NOT_OK(ArrowBufferReserve(buffer, pg_digit_bytes));
    for (auto pg_digit : pg_digits) {
      WriteUnsafe<int16_t>(buffer, pg_digit);
    }

    return ADBC_STATUS_OK;
  }

 private:
  // returns the length of the string
  template <int32_t DEC_WIDTH>
  int DecimalToString(struct ArrowDecimal* decimal, char* out) {
    constexpr size_t nwords = (DEC_WIDTH == 128) ? 2 : 4;
    uint8_t tmp[DEC_WIDTH / 8];
    ArrowDecimalGetBytes(decimal, tmp);
    uint64_t buf[DEC_WIDTH / 64];
    std::memcpy(buf, tmp, sizeof(buf));
    const int16_t sign = ArrowDecimalSign(decimal) > 0 ? kNumericPos : kNumericNeg;
    const bool is_negative = sign == kNumericNeg ? true : false;
    if (is_negative) {
      buf[0] = ~buf[0] + 1;
      for (size_t i = 1; i < nwords; i++) {
        buf[i] = ~buf[i];
      }
    }

    // Basic approach adopted from https://stackoverflow.com/a/8023862/621736
    char s[max_decimal_digits_ + 1];
    std::memset(s, '0', sizeof(s) - 1);
    s[sizeof(s) - 1] = '\0';

    for (size_t i = 0; i < DEC_WIDTH; i++) {
      int carry;

      carry = (buf[nwords - 1] >= 0x7FFFFFFFFFFFFFFF);
      for (size_t j = nwords - 1; j > 0; j--) {
        buf[j] =
            ((buf[j] << 1) & 0xFFFFFFFFFFFFFFFF) + (buf[j - 1] >= 0x7FFFFFFFFFFFFFFF);
      }
      buf[0] = ((buf[0] << 1) & 0xFFFFFFFFFFFFFFFF);

      for (int j = sizeof(s) - 2; j >= 0; j--) {
        s[j] += s[j] - '0' + carry;
        carry = (s[j] > '9');
        if (carry) {
          s[j] -= 10;
        }
      }
    }

    char* p = s;
    while ((p[0] == '0') && (p < &s[sizeof(s) - 2])) {
      p++;
    }

    const size_t ndigits = sizeof(s) - 1 - (p - s);
    std::memcpy(out, p, ndigits);
    out[ndigits] = '\0';

    return ndigits;
  }

  static constexpr uint16_t kNumericPos = 0x0000;
  static constexpr uint16_t kNumericNeg = 0x4000;
  static constexpr int32_t bitwidth_ = (T == NANOARROW_TYPE_DECIMAL128) ? 128 : 256;
  static constexpr size_t max_decimal_digits_ =
      (T == NANOARROW_TYPE_DECIMAL128) ? 39 : 78;
  const int32_t precision_;
  const int32_t scale_;
};

template <enum ArrowTimeUnit TU>
class PostgresCopyDurationFieldWriter : public PostgresCopyFieldWriter {
 public:
  ArrowErrorCode Write(ArrowBuffer* buffer, int64_t index, ArrowError* error) override {
    constexpr int32_t field_size_bytes = 16;
    NANOARROW_RETURN_NOT_OK(WriteChecked<int32_t>(buffer, field_size_bytes, error));

    int64_t raw_value = ArrowArrayViewGetIntUnsafe(array_view_, index);
    int64_t value = 0;

    bool overflow_safe = true;
    switch (TU) {
      case NANOARROW_TIME_UNIT_SECOND:
        overflow_safe =
            raw_value <= kMaxSafeSecondsToMicros && raw_value >= kMinSafeSecondsToMicros;
        if (overflow_safe) {
          value = raw_value * 1000000;
        }
        break;
      case NANOARROW_TIME_UNIT_MILLI:
        overflow_safe =
            raw_value <= kMaxSafeMillisToMicros && raw_value >= kMinSafeMillisToMicros;
        if (overflow_safe) {
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
};

class PostgresCopyBinaryFieldWriter : public PostgresCopyFieldWriter {
 public:
  ArrowErrorCode Write(ArrowBuffer* buffer, int64_t index, ArrowError* error) override {
    struct ArrowBufferView buffer_view = ArrowArrayViewGetBytesUnsafe(array_view_, index);
    NANOARROW_RETURN_NOT_OK(WriteChecked<int32_t>(buffer, buffer_view.size_bytes, error));
    NANOARROW_RETURN_NOT_OK(
        ArrowBufferAppend(buffer, buffer_view.data.as_uint8, buffer_view.size_bytes));

    return ADBC_STATUS_OK;
  }
};

class PostgresCopyBinaryDictFieldWriter : public PostgresCopyFieldWriter {
 public:
  ArrowErrorCode Write(ArrowBuffer* buffer, int64_t index, ArrowError* error) override {
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
};

template <enum ArrowTimeUnit TU>
class PostgresCopyTimestampFieldWriter : public PostgresCopyFieldWriter {
 public:
  ArrowErrorCode Write(ArrowBuffer* buffer, int64_t index, ArrowError* error) override {
    constexpr int32_t field_size_bytes = sizeof(int64_t);
    NANOARROW_RETURN_NOT_OK(WriteChecked<int32_t>(buffer, field_size_bytes, error));

    int64_t raw_value = ArrowArrayViewGetIntUnsafe(array_view_, index);
    int64_t value = 0;

    bool overflow_safe = true;
    switch (TU) {
      case NANOARROW_TIME_UNIT_SECOND:
        overflow_safe =
            raw_value <= kMaxSafeSecondsToMicros && raw_value >= kMinSafeSecondsToMicros;
        if (overflow_safe) {
          value = raw_value * 1000000;
        }
        break;
      case NANOARROW_TIME_UNIT_MILLI:
        overflow_safe =
            raw_value <= kMaxSafeMillisToMicros && raw_value >= kMinSafeMillisToMicros;
        if (overflow_safe) {
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

    if (value < (std::numeric_limits<int64_t>::min)() + kPostgresTimestampEpoch) {
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
};

static inline ArrowErrorCode MakeCopyFieldWriter(
    struct ArrowSchema* schema, std::unique_ptr<PostgresCopyFieldWriter>* out,
    ArrowError* error) {
  struct ArrowSchemaView schema_view;
  NANOARROW_RETURN_NOT_OK(ArrowSchemaViewInit(&schema_view, schema, error));

  switch (schema_view.type) {
    case NANOARROW_TYPE_BOOL:
      *out = std::make_unique<PostgresCopyBooleanFieldWriter>();
      return NANOARROW_OK;
    case NANOARROW_TYPE_INT8:
    case NANOARROW_TYPE_INT16:
      *out = std::make_unique<PostgresCopyNetworkEndianFieldWriter<int16_t>>();
      return NANOARROW_OK;
    case NANOARROW_TYPE_INT32:
      *out = std::make_unique<PostgresCopyNetworkEndianFieldWriter<int32_t>>();
      return NANOARROW_OK;
    case NANOARROW_TYPE_INT64:
      *out = std::make_unique<PostgresCopyNetworkEndianFieldWriter<int64_t>>();
      return NANOARROW_OK;
    case NANOARROW_TYPE_DATE32: {
      constexpr int32_t kPostgresDateEpoch = 10957;
      *out = std::make_unique<
          PostgresCopyNetworkEndianFieldWriter<int32_t, kPostgresDateEpoch>>();
      return NANOARROW_OK;
    }
    case NANOARROW_TYPE_FLOAT:
      *out = std::make_unique<PostgresCopyFloatFieldWriter>();
      return NANOARROW_OK;
    case NANOARROW_TYPE_DOUBLE:
      *out = std::make_unique<PostgresCopyDoubleFieldWriter>();
      return NANOARROW_OK;
    case NANOARROW_TYPE_DECIMAL128: {
      const auto precision = schema_view.decimal_precision;
      const auto scale = schema_view.decimal_scale;
      *out = std::make_unique<PostgresCopyNumericFieldWriter<NANOARROW_TYPE_DECIMAL128>>(
          precision, scale);
      return NANOARROW_OK;
    }
    case NANOARROW_TYPE_DECIMAL256: {
      const auto precision = schema_view.decimal_precision;
      const auto scale = schema_view.decimal_scale;
      *out = std::make_unique<PostgresCopyNumericFieldWriter<NANOARROW_TYPE_DECIMAL256>>(
          precision, scale);
      return NANOARROW_OK;
    }
    case NANOARROW_TYPE_BINARY:
    case NANOARROW_TYPE_STRING:
    case NANOARROW_TYPE_LARGE_STRING:
      *out = std::make_unique<PostgresCopyBinaryFieldWriter>();
      return NANOARROW_OK;
    case NANOARROW_TYPE_TIMESTAMP: {
      switch (schema_view.time_unit) {
        case NANOARROW_TIME_UNIT_NANO:
          *out = std::make_unique<
              PostgresCopyTimestampFieldWriter<NANOARROW_TIME_UNIT_NANO>>();
          break;
        case NANOARROW_TIME_UNIT_MILLI:
          *out = std::make_unique<
              PostgresCopyTimestampFieldWriter<NANOARROW_TIME_UNIT_MILLI>>();
          break;
        case NANOARROW_TIME_UNIT_MICRO:
          *out = std::make_unique<
              PostgresCopyTimestampFieldWriter<NANOARROW_TIME_UNIT_MICRO>>();
          break;
        case NANOARROW_TIME_UNIT_SECOND:
          *out = std::make_unique<
              PostgresCopyTimestampFieldWriter<NANOARROW_TIME_UNIT_SECOND>>();
          break;
      }
      return NANOARROW_OK;
    }
    case NANOARROW_TYPE_INTERVAL_MONTH_DAY_NANO:
      *out = std::make_unique<PostgresCopyIntervalFieldWriter>();
      return NANOARROW_OK;
    case NANOARROW_TYPE_DURATION: {
      switch (schema_view.time_unit) {
        case NANOARROW_TIME_UNIT_SECOND:
          *out = std::make_unique<
              PostgresCopyDurationFieldWriter<NANOARROW_TIME_UNIT_SECOND>>();
          break;
        case NANOARROW_TIME_UNIT_MILLI:
          *out = std::make_unique<
              PostgresCopyDurationFieldWriter<NANOARROW_TIME_UNIT_MILLI>>();
          break;
        case NANOARROW_TIME_UNIT_MICRO:
          *out = std::make_unique<
              PostgresCopyDurationFieldWriter<NANOARROW_TIME_UNIT_MICRO>>();

          break;
        case NANOARROW_TIME_UNIT_NANO:
          *out = std::make_unique<
              PostgresCopyDurationFieldWriter<NANOARROW_TIME_UNIT_NANO>>();
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
          *out = std::make_unique<PostgresCopyBinaryDictFieldWriter>();
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

class PostgresCopyStreamWriter {
 public:
  ArrowErrorCode Init(struct ArrowSchema* schema) {
    schema_ = schema;
    NANOARROW_RETURN_NOT_OK(
        ArrowArrayViewInitFromSchema(&array_view_.value, schema, nullptr));
    root_writer_.Init(&array_view_.value);
    ArrowBufferInit(&buffer_.value);
    return NANOARROW_OK;
  }

  ArrowErrorCode SetArray(struct ArrowArray* array) {
    NANOARROW_RETURN_NOT_OK(ArrowArrayViewSetArray(&array_view_.value, array, nullptr));
    return NANOARROW_OK;
  }

  ArrowErrorCode WriteHeader(ArrowError* error) {
    NANOARROW_RETURN_NOT_OK(ArrowBufferAppend(&buffer_.value, kPgCopyBinarySignature,
                                              sizeof(kPgCopyBinarySignature)));

    const uint32_t flag_fields = 0;
    NANOARROW_RETURN_NOT_OK(
        ArrowBufferAppend(&buffer_.value, &flag_fields, sizeof(flag_fields)));

    const uint32_t extension_bytes = 0;
    NANOARROW_RETURN_NOT_OK(
        ArrowBufferAppend(&buffer_.value, &extension_bytes, sizeof(extension_bytes)));

    return NANOARROW_OK;
  }

  ArrowErrorCode WriteRecord(ArrowError* error) {
    NANOARROW_RETURN_NOT_OK(root_writer_.Write(&buffer_.value, records_written_, error));
    records_written_++;
    return NANOARROW_OK;
  }

  ArrowErrorCode InitFieldWriters(ArrowError* error) {
    if (schema_->release == nullptr) {
      return EINVAL;
    }

    for (int64_t i = 0; i < schema_->n_children; i++) {
      std::unique_ptr<PostgresCopyFieldWriter> child_writer;
      NANOARROW_RETURN_NOT_OK(
          MakeCopyFieldWriter(schema_->children[i], &child_writer, error));
      root_writer_.AppendChild(std::move(child_writer));
    }

    return NANOARROW_OK;
  }

  const struct ArrowBuffer& WriteBuffer() const { return buffer_.value; }

  void Rewind() {
    records_written_ = 0;
    buffer_->size_bytes = 0;
  }

 private:
  PostgresCopyFieldTupleWriter root_writer_;
  struct ArrowSchema* schema_;
  Handle<struct ArrowArrayView> array_view_;
  Handle<struct ArrowBuffer> buffer_;
  int64_t records_written_ = 0;
};

}  // namespace adbcpq
