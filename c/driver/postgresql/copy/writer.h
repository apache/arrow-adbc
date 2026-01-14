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

#include <algorithm>
#include <charconv>
#include <cinttypes>
#include <limits>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <nanoarrow/nanoarrow.hpp>

#include "../connection.h"
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

  template <class T, typename... Params>
  static std::unique_ptr<T> Create(struct ArrowArrayView* array_view, Params&&... args) {
    auto writer = std::make_unique<T>(std::forward<Params>(args)...);
    writer->Init(array_view);
    return writer;
  }

  virtual ArrowErrorCode Write(ArrowBuffer* buffer, int64_t index, ArrowError* error) {
    return ENOTSUP;
  }

 protected:
  virtual void Init(struct ArrowArrayView* array_view) { array_view_ = array_view; };

  struct ArrowArrayView* array_view_;
  std::vector<std::unique_ptr<PostgresCopyFieldWriter>> children_;
};

class PostgresCopyFieldTupleWriter : public PostgresCopyFieldWriter {
 public:
  void AppendChild(std::unique_ptr<PostgresCopyFieldWriter> child) {
    children_.push_back(std::move(child));
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
  PostgresCopyNumericFieldWriter(int32_t precision, int32_t scale)
      : precision_{precision}, scale_{scale} {}

  // PostgreSQL NUMERIC Binary Format:
  // ===================================
  // PostgreSQL stores NUMERIC values in a variable-length binary format:
  //   - ndigits (int16): Number of base-10000 digits stored
  //   - weight (int16): Position of the first digit group relative to decimal point
  //                     (weight can be negative for small fractional numbers)
  //   - sign (int16): kNumericPos (0x0000) or kNumericNeg (0x4000)
  //   - dscale (int16): Number of decimal digits after the decimal point (display scale)
  //   - digits[]: Array of int16 values, each 0-9999 (base-10000 representation)
  //
  // Value calculation: sum(digits[i] * 10000^(weight - i)) * 10^(-dscale)
  //
  // Example 1: 12300 (from Arrow Decimal value=123, scale=-2)
  //   - Logical representation: "12300"
  //   - Grouped in base-10000: [1][2300]
  //   - ndigits=2, weight=1, sign=0x0000, dscale=0, digits=[1, 2300]
  //   - Calculation: 1*10000^1 + 2300*10000^0 = 10000 + 2300 = 12300
  //
  // Example 2: 123.45 (from Arrow Decimal value=12345, scale=2)
  //   - Logical representation: "123.45"
  //   - Integer part "123", fractional part "45"
  //   - Grouped in base-10000: [123][4500] (fractional part right-padded)
  //   - ndigits=2, weight=0, sign=0x0000, dscale=2, digits=[123, 4500]
  //   - Calculation: 123*10000^0 + 4500*10000^(-1) = 123 + 0.45 = 123.45
  //
  // Example 3: 0.00123 (from Arrow Decimal value=123, scale=5)
  //   - Logical representation: "0.00123"
  //   - Integer part "0", fractional part "00123"
  //   - Grouped in base-10000: [123] (leading zeros skipped via negative weight)
  //   - ndigits=1, weight=-1, sign=0x0000, dscale=5, digits=[123]
  //   - Calculation: 123*10000^(-1) * 10^0 = 0.0123, but dscale=5 means display as
  //   0.00123

  ArrowErrorCode Write(ArrowBuffer* buffer, int64_t index, ArrowError* error) override {
    struct ArrowDecimal decimal;
    ArrowDecimalInit(&decimal, bitwidth_, precision_, scale_);
    ArrowArrayViewGetDecimalUnsafe(array_view_, index, &decimal);

    const int16_t sign = ArrowDecimalSign(&decimal) > 0 ? kNumericPos : kNumericNeg;

    // Convert decimal to string and split into integer/fractional parts
    // Example transformation for Arrow Decimal(value=12345, scale=2) representing 123.45:
    //   Input: decimal.value = 12345, scale_ = 2
    //   After DecimalToString: raw_decimal_string = "12345", original_digits = 5
    //   After SplitDecimalParts: parts.integer_part = "123"
    //                           parts.fractional_part = "45"
    //                           parts.effective_scale = 2
    char raw_decimal_string[max_decimal_digits_ + 1];
    int original_digits = DecimalToString<bitwidth_>(&decimal, raw_decimal_string);
    DecimalParts parts = SplitDecimalParts(raw_decimal_string, original_digits, scale_);

    // Group into PostgreSQL base-10000 representation
    //   After GroupIntegerDigits: int_digits = [123], weight = 0
    //     (groups "123" right-to-left: "123" → 123, only 1 group so weight = 0)
    auto [int_digits, weight] = GroupIntegerDigits(parts.integer_part);

    //   After GroupFractionalDigits: frac_digits = [4500], final_weight = 0
    //     (groups "45" left-to-right with right-padding: "45" → "4500" → 4500)
    auto [frac_digits, final_weight] =
        GroupFractionalDigits(parts.fractional_part, weight, !parts.integer_part.empty());

    // Combine digit arrays
    //   After combining: all_digits = [123, 4500]
    std::vector<int16_t> all_digits = int_digits;
    all_digits.insert(all_digits.end(), frac_digits.begin(), frac_digits.end());

    // Calculate display scale by counting trailing zeros in the DECIMAL STRING
    //   For our example: frac_part="45" has 0 trailing zeros, effective_scale=2
    //   So dscale = 2 - 0 = 2 (2 fractional digits to display)
    int trailing_zeros = 0;
    for (int j = parts.fractional_part.length() - 1;
         j >= 0 && parts.fractional_part[j] == '0'; j--) {
      trailing_zeros++;
    }
    int16_t dscale = (std::max)<int16_t>(0, parts.effective_scale - trailing_zeros);

    // Optimize: remove trailing zero digit groups from fractional part
    int n_int_digit_groups = int_digits.size();
    while (static_cast<int>(all_digits.size()) > n_int_digit_groups &&
           all_digits.back() == 0) {
      all_digits.pop_back();
    }

    // Handle zero special case
    if (all_digits.empty()) {
      final_weight = 0;
      dscale = 0;
    } else if (static_cast<int>(all_digits.size()) <= n_int_digit_groups) {
      // All fractional digits were removed
      dscale = 0;
    }

    if (dscale < 0) dscale = 0;

    // Write PostgreSQL NUMERIC binary format to buffer
    //   Final values for our example: ndigits = 2
    //                                final_weight = 0
    //                                sign = 0x0000
    //                                dscale = 2
    //                                digits = [123, 4500]
    //   Binary output represents: 123 * 10000^0 + 4500 * 10000^(-1) = 123 + 0.45 = 123.45
    int16_t ndigits = all_digits.size();
    int32_t field_size_bytes = sizeof(ndigits) + sizeof(final_weight) + sizeof(sign) +
                               sizeof(dscale) + ndigits * sizeof(int16_t);

    NANOARROW_RETURN_NOT_OK(WriteChecked<int32_t>(buffer, field_size_bytes, error));
    NANOARROW_RETURN_NOT_OK(WriteChecked<int16_t>(buffer, ndigits, error));
    NANOARROW_RETURN_NOT_OK(WriteChecked<int16_t>(buffer, final_weight, error));
    NANOARROW_RETURN_NOT_OK(WriteChecked<int16_t>(buffer, sign, error));
    NANOARROW_RETURN_NOT_OK(WriteChecked<int16_t>(buffer, dscale, error));

    const size_t pg_digit_bytes = sizeof(int16_t) * all_digits.size();
    NANOARROW_RETURN_NOT_OK(ArrowBufferReserve(buffer, pg_digit_bytes));
    for (auto pg_digit : all_digits) {
      WriteUnsafe<int16_t>(buffer, pg_digit);
    }

    return ADBC_STATUS_OK;
  }

 private:
  // Helper struct for organizing data flow between functions
  struct DecimalParts {
    std::string integer_part;     // e.g., "12300" or "123"
    std::string fractional_part;  // e.g., "45" or "00123"
    int effective_scale;          // Scale after handling negative values
  };

  // Helper function implementations for decimal-to-PostgreSQL NUMERIC conversion

  // Convert decimal to string (absolute value, no sign)
  // Returns the length of the string
  template <int32_t DEC_WIDTH>
  int DecimalToString(struct ArrowDecimal* decimal, char* out) const {
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

      carry = (buf[nwords - 1] > 0x7FFFFFFFFFFFFFFF);
      for (size_t j = nwords - 1; j > 0; j--) {
        buf[j] = ((buf[j] << 1) & 0xFFFFFFFFFFFFFFFF) + (buf[j - 1] > 0x7FFFFFFFFFFFFFFF);
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

  DecimalParts SplitDecimalParts(const char* decimal_digits, int digit_count,
                                 int scale) const {
    // Virtual zeros represent the logical zeros appended for negative scale
    // Example: value=123, scale=-2 → "123" with 2 virtual zeros = "12300"
    const int virtual_zeros = (scale < 0) ? -scale : 0;
    const int effective_scale = (scale < 0) ? 0 : scale;
    const int total_logical_digits = digit_count + virtual_zeros;

    // Calculate split point
    const int n_int_digits = total_logical_digits > effective_scale
                                 ? total_logical_digits - effective_scale
                                 : 0;
    const int n_frac_digits = total_logical_digits - n_int_digits;

    DecimalParts parts;
    parts.effective_scale = effective_scale;

    // Extract integer part
    if (n_int_digits > 0) {
      if (n_int_digits <= digit_count) {
        // Integer part is within the original digits
        parts.integer_part.assign(decimal_digits, n_int_digits);
      } else {
        // Integer part includes all original digits + virtual zeros
        parts.integer_part.assign(decimal_digits, digit_count);
        parts.integer_part.append(virtual_zeros, '0');
      }
    }

    // Extract fractional part (only exists if scale > 0)
    if (n_int_digits == 0 && total_logical_digits < effective_scale) {
      // Small fractional: 0.00123 needs leading zeros
      parts.fractional_part.assign(effective_scale - total_logical_digits, '0');
      parts.fractional_part.append(decimal_digits, digit_count);
    } else if (n_frac_digits > 0 && n_int_digits < digit_count) {
      // Fractional part from remaining digits (virtual zeros don't appear in fractional
      // part)
      parts.fractional_part.assign(decimal_digits + n_int_digits,
                                   digit_count - n_int_digits);
    }

    return parts;
  }

  std::pair<std::vector<int16_t>, int16_t> GroupIntegerDigits(
      const std::string& int_part) const {
    constexpr int kDecDigits = 4;
    std::vector<int16_t> digits;

    if (int_part.empty()) {
      return {digits, -1};  // weight = -1 for pure fractional numbers
    }

    // Calculate weight: ceil(length / 4) - 1
    int16_t weight = (int_part.length() + kDecDigits - 1) / kDecDigits - 1;

    // Group right-to-left in chunks of 4
    int i = int_part.length();
    while (i > 0) {
      int chunk_size = (std::min)(i, kDecDigits);
      std::string_view chunk =
          std::string_view(int_part).substr(i - chunk_size, chunk_size);

      int16_t val{};
      std::from_chars(chunk.data(), chunk.data() + chunk.size(), val);

      // Skip trailing zeros
      if (val != 0 || !digits.empty()) {
        digits.insert(digits.begin(), val);
      }
      i -= chunk_size;
    }

    return {digits, weight};
  }

  std::pair<std::vector<int16_t>, int16_t> GroupFractionalDigits(
      const std::string& frac_part, int16_t initial_weight, bool has_integer_part) const {
    constexpr int kDecDigits = 4;
    std::vector<int16_t> digits;
    int16_t weight = initial_weight;

    if (frac_part.empty()) {
      return {digits, weight};
    }

    bool skip_leading_zeros = !has_integer_part;

    // Group left-to-right in chunks of 4, right-padding last chunk
    for (size_t i = 0; i < frac_part.length(); i += kDecDigits) {
      int chunk_size = (std::min)(kDecDigits, static_cast<int>(frac_part.length() - i));
      std::string chunk_str = frac_part.substr(i, chunk_size);

      // Right-pad to 4 digits (e.g., "45" → "4500")
      chunk_str.resize(kDecDigits, '0');

      int16_t val{};
      std::from_chars(chunk_str.data(), chunk_str.data() + chunk_str.size(), val);

      if (skip_leading_zeros && val == 0) {
        // Skip leading zero groups in fractional part (e.g., 0.0012 → skip "0012")
        weight--;
      } else {
        digits.push_back(val);
        skip_leading_zeros = false;
      }
    }

    return {digits, weight};
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

template <bool IsFixedSize>
class PostgresCopyListFieldWriter : public PostgresCopyFieldWriter {
 public:
  explicit PostgresCopyListFieldWriter(uint32_t child_oid,
                                       std::unique_ptr<PostgresCopyFieldWriter> child)
      : child_oid_{child_oid}, child_{std::move(child)} {}

  ArrowErrorCode Write(ArrowBuffer* buffer, int64_t index, ArrowError* error) override {
    if (index >= array_view_->length) {
      return ENODATA;
    }

    constexpr int32_t ndim = 1;
    constexpr int32_t has_null_flags = 0;

    // TODO: the LARGE_LIST should use 64 bit indexes
    int32_t start, end;
    if constexpr (IsFixedSize) {
      start = index * array_view_->layout.child_size_elements;
      end = start + array_view_->layout.child_size_elements;
    } else {
      start = ArrowArrayViewListChildOffset(array_view_, index);
      end = ArrowArrayViewListChildOffset(array_view_, index + 1);
    }

    const int32_t dim = end - start;
    constexpr int32_t lb = 1;

    // for children of a fixed size T we could avoid the use of a temporary buffer
    /// and theoretically just write
    //
    // const int32_t field_size_bytes =
    //    sizeof(ndim) + sizeof(has_null_flags) + sizeof(child_oid_) + sizeof(dim) * ndim
    //    + sizeof(lb) * ndim
    //    + sizeof(int32_t) * dim + T * dim;
    //
    // directly to our buffer
    nanoarrow::UniqueBuffer tmp;
    ArrowBufferInit(tmp.get());
    for (auto i = start; i < end; ++i) {
      NANOARROW_RETURN_NOT_OK(child_->Write(tmp.get(), i, error));
    }
    const int32_t field_size_bytes = sizeof(ndim) + sizeof(has_null_flags) +
                                     sizeof(child_oid_) + sizeof(dim) * ndim +
                                     sizeof(lb) * ndim + tmp->size_bytes;

    NANOARROW_RETURN_NOT_OK(WriteChecked<int32_t>(buffer, field_size_bytes, error));
    NANOARROW_RETURN_NOT_OK(WriteChecked<int32_t>(buffer, ndim, error));
    NANOARROW_RETURN_NOT_OK(WriteChecked<int32_t>(buffer, has_null_flags, error));
    NANOARROW_RETURN_NOT_OK(WriteChecked<uint32_t>(buffer, child_oid_, error));
    for (int32_t i = 0; i < ndim; ++i) {
      NANOARROW_RETURN_NOT_OK(WriteChecked<int32_t>(buffer, dim, error));
      NANOARROW_RETURN_NOT_OK(WriteChecked<int32_t>(buffer, lb, error));
    }

    NANOARROW_RETURN_NOT_OK(ArrowBufferAppend(buffer, tmp->data, tmp->size_bytes));

    return NANOARROW_OK;
  }

 private:
  const uint32_t child_oid_;
  std::unique_ptr<PostgresCopyFieldWriter> child_;
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
    struct ArrowSchema* schema, struct ArrowArrayView* array_view,
    const PostgresTypeResolver& type_resolver,
    std::unique_ptr<PostgresCopyFieldWriter>* out, ArrowError* error) {
  struct ArrowSchemaView schema_view;
  NANOARROW_RETURN_NOT_OK(ArrowSchemaViewInit(&schema_view, schema, error));

  switch (schema_view.type) {
    case NANOARROW_TYPE_BOOL:
      using T = PostgresCopyBooleanFieldWriter;
      *out = T::Create<T>(array_view);
      return NANOARROW_OK;
    case NANOARROW_TYPE_INT8:
    case NANOARROW_TYPE_INT16:
    case NANOARROW_TYPE_UINT8: {
      using T = PostgresCopyNetworkEndianFieldWriter<int16_t>;
      *out = T::Create<T>(array_view);
      return NANOARROW_OK;
    }
    case NANOARROW_TYPE_INT32:
    case NANOARROW_TYPE_UINT16: {
      using T = PostgresCopyNetworkEndianFieldWriter<int32_t>;
      *out = T::Create<T>(array_view);
      return NANOARROW_OK;
    }
    case NANOARROW_TYPE_UINT32:
    case NANOARROW_TYPE_INT64:
    case NANOARROW_TYPE_UINT64: {
      using T = PostgresCopyNetworkEndianFieldWriter<int64_t>;
      *out = T::Create<T>(array_view);
      return NANOARROW_OK;
    }
    case NANOARROW_TYPE_DATE32: {
      constexpr int32_t kPostgresDateEpoch = 10957;
      using T = PostgresCopyNetworkEndianFieldWriter<int32_t, kPostgresDateEpoch>;
      *out = T::Create<T>(array_view);
      return NANOARROW_OK;
    }
    case NANOARROW_TYPE_TIME64: {
      switch (schema_view.time_unit) {
        case NANOARROW_TIME_UNIT_MICRO:
          using T = PostgresCopyNetworkEndianFieldWriter<int64_t>;
          *out = T::Create<T>(array_view);
          return NANOARROW_OK;
        default:
          return ADBC_STATUS_NOT_IMPLEMENTED;
      }
    }
    case NANOARROW_TYPE_HALF_FLOAT:
    case NANOARROW_TYPE_FLOAT: {
      using T = PostgresCopyFloatFieldWriter;
      *out = T::Create<T>(array_view);
      return NANOARROW_OK;
    }
    case NANOARROW_TYPE_DOUBLE: {
      using T = PostgresCopyDoubleFieldWriter;
      *out = T::Create<T>(array_view);
      return NANOARROW_OK;
    }
    case NANOARROW_TYPE_DECIMAL128: {
      using T = PostgresCopyNumericFieldWriter<NANOARROW_TYPE_DECIMAL128>;
      const auto precision = schema_view.decimal_precision;
      const auto scale = schema_view.decimal_scale;
      *out = T::Create<T>(array_view, precision, scale);
      return NANOARROW_OK;
    }
    case NANOARROW_TYPE_DECIMAL256: {
      using T = PostgresCopyNumericFieldWriter<NANOARROW_TYPE_DECIMAL256>;
      const auto precision = schema_view.decimal_precision;
      const auto scale = schema_view.decimal_scale;
      *out = T::Create<T>(array_view, precision, scale);
      return NANOARROW_OK;
    }
    case NANOARROW_TYPE_BINARY:
    case NANOARROW_TYPE_LARGE_BINARY:
    case NANOARROW_TYPE_FIXED_SIZE_BINARY:
    case NANOARROW_TYPE_BINARY_VIEW:
    case NANOARROW_TYPE_STRING:
    case NANOARROW_TYPE_LARGE_STRING:
    case NANOARROW_TYPE_STRING_VIEW: {
      using T = PostgresCopyBinaryFieldWriter;
      *out = T::Create<T>(array_view);
      return NANOARROW_OK;
    }
    case NANOARROW_TYPE_TIMESTAMP: {
      switch (schema_view.time_unit) {
        case NANOARROW_TIME_UNIT_NANO: {
          using T = PostgresCopyTimestampFieldWriter<NANOARROW_TIME_UNIT_NANO>;
          *out = T::Create<T>(array_view);
          break;
        }
        case NANOARROW_TIME_UNIT_MILLI: {
          using T = PostgresCopyTimestampFieldWriter<NANOARROW_TIME_UNIT_MILLI>;
          *out = T::Create<T>(array_view);
          break;
        }
        case NANOARROW_TIME_UNIT_MICRO: {
          using T = PostgresCopyTimestampFieldWriter<NANOARROW_TIME_UNIT_MICRO>;
          *out = T::Create<T>(array_view);
          break;
        }
        case NANOARROW_TIME_UNIT_SECOND: {
          using T = PostgresCopyTimestampFieldWriter<NANOARROW_TIME_UNIT_SECOND>;
          *out = T::Create<T>(array_view);
          break;
        }
      }
      return NANOARROW_OK;
    }
    case NANOARROW_TYPE_INTERVAL_MONTH_DAY_NANO: {
      using T = PostgresCopyIntervalFieldWriter;
      *out = T::Create<T>(array_view);
      return NANOARROW_OK;
    }
    case NANOARROW_TYPE_DURATION: {
      switch (schema_view.time_unit) {
        case NANOARROW_TIME_UNIT_SECOND: {
          using T = PostgresCopyDurationFieldWriter<NANOARROW_TIME_UNIT_SECOND>;
          *out = T::Create<T>(array_view);
          break;
        }
        case NANOARROW_TIME_UNIT_MILLI: {
          using T = PostgresCopyDurationFieldWriter<NANOARROW_TIME_UNIT_MILLI>;
          *out = T::Create<T>(array_view);
          break;
        }
        case NANOARROW_TIME_UNIT_MICRO: {
          using T = PostgresCopyDurationFieldWriter<NANOARROW_TIME_UNIT_MICRO>;
          *out = T::Create<T>(array_view);
          break;
        }
        case NANOARROW_TIME_UNIT_NANO: {
          using T = PostgresCopyDurationFieldWriter<NANOARROW_TIME_UNIT_NANO>;
          *out = T::Create<T>(array_view);
          break;
        }
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
        case NANOARROW_TYPE_LARGE_STRING: {
          using T = PostgresCopyBinaryDictFieldWriter;
          *out = T::Create<T>(array_view);
          return NANOARROW_OK;
        }
        default:
          break;
      }
      break;
    }
    case NANOARROW_TYPE_LIST:
    case NANOARROW_TYPE_LARGE_LIST:
    case NANOARROW_TYPE_FIXED_SIZE_LIST: {
      // For now our implementation only supports primitive children types
      // See PostgresCopyListFieldWriter::Write for limitations
      struct ArrowSchemaView child_schema_view;
      NANOARROW_RETURN_NOT_OK(
          ArrowSchemaViewInit(&child_schema_view, schema->children[0], error));
      PostgresType child_type;
      NANOARROW_RETURN_NOT_OK(PostgresType::FromSchema(type_resolver, schema->children[0],
                                                       &child_type, error));

      std::unique_ptr<PostgresCopyFieldWriter> child_writer;
      NANOARROW_RETURN_NOT_OK(MakeCopyFieldWriter(schema->children[0],
                                                  array_view->children[0], type_resolver,
                                                  &child_writer, error));

      if (schema_view.type == NANOARROW_TYPE_FIXED_SIZE_LIST) {
        using T = PostgresCopyListFieldWriter<true>;
        *out = T::Create<T>(array_view, child_type.oid(), std::move(child_writer));
      } else {
        using T = PostgresCopyListFieldWriter<false>;
        *out = T::Create<T>(array_view, child_type.oid(), std::move(child_writer));
      }
      return NANOARROW_OK;
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
    root_writer_ = PostgresCopyFieldTupleWriter::Create<PostgresCopyFieldTupleWriter>(
        &array_view_.value);
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
    NANOARROW_RETURN_NOT_OK(root_writer_->Write(&buffer_.value, records_written_, error));
    records_written_++;
    return NANOARROW_OK;
  }

  ArrowErrorCode InitFieldWriters(const PostgresTypeResolver& type_resolver,
                                  ArrowError* error) {
    if (schema_->release == nullptr) {
      return EINVAL;
    }

    for (int64_t i = 0; i < schema_->n_children; i++) {
      std::unique_ptr<PostgresCopyFieldWriter> child_writer;
      NANOARROW_RETURN_NOT_OK(MakeCopyFieldWriter(schema_->children[i],
                                                  array_view_->children[i], type_resolver,
                                                  &child_writer, error));
      root_writer_->AppendChild(std::move(child_writer));
    }

    return NANOARROW_OK;
  }

  const struct ArrowBuffer& WriteBuffer() const { return buffer_.value; }

  void Rewind() {
    records_written_ = 0;
    buffer_->size_bytes = 0;
  }

 private:
  std::unique_ptr<PostgresCopyFieldTupleWriter> root_writer_;
  struct ArrowSchema* schema_;
  Handle<struct ArrowArrayView> array_view_;
  Handle<struct ArrowBuffer> buffer_;
  int64_t records_written_ = 0;
};

}  // namespace adbcpq
