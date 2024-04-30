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

#include "driver/framework/base_driver.h"

namespace adbc::driver {
Result<bool> Option::AsBool() const {
  return std::visit(
      [&](auto&& value) -> Result<bool> {
        using T = std::decay_t<decltype(value)>;
        if constexpr (std::is_same_v<T, std::string>) {
          if (value == ADBC_OPTION_VALUE_ENABLED) {
            return true;
          } else if (value == ADBC_OPTION_VALUE_DISABLED) {
            return false;
          }
        }
        return status::InvalidArgument("Invalid boolean value {}", *this);
      },
      value_);
}

Result<int64_t> Option::AsInt() const {
  return std::visit(
      [&](auto&& value) -> Result<int64_t> {
        using T = std::decay_t<decltype(value)>;
        if constexpr (std::is_same_v<T, int64_t>) {
          return value;
        } else if constexpr (std::is_same_v<T, std::string>) {
          int64_t parsed = 0;
          auto begin = value.data();
          auto end = value.data() + value.size();
          auto result = std::from_chars(begin, end, parsed);
          if (result.ec != std::errc()) {
            return status::InvalidArgument("Invalid integer value '{}': not an integer",
                                           value);
          } else if (result.ptr != end) {
            return status::InvalidArgument("Invalid integer value '{}': trailing data",
                                           value);
          }
          return parsed;
        }
        return status::InvalidArgument("Invalid integer value {}", *this);
      },
      value_);
}

Result<std::string_view> Option::AsString() const {
  return std::visit(
      [&](auto&& value) -> Result<std::string_view> {
        using T = std::decay_t<decltype(value)>;
        if constexpr (std::is_same_v<T, std::string>) {
          return value;
        }
        return status::InvalidArgument("Invalid string value {}", *this);
      },
      value_);
}

AdbcStatusCode Option::CGet(char* out, size_t* length, AdbcError* error) const {
  if (!out || !length) {
    return status::InvalidArgument("Must provide both out and length to GetOption")
        .ToAdbc(error);
  }
  return std::visit(
      [&](auto&& value) -> AdbcStatusCode {
        using T = std::decay_t<decltype(value)>;
        if constexpr (std::is_same_v<T, std::string>) {
          size_t value_size_with_terminator = value.size() + 1;
          if (*length >= value_size_with_terminator) {
            std::memcpy(out, value.data(), value.size());
            out[value.size()] = 0;
          }
          *length = value_size_with_terminator;
          return ADBC_STATUS_OK;
        } else if constexpr (std::is_same_v<T, Unset>) {
          return status::NotFound("Unknown option").ToAdbc(error);
        } else {
          return status::NotFound("Option value is not a string").ToAdbc(error);
        }
      },
      value_);
}

AdbcStatusCode Option::CGet(uint8_t* out, size_t* length, AdbcError* error) const {
  if (!out || !length) {
    return status::InvalidArgument("Must provide both out and length to GetOption")
        .ToAdbc(error);
  }
  return std::visit(
      [&](auto&& value) -> AdbcStatusCode {
        using T = std::decay_t<decltype(value)>;
        if constexpr (std::is_same_v<T, std::string> ||
                      std::is_same_v<T, std::vector<uint8_t>>) {
          if (*length >= value.size()) {
            std::memcpy(out, value.data(), value.size());
          }
          *length = value.size();
          return ADBC_STATUS_OK;
        } else if constexpr (std::is_same_v<T, Unset>) {
          return status::NotFound("Unknown option").ToAdbc(error);
        } else {
          return status::NotFound("Option value is not a bytestring").ToAdbc(error);
        }
      },
      value_);
}

AdbcStatusCode Option::CGet(int64_t* out, AdbcError* error) const {
  if (!out) {
    return status::InvalidArgument("Must provide out to GetOption").ToAdbc(error);
  }
  return std::visit(
      [&](auto&& value) -> AdbcStatusCode {
        using T = std::decay_t<decltype(value)>;
        if constexpr (std::is_same_v<T, int64_t>) {
          *out = value;
          return ADBC_STATUS_OK;
        } else if constexpr (std::is_same_v<T, Unset>) {
          return status::NotFound("Unknown option").ToAdbc(error);
        } else {
          return status::NotFound("Option value is not an integer").ToAdbc(error);
        }
      },
      value_);
}

AdbcStatusCode Option::CGet(double* out, AdbcError* error) const {
  if (!out) {
    return status::InvalidArgument("Must provide out to GetOption").ToAdbc(error);
  }
  return std::visit(
      [&](auto&& value) -> AdbcStatusCode {
        using T = std::decay_t<decltype(value)>;
        if constexpr (std::is_same_v<T, double> || std::is_same_v<T, int64_t>) {
          *out = static_cast<double>(value);
          return ADBC_STATUS_OK;
        } else if constexpr (std::is_same_v<T, Unset>) {
          return status::NotFound("Unknown option").ToAdbc(error);
        } else {
          return status::NotFound("Option value is not a double").ToAdbc(error);
        }
      },
      value_);
}
}  // namespace adbc::driver
