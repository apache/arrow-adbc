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

#include <cassert>
#include <cstring>
#include <memory>
#include <sstream>
#include <string>
#include <utility>
#include <variant>
#include <vector>

#if defined(ADBC_FRAMEWORK_USE_FMT)
#include <fmt/core.h>
#endif

#include <arrow-adbc/adbc.h>

/// \file status.h

namespace adbc::driver {

/// \brief A wrapper around AdbcStatusCode + AdbcError.
///
/// Drivers should prefer to use Status, and convert at the boundaries with
/// ToAdbc.
class Status {
 public:
  /// \brief Construct an OK status.
  Status() : impl_(nullptr) {}

  /// \brief Construct a non-OK status with a message.
  explicit Status(AdbcStatusCode code, std::string message)
      : Status(code, std::move(message), {}) {}

  /// \brief Construct a non-OK status with a message.
  explicit Status(AdbcStatusCode code, const char* message)
      : Status(code, std::string(message), {}) {}

  /// \brief Construct a non-OK status with a message and details.
  explicit Status(AdbcStatusCode code, std::string message,
                  std::vector<std::pair<std::string, std::string>> details)
      : impl_(std::make_unique<Impl>(code, std::move(message), std::move(details))) {
    assert(code != ADBC_STATUS_OK);
  }

  /// \brief Check if this is an error or not.
  bool ok() const { return impl_ == nullptr; }

  const char* message() const {
    if (!impl_) {
      return "";
    } else {
      return impl_->message.c_str();
    }
  }

  AdbcStatusCode code() const {
    if (ok()) {
      return ADBC_STATUS_OK;
    } else {
      return impl_->code;
    }
  }

  /// \brief Add another error detail.
  void AddDetail(std::string key, std::string value) {
    assert(impl_ != nullptr);
    impl_->details.push_back({std::move(key), std::move(value)});
  }

  /// \brief Set the sqlstate of this status
  void SetSqlState(std::string sqlstate) {
    assert(impl_ != nullptr);
    std::memset(impl_->sql_state, 0, sizeof(impl_->sql_state));
    for (size_t i = 0; i < sqlstate.size(); i++) {
      if (i >= sizeof(impl_->sql_state)) {
        break;
      }

      impl_->sql_state[i] = sqlstate[i];
    }
  }

  /// \brief Export this status to an AdbcError.
  AdbcStatusCode ToAdbc(AdbcError* adbc_error) const {
    if (impl_ == nullptr) return ADBC_STATUS_OK;
    if (adbc_error == nullptr) return impl_->code;

    if (adbc_error->release) {
      adbc_error->release(adbc_error);
    }

    if (adbc_error->vendor_code == ADBC_ERROR_VENDOR_CODE_PRIVATE_DATA) {
      auto error_owned_by_adbc_error =
          new Status(impl_->code, std::move(impl_->message), std::move(impl_->details));
      adbc_error->message =
          const_cast<char*>(error_owned_by_adbc_error->impl_->message.c_str());
      adbc_error->private_data = error_owned_by_adbc_error;
    } else {
      adbc_error->message = new char[impl_->message.size() + 1];
      if (adbc_error->message != nullptr) {
        std::memcpy(adbc_error->message, impl_->message.c_str(),
                    impl_->message.size() + 1);
      }
    }

    std::memcpy(adbc_error->sqlstate, impl_->sql_state, sizeof(impl_->sql_state));
    adbc_error->release = &CRelease;
    return impl_->code;
  }

  static Status FromAdbc(AdbcStatusCode code, AdbcError& error) {
    if (code == ADBC_STATUS_OK) {
      if (error.release) {
        error.release(&error);
      }
      return Status();
    }
    auto status = Status(code, error.message ? error.message : "(unknown error)");
    if (error.release) {
      error.release(&error);
    }
    return status;
  }

  // Helpers to create statuses with known codes
  static Status Ok() { return Status(); }

#define STATUS_CTOR(NAME, CODE)                  \
  template <typename... Args>                    \
  static Status NAME(Args&&... args) {           \
    std::stringstream ss;                        \
    ([&] { ss << args; }(), ...);                \
    return Status(ADBC_STATUS_##CODE, ss.str()); \
  }

  STATUS_CTOR(Internal, INTERNAL)
  STATUS_CTOR(InvalidArgument, INVALID_ARGUMENT)
  STATUS_CTOR(InvalidState, INVALID_STATE)
  STATUS_CTOR(IO, IO)
  STATUS_CTOR(NotFound, NOT_FOUND)
  STATUS_CTOR(NotImplemented, NOT_IMPLEMENTED)
  STATUS_CTOR(Unknown, UNKNOWN)

#undef STATUS_CTOR

 private:
  /// \brief Private Status implementation details
  struct Impl {
    // invariant: code is never OK
    AdbcStatusCode code;
    std::string message;
    std::vector<std::pair<std::string, std::string>> details;
    char sql_state[5];

    explicit Impl(AdbcStatusCode code, std::string message,
                  std::vector<std::pair<std::string, std::string>> details)
        : code(code), message(std::move(message)), details(std::move(details)) {
      std::memset(sql_state, 0, sizeof(sql_state));
    }
  };
  // invariant: code is OK iff impl_ is nullptr
  std::unique_ptr<Impl> impl_;

  // Let the Driver use these to expose C callables wrapping option setters/getters
  template <typename DatabaseT, typename ConnectionT, typename StatementT>
  friend class Driver;

  // Allow access to these for drivers transitioning to the framework
 public:
  int CDetailCount() const { return impl_ ? static_cast<int>(impl_->details.size()) : 0; }

  AdbcErrorDetail CDetail(int index) const {
    if (!impl_ || index < 0 || static_cast<size_t>(index) >= impl_->details.size()) {
      return {nullptr, nullptr, 0};
    }
    const auto& detail = impl_->details[index];
    return {detail.first.c_str(), reinterpret_cast<const uint8_t*>(detail.second.data()),
            detail.second.size()};
  }

 private:
  static void CRelease(AdbcError* error) {
    if (error->vendor_code == ADBC_ERROR_VENDOR_CODE_PRIVATE_DATA) {
      auto* error_obj = reinterpret_cast<Status*>(error->private_data);
      delete error_obj;
      std::memset(error, 0, ADBC_ERROR_1_1_0_SIZE);
    } else {
      delete[] error->message;
      std::memset(error, 0, ADBC_ERROR_1_0_0_SIZE);
    }
  }
};

/// \brief A wrapper around a value, or an error.
///
/// We could probably do better by using a library like std::expected, but
/// this will suffice for now.  There doesn't seem to be a reasonably
/// maintained std::expected backport.
template <typename T>
class Result {
 public:
  /// \brief Implicit constructor to allow returning a status in functions.
  Result(Status s)  // NOLINT(runtime/explicit)
      : value_(std::move(s)) {
    assert(!std::get<Status>(value_).ok());
  }
  /// \brief Implicit constructor to allow returning a value in functions.
  template <typename U,
            // Allow things that can construct T, not just T itself, but
            // disallow T from being Status so which constructor to use is not
            // ambiguous.
            typename E = typename std::enable_if<
                std::is_constructible<T, U>::value && std::is_convertible<U, T>::value &&
                !std::is_same<typename std::remove_reference<
                                  typename std::remove_cv<U>::type>::type,
                              Status>::value>::type>
  Result(U&& t) : value_(std::forward<U>(t)) {}  // NOLINT(runtime/explicit)

  /// \brief Check if this has a value or not.
  bool has_value() const { return !std::holds_alternative<Status>(value_); }

  /// \brief Get the status (if present).
  const Status& status() const& {
    assert(std::holds_alternative<Status>(value_));
    return std::get<Status>(value_);
  }

  /// \brief Move the status (if present).
  Status&& status() && {
    assert(std::holds_alternative<Status>(value_));
    return std::move(std::get<Status>(value_));
  }

  /// \brief Get the value (if present).
  T& value() {
    assert(!std::holds_alternative<Status>(value_));
    return std::get<T>(value_);
  }

 private:
  std::variant<Status, T> value_;
};

#define RAISE_RESULT_IMPL(NAME, ERROR, LHS, RHS) \
  auto&& NAME = (RHS);                           \
  if (!(NAME).has_value()) {                     \
    return (NAME).status().ToAdbc(ERROR);        \
  }                                              \
  LHS = std::move((NAME).value());

#define RAISE_STATUS_IMPL(NAME, ERROR, RHS) \
  auto&& NAME = (RHS);                      \
  if (!(NAME).ok()) {                       \
    return (NAME).ToAdbc(ERROR);            \
  }

#define UNWRAP_RESULT_IMPL(name, lhs, rhs) \
  auto&& name = (rhs);                     \
  if (!(name).has_value()) {               \
    return std::move(name).status();       \
  }                                        \
  lhs = std::move((name).value());

#define UNWRAP_STATUS_IMPL(name, rhs) \
  auto&& name = (rhs);                \
  if (!(name).ok()) {                 \
    return std::move(name);           \
  }

#define DRIVER_CONCAT(x, y) x##y
#define UNWRAP_RESULT_NAME(x, y) DRIVER_CONCAT(x, y)

/// \brief A helper to unwrap a Result in functions returning AdbcStatusCode.
#define RAISE_RESULT(ERROR, LHS, RHS) \
  RAISE_RESULT_IMPL(UNWRAP_RESULT_NAME(driver_raise_result, __COUNTER__), ERROR, LHS, RHS)
/// \brief A helper to unwrap a Status in functions returning AdbcStatusCode.
#define RAISE_STATUS(ERROR, RHS) \
  RAISE_STATUS_IMPL(UNWRAP_RESULT_NAME(driver_raise_status, __COUNTER__), ERROR, RHS)
/// \brief A helper to unwrap a Result in functions returning Result/Status.
#define UNWRAP_RESULT(lhs, rhs) \
  UNWRAP_RESULT_IMPL(UNWRAP_RESULT_NAME(driver_unwrap_result, __COUNTER__), lhs, rhs)
/// \brief A helper to unwrap a Status in functions returning Result/Status.
#define UNWRAP_STATUS(rhs) \
  UNWRAP_STATUS_IMPL(UNWRAP_RESULT_NAME(driver_unwrap_status, __COUNTER__), rhs)

}  // namespace adbc::driver

namespace adbc::driver::status {

inline driver::Status Ok() { return driver::Status(); }

#define STATUS_CTOR(NAME, CODE)                  \
  template <typename... Args>                    \
  static Status NAME(Args&&... args) {           \
    std::stringstream ss;                        \
    ([&] { ss << args; }(), ...);                \
    return Status(ADBC_STATUS_##CODE, ss.str()); \
  }

// TODO: unit tests for internal utilities
STATUS_CTOR(Internal, INTERNAL)
STATUS_CTOR(InvalidArgument, INVALID_ARGUMENT)
STATUS_CTOR(InvalidState, INVALID_STATE)
STATUS_CTOR(IO, IO)
STATUS_CTOR(NotFound, NOT_FOUND)
STATUS_CTOR(NotImplemented, NOT_IMPLEMENTED)
STATUS_CTOR(Unknown, UNKNOWN)

#undef STATUS_CTOR

}  // namespace adbc::driver::status

#if defined(ADBC_FRAMEWORK_USE_FMT)
namespace adbc::driver::status::fmt {

#define STATUS_CTOR(NAME, CODE)                                                     \
  template <typename... Args>                                                       \
  static Status NAME(std::string_view format_string, Args&&... args) {              \
    auto message = ::fmt::vformat(format_string, ::fmt::make_format_args(args...)); \
    return Status(ADBC_STATUS_##CODE, std::move(message));                          \
  }

// TODO: unit tests for internal utilities
STATUS_CTOR(Internal, INTERNAL)
STATUS_CTOR(InvalidArgument, INVALID_ARGUMENT)
STATUS_CTOR(InvalidState, INVALID_STATE)
STATUS_CTOR(IO, IO)
STATUS_CTOR(NotFound, NOT_FOUND)
STATUS_CTOR(NotImplemented, NOT_IMPLEMENTED)
STATUS_CTOR(Unknown, UNKNOWN)

#undef STATUS_CTOR

}  // namespace adbc::driver::status::fmt
#endif

#define UNWRAP_ERRNO_IMPL(NAME, CODE, RHS)                                             \
  auto&& NAME = (RHS);                                                                 \
  if (NAME != 0) {                                                                     \
    return adbc::driver::status::CODE("Call failed: ", #RHS, " = (errno ", NAME, ") ", \
                                      std::strerror(NAME));                            \
  }

#define UNWRAP_ERRNO(CODE, RHS) \
  UNWRAP_ERRNO_IMPL(UNWRAP_RESULT_NAME(driver_errno, __COUNTER__), CODE, RHS)

#define UNWRAP_NANOARROW_IMPL(NAME, ERROR, CODE, RHS)                                    \
  auto&& NAME = (RHS);                                                                   \
  if (NAME != 0) {                                                                       \
    return adbc::driver::status::CODE("nanoarrow call failed: ", #RHS, " = (", NAME,     \
                                      ") ", std::strerror(NAME), ". ", (ERROR).message); \
  }

#define UNWRAP_NANOARROW(ERROR, CODE, RHS)                                             \
  UNWRAP_NANOARROW_IMPL(UNWRAP_RESULT_NAME(driver_errno_na, __COUNTER__), ERROR, CODE, \
                        RHS)
