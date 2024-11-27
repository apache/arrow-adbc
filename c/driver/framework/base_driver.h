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
#include <cstring>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>

#include <arrow-adbc/adbc.h>

#include "driver/framework/status.h"

/// \file base.h ADBC Driver Framework
///
/// A base implementation of an ADBC driver that allows easier driver
/// development by overriding functions.  Databases, connections, and
/// statements can be defined by subclassing the [CRTP][crtp] base classes.
///
/// Generally, base classes provide a set of functions that correspond to the
/// ADBC functions.  These should not be directly overridden, as they provide
/// the core logic and argument checking/error handling.  Instead, override
/// the -Impl functions that are also exposed by base classes.
///
/// [crtp]: https://en.wikipedia.org/wiki/Curiously_recurring_template_pattern
namespace adbc::driver {

/// \brief The state of a database/connection/statement.
enum class LifecycleState {
  /// \brief New has been called but not Init.
  kUninitialized,
  /// \brief Init has been called.
  kInitialized,
};

/// \brief A typed option value wrapper. It currently does not attempt
/// conversion (i.e., getting a double option as a string).
class Option {
 public:
  /// \brief The option is unset.
  struct Unset {};
  /// \brief The possible values of an option.
  using Value = std::variant<Unset, std::string, std::vector<uint8_t>, int64_t, double>;

  Option() : value_(Unset{}) {}
  /// \brief Construct an option from a C string.
  /// NULL strings are treated as unset.
  explicit Option(const char* value)
      : value_(value ? Value(std::string(value)) : Value{Unset{}}) {}
  explicit Option(std::string value) : value_(std::move(value)) {}
  explicit Option(std::vector<uint8_t> value) : value_(std::move(value)) {}
  explicit Option(double value) : value_(value) {}
  explicit Option(int64_t value) : value_(value) {}

  const Value& value() const& { return value_; }
  Value& value() && { return value_; }

  /// \brief Check whether this option is set.
  bool has_value() const { return !std::holds_alternative<Unset>(value_); }

  /// \brief Try to parse a string value as a boolean.
  Result<bool> AsBool() const {
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
          return status::InvalidArgument("Invalid boolean value ", this->Format());
        },
        value_);
  }

  /// \brief Try to parse a string or integer value as an integer.
  Result<int64_t> AsInt() const {
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
              return status::InvalidArgument("Invalid integer value '", value,
                                             "': not an integer", value);
            } else if (result.ptr != end) {
              return status::InvalidArgument("Invalid integer value '", value,
                                             "': trailing data", value);
            }
            return parsed;
          } else {
            return status::InvalidArgument("Invalid integer value ", this->Format());
          }
        },
        value_);
  }

  /// \brief Get the value if it is a string.
  Result<std::string_view> AsString() const {
    return std::visit(
        [&](auto&& value) -> Result<std::string_view> {
          using T = std::decay_t<decltype(value)>;
          if constexpr (std::is_same_v<T, std::string>) {
            return value;
          } else {
            return status::InvalidArgument("Invalid string value ", this->Format());
          }
        },
        value_);
  }

  /// \brief Provide a human-readable summary of the value
  std::string Format() const {
    return std::visit(
        [&](auto&& value) -> std::string {
          using T = std::decay_t<decltype(value)>;
          if constexpr (std::is_same_v<T, adbc::driver::Option::Unset>) {
            return "(NULL)";
          } else if constexpr (std::is_same_v<T, std::string>) {
            return std::string("'") + value + "'";
          } else if constexpr (std::is_same_v<T, std::vector<uint8_t>>) {
            return std::string("(") + std::to_string(value.size()) + " bytes)";
          } else {
            return std::to_string(value);
          }
        },
        value_);
  }

 private:
  Value value_;

  // Methods used by trampolines to export option values in C below
  friend class ObjectBase;
  AdbcStatusCode CGet(char* out, size_t* length, AdbcError* error) const {
    {
      if (!length || (!out && *length > 0)) {
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
  }
  AdbcStatusCode CGet(uint8_t* out, size_t* length, AdbcError* error) const {
    if (!length || (!out && *length > 0)) {
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
  AdbcStatusCode CGet(int64_t* out, AdbcError* error) const {
    {
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
  }
  AdbcStatusCode CGet(double* out, AdbcError* error) const {
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
};

/// \brief Base class for private_data of AdbcDatabase, AdbcConnection, and
///   AdbcStatement.
///
/// This class handles option setting and getting.
class ObjectBase {
 public:
  ObjectBase() = default;
  virtual ~ObjectBase() = default;

  // Called After zero or more SetOption() calls. The parent is the
  // private_data of the AdbcDatabase, or AdbcConnection when initializing a
  // subclass of ConnectionObjectBase, and StatementObjectBase (respectively),
  // or otherwise nullptr.  For example, if you have defined
  // Driver<MyDatabase, MyConnection, MyStatement>, you can
  // reinterpret_cast<MyDatabase>(parent) in MyConnection::Init().

  /// \brief Initialize the object.
  ///
  /// Called after 0 or more SetOption calls.  Generally, you won't need to
  /// override this directly.  Instead, use the typed InitImpl provided by
  /// Database/Connection/Statement.
  ///
  /// \param[in] parent A pointer to the AdbcDatabase or AdbcConnection
  ///   implementation as appropriate, or nullptr.
  virtual AdbcStatusCode Init(void* parent, AdbcError* error) {
    lifecycle_state_ = LifecycleState::kInitialized;
    return ADBC_STATUS_OK;
  }

  /// \brief Finalize the object.
  ///
  /// This can be used to return an error if the object is not in a valid
  /// state (e.g. prevent closing a connection with open statements) or to
  /// clean up resources when resource cleanup could fail.  Infallible
  /// resource cleanup (e.g. releasing memory) should generally be handled in
  /// the destructor.
  ///
  /// Generally, you won't need to override this directly. Instead, use the
  /// typed ReleaseImpl provided by Database/Connection/Statement.
  virtual AdbcStatusCode Release(AdbcError* error) { return ADBC_STATUS_OK; }

  /// \brief Get an option value.
  virtual Result<Option> GetOption(std::string_view key) {
    Option option(nullptr);
    return option;
  }

  /// \brief Set an option value.
  virtual AdbcStatusCode SetOption(std::string_view key, Option value, AdbcError* error) {
    return ADBC_STATUS_NOT_IMPLEMENTED;
  }

 protected:
  LifecycleState lifecycle_state_;

 private:
  // Let the Driver use these to expose C callables wrapping option setters/getters
  template <typename DatabaseT, typename ConnectionT, typename StatementT>
  friend class Driver;

  template <typename T>
  AdbcStatusCode CSetOption(const char* key, T value, AdbcError* error) {
    Option option(value);
    return SetOption(key, std::move(option), error);
  }

  AdbcStatusCode CSetOptionBytes(const char* key, const uint8_t* value, size_t length,
                                 AdbcError* error) {
    std::vector<uint8_t> cppvalue(value, value + length);
    Option option(std::move(cppvalue));
    return SetOption(key, std::move(option), error);
  }

  template <typename T>
  AdbcStatusCode CGetOptionStringLike(const char* key, T* value, size_t* length,
                                      AdbcError* error) {
    RAISE_RESULT(error, auto option, GetOption(key));
    return option.CGet(value, length, error);
  }

  template <typename T>
  AdbcStatusCode CGetOptionNumeric(const char* key, T* value, AdbcError* error) {
    RAISE_RESULT(error, auto option, GetOption(key));
    return option.CGet(value, error);
  }
};

/// Helper for below: given the ADBC type, pick the right driver type.
template <typename DatabaseT, typename ConnectionT, typename StatementT, typename T>
struct ResolveObjectTImpl {};

template <typename DatabaseT, typename ConnectionT, typename StatementT>
struct ResolveObjectTImpl<DatabaseT, ConnectionT, StatementT, struct AdbcDatabase> {
  using type = DatabaseT;
};
template <typename DatabaseT, typename ConnectionT, typename StatementT>
struct ResolveObjectTImpl<DatabaseT, ConnectionT, StatementT, struct AdbcConnection> {
  using type = ConnectionT;
};
template <typename DatabaseT, typename ConnectionT, typename StatementT>
struct ResolveObjectTImpl<DatabaseT, ConnectionT, StatementT, struct AdbcStatement> {
  using type = StatementT;
};

/// Helper for below: given the ADBC type, pick the right driver type.
template <typename DatabaseT, typename ConnectionT, typename StatementT, typename T>
using ResolveObjectT =
    typename ResolveObjectTImpl<DatabaseT, ConnectionT, StatementT, T>::type;

// Driver authors can declare a template specialization of the Driver class
// and use it to provide their driver init function. It is possible, but
// rarely useful, to subclass a driver.
template <typename DatabaseT, typename ConnectionT, typename StatementT>
class Driver {
 public:
  static AdbcStatusCode Init(int version, void* raw_driver, AdbcError* error) {
    if (version != ADBC_VERSION_1_0_0 && version != ADBC_VERSION_1_1_0) {
      return ADBC_STATUS_NOT_IMPLEMENTED;
    }

    auto* driver = reinterpret_cast<AdbcDriver*>(raw_driver);
    if (version >= ADBC_VERSION_1_1_0) {
      std::memset(driver, 0, ADBC_DRIVER_1_1_0_SIZE);

      driver->ErrorGetDetailCount = &CErrorGetDetailCount;
      driver->ErrorGetDetail = &CErrorGetDetail;

      driver->DatabaseGetOption = &CGetOption<AdbcDatabase>;
      driver->DatabaseGetOptionBytes = &CGetOptionBytes<AdbcDatabase>;
      driver->DatabaseGetOptionInt = &CGetOptionInt<AdbcDatabase>;
      driver->DatabaseGetOptionDouble = &CGetOptionDouble<AdbcDatabase>;
      driver->DatabaseSetOptionBytes = &CSetOptionBytes<AdbcDatabase>;
      driver->DatabaseSetOptionInt = &CSetOptionInt<AdbcDatabase>;
      driver->DatabaseSetOptionDouble = &CSetOptionDouble<AdbcDatabase>;

      driver->ConnectionCancel = &CConnectionCancel;
      driver->ConnectionGetOption = &CGetOption<AdbcConnection>;
      driver->ConnectionGetOptionBytes = &CGetOptionBytes<AdbcConnection>;
      driver->ConnectionGetOptionInt = &CGetOptionInt<AdbcConnection>;
      driver->ConnectionGetOptionDouble = &CGetOptionDouble<AdbcConnection>;
      driver->ConnectionGetStatistics = &CConnectionGetStatistics;
      driver->ConnectionGetStatisticNames = &CConnectionGetStatisticNames;
      driver->ConnectionSetOptionBytes = &CSetOptionBytes<AdbcConnection>;
      driver->ConnectionSetOptionInt = &CSetOptionInt<AdbcConnection>;
      driver->ConnectionSetOptionDouble = &CSetOptionDouble<AdbcConnection>;

      driver->StatementCancel = &CStatementCancel;
      driver->StatementExecuteSchema = &CStatementExecuteSchema;
      driver->StatementGetOption = &CGetOption<AdbcStatement>;
      driver->StatementGetOptionBytes = &CGetOptionBytes<AdbcStatement>;
      driver->StatementGetOptionInt = &CGetOptionInt<AdbcStatement>;
      driver->StatementGetOptionDouble = &CGetOptionDouble<AdbcStatement>;
      driver->StatementSetOptionBytes = &CSetOptionBytes<AdbcStatement>;
      driver->StatementSetOptionInt = &CSetOptionInt<AdbcStatement>;
      driver->StatementSetOptionDouble = &CSetOptionDouble<AdbcStatement>;
    } else {
      std::memset(driver, 0, ADBC_DRIVER_1_0_0_SIZE);
    }

    driver->private_data = new Driver();
    driver->release = &CDriverRelease;

    driver->DatabaseInit = &CDatabaseInit;
    driver->DatabaseNew = &CNew<AdbcDatabase>;
    driver->DatabaseRelease = &CRelease<AdbcDatabase>;
    driver->DatabaseSetOption = &CSetOption<AdbcDatabase>;

    driver->ConnectionCommit = &CConnectionCommit;
    driver->ConnectionGetInfo = &CConnectionGetInfo;
    driver->ConnectionGetObjects = &CConnectionGetObjects;
    driver->ConnectionGetTableSchema = &CConnectionGetTableSchema;
    driver->ConnectionGetTableTypes = &CConnectionGetTableTypes;
    driver->ConnectionInit = &CConnectionInit;
    driver->ConnectionNew = &CNew<AdbcConnection>;
    driver->ConnectionRelease = &CRelease<AdbcConnection>;
    driver->ConnectionReadPartition = &CConnectionReadPartition;
    driver->ConnectionRollback = &CConnectionRollback;
    driver->ConnectionSetOption = &CSetOption<AdbcConnection>;

    driver->StatementBind = &CStatementBind;
    driver->StatementBindStream = &CStatementBindStream;
    driver->StatementExecutePartitions = &CStatementExecutePartitions;
    driver->StatementExecuteQuery = &CStatementExecuteQuery;
    driver->StatementGetParameterSchema = &CStatementGetParameterSchema;
    driver->StatementNew = &CStatementNew;
    driver->StatementPrepare = &CStatementPrepare;
    driver->StatementRelease = &CRelease<AdbcStatement>;
    driver->StatementSetOption = &CSetOption<AdbcStatement>;
    driver->StatementSetSqlQuery = &CStatementSetSqlQuery;
    driver->StatementSetSubstraitPlan = &CStatementSetSubstraitPlan;

    return ADBC_STATUS_OK;
  }

  // Driver trampolines
  static AdbcStatusCode CDriverRelease(AdbcDriver* driver, AdbcError* error) {
    auto driver_private = reinterpret_cast<Driver*>(driver->private_data);
    delete driver_private;
    driver->private_data = nullptr;
    return ADBC_STATUS_OK;
  }

  static int CErrorGetDetailCount(const AdbcError* error) {
    if (error->vendor_code != ADBC_ERROR_VENDOR_CODE_PRIVATE_DATA) {
      return 0;
    }

    auto error_obj = reinterpret_cast<Status*>(error->private_data);
    if (!error_obj) {
      return 0;
    }
    return error_obj->CDetailCount();
  }

  static AdbcErrorDetail CErrorGetDetail(const AdbcError* error, int index) {
    if (error->vendor_code != ADBC_ERROR_VENDOR_CODE_PRIVATE_DATA) {
      return {nullptr, nullptr, 0};
    }

    auto error_obj = reinterpret_cast<Status*>(error->private_data);
    if (!error_obj) {
      return {nullptr, nullptr, 0};
    }

    return error_obj->CDetail(index);
  }

  // Templatable trampolines

  template <typename T>
  static AdbcStatusCode CNew(T* obj, AdbcError* error) {
    using ObjectT = ResolveObjectT<DatabaseT, ConnectionT, StatementT, T>;
    auto private_data = new ObjectT();
    obj->private_data = private_data;
    return ADBC_STATUS_OK;
  }

  template <typename T>
  static AdbcStatusCode CRelease(T* obj, AdbcError* error) {
    using ObjectT = ResolveObjectT<DatabaseT, ConnectionT, StatementT, T>;
    if (obj == nullptr) return ADBC_STATUS_INVALID_STATE;
    auto private_data = reinterpret_cast<ObjectT*>(obj->private_data);
    if (private_data == nullptr) return ADBC_STATUS_INVALID_STATE;
    AdbcStatusCode result = private_data->Release(error);
    if (result != ADBC_STATUS_OK) {
      return result;
    }

    delete private_data;
    obj->private_data = nullptr;
    return ADBC_STATUS_OK;
  }

  template <typename T>
  static AdbcStatusCode CSetOption(T* obj, const char* key, const char* value,
                                   AdbcError* error) {
    using ObjectT = ResolveObjectT<DatabaseT, ConnectionT, StatementT, T>;
    auto private_data = reinterpret_cast<ObjectT*>(obj->private_data);
    return private_data->template CSetOption<>(key, value, error);
  }

  template <typename T>
  static AdbcStatusCode CSetOptionBytes(T* obj, const char* key, const uint8_t* value,
                                        size_t length, AdbcError* error) {
    using ObjectT = ResolveObjectT<DatabaseT, ConnectionT, StatementT, T>;
    auto private_data = reinterpret_cast<ObjectT*>(obj->private_data);
    return private_data->CSetOptionBytes(key, value, length, error);
  }

  template <typename T>
  static AdbcStatusCode CSetOptionInt(T* obj, const char* key, int64_t value,
                                      AdbcError* error) {
    using ObjectT = ResolveObjectT<DatabaseT, ConnectionT, StatementT, T>;
    auto private_data = reinterpret_cast<ObjectT*>(obj->private_data);
    return private_data->template CSetOption<>(key, value, error);
  }

  template <typename T>
  static AdbcStatusCode CSetOptionDouble(T* obj, const char* key, double value,
                                         AdbcError* error) {
    using ObjectT = ResolveObjectT<DatabaseT, ConnectionT, StatementT, T>;
    auto private_data = reinterpret_cast<ObjectT*>(obj->private_data);
    return private_data->template CSetOption<>(key, value, error);
  }

  template <typename T>
  static AdbcStatusCode CGetOption(T* obj, const char* key, char* value, size_t* length,
                                   AdbcError* error) {
    using ObjectT = ResolveObjectT<DatabaseT, ConnectionT, StatementT, T>;
    auto private_data = reinterpret_cast<ObjectT*>(obj->private_data);
    return private_data->template CGetOptionStringLike<>(key, value, length, error);
  }

  template <typename T>
  static AdbcStatusCode CGetOptionBytes(T* obj, const char* key, uint8_t* value,
                                        size_t* length, AdbcError* error) {
    using ObjectT = ResolveObjectT<DatabaseT, ConnectionT, StatementT, T>;
    auto private_data = reinterpret_cast<ObjectT*>(obj->private_data);
    return private_data->template CGetOptionStringLike<>(key, value, length, error);
  }

  template <typename T>
  static AdbcStatusCode CGetOptionInt(T* obj, const char* key, int64_t* value,
                                      AdbcError* error) {
    using ObjectT = ResolveObjectT<DatabaseT, ConnectionT, StatementT, T>;
    auto private_data = reinterpret_cast<ObjectT*>(obj->private_data);
    return private_data->template CGetOptionNumeric<>(key, value, error);
  }

  template <typename T>
  static AdbcStatusCode CGetOptionDouble(T* obj, const char* key, double* value,
                                         AdbcError* error) {
    using ObjectT = ResolveObjectT<DatabaseT, ConnectionT, StatementT, T>;
    auto private_data = reinterpret_cast<ObjectT*>(obj->private_data);
    return private_data->template CGetOptionNumeric<>(key, value, error);
  }

#define CHECK_INIT(DATABASE, ERROR)                                         \
  if (!(DATABASE) || !(DATABASE)->private_data) {                           \
    return status::InvalidState("Database is uninitialized").ToAdbc(ERROR); \
  }

  // Database trampolines
  static AdbcStatusCode CDatabaseInit(AdbcDatabase* database, AdbcError* error) {
    CHECK_INIT(database, error);
    auto private_data = reinterpret_cast<DatabaseT*>(database->private_data);
    return private_data->Init(nullptr, error);
  }

#undef CHECK_INIT
#define CHECK_INIT(CONNECTION, ERROR)                                         \
  if (!(CONNECTION) || !(CONNECTION)->private_data) {                         \
    return status::InvalidState("Connection is uninitialized").ToAdbc(ERROR); \
  }

  // Connection trampolines
  static AdbcStatusCode CConnectionInit(AdbcConnection* connection,
                                        AdbcDatabase* database, AdbcError* error) {
    CHECK_INIT(connection, error);
    if (!database || !database->private_data) {
      return status::InvalidState("Database is uninitialized").ToAdbc(error);
    }
    auto private_data = reinterpret_cast<ConnectionT*>(connection->private_data);
    return private_data->Init(database->private_data, error);
  }

  static AdbcStatusCode CConnectionCancel(AdbcConnection* connection, AdbcError* error) {
    CHECK_INIT(connection, error);
    auto private_data = reinterpret_cast<ConnectionT*>(connection->private_data);
    return private_data->Cancel(error);
  }

  static AdbcStatusCode CConnectionGetInfo(AdbcConnection* connection,
                                           const uint32_t* info_codes,
                                           size_t info_codes_length,
                                           ArrowArrayStream* out, AdbcError* error) {
    CHECK_INIT(connection, error);
    auto private_data = reinterpret_cast<ConnectionT*>(connection->private_data);
    return private_data->GetInfo(info_codes, info_codes_length, out, error);
  }

  static AdbcStatusCode CConnectionGetObjects(AdbcConnection* connection, int depth,
                                              const char* catalog, const char* db_schema,
                                              const char* table_name,
                                              const char** table_type,
                                              const char* column_name,
                                              ArrowArrayStream* out, AdbcError* error) {
    CHECK_INIT(connection, error);
    auto private_data = reinterpret_cast<ConnectionT*>(connection->private_data);
    return private_data->GetObjects(depth, catalog, db_schema, table_name, table_type,
                                    column_name, out, error);
  }

  static AdbcStatusCode CConnectionGetStatistics(
      AdbcConnection* connection, const char* catalog, const char* db_schema,
      const char* table_name, char approximate, ArrowArrayStream* out, AdbcError* error) {
    CHECK_INIT(connection, error);
    auto private_data = reinterpret_cast<ConnectionT*>(connection->private_data);
    return private_data->GetStatistics(catalog, db_schema, table_name, approximate, out,
                                       error);
  }

  static AdbcStatusCode CConnectionGetStatisticNames(AdbcConnection* connection,
                                                     ArrowArrayStream* out,
                                                     AdbcError* error) {
    CHECK_INIT(connection, error);
    auto private_data = reinterpret_cast<ConnectionT*>(connection->private_data);
    return private_data->GetStatisticNames(out, error);
  }

  static AdbcStatusCode CConnectionGetTableSchema(AdbcConnection* connection,
                                                  const char* catalog,
                                                  const char* db_schema,
                                                  const char* table_name,
                                                  ArrowSchema* schema, AdbcError* error) {
    CHECK_INIT(connection, error);
    auto private_data = reinterpret_cast<ConnectionT*>(connection->private_data);
    return private_data->GetTableSchema(catalog, db_schema, table_name, schema, error);
  }

  static AdbcStatusCode CConnectionGetTableTypes(AdbcConnection* connection,
                                                 ArrowArrayStream* out,
                                                 AdbcError* error) {
    CHECK_INIT(connection, error);
    auto private_data = reinterpret_cast<ConnectionT*>(connection->private_data);
    return private_data->GetTableTypes(out, error);
  }

  static AdbcStatusCode CConnectionReadPartition(AdbcConnection* connection,
                                                 const uint8_t* serialized_partition,
                                                 size_t serialized_length,
                                                 ArrowArrayStream* out,
                                                 AdbcError* error) {
    CHECK_INIT(connection, error);
    auto private_data = reinterpret_cast<ConnectionT*>(connection->private_data);
    return private_data->ReadPartition(serialized_partition, serialized_length, out,
                                       error);
  }

  static AdbcStatusCode CConnectionCommit(AdbcConnection* connection, AdbcError* error) {
    CHECK_INIT(connection, error);
    auto private_data = reinterpret_cast<ConnectionT*>(connection->private_data);
    return private_data->Commit(error);
  }

  static AdbcStatusCode CConnectionRollback(AdbcConnection* connection,
                                            AdbcError* error) {
    CHECK_INIT(connection, error);
    auto private_data = reinterpret_cast<ConnectionT*>(connection->private_data);
    return private_data->Rollback(error);
  }

#undef CHECK_INIT
#define CHECK_INIT(STATEMENT, ERROR)                                         \
  if (!(STATEMENT) || !(STATEMENT)->private_data) {                          \
    return status::InvalidState("Statement is uninitialized").ToAdbc(ERROR); \
  }

  // Statement trampolines
  static AdbcStatusCode CStatementNew(AdbcConnection* connection,
                                      AdbcStatement* statement, AdbcError* error) {
    if (!connection || !connection->private_data) {
      return status::InvalidState("Connection is uninitialized").ToAdbc(error);
    }
    auto private_data = new StatementT();
    AdbcStatusCode status = private_data->Init(connection->private_data, error);
    if (status != ADBC_STATUS_OK) {
      delete private_data;
    }

    statement->private_data = private_data;
    return ADBC_STATUS_OK;
  }

  static AdbcStatusCode CStatementBind(AdbcStatement* statement, ArrowArray* values,
                                       ArrowSchema* schema, AdbcError* error) {
    CHECK_INIT(statement, error);
    auto private_data = reinterpret_cast<StatementT*>(statement->private_data);
    return private_data->Bind(values, schema, error);
  }

  static AdbcStatusCode CStatementBindStream(AdbcStatement* statement,
                                             ArrowArrayStream* stream, AdbcError* error) {
    CHECK_INIT(statement, error);
    auto private_data = reinterpret_cast<StatementT*>(statement->private_data);
    return private_data->BindStream(stream, error);
  }

  static AdbcStatusCode CStatementCancel(AdbcStatement* statement, AdbcError* error) {
    CHECK_INIT(statement, error);
    auto private_data = reinterpret_cast<StatementT*>(statement->private_data);
    return private_data->Cancel(error);
  }

  static AdbcStatusCode CStatementExecutePartitions(AdbcStatement* statement,
                                                    struct ArrowSchema* schema,
                                                    struct AdbcPartitions* partitions,
                                                    int64_t* rows_affected,
                                                    AdbcError* error) {
    CHECK_INIT(statement, error);
    auto private_data = reinterpret_cast<StatementT*>(statement->private_data);
    return private_data->ExecutePartitions(schema, partitions, rows_affected, error);
  }

  static AdbcStatusCode CStatementExecuteQuery(AdbcStatement* statement,
                                               ArrowArrayStream* stream,
                                               int64_t* rows_affected, AdbcError* error) {
    CHECK_INIT(statement, error);
    auto private_data = reinterpret_cast<StatementT*>(statement->private_data);
    return private_data->ExecuteQuery(stream, rows_affected, error);
  }

  static AdbcStatusCode CStatementExecuteSchema(AdbcStatement* statement,
                                                ArrowSchema* schema, AdbcError* error) {
    CHECK_INIT(statement, error);
    auto private_data = reinterpret_cast<StatementT*>(statement->private_data);
    return private_data->ExecuteSchema(schema, error);
  }

  static AdbcStatusCode CStatementGetParameterSchema(AdbcStatement* statement,
                                                     ArrowSchema* schema,
                                                     AdbcError* error) {
    CHECK_INIT(statement, error);
    auto private_data = reinterpret_cast<StatementT*>(statement->private_data);
    return private_data->GetParameterSchema(schema, error);
  }

  static AdbcStatusCode CStatementPrepare(AdbcStatement* statement, AdbcError* error) {
    CHECK_INIT(statement, error);
    auto private_data = reinterpret_cast<StatementT*>(statement->private_data);
    return private_data->Prepare(error);
  }

  static AdbcStatusCode CStatementSetSqlQuery(AdbcStatement* statement, const char* query,
                                              AdbcError* error) {
    CHECK_INIT(statement, error);
    auto private_data = reinterpret_cast<StatementT*>(statement->private_data);
    return private_data->SetSqlQuery(query, error);
  }

  static AdbcStatusCode CStatementSetSubstraitPlan(AdbcStatement* statement,
                                                   const uint8_t* plan, size_t length,
                                                   AdbcError* error) {
    CHECK_INIT(statement, error);
    auto private_data = reinterpret_cast<StatementT*>(statement->private_data);
    return private_data->SetSubstraitPlan(plan, length, error);
  }

#undef CHECK_INIT
};

template <typename Derived>
class BaseDatabase : public ObjectBase {
 public:
  using Base = BaseDatabase<Derived>;

  BaseDatabase() : ObjectBase() {}
  ~BaseDatabase() = default;

  /// \internal
  AdbcStatusCode Init(void* parent, AdbcError* error) override {
    RAISE_STATUS(error, impl().InitImpl());
    return ObjectBase::Init(parent, error);
  }

  /// \internal
  AdbcStatusCode Release(AdbcError* error) override {
    RAISE_STATUS(error, impl().ReleaseImpl());
    return ADBC_STATUS_OK;
  }

  /// \internal
  AdbcStatusCode SetOption(std::string_view key, Option value,
                           AdbcError* error) override {
    RAISE_STATUS(error, impl().SetOptionImpl(key, std::move(value)));
    return ADBC_STATUS_OK;
  }

  /// \brief Initialize the database.
  virtual Status InitImpl() { return status::Ok(); }

  /// \brief Release the database.
  virtual Status ReleaseImpl() { return status::Ok(); }

  /// \brief Set an option.  May be called prior to InitImpl.
  virtual Status SetOptionImpl(std::string_view key, Option value) {
    return status::NotImplemented(Derived::kErrorPrefix, " Unknown database option ", key,
                                  "=", value.Format());
  }

 private:
  Derived& impl() { return static_cast<Derived&>(*this); }
};

template <typename Derived>
class BaseConnection : public ObjectBase {
 public:
  using Base = BaseConnection<Derived>;

  /// \brief Whether autocommit is enabled or not (by default: enabled).
  enum class AutocommitState {
    kAutocommit,
    kTransaction,
  };

  BaseConnection() : ObjectBase() {}
  ~BaseConnection() = default;

  /// \internal
  AdbcStatusCode Init(void* parent, AdbcError* error) override {
    RAISE_STATUS(error, impl().InitImpl(parent));
    return ObjectBase::Init(parent, error);
  }

  /// \brief Initialize the database.
  virtual Status InitImpl(void* parent) { return status::Ok(); }

  /// \internal
  AdbcStatusCode Cancel(AdbcError* error) { return impl().CancelImpl().ToAdbc(error); }

  Status CancelImpl() { return status::NotImplemented("Cancel"); }

  /// \internal
  AdbcStatusCode Commit(AdbcError* error) { return impl().CommitImpl().ToAdbc(error); }

  Status CommitImpl() { return status::NotImplemented("Commit"); }

  /// \internal
  AdbcStatusCode GetInfo(const uint32_t* info_codes, size_t info_codes_length,
                         ArrowArrayStream* out, AdbcError* error) {
    std::vector<uint32_t> codes(info_codes, info_codes + info_codes_length);
    RAISE_STATUS(error, impl().GetInfoImpl(codes, out));
    return ADBC_STATUS_OK;
  }

  Status GetInfoImpl(const std::vector<uint32_t> info_codes, ArrowArrayStream* out) {
    return status::NotImplemented("GetInfo");
  }

  /// \internal
  AdbcStatusCode GetObjects(int c_depth, const char* catalog, const char* db_schema,
                            const char* table_name, const char** table_type,
                            const char* column_name, ArrowArrayStream* out,
                            AdbcError* error) {
    const auto catalog_filter =
        catalog ? std::make_optional(std::string_view(catalog)) : std::nullopt;
    const auto schema_filter =
        db_schema ? std::make_optional(std::string_view(db_schema)) : std::nullopt;
    const auto table_filter =
        table_name ? std::make_optional(std::string_view(table_name)) : std::nullopt;
    const auto column_filter =
        column_name ? std::make_optional(std::string_view(column_name)) : std::nullopt;
    std::vector<std::string_view> table_type_filter;
    while (table_type && *table_type) {
      if (*table_type) {
        table_type_filter.push_back(std::string_view(*table_type));
      }
      table_type++;
    }

    RAISE_STATUS(
        error, impl().GetObjectsImpl(c_depth, catalog_filter, schema_filter, table_filter,
                                     column_filter, table_type_filter, out));

    return ADBC_STATUS_OK;
  }

  Status GetObjectsImpl(int c_depth, std::optional<std::string_view> catalog_filter,
                        std::optional<std::string_view> schema_filter,
                        std::optional<std::string_view> table_filter,
                        std::optional<std::string_view> column_filter,
                        const std::vector<std::string_view>& table_types,
                        struct ArrowArrayStream* out) {
    return status::NotImplemented("GetObjects");
  }

  /// \internal
  AdbcStatusCode GetStatistics(const char* catalog, const char* db_schema,
                               const char* table_name, char approximate,
                               ArrowArrayStream* out, AdbcError* error) {
    const auto catalog_filter =
        catalog ? std::make_optional(std::string_view(catalog)) : std::nullopt;
    const auto schema_filter =
        db_schema ? std::make_optional(std::string_view(db_schema)) : std::nullopt;
    const auto table_filter =
        table_name ? std::make_optional(std::string_view(table_name)) : std::nullopt;
    RAISE_STATUS(error, impl().GetStatisticsImpl(catalog_filter, schema_filter,
                                                 table_filter, approximate != 0, out));
    return ADBC_STATUS_OK;
  }

  Status GetStatisticsImpl(std::optional<std::string_view> catalog,
                           std::optional<std::string_view> db_schema,
                           std::optional<std::string_view> table_name, bool approximate,
                           ArrowArrayStream* out) {
    return status::NotImplemented("GetStatistics");
  }

  /// \internal
  AdbcStatusCode GetStatisticNames(ArrowArrayStream* out, AdbcError* error) {
    RAISE_STATUS(error, impl().GetStatisticNames(out));
    return ADBC_STATUS_OK;
  }

  Status GetStatisticNames(ArrowArrayStream* out) {
    return status::NotImplemented("GetStatisticNames");
  }

  /// \internal
  AdbcStatusCode GetTableSchema(const char* catalog, const char* db_schema,
                                const char* table_name, ArrowSchema* schema,
                                AdbcError* error) {
    if (!table_name) {
      return status::InvalidArgument(Derived::kErrorPrefix,
                                     " GetTableSchema: must provide table_name")
          .ToAdbc(error);
    }

    std::optional<std::string_view> catalog_param =
        catalog ? std::make_optional(std::string_view(catalog)) : std::nullopt;
    std::optional<std::string_view> db_schema_param =
        db_schema ? std::make_optional(std::string_view(db_schema)) : std::nullopt;

    RAISE_STATUS(error, impl().GetTableSchemaImpl(catalog_param, db_schema_param,
                                                  table_name, schema));
    return ADBC_STATUS_OK;
  }

  Status GetTableSchemaImpl(std::optional<std::string_view> catalog,
                            std::optional<std::string_view> db_schema,
                            std::string_view table_name, ArrowSchema* out) {
    return status::NotImplemented("GetTableSchema");
  }

  /// \internal
  AdbcStatusCode GetTableTypes(ArrowArrayStream* out, AdbcError* error) {
    RAISE_STATUS(error, impl().GetTableTypesImpl(out));
    return ADBC_STATUS_OK;
  }

  Status GetTableTypesImpl(ArrowArrayStream* out) {
    return status::NotImplemented("GetTableTypes");
  }

  /// \internal
  AdbcStatusCode ReadPartition(const uint8_t* serialized_partition,
                               size_t serialized_length, ArrowArrayStream* out,
                               AdbcError* error) {
    std::string_view partition(reinterpret_cast<const char*>(serialized_partition),
                               serialized_length);
    RAISE_STATUS(error, impl().ReadPartitionImpl(partition, out));
    return ADBC_STATUS_OK;
  }

  Status ReadPartitionImpl(std::string_view serialized_partition, ArrowArrayStream* out) {
    return status::NotImplemented("ReadPartition");
  }

  /// \internal
  AdbcStatusCode Release(AdbcError* error) override {
    RAISE_STATUS(error, impl().ReleaseImpl());
    return ADBC_STATUS_OK;
  }

  Status ReleaseImpl() { return status::Ok(); }

  /// \internal
  AdbcStatusCode Rollback(AdbcError* error) {
    RAISE_STATUS(error, impl().RollbackImpl());
    return ADBC_STATUS_OK;
  }

  Status RollbackImpl() { return status::NotImplemented("Rollback"); }

  /// \internal
  AdbcStatusCode SetOption(std::string_view key, Option value,
                           AdbcError* error) override {
    RAISE_STATUS(error, impl().SetOptionImpl(key, value));
    return ADBC_STATUS_OK;
  }

  /// \brief Set an option.  May be called prior to InitImpl.
  virtual Status SetOptionImpl(std::string_view key, Option value) {
    return status::NotImplemented(Derived::kErrorPrefix, " Unknown connection option ",
                                  key, "=", value.Format());
  }

 private:
  Derived& impl() { return static_cast<Derived&>(*this); }
};

template <typename Derived>
class BaseStatement : public ObjectBase {
 public:
  using Base = BaseStatement<Derived>;

  /// \internal
  AdbcStatusCode Init(void* parent, AdbcError* error) override {
    RAISE_STATUS(error, impl().InitImpl(parent));
    return ObjectBase::Init(parent, error);
  }

  /// \brief Initialize the statement.
  Status InitImpl(void* parent) { return status::Ok(); }

  /// \internal
  AdbcStatusCode Release(AdbcError* error) override {
    RAISE_STATUS(error, impl().ReleaseImpl());
    return ADBC_STATUS_OK;
  }

  Status ReleaseImpl() { return status::Ok(); }

  /// \internal
  AdbcStatusCode SetOption(std::string_view key, Option value,
                           AdbcError* error) override {
    RAISE_STATUS(error, impl().SetOptionImpl(key, value));
    return ADBC_STATUS_OK;
  }

  /// \brief Set an option.  May be called prior to InitImpl.
  virtual Status SetOptionImpl(std::string_view key, Option value) {
    return status::NotImplemented(Derived::kErrorPrefix, " Unknown statement option ",
                                  key, "=", value.Format());
  }

  AdbcStatusCode ExecuteQuery(ArrowArrayStream* stream, int64_t* rows_affected,
                              AdbcError* error) {
    RAISE_RESULT(error, int64_t rows_affected_result, impl().ExecuteQueryImpl(stream));
    if (rows_affected) {
      *rows_affected = rows_affected_result;
    }

    return ADBC_STATUS_OK;
  }

  Result<int64_t> ExecuteQueryImpl(ArrowArrayStream* stream) {
    return status::NotImplemented("ExecuteQuery");
  }

  AdbcStatusCode ExecuteSchema(ArrowSchema* schema, AdbcError* error) {
    RAISE_STATUS(error, impl().ExecuteSchemaImpl(schema));
    return ADBC_STATUS_OK;
  }

  Status ExecuteSchemaImpl(ArrowSchema* schema) {
    return status::NotImplemented("ExecuteSchema");
  }

  AdbcStatusCode Prepare(AdbcError* error) {
    RAISE_STATUS(error, impl().PrepareImpl());
    return ADBC_STATUS_OK;
  }

  Status PrepareImpl() { return status::NotImplemented("Prepare"); }

  AdbcStatusCode SetSqlQuery(const char* query, AdbcError* error) {
    RAISE_STATUS(error, impl().SetSqlQueryImpl(query));
    return ADBC_STATUS_OK;
  }

  Status SetSqlQueryImpl(std::string_view query) {
    return status::NotImplemented("SetSqlQuery");
  }

  AdbcStatusCode SetSubstraitPlan(const uint8_t* plan, size_t length, AdbcError* error) {
    RAISE_STATUS(error, impl().SetSubstraitPlanImpl(std::string_view(
                            reinterpret_cast<const char*>(plan), length)));
    return ADBC_STATUS_OK;
  }

  Status SetSubstraitPlanImpl(std::string_view plan) {
    return status::NotImplemented("SetSubstraitPlan");
  }

  AdbcStatusCode Bind(ArrowArray* values, ArrowSchema* schema, AdbcError* error) {
    RAISE_STATUS(error, impl().BindImpl(values, schema));
    return ADBC_STATUS_OK;
  }

  Status BindImpl(ArrowArray* values, ArrowSchema* schema) {
    return status::NotImplemented("Bind");
  }

  AdbcStatusCode BindStream(ArrowArrayStream* stream, AdbcError* error) {
    RAISE_STATUS(error, impl().BindStreamImpl(stream));
    return ADBC_STATUS_OK;
  }

  Status BindStreamImpl(ArrowArrayStream* stream) {
    return status::NotImplemented("BindStream");
  }

  AdbcStatusCode GetParameterSchema(ArrowSchema* schema, AdbcError* error) {
    RAISE_STATUS(error, impl().GetParameterSchemaImpl(schema));
    return ADBC_STATUS_OK;
  }

  Status GetParameterSchemaImpl(struct ArrowSchema* schema) {
    return status::NotImplemented("GetParameterSchema");
  }

  AdbcStatusCode ExecutePartitions(ArrowSchema* schema, AdbcPartitions* partitions,
                                   int64_t* rows_affected, AdbcError* error) {
    return ADBC_STATUS_NOT_IMPLEMENTED;
  }

  AdbcStatusCode Cancel(AdbcError* error) {
    RAISE_STATUS(error, impl().Cancel());
    return ADBC_STATUS_OK;
  }

  Status Cancel() { return status::NotImplemented("Cancel"); }

 private:
  Derived& impl() { return static_cast<Derived&>(*this); }
};

}  // namespace adbc::driver
