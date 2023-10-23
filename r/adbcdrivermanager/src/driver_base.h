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

#include <cstring>
#include <memory>
#include <sstream>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <adbc.h>

namespace adbc {

namespace r {

class Error {
 public:
  explicit Error(const std::string& message)
      : message_(message), sql_state_("\0\0\0\0\0") {}

  explicit Error(const char* message) : Error(std::string(message)) {}

  Error(const std::string& message,
        const std::vector<std::pair<std::string, std::string>>& details)
      : message_(message), details_(details), sql_state_("\0\0\0\0\0") {}

  void AddDetail(const std::string& key, const std::string& value) {
    details_.push_back({key, value});
  }

  void ToAdbc(AdbcError* adbc_error, AdbcDriver* driver = nullptr) {
    if (adbc_error->vendor_code == ADBC_ERROR_VENDOR_CODE_PRIVATE_DATA) {
      auto error_owned_by_adbc_error = new Error(message_, details_);
      adbc_error->message =
          const_cast<char*>(error_owned_by_adbc_error->message_.c_str());
      adbc_error->private_data = error_owned_by_adbc_error;
      adbc_error->private_driver = driver;
    } else {
      adbc_error->message = reinterpret_cast<char*>(std::malloc(message_.size() + 1));
      if (adbc_error->message != nullptr) {
        memcpy(adbc_error->message, message_.c_str(), message_.size() + 1);
      }
    }

    for (size_t i = 0; i < 5; i++) {
      adbc_error->sqlstate[i] = sql_state_[i];
    }
    adbc_error->release = &CRelease;
  }

 private:
  std::string message_;
  std::vector<std::pair<std::string, std::string>> details_;
  std::string sql_state_;

  // Let the Driver use these to expose C callables wrapping option setters/getters
  template <typename DatabaseT, typename ConnectionT, typename StatementT>
  friend class Driver;

  int CDetailCount() const { return details_.size(); }

  AdbcErrorDetail CDetail(int index) const {
    const auto& detail = details_[index];
    return {detail.first.c_str(), reinterpret_cast<const uint8_t*>(detail.second.data()),
            detail.second.size()};
  }

  static void CRelease(AdbcError* error) {
    if (error->vendor_code == ADBC_ERROR_VENDOR_CODE_PRIVATE_DATA) {
      auto error_obj = reinterpret_cast<Error*>(error->private_data);
      delete error_obj;
    } else {
      std::free(error->message);
    }

    std::memset(error, 0, sizeof(AdbcError));
  }
};

class Option {
 public:
  enum Type { TYPE_MISSING, TYPE_STRING, TYPE_BYTES, TYPE_INT, TYPE_DOUBLE };

  Option() : type_(TYPE_MISSING) {}
  explicit Option(const std::string& value) : type_(TYPE_STRING), value_string_(value) {}
  explicit Option(const std::basic_string<uint8_t>& value)
      : type_(TYPE_BYTES), value_bytes_(value) {}
  explicit Option(double value) : type_(TYPE_DOUBLE), value_double_(value) {}
  explicit Option(int64_t value) : type_(TYPE_INT), value_int_(value) {}

  Type type() const { return type_; }

  const std::string& GetStringUnsafe() const { return value_string_; }

  const std::basic_string<uint8_t>& GetBytesUnsafe() const { return value_bytes_; }

  int64_t GetIntUnsafe() const { return value_int_; }

  double GetDoubleUnsafe() const { return value_double_; }

 private:
  Type type_;
  std::string value_string_;
  std::basic_string<uint8_t> value_bytes_;
  double value_double_;
  int64_t value_int_;

  // Methods used by trampolines below
  friend class ObjectBase;

  AdbcStatusCode CGet(char* out, size_t* length) const {
    switch (type_) {
      case TYPE_STRING: {
        const std::string& value = GetStringUnsafe();
        if (*length < value.size()) {
          *length = value.size();
        } else {
          memcpy(out, value.data(), value.size());
        }

        return ADBC_STATUS_OK;
      }
      default:
        return ADBC_STATUS_NOT_FOUND;
    }
  }

  AdbcStatusCode CGet(uint8_t* out, size_t* length) const {
    switch (type_) {
      case TYPE_BYTES: {
        const std::basic_string<uint8_t>& value = GetBytesUnsafe();
        if (*length < value.size()) {
          *length = value.size();
        } else {
          memcpy(out, value.data(), value.size());
        }

        return ADBC_STATUS_OK;
      }
      default:
        return ADBC_STATUS_NOT_FOUND;
    }
  }

  AdbcStatusCode CGet(int64_t* value) const {
    switch (type_) {
      case TYPE_INT:
        *value = GetIntUnsafe();
        return ADBC_STATUS_OK;
      default:
        return ADBC_STATUS_NOT_FOUND;
    }
  }

  AdbcStatusCode CGet(double* value) const {
    switch (type_) {
      case TYPE_DOUBLE:
        *value = GetDoubleUnsafe();
        return ADBC_STATUS_OK;
      default:
        return ADBC_STATUS_NOT_FOUND;
    }
  }
};

// Base class for private_data of AdbcDatabase, AdbcConnection, and AdbcStatement
// This class handles option setting and getting.
class ObjectBase {
 public:
  ObjectBase() : driver_(nullptr) {}

  virtual ~ObjectBase() {}

  virtual bool OptionKeySupported(const std::string& key, const Option& value) const {
    return true;
  }

  virtual AdbcStatusCode Init(void* parent, AdbcError* error) { return ADBC_STATUS_OK; }

  virtual AdbcStatusCode Release(AdbcError* error) { return ADBC_STATUS_OK; }

  const Option& GetOption(const std::string& key,
                          const Option& default_value = Option()) const {
    auto result = options_.find(key);
    if (result == options_.end()) {
      return default_value;
    } else {
      return result->second;
    }
  }

  AdbcStatusCode SetOption(const std::string& key, const Option& value) {
    if (!OptionKeySupported(key, value)) {
      return ADBC_STATUS_NOT_IMPLEMENTED;
    }

    options_[key] = value;
    return ADBC_STATUS_OK;
  }

 private:
  AdbcDriver* driver_;
  std::unordered_map<std::string, Option> options_;

  // Let the Driver use these to expose C callables wrapping option setters/getters
  template <typename DatabaseT, typename ConnectionT, typename StatementT>
  friend class Driver;

  void set_driver(AdbcDriver* driver) { driver_ = driver; }

  template <typename T>
  AdbcStatusCode CSetOption(const char* key, T value, AdbcError* error) {
    Option option(value);
    AdbcStatusCode status = SetOption(key, option);
    if (status == ADBC_STATUS_NOT_IMPLEMENTED) {
      InitErrorOptionNotSupported(key, error);
      return ADBC_STATUS_NOT_IMPLEMENTED;
    }

    return status;
  }

  AdbcStatusCode CSetOptionBytes(const char* key, const uint8_t* value, size_t length,
                                 AdbcError* error) {
    std::basic_string<uint8_t> cppvalue(value, length);
    Option option(cppvalue);
    AdbcStatusCode status = SetOption(key, option);
    if (status == ADBC_STATUS_NOT_IMPLEMENTED) {
      InitErrorOptionNotSupported(key, error);
      return ADBC_STATUS_NOT_IMPLEMENTED;
    }

    return status;
  }

  template <typename T>
  AdbcStatusCode CGetOptionStringLike(const char* key, T* value, size_t* length,
                                      AdbcError* error) const {
    Option result = GetOption(key);
    if (result.type() == Option::TYPE_MISSING) {
      InitErrorNotFound(key, error);
      return ADBC_STATUS_NOT_FOUND;
    } else {
      AdbcStatusCode status = result.CGet(value, length);
      if (status != ADBC_STATUS_OK) {
        InitErrorWrongType(key, error);
      }

      return status;
    }
  }

  template <typename T>
  AdbcStatusCode CGetOptionNumeric(const char* key, T* value, AdbcError* error) const {
    Option result = GetOption(key);
    if (result.type() == Option::TYPE_MISSING) {
      InitErrorNotFound(key, error);
      return ADBC_STATUS_NOT_FOUND;
    } else {
      AdbcStatusCode status = result.CGet(value);
      if (status != ADBC_STATUS_OK) {
        InitErrorWrongType(key, error);
      }

      return status;
    }
  }

  void InitErrorNotFound(const char* key, AdbcError* error) const {
    std::stringstream msg_builder;
    msg_builder << "Option not found for key '" << key << "'";
    Error cpperror(msg_builder.str());
    cpperror.AddDetail("adbc.r.option_key", key);
    cpperror.ToAdbc(error, driver_);
  }

  void InitErrorWrongType(const char* key, AdbcError* error) const {
    std::stringstream msg_builder;
    msg_builder << "Wrong type requested for option key '" << key << "'";
    Error cpperror(msg_builder.str());
    cpperror.AddDetail("adbc.r.option_key", key);
    cpperror.ToAdbc(error, driver_);
  }

  void InitErrorOptionNotSupported(const char* key, AdbcError* error) const {
    std::stringstream msg_builder;
    msg_builder << "Option '" << key << "' is not supported";
    Error cpperror(msg_builder.str());
    cpperror.AddDetail("adbc.r.option_key", key);
    cpperror.ToAdbc(error, driver_);
  }
};

class DatabaseObjectBase : public ObjectBase {
 public:
  // (there are no database functions other than option getting/setting)
};

class ConnectionObjectBase : public ObjectBase {
 public:
  // TODO: Add connection functions here as methods
};

class StatementObjectBase : public ObjectBase {
 public:
  virtual AdbcStatusCode ExecuteQuery(struct ArrowArrayStream* stream,
                                      int64_t* rows_affected, struct AdbcError* error) {
    return ADBC_STATUS_NOT_IMPLEMENTED;
  }

  // TODO: Add remaining statement functions here as methods
};

template <typename DatabaseT, typename ConnectionT, typename StatementT>
class Driver {
 public:
  static AdbcStatusCode Init(int version, void* raw_driver, AdbcError* error) {
    if (version != ADBC_VERSION_1_1_0) return ADBC_STATUS_NOT_IMPLEMENTED;
    struct AdbcDriver* driver = (AdbcDriver*)raw_driver;
    std::memset(driver, 0, sizeof(AdbcDriver));

    // Driver lifecycle
    driver->private_data = new Driver();
    driver->release = &CDriverRelease;

    // Driver functions
    driver->ErrorGetDetailCount = &CErrorGetDetailCount;
    driver->ErrorGetDetail = &CErrorGetDetail;

    // Database lifecycle
    driver->DatabaseNew = &CNew<AdbcDatabase, DatabaseT>;
    driver->DatabaseInit = &CDatabaseInit;
    driver->DatabaseRelease = &CRelease<AdbcDatabase, DatabaseT>;

    // Database functions
    driver->DatabaseSetOption = &CSetOption<AdbcDatabase, DatabaseT>;
    driver->DatabaseSetOptionBytes = &CSetOptionBytes<AdbcDatabase, DatabaseT>;
    driver->DatabaseSetOptionInt = &CSetOptionInt<AdbcDatabase, DatabaseT>;
    driver->DatabaseSetOptionDouble = &CSetOptionDouble<AdbcDatabase, DatabaseT>;
    driver->DatabaseGetOption = &CGetOption<AdbcDatabase, DatabaseT>;
    driver->DatabaseGetOptionBytes = &CGetOptionBytes<AdbcDatabase, DatabaseT>;
    driver->DatabaseGetOptionInt = &CGetOptionInt<AdbcDatabase, DatabaseT>;
    driver->DatabaseGetOptionDouble = &CGetOptionDouble<AdbcDatabase, DatabaseT>;

    // Connection lifecycle
    driver->ConnectionNew = &CNew<AdbcConnection, ConnectionT>;
    driver->ConnectionInit = &CConnectionInit;
    driver->ConnectionRelease = &CRelease<AdbcConnection, ConnectionT>;

    // Connection functions
    driver->ConnectionSetOption = &CSetOption<AdbcConnection, ConnectionT>;
    driver->ConnectionSetOptionBytes = &CSetOptionBytes<AdbcConnection, ConnectionT>;
    driver->ConnectionSetOptionInt = &CSetOptionInt<AdbcConnection, ConnectionT>;
    driver->ConnectionSetOptionDouble = &CSetOptionDouble<AdbcConnection, ConnectionT>;
    driver->ConnectionGetOption = &CGetOption<AdbcConnection, ConnectionT>;
    driver->ConnectionGetOptionBytes = &CGetOptionBytes<AdbcConnection, ConnectionT>;
    driver->ConnectionGetOptionInt = &CGetOptionInt<AdbcConnection, ConnectionT>;
    driver->ConnectionGetOptionDouble = &CGetOptionDouble<AdbcConnection, ConnectionT>;

    // Statement lifecycle
    driver->StatementNew = &CStatementNew;
    driver->StatementRelease = &CRelease<AdbcStatement, StatementT>;

    // Statement functions
    driver->StatementSetOption = &CSetOption<AdbcStatement, StatementT>;
    driver->StatementSetOptionBytes = &CSetOptionBytes<AdbcStatement, StatementT>;
    driver->StatementSetOptionInt = &CSetOptionInt<AdbcStatement, StatementT>;
    driver->StatementSetOptionDouble = &CSetOptionDouble<AdbcStatement, StatementT>;
    driver->StatementGetOption = &CGetOption<AdbcStatement, StatementT>;
    driver->StatementGetOptionBytes = &CGetOptionBytes<AdbcStatement, StatementT>;
    driver->StatementGetOptionInt = &CGetOptionInt<AdbcStatement, StatementT>;
    driver->StatementGetOptionDouble = &CGetOptionDouble<AdbcStatement, StatementT>;

    driver->StatementExecuteQuery = &CStatementExecuteQuery;

    return ADBC_STATUS_OK;
  }

 private:
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

    auto error_obj = reinterpret_cast<Error*>(error->private_data);
    return error_obj->CDetailCount();
  }

  static AdbcErrorDetail CErrorGetDetail(const AdbcError* error, int index) {
    auto error_obj = reinterpret_cast<Error*>(error->private_data);
    return error_obj->CDetail(index);
  }

  // Templatable trampolines
  template <typename T, typename ObjectT>
  static AdbcStatusCode CNew(T* obj, AdbcError* error) {
    auto private_data = new ObjectT();
    obj->private_data = private_data;
    return ADBC_STATUS_OK;
  }

  template <typename T, typename ObjectT>
  static AdbcStatusCode CRelease(T* obj, AdbcError* error) {
    auto private_data = reinterpret_cast<ObjectT*>(obj->private_data);
    AdbcStatusCode result = private_data->Release(error);
    if (result != ADBC_STATUS_OK) {
      return result;
    }

    delete private_data;
    obj->private_data = nullptr;
    return ADBC_STATUS_OK;
  }

  template <typename T, typename ObjectT>
  static AdbcStatusCode CSetOption(T* obj, const char* key, const char* value,
                                   AdbcError* error) {
    auto private_data = reinterpret_cast<ObjectT*>(obj->private_data);
    return private_data->template CSetOption<>(key, value, error);
  }

  template <typename T, typename ObjectT>
  static AdbcStatusCode CSetOptionBytes(T* obj, const char* key, const uint8_t* value,
                                        size_t length, AdbcError* error) {
    auto private_data = reinterpret_cast<ObjectT*>(obj->private_data);
    return private_data->CSetOptionBytes(key, value, length, error);
  }

  template <typename T, typename ObjectT>
  static AdbcStatusCode CSetOptionInt(T* obj, const char* key, int64_t value,
                                      AdbcError* error) {
    auto private_data = reinterpret_cast<ObjectT*>(obj->private_data);
    return private_data->template CSetOption<>(key, value, error);
  }

  template <typename T, typename ObjectT>
  static AdbcStatusCode CSetOptionDouble(T* obj, const char* key, double value,
                                         AdbcError* error) {
    auto private_data = reinterpret_cast<ObjectT*>(obj->private_data);
    return private_data->template CSetOption<>(key, value, error);
  }

  template <typename T, typename ObjectT>
  static AdbcStatusCode CGetOption(T* obj, const char* key, char* value, size_t* length,
                                   AdbcError* error) {
    auto private_data = reinterpret_cast<ObjectT*>(obj->private_data);
    return private_data->template CGetOptionStringLike<>(key, value, length, error);
  }

  template <typename T, typename ObjectT>
  static AdbcStatusCode CGetOptionBytes(T* obj, const char* key, uint8_t* value,
                                        size_t* length, AdbcError* error) {
    auto private_data = reinterpret_cast<ObjectT*>(obj->private_data);
    return private_data->template CGetOptionStringLike<>(key, value, length, error);
  }

  template <typename T, typename ObjectT>
  static AdbcStatusCode CGetOptionInt(T* obj, const char* key, int64_t* value,
                                      AdbcError* error) {
    auto private_data = reinterpret_cast<ObjectT*>(obj->private_data);
    return private_data->template CGetOptionNumeric<>(key, value, error);
  }

  template <typename T, typename ObjectT>
  static AdbcStatusCode CGetOptionDouble(T* obj, const char* key, double* value,
                                         AdbcError* error) {
    auto private_data = reinterpret_cast<ObjectT*>(obj->private_data);
    return private_data->template CGetOptionNumeric<>(key, value, error);
  }

  // Database trampolines
  static AdbcStatusCode CDatabaseInit(AdbcDatabase* database, AdbcError* error) {
    auto private_data = reinterpret_cast<DatabaseT*>(database->private_data);
    private_data->set_driver(database->private_driver);
    return private_data->Init(database->private_driver->private_data, error);
  }

  // Connection trampolines
  static AdbcStatusCode CConnectionInit(AdbcConnection* connection,
                                        AdbcDatabase* database, AdbcError* error) {
    auto private_data = reinterpret_cast<ConnectionT*>(connection->private_data);
    private_data->set_driver(connection->private_driver);
    return private_data->Init(database->private_data, error);
  }

  // Statement trampolines
  static AdbcStatusCode CStatementNew(AdbcConnection* connection,
                                      AdbcStatement* statement, AdbcError* error) {
    auto private_data = new StatementT();
    private_data->set_driver(connection->private_driver);
    AdbcStatusCode status = private_data->Init(connection->private_data, error);
    if (status != ADBC_STATUS_OK) {
      delete private_data;
    }

    statement->private_data = private_data;
    return ADBC_STATUS_OK;
  }

  static AdbcStatusCode CStatementExecuteQuery(AdbcStatement* statement,
                                               struct ArrowArrayStream* stream,
                                               int64_t* rows_affected,
                                               struct AdbcError* error) {
    auto private_data = reinterpret_cast<StatementT*>(statement->private_data);
    return private_data->ExecuteQuery(stream, rows_affected, error);
  }
};

}  // namespace r

}  // namespace adbc
