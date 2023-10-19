
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
  Error(const std::string& message) : message_(message), sql_state_("\0\0\0\0\0") {}

  Error(const char* message) : Error(std::string(message)) {}

  Error(const std::string& message,
        const std::vector<std::pair<std::string, std::string>>& details)
      : message_(message), details_(details), sql_state_("\0\0\0\0\0") {}

  void AddDetail(const std::string& key, const std::string& value) {
    details_.push_back({key, value});
  }

  int DetailCount() const { return details_.size(); }

  AdbcErrorDetail Detail(int index) {
    const auto detail = details_[index];
    return {detail.first.c_str(), reinterpret_cast<const uint8_t*>(detail.second.data()),
            detail.second.size()};
  }

  void ToAdbc(AdbcError* adbc_error) {
    auto error_owned_by_adbc_error = new Error(message_, details_);
    adbc_error->message = const_cast<char*>(error_owned_by_adbc_error->message_.c_str());
    adbc_error->private_data = error_owned_by_adbc_error;
    adbc_error->private_driver = nullptr;
    adbc_error->vendor_code = ADBC_ERROR_VENDOR_CODE_PRIVATE_DATA;
    for (size_t i = 0; i < 5; i++) {
      adbc_error->sqlstate[i] = error_owned_by_adbc_error->sql_state_[i];
    }

    adbc_error->release = &CRelease;
  }

 private:
  std::string message_;
  std::vector<std::pair<std::string, std::string>> details_;
  std::string sql_state_;

  static void CRelease(AdbcError* error) {
    auto error_obj = reinterpret_cast<Error*>(error->private_data);
    delete error_obj;
    std::memset(error, 0, sizeof(AdbcError));
  }

 public:
  static int CGetDetailCount(const AdbcError* error) {
    if (error->vendor_code != ADBC_ERROR_VENDOR_CODE_PRIVATE_DATA) {
      return 0;
    }

    auto error_obj = reinterpret_cast<Error*>(error->private_data);
    return error_obj->DetailCount();
  }

  static AdbcErrorDetail CGetDetail(const AdbcError* error, int index) {
    auto error_obj = reinterpret_cast<Error*>(error->private_data);
    return error_obj->Detail(index);
  }
};

class Option {
 public:
  enum Type { TYPE_MISSING, TYPE_STRING, TYPE_BYTES, TYPE_INT, TYPE_DOUBLE };

  Option() : type_(TYPE_MISSING) {}
  Option(const std::string& value) : type_(TYPE_STRING), value_string_(value) {}
  Option(const std::basic_string<uint8_t>& value)
      : type_(TYPE_BYTES), value_bytes_(value) {}
  Option(double value) : type_(TYPE_DOUBLE), value_double_(value) {}
  Option(int64_t value) : type_(TYPE_INT), value_int_(value) {}

  AdbcStatusCode get_value(char* out, size_t* length) const {
    switch (type_) {
      case TYPE_STRING:
        if (*length < value_string_.size()) {
          *length = value_string_.size();
        } else {
          memcpy(out, value_string_.data(), value_string_.size());
        }

        return ADBC_STATUS_OK;

      default:
        return ADBC_STATUS_NOT_FOUND;
    }
  }

  AdbcStatusCode get_value(uint8_t* out, size_t* length) const {
    switch (type_) {
      case TYPE_BYTES:
        if (*length < value_bytes_.size()) {
          *length = value_bytes_.size();
        } else {
          memcpy(out, value_bytes_.data(), value_bytes_.size());
        }

        return ADBC_STATUS_OK;

      default:
        return ADBC_STATUS_NOT_FOUND;
    }
  }

  AdbcStatusCode get_value(int64_t* value) const {
    switch (type_) {
      case TYPE_INT:
        return value_int_;
      default:
        return ADBC_STATUS_NOT_FOUND;
    }
  }

  AdbcStatusCode get_value(double* value) const {
    switch (type_) {
      case TYPE_DOUBLE:
        return value_double_;
      default:
        return ADBC_STATUS_NOT_FOUND;
    }
  }

 private:
  Type type_;
  std::string value_string_;
  std::basic_string<uint8_t> value_bytes_;
  double value_double_;
  int64_t value_int_;
};

// Base class for private_data of AdbcDatabase, AdbcConnection, and AdbcStatement
// This class handles option setting and getting.
class PrivateBase {
 public:
  virtual ~PrivateBase() {}

  virtual bool OptionKeySupported(const char* key) const { return true; }

  virtual AdbcStatusCode Init(void* parent, AdbcError* error) { return ADBC_STATUS_OK; }

  virtual AdbcStatusCode Release(AdbcError* error) { return ADBC_STATUS_OK; }

 protected:
  std::unordered_map<std::string, Option> options_;

 private:
  template <typename T>
  AdbcStatusCode SetOption(const char* key, T value, AdbcError* error) {
    if (!OptionKeySupported(key)) {
      InitErrorOptionNotSupported(key, error);
      return ADBC_STATUS_NOT_IMPLEMENTED;
    }

    options_[key] = Option(value);
    return ADBC_STATUS_OK;
  }

  AdbcStatusCode SetOptionBytes(const char* key, const uint8_t* value, size_t length,
                                AdbcError* error) {
    if (!OptionKeySupported(key)) {
      InitErrorOptionNotSupported(key, error);
      return ADBC_STATUS_NOT_IMPLEMENTED;
    }

    std::basic_string<uint8_t> cppvalue(value, length);
    options_[key] = Option(cppvalue);
    return ADBC_STATUS_OK;
  }

  template <typename T>
  AdbcStatusCode GetOptionStringLike(const char* key, T* value, size_t* length,
                                     AdbcError* error) const {
    auto result = options_.find(key);
    if (result == options_.end()) {
      InitErrorNotFound(key, error);
      return ADBC_STATUS_NOT_FOUND;
    } else {
      AdbcStatusCode status = result->second.get_value(value, length);
      if (status != ADBC_STATUS_OK) {
        InitErrorWrongType(key, error);
      }

      return status;
    }
  }

  template <typename T>
  AdbcStatusCode GetOptionNumeric(const char* key, T* value, AdbcError* error) const {
    auto result = options_.find(key);
    if (result == options_.end()) {
      InitErrorNotFound(key, error);
      return ADBC_STATUS_NOT_FOUND;
    } else {
      AdbcStatusCode status = result->second.get_value(value);
      if (status != ADBC_STATUS_OK) {
        InitErrorWrongType(key, error);
      }

      return status;
    }
  }

  static void InitErrorNotFound(const char* key, AdbcError* error) {
    std::stringstream msg_builder;
    msg_builder << "Option not found for key '" << key << "'";
    Error cpperror(msg_builder.str());
    cpperror.AddDetail("adbc.r.requested_key", key);
    cpperror.ToAdbc(error);
  }

  static void InitErrorWrongType(const char* key, AdbcError* error) {
    std::stringstream msg_builder;
    msg_builder << "Wrong type requested for option key '" << key << "'";
    Error cpperror(msg_builder.str());
    cpperror.AddDetail("adbc.r.requested_key", key);
    cpperror.ToAdbc(error);
  }

  static void InitErrorOptionNotSupported(const char* key, AdbcError* error) {
    std::stringstream msg_builder;
    msg_builder << "Option '" << key << "' is not supported";
    Error cpperror(msg_builder.str());
    cpperror.AddDetail("adbc.r.requested_key", key);
    cpperror.ToAdbc(error);
  }

 public:
  template <typename T, typename PrivateCls>
  static AdbcStatusCode CSetOption(T* obj, const char* key, const char* value,
                                   AdbcError* error) {
    auto private_data = reinterpret_cast<PrivateCls*>(obj->private_data);
    return private_data->template SetOption<>(key, value, error);
  }

  template <typename T, typename PrivateCls>
  static AdbcStatusCode CSetOptionBytes(T* obj, const char* key, const uint8_t* value,
                                        size_t length, AdbcError* error) {
    auto private_data = reinterpret_cast<PrivateCls*>(obj->private_data);
    return private_data->template SetOption<>(key, value, error);
  }

  template <typename T, typename PrivateCls>
  static AdbcStatusCode CSetOptionInt(T* obj, const char* key, int64_t value,
                                      AdbcError* error) {
    auto private_data = reinterpret_cast<PrivateCls*>(obj->private_data);
    return private_data->template SetOption<>(key, value, error);
  }

  template <typename T, typename PrivateCls>
  static AdbcStatusCode CSetOptionDouble(T* obj, const char* key, double value,
                                         AdbcError* error) {
    auto private_data = reinterpret_cast<PrivateCls*>(obj->private_data);
    return private_data->template SetOption<>(key, value, error);
  }

  template <typename T, typename PrivateCls>
  static AdbcStatusCode CGetOption(T* obj, const char* key, char* value, size_t* length,
                                   AdbcError* error) {
    auto private_data = reinterpret_cast<PrivateCls*>(obj->private_data);
    return private_data->template GetOptionStringLike<>(key, value, length, error);
  }

  template <typename T, typename PrivateCls>
  static AdbcStatusCode CGetOptionBytes(T* obj, const char* key, uint8_t* value,
                                        size_t* length, AdbcError* error) {
    auto private_data = reinterpret_cast<PrivateCls*>(obj->private_data);
    return private_data->template GetOptionStringLike<>(key, value, length, error);
  }

  template <typename T, typename PrivateCls>
  static AdbcStatusCode CGetOptionInt(T* obj, const char* key, int64_t* value,
                                      AdbcError* error) {
    auto private_data = reinterpret_cast<PrivateCls*>(obj->private_data);
    return private_data->template GetOptionNumeric<>(key, value, error);
  }

  template <typename T, typename PrivateCls>
  static AdbcStatusCode CGetOptionDouble(T* obj, const char* key, double* value,
                                         AdbcError* error) {
    auto private_data = reinterpret_cast<PrivateCls*>(obj->private_data);
    return private_data->template GetOptionNumeric<>(key, value, error);
  }

  template <typename T, typename PrivateCls>
  static AdbcStatusCode CNew(T* obj, AdbcError* error) {
    auto private_data = new PrivateCls();
    obj->private_data = private_data;
    return ADBC_STATUS_OK;
  }

  template <typename T, typename PrivateCls>
  static AdbcStatusCode CRelease(T* obj, AdbcError* error) {
    auto private_data = reinterpret_cast<PrivateCls*>(obj->private_data);
    AdbcStatusCode result = private_data->Release(error);
    if (result != ADBC_STATUS_OK) {
      return result;
    }

    delete private_data;
    obj->private_data = nullptr;
    return ADBC_STATUS_OK;
  }
};

class DatabasePrivateBase : public PrivateBase {
 public:
  template <typename PrivateCls>
  static AdbcStatusCode CInit(AdbcDatabase* database, AdbcError* error) {
    auto private_data = reinterpret_cast<PrivateCls*>(database->private_data);
    return private_data->Init(nullptr, error);
  }
};

class ConnectionPrivateBase : public PrivateBase {
 public:
  template <typename PrivateCls>
  static AdbcStatusCode CInit(AdbcConnection* connection, AdbcDatabase* database,
                              AdbcError* error) {
    auto private_data = reinterpret_cast<PrivateCls*>(connection->private_data);
    return private_data->Init(database->private_data, error);
  }
};

class StatementPrivateBase : public PrivateBase {
 public:
  virtual AdbcStatusCode ExecuteQuery(struct ArrowArrayStream* stream,
                                      int64_t* rows_affected, struct AdbcError* error) {
    return ADBC_STATUS_NOT_IMPLEMENTED;
  }

  template <typename PrivateCls>
  static AdbcStatusCode CStatementNew(AdbcConnection* connection,
                                      AdbcStatement* statement, AdbcError* error) {
    auto private_data = new PrivateCls();
    AdbcStatusCode status = private_data->Init(connection->private_data, error);
    if (status != ADBC_STATUS_OK) {
      delete private_data;
    }

    statement->private_data = private_data;
    return ADBC_STATUS_OK;
  }

  template <typename PrivateCls>
  static AdbcStatusCode CExecuteQuery(AdbcStatement* statement,
                                      struct ArrowArrayStream* stream,
                                      int64_t* rows_affected, struct AdbcError* error) {
    auto private_data = reinterpret_cast<PrivateCls*>(statement->private_data);
    return private_data->ExecuteQuery(stream, rows_affected, error);
  }
};

template <typename StatementPrivateT = StatementPrivateBase,
          typename ConnectionPrivateT = ConnectionPrivateBase,
          typename DatabasePrivateT = DatabasePrivateBase>
class DriverBase {
 private:
  static AdbcStatusCode CRelease(AdbcDriver* driver, AdbcError* error) {
    auto driver_private = reinterpret_cast<DriverBase*>(driver->private_data);
    delete driver_private;
    driver->private_data = nullptr;
    return ADBC_STATUS_OK;
  }

 public:
  static AdbcStatusCode Init(int version, void* raw_driver, AdbcError* error) {
    if (version != ADBC_VERSION_1_1_0) return ADBC_STATUS_NOT_IMPLEMENTED;
    struct AdbcDriver* driver = (AdbcDriver*)raw_driver;
    std::memset(driver, 0, sizeof(AdbcDriver));

    driver->private_data = new DriverBase();

    driver->DatabaseNew = &PrivateBase::CNew<AdbcDatabase, DatabasePrivateT>;
    driver->DatabaseInit = &DatabasePrivateBase::CInit<DatabasePrivateT>;
    driver->DatabaseRelease = &PrivateBase::CRelease<AdbcDatabase, DatabasePrivateT>;

    driver->DatabaseSetOption = &PrivateBase::CSetOption<AdbcDatabase, DatabasePrivateT>;

    driver->ConnectionNew = &PrivateBase::CNew<AdbcConnection, ConnectionPrivateT>;
    driver->ConnectionInit = &ConnectionPrivateBase::CInit<ConnectionPrivateT>;
    driver->ConnectionRelease =
        &PrivateBase::CRelease<AdbcConnection, ConnectionPrivateT>;

    driver->ConnectionSetOption =
        &PrivateBase::CSetOption<AdbcConnection, ConnectionPrivateT>;

    //   driver->ConnectionCommit = VoidConnectionCommit;
    //   driver->ConnectionGetInfo = VoidConnectionGetInfo;
    //   driver->ConnectionGetObjects = VoidConnectionGetObjects;
    //   driver->ConnectionGetTableSchema = VoidConnectionGetTableSchema;
    //   driver->ConnectionGetTableTypes = VoidConnectionGetTableTypes;

    //   driver->ConnectionReadPartition = VoidConnectionReadPartition;

    //   driver->ConnectionRollback = VoidConnectionRollback;
    //   driver->ConnectionSetOption = VoidConnectionSetOption;

    driver->StatementNew = &StatementPrivateBase::CStatementNew<StatementPrivateT>;
    driver->StatementRelease = &PrivateBase::CRelease<AdbcStatement, StatementPrivateT>;

    driver->StatementSetOption =
        &PrivateBase::CSetOption<AdbcStatement, StatementPrivateT>;
    driver->StatementExecuteQuery =
        &StatementPrivateBase::CExecuteQuery<StatementPrivateT>;

    //   driver->StatementBind = VoidStatementBind;
    //   driver->StatementBindStream = VoidStatementBindStream;
    //   driver->StatementExecutePartitions = VoidStatementExecutePartitions;
    //   driver->StatementExecuteQuery = VoidStatementExecuteQuery;
    //   driver->StatementGetParameterSchema = VoidStatementGetParameterSchema;
    //   driver->StatementPrepare = VoidStatementPrepare;
    //   driver->StatementSetSqlQuery = VoidStatementSetSqlQuery;

    driver->release = &DriverBase::CRelease;

    return ADBC_STATUS_OK;
  }
};

}  // namespace r

}  // namespace adbc
