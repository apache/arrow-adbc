
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
  std::string sql_state_;
  std::vector<std::pair<std::string, std::string>> details_;

  static void CRelease(AdbcError* error) {
    auto error_obj = reinterpret_cast<Error*>(error->private_data);
    delete error_obj;
    error->release = nullptr;
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
  enum Type { TYPE_STRING, TYPE_BYTES, TYPE_INT, TYPE_DOUBLE };

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
  }

  AdbcStatusCode SetOptionBytes(const char* key, const uint8_t* value, size_t length,
                                AdbcError* error) {
    if (!OptionKeySupported(key)) {
      InitErrorOptionNotSupported(key, error);
      return ADBC_STATUS_NOT_IMPLEMENTED;
    }

    std::basic_string<uint8_t> cppvalue(value, length);
    options_[key] = Option(cppvalue);
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
      Error::InitErrorNotFound(key, error);
      return ADBC_STATUS_NOT_FOUND;
    } else {
      AdbcStatusCode status = result->second.get_value(value);
      if (status != ADBC_STATUS_OK) {
        Error::InitErrorWrongType(key, error);
      }

      return status;
    }
  }

  void InitErrorNotFound(const char* key, AdbcError* error) {
    std::stringstream msg_builder;
    msg_builder << "Option not found for key '" << key << "'";
    Error cpperror(msg_builder.str());
    cpperror.AddDetail("adbc.r.requested_key", key);
    cpperror.ToAdbc(error);
  }

  void InitErrorWrongType(const char* key, AdbcError* error) {
    std::stringstream msg_builder;
    msg_builder << "Wrong type requested for option key '" << key << "'";
    Error cpperror(msg_builder.str());
    cpperror.AddDetail("adbc.r.requested_key", key);
    cpperror.ToAdbc(error);
  }

  void InitErrorOptionNotSupported(const char* key, AdbcError* error) {
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
    return private_data->SetOption<>(key, value, error);
  }

  template <typename T, typename PrivateCls>
  static AdbcStatusCode CSetOptionBytes(T* obj, const char* key, const uint8_t* value,
                                        size_t length, AdbcError* error) {
    auto private_data = reinterpret_cast<PrivateCls*>(obj->private_data);
    return obj_private->SetOption<>(key, value, error);
  }

  template <typename T, typename PrivateCls>
  static AdbcStatusCode CSetOptionInt(T* obj, const char* key, int64_t value,
                                      AdbcError* error) {
    auto private_data = reinterpret_cast<PrivateCls*>(obj->private_data);
    return obj_private->SetOption<>(key, value, error);
  }

  template <typename T, typename PrivateCls>
  static AdbcStatusCode CSetOptionDouble(T* obj, const char* key, double value,
                                         AdbcError* error) {
    auto private_data = reinterpret_cast<PrivateCls*>(obj->private_data);
    return obj_private->SetOption<>(key, value, error);
  }

  template <typename T, typename PrivateCls>
  static AdbcStatusCode CGetOption(T* obj, const char* key, char* value, size_t* length,
                                   AdbcError* error) {
    auto private_data = reinterpret_cast<PrivateCls*>(obj->private_data);
    return obj_private->GetOptionStringLike<>(key, value, length, error);
  }

  template <typename T, typename PrivateCls>
  static AdbcStatusCode CGetOptionBytes(T* obj, const char* key, uint8_t* value,
                                        size_t* length, AdbcError* error) {
    auto private_data = reinterpret_cast<PrivateCls*>(obj->private_data);
    return obj_private->GetOptionStringLike<>(key, value, length, error);
  }

  template <typename T, typename PrivateCls>
  static AdbcStatusCode CGetOptionInt(T* obj, const char* key, int64_t* value,
                                      AdbcError* error) {
    auto private_data = reinterpret_cast<PrivateCls*>(obj->private_data);
    return obj_private->GetOptionNumeric<>(key, value, error);
  }

  template <typename T, typename PrivateCls>
  static AdbcStatusCode CGetOptionDouble(T* obj, const char* key, double* value,
                                         AdbcError* error) {
    auto private_data = reinterpret_cast<PrivateCls*>(obj->private_data);
    return obj_private->GetOptionNumeric<>(key, value, error);
  }

  template <typename T, typename PrivateCls>
  static AdbcStatusCode CNew(T* obj, AdbcError* error) {
    auto private_data = new PrivateCls();
    obj->private_data = private_data;
    return ADBC_STATUS_OK;
  }

  template <typename T, typename PrivateClsT>
  static AdbcStatusCode CRelease(T* obj, AdbcError* error) {
    auto private_data = reinterpret_cast<PrivateCls*>(obj->private_data);
    AdbcStatusCode result = private_data->Release(error);
    if (result != ADBC_STATUS_OK) {
      return result;
    }

    delete private_data;
    return ADBC_STATUS_OK;
  }
};

class DatabasePrivateBase : public PrivateBase {
 public:
  virtual AdbcStatusCode Init(AdbcError* error) { return ADBC_STATUS_OK; }

  static AdbcStatusCode CInit(AdbcDatabase* database, AdbcError* error) {
    auto private_data = reinterpret_cast<DatabasePrivateBase*>(database->private_data);
    return private_data->Init(error);
  }
};

template <typename DatabasePrivateT>
class ConnectionPrivateBase : public PrivateBase {
 public:
  virtual AdbcStatusCode Init(DatabasePrivateT* parent, AdbcError* error) {
    return ADBC_STATUS_OK;
  }

  static AdbcStatusCode CInit(AdbcConnection* connection, AdbcDatabase* database,
                              AdbcError* error) {
    auto private_data =
        reinterpret_cast<ConnectionPrivateBase*>(connection->private_data);
    auto database_private = reinterpret_cast<DatabasePrivateT*>(database->private_data);
    return private_data->Init(database_private, error);
  }
};

template <typename ConnectionPrivateT>
class StatementPrivateBase : public PrivateBase {
 public:
  virtual AdbcStatusCode Init(ConnectionPrivateT* parent, AdbcError* error) {
    return ADBC_STATUS_OK;
  }

  static AdbcStatusCode CStatementNew(AdbcConnection* connection,
                                      AdbcStatement* statement, AdbcError* error) {
    auto private_data = new StatementPrivateBase<ConnectionPrivateBase>();
    auto connection_private =
        reinterpret_cast<ConnectionPrivateT*>(connection->private_data);
    AdbcStatusCode status = private_data->Init(connection_private, error);
    if (status != ADBC_STATUS_OK) {
      delete private_data;
    }

    statement->private_data = private_data;
    return ADBC_STATUS_OK;
  }
};

template <typename DatabasePrivateT = DatabasePrivateBase,
          typename ConnectionPrivateT = ConnectionPrivateBase<DatabasePrivateT>,
          typename StatementPrivateT = StatementPrivateBase<ConnectionPrivateT>>
class DriverBase {
 private:
  void CRelease(AdbcDriver* driver, AdbcError* error) {
    auto driver_private = reinterpret_cast<DriverBase*>(driver->private_data);
    delete driver_private;
    driver->private_data = nullptr;
    return ADBC_STATUS_OK;
  }

 public:
  static AdbcStatusCode InitFunc(int version, void* raw_driver, AdbcError* error) {
    if (version != ADBC_VERSION_1_1_0) return ADBC_STATUS_NOT_IMPLEMENTED;
    struct AdbcDriver* driver = (AdbcDriver*)raw_driver;
    std::memset(driver, 0, sizeof(AdbcDriver));

    auto driver_private = new DriverBase();

    std::memset(driver_private, 0, sizeof(struct VoidDriverPrivate));
    driver->private_data = driver_private;

    //   driver->DatabaseInit = &VoidDatabaseInit;
    //   driver->DatabaseNew = VoidDatabaseNew;
    //   driver->DatabaseRelease = VoidDatabaseRelease;
    //   driver->DatabaseSetOption = VoidDatabaseSetOption;

    //   driver->ConnectionCommit = VoidConnectionCommit;
    //   driver->ConnectionGetInfo = VoidConnectionGetInfo;
    //   driver->ConnectionGetObjects = VoidConnectionGetObjects;
    //   driver->ConnectionGetTableSchema = VoidConnectionGetTableSchema;
    //   driver->ConnectionGetTableTypes = VoidConnectionGetTableTypes;
    //   driver->ConnectionInit = VoidConnectionInit;
    //   driver->ConnectionNew = VoidConnectionNew;
    //   driver->ConnectionReadPartition = VoidConnectionReadPartition;
    //   driver->ConnectionRelease = VoidConnectionRelease;
    //   driver->ConnectionRollback = VoidConnectionRollback;
    //   driver->ConnectionSetOption = VoidConnectionSetOption;

    //   driver->StatementBind = VoidStatementBind;
    //   driver->StatementBindStream = VoidStatementBindStream;
    //   driver->StatementExecutePartitions = VoidStatementExecutePartitions;
    //   driver->StatementExecuteQuery = VoidStatementExecuteQuery;
    //   driver->StatementGetParameterSchema = VoidStatementGetParameterSchema;
    //   driver->StatementNew = VoidStatementNew;
    //   driver->StatementPrepare = VoidStatementPrepare;
    //   driver->StatementRelease = VoidStatementRelease;
    //   driver->StatementSetOption = VoidStatementSetOption;
    //   driver->StatementSetSqlQuery = VoidStatementSetSqlQuery;

    driver->release = VoidDriverRelease;

    return ADBC_STATUS_OK;
  }
};

}  // namespace r

}  // namespace adbc
