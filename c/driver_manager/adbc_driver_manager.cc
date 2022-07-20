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

#include "adbc_driver_manager.h"

#include <algorithm>
#include <cstring>
#include <string>
#include <unordered_map>
#include <utility>

#if defined(_WIN32)
#include <windows.h>  // Must come first

#include <libloaderapi.h>
#include <strsafe.h>
#else
#include <dlfcn.h>
#endif  // defined(_WIN32)

namespace {
void ReleaseError(struct AdbcError* error) {
  if (error) {
    delete[] error->message;
    error->message = nullptr;
  }
}

void SetError(struct AdbcError* error, const std::string& message) {
  if (!error) return;
  if (error->message) {
    // Append
    std::string buffer = error->message;
    buffer.reserve(buffer.size() + message.size() + 1);
    buffer += '\n';
    buffer += message;
    error->release(error);

    error->message = new char[buffer.size() + 1];
    buffer.copy(error->message, buffer.size());
    error->message[buffer.size()] = '\0';
  } else {
    error->message = new char[message.size() + 1];
    message.copy(error->message, message.size());
    error->message[message.size()] = '\0';
  }
  error->release = ReleaseError;
}

// Default stubs

AdbcStatusCode ConnectionCommit(struct AdbcConnection*, struct AdbcError* error) {
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

AdbcStatusCode ConnectionGetObjects(struct AdbcConnection*, int, const char*, const char*,
                                    const char*, const char**, const char*,
                                    struct AdbcStatement*, struct AdbcError* error) {
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

AdbcStatusCode ConnectionGetTableSchema(struct AdbcConnection*, const char*, const char*,
                                        const char*, struct ArrowSchema*,
                                        struct AdbcError* error) {
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

AdbcStatusCode ConnectionGetTableTypes(struct AdbcConnection*, struct AdbcStatement*,
                                       struct AdbcError* error) {
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

AdbcStatusCode ConnectionRollback(struct AdbcConnection*, struct AdbcError* error) {
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

AdbcStatusCode ConnectionSetOption(struct AdbcConnection*, const char*, const char*,
                                   struct AdbcError* error) {
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

AdbcStatusCode StatementBind(struct AdbcStatement*, struct ArrowArray*,
                             struct ArrowSchema*, struct AdbcError* error) {
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

AdbcStatusCode StatementExecute(struct AdbcStatement*, struct AdbcError* error) {
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

AdbcStatusCode StatementPrepare(struct AdbcStatement*, struct AdbcError* error) {
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

AdbcStatusCode StatementSetOption(struct AdbcStatement*, const char*, const char*,
                                  struct AdbcError* error) {
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

AdbcStatusCode StatementSetSqlQuery(struct AdbcStatement*, const char*,
                                    struct AdbcError* error) {
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

AdbcStatusCode StatementSetSubstraitPlan(struct AdbcStatement*, const uint8_t*, size_t,
                                         struct AdbcError* error) {
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

/// Temporary state while the database is being configured.
struct TempDatabase {
  std::unordered_map<std::string, std::string> options;
  std::string driver;
  std::string entrypoint;
};

/// Temporary state while the database is being configured.
struct TempConnection {
  std::unordered_map<std::string, std::string> options;
};

#if defined(_WIN32)
/// Append a description of the Windows error to the buffer.
void GetWinError(std::string* buffer) {
  DWORD rc = GetLastError();
  LPVOID message;

  FormatMessage(FORMAT_MESSAGE_ALLOCATE_BUFFER | FORMAT_MESSAGE_FROM_SYSTEM |
                    FORMAT_MESSAGE_IGNORE_INSERTS,
                /*lpSource=*/nullptr, rc, MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT),
                reinterpret_cast<LPSTR>(&message), /*nSize=*/0, /*Arguments=*/nullptr);

  (*buffer) += '(';
  (*buffer) += std::to_string(rc);
  (*buffer) += ") ";
  (*buffer) += reinterpret_cast<char*>(message);
  LocalFree(message);
}

/// Hold the driver DLL and the driver release callback in the driver struct.
struct ManagerDriverState {
  // The loaded DLL
  HMODULE handle;
  // The original release callback
  AdbcStatusCode (*driver_release)(struct AdbcDriver* driver, struct AdbcError* error);
};

/// Unload the driver DLL.
static AdbcStatusCode ReleaseDriver(struct AdbcDriver* driver, struct AdbcError* error) {
  AdbcStatusCode status = ADBC_STATUS_OK;

  if (!driver->private_manager) return status;
  ManagerDriverState* state =
      reinterpret_cast<ManagerDriverState*>(driver->private_manager);

  if (state->driver_release) {
    status = state->driver_release(driver, error);
  }

  if (!FreeLibrary(state->handle)) {
    std::string message = "FreeLibrary() failed: ";
    GetWinError(&message);
    SetError(error, message);
  }

  driver->private_manager = nullptr;
  delete state;
  return status;
}
#endif
}  // namespace

// Direct implementations of API methods

AdbcStatusCode AdbcDatabaseNew(struct AdbcDatabase* database, struct AdbcError* error) {
  // Allocate a temporary structure to store options pre-Init
  database->private_data = new TempDatabase;
  database->private_driver = nullptr;
  return ADBC_STATUS_OK;
}

AdbcStatusCode AdbcDatabaseSetOption(struct AdbcDatabase* database, const char* key,
                                     const char* value, struct AdbcError* error) {
  if (database->private_driver) {
    return database->private_driver->DatabaseSetOption(database, key, value, error);
  }

  TempDatabase* args = reinterpret_cast<TempDatabase*>(database->private_data);
  if (std::strcmp(key, "driver") == 0) {
    args->driver = value;
  } else if (std::strcmp(key, "entrypoint") == 0) {
    args->entrypoint = value;
  } else {
    args->options[key] = value;
  }
  return ADBC_STATUS_OK;
}

AdbcStatusCode AdbcDatabaseInit(struct AdbcDatabase* database, struct AdbcError* error) {
  if (!database->private_data) {
    SetError(error, "Must call AdbcDatabaseNew first");
    return ADBC_STATUS_INVALID_STATE;
  }
  TempDatabase* args = reinterpret_cast<TempDatabase*>(database->private_data);
  if (args->driver.empty()) {
    delete args;
    SetError(error, "Must provide 'driver' parameter");
    return ADBC_STATUS_INVALID_ARGUMENT;
  } else if (args->entrypoint.empty()) {
    delete args;
    SetError(error, "Must provide 'entrypoint' parameter");
    return ADBC_STATUS_INVALID_ARGUMENT;
  }

  database->private_driver = new AdbcDriver;
  size_t initialized = 0;
  AdbcStatusCode status =
      AdbcLoadDriver(args->driver.c_str(), args->entrypoint.c_str(), ADBC_VERSION_0_0_1,
                     database->private_driver, &initialized, error);
  if (status != ADBC_STATUS_OK) {
    delete args;

    if (database->private_driver->release) {
      database->private_driver->release(database->private_driver, error);
    }
    delete database->private_driver;
    return status;
  } else if (initialized < ADBC_VERSION_0_0_1) {
    delete args;

    if (database->private_driver->release) {
      database->private_driver->release(database->private_driver, error);
    }
    delete database->private_driver;

    std::string message = "Database version is too old, expected ";
    message += std::to_string(ADBC_VERSION_0_0_1);
    message += " but got ";
    message += std::to_string(initialized);
    SetError(error, message);
    return status;
  }
  status = database->private_driver->DatabaseNew(database, error);
  if (status != ADBC_STATUS_OK) {
    delete args;

    if (database->private_driver->release) {
      database->private_driver->release(database->private_driver, error);
    }
    delete database->private_driver;
    return status;
  }
  for (const auto& option : args->options) {
    status = database->private_driver->DatabaseSetOption(database, option.first.c_str(),
                                                         option.second.c_str(), error);
    if (status != ADBC_STATUS_OK) {
      delete args;

      if (database->private_driver->release) {
        database->private_driver->release(database->private_driver, error);
      }
      delete database->private_driver;
      return status;
    }
  }
  delete args;
  return database->private_driver->DatabaseInit(database, error);
}

AdbcStatusCode AdbcDatabaseRelease(struct AdbcDatabase* database,
                                   struct AdbcError* error) {
  if (!database->private_driver) {
    if (database->private_data) {
      TempDatabase* args = reinterpret_cast<TempDatabase*>(database->private_data);
      delete args;
      database->private_data = nullptr;
      return ADBC_STATUS_OK;
    }
    return ADBC_STATUS_INVALID_STATE;
  }
  auto status = database->private_driver->DatabaseRelease(database, error);
  if (database->private_driver->release) {
    database->private_driver->release(database->private_driver, error);
  }
  delete database->private_driver;
  database->private_data = nullptr;
  database->private_driver = nullptr;
  return status;
}

AdbcStatusCode AdbcConnectionCommit(struct AdbcConnection* connection,
                                    struct AdbcError* error) {
  if (!connection->private_driver) {
    return ADBC_STATUS_INVALID_STATE;
  }
  return connection->private_driver->ConnectionCommit(connection, error);
}

AdbcStatusCode AdbcConnectionGetObjects(struct AdbcConnection* connection, int depth,
                                        const char* catalog, const char* db_schema,
                                        const char* table_name, const char** table_types,
                                        const char* column_name,
                                        struct AdbcStatement* statement,
                                        struct AdbcError* error) {
  if (!connection->private_driver) {
    return ADBC_STATUS_INVALID_STATE;
  }
  return connection->private_driver->ConnectionGetObjects(
      connection, depth, catalog, db_schema, table_name, table_types, column_name,
      statement, error);
}

AdbcStatusCode AdbcConnectionGetTableSchema(struct AdbcConnection* connection,
                                            const char* catalog, const char* db_schema,
                                            const char* table_name,
                                            struct ArrowSchema* schema,
                                            struct AdbcError* error) {
  if (!connection->private_driver) {
    return ADBC_STATUS_INVALID_STATE;
  }
  return connection->private_driver->ConnectionGetTableSchema(
      connection, catalog, db_schema, table_name, schema, error);
}

AdbcStatusCode AdbcConnectionGetTableTypes(struct AdbcConnection* connection,
                                           struct AdbcStatement* statement,
                                           struct AdbcError* error) {
  if (!connection->private_driver) {
    return ADBC_STATUS_INVALID_STATE;
  }
  return connection->private_driver->ConnectionGetTableTypes(connection, statement,
                                                             error);
}

AdbcStatusCode AdbcConnectionInit(struct AdbcConnection* connection,
                                  struct AdbcDatabase* database,
                                  struct AdbcError* error) {
  if (!connection->private_data) {
    SetError(error, "Must call AdbcConnectionNew first");
    return ADBC_STATUS_INVALID_STATE;
  }
  TempConnection* args = reinterpret_cast<TempConnection*>(connection->private_data);
  std::unordered_map<std::string, std::string> options = std::move(args->options);
  delete args;

  auto status = database->private_driver->ConnectionNew(connection, error);
  if (status != ADBC_STATUS_OK) return status;
  connection->private_driver = database->private_driver;

  for (const auto& option : options) {
    status = database->private_driver->ConnectionSetOption(
        connection, option.first.c_str(), option.second.c_str(), error);
    if (status != ADBC_STATUS_OK) return status;
  }
  return connection->private_driver->ConnectionInit(connection, database, error);
}

AdbcStatusCode AdbcConnectionNew(struct AdbcConnection* connection,
                                 struct AdbcError* error) {
  // Allocate a temporary structure to store options pre-Init
  connection->private_data = new TempConnection;
  connection->private_driver = nullptr;
  return ADBC_STATUS_OK;
}

AdbcStatusCode AdbcConnectionRelease(struct AdbcConnection* connection,
                                     struct AdbcError* error) {
  if (!connection->private_driver) {
    if (connection->private_data) {
      TempConnection* args = reinterpret_cast<TempConnection*>(connection->private_data);
      delete args;
      connection->private_data = nullptr;
      return ADBC_STATUS_OK;
    }
    return ADBC_STATUS_INVALID_STATE;
  }
  auto status = connection->private_driver->ConnectionRelease(connection, error);
  connection->private_driver = nullptr;
  return status;
}

AdbcStatusCode AdbcConnectionRollback(struct AdbcConnection* connection,
                                      struct AdbcError* error) {
  if (!connection->private_driver) {
    return ADBC_STATUS_INVALID_STATE;
  }
  return connection->private_driver->ConnectionRollback(connection, error);
}

AdbcStatusCode AdbcConnectionSetOption(struct AdbcConnection* connection, const char* key,
                                       const char* value, struct AdbcError* error) {
  if (!connection->private_driver) {
    return ADBC_STATUS_INVALID_STATE;
  }
  return connection->private_driver->ConnectionSetOption(connection, key, value, error);
}

AdbcStatusCode AdbcStatementBind(struct AdbcStatement* statement,
                                 struct ArrowArray* values, struct ArrowSchema* schema,
                                 struct AdbcError* error) {
  if (!statement->private_driver) {
    return ADBC_STATUS_INVALID_STATE;
  }
  return statement->private_driver->StatementBind(statement, values, schema, error);
}

AdbcStatusCode AdbcStatementBindStream(struct AdbcStatement* statement,
                                       struct ArrowArrayStream* stream,
                                       struct AdbcError* error) {
  if (!statement->private_driver) {
    return ADBC_STATUS_INVALID_STATE;
  }
  return statement->private_driver->StatementBindStream(statement, stream, error);
}

AdbcStatusCode AdbcStatementExecute(struct AdbcStatement* statement,
                                    struct AdbcError* error) {
  if (!statement->private_driver) {
    return ADBC_STATUS_INVALID_STATE;
  }
  return statement->private_driver->StatementExecute(statement, error);
}

AdbcStatusCode AdbcStatementGetStream(struct AdbcStatement* statement,
                                      struct ArrowArrayStream* out,
                                      struct AdbcError* error) {
  if (!statement->private_driver) {
    return ADBC_STATUS_INVALID_STATE;
  }
  return statement->private_driver->StatementGetStream(statement, out, error);
}

AdbcStatusCode AdbcStatementNew(struct AdbcConnection* connection,
                                struct AdbcStatement* statement,
                                struct AdbcError* error) {
  if (!connection->private_driver) {
    return ADBC_STATUS_INVALID_STATE;
  }
  auto status = connection->private_driver->StatementNew(connection, statement, error);
  statement->private_driver = connection->private_driver;
  return status;
}

AdbcStatusCode AdbcStatementPrepare(struct AdbcStatement* statement,
                                    struct AdbcError* error) {
  if (!statement->private_driver) {
    return ADBC_STATUS_INVALID_STATE;
  }
  return statement->private_driver->StatementPrepare(statement, error);
}

AdbcStatusCode AdbcStatementRelease(struct AdbcStatement* statement,
                                    struct AdbcError* error) {
  if (!statement->private_driver) {
    return ADBC_STATUS_INVALID_STATE;
  }
  auto status = statement->private_driver->StatementRelease(statement, error);
  statement->private_driver = nullptr;
  return status;
}

AdbcStatusCode AdbcStatementSetOption(struct AdbcStatement* statement, const char* key,
                                      const char* value, struct AdbcError* error) {
  if (!statement->private_driver) {
    return ADBC_STATUS_INVALID_STATE;
  }
  return statement->private_driver->StatementSetOption(statement, key, value, error);
}

AdbcStatusCode AdbcStatementSetSqlQuery(struct AdbcStatement* statement,
                                        const char* query, struct AdbcError* error) {
  if (!statement->private_driver) {
    return ADBC_STATUS_INVALID_STATE;
  }
  return statement->private_driver->StatementSetSqlQuery(statement, query, error);
}

AdbcStatusCode AdbcStatementSetSubstraitPlan(struct AdbcStatement* statement,
                                             const uint8_t* plan, size_t length,
                                             struct AdbcError* error) {
  if (!statement->private_driver) {
    return ADBC_STATUS_INVALID_STATE;
  }
  return statement->private_driver->StatementSetSubstraitPlan(statement, plan, length,
                                                              error);
}

const char* AdbcStatusCodeMessage(AdbcStatusCode code) {
#define STRINGIFY(s) #s
#define STRINGIFY_VALUE(s) STRINGIFY(s)
#define CASE(CONSTANT) \
  case CONSTANT:       \
    return #CONSTANT " (" STRINGIFY_VALUE(CONSTANT) ")";

  switch (code) {
    CASE(ADBC_STATUS_OK);
    CASE(ADBC_STATUS_UNKNOWN);
    CASE(ADBC_STATUS_NOT_IMPLEMENTED);
    CASE(ADBC_STATUS_NOT_FOUND);
    CASE(ADBC_STATUS_ALREADY_EXISTS);
    CASE(ADBC_STATUS_INVALID_ARGUMENT);
    CASE(ADBC_STATUS_INVALID_STATE);
    CASE(ADBC_STATUS_INVALID_DATA);
    CASE(ADBC_STATUS_INTEGRITY);
    CASE(ADBC_STATUS_INTERNAL);
    CASE(ADBC_STATUS_IO);
    CASE(ADBC_STATUS_CANCELLED);
    CASE(ADBC_STATUS_TIMEOUT);
    CASE(ADBC_STATUS_UNAUTHENTICATED);
    CASE(ADBC_STATUS_UNAUTHORIZED);
    default:
      return "(invalid code)";
  }
#undef CASE
#undef STRINGIFY_VALUE
#undef STRINGIFY
}

AdbcStatusCode AdbcLoadDriver(const char* driver_name, const char* entrypoint,
                              size_t count, struct AdbcDriver* driver,
                              size_t* initialized, struct AdbcError* error) {
#define FILL_DEFAULT(DRIVER, STUB) \
  if (!DRIVER->STUB) {             \
    DRIVER->STUB = &STUB;          \
  }
#define CHECK_REQUIRED(DRIVER, STUB)                                           \
  if (!DRIVER->STUB) {                                                         \
    SetError(error, "Driver does not implement required function Adbc" #STUB); \
    return ADBC_STATUS_INTERNAL;                                               \
  }

  AdbcDriverInitFunc init_func;
  std::string error_message;

#if defined(_WIN32)

  HMODULE handle = LoadLibraryExA(driver_name, NULL, 0);
  if (!handle) {
    error_message += driver_name;
    error_message += ": LoadLibraryExA() failed: ";
    GetWinError(&error_message);

    std::string full_driver_name = driver_name;
    full_driver_name += ".lib";
    handle = LoadLibraryExA(full_driver_name.c_str(), NULL, 0);
    if (!handle) {
      error_message += '\n';
      error_message += full_driver_name;
      error_message += ": LoadLibraryExA() failed: ";
      GetWinError(&error_message);
    }
  }
  if (!handle) {
    SetError(error, error_message);
    return ADBC_STATUS_INTERNAL;
  }

  void* load_handle = GetProcAddress(handle, entrypoint);
  init_func = reinterpret_cast<AdbcDriverInitFunc>(load_handle);
  if (!init_func) {
    std::string message = "GetProcAddress() failed: ";
    GetWinError(&message);
    if (!FreeLibrary(handle)) {
      message += "\nFreeLibrary() failed: ";
      GetWinError(&message);
    }
    SetError(error, message);
    return ADBC_STATUS_INTERNAL;
  }

#else

#if defined(__APPLE__)
  static const std::string kPlatformLibraryPrefix = "lib";
  static const std::string kPlatformLibrarySuffix = ".dylib";
#else
  static const std::string kPlatformLibraryPrefix = "lib";
  static const std::string kPlatformLibrarySuffix = ".so";
#endif  // defined(__APPLE__)

  void* handle = dlopen(driver_name, RTLD_NOW | RTLD_LOCAL);
  if (!handle) {
    error_message = "dlopen() failed: ";
    error_message += dlerror();

    // If applicable, append the shared library prefix/extension and
    // try again (this way you don't have to hardcode driver names by
    // platform in the application)
    const std::string driver_str = driver_name;

    std::string full_driver_name;
    if (driver_str.size() < kPlatformLibraryPrefix.size() ||
        driver_str.compare(0, kPlatformLibraryPrefix.size(), kPlatformLibraryPrefix) !=
            0) {
      full_driver_name += kPlatformLibraryPrefix;
    }
    full_driver_name += driver_name;
    if (driver_str.size() < kPlatformLibrarySuffix.size() ||
        driver_str.compare(full_driver_name.size() - kPlatformLibrarySuffix.size(),
                           kPlatformLibrarySuffix.size(), kPlatformLibrarySuffix) != 0) {
      full_driver_name += kPlatformLibrarySuffix;
    }
    handle = dlopen(full_driver_name.c_str(), RTLD_NOW | RTLD_LOCAL);
    if (!handle) {
      error_message += "\ndlopen() failed: ";
      error_message += dlerror();
    }
  }
  if (!handle) {
    SetError(error, error_message);
    return ADBC_STATUS_INTERNAL;
  }

  void* load_handle = dlsym(handle, entrypoint);
  init_func = reinterpret_cast<AdbcDriverInitFunc>(load_handle);
  if (!init_func) {
    std::string message = "dlsym() failed: ";
    message += dlerror();
    SetError(error, message);
    return ADBC_STATUS_INTERNAL;
  }

#endif  // defined(_WIN32)

  auto result = init_func(count, driver, initialized, error);
#if defined(_WIN32)
  driver->private_manager = new ManagerDriverState{handle, driver->release};
  driver->release = &ReleaseDriver;
#endif  // defined(_WIN32)
  if (result != ADBC_STATUS_OK) {
    return result;
  }

  CHECK_REQUIRED(driver, DatabaseNew);
  CHECK_REQUIRED(driver, DatabaseInit);
  CHECK_REQUIRED(driver, DatabaseRelease);

  CHECK_REQUIRED(driver, ConnectionNew);
  CHECK_REQUIRED(driver, ConnectionInit);
  CHECK_REQUIRED(driver, ConnectionRelease);
  FILL_DEFAULT(driver, ConnectionCommit);
  FILL_DEFAULT(driver, ConnectionGetObjects);
  FILL_DEFAULT(driver, ConnectionGetTableSchema);
  FILL_DEFAULT(driver, ConnectionGetTableTypes);
  FILL_DEFAULT(driver, ConnectionRollback);
  FILL_DEFAULT(driver, ConnectionSetOption);

  CHECK_REQUIRED(driver, StatementNew);
  CHECK_REQUIRED(driver, StatementRelease);
  FILL_DEFAULT(driver, StatementBind);
  FILL_DEFAULT(driver, StatementExecute);
  FILL_DEFAULT(driver, StatementPrepare);
  FILL_DEFAULT(driver, StatementSetOption);
  FILL_DEFAULT(driver, StatementSetSqlQuery);
  FILL_DEFAULT(driver, StatementSetSubstraitPlan);

  return ADBC_STATUS_OK;

#undef FILL_DEFAULT
#undef CHECK_REQUIRED
}
