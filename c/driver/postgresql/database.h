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

#include <array>
#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include <arrow-adbc/adbc.h>
#include <libpq-fe.h>
#include <postgresql/redshift_auth.h>

#include "driver/framework/status.h"
#include "postgres_type.h"

namespace adbcpq {
using adbc::driver::Status;

struct ConnectionParams {
  // Validate connection parameters and return error message if invalid
  std::optional<std::string> Validate() const;

  // Convert to PQconnectdbParams format including custom parameters
  std::pair<std::vector<const char*>, std::vector<const char*>> BuildAllConnectionParams()
      const;

  // See: https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING
  std::string host;
  std::string user;
  std::optional<std::string> password;
  std::string port = "5432";
  std::string dbname;

  // Optional parameters
  std::string connect_timeout;
  std::string application_name;
  std::string sslmode;
  std::string sslcert;
  std::string sslkey;
  std::string sslrootcert;

  // Custom parameters
  std::unordered_map<std::string, std::string> custom_params;
  // Add more as needed
};

class PostgresDatabase {
 public:
  PostgresDatabase();
  ~PostgresDatabase();

  // Public ADBC API

  AdbcStatusCode Init(struct AdbcError* error);
  AdbcStatusCode Release(struct AdbcError* error);
  AdbcStatusCode GetOption(const char* option, char* value, size_t* length,
                           struct AdbcError* error);
  AdbcStatusCode GetOptionBytes(const char* option, uint8_t* value, size_t* length,
                                struct AdbcError* error);
  AdbcStatusCode GetOptionDouble(const char* option, double* value,
                                 struct AdbcError* error);
  AdbcStatusCode GetOptionInt(const char* option, int64_t* value,
                              struct AdbcError* error);
  AdbcStatusCode SetOption(const char* key, const char* value, struct AdbcError* error);
  AdbcStatusCode SetOptionBytes(const char* key, const uint8_t* value, size_t length,
                                struct AdbcError* error);
  AdbcStatusCode SetOptionDouble(const char* key, double value, struct AdbcError* error);
  AdbcStatusCode SetOptionInt(const char* key, int64_t value, struct AdbcError* error);

  // Internal implementation

  AdbcStatusCode Connect(PGconn** conn, struct AdbcError* error);
  AdbcStatusCode Disconnect(PGconn** conn, struct AdbcError* error);
  const std::shared_ptr<PostgresTypeResolver>& type_resolver() const {
    return type_resolver_;
  }

  Status InitVersions(PGconn* conn);
  Status RebuildTypeResolver(PGconn* conn);
  std::string_view VendorName() {
    if (redshift_server_version_[0] != 0) {
      return "Redshift";
    } else {
      return "PostgreSQL";
    }
  }
  const std::array<int, 3>& VendorVersion() {
    if (redshift_server_version_[0] != 0) {
      return redshift_server_version_;
    } else {
      return postgres_server_version_;
    }
  }

 private:
  int32_t open_connections_;
  std::string uri_;
  ConnectionParams params_;
#ifdef ADBC_REDSHIFT_FLAVOR
  AwsAuthOptions aws_opts;
#endif
  std::shared_ptr<PostgresTypeResolver> type_resolver_;
  std::array<int, 3> postgres_server_version_{};
  std::array<int, 3> redshift_server_version_{};
};
}  // namespace adbcpq

extern "C" {
/// For applications that want to use the driver struct directly, this gives
/// them access to the Init routine.
ADBC_EXPORT
AdbcStatusCode PostgresqlDriverInit(int, void*, struct AdbcError*);
}
