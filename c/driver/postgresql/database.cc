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

#include "database.h"

#include <array>
#include <charconv>
#include <cinttypes>
#include <cstring>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <arrow-adbc/adbc.h>
#include <libpq-fe.h>
#include <nanoarrow/nanoarrow.h>

#include "driver/common/utils.h"
#include "result_helper.h"

#ifdef ADBC_REDSHIFT_FLAVOR
#include "redshift_auth.h"
#endif  // ADBC_REDSHIFT_FLAVOR

namespace adbcpq {

PostgresDatabase::PostgresDatabase() : open_connections_(0) {
  type_resolver_ = std::make_shared<PostgresTypeResolver>();
}
PostgresDatabase::~PostgresDatabase() = default;

AdbcStatusCode PostgresDatabase::GetOption(const char* option, char* value,
                                           size_t* length, struct AdbcError* error) {
  return ADBC_STATUS_NOT_FOUND;
}
AdbcStatusCode PostgresDatabase::GetOptionBytes(const char* option, uint8_t* value,
                                                size_t* length, struct AdbcError* error) {
  return ADBC_STATUS_NOT_FOUND;
}
AdbcStatusCode PostgresDatabase::GetOptionInt(const char* option, int64_t* value,
                                              struct AdbcError* error) {
  return ADBC_STATUS_NOT_FOUND;
}
AdbcStatusCode PostgresDatabase::GetOptionDouble(const char* option, double* value,
                                                 struct AdbcError* error) {
  return ADBC_STATUS_NOT_FOUND;
}

AdbcStatusCode PostgresDatabase::Init(struct AdbcError* error) {
  // Connect to initialize the version information and build the type table
  PGconn* conn = nullptr;
  RAISE_ADBC(Connect(&conn, error));

  Status status = InitVersions(conn);
  if (!status.ok()) {
    RAISE_ADBC(Disconnect(&conn, nullptr));
    return status.ToAdbc(error);
  }

  status = RebuildTypeResolver(conn);
  RAISE_ADBC(Disconnect(&conn, nullptr));
  return status.ToAdbc(error);
}

AdbcStatusCode PostgresDatabase::Release(struct AdbcError* error) {
  if (open_connections_ != 0) {
    SetError(error, "%s%" PRId32 "%s", "[libpq] Database released with ",
             open_connections_, " open connections");
    return ADBC_STATUS_INVALID_STATE;
  }
  return ADBC_STATUS_OK;
}

AdbcStatusCode PostgresDatabase::SetOption(const char* key, const char* value,
                                           struct AdbcError* error) {
  if (strcmp(key, "uri") == 0) {
    uri_ = value;
    return ADBC_STATUS_OK;
  }
  // parameter based connection
  if (strcmp(key, "user") == 0) {
    params_.user = value;
    return ADBC_STATUS_OK;
  } else if (strcmp(key, "password") == 0) {
    params_.password = value;
    return ADBC_STATUS_OK;
  } else if (strcmp(key, "host") == 0) {
    params_.host = value;
    return ADBC_STATUS_OK;
  } else if (strcmp(key, "port") == 0) {
    params_.port = value;
    return ADBC_STATUS_OK;
  } else if (strcmp(key, "database") == 0) {
    params_.dbname = value;
    return ADBC_STATUS_OK;
  } else if (strcmp(key, "connect_timeout") == 0) {
    params_.connect_timeout = value;
    return ADBC_STATUS_OK;
  } else if (strcmp(key, "application_name") == 0) {
    params_.application_name = value;
    return ADBC_STATUS_OK;
  } else if (strcmp(key, "sslmode") == 0) {
    params_.sslmode = value;
    return ADBC_STATUS_OK;
  } else if (strcmp(key, "sslcert") == 0) {
    params_.sslcert = value;
    return ADBC_STATUS_OK;
  } else if (strcmp(key, "sslkey") == 0) {
    params_.sslkey = value;
    return ADBC_STATUS_OK;
  } else if (strcmp(key, "sslrootcert") == 0) {
    params_.sslrootcert = value;
    return ADBC_STATUS_OK;
  }
#ifdef ADBC_REDSHIFT_FLAVOR
  // IAM Authentication for Redshift
  if (strcmp(key, "auth_profile") == 0) {
    aws_opts.profile = value;
    return ADBC_STATUS_OK;
  } else if (strcmp(key, "cluster_id") == 0) {
    aws_opts.cluster_id = value;
    return ADBC_STATUS_OK;
  } else if (strcmp(key, "region") == 0) {
    aws_opts.region = value;
    return ADBC_STATUS_OK;
  } else if (strcmp(key, "access_key_id") == 0) {
    aws_opts.access_key_id = value;
    return ADBC_STATUS_OK;
  } else if (strcmp(key, "secret_access_key") == 0) {
    aws_opts.secret_access_key = value;
    return ADBC_STATUS_OK;
  }
#endif  // ADBC_REDSHIFT_FLAVOR
  SetError(error, "%s%s", "[libpq] Unknown database option ", key);
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

AdbcStatusCode PostgresDatabase::SetOptionBytes(const char* key, const uint8_t* value,
                                                size_t length, struct AdbcError* error) {
  SetError(error, "%s%s", "[libpq] Unknown option ", key);
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

AdbcStatusCode PostgresDatabase::SetOptionDouble(const char* key, double value,
                                                 struct AdbcError* error) {
  SetError(error, "%s%s", "[libpq] Unknown option ", key);
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

AdbcStatusCode PostgresDatabase::SetOptionInt(const char* key, int64_t value,
                                              struct AdbcError* error) {
  SetError(error, "%s%s", "[libpq] Unknown option ", key);
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

AdbcStatusCode PostgresDatabase::Connect(PGconn** conn, struct AdbcError* error) {
#ifdef ADBC_REDSHIFT_FLAVOR
  // Check for IAM authentication parameters
  bool use_iam_auth = !aws_opts.profile.empty() || (!aws_opts.access_key_id.empty() &&
                                                    !aws_opts.secret_access_key.empty());

  // provided password takes priority
  if (use_iam_auth && !params_.password.has_value()) {
    aws_opts.host = params_.host;
    aws_opts.port = params_.port;
    aws_opts.database = params_.dbname;
    aws_opts.user = params_.user;

    // Get Redshift credentials using IAM authentication
    auto& aws_auth_client = AwsAuthClient::Instance();
    RedshiftCredentials credentials;
    Status status = aws_auth_client.GetRedshiftCredentials(aws_opts, &credentials);
    if (!status.ok()) {
      return status.ToAdbc(error);
    }

    // Hydrate connection params with username and password
    params_.user = credentials.db_user;
    params_.password = credentials.db_password;
  }
#endif  // ADBC_REDSHIFT_FLAVOR

  if (uri_.empty()) {
    std::optional<std::string> param_error = params_.Validate();
    if (param_error.has_value()) {
      SetError(error, "%s%s",
               "[libpq] Must set database option 'uri' or valid connection parameters "
               "before creating a connection. Parameter validation error: ",
               param_error.value().c_str());
      return ADBC_STATUS_INVALID_STATE;
    }
  }

  // uri takes priority
  if (!uri_.empty()) {
    *conn = PQconnectdb(uri_.c_str());
  } else {
    auto [keywords, values] = params_.BuildAllConnectionParams();
    *conn = PQconnectdbParams(keywords.data(), values.data(), 0);
  }
  if (PQstatus(*conn) != CONNECTION_OK) {
    SetError(error, "%s%s", "[libpq] Failed to connect: ", PQerrorMessage(*conn));
    PQfinish(*conn);
    *conn = nullptr;
    return ADBC_STATUS_IO;
  }
  open_connections_++;
  return ADBC_STATUS_OK;
}

AdbcStatusCode PostgresDatabase::Disconnect(PGconn** conn, struct AdbcError* error) {
  PQfinish(*conn);
  *conn = nullptr;
  if (--open_connections_ < 0) {
    SetError(error, "%s", "[libpq] Open connection count underflowed");
    return ADBC_STATUS_INTERNAL;
  }
  return ADBC_STATUS_OK;
}

namespace {

// Parse an individual version in the form of "xxx.xxx.xxx".
// If the version components aren't numeric, they will be zero.
std::array<int, 3> ParseVersion(std::string_view version) {
  std::array<int, 3> out{};
  size_t component = 0;
  size_t component_begin = 0;
  size_t component_end = 0;

  // While there are remaining version components and we haven't reached the end of the
  // string
  while (component_begin < version.size() && component < out.size()) {
    // Find the next character that marks a version component separation or the end of the
    // string
    component_end = version.find_first_of(".-", component_begin);
    if (component_end == version.npos) {
      component_end = version.size();
    }

    // Try to parse the component as an integer (assigning zero if this fails)
    int value = 0;
    std::from_chars(version.data() + component_begin, version.data() + component_end,
                    value);
    out[component] = value;

    // Move on to the next component
    component_begin = component_end + 1;
    component_end = component_begin;
    component++;
  }

  return out;
}

// Parse the PostgreSQL version() string that looks like:
// PostgreSQL 8.0.2 on i686-pc-linux-gnu, compiled by GCC gcc (GCC) 3.4.2 20041017 (Red
// Hat 3.4.2-6.fc3), Redshift 1.0.77467
std::array<int, 3> ParsePrefixedVersion(std::string_view version_info,
                                        std::string_view prefix) {
  size_t pos = version_info.find(prefix);
  if (pos == version_info.npos) {
    return {0, 0, 0};
  }

  // Skip the prefix and any leading whitespace
  pos = version_info.find_first_not_of(' ', pos + prefix.size());
  if (pos == version_info.npos) {
    return {0, 0, 0};
  }

  return ParseVersion(version_info.substr(pos));
}

}  // namespace

Status PostgresDatabase::InitVersions(PGconn* conn) {
  PqResultHelper helper(conn, "SELECT version();");
  UNWRAP_STATUS(helper.Execute());
  if (helper.NumRows() != 1 || helper.NumColumns() != 1) {
    return Status::Internal("Expected 1 row and 1 column for SELECT version(); but got ",
                            helper.NumRows(), "/", helper.NumColumns());
  }

  std::string_view version_info = helper.Row(0)[0].value();
  postgres_server_version_ = ParsePrefixedVersion(version_info, "PostgreSQL");
  redshift_server_version_ = ParsePrefixedVersion(version_info, "Redshift");

  return Status::Ok();
}

std::optional<std::string> ConnectionParams::Validate() const {
  // Check required parameters
  if (host.empty()) {
    return "Missing required parameter: host";
  }

  if (user.empty()) {
    return "Missing required parameter: user";
  }

  if (dbname.empty()) {
    return "Missing required parameter: dbname";
  }

  // Port must be 1-65535
  try {
    int port_num = std::stoi(port);
    if (port_num <= 0 || port_num > 65535) {
      return "Invalid port number: " + port + " (must be 1-65535)";
    }
  } catch (const std::exception&) {
    return "Invalid port number: " + port + " (must be a number)";
  }

  if (!sslmode.empty()) {
    if (sslmode != "disable" && sslmode != "allow" && sslmode != "prefer" &&
        sslmode != "require" && sslmode != "verify-ca" && sslmode != "verify-full") {
      return "Invalid sslmode: " + sslmode +
             " (must be one of: disable, allow, prefer, require, verify-ca, verify-full)";
    }
  }

  if (sslkey.empty() && !sslcert.empty()) {
    return "SSL certificate specified without SSL key";
  } else if (!sslkey.empty() && sslcert.empty()) {
    return "SSL key specified without SSL certificate";
  }

  if (!connect_timeout.empty()) {
    try {
      int timeout = std::stoi(connect_timeout);
      if (timeout < 0) {
        return "connect_timeout must be non-negative, got: " + connect_timeout;
      }
    } catch (const std::exception&) {
      return "Invalid connect_timeout: " + connect_timeout + " (must be a number)";
    }
  }

  return std::nullopt;
}

std::pair<std::vector<const char*>, std::vector<const char*>>
ConnectionParams::BuildAllConnectionParams() const {
  std::vector<const char*> keywords;
  std::vector<const char*> values;

  // Required parameters
  keywords.push_back("host");
  values.push_back(host.c_str());

  keywords.push_back("user");
  values.push_back(user.c_str());

  if (password.has_value()) {
    keywords.push_back("password");
    values.push_back(password->c_str());
  }

  keywords.push_back("port");
  values.push_back(port.c_str());

  keywords.push_back("dbname");
  values.push_back(dbname.c_str());

  // Optional parameters
  if (!connect_timeout.empty()) {
    keywords.push_back("connect_timeout");
    values.push_back(connect_timeout.c_str());
  }
  if (!application_name.empty()) {
    keywords.push_back("application_name");
    values.push_back(application_name.c_str());
  }
  if (!sslmode.empty()) {
    keywords.push_back("sslmode");
    values.push_back(sslmode.c_str());
  }
  if (!sslcert.empty()) {
    keywords.push_back("sslcert");
    values.push_back(sslcert.c_str());
  }
  if (!sslkey.empty()) {
    keywords.push_back("sslkey");
    values.push_back(sslkey.c_str());
  }
  if (!sslrootcert.empty()) {
    keywords.push_back("sslrootcert");
    values.push_back(sslrootcert.c_str());
  }
  for (const auto& [key, value] : custom_params) {
    keywords.push_back(key.c_str());
    values.push_back(value.c_str());
  }
  // Add null terminators
  keywords.push_back(nullptr);
  values.push_back(nullptr);
  return {keywords, values};
}

// Helpers for building the type resolver from queries
static std::string BuildPgTypeQuery(bool has_typarray);

static Status InsertPgAttributeResult(
    const PqResultHelper& result, const std::shared_ptr<PostgresTypeResolver>& resolver);

static Status InsertPgTypeResult(const PqResultHelper& result,
                                 const std::shared_ptr<PostgresTypeResolver>& resolver);

Status PostgresDatabase::RebuildTypeResolver(PGconn* conn) {
  // We need a few queries to build the resolver. The current strategy might
  // fail for some recursive definitions (e.g., arrays of records of arrays).
  // First, one on the pg_attribute table to resolve column names/oids for
  // record types.
  const std::string kColumnsQuery = R"(
SELECT
    attrelid,
    attname,
    atttypid
FROM
    pg_catalog.pg_attribute
ORDER BY
    attrelid, attnum
)";

  // Second, a query of the pg_type table. This query may need a few attempts to handle
  // recursive definitions (e.g., record types with array column). This currently won't
  // handle range types because those rows don't have child OID information. Arrays types
  // are inserted after a successful insert of the element type.
  std::string type_query =
      BuildPgTypeQuery(/*has_typarray*/ redshift_server_version_[0] == 0);

  // Create a new type resolver (this instance's type_resolver_ member
  // will be updated at the end if this succeeds).
  auto resolver = std::make_shared<PostgresTypeResolver>();

  // Insert record type definitions (this includes table schemas)
  PqResultHelper columns(conn, kColumnsQuery.c_str());
  UNWRAP_STATUS(columns.Execute());
  UNWRAP_STATUS(InsertPgAttributeResult(columns, resolver));

  // Attempt filling the resolver a few times to handle recursive definitions.
  int32_t max_attempts = 3;
  PqResultHelper types(conn, type_query);
  for (int32_t i = 0; i < max_attempts; i++) {
    UNWRAP_STATUS(types.Execute());
    UNWRAP_STATUS(InsertPgTypeResult(types, resolver));
  }

  type_resolver_ = std::move(resolver);
  return Status::Ok();
}

static std::string BuildPgTypeQuery(bool has_typarray) {
  std::string maybe_typarray_col;
  std::string maybe_array_recv_filter;
  if (has_typarray) {
    maybe_typarray_col = ", typarray";
    maybe_array_recv_filter = "AND typreceive::TEXT != 'array_recv'";
  }

  return std::string() + "SELECT oid, typname, typreceive, typbasetype, typrelid" +
         maybe_typarray_col + " FROM pg_catalog.pg_type " +
         " WHERE (typreceive != 0 OR typsend != 0) AND typtype != 'r' " +
         maybe_array_recv_filter;
}

static Status InsertPgAttributeResult(
    const PqResultHelper& result, const std::shared_ptr<PostgresTypeResolver>& resolver) {
  int num_rows = result.NumRows();
  std::vector<std::pair<std::string, uint32_t>> columns;
  int64_t current_type_oid = 0;

  if (result.NumColumns() != 3) {
    return Status::Internal(
        "Expected 3 columns from type resolver pg_attribute query but got ",
        result.NumColumns());
  }

  for (int row = 0; row < num_rows; row++) {
    PqResultRow item = result.Row(row);
    UNWRAP_RESULT(int64_t type_oid, item[0].ParseInteger());
    std::string_view col_name = item[1].value();
    UNWRAP_RESULT(int64_t col_oid, item[2].ParseInteger());

    if (type_oid != current_type_oid && !columns.empty()) {
      resolver->InsertClass(static_cast<uint32_t>(current_type_oid), columns);
      columns.clear();
      current_type_oid = type_oid;
    }

    columns.push_back({std::string(col_name), static_cast<uint32_t>(col_oid)});
  }

  if (!columns.empty()) {
    resolver->InsertClass(static_cast<uint32_t>(current_type_oid), columns);
  }

  return Status::Ok();
}

static Status InsertPgTypeResult(const PqResultHelper& result,
                                 const std::shared_ptr<PostgresTypeResolver>& resolver) {
  if (result.NumColumns() != 5 && result.NumColumns() != 6) {
    return Status::Internal(
        "Expected 5 or 6 columns from type resolver pg_type query but got ",
        result.NumColumns());
  }

  int num_rows = result.NumRows();
  int num_cols = result.NumColumns();
  PostgresTypeResolver::Item type_item;

  for (int row = 0; row < num_rows; row++) {
    PqResultRow item = result.Row(row);
    UNWRAP_RESULT(int64_t oid, item[0].ParseInteger());
    const char* typname = item[1].data;
    const char* typreceive = item[2].data;
    UNWRAP_RESULT(int64_t typbasetype, item[3].ParseInteger());
    UNWRAP_RESULT(int64_t typrelid, item[4].ParseInteger());

    int64_t typarray;
    if (num_cols == 6) {
      UNWRAP_RESULT(typarray, item[5].ParseInteger());
    } else {
      typarray = 0;
    }

    // Special case the aclitem because it shows up in a bunch of internal tables
    if (strcmp(typname, "aclitem") == 0) {
      typreceive = "aclitem_recv";
    }

    type_item.oid = static_cast<uint32_t>(oid);
    type_item.typname = typname;
    type_item.typreceive = typreceive;
    type_item.class_oid = static_cast<uint32_t>(typrelid);
    type_item.base_oid = static_cast<uint32_t>(typbasetype);

    int insert_result = resolver->Insert(type_item, nullptr);

    // If there's an array type and the insert succeeded, add that now too
    if (insert_result == NANOARROW_OK && typarray != 0) {
      std::string array_typname = "_" + std::string(typname);
      type_item.oid = static_cast<uint32_t>(typarray);
      type_item.typname = array_typname.c_str();
      type_item.typreceive = "array_recv";
      type_item.child_oid = static_cast<uint32_t>(oid);

      resolver->Insert(type_item, nullptr);
    }
  }

  return Status::Ok();
}

}  // namespace adbcpq
