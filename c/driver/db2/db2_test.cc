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
#include <string>

#include <arrow-adbc/adbc.h>
#include <arrow-adbc/driver/db2.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "validation/adbc_validation.h"
#include "validation/adbc_validation_util.h"

using adbc_validation::IsOkStatus;
using adbc_validation::IsStatus;

namespace {

// Read the connection URI from the environment.  When unset, all
// tests that need a live Db2 server skip.
const char* GetTestUri() {
  const char* uri = std::getenv("ADBC_DB2_TEST_URI");
  return (uri != nullptr && uri[0] != '\0') ? uri : nullptr;
}

class Db2Quirks : public adbc_validation::DriverQuirks {
 public:
  AdbcStatusCode SetupDatabase(struct AdbcDatabase* database,
                               struct AdbcError* error) const override {
    const char* uri = GetTestUri();
    if (uri == nullptr) return ADBC_STATUS_INVALID_STATE;
    return AdbcDatabaseSetOption(database, "uri", uri, error);
  }

  // The driver currently scopes itself to connection management; the
  // following capabilities are intentionally disabled until follow-up
  // PRs add execution, transactions, ingestion, and metadata.
  bool supports_bulk_ingest(const char* /*mode*/) const override { return false; }
  bool supports_concurrent_statements() const override { return false; }
  bool supports_transactions() const override { return false; }
  bool supports_get_sql_info() const override { return false; }
  bool supports_get_objects() const override { return false; }
  bool supports_metadata_current_catalog() const override { return false; }
  bool supports_metadata_current_db_schema() const override { return false; }
  bool supports_partitioned_data() const override { return false; }
  bool supports_dynamic_parameter_binding() const override { return false; }
  bool supports_error_on_incompatible_schema() const override { return false; }
  bool supports_ingest_view_types() const override { return false; }
  bool supports_ingest_float16() const override { return false; }
};

}  // namespace

// ---------------------------------------------------------------------------
// AdbcDatabase lifecycle (matches the standard ADBCV_TEST_DATABASE coverage)
// ---------------------------------------------------------------------------

class Db2DatabaseTest : public ::testing::Test, public adbc_validation::DatabaseTest {
 public:
  const adbc_validation::DriverQuirks* quirks() const override { return &quirks_; }
  void SetUp() override {
    if (GetTestUri() == nullptr) {
      GTEST_SKIP() << "ADBC_DB2_TEST_URI not set";
    }
    ASSERT_NO_FATAL_FAILURE(SetUpTest());
  }
  void TearDown() override {
    if (GetTestUri() != nullptr) ASSERT_NO_FATAL_FAILURE(TearDownTest());
  }

 protected:
  Db2Quirks quirks_;
};
ADBCV_TEST_DATABASE(Db2DatabaseTest)

// ---------------------------------------------------------------------------
// AdbcConnection lifecycle
//
// We deliberately do not invoke ADBCV_TEST_CONNECTION here because the bulk
// of those tests exercise metadata APIs (GetObjects/GetTableSchema/...) and
// transactions that are out of scope for this initial driver.  We instead
// reuse the framework's connection-flow helpers directly so behavior stays
// in lock-step with the other ADBC drivers.
// ---------------------------------------------------------------------------

class Db2ConnectionFlowTest : public ::testing::Test,
                              public adbc_validation::ConnectionTest {
 public:
  const adbc_validation::DriverQuirks* quirks() const override { return &quirks_; }
  void SetUp() override {
    if (GetTestUri() == nullptr) {
      GTEST_SKIP() << "ADBC_DB2_TEST_URI not set";
    }
    ASSERT_NO_FATAL_FAILURE(SetUpTest());
  }
  void TearDown() override {
    if (GetTestUri() != nullptr) ASSERT_NO_FATAL_FAILURE(TearDownTest());
  }

 protected:
  Db2Quirks quirks_;
};

TEST_F(Db2ConnectionFlowTest, NewInit) { TestNewInit(); }
TEST_F(Db2ConnectionFlowTest, Release) { TestRelease(); }
TEST_F(Db2ConnectionFlowTest, Concurrent) { TestConcurrent(); }
TEST_F(Db2ConnectionFlowTest, AutocommitDefault) { TestAutocommitDefault(); }

// ---------------------------------------------------------------------------
// Db2-specific connection-management tests (option parsing, error mapping)
// ---------------------------------------------------------------------------

class Db2ConnectionOptionsTest : public ::testing::Test {
 protected:
  void SetUp() override {
    if (GetTestUri() == nullptr) {
      GTEST_SKIP() << "ADBC_DB2_TEST_URI not set";
    }
    std::memset(&error_, 0, sizeof(error_));
    std::memset(&database_, 0, sizeof(database_));
  }

  void TearDown() override {
    if (database_.private_data != nullptr) {
      AdbcDatabaseRelease(&database_, &error_);
    }
    if (error_.release) error_.release(&error_);
  }

  struct AdbcError error_;
  struct AdbcDatabase database_;
};

TEST_F(Db2ConnectionOptionsTest, MissingConnectionStringFailsCleanly) {
  ASSERT_THAT(AdbcDatabaseNew(&database_, &error_), IsOkStatus(&error_));
  // No "uri" and no per-field options have been set.
  ASSERT_THAT(AdbcDatabaseInit(&database_, &error_),
              IsStatus(ADBC_STATUS_INVALID_ARGUMENT, &error_));
}

TEST_F(Db2ConnectionOptionsTest, UnknownOptionIsRejected) {
  ASSERT_THAT(AdbcDatabaseNew(&database_, &error_), IsOkStatus(&error_));
  // Unknown option must surface as NOT_IMPLEMENTED for forward compat,
  // matching the ADBC framework default.
  ASSERT_THAT(
      AdbcDatabaseSetOption(&database_, "adbc.db2.does_not_exist", "x", &error_),
      IsStatus(ADBC_STATUS_NOT_IMPLEMENTED, &error_));
}

TEST_F(Db2ConnectionOptionsTest, InvalidCredentialsAreRejected) {
  // Reuse the connection coordinates from ADBC_DB2_TEST_URI but
  // substitute an obviously-invalid password.  The connect must fail
  // with an authentication-class status (Db2 SQLSTATE 28xxx maps to
  // UNAUTHENTICATED; some installs report 08001, which maps to IO).
  const std::string uri = GetTestUri();
  auto extract = [&](const std::string& key) -> std::string {
    std::string needle = key + "=";
    auto pos = uri.find(needle);
    if (pos == std::string::npos) return "";
    auto end = uri.find(';', pos);
    return uri.substr(pos + needle.size(),
                      (end == std::string::npos) ? std::string::npos
                                                 : end - pos - needle.size());
  };
  const std::string database = extract("DATABASE");
  const std::string hostname = extract("HOSTNAME");
  const std::string port = extract("PORT");
  const std::string uid = extract("UID");
  if (database.empty() || hostname.empty() || port.empty() || uid.empty()) {
    GTEST_SKIP() << "ADBC_DB2_TEST_URI does not expose all of "
                    "DATABASE/HOSTNAME/PORT/UID; skipping credential test";
  }

  ASSERT_THAT(AdbcDatabaseNew(&database_, &error_), IsOkStatus(&error_));
  ASSERT_THAT(AdbcDatabaseSetOption(&database_, ADBC_DB2_OPTION_DATABASE,
                                    database.c_str(), &error_),
              IsOkStatus(&error_));
  ASSERT_THAT(AdbcDatabaseSetOption(&database_, ADBC_DB2_OPTION_HOSTNAME,
                                    hostname.c_str(), &error_),
              IsOkStatus(&error_));
  ASSERT_THAT(AdbcDatabaseSetOption(&database_, ADBC_DB2_OPTION_PORT, port.c_str(),
                                    &error_),
              IsOkStatus(&error_));
  ASSERT_THAT(AdbcDatabaseSetOption(&database_, ADBC_DB2_OPTION_UID, uid.c_str(),
                                    &error_),
              IsOkStatus(&error_));
  ASSERT_THAT(AdbcDatabaseSetOption(&database_, ADBC_DB2_OPTION_PWD,
                                    "definitely-not-the-password", &error_),
              IsOkStatus(&error_));
  ASSERT_THAT(AdbcDatabaseInit(&database_, &error_), IsOkStatus(&error_));

  struct AdbcConnection connection;
  std::memset(&connection, 0, sizeof(connection));
  ASSERT_THAT(AdbcConnectionNew(&connection, &error_), IsOkStatus(&error_));
  AdbcStatusCode rc = AdbcConnectionInit(&connection, &database_, &error_);
  EXPECT_THAT(rc, ::testing::AnyOf(::testing::Eq(ADBC_STATUS_UNAUTHENTICATED),
                                   ::testing::Eq(ADBC_STATUS_IO)));
  AdbcConnectionRelease(&connection, &error_);
}

TEST_F(Db2ConnectionOptionsTest, IndividualOptionsBuildConnectionString) {
  // Verify that the per-field options compose into a usable connection
  // string by parsing the values out of the test URI.
  const std::string uri = GetTestUri();
  auto extract = [&](const std::string& key) -> std::string {
    std::string needle = key + "=";
    auto pos = uri.find(needle);
    if (pos == std::string::npos) return "";
    auto end = uri.find(';', pos);
    return uri.substr(pos + needle.size(),
                      (end == std::string::npos) ? std::string::npos
                                                 : end - pos - needle.size());
  };
  const std::string database = extract("DATABASE");
  const std::string hostname = extract("HOSTNAME");
  const std::string port = extract("PORT");
  const std::string uid = extract("UID");
  const std::string pwd = extract("PWD");
  if (database.empty() || hostname.empty() || port.empty() || uid.empty() ||
      pwd.empty()) {
    GTEST_SKIP() << "ADBC_DB2_TEST_URI does not contain all of "
                    "DATABASE/HOSTNAME/PORT/UID/PWD; skipping field-style test";
  }

  ASSERT_THAT(AdbcDatabaseNew(&database_, &error_), IsOkStatus(&error_));
  ASSERT_THAT(AdbcDatabaseSetOption(&database_, ADBC_DB2_OPTION_DATABASE,
                                    database.c_str(), &error_),
              IsOkStatus(&error_));
  ASSERT_THAT(AdbcDatabaseSetOption(&database_, ADBC_DB2_OPTION_HOSTNAME,
                                    hostname.c_str(), &error_),
              IsOkStatus(&error_));
  ASSERT_THAT(AdbcDatabaseSetOption(&database_, ADBC_DB2_OPTION_PORT, port.c_str(),
                                    &error_),
              IsOkStatus(&error_));
  ASSERT_THAT(AdbcDatabaseSetOption(&database_, ADBC_DB2_OPTION_UID, uid.c_str(),
                                    &error_),
              IsOkStatus(&error_));
  ASSERT_THAT(AdbcDatabaseSetOption(&database_, ADBC_DB2_OPTION_PWD, pwd.c_str(),
                                    &error_),
              IsOkStatus(&error_));
  ASSERT_THAT(AdbcDatabaseInit(&database_, &error_), IsOkStatus(&error_));

  struct AdbcConnection connection;
  std::memset(&connection, 0, sizeof(connection));
  ASSERT_THAT(AdbcConnectionNew(&connection, &error_), IsOkStatus(&error_));
  ASSERT_THAT(AdbcConnectionInit(&connection, &database_, &error_),
              IsOkStatus(&error_));
  ASSERT_THAT(AdbcConnectionRelease(&connection, &error_), IsOkStatus(&error_));
}

TEST_F(Db2ConnectionOptionsTest, UsernamePasswordSynonyms) {
  ASSERT_THAT(AdbcDatabaseNew(&database_, &error_), IsOkStatus(&error_));
  // The standard "username"/"password" option names must be accepted
  // as synonyms for adbc.db2.uid / adbc.db2.pwd.
  ASSERT_THAT(AdbcDatabaseSetOption(&database_, "username", "ignored", &error_),
              IsOkStatus(&error_));
  ASSERT_THAT(AdbcDatabaseSetOption(&database_, "password", "ignored", &error_),
              IsOkStatus(&error_));
}

TEST_F(Db2ConnectionOptionsTest, UnreachablePortMapsToIoStatus) {
  const std::string uri = GetTestUri();
  auto extract = [&](const std::string& key) -> std::string {
    std::string needle = key + "=";
    auto pos = uri.find(needle);
    if (pos == std::string::npos) return "";
    auto end = uri.find(';', pos);
    return uri.substr(pos + needle.size(),
                      (end == std::string::npos) ? std::string::npos
                                                 : end - pos - needle.size());
  };
  const std::string database = extract("DATABASE");
  const std::string uid = extract("UID");
  const std::string pwd = extract("PWD");
  if (database.empty() || uid.empty() || pwd.empty()) {
    GTEST_SKIP() << "ADBC_DB2_TEST_URI does not contain DATABASE/UID/PWD";
  }

  ASSERT_THAT(AdbcDatabaseNew(&database_, &error_), IsOkStatus(&error_));
  ASSERT_THAT(AdbcDatabaseSetOption(&database_, ADBC_DB2_OPTION_DATABASE,
                                    database.c_str(), &error_),
              IsOkStatus(&error_));
  ASSERT_THAT(AdbcDatabaseSetOption(&database_, ADBC_DB2_OPTION_HOSTNAME, "localhost",
                                    &error_),
              IsOkStatus(&error_));
  ASSERT_THAT(AdbcDatabaseSetOption(&database_, ADBC_DB2_OPTION_PORT, "1", &error_),
              IsOkStatus(&error_));
  ASSERT_THAT(AdbcDatabaseSetOption(&database_, ADBC_DB2_OPTION_UID, uid.c_str(),
                                    &error_),
              IsOkStatus(&error_));
  ASSERT_THAT(AdbcDatabaseSetOption(&database_, ADBC_DB2_OPTION_PWD, pwd.c_str(),
                                    &error_),
              IsOkStatus(&error_));
  ASSERT_THAT(AdbcDatabaseInit(&database_, &error_), IsOkStatus(&error_));

  struct AdbcConnection connection;
  std::memset(&connection, 0, sizeof(connection));
  ASSERT_THAT(AdbcConnectionNew(&connection, &error_), IsOkStatus(&error_));
  EXPECT_THAT(AdbcConnectionInit(&connection, &database_, &error_),
              IsStatus(ADBC_STATUS_IO, &error_));
  AdbcConnectionRelease(&connection, &error_);
}

TEST_F(Db2ConnectionOptionsTest, MalformedPortIsRejected) {
  ASSERT_THAT(AdbcDatabaseNew(&database_, &error_), IsOkStatus(&error_));
  EXPECT_THAT(AdbcDatabaseSetOption(&database_, ADBC_DB2_OPTION_PORT, "fifty-thousand",
                                    &error_),
              IsStatus(ADBC_STATUS_INVALID_ARGUMENT, &error_));
}

TEST_F(Db2ConnectionOptionsTest, EmptyUidOrPwdIsRejected) {
  ASSERT_THAT(AdbcDatabaseNew(&database_, &error_), IsOkStatus(&error_));
  EXPECT_THAT(AdbcDatabaseSetOption(&database_, ADBC_DB2_OPTION_UID, "", &error_),
              IsStatus(ADBC_STATUS_INVALID_ARGUMENT, &error_));
  EXPECT_THAT(AdbcDatabaseSetOption(&database_, ADBC_DB2_OPTION_PWD, "", &error_),
              IsStatus(ADBC_STATUS_INVALID_ARGUMENT, &error_));
}

TEST_F(Db2ConnectionOptionsTest, UriTakesPrecedenceOverFieldOptions) {
  const std::string uri = GetTestUri();
  if (uri.empty()) {
    GTEST_SKIP() << "ADBC_DB2_TEST_URI not set";
  }

  ASSERT_THAT(AdbcDatabaseNew(&database_, &error_), IsOkStatus(&error_));
  ASSERT_THAT(AdbcDatabaseSetOption(&database_, "uri", uri.c_str(), &error_),
              IsOkStatus(&error_));
  // Intentionally conflicting field options; these must be ignored when uri is set.
  ASSERT_THAT(AdbcDatabaseSetOption(&database_, ADBC_DB2_OPTION_HOSTNAME,
                                    "unreachable.invalid", &error_),
              IsOkStatus(&error_));
  ASSERT_THAT(AdbcDatabaseSetOption(&database_, ADBC_DB2_OPTION_PORT, "1", &error_),
              IsOkStatus(&error_));
  ASSERT_THAT(AdbcDatabaseInit(&database_, &error_), IsOkStatus(&error_));

  struct AdbcConnection connection;
  std::memset(&connection, 0, sizeof(connection));
  ASSERT_THAT(AdbcConnectionNew(&connection, &error_), IsOkStatus(&error_));
  ASSERT_THAT(AdbcConnectionInit(&connection, &database_, &error_),
              IsOkStatus(&error_));
  ASSERT_THAT(AdbcConnectionRelease(&connection, &error_), IsOkStatus(&error_));
}
