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

#ifndef ADBC_VALIDATION_H
#define ADBC_VALIDATION_H

#include <optional>
#include <string>
#include <vector>

#include <adbc.h>
#include <gtest/gtest.h>
#include <nanoarrow/nanoarrow.h>

namespace adbc_validation {

#define ADBCV_STRINGIFY(s) #s
#define ADBCV_STRINGIFY_VALUE(s) ADBCV_STRINGIFY(s)

/// \brief Configuration for driver-specific behavior.
class DriverQuirks {
 public:
  /// \brief Do any initialization between New and Init.
  virtual AdbcStatusCode SetupDatabase(struct AdbcDatabase* database,
                                       struct AdbcError* error) const {
    return ADBC_STATUS_OK;
  }

  /// \brief Drop the given table. Used by tests to reset state.
  virtual AdbcStatusCode DropTable(struct AdbcConnection* connection,
                                   const std::string& name,
                                   struct AdbcError* error) const {
    return ADBC_STATUS_OK;
  }

  /// \brief Drop the given view. Used by tests to reset state.
  virtual AdbcStatusCode DropView(struct AdbcConnection* connection,
                                  const std::string& name,
                                  struct AdbcError* error) const {
    return ADBC_STATUS_NOT_IMPLEMENTED;
  }

  virtual AdbcStatusCode EnsureSampleTable(struct AdbcConnection* connection,
                                           const std::string& name,
                                           struct AdbcError* error) const;

  /// \brief Create a table of sample data with a fixed schema for testing.
  ///
  /// The table should have two columns:
  /// - "int64s" with Arrow type int64.
  /// - "strings" with Arrow type utf8.
  virtual AdbcStatusCode CreateSampleTable(struct AdbcConnection* connection,
                                           const std::string& name,
                                           struct AdbcError* error) const;

  /// \brief Get the statement to create a table with a primary key, or nullopt if not
  /// supported.
  ///
  /// The table should have one column:
  /// - "id" with Arrow type int64 (primary key)
  virtual std::optional<std::string> PrimaryKeyTableDdl(std::string_view name) const {
    return std::nullopt;
  }

  /// \brief Return the SQL to reference the bind parameter of the given index
  virtual std::string BindParameter(int index) const { return "?"; }

  /// \brief For a given Arrow type of ingested data, what Arrow type
  ///   will the database return when that column is selected?
  virtual ArrowType IngestSelectRoundTripType(ArrowType ingest_type) const {
    return ingest_type;
  }

  /// \brief Whether two statements can be used at the same time on a
  ///   single connection
  virtual bool supports_concurrent_statements() const { return false; }

  /// \brief Whether AdbcStatementExecutePartitions should work
  virtual bool supports_partitioned_data() const { return false; }

  /// \brief Whether transaction methods are implemented
  virtual bool supports_transactions() const { return true; }

  /// \brief Whether or not DDL implicitly commits a transaction
  virtual bool ddl_implicit_commit_txn() const { return false; }

  /// \brief Whether GetSqlInfo is implemented
  virtual bool supports_get_sql_info() const { return true; }

  /// \brief Whether GetObjects is implemented
  virtual bool supports_get_objects() const { return true; }

  /// \brief Whether bulk ingest is supported
  virtual bool supports_bulk_ingest() const { return true; }

  /// \brief Whether dynamic parameter bindings are supported for prepare
  virtual bool supports_dynamic_parameter_binding() const { return true; }

  /// \brief Whether ExecuteQuery sets rows_affected appropriately
  virtual bool supports_rows_affected() const { return true; }

  /// \brief Default catalog to use for tests
  virtual std::string catalog() const { return ""; }

  /// \brief Default Schema to use for tests
  virtual std::string db_schema() const { return ""; }
};

class DatabaseTest {
 public:
  virtual const DriverQuirks* quirks() const = 0;

  void SetUpTest();
  void TearDownTest();

  // Test methods
  void TestNewInit();
  void TestRelease();

 protected:
  struct AdbcError error;
  struct AdbcDatabase database;
};

#define ADBCV_TEST_DATABASE(FIXTURE)                                            \
  static_assert(std::is_base_of<adbc_validation::DatabaseTest, FIXTURE>::value, \
                ADBCV_STRINGIFY(FIXTURE) " must inherit from DatabaseTest");    \
  TEST_F(FIXTURE, NewInit) { TestNewInit(); }                                   \
  TEST_F(FIXTURE, Release) { TestRelease(); }

class ConnectionTest {
 public:
  virtual const DriverQuirks* quirks() const = 0;

  void SetUpTest();
  void TearDownTest();

  // Test methods
  void TestNewInit();
  void TestRelease();
  void TestConcurrent();
  void TestAutocommitDefault();

  void TestAutocommitToggle();

  void TestMetadataGetInfo();
  void TestMetadataGetTableSchema();
  void TestMetadataGetTableTypes();

  void TestMetadataGetObjectsCatalogs();
  void TestMetadataGetObjectsDbSchemas();
  void TestMetadataGetObjectsTables();
  void TestMetadataGetObjectsTablesTypes();
  void TestMetadataGetObjectsColumns();
  void TestMetadataGetObjectsConstraints();
  void TestMetadataGetObjectsPrimaryKey();

 protected:
  struct AdbcError error;
  struct AdbcDatabase database;
  struct AdbcConnection connection;
};

#define ADBCV_TEST_CONNECTION(FIXTURE)                                                \
  static_assert(std::is_base_of<adbc_validation::ConnectionTest, FIXTURE>::value,     \
                ADBCV_STRINGIFY(FIXTURE) " must inherit from ConnectionTest");        \
  TEST_F(FIXTURE, NewInit) { TestNewInit(); }                                         \
  TEST_F(FIXTURE, Release) { TestRelease(); }                                         \
  TEST_F(FIXTURE, Concurrent) { TestConcurrent(); }                                   \
  TEST_F(FIXTURE, AutocommitDefault) { TestAutocommitDefault(); }                     \
  TEST_F(FIXTURE, AutocommitToggle) { TestAutocommitToggle(); }                       \
  TEST_F(FIXTURE, MetadataGetInfo) { TestMetadataGetInfo(); }                         \
  TEST_F(FIXTURE, MetadataGetTableSchema) { TestMetadataGetTableSchema(); }           \
  TEST_F(FIXTURE, MetadataGetTableTypes) { TestMetadataGetTableTypes(); }             \
  TEST_F(FIXTURE, MetadataGetObjectsCatalogs) { TestMetadataGetObjectsCatalogs(); }   \
  TEST_F(FIXTURE, MetadataGetObjectsDbSchemas) { TestMetadataGetObjectsDbSchemas(); } \
  TEST_F(FIXTURE, MetadataGetObjectsTables) { TestMetadataGetObjectsTables(); }       \
  TEST_F(FIXTURE, MetadataGetObjectsTablesTypes) {                                    \
    TestMetadataGetObjectsTablesTypes();                                              \
  }                                                                                   \
  TEST_F(FIXTURE, MetadataGetObjectsColumns) { TestMetadataGetObjectsColumns(); }     \
  TEST_F(FIXTURE, MetadataGetObjectsConstraints) {                                    \
    TestMetadataGetObjectsConstraints();                                              \
  }                                                                                   \
  TEST_F(FIXTURE, MetadataGetObjectsPrimaryKey) { TestMetadataGetObjectsPrimaryKey(); }

class StatementTest {
 public:
  virtual const DriverQuirks* quirks() const = 0;

  void SetUpTest();
  void TearDownTest();

  // Test methods
  void TestNewInit();
  void TestRelease();

  // ---- Type-specific tests --------------------

  // Integers
  void TestSqlIngestInt8();
  void TestSqlIngestInt16();
  void TestSqlIngestInt32();
  void TestSqlIngestInt64();
  void TestSqlIngestUInt8();
  void TestSqlIngestUInt16();
  void TestSqlIngestUInt32();
  void TestSqlIngestUInt64();

  // Floats
  void TestSqlIngestFloat32();
  void TestSqlIngestFloat64();

  // Strings
  void TestSqlIngestString();
  void TestSqlIngestBinary();

  // Temporal
  void TestSqlIngestTimestamp();

  // ---- End Type-specific tests ----------------

  void TestSqlIngestAppend();
  void TestSqlIngestErrors();
  void TestSqlIngestMultipleConnections();
  void TestSqlIngestSample();

  void TestSqlPartitionedInts();

  void TestSqlPrepareGetParameterSchema();
  void TestSqlPrepareSelectNoParams();
  void TestSqlPrepareSelectParams();
  void TestSqlPrepareUpdate();
  void TestSqlPrepareUpdateNoParams();
  void TestSqlPrepareUpdateStream();
  void TestSqlPrepareErrorNoQuery();
  void TestSqlPrepareErrorParamCountMismatch();

  void TestSqlQueryInts();
  void TestSqlQueryFloats();
  void TestSqlQueryStrings();

  void TestSqlQueryErrors();

  void TestTransactions();

  void TestConcurrentStatements();
  void TestResultInvalidation();

 protected:
  struct AdbcError error;
  struct AdbcDatabase database;
  struct AdbcConnection connection;
  struct AdbcStatement statement;

  template <typename CType>
  void TestSqlIngestType(ArrowType type, const std::vector<std::optional<CType>>& values);

  template <typename CType>
  void TestSqlIngestNumericType(ArrowType type);

  template <enum ArrowTimeUnit TU>
  void TestSqlIngestTemporalType();
};

#define ADBCV_TEST_STATEMENT(FIXTURE)                                                   \
  static_assert(std::is_base_of<adbc_validation::StatementTest, FIXTURE>::value,        \
                ADBCV_STRINGIFY(FIXTURE) " must inherit from StatementTest");           \
  TEST_F(FIXTURE, NewInit) { TestNewInit(); }                                           \
  TEST_F(FIXTURE, Release) { TestRelease(); }                                           \
  TEST_F(FIXTURE, SqlIngestInt8) { TestSqlIngestInt8(); }                               \
  TEST_F(FIXTURE, SqlIngestInt16) { TestSqlIngestInt16(); }                             \
  TEST_F(FIXTURE, SqlIngestInt32) { TestSqlIngestInt32(); }                             \
  TEST_F(FIXTURE, SqlIngestInt64) { TestSqlIngestInt64(); }                             \
  TEST_F(FIXTURE, SqlIngestUInt8) { TestSqlIngestUInt8(); }                             \
  TEST_F(FIXTURE, SqlIngestUInt16) { TestSqlIngestUInt16(); }                           \
  TEST_F(FIXTURE, SqlIngestUInt32) { TestSqlIngestUInt32(); }                           \
  TEST_F(FIXTURE, SqlIngestUInt64) { TestSqlIngestUInt64(); }                           \
  TEST_F(FIXTURE, SqlIngestFloat32) { TestSqlIngestFloat32(); }                         \
  TEST_F(FIXTURE, SqlIngestFloat64) { TestSqlIngestFloat64(); }                         \
  TEST_F(FIXTURE, SqlIngestString) { TestSqlIngestString(); }                           \
  TEST_F(FIXTURE, SqlIngestBinary) { TestSqlIngestBinary(); }                           \
  TEST_F(FIXTURE, SqlIngestTimestamp) { TestSqlIngestTimestamp(); }                     \
  TEST_F(FIXTURE, SqlIngestAppend) { TestSqlIngestAppend(); }                           \
  TEST_F(FIXTURE, SqlIngestErrors) { TestSqlIngestErrors(); }                           \
  TEST_F(FIXTURE, SqlIngestMultipleConnections) { TestSqlIngestMultipleConnections(); } \
  TEST_F(FIXTURE, SqlIngestSample) { TestSqlIngestSample(); }                           \
  TEST_F(FIXTURE, SqlPartitionedInts) { TestSqlPartitionedInts(); }                     \
  TEST_F(FIXTURE, SqlPrepareGetParameterSchema) { TestSqlPrepareGetParameterSchema(); } \
  TEST_F(FIXTURE, SqlPrepareSelectNoParams) { TestSqlPrepareSelectNoParams(); }         \
  TEST_F(FIXTURE, SqlPrepareSelectParams) { TestSqlPrepareSelectParams(); }             \
  TEST_F(FIXTURE, SqlPrepareUpdate) { TestSqlPrepareUpdate(); }                         \
  TEST_F(FIXTURE, SqlPrepareUpdateNoParams) { TestSqlPrepareUpdateNoParams(); }         \
  TEST_F(FIXTURE, SqlPrepareUpdateStream) { TestSqlPrepareUpdateStream(); }             \
  TEST_F(FIXTURE, SqlPrepareErrorNoQuery) { TestSqlPrepareErrorNoQuery(); }             \
  TEST_F(FIXTURE, SqlPrepareErrorParamCountMismatch) {                                  \
    TestSqlPrepareErrorParamCountMismatch();                                            \
  }                                                                                     \
  TEST_F(FIXTURE, SqlQueryInts) { TestSqlQueryInts(); }                                 \
  TEST_F(FIXTURE, SqlQueryFloats) { TestSqlQueryFloats(); }                             \
  TEST_F(FIXTURE, SqlQueryStrings) { TestSqlQueryStrings(); }                           \
  TEST_F(FIXTURE, SqlQueryErrors) { TestSqlQueryErrors(); }                             \
  TEST_F(FIXTURE, Transactions) { TestTransactions(); }                                 \
  TEST_F(FIXTURE, ConcurrentStatements) { TestConcurrentStatements(); }                 \
  TEST_F(FIXTURE, ResultInvalidation) { TestResultInvalidation(); }

}  // namespace adbc_validation

#endif  // ADBC_VALIDATION_H
