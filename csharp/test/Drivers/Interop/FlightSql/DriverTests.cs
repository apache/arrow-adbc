﻿/*
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements.  See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to You under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

using System;
using System.Collections.Generic;
using System.Linq;
using Apache.Arrow.Adbc.C;
using Apache.Arrow.Adbc.Tests.Metadata;
using Apache.Arrow.Adbc.Tests.Xunit;
using Apache.Arrow.Ipc;
using Xunit;
using Xunit.Abstractions;

namespace Apache.Arrow.Adbc.Tests.Drivers.Interop.FlightSql
{
    /// <summary>
    /// Class for testing the Flight SQL ADBC driver connection tests.
    /// </summary>
    /// <remarks>
    /// Tests are ordered to ensure data is created for the other
    /// queries to run.
    /// </remarks>
    [TestCaseOrderer("Apache.Arrow.Adbc.Tests.Xunit.TestOrderer", "Apache.Arrow.Adbc.Tests")]
    public class DriverTests : IDisposable
    {
        readonly FlightSqlTestConfiguration _testConfiguration;
        readonly List<FlightSqlTestEnvironment> _environments;
        readonly Dictionary<string, AdbcConnection> _configuredConnections = new Dictionary<string, AdbcConnection>();
        readonly ITestOutputHelper _outputHelper;

        public static IEnumerable<object[]> GetPatterns(string? namePattern)
        {
            string name = namePattern!;
            yield return new object[] { name };
            yield return new object[] { $"{DriverTests.GetPartialNameForPatternMatch(name)}%" };
            yield return new object[] { $"{DriverTests.GetPartialNameForPatternMatch(name).ToLower()}%" };
            yield return new object[] { $"{DriverTests.GetPartialNameForPatternMatch(name).ToUpper()}%" };
            yield return new object[] { $"_{DriverTests.GetNameWithoutFirstChatacter(name)}" };
            yield return new object[] { $"_{DriverTests.GetNameWithoutFirstChatacter(name).ToLower()}" };
            yield return new object[] { $"_{DriverTests.GetNameWithoutFirstChatacter(name).ToUpper()}" };
        }

        public static IEnumerable<object[]> CatalogNamePatternData()
        {
            foreach (FlightSqlTestEnvironment environment in FlightSqlTestingUtils.GetTestEnvironments(FlightSqlTestingUtils.LoadFlightSqlTestConfiguration()))
            {
                string? databaseName = environment.Metadata.Catalog;
                yield return GetPatterns(databaseName).ToArray();
            }
        }

        public static IEnumerable<object[]> DbSchemasNamePatternData()
        {
            foreach (FlightSqlTestEnvironment environment in FlightSqlTestingUtils.GetTestEnvironments(FlightSqlTestingUtils.LoadFlightSqlTestConfiguration()))
            {
                string? dbSchemaName = environment.Metadata.Schema;
                yield return GetPatterns(dbSchemaName).ToArray();
            }
        }

        public static IEnumerable<object[]> TableNamePatternData()
        {
            foreach (FlightSqlTestEnvironment environment in FlightSqlTestingUtils.GetTestEnvironments(FlightSqlTestingUtils.LoadFlightSqlTestConfiguration()))
            {
                string? tableName = environment.Metadata.Table;
                yield return GetPatterns(tableName).ToArray();
            }
        }

        public DriverTests(ITestOutputHelper outputHelper)
        {
            Skip.IfNot(Utils.CanExecuteTestConfig(FlightSqlTestingUtils.FLIGHTSQL_TEST_CONFIG_VARIABLE));
            _testConfiguration = FlightSqlTestingUtils.LoadFlightSqlTestConfiguration();
            _environments = FlightSqlTestingUtils.GetTestEnvironments(_testConfiguration);
            _outputHelper = outputHelper;

            foreach (FlightSqlTestEnvironment environment in _environments)
            {
                Dictionary<string, string> parameters = new Dictionary<string, string>();
                Dictionary<string, string> options = new Dictionary<string, string>();
                AdbcDriver driver = FlightSqlTestingUtils.GetAdbcDriver(_testConfiguration, environment, out parameters);
                AdbcDatabase database = driver.Open(parameters);
                AdbcConnection connection = database.Connect(options);

                _configuredConnections.Add(environment.Name!, connection);
            }
        }

        /// <summary>
        /// Validates if the driver can connect to a live server and
        /// parse the results.
        /// </summary>
        /// <remarks>
        /// Tests are ordered to ensure data is created
        /// for the other queries to run.
        /// </remarks>
        [SkippableFact, Order(1)]
        public void CanExecuteUpdate()
        {
            foreach (FlightSqlTestEnvironment environment in _environments)
            {
                // Dremio doesn't have acceptPut implemented by design.
                if (environment.SupportsWriteUpdate)
                {
                    string[] queries = FlightSqlTestingUtils.GetQueries(environment);

                    List<int> expectedResults = new List<int>() { -1, 1, 1 };

                    for (int i = 0; i < queries.Length; i++)
                    {
                        string query = queries[i];
                        UpdateResult updateResult = ExecuteUpdateStatement(environment, query);
                        Assert.Equal(expectedResults[i], updateResult.AffectedRows);
                    }
                }
                else
                {
                    _outputHelper.WriteLine("WriteUpdate is not supported in the [" + environment.Name + "] environment");
                }
            }
        }

        /// <summary>
        /// Validates error message returned when acceptPut not implemented by server.
        /// </summary>
        /// <remarks>
        /// Tests are ordered to ensure data is created
        /// for the other queries to run.
        /// </remarks>
        [SkippableFact, Order(1)]
        public void AcceptPutNotImplemented()
        {
            foreach (FlightSqlTestEnvironment environment in _environments)
            {
                if (environment.SupportsWriteUpdate)
                {
                    string[] queries = FlightSqlTestingUtils.GetQueries(environment);

                    List<int> expectedResults = new List<int>() { -1, 1, 1 };

                    for (int i = 0; i < queries.Length; i++)
                    {
                        string query = queries[i];
                        CAdbcDriverImporter.ImportedAdbcException ex = Assert.Throws<CAdbcDriverImporter.ImportedAdbcException>(() => ExecuteUpdateStatement(environment, query));
                        Assert.Equal("[FlightSQL] [FlightSQL] acceptPut is not implemented. (Unimplemented; ExecuteQuery)", ex.Message);
                    }
                }
                else
                {
                    _outputHelper.WriteLine("WriteUpdate is not supported in the [" + environment.Name + "] environment");
                }
            }
        }

        /// <summary>
        /// Validates if the driver can call GetInfo.
        /// </summary>
        [SkippableFact, Order(2)]
        public void CanGetInfo()
        {
            foreach (FlightSqlTestEnvironment environment in _environments)
            {
                using IArrowArrayStream stream = GetAdbcConnection(environment.Name).GetInfo(new List<AdbcInfoCode>() { AdbcInfoCode.DriverName, AdbcInfoCode.DriverVersion, AdbcInfoCode.VendorName });
                using RecordBatch recordBatch = stream.ReadNextRecordBatchAsync().Result;
                UInt32Array infoNameArray = (UInt32Array)recordBatch.Column("info_name");

                List<string> expectedValues = new List<string>() { "DriverName", "DriverVersion", "VendorName" };

                for (int i = 0; i < infoNameArray.Length; i++)
                {
                    uint? uintValue = infoNameArray.GetValue(i);

                    if (uintValue.HasValue)
                    {
                        AdbcInfoCode value = (AdbcInfoCode)uintValue;
                        DenseUnionArray valueArray = (DenseUnionArray)recordBatch.Column("info_value");

                        Assert.Contains(value.ToString(), expectedValues);

                        StringArray stringArray = (StringArray)valueArray.Fields[0];
                        _outputHelper.WriteLine($"{value}={stringArray.GetString(i)}");
                    }
                }
            }
        }

        /// <summary>
        /// Validates if the driver can call GetObjects with GetObjectsDepth as Catalogs and CatalogName passed as a pattern.
        /// </summary>
        [SkippableTheory, Order(3)]
        [MemberData(nameof(CatalogNamePatternData))]
        public void CanGetObjectsCatalogs(string catalogPattern)
        {
            foreach (FlightSqlTestEnvironment environment in _environments)
            {
                // Dremio doesn't use catalogs
                if (environment.SupportsCatalogs)
                {
                    string databaseName = environment.Metadata.Catalog;
                    string schemaName = environment.Metadata.Schema;

                    using IArrowArrayStream stream = GetAdbcConnection(environment.Name).GetObjects(
                            depth: AdbcConnection.GetObjectsDepth.Catalogs,
                            catalogPattern: catalogPattern,
                            dbSchemaPattern: null,
                            tableNamePattern: null,
                            tableTypes: environment.TableTypes,
                            columnNamePattern: null);

                    using RecordBatch recordBatch = stream.ReadNextRecordBatchAsync().Result;

                    List<AdbcCatalog> catalogs = GetObjectsParser.ParseCatalog(recordBatch, null);
                    AdbcCatalog? catalog = catalogs.Where((catalog) => string.Equals(catalog.Name, databaseName)).FirstOrDefault();

                    Assert.True(catalog != null, "catalog should not be null in the [" + environment.Name + "] environment");
                }
                else
                {
                    _outputHelper.WriteLine("Catalogs are not supported in the [" + environment.Name + "] environment");
                }
            }
        }

        /// <summary>
        /// Validates if the driver can call GetObjects with GetObjectsDepth as DbSchemas with DbSchemaName as a pattern.
        /// </summary>
        [SkippableTheory, Order(3)]
        [MemberData(nameof(DbSchemasNamePatternData))]
        public void CanGetObjectsDbSchemas(string dbSchemaPattern)
        {
            foreach (FlightSqlTestEnvironment environment in _environments)
            {
                // need to add the database
                string databaseName = environment.Metadata.Catalog;
                string schemaName = environment.Metadata.Schema;

                using IArrowArrayStream stream = GetAdbcConnection(environment.Name).GetObjects(
                        depth: AdbcConnection.GetObjectsDepth.DbSchemas,
                        catalogPattern: databaseName,
                        dbSchemaPattern: dbSchemaPattern,
                        tableNamePattern: null,
                        tableTypes: environment.TableTypes,
                        columnNamePattern: null);

                using RecordBatch recordBatch = stream.ReadNextRecordBatchAsync().Result;

                List<AdbcCatalog> catalogs = GetObjectsParser.ParseCatalog(recordBatch, schemaName);

                List<AdbcDbSchema>? dbSchemas = catalogs
                    .Where(c => string.Equals(c.Name, databaseName))
                    .Select(c => c.DbSchemas)
                    .FirstOrDefault();

                Assert.True(dbSchemas != null, "dbSchemas should not be null in the [" + environment.Name + "] environment");

                AdbcDbSchema? dbSchema = dbSchemas.Where((dbSchema) => string.Equals(dbSchema.Name, schemaName)).FirstOrDefault();

                Assert.True(dbSchema != null, "dbSchema should not be null in the [" + environment.Name + "] environment");
            }
        }

        /// <summary>
        /// Validates if the driver can call GetObjects with GetObjectsDepth as Tables with TableName as a pattern.
        /// </summary>
        [SkippableTheory, Order(3)]
        [MemberData(nameof(TableNamePatternData))]
        public void CanGetObjectsTables(string tableNamePattern)
        {
            foreach (FlightSqlTestEnvironment environment in _environments)
            {
                // need to add the database
                string databaseName = environment.Metadata.Catalog;
                string schemaName = environment.Metadata.Schema;
                string tableName = environment.Metadata.Table;

                using IArrowArrayStream stream = GetAdbcConnection(environment.Name).GetObjects(
                        depth: AdbcConnection.GetObjectsDepth.Tables,
                        catalogPattern: databaseName,
                        dbSchemaPattern: schemaName,
                        tableNamePattern: tableNamePattern,
                        tableTypes: environment.TableTypes,
                        columnNamePattern: null);

                using RecordBatch recordBatch = stream.ReadNextRecordBatchAsync().Result;

                List<AdbcCatalog> catalogs = GetObjectsParser.ParseCatalog(recordBatch, schemaName);

                List<AdbcDbSchema>? schemas = catalogs
                    .Where(c => string.Equals(c.Name, databaseName))
                    .Select(c => c.DbSchemas)
                    .FirstOrDefault();

                Assert.True(schemas != null, "schemas should not be null in the [" + environment.Name + "] environment");

                List<AdbcTable>? tables = schemas
                    .Where(s => string.Equals(s.Name, schemaName))
                    .Select(s => s.Tables)
                    .FirstOrDefault();

                Assert.True(tables != null, "schemas should not be null in the [" + environment.Name + "] environment");

                AdbcTable? table = tables.Where((table) => string.Equals(table.Name, tableName)).FirstOrDefault();
                Assert.True(table != null, "table should not be null in the [" + environment.Name + "] environment");
                Assert.Equal("TABLE", table.Type);
            }
        }

        /// <summary>
        /// Validates if the driver can call GetObjects for GetObjectsDepth as All.
        /// </summary>
        [SkippableFact, Order(3)]
        public void CanGetObjectsAll()
        {
            foreach (FlightSqlTestEnvironment environment in _environments)
            {
                string databaseName = environment.Metadata.Catalog;
                string schemaName = environment.Metadata.Schema;
                string tableName = environment.Metadata.Table;
                string? columnName = null;

                using IArrowArrayStream stream = GetAdbcConnection(environment.Name).GetObjects(
                        depth: AdbcConnection.GetObjectsDepth.All,
                        catalogPattern: databaseName,
                        dbSchemaPattern: schemaName,
                        tableNamePattern: tableName,
                        tableTypes: environment.TableTypes,
                        columnNamePattern: columnName);

                using RecordBatch recordBatch = stream.ReadNextRecordBatchAsync().Result;

                List<AdbcCatalog> catalogs = GetObjectsParser.ParseCatalog(recordBatch, schemaName);

                List<AdbcDbSchema>? schemas = catalogs
                    .Where(c => string.Equals(c.Name, databaseName))
                    .Select(c => c.DbSchemas)
                    .FirstOrDefault();

                Assert.True(schemas != null, "schemas should not be null in the [" + environment.Name + "] environment");

                List<AdbcTable>? tables = schemas
                    .Where(s => string.Equals(s.Name, schemaName))
                    .Select(s => s.Tables)
                    .FirstOrDefault();

                Assert.True(tables != null, "tables should not be null in the [" + environment.Name + "] environment");

                AdbcTable? table = tables
                    .Where(t => string.Equals(t.Name, tableName))
                    .FirstOrDefault();

                Assert.True(table != null, "table should not be null in the [" + environment.Name + "] environment");

                List<AdbcColumn>? columns = table.Columns;

                Assert.True(columns != null, "Columns cannot be null in the [" + environment.Name + "] environment");
                Assert.Equal(environment.Metadata.ExpectedColumnCount, columns.Count);
            }
        }

        /// <summary>
        /// Validates if the driver can call GetObjects with GetObjectsDepth as Tables with TableName as a Special Character.
        /// </summary>
        [SkippableTheory, Order(3)]
        [InlineData(@"ADBCDEMO_DB", @"PUBLIC", "MyIdentifier")]
        [InlineData(@"ADBCDEMO'DB", @"PUBLIC'SCHEMA", "my.identifier")]
        [InlineData(@"ADBCDEM""DB", @"PUBLIC""SCHEMA", "my.identifier")]
        [InlineData(@"ADBCDEMO_DB", @"PUBLIC", "my identifier")]
        [InlineData(@"ADBCDEMO_DB", @"PUBLIC", "My 'Identifier'")]
        [InlineData(@"ADBCDEMO_DB", @"PUBLIC", "3rd_identifier")]
        [InlineData(@"ADBCDEMO_DB", @"PUBLIC", "$Identifier")]
        [InlineData(@"ADBCDEMO_DB", @"PUBLIC", "My ^Identifier")]
        [InlineData(@"ADBCDEMO_DB", @"PUBLIC", "My ^Ident~ifier")]
        [InlineData(@"ADBCDEMO_DB", @"PUBLIC", @"My\^Ident~ifier")]
        [InlineData(@"ADBCDEMO_DB", @"PUBLIC", "идентификатор")]
        [InlineData(@"ADBCDEMO_DB", @"PUBLIC", @"ADBCTest_""ALL""TYPES")]
        [InlineData(@"ADBCDEMO_DB", @"PUBLIC", @"ADBC\TEST""\TAB_""LE")]
        [InlineData(@"ADBCDEMO_DB", @"PUBLIC", "ONE")]
        public void CanGetObjectsTablesWithSpecialCharacter(string databaseName, string schemaName, string tableName)
        {
            foreach (FlightSqlTestEnvironment environment in _environments)
            {
                // Dremio doesn't support write operations so temporary table needs to be re-thought
                if (environment.SupportsWriteUpdate)
                {
                    CreateDatabaseAndTable(environment, databaseName, schemaName, tableName);

                    using IArrowArrayStream stream = GetAdbcConnection(environment.Name).GetObjects(
                            depth: AdbcConnection.GetObjectsDepth.Tables,
                            catalogPattern: databaseName,
                            dbSchemaPattern: schemaName,
                            tableNamePattern: tableName,
                            tableTypes: environment.TableTypes,
                            columnNamePattern: null);

                    using RecordBatch recordBatch = stream.ReadNextRecordBatchAsync().Result;

                    List<AdbcCatalog> catalogs = GetObjectsParser.ParseCatalog(recordBatch, schemaName);

                    List<AdbcDbSchema>? schemas = catalogs
                        .Where(c => string.Equals(c.Name, databaseName))
                        .Select(c => c.DbSchemas)
                        .FirstOrDefault();

                    Assert.True(schemas != null, "schemas should not be null in the [" + environment.Name + "] environment");

                    List<AdbcTable>? tables = schemas
                        .Where(s => string.Equals(s.Name, schemaName))
                        .Select(s => s.Tables)
                        .FirstOrDefault();

                    Assert.True(tables != null, "tables should not be null in the [" + environment.Name + "] environment");

                    AdbcTable? table = tables.FirstOrDefault();

                    Assert.True(table != null, "table should not be null in the [" + environment.Name + "] environment");
                    Assert.Equal(tableName, table.Name, true);
                    DropDatabaseAndTable(environment, databaseName, schemaName, tableName);
                }
                else
                {
                    _outputHelper.WriteLine("WriteUpdate is not supported in the [" + environment.Name + "] environment");
                }
            }
        }

        /// <summary>
        /// Validates if the driver can call GetTableSchema.
        /// </summary>
        [SkippableFact, Order(4)]
        public void CanGetTableSchema()
        {
            foreach (FlightSqlTestEnvironment environment in _environments)
            {
                string databaseName = environment.Metadata.Catalog;
                string schemaName = environment.Metadata.Schema;
                string tableName = environment.Metadata.Table;

                Schema schema = GetAdbcConnection(environment.Name).GetTableSchema(databaseName, schemaName, tableName);

                int numberOfFields = schema.FieldsList.Count;

                Assert.True(environment.Metadata.ExpectedColumnCount == numberOfFields, "ExpectedColumnCount not equal in the [" + environment.Name + "] environment");
            }
        }

        /// <summary>
        /// Validates if the driver can call GetTableTypes.
        /// </summary>
        [SkippableFact, Order(5)]
        public void CanGetTableTypes()
        {
            foreach (FlightSqlTestEnvironment environment in _environments)
            {
                using IArrowArrayStream arrowArrayStream = GetAdbcConnection(environment.Name).GetTableTypes();

                using RecordBatch recordBatch = arrowArrayStream.ReadNextRecordBatchAsync().Result;

                StringArray stringArray = (StringArray)recordBatch.Column("table_type");

                List<string> known_types = environment.TableTypes;

                int results = 0;

                for (int i = 0; i < stringArray.Length; i++)
                {
                    string value = stringArray.GetString(i);

                    if (known_types.Contains(value))
                    {
                        results++;
                    }
                }

                Assert.True(known_types.Count == results, "TableTypes not equal in the [" + environment.Name + "] environment");
            }
        }

        /// <summary>
        /// Validates if the driver can connect to a live server and
        /// parse the results.
        /// </summary>
        [SkippableFact, Order(6)]
        public void CanExecuteQuery()
        {
            foreach (FlightSqlTestEnvironment environment in _environments)
            {
                using AdbcStatement statement = GetAdbcConnection(environment.Name).CreateStatement();
                statement.SqlQuery = environment.Query;

                QueryResult queryResult = statement.ExecuteQuery();

                Tests.DriverTests.CanExecuteQuery(queryResult, environment.ExpectedResultsCount);
            }
        }

        private void CreateDatabaseAndTable(FlightSqlTestEnvironment environment, string databaseName, string schemaName, string tableName)
        {
            databaseName = databaseName.Replace("\"", "\"\"");
            schemaName = schemaName.Replace("\"", "\"\"");
            tableName = tableName.Replace("\"", "\"\"");

            string createDatabase = string.Format("CREATE DATABASE IF NOT EXISTS \"{0}\"", databaseName);
            ExecuteUpdateStatement(environment, createDatabase);

            string createSchema = string.Format("CREATE SCHEMA IF NOT EXISTS \"{0}\".\"{1}\"", databaseName, schemaName);
            ExecuteUpdateStatement(environment, createSchema);

            string fullyQualifiedTableName = string.Format("\"{0}\".\"{1}\".\"{2}\"", databaseName, schemaName, tableName);
            string createTableStatement = string.Format("CREATE OR REPLACE TABLE {0} (INDEX INT)", fullyQualifiedTableName);
            ExecuteUpdateStatement(environment, createTableStatement);

        }

        private void DropDatabaseAndTable(FlightSqlTestEnvironment environment, string databaseName, string schemaName, string tableName)
        {
            tableName = tableName.Replace("\"", "\"\"");
            schemaName = schemaName.Replace("\"", "\"\"");
            databaseName = databaseName.Replace("\"", "\"\"");

            string fullyQualifiedTableName = string.Format("\"{0}\".\"{1}\".\"{2}\"", databaseName, schemaName, tableName);
            string createTableStatement = string.Format("DROP TABLE IF EXISTS {0} ", fullyQualifiedTableName);
            ExecuteUpdateStatement(environment, createTableStatement);

            string createSchema = string.Format("DROP SCHEMA IF EXISTS \"{0}\".\"{1}\"", databaseName, schemaName);
            ExecuteUpdateStatement(environment, createSchema);

            string createDatabase = string.Format("DROP DATABASE IF EXISTS \"{0}\"", databaseName);
            ExecuteUpdateStatement(environment, createDatabase);

        }

        private UpdateResult ExecuteUpdateStatement(FlightSqlTestEnvironment environment, string query)
        {
            using AdbcStatement statement = GetAdbcConnection(environment.Name).CreateStatement();
            statement.SqlQuery = query;
            UpdateResult updateResult = statement.ExecuteUpdate();
            return updateResult;
        }

        private AdbcConnection GetAdbcConnection(string? environmentName)
        {
            if (string.IsNullOrEmpty(environmentName))
            {
                throw new ArgumentNullException(nameof(environmentName));
            }

            return _configuredConnections[environmentName!];
        }

        private static string GetPartialNameForPatternMatch(string name)
        {
            if (string.IsNullOrEmpty(name) || name.Length == 1) return name;

            return name.Substring(0, name.Length / 2);
        }

        private static string GetNameWithoutFirstChatacter(string name)
        {
            if (string.IsNullOrEmpty(name)) return name;

            return name.Substring(1);
        }

        public void Dispose()
        {
            foreach (AdbcConnection configuredConnection in this._configuredConnections.Values)
            {
                configuredConnection.Dispose();
            }

            _configuredConnections.Clear();
        }
    }
}