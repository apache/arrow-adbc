/*
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
using Apache.Arrow.Adbc.Tests.Metadata;
using Apache.Arrow.Adbc.Tests.Xunit;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;
using Xunit;

namespace Apache.Arrow.Adbc.Tests.Drivers.Interop.Snowflake
{
    /// <summary>
    /// Class for testing the Snowflake ADBC driver connection tests.
    /// </summary>
    /// <remarks>
    /// Tests are ordered to ensure data is created for the other
    /// queries to run.
    /// </remarks>
    [TestCaseOrderer("Apache.Arrow.Adbc.Tests.Xunit.TestOrderer", "Apache.Arrow.Adbc.Tests")]
    public class DriverTests : IDisposable
    {
        readonly SnowflakeTestConfiguration _testConfiguration;
        readonly AdbcDriver _snowflakeDriver;
        readonly AdbcDatabase _database;
        readonly AdbcConnection _connection;
        readonly List<string> _tableTypes;

        public static IEnumerable<object[]> GetPatterns(string? name)
        {
            if (string.IsNullOrEmpty(name)) yield break;

            yield return new object[] { name! };
            yield return new object[] { $"{DriverTests.GetPartialNameForPatternMatch(name!)}%" };
            yield return new object[] { $"{DriverTests.GetPartialNameForPatternMatch(name!).ToLower()}%" };
            yield return new object[] { $"{DriverTests.GetPartialNameForPatternMatch(name!).ToUpper()}%" };
            yield return new object[] { $"_{DriverTests.GetNameWithoutFirstChatacter(name!)}" };
            yield return new object[] { $"_{DriverTests.GetNameWithoutFirstChatacter(name!).ToLower()}" };
            yield return new object[] { $"_{DriverTests.GetNameWithoutFirstChatacter(name!).ToUpper()}" };
        }

        public static IEnumerable<object[]> CatalogNamePatternData()
        {
            string? databaseName = SnowflakeTestingUtils.TestConfiguration?.Metadata.Catalog;
            return GetPatterns(databaseName);
        }

        public static IEnumerable<object[]> DbSchemasNamePatternData()
        {
            string? dbSchemaName = SnowflakeTestingUtils.TestConfiguration?.Metadata.Schema;
            return GetPatterns(dbSchemaName);
        }

        public static IEnumerable<object[]> TableNamePatternData()
        {
            string? tableName = SnowflakeTestingUtils.TestConfiguration?.Metadata.Table;
            return GetPatterns(tableName);
        }

        public DriverTests()
        {
            Skip.IfNot(Utils.CanExecuteTestConfig(SnowflakeTestingUtils.SNOWFLAKE_TEST_CONFIG_VARIABLE));
            _testConfiguration = SnowflakeTestingUtils.TestConfiguration;

            _tableTypes = new List<string> { "BASE TABLE", "VIEW" };
            Dictionary<string, string> parameters = new Dictionary<string, string>();
            Dictionary<string, string> options = new Dictionary<string, string>();
            _snowflakeDriver = SnowflakeTestingUtils.GetSnowflakeAdbcDriver(_testConfiguration, out parameters);

            _database = _snowflakeDriver.Open(parameters);
            _connection = _database.Connect(options);
        }

        /// <summary>
        /// Validates if the DEFAULT_ROLE works correctly for ADBC.
        /// </summary>
        [SkippableFact, Order(1)]
        public void ValidateUserRole()
        {
            Skip.If(_testConfiguration.RoleInfo == null);

            // first test with the DEFAULT_ROLE value
            Assert.True(CurrentRoleIsExpectedRole(_connection, _testConfiguration.RoleInfo.DefaultRole)); ;

            // now connect with the new role and ensure we get the new role successfully
            Dictionary<string, string> parameters = new Dictionary<string, string>();
            Dictionary<string, string> options = new Dictionary<string, string>();

            using AdbcDriver localSnowflakeDriver = SnowflakeTestingUtils.GetSnowflakeAdbcDriver(_testConfiguration, out parameters);
            parameters.Add(SnowflakeParameters.ROLE, _testConfiguration.RoleInfo.NewRole);

            using AdbcDatabase localDatabase = localSnowflakeDriver.Open(parameters);
            using AdbcConnection localConnection = localDatabase.Connect(options);
            Assert.True(CurrentRoleIsExpectedRole(localConnection, _testConfiguration.RoleInfo.NewRole));
        }

        private bool CurrentRoleIsExpectedRole(AdbcConnection cn, string expectedRole)
        {
            using AdbcStatement statement = cn.CreateStatement();
            statement.SqlQuery = "SELECT CURRENT_ROLE() as CURRENT_ROLE;";

            QueryResult queryResult = statement.ExecuteQuery();
            using RecordBatch? recordBatch = queryResult.Stream?.ReadNextRecordBatchAsync().Result;
            Assert.True(recordBatch != null);

            StringArray stringArray = (StringArray)recordBatch.Column("CURRENT_ROLE");
            Assert.True(stringArray.Length > 0);

            return expectedRole == stringArray.GetString(0);
        }

        [SkippableFact, Order(1)]
        public void CanSetDatabase()
        {
            Skip.If(string.IsNullOrEmpty(_testConfiguration.Metadata.Catalog));

            // connect without the parameter and ensure we get the DATABASE successfully
            Dictionary<string, string> parameters = new Dictionary<string, string>();
            Dictionary<string, string> options = new Dictionary<string, string>();

            using AdbcDriver localSnowflakeDriver = SnowflakeTestingUtils.GetSnowflakeAdbcDriver(_testConfiguration, out parameters);
            parameters.Remove(SnowflakeParameters.DATABASE);
            using AdbcDatabase localDatabase = localSnowflakeDriver.Open(parameters);
            using AdbcConnection localConnection = localDatabase.Connect(options);

            localConnection.SetOption(AdbcOptions.Connection.CurrentCatalog, _testConfiguration.Metadata.Catalog);

            Assert.True(CurrentDatabaseIsExpectedCatalog(localConnection, _testConfiguration.Metadata.Catalog));

            localConnection.GetObjects(AdbcConnection.GetObjectsDepth.All, _testConfiguration.Metadata.Catalog, _testConfiguration.Metadata.Schema, _testConfiguration.Metadata.Table, _tableTypes, null);
        }

        private bool CurrentDatabaseIsExpectedCatalog(AdbcConnection cn, string expectedCatalog)
        {
            using AdbcStatement statement = cn.CreateStatement();
            statement.SqlQuery = "SELECT CURRENT_DATABASE() as CURRENT_DATABASE;"; // GetOption doesn't exist in 1.0, only 1.1

            QueryResult queryResult = statement.ExecuteQuery();
            using RecordBatch? recordBatch = queryResult.Stream?.ReadNextRecordBatchAsync().Result;
            Assert.True(recordBatch != null);

            StringArray stringArray = (StringArray)recordBatch.Column("CURRENT_DATABASE");
            Assert.True(stringArray.Length > 0);

            return expectedCatalog == stringArray.GetString(0);
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
            string[] queries = SnowflakeTestingUtils.GetQueries(_testConfiguration);

            List<int> expectedResults = new List<int>() { -1, 1, 1 };

            for (int i = 0; i < queries.Length; i++)
            {
                string query = queries[i];
                UpdateResult updateResult = ExecuteUpdateStatement(query);

                Assert.Equal(expectedResults[i], updateResult.AffectedRows);
            }
        }

        /// <summary>
        /// Validates if the driver can call GetInfo.
        /// </summary>
        [SkippableFact, Order(2)]
        public void CanGetInfo()
        {
            using IArrowArrayStream stream = _connection.GetInfo(new List<AdbcInfoCode>() { AdbcInfoCode.DriverName, AdbcInfoCode.DriverVersion, AdbcInfoCode.VendorName });
            using RecordBatch recordBatch = stream.ReadNextRecordBatchAsync().Result;
            UInt32Array infoNameArray = (UInt32Array)recordBatch.Column("info_name");

            List<string> expectedValues = new List<string>() { "DriverName", "DriverVersion", "VendorName" };

            for (int i = 0; i < infoNameArray.Length; i++)
            {
                AdbcInfoCode value = (AdbcInfoCode)infoNameArray.GetValue(i)!.Value;
                DenseUnionArray valueArray = (DenseUnionArray)recordBatch.Column("info_value");

                Assert.Contains(value.ToString(), expectedValues);

                StringArray stringArray = (StringArray)valueArray.Fields[0];
                Console.WriteLine($"{value}={stringArray.GetString(i)}");
            }
        }

        /// <summary>
        /// Validates if the driver can call GetObjects with GetObjectsDepth as Catalogs and CatalogName passed as a pattern.
        /// </summary>
        [SkippableTheory, Order(3)]
        [MemberData(nameof(CatalogNamePatternData))]
        public void CanGetObjectsCatalogs(string catalogPattern)
        {
            string? databaseName = _testConfiguration.Metadata.Catalog;
            string? schemaName = _testConfiguration.Metadata.Schema;

            using IArrowArrayStream stream = _connection.GetObjects(
                    depth: AdbcConnection.GetObjectsDepth.Catalogs,
                    catalogPattern: catalogPattern,
                    dbSchemaPattern: null,
                    tableNamePattern: null,
                    tableTypes: _tableTypes,
                    columnNamePattern: null);

            using RecordBatch recordBatch = stream.ReadNextRecordBatchAsync().Result;

            List<AdbcCatalog> catalogs = GetObjectsParser.ParseCatalog(recordBatch, null);
            AdbcCatalog? catalog = catalogs.Where((catalog) => string.Equals(catalog.Name, databaseName)).FirstOrDefault();

            Assert.True(catalog != null, "catalog should not be null");
        }

        /// <summary>
        /// Validates if the driver can call GetObjects with GetObjectsDepth as DbSchemas with DbSchemaName as a pattern.
        /// </summary>
        [SkippableTheory, Order(3)]
        [MemberData(nameof(DbSchemasNamePatternData))]
        public void CanGetObjectsDbSchemas(string dbSchemaPattern)
        {
            // need to add the database
            string? databaseName = _testConfiguration.Metadata.Catalog;
            string? schemaName = _testConfiguration.Metadata.Schema;

            using IArrowArrayStream stream = _connection.GetObjects(
                    depth: AdbcConnection.GetObjectsDepth.DbSchemas,
                    catalogPattern: databaseName,
                    dbSchemaPattern: dbSchemaPattern,
                    tableNamePattern: null,
                    tableTypes: _tableTypes,
                    columnNamePattern: null);

            using RecordBatch recordBatch = stream.ReadNextRecordBatchAsync().Result;

            List<AdbcCatalog> catalogs = GetObjectsParser.ParseCatalog(recordBatch, schemaName);

            List<AdbcDbSchema>? dbSchemas = catalogs
                .Where(c => string.Equals(c.Name, databaseName))
                .Select(c => c.DbSchemas)
                .FirstOrDefault();
            AdbcDbSchema? dbSchema = dbSchemas?.Where((dbSchema) => string.Equals(dbSchema.Name, schemaName)).FirstOrDefault();

            Assert.True(dbSchema != null, "dbSchema should not be null");
        }

        /// <summary>
        /// Validates if the driver can call GetObjects with GetObjectsDepth as Tables with TableName as a pattern.
        /// </summary>
        [SkippableTheory, Order(3)]
        [MemberData(nameof(TableNamePatternData))]
        public void CanGetObjectsTables(string tableNamePattern)
        {
            // need to add the database
            string? databaseName = _testConfiguration.Metadata.Catalog;
            string? schemaName = _testConfiguration.Metadata.Schema;
            string? tableName = _testConfiguration.Metadata.Table;

            using IArrowArrayStream stream = _connection.GetObjects(
                    depth: AdbcConnection.GetObjectsDepth.Tables,
                    catalogPattern: databaseName,
                    dbSchemaPattern: schemaName,
                    tableNamePattern: tableNamePattern,
                    tableTypes: _tableTypes,
                    columnNamePattern: null);

            using RecordBatch recordBatch = stream.ReadNextRecordBatchAsync().Result;

            List<AdbcCatalog> catalogs = GetObjectsParser.ParseCatalog(recordBatch, schemaName);

            List<AdbcTable>? tables = catalogs
                .Where(c => string.Equals(c.Name, databaseName))
                .Select(c => c.DbSchemas)
                .FirstOrDefault()
                ?.Where(s => string.Equals(s.Name, schemaName))
                .Select(s => s.Tables)
                .FirstOrDefault();

            AdbcTable? table = tables?.Where((table) => string.Equals(table.Name, tableName)).FirstOrDefault();
            Assert.True(table != null, "table should not be null");
            Assert.Equal("BASE TABLE", table.Type);
        }

        /// <summary>
        /// Validates if the driver can call GetObjects for GetObjectsDepth as All.
        /// </summary>
        [SkippableFact, Order(3)]
        public void CanGetObjectsAll()
        {
            // need to add the database
            string? databaseName = _testConfiguration.Metadata.Catalog;
            string? schemaName = _testConfiguration.Metadata.Schema;
            string? tableName = _testConfiguration.Metadata.Table;
            string? columnName = null;

            using IArrowArrayStream stream = _connection.GetObjects(
                    depth: AdbcConnection.GetObjectsDepth.All,
                    catalogPattern: databaseName,
                    dbSchemaPattern: schemaName,
                    tableNamePattern: tableName,
                    tableTypes: _tableTypes,
                    columnNamePattern: columnName);

            using RecordBatch recordBatch = stream.ReadNextRecordBatchAsync().Result;

            List<AdbcCatalog> catalogs = GetObjectsParser.ParseCatalog(recordBatch, schemaName);
            AdbcTable? table = catalogs
                .Where(c => string.Equals(c.Name, databaseName))
                .Select(c => c.DbSchemas)
                .FirstOrDefault()
                ?.Where(s => string.Equals(s.Name, schemaName))
                .Select(s => s.Tables)
                .FirstOrDefault()
                ?.Where(t => string.Equals(t.Name, tableName))
                .FirstOrDefault();


            Assert.True(table != null, "table should not be null");
            Assert.Equal("BASE TABLE", table.Type);
            List<AdbcColumn>? columns = table.Columns;

            Assert.True(columns != null, "Columns cannot be null");
            Assert.Equal(_testConfiguration.Metadata.ExpectedColumnCount, columns.Count);

            if (_testConfiguration.UseHighPrecision)
            {
                IEnumerable<AdbcColumn> highPrecisionColumns = columns.Where(c => c.XdbcTypeName == "NUMBER");

                if (highPrecisionColumns.Count() > 0)
                {
                    // ensure they all are coming back as XdbcDataType_XDBC_DECIMAL because they are Decimal128
                    short XdbcDataType_XDBC_DECIMAL = 3;
                    IEnumerable<AdbcColumn> invalidHighPrecisionColumns = highPrecisionColumns.Where(c => c.XdbcSqlDataType != XdbcDataType_XDBC_DECIMAL);
                    int count = invalidHighPrecisionColumns.Count();
                    Assert.True(count == 0, $"There are {count} columns that do not map to the correct XdbcSqlDataType when UseHighPrecision=true");
                }
            }
        }

        /// <summary>
        /// Validates if the driver can call GetObjects with GetObjectsDepth as Tables with TableName as a Special Character.
        /// </summary>
        [SkippableTheory, Order(3)]
        [InlineData(@"ADBCDEMO_DB",@"PUBLIC","MyIdentifier")]
        [InlineData(@"ADBCDEMO'DB", @"PUBLIC'SCHEMA","my.identifier")]
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
            CreateDatabaseAndTable(databaseName, schemaName, tableName);

            using IArrowArrayStream stream = _connection.GetObjects(
                    depth: AdbcConnection.GetObjectsDepth.Tables,
                    catalogPattern: databaseName,
                    dbSchemaPattern: schemaName,
                    tableNamePattern: tableName,
                    tableTypes: new List<string> { "BASE TABLE", "VIEW" },
                    columnNamePattern: null);

            using RecordBatch recordBatch = stream.ReadNextRecordBatchAsync().Result;

            List<AdbcCatalog> catalogs = GetObjectsParser.ParseCatalog(recordBatch, schemaName);

            List<AdbcTable>? tables = catalogs
                .Where(c => string.Equals(c.Name, databaseName))
                .Select(c => c.DbSchemas)
                .FirstOrDefault()
                ?.Where(s => string.Equals(s.Name, schemaName))
                .Select(s => s.Tables)
                .FirstOrDefault();

            AdbcTable? table = tables?.FirstOrDefault();

            Assert.True(table != null, "table should not be null");
            Assert.Equal(tableName, table.Name, true);
            DropDatabaseAndTable(databaseName, schemaName, tableName);
        }

        /// <summary>
        /// Validates if the driver can call GetTableSchema.
        /// </summary>
        [SkippableFact, Order(4)]
        public void CanGetTableSchema()
        {
            string? databaseName = _testConfiguration.Metadata.Catalog;
            string? schemaName = _testConfiguration.Metadata.Schema;
            string tableName = _testConfiguration.Metadata.Table;

            Schema schema = _connection.GetTableSchema(databaseName, schemaName, tableName);

            int numberOfFields = schema.FieldsList.Count;

            Assert.Equal(_testConfiguration.Metadata.ExpectedColumnCount, numberOfFields);
        }

        /// <summary>
        /// Validates if the driver can call GetTableTypes.
        /// </summary>
        [SkippableFact, Order(5)]
        public void CanGetTableTypes()
        {
            using IArrowArrayStream arrowArrayStream = _connection.GetTableTypes();

            using RecordBatch recordBatch = arrowArrayStream.ReadNextRecordBatchAsync().Result;

            StringArray stringArray = (StringArray)recordBatch.Column("table_type");

            List<string> known_types = new List<string>
            {
                "BASE TABLE", "TEMPORARY TABLE", "VIEW"
            };

            int results = 0;

            for (int i = 0; i < stringArray.Length; i++)
            {
                string value = stringArray.GetString(i);

                if (known_types.Contains(value))
                {
                    results++;
                }
            }

            Assert.Equal(known_types.Count, results);
        }

        /// <summary>
        /// Validates if the driver can connect to a live server and
        /// parse the results.
        /// </summary>
        [SkippableFact, Order(6)]
        public void CanExecuteQuery()
        {
            using AdbcStatement statement = _connection.CreateStatement();
            statement.SqlQuery = _testConfiguration.Query;

            QueryResult queryResult = statement.ExecuteQuery();

            Tests.DriverTests.CanExecuteQuery(queryResult, _testConfiguration.ExpectedResultsCount);
        }

        /// <summary>
        /// Validates if the driver can connect to a live server and execute a parameterized query.
        /// </summary>
        [SkippableFact, Order(6)]
        public void CanExecuteParameterizedQuery()
        {
            using AdbcStatement statement = _connection.CreateStatement();
            statement.SqlQuery = "SELECT * FROM (SELECT column1 FROM (VALUES (1), (2), (3))) WHERE column1 < ?";

            Schema parameterSchema = new Schema(new[] { new Field("column1", Int32Type.Default, false) }, null);
            RecordBatch parameters = new RecordBatch(parameterSchema, new[] { new Int32Array.Builder().Append(2).Build() }, 1);
            statement.Bind(parameters, parameterSchema);

            QueryResult queryResult = statement.ExecuteQuery();

            Tests.DriverTests.CanExecuteQuery(queryResult, 1);
        }

        [SkippableFact, Order(7)]
        public void CanIngestData()
        {
            const string tableName = "AdbcIngestTest";

            using var statement = _connection.CreateStatement();
            statement.SqlQuery = $"USE SCHEMA \"{_testConfiguration.Metadata.Schema}\"";
            statement.ExecuteUpdate();
            statement.SqlQuery = $"DROP TABLE IF EXISTS \"{tableName}\"";
            statement.ExecuteUpdate();

            statement.SqlQuery = null;
            statement.SetOption("adbc.ingest.target_table", tableName);
            statement.SetOption("adbc.ingest.mode", "adbc.ingest.mode.create");

            Schema schema = new Schema([new Field("key", Int32Type.Default, false), new Field("value", StringType.Default, false)], null);
            RecordBatch recordBatch = new RecordBatch(schema, [
                new Int32Array.Builder().AppendRange([1, 2, 3]).Build(),
                new StringArray.Builder().AppendRange(["foo", "bar", "baz"]).Build()
                ], 3);
            statement.Bind(recordBatch, schema);
            statement.ExecuteUpdate();

            Schema foundSchema = _connection.GetTableSchema(null, null, tableName);
            Assert.Equal(schema.FieldsList.Count, foundSchema.FieldsList.Count);

            statement.SqlQuery = $"SELECT * FROM \"{tableName}\"";
            var result = statement.ExecuteQuery();
            Assert.Equal(3, result.RowCount);
            result.Stream?.Dispose();

            using var statement2 = _connection.BulkIngest(tableName, BulkIngestMode.Append);

            recordBatch = new RecordBatch(schema, [
                new Int32Array.Builder().AppendRange([4, 5]).Build(),
                new StringArray.Builder().AppendRange(["quux", "zozzle"]).Build()
                ], 2);
            statement2.Bind(recordBatch, schema);
            statement2.ExecuteUpdate();

            statement.SqlQuery = $"SELECT * FROM \"{tableName}\"";
            result = statement.ExecuteQuery();
            Assert.Equal(5, result.RowCount);
            result.Stream?.Dispose();

            statement.SqlQuery = $"DROP TABLE IF EXISTS \"{tableName}\"";
            statement.ExecuteUpdate();
        }


        private void CreateDatabaseAndTable(string databaseName, string schemaName, string tableName)
        {
            databaseName = databaseName.Replace("\"", "\"\"");
            schemaName = schemaName.Replace("\"", "\"\"");
            tableName = tableName.Replace("\"", "\"\"");

            string createDatabase = string.Format("CREATE DATABASE IF NOT EXISTS \"{0}\"", databaseName);
            ExecuteUpdateStatement(createDatabase);

            string createSchema = string.Format("CREATE SCHEMA IF NOT EXISTS \"{0}\".\"{1}\"", databaseName, schemaName);
            ExecuteUpdateStatement(createSchema);

            string fullyQualifiedTableName = string.Format("\"{0}\".\"{1}\".\"{2}\"", databaseName, schemaName, tableName);
            string createTableStatement = string.Format("CREATE OR REPLACE TABLE {0} (INDEX INT)", fullyQualifiedTableName);
            ExecuteUpdateStatement(createTableStatement);

        }

        private void DropDatabaseAndTable(string databaseName, string schemaName, string tableName)
        {
            tableName = tableName.Replace("\"", "\"\"");
            schemaName = schemaName.Replace("\"", "\"\"");
            databaseName = databaseName.Replace("\"", "\"\"");

            string fullyQualifiedTableName = string.Format("\"{0}\".\"{1}\".\"{2}\"", databaseName, schemaName, tableName);
            string createTableStatement = string.Format("DROP TABLE IF EXISTS {0} ", fullyQualifiedTableName);
            ExecuteUpdateStatement(createTableStatement);

            string createSchema = string.Format("DROP SCHEMA IF EXISTS \"{0}\".\"{1}\"", databaseName, schemaName);
            ExecuteUpdateStatement(createSchema);

            string createDatabase = string.Format("DROP DATABASE IF EXISTS \"{0}\"", databaseName);
            ExecuteUpdateStatement(createDatabase);

        }

        private UpdateResult ExecuteUpdateStatement(string query)
        {
            using AdbcStatement statement = _connection.CreateStatement();
            statement.SqlQuery = query;
            UpdateResult updateResult = statement.ExecuteUpdate();
            return updateResult;
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
            _connection.Dispose();
            _database.Dispose();
            _snowflakeDriver.Dispose();
        }
    }
}
