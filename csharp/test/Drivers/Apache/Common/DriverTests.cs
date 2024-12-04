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
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Apache.Arrow.Adbc.Drivers.Apache.Spark;
using Apache.Arrow.Adbc.Tests.Drivers.Apache.Hive2;
using Apache.Arrow.Adbc.Tests.Metadata;
using Apache.Arrow.Adbc.Tests.Xunit;
using Apache.Arrow.Ipc;
using Xunit;
using Xunit.Abstractions;
using ColumnTypeId = Apache.Arrow.Adbc.Drivers.Apache.Spark.SparkConnection.ColumnTypeId;

namespace Apache.Arrow.Adbc.Tests.Drivers.Apache.Common
{
    /// <summary>
    /// Class for testing the Spark ADBC driver connection tests.
    /// </summary>
    /// <remarks>
    /// Tests are ordered to ensure data is created for the other
    /// queries to run.
    /// <para>Note: This test creates/replaces the table identified in the configuration (metadata/table).
    /// It uses the test collection "TableCreateTestCollection" to ensure it does not run
    /// as the same time as any other tests that may create/update the same table.</para>
    /// </remarks>
    [TestCaseOrderer("Apache.Arrow.Adbc.Tests.Xunit.TestOrderer", "Apache.Arrow.Adbc.Tests")]
    [Collection("TableCreateTestCollection")]
    public abstract class DriverTests<TConfig, TEnv> : TestBase<TConfig, TEnv>
        where TConfig : ApacheTestConfiguration
        where TEnv : HiveServer2TestEnvironment<TConfig>
    {
        /// <summary>
        /// Supported Spark data types as a subset of <see cref="ColumnTypeId"/>
        /// </summary>
        private enum SupportedSparkDataType : short
        {
            ARRAY = ColumnTypeId.ARRAY,
            BIGINT = ColumnTypeId.BIGINT,
            BINARY = ColumnTypeId.BINARY,
            BOOLEAN = ColumnTypeId.BOOLEAN,
            CHAR = ColumnTypeId.CHAR,
            DATE = ColumnTypeId.DATE,
            DECIMAL = ColumnTypeId.DECIMAL,
            DOUBLE = ColumnTypeId.DOUBLE,
            FLOAT = ColumnTypeId.FLOAT,
            INTEGER = ColumnTypeId.INTEGER,
            JAVA_OBJECT = ColumnTypeId.JAVA_OBJECT,
            LONGNVARCHAR = ColumnTypeId.LONGNVARCHAR,
            LONGVARBINARY = ColumnTypeId.LONGVARBINARY,
            LONGVARCHAR = ColumnTypeId.LONGVARCHAR,
            NCHAR = ColumnTypeId.NCHAR,
            NULL = ColumnTypeId.NULL,
            NUMERIC = ColumnTypeId.NUMERIC,
            NVARCHAR = ColumnTypeId.NVARCHAR,
            REAL = ColumnTypeId.REAL,
            SMALLINT = ColumnTypeId.SMALLINT,
            STRUCT = ColumnTypeId.STRUCT,
            TIMESTAMP = ColumnTypeId.TIMESTAMP,
            TINYINT = ColumnTypeId.TINYINT,
            VARBINARY = ColumnTypeId.VARBINARY,
            VARCHAR = ColumnTypeId.VARCHAR,
        }

        private static List<string> DefaultTableTypes => new() { "TABLE", "VIEW" };

        public DriverTests(ITestOutputHelper? outputHelper, TestEnvironment<TConfig>.Factory<TEnv> testEnvFactory)
            : base(outputHelper, testEnvFactory)
        {
            Skip.IfNot(Utils.CanExecuteTestConfig(TestConfigVariable));
        }

        /// <summary>
        /// Validates if the driver can execute update statements.
        /// </summary>
        [SkippableFact, Order(1)]
        public void CanExecuteUpdate()
        {
            AdbcConnection adbcConnection = NewConnection();

            string[] queries = GetQueries();

            //List<int> expectedResults = TestEnvironment.ServerType != SparkServerType.Databricks
            //    ?
            //    [
            //        -1, // CREATE TABLE
            //        1,  // INSERT
            //        1,  // INSERT
            //        1,  // INSERT
            //        //1,  // UPDATE
            //        //1,  // DELETE
            //    ]
            //    :
            //    [
            //        -1, // CREATE TABLE
            //        1,  // INSERT
            //        1,  // INSERT
            //        1,  // INSERT
            //        1,  // UPDATE
            //        1,  // DELETE
            //    ];

            var expectedResults = GetUpdateExpectedResults();
            for (int i = 0; i < queries.Length; i++)
            {
                string query = queries[i];
                using AdbcStatement statement = adbcConnection.CreateStatement();
                statement.SqlQuery = query;

                UpdateResult updateResult = statement.ExecuteUpdate();

                if (ValidateAffectedRows) Assert.Equal(expectedResults[i], updateResult.AffectedRows);
            }
        }

        protected abstract IReadOnlyList<int> GetUpdateExpectedResults();

        /// <summary>
        /// Validates if the driver can call GetInfo.
        /// </summary>
        [SkippableFact, Order(2)]
        public async Task CanGetInfo()
        {
            AdbcConnection adbcConnection = NewConnection();

            // Test the supported info codes
            List<AdbcInfoCode> handledCodes = new List<AdbcInfoCode>()
            {
                AdbcInfoCode.DriverName,
                AdbcInfoCode.DriverVersion,
                AdbcInfoCode.VendorName,
                AdbcInfoCode.DriverArrowVersion,
                AdbcInfoCode.VendorVersion,
                AdbcInfoCode.VendorSql
            };
            using IArrowArrayStream stream = adbcConnection.GetInfo(handledCodes);

            RecordBatch recordBatch = await stream.ReadNextRecordBatchAsync();
            UInt32Array infoNameArray = (UInt32Array)recordBatch.Column("info_name");

            List<string> expectedValues = new List<string>()
            {
                "DriverName",
                "DriverVersion",
                "VendorName",
                "DriverArrowVersion",
                "VendorVersion",
                "VendorSql"
            };

            for (int i = 0; i < infoNameArray.Length; i++)
            {
                AdbcInfoCode? value = (AdbcInfoCode?)infoNameArray.GetValue(i);
                DenseUnionArray valueArray = (DenseUnionArray)recordBatch.Column("info_value");

                Assert.Contains(value.ToString(), expectedValues);

                switch (value)
                {
                    case AdbcInfoCode.VendorSql:
                        // TODO: How does external developer know the second field is the boolean field?
                        BooleanArray booleanArray = (BooleanArray)valueArray.Fields[1];
                        bool? boolValue = booleanArray.GetValue(i);
                        OutputHelper?.WriteLine($"{value}={boolValue}");
                        Assert.True(boolValue);
                        break;
                    default:
                        StringArray stringArray = (StringArray)valueArray.Fields[0];
                        string stringValue = stringArray.GetString(i);
                        OutputHelper?.WriteLine($"{value}={stringValue}");
                        Assert.NotNull(stringValue);
                        break;
                }
            }

            // Test the unhandled info codes.
            List<AdbcInfoCode> unhandledCodes = new List<AdbcInfoCode>()
            {
                AdbcInfoCode.VendorArrowVersion,
                AdbcInfoCode.VendorSubstrait,
                AdbcInfoCode.VendorSubstraitMaxVersion
            };
            using IArrowArrayStream stream2 = adbcConnection.GetInfo(unhandledCodes);

            recordBatch = await stream2.ReadNextRecordBatchAsync();
            infoNameArray = (UInt32Array)recordBatch.Column("info_name");

            List<string> unexpectedValues = new List<string>()
            {
                "VendorArrowVersion",
                "VendorSubstrait",
                "VendorSubstraitMaxVersion"
            };
            for (int i = 0; i < infoNameArray.Length; i++)
            {
                AdbcInfoCode? value = (AdbcInfoCode?)infoNameArray.GetValue(i);
                DenseUnionArray valueArray = (DenseUnionArray)recordBatch.Column("info_value");

                Assert.Contains(value.ToString(), unexpectedValues);
                switch (value)
                {
                    case AdbcInfoCode.VendorSql:
                        BooleanArray booleanArray = (BooleanArray)valueArray.Fields[1];
                        Assert.Null(booleanArray.GetValue(i));
                        break;
                    default:
                        StringArray stringArray = (StringArray)valueArray.Fields[0];
                        Assert.Null(stringArray.GetString(i));
                        break;
                }
            }
        }

        /// <summary>
        /// Validates if the driver can call GetObjects with GetObjectsDepth as Catalogs with CatalogPattern as a pattern.
        /// </summary>
        /// <param name="pattern"></param>
        [SkippableFact, Order(3)]
        public abstract void CanGetObjectsCatalogs(string? pattern);

        /// <summary>
        /// Validates if the driver can call GetObjects with GetObjectsDepth as Catalogs with CatalogPattern as a pattern.
        /// </summary>
        /// <param name="pattern"></param>
        protected void GetObjectsCatalogsTest(string? pattern)
        {
            string? catalogName = TestConfiguration.Metadata.Catalog;
            string? schemaName = TestConfiguration.Metadata.Schema;

            using IArrowArrayStream stream = Connection.GetObjects(
                    depth: AdbcConnection.GetObjectsDepth.Catalogs,
                    catalogPattern: pattern,
                    dbSchemaPattern: null,
                    tableNamePattern: null,
                    tableTypes: DefaultTableTypes,
                    columnNamePattern: null);

            using RecordBatch recordBatch = stream.ReadNextRecordBatchAsync().Result;

            List<AdbcCatalog> catalogs = GetObjectsParser.ParseCatalog(recordBatch, schemaName);
            AdbcCatalog? catalog = catalogs.Where((catalog) => string.Equals(catalog.Name, catalogName)).FirstOrDefault();

            Assert.True(pattern == string.Empty && catalog == null || catalog != null, "catalog should not be null");
        }


        /// <summary>
        /// Validates if the driver can call GetObjects with GetObjectsDepth as DbSchemas with DbSchemaName as a pattern.
        /// </summary>
        [SkippableFact, Order(4)]
        public abstract void CanGetObjectsDbSchemas(string dbSchemaPattern);

        /// <summary>
                                                                          /// Validates if the driver can call GetObjects with GetObjectsDepth as DbSchemas with DbSchemaName as a pattern.
                                                                          /// </summary>
        protected void GetObjectsDbSchemasTest(string dbSchemaPattern)
        {
            // need to add the database
            string? databaseName = TestConfiguration.Metadata.Catalog;
            string? schemaName = TestConfiguration.Metadata.Schema;

            using IArrowArrayStream stream = Connection.GetObjects(
                    depth: AdbcConnection.GetObjectsDepth.DbSchemas,
                    catalogPattern: databaseName,
                    dbSchemaPattern: dbSchemaPattern,
                    tableNamePattern: null,
                    tableTypes: DefaultTableTypes,
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
        [SkippableFact, Order(5)]
        public abstract void CanGetObjectsTables(string tableNamePattern);

        /// <summary>
        /// Validates if the driver can call GetObjects with GetObjectsDepth as Tables with TableName as a pattern.
        /// </summary>
        protected void GetObjectsTablesTest(string tableNamePattern)
        {
            // need to add the database
            string? databaseName = TestConfiguration.Metadata.Catalog;
            string? schemaName = TestConfiguration.Metadata.Schema;
            string? tableName = TestConfiguration.Metadata.Table;

            using IArrowArrayStream stream = Connection.GetObjects(
                    depth: AdbcConnection.GetObjectsDepth.Tables,
                    catalogPattern: databaseName,
                    dbSchemaPattern: schemaName,
                    tableNamePattern: tableNamePattern,
                    tableTypes: DefaultTableTypes,
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
            // TODO: Determine why this is returned blank.
            //Assert.Equal("TABLE", table.Type);
        }

        /// <summary>
        /// Validates if the driver can call GetObjects for GetObjectsDepth as All.
        /// </summary>
        [SkippableFact, Order(6)]
        public void CanGetObjectsAll()
        {
            // need to add the database
            string? databaseName = TestConfiguration.Metadata.Catalog;
            string? schemaName = TestConfiguration.Metadata.Schema;
            string? tableName = TestConfiguration.Metadata.Table;
            string? columnName = null;

            using IArrowArrayStream stream = Connection.GetObjects(
                    depth: AdbcConnection.GetObjectsDepth.All,
                    catalogPattern: databaseName,
                    dbSchemaPattern: schemaName,
                    tableNamePattern: tableName,
                    tableTypes: DefaultTableTypes,
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
            // TODO: Determine why this is returned blank.
            //Assert.Equal("TABLE", table.Type);
            List<AdbcColumn>? columns = table.Columns;

            Assert.True(columns != null, "Columns cannot be null");
            Assert.Equal(TestConfiguration.Metadata.ExpectedColumnCount, columns.Count);

            for (int i = 0; i < columns.Count; i++)
            {
                // Verify column metadata is returned/consistent.
                AdbcColumn column = columns[i];
                Assert.Equal(i + 1, column.OrdinalPosition);
                Assert.False(string.IsNullOrEmpty(column.Name));
                Assert.False(string.IsNullOrEmpty(column.XdbcTypeName));
                Assert.False(Regex.IsMatch(column.XdbcTypeName, @"[_,\d\<\>\(\)]", RegexOptions.IgnoreCase | RegexOptions.CultureInvariant),
                    "Unexpected character found in field XdbcTypeName");

                var supportedTypes = Enum.GetValues(typeof(SupportedSparkDataType)).Cast<SupportedSparkDataType>();
                Assert.Contains((SupportedSparkDataType)column.XdbcSqlDataType!, supportedTypes);
                Assert.Equal(column.XdbcDataType, column.XdbcSqlDataType);

                Assert.NotNull(column.XdbcDataType);
                Assert.Contains((SupportedSparkDataType)column.XdbcDataType!, supportedTypes);

                HashSet<short> typesHaveColumnSize = new()
                {
                    (short)SupportedSparkDataType.DECIMAL,
                    (short)SupportedSparkDataType.NUMERIC,
                    (short)SupportedSparkDataType.CHAR,
                    (short)SupportedSparkDataType.VARCHAR,
                };
                HashSet<short> typesHaveDecimalDigits = new()
                {
                    (short)SupportedSparkDataType.DECIMAL,
                    (short)SupportedSparkDataType.NUMERIC,
                };

                bool typeHasColumnSize = typesHaveColumnSize.Contains(column.XdbcDataType.Value);
                Assert.Equal(column.XdbcColumnSize.HasValue, typeHasColumnSize);

                bool typeHasDecimalDigits = typesHaveDecimalDigits.Contains(column.XdbcDataType.Value);
                Assert.Equal(column.XdbcDecimalDigits.HasValue, typeHasDecimalDigits);

                Assert.False(string.IsNullOrEmpty(column.Remarks));

                Assert.NotNull(column.XdbcColumnDef);

                Assert.NotNull(column.XdbcNullable);
                Assert.Contains(new short[] { 1, 0 }, i => i == column.XdbcNullable);

                Assert.NotNull(column.XdbcIsNullable);
                Assert.Contains(new string[] { "YES", "NO" }, i => i.Equals(column.XdbcIsNullable));

                Assert.NotNull(column.XdbcIsAutoIncrement);

                Assert.Null(column.XdbcCharOctetLength);
                Assert.Null(column.XdbcDatetimeSub);
                Assert.Null(column.XdbcNumPrecRadix);
                Assert.Null(column.XdbcScopeCatalog);
                Assert.Null(column.XdbcScopeSchema);
                Assert.Null(column.XdbcScopeTable);
            }
        }

        /// <summary>
        /// Validates if the driver can call GetObjects with GetObjectsDepth as Tables with TableName as a Special Character.
        /// </summary>
        [SkippableTheory, Order(7)]
        [InlineData("MyIdentifier")]
        [InlineData("ONE")]
        [InlineData("mYiDentifier")]
        [InlineData("3rd_identifier")]
        // Note: Tables in 'hive_metastore' only support ASCII alphabetic, numeric and underscore.
        public void CanGetObjectsTablesWithSpecialCharacter(string tableName)
        {
            string catalogName = TestConfiguration.Metadata.Catalog;
            string schemaPrefix = Guid.NewGuid().ToString().Replace("-", "");
            using TemporarySchema schema = TemporarySchema.NewTemporarySchemaAsync(catalogName, Statement).Result;
            string schemaName = schema.SchemaName;
            string catalogFormatted = string.IsNullOrEmpty(catalogName) ? string.Empty : DelimitIdentifier(catalogName) + ".";
            string fullTableName = $"{catalogFormatted}{DelimitIdentifier(schemaName)}.{DelimitIdentifier(tableName)}";
            using TemporaryTable temporaryTable = TemporaryTable.NewTemporaryTableAsync(Statement, fullTableName, $"CREATE TABLE IF NOT EXISTS {fullTableName} (INDEX INT)").Result;

            using IArrowArrayStream stream = Connection.GetObjects(
                    depth: AdbcConnection.GetObjectsDepth.Tables,
                    catalogPattern: catalogName,
                    dbSchemaPattern: schemaName,
                    tableNamePattern: tableName,
                    tableTypes: DefaultTableTypes,
                    columnNamePattern: null);

            using RecordBatch recordBatch = stream.ReadNextRecordBatchAsync().Result;

            List<AdbcCatalog> catalogs = GetObjectsParser.ParseCatalog(recordBatch, schemaName);

            List<AdbcTable>? tables = catalogs
                .Where(c => string.Equals(c.Name, catalogName))
                .Select(c => c.DbSchemas)
                .FirstOrDefault()
                ?.Where(s => string.Equals(s.Name, schemaName))
                .Select(s => s.Tables)
                .FirstOrDefault();

            AdbcTable? table = tables?.FirstOrDefault();

            Assert.True(table != null, "table should not be null");
            Assert.Equal(tableName, table.Name, true);
        }

        /// <summary>
        /// Validates if the driver can call GetTableSchema.
        /// </summary>
        [SkippableFact, Order(8)]
        public void CanGetTableSchema()
        {
            AdbcConnection adbcConnection = NewConnection();

            string? catalogName = TestConfiguration.Metadata.Catalog;
            string? schemaName = TestConfiguration.Metadata.Schema;
            string tableName = TestConfiguration.Metadata.Table!;

            Schema schema = adbcConnection.GetTableSchema(catalogName, schemaName, tableName);

            int numberOfFields = schema.FieldsList.Count;

            Assert.Equal(TestConfiguration.Metadata.ExpectedColumnCount, numberOfFields);
        }

        /// <summary>
        /// Validates if the driver can call GetTableTypes.
        /// </summary>
        [SkippableFact, Order(9)]
        public async Task CanGetTableTypes()
        {
            AdbcConnection adbcConnection = NewConnection();

            using IArrowArrayStream arrowArrayStream = adbcConnection.GetTableTypes();

            RecordBatch recordBatch = await arrowArrayStream.ReadNextRecordBatchAsync();

            StringArray stringArray = (StringArray)recordBatch.Column("table_type");

            List<string> known_types = new List<string>
            {
                "TABLE", "VIEW"
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
        [SkippableTheory, Order(10)]
        [InlineData(0.1)]
        [InlineData(0.25)]
        [InlineData(1.0)]
        [InlineData(2.0)]
        [InlineData(null)]
        public void CanExecuteQuery(double? batchSizeFactor)
        {
            // Ensure all records can be retrieved, independent of the batch size.
            TConfig testConfiguration = (TConfig)TestConfiguration.Clone();
            long expectedResultCount = testConfiguration.ExpectedResultsCount;
            long nonZeroExpectedResultCount = expectedResultCount == 0 ? 1 : expectedResultCount;
            testConfiguration.BatchSize = batchSizeFactor != null ? ((long)(nonZeroExpectedResultCount * batchSizeFactor)).ToString() : string.Empty;
            OutputHelper?.WriteLine($"BatchSize: {testConfiguration.BatchSize}. ExpectedResultCount: {expectedResultCount}");

            using AdbcConnection adbcConnection = NewConnection(testConfiguration);

            using AdbcStatement statement = adbcConnection.CreateStatement();
            statement.SqlQuery = TestConfiguration.Query;
            OutputHelper?.WriteLine(statement.SqlQuery);

            QueryResult queryResult = statement.ExecuteQuery();

            Tests.DriverTests.CanExecuteQuery(queryResult, TestConfiguration.ExpectedResultsCount);
        }

        /// <summary>
        /// Validates if the driver can connect to a live server and
        /// parse the results using the asynchronous methods.
        /// </summary>
        [SkippableFact, Order(11)]
        public async Task CanExecuteQueryAsync()
        {
            using AdbcConnection adbcConnection = NewConnection();
            using AdbcStatement statement = adbcConnection.CreateStatement();

            statement.SqlQuery = TestConfiguration.Query;
            QueryResult queryResult = await statement.ExecuteQueryAsync();

            await Tests.DriverTests.CanExecuteQueryAsync(queryResult, TestConfiguration.ExpectedResultsCount);
        }

        /// <summary>
        /// Validates if the driver can connect to a live server and
        /// perform and update asynchronously.
        /// </summary>
        [SkippableFact, Order(12)]
        public async Task CanExecuteUpdateAsync()
        {
            using AdbcConnection adbcConnection = NewConnection();
            using AdbcStatement statement = adbcConnection.CreateStatement();
            using TemporaryTable temporaryTable = await NewTemporaryTableAsync(statement, "INDEX INT");

            statement.SqlQuery = GetInsertStatement(temporaryTable.TableName, "INDEX", "1");
            UpdateResult updateResult = await statement.ExecuteUpdateAsync();

            if (ValidateAffectedRows) Assert.Equal(1, updateResult.AffectedRows);
        }

        [SkippableFact, Order(13)]
        public void CanDetectInvalidAuthentication()
        {
            AdbcDriver driver = NewDriver;
            Assert.NotNull(driver);
            Dictionary<string, string> parameters = GetDriverParameters(TestConfiguration);

            bool hasToken = parameters.TryGetValue(SparkParameters.Token, out var token) && !string.IsNullOrEmpty(token);
            bool hasUsername = parameters.TryGetValue(AdbcOptions.Username, out var username) && !string.IsNullOrEmpty(username);
            bool hasPassword = parameters.TryGetValue(AdbcOptions.Password, out var password) && !string.IsNullOrEmpty(password);
            if (hasToken)
            {
                parameters[SparkParameters.Token] = "invalid-token";
            }
            else if (hasUsername && hasPassword)
            {
                parameters[AdbcOptions.Password] = "invalid-password";
            }
            else
            {
                Assert.Fail($"Unexpected configuration. Must provide '{SparkParameters.Token}' or '{AdbcOptions.Username}' and '{AdbcOptions.Password}'.");
            }

            AdbcDatabase database = driver.Open(parameters);
            AggregateException exception = Assert.ThrowsAny<AggregateException>(() => database.Connect(parameters));
            OutputHelper?.WriteLine(exception.Message);
        }

        [SkippableFact, Order(14)]
        public void CanDetectInvalidServer()
        {
            AdbcDriver driver = NewDriver;
            Assert.NotNull(driver);
            Dictionary<string, string> parameters = GetDriverParameters(TestConfiguration);

            bool hasUri = parameters.TryGetValue(AdbcOptions.Uri, out var uri) && !string.IsNullOrEmpty(uri);
            bool hasHostName = parameters.TryGetValue(SparkParameters.HostName, out var hostName) && !string.IsNullOrEmpty(hostName);
            if (hasUri)
            {
                parameters[AdbcOptions.Uri] = "http://unknownhost.azure.com/cliservice";
            }
            else if (hasHostName)
            {
                parameters[SparkParameters.HostName] = "unknownhost.azure.com";
            }
            else
            {
                Assert.Fail($"Unexpected configuration. Must provide '{AdbcOptions.Uri}' or '{SparkParameters.HostName}'.");
            }

            AdbcDatabase database = driver.Open(parameters);
            AggregateException exception = Assert.ThrowsAny<AggregateException>(() => database.Connect(parameters));
            OutputHelper?.WriteLine(exception.Message);
        }

        /// <summary>
        /// Validates if the driver can connect to a live server and
        /// parse the results using the asynchronous methods.
        /// </summary>
        [SkippableFact, Order(15)]
        public async Task CanExecuteQueryAsyncEmptyResult()
        {
            using AdbcConnection adbcConnection = NewConnection();
            using AdbcStatement statement = adbcConnection.CreateStatement();

            statement.SqlQuery = $"SELECT * from {TestConfiguration.Metadata.Table} WHERE FALSE";
            QueryResult queryResult = await statement.ExecuteQueryAsync();

            await Tests.DriverTests.CanExecuteQueryAsync(queryResult, 0);
        }
    }
}
