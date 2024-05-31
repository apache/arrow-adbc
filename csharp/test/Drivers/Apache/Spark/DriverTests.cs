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
using System.Threading.Tasks;
using Apache.Arrow.Adbc.Tests.Metadata;
using Apache.Arrow.Adbc.Tests.Xunit;
using Apache.Arrow.Ipc;
using Xunit;
using Xunit.Abstractions;
using ColumnTypeId = Apache.Arrow.Adbc.Drivers.Apache.Spark.SparkConnection.ColumnTypeId;

namespace Apache.Arrow.Adbc.Tests.Drivers.Apache.Spark
{
    /// <summary>
    /// Class for testing the Snowflake ADBC driver connection tests.
    /// </summary>
    /// <remarks>
    /// Tests are ordered to ensure data is created for the other
    /// queries to run.
    /// </remarks>
    [TestCaseOrderer("Apache.Arrow.Adbc.Tests.Xunit.TestOrderer", "Apache.Arrow.Adbc.Tests")]
    public class DriverTests : SparkTestBase
    {
        /// <summary>
        /// Supported Spark data types as a subset of <see cref="SparkConnection.ColumnTypeId"/>
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

        public DriverTests(ITestOutputHelper? outputHelper) : base(outputHelper)
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

            List<int> expectedResults = new() {
                -1, // DROP   TABLE
                -1, // CREATE TABLE
                1,  // INSERT
                1,  // INSERT
                1,  // INSERT
                1,  // UPDATE
                1,  // DELETE
            };

            for (int i = 0; i < queries.Length; i++)
            {
                string query = queries[i];
                AdbcStatement statement = adbcConnection.CreateStatement();
                statement.SqlQuery = query;

                UpdateResult updateResult = statement.ExecuteUpdate();

                Assert.Equal(expectedResults[i], updateResult.AffectedRows);
            }
        }

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
        [SkippableTheory, Order(3)]
        [MemberData(nameof(CatalogNamePatternData))]
        public void GetGetObjectsCatalogs(string pattern)
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

            List<AdbcCatalog> catalogs = GetObjectsParser.ParseCatalog(recordBatch, catalogName, null);
            AdbcCatalog? catalog = catalogs.Where((catalog) => string.Equals(catalog.Name, catalogName)).FirstOrDefault();

            Assert.True(catalog != null, "catalog should not be null");
        }

        /// <summary>
        /// Validates if the driver can call GetObjects with GetObjectsDepth as DbSchemas with DbSchemaName as a pattern.
        /// </summary>
        [SkippableTheory, Order(4)]
        [MemberData(nameof(DbSchemasNamePatternData))]
        public void CanGetObjectsDbSchemas(string dbSchemaPattern)
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

            List<AdbcCatalog> catalogs = GetObjectsParser.ParseCatalog(recordBatch, databaseName, schemaName);

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
        [SkippableTheory, Order(5)]
        [MemberData(nameof(TableNamePatternData))]
        public void CanGetObjectsTables(string tableNamePattern)
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

            List<AdbcCatalog> catalogs = GetObjectsParser.ParseCatalog(recordBatch, databaseName, schemaName);

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

            List<AdbcCatalog> catalogs = GetObjectsParser.ParseCatalog(recordBatch, databaseName, schemaName);
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

                var types = Enum.GetValues(typeof(SupportedSparkDataType)).Cast<SupportedSparkDataType>();
                Assert.Contains((SupportedSparkDataType)column.XdbcSqlDataType!, types);
                Assert.Equal(column.XdbcDataType, column.XdbcSqlDataType);

                Assert.NotNull(column.XdbcDataType);
                Assert.Contains((SupportedSparkDataType)column.XdbcDataType!, types);

                bool isDecimalType = column.XdbcDataType == (short)SupportedSparkDataType.DECIMAL || column.XdbcDataType == (short)SupportedSparkDataType.NUMERIC;
                Assert.Equal(column.XdbcColumnSize.HasValue, isDecimalType);
                Assert.Equal(column.XdbcDecimalDigits.HasValue, isDecimalType);

                Assert.NotNull(column.Remarks);
                Assert.True(string.IsNullOrEmpty(column.Remarks));

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
            string fullTableName = $"{DelimitIdentifier(catalogName)}.{DelimitIdentifier(schemaName)}.{DelimitIdentifier(tableName)}";
            using TemporaryTable temporaryTable = TemporaryTable.NewTemporaryTableAsync(Statement, fullTableName, $"CREATE TABLE IF NOT EXISTS {fullTableName} (INDEX INT)").Result;

            using IArrowArrayStream stream = Connection.GetObjects(
                    depth: AdbcConnection.GetObjectsDepth.Tables,
                    catalogPattern: catalogName,
                    dbSchemaPattern: schemaName,
                    tableNamePattern: tableName,
                    tableTypes: DefaultTableTypes,
                    columnNamePattern: null);

            using RecordBatch recordBatch = stream.ReadNextRecordBatchAsync().Result;

            List<AdbcCatalog> catalogs = GetObjectsParser.ParseCatalog(recordBatch, catalogName, schemaName);

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
        [SkippableFact, Order(10)]
        public void CanExecuteQuery()
        {
            using AdbcConnection adbcConnection = NewConnection();

            using AdbcStatement statement = adbcConnection.CreateStatement();
            statement.SqlQuery = TestConfiguration.Query;

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

            statement.SqlQuery = GetInsertValueStatement(temporaryTable.TableName, "INDEX", "1");
            UpdateResult updateResult = await statement.ExecuteUpdateAsync();

            Assert.Equal(1, updateResult.AffectedRows);
        }

        public static IEnumerable<object[]> CatalogNamePatternData()
        {
            string? catalogName = new DriverTests(null).TestConfiguration?.Metadata?.Catalog;
            return GetPatterns(catalogName);
        }

        public static IEnumerable<object[]> DbSchemasNamePatternData()
        {
            string? dbSchemaName = new DriverTests(null).TestConfiguration?.Metadata?.Schema;
            return GetPatterns(dbSchemaName);
        }

        public static IEnumerable<object[]> TableNamePatternData()
        {
            string? tableName = new DriverTests(null).TestConfiguration?.Metadata?.Table;
            return GetPatterns(tableName);
        }
    }
}
