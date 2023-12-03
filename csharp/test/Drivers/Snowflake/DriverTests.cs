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
        readonly AdbcStatement _statement;
        readonly string _catalogSchema;

        public DriverTests()
        {
            Skip.IfNot(Utils.CanExecuteTestConfig(SnowflakeTestingUtils.SNOWFLAKE_TEST_CONFIG_VARIABLE));
            _testConfiguration = Utils.LoadTestConfiguration<SnowflakeTestConfiguration>(SnowflakeTestingUtils.SNOWFLAKE_TEST_CONFIG_VARIABLE);

            Dictionary<string, string> parameters = new Dictionary<string, string>();
            Dictionary<string, string> options = new Dictionary<string, string>();
            _snowflakeDriver = SnowflakeTestingUtils.GetSnowflakeAdbcDriver(_testConfiguration, out parameters);

            string databaseName = _testConfiguration.Metadata.Catalog;
            string schemaName = _testConfiguration.Metadata.Schema;

            parameters[SnowflakeParameters.DATABASE] = databaseName;
            parameters[SnowflakeParameters.SCHEMA] = schemaName;

            _database = _snowflakeDriver.Open(parameters);
            _connection = _database.Connect(options);
            _statement = _connection.CreateStatement();
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
                using AdbcStatement statement = _connection.CreateStatement();
                statement.SqlQuery = query;

                UpdateResult updateResult = statement.ExecuteUpdate();

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
                AdbcInfoCode value = (AdbcInfoCode)infoNameArray.GetValue(i);
                DenseUnionArray valueArray = (DenseUnionArray)recordBatch.Column("info_value");

                Assert.Contains(value.ToString(), expectedValues);

                StringArray stringArray = (StringArray)valueArray.Fields[0];
                Console.WriteLine($"{value}={stringArray.GetString(i)}");
            }
        }

        /// <summary>
        /// Validates if the driver can call GetObjects with GetObjectsDepth as Catalogs.
        /// </summary>
        [SkippableFact, Order(3)]
        public void CanGetObjectsCatalogs()
        {
            string databaseName = _testConfiguration.Metadata.Catalog;

            using IArrowArrayStream stream = _connection.GetObjects(
                    depth: AdbcConnection.GetObjectsDepth.Catalogs,
                    catalogPattern: databaseName,
                    dbSchemaPattern: null,
                    tableNamePattern: null,
                    tableTypes: new List<string> { "BASE TABLE", "VIEW" },
                    columnNamePattern: null);

            using RecordBatch recordBatch = stream.ReadNextRecordBatchAsync().Result;

            List<AdbcCatalog> catalogs = GetObjectsParser.ParseCatalog(recordBatch, databaseName, null);

            AdbcCatalog catalog = catalogs
                .Where(s => s.Name.Equals(databaseName))
                .FirstOrDefault();

            Assert.True(catalog != null, "catalog should not be null");
            Assert.Equal(databaseName, catalog.Name);
        }

        /// <summary>
        /// Validates if the driver can call GetObjects with GetObjectsDepth as Catalogs and CatalogName passed as a pattern.
        /// </summary>
        [SkippableFact, Order(3)]
        public void CanGetObjectsCatalogsWithPattern()
        {
            string databaseName = _testConfiguration.Metadata.Catalog;
            string schemaName = _testConfiguration.Metadata.Schema;
            string partialDatabaseName = GetPartialNameForPatternMatch(databaseName);


            using IArrowArrayStream stream = _connection.GetObjects(
                    depth: AdbcConnection.GetObjectsDepth.Catalogs,
                    catalogPattern: $"{partialDatabaseName}%",
                    dbSchemaPattern: null,
                    tableNamePattern: null,
                    tableTypes: new List<string> { "BASE TABLE", "VIEW" },
                    columnNamePattern: null);

            using RecordBatch recordBatch = stream.ReadNextRecordBatchAsync().Result;

            List<AdbcCatalog> catalogs = GetObjectsParser.ParseCatalog(recordBatch, databaseName, null);

            AdbcCatalog catalog = catalogs
                .Where(s => s.Name.Equals(databaseName))
                .FirstOrDefault();

            Assert.True(catalog != null, "catalog should not be null");
            Assert.StartsWith(databaseName, catalog.Name);
        }

        /// <summary>
        /// Validates if the driver can call GetObjects with GetObjectsDepth as DbSchemas.
        /// </summary>
        [SkippableFact, Order(3)]
        public void CanGetObjectsDbSchemas()
        {
            // need to add the database
            string databaseName = _testConfiguration.Metadata.Catalog;
            string schemaName = _testConfiguration.Metadata.Schema;

            using IArrowArrayStream stream = _connection.GetObjects(
                    depth: AdbcConnection.GetObjectsDepth.DbSchemas,
                    catalogPattern: databaseName,
                    dbSchemaPattern: schemaName,
                    tableNamePattern: null,
                    tableTypes: new List<string> { "BASE TABLE", "VIEW" },
                    columnNamePattern: null);

            using RecordBatch recordBatch = stream.ReadNextRecordBatchAsync().Result;

            List<AdbcCatalog> catalogs = GetObjectsParser.ParseCatalog(recordBatch, databaseName, schemaName);

            List<AdbcDbSchema> dbSchemas = catalogs
                .Where(s => s.Name.Equals(databaseName))
                .Select(s => s.DbSchemas)
                .FirstOrDefault();
            AdbcDbSchema dbSchema = dbSchemas.FirstOrDefault();

            Assert.True(dbSchema != null, "dbSchema should not be null");
            Assert.Equal(schemaName, dbSchema.Name);
        }

        /// <summary>
        /// Validates if the driver can call GetObjects with GetObjectsDepth as DbSchemas with DbSchemaName as a pattern.
        /// </summary>
        [SkippableFact, Order(3)]
        public void CanGetObjectsDbSchemasWithPattern()
        {
            // need to add the database
            string databaseName = _testConfiguration.Metadata.Catalog;
            string schemaName = _testConfiguration.Metadata.Schema;
            string partialSchemaName = GetPartialNameForPatternMatch(schemaName);

            using IArrowArrayStream stream = _connection.GetObjects(
                    depth: AdbcConnection.GetObjectsDepth.DbSchemas,
                    catalogPattern: databaseName,
                    dbSchemaPattern: $"{partialSchemaName}%",
                    tableNamePattern: null,
                    tableTypes: new List<string> { "BASE TABLE", "VIEW" },
                    columnNamePattern: null);

            using RecordBatch recordBatch = stream.ReadNextRecordBatchAsync().Result;

            List<AdbcCatalog> catalogs = GetObjectsParser.ParseCatalog(recordBatch, databaseName, schemaName);

            List<AdbcDbSchema> dbSchemas = catalogs
                .Where(s => s.Name.Equals(databaseName))
                .Select(s => s.DbSchemas)
                .FirstOrDefault();
            AdbcDbSchema dbSchema = dbSchemas.FirstOrDefault();

            Assert.True(dbSchema != null, "dbSchema should not be null");
            Assert.StartsWith(partialSchemaName, dbSchema.Name);
        }

        /// <summary>
        /// Validates if the driver can call GetObjects with GetObjectsDepth as Tables.
        /// </summary>
        [SkippableFact, Order(3)]
        public void CanGetObjectsTables()
        {
            // need to add the database
            string databaseName = _testConfiguration.Metadata.Catalog;
            string schemaName = _testConfiguration.Metadata.Schema;
            string tableName = _testConfiguration.Metadata.Table;

            using IArrowArrayStream stream = _connection.GetObjects(
                    depth: AdbcConnection.GetObjectsDepth.All,
                    catalogPattern: databaseName,
                    dbSchemaPattern: schemaName,
                    tableNamePattern: tableName,
                    tableTypes: new List<string> { "BASE TABLE", "VIEW" },
                    columnNamePattern: null);

            using RecordBatch recordBatch = stream.ReadNextRecordBatchAsync().Result;

            List<AdbcCatalog> catalogs = GetObjectsParser.ParseCatalog(recordBatch, databaseName, schemaName);

            List<AdbcTable> tables = catalogs
                .Where(s => s.Name.Equals(databaseName))
                .Select(s => s.DbSchemas)
                .FirstOrDefault()
                .Select(t => t.Tables)
                .FirstOrDefault();
            AdbcTable table = tables.FirstOrDefault();

            Assert.True(table != null, "table should not be null");
            Assert.Equal(tableName, table.Name, true);
        }

        /// <summary>
        /// Validates if the driver can call GetObjects with GetObjectsDepth as Tables with TableName as a pattern.
        /// </summary>
        [SkippableFact, Order(3)]
        public void CanGetObjectsTablesWithPattern()
        {
            // need to add the database
            string databaseName = _testConfiguration.Metadata.Catalog;
            string schemaName = _testConfiguration.Metadata.Schema;
            string tableName = _testConfiguration.Metadata.Table;
            string partialTableName = GetPartialNameForPatternMatch(tableName);

            using IArrowArrayStream stream = _connection.GetObjects(
                    depth: AdbcConnection.GetObjectsDepth.All,
                    catalogPattern: databaseName,
                    dbSchemaPattern: schemaName,
                    tableNamePattern: $"{partialTableName}%",
                    tableTypes: new List<string> { "BASE TABLE", "VIEW" },
                    columnNamePattern: null);

            using RecordBatch recordBatch = stream.ReadNextRecordBatchAsync().Result;

            List<AdbcCatalog> catalogs = GetObjectsParser.ParseCatalog(recordBatch, databaseName, schemaName);

            List<AdbcTable> tables = catalogs
                .Where(s => s.Name.Equals(databaseName))
                .Select(s => s.DbSchemas)
                .FirstOrDefault()
                .Select(t => t.Tables)
                .FirstOrDefault();
            AdbcTable table = tables.FirstOrDefault();

            Assert.True(table != null, "table should not be null");
            Assert.StartsWith(partialTableName, table.Name);
        }

        /// <summary>
        /// Validates if the driver can call GetObjects for GetObjectsDepth as All.
        /// </summary>
        [SkippableFact, Order(3)]
        public void CanGetObjectsAll()
        {
            // need to add the database
            string databaseName = _testConfiguration.Metadata.Catalog;
            string schemaName = _testConfiguration.Metadata.Schema;
            string tableName = _testConfiguration.Metadata.Table;
            string columnName = null;

            using IArrowArrayStream stream = _connection.GetObjects(
                    depth: AdbcConnection.GetObjectsDepth.All,
                    catalogPattern: databaseName,
                    dbSchemaPattern: schemaName,
                    tableNamePattern: tableName,
                    tableTypes: new List<string> { "BASE TABLE", "VIEW" },
                    columnNamePattern: columnName);

            using RecordBatch recordBatch = stream.ReadNextRecordBatchAsync().Result;

            List<AdbcCatalog> catalogs = GetObjectsParser.ParseCatalog(recordBatch, databaseName, schemaName);

            List<AdbcColumn> columns = catalogs
                .Where(s => s.Name.Equals(databaseName))
                .Select(s => s.DbSchemas)
                .FirstOrDefault()
                .Select(t => t.Tables)
                .FirstOrDefault()
                .Select(c => c.Columns)
                .FirstOrDefault();

            Assert.True(columns != null, "Columns cannot be null");
            Assert.Equal(_testConfiguration.Metadata.ExpectedColumnCount, columns.Count);

            if (_testConfiguration.UseHighPrecision)
            {
                IEnumerable<AdbcColumn> highPrecisionColumns = columns.Where(c => c.XdbcTypeName == "NUMBER");

                if(highPrecisionColumns.Count() > 0)
                {
                    // ensure they all are coming back as XdbcDataType_XDBC_DECIMAL because they are Decimal128
                    short XdbcDataType_XDBC_DECIMAL = 3;
                    IEnumerable<AdbcColumn> invalidHighPrecisionColumns  = highPrecisionColumns.Where(c => c.XdbcSqlDataType != XdbcDataType_XDBC_DECIMAL);
                    int count = invalidHighPrecisionColumns.Count();
                    Assert.True(count == 0, $"There are {count} columns that do not map to the correct XdbcSqlDataType when UseHighPrecision=true");
                }
            }
        }

        /// <summary>
        /// Validates if the driver can call GetTableSchema.
        /// </summary>
        [SkippableFact, Order(3)]
        public void CanGetTableSchema()
        {
            string databaseName = _testConfiguration.Metadata.Catalog;
            string schemaName = _testConfiguration.Metadata.Schema;
            string tableName = _testConfiguration.Metadata.Table;

            Schema schema = _connection.GetTableSchema(databaseName, schemaName, tableName);

            int numberOfFields = schema.FieldsList.Count;

            //Assert.Equal(tableName, table.Name, true);
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

        private string GetPartialNameForPatternMatch(string name)
        {
            if (string.IsNullOrEmpty(name) || name.Length == 1) return name;

            return name.Substring(0, name.Length / 2);
        }

        public void Dispose()
        {
            _connection.Dispose();
            _database.Dispose();
            _snowflakeDriver.Dispose();
        }

        private string CreateTemporaryTable(string databaseName, string schemaName, string tableName)
        {
            tableName = string.Format("\"{0}\".\"{1}\".\"{2}\"", databaseName, schemaName, tableName);
            string createTableStatement = string.Format("CREATE TEMPORARY TABLE {0} (index int)", tableName);
            _statement.SqlQuery = createTableStatement;
            _statement.ExecuteUpdate();
            return tableName;
        }
    }
}
