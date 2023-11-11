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
    public class DriverTests
    {
        public DriverTests()
        {
           Skip.IfNot(Utils.CanExecuteTestConfig(SnowflakeTestingUtils.SNOWFLAKE_TEST_CONFIG_VARIABLE));
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
            SnowflakeTestConfiguration testConfiguration = Utils.LoadTestConfiguration<SnowflakeTestConfiguration>(SnowflakeTestingUtils.SNOWFLAKE_TEST_CONFIG_VARIABLE);

            Dictionary<string, string> parameters = new Dictionary<string, string>();
            Dictionary<string, string> options = new Dictionary<string, string>();

            AdbcDriver snowflakeDriver = SnowflakeTestingUtils.GetSnowflakeAdbcDriver(testConfiguration, out parameters);

            AdbcDatabase adbcDatabase = snowflakeDriver.Open(parameters);
            AdbcConnection adbcConnection = adbcDatabase.Connect(options);

            string[] queries = SnowflakeTestingUtils.GetQueries(testConfiguration);

            List<int> expectedResults = new List<int>() { -1, 1, 1 };

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
        public void CanGetInfo()
        {
            Dictionary<string, string> parameters = new Dictionary<string, string>();

            SnowflakeTestConfiguration testConfiguration = Utils.LoadTestConfiguration<SnowflakeTestConfiguration>(SnowflakeTestingUtils.SNOWFLAKE_TEST_CONFIG_VARIABLE);

            AdbcDriver driver = SnowflakeTestingUtils.GetSnowflakeAdbcDriver(testConfiguration, out parameters);

            AdbcDatabase adbcDatabase = driver.Open(parameters);
            AdbcConnection adbcConnection = adbcDatabase.Connect(new Dictionary<string, string>());

            IArrowArrayStream stream = adbcConnection.GetInfo(new List<AdbcInfoCode>() { AdbcInfoCode.DriverName, AdbcInfoCode.DriverVersion, AdbcInfoCode.VendorName });

            RecordBatch recordBatch = stream.ReadNextRecordBatchAsync().Result;
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
            Dictionary<string, string> parameters = new Dictionary<string, string>();

            SnowflakeTestConfiguration testConfiguration = Utils.LoadTestConfiguration<SnowflakeTestConfiguration>(SnowflakeTestingUtils.SNOWFLAKE_TEST_CONFIG_VARIABLE);

            AdbcDriver driver = SnowflakeTestingUtils.GetSnowflakeAdbcDriver(testConfiguration, out parameters);

            string databaseName = testConfiguration.Metadata.Catalog;
            string schemaName = testConfiguration.Metadata.Schema;

            parameters[SnowflakeParameters.DATABASE] = databaseName;
            parameters[SnowflakeParameters.SCHEMA] = schemaName;

            AdbcDatabase adbcDatabase = driver.Open(parameters);
            AdbcConnection adbcConnection = adbcDatabase.Connect(new Dictionary<string, string>());

            IArrowArrayStream stream = adbcConnection.GetObjects(
                    depth: AdbcConnection.GetObjectsDepth.Catalogs,
                    catalogPattern: databaseName,
                    dbSchemaPattern: null,
                    tableNamePattern: null,
                    tableTypes: new List<string> { "BASE TABLE", "VIEW" },
                    columnNamePattern: null);

            RecordBatch recordBatch = stream.ReadNextRecordBatchAsync().Result;

            List<AdbcCatalog> catalogs = GetObjectsParser.ParseCatalog(recordBatch, databaseName, null);

            AdbcCatalog catalog = catalogs.FirstOrDefault();

            Assert.True(catalog != null, "catalog should not be null");
            Assert.Equal(databaseName, catalog.Name);
        }

        /// <summary>
        /// Validates if the driver can call GetObjects with GetObjectsDepth as Catalogs and CatalogName passed as a pattern.
        /// </summary>
        [SkippableFact, Order(3)]
        public void CanGetObjectsCatalogsWithPattern()
        {
            Dictionary<string, string> parameters = new Dictionary<string, string>();

            SnowflakeTestConfiguration testConfiguration = Utils.LoadTestConfiguration<SnowflakeTestConfiguration>(SnowflakeTestingUtils.SNOWFLAKE_TEST_CONFIG_VARIABLE);

            AdbcDriver driver = SnowflakeTestingUtils.GetSnowflakeAdbcDriver(testConfiguration, out parameters);

            string databaseName = testConfiguration.Metadata.Catalog;
            string schemaName = testConfiguration.Metadata.Schema;
            string partialDatabaseName = $"{GetPartialNameForPatternMatch(databaseName)}%";

            parameters[SnowflakeParameters.DATABASE] = databaseName;
            parameters[SnowflakeParameters.SCHEMA] = schemaName;

            AdbcDatabase adbcDatabase = driver.Open(parameters);
            AdbcConnection adbcConnection = adbcDatabase.Connect(new Dictionary<string, string>());

            IArrowArrayStream stream = adbcConnection.GetObjects(
                    depth: AdbcConnection.GetObjectsDepth.Catalogs,
                    catalogPattern: partialDatabaseName,
                    dbSchemaPattern: null,
                    tableNamePattern: null,
                    tableTypes: new List<string> { "BASE TABLE", "VIEW" },
                    columnNamePattern: null);

            RecordBatch recordBatch = stream.ReadNextRecordBatchAsync().Result;

            List<AdbcCatalog> catalogs = GetObjectsParser.ParseCatalog(recordBatch, databaseName, null);

            AdbcCatalog catalog = catalogs.FirstOrDefault();

            Assert.True(catalog != null, "catalog should not be null");
        }

        /// <summary>
        /// Validates if the driver can call GetObjects with GetObjectsDepth as DbSchemas.
        /// </summary>
        [SkippableFact, Order(3)]
        public void CanGetObjectsDbSchemas()
        {
            Dictionary<string, string> parameters = new Dictionary<string, string>();

            SnowflakeTestConfiguration testConfiguration = Utils.LoadTestConfiguration<SnowflakeTestConfiguration>(SnowflakeTestingUtils.SNOWFLAKE_TEST_CONFIG_VARIABLE);

            AdbcDriver driver = SnowflakeTestingUtils.GetSnowflakeAdbcDriver(testConfiguration, out parameters);

            // need to add the database
            string databaseName = testConfiguration.Metadata.Catalog;
            string schemaName = testConfiguration.Metadata.Schema;

            parameters[SnowflakeParameters.DATABASE] = databaseName;
            parameters[SnowflakeParameters.SCHEMA] = schemaName;

            AdbcDatabase adbcDatabase = driver.Open(parameters);
            AdbcConnection adbcConnection = adbcDatabase.Connect(new Dictionary<string, string>());

            IArrowArrayStream stream = adbcConnection.GetObjects(
                    depth: AdbcConnection.GetObjectsDepth.DbSchemas,
                    catalogPattern: databaseName,
                    dbSchemaPattern: schemaName,
                    tableNamePattern: null,
                    tableTypes: new List<string> { "BASE TABLE", "VIEW" },
                    columnNamePattern: null);

            RecordBatch recordBatch = stream.ReadNextRecordBatchAsync().Result;

            List<AdbcCatalog> catalogs = GetObjectsParser.ParseCatalog(recordBatch, databaseName, schemaName);

            List<AdbcDbSchema> dbSchemas = catalogs
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
            Dictionary<string, string> parameters = new Dictionary<string, string>();

            SnowflakeTestConfiguration testConfiguration = Utils.LoadTestConfiguration<SnowflakeTestConfiguration>(SnowflakeTestingUtils.SNOWFLAKE_TEST_CONFIG_VARIABLE);

            AdbcDriver driver = SnowflakeTestingUtils.GetSnowflakeAdbcDriver(testConfiguration, out parameters);

            // need to add the database
            string databaseName = testConfiguration.Metadata.Catalog;
            string schemaName = testConfiguration.Metadata.Schema;
            string partialSchemaName = $"{GetPartialNameForPatternMatch(schemaName)}%";

            parameters[SnowflakeParameters.DATABASE] = databaseName;
            parameters[SnowflakeParameters.SCHEMA] = schemaName;

            AdbcDatabase adbcDatabase = driver.Open(parameters);
            AdbcConnection adbcConnection = adbcDatabase.Connect(new Dictionary<string, string>());

            IArrowArrayStream stream = adbcConnection.GetObjects(
                    depth: AdbcConnection.GetObjectsDepth.DbSchemas,
                    catalogPattern: databaseName,
                    dbSchemaPattern: partialSchemaName,
                    tableNamePattern: null,
                    tableTypes: new List<string> { "BASE TABLE", "VIEW" },
                    columnNamePattern: null);

            RecordBatch recordBatch = stream.ReadNextRecordBatchAsync().Result;

            List<AdbcCatalog> catalogs = GetObjectsParser.ParseCatalog(recordBatch, databaseName, schemaName);

            List<AdbcDbSchema> dbSchemas = catalogs
                .Select(s => s.DbSchemas)
                .FirstOrDefault();
            AdbcDbSchema dbSchema = dbSchemas.FirstOrDefault();

            Assert.True(dbSchema != null, "dbSchema should not be null");
        }

        /// <summary>
        /// Validates if the driver can call GetObjects with GetObjectsDepth as Tables.
        /// </summary>
        [SkippableFact, Order(3)]
        public void CanGetObjectsTables()
        {
            Dictionary<string, string> parameters = new Dictionary<string, string>();

            SnowflakeTestConfiguration testConfiguration = Utils.LoadTestConfiguration<SnowflakeTestConfiguration>(SnowflakeTestingUtils.SNOWFLAKE_TEST_CONFIG_VARIABLE);

            AdbcDriver driver = SnowflakeTestingUtils.GetSnowflakeAdbcDriver(testConfiguration, out parameters);

            // need to add the database
            string databaseName = testConfiguration.Metadata.Catalog;
            string schemaName = testConfiguration.Metadata.Schema;
            string tableName = testConfiguration.Metadata.Table;

            parameters[SnowflakeParameters.DATABASE] = databaseName;
            parameters[SnowflakeParameters.SCHEMA] = schemaName;

            AdbcDatabase adbcDatabase = driver.Open(parameters);
            AdbcConnection adbcConnection = adbcDatabase.Connect(new Dictionary<string, string>());

            IArrowArrayStream stream = adbcConnection.GetObjects(
                    depth: AdbcConnection.GetObjectsDepth.All,
                    catalogPattern: databaseName,
                    dbSchemaPattern: schemaName,
                    tableNamePattern: tableName,
                    tableTypes: new List<string> { "BASE TABLE", "VIEW" },
                    columnNamePattern: null);

            RecordBatch recordBatch = stream.ReadNextRecordBatchAsync().Result;

            List<AdbcCatalog> catalogs = GetObjectsParser.ParseCatalog(recordBatch, databaseName, schemaName);

            List<AdbcTable> tables = catalogs
                .Select(s => s.DbSchemas)
                .FirstOrDefault()
                .Select(t => t.Tables)
                .FirstOrDefault();
            AdbcTable table = tables.FirstOrDefault();

            Assert.True(table != null, "table should not be null");
            Assert.Equal(tableName, table.Name);
        }

        /// <summary>
        /// Validates if the driver can call GetObjects with GetObjectsDepth as Tables with TableName as a pattern.
        /// </summary>
        [SkippableFact, Order(3)]
        public void CanGetObjectsTablesWithPattern()
        {
            Dictionary<string, string> parameters = new Dictionary<string, string>();

            SnowflakeTestConfiguration testConfiguration = Utils.LoadTestConfiguration<SnowflakeTestConfiguration>(SnowflakeTestingUtils.SNOWFLAKE_TEST_CONFIG_VARIABLE);

            AdbcDriver driver = SnowflakeTestingUtils.GetSnowflakeAdbcDriver(testConfiguration, out parameters);

            // need to add the database
            string databaseName = testConfiguration.Metadata.Catalog;
            string schemaName = testConfiguration.Metadata.Schema;
            string tableName = testConfiguration.Metadata.Table;
            string partialTableName = $"{GetPartialNameForPatternMatch(tableName)}%";

            parameters[SnowflakeParameters.DATABASE] = databaseName;
            parameters[SnowflakeParameters.SCHEMA] = schemaName;

            AdbcDatabase adbcDatabase = driver.Open(parameters);
            AdbcConnection adbcConnection = adbcDatabase.Connect(new Dictionary<string, string>());

            IArrowArrayStream stream = adbcConnection.GetObjects(
                    depth: AdbcConnection.GetObjectsDepth.All,
                    catalogPattern: databaseName,
                    dbSchemaPattern: schemaName,
                    tableNamePattern: partialTableName,
                    tableTypes: new List<string> { "BASE TABLE", "VIEW" },
                    columnNamePattern: null);

            RecordBatch recordBatch = stream.ReadNextRecordBatchAsync().Result;

            List<AdbcCatalog> catalogs = GetObjectsParser.ParseCatalog(recordBatch, databaseName, schemaName);

            List<AdbcTable> tables = catalogs
                .Select(s => s.DbSchemas)
                .FirstOrDefault()
                .Select(t => t.Tables)
                .FirstOrDefault();
            AdbcTable table = tables.FirstOrDefault();

            Assert.True(table != null, "table should not be null");
        }

        /// <summary>
        /// Validates if the driver can call GetObjects for GetObjectsDepth as All.
        /// </summary>
        [SkippableFact, Order(3)]
        public void CanGetObjectsAll()
        {
            Dictionary<string, string> parameters = new Dictionary<string, string>();

            SnowflakeTestConfiguration testConfiguration = Utils.LoadTestConfiguration<SnowflakeTestConfiguration>(SnowflakeTestingUtils.SNOWFLAKE_TEST_CONFIG_VARIABLE);

            AdbcDriver driver = SnowflakeTestingUtils.GetSnowflakeAdbcDriver(testConfiguration, out parameters);

            // need to add the database
            string databaseName = testConfiguration.Metadata.Catalog;
            string schemaName = testConfiguration.Metadata.Schema;
            string tableName = testConfiguration.Metadata.Table;
            string columnName = null;

            parameters[SnowflakeParameters.DATABASE] = databaseName;
            parameters[SnowflakeParameters.SCHEMA] = schemaName;

            AdbcDatabase adbcDatabase = driver.Open(parameters);
            AdbcConnection adbcConnection = adbcDatabase.Connect(new Dictionary<string, string>());

            IArrowArrayStream stream = adbcConnection.GetObjects(
                    depth: AdbcConnection.GetObjectsDepth.All,
                    catalogPattern: databaseName,
                    dbSchemaPattern: schemaName,
                    tableNamePattern: tableName,
                    tableTypes: new List<string> { "BASE TABLE", "VIEW" },
                    columnNamePattern: columnName);

            RecordBatch recordBatch = stream.ReadNextRecordBatchAsync().Result;

            List<AdbcCatalog> catalogs = GetObjectsParser.ParseCatalog(recordBatch, databaseName, schemaName);

            List<AdbcColumn> columns = catalogs
                .Select(s => s.DbSchemas)
                .FirstOrDefault()
                .Select(t => t.Tables)
                .FirstOrDefault()
                .Select(c => c.Columns)
                .FirstOrDefault();

            Assert.True(columns != null, "Columns cannot be null");

            Assert.Equal(testConfiguration.Metadata.ExpectedColumnCount, columns.Count);
        }

        /// <summary>
        /// Validates if the driver can call GetTableSchema.
        /// </summary>
        [SkippableFact, Order(4)]
        public void CanGetTableSchema()
        {
            Dictionary<string, string> parameters = new Dictionary<string, string>();

            SnowflakeTestConfiguration testConfiguration = Utils.LoadTestConfiguration<SnowflakeTestConfiguration>(SnowflakeTestingUtils.SNOWFLAKE_TEST_CONFIG_VARIABLE);

            AdbcDriver driver = SnowflakeTestingUtils.GetSnowflakeAdbcDriver(testConfiguration, out parameters);

            AdbcDatabase adbcDatabase = driver.Open(parameters);
            AdbcConnection adbcConnection = adbcDatabase.Connect(new Dictionary<string, string>());

            string databaseName = testConfiguration.Metadata.Catalog;
            string schemaName = testConfiguration.Metadata.Schema;
            string tableName = testConfiguration.Metadata.Table;

            Schema schema = adbcConnection.GetTableSchema(databaseName, schemaName, tableName);

            int numberOfFields = schema.FieldsList.Count;

            Assert.Equal(testConfiguration.Metadata.ExpectedColumnCount, numberOfFields);
        }

        /// <summary>
        /// Validates if the driver can call GetTableTypes.
        /// </summary>
        [SkippableFact, Order(5)]
        public void CanGetTableTypes()
        {
            Dictionary<string, string> parameters = new Dictionary<string, string>();

            SnowflakeTestConfiguration testConfiguration = Utils.LoadTestConfiguration<SnowflakeTestConfiguration>(SnowflakeTestingUtils.SNOWFLAKE_TEST_CONFIG_VARIABLE);

            AdbcDriver driver = SnowflakeTestingUtils.GetSnowflakeAdbcDriver(testConfiguration, out parameters);

            AdbcDatabase adbcDatabase = driver.Open(parameters);
            AdbcConnection adbcConnection = adbcDatabase.Connect(new Dictionary<string, string>());

            IArrowArrayStream arrowArrayStream = adbcConnection.GetTableTypes();

            RecordBatch recordBatch = arrowArrayStream.ReadNextRecordBatchAsync().Result;

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
            SnowflakeTestConfiguration testConfiguration = Utils.LoadTestConfiguration<SnowflakeTestConfiguration>(SnowflakeTestingUtils.SNOWFLAKE_TEST_CONFIG_VARIABLE);

            Dictionary<string, string> parameters = new Dictionary<string, string>();
            Dictionary<string, string> options = new Dictionary<string, string>();

            AdbcDriver snowflakeDriver = SnowflakeTestingUtils.GetSnowflakeAdbcDriver(testConfiguration, out parameters);

            AdbcDatabase adbcDatabase = snowflakeDriver.Open(parameters);
            AdbcConnection adbcConnection = adbcDatabase.Connect(options);

            Console.WriteLine(testConfiguration.Query);

            AdbcStatement statement = adbcConnection.CreateStatement();
            statement.SqlQuery = testConfiguration.Query;

            QueryResult queryResult = statement.ExecuteQuery();

            Tests.DriverTests.CanExecuteQuery(queryResult, testConfiguration.ExpectedResultsCount);
        }


        private string GetPartialNameForPatternMatch(string name) {
            if (string.IsNullOrEmpty(name) || name.Length == 1) return name;

            return name.Substring(0, name.Length / 2);
        }
    }
}
