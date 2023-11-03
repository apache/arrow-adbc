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
        /// Validates if the driver can call GetObjects.
        /// </summary>
        [SkippableFact, Order(3)]
        public void CanGetObjects()
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

        /// <summary>
        /// Validates if the driver can receive the correct values for numeric data types
        /// integers
        /// </summary>
        [Test, Order(7)]
        public void TestNumericRanges()
        {
            if (Utils.CanExecuteTestConfig(SnowflakeTestingUtils.SNOWFLAKE_TEST_CONFIG_VARIABLE))
            {
                SnowflakeTestConfiguration testConfiguration = Utils.LoadTestConfiguration<SnowflakeTestConfiguration>(SnowflakeTestingUtils.SNOWFLAKE_TEST_CONFIG_VARIABLE);
                Dictionary<string, string> parameters = new();
                Dictionary<string, string> options = new();

                AdbcDriver snowflakeDriver = SnowflakeTestingUtils.GetSnowflakeAdbcDriver(testConfiguration, out parameters);

                AdbcDatabase adbcDatabase = snowflakeDriver.Open(parameters);
                AdbcConnection adbcConnection = adbcDatabase.Connect(options);

                string insertNumberStatement = string.Format("INSERT INTO {0}.{1}.{2} (NUMBERTYPE) VALUES (-1);", testConfiguration.Metadata.Catalog, testConfiguration.Metadata.Schema, testConfiguration.Metadata.Table);
                Console.WriteLine(insertNumberStatement);

                AdbcStatement statement = adbcConnection.CreateStatement();
                statement.SqlQuery = insertNumberStatement;

                UpdateResult updateResult = statement.ExecuteUpdate();

                Assert.AreEqual(updateResult.AffectedRows, 1);

                string selectNumberStatement = string.Format("SELECT NUMBERTYPE FROM {0}.{1}.{2} WHERE NUMBERTYPE={3};", testConfiguration.Metadata.Catalog, testConfiguration.Metadata.Schema, testConfiguration.Metadata.Table, -1);
                Console.WriteLine(selectNumberStatement);

                statement.SqlQuery = selectNumberStatement;

                QueryResult queryResult = statement.ExecuteQuery();


                string deleteNumberStatement = string.Format("DELETE FROM {0}.{1}.{2} WHERE NUMBERTYPE={3};", testConfiguration.Metadata.Catalog, testConfiguration.Metadata.Schema, testConfiguration.Metadata.Table, -1);
                Console.WriteLine(deleteNumberStatement);

                statement.SqlQuery = deleteNumberStatement;

                updateResult = statement.ExecuteUpdate();

                Assert.AreEqual(updateResult.AffectedRows, 1);

            }
        }
    }
}
