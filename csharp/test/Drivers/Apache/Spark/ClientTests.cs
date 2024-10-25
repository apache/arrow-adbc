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
using Apache.Arrow.Adbc.Drivers.Apache.Spark;
using Apache.Arrow.Adbc.Tests.Xunit;
using Xunit;
using Xunit.Abstractions;

namespace Apache.Arrow.Adbc.Tests.Drivers.Apache.Spark
{
    /// <summary>
    /// Class for testing the ADBC Client using the Spark ADBC driver.
    /// </summary>
    /// <remarks>
    /// Tests are ordered to ensure data is created for the other
    /// queries to run.
    /// </remarks>
    [TestCaseOrderer("Apache.Arrow.Adbc.Tests.Xunit.TestOrderer", "Apache.Arrow.Adbc.Tests")]
    public class ClientTests : TestBase<SparkTestConfiguration, SparkTestEnvironment>
    {
        public ClientTests(ITestOutputHelper? outputHelper) : base(outputHelper, new SparkTestEnvironment.Factory())
        {
            Skip.IfNot(Utils.CanExecuteTestConfig(TestConfigVariable));
        }

        /// <summary>
        /// Validates if the client execute updates.
        /// </summary>
        [SkippableFact, Order(1)]
        public void CanClientExecuteUpdate()
        {
            using (Adbc.Client.AdbcConnection adbcConnection = GetAdbcConnection())
            {
                adbcConnection.Open();

                string[] queries = GetQueries();
                int affectedRows = ValidateAffectedRows ? 1 : -1;

                List<int> expectedResults = TestEnvironment.ServerType != SparkServerType.Databricks
                    ? [
                        -1, // DROP   TABLE
                        -1, // CREATE TABLE
                        affectedRows,  // INSERT
                        affectedRows,  // INSERT
                        affectedRows,  // INSERT
                        //1,  // UPDATE
                        //1,  // DELETE
                    ]
                    : [
                        -1, // DROP   TABLE
                        -1, // CREATE TABLE
                        affectedRows,  // INSERT
                        affectedRows,  // INSERT
                        affectedRows,  // INSERT
                        affectedRows,  // UPDATE
                        affectedRows,  // DELETE
                    ];

                Tests.ClientTests.CanClientExecuteUpdate(adbcConnection, TestConfiguration, queries, expectedResults);
            }
        }

        /// <summary>
        /// Validates if the client can get the schema.
        /// </summary>
        [SkippableFact, Order(2)]
        public void CanClientGetSchema()
        {
            using (Adbc.Client.AdbcConnection adbcConnection = GetAdbcConnection())
            {
                Tests.ClientTests.CanClientGetSchema(adbcConnection, TestConfiguration, $"SELECT * FROM {TestConfiguration.Metadata.Table}");
            }
        }

        /// <summary>
        /// Validates if the client can connect to a live server and
        /// parse the results.
        /// </summary>
        [SkippableFact, Order(3)]
        public void CanClientExecuteQuery()
        {
            using (Adbc.Client.AdbcConnection adbcConnection = GetAdbcConnection())
            {
                Tests.ClientTests.CanClientExecuteQuery(adbcConnection, TestConfiguration);
            }
        }

        /// <summary>
        /// Validates if the client can connect to a live server and
        /// parse the results.
        /// </summary>
        [SkippableFact, Order(5)]
        public void CanClientExecuteEmptyQuery()
        {
            using (Adbc.Client.AdbcConnection adbcConnection = GetAdbcConnection())
            {
                Tests.ClientTests.CanClientExecuteQuery(
                    adbcConnection,
                    TestConfiguration,
                    customQuery: $"SELECT * FROM {TestConfiguration.Metadata.Table} WHERE FALSE",
                    expectedResultsCount: 0);
            }
        }

        /// <summary>
        /// Validates if the client is retrieving and converting values
        /// to the expected types.
        /// </summary>
        [SkippableFact, Order(4)]
        public void VerifyTypesAndValues()
        {
            using (Adbc.Client.AdbcConnection dbConnection = GetAdbcConnection())
            {
                SampleDataBuilder sampleDataBuilder = GetSampleDataBuilder();

                Tests.ClientTests.VerifyTypesAndValues(dbConnection, sampleDataBuilder);
            }
        }

        [SkippableFact]
        public void VerifySchemaTablesWithNoConstraints()
        {
            using (Adbc.Client.AdbcConnection adbcConnection = GetAdbcConnection(includeTableConstraints: false))
            {
                adbcConnection.Open();

                string schema = "Tables";

                var tables = adbcConnection.GetSchema(schema);

                Assert.True(tables.Rows.Count > 0, $"No tables were found in the schema '{schema}'");
            }
        }

        [SkippableFact]
        public void VerifySchemaTables()
        {
            using (Adbc.Client.AdbcConnection adbcConnection = GetAdbcConnection())
            {
                adbcConnection.Open();

                var collections = adbcConnection.GetSchema("MetaDataCollections");
                Assert.Equal(7, collections.Rows.Count);
                Assert.Equal(2, collections.Columns.Count);

                var restrictions = adbcConnection.GetSchema("Restrictions");
                Assert.Equal(11, restrictions.Rows.Count);
                Assert.Equal(3, restrictions.Columns.Count);

                var catalogs = adbcConnection.GetSchema("Catalogs");
                Assert.Single(catalogs.Columns);
                var catalog = (string?)catalogs.Rows[0].ItemArray[0];

                catalogs = adbcConnection.GetSchema("Catalogs", new[] { catalog });
                Assert.Equal(1, catalogs.Rows.Count);

                string random = "X" + Guid.NewGuid().ToString("N");

                catalogs = adbcConnection.GetSchema("Catalogs", new[] { random });
                Assert.Equal(0, catalogs.Rows.Count);

                var schemas = adbcConnection.GetSchema("Schemas", new[] { catalog });
                Assert.Equal(2, schemas.Columns.Count);
                var schema = (string?)schemas.Rows[0].ItemArray[1];

                schemas = adbcConnection.GetSchema("Schemas", new[] { catalog, schema });
                Assert.Equal(1, schemas.Rows.Count);

                schemas = adbcConnection.GetSchema("Schemas", new[] { random });
                Assert.Equal(0, schemas.Rows.Count);

                schemas = adbcConnection.GetSchema("Schemas", new[] { catalog, random });
                Assert.Equal(0, schemas.Rows.Count);

                schemas = adbcConnection.GetSchema("Schemas", new[] { random, random });
                Assert.Equal(0, schemas.Rows.Count);

                var tableTypes = adbcConnection.GetSchema("TableTypes");
                Assert.Single(tableTypes.Columns);

                var tables = adbcConnection.GetSchema("Tables", new[] { catalog, schema });
                Assert.Equal(4, tables.Columns.Count);

                tables = adbcConnection.GetSchema("Tables", new[] { catalog, random });
                Assert.Equal(0, tables.Rows.Count);

                tables = adbcConnection.GetSchema("Tables", new[] { random, schema });
                Assert.Equal(0, tables.Rows.Count);

                tables = adbcConnection.GetSchema("Tables", new[] { random, random });
                Assert.Equal(0, tables.Rows.Count);

                tables = adbcConnection.GetSchema("Tables", new[] { catalog, schema, random });
                Assert.Equal(0, tables.Rows.Count);

                var columns = adbcConnection.GetSchema("Columns", new[] { catalog, schema });
                Assert.Equal(16, columns.Columns.Count);
            }
        }

        private Adbc.Client.AdbcConnection GetAdbcConnection(bool includeTableConstraints = true)
        {
            return new Adbc.Client.AdbcConnection(
                NewDriver, GetDriverParameters(TestConfiguration),
                []
            );
        }
    }
}
