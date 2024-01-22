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
using System.Data;
using System.Data.Common;
using System.Data.SqlTypes;
using System.Diagnostics;
using System.IO;
using Apache.Arrow.Adbc.Client;
using Apache.Arrow.Adbc.Tests.Xunit;
using Xunit;

namespace Apache.Arrow.Adbc.Tests.Drivers.Interop.Snowflake
{
    /// <summary>
    /// Class for testing the ADBC Client using the Snowflake ADBC driver.
    /// </summary>
    /// <remarks>
    /// Tests are ordered to ensure data is created
    /// for the other queries to run.
    /// </remarks>
    [TestCaseOrderer("Apache.Arrow.Adbc.Tests.Xunit.TestOrderer", "Apache.Arrow.Adbc.Tests")]
    public class ClientTests
    {
        public ClientTests()
        {
            Skip.IfNot(Utils.CanExecuteTestConfig(SnowflakeTestingUtils.SNOWFLAKE_TEST_CONFIG_VARIABLE));
        }

        /// <summary>
        /// Validates if the client execute updates.
        /// </summary>
        [SkippableFact, Order(1)]
        public void CanClientExecuteUpdate()
        {
            SnowflakeTestConfiguration testConfiguration = Utils.LoadTestConfiguration<SnowflakeTestConfiguration>(SnowflakeTestingUtils.SNOWFLAKE_TEST_CONFIG_VARIABLE);

            using (Adbc.Client.AdbcConnection adbcConnection = GetSnowflakeAdbcConnectionUsingConnectionString(testConfiguration))
            {
                string[] queries = SnowflakeTestingUtils.GetQueries(testConfiguration);

                List<int> expectedResults = new List<int>() { -1, 1, 1 };

                Tests.ClientTests.CanClientExecuteUpdate(adbcConnection, testConfiguration, queries, expectedResults);
            }
        }

        /// <summary>
        /// Validates if the client execute updates using the reader.
        /// </summary>
        [SkippableFact, Order(2)]
        public void CanClientExecuteUpdateUsingExecuteReader()
        {
            SnowflakeTestConfiguration testConfiguration = Utils.LoadTestConfiguration<SnowflakeTestConfiguration>(SnowflakeTestingUtils.SNOWFLAKE_TEST_CONFIG_VARIABLE);

            using (Adbc.Client.AdbcConnection adbcConnection = GetSnowflakeAdbcConnectionUsingConnectionString(testConfiguration))
            {
                adbcConnection.Open();

                string[] queries = SnowflakeTestingUtils.GetQueries(testConfiguration);

                List<object> expectedResults = new List<object>() { $"Table {testConfiguration.Metadata.Table} successfully created.", new SqlDecimal(1L), new SqlDecimal(1L) };

                for (int i = 0; i < queries.Length; i++)
                {
                    string query = queries[i];
                    AdbcCommand adbcCommand = adbcConnection.CreateCommand();
                    adbcCommand.CommandText = query;

                    AdbcDataReader reader = adbcCommand.ExecuteReader(CommandBehavior.Default);

                    if (reader.Read())
                    {
                        object result = expectedResults[i];
                        Assert.True(result.Equals(reader.GetValue(0)), $"The expected affected rows do not match the actual affected rows at position {i}.");
                    }
                    else
                    {
                        Assert.Fail("Could not read the records");
                    }
                }
            }
        }

        /// <summary>
        /// Validates if the client can get the schema.
        /// </summary>
        [SkippableFact, Order(3)]
        public void CanClientGetSchema()
        {
            SnowflakeTestConfiguration testConfiguration = Utils.LoadTestConfiguration<SnowflakeTestConfiguration>(SnowflakeTestingUtils.SNOWFLAKE_TEST_CONFIG_VARIABLE);

            using (Adbc.Client.AdbcConnection adbcConnection = GetSnowflakeAdbcConnectionUsingConnectionString(testConfiguration))
            {
                Tests.ClientTests.CanClientGetSchema(adbcConnection, testConfiguration);
            }
        }

        /// <summary>
        /// Validates if the client can connect to a live server
        /// and parse the results.
        /// </summary>
        [SkippableFact, Order(4)]
        public void CanClientExecuteQuery()
        {
            SnowflakeTestConfiguration testConfiguration = Utils.LoadTestConfiguration<SnowflakeTestConfiguration>(SnowflakeTestingUtils.SNOWFLAKE_TEST_CONFIG_VARIABLE);

            using (Adbc.Client.AdbcConnection adbcConnection = GetSnowflakeAdbcConnectionUsingConnectionString(testConfiguration))
            {
                Tests.ClientTests.CanClientExecuteQuery(adbcConnection, testConfiguration);
            }
        }

        /// <summary>
        /// SHOW TABLES has an issue with the maximum number of results.
        /// In Snowflake, this is 10,000. However, with Arrow, this appears to be somewhere around 500
        /// in a result set. See https://github.com/apache/arrow-adbc/issues/1454.
        /// To be able to run this command and get all of the results, the caller can use pagination
        /// by including the LIMIT and FROM keywords in the query. See https://docs.snowflake.com/en/sql-reference/sql/show-tables
        /// This test demonstrates how to return a large number of tables (in this tested example, there
        /// were 1061 tables).
        /// </summary>
        [SkippableFact, Order(4)]
        public void CanClientExecuteShowTables()
        {
            SnowflakeTestConfiguration testConfiguration = Utils.LoadTestConfiguration<SnowflakeTestConfiguration>(SnowflakeTestingUtils.SNOWFLAKE_TEST_CONFIG_VARIABLE);

            using (Adbc.Client.AdbcConnection adbcConnection = GetSnowflakeAdbcConnectionUsingConnectionString(testConfiguration))
            {
                string lastTable = string.Empty;
                int maxLimit = 100;
                int totalCount = 0;

                while (true)
                {
                    testConfiguration.Query = $"SHOW TABLES LIMIT {maxLimit}";

                    if(!string.IsNullOrEmpty(lastTable))
                    {
                        testConfiguration.Query += $" FROM '{lastTable}'";
                    }

                    adbcConnection.Open();

                    using AdbcCommand adbcCommand = new AdbcCommand(testConfiguration.Query, adbcConnection);

                    using AdbcDataReader reader = adbcCommand.ExecuteReader();
                    int count = 0;

                    try
                    {
                        while (reader.Read())
                        {
                            object value = reader["name"];

                            if (value != null)
                            {
                                totalCount++;
                                count++;
                                lastTable = value.ToString();

                                SnowflakeTestingUtils.Output($"{totalCount}.) {lastTable}");
                            }
                        }
                    }
                    finally { reader.Close(); }

                    if (count == 0)
                        break;
                }

                SnowflakeTestingUtils.Output($"Found {totalCount} tables");
            }
        }

        // <summary>
        /// Validates if the client can connect to a live server
        /// and parse the results.
        /// </summary>
        [SkippableFact, Order(4)]
        public void CanClientExecuteQueryWithNoResults()
        {
            SnowflakeTestConfiguration testConfiguration = Utils.LoadTestConfiguration<SnowflakeTestConfiguration>(SnowflakeTestingUtils.SNOWFLAKE_TEST_CONFIG_VARIABLE);
            testConfiguration.Query = "SELECT * WHERE 0=1";
            testConfiguration.ExpectedResultsCount = 0;

            using (Adbc.Client.AdbcConnection adbcConnection = GetSnowflakeAdbcConnectionUsingConnectionString(testConfiguration))
            {
                Tests.ClientTests.CanClientExecuteQuery(adbcConnection, testConfiguration);
            }
        }

        /// <summary>
        /// Validates if the client can connect to a live server
        /// using a connection string / private key and parse the results.
        /// </summary>
        [SkippableFact, Order(5)]
        public void CanClientExecuteQueryUsingPrivateKey()
        {
            SnowflakeTestConfiguration testConfiguration = Utils.LoadTestConfiguration<SnowflakeTestConfiguration>(SnowflakeTestingUtils.SNOWFLAKE_TEST_CONFIG_VARIABLE);

            Skip.If(testConfiguration.Authentication.SnowflakeJwt is null, "JWT authentication is not configured");

            using (Adbc.Client.AdbcConnection adbcConnection = GetSnowflakeAdbcConnectionUsingConnectionString(testConfiguration, SnowflakeAuthentication.AuthJwt))
            {
                Tests.ClientTests.CanClientExecuteQuery(adbcConnection, testConfiguration);
            }
        }

        /// <summary>
        /// Validates if the client is retrieving and converting values
        /// to the expected types.
        /// </summary>
        [SkippableFact, Order(6)]
        public void VerifyTypesAndValues()
        {
            SnowflakeTestConfiguration testConfiguration = Utils.LoadTestConfiguration<SnowflakeTestConfiguration>(SnowflakeTestingUtils.SNOWFLAKE_TEST_CONFIG_VARIABLE);

            using (Adbc.Client.AdbcConnection adbcConnection = GetSnowflakeAdbcConnectionUsingConnectionString(testConfiguration))
            {
                SampleDataBuilder sampleDataBuilder = SnowflakeData.GetSampleData();

                Tests.ClientTests.VerifyTypesAndValues(adbcConnection, sampleDataBuilder);
            }
        }

        [SkippableFact, Order(7)]
        public void VerifySchemaTables()
        {
            SnowflakeTestConfiguration testConfiguration = Utils.LoadTestConfiguration<SnowflakeTestConfiguration>(SnowflakeTestingUtils.SNOWFLAKE_TEST_CONFIG_VARIABLE);

            using (Adbc.Client.AdbcConnection adbcConnection = GetSnowflakeAdbcConnectionUsingConnectionString(testConfiguration))
            {
                adbcConnection.Open();

                var collections = adbcConnection.GetSchema("MetaDataCollections");
                Assert.Equal(7, collections.Rows.Count);
                Assert.Equal(2, collections.Columns.Count);

                var restrictions = adbcConnection.GetSchema("Restrictions");
                Assert.Equal(11, restrictions.Rows.Count);
                Assert.Equal(3, restrictions.Columns.Count);

                var catalogs = adbcConnection.GetSchema("Catalogs");
                Assert.Equal(1, catalogs.Columns.Count);
                var catalog = (string)catalogs.Rows[0].ItemArray[0];

                catalogs = adbcConnection.GetSchema("Catalogs", new[] { catalog });
                Assert.Equal(1, catalogs.Rows.Count);

                var schemas = adbcConnection.GetSchema("Schemas", new[] { catalog });
                Assert.Equal(2, schemas.Columns.Count);

                var schema = "INFORMATION_SCHEMA";
                schemas = adbcConnection.GetSchema("Schemas", new[] { catalog, schema });
                Assert.Equal(1, schemas.Rows.Count);

                var tableTypes = adbcConnection.GetSchema("TableTypes");
                Assert.Equal(1, tableTypes.Columns.Count);

                var tables = adbcConnection.GetSchema("Tables", new[] { catalog, schema });
                Assert.Equal(4, tables.Columns.Count);
                Assert.Equal(32, tables.Rows.Count);

                var columns = adbcConnection.GetSchema("Columns", new[] { catalog, schema });
                Assert.Equal(16, columns.Columns.Count);
                Assert.Equal(441, columns.Rows.Count);
            }
        }

        [SkippableFact, Order(8)]
        public void CanClientDeleteRecords()
        {
            SnowflakeTestConfiguration testConfiguration = Utils.LoadTestConfiguration<SnowflakeTestConfiguration>(SnowflakeTestingUtils.SNOWFLAKE_TEST_CONFIG_VARIABLE);

            using (Adbc.Client.AdbcConnection adbcConnection = GetSnowflakeAdbcConnectionUsingConnectionString(testConfiguration))
            {
                testConfiguration.Query = $"DELETE FROM {testConfiguration.Metadata.Catalog}.{testConfiguration.Metadata.Schema}.{testConfiguration.Metadata.Table}";

                Tests.ClientTests.CanClientExecuteDeleteQuery(adbcConnection, testConfiguration);
            }
        }

        [SkippableFact, Order(9)]
        public void CanClientExecuteMultipleQueries()
        {
            SnowflakeTestConfiguration testConfiguration = Utils.LoadTestConfiguration<SnowflakeTestConfiguration>(SnowflakeTestingUtils.SNOWFLAKE_TEST_CONFIG_VARIABLE);

            using (Adbc.Client.AdbcConnection adbcConnection = GetSnowflakeAdbcConnectionUsingConnectionString(testConfiguration))
            {
                string multi_query = string.Empty;

                string[] queries = SnowflakeTestingUtils.GetQueries(testConfiguration);

                foreach(string query in queries)
                {
                    multi_query += query;

                    if(!multi_query.EndsWith(";"))
                        multi_query += ";";
                }

                multi_query += $"SELECT * FROM {testConfiguration.Metadata.Catalog}.{testConfiguration.Metadata.Schema}.{testConfiguration.Metadata.Table};";
                multi_query += $"DELETE FROM {testConfiguration.Metadata.Catalog}.{testConfiguration.Metadata.Schema}.{testConfiguration.Metadata.Table}";
                testConfiguration.Query = multi_query;

                Tests.ClientTests.CanClientExecuteMultipleQueries(adbcConnection, testConfiguration);
            }
        }

        private Adbc.Client.AdbcConnection GetSnowflakeAdbcConnectionUsingConnectionString(SnowflakeTestConfiguration testConfiguration, string authType = null)
        {
            // see https://arrow.apache.org/adbc/0.5.1/driver/snowflake.html

            DbConnectionStringBuilder builder = new DbConnectionStringBuilder(true);
            builder[SnowflakeParameters.ACCOUNT] = testConfiguration.Account;
            builder[SnowflakeParameters.WAREHOUSE] = testConfiguration.Warehouse;
            builder[SnowflakeParameters.HOST] = testConfiguration.Host;
            builder[SnowflakeParameters.DATABASE] = testConfiguration.Database;
            builder[SnowflakeParameters.USERNAME] = testConfiguration.User;

            if (authType == SnowflakeAuthentication.AuthJwt || testConfiguration.Authentication.SnowflakeJwt != null)
            {
                string privateKey = testConfiguration.Authentication.SnowflakeJwt.PrivateKey;

                if(string.IsNullOrEmpty(privateKey))
                {
                    if (!string.IsNullOrEmpty(testConfiguration.Authentication.SnowflakeJwt.PrivateKeyFile))
                        privateKey = File.ReadAllText(testConfiguration.Authentication.SnowflakeJwt.PrivateKeyFile);
                }

                builder[SnowflakeParameters.AUTH_TYPE] = SnowflakeAuthentication.AuthJwt;
                builder[SnowflakeParameters.PKCS8_VALUE] = privateKey;
                builder[SnowflakeParameters.USERNAME] = testConfiguration.Authentication.SnowflakeJwt.User;
                if (!string.IsNullOrEmpty(testConfiguration.Authentication.SnowflakeJwt.PrivateKeyPassPhrase))
                {
                    builder[SnowflakeParameters.PKCS8_PASS] = testConfiguration.Authentication.SnowflakeJwt.PrivateKeyPassPhrase;
                }
            }
            else if (authType == SnowflakeAuthentication.AuthOAuth)
            {
                builder[SnowflakeParameters.AUTH_TYPE] = SnowflakeAuthentication.AuthOAuth;
                builder[SnowflakeParameters.AUTH_TOKEN] = testConfiguration.Authentication.OAuth.Token;
                if (testConfiguration.Authentication.OAuth.User != null)
                {
                    builder[SnowflakeParameters.USERNAME] = testConfiguration.Authentication.OAuth.User;
                }
            }
            else if (string.IsNullOrEmpty(authType) || authType == SnowflakeAuthentication.AuthSnowflake)
            {
                // if no auth type is specified, use the snowflake auth
                builder[SnowflakeParameters.AUTH_TYPE] = SnowflakeAuthentication.AuthSnowflake;
                builder[SnowflakeParameters.USERNAME] = testConfiguration.Authentication.Default.User;
                builder[SnowflakeParameters.PASSWORD] = testConfiguration.Authentication.Default.Password;
            }
            AdbcDriver snowflakeDriver = SnowflakeTestingUtils.GetSnowflakeAdbcDriver(testConfiguration);
            return new Adbc.Client.AdbcConnection(builder.ConnectionString)
            {
                AdbcDriver = snowflakeDriver
            };
        }
    }
}
