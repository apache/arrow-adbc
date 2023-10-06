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
using System.Collections.ObjectModel;
using System.Data;
using System.Data.Common;
using System.IO;
using Apache.Arrow.Adbc.Client;
using NUnit.Framework;

namespace Apache.Arrow.Adbc.Tests.Drivers.Interop.Snowflake
{
    /// <summary>
    /// Class for testing the ADBC Client using the Snowflake ADBC driver.
    /// </summary>
    /// <remarks>
    /// Tests are ordered to ensure data is created
    /// for the other queries to run.
    /// </remarks>
    public class ClientTests
    {
        /// <summary>
        /// Validates if the client execute updates.
        /// </summary>
        [Test, Order(1)]
        public void CanClientExecuteUpdate()
        {
            if (Utils.CanExecuteTestConfig(SnowflakeTestingUtils.SNOWFLAKE_TEST_CONFIG_VARIABLE))
            {
                SnowflakeTestConfiguration testConfiguration = Utils.LoadTestConfiguration<SnowflakeTestConfiguration>(SnowflakeTestingUtils.SNOWFLAKE_TEST_CONFIG_VARIABLE);

                using (Client.AdbcConnection adbcConnection = GetSnowflakeAdbcConnection(testConfiguration))
                {
                    adbcConnection.Open();

                    string[] queries = SnowflakeTestingUtils.GetQueries(testConfiguration);

                    List<int> expectedResults = new List<int>() { -1, 1, 1 };

                    for (int i = 0; i < queries.Length; i++)
                    {
                        string query = queries[i];
                        AdbcCommand adbcCommand = adbcConnection.CreateCommand();
                        adbcCommand.CommandText = query;

                        int rows = adbcCommand.ExecuteNonQuery();

                        Assert.AreEqual(expectedResults[i], rows, $"The expected affected rows do not match the actual affected rows at position {i}.");
                    }
                }
            }
        }

        /// <summary>
        /// Validates if the client execute updates using the reader.
        /// </summary>
        [Test, Order(2)]
        public void CanClientExecuteUpdateUsingExecuteReader()
        {
            if (Utils.CanExecuteTestConfig(SnowflakeTestingUtils.SNOWFLAKE_TEST_CONFIG_VARIABLE))
            {
                SnowflakeTestConfiguration testConfiguration = Utils.LoadTestConfiguration<SnowflakeTestConfiguration>(SnowflakeTestingUtils.SNOWFLAKE_TEST_CONFIG_VARIABLE);

                using (Client.AdbcConnection adbcConnection = GetSnowflakeAdbcConnection(testConfiguration))
                {
                    adbcConnection.Open();

                    string[] queries = SnowflakeTestingUtils.GetQueries(testConfiguration);

                    List<object> expectedResults = new List<object>() { "Table ADBC_ALLTYPES successfully created.", 1L, 1L };

                    for (int i = 0; i < queries.Length; i++)
                    {
                        string query = queries[i];
                        AdbcCommand adbcCommand = adbcConnection.CreateCommand();
                        adbcCommand.CommandText = query;

                        AdbcDataReader reader = adbcCommand.ExecuteReader(CommandBehavior.Default);

                        if (reader.Read())
                        {
                            Assert.AreEqual(expectedResults[i], reader.GetValue(0), $"The expected affected rows do not match the actual affected rows at position {i}.");
                        }
                        else
                        {
                            Assert.Fail("Could not read the records");
                        }
                    }
                }
            }
        }

        /// <summary>
        /// Validates if the client can connect to a live server
        /// and parse the results.
        /// </summary>
        [Test, Order(3)]
        public void CanClientExecuteQuery()
        {
            if (Utils.CanExecuteTestConfig(SnowflakeTestingUtils.SNOWFLAKE_TEST_CONFIG_VARIABLE))
            {
                SnowflakeTestConfiguration testConfiguration = Utils.LoadTestConfiguration<SnowflakeTestConfiguration>(SnowflakeTestingUtils.SNOWFLAKE_TEST_CONFIG_VARIABLE);

                long count = 0;

                using (Client.AdbcConnection adbcConnection = GetSnowflakeAdbcConnection(testConfiguration))
                {
                    AdbcCommand adbcCommand = new AdbcCommand(testConfiguration.Query, adbcConnection);

                    adbcConnection.Open();

                    AdbcDataReader reader = adbcCommand.ExecuteReader();

                    try
                    {
                        while (reader.Read())
                        {
                            count++;
                        }
                    }
                    finally { reader.Close(); }
                }

                Assert.AreEqual(testConfiguration.ExpectedResultsCount, count);
            }
        }

        /// <summary>
        /// Validates if the client can connect to a live server
        /// using a connection string / private key and parse the results.
        /// </summary>
        [Test, Order(4)]
        public void CanClientExecuteQueryUsingPrivateKey()
        {
            if (Utils.CanExecuteTestConfig(SnowflakeTestingUtils.SNOWFLAKE_TEST_CONFIG_VARIABLE))
            {
                SnowflakeTestConfiguration testConfiguration = Utils.LoadTestConfiguration<SnowflakeTestConfiguration>(SnowflakeTestingUtils.SNOWFLAKE_TEST_CONFIG_VARIABLE);

                long count = 0;

                using (Client.AdbcConnection adbcConnection = GetSnowflakeAdbcConnectionUsingConnectionString(testConfiguration))
                {
                    AdbcCommand adbcCommand = new AdbcCommand(testConfiguration.Query, adbcConnection);

                    adbcConnection.Open();

                    AdbcDataReader reader = adbcCommand.ExecuteReader();

                    try
                    {
                        while (reader.Read())
                        {
                            count++;
                        }
                    }
                    finally { reader.Close(); }
                }

                Assert.AreEqual(testConfiguration.ExpectedResultsCount, count);
            }
        }

        /// <summary>
        /// Validates if the client is retrieving and converting values
        /// to the expected types.
        /// </summary>
        [Test, Order(5)]
        public void VerifyTypesAndValues()
        {
            if (Utils.CanExecuteTestConfig(SnowflakeTestingUtils.SNOWFLAKE_TEST_CONFIG_VARIABLE))
            {
                SnowflakeTestConfiguration testConfiguration = Utils.LoadTestConfiguration<SnowflakeTestConfiguration>(SnowflakeTestingUtils.SNOWFLAKE_TEST_CONFIG_VARIABLE);

                Client.AdbcConnection dbConnection = GetSnowflakeAdbcConnection(testConfiguration);
                dbConnection.Open();

                DbCommand dbCommand = dbConnection.CreateCommand();
                dbCommand.CommandText = testConfiguration.Query;

                DbDataReader reader = dbCommand.ExecuteReader(CommandBehavior.Default);

                if (reader.Read())
                {
                    ReadOnlyCollection<DbColumn> column_schema = reader.GetColumnSchema();

                    DataTable dataTable = reader.GetSchemaTable();

                    List<ColumnNetTypeArrowTypeValue> expectedValues = SampleData.GetSampleData();

                    for (int i = 0; i < reader.FieldCount; i++)
                    {
                        object value = reader.GetValue(i);
                        ColumnNetTypeArrowTypeValue ctv = expectedValues[i];

                        string readerColumnName = reader.GetName(i);
                        string dataTableColumnName = dataTable.Rows[i][SchemaTableColumn.ColumnName].ToString();

                        Assert.IsTrue(readerColumnName.Equals(ctv.Name, StringComparison.OrdinalIgnoreCase), $"`{readerColumnName}` != `{ctv.Name}` at position {i}. Verify the test query and sample data return in the same order in the reader.");

                        Assert.IsTrue(dataTableColumnName.Equals(ctv.Name, StringComparison.OrdinalIgnoreCase), $"`{dataTableColumnName}` != `{ctv.Name}` at position {i}. Verify the test query and sample data return in the same order in the data table.");

                        Tests.ClientTests.AssertTypeAndValue(ctv, value, reader, column_schema, dataTable);
                    }
                }
            }
        }

        private Client.AdbcConnection GetSnowflakeAdbcConnectionUsingConnectionString(SnowflakeTestConfiguration testConfiguration)
        {
            // see https://arrow.apache.org/adbc/0.5.1/driver/snowflake.html

            DbConnectionStringBuilder builder = new DbConnectionStringBuilder(true);

            builder["adbc.snowflake.sql.account"] = testConfiguration.Account;
            builder["adbc.snowflake.sql.warehouse"] = testConfiguration.Warehouse;
            builder["username"] = testConfiguration.User;

            if (!string.IsNullOrEmpty(testConfiguration.AuthenticationTokenPath))
            {
                string privateKey = File.ReadAllText(testConfiguration.AuthenticationTokenPath);
                builder["adbc.snowflake.sql.auth_type"] = testConfiguration.AuthenticationType;
                builder["adbc.snowflake.sql.client_option.auth_token"] = privateKey;
            }
            else
            {
                builder["password"] = testConfiguration.Password;
            }

            AdbcDriver snowflakeDriver = SnowflakeTestingUtils.GetSnowflakeAdbcDriver(testConfiguration);

            return new Client.AdbcConnection(builder.ConnectionString)
            {
                AdbcDriver = snowflakeDriver
            };
        }

        private Client.AdbcConnection GetSnowflakeAdbcConnection(SnowflakeTestConfiguration testConfiguration)
        {
            Dictionary<string, string> parameters = new Dictionary<string, string>();

            AdbcDriver snowflakeDriver = SnowflakeTestingUtils.GetSnowflakeAdbcDriver(testConfiguration, out parameters);

            Client.AdbcConnection adbcConnection = new Client.AdbcConnection(
                snowflakeDriver,
                parameters: parameters,
                options: new Dictionary<string, string>()
            );

            return adbcConnection;
        }
    }
}
