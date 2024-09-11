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
using Apache.Arrow.Adbc.Client;
using Apache.Arrow.Adbc.Tests.Xunit;
using Xunit;

namespace Apache.Arrow.Adbc.Tests.Drivers.Interop.FlightSql
{
    /// <summary>
    /// Class for testing the ADBC Client using the Flight SQL ADBC driver.
    /// </summary>
    /// <remarks>
    /// Tests are ordered to ensure data is created
    /// for the other queries to run.
    /// </remarks>
    [TestCaseOrderer("Apache.Arrow.Adbc.Tests.Xunit.TestOrderer", "Apache.Arrow.Adbc.Tests")]
    public class ClientTests
    {
        readonly FlightSqlTestConfiguration _testConfiguration;
        readonly FlightSqlTestEnvironment _environment;

        public ClientTests()
        {
            Skip.IfNot(Utils.CanExecuteTestConfig(FlightSqlTestingUtils.FLIGHTSQL_TEST_CONFIG_VARIABLE));
            _testConfiguration = FlightSqlTestingUtils.LoadFlightSqlTestConfiguration(FlightSqlTestingUtils.FLIGHTSQL_TEST_CONFIG_VARIABLE);
            _environment = FlightSqlTestingUtils.GetTestEnvironment(_testConfiguration);
        }

        /// <summary>
        /// Validates if the client execute updates.
        /// </summary>
        [SkippableFact, Order(1)]
        public void CanClientExecuteUpdate()
        {
            using (Adbc.Client.AdbcConnection adbcConnection = GetFlightSqlAdbcConnectionUsingConnectionString(_testConfiguration))
            {
                string[] queries = FlightSqlTestingUtils.GetQueries(_environment);

                List<int> expectedResults = new List<int>() { -1, 1, 1 };

                Tests.ClientTests.CanClientExecuteUpdate(adbcConnection, _environment, queries, expectedResults);
            }
        }

        /// <summary>
        /// Validates if the client execute updates using the reader.
        /// </summary>
        [SkippableFact, Order(2)]
        public void CanClientExecuteUpdateUsingExecuteReader()
        {
            using (Adbc.Client.AdbcConnection adbcConnection = GetFlightSqlAdbcConnectionUsingConnectionString(_testConfiguration))
            {
                adbcConnection.Open();

                string[] queries = FlightSqlTestingUtils.GetQueries(_environment);

                List<object> expectedResults = new List<object>() { $"Table {_environment.Metadata.Table} successfully created.", new SqlDecimal(1L), new SqlDecimal(1L) };

                for (int i = 0; i < queries.Length; i++)
                {
                    string query = queries[i];
                    AdbcCommand adbcCommand = adbcConnection.CreateCommand();
                    adbcCommand.CommandText = query;

                    AdbcDataReader reader = adbcCommand.ExecuteReader(CommandBehavior.Default);

                    if (reader.Read())
                    {
                        Assert.True(expectedResults[i].Equals(reader.GetValue(0)), $"The expected affected rows do not match the actual affected rows at position {i}.");
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
            using (Adbc.Client.AdbcConnection adbcConnection = GetFlightSqlAdbcConnectionUsingConnectionString(_testConfiguration))
            {
                Tests.ClientTests.CanClientGetSchema(adbcConnection, _environment);
            }
        }

        /// <summary>
        /// Validates if the client can connect to a live server
        /// and parse the results.
        /// </summary>
        [SkippableFact, Order(4)]
        public void CanClientExecuteQuery()
        {
            using (Adbc.Client.AdbcConnection adbcConnection = GetFlightSqlAdbcConnectionUsingConnectionString(_testConfiguration))
            {
                Tests.ClientTests.CanClientExecuteQuery(adbcConnection, _environment);
            }
        }

        // <summary>
        /// Validates if the client can connect to a live server
        /// and parse the results.
        /// </summary>
        [SkippableFact, Order(4)]
        public void CanClientExecuteQueryWithNoResults()
        {
            FlightSqlTestEnvironment environment = FlightSqlTestingUtils.GetTestEnvironment(_testConfiguration);
            environment.Query = "SELECT * WHERE 0=1";
            environment.ExpectedResultsCount = 0;

            using (Adbc.Client.AdbcConnection adbcConnection = GetFlightSqlAdbcConnectionUsingConnectionString(_testConfiguration))
            {
                Tests.ClientTests.CanClientExecuteQuery(adbcConnection, environment);
            }
        }

        /// <summary>
        /// Validates if the client is retrieving and converting values
        /// to the expected types.
        /// </summary>
        [SkippableFact, Order(6)]
        public void VerifyTypesAndValues()
        {
            using (Adbc.Client.AdbcConnection adbcConnection = GetFlightSqlAdbcConnectionUsingConnectionString(_testConfiguration))
            {
                SampleDataBuilder sampleDataBuilder = FlightSqlData.GetSampleData();

                Tests.ClientTests.VerifyTypesAndValues(adbcConnection, sampleDataBuilder);
            }
        }

        [SkippableFact]
        public void VerifySchemaTables()
        {
            using (Adbc.Client.AdbcConnection adbcConnection = GetFlightSqlAdbcConnectionUsingConnectionString(_testConfiguration))
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
                object? catalogObj = catalogs.Rows[0].ItemArray[0];

                Assert.True(catalogObj != null);

                string catalog = (string)catalogObj!;

                catalogs = adbcConnection.GetSchema("Catalogs", new[] { catalog });
                Assert.Equal(1, catalogs.Rows.Count);

                var schemas = adbcConnection.GetSchema("Schemas", new[] { catalog });
                Assert.Equal(2, schemas.Columns.Count);

                var schema = "INFORMATION_SCHEMA";
                schemas = adbcConnection.GetSchema("Schemas", new[] { catalog, schema });
                Assert.Equal(1, schemas.Rows.Count);

                var tableTypes = adbcConnection.GetSchema("TableTypes");
                Assert.Single(tableTypes.Columns);

                var tables = adbcConnection.GetSchema("Tables", new[] { catalog, schema });
                Assert.Equal(4, tables.Columns.Count);
                Assert.Equal(32, tables.Rows.Count);

                var columns = adbcConnection.GetSchema("Columns", new[] { catalog, schema });
                Assert.Equal(16, columns.Columns.Count);
                Assert.Equal(441, columns.Rows.Count);
            }
        }

        private Adbc.Client.AdbcConnection GetFlightSqlAdbcConnectionUsingConnectionString(FlightSqlTestConfiguration testConfiguration, string? authType = null)
        {
            // see https://arrow.apache.org/adbc/main/driver/flight_sql.html

            FlightSqlTestEnvironment environment = _environment;

            DbConnectionStringBuilder builder = new DbConnectionStringBuilder(true);
            if (!string.IsNullOrEmpty(environment.Uri))
            {
                builder[FlightSqlParameters.Uri] = environment.Uri;
            }

            foreach (string key in environment.RPCCallHeaders.Keys)
            {
                builder[FlightSqlParameters.OptionRPCCallHeaderPrefix + key] = environment.RPCCallHeaders[key];
            }

            if (!string.IsNullOrEmpty(environment.AuthorizationHeader))
            {
                builder[FlightSqlParameters.OptionAuthorizationHeader] = environment.AuthorizationHeader;
            }
            else
            {
                if (!string.IsNullOrEmpty(environment.Username) && !string.IsNullOrEmpty(environment.Password))
                {
                    builder[FlightSqlParameters.Username] = environment.Username;
                    builder[FlightSqlParameters.Password] = environment.Password;
                }
            }

            if (!string.IsNullOrEmpty(environment.TimeoutQuery))
                builder[FlightSqlParameters.OptionTimeoutQuery] = environment.TimeoutQuery;

            if (!string.IsNullOrEmpty(environment.TimeoutFetch))
                builder[FlightSqlParameters.OptionTimeoutFetch] = environment.TimeoutFetch;

            if (!string.IsNullOrEmpty(environment.TimeoutUpdate))
                builder[FlightSqlParameters.OptionTimeoutUpdate] = environment.TimeoutUpdate;

            if (environment.SSLSkipVerify)
                builder[FlightSqlParameters.OptionSSLSkipVerify] = Convert.ToString(environment.SSLSkipVerify).ToLowerInvariant();

            if (!string.IsNullOrEmpty(environment.Authority))
                builder[FlightSqlParameters.OptionAuthority] = environment.Authority;

            AdbcDriver driver = FlightSqlTestingUtils.GetFlightSqlAdbcDriver(testConfiguration);

            return new Adbc.Client.AdbcConnection(builder.ConnectionString)
            {
                AdbcDriver = driver
            };
        }
    }
}
