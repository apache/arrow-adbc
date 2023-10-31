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
using Apache.Arrow.Adbc.Client;
using Apache.Arrow.Adbc.Drivers.FlightSql;
using Xunit;

namespace Apache.Arrow.Adbc.Tests.Drivers.FlightSql
{
    /// <summary>
    /// Class for testing the ADBC Client using the FlightSql ADBC driver.
    /// </summary>
    public class ClientTests
    {
        public ClientTests()
        {
            Skip.IfNot(Utils.CanExecuteTestConfig(FlightSqlTestingUtils.FLIGHTSQL_TEST_CONFIG_VARIABLE));
        }

        /// <summary>
        /// Validates if the client can connect to a live server and
        /// parse the results.
        /// </summary>
        [SkippableFact]
        public void CanFlightSqlConnectUsingClient()
        {
            FlightSqlTestConfiguration flightSqlTestConfiguration = Utils.LoadTestConfiguration<FlightSqlTestConfiguration>(FlightSqlTestingUtils.FLIGHTSQL_TEST_CONFIG_VARIABLE);

            Dictionary<string, string> parameters = new Dictionary<string, string>
            {
                { FlightSqlParameters.ServerAddress, flightSqlTestConfiguration.ServerAddress },
                { FlightSqlParameters.RoutingTag, flightSqlTestConfiguration.RoutingTag },
                { FlightSqlParameters.RoutingQueue, flightSqlTestConfiguration.RoutingQueue },
                { FlightSqlParameters.Authorization, flightSqlTestConfiguration.Authorization}
            };

            Dictionary<string, string> options = new Dictionary<string, string>()
            {
                { FlightSqlParameters.ServerAddress, flightSqlTestConfiguration.ServerAddress },
            };

            long count = 0;

            using (Client.AdbcConnection adbcConnection = new Client.AdbcConnection(
                new FlightSqlDriver(),
                parameters,
                options)
            )
            {
                string query = flightSqlTestConfiguration.Query;

                AdbcCommand adbcCommand = new AdbcCommand(query, adbcConnection);

                adbcConnection.Open();

                AdbcDataReader reader = adbcCommand.ExecuteReader();

                try
                {
                    while (reader.Read())
                    {
                        count++;

                        for (int i = 0; i < reader.FieldCount; i++)
                        {
                            object value = reader.GetValue(i);

                            if (value == null)
                                value = "(null)";

                            Console.WriteLine($"{reader.GetName(i)}: {value}");
                        }
                    }
                }
                finally { reader.Close(); }
            }

            Assert.Equal(flightSqlTestConfiguration.ExpectedResultsCount, count);
        }
    }
}
