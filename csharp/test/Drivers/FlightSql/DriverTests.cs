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
using Apache.Arrow.Adbc.Drivers.FlightSql;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Moq;

namespace Apache.Arrow.Adbc.Tests.Drivers.FlightSql
{
    /// <summary>
    /// Abstract class for the ADBC connection tests.
    /// </summary>
    [TestClass]
    public class DriverTests
    {
        /// <summary>
        /// Validates if the driver can connect to a live server and
        /// parse the results.
        /// </summary>
        [TestMethod]
        public void CanDriverExecuteQuery()
        {
            if (Utils.CanExecuteTestConfig(FlightSqlTestingUtils.FLIGHTSQL_TEST_CONFIG_VARIABLE))
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

                FlightSqlDriver flightSqlDriver = new FlightSqlDriver();
                FlightSqlDatabase flightSqlDatabase = flightSqlDriver.Open(parameters) as FlightSqlDatabase;
                FlightSqlConnection connection = flightSqlDatabase.Connect(options) as FlightSqlConnection;
                FlightSqlStatement statement = connection.CreateStatement() as FlightSqlStatement;

                statement.SqlQuery = flightSqlTestConfiguration.Query;
                QueryResult queryResult = statement.ExecuteQuery();

                Tests.DriverTests.CanExecuteQuery(queryResult, flightSqlTestConfiguration.ExpectedResultsCount);
            }
        }
    }
}
