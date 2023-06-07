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
using System.IO;
using System.Linq;
using System.Text.Json;
using Apache.Arrow.Adbc.Tests;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Moq;

namespace Apache.Arrow.Adbc.FlightSql.Tests
{
    /// <summary>
    /// Abstract class for the ADBC connection tests.
    /// </summary>
    [TestClass]
    public class ConnectionTests
    {
        /// <summary>
        /// Validates if the driver behaves as it should with missing values and parsing mock results.
        /// </summary>
        [TestMethod]
        public void CanMockDriverConnect()
        {
            Mock<FlightSqlStatement> mockFlightSqlStatement = GetMockSqlStatement();

            FlightSqlDatabase db = new FlightSqlDatabase(new Dictionary<string, string>());

            Assert.ThrowsException<ArgumentNullException>(() => db.Connect(null));

            Assert.ThrowsException<ArgumentException>(() => db.Connect(new Dictionary<string, string>()));

            QueryResult queryResult = mockFlightSqlStatement.Object.ExecuteQuery();

            Adbc.Tests.ConnectionTests.CanDriverConnect(queryResult, 50);
        }

        /// <summary>
        /// Validates if the driver can connect to a live server and parse the results.
        /// </summary>
        [TestMethod]
        public void CanDriverConnect()
        {
            FlightSqlTestConfiguration flightSqlTestConfiguration = GetFlightSqlTestConfiguration();

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

            Adbc.Tests.ConnectionTests.CanDriverConnect(queryResult, flightSqlTestConfiguration.ExpectedResultsCount);
        }

        /// <summary>
        /// Validates exceptions thrown are ADBC exceptions
        /// </summary>
        [TestMethod]
        public void VerifyBadQueryGeneratesError()
        {
            Mock<FlightSqlStatement> mockFlightSqlStatement = GetMockSqlStatement();

            mockFlightSqlStatement.Setup(s => s.ExecuteQuery()).Throws(new MockAdbcException());

            try
            {
                mockFlightSqlStatement.Object.ExecuteQuery();
            }
            catch(AdbcException e)
            {
                Adbc.Tests.ConnectionTests.VerifyBadQueryGeneratesError(e);
            }
        }

        /// <summary>
        /// Loads a FlightSqlStatement with mocked results. 
        /// </summary>
        /// <returns></returns>
        private Mock<FlightSqlStatement> GetMockSqlStatement()
        {
            List<RecordBatch> recordBatches = Utils.LoadTestRecordBatches();

            Schema s = recordBatches.First().Schema;
            QueryResult mockQueryResult = new QueryResult(50, new MockArrayStream(s, recordBatches));
            FlightSqlConnection cn = new FlightSqlConnection(null);

            Mock<FlightSqlStatement> mockFlightSqlStatement = new Mock<FlightSqlStatement>(cn);
            mockFlightSqlStatement.Setup(s => s.ExecuteQuery()).Returns(mockQueryResult);

            return mockFlightSqlStatement;
        }

        /// <summary>
        /// Gets the configuration for connecting to a live Flight SQL server.
        /// </summary>
        /// <returns></returns>
        private FlightSqlTestConfiguration GetFlightSqlTestConfiguration()
        {
            // use a JSON file vs. setting up environment variables
            string json = File.ReadAllText("flightsqlconfig.json");

            FlightSqlTestConfiguration flightSqlTestConfiguration = JsonSerializer.Deserialize<FlightSqlTestConfiguration>(json);

            return flightSqlTestConfiguration;
        }
    }
}
