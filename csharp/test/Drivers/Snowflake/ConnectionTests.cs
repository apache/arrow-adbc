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
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Moq;

namespace Apache.Arrow.Adbc.Tests.Drivers.Interop.Snowflake
{
    /// <summary>
    /// Class for testing the Snowflake ADBC driver connection tests.
    /// </summary>
    [TestClass]
    public class ConnectionTests
    {
        int expectedResultsCount = 1;

        /// <summary>
        /// Validates if the driver behaves as it should with missing
        /// values and parsing mock results.
        /// </summary>
        [TestMethod]
        public void CanMockDriverConnect()
        {
            Mock<IAdbcStatement> mockStatement = Utils.GetMockStatement(
                "resources/snowflake.arrow",
                expectedResultsCount
            );

            QueryResult queryResult = mockStatement.Object.ExecuteQuery();

            Adbc.Tests.ConnectionTests.CanDriverExecuteQuery(queryResult, expectedResultsCount);
        }

        /// <summary>
        /// Validates if the driver can connect to a live server and
        /// parse the results.
        /// </summary>
        [TestMethod]
        public void CanDriverExecuteQuery()
        {
            SnowflakeTestConfiguration testConfiguration = Utils.GetTestConfiguration<SnowflakeTestConfiguration>("resources/snowflakeconfig.json");

            Dictionary<string, string> parameters = new Dictionary<string, string>();
            Dictionary<string, string> options = new Dictionary<string, string>();

            AdbcDriver snowflakeDriver = SnowflakeTestingUtils.GetSnowflakeAdbcDriver(testConfiguration, out parameters);

            AdbcDatabase adbcDatabase = snowflakeDriver.Open(parameters);
            AdbcConnection adbcConnection = adbcDatabase.Connect(options);

            Console.WriteLine(testConfiguration.Query);

            AdbcStatement statement = adbcConnection.CreateStatement();
            statement.SqlQuery = testConfiguration.Query;

            QueryResult queryResult = statement.ExecuteQuery();

            Adbc.Tests.ConnectionTests.CanDriverExecuteQuery(queryResult, testConfiguration.ExpectedResultsCount);
        }

        /// <summary>
        /// Validates if the driver can connect to a live server and
        /// parse the results.
        /// </summary>
        [TestMethod]
        public void CanDriverExecuteUpdate()
        {
            SnowflakeTestConfiguration testConfiguration = Utils.GetTestConfiguration<SnowflakeTestConfiguration>("resources/snowflakeconfig.json");

            Dictionary<string, string> parameters = new Dictionary<string, string>();
            Dictionary<string, string> options = new Dictionary<string, string>();

            AdbcDriver snowflakeDriver = SnowflakeTestingUtils.GetSnowflakeAdbcDriver(testConfiguration, out parameters);

            AdbcDatabase adbcDatabase = snowflakeDriver.Open(parameters);
            AdbcConnection adbcConnection = adbcDatabase.Connect(options);

            string[] queries = File.ReadAllText("resources/SnowflakeData.sql").Split(";".ToCharArray());

            List<int> expectedResults = new List<int>() { -1,1,1 };

            // the last query is blank
            for (int i=0; i<queries.Length-1;i++)
            {
                string query = queries[i];
                AdbcStatement statement = adbcConnection.CreateStatement();
                statement.SqlQuery = query;

                UpdateResult updateResult = statement.ExecuteUpdate();

                Assert.AreEqual(expectedResults[i], updateResult.AffectedRows, $"The expected affected rows do not match the actual affected rows at position {i}.");
            }
        }

        /// <summary>
        /// Validates exceptions thrown are ADBC exceptions
        /// </summary>
        [TestMethod]
        public void VerifyBadQueryGeneratesError()
        {
            Mock<IAdbcStatement> mockStatement = Utils.GetMockStatement(
                "resources/snowflake.arrow",
                expectedResultsCount
            );

            mockStatement.Setup(s => s.ExecuteQuery()).Throws(new MockAdbcException());

            try
            {
                mockStatement.Object.ExecuteQuery();
            }
            catch (AdbcException e)
            {
                Adbc.Tests.ConnectionTests.VerifyBadQueryGeneratesError(e);
            }
        }
    }
}
