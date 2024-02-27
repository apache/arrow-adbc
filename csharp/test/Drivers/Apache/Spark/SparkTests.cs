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
using Xunit;

namespace Apache.Arrow.Adbc.Tests.Drivers.Apache.Spark
{
    [TestCaseOrderer("Apache.Arrow.Adbc.Tests.Xunit.TestOrderer", "Apache.Arrow.Adbc.Tests")]
    public class SparkTests
    {
        /// <summary>
        /// Validates if the driver can connect to a live server and
        /// parse the results.
        /// </summary>
        [SkippableFact]
        public void CanDriverConnect()
        {
            ApacheTestConfiguration testConfiguration = Utils.GetTestConfiguration<ApacheTestConfiguration>("sparkconfig.json");

            Dictionary<string, string> parameters = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
            {
                { "HostName", testConfiguration.HostName  },
                { "Path", testConfiguration.Path },
                { "Token", testConfiguration.Token },
            };

            AdbcDatabase database = new SparkDriver().Open(parameters);
            AdbcConnection connection = database.Connect(new Dictionary<string, string>());
            AdbcStatement statement = connection.CreateStatement();
            statement.SqlQuery = testConfiguration.Query;

            QueryResult queryResult = statement.ExecuteQuery();

            //Adbc.Tests.ConnectionTests.CanDriverConnect(queryResult, testConfiguration.ExpectedResultsCount);
        }
    }
}
