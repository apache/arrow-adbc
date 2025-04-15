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
using System.Threading.Tasks;
using Apache.Arrow.Adbc.Drivers.Databricks;
using Xunit;
using Xunit.Abstractions;

namespace Apache.Arrow.Adbc.Tests.Drivers.Databricks
{
    /// <summary>
    /// End-to-end tests for the server-side property passthrough feature in the Databricks ADBC driver.
    /// </summary>
    public class ServerSidePropertyE2ETest : TestBase<DatabricksTestConfiguration, DatabricksTestEnvironment>
    {
        public ServerSidePropertyE2ETest(ITestOutputHelper? outputHelper)
            : base(outputHelper, new DatabricksTestEnvironment.Factory())
        {
            // Skip the test if the DATABRICKS_TEST_CONFIG_FILE environment variable is not set
            Skip.IfNot(Utils.CanExecuteTestConfig(TestConfigVariable));
        }

        /// <summary>
        /// Tests setting server-side properties.
        /// </summary>
        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public async Task TestServerSideProperty(bool applyWithQueries)
        {
            var additionalConnectionParams = new Dictionary<string, string>()
            {
                [DatabricksParameters.ServerSidePropertyPrefix + "use_cached_result"] = "false",
                [DatabricksParameters.ServerSidePropertyPrefix + "statement_timeout"] = "12345",
                [DatabricksParameters.ApplySSPWithQueries] = applyWithQueries.ToString().ToLower()
            };
            using var connection = NewConnection(TestConfiguration, additionalConnectionParams);

            // Verify the server-side property was set by querying it
            using var statement = connection.CreateStatement();
            statement.SqlQuery = "SET";

            var result = await statement.ExecuteQueryAsync();
            Assert.NotNull(result.Stream);

            var batch = await result.Stream.ReadNextRecordBatchAsync();
            Assert.NotNull(batch);
            Assert.True(batch.Length > 0);
            Assert.Equal(2, batch.ColumnCount);

            var returnedProperties = new Dictionary<string, string>();
            var keys = (StringArray)batch.Column(0);
            var values = (StringArray)batch.Column(1);
            for (int i = 0; i < batch.Length; i++)
            {
                string key = keys.GetString(i);
                string value = values.GetString(i);
                returnedProperties[key] = value;
                Console.WriteLine($"Property: {key} = {value}");
            }

            Assert.True(returnedProperties.ContainsKey("use_cached_result"));
            Assert.Equal("false", returnedProperties["use_cached_result"]);

            Assert.True(returnedProperties.ContainsKey("statement_timeout"));
            Assert.Equal("12345", returnedProperties["statement_timeout"]);
        }
    }
}
