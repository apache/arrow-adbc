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

using System.Collections.Generic;
using System.Linq;
using Apache.Arrow.Adbc.Tests.Xunit;
using Xunit;
using Xunit.Abstractions;

namespace Apache.Arrow.Adbc.Tests.Drivers.BigQuery
{
    [TestCaseOrderer("Apache.Arrow.Adbc.Tests.Xunit.TestOrderer", "Apache.Arrow.Adbc.Tests")]
    public class ProxyTests
    {
        private BigQueryTestConfiguration _testConfiguration;
        readonly List<BigQueryTestEnvironment> _environments;
        readonly ITestOutputHelper _outputHelper;

        public ProxyTests(ITestOutputHelper outputHelper)
        {
            Skip.IfNot(Utils.CanExecuteTestConfig(BigQueryTestingUtils.BIGQUERY_TEST_CONFIG_VARIABLE));

            _testConfiguration = MultiEnvironmentTestUtils.LoadMultiEnvironmentTestConfiguration<BigQueryTestConfiguration>(BigQueryTestingUtils.BIGQUERY_TEST_CONFIG_VARIABLE);
            _environments = MultiEnvironmentTestUtils.GetTestEnvironments<BigQueryTestEnvironment>(_testConfiguration);
            _outputHelper = outputHelper;
        }

        /// <summary>
        /// Validates if can connect using a proxy.
        /// </summary>
        [SkippableFact, Order(1)]
        public void CanExecuteQueryViaProxy()
        {
            List<BigQueryTestEnvironment> environments = _environments.Where(x => x.HttpProxy != null).ToList();
            Assert.True(environments.Count > 0);

            foreach (BigQueryTestEnvironment environment in environments)
            {
                AdbcConnection connection = BigQueryTestingUtils.GetBigQueryAdbcConnection(environment);
                Assert.NotNull(connection);

                AdbcStatement statement = connection.CreateStatement();
                statement.SqlQuery = environment.Query;

                QueryResult queryResult = statement.ExecuteQuery();

                Tests.DriverTests.CanExecuteQuery(queryResult, environment.ExpectedResultsCount, environment.Name);
            }
        }
    }
}
