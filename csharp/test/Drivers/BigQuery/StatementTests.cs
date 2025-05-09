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
using Apache.Arrow.Adbc.Drivers.BigQuery;
using Xunit;
using Xunit.Abstractions;

namespace Apache.Arrow.Adbc.Tests.Drivers.BigQuery
{
    [TestCaseOrderer("Apache.Arrow.Adbc.Tests.Xunit.TestOrderer", "Apache.Arrow.Adbc.Tests")]
    public class StatementTests
    {
        BigQueryTestConfiguration _testConfiguration;
        readonly List<BigQueryTestEnvironment> _environments;
        readonly Dictionary<string, AdbcConnection> _configuredConnections = new Dictionary<string, AdbcConnection>();
        readonly ITestOutputHelper? _outputHelper;

        public StatementTests(ITestOutputHelper? outputHelper)
        {
            Skip.IfNot(Utils.CanExecuteTestConfig(BigQueryTestingUtils.BIGQUERY_TEST_CONFIG_VARIABLE));

            _testConfiguration = MultiEnvironmentTestUtils.LoadMultiEnvironmentTestConfiguration<BigQueryTestConfiguration>(BigQueryTestingUtils.BIGQUERY_TEST_CONFIG_VARIABLE);
            _environments = MultiEnvironmentTestUtils.GetTestEnvironments<BigQueryTestEnvironment>(_testConfiguration);
            _outputHelper = outputHelper;
        }

        [Fact]
        public void CanSetStatementOption()
        {
            foreach (BigQueryTestEnvironment environment in _environments)
            {
                // ensure the value isn't already set
                Assert.False(environment.AllowLargeResults);

                AdbcConnection adbcConnection = BigQueryTestingUtils.GetBigQueryAdbcConnection(environment);
                AdbcStatement statement = adbcConnection.CreateStatement();
                statement.SetOption(BigQueryParameters.AllowLargeResults, "true");

                // BigQuery is currently on ADBC 1.0, so it doesn't have the GetOption interface. Therefore, use reflection to validate the value is set correctly.
                IReadOnlyDictionary<string, string>? options = statement.GetType().GetProperty("Options")!.GetValue(statement) as IReadOnlyDictionary<string, string>;
                Assert.True(options != null);
                Assert.True(options[BigQueryParameters.AllowLargeResults] == "true");
            }
        }
    }
}
