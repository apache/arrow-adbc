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
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Apache.Arrow.Adbc.Drivers.BigQuery;
using Apache.Arrow.Adbc.Tests.Xunit;
using Xunit;
using Xunit.Abstractions;

namespace Apache.Arrow.Adbc.Tests.Drivers.BigQuery
{
    [TestCaseOrderer("Apache.Arrow.Adbc.Tests.Xunit.TestOrderer", "Apache.Arrow.Adbc.Tests")]
    public class AuthenticationTests
    {
        private BigQueryTestConfiguration _testConfiguration;
        readonly List<BigQueryTestEnvironment> _environments;
        readonly ITestOutputHelper _outputHelper;

        public AuthenticationTests(ITestOutputHelper outputHelper)
        {
            Skip.IfNot(Utils.CanExecuteTestConfig(BigQueryTestingUtils.BIGQUERY_TEST_CONFIG_VARIABLE));

            _testConfiguration = MultiEnvironmentTestUtils.LoadMultiEnvironmentTestConfiguration<BigQueryTestConfiguration>(BigQueryTestingUtils.BIGQUERY_TEST_CONFIG_VARIABLE);
            _environments = MultiEnvironmentTestUtils.GetTestEnvironments<BigQueryTestEnvironment>(_testConfiguration);
            _outputHelper = outputHelper;
        }

        /// <summary>
        /// Validates if the Entra token can sign in.
        /// </summary>
        [SkippableTheory, Order(1)]
        [InlineData(false)]
        [InlineData(true)]
        public void CanSignInWithEntraToken(bool withRefresh)
        {
            BigQueryTestEnvironment? environment = _environments.Where(x => x.AuthenticationType == BigQueryConstants.EntraIdAuthenticationType).FirstOrDefault();
            Assert.NotNull(environment);

            BigQueryConnection? connection = BigQueryTestingUtils.GetEntraProtectedBigQueryAdbcConnection(environment, BigQueryTestingUtils.GetAccessToken(environment)) as BigQueryConnection;
            Assert.NotNull(connection);

            if (withRefresh)
            {
                connection.UpdateToken = () => Task.Run(() =>
                {
                    connection.SetOption(BigQueryParameters.AccessToken, BigQueryTestingUtils.GetAccessToken(environment));
                });
            }

            Stopwatch stopwatch = Stopwatch.StartNew();

            AdbcStatement statement = connection.CreateStatement();
            statement.SqlQuery = environment.Query;

            QueryResult queryResult = statement.ExecuteQuery();

            Tests.DriverTests.CanExecuteQuery(queryResult, environment.ExpectedResultsCount, environment.Name);

            stopwatch.Stop();

            if (withRefresh)
            {
                _outputHelper.WriteLine($"With refresh (async) took {stopwatch.Elapsed.TotalSeconds} seconds");
            }
            else
            {
                _outputHelper.WriteLine($"Without refresh took {stopwatch.Elapsed.TotalSeconds} seconds");
            }
        }

        /// <summary>
        /// Validates if the Entra token can sign in and refresh.
        /// </summary>
        [SkippableFact, Order(2)]
        public void LongRunningQuerySucceedsByRefreshingEntraToken()
        {
            BigQueryTestEnvironment? environment = _environments.Where(x => x.AuthenticationType == BigQueryConstants.EntraIdAuthenticationType).FirstOrDefault();
            Assert.NotNull(environment);

            BigQueryConnection connection = (BigQueryConnection)BigQueryTestingUtils.GetEntraProtectedBigQueryAdbcConnection(environment, BigQueryTestingUtils.GetAccessToken(environment));
            Assert.NotNull(connection);

            connection.UpdateToken = () => Task.Run(() =>
            {
                connection.SetOption(BigQueryParameters.AccessToken, BigQueryTestingUtils.GetAccessToken(environment));

                _outputHelper.WriteLine("Successfully set a new token");
            });

            // create a query that takes 75 minutes because Entra tokens typically expire in 60 minutes
            AdbcStatement statement = connection.CreateStatement();
            statement.SqlQuery = @"
                DECLARE end_time TIMESTAMP;
                SET end_time = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 75 MINUTE);

                WHILE CURRENT_TIMESTAMP() < end_time DO
                END WHILE;

                SELECT 'Query completed after 75 minutes' AS result;";

            QueryResult queryResult = statement.ExecuteQuery();

            _outputHelper.WriteLine($"Retrieve query result with {queryResult.RowCount} rows");
        }

        /// <summary>
        /// Validates the retry logic works but still fails when a long running query runs
        /// and the token refresh handler isn't defined for the BigQueryConnection.
        /// </summary>
        [SkippableFact, Order(1)]
        public void LongRunningQueryFailsByNotRefreshingEntraToken()
        {
            BigQueryTestEnvironment? environment = _environments.Where(x => x.AuthenticationType == BigQueryConstants.EntraIdAuthenticationType).FirstOrDefault();
            Assert.NotNull(environment);

            BigQueryConnection connection = (BigQueryConnection)BigQueryTestingUtils.GetEntraProtectedBigQueryAdbcConnection(environment, BigQueryTestingUtils.GetAccessToken(environment), 2);
            Assert.NotNull(connection);

            // create a query that takes 75 minutes because Entra tokens typically expire in 60 minutes
            AdbcStatement statement = connection.CreateStatement();
            statement.SqlQuery = @"
                DECLARE end_time TIMESTAMP;
                SET end_time = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 75 MINUTE);

                WHILE CURRENT_TIMESTAMP() < end_time DO
                END WHILE;

                SELECT 'Query completed after 75 minutes' AS result;";

            Assert.ThrowsAny<AdbcException>(() => statement.ExecuteQuery());
        }
    }
}
