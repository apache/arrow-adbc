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
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using Apache.Arrow.Adbc.Drivers.BigQuery;
using Apache.Arrow.Adbc.Tests.Drivers.BigQuery.Mocks;
using Apache.Arrow.Adbc.Tests.Xunit;
using Azure.Core;
using Azure.Identity;
using Google.Apis.Auth.OAuth2;
using Moq;
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
        readonly Dictionary<string, AdbcConnection> _configuredConnections = new Dictionary<string, AdbcConnection>();

        public AuthenticationTests(ITestOutputHelper outputHelper)
        {
            Skip.IfNot(Utils.CanExecuteTestConfig(BigQueryTestingUtils.BIGQUERY_TEST_CONFIG_VARIABLE));

            _testConfiguration = MultiEnvironmentTestUtils.LoadMultiEnvironmentTestConfiguration<BigQueryTestConfiguration>(BigQueryTestingUtils.BIGQUERY_TEST_CONFIG_VARIABLE);
            _environments = MultiEnvironmentTestUtils.GetTestEnvironments<BigQueryTestEnvironment>(_testConfiguration);
            _outputHelper = outputHelper;

            //foreach (BigQueryTestEnvironment environment in _environments)
            //{
            //    AdbcConnection connection = BigQueryTestingUtils.GetRetryableBigQueryAdbcConnection(environment);
            //    _configuredConnections.Add(environment.Name!, connection);
            //}
        }

        /// <summary>
        /// Validates if the Entra token can sign in and refresh.
        /// </summary>
        [SkippableFact, Order(1)]
        public void CanSignInWithEntraToken()
        {
            BigQueryTestEnvironment? environment = _environments.Where(x => x.AuthenticationType == BigQueryConstants.EntraIdAuthenticationType).FirstOrDefault();
            Assert.NotNull(environment);

            BigQueryConnection? connection = BigQueryTestingUtils.GetRetryableEntraBigQueryAdbcConnection(environment, GetAccessToken(environment)) as BigQueryConnection;
            Assert.NotNull(connection);

            connection.UpdateToken = () => Task.Run(() =>
            {
                connection.SetOption(BigQueryParameters.AccessToken, GetAccessToken(environment));
            });

            AdbcStatement statement = connection.CreateStatement();
            statement.SqlQuery = environment.Query;

            QueryResult queryResult = statement.ExecuteQuery();

            Tests.DriverTests.CanExecuteQuery(queryResult, environment.ExpectedResultsCount, environment.Name);
        }

        [SkippableFact, Order(1)]
        public void CanRefreshTokenWithMockedClient()
        {
            MockedBigQueryClient mockedBigQueryClient = new MockedBigQueryClient();
            BigQueryTestEnvironment? environment = _environments.Where(x => x.AuthenticationType == BigQueryConstants.EntraIdAuthenticationType).FirstOrDefault();
            Assert.NotNull(environment);

            BigQueryConnection connection = (BigQueryConnection) BigQueryTestingUtils.GetRetryableEntraBigQueryAdbcConnection(mockedBigQueryClient.Client, environment, GetAccessToken(environment));
            Assert.NotNull(connection);

            connection.UpdateToken = () => Task.Run(() =>
            {
                connection.SetOption(BigQueryParameters.AccessToken, Guid.NewGuid().ToString());
            });

            AdbcStatement statement = connection.CreateStatement();

            QueryResult queryResult = statement.ExecuteQuery();
        }

        private string GetAccessToken(BigQueryTestEnvironment environment)
        {
            if (environment?.EntraConfiguration?.Scopes == null || environment?.EntraConfiguration?.Claims == null)
            {
                throw new InvalidOperationException("The test environment is not configured correctly");
            }

            // the easiest way is to log in to Visual Studio using Tools > Options > Azure Service Authentication
            DefaultAzureCredential credential = new DefaultAzureCredential();

            // Request the token
            string claimJson = JsonSerializer.Serialize(environment.EntraConfiguration.Claims);
            TokenRequestContext requestContext = new TokenRequestContext(environment.EntraConfiguration.Scopes, claims: claimJson);
            AccessToken accessToken = credential.GetToken(requestContext);

            return accessToken.Token;
        }
    }
}
