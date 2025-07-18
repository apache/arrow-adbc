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
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow.Adbc.Drivers.Databricks.Auth;
using Xunit;
using Xunit.Abstractions;

namespace Apache.Arrow.Adbc.Tests.Drivers.Databricks.Auth
{
    public class TokenExchangeTests : TestBase<DatabricksTestConfiguration, DatabricksTestEnvironment>, IDisposable
    {
        private readonly HttpClient _httpClient;

        public TokenExchangeTests(ITestOutputHelper? outputHelper)
            : base(outputHelper, new DatabricksTestEnvironment.Factory())
        {
            _httpClient = new HttpClient();
        }

        private string GetHost()
        {
            string host;
            if (!string.IsNullOrEmpty(TestConfiguration.HostName))
            {
                host = TestConfiguration.HostName;
            }
            else if (!string.IsNullOrEmpty(TestConfiguration.Uri))
            {
                if (Uri.TryCreate(TestConfiguration.Uri, UriKind.Absolute, out Uri? parsedUri))
                {
                    host = parsedUri.Host;
                }
                else
                {
                    throw new ArgumentException($"Invalid URI format: {TestConfiguration.Uri}");
                }
            }
            else
            {
                throw new ArgumentException("Either HostName or Uri must be provided in the test configuration");
            }

            return host;
        }

        [SkippableFact]
        public async Task ExchangeToken_WithValidToken_ReturnsNewToken()
        {
            Skip.IfNot(!string.IsNullOrEmpty(TestConfiguration.AccessToken), "OAuth access token not configured");

            string host = GetHost();
            var tokenExchangeClient = new TokenExchangeClient(_httpClient, host);

            var response = await tokenExchangeClient.ExchangeTokenAsync(TestConfiguration.AccessToken, CancellationToken.None);

            Assert.NotNull(response);
            Assert.NotEmpty(response.AccessToken);
            Assert.NotEqual(TestConfiguration.AccessToken, response.AccessToken);
            Assert.Equal("Bearer", response.TokenType);
            Assert.True(response.ExpiresIn > 0);
            Assert.True(response.ExpiryTime > DateTime.UtcNow);
        }

        [SkippableFact]
        public async Task TokenExchangeHandler_WithValidToken_RefreshesToken()
        {
            Skip.IfNot(!string.IsNullOrEmpty(TestConfiguration.AccessToken), "OAuth access token not configured");

            bool isValidJwt = JwtTokenDecoder.TryGetExpirationTime(TestConfiguration.AccessToken, out DateTime expiryTime);
            Skip.IfNot(isValidJwt, "Access token is not a valid JWT token with expiration claim");

            // Create a token that's about to expire (by setting expiry time to near future)
            DateTime nearFutureExpiry = DateTime.UtcNow.AddMinutes(5);

            string host = GetHost();
            var tokenExchangeClient = new TokenExchangeClient(_httpClient, host);

            var handler = new TokenExchangeDelegatingHandler(
                new HttpClientHandler(),
                tokenExchangeClient,
                TestConfiguration.AccessToken,
                nearFutureExpiry,
                10);

            var httpClient = new HttpClient(handler);

            var request = new HttpRequestMessage(HttpMethod.Get, $"https://{host}/api/2.0/sql/config/warehouses");
            var response = await httpClient.SendAsync(request, CancellationToken.None);

            // The request should succeed with the refreshed token
            response.EnsureSuccessStatusCode();

            string content = await response.Content.ReadAsStringAsync();
            Assert.Contains("sql_configuration_parameters", content);
        }

        [SkippableFact]
        public async Task TokenExchangeHandler_WithValidTokenNotNearExpiry_UsesOriginalToken()
        {
            Skip.IfNot(!string.IsNullOrEmpty(TestConfiguration.AccessToken), "OAuth access token not configured");

            bool isValidJwt = JwtTokenDecoder.TryGetExpirationTime(TestConfiguration.AccessToken, out DateTime expiryTime);
            Skip.IfNot(isValidJwt, "Access token is not a valid JWT token with expiration claim");
            Skip.If(DateTime.UtcNow.AddMinutes(20) >= expiryTime, "Access token is too close to expiration for this test");

            string host = GetHost();
            var tokenExchangeClient = new TokenExchangeClient(_httpClient, host);

            // Create a handler that should not refresh the token (token not near expiry)
            var handler = new TokenExchangeDelegatingHandler(
                new HttpClientHandler(),
                tokenExchangeClient,
                TestConfiguration.AccessToken,
                expiryTime,
                10);

            var httpClient = new HttpClient(handler);

            var request = new HttpRequestMessage(HttpMethod.Get, $"https://{host}/api/2.0/sql/config/warehouses");
            var response = await httpClient.SendAsync(request, CancellationToken.None);

            // The request should succeed with the original token
            response.EnsureSuccessStatusCode();

            string content = await response.Content.ReadAsStringAsync();
            Assert.Contains("sql_configuration_parameters", content);
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                _httpClient?.Dispose();
            }

            base.Dispose(disposing);
        }
    }
}
