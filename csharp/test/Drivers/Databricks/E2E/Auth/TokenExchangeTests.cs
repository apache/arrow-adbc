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
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow.Adbc.Drivers.Databricks.Auth;
using Xunit;
using Xunit.Abstractions;

namespace Apache.Arrow.Adbc.Tests.Drivers.Databricks.Auth
{
    public class TokenCapturingHandler : DelegatingHandler
    {
        public List<string> CapturedTokens { get; } = new List<string>();
        public List<DateTime> RequestTimes { get; } = new List<DateTime>();

        public TokenCapturingHandler(HttpMessageHandler innerHandler) : base(innerHandler)
        {
        }

        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            // Capture the authorization token
            if (request.Headers.Authorization != null)
            {
                CapturedTokens.Add(request.Headers.Authorization.Parameter ?? string.Empty);
                RequestTimes.Add(DateTime.UtcNow);
            }

            return await base.SendAsync(request, cancellationToken);
        }
    }

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
        public async Task TokenExchangeHandler_WithValidToken_RefreshesTokenInBackgroundAcrossRequests()
        {
            Skip.IfNot(!string.IsNullOrEmpty(TestConfiguration.AccessToken), "OAuth access token not configured");

            bool isValidJwt = JwtTokenDecoder.TryGetExpirationTime(TestConfiguration.AccessToken, out DateTime expiryTime);
            Skip.IfNot(isValidJwt, "Access token is not a valid JWT token with expiration claim");

            // Create a token that's about to expire (by setting expiry time to near future)
            DateTime nearFutureExpiry = DateTime.UtcNow.AddMinutes(5);

            string host = GetHost();
            var tokenExchangeClient = new TokenExchangeClient(_httpClient, host);

            // Create a token capturing handler to intercept the actual tokens being sent
            var tokenCapturingHandler = new TokenCapturingHandler(new HttpClientHandler());

            var handler = new TokenExchangeDelegatingHandler(
                tokenCapturingHandler,
                tokenExchangeClient,
                TestConfiguration.AccessToken,
                nearFutureExpiry,
                10);

            var httpClient = new HttpClient(handler);

            // First request - should trigger background token refresh but use original token
            var firstRequest = new HttpRequestMessage(HttpMethod.Get, $"https://{host}/api/2.0/sql/config/warehouses");
            var startTime = DateTime.UtcNow;
            var firstResponse = await httpClient.SendAsync(firstRequest, CancellationToken.None);
            var firstRequestDuration = DateTime.UtcNow - startTime;

            // The first request should succeed quickly (not waiting for token refresh)
            firstResponse.EnsureSuccessStatusCode();
            string firstContent = await firstResponse.Content.ReadAsStringAsync();
            Assert.Contains("sql_configuration_parameters", firstContent);

            // Verify the request completed quickly (token refresh happens in background)
            Assert.True(firstRequestDuration < TimeSpan.FromSeconds(5),
                $"First request took {firstRequestDuration.TotalMilliseconds}ms, which may indicate it waited for token refresh");

            // Verify the first request used the original token
            Assert.Single(tokenCapturingHandler.CapturedTokens);

            // Wait for background token refresh to complete
            await Task.Delay(TimeSpan.FromSeconds(10));

            // Second request - should use the refreshed token
            var secondRequest = new HttpRequestMessage(HttpMethod.Get, $"https://{host}/api/2.0/sql/config/warehouses");
            var secondResponse = await httpClient.SendAsync(secondRequest, CancellationToken.None);

            // The second request should also succeed (now with refreshed token)
            secondResponse.EnsureSuccessStatusCode();
            string secondContent = await secondResponse.Content.ReadAsStringAsync();
            Assert.Contains("sql_configuration_parameters", secondContent);

            // Verify we now have two different tokens
            Assert.Equal(2, tokenCapturingHandler.CapturedTokens.Count);
            Assert.Equal(TestConfiguration.AccessToken, tokenCapturingHandler.CapturedTokens[0]);
            Assert.NotEqual(TestConfiguration.AccessToken, tokenCapturingHandler.CapturedTokens[1]);
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

            // Create a token capturing handler to verify no token refresh occurs
            var tokenCapturingHandler = new TokenCapturingHandler(new HttpClientHandler());

            // Create a handler that should not refresh the token (token not near expiry)
            var handler = new TokenExchangeDelegatingHandler(
                tokenCapturingHandler,
                tokenExchangeClient,
                TestConfiguration.AccessToken,
                expiryTime,
                10);

            var httpClient = new HttpClient(handler);

            // Make multiple requests to ensure no token refresh is triggered
            var firstRequest = new HttpRequestMessage(HttpMethod.Get, $"https://{host}/api/2.0/sql/config/warehouses");
            var firstResponse = await httpClient.SendAsync(firstRequest, CancellationToken.None);

            // The first request should succeed with the original token
            firstResponse.EnsureSuccessStatusCode();
            string firstContent = await firstResponse.Content.ReadAsStringAsync();
            Assert.Contains("sql_configuration_parameters", firstContent);

            // Similar wait as the token refresh case
            await Task.Delay(TimeSpan.FromSeconds(10));

            // Second request should also use original token (no refresh needed)
            var secondRequest = new HttpRequestMessage(HttpMethod.Get, $"https://{host}/api/2.0/sql/config/warehouses");
            var secondResponse = await httpClient.SendAsync(secondRequest, CancellationToken.None);

            secondResponse.EnsureSuccessStatusCode();
            string secondContent = await secondResponse.Content.ReadAsStringAsync();
            Assert.Contains("sql_configuration_parameters", secondContent);

            // Verify both requests used the same original token (no refresh occurred)
            Assert.Equal(2, tokenCapturingHandler.CapturedTokens.Count);
            Assert.Equal(TestConfiguration.AccessToken, tokenCapturingHandler.CapturedTokens[0]);
            Assert.Equal(TestConfiguration.AccessToken, tokenCapturingHandler.CapturedTokens[1]);
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
