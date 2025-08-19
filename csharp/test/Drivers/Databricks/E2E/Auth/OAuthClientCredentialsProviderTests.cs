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
using Apache.Arrow.Adbc.Drivers.Databricks.Auth;
using System;
using System.Threading;
using System.Threading.Tasks;
using System.Net.Http;
using Xunit;
using Xunit.Abstractions;

namespace Apache.Arrow.Adbc.Tests.Drivers.Databricks.Auth
{
    public class OAuthClientCredentialsProviderTests : TestBase<DatabricksTestConfiguration, DatabricksTestEnvironment>, IDisposable
    {
        public OAuthClientCredentialsProviderTests(ITestOutputHelper? outputHelper)
            : base(outputHelper, new DatabricksTestEnvironment.Factory())
        {
        }

        private OAuthClientCredentialsProvider CreateService(int refreshBufferMinutes = 5, string scope = "sql")
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

            return new OAuthClientCredentialsProvider(
                new HttpClient(),
                TestConfiguration.OAuthClientId,
                TestConfiguration.OAuthClientSecret,
                host,
                scope,
                timeoutMinutes: 1,
                refreshBufferMinutes: refreshBufferMinutes);
        }

        [SkippableFact]
        public async Task GetAccessToken_WithValidCredentials_ReturnsToken()
        {
            Skip.IfNot(!string.IsNullOrEmpty(TestConfiguration.OAuthClientId), "OAuth credentials not configured");

            var service = CreateService();
            var token = await service.GetAccessTokenAsync();

            Assert.NotNull(token);
            Assert.NotEmpty(token);
        }

        [SkippableFact]
        public async Task GetAccessToken_WithCancellation_ThrowsOperationCanceledException()
        {
            Skip.IfNot(!string.IsNullOrEmpty(TestConfiguration.OAuthClientId), "OAuth credentials not configured");

            var service = CreateService();
            using var cts = new CancellationTokenSource();
            cts.Cancel();

            await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
                await service.GetAccessTokenAsync(cts.Token));
        }

        [SkippableFact]
        public async Task GetAccessToken_MultipleCalls_ReusesCachedToken()
        {
            Skip.IfNot(!string.IsNullOrEmpty(TestConfiguration.OAuthClientId), "OAuth credentials not configured");

            var service = CreateService();
            var token1 = await service.GetAccessTokenAsync();
            var token2 = await service.GetAccessTokenAsync();

            Assert.Equal(token1, token2);
        }

        [SkippableFact]
        public async Task GetAccessToken_WithLargeRefreshBuffer_RefetchesToken()
        {
            Skip.IfNot(!string.IsNullOrEmpty(TestConfiguration.OAuthClientId), "OAuth credentials not configured");

            // Create service with a refresh buffer of 60 minutes (typical token lifetime)
            var service = CreateService(refreshBufferMinutes: 60);

            // Get initial token
            var token1 = await service.GetAccessTokenAsync();

            // Wait a short time to ensure we're not hitting any caching
            await Task.Delay(100);

            // Get second token - should be different since the refresh buffer is larger than token lifetime
            var token2 = await service.GetAccessTokenAsync();

            // Tokens should be different since we're forcing a refresh
            Assert.NotEqual(token1, token2);
        }

        [SkippableFact]
        public async Task GetAccessToken_WithCustomScope_ReturnsToken()
        {
            Skip.IfNot(!string.IsNullOrEmpty(TestConfiguration.OAuthClientId), "OAuth credentials not configured");
            String scope = "all-apis";
            var service = CreateService(scope: scope);
            var token = await service.GetAccessTokenAsync();

            Assert.NotNull(token);
            Assert.NotEmpty(token);
            Assert.Equal(scope, service.GetCachedTokenScope());
        }
    }
}
