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
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow.Adbc.Drivers.Databricks.Auth;
using Xunit;
using Xunit.Abstractions;
using Apache.Arrow.Adbc.Tests.Drivers.Databricks;

namespace Apache.Arrow.Adbc.Tests.Drivers.Databricks.Auth
{
    public class OAuthClientCredentialsProviderTests : TestBase<DatabricksTestConfiguration, DatabricksTestEnvironment>, IDisposable
    {
        private readonly OAuthClientCredentialsProvider _service;

        public OAuthClientCredentialsProviderTests(ITestOutputHelper? outputHelper)
            : base(outputHelper, new DatabricksTestEnvironment.Factory())
        {
            _service = new OAuthClientCredentialsProvider(
                TestConfiguration.OAuthClientId,
                TestConfiguration.OAuthClientSecret,
                TestConfiguration.HostName,
                timeoutMinutes: 1);
        }

        [Fact]
        public void GetAccessToken_WithValidCredentials_ReturnsToken()
        {
            Skip.IfNot(!string.IsNullOrEmpty(TestConfiguration.OAuthClientId), "OAuth credentials not configured");

            var token = _service.GetAccessToken();

            Assert.NotNull(token);
            Assert.NotEmpty(token);
        }

        [Fact]
        public void GetAccessToken_WithCancellation_ThrowsOperationCanceledException()
        {
            Skip.IfNot(!string.IsNullOrEmpty(TestConfiguration.OAuthClientId), "OAuth credentials not configured");

            using var cts = new CancellationTokenSource();
            cts.Cancel();

            Assert.Throws<OperationCanceledException>(() =>
                _service.GetAccessToken(cts.Token));
        }

        [Fact]
        public void GetAccessToken_MultipleCalls_ReusesCachedToken()
        {
            Skip.IfNot(!string.IsNullOrEmpty(TestConfiguration.OAuthClientId), "OAuth credentials not configured");

            var token1 = _service.GetAccessToken();
            var token2 = _service.GetAccessToken();

            Assert.Equal(token1, token2);
        }

        void IDisposable.Dispose()
        {
            _service.Dispose();
            base.Dispose();
        }
    }
}
