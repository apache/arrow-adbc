using System;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow.Adbc.Drivers.Databricks.Auth;
using Apache.Arrow.Adbc.Tests.Drivers.Databricks;
using Xunit;
using Xunit.Abstractions;

namespace Apache.Arrow.Adbc.Tests.Drivers.Databricks.Auth
{
    public class OAuthClientCredentialsServiceTests : TestBase<DatabricksTestConfiguration, DatabricksTestEnvironment>, IDisposable
    {
        private readonly OAuthClientCredentialsService _service;

        public OAuthClientCredentialsServiceTests(ITestOutputHelper? outputHelper)
            : base(outputHelper, new DatabricksTestEnvironment.Factory())
        {
            _service = new OAuthClientCredentialsService(
                TestConfiguration.OAuthClientId,
                TestConfiguration.OAuthClientSecret,
                new Uri(TestConfiguration.Uri),
                timeoutMinutes: 1);
        }

        [Fact]
        public async Task GetAccessToken_WithValidCredentials_ReturnsToken()
        {
            Skip.IfNot(!string.IsNullOrEmpty(TestConfiguration.OAuthClientId), "OAuth credentials not configured");

            var token = await _service.GetAccessTokenAsync(CancellationToken.None);

            Assert.NotNull(token);
            Assert.NotEmpty(token);
        }

        [Fact]
        public async Task GetAccessToken_WithCancellation_ThrowsOperationCanceledException()
        {
            Skip.IfNot(!string.IsNullOrEmpty(TestConfiguration.OAuthClientId), "OAuth credentials not configured");

            using var cts = new CancellationTokenSource();
            cts.Cancel();

            await Assert.ThrowsAsync<TaskCanceledException>(() =>
                _service.GetAccessTokenAsync(cts.Token));
        }

        [Fact]
        public async Task GetAccessToken_MultipleCalls_ReusesCachedToken()
        {
            Skip.IfNot(!string.IsNullOrEmpty(TestConfiguration.OAuthClientId), "OAuth credentials not configured");

            var token1 = await _service.GetAccessTokenAsync(CancellationToken.None);
            var token2 = await _service.GetAccessTokenAsync(CancellationToken.None);

            Assert.Equal(token1, token2);
        }

        void IDisposable.Dispose()
        {
            _service.Dispose();
            base.Dispose();
        }
    }
} 