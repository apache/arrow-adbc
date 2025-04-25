using System;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow.Adbc.Drivers.Databricks.Auth;
using Apache.Arrow.Adbc.Tests;
using Apache.Arrow.Adbc.Tests.Drivers.Apache.Spark;
using Xunit;
using Xunit.Abstractions;

namespace Apache.Arrow.Adbc.Tests.Drivers.Databricks.Auth
{
    public class OAuthClientCredentialsServiceTests : TestBase<SparkTestConfiguration, SparkTestEnvironment>, IDisposable
    {
        private readonly OAuthClientCredentialsService _service;

        public OAuthClientCredentialsServiceTests(ITestOutputHelper? outputHelper)
            : base(outputHelper, new SparkTestEnvironment.Factory())
        {
            _service = new OAuthClientCredentialsService(
                TestConfiguration.Username,
                TestConfiguration.Password,
                new Uri(TestConfiguration.Uri),
                timeoutMinutes: 1);
        }

        [Fact]
        public async Task GetAccessToken_WithValidCredentials_ReturnsToken()
        {
            var token = await _service.GetAccessTokenAsync(CancellationToken.None);

            Assert.NotNull(token);
            Assert.NotEmpty(token);
        }

        [Fact]
        public async Task GetAccessToken_WithCancellation_ThrowsOperationCanceledException()
        {
            using var cts = new CancellationTokenSource();
            cts.Cancel();

            await Assert.ThrowsAsync<OperationCanceledException>(() =>
                _service.GetAccessTokenAsync(cts.Token));
        }

        [Fact]
        public async Task GetAccessToken_MultipleCalls_ReusesCachedToken()
        {
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