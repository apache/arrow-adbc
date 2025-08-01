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
using System.Net;
using System.Net.Http;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow.Adbc.Drivers.Databricks.Auth;
using Moq;
using Moq.Protected;
using Xunit;

namespace Apache.Arrow.Adbc.Tests.Drivers.Databricks.Unit.Auth
{
    public class MandatoryTokenExchangeDelegatingHandlerTests : IDisposable
    {
        private readonly Mock<HttpMessageHandler> _mockInnerHandler;
        private readonly Mock<ITokenExchangeClient> _mockTokenExchangeClient;
        private readonly string _identityFederationClientId = "test-client-id";

        // Real JWT tokens for testing (these are valid JWT structure but not real credentials)
        private readonly string _databricksToken;
        private readonly string _externalToken;
        private readonly string _exchangedToken = "exchanged-databricks-token";

        public MandatoryTokenExchangeDelegatingHandlerTests()
        {
            _mockInnerHandler = new Mock<HttpMessageHandler>();
            _mockTokenExchangeClient = new Mock<ITokenExchangeClient>();

            // Setup token exchange endpoint for host comparison
            _mockTokenExchangeClient.Setup(x => x.TokenExchangeEndpoint)
                .Returns("https://databricks-workspace.cloud.databricks.com/v1/token");

            // Create real JWT tokens with proper issuers
            _databricksToken = CreateJwtToken("https://databricks-workspace.cloud.databricks.com", DateTime.UtcNow.AddHours(1));
            _externalToken = CreateJwtToken("https://external-provider.com", DateTime.UtcNow.AddHours(1));
        }

        [Fact]
        public void Constructor_WithValidParameters_InitializesCorrectly()
        {
            var handler = new MandatoryTokenExchangeDelegatingHandler(
                _mockInnerHandler.Object,
                _mockTokenExchangeClient.Object,
                _identityFederationClientId);

            Assert.NotNull(handler);
        }

        [Fact]
        public void Constructor_WithNullTokenExchangeClient_ThrowsArgumentNullException()
        {
            Assert.Throws<ArgumentNullException>(() => new MandatoryTokenExchangeDelegatingHandler(
                _mockInnerHandler.Object,
                null!,
                _identityFederationClientId));
        }

        [Fact]
        public void Constructor_WithoutIdentityFederationClientId_InitializesCorrectly()
        {
            var handler = new MandatoryTokenExchangeDelegatingHandler(
                _mockInnerHandler.Object,
                _mockTokenExchangeClient.Object);

            Assert.NotNull(handler);
        }

        [Fact]
        public async Task SendAsync_WithDatabricksToken_UsesTokenDirectlyWithoutExchange()
        {
            var handler = new MandatoryTokenExchangeDelegatingHandler(
                _mockInnerHandler.Object,
                _mockTokenExchangeClient.Object,
                _identityFederationClientId);

            var request = new HttpRequestMessage(HttpMethod.Get, "https://example.com");
            request.Headers.Authorization = new System.Net.Http.Headers.AuthenticationHeaderValue("Bearer", _databricksToken);
            var expectedResponse = new HttpResponseMessage(HttpStatusCode.OK);

            HttpRequestMessage? capturedRequest = null;

            _mockInnerHandler.Protected()
                .Setup<Task<HttpResponseMessage>>(
                    "SendAsync",
                    ItExpr.IsAny<HttpRequestMessage>(),
                    ItExpr.IsAny<CancellationToken>())
                .Callback<HttpRequestMessage, CancellationToken>((req, ct) => capturedRequest = req)
                .ReturnsAsync(expectedResponse);

            var httpClient = new HttpClient(handler);

            var response = await httpClient.SendAsync(request);

            Assert.Equal(expectedResponse, response);
            Assert.NotNull(capturedRequest);
            Assert.Equal("Bearer", capturedRequest.Headers.Authorization?.Scheme);
            Assert.Equal(_databricksToken, capturedRequest.Headers.Authorization?.Parameter);

            // Wait for any background tasks
            await Task.Delay(1000);

            // Verify no token exchange was attempted
            _mockTokenExchangeClient.Verify(
                x => x.ExchangeTokenAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<CancellationToken>()),
                Times.Never);
        }

        [Fact]
        public async Task SendAsync_WithExternalToken_StartsTokenExchangeInBackground()
        {
            var tokenExchangeDelay = TimeSpan.FromMilliseconds(500);
            var handler = new MandatoryTokenExchangeDelegatingHandler(
                _mockInnerHandler.Object,
                _mockTokenExchangeClient.Object,
                _identityFederationClientId);

            var request = new HttpRequestMessage(HttpMethod.Get, "https://example.com");
            request.Headers.Authorization = new System.Net.Http.Headers.AuthenticationHeaderValue("Bearer", _externalToken);
            var expectedResponse = new HttpResponseMessage(HttpStatusCode.OK);

            var tokenExchangeResponse = new TokenExchangeResponse
            {
                AccessToken = _exchangedToken,
                TokenType = "Bearer",
                ExpiresIn = 3600,
                ExpiryTime = DateTime.UtcNow.AddHours(1)
            };

            _mockTokenExchangeClient
                .Setup(x => x.ExchangeTokenAsync(_externalToken, _identityFederationClientId, It.IsAny<CancellationToken>()))
                .Returns(async (string token, string clientId, CancellationToken ct) =>
                {
                    await Task.Delay(tokenExchangeDelay, ct);
                    return tokenExchangeResponse;
                });

            HttpRequestMessage? capturedRequest = null;

            _mockInnerHandler.Protected()
                .Setup<Task<HttpResponseMessage>>(
                    "SendAsync",
                    ItExpr.IsAny<HttpRequestMessage>(),
                    ItExpr.IsAny<CancellationToken>())
                .Callback<HttpRequestMessage, CancellationToken>((req, ct) => capturedRequest = req)
                .ReturnsAsync(expectedResponse);

            var httpClient = new HttpClient(handler);

            // First request should use original token and start background exchange
            var startTime = DateTime.UtcNow;
            var response = await httpClient.SendAsync(request);
            var requestDuration = DateTime.UtcNow - startTime;

            Assert.Equal(expectedResponse, response);
            Assert.True(requestDuration < tokenExchangeDelay,
                $"Request took {requestDuration.TotalMilliseconds}ms, which is longer than the token exchange delay of {tokenExchangeDelay.TotalMilliseconds}ms");

            Assert.NotNull(capturedRequest);
            Assert.Equal("Bearer", capturedRequest.Headers.Authorization?.Scheme);
            Assert.Equal(_externalToken, capturedRequest.Headers.Authorization?.Parameter); // First request uses original token

            // Wait for background task to complete
            await Task.Delay(tokenExchangeDelay + TimeSpan.FromMilliseconds(1000));

            // Make a second request - this should use the exchanged token
            var request2 = new HttpRequestMessage(HttpMethod.Get, "https://example.com/2");
            request2.Headers.Authorization = new System.Net.Http.Headers.AuthenticationHeaderValue("Bearer", _externalToken);
            HttpRequestMessage? capturedRequest2 = null;

            _mockInnerHandler.Protected()
                .Setup<Task<HttpResponseMessage>>(
                    "SendAsync",
                    ItExpr.Is<HttpRequestMessage>(r => r.RequestUri!.PathAndQuery == "/2"),
                    ItExpr.IsAny<CancellationToken>())
                .Callback<HttpRequestMessage, CancellationToken>((req, ct) => capturedRequest2 = req)
                .ReturnsAsync(new HttpResponseMessage(HttpStatusCode.OK));

            await httpClient.SendAsync(request2);

            Assert.NotNull(capturedRequest2);
            Assert.Equal("Bearer", capturedRequest2.Headers.Authorization?.Scheme);
            Assert.Equal(_exchangedToken, capturedRequest2.Headers.Authorization?.Parameter); // Second request uses exchanged token

            _mockTokenExchangeClient.Verify(
                x => x.ExchangeTokenAsync(_externalToken, _identityFederationClientId, It.IsAny<CancellationToken>()),
                Times.Once);
        }

        [Fact]
        public async Task SendAsync_WithTokenExchangeFailure_ContinuesWithOriginalToken()
        {
            var handler = new MandatoryTokenExchangeDelegatingHandler(
                _mockInnerHandler.Object,
                _mockTokenExchangeClient.Object,
                _identityFederationClientId);

            var request = new HttpRequestMessage(HttpMethod.Get, "https://example.com");
            request.Headers.Authorization = new System.Net.Http.Headers.AuthenticationHeaderValue("Bearer", _externalToken);
            var expectedResponse = new HttpResponseMessage(HttpStatusCode.OK);

            // Setup token exchange to fail
            _mockTokenExchangeClient
                .Setup(x => x.ExchangeTokenAsync(_externalToken, _identityFederationClientId, It.IsAny<CancellationToken>()))
                .ThrowsAsync(new Exception("Token exchange failed"));

            HttpRequestMessage? capturedRequest = null;

            _mockInnerHandler.Protected()
                .Setup<Task<HttpResponseMessage>>(
                    "SendAsync",
                    ItExpr.IsAny<HttpRequestMessage>(),
                    ItExpr.IsAny<CancellationToken>())
                .Callback<HttpRequestMessage, CancellationToken>((req, ct) => capturedRequest = req)
                .ReturnsAsync(expectedResponse);

            var httpClient = new HttpClient(handler);
            var response = await httpClient.SendAsync(request);

            Assert.Equal(expectedResponse, response);
            Assert.NotNull(capturedRequest);
            Assert.Equal("Bearer", capturedRequest.Headers.Authorization?.Scheme);
            Assert.Equal(_externalToken, capturedRequest.Headers.Authorization?.Parameter); // Should still use original token

            // Wait for background task to complete
            await Task.Delay(1000);

            var request2 = new HttpRequestMessage(HttpMethod.Get, "https://example.com/2");
            request2.Headers.Authorization = new System.Net.Http.Headers.AuthenticationHeaderValue("Bearer", _externalToken);
            HttpRequestMessage? capturedRequest2 = null;

            _mockInnerHandler.Protected()
                .Setup<Task<HttpResponseMessage>>(
                    "SendAsync",
                    ItExpr.Is<HttpRequestMessage>(r => r.RequestUri!.PathAndQuery == "/2"),
                    ItExpr.IsAny<CancellationToken>())
                .Callback<HttpRequestMessage, CancellationToken>((req, ct) => capturedRequest2 = req)
                .ReturnsAsync(new HttpResponseMessage(HttpStatusCode.OK));

            await httpClient.SendAsync(request2);

            Assert.NotNull(capturedRequest2);
            Assert.Equal("Bearer", capturedRequest2.Headers.Authorization?.Scheme);
            Assert.Equal(_externalToken, capturedRequest2.Headers.Authorization?.Parameter); // Should still use original token

            // Verify token exchange was attempted
            _mockTokenExchangeClient.Verify(
                x => x.ExchangeTokenAsync(_externalToken, _identityFederationClientId, It.IsAny<CancellationToken>()),
                Times.Once);
        }

        [Fact]
        public async Task SendAsync_WithSameExternalTokenMultipleTimes_ExchangesTokenOnlyOnce()
        {
            var handler = new MandatoryTokenExchangeDelegatingHandler(
                _mockInnerHandler.Object,
                _mockTokenExchangeClient.Object,
                _identityFederationClientId);

            var tokenExchangeResponse = new TokenExchangeResponse
            {
                AccessToken = _exchangedToken,
                TokenType = "Bearer",
                ExpiresIn = 3600,
                ExpiryTime = DateTime.UtcNow.AddHours(1)
            };

            _mockTokenExchangeClient
                .Setup(x => x.ExchangeTokenAsync(_externalToken, _identityFederationClientId, It.IsAny<CancellationToken>()))
                .ReturnsAsync(tokenExchangeResponse);

            _mockInnerHandler.Protected()
                .Setup<Task<HttpResponseMessage>>(
                    "SendAsync",
                    ItExpr.IsAny<HttpRequestMessage>(),
                    ItExpr.IsAny<CancellationToken>())
                .ReturnsAsync(new HttpResponseMessage(HttpStatusCode.OK));

            var httpClient = new HttpClient(handler);

            // Make multiple requests with the same token
            for (int i = 0; i < 3; i++)
            {
                var request = new HttpRequestMessage(HttpMethod.Get, $"https://example.com/{i}");
                request.Headers.Authorization = new System.Net.Http.Headers.AuthenticationHeaderValue("Bearer", _externalToken);
                await httpClient.SendAsync(request);
            }

            // Wait for background exchange to complete
            await Task.Delay(1000);

            // Token exchange should only be called once
            _mockTokenExchangeClient.Verify(
                x => x.ExchangeTokenAsync(_externalToken, _identityFederationClientId, It.IsAny<CancellationToken>()),
                Times.Once);
        }

        [Fact]
        public async Task SendAsync_WithDifferentExternalTokens_ExchangesEachTokenOnce()
        {
            var handler = new MandatoryTokenExchangeDelegatingHandler(
                _mockInnerHandler.Object,
                _mockTokenExchangeClient.Object,
                _identityFederationClientId);

            var externalToken1 = CreateJwtToken("https://external-provider.com", DateTime.UtcNow.AddHours(1));
            var externalToken2 = CreateJwtToken("https://another-provider.com", DateTime.UtcNow.AddHours(1));
            var exchangedToken1 = "exchanged-token-1";
            var exchangedToken2 = "exchanged-token-2";

            _mockTokenExchangeClient
                .Setup(x => x.ExchangeTokenAsync(externalToken1, _identityFederationClientId, It.IsAny<CancellationToken>()))
                .ReturnsAsync(new TokenExchangeResponse
                {
                    AccessToken = exchangedToken1,
                    TokenType = "Bearer",
                    ExpiresIn = 3600,
                    ExpiryTime = DateTime.UtcNow.AddHours(1)
                });

            _mockTokenExchangeClient
                .Setup(x => x.ExchangeTokenAsync(externalToken2, _identityFederationClientId, It.IsAny<CancellationToken>()))
                .ReturnsAsync(new TokenExchangeResponse
                {
                    AccessToken = exchangedToken2,
                    TokenType = "Bearer",
                    ExpiresIn = 3600,
                    ExpiryTime = DateTime.UtcNow.AddHours(1)
                });

            _mockInnerHandler.Protected()
                .Setup<Task<HttpResponseMessage>>(
                    "SendAsync",
                    ItExpr.IsAny<HttpRequestMessage>(),
                    ItExpr.IsAny<CancellationToken>())
                .ReturnsAsync(new HttpResponseMessage(HttpStatusCode.OK));

            var httpClient = new HttpClient(handler);

            // Make request with first token
            var request1 = new HttpRequestMessage(HttpMethod.Get, "https://example.com/1");
            request1.Headers.Authorization = new System.Net.Http.Headers.AuthenticationHeaderValue("Bearer", externalToken1);
            await httpClient.SendAsync(request1);

            // Wait for first exchange to complete
            await Task.Delay(1000);

            // Make request with second token
            var request2 = new HttpRequestMessage(HttpMethod.Get, "https://example.com/2");
            request2.Headers.Authorization = new System.Net.Http.Headers.AuthenticationHeaderValue("Bearer", externalToken2);
            await httpClient.SendAsync(request2);

            // Wait for second exchange to complete
            await Task.Delay(1000);

            // Verify both tokens were exchanged
            _mockTokenExchangeClient.Verify(
                x => x.ExchangeTokenAsync(externalToken1, _identityFederationClientId, It.IsAny<CancellationToken>()),
                Times.Once);
            _mockTokenExchangeClient.Verify(
                x => x.ExchangeTokenAsync(externalToken2, _identityFederationClientId, It.IsAny<CancellationToken>()),
                Times.Once);
        }

        [Fact]
        public async Task SendAsync_WithConcurrentRequestsSameToken_ExchangesTokenOnlyOnce()
        {
            var handler = new MandatoryTokenExchangeDelegatingHandler(
                _mockInnerHandler.Object,
                _mockTokenExchangeClient.Object,
                _identityFederationClientId);

            var tokenExchangeResponse = new TokenExchangeResponse
            {
                AccessToken = _exchangedToken,
                TokenType = "Bearer",
                ExpiresIn = 3600,
                ExpiryTime = DateTime.UtcNow.AddHours(1)
            };

            // Add a small delay to token exchange to simulate concurrent access
            _mockTokenExchangeClient
                .Setup(x => x.ExchangeTokenAsync(_externalToken, _identityFederationClientId, It.IsAny<CancellationToken>()))
                .Returns(async () =>
                {
                    await Task.Delay(200);
                    return tokenExchangeResponse;
                });

            _mockInnerHandler.Protected()
                .Setup<Task<HttpResponseMessage>>(
                    "SendAsync",
                    ItExpr.IsAny<HttpRequestMessage>(),
                    ItExpr.IsAny<CancellationToken>())
                .ReturnsAsync(new HttpResponseMessage(HttpStatusCode.OK));

            var httpClient = new HttpClient(handler);

            // Make concurrent requests with the same token
            var tasks = new[]
            {
                CreateAndSendRequest(httpClient, _externalToken, "https://example.com/1"),
                CreateAndSendRequest(httpClient, _externalToken, "https://example.com/2"),
                CreateAndSendRequest(httpClient, _externalToken, "https://example.com/3")
            };

            await Task.WhenAll(tasks);

            // Wait for any background token exchange to complete
            await Task.Delay(1000);

            // Token exchange should only be called once despite concurrent requests
            _mockTokenExchangeClient.Verify(
                x => x.ExchangeTokenAsync(_externalToken, _identityFederationClientId, It.IsAny<CancellationToken>()),
                Times.Once);
        }

        [Fact]
        public async Task SendAsync_WithInvalidJwtToken_UsesTokenDirectlyWithoutExchange()
        {
            var handler = new MandatoryTokenExchangeDelegatingHandler(
                _mockInnerHandler.Object,
                _mockTokenExchangeClient.Object,
                _identityFederationClientId);

            var invalidToken = "invalid-jwt-token";
            var request = new HttpRequestMessage(HttpMethod.Get, "https://example.com");
            request.Headers.Authorization = new System.Net.Http.Headers.AuthenticationHeaderValue("Bearer", invalidToken);
            var expectedResponse = new HttpResponseMessage(HttpStatusCode.OK);

            HttpRequestMessage? capturedRequest = null;

            _mockInnerHandler.Protected()
                .Setup<Task<HttpResponseMessage>>(
                    "SendAsync",
                    ItExpr.IsAny<HttpRequestMessage>(),
                    ItExpr.IsAny<CancellationToken>())
                .Callback<HttpRequestMessage, CancellationToken>((req, ct) => capturedRequest = req)
                .ReturnsAsync(expectedResponse);

            var httpClient = new HttpClient(handler);
            var response = await httpClient.SendAsync(request);

            Assert.Equal(expectedResponse, response);
            Assert.NotNull(capturedRequest);
            Assert.Equal("Bearer", capturedRequest.Headers.Authorization?.Scheme);
            Assert.Equal(invalidToken, capturedRequest.Headers.Authorization?.Parameter);

            // Wait for any background tasks
            await Task.Delay(1000);

            // Verify no token exchange was attempted
            _mockTokenExchangeClient.Verify(
                x => x.ExchangeTokenAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<CancellationToken>()),
                Times.Never);
        }

        [Fact]
        public async Task SendAsync_WithNoAuthorizationHeader_PassesThroughWithoutModification()
        {
            var handler = new MandatoryTokenExchangeDelegatingHandler(
                _mockInnerHandler.Object,
                _mockTokenExchangeClient.Object,
                _identityFederationClientId);

            var request = new HttpRequestMessage(HttpMethod.Get, "https://example.com");
            var expectedResponse = new HttpResponseMessage(HttpStatusCode.OK);

            HttpRequestMessage? capturedRequest = null;

            _mockInnerHandler.Protected()
                .Setup<Task<HttpResponseMessage>>(
                    "SendAsync",
                    ItExpr.IsAny<HttpRequestMessage>(),
                    ItExpr.IsAny<CancellationToken>())
                .Callback<HttpRequestMessage, CancellationToken>((req, ct) => capturedRequest = req)
                .ReturnsAsync(expectedResponse);

            var httpClient = new HttpClient(handler);
            var response = await httpClient.SendAsync(request);

            Assert.Equal(expectedResponse, response);
            Assert.NotNull(capturedRequest);
            Assert.Null(capturedRequest.Headers.Authorization);

            // Verify no token exchange was attempted
            _mockTokenExchangeClient.Verify(
                x => x.ExchangeTokenAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<CancellationToken>()),
                Times.Never);
        }

        private async Task<HttpResponseMessage> CreateAndSendRequest(HttpClient httpClient, string token, string url)
        {
            var request = new HttpRequestMessage(HttpMethod.Get, url);
            request.Headers.Authorization = new System.Net.Http.Headers.AuthenticationHeaderValue("Bearer", token);
            return await httpClient.SendAsync(request);
        }

        /// <summary>
        /// Creates a valid JWT token with the specified issuer and expiration time.
        /// This is for testing purposes only and creates a properly formatted JWT.
        /// </summary>
        private static string CreateJwtToken(string issuer, DateTime expiryTime)
        {
            // Create header
            var header = new
            {
                alg = "HS256",
                typ = "JWT"
            };

            // Create payload
            var payload = new
            {
                iss = issuer,
                exp = new DateTimeOffset(expiryTime).ToUnixTimeSeconds(),
                iat = new DateTimeOffset(DateTime.UtcNow).ToUnixTimeSeconds(),
                sub = "test-subject"
            };

            // Encode header and payload
            string encodedHeader = EncodeBase64Url(JsonSerializer.Serialize(header));
            string encodedPayload = EncodeBase64Url(JsonSerializer.Serialize(payload));

            // For testing, we'll use a dummy signature
            string signature = EncodeBase64Url("dummy-signature");

            return $"{encodedHeader}.{encodedPayload}.{signature}";
        }

        /// <summary>
        /// Encodes a string to base64url format.
        /// </summary>
        private static string EncodeBase64Url(string input)
        {
            byte[] bytes = Encoding.UTF8.GetBytes(input);
            string base64 = Convert.ToBase64String(bytes);

            // Convert base64 to base64url
            return base64
                .Replace('+', '-')
                .Replace('/', '_')
                .TrimEnd('=');
        }

        protected virtual void Dispose(bool disposing)
        {
            if (disposing)
            {
                _mockInnerHandler?.Object?.Dispose();
            }
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }
    }
}
