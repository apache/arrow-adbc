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
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow.Adbc.Drivers.Databricks.Auth;
using Apache.Arrow.Adbc.Drivers.Databricks;
using Moq;
using Moq.Protected;
using Xunit;

namespace Apache.Arrow.Adbc.Tests.Drivers.Databricks.Unit.Auth
{
    public class TokenExchangeClientTests : IDisposable
    {
        private readonly Mock<HttpMessageHandler> _mockHttpMessageHandler;
        private readonly HttpClient _httpClient;
        private readonly string _testHost = "test.databricks.com";

        public TokenExchangeClientTests()
        {
            _mockHttpMessageHandler = new Mock<HttpMessageHandler>();
            _httpClient = new HttpClient(_mockHttpMessageHandler.Object);
        }

        [Fact]
        public void Constructor_WithValidParameters_SetsEndpointCorrectly()
        {
            var client = new TokenExchangeClient(_httpClient, _testHost);
            Assert.NotNull(client);
            Assert.Equal($"https://{_testHost}/oidc/v1/token", client.TokenExchangeEndpoint);
        }

        [Fact]
        public void Constructor_WithHostTrailingSlash_RemovesTrailingSlash()
        {
            var hostWithSlash = "test.databricks.com/";
            var client = new TokenExchangeClient(_httpClient, hostWithSlash);
            Assert.Equal("https://test.databricks.com/oidc/v1/token", client.TokenExchangeEndpoint);
        }

        [Fact]
        public void Constructor_WithEmptyHost_ThrowsArgumentNullException()
        {
            Assert.Throws<ArgumentNullException>(() => new TokenExchangeClient(_httpClient, string.Empty));
        }

        [Fact]
        public async Task RefreshTokenAsync_WithValidResponse_ReturnsTokenExchangeResponse()
        {
            var testToken = "test-jwt-token";
            var expectedAccessToken = "new-access-token";
            var expectedTokenType = "Bearer";
            var expectedExpiresIn = 3600;

            var responseJson = JsonSerializer.Serialize(new
            {
                access_token = expectedAccessToken,
                token_type = expectedTokenType,
                expires_in = expectedExpiresIn
            });

            var httpResponseMessage = new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = new StringContent(responseJson)
            };

            _mockHttpMessageHandler.Protected()
                .Setup<Task<HttpResponseMessage>>(
                    "SendAsync",
                    ItExpr.Is<HttpRequestMessage>(req =>
                        req.Method == HttpMethod.Post &&
                        req.RequestUri != null &&
                        req.RequestUri.ToString() == $"https://{_testHost}/oidc/v1/token"),
                    ItExpr.IsAny<CancellationToken>())
                .ReturnsAsync(httpResponseMessage);

            var client = new TokenExchangeClient(_httpClient, _testHost);

            var result = await client.RefreshTokenAsync(testToken, CancellationToken.None);

            Assert.NotNull(result);
            Assert.Equal(expectedAccessToken, result.AccessToken);
            Assert.Equal(expectedTokenType, result.TokenType);
            Assert.Equal(expectedExpiresIn, result.ExpiresIn);
            Assert.True(result.ExpiryTime > DateTime.UtcNow);
            Assert.True(result.ExpiryTime <= DateTime.UtcNow.AddSeconds(expectedExpiresIn + 1));
        }

        [Fact]
        public async Task RefreshTokenAsync_SendsCorrectRequestFormat()
        {
            var testToken = "test-jwt-token";
            var responseJson = JsonSerializer.Serialize(new
            {
                access_token = "token",
                token_type = "Bearer",
                expires_in = 3600
            });

            var httpResponseMessage = new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = new StringContent(responseJson)
            };

            HttpRequestMessage? capturedRequest = null;

            _mockHttpMessageHandler.Protected()
                .Setup<Task<HttpResponseMessage>>(
                    "SendAsync",
                    ItExpr.IsAny<HttpRequestMessage>(),
                    ItExpr.IsAny<CancellationToken>())
                .Callback<HttpRequestMessage, CancellationToken>((req, ct) => capturedRequest = req)
                .ReturnsAsync(httpResponseMessage);

            var client = new TokenExchangeClient(_httpClient, _testHost);

            await client.RefreshTokenAsync(testToken, CancellationToken.None);

            Assert.NotNull(capturedRequest);
            Assert.Equal(HttpMethod.Post, capturedRequest.Method);
            Assert.Equal($"https://{_testHost}/oidc/v1/token", capturedRequest?.RequestUri?.ToString());
            Assert.True(capturedRequest?.Headers.Accept.Contains(new System.Net.Http.Headers.MediaTypeWithQualityHeaderValue("*/*")));

            var content = capturedRequest?.Content as FormUrlEncodedContent;
            Assert.NotNull(content);

            var formContent = await content.ReadAsStringAsync();
            Assert.Contains("grant_type=urn%3Aietf%3Aparams%3Aoauth%3Agrant-type%3Ajwt-bearer", formContent);
            Assert.Contains($"assertion={testToken}", formContent);
        }

        [Fact]
        public async Task RefreshTokenAsync_WithHttpError_ThrowsHttpRequestException()
        {
            var testToken = "test-jwt-token";
            var httpResponseMessage = new HttpResponseMessage(HttpStatusCode.Unauthorized)
            {
                Content = new StringContent("Unauthorized")
            };

            _mockHttpMessageHandler.Protected()
                .Setup<Task<HttpResponseMessage>>(
                    "SendAsync",
                    ItExpr.IsAny<HttpRequestMessage>(),
                    ItExpr.IsAny<CancellationToken>())
                .ReturnsAsync(httpResponseMessage);

            var client = new TokenExchangeClient(_httpClient, _testHost);

            await Assert.ThrowsAsync<HttpRequestException>(() =>
                client.RefreshTokenAsync(testToken, CancellationToken.None));
        }

        [Fact]
        public async Task RefreshTokenAsync_WithMissingAccessToken_ThrowsDatabricksException()
        {
            var testToken = "test-jwt-token";
            var responseJson = JsonSerializer.Serialize(new
            {
                token_type = "Bearer",
                expires_in = 3600
            });

            var httpResponseMessage = new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = new StringContent(responseJson)
            };

            _mockHttpMessageHandler.Protected()
                .Setup<Task<HttpResponseMessage>>(
                    "SendAsync",
                    ItExpr.IsAny<HttpRequestMessage>(),
                    ItExpr.IsAny<CancellationToken>())
                .ReturnsAsync(httpResponseMessage);

            var client = new TokenExchangeClient(_httpClient, _testHost);

            var exception = await Assert.ThrowsAsync<DatabricksException>(() =>
                client.RefreshTokenAsync(testToken, CancellationToken.None));

            Assert.Contains("access_token", exception.Message);
        }

        [Fact]
        public async Task RefreshTokenAsync_WithEmptyAccessToken_ThrowsDatabricksException()
        {
            var testToken = "test-jwt-token";
            var responseJson = JsonSerializer.Serialize(new
            {
                access_token = "",
                token_type = "Bearer",
                expires_in = 3600
            });

            var httpResponseMessage = new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = new StringContent(responseJson)
            };

            _mockHttpMessageHandler.Protected()
                .Setup<Task<HttpResponseMessage>>(
                    "SendAsync",
                    ItExpr.IsAny<HttpRequestMessage>(),
                    ItExpr.IsAny<CancellationToken>())
                .ReturnsAsync(httpResponseMessage);

            var client = new TokenExchangeClient(_httpClient, _testHost);

            var exception = await Assert.ThrowsAsync<DatabricksException>(() =>
                client.RefreshTokenAsync(testToken, CancellationToken.None));

            Assert.Contains("access_token was null or empty", exception.Message);
        }

        [Fact]
        public async Task RefreshTokenAsync_WithMissingTokenType_ThrowsDatabricksException()
        {
            var testToken = "test-jwt-token";
            var responseJson = JsonSerializer.Serialize(new
            {
                access_token = "token",
                expires_in = 3600
            });

            var httpResponseMessage = new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = new StringContent(responseJson)
            };

            _mockHttpMessageHandler.Protected()
                .Setup<Task<HttpResponseMessage>>(
                    "SendAsync",
                    ItExpr.IsAny<HttpRequestMessage>(),
                    ItExpr.IsAny<CancellationToken>())
                .ReturnsAsync(httpResponseMessage);

            var client = new TokenExchangeClient(_httpClient, _testHost);

            var exception = await Assert.ThrowsAsync<DatabricksException>(() =>
                client.RefreshTokenAsync(testToken, CancellationToken.None));

            Assert.Contains("token_type", exception.Message);
        }

        [Fact]
        public async Task RefreshTokenAsync_WithMissingExpiresIn_ThrowsDatabricksException()
        {
            var testToken = "test-jwt-token";
            var responseJson = JsonSerializer.Serialize(new
            {
                access_token = "token",
                token_type = "Bearer"
            });

            var httpResponseMessage = new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = new StringContent(responseJson)
            };

            _mockHttpMessageHandler.Protected()
                .Setup<Task<HttpResponseMessage>>(
                    "SendAsync",
                    ItExpr.IsAny<HttpRequestMessage>(),
                    ItExpr.IsAny<CancellationToken>())
                .ReturnsAsync(httpResponseMessage);

            var client = new TokenExchangeClient(_httpClient, _testHost);

            var exception = await Assert.ThrowsAsync<DatabricksException>(() =>
                client.RefreshTokenAsync(testToken, CancellationToken.None));

            Assert.Contains("expires_in", exception.Message);
        }

        [Fact]
        public async Task RefreshTokenAsync_WithNegativeExpiresIn_ThrowsDatabricksException()
        {
            var testToken = "test-jwt-token";
            var responseJson = JsonSerializer.Serialize(new
            {
                access_token = "token",
                token_type = "Bearer",
                expires_in = -1
            });

            var httpResponseMessage = new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = new StringContent(responseJson)
            };

            _mockHttpMessageHandler.Protected()
                .Setup<Task<HttpResponseMessage>>(
                    "SendAsync",
                    ItExpr.IsAny<HttpRequestMessage>(),
                    ItExpr.IsAny<CancellationToken>())
                .ReturnsAsync(httpResponseMessage);

            var client = new TokenExchangeClient(_httpClient, _testHost);

            var exception = await Assert.ThrowsAsync<DatabricksException>(() =>
                client.RefreshTokenAsync(testToken, CancellationToken.None));

            Assert.Contains("expires_in value must be positive", exception.Message);
        }

        [Fact]
        public async Task RefreshTokenAsync_WithInvalidJson_ThrowsJsonException()
        {
            var testToken = "test-jwt-token";
            var invalidJson = "{ invalid json }";

            var httpResponseMessage = new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = new StringContent(invalidJson)
            };

            _mockHttpMessageHandler.Protected()
                .Setup<Task<HttpResponseMessage>>(
                    "SendAsync",
                    ItExpr.IsAny<HttpRequestMessage>(),
                    ItExpr.IsAny<CancellationToken>())
                .ReturnsAsync(httpResponseMessage);

            var client = new TokenExchangeClient(_httpClient, _testHost);

            await Assert.ThrowsAnyAsync<JsonException>(() =>
                client.RefreshTokenAsync(testToken, CancellationToken.None));
        }

        [Fact]
        public async Task RefreshTokenAsync_WithCancellationToken_PropagatesCancellation()
        {
            var testToken = "test-jwt-token";
            var cts = new CancellationTokenSource();
            cts.Cancel();

            _mockHttpMessageHandler.Protected()
                .Setup<Task<HttpResponseMessage>>(
                    "SendAsync",
                    ItExpr.IsAny<HttpRequestMessage>(),
                    ItExpr.IsAny<CancellationToken>())
                .ThrowsAsync(new TaskCanceledException());

            var client = new TokenExchangeClient(_httpClient, _testHost);

            await Assert.ThrowsAsync<TaskCanceledException>(() =>
                client.RefreshTokenAsync(testToken, cts.Token));
        }

        [Fact]
        public async Task RefreshTokenAsync_CalculatesExpiryTimeCorrectly()
        {
            var testToken = "test-jwt-token";
            var expiresIn = 1800; // 30 minutes
            var beforeCall = DateTime.UtcNow;

            var responseJson = JsonSerializer.Serialize(new
            {
                access_token = "token",
                token_type = "Bearer",
                expires_in = expiresIn
            });

            var httpResponseMessage = new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = new StringContent(responseJson)
            };

            _mockHttpMessageHandler.Protected()
                .Setup<Task<HttpResponseMessage>>(
                    "SendAsync",
                    ItExpr.IsAny<HttpRequestMessage>(),
                    ItExpr.IsAny<CancellationToken>())
                .ReturnsAsync(httpResponseMessage);

            var client = new TokenExchangeClient(_httpClient, _testHost);

            var result = await client.RefreshTokenAsync(testToken, CancellationToken.None);
            var afterCall = DateTime.UtcNow;

            var expectedMinExpiry = beforeCall.AddSeconds(expiresIn);
            var expectedMaxExpiry = afterCall.AddSeconds(expiresIn);

            Assert.True(result.ExpiryTime >= expectedMinExpiry);
            Assert.True(result.ExpiryTime <= expectedMaxExpiry);
        }

        [Fact]
        public async Task ExchangeTokenAsync_WithoutIdentityFederationClientId_SendsCorrectRequest()
        {
            var testToken = "test-jwt-token";
            var responseJson = JsonSerializer.Serialize(new
            {
                access_token = "exchanged-token",
                token_type = "Bearer",
                expires_in = 3600
            });

            var httpResponseMessage = new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = new StringContent(responseJson)
            };

            HttpRequestMessage? capturedRequest = null;

            _mockHttpMessageHandler.Protected()
                .Setup<Task<HttpResponseMessage>>(
                    "SendAsync",
                    ItExpr.IsAny<HttpRequestMessage>(),
                    ItExpr.IsAny<CancellationToken>())
                .Callback<HttpRequestMessage, CancellationToken>((req, ct) => capturedRequest = req)
                .ReturnsAsync(httpResponseMessage);

            var client = new TokenExchangeClient(_httpClient, _testHost);

            var result = await client.ExchangeTokenAsync(testToken, null, CancellationToken.None);

            Assert.NotNull(result);
            Assert.Equal("exchanged-token", result.AccessToken);

            Assert.NotNull(capturedRequest);
            Assert.NotNull(capturedRequest.Content);
            var formContent = await capturedRequest.Content.ReadAsStringAsync();
            Assert.Contains("grant_type=urn%3Aietf%3Aparams%3Aoauth%3Agrant-type%3Ajwt-bearer", formContent);
            Assert.Contains($"assertion={testToken}", formContent);
            Assert.Contains("scope=sql", formContent);
            Assert.Contains("return_original_token_if_authenticated=true", formContent);
            Assert.DoesNotContain("identity_federation_client_id", formContent);
        }

        [Fact]
        public async Task ExchangeTokenAsync_WithIdentityFederationClientId_SendsCorrectRequest()
        {
            var testToken = "test-jwt-token";
            var clientId = "test-client-id";
            var responseJson = JsonSerializer.Serialize(new
            {
                access_token = "exchanged-token",
                token_type = "Bearer",
                expires_in = 3600
            });

            var httpResponseMessage = new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = new StringContent(responseJson)
            };

            HttpRequestMessage? capturedRequest = null;

            _mockHttpMessageHandler.Protected()
                .Setup<Task<HttpResponseMessage>>(
                    "SendAsync",
                    ItExpr.IsAny<HttpRequestMessage>(),
                    ItExpr.IsAny<CancellationToken>())
                .Callback<HttpRequestMessage, CancellationToken>((req, ct) => capturedRequest = req)
                .ReturnsAsync(httpResponseMessage);

            var client = new TokenExchangeClient(_httpClient, _testHost);

            var result = await client.ExchangeTokenAsync(testToken, clientId, CancellationToken.None);

            Assert.NotNull(result);
            Assert.Equal("exchanged-token", result.AccessToken);

            Assert.NotNull(capturedRequest);
            Assert.NotNull(capturedRequest.Content);
            var formContent = await capturedRequest.Content.ReadAsStringAsync();
            Assert.Contains("grant_type=urn%3Aietf%3Aparams%3Aoauth%3Agrant-type%3Ajwt-bearer", formContent);
            Assert.Contains($"assertion={testToken}", formContent);
            Assert.Contains("scope=sql", formContent);
            Assert.Contains($"identity_federation_client_id={clientId}", formContent);
            Assert.DoesNotContain("return_original_token_if_authenticated", formContent);
        }

        [Fact]
        public async Task ExchangeTokenAsync_WithEmptyIdentityFederationClientId_SendsReturnOriginalToken()
        {
            var testToken = "test-jwt-token";
            var clientId = "";
            var responseJson = JsonSerializer.Serialize(new
            {
                access_token = "exchanged-token",
                token_type = "Bearer",
                expires_in = 3600
            });

            var httpResponseMessage = new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = new StringContent(responseJson)
            };

            HttpRequestMessage? capturedRequest = null;

            _mockHttpMessageHandler.Protected()
                .Setup<Task<HttpResponseMessage>>(
                    "SendAsync",
                    ItExpr.IsAny<HttpRequestMessage>(),
                    ItExpr.IsAny<CancellationToken>())
                .Callback<HttpRequestMessage, CancellationToken>((req, ct) => capturedRequest = req)
                .ReturnsAsync(httpResponseMessage);

            var client = new TokenExchangeClient(_httpClient, _testHost);

            var result = await client.ExchangeTokenAsync(testToken, clientId, CancellationToken.None);

            Assert.NotNull(result);
            Assert.NotNull(capturedRequest);
            Assert.NotNull(capturedRequest.Content);
            var formContent = await capturedRequest.Content.ReadAsStringAsync();
            Assert.Contains("return_original_token_if_authenticated=true", formContent);
            Assert.DoesNotContain("identity_federation_client_id", formContent);
        }

        [Fact]
        public async Task ExchangeTokenAsync_WithHttpError_ThrowsHttpRequestException()
        {
            var testToken = "test-jwt-token";
            var httpResponseMessage = new HttpResponseMessage(HttpStatusCode.Unauthorized)
            {
                Content = new StringContent("Unauthorized")
            };

            _mockHttpMessageHandler.Protected()
                .Setup<Task<HttpResponseMessage>>(
                    "SendAsync",
                    ItExpr.IsAny<HttpRequestMessage>(),
                    ItExpr.IsAny<CancellationToken>())
                .ReturnsAsync(httpResponseMessage);

            var client = new TokenExchangeClient(_httpClient, _testHost);

            await Assert.ThrowsAsync<HttpRequestException>(() =>
                client.ExchangeTokenAsync(testToken, null, CancellationToken.None));
        }

        [Fact]
        public async Task ExchangeTokenAsync_UsesCorrectEndpoint()
        {
            var testToken = "test-jwt-token";
            var responseJson = JsonSerializer.Serialize(new
            {
                access_token = "token",
                token_type = "Bearer",
                expires_in = 3600
            });

            var httpResponseMessage = new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = new StringContent(responseJson)
            };

            HttpRequestMessage? capturedRequest = null;

            _mockHttpMessageHandler.Protected()
                .Setup<Task<HttpResponseMessage>>(
                    "SendAsync",
                    ItExpr.IsAny<HttpRequestMessage>(),
                    ItExpr.IsAny<CancellationToken>())
                .Callback<HttpRequestMessage, CancellationToken>((req, ct) => capturedRequest = req)
                .ReturnsAsync(httpResponseMessage);

            var client = new TokenExchangeClient(_httpClient, _testHost);

            await client.ExchangeTokenAsync(testToken, null, CancellationToken.None);

            Assert.NotNull(capturedRequest);
            Assert.Equal(HttpMethod.Post, capturedRequest.Method);
            Assert.Equal($"https://{_testHost}/oidc/v1/token", capturedRequest?.RequestUri?.ToString());
        }

        protected virtual void Dispose(bool disposing)
        {
            if (disposing)
            {
                _httpClient?.Dispose();
            }
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }
    }
}
