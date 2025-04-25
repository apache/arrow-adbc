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
using System.Net.Http.Headers;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace Apache.Arrow.Adbc.Drivers.Databricks.Auth
{
    /// <summary>
    /// Service for obtaining OAuth access tokens using the client credentials grant type.
    /// </summary>
    internal class OAuthClientCredentialsService : IDisposable
    {
        private readonly Lazy<HttpClient> _httpClient;
        private readonly string _clientId;
        private readonly string _clientSecret;
        private readonly Uri _baseUri;
        private readonly string _tokenEndpoint;
        private readonly int _timeoutMinutes;
        private readonly SemaphoreSlim _tokenLock = new SemaphoreSlim(1, 1);
        private TokenInfo? _cachedToken;

        private class TokenInfo
        {
            public string? AccessToken { get; set; }
            public DateTime ExpiresAt { get; set; }
            
            public bool IsExpired => DateTime.UtcNow >= ExpiresAt;
            
            // Add buffer time to refresh token before actual expiration
            public bool NeedsRefresh => DateTime.UtcNow >= ExpiresAt.AddMinutes(-5);
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="OAuthClientCredentialsService"/> class.
        /// </summary>
        /// <param name="clientId">The OAuth client ID.</param>
        /// <param name="clientSecret">The OAuth client secret.</param>
        /// <param name="baseUri">The base URI of the Databricks workspace.</param>
        public OAuthClientCredentialsService(
            string clientId,
            string clientSecret,
            Uri baseUri,
            int timeoutMinutes = 1,
            HttpClient? httpClient = null)
        {
            _clientId = clientId ?? throw new ArgumentNullException(nameof(clientId));
            _clientSecret = clientSecret ?? throw new ArgumentNullException(nameof(clientSecret));
            _baseUri = baseUri ?? throw new ArgumentNullException(nameof(baseUri));
            _timeoutMinutes = timeoutMinutes;
            _tokenEndpoint = DetermineTokenEndpoint();

            _httpClient = httpClient != null
                ? new Lazy<HttpClient>(() => httpClient)
                : new Lazy<HttpClient>(() =>
                {
                    var client = new HttpClient();
                    client.Timeout = TimeSpan.FromMinutes(_timeoutMinutes);
                    return client;
                });
        }

        private HttpClient HttpClient => _httpClient.Value;

        private string DetermineTokenEndpoint()
        {
            // For workspace URLs, the token endpoint is always /oidc/v1/token
            // TODO: Might be different for Azure AAD SPs
            return $"{_baseUri.Scheme}://{_baseUri.Host}/oidc/v1/token";
        }

        private string? GetValidCachedToken()
        {
            return _cachedToken != null && !_cachedToken.NeedsRefresh && _cachedToken.AccessToken != null
                ? _cachedToken.AccessToken
                : null;
        }


        private async Task<string> RefreshTokenInternalAsync(CancellationToken cancellationToken)
        {
            var request = CreateTokenRequest();

            HttpResponseMessage response;
            try
            {
                response = await HttpClient.SendAsync(request, cancellationToken);
                response.EnsureSuccessStatusCode();
            }
            catch (HttpRequestException ex)
            {
                throw new DatabricksException($"Failed to acquire OAuth access token: {ex.Message}", ex);
            }

            string content = await response.Content.ReadAsStringAsync();
            
            try
            {
                _cachedToken = ParseTokenResponse(content);
                return _cachedToken.AccessToken!;
            }
            catch (JsonException ex)
            {
                throw new DatabricksException($"Failed to parse OAuth response: {ex.Message}", ex);
            }
        }

        private HttpRequestMessage CreateTokenRequest()
        {
            var requestContent = new FormUrlEncodedContent(new[]
            {
                new KeyValuePair<string, string>("grant_type", "client_credentials"),
                new KeyValuePair<string, string>("scope", "sql")
            });

            var request = new HttpRequestMessage(HttpMethod.Post, _tokenEndpoint)
            {
                Content = requestContent
            };

            // Use Basic Auth with client ID and secret
            var authHeader = Convert.ToBase64String(
                System.Text.Encoding.ASCII.GetBytes($"{_clientId}:{_clientSecret}"));
            request.Headers.Authorization = new AuthenticationHeaderValue("Basic", authHeader);
            request.Headers.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));

            return request;
        }

        private TokenInfo ParseTokenResponse(string content)
        {
            using var jsonDoc = JsonDocument.Parse(content);
            
            if (!jsonDoc.RootElement.TryGetProperty("access_token", out var accessTokenElement))
            {
                throw new DatabricksException("OAuth response did not contain an access_token");
            }

            string? accessToken = accessTokenElement.GetString();
            if (string.IsNullOrEmpty(accessToken))
            {
                throw new DatabricksException("OAuth access_token was null or empty");
            }

            // Get expiration time from response
            if (!jsonDoc.RootElement.TryGetProperty("expires_in", out var expiresInElement))
            {
                throw new DatabricksException("OAuth response did not contain expires_in");
            }

            int expiresIn = expiresInElement.GetInt32();
            if (expiresIn <= 0)
            {
                throw new DatabricksException("OAuth expires_in value must be positive");
            }

            return new TokenInfo
            {
                AccessToken = accessToken!,
                ExpiresAt = DateTime.UtcNow.AddSeconds(expiresIn)
            };
        }

        /// <summary>
        /// Gets an OAuth access token using the client credentials grant type.
        /// </summary>
        /// <param name="cancellationToken">A cancellation token to cancel the operation.</param>
        /// <returns>The access token.</returns>
        /// <exception cref="DatabricksException">Thrown when the token request fails or the response is invalid.</exception>
        public async Task<string> GetAccessTokenAsync(CancellationToken cancellationToken = default)
        {
            // First try to get cached token without acquiring lock
            if (GetValidCachedToken() is string cachedToken)
            {
                return cachedToken;
            }

            // If token needs refresh, acquire lock with timeout
            var lockTimeout = TimeSpan.FromSeconds(30); // Reasonable timeout for lock acquisition
            if (!await _tokenLock.WaitAsync(lockTimeout, cancellationToken).ConfigureAwait(false))
            {
                throw new TimeoutException("Timeout waiting for token refresh lock");
            }

            try
            {
                // Double-check pattern in case another thread refreshed while we were waiting
                if (GetValidCachedToken() is string refreshedToken)
                {
                    return refreshedToken;
                }

                return await RefreshTokenInternalAsync(cancellationToken).ConfigureAwait(false);
            }
            finally
            {
                _tokenLock.Release();
            }
        }


        public void Dispose()
        {
            _tokenLock.Dispose();
            if (_httpClient.IsValueCreated)
            {
                HttpClient.Dispose();
            }
        }

    }
}