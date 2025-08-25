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
    internal class OAuthClientCredentialsProvider : IDisposable
    {
        private readonly HttpClient _httpClient;
        private readonly string _clientId;
        private readonly string _clientSecret;
        private readonly string _host;
        private readonly string _tokenEndpoint;
        private readonly int _timeoutMinutes;
        private readonly int _refreshBufferMinutes;
        private readonly string _scope;
        private readonly SemaphoreSlim _tokenLock = new SemaphoreSlim(1, 1);
        private TokenInfo? _cachedToken;

        private class TokenInfo
        {
            public string? AccessToken { get; set; }
            public DateTime ExpiresAt { get; set; }
            private readonly int _refreshBufferMinutes;

            public string? Scope { get; set; }

            public TokenInfo(int refreshBufferMinutes)
            {
                _refreshBufferMinutes = refreshBufferMinutes;
            }

            // Add buffer time to refresh token before actual expiration
            public bool NeedsRefresh => DateTime.UtcNow >= ExpiresAt.AddMinutes(-_refreshBufferMinutes);
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="OAuthClientCredentialsService"/> class.
        /// </summary>
        /// <param name="httpClient">The HTTP client to use for requests.</param>
        /// <param name="clientId">The OAuth client ID.</param>
        /// <param name="clientSecret">The OAuth client secret.</param>
        /// <param name="host">The base host of the Databricks workspace.</param>
        /// <param name="scope">The scope for the OAuth token.</param>
        /// <param name="timeoutMinutes">The timeout in minutes for HTTP requests.</param>
        /// <param name="refreshBufferMinutes">The number of minutes before token expiration to refresh the token.</param>
        public OAuthClientCredentialsProvider(
            HttpClient httpClient,
            string clientId,
            string clientSecret,
            string host,
            string scope = "sql",
            int timeoutMinutes = 1,
            int refreshBufferMinutes = 5)
        {
            _clientId = clientId ?? throw new ArgumentNullException(nameof(clientId));
            _clientSecret = clientSecret ?? throw new ArgumentNullException(nameof(clientSecret));
            _host = host ?? throw new ArgumentNullException(nameof(host));
            _timeoutMinutes = timeoutMinutes;
            _refreshBufferMinutes = refreshBufferMinutes;
            _scope = scope ?? throw new ArgumentNullException(nameof(scope));
            _tokenEndpoint = DetermineTokenEndpoint();

            _httpClient = httpClient;
            _httpClient.Timeout = TimeSpan.FromMinutes(_timeoutMinutes);
        }

        private string DetermineTokenEndpoint()
        {
            // For workspace URLs, the token endpoint is always /oidc/v1/token
            return $"https://{_host}/oidc/v1/token";
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
                response = await _httpClient.SendAsync(request, cancellationToken);
                response.EnsureSuccessStatusCode();
            }
            catch (Exception ex)
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
                new KeyValuePair<string, string>("scope", _scope)
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

            if (!jsonDoc.RootElement.TryGetProperty("scope", out var scopeElement))
            {
                throw new DatabricksException("OAuth response did not contain scope");
            }

            string? scope = scopeElement.GetString();
            if (string.IsNullOrEmpty(scope))
            {
                throw new DatabricksException("OAuth scope was null or empty");
            }

            return new TokenInfo(_refreshBufferMinutes)
            {
                AccessToken = accessToken!,
                ExpiresAt = DateTime.UtcNow.AddSeconds(expiresIn),
                Scope = scope!
            };
        }

        public async Task<string> GetAccessTokenAsync(CancellationToken cancellationToken = default)
        {
            // First try to get cached token without acquiring lock
            if (GetValidCachedToken() is string cachedToken)
            {
                return cachedToken;
            }

            await _tokenLock.WaitAsync(cancellationToken);

            try
            {
                // Double-check pattern in case another thread refreshed while we were waiting
                if (GetValidCachedToken() is string refreshedToken)
                {
                    return refreshedToken;
                }

                return await RefreshTokenInternalAsync(cancellationToken);
            }
            finally
            {
                _tokenLock.Release();
            }
        }

        public void Dispose()
        {
            _tokenLock.Dispose();
        }

        public string? GetCachedTokenScope()
        {
            return _cachedToken?.Scope;
        }
    }
}
