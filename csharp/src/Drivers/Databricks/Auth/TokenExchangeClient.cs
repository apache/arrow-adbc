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
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace Apache.Arrow.Adbc.Drivers.Databricks.Auth
{
    /// <summary>
    /// Response from the token exchange API.
    /// </summary>
    internal class TokenExchangeResponse
    {
        /// <summary>
        /// The new access token.
        /// </summary>
        public string AccessToken { get; set; } = string.Empty;

        /// <summary>
        /// The token type (e.g., "Bearer").
        /// </summary>
        public string TokenType { get; set; } = string.Empty;

        /// <summary>
        /// The number of seconds until the token expires.
        /// </summary>
        public int ExpiresIn { get; set; }

        /// <summary>
        /// The calculated expiration time based on ExpiresIn.
        /// </summary>
        public DateTime ExpiryTime { get; set; }
    }

    /// <summary>
    /// Interface for token exchange operations.
    /// </summary>
    internal interface ITokenExchangeClient
    {
        /// <summary>
        /// Gets the token exchange endpoint URL.
        /// </summary>
        string TokenExchangeEndpoint { get; }

        /// <summary>
        /// Refreshes the provided token to extend the lifetime.
        /// </summary>
        /// <param name="token">The token to refresh.</param>
        /// <param name="cancellationToken">A cancellation token.</param>
        /// <returns>The response from the token exchange API.</returns>
        Task<TokenExchangeResponse> RefreshTokenAsync(string token, CancellationToken cancellationToken);

        /// <summary>
        /// Exchanges the provided token for a Databricks OAuth token.
        /// </summary>
        /// <param name="token">The token to exchange.</param>
        /// <param name="identityFederationClientId">Optional identity federation client ID.</param>
        /// <param name="cancellationToken">A cancellation token.</param>
        /// <returns>The response from the token exchange API.</returns>
        Task<TokenExchangeResponse> ExchangeTokenAsync(string token, string? identityFederationClientId, CancellationToken cancellationToken);
    }

    /// <summary>
    /// Client for exchanging tokens using the Databricks token exchange API.
    /// </summary>
    internal class TokenExchangeClient : ITokenExchangeClient
    {
        private readonly HttpClient _httpClient;
        private readonly string _tokenExchangeEndpoint;

        public string TokenExchangeEndpoint => _tokenExchangeEndpoint;

        /// <summary>
        /// Initializes a new instance of the <see cref="TokenExchangeClient"/> class.
        /// </summary>
        /// <param name="httpClient">The HTTP client to use for requests.</param>
        /// <param name="host">The host of the Databricks workspace.</param>
        public TokenExchangeClient(HttpClient httpClient, string host)
        {
            _httpClient = httpClient ?? throw new ArgumentNullException(nameof(httpClient));

            if (string.IsNullOrEmpty(host))
            {
                throw new ArgumentNullException(nameof(host));
            }

            // Ensure the host doesn't have a trailing slash
            host = host.TrimEnd('/');

            _tokenExchangeEndpoint = $"https://{host}/oidc/v1/token";
        }

        /// <summary>
        /// Refreshes the provided token to extend the lifetime.
        /// </summary>
        /// <param name="token">The token to refresh.</param>
        /// <param name="cancellationToken">A cancellation token.</param>
        /// <returns>The response from the token exchange API.</returns>
        public async Task<TokenExchangeResponse> RefreshTokenAsync(string token, CancellationToken cancellationToken)
        {
            var content = new FormUrlEncodedContent(new[]
            {
                new KeyValuePair<string, string>("grant_type", "urn:ietf:params:oauth:grant-type:jwt-bearer"),
                new KeyValuePair<string, string>("assertion", token)
            });

            var request = new HttpRequestMessage(HttpMethod.Post, _tokenExchangeEndpoint)
            {
                Content = content
            };
            request.Headers.Accept.Add(new System.Net.Http.Headers.MediaTypeWithQualityHeaderValue("*/*"));

            HttpResponseMessage response = await _httpClient.SendAsync(request, cancellationToken);

            response.EnsureSuccessStatusCode();

            string responseContent = await response.Content.ReadAsStringAsync();
            return ParseTokenResponse(responseContent);
        }

        /// <summary>
        /// Exchanges the provided token for a Databricks OAuth token.
        /// </summary>
        /// <param name="token">The token to exchange.</param>
        /// <param name="identityFederationClientId">Optional identity federation client ID.</param>
        /// <param name="cancellationToken">A cancellation token.</param>
        /// <returns>The response from the token exchange API.</returns>
        public async Task<TokenExchangeResponse> ExchangeTokenAsync(
            string token,
            string? identityFederationClientId,
            CancellationToken cancellationToken)
        {
            var formData = new List<KeyValuePair<string, string>>
            {
                new KeyValuePair<string, string>("grant_type", "urn:ietf:params:oauth:grant-type:jwt-bearer"),
                new KeyValuePair<string, string>("assertion", token),
                new KeyValuePair<string, string>("scope", "sql")
            };

            if (!string.IsNullOrEmpty(identityFederationClientId))
            {
                formData.Add(new KeyValuePair<string, string>("identity_federation_client_id", identityFederationClientId!));
            }
            else
            {
                formData.Add(new KeyValuePair<string, string>("return_original_token_if_authenticated", "true"));
            }

            var content = new FormUrlEncodedContent(formData);

            var request = new HttpRequestMessage(HttpMethod.Post, _tokenExchangeEndpoint)
            {
                Content = content
            };
            request.Headers.Accept.Add(new System.Net.Http.Headers.MediaTypeWithQualityHeaderValue("*/*"));

            HttpResponseMessage response = await _httpClient.SendAsync(request, cancellationToken);

            response.EnsureSuccessStatusCode();

            string responseContent = await response.Content.ReadAsStringAsync();
            return ParseTokenResponse(responseContent);
        }

        /// <summary>
        /// Parses the token exchange API response.
        /// </summary>
        /// <param name="responseContent">The response content to parse.</param>
        /// <returns>The parsed token exchange response.</returns>
        private TokenExchangeResponse ParseTokenResponse(string responseContent)
        {
            using JsonDocument jsonDoc = JsonDocument.Parse(responseContent);
            var root = jsonDoc.RootElement;

            if (!root.TryGetProperty("access_token", out JsonElement accessTokenElement))
            {
                throw new DatabricksException("Token exchange response did not contain an access_token");
            }

            string? accessToken = accessTokenElement.GetString();
            if (string.IsNullOrEmpty(accessToken))
            {
                throw new DatabricksException("Token exchange access_token was null or empty");
            }

            if (!root.TryGetProperty("token_type", out JsonElement tokenTypeElement))
            {
                throw new DatabricksException("Token exchange response did not contain token_type");
            }

            string? tokenType = tokenTypeElement.GetString();
            if (string.IsNullOrEmpty(tokenType))
            {
                throw new DatabricksException("Token exchange token_type was null or empty");
            }

            if (!root.TryGetProperty("expires_in", out JsonElement expiresInElement))
            {
                throw new DatabricksException("Token exchange response did not contain expires_in");
            }

            int expiresIn = expiresInElement.GetInt32();
            if (expiresIn <= 0)
            {
                throw new DatabricksException("Token exchange expires_in value must be positive");
            }

            DateTime expiryTime = DateTime.UtcNow.AddSeconds(expiresIn);

            return new TokenExchangeResponse
            {
                AccessToken = accessToken!,
                TokenType = tokenType!,
                ExpiresIn = expiresIn,
                ExpiryTime = expiryTime
            };
        }
    }
}
