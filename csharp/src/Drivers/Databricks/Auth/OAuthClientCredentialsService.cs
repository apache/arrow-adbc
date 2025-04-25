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
    internal class OAuthClientCredentialsService
    {
        private readonly HttpClient _httpClient;
        private readonly string _clientId;
        private readonly string _clientSecret;
        private readonly Uri _baseUri;
        private readonly string? _tenantId;
        private readonly string _scope;
        private readonly string _tokenEndpoint;

        /// <summary>
        /// Initializes a new instance of the <see cref="OAuthClientCredentialsService"/> class.
        /// </summary>
        /// <param name="httpClient">The HTTP client to use for token requests.</param>
        /// <param name="clientId">The OAuth client ID.</param>
        /// <param name="clientSecret">The OAuth client secret.</param>
        /// <param name="baseUri">The base URI of the Databricks workspace.</param>
        /// <param name="tenantId">The Azure AD tenant ID. Required for Azure Databricks.</param>
        /// <param name="scope">The OAuth scope to request. Default is "all-apis".</param>
        public OAuthClientCredentialsService(
            HttpClient httpClient,
            string clientId,
            string clientSecret,
            Uri baseUri,
            string? tenantId = null,
            string scope = "all-apis")
        {
            _httpClient = httpClient ?? throw new ArgumentNullException(nameof(httpClient));
            _clientId = clientId ?? throw new ArgumentNullException(nameof(clientId));
            _clientSecret = clientSecret ?? throw new ArgumentNullException(nameof(clientSecret));
            _baseUri = baseUri ?? throw new ArgumentNullException(nameof(baseUri));
            _tenantId = tenantId;
            _scope = scope ?? "all-apis";
            _tokenEndpoint = DetermineTokenEndpoint();
        }

        private string DetermineTokenEndpoint()
        {
            string host = _baseUri.Host.ToLowerInvariant();
            if (host.Contains("azuredatabricks.net"))
            {
                if (string.IsNullOrEmpty(_tenantId))
                {
                    throw new ArgumentException("Azure Databricks requires a tenantId to determine the token endpoint.");
                }

                return $"https://login.microsoftonline.com/{_tenantId}/oauth2/v2.0/token";
            }
            else
            {
                // Applies to AWS and GCP (if using Databricks-hosted IdP)
                return "https://accounts.cloud.databricks.com/oidc/token";
            }
        }

        /// <summary>
        /// Gets an OAuth access token using the client credentials grant type.
        /// </summary>
        /// <param name="cancellationToken">A cancellation token to cancel the operation.</param>
        /// <returns>The access token.</returns>
        /// <exception cref="DatabricksException">Thrown when the token request fails or the response is invalid.</exception>
        public async Task<string> GetAccessTokenAsync(CancellationToken cancellationToken = default)
        {
            var requestContent = new FormUrlEncodedContent(new[]
            {
                new KeyValuePair<string, string>("grant_type", "client_credentials"),
                new KeyValuePair<string, string>("client_id", _clientId),
                new KeyValuePair<string, string>("client_secret", _clientSecret),
                new KeyValuePair<string, string>("scope", _scope)
            });

            var request = new HttpRequestMessage(HttpMethod.Post, _tokenEndpoint)
            {
                Content = requestContent
            };

            request.Headers.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));

            HttpResponseMessage response;
            try
            {
                response = await _httpClient.SendAsync(request, cancellationToken);
                response.EnsureSuccessStatusCode();
            }
            catch (HttpRequestException ex)
            {
                throw new DatabricksException($"Failed to acquire OAuth access token: {ex.Message}", ex);
            }

            string content = await response.Content.ReadAsStringAsync();
            
            try
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

                return accessToken!;
            }
            catch (JsonException ex)
            {
                throw new DatabricksException($"Failed to parse OAuth response: {ex.Message}", ex);
            }
        }
    }
}