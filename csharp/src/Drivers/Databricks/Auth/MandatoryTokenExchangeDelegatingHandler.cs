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
using System.Net.Http;
using System.Net.Http.Headers;
using System.Threading;
using System.Threading.Tasks;

namespace Apache.Arrow.Adbc.Drivers.Databricks.Auth
{
    /// <summary>
    /// HTTP message handler that performs mandatory token exchange for non-Databricks tokens.
    /// Blocks requests while exchanging tokens to ensure the exchanged token is used.
    /// Falls back to the original token if the exchange fails.
    /// </summary>
    internal class MandatoryTokenExchangeDelegatingHandler : DelegatingHandler
    {
        private readonly string? _identityFederationClientId;
        private readonly object _tokenLock = new object();
        private readonly ITokenExchangeClient _tokenExchangeClient;
        private string? _currentToken;
        private string? _lastSeenToken;

        /// <summary>
        /// Initializes a new instance of the <see cref="MandatoryTokenExchangeDelegatingHandler"/> class.
        /// </summary>
        /// <param name="innerHandler">The inner handler to delegate to.</param>
        /// <param name="tokenExchangeClient">The client for token exchange operations.</param>
        /// <param name="identityFederationClientId">Optional identity federation client ID.</param>
        public MandatoryTokenExchangeDelegatingHandler(
            HttpMessageHandler innerHandler,
            ITokenExchangeClient tokenExchangeClient,
            string? identityFederationClientId = null)
            : base(innerHandler)
        {
            _tokenExchangeClient = tokenExchangeClient ?? throw new ArgumentNullException(nameof(tokenExchangeClient));
            _identityFederationClientId = identityFederationClientId;
        }

        /// <summary>
        /// Determines if token exchange is needed by checking if the token is a Databricks token.
        /// </summary>
        /// <returns>True if token exchange is needed, false otherwise.</returns>
        private bool NeedsTokenExchange(string bearerToken)
        {
            // If we already started exchange for this token, no need to check again
            if (_lastSeenToken == bearerToken)
            {
                return false;
            }

            // If we can't parse the token as JWT, default to use existing token
            if (!JwtTokenDecoder.TryGetIssuer(bearerToken, out string issuer))
            {
                return false;
            }

            // Check if the issuer matches the current workspace host
            // If the issuer is from the same host, it's already a Databricks token
            string normalizedHost = _tokenExchangeClient.TokenExchangeEndpoint.Replace("/v1/token", "").ToLowerInvariant();
            string normalizedIssuer = issuer.TrimEnd('/').ToLowerInvariant();

            return normalizedIssuer != normalizedHost;
        }

        /// <summary>
        /// Performs token exchange if needed.
        /// </summary>
        /// <param name="bearerToken">The bearer token to potentially exchange.</param>
        /// <param name="cancellationToken">A cancellation token.</param>
        private async Task PerformTokenExchangeIfNeeded(string bearerToken, CancellationToken cancellationToken)
        {
            bool needsExchange;
            lock (_tokenLock)
            {
                if (_lastSeenToken == bearerToken)
                {
                    return;
                }

                needsExchange = NeedsTokenExchange(bearerToken);
                _lastSeenToken = bearerToken;
            }

            if (!needsExchange)
            {
                return;
            }

            try
            {
                TokenExchangeResponse response = await _tokenExchangeClient.ExchangeTokenAsync(
                    bearerToken,
                    _identityFederationClientId,
                    cancellationToken);

                lock (_tokenLock)
                {
                    _currentToken = response.AccessToken;
                }
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine($"Mandatory token exchange failed: {ex.Message}. Continuing with original token.");
            }
        }

        /// <summary>
        /// Sends an HTTP request with the current token.
        /// </summary>
        /// <param name="request">The HTTP request message to send.</param>
        /// <param name="cancellationToken">A cancellation token.</param>
        /// <returns>The HTTP response message.</returns>
        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            string? bearerToken = request.Headers.Authorization?.Parameter;
            if (!string.IsNullOrEmpty(bearerToken))
            {
                await PerformTokenExchangeIfNeeded(bearerToken!, cancellationToken);

                string tokenToUse;
                lock (_tokenLock)
                {
                    tokenToUse = _currentToken ?? bearerToken!;
                }

                request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", tokenToUse);
            }

            return await base.SendAsync(request, cancellationToken);
        }

    }
}
