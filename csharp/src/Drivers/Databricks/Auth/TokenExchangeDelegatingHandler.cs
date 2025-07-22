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
    /// HTTP message handler that automatically refreshes OAuth tokens before they expire.
    /// </summary>
    internal class TokenExchangeDelegatingHandler : DelegatingHandler
    {
        private readonly string _initialToken;
        private readonly int _tokenRenewLimitMinutes;
        private readonly SemaphoreSlim _tokenLock = new SemaphoreSlim(1, 1);
        private readonly ITokenExchangeClient _tokenExchangeClient;

        private string _currentToken;
        private DateTime _tokenExpiryTime;
        private bool _tokenExchangeAttempted = false;

        /// <summary>
        /// Initializes a new instance of the <see cref="TokenExchangeDelegatingHandler"/> class.
        /// </summary>
        /// <param name="innerHandler">The inner handler to delegate to.</param>
        /// <param name="tokenExchangeClient">The client for token exchange operations.</param>
        /// <param name="initialToken">The initial token from the connection string.</param>
        /// <param name="tokenExpiryTime">The expiry time of the initial token.</param>
        /// <param name="tokenRenewLimitMinutes">The minutes before token expiration when we should start renewing the token.</param>
        public TokenExchangeDelegatingHandler(
            HttpMessageHandler innerHandler,
            ITokenExchangeClient tokenExchangeClient,
            string initialToken,
            DateTime tokenExpiryTime,
            int tokenRenewLimitMinutes)
            : base(innerHandler)
        {
            _tokenExchangeClient = tokenExchangeClient ?? throw new ArgumentNullException(nameof(tokenExchangeClient));
            _initialToken = initialToken ?? throw new ArgumentNullException(nameof(initialToken));
            _tokenExpiryTime = tokenExpiryTime;
            _tokenRenewLimitMinutes = tokenRenewLimitMinutes;
            _currentToken = initialToken;
        }

        /// <summary>
        /// Checks if the token needs to be renewed.
        /// </summary>
        /// <returns>True if the token needs to be renewed, false otherwise.</returns>
        private bool NeedsTokenRenewal()
        {
            // Only renew if:
            // 1. We haven't already attempted token exchange (a token can only be renewed once)
            // 2. The token will expire within the renewal limit
            return !_tokenExchangeAttempted &&
                   DateTime.UtcNow.AddMinutes(_tokenRenewLimitMinutes) >= _tokenExpiryTime;
        }

        /// <summary>
        /// Renews the token if needed.
        /// </summary>
        /// <param name="cancellationToken">A cancellation token.</param>
        /// <returns>A task representing the asynchronous operation.</returns>
        private async Task RenewTokenIfNeededAsync(CancellationToken cancellationToken)
        {
            if (!NeedsTokenRenewal())
            {
                return;
            }

            // Acquire the lock to ensure only one thread attempts renewal
            await _tokenLock.WaitAsync(cancellationToken);

            try
            {
                // Double-check pattern in case another thread renewed while we were waiting
                if (!NeedsTokenRenewal())
                {
                    return;
                }

                try
                {
                    _tokenExchangeAttempted = true;

                    TokenExchangeResponse response = await _tokenExchangeClient.ExchangeTokenAsync(_initialToken, cancellationToken);

                    _currentToken = response.AccessToken;
                    _tokenExpiryTime = response.ExpiryTime;
                }
                catch (Exception ex)
                {
                    // Log the error but continue with the current token
                    // This is to avoid interrupting the operation if token exchange fails
                    System.Diagnostics.Debug.WriteLine($"Token exchange failed: {ex.Message}");
                }
            }
            finally
            {
                _tokenLock.Release();
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
            await RenewTokenIfNeededAsync(cancellationToken);
            request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", _currentToken);
            return await base.SendAsync(request, cancellationToken);
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                _tokenLock.Dispose();
            }

            base.Dispose(disposing);
        }
    }
}
