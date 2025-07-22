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
    /// Uses a non-blocking approach to refresh tokens in the background.
    /// </summary>
    internal class TokenExchangeDelegatingHandler : DelegatingHandler
    {
        private readonly string _initialToken;
        private readonly int _tokenRenewLimitMinutes;
        private readonly object _tokenLock = new object();
        private readonly ITokenExchangeClient _tokenExchangeClient;

        private string _currentToken;
        private DateTime _tokenExpiryTime;
        private bool _tokenExchangeAttempted = false;
        private Task? _pendingTokenTask = null;

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
            // 3. We don't already have a pending refresh task
            return !_tokenExchangeAttempted &&
                   DateTime.UtcNow.AddMinutes(_tokenRenewLimitMinutes) >= _tokenExpiryTime &&
                   _pendingTokenTask == null;
        }

        /// <summary>
        /// Starts token renewal in the background if needed.
        /// </summary>
        /// <param name="cancellationToken">A cancellation token.</param>
        private void StartTokenRenewalIfNeeded(CancellationToken cancellationToken)
        {
            if (!NeedsTokenRenewal())
            {
                return;
            }

            bool needsRenewal;
            lock (_tokenLock)
            {
                // Double-check pattern in case another thread renewed while we were waiting
                needsRenewal = NeedsTokenRenewal();
                if (needsRenewal)
                {
                    // Mark that we've attempted token exchange to prevent multiple attempts
                    // Specifically, NeedsTokenRenewal checks this flag
                    _tokenExchangeAttempted = true;
                }
            }

            if (!needsRenewal)
            {
                return;
            }

            // Start token refresh in the background
            _pendingTokenTask = Task.Run(async () =>
            {
                try
                {
                    TokenExchangeResponse response = await _tokenExchangeClient.ExchangeTokenAsync(_initialToken, cancellationToken);

                    // Update the token atomically when ready
                    lock (_tokenLock)
                    {
                        _currentToken = response.AccessToken;
                        _tokenExpiryTime = response.ExpiryTime;
                    }
                }
                catch (Exception ex)
                {
                    // Log the error but continue with the current token
                    // This is to avoid interrupting the operation if token exchange fails
                    System.Diagnostics.Debug.WriteLine($"Token exchange failed: {ex.Message}");
                }
            }, cancellationToken);
        }

        /// <summary>
        /// Sends an HTTP request with the current token.
        /// </summary>
        /// <param name="request">The HTTP request message to send.</param>
        /// <param name="cancellationToken">A cancellation token.</param>
        /// <returns>The HTTP response message.</returns>
        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            StartTokenRenewalIfNeeded(cancellationToken);

            // Use the current token (which might be the old one while refresh is in progress)
            string tokenToUse;
            lock (_tokenLock)
            {
                tokenToUse = _currentToken;
            }

            request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", tokenToUse);
            return await base.SendAsync(request, cancellationToken);
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                // Wait for any pending token task to complete to avoid leaking tasks
                if (_pendingTokenTask != null)
                {
                    try
                    {
                        // Try to wait for the task to complete, but don't block indefinitely
                        _pendingTokenTask.Wait(TimeSpan.FromSeconds(10));
                    }
                    catch (Exception ex)
                    {
                        System.Diagnostics.Debug.WriteLine($"Exception during token task cleanup: {ex.Message}");
                    }
                }
            }

            base.Dispose(disposing);
        }
    }
}
