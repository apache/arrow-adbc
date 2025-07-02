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
using System.Threading;
using System.Threading.Tasks;
using System.IO;

namespace Apache.Arrow.Adbc.Drivers.Databricks
{
    /// <summary>
    /// HTTP handler that implements retry behavior for 503 responses with Retry-After headers.
    /// </summary>
    internal class RetryHttpHandler : DelegatingHandler
    {
        private readonly int _retryTimeoutSeconds;

        /// <summary>
        /// Initializes a new instance of the <see cref="RetryHttpHandler"/> class.
        /// </summary>
        /// <param name="innerHandler">The inner handler to delegate to.</param>
        /// <param name="retryEnabled">Whether retry behavior is enabled.</param>
        /// <param name="retryTimeoutSeconds">Maximum total time in seconds to retry before failing.</param>
        public RetryHttpHandler(HttpMessageHandler innerHandler, int retryTimeoutSeconds)
            : base(innerHandler)
        {
            _retryTimeoutSeconds = retryTimeoutSeconds;
        }

        /// <summary>
        /// Sends an HTTP request to the inner handler with retry logic for 503 responses.
        /// </summary>
        protected override async Task<HttpResponseMessage> SendAsync(
            HttpRequestMessage request,
            CancellationToken cancellationToken)
        {
            // Clone the request content if it's not null so we can reuse it for retries
            var requestContentClone = request.Content != null
                ? await CloneHttpContentAsync(request.Content)
                : null;

            HttpResponseMessage response;
            string? lastErrorMessage = null;
            DateTime startTime = DateTime.UtcNow;
            int totalRetrySeconds = 0;

            do
            {
                // Set the content for each attempt (if needed)
                if (requestContentClone != null && request.Content == null)
                {
                    request.Content = await CloneHttpContentAsync(requestContentClone);
                }

                response = await base.SendAsync(request, cancellationToken);

                // If it's not a 503 response, return immediately
                if (response.StatusCode != HttpStatusCode.ServiceUnavailable)
                {
                    return response;
                }

                // Check for Retry-After header
                if (!response.Headers.TryGetValues("Retry-After", out var retryAfterValues))
                {
                    // No Retry-After header, so return the response as is
                    return response;
                }

                // Parse the Retry-After value
                string retryAfterValue = string.Join(",", retryAfterValues);
                if (!int.TryParse(retryAfterValue, out int retryAfterSeconds) || retryAfterSeconds <= 0)
                {
                    // Invalid Retry-After value, return the response as is
                    return response;
                }

                lastErrorMessage = $"Service temporarily unavailable (HTTP 503). Retry after {retryAfterSeconds} seconds.";

                // Dispose the response before retrying
                response.Dispose();

                // Reset the request content for the next attempt
                request.Content = null;

                // Check if we've exceeded the timeout
                totalRetrySeconds += retryAfterSeconds;
                if (_retryTimeoutSeconds > 0 && totalRetrySeconds > _retryTimeoutSeconds)
                {
                    // We've exceeded the timeout, so break out of the loop
                    break;
                }

                // Wait for the specified retry time
                await Task.Delay(TimeSpan.FromSeconds(retryAfterSeconds), cancellationToken);
            } while (!cancellationToken.IsCancellationRequested);

            // If we get here, we've either exceeded the timeout or been cancelled
            if (cancellationToken.IsCancellationRequested)
            {
                throw new OperationCanceledException("Request cancelled during retry wait", cancellationToken);
            }

            throw new DatabricksException(
                "[SQLState: 08001]" + (lastErrorMessage ?? "Service temporarily unavailable and retry timeout exceeded"),
                 AdbcStatusCode.IOError);
        }

        /// <summary>
        /// Clones an HttpContent object so it can be reused for retries.
        /// per .net guidance, we should not reuse the http content across multiple
        /// request, as it maybe disposed.
        /// </summary>
        private static async Task<HttpContent> CloneHttpContentAsync(HttpContent content)
        {
            var ms = new MemoryStream();
            await content.CopyToAsync(ms);
            ms.Position = 0;

            var clone = new StreamContent(ms);
            if (content.Headers != null)
            {
                foreach (var header in content.Headers)
                {
                    clone.Headers.Add(header.Key, header.Value);
                }
            }
            return clone;
        }
    }
}
