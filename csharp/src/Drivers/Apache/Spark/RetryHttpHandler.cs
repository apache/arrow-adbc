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
using System.Text;
using Thrift;
using Thrift.Protocol;
using Thrift.Transport;
using Apache.Hive.Service.Rpc.Thrift;

namespace Apache.Arrow.Adbc.Drivers.Apache.Spark
{
    /// <summary>
    /// HTTP handler that implements retry behavior for 503 responses with Retry-After headers.
    /// </summary>
    internal class RetryHttpHandler : DelegatingHandler
    {
        private readonly bool _retryEnabled;
        private readonly int _retryTimeoutSeconds;

        /// <summary>
        /// Initializes a new instance of the <see cref="RetryHttpHandler"/> class.
        /// </summary>
        /// <param name="innerHandler">The inner handler to delegate to.</param>
        /// <param name="retryEnabled">Whether retry behavior is enabled.</param>
        /// <param name="retryTimeoutSeconds">Maximum total time in seconds to retry before failing.</param>
        public RetryHttpHandler(HttpMessageHandler innerHandler, bool retryEnabled, int retryTimeoutSeconds)
            : base(innerHandler)
        {
            _retryEnabled = retryEnabled;
            _retryTimeoutSeconds = retryTimeoutSeconds;
        }

        /// <summary>
        /// Sends an HTTP request to the inner handler with retry logic for 503 responses.
        /// </summary>
        protected override async Task<HttpResponseMessage> SendAsync(
            HttpRequestMessage request,
            CancellationToken cancellationToken)
        {
            // If retry is disabled, just pass through to the inner handler
            if (!_retryEnabled)
            {
                return await base.SendAsync(request, cancellationToken);
            }

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

                // Extract error message from response if possible
                try
                {
                    lastErrorMessage = await ExtractErrorMessageAsync(response);
                }
                catch
                {
                    // If we can't extract the error message, just use a generic one
                    lastErrorMessage = $"Service temporarily unavailable (HTTP 503). Retry after {retryAfterSeconds} seconds.";
                }

                // Check if we've exceeded the timeout
                totalRetrySeconds += retryAfterSeconds;
                if (_retryTimeoutSeconds > 0 && totalRetrySeconds > _retryTimeoutSeconds)
                {
                    // We've exceeded the timeout, so break out of the loop
                    break;
                }

                // Dispose the response before retrying
                response.Dispose();

                // Wait for the specified retry time
                await Task.Delay(TimeSpan.FromSeconds(retryAfterSeconds), cancellationToken);

                // Reset the request content for the next attempt
                request.Content = null;

            } while (!cancellationToken.IsCancellationRequested);

            // If we get here, we've either exceeded the timeout or been cancelled
            if (cancellationToken.IsCancellationRequested)
            {
                throw new OperationCanceledException("Request cancelled during retry wait", cancellationToken);
            }

            // Create a custom exception with the SQL state code and last error message
            var exception = new AdbcException(
                lastErrorMessage ?? "Service temporarily unavailable and retry timeout exceeded",
                AdbcStatusCode.IOError);

            // Add SQL state as part of the message since we can't set it directly
            throw new AdbcException(
                $"[SQLState: 08001] {exception.Message}",
                AdbcStatusCode.IOError);
        }

        /// <summary>
        /// Clones an HttpContent object so it can be reused for retries.
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

        /// <summary>
        /// Attempts to extract the error message from a Thrift TApplicationException in the response body.
        /// </summary>
        private static async Task<string?> ExtractErrorMessageAsync(HttpResponseMessage response)
        {
            if (response.Content == null)
            {
                return null;
            }

            // Check if the content type is application/x-thrift
            if (response.Content.Headers.ContentType?.MediaType != "application/x-thrift")
            {
                // If it's not Thrift, just return the content as a string
                return await response.Content.ReadAsStringAsync();
            }

            try
            {
                // For Thrift content, just return a generic message
                // We can't easily parse the Thrift message without access to the specific methods
                return await response.Content.ReadAsStringAsync();
            }
            catch
            {
                // If we can't read the content, return null
                return null;
            }
        }
    }
}